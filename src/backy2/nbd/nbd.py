#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import errno
import os
from io import BytesIO

from backy2.logging import logger
from backy2.meta_backend import BlockUid
from backy2.utils import data_hexdigest


class BackyStore:
    """ Makes backy storage look linear.
    Also has a COW method.
    """

    def __init__(self, backy, cachedir, hash_function):
        self.backy = backy
        self.cachedir = cachedir
        self.hash_function = hash_function
        self.blocks = {}  # block list cache by version
        self.block_cache = set()
        self.cow = {}  # contains version_uid: dict() of block id -> block

    def get_versions(self):
        return self.backy.ls()

    def get_version(self, uid):
        return self.backy.meta_backend.get_version(uid)

    def _block_list(self, version, offset, length):
        # get cached blocks data
        if not self.blocks.get(version.uid):
            # Only work with dereferenced blocks
            self.blocks[version.uid] = [block.deref() for block in self.backy.meta_backend.get_blocks_by_version(version.uid)]
        blocks = self.blocks[version.uid]

        block_number = offset // version.block_size
        block_offset = offset % version.block_size

        read_list = []
        while True:
            try:
                block = blocks[block_number]
            except IndexError:
                # We round up the size reported by the NBD server to a multiple of 4096 which is the maximum
                # block size supported by NBD. So we might need to fake up to 4095 bytes (of zeros) here.
                if length > 4095:
                    # Don't throw one of our own exceptions here as we need an exception with an errno value
                    # to communicate it back in the NBD response.
                    raise OSError(errno.EIO)
                read_length = min(block.size-block_offset, length)
                read_list.append((None, 0, read_length))  # hint: return \0s
            else:
                assert block.id == block_number
                if block.uid is None:
                    read_length = min(block.size-block_offset, length)
                    read_list.append((None, 0, read_length))  # hint: return \0s
                else:
                    read_length = min(block.size-block_offset, length)
                    read_list.append((block, block_offset, read_length))
            block_number += 1
            block_offset = 0
            length -= read_length
            assert length >= 0
            if length == 0:
                break

        return read_list

    def _read(self, block, offset=0, length=None):
        if self.backy.data_backend.SUPPORTS_PARTIAL_READS:
            return self.backy.data_backend.read(block, offset=offset, length=length, sync=True)
        else:
            if block.uid not in self.block_cache:
                data = self.backy.data_backend.read(block, sync=True)
                with open(os.path.join(self.cachedir, block.uid), 'wb') as f:
                    f.write(data)
                self.block_cache.add(block.uid)
            with open(os.path.join(self.cachedir, block.uid), 'rb') as f:
                f.seek(offset)
                if length is None:
                    return f.read()
                else:
                    return f.read(length)

    def read(self, version, offset, length):
        read_list = self._block_list(version, offset, length)
        data = []
        for block, offset, length in read_list:
            logger.debug('Reading block {}:{}:{}'.format(block, offset, length))
            if block is None:
                data.append(b'\0'*length)
            else:
                data.append(self._read(block, offset, length))
        return b''.join(data)

    def get_cow_version(self, from_version):
        cow_version = self.backy.clone_version('copy on write', from_version.uid, from_version.uid)
        self.cow[cow_version.uid] = {}  # contains version_uid: dict() of block id -> uid
        return cow_version

    def _update(self, block, data, offset=0):
        # update a given block_uid
        if self.backy.data_backend.SUPPORTS_PARTIAL_WRITES:
            return self.backy.data_backend.update(block, data, offset)
        else:
            # update local copy
            with open(os.path.join(self.cachedir, block.uid), 'r+b') as f:
                f.seek(offset)
                return f.write(data)

    def _save(self, block, data):
        # update a given block_uid
        if self.backy.data_backend.SUPPORTS_PARTIAL_WRITES:
            self.backy.data_backend.save(block, data, sync=True)
        else:
            filename = '{:016x}-{:016x}'.format(block.uid.left, block.uid.right)
            with open(os.path.join(self.cachedir, filename), 'wb') as f:
                f.write(data)
            self.block_cache.add(block.uid)

    def write(self, version, offset, data):
        """ Copy on write backup writer """
        dataio = BytesIO(data)
        cow = self.cow[version.uid.int]
        write_list = self._block_list(version, offset, len(data))
        for block, _offset, length in write_list:
            if block is None:
                logger.warning('Tried to save data beyond device, it will be lost (offset {})'.format(offset))
                continue
            if block.id in cow:
                # the block is already copied, so update it.
                self._update(cow[block.id], dataio.read(length), _offset)
                logger.debug('COW: Updated block {}'.format(block.id))
            else:
                # read the block from the original, update it and write it back
                write_data = BytesIO(self.backy.data_backend.read(block, sync=True))
                write_data.seek(_offset)
                write_data.write(dataio.read(length))
                write_data.seek(0)
                # Save a copy of the changed data and record the changed block UID
                save_block = block.deref()
                save_block.uid = BlockUid(version.uid.int, block.id + 1)
                save_block.checksum = None
                self._save(save_block, write_data.read())
                cow[block.id] = save_block
                logger.debug('COW: Wrote block {} into {}'.format(block.id, save_block.uid))

    def flush(self, cow_version):
        # TODO: Maybe fixate partly?
        pass

    def fixate(self, cow_version):
        # save blocks into version
        logger.info('Fixating version {} with {} blocks (PLEASE WAIT)'.format(
            cow_version.uid,
            len(self.cow[cow_version.uid].items())
            ))

        for block in self.cow[cow_version.uid].values():
            logger.debug('Fixating block {} uid {}'.format(block.id, block.uid))
            data = self._read(block)

            if not self.backy.data_backend.SUPPORTS_PARTIAL_WRITES:
                # dump changed data
                self.backy.data_backend.save(block, data, sync=True)
                logger.debug('Stored block {} uid {}'.format(block.id, block.uid))

            checksum = data_hexdigest(self.hash_function, data)
            self.backy.meta_backend.set_block(block.id, cow_version.uid, block.uid, checksum, len(data), valid=True)

        self.backy.meta_backend.set_version_valid(cow_version.uid)
        self.backy.meta_backend.commit()
        logger.info('Fixation done. Deleting temporary data (PLEASE WAIT)')
        # TODO: Delete COW blocks and also those from block_cache
        if self.backy.data_backend.SUPPORTS_PARTIAL_WRITES:
            for block_uid in self.block_cache:
                # TODO if this block is in the current version (and in no other?)
                # rm this block from cache
                # rm block uid from self.block_cache
                pass
            for block_id, block in self.cow[cow_version.uid].items():
                # TODO: rm block_uid from cache
                pass
        else:
            # backends that support partial writes will be written to directly.
            # So there's no need to cleanup.
            pass
        del(self.cow[cow_version.uid])
        logger.info('Finished.')
