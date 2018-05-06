#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import errno

import os
from io import BytesIO

from backy2.logging import logger
from backy2.utils import data_hexdigest


class BackyStore():
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
                if (length > 4095):
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

    def _save(self, data):
        # update a given block_uid
        if self.backy.data_backend.SUPPORTS_PARTIAL_WRITES:
            return self.backy.data_backend.save(data, sync=True)  # returns block uid
        else:
            new_uid = self.backy.data_backend._uid()
            with open(os.path.join(self.cachedir, new_uid), 'wb') as f:
                f.write(data)
            self.block_cache.add(new_uid)
            return new_uid

    def write(self, version, offset, data):
        """ Copy on write backup writer """
        dataio = BytesIO(data)
        cow = self.cow[version.uid]
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
                block_uid = self._save(write_data.read())
                cow[block.id] = block._replace(uid=block_uid, checksum=None)
                logger.debug('COW: Wrote block {} into {}'.format(block.id, block_uid))

    def flush(self):
        # TODO: Maybe fixate partly?
        pass

    def fixate(self, cow_version):
        # save blocks into version
        logger.info('Fixating version {} with {} blocks (PLEASE WAIT)'.format(
            cow_version.uid,
            len(self.cow[cow_version.uid].items())
            ))

        for block_id, block in self.cow[cow_version.uid].items():
            block_uid = block.uid
            logger.debug('Fixating block {} uid {}'.format(block_id, block_uid))
            data = self._read(block)

            if not self.backy.data_backend.SUPPORTS_PARTIAL_WRITES:
                # dump changed data
                new_block_uid = self.backy.data_backend.save(data, sync=True)
                logger.debug('Stored block {} with local uid {} to uid {}'.format(block_id, block_uid, new_block_uid))
                block_uid = new_block_uid

            checksum = data_hexdigest(self.hash_function, data)
            self.backy.meta_backend.set_block(block_id, cow_version.uid, block_uid, checksum, len(data), valid=True, _commit=False)

        self.backy.meta_backend.set_version_valid(cow_version.uid)
        self.backy.meta_backend._commit()
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
