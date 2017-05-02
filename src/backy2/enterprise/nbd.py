#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.logging import logger
from io import BytesIO
import os


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
        self.cow = {}  # contains version_uid: dict() of block id -> uid


    def get_versions(self):
        return self.backy.ls()


    def get_version(self, uid):
        return self.backy.meta_backend.get_version(uid)


    def _block_list(self, version_uid, offset, length):
        # get cached blocks data
        if not self.blocks.get(version_uid):
            self.blocks[version_uid] = self.backy.meta_backend.get_blocks_by_version(version_uid)
        blocks = self.blocks[version_uid]

        block_number = offset // self.backy.block_size
        block_offset = offset % self.backy.block_size

        read_list = []
        while True:
            try:
                block = blocks[block_number]
            except IndexError:
                # In case the backup file is not a multiple of 4096 in size,
                # we need to fake blocks to the end until it matches. That means,
                # that we return b'\0' until the block size is reached.
                # This is a nbd (or even block device) limitation
                block = None
                read_length = length
                read_list.append((None, 0, length))  # hint: return \0s
            else:
                assert block.id == block_number
                if block.uid is None:
                    block = None
                    read_length = length
                    read_list.append((None, 0, length))  # hint: return \0s
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


    def _read(self, block_uid, offset=0, length=None):
        if self.backy.data_backend._SUPPORTS_PARTIAL_READS:
            return self.backy.data_backend.read_raw(block_uid, offset=offset, length=length)
        else:
            if block_uid not in self.block_cache:
                data = self.backy.data_backend.read_raw(block_uid)
                open(os.path.join(self.cachedir, block_uid), 'wb').write(data)
                self.block_cache.add(block_uid)
            with open(os.path.join(self.cachedir, block_uid), 'rb') as f:
                f.seek(offset)
                if length is None:
                    return f.read()
                else:
                    return f.read(length)


    def read(self, version_uid, offset, length):
        read_list = self._block_list(version_uid, offset, length)
        data = []
        for block, offset, length in read_list:
            logger.debug('Reading block {}:{}:{}'.format(block, offset, length))
            if block is None:
                data.append(b'\0'*length)
            else:
                data.append(self._read(block.uid, offset, length))
        return b''.join(data)


    def get_cow_version(self, from_version):
        cow_version_uid = self.backy._prepare_version(
            'copy on write', from_version.uid,
            from_version.size_bytes, from_version.uid)
        self.cow[cow_version_uid] = {}  # contains version_uid: dict() of block id -> uid
        return cow_version_uid


    def _update(self, block_uid, data, offset=0):
        # update a given block_uid
        if self.backy.data_backend._SUPPORTS_PARTIAL_WRITES:
            return self.backy.data_backend.update(block_uid, data, offset)
        else:
            # update local copy
            with open(os.path.join(self.cachedir, block_uid), 'r+b') as f:
                f.seek(offset)
                return f.write(data)


    def _save(self, data):
        # update a given block_uid
        if self.backy.data_backend._SUPPORTS_PARTIAL_WRITES:
            return self.backy.data_backend.save(data, _sync=True)  # returns block uid
        else:
            new_uid = self.backy.data_backend._uid()
            with open(os.path.join(self.cachedir, new_uid), 'wb') as f:
                f.write(data)
            self.block_cache.add(new_uid)
            return new_uid


    def write(self, version_uid, offset, data):
        """ Copy on write backup writer """
        dataio = BytesIO(data)
        cow = self.cow[version_uid]
        write_list = self._block_list(version_uid, offset, len(data))
        for block, _offset, length in write_list:
            if block is None:
                logger.warning('Tried to save data beyond device (offset {})'.format(offset))
                continue  # raise? That'd be a write outside the device...
            if block.id in cow:
                # the block is already copied, so update it.
                block_uid = cow[block.id]
                self._update(block_uid, dataio.read(length), _offset)
                logger.debug('Updated cow changed block {} into {})'.format(block.id, block_uid))
            else:
                # read the block from the original, update it and write it back
                write_data = BytesIO(self.backy.data_backend.read_raw(block.uid))
                write_data.seek(_offset)
                write_data.write(dataio.read(length))
                write_data.seek(0)
                block_uid = self._save(write_data.read())
                cow[block.id] = block_uid
                logger.debug('Wrote cow changed block {} into {})'.format(block.id, block_uid))


    def flush(self):
        # TODO: Maybe fixate partly?
        pass


    def fixate(self, cow_version_uid):
        # save blocks into version
        logger.info('Fixating version {} with {} blocks (PLEASE WAIT)'.format(
            cow_version_uid,
            len(self.cow[cow_version_uid].items())
            ))
        for block_id, block_uid in self.cow[cow_version_uid].items():
            logger.debug('Fixating block {} uid {}'.format(block_id, block_uid))
            data = self._read(block_uid)
            checksum = self.hash_function(data).hexdigest()
            if not self.backy.data_backend._SUPPORTS_PARTIAL_WRITES:
                # dump changed data
                new_uid = self.backy.data_backend.save(data, _sync=True)
                logger.debug('Stored block {} with local uid {} to uid {}'.format(block_id, block_uid, new_uid))
                block_uid = new_uid

            self.backy.meta_backend.set_block(block_id, cow_version_uid, block_uid, checksum, len(data), valid=1, _commit=False)
        self.backy.meta_backend.set_version_valid(cow_version_uid)
        self.backy.meta_backend._commit()
        logger.info('Fixation done. Deleting temporary data (PLEASE WAIT)')
        # TODO: Delete COW blocks and also those from block_cache
        if self.backy.data_backend._SUPPORTS_PARTIAL_WRITES:
            for block_uid in self.block_cache:
                # TODO if this block is in the current version (and in no other?)
                # rm this block from cache
                # rm block uid from self.block_cache
                pass
            for block_id, block_uid in self.cow[cow_version_uid].items():
                # TODO: rm block_uid from cache
                pass
        else:
            # backends that support partial writes will be written to directly.
            # So there's no need to cleanup.
            pass
        del(self.cow[cow_version_uid])
        logger.info('Finished.')


