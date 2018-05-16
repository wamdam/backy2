#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import hashlib

from backy2.meta_backends.sql import BlockUid


class DataBackend:

    NAME = 'null'

    SUPPORTS_PARTIAL_READS = False
    SUPPORTS_PARTIAL_WRITES = False
    SUPPORTS_METADATA = False

    PACKAGE_PREFIX = 'backy2.data_backends'

    def __init__(self, config):
        # FIXME: This won't work when something other than the default block size is used
        self.block_size = config.get('blockSize', types=int)
        self._read_list = []

    @staticmethod
    def block_uid_to_key(block_uid):
        key_name = '{:016x}-{:016x}'.format(block_uid.left, block_uid.right)
        hash = hashlib.md5(key_name.encode('ascii')).hexdigest()
        return '{}-{}'.format(hash[:8], key_name)

    @staticmethod
    def key_to_block_uid(key):
        if len(key) != 42:
            raise RuntimeError('Invalid key name {}'.format(key))
        return BlockUid(int(key[9:9 + 16], 16), int(key[26:26 + 16], 16))

    def _data(self, block):
        key = self.block_uid_to_key(block.uid)
        return (key * (block.size // len(key) + 1))[:block.size].encode('ascii')

    # noinspection PyMethodMayBeStatic
    def save(self, block_uid, data, sync=False):
        pass

    def read(self, block, offset=0, length=None, sync=False):
        if sync:
            return self._data(block)
        else:
            self._read_list.append(block)

    def read_get_completed(self, timeout=None):
        while self._read_list:
            block = self._read_list.pop()
            yield block, 0, self.block_size, self._data(block)

    # noinspection PyMethodMayBeStatic
    def rm(self, uid):
        pass

    # noinspection PyMethodMayBeStatic
    def rm_many(self, uids):
        return []

    # noinspection PyMethodMayBeStatic
    def get_all_blob_uids(self, prefix=None):
        return []

    # noinspection PyMethodMayBeStatic
    def wait_read_finished(self):
        pass

    # noinspection PyMethodMayBeStatic
    def wait_write_finished(self):
        pass

    # noinspection PyMethodMayBeStatic
    def close(self):
        pass
