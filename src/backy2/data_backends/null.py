#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import hashlib

import shortuuid


class DataBackend:

    NAME = 'null'

    SUPPORTS_PARTIAL_READS = False
    SUPPORTS_PARTIAL_WRITES = False
    SUPPORTS_METADATA = False

    PACKAGE_PREFIX = 'backy2.data_backends'

    def __init__(self, config):
        self.block_size = config.get('blockSize', types=int)
        self._read_list = []

    @staticmethod
    def _data(block):
        return (block.uid * (block.size // len(block.uid) + 1))[:block.size].encode('ascii')

    # noinspection PyMethodMayBeStatic
    def _uid(self):
        suuid = shortuuid.uuid()
        hash = hashlib.md5(suuid.encode('ascii')).hexdigest()
        return hash[:10] + suuid

    def save(self, data, sync=False):
        return self._uid()

    def read(self, block, offset=0, length=None, sync=False):
        if sync:
            return self._data(block)
        else:
            self._read_list.append(block)

    def read_get_completed(self, timeout=None):
        while self._read_list:
            block = self._read_list.pop()
            yield block, 0, self.block_size, self._data(block)

    def rm(self, uid):
        pass

    def rm_many(self, uids):
        return []

    def get_all_blob_uids(self, prefix=None):
        return []

    def wait_read_finished(self):
        pass

    def wait_write_finished(self):
        pass

    def close(self):
        pass
