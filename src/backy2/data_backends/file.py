#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import fnmatch
import hashlib
import os

from backy2.data_backends import DataBackend as _DataBackend
from backy2.exception import UsageError
from backy2.meta_backends.sql import BlockUid
from backy2.utils import makedirs


class DataBackend(_DataBackend):
    """ A DataBackend which stores in files. The files are stored in directories
    starting with the bytes of the generated key. The depth of this structure
    is configurable via the DEPTH parameter, which defaults to 2. """

    NAME = 'file'

    DEPTH = 2
    SPLIT = 2
    SUFFIX = '.blob'
    WRITE_QUEUE_LENGTH = 10
    READ_QUEUE_LENGTH = 20

    SUPPORTS_PARTIAL_READS = True
    SUPPORTS_PARTIAL_WRITES = True
    SUPPORTS_METADATA = True

    def __init__(self, config):
        super().__init__(config)

        our_config = config.get('dataBackend.{}'.format(self.NAME), types=dict)
        self.path = config.get_from_dict(our_config, 'path', types=str)

    @staticmethod
    def _block_uid_to_key(block_uid):
        key_name = '{:016x}-{:016x}'.format(block_uid.left, block_uid.right)
        hash = hashlib.md5(key_name.encode('ascii')).hexdigest()
        return '{}-{}'.format(hash[:8], key_name)

    @staticmethod
    def _key_to_block_uid(key):
        if len(key) != 42:
            raise RuntimeError('Invalid key name {}'.format(key))
        return BlockUid(int(key[9:9 + 16], 16), int(key[26:26 + 16], 16))

    def _path(self, key):
        """ Returns a generated path (depth = self.DEPTH) from a key.
        Example key=831bde887afc11e5b45aa44e314f9270 and depth=2, then
        it returns "83/1b".
        If depth is larger than available bytes, then available bytes
        are returned only as path."""

        parts = [key[i:i+self.SPLIT] for i in range(0, len(key), self.SPLIT)]
        return os.path.join(*parts[:self.DEPTH])

    def _filename(self, key):
        path = os.path.join(self.path, self._path(key))
        return os.path.join(path, key + self.SUFFIX)

    def _write_object(self, key, data, metadata):
        path = os.path.join(self.path, self._path(key))
        filename = self._filename(key)
        try:
            with open(filename, 'wb') as f:
                r = f.write(data)
        except FileNotFoundError:
            makedirs(path)
            with open(filename, 'wb') as f:
                r = f.write(data)

    def _read_object(self, key, offset=0, length=None):
        filename = self._filename(key)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        if offset==0 and length is None:
            with open(filename, 'rb') as f:
                data = f.read()
            return data, {}
        else:
            with open(filename, 'rb') as f:
                if offset:
                    f.seek(offset)
                if length:
                    data = f.read(length)
                else:
                    data = f.read()
            return data, {}

    def update(self, block, data, offset=0):
        with open(self._filename(self.block_uid_to_key(block.uid)), 'r+b') as f:
            f.seek(offset)
            return f.write(data)

    def _rm_object(self, key):
        filename = self._filename(key)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        os.unlink(filename)

    def _rm_many_objects(self, keys):
        """ Deletes many keys from the data backend and returns a list
        of keys that couldn't be deleted.
        """
        errors = []
        for key in keys:
            try:
                self._rm_object(key)
            except FileNotFoundError:
                errors.append(key)
        return errors

    def _list_objects(self, prefix=None):
        if prefix:
            raise UsageError('Specifying a prefix isn\'t supported on file backends.')
        matches = []
        for root, dirnames, filenames in os.walk(self.path):
            for filename in fnmatch.filter(filenames, '*.blob'):
                key = filename.split('.')[0]
                matches.append(key)
        return matches



