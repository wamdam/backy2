#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import fnmatch
import os

from backy2.data_backends import DataBackend as _DataBackend


# This backend assumes the slash ("/") as the path separator!
class DataBackend(_DataBackend):
    """ A DataBackend which stores in files. The files are stored in directories
    starting with the bytes of the generated key. The depth of this structure
    is configurable via the DEPTH parameter, which defaults to 2. """

    NAME = 'file'

    WRITE_QUEUE_LENGTH = 10
    READ_QUEUE_LENGTH = 20

    SUPPORTS_PARTIAL_READS = True
    SUPPORTS_PARTIAL_WRITES = True
    SUPPORTS_METADATA = False

    _SUFFIX = '.blob'

    def __init__(self, config):
        super().__init__(config)

        if os.sep != '/':
            raise RuntimeError('The file data backend only works with / as path separator.')

        our_config = config.get('dataBackend.{}'.format(self.NAME), types=dict)
        self.path = config.get_from_dict(our_config, 'path', types=str)

        # Ensure that self.path ends in a slash
        if not self.path.endswith('/'):
            self.path = self.path + '/'

    def _write_object(self, key, data, metadata):
        filename = os.path.join(self.path, key) + self._SUFFIX

        try:
            with open(filename, 'wb') as f:
                f.write(data)
        except FileNotFoundError:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb') as f:
                f.write(data)

    def _read_object(self, key, offset=0, length=None):
        filename = os.path.join(self.path, key) + self._SUFFIX

        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))

        if offset == 0 and length is None:
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
        filename = os.path.join(self.path, self.block_uid_to_key(block.uid)) + self._SUFFIX

        with open(filename, 'r+b') as f:
            f.seek(offset)
            return f.write(data)

    def _rm_object(self, key):
        filename = os.path.join(self.path, key) + self._SUFFIX

        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        os.unlink(filename)

    def _rm_many_objects(self, keys):
        errors = []
        for key in keys:
            try:
                self._rm_object(key)
            except FileNotFoundError:
                errors.append(key)
        return errors

    def _list_objects(self, prefix):
        matches = []
        for root, dirnames, filenames in os.walk(os.path.join(self.path, prefix)):
            for filename in fnmatch.filter(filenames, '*' + self._SUFFIX):
                key = (os.path.join(root, filename[:-len(self._SUFFIX)]))[len(self.path):]
                matches.append(key)
        return matches



