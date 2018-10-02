#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os

from benji.data_backends import DataBackend as _DataBackend


# This backend assumes the slash ("/") as the path separator!
class DataBackend(_DataBackend):
    """ A DataBackend which stores in files. The files are stored in directories
    starting with the bytes of the generated key. The depth of this structure
    is configurable via the DEPTH parameter, which defaults to 2. """

    NAME = 'file'

    WRITE_QUEUE_LENGTH = 10
    READ_QUEUE_LENGTH = 20

    def __init__(self, config):
        super().__init__(config)

        if os.sep != '/':
            raise RuntimeError('The file data backend only works with / as path separator.')

        our_config = config.get('dataBackend.{}'.format(self.NAME), types=dict)
        self.path = config.get_from_dict(our_config, 'path', types=str)

        # Ensure that self.path ends in a slash
        if not self.path.endswith('/'):
            self.path = self.path + '/'

    def _write_object(self, key, data):
        filename = os.path.join(self.path, key)

        try:
            with open(filename, 'wb') as f:
                f.write(data)
        except FileNotFoundError:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb') as f:
                f.write(data)

    def _read_object(self, key):
        filename = os.path.join(self.path, key)

        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))

        with open(filename, 'rb') as f:
            data = f.read()

        return data

    def _read_object_length(self, key):
        filename = os.path.join(self.path, key)

        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))

        return os.path.getsize(filename)

    def _rm_object(self, key):
        filename = os.path.join(self.path, key)

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
            for filename in filenames:
                key = (os.path.join(root, filename))[len(self.path):]
                matches.append(key)
        return matches
