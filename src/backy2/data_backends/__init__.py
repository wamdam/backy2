#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import importlib
import json

class DataBackend():
    """ Holds BLOBs
    """

    # Does this filestore support partial reads of blocks?
    #
    # # Does this filestore support partial reads of blocks?
    _SUPPORTS_PARTIAL_READS = False
    _SUPPORTS_PARTIAL_WRITES = False

    _COMPRESSION_HEADER = "x-backy2-comp-type"
    _ENCRYPTION_HEADER = "x-backy2-enc-type"

    def __init__(self, config):
        self.encryption = {}
        self.compression = {}
        self.encryption_default = None
        self.compression_default = None

        encryption_types = config.get('encryption', '')
        if encryption_types != '':
            encryption_types = [type.strip() for type in encryption_types.split(',')]
            for encryption_type in encryption_types:
                materials = json.loads(config.get('encryption_materials', '{}'))
                try:
                    encryption_module = importlib.import_module(encryption_type)
                except ImportError:
                    raise NotImplementedError('encryption type {} is not supported'.format(encryption_type))
                else:
                    self.encryption[encryption_module.Encryption.NAME] = encryption_module.Encryption(materials)

        encryption_default = config.get('encryption_default', '')
        if encryption_default != '' and encryption_default != 'none':
            if encryption_default in self.encryption:
                self.encryption_default = self.encryption[encryption_default]
            else:
                raise NotImplementedError('encryption default {} is not supported'.format(encryption_type))

        compression_types = config.get('compression', '')
        if compression_types != '':
            compression_types = [type.strip() for type in compression_types.split(',')]
            for compression_type in compression_types:
                materials = json.loads(config.get('compression_materials', '{}'))
                try:
                    compression_module = importlib.import_module(compression_type)
                except ImportError:
                    raise NotImplementedError('compression type {} is not supported'.format(compression_type))
                else:
                    self.compression[compression_module.Compression.NAME] = compression_module.Compression(materials)

        compression_default = config.get('compression_default', '')
        if compression_default != '' and compression_default != 'none':
            if compression_default in self.compression:
                self.compression_default = self.compression[compression_default]
            else:
                raise NotImplementedError('compression default {} is not supported'.format(compression_type))

    def save(self, data):
        """ Saves data, returns unique ID """
        raise NotImplementedError()


    def update(self, uid, data, offset=0):
        """ Updates data, returns written bytes.
        This is only available on *some* data backends.
        """
        raise NotImplementedError()


    def read(self, uid, offset=0, length=None):
        """ Returns b'<data>' or raises FileNotFoundError.
        With length==None, all known data is read for this uid.
        """
        raise NotImplementedError()


    def rm(self, uid):
        """ Deletes a block """
        raise NotImplementedError()


    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """
        raise NotImplementedError()


    def get_all_blob_uids(self, prefix=None):
        """ Get all existing blob uids """
        raise NotImplementedError()

    def encrypt(self, data):
        if self.encryption_default is not None:
            data, metadata = self.encryption_default.encrypt(data)
            metadata[self._ENCRYPTION_HEADER] = self.encryption_default.NAME
            return data, metadata
        else:
            return data, {}

    def decrypt(self, data, metadata):
        if self._ENCRYPTION_HEADER in metadata:
            type = metadata[self._ENCRYPTION_HEADER]
            if type in self.encryption:
                return self.encryption[type].decrypt(data, metadata)
            else:
                raise IOError('unsupported encryption type {}'.format(type))
        else:
            return data

    def compress(self, data):
        if self.compression_default is not None:
            data, metadata = self.compression_default.compress(data)
            metadata[self._COMPRESSION_HEADER] = self.compression_default.NAME
            return data, metadata
        else:
            return data, {}

    def uncompress(self, data, metadata):
        if self._COMPRESSION_HEADER in metadata:
            type = metadata[self._COMPRESSION_HEADER]
            if type in self.compression:
                return self.compression[type].uncompress(data, metadata)
            else:
                raise IOError('unsupported compression type {}'.format(type))
        else:
            return data

    def close(self):
        pass
