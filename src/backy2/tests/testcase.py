import logging
import string
from binascii import hexlify

import importlib
import os
import random
import shutil

from backy2.config import Config
from backy2.logging import init_logging


class BackyTestCase(object):
    @classmethod
    def random_string(self, length):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    @classmethod
    def random_bytes(self, length):
        return bytes(random.getrandbits(8) for _ in range(length))

    @classmethod
    def random_hex(self, length):
        return hexlify(bytes(random.getrandbits(8) for _ in range(length)))

    class TestPath():
        def __init__(self):
            self.path = 'backy2-test_' + BackyTestCase.random_string(16)
            os.mkdir(self.path)
            os.mkdir(self.path + '/data')

        def close(self):
            pass
            shutil.rmtree(self.path)

    def setUp(self, config):
        self.testpath = self.TestPath()
        init_logging(None, logging.INFO)

        config = self.CONFIG.format(testpath=self.testpath.path)
        self.config_DataBackend = Config(cfg=config, section='DataBackend')
        if self.config_DataBackend.get('type', '') != '':
            try:
                DataBackendLib = importlib.import_module(self.config_DataBackend.get('type'))
            except ImportError:
                raise NotImplementedError('DataBackend type {} unsupported.'.format(self.config_DataBackend.get('type')))
            else:
                self.data_backend = DataBackendLib.DataBackend(self.config_DataBackend)
                self.data_backend.rm_many(self.data_backend.get_all_blob_uids())

        config_MetaBackend = Config(cfg=config, section='MetaBackend')
        if config_MetaBackend.get('type', '') != '':
            try:
                MetaBackendLib = importlib.import_module(config_MetaBackend.get('type'))
            except ImportError:
                raise NotImplementedError('MetaBackend type {} unsupported.'.format(config_MetaBackend.get('type')))
            else:
                meta_backend = MetaBackendLib.MetaBackend(config_MetaBackend)
                meta_backend.initdb()
                self.meta_backend = meta_backend.open()

    def tearDown(self):
        if hasattr(self, 'data_backend'):
            self.data_backend.close()
        if hasattr(self, 'meta_backend'):
            self.meta_backend.close()
        self.testpath.close()

