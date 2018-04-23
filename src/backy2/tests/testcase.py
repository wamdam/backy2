import logging
import string
from binascii import hexlify

import importlib
import os
import random
import shutil
from functools import partial

from backy2.config import Config
from backy2.logging import init_logging
from backy2.utils import backy_from_config


class TestCase(object):
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
            self.path = 'backy2-test_' + TestCase.random_string(16)
            os.mkdir(self.path)
            os.mkdir(self.path + '/data')
            os.mkdir(self.path + '/lock')

        def close(self):
            pass
            shutil.rmtree(self.path)

class BackendTestCase(TestCase):

    def setUp(self):
        self.testpath = self.TestPath()
        init_logging(None, logging.INFO)

        config = self.CONFIG.format(testpath=self.testpath.path)
        config_DataBackend = Config(cfg=config, section='DataBackend')
        if config_DataBackend.get('type', '') != '':
            try:
                DataBackendLib = importlib.import_module(config_DataBackend.get('type'))
            except ImportError:
                raise NotImplementedError('DataBackend type {} unsupported.'.format(config_DataBackend.get('type')))
            else:
                self.data_backend = DataBackendLib.DataBackend(config_DataBackend)
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

class BackyTestCase(TestCase):

    def setUp(self):
        self.testpath = self.TestPath()
        init_logging(None, logging.INFO)

        config = self.CONFIG.format(testpath=self.testpath.path)
        self.Config = partial(Config, cfg=config)

    def tearDown(self):
        self.testpath.close()

    def backyOpen(self, initdb=False):
        self.backy = backy_from_config(self.Config)(initdb=initdb)
        return self.backy
