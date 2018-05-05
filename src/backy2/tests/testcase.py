import logging
import string
from binascii import hexlify

import importlib
import os
import random
import shutil

from backy2.config import Config
from backy2.data_backends import DataBackend
from backy2.logging import init_logging
from backy2.meta_backends import MetaBackend
from backy2.utils import backy_from_config


class TestCase():
    @staticmethod
    def random_string(length):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def random_bytes(length):
        return bytes(random.getrandbits(8) for _ in range(length))

    @staticmethod
    def random_hex(length):
        return hexlify(bytes(random.getrandbits(8) for _ in range(length))).decode('ascii')

    class TestPath():
        def __init__(self):
            self.path = 'backy2-test_' + TestCase.random_string(16)
            for dir in [self.path, self.path + '/data', self.path + '/lock', self.path + '/nbd', self.path + '/nbd/cache']:
                os.mkdir(dir)

        def close(self):
            pass
            shutil.rmtree(self.path)

    def setUp(self):
        self.testpath = self.TestPath()
        init_logging(None, logging.INFO)

        self.config = Config(cfg=self.CONFIG.format(testpath=self.testpath.path), merge_defaults=False)

    def tearDown(self):
        self.testpath.close()

class BackendTestCase(TestCase):

    def setUp(self):
        super().setUp()

        name = self.config.get('dataBackend.type', None, types=str)
        if name is not None:
            try:
                DataBackendLib = importlib.import_module('{}.{}'.format(DataBackend.PACKAGE_PREFIX, name))
            except ImportError:
                raise NotImplementedError('Data backend type {} unsupported'.format(name))
            else:
                self.data_backend = DataBackendLib.DataBackend(self.config)
                self.data_backend.rm_many(self.data_backend.get_all_blob_uids())

        name = self.config.get('metaBackend.type', None, types=str)
        if name is not None:
            try:
                MetaBackendLib = importlib.import_module('{}.{}'.format(MetaBackend.PACKAGE_PREFIX, name))
            except ImportError:
                raise NotImplementedError('Meta backend type {} unsupported'.format(name))
            else:
                meta_backend = MetaBackendLib.MetaBackend(self.config)
                meta_backend.initdb(_migratedb=False)
                self.meta_backend = meta_backend.open(_migratedb=False)

    def tearDown(self):
        if hasattr(self, 'data_backend'):
            uids = self.data_backend.get_all_blob_uids()
            self.assertEqual(0, len(uids))
            self.data_backend.close()
        if hasattr(self, 'meta_backend'):
            self.meta_backend.close()
        super().tearDown()

class BackyTestCase(TestCase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def backyOpen(self, initdb=False):
        self.backy = backy_from_config(self.config)(initdb=initdb, _destroydb=initdb, _migratedb=False)
        return self.backy
