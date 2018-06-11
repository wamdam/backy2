import importlib
import logging
import os
import random
import shutil
import string
from binascii import hexlify

from benji.benji import Benji
from benji.config import Config
from benji.data_backends import DataBackend
from benji.exception import ConfigurationError
from benji.logging import init_logging
from benji.metadata import MetadataBackend


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
            self.path = 'benji-test_' + TestCase.random_string(16)
            for dir in [
                    self.path, self.path + '/data', self.path + '/lock', self.path + '/nbd-cache',
                    self.path + '/read-cache'
            ]:
                os.mkdir(dir)

        def close(self):
            pass
            shutil.rmtree(self.path)

    def setUp(self):
        self.testpath = self.TestPath()
        init_logging(None, logging.DEBUG)

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
                raise ConfigurationError('Data backend type {} not found.'.format(name))
            else:
                self.data_backend = DataBackendLib.DataBackend(self.config)
                self.data_backend.rm_many(self.data_backend.list_blocks())
                for version_uid in self.data_backend.list_versions():
                    self.data_backend.rm_version(version_uid)

        name = self.config.get('metadataBackend', None, types=dict)
        if name is not None:
            metadata_backend = MetadataBackend(self.config)
            metadata_backend.initdb(_migratedb=False, _destroydb=True)
            self.metadata_backend = metadata_backend.open(_migratedb=False)

    def tearDown(self):
        if hasattr(self, 'data_backend'):
            uids = self.data_backend.list_blocks()
            self.assertEqual(0, len(uids))
            self.data_backend.close()
        if hasattr(self, 'metadata_backend'):
            self.metadata_backend.close()
        super().tearDown()


class BenjiTestCase(TestCase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def benjiOpen(self, initdb=False, block_size=None, in_memory=False):
        self.benji = Benji(
            self.config, initdb=initdb, _destroydb=initdb, _migratedb=False, block_size=block_size, in_memory=in_memory)
        return self.benji
