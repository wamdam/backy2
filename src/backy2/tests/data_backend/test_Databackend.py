import logging
import string

import importlib
import random

import shutil
import unittest
from unittest.mock import Mock

import os

from backy2.logging import init_logging
from backy2.config import Config
from backy2.meta_backends.sql import Block

class test_Databackend(unittest.TestCase):
    @classmethod
    def random_string(self, length):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    @classmethod
    def random_bytes(self, length):
        return bytes(random.getrandbits(8) for _ in range(length))

    class TestPath():
        def __init__(self):
            self.path = 'backy2-test_' + test_Databackend.random_string(16)
            os.mkdir(self.path)
            os.mkdir(self.path + '/data')

        def close(self):
            pass
            shutil.rmtree(self.path)

    def setUp(self, config):
        self.testpath = self.TestPath()
        init_logging(None, logging.DEBUG)

        config = self.CONFIG.format(testpath=self.testpath.path)
        self.config_DataBackend = Config(cfg=config, section='DataBackend')
        try:
            DataBackendLib = importlib.import_module(self.config_DataBackend.get('type'))
        except ImportError:
            raise NotImplementedError('DataBackend type {} unsupported.'.format(self.config_DataBackend.get('type')))
        else:
            self.data_backend = DataBackendLib.DataBackend(self.config_DataBackend)

        self.data_backend.rm_many(self.data_backend.get_all_blob_uids())

    def tearDown(self):
        self.data_backend.close()
        self.testpath.close()

    def test_save_rm(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.data_backend.get_all_blob_uids()
        self.assertEqual(0, len(saved_uids))

        data_by_uid = {}
        for _ in range(NUM_BLOBS):
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            uid = self.data_backend.save(data,_sync=True)
            data_by_uid[uid] = data
        uids = list(data_by_uid.keys())
        self.assertEqual(NUM_BLOBS, len(uids))
        
        saved_uids = self.data_backend.get_all_blob_uids()
        self.assertEqual(NUM_BLOBS, len(saved_uids))
        
        uids_set = set(uids)
        saved_uids_set = set(saved_uids)
        self.assertEqual(NUM_BLOBS, len(uids_set))
        self.assertEqual(NUM_BLOBS, len(saved_uids_set))
        self.assertEqual(0, len(uids_set.symmetric_difference(saved_uids_set)))

        for uid in uids:
            block = Mock(Block, uid=uid)
            data = self.data_backend.read(block, sync=True)
            self.assertEqual(data_by_uid[uid], data)

        for uid in uids:
            self.data_backend.rm(uid)
        saved_uids = self.data_backend.get_all_blob_uids()
        self.assertEqual(0, len(saved_uids))

    def _test_rm_many(self):
        NUM_BLOBS = 1500

        uids = [self.data_backend.save(b'B',_sync=True) for _ in range(NUM_BLOBS)]

        self.data_backend.rm_many(uids)

        saved_uids = self.data_backend.get_all_blob_uids()
        self.assertEqual(0, len(saved_uids))

    def test_rm_many(self):
        self._test_rm_many()

    def test_rm_many_wo_multidelete(self):
        if self.data_backend.NAME == 's3_boto3':
            self.data_backend.multi_delete = False
            self._test_rm_many()
        else:
            self.skipTest('not applicable to this backend')

    def test_not_exists(self):
        uid = self.data_backend.save(b'B',_sync=True)
        self.assertTrue(len(uid) > 0)

        block = Mock(Block, uid=uid)
        data = self.data_backend.read(block, sync=True)
        self.assertTrue(len(data) > 0)

        self.data_backend.rm(uid)

        self.assertRaises(FileNotFoundError, lambda: self.data_backend.rm(uid))

        block = Mock(Block, uid=uid)
        self.assertRaises(FileNotFoundError, lambda: self.data_backend.read(block, sync=True))

