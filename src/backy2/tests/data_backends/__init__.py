from queue import Empty
from unittest.mock import Mock

from backy2.meta_backends.sql import Block
from backy2.tests.testcase import BackendTestCase


class DatabackendTestCase(BackendTestCase):

    def test_save_rm_sync(self):
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

    def test_save_rm_async(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.data_backend.get_all_blob_uids()
        self.assertEqual(0, len(saved_uids))

        data_by_uid = {}
        for _ in range(NUM_BLOBS):
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            uid = self.data_backend.save(data)
            data_by_uid[uid] = data
        uids = list(data_by_uid.keys())
        self.assertEqual(NUM_BLOBS, len(uids))

        self.data_backend.wait_write_finished()

        saved_uids = self.data_backend.get_all_blob_uids()
        self.assertEqual(NUM_BLOBS, len(saved_uids))

        uids_set = set(uids)
        saved_uids_set = set(saved_uids)
        self.assertEqual(NUM_BLOBS, len(uids_set))
        self.assertEqual(NUM_BLOBS, len(saved_uids_set))
        self.assertEqual(0, len(uids_set.symmetric_difference(saved_uids_set)))

        for uid in uids:
            block = Mock(Block, uid=uid)
            self.data_backend.read(block)

        self.data_backend.wait_read_finished()

        for _ in uids:
            block, offset, length, data = self.data_backend.read_get(qtimeout=1)
            self.assertEqual(0, offset)
            self.assertEqual(BLOB_SIZE, length)
            self.assertEqual(data_by_uid[block.uid], data)

        self.assertRaises(Empty, lambda: self.data_backend.read_get(qtimeout=1))

        for uid in uids:
            self.data_backend.rm(uid)
        saved_uids = self.data_backend.get_all_blob_uids()
        self.assertEqual(0, len(saved_uids))

    def _test_rm_many(self):
        NUM_BLOBS = 15

        uids = [self.data_backend.save(b'B',_sync=True) for _ in range(NUM_BLOBS)]

        self.data_backend.rm_many(uids)

        saved_uids = self.data_backend.get_all_blob_uids()
        self.assertEqual(0, len(saved_uids))

    def test_rm_many(self):
        self._test_rm_many()

    def test_rm_many_wo_multidelete(self):
        if hasattr(self.data_backend, 'multi_delete') and self.data_backend.multi_delete:
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

    def test_compression(self):
        if self.data_backend.compression_active is not None:
            uid = self.data_backend.save(b'\0' * 8192,_sync=True)
            self.assertTrue(len(uid) > 0)
            self.data_backend.rm(uid)
        else:
            self.skipTest('compression not enabled')
