import random
from unittest.mock import Mock

from backy2.meta_backends.sql import Block, BlockUid
from backy2.tests.testcase import BackendTestCase


class DatabackendTestCase(BackendTestCase):

    def test_save_rm_sync(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.data_backend.list()
        self.assertEqual(0, len(saved_uids))

        uids = [BlockUid(i + 1, i + 2) for i in range(NUM_BLOBS)]
        data_by_uid = {}
        for uid in uids:
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            self.data_backend.save(uid, data, sync=True)
            data_by_uid[uid] = data

        saved_uids = self.data_backend.list()
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
        saved_uids = self.data_backend.list()
        self.assertEqual(0, len(saved_uids))

    def test_save_rm_async(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.data_backend.list()
        self.assertEqual(0, len(saved_uids))

        uids = [BlockUid(i + 1, i + 2) for i in range(NUM_BLOBS)]
        data_by_uid = {}
        for uid in uids:
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            self.data_backend.save(uid, data)
            data_by_uid[uid] = data

        self.data_backend.wait_write_finished()

        saved_uids = self.data_backend.list()
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

        for block, offset, length, data in self.data_backend.read_get_completed(timeout=1):
            self.assertEqual(0, offset)
            self.assertEqual(BLOB_SIZE, length)
            self.assertEqual(data_by_uid[block.uid], data)

        self.assertEqual([], [future for future in self.data_backend.read_get_completed(timeout=1)])

        for uid in uids:
            self.data_backend.rm(uid)
        saved_uids = self.data_backend.list()
        self.assertEqual(0, len(saved_uids))

    def _test_rm_many(self):
        NUM_BLOBS = 15

        uids = [BlockUid(i, i + 1) for i in range(1, NUM_BLOBS)]
        for uid in uids:
            self.data_backend.save(uid, b'B', sync=True)

        self.assertEqual([], self.data_backend.rm_many(uids))

        saved_uids = self.data_backend.list()
        self.assertEqual(0, len(saved_uids))

    def test_rm_many(self):
        self._test_rm_many()

    def test_rm_many_wo_multidelete(self):
        if hasattr(self.data_backend, '_multi_delete') and self.data_backend._multi_delete:
            self.data_backend.multi_delete = False
            self._test_rm_many()
        else:
            self.skipTest('not applicable to this backend')

    def test_not_exists(self):
        uid = BlockUid(1,2)
        self.data_backend.save(uid, b'test_not_exists', sync=True)

        block = Mock(Block, uid=uid)
        data = self.data_backend.read(block, sync=True)
        self.assertTrue(len(data) > 0)

        self.data_backend.rm(uid)

        self.assertRaises(FileNotFoundError, lambda: self.data_backend.rm(uid))

        block = Mock(Block, uid=uid)
        self.assertRaises(FileNotFoundError, lambda: self.data_backend.read(block, sync=True))

    def test_compression(self):
        if self.data_backend.compression_active is not None:
            uid = BlockUid(1, 2)
            self.data_backend.save(uid, b'\0' * 8192, sync=True)
            self.data_backend.rm(uid)
        else:
            self.skipTest('compression not enabled')

    def test_block_uid_to_key(self):
        for i in range(100):
            block_uid = BlockUid(random.randint(1, pow(2,32) - 1), random.randint(1, pow(2,32) - 1))
            key = self.data_backend._block_uid_to_key(block_uid)
            block_uid_2 = self.data_backend._key_to_block_uid(key)
            self.assertEqual(block_uid, block_uid_2)
            self.assertEqual(block_uid.left, block_uid_2.left)
            self.assertEqual(block_uid.right, block_uid_2.right)