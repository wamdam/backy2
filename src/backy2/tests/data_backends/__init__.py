import random
from unittest.mock import Mock

from backy2.meta_backend import Block, BlockUid, VersionUid
from backy2.tests.testcase import BackendTestCase


class DatabackendTestCase(BackendTestCase):

    def test_save_rm_sync(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.data_backend.list_blocks()
        self.assertEqual(0, len(saved_uids))

        blocks = [Mock('Block', uid=BlockUid(i + 1, i + 100), size=BLOB_SIZE, checksum='CHECKSUM') for i in range(NUM_BLOBS)]
        data_by_uid = {}
        for block in blocks:
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            self.data_backend.save(block, data, sync=True)
            data_by_uid[block.uid] = data

        saved_uids = self.data_backend.list_blocks()
        self.assertEqual(NUM_BLOBS, len(saved_uids))

        uids_set = set([block.uid for block in blocks])
        saved_uids_set = set(saved_uids)
        self.assertEqual(NUM_BLOBS, len(uids_set))
        self.assertEqual(NUM_BLOBS, len(saved_uids_set))
        self.assertEqual(0, len(uids_set.symmetric_difference(saved_uids_set)))

        for block in blocks:
            data = self.data_backend.read(block, sync=True)
            self.assertEqual(data_by_uid[block.uid], data)

        for block in blocks:
            self.data_backend.rm(block.uid)
        saved_uids = self.data_backend.list_blocks()
        self.assertEqual(0, len(saved_uids))

    def test_save_rm_async(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.data_backend.list_blocks()
        self.assertEqual(0, len(saved_uids))

        blocks = [Mock('Block', uid=BlockUid(i + 1, i + 100), size=BLOB_SIZE, checksum='CHECKSUM')
                                                                                for i in range(NUM_BLOBS)]
        data_by_uid = {}
        for block in blocks:
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            self.data_backend.save(block, data)
            data_by_uid[block.uid] = data

        self.data_backend.wait_saves_finished()

        for saved_block in self.data_backend.save_get_completed(timeout=1):
            pass

        saved_uids = self.data_backend.list_blocks()
        self.assertEqual(NUM_BLOBS, len(saved_uids))

        uids_set = set([block.uid for block in blocks])
        saved_uids_set = set(saved_uids)
        self.assertEqual(NUM_BLOBS, len(uids_set))
        self.assertEqual(NUM_BLOBS, len(saved_uids_set))
        self.assertEqual(0, len(uids_set.symmetric_difference(saved_uids_set)))

        for block in blocks:
            self.data_backend.read(block)

        self.data_backend.wait_reads_finished()

        for block, data, metadata in self.data_backend.read_get_completed(timeout=1):
            self.assertEqual(data_by_uid[block.uid], data)

        self.assertEqual([], [future for future in self.data_backend.read_get_completed(timeout=1)])

        for block in blocks:
            self.data_backend.rm(block.uid)
        saved_uids = self.data_backend.list_blocks()
        self.assertEqual(0, len(saved_uids))

    def _test_rm_many(self):
        NUM_BLOBS = 15

        blocks = [Mock('Block', uid=BlockUid(i + 1, i + 100), size=1, checksum='CHECKSUM') for i in range(NUM_BLOBS)]
        for block in blocks:
            self.data_backend.save(block, b'B', sync=True)

        self.assertEqual([], self.data_backend.rm_many([block.uid for block in blocks]))

        saved_uids = self.data_backend.list_blocks()
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
        block = Mock(Block, uid=BlockUid(1,2), size=15, checksum='CHECKSUM')
        self.data_backend.save(block, b'test_not_exists', sync=True)

        data = self.data_backend.read(block, sync=True)
        self.assertTrue(len(data) > 0)

        self.data_backend.rm(block.uid)

        self.assertRaises(FileNotFoundError, lambda: self.data_backend.rm(block.uid))
        self.assertRaises(FileNotFoundError, lambda: self.data_backend.read(block, sync=True))

    def test_compression(self):
        if self.data_backend.compression_active is not None:
            block = Mock('Block', uid=BlockUid(1, 2), size=8192, checksum='CHECKSUM')
            self.data_backend.save(block, b'\0' * 8192, sync=True)
            self.data_backend.rm(block.uid)
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

    def test_version_uid_to_key(self):
        for i in range(100):
            version_uid = VersionUid(random.randint(1, pow(2,32) - 1))
            key = self.data_backend._version_uid_to_key(version_uid)
            version_uid_2 = self.data_backend._key_to_version_uid(key)
            self.assertEqual(version_uid, version_uid_2)

    def test_version(self):
        version_uid = VersionUid(1)
        self.data_backend.save_version(version_uid, 'Hallo')
        data = self.data_backend.read_version(version_uid)
        self.assertEqual('Hallo', data)
        version_uids = self.data_backend.list_versions()
        self.assertTrue(len(version_uids) == 1)
        self.assertEqual(version_uid, version_uids[0])
        self.data_backend.rm_version(version_uid)
        version_uids = self.data_backend.list_versions()
        self.assertTrue(len(version_uids) == 0)
