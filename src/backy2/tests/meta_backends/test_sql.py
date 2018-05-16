from unittest import TestCase

from backy2.exception import InternalError, NoChange
from backy2.meta_backends.sql import BlockUid
from backy2.tests.testcase import BackendTestCase


class SQLTestCase:

    def test_version(self):
            version = self.meta_backend.set_version(
                version_name='backup-name',
                snapshot_name='snapshot-name',
                size=16 * 1024 * 4096,
                block_size=4 * 1024 * 4096,
                valid=False
            )
            self.meta_backend._commit()

            version = self.meta_backend.get_version(version.uid)
            self.assertEqual('backup-name', version.name)
            self.assertEqual('snapshot-name', version.snapshot_name)
            self.assertEqual(16 * 1024 * 4096, version.size)
            self.assertEqual(4 * 1024 * 4096, version.block_size)
            self.assertFalse(version.valid)
            self.assertFalse(version.protected)

            self.meta_backend.set_version_valid(version.uid)
            version = self.meta_backend.get_version(version.uid)
            self.assertTrue(version.valid)

            self.meta_backend.set_version_invalid(version.uid)
            version = self.meta_backend.get_version(version.uid)
            self.assertFalse(version.valid)

            self.meta_backend.protect_version(version.uid)
            version = self.meta_backend.get_version(version.uid)
            self.assertTrue(version.protected)

            self.meta_backend.unprotect_version(version.uid)
            version = self.meta_backend.get_version(version.uid)
            self.assertFalse(version.protected)

            self.meta_backend.add_tag(version.uid, 'tag-123')
            self.assertRaises(NoChange, lambda: self.meta_backend.add_tag(version.uid, 'tag-123'))

            version = self.meta_backend.get_version(version.uid)
            self.assertEqual(1, len(version.tags))
            self.assertIn(version.uid, map(lambda tag: tag.version_uid, version.tags))
            self.assertIn('tag-123', map(lambda tag: tag.name, version.tags))

            self.meta_backend.remove_tag(version.uid, 'tag-123')
            self.assertRaises(NoChange, lambda: self.meta_backend.remove_tag(version.uid, 'tag-123'))
            version = self.meta_backend.get_version(version.uid)
            self.assertEqual(0, len(version.tags))

            version_uids = {}
            for _ in range(256):
                version = self.meta_backend.set_version(
                    version_name='backup-name',
                    snapshot_name='snapshot-name',
                    size=16 * 1024 * 4096,
                    block_size=4 * 1024 * 4096,
                    valid=False
                )
                version = self.meta_backend.get_version(version.uid)
                self.assertNotIn(version.uid, version_uids)
                version_uids[version.uid] = True


    def test_block(self):
        version = self.meta_backend.set_version(
            version_name='name-' + self.random_string(12),
            snapshot_name='snapshot-name-' + self.random_string(12),
            size=256 * 1024 * 4096,
            block_size=1024 * 4096,
            valid=False
        )
        self.meta_backend._commit()

        checksums = []
        uids = []
        num_blocks = 256
        for id in range(num_blocks):
            checksums.append(self.random_hex(64))
            uids.append(BlockUid(1, id))
            self.meta_backend.set_block(
                id,
                version.uid,
                uids[id],
                checksums[id],
                1024 * 4096,
                True,
                _commit=False,
                _upsert=False)
        self.meta_backend._commit()

        for id, checksum in enumerate(checksums):
            block = self.meta_backend.get_block_by_checksum(checksum)
            self.assertEqual(id, block.id)
            self.assertEqual(version.uid, block.version_uid)
            self.assertEqual(uids[id], block.uid)
            self.assertEqual(checksum, block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        for id, uid in enumerate(uids):
            block = self.meta_backend.get_block(uid)
            self.assertEqual(id, block.id)
            self.assertEqual(version.uid, block.version_uid)
            self.assertEqual(uid, block.uid)
            self.assertEqual(checksums[id], block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        blocks = self.meta_backend.get_blocks_by_version(version.uid)
        self.assertEqual(num_blocks, len(blocks))
        for id, block in enumerate(blocks):
            self.assertEqual(id, block.id)
            self.assertEqual(version.uid, block.version_uid)
            self.assertEqual(uids[id], block.uid)
            self.assertEqual(checksums[id], block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        for id, block in enumerate(blocks):
            dereferenced_block = block.deref()
            self.assertEqual(id, dereferenced_block.id)
            self.assertEqual(version.uid, dereferenced_block.version_uid)
            self.assertEqual(uids[id].left, dereferenced_block.uid.left)
            self.assertEqual(uids[id].right, dereferenced_block.uid.right)
            self.assertEqual(checksums[id], dereferenced_block.checksum)
            self.assertEqual(1024 * 4096, dereferenced_block.size)
            self.assertTrue(dereferenced_block.valid)

        uids_all = self.meta_backend.get_all_block_uids()
        for uid in uids_all:
            self.assertIn(uid, uids)
        self.assertEqual(num_blocks, len(uids_all))

        self.meta_backend.rm_version(version.uid)
        self.meta_backend._commit()
        blocks = self.meta_backend.get_blocks_by_version(version.uid)
        self.assertEqual(0, len(blocks))

        count = 0
        for uids_deleted in self.meta_backend.get_delete_candidates(-1):
            for uid in uids_deleted:
                self.assertIn(uid, uids)
                count += 1
        self.assertEqual(num_blocks, count)

    def test_lock_version(self):
        locking = self.meta_backend.locking()
        self.assertTrue(locking.lock(lock_name='V0000000001', reason='locking test'))
        self.assertRaises(InternalError, lambda: locking.lock(lock_name='V0000000001', reason='locking test'))
        locking.unlock(lock_name='V0000000001')

    def test_lock_global(self):
        locking = self.meta_backend.locking()
        locking.lock(reason='locking test')
        self.assertRaises(InternalError, lambda: locking.lock(reason='locking test'))
        locking.unlock()

    def test_lock_singleton(self):
        locking = self.meta_backend.locking()
        locking2 = self.meta_backend.locking()
        self.assertEqual(locking, locking2)

    def test_is_locked(self):
        locking = self.meta_backend.locking()
        lock = locking.lock(reason='locking test')
        self.assertTrue(locking.is_locked())
        locking.unlock()
        self.assertFalse(locking.is_locked())


class SQLTestCaseSQLLite(SQLTestCase, BackendTestCase, TestCase):

    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        lockDirectory: {testpath}/lock
        hashFunction: blake2b,digest_size=32
        metaBackend: 
          type: sql
          sql:
            engine: sqlite:///{testpath}/backy.sqlite
        """

class SQLTestCasePostgreSQL(SQLTestCase, BackendTestCase, TestCase):

    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        lockDirectory: {testpath}/lock
        hashFunction: blake2b,digest_size=32
        metaBackend: 
          type: sql
          sql:
            engine: postgresql://backy2:verysecret@localhost:15432/backy2
        """