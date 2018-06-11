from unittest import TestCase

from benji.exception import InternalError, NoChange
from benji.metadata import BlockUid, VersionUid
from benji.tests.testcase import BackendTestCase


class SQLTestCase:

    def test_version(self):
        version = self.metadata_backend.set_version(
            version_name='backup-name',
            snapshot_name='snapshot-name',
            size=16 * 1024 * 4096,
            block_size=4 * 1024 * 4096,
            valid=False)
        self.metadata_backend.commit()

        version = self.metadata_backend.get_version(version.uid)
        self.assertEqual('backup-name', version.name)
        self.assertEqual('snapshot-name', version.snapshot_name)
        self.assertEqual(16 * 1024 * 4096, version.size)
        self.assertEqual(4 * 1024 * 4096, version.block_size)
        self.assertFalse(version.valid)
        self.assertFalse(version.protected)

        self.metadata_backend.set_version_valid(version.uid)
        version = self.metadata_backend.get_version(version.uid)
        self.assertTrue(version.valid)

        self.metadata_backend.set_version_invalid(version.uid)
        version = self.metadata_backend.get_version(version.uid)
        self.assertFalse(version.valid)

        self.metadata_backend.protect_version(version.uid)
        version = self.metadata_backend.get_version(version.uid)
        self.assertTrue(version.protected)

        self.metadata_backend.unprotect_version(version.uid)
        version = self.metadata_backend.get_version(version.uid)
        self.assertFalse(version.protected)

        self.metadata_backend.add_tag(version.uid, 'tag-123')
        self.assertRaises(NoChange, lambda: self.metadata_backend.add_tag(version.uid, 'tag-123'))

        version = self.metadata_backend.get_version(version.uid)
        self.assertEqual(1, len(version.tags))
        self.assertIn(version.uid, map(lambda tag: tag.version_uid, version.tags))
        self.assertIn('tag-123', map(lambda tag: tag.name, version.tags))

        self.metadata_backend.rm_tag(version.uid, 'tag-123')
        self.assertRaises(NoChange, lambda: self.metadata_backend.rm_tag(version.uid, 'tag-123'))
        version = self.metadata_backend.get_version(version.uid)
        self.assertEqual(0, len(version.tags))

        version_uids = {}
        for _ in range(256):
            version = self.metadata_backend.set_version(
                version_name='backup-name',
                snapshot_name='snapshot-name',
                size=16 * 1024 * 4096,
                block_size=4 * 1024 * 4096,
                valid=False)
            version = self.metadata_backend.get_version(version.uid)
            self.assertNotIn(version.uid, version_uids)
            version_uids[version.uid] = True

    def test_block(self):
        version = self.metadata_backend.set_version(
            version_name='name-' + self.random_string(12),
            snapshot_name='snapshot-name-' + self.random_string(12),
            size=256 * 1024 * 4096,
            block_size=1024 * 4096,
            valid=False)
        self.metadata_backend.commit()

        checksums = []
        uids = []
        num_blocks = 256
        for id in range(num_blocks):
            checksums.append(self.random_hex(64))
            uids.append(BlockUid(1, id))
            self.metadata_backend.set_block(id, version.uid, uids[id], checksums[id], 1024 * 4096, True)
        self.metadata_backend.commit()

        for id, checksum in enumerate(checksums):
            block = self.metadata_backend.get_block_by_checksum(checksum)
            self.assertEqual(id, block.id)
            self.assertEqual(version.uid, block.version_uid)
            self.assertEqual(uids[id], block.uid)
            self.assertEqual(checksum, block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        for id, uid in enumerate(uids):
            block = self.metadata_backend.get_block(uid)
            self.assertEqual(id, block.id)
            self.assertEqual(version.uid, block.version_uid)
            self.assertEqual(uid, block.uid)
            self.assertEqual(checksums[id], block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        blocks = self.metadata_backend.get_blocks_by_version(version.uid)
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

        uids_all = self.metadata_backend.get_all_block_uids()
        for uid in uids_all:
            self.assertIn(uid, uids)
        self.assertEqual(num_blocks, len(uids_all))

        self.metadata_backend.rm_version(version.uid)
        self.metadata_backend.commit()
        blocks = self.metadata_backend.get_blocks_by_version(version.uid)
        self.assertEqual(0, len(blocks))

        count = 0
        for uids_deleted in self.metadata_backend.get_delete_candidates(-1):
            for uid in uids_deleted:
                self.assertIn(uid, uids)
                count += 1
        self.assertEqual(num_blocks, count)

    def test_lock_version(self):
        locking = self.metadata_backend.locking()
        locking.lock_version(VersionUid(1), reason='locking test')
        self.assertRaises(InternalError, lambda: locking.lock_version(VersionUid(1), reason='locking test'))
        locking.unlock_version(VersionUid(1))

    def test_lock_global(self):
        locking = self.metadata_backend.locking()
        locking.lock(reason='locking test')
        self.assertRaises(InternalError, lambda: locking.lock(reason='locking test'))
        locking.unlock()

    def test_lock_singleton(self):
        locking = self.metadata_backend.locking()
        locking2 = self.metadata_backend.locking()
        self.assertEqual(locking, locking2)

    def test_is_locked(self):
        locking = self.metadata_backend.locking()
        lock = locking.lock(reason='locking test')
        self.assertTrue(locking.is_locked())
        locking.unlock()
        self.assertFalse(locking.is_locked())

    def test_is_version_locked(self):
        locking = self.metadata_backend.locking()
        lock = locking.lock_version(VersionUid(1), reason='locking test')
        self.assertTrue(locking.is_version_locked(VersionUid(1)))
        locking.unlock_version(VersionUid(1))
        self.assertFalse(locking.is_version_locked(VersionUid(1)))

    def test_lock_version_context_manager(self):
        locking = self.metadata_backend.locking()
        with locking.with_version_lock(VersionUid(1), reason='locking test'):
            with self.assertRaises(InternalError):
                locking.lock_version(VersionUid(1), reason='locking test')
        locking.lock_version(VersionUid(1), reason='locking test')
        locking.unlock_version(VersionUid(1))

    def test_lock_context_manager(self):
        locking = self.metadata_backend.locking()
        with locking.with_lock(reason='locking test'):
            with self.assertRaises(InternalError):
                locking.lock(reason='locking test')
        locking.lock(reason='locking test')
        locking.unlock()

    def test_version_uid_create_from_readable(self):
        self.assertEqual(VersionUid(1), VersionUid.create_from_readables(1))
        self.assertEqual(VersionUid(1), VersionUid.create_from_readables('V1'))
        uids = VersionUid.create_from_readables(['V1'])
        self.assertTrue(isinstance(uids, list))
        self.assertTrue(len(uids) == 1)
        self.assertEqual(VersionUid(1), uids[0])
        uids = VersionUid.create_from_readables(['V1', 'V2', 3])
        self.assertTrue(isinstance(uids, list))
        self.assertTrue(len(uids) == 3)
        self.assertEqual(VersionUid(1), uids[0])
        self.assertEqual(VersionUid(2), uids[1])
        self.assertEqual(VersionUid(3), uids[2])


class SQLTestCaseSQLLite(SQLTestCase, BackendTestCase, TestCase):

    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        hashFunction: blake2b,digest_size=32
        metadataBackend: 
          engine: sqlite:///{testpath}/benji.sqlite
        """


class SQLTestCasePostgreSQL(SQLTestCase, BackendTestCase, TestCase):

    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        lockDirectory: {testpath}/lock
        hashFunction: blake2b,digest_size=32
        metadataBackend: 
          engine: postgresql://benji:verysecret@localhost:15432/benji
        """
