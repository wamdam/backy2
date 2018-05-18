# This a port and update of almost all test cases from the original test_main.py.
# Some of them a somewhat redundant now.
import uuid
from unittest import TestCase
from unittest.mock import Mock

import backy2.backy
from backy2.meta_backend import BlockUid
from backy2.tests.testcase import BackendTestCase

BLOCK_SIZE = 1024*4096

class MiscTestCase(BackendTestCase, TestCase):
    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        lockDirectory: {testpath}/lock
        hashFunction: blake2b,digest_size=32
        exportMetadata: True
        dataBackend:
          type: file
          file:
            path: {testpath}/data
          simultaneousWrites: 1
          simultaneousReads: 1
          bandwidthRead: 0
          bandwidthWrite: 0
        metaBackend: 
          engine: sqlite:///{testpath}/backy.sqlite                  
        """

    def test_blocks_from_hints(self):
        hints = [
            (10, 100, True),
            (1024, 2048, True),
            (4096, 3000, True),
            (14000, 10, True),
            (16383, 1025, True),
            (8657, 885, True),
            (32768, 4500, False),
            (65537, 2000, False)
        ]
        block_size = 1024
        sparse_blocks, read_blocks = backy2.backy.blocks_from_hints(hints, block_size)
        self.assertEqual(sparse_blocks, {32, 33, 34, 35, 36, 64, 65})
        self.assertEqual(read_blocks, {0, 1, 2, 4, 5, 6, 8, 9, 13, 15, 16, 36, 64, 65})

    def test_FileBackend_save_read(self):
        backend = self.data_backend
        block = Mock('Block', uid=BlockUid(1, 2))
        backend.save(block, b'test', sync=True)
        self.assertEqual(backend.read(block, sync=True), b'test')
        backend.rm(block.uid)

    def test_metabackend_set_version(self):
        backend = self.meta_backend
        name = 'backup-mysystem1-20150110140015'
        snapshot_name = 'snapname'
        version = backend.set_version(name, snapshot_name, 50000, 5000, True)
        uid = version.uid
        self.assertIsNotNone(version)
        version = backend.get_version(uid)
        self.assertEqual(version.name, name)
        self.assertEqual(version.size, 50000)
        self.assertEqual (version.block_size, 5000)
        self.assertEqual(version.uid, uid)
        self.assertTrue(version.valid)

    def test_metabackend_version_not_found(self):
        backend = self.meta_backend
        self.assertRaises(KeyError, lambda: backend.get_version('123'))

    def test_metabackend_block(self):
        backend = self.meta_backend
        name = 'backup-mysystem1-20150110140015'
        snapshot_name = 'snapname'
        block_uid = BlockUid(1, 2)
        checksum = '1234567890'
        size = 5000
        id = 0
        version = backend.set_version(
            version_name=name,
            snapshot_name=snapshot_name,
            size=50000,
            block_size=5000,
            valid=True
        )
        backend.set_block(id, version.uid, block_uid, checksum, size, True)

        block = backend.get_block(block_uid)

        self.assertEqual(block.checksum, checksum)
        self.assertEqual(block.uid, block_uid)
        self.assertEqual(block.id, id)
        self.assertEqual(block.size, size)
        self.assertEqual(block.version_uid, version.uid)

    def test_metabackend_blocks_by_version(self):
        TESTLEN = 10
        backend = self.meta_backend
        version_name = 'backup-mysystem1-20150110140015'
        snapshot_name = 'snapname'
        version = backend.set_version(
            version_name=version_name,
            snapshot_name=snapshot_name,
            size=TESTLEN * 5000,
            block_size=5000,
            valid=True
        )
        block_uids = [BlockUid(i + 1, i + 2) for i in range(TESTLEN)]
        checksums = [uuid.uuid1().hex for i in range(TESTLEN)]
        size = 5000

        for id in range(TESTLEN):
            backend.set_block(id, version.uid, block_uids[id], checksums[id], size, True)

        blocks = backend.get_blocks_by_version(version.uid)
        self.assertEqual(len(blocks), TESTLEN)

        # blocks are always ordered by id
        for id in range(TESTLEN):
            block = blocks[id]
            self.assertEqual(block.id, id)
            self.assertEqual(block.checksum, checksums[id])
            self.assertEqual(block.uid, block_uids[id])
            self.assertEqual(block.size, size)
            self.assertEqual(block.version_uid, version.uid)
