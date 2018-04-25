# This a port and update of almost all test cases from the original test_main.py.
# Some of them a somewhat redundant now.
import uuid
from unittest import TestCase
from unittest.mock import Mock

import backy2.backy
from backy2.meta_backends.sql import Block
from backy2.tests.testcase import BackendTestCase

BLOCK_SIZE = 1024*4096

class MiscTestCase(BackendTestCase, TestCase):
    CONFIG = """
        [DEFAULTS]
        logfile: /dev/stderr
        block_size: 4096
        hash_function: sha512
        lock_dir: /tmp
        process_name: backy2-test

        [MetaBackend]
        type: backy2.meta_backends.sql
        engine: sqlite:///{testpath}/backy.sqlite

        [DataBackend]
        type: backy2.data_backends.file
        path: {testpath}/data
        simultaneous_writes: 5
        bandwidth_read: 100000
        bandwidth_write: 100000

        [NBD]
        cachedir: /tmp

        [io_file]
        simultaneous_reads: 5

        [io_rbd]
        ceph_conffile: /etc/ceph/ceph.conf
        simultaneous_reads: 10
        """

    def test_blocks_from_hints(self):
        hints = [
            (10, 100, True),
            (1024, 2048, True),
            (4096, 3000, True),
            (14000, 10, True),
            (16383, 1025, True),
            (8657, 885, True),
            #(35458871, 3624441, True),
        ]
        #         0          1, 2          4, 5, 6       13,          15, 16
        block_size = 1024
        cfh = backy2.backy.blocks_from_hints(hints, block_size)
        self.assertEqual(sorted(list(cfh)), [0, 1, 2, 4, 5, 6, 8, 9, 13, 15, 16])

    def test_FileBackend_path(self):
        uid = 'c2cac25a7afd11e5b45aa44e314f9270'

        backend = self.data_backend
        backend.DEPTH = 2
        backend.SPLIT = 2
        path = backend._path(uid)
        assert path == 'c2/ca'

        backend.DEPTH = 3
        backend.SPLIT = 2
        path = backend._path(uid)
        assert path == 'c2/ca/c2'

        backend.DEPTH = 3
        backend.SPLIT = 3
        path = backend._path(uid)
        assert path == 'c2c/ac2/5a7'

        backend.DEPTH = 3
        backend.SPLIT = 1
        path = backend._path(uid)
        assert path == 'c/2/c'

        backend.DEPTH = 1
        backend.SPLIT = 2
        path = backend._path(uid)
        assert path == 'c2'

    def test_FileBackend_save_read(self):
        backend = self.data_backend
        uid = backend.save(b'test', _sync=True)
        block = Mock(Block, uid=uid)
        self.assertEqual(backend.read(block, sync=True), b'test')

    def test_metabackend_set_version(self):
        backend = self.meta_backend
        name = 'backup-mysystem1-20150110140015'
        snapshot_name = 'snapname'
        uid = backend.set_version(name, snapshot_name, 10, 5000, True)
        self.assertIsNotNone(uid)
        version = backend.get_version(uid)
        self.assertEqual(version.name, name)
        self.assertEqual(version.size, 10)
        self.assertEqual (version.size_bytes, 5000)
        self.assertEqual(version.uid, uid)
        self.assertTrue(version.valid)

    def test_metabackend_version_not_found(self):
        backend = self.meta_backend
        self.assertRaises(KeyError, lambda: backend.get_version('123'))

    def test_metabackend_block(self):
        backend = self.meta_backend
        name = 'backup-mysystem1-20150110140015'
        snapshot_name = 'snapname'
        block_uid = 'asdfgh'
        checksum = '1234567890'
        size = 5000
        id = 0
        version_uid = backend.set_version(name, snapshot_name, 10, 5000, True)
        backend.set_block(id, version_uid, block_uid, checksum, size, True)

        block = backend.get_block(block_uid)

        self.assertEqual(block.checksum, checksum)
        self.assertEqual(block.uid, block_uid)
        self.assertEqual(block.id, id)
        self.assertEqual(block.size, size)
        self.assertEqual(block.version_uid, version_uid)

    def test_metabackend_blocks_by_version(self):
        TESTLEN = 10
        backend = self.meta_backend
        version_name = 'backup-mysystem1-20150110140015'
        snapshot_name = 'snapname'
        version_uid = backend.set_version(version_name, snapshot_name, TESTLEN, 5000, True)
        block_uids = [uuid.uuid1().hex for i in range(TESTLEN)]
        checksums = [uuid.uuid1().hex for i in range(TESTLEN)]
        size = 5000

        for id in range(TESTLEN):
            backend.set_block(id, version_uid, block_uids[id], checksums[id], size, True)

        blocks = backend.get_blocks_by_version(version_uid)
        self.assertEqual(len(blocks), TESTLEN)

        # blocks are always ordered by id
        for id in range(TESTLEN):
            block = blocks[id]
            self.assertEqual(block.id, id)
            self.assertEqual(block.checksum, checksums[id])
            self.assertEqual(block.uid, block_uids[id])
            self.assertEqual(block.size, size)
            self.assertEqual(block.version_uid, version_uid)
