import unittest

from . import DatabackendTestCase


class test_file(DatabackendTestCase, unittest.TestCase):
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

if __name__ == '__main__':
    unittest.main()
