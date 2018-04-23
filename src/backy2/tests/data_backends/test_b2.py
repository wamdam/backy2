import unittest

from . import DatabackendTestCase


class test_b2(DatabackendTestCase, unittest.TestCase):
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
        type: backy2.data_backends.b2
        
        #compression: backy2.data_backends.compression.zstd
        #compression_default: zstd
        
        #encryption: backy2.data_backends.encryption.aws_s3_cse
        #encryption_materials: {{"MasterKey": "0000000000000000"}}
        #encryption_default: aws_s3_cse
        
        account_id: asdasdasdsadasdasdasdasdadasda
        application_key: dsadsadsadasdasdasdasdasdasdasdaasdsadasd
        bucket_name: elemental-backy2-legolas
        
        simultaneous_writes: 5
        simultaneous_reads: 5

        bandwidth_read: 78643200
        bandwidth_write: 78643200
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
