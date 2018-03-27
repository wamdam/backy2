import unittest

from .test_Databackend import test_Databackend

class test_Databackend(test_Databackend):
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
        type: backy2.data_backends.s3_boto3
        
        aws_access_key_id: minio
        aws_secret_access_key: minio123
        host: 127.0.0.1
        port: 9901
        is_secure: false
        bucket_name: backy2
        
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

    def setUp(self):
        super(test_Databackend, self)._setUp(self.CONFIG)

if __name__ == '__main__':
    unittest.main()
