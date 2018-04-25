# This is an port and update of the original smoketest.py
import json
from unittest import TestCase

import os
import random

from backy2.scripts.backy import hints_from_rbd_diff
from backy2.tests.testcase import BackyTestCase

kB = 1024
MB = kB * 1024
GB = MB * 1024

class SmokeTestCase():

    @classmethod
    def patch(self, path, filename, offset, data=None):
        """ write data into a file at offset """
        filename = os.path.join(path, filename)
        with open(filename, 'a+b') as f:
            f.seek(offset)
            f.write(data)

    @classmethod
    def same(self, f1, f2):
        """ returns False if files differ, True if they are the same """
        d1 = open(f1, 'rb').read()
        d2 = open(f1, 'rb').read()
        return d1 == d2

    def test(self):
        testpath = self.testpath.path
        from_version = None
        version_uids = []
        old_size = 0
        initdb = True
        for i in range(100):
            print('Run {}'.format(i+1))
            hints = []
            if old_size and random.randint(0, 10) == 0:  # every 10th time or so do not apply any changes.
                size = old_size
            else:
                size = 32*4*kB + random.randint(-4*kB, 4*kB)
                old_size = size
                for j in range(random.randint(0, 10)):  # up to 10 changes
                    if random.randint(0, 1):
                        patch_size = random.randint(0, 4*kB)
                        data = os.urandom(patch_size)
                        exists = "true"
                    else:
                        patch_size = random.randint(0, 4*4*kB)  # we want full blocks sometimes
                        data = b'\0' * patch_size
                        exists = "false"
                    offset = random.randint(0, size-1-patch_size)
                    print('    Applied change at {}:{}, exists {}'.format(offset, patch_size, exists))
                    self.patch(testpath, 'image', offset, data)
                    hints.append({'offset': offset, 'length': patch_size, 'exists': exists})
            # truncate?
            with open(os.path.join(testpath, 'image'), 'a+b') as f:
                f.truncate(size)

            print('  Applied {} changes, size is {}.'.format(len(hints), size))
            with open(os.path.join(testpath, 'hints'), 'w') as f:
                    f.write(json.dumps(hints))

            backy = self.backyOpen(initdb=initdb)
            initdb = False
            with open(os.path.join(testpath, 'hints')) as hints:
                version_uid = backy.backup(
                    'data-backup',
                    'snapshot-name',
                    'file://'+os.path.join(testpath, 'image'),
                    hints_from_rbd_diff(hints.read()),
                    from_version
                    )
            backy.close()
            version_uids.append(version_uid)

            backy = self.backyOpen(initdb=initdb)
            self.assertTrue(backy.scrub(version_uid))
            backy.close()
            print('  Scrub successful')
            backy = self.backyOpen(initdb=initdb)
            self.assertTrue(backy.scrub(version_uid, 'file://'+os.path.join(testpath, 'image')))
            backy.close()
            print('  Deep scrub successful')
            backy = self.backyOpen(initdb=initdb)
            backy.restore(version_uid, 'file://'+os.path.join(testpath, 'restore'), sparse=False, force=False)
            backy.close()
            self.assertTrue(self.same(os.path.join(testpath, 'image'), os.path.join(testpath, 'restore')))
            os.unlink(os.path.join(testpath, 'restore'))
            print('  Restore successful')

            from_version = version_uid

            # delete old versions
            if len(version_uids) > 10:
                backy = self.backyOpen(initdb=initdb)
                backy.rm(version_uids.pop(0))
                backy.close()

            if (i%7) == 0:
                backy = self.backyOpen(initdb=initdb)
                backy.cleanup_fast(dt=0)
                backy.close()

class SmokeTestCaseSQLLite_File(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            [DEFAULTS]
            logfile: /var/log/backy.log
            block_size: 4096
            hash_function: blake2b,digest_size=32
            lock_dir: {testpath}/lock
            process_name: backy2

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
class SmokeTestCasePostgreSQL_File(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            [DEFAULTS]
            logfile: /var/log/backy.log
            block_size: 4096
            hash_function: blake2b,digest_size=32
            lock_dir: {testpath}/lock
            process_name: backy2

            [MetaBackend]
            type: backy2.meta_backends.sql
            engine: postgresql://backy2:verysecret@localhost:15432/backy2

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
class SmokeTestCasePostgreSQL_S3(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            [DEFAULTS]
            logfile: /var/log/backy.log
            block_size: 4096
            hash_function: blake2b,digest_size=32
            lock_dir: {testpath}/lock
            process_name: backy2

            [MetaBackend]
            type: backy2.meta_backends.sql
            engine: postgresql://backy2:verysecret@localhost:15432/backy2

            [DataBackend]
            type: backy2.data_backends.s3
            
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

class SmokeTestCasePostgreSQL_S3_Boto3(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            [DEFAULTS]
            logfile: /var/log/backy.log
            block_size: 4096
            hash_function: blake2b,digest_size=32
            lock_dir: {testpath}/lock
            process_name: backy2

            [MetaBackend]
            type: backy2.meta_backends.sql
            engine: postgresql://backy2:verysecret@localhost:15432/backy2

            [DataBackend]
            type: backy2.data_backends.s3_boto3
            
            compression: backy2.data_backends.compression.zstd
            compression_default: zstd
            
            encryption: backy2.data_backends.encryption.aws_s3_cse
            encryption_materials: {{"MasterKey": "0000000000000000"}}
            encryption_default: aws_s3_cse
            
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

class SmokeTestCasePostgreSQL_B2(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            [DEFAULTS]
            logfile: /var/log/backy.log
            block_size: 4096
            hash_function: blake2b,digest_size=32
            lock_dir: {testpath}/lock
            process_name: backy2

            [MetaBackend]
            type: backy2.meta_backends.sql
            engine: postgresql://backy2:verysecret@localhost:15432/backy2

            [DataBackend]
            type: backy2.data_backends.b2
            
            account_id: **************
            application_key: **************
            bucket_name: **************
            
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