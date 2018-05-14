# This is an port and update of the original smoketest.py
import json
import os
import random
from functools import reduce
from operator import and_
from shutil import copyfile
from unittest import TestCase

from backy2.scripts.backy import hints_from_rbd_diff
from backy2.tests.testcase import BackyTestCase

kB = 1024
MB = kB * 1024
GB = MB * 1024

class SmokeTestCase:

    @staticmethod
    def patch(filename, offset, data=None):
        """ write data into a file at offset """
        if not os.path.exists(filename):
            open(filename, 'wb').close()
        with open(filename, 'r+b') as f:
            f.seek(offset)
            f.write(data)

    @staticmethod
    def same(file1, file2):
        """ returns False if files differ, True if they are the same """
        with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
            d1 = f1.read()
            d2 = f2.read()
        return d1 == d2

    def test_sanity(self):
        file1 = os.path.join(self.testpath.path, 'file1')
        file2 = os.path.join(self.testpath.path, 'file2')
        with open(file1, 'w') as f1, open(file2, 'w') as f2:
            f1.write('hallo' * 100)
            f2.write('huhu' * 100)
        self.assertTrue(self.same(file1, file1))
        self.assertFalse(self.same(file1, file2))
        os.unlink(file1)
        os.unlink(file2)

    def test(self):
        testpath = self.testpath.path
        from_version = None
        version_uids = []
        old_size = 0
        initdb = True
        image_filename = os.path.join(testpath, 'image')
        block_size = random.sample({512, 1024, 2048, 4096}, 1)[0]
        for i in range(100):
            print('Run {}'.format(i+1))
            hints = []
            if not os.path.exists(image_filename):
                open(image_filename, 'wb').close()
            if old_size and random.randint(0, 10) == 0:  # every 10th time or so do not apply any changes.
                size = old_size
            else:
                size = 32*4*kB + random.randint(-4*kB, 4*kB)
                for j in range(random.randint(0, 10)):  # up to 10 changes
                    if random.randint(0, 1):
                        patch_size = random.randint(0, 4*kB)
                        data = self.random_bytes(patch_size)
                        exists = "true"
                    else:
                        patch_size = random.randint(0, 4*4*kB)  # we want full blocks sometimes
                        data = b'\0' * patch_size
                        exists = "false"
                    offset = random.randint(0, size - patch_size - 1)
                    print('    Applied change at {}({}):{}, exists {}'.format(offset, int(offset / 4096), patch_size, exists))
                    self.patch(image_filename, offset, data)
                    hints.append({'offset': offset, 'length': patch_size, 'exists': exists})

            # truncate?
            with open(image_filename, 'r+b') as f:
                f.truncate(size)

            if old_size and size > old_size:
                patch_size = size - old_size + 1
                offset = old_size - 1
                print('    Image got bigger at {}({}):{}'.format(offset, int(offset / 4096), patch_size))
                hints.append({'offset': offset, 'length': patch_size, 'exists': 'true'})

            old_size = size

            copyfile(image_filename, '{}.{}'.format(image_filename, i + 1))

            print('  Applied {} changes, size is {}.'.format(len(hints), size))
            with open(os.path.join(testpath, 'hints'), 'w') as f:
                    f.write(json.dumps(hints))

            backy = self.backyOpen(initdb=initdb, block_size=block_size)
            initdb = False
            with open(os.path.join(testpath, 'hints')) as hints:
                version_uid = backy.backup(
                    'data-backup',
                    'snapshot-name',
                    'file://' + image_filename,
                    hints_from_rbd_diff(hints.read()),
                    from_version
                )
            backy.close()
            version_uids.append(version_uid)

            backy = self.backyOpen(initdb=initdb)
            blocks = backy.ls_version(version_uid)
            self.assertEqual(list(range(len(blocks))), sorted([block.id for block in blocks]))
            self.assertTrue(len(blocks) > 0)
            if len(blocks) > 1:
                self.assertTrue(reduce(and_, [block.size == block_size for block in blocks[:-1]]))
            print('  Block list successful')
            backy.close()

            backy = self.backyOpen(initdb=initdb)
            versions = backy.ls()
            self.assertEqual(set(), set([version.uid for version in versions]) ^ set(version_uids))
            self.assertTrue(reduce(and_, [version.name == 'data-backup' for version in versions]))
            self.assertTrue(reduce(and_, [version.snapshot_name == 'snapshot-name' for version in versions]))
            self.assertTrue(reduce(and_, [version.block_size == backy.block_size for version in versions]))
            self.assertTrue(reduce(and_, [version.size > 0 for version in versions]))
            print('  Version list successful')
            backy.close()

            backy = self.backyOpen(initdb=initdb)
            self.assertTrue(backy.scrub(version_uid))
            backy.close()
            print('  Scrub successful')
            backy = self.backyOpen(initdb=initdb)
            self.assertTrue(backy.scrub(version_uid, 'file://' + image_filename))
            backy.close()
            print('  Deep scrub successful')
            backy = self.backyOpen(initdb=initdb)
            restore_filename = os.path.join(testpath, 'restore.{}'.format(i + 1))
            backy.restore(version_uid, 'file://' + restore_filename, sparse=False, force=False)
            backy.close()
            self.assertTrue(self.same(image_filename, restore_filename))
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
            configurationVersion: '1.0.0'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
            hashFunction: blake2b,digest_size=32
            blockSize: 4096
            io:
              file:
                simultaneousReads: 2
            dataBackend:
              type: file
              file:
                path: {testpath}/data
              simultaneousWrites: 5
              simultaneousReads: 5
              bandwidthRead: 0
              bandwidthWrite: 0
            metaBackend: 
              type: sql
              sql:
                engine: sqlite:///{testpath}/backy.sqlite
            """
class SmokeTestCasePostgreSQL_File(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
            hashFunction: blake2b,digest_size=32
            blockSize: 4096
            io:
              file:
                simultaneousReads: 2
            dataBackend:
              type: file
              file:
                path: {testpath}/data
              simultaneousWrites: 5
              simultaneousReads: 5
              bandwidthRead: 0
              bandwidthWrite: 0
            metaBackend: 
              type: sql
              sql:
                engine: postgresql://backy2:verysecret@localhost:15432/backy2
            """
class SmokeTestCasePostgreSQL_S3(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
            hashFunction: blake2b,digest_size=32
            blockSize: 4096
            io:
              file:
                simultaneousReads: 2
            dataBackend:
              type: s3
              s3:
                awsAccessKeyId: minio
                awsSecretAccessKey: minio123
                host: 127.0.0.1
                port: 9901
                isSecure: False
                bucketName: backy2
              simultaneousWrites: 5
              simultaneousReads: 5
              bandwidthRead: 0
              bandwidthWrite: 0
            metaBackend: 
              type: sql
              sql:
                engine: postgresql://backy2:verysecret@localhost:15432/backy2
            """

class SmokeTestCasePostgreSQL_S3_Boto3(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
            hashFunction: blake2b,digest_size=32
            blockSize: 4096
            io:
              file:
                simultaneousReads: 2
            dataBackend:
              type: s3_boto3
              s3_boto3:
                awsAccessKeyId: minio
                awsSecretAccessKey: minio123
                endpointUrl: http://127.0.0.1:9901/
                bucketName: backy2
                multiDelete: true
                addressingStyle: path
                disableEncodingType: false
              simultaneousWrites: 1
              simultaneousReads: 1
              bandwidthRead: 0
              bandwidthWrite: 0
            metaBackend: 
              type: sql
              sql:
                engine: sqlite:///{testpath}/backy.sqlite
            """

class SmokeTestCasePostgreSQL_S3_Boto3_ReadCache(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
            hashFunction: blake2b,digest_size=32
            blockSize: 4194304
            io:
              file:
                simultaneousReads: 2
            dataBackend:
              type: s3_boto3
              s3_boto3:
                awsAccessKeyId: minio
                awsSecretAccessKey: minio123
                endpointUrl: http://127.0.0.1:9901/
                bucketName: backy2
                multiDelete: false
                addressingStyle: path
                disableEncodingType: false
              simultaneousWrites: 1
              simultaneousReads: 1
              bandwidthRead: 0
              bandwidthWrite: 0
              readCacheDirectory: {testpath}/read-cache
              readCacheMaximumSize: 16777216
            metaBackend: 
              type: sql
              sql:
                engine: sqlite:///{testpath}/backy.sqlite
            """

class SmokeTestCasePostgreSQL_B2(SmokeTestCase, BackyTestCase, TestCase):

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
            hashFunction: blake2b,digest_size=32
            blockSize: 4096
            io:
              file:
                simultaneousReads: 2
            dataBackend:
              type: b2
              b2:
                 accountId: **************
                 applicationKey: **************
                 bucketName: backy2
              simultaneousWrites: 5
              simultaneousReads: 5
              bandwidthRead: 0
              bandwidthWrite: 0
            metaBackend: 
              type: sql
              sql:
                engine: postgresql://backy2:verysecret@localhost:15432/backy2
            """