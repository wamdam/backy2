# This is an port and update of the original smoketest.py
import os
import random
import re
import subprocess
import threading
from unittest import TestCase

from benji.benji import BenjiStore
from benji.logging import logger
from benji.metadata import VersionUid
from benji.nbdserver import NbdServer
from benji.tests.testcase import BenjiTestCase

kB = 1024
MB = kB * 1024
GB = MB * 1024

class NbdTestCase:

    @staticmethod
    def patch(filename, offset, data=None):
        """ write data into a file at offset """
        if not os.path.exists(filename):
            open(filename, 'wb').close()
        with open(filename, 'r+b') as f:
            f.seek(offset)
            f.write(data)

    @staticmethod
    def read_file(file1):
        with open(file1, 'rb') as f1:
            data = f1.read()
        return data

    def generate_version(self, testpath):
        size = 4*MB
        image_filename = os.path.join(testpath, 'image')
        with open(image_filename, 'wb') as f:
            f.truncate(size)
        for j in range(random.randint(20, 30)):
            patch_size = random.randint(0, 128*kB)
            data = self.random_bytes(patch_size)
            offset = random.randint(0, size-1-patch_size)
            self.patch(image_filename, offset, data)

        benji_obj = self.benjiOpen(initdb=True)
        version_uid = benji_obj.backup(
            'data-backup',
            'snapshot-name',
            'file://' + image_filename,
            None,
            None
        )
        benji_obj.close()
        return version_uid, size

    def setUp(self):
        super().setUp()
        self.version_uid = self.generate_version(self.testpath.path)

    def tearDown(self):
        self.subprocess_run(args=['sudo', 'nbd-client', '-d', self.NBD_DEVICE], check=False)
        super().tearDown()

    def test(self):
        benji_obj = self.benjiOpen(initdb=False)
        store = BenjiStore(benji_obj)
        addr = ('127.0.0.1', self.SERVER_PORT)
        read_only = False
        self.nbd_server = NbdServer(addr, store, read_only)
        logger.info("Starting to serve nbd on %s:%s" % (addr[0], addr[1]))

        self.nbd_client_thread = threading.Thread(target=self.nbd_client, daemon=True, args=(self.version_uid,))
        self.nbd_client_thread.start()
        self.nbd_server.serve_forever()
        self.nbd_client_thread.join()

        self.assertEqual({self.version_uid[0], VersionUid(2)}, set([version.uid for version  in benji_obj.ls()]))

        benji_obj.close()

    def subprocess_run(self, args, success_regexp = None, check=True):
        completed = subprocess.run(args=args,
                                   stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   encoding='utf-8',
                                   errors='ignore'
                                   )

        if check and completed.returncode != 0:
          self.fail('command {} failed: {}'.format(' '.join(args), completed.stdout.replace('\n', '|')))

        if success_regexp:
            if not re.match(success_regexp, completed.stdout, re.I|re.M|re.S):
                self.fail('command {} failed: {}'.format(' '.join(args), completed.stdout.replace('\n', '|')))

    def nbd_client(self, version_uid):
        self.subprocess_run(args=['sudo', 'nbd-client', '127.0.0.1', '-p', str(self.SERVER_PORT), '-l'],
                            success_regexp='^Negotiation: ..\n{}\n$'.format(version_uid[0].readable))

        version_uid, size = version_uid
        self.subprocess_run(args=['sudo', 'nbd-client', '-N', version_uid.readable, '127.0.0.1', '-p', str(self.SERVER_PORT), self.NBD_DEVICE],
                            success_regexp='^Negotiation: ..size = \d+MB\nbs=1024, sz=\d+ bytes\n$')

        count = 0
        nbd_data = bytearray()
        with open(self.NBD_DEVICE, 'rb') as f:
            while True:
                data = f.read(64 * 1024 + random.randint(0, 8192))
                if not data:
                    break
                count += len(data)
                nbd_data += data
        self.assertEqual(size, count)

        image_data = self.read_file(self.testpath.path + '/image')
        logger.info('image_data size {}, nbd_data size {}'.format(len(image_data), len(nbd_data)))
        self.assertEqual(image_data, bytes(nbd_data))

        f = os.open(self.NBD_DEVICE, os.O_RDWR)
        for offset in range(0,size,4096):
            os.lseek(f, offset, os.SEEK_SET)
            data = self.random_bytes(4096)
            written = os.write(f, data)
            os.fsync(f)
            self.assertEqual(len(data), written)
            # Discard cache so that the read request below really goes to the NBD server
            os.posix_fadvise(f, offset, len(data), os.POSIX_FADV_DONTNEED)

            os.lseek(f, offset, os.SEEK_SET)
            read_data = os.read(f, 4096)
            self.assertEqual(data, read_data)
        os.close(f)

        self.subprocess_run(args=['sudo', 'nbd-client', '-d', self.NBD_DEVICE],
                            success_regexp='^disconnect, sock, done\n$')

        # Signal NBD server to stop
        self.nbd_server.stop()


class NbdTestCaseSQLLite_File(NbdTestCase, BenjiTestCase, TestCase):

    SERVER_PORT = 1315

    NBD_DEVICE = '/dev/nbd15'

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: benji
            logFile: /dev/stderr
            hashFunction: blake2b,digest_size=32
            blockSize: 4096
            io:
              file:
                simultaneousReads: 5
            dataBackend:
              type: file
              file:
                path: {testpath}/data
              simultaneousWrites: 5
              simultaneousReads: 5
              bandwidthRead: 0
              bandwidthWrite: 0
            metadataBackend:
              engine: sqlite:///{testpath}/benji.sqlite
            nbd:
              cacheDirectory: {testpath}/nbd-cache
            """


class NbdTestCasePostgreSQL_S3(NbdTestCase, BenjiTestCase, TestCase):

    SERVER_PORT = 1315

    NBD_DEVICE = '/dev/nbd15'

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: benji
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
            hashFunction: blake2b,digest_size=32
            blockSize: 4096
            exportMetadata: false
            io:
              file:
                simultaneousReads: 5
            dataBackend:
              type: s3
              s3:
                awsAccessKeyId: minio
                awsSecretAccessKey: minio123
                endpointUrl: http://127.0.0.1:9901/
                bucketName: benji
                multiDelete: true
                addressingStyle: path
                disableEncodingType: false
                
                compression:
                  - name: zstd
                    materials:
                      level: 1
                    active: true
                      
                encryption:
                  - name: aws_s3_cse
                    materials:
                      masterKey: !!binary |
                        e/i1X4NsuT9k+FIVe2kd3vtHVkzZsbeYv35XQJeV8nA=
                    active: true
                    
              simultaneousWrites: 1
              simultaneousReads: 1
              bandwidthRead: 0
              bandwidthWrite: 0   
            metadataBackend:
              engine: postgresql://benji:verysecret@localhost:15432/benji
            nbd:
              cacheDirectory: {testpath}/nbd-cache
            """