# This is an port and update of the original smoketest.py
import json
import subprocess
import threading
from unittest import TestCase

import os
import random
import re

from backy2.logging import logger
from backy2.scripts.backy import hints_from_rbd_diff
from backy2.tests.testcase import BackyTestCase
from backy2.utils import parametrized_hash_function

kB = 1024
MB = kB * 1024
GB = MB * 1024

class NbdTestCase():

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

    def generate_versions(self, testpath):
        from_version = None
        version_uids = []
        old_size = 0
        initdb = True
        for i in range(self.VERSIONS):
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
            version_uids.append((version_uid, size))
        return version_uids

    def setUp(self):
        super().setUp()
        self.version_uids = self.generate_versions(self.testpath.path)

    def tearDown(self):
        self.subprocess_run(args=['sudo', 'nbd-client', '-d', self.NBD_DEVICE], check=False)
        super().tearDown()

    def test(self):
        from backy2.enterprise.nbdserver import Server as NbdServer
        from backy2.enterprise.nbd import BackyStore
        backy = self.backyOpen(initdb=False)

        hash_function = parametrized_hash_function(self.config.get('hashFunction', types=str))
        cache_dir = self.config.get('nbd.cacheDirectory', types=str)
        store = BackyStore(backy, cachedir=cache_dir, hash_function=hash_function)
        addr = ('127.0.0.1', 1313)
        read_only = False
        self.nbd_server = NbdServer(addr, store, read_only)
        logger.info("Starting to serve nbd on %s:%s" % (addr[0], addr[1]))

        self.nbd_client_thread = threading.Thread(target=self.nbd_client, args=(self.version_uids,))
        self.nbd_client_thread.start()
        self.nbd_server.serve_forever()
        self.nbd_client_thread.join()

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


    def nbd_client(self, version_uids):
        self.subprocess_run(args=['sudo', 'nbd-client', '127.0.0.1', '-p', '1313', '-l'],
                            success_regexp='^Negotiation: ..\n{}\n$'.format('\n'.join([tuple[0] for tuple in version_uids])))

        for i in range(self.VERSIONS):
            version_uid, size = version_uids[i]
            self.subprocess_run(args=['sudo', 'nbd-client', '-N', version_uid, '127.0.0.1', '-p', '1313', self.NBD_DEVICE],
                                success_regexp='^Negotiation: ..size = \d+MB\nbs=1024, sz=\d+ bytes\n$')

            count = 0
            with open(self.NBD_DEVICE, 'rb') as f:
                while True:
                    data = f.read(1024 * 1024)
                    if not data:
                        break
                    count += len(data)
            self.assertEqual(size + 4096 - size % 4096, count)

            self.subprocess_run(args=['sudo', 'nbd-client', '-d', self.NBD_DEVICE],
                                success_regexp='^disconnect, sock, done\n$')

        # Signal NBD server to stop
        self.nbd_server.stop()


class NbdTestCaseSQLLite_File(NbdTestCase, BackyTestCase, TestCase):

    VERSIONS = 10

    NBD_DEVICE = '/dev/nbd15'

    CONFIG = """
            configurationVersion: '0.1'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
            hashFunction: blake2b,digest_size=32
            blockSize: 131072
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
            metaBackend: 
              type: sql
              sql:
                engine: sqlite:///{testpath}/backy.sqlite
            nbd:
              cacheDirectory: {testpath}/nbd/cache
            """

class NbdTestCasePostgreSQL_S3(NbdTestCase, BackyTestCase):

    VERSIONS = 10

    NBD_DEVICE = '/dev/nbd15'

    CONFIG = """
            configurationVersion: '0.1'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
            hashFunction: blake2b,digest_size=32
            blockSize: 131072
            io:
              file:
                simultaneousReads: 5
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
            nbd:
              cacheDirectory: {testpath}/nbd/cache
            """