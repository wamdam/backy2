# This is an port and update of the original smoketest.py
import datetime
import json
from unittest import TestCase

import os
import random
from io import StringIO

from backy2.meta_backends import MetaBackend
from backy2.scripts.backy import hints_from_rbd_diff
from backy2.tests.testcase import BackyTestCase

kB = 1024
MB = kB * 1024
GB = MB * 1024

class ImportExportTestCase():

    @classmethod
    def patch(self, filename, offset, data=None):
        """ write data into a file at offset """
        if not os.path.exists(filename):
            open(filename, 'wb').close()
        with open(filename, 'r+b') as f:
            f.seek(offset)
            f.write(data)

    def generate_versions(self, testpath):
        from_version = None
        version_uids = []
        old_size = 0
        initdb = True
        image_filename = os.path.join(testpath, 'image')
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
                        data = self.random_bytes(patch_size)
                        exists = "true"
                    else:
                        patch_size = random.randint(0, 4*4*kB)  # we want full blocks sometimes
                        data = b'\0' * patch_size
                        exists = "false"
                    offset = random.randint(0, size-1-patch_size)
                    print('    Applied change at {}:{}, exists {}'.format(offset, patch_size, exists))
                    self.patch(image_filename, offset, data)
                    hints.append({'offset': offset, 'length': patch_size, 'exists': exists})
            # truncate?
            if not os.path.exists(image_filename):
                open(image_filename, 'wb').close()
            with open(image_filename, 'r+b') as f:
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
                    'file://' + image_filename,
                    hints_from_rbd_diff(hints.read()),
                    from_version
                )
            backy.close()
            version_uids.append((version_uid, size))
        return version_uids

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_export(self):
        backy = self.backyOpen(initdb=True)
        self.version_uids = self.generate_versions(self.testpath.path)
        with StringIO() as f:
            backy.export([version_uid[0] for version_uid in self.version_uids], f)
            f.seek(0)
            export = json.load(f)
            f.seek(0)
            print(f.getvalue())
            a = f.getvalue()
        backy.close()
        self.assertEqual(MetaBackend.METADATA_VERSION, export['metadataVersion'])
        self.assertIsInstance(export['versions'], list)
        self.assertTrue(len(export['versions']) == 3)
        version = export['versions'][0]
        self.assertTrue(version['uid'].startswith('V'))
        self.assertEqual('data-backup', version['name'])
        self.assertEqual('snapshot-name', version['snapshot_name'])
        self.assertEqual(4096, version['block_size'])
        self.assertTrue(version['valid'])
        self.assertFalse(version['protected'])


    def test_import(self):
        backy = self.backyOpen(initdb=True)
        backy.import_(StringIO(self.IMPORT))
        version = backy.meta_backend.get_version('V0000000001')
        self.assertEqual('V0000000001', version.uid)
        self.assertEqual('data-backup', version.name)
        self.assertEqual('snapshot-name', version.snapshot_name)
        self.assertEqual(4096, version.block_size)
        self.assertTrue(version.valid)
        self.assertFalse(version.protected)
        self.assertIsInstance(version.blocks, list)
        self.assertIsInstance(version.tags, list)
        self.assertEqual(set(['b_daily', 'b_weekly', 'b_monthly']), set([tag.name for tag in version.tags]))
        self.assertEqual(datetime.datetime.strptime('2018-05-02T22:10:36', '%Y-%m-%dT%H:%M:%S'), version.date)
        blocks = backy.meta_backend.get_blocks_by_version('V0000000001')
        self.assertTrue(len(blocks) > 0)
        max_i = len(blocks) - 1
        for i, block in enumerate(blocks):
            self.assertEqual('V0000000001', block.version_uid)
            self.assertEqual(i, block.id)
            if i != max_i:
                self.assertEqual(4096, block.size)
            self.assertEqual(datetime.datetime.strptime('2018-05-03T00:10:36', '%Y-%m-%dT%H:%M:%S'), block.date)
            self.assertTrue(block.valid)
        backy.close()

class ImportExportCaseSQLLite_File(ImportExportTestCase, BackyTestCase, TestCase):

    VERSIONS = 3

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
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
            metaBackend: 
              type: sql
              sql:
                engine: sqlite:///{testpath}/backy.sqlite
            """

    IMPORT = """
            {
              "metadataVersion": "1.0.0",
              "versions": [
                {
                  "uid": "V0000000001",
                  "date": "2018-05-02T22:10:36",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 127528,
                  "block_size": 4096,
                  "valid": true,
                  "protected": false,
                  "tags": [
                    {
                      "name": "b_daily"
                    },
                    {
                      "name": "b_monthly"
                    },
                    {
                      "name": "b_weekly"
                    }
                  ],
                  "blocks": [
                    {
                      "uid": null,
                      "id": 0,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 1,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 2,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 3,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 4,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 5,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 6,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 7,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 8,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "9aeeb45698GD59NZ7YEnky4VeXMMgv63",
                      "id": 9,
                      "date": "2018-05-03T00:10:36",
                      "checksum": "32c8d7ac57e626439dfe2e2de0ba3fd6157f5740124b0fc45def2c4a77da3394",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 10,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 11,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 12,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 13,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 14,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 15,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 16,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 17,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 18,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 19,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 20,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 21,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 22,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 23,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 24,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 25,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 26,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 27,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 28,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 29,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 30,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 31,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 552,
                      "valid": true
                    }
                  ]
                },
                {
                  "uid": "V0000000002",
                  "date": "2018-05-02T22:10:36",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 134350,
                  "block_size": 4096,
                  "valid": true,
                  "protected": false,
                  "tags": [
                    {
                      "name": "b_daily"
                    }
                  ],
                  "blocks": [
                    {
                      "uid": "d918eed5c9VvwGw6NwWAWhsP5sT4WX3k",
                      "id": 0,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "0864b8007b9319a3fa63d11d14632de2174385f1277703c0a754e86f6783c22a",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "311f117968bNucpXfwz5i6nrhd9yNDff",
                      "id": 1,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "c638bfbb4d4e66f67197a0a86530d3258d9fdb7b9685c153fa6eb22d8c5fb61f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 2,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "5d1f94ab26BTop73T4JQyuaxyuxJmqoK",
                      "id": 3,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "0b775191d582ace43536fc6a592ff98f00f816feb6fe1c0813b807e2654b524f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 4,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "90b7f3a461utWgSLFKnRZ9svip4qusKh",
                      "id": 5,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "64fc140ba60f36c9b4be7fa9209b6cd76e3400303e90a51ad0d1d8b100831dc0",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "08f3f143e13sEhCXFGJTBfBxNhTQY7CN",
                      "id": 6,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "254878a238f5d11fe6c6818be064334aba75f4c91de73e26a241941f2e13769a",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 7,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 8,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 9,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 10,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 11,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 12,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 13,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 14,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 15,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 16,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "e08234e14bVBFFJbJu2s956hsTTTFzdn",
                      "id": 17,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "1853e1b26ef552c7639683a5fa21c66a49d9f43437cdf681dea4d28aa57cba4f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "c07e01f145oPRvZtX7yq7c8PdkHRheUW",
                      "id": 18,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "45f6a923b17d53d9cca019296ea4c5b722fc6281f3c5802ecde117351afc2ae9",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 19,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 20,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 21,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 22,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 23,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 24,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 25,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 26,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "8331573fb4iHhBYRWERMn5m4wfGuBaYf",
                      "id": 27,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "e1504680de9932a5352ba366d34750fcca925cddf6d1b8b6d71aeabdbb3d0365",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "aa947814dcFbwoeimBSVYodtDF4U3WcX",
                      "id": 28,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "7e7bea564218a5b453b677fd1b0f7cb39acff3fd4d9a1237804ee1c7f5c82ce2",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 29,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 30,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 31,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 32,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 3278,
                      "valid": true
                    }
                  ]
                },
                {
                  "uid": "V0000000003",
                  "date": "2018-05-02T22:10:37",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 134350,
                  "block_size": 4096,
                  "valid": true,
                  "protected": false,
                  "tags": [
                    {
                      "name": "b_daily"
                    }
                  ],
                  "blocks": [
                    {
                      "uid": "d918eed5c9VvwGw6NwWAWhsP5sT4WX3k",
                      "id": 0,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "0864b8007b9319a3fa63d11d14632de2174385f1277703c0a754e86f6783c22a",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "311f117968bNucpXfwz5i6nrhd9yNDff",
                      "id": 1,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "c638bfbb4d4e66f67197a0a86530d3258d9fdb7b9685c153fa6eb22d8c5fb61f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 2,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "5d1f94ab26BTop73T4JQyuaxyuxJmqoK",
                      "id": 3,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "0b775191d582ace43536fc6a592ff98f00f816feb6fe1c0813b807e2654b524f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 4,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "90b7f3a461utWgSLFKnRZ9svip4qusKh",
                      "id": 5,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "64fc140ba60f36c9b4be7fa9209b6cd76e3400303e90a51ad0d1d8b100831dc0",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "08f3f143e13sEhCXFGJTBfBxNhTQY7CN",
                      "id": 6,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "254878a238f5d11fe6c6818be064334aba75f4c91de73e26a241941f2e13769a",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 7,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 8,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "9aeeb45698GD59NZ7YEnky4VeXMMgv63",
                      "id": 9,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "32c8d7ac57e626439dfe2e2de0ba3fd6157f5740124b0fc45def2c4a77da3394",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 10,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 11,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 12,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 13,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 14,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 15,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 16,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "e08234e14bVBFFJbJu2s956hsTTTFzdn",
                      "id": 17,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "1853e1b26ef552c7639683a5fa21c66a49d9f43437cdf681dea4d28aa57cba4f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "c07e01f145oPRvZtX7yq7c8PdkHRheUW",
                      "id": 18,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "45f6a923b17d53d9cca019296ea4c5b722fc6281f3c5802ecde117351afc2ae9",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 19,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 20,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 21,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 22,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 23,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 24,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 25,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 26,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "8331573fb4iHhBYRWERMn5m4wfGuBaYf",
                      "id": 27,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "e1504680de9932a5352ba366d34750fcca925cddf6d1b8b6d71aeabdbb3d0365",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "aa947814dcFbwoeimBSVYodtDF4U3WcX",
                      "id": 28,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "7e7bea564218a5b453b677fd1b0f7cb39acff3fd4d9a1237804ee1c7f5c82ce2",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 29,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 30,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 31,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 32,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 3278,
                      "valid": true
                    }
                  ]
                }
              ]
            }
            """

class ImportExportTestCasePostgreSQL_File(ImportExportTestCase, BackyTestCase, TestCase):

    VERSIONS = 3

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: backy2
            logFile: /dev/stderr
            lockDirectory: {testpath}/lock
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
            metaBackend: 
              type: sql
              sql:
                engine: postgresql://backy2:verysecret@localhost:15432/backy2
            """
    IMPORT = """
            {
              "metadataVersion": "1.0.0",
              "versions": [
                {
                  "uid": "V0000000001",
                  "date": "2018-05-02T22:10:36",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 127528,
                  "block_size": 4096,
                  "valid": true,
                  "protected": false,
                  "tags": [
                    {
                      "name": "b_daily"
                    },
                    {
                      "name": "b_monthly"
                    },
                    {
                      "name": "b_weekly"
                    }
                  ],
                  "blocks": [
                    {
                      "uid": null,
                      "id": 0,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 1,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 2,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 3,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 4,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 5,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 6,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 7,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 8,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "9aeeb45698GD59NZ7YEnky4VeXMMgv63",
                      "id": 9,
                      "date": "2018-05-03T00:10:36",
                      "checksum": "32c8d7ac57e626439dfe2e2de0ba3fd6157f5740124b0fc45def2c4a77da3394",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 10,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 11,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 12,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 13,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 14,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 15,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 16,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 17,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 18,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 19,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 20,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 21,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 22,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 23,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 24,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 25,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 26,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 27,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 28,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 29,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 30,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 31,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 552,
                      "valid": true
                    }
                  ]
                },
                {
                  "uid": "V0000000002",
                  "date": "2018-05-02T22:10:36",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 134350,
                  "block_size": 4096,
                  "valid": true,
                  "protected": false,
                  "tags": [
                    {
                      "name": "b_daily"
                    }
                  ],
                  "blocks": [
                    {
                      "uid": "d918eed5c9VvwGw6NwWAWhsP5sT4WX3k",
                      "id": 0,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "0864b8007b9319a3fa63d11d14632de2174385f1277703c0a754e86f6783c22a",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "311f117968bNucpXfwz5i6nrhd9yNDff",
                      "id": 1,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "c638bfbb4d4e66f67197a0a86530d3258d9fdb7b9685c153fa6eb22d8c5fb61f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 2,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "5d1f94ab26BTop73T4JQyuaxyuxJmqoK",
                      "id": 3,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "0b775191d582ace43536fc6a592ff98f00f816feb6fe1c0813b807e2654b524f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 4,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "90b7f3a461utWgSLFKnRZ9svip4qusKh",
                      "id": 5,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "64fc140ba60f36c9b4be7fa9209b6cd76e3400303e90a51ad0d1d8b100831dc0",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "08f3f143e13sEhCXFGJTBfBxNhTQY7CN",
                      "id": 6,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "254878a238f5d11fe6c6818be064334aba75f4c91de73e26a241941f2e13769a",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 7,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 8,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 9,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 10,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 11,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 12,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 13,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 14,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 15,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 16,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "e08234e14bVBFFJbJu2s956hsTTTFzdn",
                      "id": 17,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "1853e1b26ef552c7639683a5fa21c66a49d9f43437cdf681dea4d28aa57cba4f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "c07e01f145oPRvZtX7yq7c8PdkHRheUW",
                      "id": 18,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "45f6a923b17d53d9cca019296ea4c5b722fc6281f3c5802ecde117351afc2ae9",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 19,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 20,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 21,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 22,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 23,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 24,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 25,
                      "date": "2018-05-03T00:10:36",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 26,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "8331573fb4iHhBYRWERMn5m4wfGuBaYf",
                      "id": 27,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "e1504680de9932a5352ba366d34750fcca925cddf6d1b8b6d71aeabdbb3d0365",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "aa947814dcFbwoeimBSVYodtDF4U3WcX",
                      "id": 28,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "7e7bea564218a5b453b677fd1b0f7cb39acff3fd4d9a1237804ee1c7f5c82ce2",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 29,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 30,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 31,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 32,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 3278,
                      "valid": true
                    }
                  ]
                },
                {
                  "uid": "V0000000003",
                  "date": "2018-05-02T22:10:37",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 134350,
                  "block_size": 4096,
                  "valid": true,
                  "protected": false,
                  "tags": [
                    {
                      "name": "b_daily"
                    }
                  ],
                  "blocks": [
                    {
                      "uid": "d918eed5c9VvwGw6NwWAWhsP5sT4WX3k",
                      "id": 0,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "0864b8007b9319a3fa63d11d14632de2174385f1277703c0a754e86f6783c22a",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "311f117968bNucpXfwz5i6nrhd9yNDff",
                      "id": 1,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "c638bfbb4d4e66f67197a0a86530d3258d9fdb7b9685c153fa6eb22d8c5fb61f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 2,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "5d1f94ab26BTop73T4JQyuaxyuxJmqoK",
                      "id": 3,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "0b775191d582ace43536fc6a592ff98f00f816feb6fe1c0813b807e2654b524f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 4,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "90b7f3a461utWgSLFKnRZ9svip4qusKh",
                      "id": 5,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "64fc140ba60f36c9b4be7fa9209b6cd76e3400303e90a51ad0d1d8b100831dc0",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "08f3f143e13sEhCXFGJTBfBxNhTQY7CN",
                      "id": 6,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "254878a238f5d11fe6c6818be064334aba75f4c91de73e26a241941f2e13769a",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 7,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 8,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "9aeeb45698GD59NZ7YEnky4VeXMMgv63",
                      "id": 9,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "32c8d7ac57e626439dfe2e2de0ba3fd6157f5740124b0fc45def2c4a77da3394",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 10,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 11,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 12,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 13,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 14,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 15,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 16,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "e08234e14bVBFFJbJu2s956hsTTTFzdn",
                      "id": 17,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "1853e1b26ef552c7639683a5fa21c66a49d9f43437cdf681dea4d28aa57cba4f",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "c07e01f145oPRvZtX7yq7c8PdkHRheUW",
                      "id": 18,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "45f6a923b17d53d9cca019296ea4c5b722fc6281f3c5802ecde117351afc2ae9",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 19,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 20,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 21,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 22,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 23,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 24,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 25,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 26,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "8331573fb4iHhBYRWERMn5m4wfGuBaYf",
                      "id": 27,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "e1504680de9932a5352ba366d34750fcca925cddf6d1b8b6d71aeabdbb3d0365",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": "aa947814dcFbwoeimBSVYodtDF4U3WcX",
                      "id": 28,
                      "date": "2018-05-03T00:10:37",
                      "checksum": "7e7bea564218a5b453b677fd1b0f7cb39acff3fd4d9a1237804ee1c7f5c82ce2",
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 29,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 30,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 31,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 4096,
                      "valid": true
                    },
                    {
                      "uid": null,
                      "id": 32,
                      "date": "2018-05-03T00:10:37",
                      "checksum": null,
                      "size": 3278,
                      "valid": true
                    }
                  ]
                }
              ]
            }
            """