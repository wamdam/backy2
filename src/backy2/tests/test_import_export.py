# This is an port and update of the original smoketest.py
import datetime
import json
import os
import random
from io import StringIO
from unittest import TestCase

from backy2.meta_backend import MetaBackend, VersionUid
from backy2.scripts.backy import hints_from_rbd_diff
from backy2.tests.testcase import BackyTestCase

kB = 1024
MB = kB * 1024
GB = MB * 1024

class ImportExportTestCase():

    @staticmethod
    def patch(filename, offset, data=None):
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
        self.assertEqual(1, version['uid'])
        self.assertEqual('data-backup', version['name'])
        self.assertEqual('snapshot-name', version['snapshot_name'])
        self.assertEqual(4096, version['block_size'])
        self.assertTrue(version['valid'])
        self.assertFalse(version['protected'])


    def test_import(self):
        backy = self.backyOpen(initdb=True)
        backy.import_(StringIO(self.IMPORT))
        version = backy._meta_backend.get_version(VersionUid(1))
        self.assertTrue(isinstance(version.uid, VersionUid))
        self.assertEqual(1, version.uid)
        self.assertEqual('data-backup', version.name)
        self.assertEqual('snapshot-name', version.snapshot_name)
        self.assertEqual(4096, version.block_size)
        self.assertTrue(version.valid)
        self.assertFalse(version.protected)
        self.assertIsInstance(version.blocks, list)
        self.assertIsInstance(version.tags, list)
        self.assertEqual({'b_daily', 'b_weekly', 'b_monthly'}, set([tag.name for tag in version.tags]))
        self.assertEqual(datetime.datetime.strptime('2018-05-16T11:57:10', '%Y-%m-%dT%H:%M:%S'), version.date)
        blocks = backy._meta_backend.get_blocks_by_version(VersionUid(1))
        self.assertTrue(len(blocks) > 0)
        max_i = len(blocks) - 1
        for i, block in enumerate(blocks):
            self.assertEqual(VersionUid(1), block.version_uid)
            self.assertEqual(i, block.id)
            if i != max_i:
                self.assertEqual(4096, block.size)
            self.assertEqual(datetime.datetime.strptime('2018-05-16T11:57:10', '%Y-%m-%dT%H:%M:%S'), block.date)
            self.assertTrue(block.valid)
        backy.close()

    IMPORT = """
            {
              "metadataVersion": "1.0.0",
              "versions": [
                {
                  "uid": 1,
                  "date": "2018-05-16T11:57:10",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 132159,
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
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 0,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 1,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 1,
                        "right": 3
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 2,
                      "size": 4096,
                      "valid": true,
                      "checksum": "ce8ddb524f0dcbf6ad3998df1709878507b0634eebd7e0d564aca752c58b48e8"
                    },
                    {
                      "uid": {
                        "left": 1,
                        "right": 4
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 3,
                      "size": 4096,
                      "valid": true,
                      "checksum": "0f77aaef3ee84642e737106599ab1a5daf229f778798344836f7f88756420267"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 4,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 5,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 6,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 7,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 8,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 9,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 1,
                        "right": 11
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 10,
                      "size": 4096,
                      "valid": true,
                      "checksum": "1f655ceb7df42e5ecab44b8911000a466aba02cd2d1ee3369c14f1b28801bde4"
                    },
                    {
                      "uid": {
                        "left": 1,
                        "right": 12
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 11,
                      "size": 4096,
                      "valid": true,
                      "checksum": "8ce603411abcdf8208c2a4f8ea2e95f77bc6a455fbb4f7ee5387390ecec8ef59"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 12,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 1,
                        "right": 14
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 13,
                      "size": 4096,
                      "valid": true,
                      "checksum": "e4f25b98a3ce44ad6956a73cdf723a68b093525701f86b7f93350909c17d2b3f"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 14,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 15,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 16,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 17,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 18,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 19,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 20,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 21,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 22,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 23,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 24,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 1,
                        "right": 26
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 25,
                      "size": 4096,
                      "valid": true,
                      "checksum": "7d428f90f0d41de69602f63ca0198bab986af92f2c8d618fda3d6f76e8fe9200"
                    },
                    {
                      "uid": {
                        "left": 1,
                        "right": 27
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 26,
                      "size": 4096,
                      "valid": true,
                      "checksum": "4ca4ff7b160a9d70b6e59b2e5350cbd5ad12f7b8678a9da9322a43beb8f594cb"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 27,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 28,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 29,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 30,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 31,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 1,
                        "right": 33
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 32,
                      "size": 1087,
                      "valid": true,
                      "checksum": "e1176a70d65834551e21d09f836c12316f4df4552f6c6b0220a8b162a984381b"
                    }
                  ]
                },
                {
                  "uid": 2,
                  "date": "2018-05-16T11:57:10",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 130869,
                  "block_size": 4096,
                  "valid": true,
                  "protected": false,
                  "tags": [],
                  "blocks": [
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 0,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 1,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 2,
                        "right": 3
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 2,
                      "size": 4096,
                      "valid": true,
                      "checksum": "b848eb7e8473531748a3b94cf4b699675ad4d1be830afe80d9de9418f734dde7"
                    },
                    {
                      "uid": {
                        "left": 2,
                        "right": 4
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 3,
                      "size": 4096,
                      "valid": true,
                      "checksum": "66dd04f289222a5bd0f3cc6b8422d32bbb04a33a6b6daf0354d2a85055d77f9b"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 4,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 5,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 6,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 7,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 8,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 9,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 10,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 11,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 12,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 13,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 14,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 15,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 16,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 17,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 18,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 19,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 20,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 21,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 22,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 23,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 24,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 25,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 26,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 27,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 28,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 29,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 30,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 2,
                        "right": 32
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 31,
                      "size": 3893,
                      "valid": true,
                      "checksum": "d8eb4adfe3e0f98b2ba106c09cd25b2cbad96d38a38353a338e674fc3d2adbb9"
                    }
                  ]
                },
                {
                  "uid": 3,
                  "date": "2018-05-16T11:57:10",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 128699,
                  "block_size": 4096,
                  "valid": true,
                  "protected": false,
                  "tags": [],
                  "blocks": [
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 0,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 1,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 2,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 2,
                        "right": 4
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 3,
                      "size": 4096,
                      "valid": true,
                      "checksum": "66dd04f289222a5bd0f3cc6b8422d32bbb04a33a6b6daf0354d2a85055d77f9b"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 4,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 6
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 5,
                      "size": 4096,
                      "valid": true,
                      "checksum": "447c28c898d66feb748f33aefbf5f8a4fe5b240f0836dfb8a3cf023e150e85aa"
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 7
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 6,
                      "size": 4096,
                      "valid": true,
                      "checksum": "d3fb67022cb478794623dddce2f71fa45bae895b0ad460dda59d8dac9049e2c5"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 7,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 8,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 9,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 10,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 11,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 12,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 14
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 13,
                      "size": 4096,
                      "valid": true,
                      "checksum": "0f3de8fa9dfd7d20df489798a6c16c03d36563e9b40a827a9b3d33bdf41fe2b4"
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 15
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 14,
                      "size": 4096,
                      "valid": true,
                      "checksum": "004a1d16856d02feadb7c706fa696ae2929b169f7c1f3075a2c32fe7c2b679f2"
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 16
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 15,
                      "size": 4096,
                      "valid": true,
                      "checksum": "146483e5eced43f07b52e65f34fd047a5eaeb15205cca9cb632173bbd31eeafe"
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 17
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 16,
                      "size": 4096,
                      "valid": true,
                      "checksum": "63d06e992e01f4fcd94da026863f5b78ef041d53608cb39be6b5ac5c18aaedfa"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 17,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 18,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 19,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 20,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 22
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 21,
                      "size": 4096,
                      "valid": true,
                      "checksum": "bce9c77e7a5dc78fe4568197c89973a8f129645a2530ff307524266519906276"
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 23
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 22,
                      "size": 4096,
                      "valid": true,
                      "checksum": "8379d251664199ba838fe0332094cfc898d6aedfc5a1b490fc1a74cd0cde3e3e"
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 24
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 23,
                      "size": 4096,
                      "valid": true,
                      "checksum": "b9dba5fd41dca22d1055651e2a640fdb4ae45254eeca2ee9a7896f0b9b39d668"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 24,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 25,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 26,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 27,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 28,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 29,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 30,
                      "size": 4096,
                      "valid": true,
                      "checksum": null
                    },
                    {
                      "uid": {
                        "left": 3,
                        "right": 32
                      },
                      "date": "2018-05-16T11:57:10",
                      "id": 31,
                      "size": 1723,
                      "valid": true,
                      "checksum": "27975b75c000899f95ba914c972e38c439e16e0b7919b5a04c42ba70f46d8b67"
                    }
                  ]
                }
              ]
            }
            """

class ImportExportCaseSQLLite_File(ImportExportTestCase, BackyTestCase, TestCase):

    VERSIONS = 3

    CONFIG = """
            configurationVersion: '1.0.0'
            processName: backy2
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
            metaBackend: 
              engine: sqlite:///{testpath}/backy.sqlite
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
            exportMetadata: True
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
              engine: postgresql://backy2:verysecret@localhost:15432/backy2
            """

