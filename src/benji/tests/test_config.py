import os
import unittest

from benji.config import Config
from benji.exception import ConfigurationError
from benji.tests.testcase import TestCase


class ConfigTestCase(TestCase, unittest.TestCase):

    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /var/log/benji.log
        blockSize: 4194304
        hashFunction: sha512
        processName: benji
        metadataBackend:
          type: sql
          sql:
            engine: sqlite:////var/lib/benji/benji.sqlite
        dataBackend:
          type: file
          file:
            path: /var/lib/benji/data
          simultaneousWrites: 5
          simultaneousReads: 5
        nbd:
          cacheDirectory: /tmp
        io:
          rbd:
            ceph_conffile: /etc/ceph/ceph.conf
            simultaneousReads: 10
            newImageFeatures:
              - RBD_FEATURE_LAYERING
              - RBD_FEATURE_EXCLUSIVE_LOCK
        """

    def test_load_from_string(self):
        config = Config(cfg=self.CONFIG, merge_defaults=False)
        self.assertEqual(5, config.get('dataBackend.simultaneousReads'))
        self.assertEqual(10, config.get('io.rbd.simultaneousReads', types=int))

    def test_dict(self):
        config = Config(cfg=self.CONFIG, merge_defaults=False)
        self.assertEqual({'__position': 'nbd', 'cacheDirectory': '/tmp'}, config.get('nbd', types=dict))

    def test_lists(self):
        config = Config(cfg=self.CONFIG, merge_defaults=False)
        self.assertTrue(type(config.get('io.rbd.newImageFeatures')) is list)
        self.assertRaises(TypeError, config.get('io.rbd.newImageFeatures', types=list))
        self.assertEqual('RBD_FEATURE_EXCLUSIVE_LOCK', config.get('io.rbd.newImageFeatures')[1])

    def test_correct_version(self):
        self.assertTrue(isinstance(Config(cfg='configurationVersion: \'{}\''.format(Config.CONFIG_VERSION), merge_defaults=False), Config))

    def test_wrong_version(self):
        self.assertRaises(ConfigurationError, lambda : Config(cfg='configurationVersion: \'234242.2343242\'', merge_defaults=False))

    def test_missing_version(self):
        self.assertRaises(ConfigurationError, lambda : Config(cfg='a: {b: 1, c: 2}', merge_defaults=False))

    def test_defaults(self):
        config = Config(cfg='configurationVersion: \'{}\''.format(Config.CONFIG_VERSION), merge_defaults=True)
        self.assertEqual(1, config.get('dataBackend.simultaneousReads'))
        self.assertEqual(1, config.get('io.rbd.simultaneousReads'))

    def test_default_overwrite(self):
        config = Config(cfg="""
        configurationVersion: '{}'
        dataBackend:
          simultaneousReads: 12345678
        """.format(Config.CONFIG_VERSION), merge_defaults=True)
        self.assertEqual(12345678, config.get('dataBackend.simultaneousReads'))
        self.assertEqual(1, config.get('io.rbd.simultaneousReads'))

    def test_missing(self):
        config = Config(cfg='configurationVersion: \'{}\''.format(Config.CONFIG_VERSION), merge_defaults=False)
        self.assertRaises(KeyError, lambda : config.get('missing.option'))

    def test_with_default(self):
        config = Config(cfg='configurationVersion: \'{}\''.format(Config.CONFIG_VERSION), merge_defaults=False)
        self.assertEqual('test', config.get('missing.option', 'test'))

    def test_get_with_dict(self):
        self.assertEqual('Hi there!', Config.get_from_dict({'a': { 'b': 'Hi there!' } }, 'a.b', types=str))

    def test_load_from_file(self):
        cfile = os.path.join(self.testpath.path, 'test-config.yaml')
        with open(cfile, 'w') as f:
            f.write(self.CONFIG)
        config = Config(sources=[cfile], merge_defaults=False)
        self.assertEqual(10, config.get('io.rbd.simultaneousReads'))
