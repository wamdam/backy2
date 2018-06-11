#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
import operator
import os
from functools import reduce
from io import StringIO
from os.path import expanduser
from pathlib import Path

from ruamel.yaml import YAML

from benji.exception import ConfigurationError, InternalError
from benji.logging import logger


class Config:

    CONFIG_VERSION = '1.0.0'

    CONFIG_DIR = 'benji'
    CONFIG_FILE = 'benji.yaml'

    DEFAULT_CONFIG = """
    logFile: /tmp/benji.log
    blockSize: 4194304
    hashFunction: blake2b,digest_size=32
    process_name: benji
    disallowRemoveWhenYounger: 6
    dataBackend:
      simultaneousWrites: 1
      simultaneousReads: 1
      bandwidthRead: 0
      bandwidthWrite: 0
      s3:
        multiDelete: true
        useSsl: true
        addressingStyle: path
        disableEncodingType: false
      b2:
        writeObjectAttempts: 1
        readObjectAttempts: 1
        uploadAttempts: 5
    nbd:
      cacheDirectory: /tmp
    io:
      file:
        simultaneousReads: 1
      rbd:
        cephConfigFile: /etc/ceph/ceph.conf
        simultaneousReads: 1
    """

    REDACT = """
        dataBackend:
          s3:
            awsAccessKeyId: '<redacted>'
            awsSecretAccessKey: '<redacted>'
          b2:
            accountId: '<redacted>'
            applicationKey: '<redacted>'
          encryption: '<redacted>'
        """

    # Source: https://stackoverflow.com/questions/823196/yaml-merge-in-python
    @classmethod
    def _merge_dicts(cls, user, default):
        if isinstance(user, dict) and isinstance(default, dict):
            for k, v in default.items():
                if k not in user:
                    user[k] = v
                else:
                    user[k] = cls._merge_dicts(user[k], v)
        return user

    def __init__(self, cfg=None, sources=None, merge_defaults=True):
        yaml = YAML(typ='safe', pure=True)
        default_config = yaml.load(self.DEFAULT_CONFIG)

        if cfg is None:
            if not sources:
                sources = self._get_sources()

            config = None
            for source in sources:
                if os.path.isfile(source):
                    try:
                        config = yaml.load(Path(source))
                    except Exception as exception:
                        raise ConfigurationError('Configuration file {} is invalud.'.format(source)) from exception
                    if config is None:
                        raise ConfigurationError('Configuration file {} is empty.'.format(source))
                    break

            if not config:
                raise ConfigurationError('No configuration file found in the default places ({}).'.format(
                    ', '.join(sources)))
        else:
            config = yaml.load(cfg)
            if config is None:
                raise ConfigurationError('Configuration string is empty.')

        if 'configurationVersion' not in config or type(config['configurationVersion']) is not str:
            raise ConfigurationError('Configuration version is missing or not a string.')

        if config['configurationVersion'] != self.CONFIG_VERSION:
            raise ConfigurationError('Unknown configuration version {}.'.format(config['configurationVersion']))

        if merge_defaults:
            self._merge_dicts(config, default_config)

        with StringIO() as redacted_config_string:
            redacted_config = yaml.load(self.REDACT)
            self._merge_dicts(redacted_config, config)
            yaml.dump(redacted_config, redacted_config_string)
            logger.debug('Loaded configuration: {}'.format(redacted_config_string.getvalue()))

        self.config = config

    def _get_sources(self):
        sources = ['/etc/{file}'.format(file=self.CONFIG_FILE)]
        sources.append('/etc/{dir}/{file}'.format(dir=self.CONFIG_DIR, file=self.CONFIG_FILE))
        sources.append(expanduser('~/.{file}'.format(file=self.CONFIG_FILE)))
        sources.append(expanduser('~/{file}'.format(file=self.CONFIG_FILE)))
        return sources

    @staticmethod
    def _get(dict_, name, *args, types=None, check_func=None, check_message=None):
        if '__position' in dict_:
            full_name = '{}.{}'.format(dict_['__position'], name)
        else:
            full_name = name

        if len(args) > 1:
            raise InternalError('Called with more than two arguments for key {}.'.format(full_name))

        try:
            value = reduce(operator.getitem, name.split('.'), dict_)
            if types is not None and not isinstance(value, types):
                raise TypeError('Config value {} has wrong type {}, expected {}.'.format(full_name, type(value), types))
            if check_func is not None and not check_func(value):
                if check_message is None:
                    raise ConfigurationError('Config option {} has the right type but the supplied value is invalid.'
                                             .format(full_name))
                else:
                    raise ConfigurationError('Config option {} is invalid: {}.'.format(full_name, check_message))
            if isinstance(value, dict):
                value['__position'] = name
            return value
        except KeyError:
            if len(args) == 1:
                return args[0]
            else:
                if types and isinstance({}, types):
                    raise KeyError('Config section {} is missing.'.format(full_name)) from None
                else:
                    raise KeyError('Config option {} is missing.'.format(full_name)) from None

    def get(self, name, *args, **kwargs):
        return Config._get(self.config, name, *args, **kwargs)

    @staticmethod
    def get_from_dict(dict_, name, *args, **kwargs):
        return Config._get(dict_, name, *args, **kwargs)
