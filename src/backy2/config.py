#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

# Credits: scrapy.

import glob
from io import StringIO
from configparser import SafeConfigParser, NoSectionError, NoOptionError
from os.path import expanduser

default_config = """
[DEFAULTS]
logfile: /tmp/backy.log
block_size: 4194304
hash_function: sha512

[MetaBackend]
type: backy2.meta_backends.sql
engine: sqlite:////tmp/backy.sqlite
#engine: postgresql:///dk

[DataBackend]
#type: backy2.data_backends.file
#path: /home/dk/develop/backy2/tmp
type: backy2.data_backends.s3
aws_access_key_id:
aws_secret_access_key:
host: 127.0.0.1
port: 10001
is_secure: false
bucket_name: backy2
simultaneous_writes: 5

[NBD]
cachedir: /tmp

[Reader]
#type: file
#simultaneous_reads: 1
# ---
type: librbd
ceph_conffile: /etc/ceph/ceph.conf
simultaneous_reads: 10
"""

class Config(object):
    """A ConfigParser wrapper to support defaults when calling instance
    methods, and also tied to a single section"""

    SECTION = 'DEFAULTS'

    def __init__(self, values=None, extra_sources=(), section=None, conf_name=None):
        if section is not None:
            self.SECTION = section
        if values is None:
            self.cp = SafeConfigParser()
            self.cp.readfp(StringIO(default_config))
            if conf_name:
                sources = self._getsources(conf_name)
            self.cp.read(sources)
            for fp in extra_sources:
                self.cp.readfp(fp)
        else:
            self.cp = SafeConfigParser(values)
            self.cp.add_section(self.SECTION)

    def _getsources(self, conf_name):
        sources = ['/etc/{name}.cfg'.format(name=conf_name)]
        sources.append('/etc/{name}/{name}.cfg'.format(name=conf_name))
        sources.extend(sorted(glob.glob('/etc/{name}/conf.d/*'.format(name=conf_name))))
        sources.append('~/.{name}.cfg'.format(name=conf_name))
        sources.append(expanduser('{name}.cfg'.format(name=conf_name)))
        return sources

    def _getany(self, method, option, default):
        try:
            return method(self.SECTION, option)
        except (NoSectionError, NoOptionError):
            if default is not None:
                return default
            raise

    def get(self, option, default=None):
        return self._getany(self.cp.get, option, default)

    def getint(self, option, default=None):
        return self._getany(self.cp.getint, option, default)

    def getfloat(self, option, default=None):
        return self._getany(self.cp.getfloat, option, default)

    def getboolean(self, option, default=None):
        return self._getany(self.cp.getboolean, option, default)

    def items(self, section, default=None):
        try:
            return self.cp.items(section)
        except (NoSectionError, NoOptionError):
            if default is not None:
                return default
            raise
