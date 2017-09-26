#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

from configparser import SafeConfigParser, NoSectionError, NoOptionError
from io import StringIO
from os.path import expanduser
import glob


default_config = """
[DEFAULTS]
logfile: /tmp/backy.log
block_size: 4194304
hash_function: sha512
disallow_rm_when_younger_than_days: 0
lock_dir: /tmp

[MetaBackend]
type: backy2.meta_backends.sql
engine: sqlite:////tmp/backy.sqlite

[DataBackend]
type: backy2.data_backends.file
path: /tmp
simultaneous_writes: 1
simultaneous_reads: 1

[NBD]
cachedir: /tmp

[io_file]
simultaneous_reads: 1

[io_rbd]
simultaneous_reads: 1
"""

class Config(object):
    """A ConfigParser wrapper to support defaults when calling instance
    methods, and also tied to a single section"""

    SECTION = 'DEFAULTS'

    def __init__(self, cfg=None, extra_sources=(), section=None, conf_name=None):
        """
            cfg may be a string containing a configuration.
            extra_sources is a list of explicit filenames to parse.
            section initializes this class to default to this section.
            conf_name, if given, is resolved to {conf_name}.cfg in several places (see _getsources)
        """
        if section is not None:
            self.SECTION = section
        if cfg is None:
            self.cp = SafeConfigParser()
            self.cp.readfp(StringIO(default_config))
            if conf_name:
                sources = self._getsources(conf_name)
                self.cp.read(sources)
            for fp in extra_sources:
                self.cp.readfp(fp)
        else:
            self.cp = SafeConfigParser()
            self.cp.readfp(StringIO(cfg))

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

    def getlist(self, option, default=None):
        return self._getany(self.cp.get, option, default).split()

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
