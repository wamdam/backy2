#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.backy import Backy
from functools import partial
import ctypes
import hashlib
import importlib
import json
import psutil
import sys


def hints_from_rbd_diff(rbd_diff):
    """ Return the required offset:length tuples from a rbd json diff
    """
    data = json.loads(rbd_diff)
    return [(l['offset'], l['length'], False if l['exists']=='false' or not l['exists'] else True) for l in data]


def backy_from_config(Config):
    """ Create a partial backy class from a given Config object
    """
    config_DEFAULTS = Config(section='DEFAULTS')
    block_size = config_DEFAULTS.getint('block_size')
    hash_function = getattr(hashlib, config_DEFAULTS.get('hash_function', 'sha512'))
    lock_dir = config_DEFAULTS.get('lock_dir', None)
    process_name = config_DEFAULTS.get('process_name', 'backy2')

    # configure meta backend
    config_MetaBackend = Config(section='MetaBackend')
    try:
        MetaBackendLib = importlib.import_module(config_MetaBackend.get('type'))
    except ImportError:
        raise NotImplementedError('MetaBackend type {} unsupported.'.format(config_MetaBackend.get('type')))
    else:
        meta_backend = MetaBackendLib.MetaBackend(config_MetaBackend)

    # configure file backend
    config_DataBackend = Config(section='DataBackend')
    try:
        DataBackendLib = importlib.import_module(config_DataBackend.get('type'))
    except ImportError:
        raise NotImplementedError('DataBackend type {} unsupported.'.format(config_DataBackend.get('type')))
    else:
        data_backend = DataBackendLib.DataBackend(config_DataBackend)

    # configure reader
    config_Reader = Config(section='Reader')
    try:
        ReaderLib = importlib.import_module(config_Reader.get('type'))
    except ImportError:
        raise NotImplementedError('Reader type {} unsupported.'.format(config_Reader.get('type')))
    else:
        reader = ReaderLib.Reader(
                config_Reader,
                block_size=block_size,
                hash_function=hash_function,
                )

    backy = partial(Backy,
            meta_backend=meta_backend,
            data_backend=data_backend,
            reader=reader,
            block_size=block_size,
            hash_function=hash_function,
            lock_dir=lock_dir,
            process_name=process_name,
            )
    return backy


def setprocname(name):
    """ Set own process name """
    if sys.platform not in ('linux', 'linux2'):
        # XXX: Would this work in bsd or so too similarly?
        raise RuntimeError("Unable to set procname when not in linux.")
    if type(name) is not bytes:
        name = str(name)
        name = bytes(name.encode("utf-8"))
    libc = ctypes.cdll.LoadLibrary('libc.so.6')
    return libc.prctl(15, name, 0, 0, 0)  # 15 = PR_SET_NAME, returns 0 on success.


def getprocname():
    return sys.argv[0]


def find_other_procs(name):
    """ returns other processes by given name """
    return [p for p in psutil.process_iter() if p.name() == name]

