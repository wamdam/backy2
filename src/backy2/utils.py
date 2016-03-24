#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.backy import Backy
from functools import partial
import hashlib
import importlib
import json


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


