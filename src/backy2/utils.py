#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from functools import partial
from time import time
from threading import Lock
import itertools
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

    from backy2.backy import Backy
    backy = partial(Backy,
            meta_backend=meta_backend,
            data_backend=data_backend,
            config=Config,
            block_size=block_size,
            hash_function=hash_function,
            lock_dir=lock_dir,
            process_name=process_name,
            )
    return backy


def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk


# token_bucket.py
class TokenBucket:
    """
    An implementation of the token bucket algorithm.
    """
    def __init__(self):
        self.tokens = 0
        self.rate = 0
        self.last = time()
        self.lock = Lock()


    def set_rate(self, rate):
        with self.lock:
            self.rate = rate
            self.tokens = self.rate


    def consume(self, tokens):
        with self.lock:
            if not self.rate:
                return 0

            now = time()
            lapse = now - self.last
            self.last = now
            self.tokens += lapse * self.rate

            if self.tokens > self.rate:
                self.tokens = self.rate

            self.tokens -= tokens

            if self.tokens >= 0:
                #print("Tokens: {}".format(self.tokens))
                return 0
            else:
                #print("Recommended nap: {}".format(-self.tokens / self.rate))
                return -self.tokens / self.rate


#if __name__ == '__main__':
#    import sys
#    from time import sleep
#    bucket = TokenBucket()
#    bucket.set_rate(80*1024*1024)  # 80MB/s
#    for _ in range(100):
#        print("Tokens: {}".format(bucket.tokens))
#        nap = bucket.consume(4*1024*1024)
#        print(nap)
#        sleep(nap)
#        print(".")
#    sys.exit(0)
