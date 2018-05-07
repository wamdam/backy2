#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import json
import setproctitle
from ast import literal_eval
from threading import Lock
from time import time

import hashlib
import importlib
from functools import partial

from backy2.exception import ConfigurationError
from backy2.logging import logger


def hints_from_rbd_diff(rbd_diff):
    """ Return the required offset:length tuples from a rbd json diff
    """
    data = json.loads(rbd_diff)
    return [(l['offset'], l['length'], False if l['exists']=='false' or not l['exists'] else True) for l in data]


def parametrized_hash_function(config_hash_function):
    hash_name = None
    hash_args = None
    try:
        hash_name, hash_args = config_hash_function.split(',', 1)
    except ValueError:
        hash_name = config_hash_function
    hash_function = getattr(hashlib, hash_name)
    if hash_function is None:
        raise NotImplementedError('Unsupported hash function {}'.format(hash_name))
    kwargs = {}
    if hash_args is not None:
        kwargs = dict((k, literal_eval(v)) for k, v in (pair.split('=') for pair in hash_args.split(',')))
    logger.debug('Using hash function {} with kwargs {}'.format(hash_name, kwargs))
    hash_function_w_kwargs = hash_function(**kwargs)

    from backy2.meta_backends import MetaBackend
    if len(hash_function_w_kwargs.digest()) > MetaBackend.MAXIMUM_CHECKSUM_LENGTH:
        raise RuntimeError('Specified hash function exceeds maximum digest length of {}'
                           .format(MetaBackend.MAXIMUM_CHECKSUM_LENGTH))

    return hash_function_w_kwargs


def data_hexdigest(hash_function, data):
    hash = hash_function.copy()
    hash.update(data)
    return hash.hexdigest()

def backy_from_config(config):
    """ Create a partial backy class from a given Config object
    """
    block_size = config.get('blockSize', types=int)
    hash_function = parametrized_hash_function(config.get('hashFunction', types=str))
    lock_dir = config.get('lockDirectory', types=str)
    process_name = config.get('processName', types=str)

    from backy2.data_backends import DataBackend
    name = config.get('dataBackend.type', None, types=str)
    if name is not None:
        try:
            DataBackendLib = importlib.import_module('{}.{}'.format(DataBackend.PACKAGE_PREFIX, name))
        except ImportError:
            raise NotImplementedError('Data backend type {} unsupported'.format(name))
        else:
            data_backend = DataBackendLib.DataBackend(config)

    from backy2.meta_backends import MetaBackend
    name = config.get('metaBackend.type', None, types=str)
    if name is not None:
        try:
            MetaBackendLib = importlib.import_module('{}.{}'.format(MetaBackend.PACKAGE_PREFIX, name))
        except ImportError:
            raise NotImplementedError('Meta backend type {} unsupported'.format(name))
        else:
            meta_backend = MetaBackendLib.MetaBackend(config)

    from backy2.backy import Backy
    backy = partial(Backy,
                    meta_backend=meta_backend,
                    data_backend=data_backend,
                    config=config,
                    block_size=block_size,
                    hash_function=hash_function,
                    lock_dir=lock_dir,
                    process_name=process_name,
            )
    return backy


def notify(process_name, msg=''):
    """ This method can receive notifications and append them in '[]' to the
    process name seen in ps, top, ...
    """
    if msg:
        new_msg = '{} [{}]'.format(
            process_name,
            msg.replace('\n', ' ')
        )
    else:
        new_msg = process_name

    setproctitle.setproctitle(new_msg)


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
