#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import concurrent
import hashlib
import importlib
import json
import os
import setproctitle
import sys
from ast import literal_eval
from functools import partial
from threading import Lock
from time import time

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
        raise ConfigurationError('Unsupported hash function {}.'.format(hash_name))
    kwargs = {}
    if hash_args is not None:
        kwargs = dict((k, literal_eval(v)) for k, v in (pair.split('=') for pair in hash_args.split(',')))
    logger.debug('Using hash function {} with kwargs {}'.format(hash_name, kwargs))
    hash_function_w_kwargs = hash_function(**kwargs)

    from backy2.meta_backends import MetaBackend
    if len(hash_function_w_kwargs.digest()) > MetaBackend.MAXIMUM_CHECKSUM_LENGTH:
        raise ConfigurationError('Specified hash function exceeds maximum digest length of {}.'
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
            raise ConfigurationError('Data backend type {} not found.'.format(name))
        else:
            data_backend = DataBackendLib.DataBackend(config)

    from backy2.meta_backends import MetaBackend
    name = config.get('metaBackend.type', None, types=str)
    if name is not None:
        try:
            MetaBackendLib = importlib.import_module('{}.{}'.format(MetaBackend.PACKAGE_PREFIX, name))
        except ImportError:
            raise ConfigurationError('Meta backend type {} not found.'.format(name))
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


def makedirs(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


# This is tricky to implement as we need to make sure that we don't hold a reference to the completed Future anymore.
# Indeed it's so tricky that older Python versions had the same problem. See https://bugs.python.org/issue27144.
def future_results_as_completed(futures, semaphore):
    if sys.version_info < (3,6,4):
        logger.warn('Large backup jobs are likely to fail because of excessive memory usage. '
                    + 'Upgrade your Python to at least 3.6.4.')

    for future in concurrent.futures.as_completed(futures):
        futures.remove(future)
        semaphore.release()
        result = future.result()
        del future
        yield result


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
