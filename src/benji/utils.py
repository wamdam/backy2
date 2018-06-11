#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import concurrent
import hashlib
import json
import setproctitle
import sys
from ast import literal_eval
from threading import Lock
from time import time

from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import PBKDF2
from dateutil.relativedelta import relativedelta

from benji.exception import ConfigurationError
from benji.logging import logger


def hints_from_rbd_diff(rbd_diff):
    """ Return the required offset:length tuples from a rbd json diff
    """
    data = json.loads(rbd_diff)
    return [(l['offset'], l['length'], False if l['exists'] == 'false' or not l['exists'] else True) for l in data]


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

    from benji.metadata import Block
    if len(hash_function_w_kwargs.digest()) > Block.MAXIMUM_CHECKSUM_LENGTH:
        raise ConfigurationError('Specified hash function exceeds maximum digest length of {}.'
                                 .format(Block.MAXIMUM_CHECKSUM_LENGTH))

    return hash_function_w_kwargs


def data_hexdigest(hash_function, data):
    hash = hash_function.copy()
    hash.update(data)
    return hash.hexdigest()


# old_msg is used as a stateful storage between calls
def notify(process_name, msg='', old_msg=''):
    """ This method can receive notifications and append them in '[]' to the
    process name seen in ps, top, ...
    """
    if msg:
        new_msg = '{} [{}]'.format(process_name, msg.replace('\n', ' '))
    else:
        new_msg = process_name

    if old_msg != new_msg:
        old_msg = new_msg
        setproctitle.setproctitle(new_msg)


# This is tricky to implement as we need to make sure that we don't hold a reference to the completed Future anymore.
# Indeed it's so tricky that older Python versions had the same problem. See https://bugs.python.org/issue27144.
def future_results_as_completed(futures, semaphore=None, timeout=None):
    if sys.version_info < (3, 6, 4):
        logger.warning('Large backup jobs are likely to fail because of excessive memory usage. ' + 'Upgrade your Python to at least 3.6.4.')

    for future in concurrent.futures.as_completed(futures, timeout=timeout):
        futures.remove(future)
        if semaphore and not future.cancelled():
            semaphore.release()
        try:
            result = future.result()
        except Exception as exception:
            result = exception
        del future
        yield result


def derive_key(*, password, salt, iterations, key_length):
    return PBKDF2(password=password, salt=salt, dkLen=key_length, count=iterations, hmac_hash_module=SHA512)


# Based on https://code.activestate.com/recipes/578113-human-readable-format-for-a-given-time-delta/
def human_readable_duration(duration):
    delta = relativedelta(seconds=duration)
    attrs = ['years', 'months', 'days', 'hours', 'minutes', 'seconds']
    readable = []
    for attr in attrs:
        if getattr(delta, attr) or attr == attrs[-1]:
            readable.append('{}{}'.format(getattr(delta, attr), attr[:1]))
    return ' '.join(readable)


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
