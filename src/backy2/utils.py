#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from functools import partial
from time import time
from threading import Lock
import itertools
import hashlib
import importlib
import json
import random
from datetime import timedelta, datetime


def convert_to_timedelta(time_val):
    """
    Given a *time_val* (string) such as '5d', returns a timedelta object
    representing the given value (e.g. timedelta(days=5)).  Accepts the
    following '<num><char>' formats:

    =========   ======= ===================
    Character   Meaning Example
    =========   ======= ===================
    s           Seconds '60s' -> 60 Seconds
    m           Minutes '5m'  -> 5 Minutes
    h           Hours   '24h' -> 24 Hours
    d           Days    '7d'  -> 7 Days
    =========   ======= ===================

    Examples::

        >>> convert_to_timedelta('7d')
        datetime.timedelta(7)
        >>> convert_to_timedelta('24h')
        datetime.timedelta(1)
        >>> convert_to_timedelta('60m')
        datetime.timedelta(0, 3600)
        >>> convert_to_timedelta('120s')
        datetime.timedelta(0, 120)
    """
    num = int(time_val[:-1])
    if time_val.endswith('s'):
        return timedelta(seconds=num)
    elif time_val.endswith('m'):
        return timedelta(minutes=num)
    elif time_val.endswith('h'):
        return timedelta(hours=num)
    elif time_val.endswith('d'):
        return timedelta(days=num)
    else:
        raise ValueError('Unknown timedelta format: {}'.format(time_val))


# Credits: https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def humanize(num, suffix='B'):
    if num == 0:
        return '0'
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%.0f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.0f %s%s" % (num, 'Yi', suffix)


def parse_expire_date(date_string):
    try:
        date = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            date = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            try:
                date = datetime.strptime(date_string, '%Y-%m-%d')
            except ValueError:
                raise
    return date


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
    dedup = config_DEFAULTS.getboolean('deduplication', True)
    encryption_password = config_DEFAULTS.get('encryption_password', None)  # if None then version=0 will be used (no encryption)

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
        data_backend = DataBackendLib.DataBackend(config_DataBackend, encryption_password)

    from backy2.backy import Backy
    backy = partial(Backy,
            meta_backend=meta_backend,
            data_backend=data_backend,
            config=Config,
            block_size=block_size,
            hash_function=hash_function,
            lock_dir=lock_dir,
            process_name=process_name,
            dedup=dedup,
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


def generate_block(id_, size):
    payload = (id_).to_bytes(16, byteorder='little')
    data = (payload + b' ' * (size))[:size]
    return data


def _progressbar(pct):
    return ("[%-10s]" % ('='*(round(pct/10))))


def _eta(eta_s):
    if eta_s < 2 * 60:
        return "{:d}s".format(eta_s)
    elif eta_s < 60 * 60:
        #return "{:d}m{:d}s".format(eta_s//60, eta_s-eta_s//60*60)
        return "{:d}m{:d}s".format(eta_s//60, eta_s%60)
    else:
        #return "{:d}h{:d}m".format(eta_s//60//60, eta_s//60-eta_s//60//60*60*60)
        return "{:d}h{:d}m".format(eta_s//60//60, eta_s//60%(60))


def status(msg, rq_len_pct, wq_len_pct, progress_pct, write_tp, eta_s, other=""):
    """
    Restoring to null:// Read Queue [====      ] Write Queue [==========] (23.5% 71MB/s ETA 1h32m)
    Backing up rbd://vms/test Read Queue [=         ] Write Queue [==========] (11.3% 96MB/s ETA 120s)
    """
    return("{} Read Queue {} Write Queue {} ({:.1f}% {:.1f}MB/sØ ETA {}) {}".format(
        msg,
        _progressbar(rq_len_pct),
        _progressbar(wq_len_pct),
        progress_pct,
        write_tp / 1024 / 1024,
        _eta(eta_s),
        other,
        ))


class MinSequential():
    OPTIMIZE_PROBABILITY = 0.1  # 1 means every put

    def __init__(self, absolute_minimum=0):
        self._entries = []
        self.lock = Lock()
        self.absolute_minimum = absolute_minimum


    def put(self, entry):
        with self.lock:
            self._entries.append(entry)
            if random.random() < self.OPTIMIZE_PROBABILITY:
                self._optimize()


    def _optimize(self):
        """
        optimize self._entries
        [0, 1, 2, 3, 5, 7] =>  [3, 5, 7]
        """
        if len(self._entries) < 2:
            return

        self._entries.sort()
        new_entries = []
        if self._entries[0] != self.absolute_minimum:
            # we must start at this value.
            return

        for entry in self._entries[1:]:
            if entry == self.absolute_minimum + 1:
                self.absolute_minimum = entry
            else:
                new_entries.append(entry)
        new_entries.insert(0, self.absolute_minimum)
        self._entries = new_entries


    def get(self):
        """
        Returns the smallest integer in entries up to which numbers
        are in sequence, i.e. where there's no gap.

        Example: In a list of [4, 5, 6, 8, 9] it would return 6.
        In a list of [10, 13, 15] it would return 10.

        Note: The list is ordered first.
        """
        if len(self._entries) == 0:
            return None

        with self.lock:
            self._optimize()
            return self._entries[0] if self._entries[0] == self.absolute_minimum else None


#print(status("Restoring to null://", 23, 87, 23.5, 71541112, 3700))

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
