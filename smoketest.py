#!/usr/bin/env python3

import os
import shutil
import random
import json
import hashlib
from backy2.scripts.backy import hints_from_rbd_diff
from backy2.logging import init_logging
from backy2.utils import backy_from_config
from backy2.config import Config as _Config
from functools import partial
import logging

kB = 1024
MB = kB * 1024
GB = MB * 1024

HASH_FUNCTION = hashlib.sha512

def patch(path, filename, offset, data=None):
    """ write data into a file at offset """
    filename = os.path.join(path, filename)
    if not os.path.exists(filename):
        open(filename, 'wb')
    with open(filename, 'r+b') as f:
        f.seek(offset)
        f.write(data)


def same(f1, f2):
    """ returns False if files differ, True if they are the same """
    d1 = open(f1, 'rb').read()
    d2 = open(f1, 'rb').read()
    return d1 == d2


class TestPath():
    path = '_smoketest'

    def __enter__(self):
        os.mkdir(self.path)
        return self.path


    def __exit__(self, type, value, traceback):
        shutil.rmtree(self.path)


with TestPath() as testpath:
    from_version = None
    init_logging(testpath+'/backy.log', logging.INFO)

    for i in range(100):
        size = 32*4*kB + random.randint(-4*kB, 4*kB)
        print('Run {}'.format(i+1))
        hints = []
        for i in range(random.randint(0, 10)):  # up to 10 changes
            if random.randint(0, 1):
                patch_size = random.randint(0, 4*kB)
                data = os.urandom(patch_size)
                #exists = True
                exists = "true"
            else:
                patch_size = random.randint(0, 4*4*kB)  # we want full blocks sometimes
                data = b'\0' * patch_size
                #exists = False
                exists = "false"
            offset = random.randint(0, size-1-patch_size)
            print('    Applied change at {}:{}, exists {}'.format(offset, patch_size, exists))
            patch(testpath, 'data', offset, data)
            hints.append({'offset': offset, 'length': patch_size, 'exists': exists})
        # truncate?
        open(os.path.join(testpath, 'data'), 'r+b').truncate(size)

        print('  Applied {} changes, size is {}.'.format(len(hints), size))
        open(os.path.join(testpath, 'hints'), 'w').write(json.dumps(hints))

        # create backy
        config = """
        [DEFAULTS]
        logfile: /var/log/backy.log
        block_size: 4096
        hash_function: sha512

        [MetaBackend]
        type: backy2.meta_backends.sql
        engine: sqlite:///{testpath}/backy.sqlite

        [DataBackend]
        type: backy2.data_backends.file
        path: {testpath}
        simultaneous_writes: 5

        [NBD]
        cachedir: /tmp

        [Reader]
        type: backy2.readers.file
        simultaneous_reads: 5
        """.format(testpath=testpath)
        Config = partial(_Config, cfg=config)
        backy = backy_from_config(Config)()
        version_uid = backy.backup(
            'data-backup',
            os.path.join(testpath, 'data'),
            hints_from_rbd_diff(open(os.path.join(testpath, 'hints')).read()),
            from_version
            )

        try:
            assert backy.scrub(version_uid) == True
            print('  Scrub successful')
            assert backy.scrub(version_uid, os.path.join(testpath, 'data')) == True
            print('  Deep scrub successful')
            backy.restore(version_uid, os.path.join(testpath, 'restore'), sparse=False)
            assert same(os.path.join(testpath, 'data'), os.path.join(testpath, 'restore')) == True
            print('  Restore successful')
        except AssertionError:
            import pdb; pdb.set_trace()

        from_version = version_uid
        backy.close()
    #import pdb; pdb.set_trace()
