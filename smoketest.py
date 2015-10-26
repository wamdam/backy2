#!/usr/bin/env python3

import os
import shutil
import random
import json
from backy2.backy import Backy
from backy2.backy import hints_from_rbd_diff

kB = 1024
MB = kB * 1024
GB = MB * 1024

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
    size = 32*4*kB + random.randint(-4*kB, 4*kB)
    from_version = None

    for i in range(100):
        print('Run {}'.format(i+1))
        hints = []
        for i in range(random.randint(0, 10)):  # up to 10 changes
            if random.randint(0, 1):
                patch_size = random.randint(0, 4*kB)
                data = os.urandom(patch_size)
                exists = True
            else:
                patch_size = random.randint(0, 4*4*kB)  # we want full blocks sometimes
                data = b'\0' * patch_size
                exists = False
            offset = random.randint(0, size-1-patch_size)
            print('    Applied change at {}:{}, exists {}'.format(offset, patch_size, exists))
            patch(testpath, 'data', offset, data)
            hints.append({'offset': offset, 'length': patch_size, 'exists': exists})
        print('  Applied {} changes.'.format(len(hints)))
        open(os.path.join(testpath, 'hints'), 'w').write(json.dumps(hints))
        backy = Backy(os.path.join(testpath, 'backy'), block_size=4096)
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
