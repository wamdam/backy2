import pytest
import os
#import sys
#import backy2.backy
import shutil
import backy2.lfstore
#import time
#import random
#import uuid

BLOCK_SIZE = 1024*4096

@pytest.fixture(scope="function")
def test_path(request):
    path = '_testbackup'
    os.mkdir(path)
    def fin():
        shutil.rmtree(path)
    request.addfinalizer(fin)
    return path


def test_bla(test_path):
    lfstore = backy2.lfstore.LFStore(test_path, [])

