import pytest
import os
import sys
import backy2.backy
import shutil
#import time
#import random
import uuid

CHUNK_SIZE = 1024*4096
CHUNK_SIZE_MIN = 1024

@pytest.yield_fixture
def argv():
    original = sys.argv
    new = original[:1]
    sys.argv = new
    yield new
    sys.argv = original


@pytest.fixture(scope="function")
def test_path(request):
    path = '_testbackup'
    os.mkdir(path)
    def fin():
        shutil.rmtree(path)
    request.addfinalizer(fin)
    return path


def test_FileBackend_path(test_path):
    uid = 'c2cac25a7afd11e5b45aa44e314f9270'

    backend = backy2.backy.FileBackend(test_path)
    backend.DEPTH = 2
    backend.SPLIT = 2
    path = backend._path(uid)
    assert path == 'c2/ca'

    backend.DEPTH = 3
    backend.SPLIT = 2
    path = backend._path(uid)
    assert path == 'c2/ca/c2'

    backend.DEPTH = 3
    backend.SPLIT = 3
    path = backend._path(uid)
    assert path == 'c2c/ac2/5a7'

    backend.DEPTH = 3
    backend.SPLIT = 1
    path = backend._path(uid)
    assert path == 'c/2/c'

    backend.DEPTH = 1
    backend.SPLIT = 2
    path = backend._path(uid)
    assert path == 'c2'

    backend.close()


def test_FileBackend_save_read(test_path):
    backend = backy2.backy.FileBackend(test_path)
    uid = backend.save(b'test')
    assert backend.read(uid) == b'test'
    backend.close()


def test_SQLiteBackend_set_version(test_path):
    backend = backy2.backy.SQLiteBackend(test_path)
    name = 'backup-mysystem1-20150110140015'
    uid = backend.set_version(name, 10, 5000, 1)
    assert(uid)
    version = backend.get_version(uid)
    assert version['name'] == name
    assert version['size'] == 10
    assert version['size_bytes'] == 5000
    assert version['uid'] == uid
    assert version['valid'] == 1
    backend.close()


def test_SQLiteBackend_version_not_found(test_path):
    backend = backy2.backy.SQLiteBackend(test_path)
    with pytest.raises(KeyError) as e:
        backend.get_version('123')
    assert str(e.exconly()) == "KeyError: 'Version 123 not found.'"
    backend.close()


def test_SQLiteBackend_block(test_path):
    backend = backy2.backy.SQLiteBackend(test_path)
    name = 'backup-mysystem1-20150110140015'
    block_uid = 'asdfgh'
    checksum = '1234567890'
    size = 5000
    id = 0
    version_uid = backend.set_version(name, 10, 5000, 1)
    backend.set_block(id, version_uid, block_uid, checksum, size, 1)

    block = backend.get_block(block_uid)

    assert block['checksum'] == checksum
    assert block['uid'] == block_uid
    assert block['id'] == id
    assert block['size'] == size
    assert block['version_uid'] == version_uid

    backend.close()


def test_SQLiteBackend_blocks_by_version(test_path):
    TESTLEN = 10
    backend = backy2.backy.SQLiteBackend(test_path)
    version_name = 'backup-mysystem1-20150110140015'
    version_uid = backend.set_version(version_name, TESTLEN, 5000, 1)
    block_uids = [uuid.uuid1().hex for i in range(TESTLEN)]
    checksums = [uuid.uuid1().hex for i in range(TESTLEN)]
    size = 5000

    for id in range(TESTLEN):
        backend.set_block(id, version_uid, block_uids[id], checksums[id], size, 1)

    blocks = backend.get_blocks_by_version(version_uid)
    assert len(blocks) == TESTLEN

    # blocks are always ordered by id
    for id in range(TESTLEN):
        block = blocks[id]
        assert block['id'] == id
        assert block['checksum'] == checksums[id]
        assert block['uid'] == block_uids[id]
        assert block['size'] == size
        assert block['version_uid'] == version_uid

    backend.close()



def _patch(filename, offset, data=None):
    """ write data into a file at offset """
    if not os.path.exists(filename):
        open(filename, 'wb')
    with open(filename, 'r+b') as f:
        f.seek(offset)
        f.write(data)

