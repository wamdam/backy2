import pytest
import os
import sys
import backy2.main
import shutil

CHUNK_SIZE = 1024*4096

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


def test_display_usage(capsys, argv):
    with pytest.raises(SystemExit) as exit:
        backy2.main.main()
    assert exit.value.code == 0
    out, err = capsys.readouterr()
    assert """\
usage: py.test [-h] [-v] [-b BACKUPDIR] {backup,restore,scrub} ...
""" == out
    assert err == ""



# Test Level

def test_level_consistency(test_path):
    CHUNK_SIZE = 1024*4096
    s1 = os.urandom(CHUNK_SIZE)
    s2 = os.urandom(CHUNK_SIZE)
    s3 = os.urandom(CHUNK_SIZE)
    data_file = os.path.join(test_path, '_test.data')
    index_file = os.path.join(test_path, '_test.index')
    with backy2.main.Level(data_file, index_file, CHUNK_SIZE) as lw:
        lw.write(10, s1)
        lw.write(8, s2)
        lw.write(12, s3)

    with backy2.main.Level(data_file, index_file, CHUNK_SIZE) as lw:
        assert lw.read(8) == s2
        assert lw.read(10) == s1
        assert lw.read(12) == s3


def test_level_wrong_size(test_path):
    s1 = os.urandom(10)
    s2 = os.urandom(10)
    s3 = os.urandom(11)
    data_file = os.path.join(test_path, '_test.data')
    index_file = os.path.join(test_path, '_test.index')
    with backy2.main.Level(data_file, index_file, CHUNK_SIZE) as lw:
        lw.write(1, s1)
        lw.write(2, s2)
        with pytest.raises(backy2.main.BackyException):
            lw.write(1, s3)


def test_level_wrong_size_last_chunk(test_path):
    s1 = os.urandom(10)
    s2 = os.urandom(10)
    s3 = os.urandom(11)
    data_file = os.path.join(test_path, '_test.data')
    index_file = os.path.join(test_path, '_test.index')
    with backy2.main.Level(data_file, index_file, CHUNK_SIZE) as lw:
        lw.write(1, s1)
        lw.write(2, s2)
        lw.write(2, s3)

    with backy2.main.Level(data_file, index_file, CHUNK_SIZE) as lw:
        assert lw.read(1) == s1
        assert lw.read(2) == s3


def test_level_wrong_checksum(caplog, test_path):
    s1 = os.urandom(10)
    data_file = os.path.join(test_path, '_test.data')
    index_file = os.path.join(test_path, '_test.index')
    with backy2.main.Level(data_file, index_file, CHUNK_SIZE) as lw:
        lw.write(1, s1)
        # nasty hack, destroy checksum for chunk_id 1
        lw.index[1].checksum = 'haha'

    with backy2.main.Level(data_file, index_file, CHUNK_SIZE) as lw:
        lw.read(1)
        assert 'CRITICAL Checksum for chunk 1 does not match' in caplog.text()



# Test Backup

def test_backup(test_path):
    backy = backy2.main.Backy(test_path, 'backup', CHUNK_SIZE)

    data_1 = os.urandom(4*CHUNK_SIZE)   # 4 complete chunks
    data_2 = data_1 + os.urandom(10)    # append 10 bytes
    data_3 = data_2[:3*CHUNK_SIZE]      # truncate to 3 chunks
    data_4 = data_3 + os.urandom(10)    # append 10 bytes
    data_5 = data_3                     # remove those 10 bytes again

    src_1 = os.path.join(test_path, 'data_1')
    src_2 = os.path.join(test_path, 'data_2')
    src_3 = os.path.join(test_path, 'data_3')
    src_4 = os.path.join(test_path, 'data_4')
    src_5 = os.path.join(test_path, 'data_5')

    # this test backups and restores the generated data files and
    # tests them after restoring against filesize and content.

    # create backups

    with open(src_1, 'wb') as f:
        f.write(data_1)
    with open(src_2, 'wb') as f:
        f.write(data_2)
    with open(src_3, 'wb') as f:
        f.write(data_3)
    with open(src_4, 'wb') as f:
        f.write(data_4)
    with open(src_5, 'wb') as f:
        f.write(data_5)

    restore = os.path.join(test_path, 'restore')

    # 1st day, test backup
    backy.backup(src_1)
    # restore of level 0 is a 0 byte file.
    backy.restore(restore)
    assert open(restore, 'rb').read() == data_1
    backy.restore(restore, 0)
    assert open(restore, 'rb').read() == b''

    # 2nd day, test both backups
    backy.backup(src_2)
    backy.restore(restore)
    assert open(restore, 'rb').read() == data_2
    backy.restore(restore, 1)
    assert open(restore, 'rb').read() == data_1
    backy.restore(restore, 0)
    assert open(restore, 'rb').read() == b''

    # 3rd day, test all backups
    backy.backup(src_3)
    backy.restore(restore)
    assert open(restore, 'rb').read() == data_3
    backy.restore(restore, 2)
    assert open(restore, 'rb').read() == data_2
    backy.restore(restore, 1)
    assert open(restore, 'rb').read() == data_1
    backy.restore(restore, 0)
    assert open(restore, 'rb').read() == b''

    # 4th day, test all backups
    backy.backup(src_4)
    backy.restore(restore)
    assert open(restore, 'rb').read() == data_4
    backy.restore(restore, 3)
    assert open(restore, 'rb').read() == data_3
    backy.restore(restore, 2)
    assert open(restore, 'rb').read() == data_2
    backy.restore(restore, 1)
    assert open(restore, 'rb').read() == data_1
    backy.restore(restore, 0)
    assert open(restore, 'rb').read() == b''

    # 5th day, test all backups
    backy.backup(src_5)
    backy.restore(restore)
    assert open(restore, 'rb').read() == data_5
    backy.restore(restore, 4)
    assert open(restore, 'rb').read() == data_4
    backy.restore(restore, 3)
    assert open(restore, 'rb').read() == data_3
    backy.restore(restore, 2)
    assert open(restore, 'rb').read() == data_2
    backy.restore(restore, 1)
    assert open(restore, 'rb').read() == data_1
    backy.restore(restore, 0)
    assert open(restore, 'rb').read() == b''

