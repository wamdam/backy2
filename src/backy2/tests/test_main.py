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

def test_level_consistency():
    CHUNK_SIZE = 1024*4096
    s1 = os.urandom(CHUNK_SIZE)
    s2 = os.urandom(CHUNK_SIZE)
    s3 = os.urandom(CHUNK_SIZE)
    with backy2.main.Level('_test.data', '_test.index', CHUNK_SIZE) as lw:
        lw.write(10, s1)
        lw.write(8, s2)
        lw.write(12, s3)

    with backy2.main.Level('_test.data', '_test.index', CHUNK_SIZE, temporary=True) as lw:
        assert lw.read(8) == s2
        assert lw.read(10) == s1
        assert lw.read(12) == s3


def test_level_wrong_size():
    s1 = os.urandom(10)
    s2 = os.urandom(10)
    s3 = os.urandom(11)
    with backy2.main.Level('_test.data', '_test.index', CHUNK_SIZE, temporary=True) as lw:
        lw.write(1, s1)
        lw.write(2, s2)
        with pytest.raises(backy2.main.BackyException):
            lw.write(1, s3)


def test_level_wrong_size_last_chunk():
    s1 = os.urandom(10)
    s2 = os.urandom(10)
    s3 = os.urandom(11)
    with backy2.main.Level('_test.data', '_test.index', CHUNK_SIZE) as lw:
        lw.write(1, s1)
        lw.write(2, s2)
        lw.write(2, s3)

    with backy2.main.Level('_test.data', '_test.index', CHUNK_SIZE, temporary=True) as lw:
        assert lw.read(1) == s1
        assert lw.read(2) == s3


def test_level_wrong_checksum(caplog):
    s1 = os.urandom(10)
    with backy2.main.Level('_test.data', '_test.index', CHUNK_SIZE) as lw:
        lw.write(1, s1)
        # nasty hack, destroy checksum for chunk_id 1
        lw.index[1].checksum = 'haha'

    with backy2.main.Level('_test.data', '_test.index', CHUNK_SIZE, temporary=True) as lw:
        lw.read(1)
        assert 'CRITICAL Checksum for chunk 1 does not match' in caplog.text()



# Test Backup

def test_backup():
    path = '_testbackup'
    src = 'src'
    dst = 'dst'

    os.mkdir(path)
    with open(os.path.join(path, src), 'wb') as f:
        f.write(os.urandom(4*CHUNK_SIZE))
    backy = backy2.main.Backy(path, dst, CHUNK_SIZE)
    backy.backup(os.path.join(path, src), dst)

    shutil.rmtree(path)

