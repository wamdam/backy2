import pytest
import os
import sys
import backy2.main
import shutil
import time

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
    s1 = os.urandom(CHUNK_SIZE_MIN)
    s2 = os.urandom(CHUNK_SIZE_MIN)
    s3 = os.urandom(CHUNK_SIZE_MIN)
    data_file = os.path.join(test_path, '_test.data')
    index_file = os.path.join(test_path, '_test.index')
    with backy2.main.Level(data_file, index_file, CHUNK_SIZE_MIN) as lw:
        lw.write(10, s1)
        lw.write(8, s2)
        lw.write(12, s3)

    with backy2.main.Level(data_file, index_file, CHUNK_SIZE_MIN) as lw:
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
    backy = backy2.main.Backy(test_path, 'backup', CHUNK_SIZE_MIN)

    data_1 = os.urandom(4*CHUNK_SIZE_MIN)                           # 4 complete chunks
    data_2 = data_1 + os.urandom(10)                                # append 10 bytes
    data_3 = data_2[:3*CHUNK_SIZE_MIN-10]                           # truncate to 3 chunks - 10 bytes
    data_4 = data_3 + os.urandom(10)                                # append 10 bytes
    data_5 = data_3                                                 # remove those 10 bytes again
    data_6 = os.urandom(CHUNK_SIZE_MIN) + data_5[CHUNK_SIZE_MIN:]   # Change 1st chunk

    src_1 = os.path.join(test_path, 'data_1')
    src_2 = os.path.join(test_path, 'data_2')
    src_3 = os.path.join(test_path, 'data_3')
    src_4 = os.path.join(test_path, 'data_4')
    src_5 = os.path.join(test_path, 'data_5')
    src_6 = os.path.join(test_path, 'data_6')

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
    with open(src_6, 'wb') as f:
        f.write(data_6)

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

    # 6th day, test all backups
    backy.backup(src_6)
    backy.restore(restore)
    assert open(restore, 'rb').read() == data_6
    backy.restore(restore, 5)
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


def test_restore_wrong_checksum(test_path, caplog):
    backy = backy2.main.Backy(test_path, 'backup', CHUNK_SIZE_MIN)

    data = os.urandom(4*CHUNK_SIZE_MIN)   # 4 complete chunks
    src = os.path.join(test_path, 'data')
    with open(src, 'wb') as f:
        f.write(data)

    backy.backup(src)

    # test if all is good
    backy.restore(os.path.join(test_path, 'restore'))
    assert 'CRITICAL Checksum for chunk 0 does not match' not in caplog.text()

    # change something (i.e. sun flare changes some bits)
    backup_data = open(backy.data_filename(), 'rb').read()
    _ = list(backup_data)
    _[0] = (_[0] + 1) % 256
    backup_data = bytes(_)
    with open(backy.data_filename(), 'wb') as f:
        f.write(backup_data)

    # test if all is good
    backy.restore(os.path.join(test_path, 'restore'))
    assert 'CRITICAL Checksum for chunk 0 does not match' in caplog.text()


def test_scrub_wrong_checksum(test_path, caplog):
    backy = backy2.main.Backy(test_path, 'backup', CHUNK_SIZE_MIN)

    data = os.urandom(4*CHUNK_SIZE_MIN)   # 4 complete chunks
    src = os.path.join(test_path, 'data')
    with open(src, 'wb') as f:
        f.write(data)

    backy.backup(src)

    # test if all is good
    backy.scrub()
    assert 'SCRUB: Checksum for chunk' not in caplog.text()
    assert backy2.main.Level(backy.data_filename(), backy.index_filename(), CHUNK_SIZE_MIN).open().index[0].checksum != ''

    # change something in backup data (i.e. sun flare changes some bits)
    backup_data = open(backy.data_filename(), 'rb').read()
    _ = list(backup_data)
    _[0] = (_[0] + 1) % 256  # add 1 to the first byte
    backup_data = bytes(_)
    with open(backy.data_filename(), 'wb') as f:
        f.write(backup_data)

    # test if all is good
    backy.scrub()
    assert 'SCRUB: Checksum for chunk 0 does not match.' in caplog.text()
    assert backy2.main.Level(backy.data_filename(), backy.index_filename(), CHUNK_SIZE_MIN).open().index[0].checksum == ''


def test_deep_scrub_wrong_data(test_path, caplog):
    backy = backy2.main.Backy(test_path, 'backup', CHUNK_SIZE_MIN)

    data = os.urandom(4*CHUNK_SIZE_MIN)   # 4 complete chunks
    src = os.path.join(test_path, 'data')
    with open(src, 'wb') as f:
        f.write(data)

    backy.backup(src)

    # test if all is good
    backy.deep_scrub(src)
    assert 'SCRUB: Source data for chunk' not in caplog.text()
    assert backy2.main.Level(backy.data_filename(), backy.index_filename(), CHUNK_SIZE_MIN).open().index[0].checksum != ''

    # change something in source data (i.e. sun flare changes some bits)
    src_data = open(src, 'rb').read()
    _ = list(src_data)
    _[0] = (_[0] + 1) % 256  # add 1 to the first byte
    src_data = bytes(_)
    with open(src, 'wb') as f:
        f.write(src_data)

    # test if all is good
    backy.deep_scrub(src)
    assert 'SCRUB: Source data for chunk 0 does not match.' in caplog.text()
    assert backy2.main.Level(backy.data_filename(), backy.index_filename(), CHUNK_SIZE_MIN).open().index[0].checksum == ''


def test_deep_scrub_performance_percentile(test_path):
    """ This test could behave better. When system i/o is heavy, this could fail
    because we're measuring performance here..."""
    backy = backy2.main.Backy(test_path, 'backup', CHUNK_SIZE)

    src = os.path.join(test_path, 'data')
    with open(src, 'wb') as f:
        f.write(b'\0' * CHUNK_SIZE * 10)

    backy.backup(src)

    t = time.time()
    c1 = backy.deep_scrub(src)
    dt1 = time.time() - t

    t = time.time()
    c2 = backy.deep_scrub(src, percentile=50)
    dt2 = time.time() - t

    assert dt1 > dt2
    assert c1 > c2


def test_hints(test_path):
    def write_chunk(f, chunk_id, offset, length):
        f.seek(CHUNK_SIZE_MIN*chunk_id + offset)
        _from = f.tell()
        _length = f.write(os.urandom(length))
        return _from, _length

    backy = backy2.main.Backy(test_path, 'backup', CHUNK_SIZE_MIN)

    src = os.path.join(test_path, 'data')
    #open(src, 'wb').write(os.urandom(CHUNK_SIZE_MIN*8))
    open(src, 'wb').write(b'\0' * CHUNK_SIZE_MIN*8)
    backy.backup(src)

    # change a chunk and backup using hints
    with open(src, 'r+b') as f:
        _from, length = write_chunk(f, 3, 10, 30)
    backy.backup(src, hints=[(_from, length)])

    assert open(src, 'rb').read() == open(backy.data_filename(), 'rb').read()


def test_chunks_from_hints():
    hints = [(10, 100), (1024, 2048), (4096, 3000), (14000, 10), (16383, 1025)]
    #         0          1, 2          4, 5, 6       13,          15, 16
    chunk_size = 1024
    cfh = backy2.main.chunks_from_hints(hints, chunk_size)
    assert sorted(list(cfh)) == [0, 1, 2, 4, 5, 6, 13, 15, 16]
