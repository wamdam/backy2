#!/usr/bin/env python
import logging

from collections import defaultdict
from errno import ENOENT; ENOATTR = 93
from functools import lru_cache
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_IFBLK
from threading import Lock
import io
import os
import re
import tempfile
import time

r_by_version_uid = r'\/by_version_uid\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/data'


def block_list(offset, length, blocksize=4*1024*1024):
    # calculates a list to read data from based on a given blocksize
    # Returns a list of (block_id, offset, length)
    block_number = offset // blocksize
    block_offset = offset % blocksize

    read_list = []
    while True:
        read_length = min(length, blocksize-block_offset)
        read_list.append((block_number, block_offset, read_length))
        block_offset = 0
        length -= read_length
        block_number += 1
        assert length >= 0
        if length == 0:
            break

    return read_list


class Tree:
    def __init__(self):
        self.tree = {'children': {'': {'attrs': self.dir(), 'children': {}}}}
        self.data = defaultdict(bytes)


    def _time(self, date=None):
        if date:
            t = time.mktime(date.timetuple()) + date.microsecond / 1E6
        else:
            t = time.time()
        return t


    def dir(self, size=0, date=None):
        return dict(
            st_mode=(S_IFDIR | 0o700),
            st_nlink=0,
            st_size=size,
            st_ctime=self._time(date),
            st_mtime=self._time(date),
            st_atime=self._time(date))


    def file(self, size=0, date=None):
        return dict(
            st_mode=(S_IFREG | 0o600),
            st_nlink=1,
            st_size=size,
            st_ctime=self._time(date),
            st_mtime=self._time(date),
            st_atime=self._time(date))


    def blk(self, size=0, date=None):
        # TODO: This creates "permission denied" when trying to access it - even as root
        return dict(
            st_mode=(S_IFBLK | 0o660),
            st_nlink=2,
            st_size=0,
            st_ctime=self._time(date),
            st_mtime=self._time(date),
            st_atime=self._time(date))


    def symlink(self, size=0, date=None):
        return dict(
            st_mode=(S_IFLNK | 0o777),
            st_nlink=1,
            st_size=size)


    def get_path(self, path):
        if path == '/':
            return self.tree['children']['']
        p = path.split('/')
        pos = self.tree
        # traverse
        for _p in p:
            if _p not in pos['children']:
                raise FileNotFoundError()
            pos = pos['children'][_p]
        return pos


    def create(self, path, attrs, has_children=False, data=None):
        name = path.split('/')[-1]
        parent_path = os.path.realpath(os.path.join(path, '..'))
        pos = self.get_path(parent_path)
        if name in pos['children']:
            raise FileExistsError()
        pos['children'][name] = {'attrs': attrs, 'data': data}
        if has_children:
            pos['children'][name]['children'] = {}


    def mkdir(self, path, date=None):
        self.create(path, self.dir(date=date), True)



class TemporaryBlockStore:
    def __init__(self, cachedir):
        self._lock = Lock()
        self.tempfile = tempfile.TemporaryFile(prefix='backy2cow', suffix='.img', dir=cachedir)
        self.db = {}  # key is block_id, value is (offset, length)
        self.offset = 0  # next write offset


    def has_block(self, block_id):
        return block_id in self.db


    def write_block(self, block_id, data):
        with self._lock:
            self.tempfile.seek(self.offset)
            self.tempfile.write(data)
            self.db[block_id] = (self.offset, len(data))
            self.offset += len(data)


    def patch_block(self, block_id, data, offset):
        tempfile_block_offset, tempfile_block_length = self.db[block_id]  # raise if it's not in there b/c that's a programming error.
        assert offset + len(data) <= tempfile_block_length  # don't write beyond block boundaries
        with self._lock:
            self.tempfile.seek(tempfile_block_offset + offset)
            self.tempfile.write(data)
        return len(data)


    def read_block(self, block_id):
        tempfile_block_offset, tempfile_block_length = self.db[block_id]  # raise if it's not in there b/c that's a programming error.
        with self._lock:
            self.tempfile.seek(tempfile_block_offset)
            return self.tempfile.read(tempfile_block_length)



class BackyFuse(LoggingMixIn, Operations):
    def __init__(self, backy, cachedir):
        self.backy = backy

        self.fd = 0
        self.tree = self._tree()
        self.fd_versions = {}  # version data per filehandle kept in RAM
        self.fd_blocks = {}  # block data per filehandle kept in RAM
        self._lock = Lock()
        self._temporary_block_store = {}
        self.cachedir = cachedir


    def _tree(self):
        tree = Tree()
        tree.mkdir('/by_version_uid')
        tree.mkdir('/by_name')

        for version in self.backy.meta_backend.get_versions():
            version_uid_path = os.path.join('/', 'by_version_uid', version.uid)
            tree.mkdir(version_uid_path, date=version.date)

            # add files to the version_uid_path:
            _data_path = os.path.join(version_uid_path, 'data')
            tree.create(_data_path, tree.file(size=version.size_bytes, date=version.date))
            _name_path = os.path.join(version_uid_path, 'name')
            tree.create(_name_path, tree.file(size=len(version.name), date=version.date), data=version.name.encode('utf-8'))
            _expire_path = os.path.join(version_uid_path, 'expire')
            _expire_data = version.expire.isoformat() if version.expire else ''
            tree.create(_expire_path, tree.file(size=len(_expire_data), date=version.date), data=_expire_data.encode('utf-8'))
            _snapshot_name_path = os.path.join(version_uid_path, 'snapshot_name')
            tree.create(_snapshot_name_path, tree.file(size=len(version.snapshot_name), date=version.date), data=version.snapshot_name.encode('utf-8'))
            if version.valid:
                _valid_path = os.path.join(version_uid_path, 'valid')
                tree.create(_valid_path, tree.file(size=5, date=version.date), data=b'valid')
            else:
                _invalid_path = os.path.join(version_uid_path, 'invalid')
                tree.create(_invalid_path, tree.file(size=7, date=version.date), data=b'invalid')
            if version.protected:
                _protected_path = os.path.join(version_uid_path, 'protected')
                tree.create(_protected_path, tree.file(size=9, date=version.date), data=b'protected')
            _tags_path = os.path.join(version_uid_path, 'tags')
            _tags_data = ",".join([t.name for t in version.tags])
            tree.create(_tags_path, tree.file(size=len(_tags_data), date=version.date), data=_tags_data)

            name_path = os.path.join('/', 'by_name', version.name)
            try:
                tree.mkdir(name_path)
            except FileExistsError:
                pass
            version_uid_path2 = os.path.join('/', 'by_name', version.name, version.uid)
            symlink_target = os.path.join('..', '..', 'by_version_uid', version.uid)
            tree.create(version_uid_path2, tree.symlink(date=version.date), data=symlink_target)

        return tree


    @lru_cache(maxsize=128)  # 128*4MB = 512MB RAM / block buffer
    def _read(self, fh, block_id):
        block = self.fd_blocks[fh].filter_by(id=block_id).one()
        if block.uid is None:  # sparse block
            return b'\x00' * self.backy.block_size
        return self.backy.data_backend.read_sync(block)


    def getattr(self, path, fh=None):
        tree = self._tree()
        try:
            return tree.get_path(path)['attrs']
        except FileNotFoundError:
            raise FuseOSError(ENOENT)


    def open(self, path, flags):
        self.fd += 1
        #print("Opened", path, self.fd)
        match = re.match(r_by_version_uid, path)
        if match:
            uid = match.group(1)
            self.fd_versions[self.fd] = self.backy.meta_backend.get_version(uid)
            self.fd_blocks[self.fd] = self.backy.meta_backend.get_blocks_by_version(uid)
        return self.fd


    def release(self, path, fh):
        #print("Released", path, fh)
        if fh in self.fd_versions:
            del(self.fd_versions[fh])


    def read(self, path, size, offset, fh):
        #print("read", path, size, offset, fh)
        if fh in self.fd_versions:
            tbs = self.get_tempoprary_block_store(path)
            _block_list = block_list(offset, size, self.backy.block_size)
            _data = b''
            for block_id, offset, length in _block_list:
                if tbs.has_block(block_id):
                    _data += tbs.read_block(block_id)[offset:offset+length]
                else:
                    with self._lock:  # Or lru_cache will be useless until the given block has arrived
                        _data += self._read(fh, block_id)[offset:offset+length]
            #assert len(_data) == size  # 'cat' reads more bytes. Seems to be normal.
            return _data
        else:
            try:
                p = self._tree().get_path(path)
            except FileNotFoundError:
                raise FuseOSError(ENOENT)
            if p['data'] is not None:
                return p['data']

        # if that's not a version and there's no data, finally throw an error.
        raise FuseOSError(ENOENT)


    def readdir(self, path, fh):
        p = self._tree().get_path(path)
        paths = ['.', '..'] + list(self._tree().get_path(path)['children'].keys())
        return paths


    def readlink(self, path):
        p = self._tree().get_path(path)
        return p['data']


    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)


    def get_tempoprary_block_store(self, path):
        if self._temporary_block_store.get(path) is None:
            self._temporary_block_store[path] = TemporaryBlockStore(self.cachedir)
        return self._temporary_block_store[path]


    # make it writable
    def write(self, path, data, offset, fh):
        if fh not in self.fd_versions:
            # writing to anywhere else except data is not supported.
            raise FuseOSError(ENOENT)

        # Copy on write of an existing version
        tbs = self.get_tempoprary_block_store(path)
        data_io = io.BytesIO(data)

        # create copy-on-write blocks in self.tempfile
        _block_list = block_list(offset, len(data), self.backy.block_size)
        for block_id, block_offset, length in _block_list:
            if not tbs.has_block(block_id):
                with self._lock:  # Or lru_cache will be useless until the given block has arrived
                    tbs.write_block(block_id, self._read(fh, block_id))
            # patch them at offset, length
            #print("patch block_id {} (global offset {}), block offset {}, length {}".format(
            #    block_id,
            #    offset,
            #    block_offset,
            #    length
            #    ))
            tbs.patch_block(block_id, data_io.read(length), block_offset)

        assert data_io.tell() == len(data)
        #print("written", path, len(data), offset, fh)
        return len(data)



def get_fuse(backy, mount, cachedir='/tmp'):
    #logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(BackyFuse(backy, cachedir), mount, foreground=True, allow_other=True)

