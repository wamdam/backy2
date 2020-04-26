#!/usr/bin/env python
import logging

from collections import defaultdict
from errno import ENOENT
ENOATTR = 93
from stat import S_IFDIR, S_IFLNK, S_IFREG
import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import os


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
            st_mode=(S_IFDIR | 0o550),
            st_nlink=0,
            st_size=size,
            st_ctime=self._time(date),
            st_mtime=self._time(date),
            st_atime=self._time(date))


    def file(self, size=0, date=None):
        return dict(
            st_mode=(S_IFREG | 0o440),
            st_nlink=1,
            st_size=size,
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


    def mkdir(self, path):
        self.create(path, self.dir(), True)



class BackyFuse(LoggingMixIn, Operations):
    def __init__(self, backy):
        self.backy = backy

        self.fd = 0
        self.tree = self._tree()


    def _tree(self):
        tree = Tree()
        tree.mkdir('/by_version_uid')
        tree.mkdir('/by_name')

        for version in self.backy.meta_backend.get_versions():
            version_uid_path = os.path.join('/', 'by_version_uid', version.uid)
            tree.create(version_uid_path, tree.file(size=version.size_bytes, date=version.date))

            name_path = os.path.join('/', 'by_name', version.name)
            try:
                tree.mkdir(name_path)
            except FileExistsError:
                pass
            version_uid_path2 = os.path.join('/', 'by_name', version.name, version.uid)
            symlink_target = os.path.join('..', '..', 'by_version_uid', version.uid)
            tree.create(version_uid_path2, tree.symlink(), data=symlink_target)

        return tree


    def getattr(self, path, fh=None):
        tree = self._tree()
        try:
            return tree.get_path(path)['attrs']
        except FileNotFoundError:
            raise FuseOSError(ENOENT)


    def open(self, path, flags):
        self.fd += 1
        return self.fd


    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]


    def readdir(self, path, fh):
        p = self._tree().get_path(path)
        paths = ['.', '..'] + list(self._tree().get_path(path)['children'].keys())
        return paths


    def readlink(self, path):
        p = self._tree().get_path(path)
        return p['data']


    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)


def get_fuse(backy, mount):
    #logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(BackyFuse(backy), mount, foreground=True, allow_other=True)

