#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import contextlib
import fnmatch
import os

LF_SIZE = 4*1024*1024*1024  # 4GB
BLOCK_SIZE = 4*1024*1024  # 4MB

class LFStore:
    """ A block based blob store where each block is the same size, but not
    necessary full. It has an index to find block uids again.
    It scales over multiple storages and terabytes.
    Blocks can be stored, read and deleted.
    This store is optimized for NFS shares by using only few large files for
    blob storage.
    """
    def __init__(self, path, additional_read_paths, lf_size=LF_SIZE, block_size=BLOCK_SIZE):
        self.path = path
        self.additional_read_paths = additional_read_paths
        self.lf_size = lf_size
        self.block_size = block_size


    def _get_lf_filenames(self):
        files = []
        for filename in fnmatch.filter(os.listdir(self.path), '*.lf'):
            files.append((os.path.join(self.path, filename), 'r+b'))
        for path in self.additional_read_paths:
            for filename in fnmatch.filter(os.listdir(path), '*.lf'):
                files.append((os.path.join(self.path, filename), 'r'))
        return files


    def _new_lf(self):
        pass



    @contextlib.contextmanager
    def open(self):
        pass
