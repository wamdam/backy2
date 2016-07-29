#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.logging import logger
from backy2.io import IO as _IO
import os
import time

if hasattr(os, 'posix_fadvise'):
    posix_fadvise = os.posix_fadvise
else:  # pragma: no cover
    logger.warn('Running without `posix_fadvise`.')
    os.POSIX_FADV_RANDOM = None
    os.POSIX_FADV_SEQUENTIAL = None
    os.POSIX_FADV_WILLNEED = None
    os.POSIX_FADV_DONTNEED = None

    def posix_fadvise(*args, **kw):
        return


class IO(_IO):

    def __init__(self, config, block_size, hash_function):
        self.block_size = block_size
        self.hash_function = hash_function


    def open(self, source):
        self.source = source
        self._read_list = []
        self.source_file = open(self.source, 'rb')


    def size(self):
        source_size = 0
        with open(self.source, 'rb') as source_file:
            #posix_fadvise(source_file.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
            # determine source size
            source_file.seek(0, 2)  # to the end
            source_size = source_file.tell()
            source_file.seek(0)
        return source_size


    def read(self, block, sync=False):
        """ Adds a read job """
        self._read_list.append(block)
        if sync:
            rblock, data, data_checksum = self.get()
            if rblock.id != block.id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            return data


    def get(self):
        try:
            block = self._read_list.pop()
        except IndexError:
            raise  # pop from an empty list

        offset = block.id * self.block_size
        t1 = time.time()
        self.source_file.seek(offset)
        t2 = time.time()
        data = self.source_file.read(self.block_size)
        t3 = time.time()
        # throw away cache
        posix_fadvise(self.source_file.fileno(), offset, offset + self.block_size, os.POSIX_FADV_DONTNEED)
        if not data:
            raise RuntimeError('EOF reached on source when there should be data.')

        data_checksum = self.hash_function(data).hexdigest()
        if not block.valid:
            logger.debug('Re-read block (because it was invalid) {} (checksum {})'.format(block.id, data_checksum))
        else:
            logger.debug('Read block {} (len {}, checksum {}...) in {:.2f}s (seek in {:.2f}s)'.format(block.id, len(data), data_checksum[:16], t3-t1, t2-t1))

        return block, data, data_checksum


    def close(self):
        self.source_file.close()


