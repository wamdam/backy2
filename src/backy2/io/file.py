#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.logging import logger
from backy2.io import IO as _IO
import os
import queue
import re
import threading
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
    simultaneous_reads = 1
    mode = None
    _writer = None

    def __init__(self, config, block_size, hash_function):
        self.simultaneous_reads = config.getint('simultaneous_reads')
        self.block_size = block_size
        self.hash_function = hash_function


    def open_r(self, io_name):
        self.mode = 'r'
        _s = re.match('^file://(.+)$', io_name)
        if not _s:
            raise RuntimeError('Not a valid io name: {} . Need a file path, e.g. file:///somepath/file'.format(io_name))
        self.io_name = _s.groups()[0]

        self._reader_threads = []
        self._inqueue = queue.Queue()  # infinite size for all the blocks
        self._outqueue = queue.Queue(self.simultaneous_reads)
        for i in range(self.simultaneous_reads):
            _reader_thread = threading.Thread(target=self._reader, args=(i,))
            _reader_thread.daemon = True
            _reader_thread.start()
            self._reader_threads.append(_reader_thread)


    def open_w(self, io_name, size=None, force=False):
        self.mode = 'w'
        _s = re.match('^file://(.+)$', io_name)
        if not _s:
            raise RuntimeError('Not a valid io name: {} . Need a file path, e.g. file:///somepath/file'.format(io_name))
        self.io_name = _s.groups()[0]

        if os.path.exists(self.io_name):
            if not force:
                logger.error('Target already exists: {}'.format(io_name))
                exit('Error opening restore target. You must force the restore.')
            else:
                if size < self.size():
                    logger.error('Target size is too small. Has {}b, need {}b.'.format(self.size(), size))
                    exit('Error opening restore target.')
        else:
            # create the file
            with open(self.io_name, 'wb') as f:
                f.seek(size - 1)
                f.write(b'\0')


    def size(self):
        source_size = 0
        with open(self.io_name, 'rb') as source_file:
            #posix_fadvise(source_file.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
            # determine source size
            source_file.seek(0, 2)  # to the end
            source_size = source_file.tell()
            source_file.seek(0)
        return source_size


    def _reader(self, id_):
        """ self._inqueue contains Blocks.
        self._outqueue contains (block, data, data_checksum)
        """
        with open(self.io_name, 'rb') as source_file:
            while True:
                block = self._inqueue.get()
                if block is None:
                    logger.debug("IO {} finishing.".format(id_))
                    self._outqueue.put(None)  # also let the outqueue end
                    break
                offset = block.id * self.block_size
                t1 = time.time()
                source_file.seek(offset)
                t2 = time.time()
                data = source_file.read(self.block_size)
                t3 = time.time()
                # throw away cache
                posix_fadvise(source_file.fileno(), offset, offset + self.block_size, os.POSIX_FADV_DONTNEED)
                if not data:
                    raise RuntimeError('EOF reached on source when there should be data.')

                data_checksum = self.hash_function(data).hexdigest()
                if not block.valid:
                    logger.debug('IO {} re-read block (because it was invalid) {} (checksum {})'.format(id_, block.id, data_checksum))
                else:
                    logger.debug('IO {} read block {} (len {}, checksum {}...) in {:.2f}s (seek in {:.2f}s) (Inqueue size: {}, Outqueue size: {})'.format(id_, block.id, len(data), data_checksum[:16], t3-t1, t2-t1, self._inqueue.qsize(), self._outqueue.qsize()))

                self._outqueue.put((block, data, data_checksum))
                self._inqueue.task_done()


    def read(self, block, sync=False):
        """ Adds a read job """
        self._inqueue.put(block)
        if sync:
            rblock, data, data_checksum = self.get()
            if rblock.id != block.id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            return data


    def get(self):
        d = self._outqueue.get()
        self._outqueue.task_done()
        return d


    def write(self, block, data):
        # print("Writing block {} with {} bytes of data".format(block.id, len(data)))
        if not self._writer:
            self._writer = open(self.io_name, 'rb+')

        offset = block.id * self.block_size
        self._writer.seek(offset)
        written = self._writer.write(data)
        posix_fadvise(self._writer.fileno(), offset, offset + self.block_size, os.POSIX_FADV_DONTNEED)
        assert written == len(data)


    def close(self):
        if self.mode == 'r':
            for _reader_thread in self._reader_threads:
                self._inqueue.put(None)  # ends the threads
            for _reader_thread in self._reader_threads:
                _reader_thread.join()
        elif self.mode == 'w':
            self._writer.close()

