#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.logging import logger
from backy2.enterprise.readers import Reader as _Reader
import os
import queue
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


class Reader(_Reader):
    simultaneous_reads = 1

    def __init__(self, config, block_size, hash_function):
        self.simultaneous_reads = config.getint('simultaneous_reads')
        self.block_size = block_size
        self.hash_function = hash_function


    def open(self, source):
        self.source = source
        self._reader_threads = []
        self._inqueue = queue.Queue()  # infinite size for all the blocks
        self._outqueue = queue.Queue(self.simultaneous_reads)
        for i in range(self.simultaneous_reads):
            _reader_thread = threading.Thread(target=self._reader, args=(i,))
            _reader_thread.daemon = True
            _reader_thread.start()
            self._reader_threads.append(_reader_thread)


    def size(self):
        source_size = 0
        with open(self.source, 'rb') as source_file:
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
        with open(self.source, 'rb') as source_file:
            while True:
                block = self._inqueue.get()
                if block is None:
                    logger.debug("Reader {} finishing.".format(id_))
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
                    logger.debug('Reader {} re-read block (because it was invalid) {} (checksum {})'.format(id_, block.id, data_checksum))
                else:
                    logger.debug('Reader {} read block {} (len {}, checksum {}...) in {:.2f}s (seek in {:.2f}s) (Inqueue size: {}, Outqueue size: {})'.format(id_, block.id, len(data), data_checksum[:16], t3-t1, t2-t1, self._inqueue.qsize(), self._outqueue.qsize()))

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


    def close(self):
        for _reader_thread in self._reader_threads:
            self._inqueue.put(None)  # ends the threads
        for _reader_thread in self._reader_threads:
            _reader_thread.join()


