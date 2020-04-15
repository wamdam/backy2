#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.logging import logger
from backy2.io import IO as _IO
from collections import namedtuple
import os
import queue
import re
import threading
import time

STATUS_NOTHING = 0
STATUS_READING = 1
STATUS_WRITING = 2
STATUS_SEEKING = 3
STATUS_FADVISE = 4

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
    mode = None
    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    def __init__(self, config, block_size, hash_function):
        self.simultaneous_reads = config.getint('simultaneous_reads', 1)
        self.simultaneous_writes = config.getint('simultaneous_reads', 1)
        self.block_size = block_size
        self.hash_function = hash_function

        self._reader_threads = []
        self._writer_threads = []

        self.reader_thread_status = {}
        self.writer_thread_status = {}

        self._inqueue = queue.Queue()  # infinite size for all the blocks
        self._outqueue = queue.Queue(self.simultaneous_reads + self.READ_QUEUE_LENGTH)  # data of read blocks
        self._write_queue = queue.Queue(self.simultaneous_writes + self.WRITE_QUEUE_LENGTH)  # blocks to be written


    def open_r(self, io_name):
        self.mode = 'r'
        _s = re.match('^file://(.+)$', io_name)
        if not _s:
            raise RuntimeError('Not a valid io name: {} . Need a file path, e.g. file:///somepath/file'.format(io_name))
        self.io_name = _s.groups()[0]

        for i in range(self.simultaneous_reads):
            _reader_thread = threading.Thread(target=self._reader, args=(i,))
            _reader_thread.daemon = True
            _reader_thread.start()
            self._reader_threads.append(_reader_thread)
            self.reader_thread_status[i] = STATUS_NOTHING


    def open_w(self, io_name, size=None, force=False):
        # parameter size is version's size.
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
                if self.size() < size:
                    logger.error('Target size is too small. Has {}b, need {}b.'.format(self.size(), size))
                    exit('Error opening restore target.')
        else:
            # create the file
            with open(self.io_name, 'wb') as f:
                f.seek(size - 1)
                f.write(b'\0')

        for i in range(self.simultaneous_writes):
            _writer_thread = threading.Thread(target=self._writer, args=(i,))
            _writer_thread.daemon = True
            _writer_thread.start()
            self._writer_threads.append(_writer_thread)
            self.writer_thread_status[i] = STATUS_NOTHING


    def size(self):
        source_size = 0
        with open(self.io_name, 'rb') as source_file:
            #posix_fadvise(source_file.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
            # determine source size
            source_file.seek(0, 2)  # to the end
            source_size = source_file.tell()
            source_file.seek(0)
        return source_size


    def _writer(self, id_):
        """ self._write_queue contains a list of (Block, data) to be written.
        """
        with open(self.io_name, 'rb+') as _write_file:
            while True:
                entry = self._write_queue.get()
                if entry is None:
                    logger.debug("IO writer {} finishing.".format(id_))
                    self._write_queue.task_done()
                    break
                block, data, callback = entry

                offset = block.id * self.block_size

                self.writer_thread_status[id_] = STATUS_SEEKING
                _write_file.seek(offset)
                self.writer_thread_status[id_] = STATUS_WRITING
                written = _write_file.write(data)
                posix_fadvise(_write_file.fileno(), offset, offset + written, os.POSIX_FADV_DONTNEED)
                self.writer_thread_status[id_] = STATUS_NOTHING
                assert written == len(data)
                if callback:
                    callback()

                self._write_queue.task_done()


    def _reader(self, id_):
        """ self._inqueue contains block_ids to be read.
        self._outqueue contains (block_id, data, data_checksum)
        """
        with open(self.io_name, 'rb') as source_file:
            while True:
                entry = self._inqueue.get()
                if entry is None:
                    logger.debug("IO {} finishing.".format(id_))
                    self._outqueue.put(None)  # also let the outqueue end
                    self._inqueue.task_done()
                    break
                block_id, read, metadata = entry
                if not read:
                    self._outqueue.put((block_id, None, None, metadata))
                else:
                    offset = block_id * self.block_size
                    t1 = time.time()
                    self.reader_thread_status[id_] = STATUS_SEEKING
                    source_file.seek(offset)
                    t2 = time.time()
                    self.reader_thread_status[id_] = STATUS_READING
                    data = source_file.read(self.block_size)
                    t3 = time.time()
                    # throw away cache
                    self.reader_thread_status[id_] = STATUS_FADVISE
                    posix_fadvise(source_file.fileno(), offset, offset + self.block_size, os.POSIX_FADV_DONTNEED)
                    self.reader_thread_status[id_] = STATUS_NOTHING
                    if not data:
                        raise RuntimeError('EOF reached on source when there should be data.')

                    data_checksum = self.hash_function(data).hexdigest()

                    self._outqueue.put((block_id, data, data_checksum, metadata))
                self._inqueue.task_done()


    def read(self, block_id, sync=False, read=True, metadata=None):
        """ Adds a read job, passes through metadata.
        read False means the real data will not be read."""
        self._inqueue.put((block_id, read, metadata))
        if sync:
            rblock_id, data, data_checksum, metadata = self.get()
            if rblock_id != block_id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            return data


    def get(self):
        d = self._outqueue.get()
        self._outqueue.task_done()
        return d


    def write(self, block, data, callback=None):
        """ Adds a write job"""
        self._write_queue.put((block, data, callback))


    def queue_status(self):
        return {
            'rq_filled': self._outqueue.qsize() / self._outqueue.maxsize,  # 0..1
            'wq_filled': self._write_queue.qsize() / self._write_queue.maxsize,
        }


    def thread_status(self):
        return "IOR: N{} R{} S{} F{} IQ{} OQ{}  IOW: N{} W{} S{} F{} QL{}".format(
                len([t for t in self.reader_thread_status.values() if t==STATUS_NOTHING]),
                len([t for t in self.reader_thread_status.values() if t==STATUS_READING]),
                len([t for t in self.reader_thread_status.values() if t==STATUS_SEEKING]),
                len([t for t in self.reader_thread_status.values() if t==STATUS_FADVISE]),
                self._inqueue.qsize(),
                self._outqueue.qsize(),
                len([t for t in self.writer_thread_status.values() if t==STATUS_NOTHING]),
                len([t for t in self.writer_thread_status.values() if t==STATUS_WRITING]),
                len([t for t in self.writer_thread_status.values() if t==STATUS_SEEKING]),
                len([t for t in self.writer_thread_status.values() if t==STATUS_FADVISE]),
                self._write_queue.qsize(),
                )


    def close(self):
        if self.mode == 'r':
            for _reader_thread in self._reader_threads:
                self._inqueue.put(None)  # ends the threads
            for _reader_thread in self._reader_threads:
                _reader_thread.join()
        elif self.mode == 'w':
            t1 = time.time()
            for _writer_thread in self._writer_threads:
                self._write_queue.put(None)  # ends the threads
            for _writer_thread in self._writer_threads:
                _writer_thread.join()
            t2 = time.time()

