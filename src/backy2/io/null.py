#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.logging import logger
from backy2.io import IO as _IO
from backy2.utils import generate_block
import os
import queue
import re
import threading
import time

"""
THIS IMPLEMENTATION IS FOR TESTING ONLY
DO NOT USE IN PRODUCTION
This is a sink to /dev/null for writing restores.
Use by restoring to null://
"""

STATUS_NOTHING = 0
STATUS_WRITING = 2

class IO(_IO):
    mode = None
    _writer = None
    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    def __init__(self, config, block_size, hash_function):
        self.simultaneous_reads = config.getint('simultaneous_reads', 1)
        self.simultaneous_writes = config.getint('simultaneous_writes', 1)
        self.block_size = block_size
        self.hash_function = hash_function

        self._reader_threads = []
        self._writer_threads = []

        self.reader_thread_status = {}
        self.writer_thread_status = {}


    def open_r(self, size_str):
        self.mode = 'r'
        _s = re.match('^null://([0-9]+[kMGTP]?)B?$', size_str)
        if not _s:
            raise RuntimeError('Not a valid io name: {} . Need a virtual file size, e.g. random://200TB or random://10GB'.format(size_str))
        size_str = _s.groups()[0]
        if size_str.endswith('k'):
            size = int(size_str[:-1]) * 1024
        elif size_str.endswith('M'):
            size = int(size_str[:-1]) * 1024 * 1024
        elif size_str.endswith('G'):
            size = int(size_str[:-1]) * 1024 * 1024 * 1024
        elif size_str.endswith('T'):
            size = int(size_str[:-1]) * 1024 * 1024 * 1024 * 1024
        elif size_str.endswith('P'):
            size = int(size_str[:-1]) * 1024 * 1024 * 1024 * 1024 * 1024
        else:
            size = int(size_str)

        self._size = size

        self._reader_threads = []
        self._inqueue = queue.Queue()  # infinite size for all the blocks
        self._outqueue = queue.Queue(self.simultaneous_reads + self.READ_QUEUE_LENGTH)  # data of read blocks
        for i in range(self.simultaneous_reads):
            _reader_thread = threading.Thread(target=self._reader, args=(i,))
            _reader_thread.daemon = True
            _reader_thread.start()
            self._reader_threads.append(_reader_thread)
            self.reader_thread_status[i] = STATUS_NOTHING


    def open_w(self, io_name, size=None, force=False):
        # parameter size is version's size.
        self.mode = 'w'
        self._size = size

        self._write_queue = queue.Queue(self.simultaneous_writes + self.WRITE_QUEUE_LENGTH)  # blocks to be written
        for i in range(self.simultaneous_writes):
            _writer_thread = threading.Thread(target=self._writer, args=(i,))
            _writer_thread.daemon = True
            _writer_thread.start()
            self._writer_threads.append(_writer_thread)
            self.writer_thread_status[i] = STATUS_NOTHING


    def size(self):
        return self._size


    def _writer(self, id_):
        """ self._write_queue contains a list of (Block, data) to be written.
        """
        while True:
            entry = self._write_queue.get()
            if entry is None:
                logger.debug("IO writer {} finishing.".format(id_))
                break
            block, data = entry

            self.writer_thread_status[id_] = STATUS_WRITING
            # write nothing
            time.sleep(.1)
            self.writer_thread_status[id_] = STATUS_NOTHING

            self._write_queue.task_done()


    def _reader(self, id_):
        """ self._inqueue contains Blocks.
        self._outqueue contains (block, data, data_checksum)
        """
        while True:
            block = self._inqueue.get()
            if block is None:
                logger.debug("IO {} finishing.".format(id_))
                self._outqueue.put(None)  # also let the outqueue end
                break

            start_offset = block.id * self.block_size
            end_offset = min(block.id * self.block_size + self.block_size, self._size)
            block_size = end_offset - start_offset
            data = generate_block(block.id, block_size)

            if not data:
                raise RuntimeError('EOF reached on source when there should be data.')

            data_checksum = self.hash_function(data).hexdigest()
            if not block.valid:
                logger.debug('IO {} re-read block (because it was invalid) {} (checksum {})'.format(id_, block.id, data_checksum))
            else:
                logger.debug('IO {} read block {} (len {}, checksum {}...) (Inqueue size: {}, Outqueue size: {})'.format(id_, block.id, len(data), data_checksum[:16], self._inqueue.qsize(), self._outqueue.qsize()))

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
        self._write_queue.put((block, data))


    def thread_status(self):
        return "IO Writer Threads: N:{} W:{} Queue-Length:{}".format(
                len([t for t in self.writer_thread_status.values() if t==STATUS_NOTHING]),
                len([t for t in self.writer_thread_status.values() if t==STATUS_WRITING]),
                self._write_queue.qsize(),
                )


    def close(self):
        if self.mode == 'r':
            for _reader_thread in self._reader_threads:
                self._inqueue.put(None)  # ends the threads
            for _reader_thread in self._reader_threads:
                _reader_thread.join()
        elif self.mode == 'w':
            for _writer_thread in self._writer_threads:
                self._write_queue.put(None)  # ends the threads
            for _writer_thread in self._writer_threads:
                _writer_thread.join()
