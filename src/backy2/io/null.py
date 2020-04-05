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

class IO(_IO):
    simultaneous_reads = 1
    mode = None
    _writer = None

    def __init__(self, config, block_size, hash_function):
        self.simultaneous_reads = config.getint('simultaneous_reads')
        self.block_size = block_size
        self.hash_function = hash_function


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
        self._outqueue = queue.Queue(self.simultaneous_reads)
        for i in range(self.simultaneous_reads):
            _reader_thread = threading.Thread(target=self._reader, args=(i,))
            _reader_thread.daemon = True
            _reader_thread.start()
            self._reader_threads.append(_reader_thread)


    def open_w(self, io_name, size=None, force=False):
        # parameter size is version's size.
        self.mode = 'w'
        self._size = size


    def size(self):
        return self._size


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
            #payload = (start_offset).to_bytes(16, byteorder='big') + (end_offset).to_bytes(16, byteorder='big')
            #data = (payload + b' ' * (self.block_size - len(payload)))[:block_size]

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
        pass


    def close(self):
        if self.mode == 'r':
            for _reader_thread in self._reader_threads:
                self._inqueue.put(None)  # ends the threads
            for _reader_thread in self._reader_threads:
                _reader_thread.join()
        elif self.mode == 'w':
            pass
