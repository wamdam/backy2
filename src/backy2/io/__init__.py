#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore

from backy2.utils import future_results_as_completed


class IO:

    PACKAGE_PREFIX = 'backy2.io'

    READ_QUEUE_LENGTH = 5

    def __init__(self, config, block_size, hash_function):
        self._block_size = block_size
        self._hash_function = hash_function

        our_config = config.get('io.{}'.format(self.NAME), types=dict)
        self.simultaneous_reads = config.get_from_dict(our_config, 'simultaneousReads', types=int)

        self._read_executor = None

    def open_r(self, io_name):
        self._read_executor = ThreadPoolExecutor(max_workers=self.simultaneous_reads, thread_name_prefix='IO-Reader')
        self._read_futures = []
        self._read_semaphore = BoundedSemaphore(self.simultaneous_reads + self.READ_QUEUE_LENGTH)

    def open_w(self, io_name, size=None, force=False):
        raise NotImplementedError()

    def size(self):
        """ Return the size in bytes of the opened io_name
        """
        raise NotImplementedError()

    def _read(self, block):
        raise NotImplementedError()

    def read(self, block, sync=False):
        """ Adds a read job or directly reads and returns the data """
        if sync:
            return self._read(block)[1]
        else:
            def read_with_acquire():
                self._read_semaphore.acquire()
                return self._read(block)

            self._read_futures.append(self._read_executor.submit(read_with_acquire))

    def read_get_completed(self, timeout=None):
        """ Returns a generator for all completed read jobs
        """
        return future_results_as_completed(self._read_futures, self._read_semaphore, timeout=timeout)

    def write(self, block, data):
        """ Writes data to the given block
        """
        raise NotImplementedError()

    def close(self):
        """ Close the io
        """
        if self._read_executor:
            self._read_executor.shutdown()
