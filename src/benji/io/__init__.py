#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore

from benji.logging import logger
from benji.utils import future_results_as_completed


class IO:

    PACKAGE_PREFIX = 'benji.io'

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
        return future_results_as_completed(self._read_futures, semaphore=self._read_semaphore, timeout=timeout)

    def write(self, block, data):
        """ Writes data to the given block
        """
        raise NotImplementedError()

    def close(self):
        """ Close the io
        """
        if self._read_executor:
            if len(self._read_futures) > 0:
                logger.warning('IO backend closed with {} outstanding read jobs, cancelling them.'.format(
                    len(self._read_futures)))
                for future in self._read_futures:
                    future.cancel()
                logger.debug('IO backend cancelled all outstanding read jobs.')
                # Get all jobs so that the semaphore gets released and still waiting jobs can complete
                for future in self.read_get_completed():
                    pass
                logger.debug('IO backend read results from all outstanding read jobs.')
            self._read_executor.shutdown()
