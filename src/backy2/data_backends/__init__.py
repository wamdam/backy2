#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import concurrent
import hashlib
import importlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore

import shortuuid
from diskcache import Cache

from backy2.exception import InternalError, ConfigurationError
from backy2.logging import logger
from backy2.utils import TokenBucket, makedirs, future_results_as_completed


class DataBackend():
    """ Holds BLOBs
    """

    # Does this data store support partial reads of blocks?
    SUPPORTS_PARTIAL_READS = False
    # Does this data store support partial reads of blocks?
    SUPPORTS_PARTIAL_WRITES = False
    # Does this data store support saving metadata?
    SUPPORTS_METADATA = False

    _COMPRESSION_HEADER = "x-backy2-comp-type"
    _ENCRYPTION_HEADER = "x-backy2-enc-type"

    PACKAGE_PREFIX = 'backy2.data_backends'
    _ENCRYPTION_PACKAGE_PREFIX = PACKAGE_PREFIX + '.encryption'
    _COMPRESSION_PACKAGE_PREFIX = PACKAGE_PREFIX + '.compression'

    def __init__(self, config):
        self.encryption = {}
        self.compression = {}
        self.encryption_active = None
        self.compression_active = None

        encryption_modules = config.get('dataBackend.{}.encryption'.format(self.NAME), None, types=list)
        if encryption_modules is not None:
            for encryption_module_dict in encryption_modules:
                name = config.get_from_dict(encryption_module_dict, 'name', types=(str,))
                materials = config.get_from_dict(encryption_module_dict, 'materials', types=dict)
                try:
                    encryption_module = importlib.import_module('{}.{}'.format(self._ENCRYPTION_PACKAGE_PREFIX, name))
                except ImportError:
                    raise ConfigurationError('Module file {}.{} not found or related import error.'
                                             .format(self._ENCRYPTION_PACKAGE_PREFIX, name))
                else:
                    if (name != encryption_module.Encryption.NAME):
                        raise InternalError('Encryption module file name and name don\'t agree ({} != {}).'
                                         .format(name, encryption_module.Encryption.NAME))

                    self.encryption[name] = encryption_module.Encryption(materials)
                    
                if config.get_from_dict(encryption_module_dict, 'active', types=bool):
                    if self.encryption_active is not None:
                        raise ConfigurationError('Only one encryption module can be active at the same time.')
                    self.encryption_active = self.encryption[name]

        compression_modules = config.get('dataBackend.{}.compression'.format(self.NAME), None, types=list)
        if compression_modules is not None:
            for compression_module_dict in compression_modules:
                name = config.get_from_dict(compression_module_dict, 'name', types=str)
                materials = config.get_from_dict(compression_module_dict, 'materials', None, types=dict)
                try:
                    compression_module = importlib.import_module('{}.{}'.format(self._COMPRESSION_PACKAGE_PREFIX, name))
                except ImportError:
                    raise ConfigurationError('Module file {}.{} not found or related import error.'
                                             .format(self._COMPRESSION_PACKAGE_PREFIX, name))
                else:
                    if (name != compression_module.Compression.NAME):
                        raise InternalError('Compression module file name and name don\'t agree ({} != {}).'
                                           .format(name, compression_module.Compression.NAME))

                    self.compression[name] = compression_module.Compression(materials)

                if config.get_from_dict(compression_module_dict, 'active', types=bool):
                    if self.compression_active is not None:
                        raise ConfigurationError('Only one compression module can be active at the same time.')
                    self.compression_active = self.compression[name]

        simultaneous_writes = config.get('dataBackend.simultaneousWrites', types=int)
        simultaneous_reads = config.get('dataBackend.simultaneousReads', types=int)
        bandwidth_read = config.get('dataBackend.bandwidthRead', types=int)
        bandwidth_write = config.get('dataBackend.bandwidthWrite', types=int)

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling

        self._read_executor = ThreadPoolExecutor(max_workers=simultaneous_reads, thread_name_prefix='IO-Reader-')
        self._read_futures = []
        self._read_semaphore = BoundedSemaphore(simultaneous_reads + self.READ_QUEUE_LENGTH)

        self._write_executor = ThreadPoolExecutor(max_workers=simultaneous_writes, thread_name_prefix='IO-Writer-')
        self._write_futures = []
        self._write_semaphore = BoundedSemaphore(simultaneous_writes + self.WRITE_QUEUE_LENGTH)

    def _write(self, uid, data):
        data, metadata = self._compress(data)
        data, metadata_2 = self._encrypt(data)
        metadata.update(metadata_2)

        time.sleep(self.write_throttling.consume(len(data)))
        t1 = time.time()
        self._write_raw(uid, data, metadata)
        t2 = time.time()

        logger.debug('Writer {} wrote data. uid {} in {:.2f}s'.format(threading.current_thread().name, uid, t2-t1))

    def _read(self, block, offset, length):
        t1 = time.time()
        data, metadata = self._read_raw(block.uid, offset, length)
        time.sleep(self.read_throttling.consume(len(data)))
        t2 = time.time()

        data = self._decrypt(data, metadata)
        data = self._uncompress(data, metadata)

        logger.debug('Reader {} read data. uid {} in {:.2f}s'.format(threading.current_thread().name, block.uid, t2-t1))
        return block, offset, len(data), data

    def _uid(self):
        # 32 chars are allowed and we need to spread the first few chars so
        # that blobs are distributed nicely. And want to avoid hash collisions.
        # So we create a real base57-encoded uuid (22 chars) and prefix it with
        # its own md5 hash[:10].
        suuid = shortuuid.uuid()
        hash = hashlib.md5(suuid.encode('ascii')).hexdigest()
        return hash[:10] + suuid

    def save(self, data, sync=False):
        for future in [future for future in self._write_futures if future.done()]:
            self._write_futures.remove(future)
            # Make sure that exceptions are delivered
            future.result()
            del future

        uid = self._uid()
        if sync:
            self._write(uid, data)
        else:
            self._write_semaphore.acquire()

            def write_with_release():
                try:
                    return self._write(uid, data)
                except Exception:
                    raise
                finally:
                    self._write_semaphore.release()

            self._write_futures.append(self._write_executor.submit(write_with_release))

        return uid

    def read(self, block, offset=0, length=None, sync=False):
        if sync:
            return self._read(block, offset, length)[3]
        else:
            def read_with_acquire():
                self._read_semaphore.acquire()
                return self._read(block, offset, length)

            # This callback would also be called if the job was cancel which we currently don't do and it would be
            # wrong to release the semaphore in that case.
            def release_callback(future):
                self._read_semaphore.release()

            future = self._read_executor.submit(read_with_acquire)
            future.add_done_callback(release_callback)
            self._read_futures.append(future)

    def read_get_completed(self, timeout=None):
        """ Returns a generator for all completed read jobs
        """
        return future_results_as_completed(self._read_futures)

    def update(self, block, data, offset=0):
        """ Updates data, returns written bytes.
            This is only available on *some* data backends.
        """
        raise NotImplementedError()

    def rm(self, uid):
        """ Deletes a block """
        raise NotImplementedError()

    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """
        raise NotImplementedError()

    def get_all_blob_uids(self, prefix=None):
        """ Get all existing blob uids """
        raise NotImplementedError()

    def _encrypt(self, data):
        if self.encryption_active is not None:
            data, metadata = self.encryption_active.encrypt(data)
            metadata[self._ENCRYPTION_HEADER] = self.encryption_active.NAME
            return data, metadata
        else:
            return data, {}

    def _decrypt(self, data, metadata):
        if self._ENCRYPTION_HEADER in metadata:
            name = metadata[self._ENCRYPTION_HEADER]
            if name in self.encryption:
                return self.encryption[name].decrypt(data, metadata)
            else:
                raise IOError('Unsupported encryption type {} in object metadata.'.format(name))
        else:
            return data

    def _compress(self, data):
        if self.compression_active is not None:
            compressed_data, metadata = self.compression_active.compress(data)
            if len(compressed_data) < len(data):
                metadata[self._COMPRESSION_HEADER] = self.compression_active.NAME
                return compressed_data, metadata
            else:
                return data, {}
        else:
            return data, {}

    def _uncompress(self, data, metadata):
        if self._COMPRESSION_HEADER in metadata:
            name = metadata[self._COMPRESSION_HEADER]
            if name in self.compression:
                return self.compression[name].uncompress(data, metadata)
            else:
                raise IOError('Unsupported compression type {} in object metadata.'.format(name))
        else:
            return data

    def wait_read_finished(self):
        for future in concurrent.futures.as_completed(self._read_futures):
            self._read_futures.remove(future)
            # Make sure exceptions are delivered
            future.result()
            del future

    def wait_write_finished(self):
        for future in concurrent.futures.as_completed(self._write_futures):
            self._write_futures.remove(future)
            # Make sure exceptions are delivered
            future.result()
            del future

    def close(self):
        self._write_executor.shutdown()
        self._read_executor.shutdown()


class ROSDataBackend(DataBackend):

    def __init__(self, config):
        read_cache_directory = config.get('dataBackend.readCacheDirectory', None, types=str)
        read_cache_maximum_size = config.get('dataBackend.readCacheMaximumSize', None, types=int)
    
        if read_cache_directory and not read_cache_maximum_size or not read_cache_directory and read_cache_maximum_size:
            raise ConfigurationError('Both dataBackend.cacheDirectory and dataBackend.cacheMaximumSize need to be set '
                                  + 'to enable disk based caching.')
    
        if read_cache_directory and read_cache_maximum_size:
            makedirs(read_cache_directory)
            try:
                self._read_cache = Cache(read_cache_directory,
                                    size_limit=read_cache_maximum_size,
                                    eviction_policy='least-frequently-used',
                                    statistics=1,
                                    )
            except Exception:
                logger.warn('Unable to enable disk based read caching. Continuing without it.')
                self._read_cache = None
            else:
                logger.debug('Disk based read caching enabled (cache size {})'.format(read_cache_maximum_size))
        else:
            self._read_cache = None

        # Start reader and write threads after the disk cached is created, so that they see it.
        super().__init__(config)

    def _read(self, block, offset, length):
        if offset != 0 or length is not None:
            raise InternalError('Remote object based storage called invalid offset or length '
                                + '(offset {} != 0, length {} != None)')

        if self._read_cache is not None:
            data = self._read_cache.get(block.uid)
            if data:
                return block, offset, len(data), data

        block, offset, length, data = super()._read(block, offset, length)

        if self._read_cache is not None:
            self._read_cache.set(block.uid, data)

        return block, offset, length, data
        
    def close(self):
        super().close()
        if self._read_cache is not None:
            (cache_hits, cache_misses) = self._read_cache.stats()
            logger.info('Disk based cache statistics (since cache creation): {} hits, {} misses'.format(cache_hits, cache_misses))
            self._read_cache.close()
        