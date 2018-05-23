#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import concurrent
import hashlib
import importlib
import json
import os
import textwrap
import threading
import time
from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore

from diskcache import Cache
from prettytable import PrettyTable

from backy2.exception import InternalError, ConfigurationError
from backy2.logging import logger
from backy2.meta_backend import BlockUid, VersionUid
from backy2.utils import TokenBucket, future_results_as_completed


class DataBackend(metaclass=ABCMeta):

    _COMPRESSION_KEY = 'compression'
    _ENCRYPTION_KEY = 'encryption'
    _SIZE_KEY = 'size'
    _OBJECT_SIZE_KEY = 'object_size'
    _CHECKSUM_KEY = 'checksum'

    PACKAGE_PREFIX = 'backy2.data_backends'
    _ENCRYPTION_PACKAGE_PREFIX = PACKAGE_PREFIX + '.encryption'
    _COMPRESSION_PACKAGE_PREFIX = PACKAGE_PREFIX + '.compression'

    # For the benefit of the file and B2 backends these must end in a slash
    _BLOCKS_PREFIX = 'blocks/'
    _VERSIONS_PREFIX = 'versions/'

    _META_SUFFIX = '.meta'

    def __init__(self, config):
        self.encryption = {}
        self.compression = {}
        self.encryption_active = None
        self.compression_active = None

        encryption_modules = config.get('dataBackend.encryption'.format(self.NAME), None, types=list)
        if encryption_modules is not None:
            for encryption_module_dict in encryption_modules:
                type = config.get_from_dict(encryption_module_dict, 'type', types=str)
                identifier = config.get_from_dict(encryption_module_dict, 'identifier', types=str)
                materials = config.get_from_dict(encryption_module_dict, 'materials', types=dict)
                try:
                    encryption_module = importlib.import_module('{}.{}'.format(self._ENCRYPTION_PACKAGE_PREFIX, type))
                except ImportError:
                    raise ConfigurationError('Module file {}.{} not found or related import error.'
                                             .format(self._ENCRYPTION_PACKAGE_PREFIX, type))
                else:
                    if type != encryption_module.Encryption.NAME:
                        raise InternalError('Encryption module type and name don\'t agree ({} != {}).'
                                         .format(type, encryption_module.Encryption.NAME))

                    self.encryption[identifier] = encryption_module.Encryption(identifier=identifier,
                                                                               materials=materials)

                if config.get_from_dict(encryption_module_dict, 'active', None, types=bool):
                    if self.encryption_active is not None:
                        raise ConfigurationError('Only one encryption module can be active at the same time.')
                    logger.info('Encryption is enabled for the data backend.')
                    self.encryption_active = self.encryption[identifier]

        compression_modules = config.get('dataBackend.compression'.format(self.NAME), None, types=list)
        if compression_modules is not None:
            for compression_module_dict in compression_modules:
                type = config.get_from_dict(compression_module_dict, 'type', types=str)
                materials = config.get_from_dict(compression_module_dict, 'materials', None, types=dict)
                try:
                    compression_module = importlib.import_module('{}.{}'.format(self._COMPRESSION_PACKAGE_PREFIX, type))
                except ImportError:
                    raise ConfigurationError('Module file {}.{} not found or related import error.'
                                             .format(self._COMPRESSION_PACKAGE_PREFIX, type))
                else:
                    if type != compression_module.Compression.NAME:
                        raise InternalError('Compression module type and name don\'t agree ({} != {}).'
                                           .format(type, compression_module.Compression.NAME))

                    self.compression[type] = compression_module.Compression(materials=materials)

                if config.get_from_dict(compression_module_dict, 'active', None, types=bool):
                    if self.compression_active is not None:
                        raise ConfigurationError('Only one compression module can be active at the same time.')
                    self.compression_active = self.compression[type]

        simultaneous_writes = config.get('dataBackend.simultaneousWrites', types=int)
        simultaneous_reads = config.get('dataBackend.simultaneousReads', types=int)
        bandwidth_read = config.get('dataBackend.bandwidthRead', types=int)
        bandwidth_write = config.get('dataBackend.bandwidthWrite', types=int)

        self._consistency_check_writes = config.get('dataBackend.consistencyCheckWrites'.format(self.NAME), False, types=bool)

        self._compression_statistics = {
            'objects_considered': 0,
            'objects_compressed': 0,
            'data_in': 0,
            'data_out': 0,
            'data_in_compression': 0,
            'data_out_compression': 0
        }

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling

        self._read_executor = ThreadPoolExecutor(max_workers=simultaneous_reads, thread_name_prefix='DataBackend-Reader')
        self._read_futures = []
        self._read_semaphore = BoundedSemaphore(simultaneous_reads + self.READ_QUEUE_LENGTH)

        self._write_executor = ThreadPoolExecutor(max_workers=simultaneous_writes, thread_name_prefix='DataBackend-Writer')
        self._write_futures = []
        self._write_semaphore = BoundedSemaphore(simultaneous_writes + self.WRITE_QUEUE_LENGTH)

    def _check_write(self, key, metadata_key, data, metadata):
        # Source: https://stackoverflow.com/questions/4527942/comparing-two-dictionaries-in-python
        def dict_compare(d1, d2):
            d1_keys = set(d1.keys())
            d2_keys = set(d2.keys())
            intersect_keys = d1_keys.intersection(d2_keys)
            added = d1_keys - d2_keys
            removed = d2_keys - d1_keys
            modified = {o : (d1[o], d2[o]) for o in intersect_keys if d1[o] != d2[o]}
            same = set(o for o in intersect_keys if d1[o] == d2[o])
            return added, removed, modified, same

        rdata = self._read_object(key)
        rmetadata = self._read_object(metadata_key)
        rmetadata = json.loads(rmetadata.decode('utf-8'))

        if metadata:
            added, removed, modified, same = dict_compare(rmetadata, metadata)
            logger.debug('Comparing written and read metadata of {}:'.format(key))
            logger.debug('  added: {}, removed: {}, modified: {}, same: {}'.format(added, removed, modified, same))
            if removed:
                raise InternalError('Consistency check: Metadata headers are missing in read back data: {}'
                                    .format(', '.join(removed)))
            different_for = []
            for name in modified:
                logger.debug('Metadata differences: ')
                logger.debug('  {}: wrote {}, read {}'.format(name, metadata[name], rmetadata[name]))
                if metadata[name] != rmetadata[name]:
                    different_for.append(name)
            if different_for:
                raise InternalError('Consistency check: Written and read metadata of {} are different for {}.'
                                    .format(', '.join(different_for)))
        # Comparing encrypted/compressed data here
        if data != rdata:
            raise InternalError('Consistency check: Written and read data of {} differ.'.format(key))

    def _write(self, block, data):
        data, metadata = self._compress(data)
        data, metadata_2 = self._encrypt(data)
        metadata.update(metadata_2)

        metadata[self._SIZE_KEY] = block.size
        metadata[self._OBJECT_SIZE_KEY] = len(data)
        metadata[self._CHECKSUM_KEY] = block.checksum
        metadata_json = json.dumps(metadata, separators=(',', ':')).encode('utf-8')

        logger.debug('Metadata of block {}: {}'.format(block.uid, metadata))

        key = self._block_uid_to_key(block.uid)
        metadata_key = key + self._META_SUFFIX

        time.sleep(self.write_throttling.consume(len(data) + len(metadata_json)))
        t1 = time.time()
        try:
            self._write_object(key, data)
            self._write_object(metadata_key, metadata_json)
        except:
            try:
                self._rm_object(key)
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass
            raise
        t2 = time.time()

        logger.debug('{} wrote data of uid {} in {:.2f}s'.format(threading.current_thread().name, block.uid, t2-t1))
        if self._consistency_check_writes:
            self._check_write(key, metadata_key, data, metadata)

        return block

    def save(self, block, data, sync=False):
        if sync:
            self._write(block, data)
        else:
            self._write_semaphore.acquire()

            def write_with_release():
                try:
                    return self._write(block, data)
                except Exception:
                    raise
                finally:
                    self._write_semaphore.release()

            self._write_futures.append(self._write_executor.submit(write_with_release))


    def save_get_completed(self, timeout=None):
        """ Returns a generator for all completed read jobs
        """
        return future_results_as_completed(self._write_futures, timeout=timeout)

    def _read(self, block):
        key = self._block_uid_to_key(block.uid)
        metadata_key = key + self._META_SUFFIX
        t1 = time.time()
        data = self._read_object(key)
        metadata = self._read_object(metadata_key)
        time.sleep(self.read_throttling.consume(len(data) + len(metadata_key)))
        t2 = time.time()

        metadata = json.loads(metadata.decode('utf-8'))
        for required_key in [self._OBJECT_SIZE_KEY, self._SIZE_KEY]:
            if required_key not in metadata:
                raise KeyError('Required metadata key {} is missing for object {}.'.format(required_key, key))

        if len(data) != metadata[self._OBJECT_SIZE_KEY]:
            raise ValueError('Length mismatch for object {}. Expected: {}, got: {}.'
                                .format(key, metadata[self.self._OBJECT_SIZE_KEY], len(data)))

        data = self._decrypt(data, metadata)
        data = self._uncompress(data, metadata)

        if len(data) != metadata[self._SIZE_KEY]:
            raise ValueError('Length mismatch of original data for object {}. Expected: {}, got: {}.'
                             .format(key, metadata[self.self._SIZE_KEY], len(data)))

        logger.debug('{} read data of uid {} in {:.2f}s'.format(threading.current_thread().name, block.uid, t2-t1))
        return block, data

    def read(self, block, sync=False):
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

    def rm(self, uid):
        key = self._block_uid_to_key(uid)
        metadata_key = key + self._META_SUFFIX
        try:
            self._rm_object(key)
        finally:
            try:
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass

    def rm_many(self, uids):
        keys =  [self._block_uid_to_key(uid) for uid in uids]
        metadata_keys = [key + self._META_SUFFIX for key in keys]

        errors = self._rm_many_objects(keys)
        self._rm_many_objects(metadata_keys)
        return [self._key_to_block_uid(error) for error in errors]

    def list_blocks(self):
        keys = self._list_objects(self._BLOCKS_PREFIX)
        block_uids = []
        for key in keys:
            if key.endswith(self._META_SUFFIX):
                continue
            try:
                block_uids.append(self._key_to_block_uid(key))
            except (RuntimeError, ValueError):
                # Ignore any keys which don't match our pattern to account for stray objects/files
                pass
        return block_uids

    def list_versions(self):
        keys = self._list_objects(self._VERSIONS_PREFIX)
        version_uids = []
        for key in keys:
            if key.endswith(self._META_SUFFIX):
                continue
            try:
                version_uids.append(self._key_to_version_uid(key))
            except (RuntimeError, ValueError):
                # Ignore any keys which don't match our pattern to account for stray objects/files
                pass
        return version_uids

    def read_version(self, version_uid):
        key = self._version_uid_to_key(version_uid)
        metadata_key = key + self._META_SUFFIX
        data = self._read_object(key)
        metadata = self._read_object(metadata_key)

        metadata = json.loads(metadata.decode('utf-8'))
        for required_key in [self._OBJECT_SIZE_KEY, self._SIZE_KEY]:
            if required_key not in metadata:
                raise KeyError('Required metadata key {} is missing for object {}.'.format(required_key, key))

        if len(data) != metadata[self._OBJECT_SIZE_KEY]:
            raise ValueError('Length mismatch for object {}. Expected: {}, got: {}.'
                                 .format(key, metadata[self.self._OBJECT_SIZE_KEY], len(data)))

        data = self._decrypt(data, metadata)
        data = self._uncompress(data, metadata)

        if len(data) != metadata[self._SIZE_KEY]:
            raise ValueError('Length mismatch of original data for object {}. Expected: {}, got: {}.'
                             .format(key, metadata[self.self._SIZE_KEY], len(data)))

        data = data.decode('utf-8')
        return data

    def save_version(self, version_uid, data, overwrite=False):
        key = self._version_uid_to_key(version_uid)
        metadata_key = key + self._META_SUFFIX

        if not overwrite:
            try:
                self._read_object(key)
            except FileNotFoundError:
                pass
            else:
                raise FileExistsError('Version {} already exists in data backend.'.format(version_uid.readable))

        data = data.encode('utf-8')
        size = len(data)
        data, metadata = self._compress(data)
        data, metadata_2 = self._encrypt(data)
        metadata.update(metadata_2)

        metadata[self._SIZE_KEY] = size
        metadata[self._OBJECT_SIZE_KEY] = len(data)
        metadata_json = json.dumps(metadata, separators=(',', ':')).encode('utf-8')

        try:
            self._write_object(key, data)
            self._write_object(metadata_key, metadata_json)
        except:
            try:
                self._rm_object(key)
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass
            raise

        if self._consistency_check_writes:
            self._check_write(key, metadata_key, data, metadata)

    def rm_version(self, version_uid):
        key = self._version_uid_to_key(version_uid)
        metadata_key = key + self._META_SUFFIX
        try:
            self._rm_object(key)
        finally:
            try:
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass

    def _encrypt(self, data):
        if self.encryption_active is not None:
            data, materials = self.encryption_active.encrypt(data=data)
            metadata = {
                self._ENCRYPTION_KEY: {
                    'identifier': self.encryption_active.identifier,
                    'type': self.encryption_active.NAME,
                    'materials': materials
                }
            }
            return data, metadata
        else:
            return data, {}

    def _decrypt(self, data, metadata):
        if self._ENCRYPTION_KEY in metadata:
            identifier = metadata[self._ENCRYPTION_KEY]['identifier']
            type = metadata[self._ENCRYPTION_KEY]['type']
            if identifier in self.encryption:
                encryption = self.encryption[identifier]
                if type != encryption.NAME:
                    raise ConfigurationError('Mismatch between object encryption type and configured type for identifier '
                                             + '{} ({} != {})'.format(identifier, type, encryption.NAME))

                return encryption.decrypt(data=data, materials=metadata[self._ENCRYPTION_KEY]['materials'])
            else:
                raise IOError('Unknown encryption identifier {} in object metadata.'.format(identifier))
        else:
            return data

    def _compress(self, data):
        self._compression_statistics['objects_considered'] += 1
        self._compression_statistics['data_in'] += len(data)

        if self.compression_active is not None:
            compressed_data, materials = self.compression_active.compress(data=data)
            if len(compressed_data) < len(data):
                self._compression_statistics['objects_compressed'] += 1
                self._compression_statistics['data_in_compression'] += len(data)
                self._compression_statistics['data_out_compression'] += len(compressed_data)
                self._compression_statistics['data_out'] += len(compressed_data)

                metadata = {
                    self._COMPRESSION_KEY: {
                        'type': self.compression_active.NAME,
                        'materials': materials
                    }
                }
                return compressed_data, metadata
            else:
                self._compression_statistics['data_out'] += len(data)
                return data, {}
        else:
            self._compression_statistics['data_out'] += len(data)
            return data, {}

    def _uncompress(self, data, metadata):
        if self._COMPRESSION_KEY in metadata:
            type = metadata[self._COMPRESSION_KEY]['type']
            if type in self.compression:
                return self.compression[type].uncompress(data=data,
                                                         materials=metadata[self._COMPRESSION_KEY]['materials'],
                                                         original_size=metadata[self._SIZE_KEY])
            else:
                raise IOError('Unsupported compression type {} in object metadata.'.format(type))
        else:
            return data

    def wait_reads_finished(self):
        concurrent.futures.wait(self._read_futures)

    def wait_saves_finished(self):
        concurrent.futures.wait(self._write_futures)

    def use_read_cache(self, enable):
        return False

    def _log_compression_statistics(self):
        if self._compression_statistics['objects_considered'] == 0:
            return

        tbl = PrettyTable()
        tbl.field_names = ['Objects considered', 'Objects compressed', 'Data in', 'Data out',
                           'Overall compression ratio', 'Data input to compression', 'Data output from compression',
                           'Compression ratio']
        tbl.align['Objects considered'] = 'r'
        tbl.align['Objects compressed'] = 'r'
        tbl.align['Data in'] = 'r'
        tbl.align['Data out'] = 'r'
        tbl.align['Overall compression ratio'] = 'r'
        tbl.align['Data input to compression'] = 'r'
        tbl.align['Data output from compression'] = 'r'
        tbl.align['Compression ratio'] = 'r'
        tbl.add_row([
            self._compression_statistics['objects_considered'],
            self._compression_statistics['objects_compressed'],
            self._compression_statistics['data_in'],
            self._compression_statistics['data_out'],
            '{:.2f}'.format(self._compression_statistics['data_in'] / self._compression_statistics['data_out']),
            self._compression_statistics['data_in_compression'],
            self._compression_statistics['data_out_compression'],
            '{:.2f}'.format(self._compression_statistics['data_in_compression']
                                / self._compression_statistics['data_out_compression'])
        ])
        logger.info('Compression statistics:  \n' + textwrap.indent(str(tbl), '          '))

    def close(self):
        self._log_compression_statistics()

        if len(self._read_futures) > 0:
            logger.warning('Data backend closed with {} outstanding read jobs, cancelling them.'
                        .format(len(self._read_futures)))
            for future in self._read_futures:
                future.cancel()
            self._read_futures = []
        if len(self._write_futures) > 0:
            logger.warning('Data backend closed with {} outstanding write jobs, cancelling them.'
                        .format(len(self._write_futures)))
            for future in self._write_futures:
                future.cancel()
            self._write_futures = []
        self._write_executor.shutdown()
        self._read_executor.shutdown()

    def _block_uid_to_key(self, block_uid):
        key_name = '{:016x}-{:016x}'.format(block_uid.left, block_uid.right)
        hash = hashlib.md5(key_name.encode('ascii')).hexdigest()
        return '{}{}/{}/{}-{}'.format(self._BLOCKS_PREFIX, hash[0:2], hash[2:4], hash[:8], key_name)

    def _key_to_block_uid(self, key):
        bpl = len(self._BLOCKS_PREFIX)
        if len(key) != 48 + bpl:
            raise RuntimeError('Invalid key name {}'.format(key))
        return BlockUid(int(key[15 + bpl:15 + bpl + 16], 16), int(key[32 + bpl:32 + bpl + 16], 16))

    def _version_uid_to_key(self, version_uid):
        return '{}{}/{}/{}'.format(self._VERSIONS_PREFIX, version_uid.readable[-1:], version_uid.readable[-2:-1], version_uid.readable)

    def _key_to_version_uid(self, key):
        vpl = len(self._VERSIONS_PREFIX)
        vl = len(VersionUid(1).readable)
        if len(key) != vpl + vl + 4:
            raise RuntimeError('Invalid key name {}'.format(key))
        return VersionUid.create_from_readables(key[vpl + 4:vpl + vl + 4])

    @abstractmethod
    def _write_object(self, key, data):
        raise NotImplementedError

    @abstractmethod
    def _read_object(self, key):
        raise NotImplementedError

    @abstractmethod
    def _rm_object(self):
        raise NotImplementedError

    @abstractmethod
    def _rm_many_objects(self):
        raise NotImplementedError

    @abstractmethod
    def _list_objects(self):
        raise NotImplementedError


class ReadCacheDataBackend(DataBackend):

    def __init__(self, config):
        read_cache_directory = config.get('dataBackend.readCache.directory', None, types=str)
        read_cache_maximum_size = config.get('dataBackend.readCache.maximumSize', None, types=int)
    
        if read_cache_directory and not read_cache_maximum_size or not read_cache_directory and read_cache_maximum_size:
            raise ConfigurationError('Both dataBackend.readCache.directory and dataBackend.readCache.maximumSize need to be set '
                                  + 'to enable disk based caching.')
    
        if read_cache_directory and read_cache_maximum_size:
            os.makedirs(read_cache_directory, exist_ok=True)
            try:
                self._read_cache = Cache(read_cache_directory,
                                    size_limit=read_cache_maximum_size,
                                    eviction_policy='least-frequently-used',
                                    statistics=1,
                                    )
            except Exception:
                logger.warning('Unable to enable disk based read caching. Continuing without it.')
                self._read_cache = None
            else:
                logger.debug('Disk based read caching instantiated (cache size {}).'.format(read_cache_maximum_size))
        else:
            self._read_cache = None
        self._use_read_cache = True

        # Start reader and write threads after the disk cached is created, so that they see it.
        super().__init__(config)

    def _read(self, block):
        key = self._block_uid_to_key(block.uid)
        if self._read_cache is not None and self._use_read_cache:
            data = self._read_cache.get(key)
            if data:
                return block, data

        block, data = super()._read(block)

        # We always put blocks into the cache even when self._use_read_cache is False
        if self._read_cache is not None:
            self._read_cache.set(key, data)

        return block, data

    def use_read_cache(self, enable):
        old_value =  self._use_read_cache
        self._use_read_cache = enable
        return old_value
        
    def close(self):
        super().close()
        if self._read_cache is not None:
            (cache_hits, cache_misses) = self._read_cache.stats()
            logger.debug('Disk based cache statistics (since cache creation): {} hits, {} misses.'.format(cache_hits, cache_misses))
            self._read_cache.close()
