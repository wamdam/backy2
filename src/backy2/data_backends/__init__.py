#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import queue
import threading
import time

import hashlib
import importlib
import shortuuid

from backy2.logging import logger
from backy2.utils import TokenBucket


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
                    raise NotImplementedError('Encryption type {} is not supported'.format(name))
                else:
                    if (name != encryption_module.Encryption.NAME):
                        raise RuntimeError('Encryption module file name and name don\'t agree ({} != {})'
                                         .format(name, encryption_module.Encryption.NAME))

                    self.encryption[name] = encryption_module.Encryption(materials)
                    
                if config.get_from_dict(encryption_module_dict, 'active', types=bool):
                    if self.encryption_active is not None:
                        raise RuntimeError('Only one encryption module can be active')
                    self.encryption_active = self.encryption[name]

        compression_modules = config.get('dataBackend.{}.compression'.format(self.NAME), None, types=list)
        if compression_modules is not None:
            for compression_module_dict in compression_modules:
                name = config.get_from_dict(compression_module_dict, 'name', types=str)
                materials = config.get_from_dict(compression_module_dict, 'materials', None, types=dict)
                try:
                    compression_module = importlib.import_module('{}.{}'.format(self._COMPRESSION_PACKAGE_PREFIX, name))
                except ImportError:
                    raise NotImplementedError('Compression type {} is not supported'.format(name))
                else:
                    if (name != compression_module.Compression.NAME):
                        raise RuntimeError('Compression module file name and name don\'t agree ({} != {})'
                                           .format(name, compression_module.Compression.NAME))

                    self.compression[name] = compression_module.Compression(materials)

                if config.get_from_dict(compression_module_dict, 'active', types=bool):
                    if self.compression_active is not None:
                        raise RuntimeError('Only one compression module can be active')
                    self.compression_active = self.compression[name]

        simultaneous_writes = config.get('dataBackend.simultaneousWrites', types=int)
        simultaneous_reads = config.get('dataBackend.simultaneousReads', types=int)
        bandwidth_read = config.get('dataBackend.bandwidthRead', types=int)
        bandwidth_write = config.get('dataBackend.bandwidthWrite', types=int)

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling

        self.write_queue_length = simultaneous_writes + self.WRITE_QUEUE_LENGTH
        self.read_queue_length = simultaneous_reads + self.READ_QUEUE_LENGTH
        self._write_queue = queue.Queue(self.write_queue_length)
        self._write_queue_exception = queue.Queue()
        # The read queue has no limit, so that we can queue a whole version worth of blocks
        self._read_queue = queue.Queue()
        self._read_data_queue = queue.Queue(self.read_queue_length)
        self._writer_threads = []
        self._reader_threads = []
        for i in range(simultaneous_writes):
            _writer_thread = threading.Thread(target=self._writer, args=(i,))
            _writer_thread.daemon = True
            _writer_thread.start()
            self._writer_threads.append(_writer_thread)
        for i in range(simultaneous_reads):
            _reader_thread = threading.Thread(target=self._reader, args=(i,))
            _reader_thread.daemon = True
            _reader_thread.start()
            self._reader_threads.append(_reader_thread)

    def _writer(self, id_):
        """ A threaded background writer """
        while True:
            entry = self._write_queue.get()
            if entry is None:
                logger.debug("Writer {}({}) finishing.".format(threading.current_thread().name, id_))
                break
            uid, data = entry
            time.sleep(self.write_throttling.consume(len(data)))
            t1 = time.time()
            try:
                data, metadata = self.compress(data)
                data, metadata_2 = self.encrypt(data)
                metadata.update(metadata_2)

                self._write_raw(uid, data, metadata)
            except Exception as exception:
                self._write_queue_exception.put((id_, threading.current_thread.name, uid, exception))
                return
            t2 = time.time()
            self._write_queue.task_done()
            logger.debug('Writer {}({}) wrote data async. uid {} in {:.2f}s (Queue size is {})'.format(threading.current_thread().name, id_, uid, t2-t1, self._write_queue.qsize()))

    def _reader(self, id_):
        """ A threaded background reader """
        while True:
            entry = self._read_queue.get()
            if entry is None:
                logger.debug("Reader {}({}) finishing.".format(threading.current_thread().name, id_))
                break
            block, offset, length = entry
            t1 = time.time()
            try:
                data, metadata = self._read_raw(block.uid, offset, length)
                data = self.decrypt(data, metadata)
                data = self.uncompress(data, metadata)
            except Exception as exception:
                self._read_data_queue.put((exception, block, 0, None, 0))
            else:
                time.sleep(self.read_throttling.consume(len(data)))
                self._read_data_queue.put((None, block, offset, len(data), data))
                t2 = time.time()
                self._read_queue.task_done()
                logger.debug('Reader {}({}) read data async. uid {} in {:.2f}s (Queue size is {})'.format(threading.current_thread().name, id_, block.uid, t2-t1, self._read_queue.qsize()))

    def _uid(self):
        # 32 chars are allowed and we need to spread the first few chars so
        # that blobs are distributed nicely. And want to avoid hash collisions.
        # So we create a real base57-encoded uuid (22 chars) and prefix it with
        # its own md5 hash[:10].
        suuid = shortuuid.uuid()
        hash = hashlib.md5(suuid.encode('ascii')).hexdigest()
        return hash[:10] + suuid

    def save(self, data, _sync=False):
        try:
            (id_, thread_name, uid, exception) = self._write_queue_exception.get(block=False)
            raise RuntimeError('Writer {}({}) failed for {}'.format(thread_name, id_, uid)) from exception
        except queue.Empty:
            pass

        uid = self._uid()
        self._write_queue.put((uid, data))

        if _sync:
            self._write_queue.join()
            try:
                (id_, thread_name, uid, exception) = self._write_queue_exception.get(block=False)
                raise ('Writer {}({}) failed for {}'.format(thread_name, id_, uid)) from exception
            except queue.Empty:
                pass

        return uid

    def read(self, block, sync=False):
        self._read_queue.put((block, 0, None))
        if sync:
            rblock, offset, length, data = self.read_get()
            if rblock.id != block.id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            return data

    def read_get(self, qblock=True, qtimeout=None):
        exception, block, offset, length, data = self._read_data_queue.get(block=qblock, timeout=qtimeout)
        self._read_data_queue.task_done()
        if exception is not None:
            if isinstance(exception, FileNotFoundError):
                raise FileNotFoundError('Reader failed for {}'.format(block.uid)) from exception
            else:
                raise RuntimeError('Reader failed for {}'.format(block.uid)) from exception
        return block, offset, length, data

    def update(self, uid, data, offset=0):
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

    def encrypt(self, data):
        if self.encryption_active is not None:
            data, metadata = self.encryption_active.encrypt(data)
            metadata[self._ENCRYPTION_HEADER] = self.encryption_active.NAME
            return data, metadata
        else:
            return data, {}

    def decrypt(self, data, metadata):
        if self._ENCRYPTION_HEADER in metadata:
            name = metadata[self._ENCRYPTION_HEADER]
            if name in self.encryption:
                return self.encryption[name].decrypt(data, metadata)
            else:
                raise IOError('Unsupported encryption type {}'.format(name))
        else:
            return data

    def compress(self, data):
        if self.compression_active is not None:
            compressed_data, metadata = self.compression_active.compress(data)
            if len(compressed_data) < len(data):
                metadata[self._COMPRESSION_HEADER] = self.compression_active.NAME
                return compressed_data, metadata
            else:
                return data, {}
        else:
            return data, {}

    def uncompress(self, data, metadata):
        if self._COMPRESSION_HEADER in metadata:
            name = metadata[self._COMPRESSION_HEADER]
            if name in self.compression:
                return self.compression[name].uncompress(data, metadata)
            else:
                raise IOError('Unsupported compression type {}'.format(name))
        else:
            return data

    def wait_read_finished(self):
        self._read_queue.join()

    def wait_write_finished(self):
        self._write_queue.join()

    def close(self):
        for _writer_thread in self._writer_threads:
            self._write_queue.put(None)  # ends the thread
        for _writer_thread in self._writer_threads:
            _writer_thread.join()
        for _reader_thread in self._reader_threads:
            self._read_queue.put(None)  # ends the thread
        for _reader_thread in self._reader_threads:
            _reader_thread.join()

