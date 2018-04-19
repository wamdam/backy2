#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import hashlib
import queue
import threading
import time

import os

import importlib
import json

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

    def __init__(self, config):
        self.encryption = {}
        self.compression = {}
        self.encryption_default = None
        self.compression_default = None

        encryption_types = config.get('encryption', '')
        if encryption_types != '':
            encryption_types = [type.strip() for type in encryption_types.split(',')]
            for encryption_type in encryption_types:
                materials = json.loads(config.get('encryption_materials', '{}'))
                try:
                    encryption_module = importlib.import_module(encryption_type)
                except ImportError:
                    raise NotImplementedError('encryption type {} is not supported'.format(encryption_type))
                else:
                    self.encryption[encryption_module.Encryption.NAME] = encryption_module.Encryption(materials)

        encryption_default = config.get('encryption_default', '')
        if encryption_default != '' and encryption_default != 'none':
            if not self.SUPPORTS_METADATA:
                raise NotImplementedError('data store doesn\'t support metadata, no encryption possible')
            if encryption_default in self.encryption:
                self.encryption_default = self.encryption[encryption_default]
            else:
                raise NotImplementedError('encryption default {} is not supported'.format(encryption_type))

        compression_types = config.get('compression', '')
        if compression_types != '':
            compression_types = [type.strip() for type in compression_types.split(',')]
            for compression_type in compression_types:
                materials = json.loads(config.get('compression_materials', '{}'))
                try:
                    compression_module = importlib.import_module(compression_type)
                except ImportError:
                    raise NotImplementedError('compression type {} is not supported'.format(compression_type))
                else:
                    self.compression[compression_module.Compression.NAME] = compression_module.Compression(materials)

        compression_default = config.get('compression_default', '')
        if compression_default != '' and compression_default != 'none':
            if not self.SUPPORTS_METADATA:
                raise NotImplementedError('data store doesn\'t support metadata, no compression possible')
            if compression_default in self.compression:
                self.compression_default = self.compression[compression_default]
            else:
                raise NotImplementedError('compression default {} is not supported'.format(compression_type))

        simultaneous_writes = config.getint('simultaneous_writes', 1)
        simultaneous_reads = config.getint('simultaneous_reads', 1)
        bandwidth_read = config.getint('bandwidth_read', 0)
        bandwidth_write = config.getint('bandwidth_write', 0)

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
        if self.encryption_default is not None:
            data, metadata = self.encryption_default.encrypt(data)
            metadata[self._ENCRYPTION_HEADER] = self.encryption_default.NAME
            return data, metadata
        else:
            return data, {}

    def decrypt(self, data, metadata):
        if self._ENCRYPTION_HEADER in metadata:
            type = metadata[self._ENCRYPTION_HEADER]
            if type in self.encryption:
                return self.encryption[type].decrypt(data, metadata)
            else:
                raise IOError('unsupported encryption type {}'.format(type))
        else:
            return data

    def compress(self, data):
        if self.compression_default is not None:
            compressed_data, metadata = self.compression_default.compress(data)
            if len(compressed_data) < len(data):
                metadata[self._COMPRESSION_HEADER] = self.compression_default.NAME
                return compressed_data, metadata
            else:
                return data, {}
        else:
            return data, {}

    def uncompress(self, data, metadata):
        if self._COMPRESSION_HEADER in metadata:
            type = metadata[self._COMPRESSION_HEADER]
            if type in self.compression:
                return self.compression[type].uncompress(data, metadata)
            else:
                raise IOError('unsupported compression type {}'.format(type))
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

