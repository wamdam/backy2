#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.data_backends import DataBackend as _DataBackend
from backy2.data_backends import (STATUS_NOTHING, STATUS_READING, STATUS_WRITING, STATUS_THROTTLING, STATUS_QUEUE)
from backy2.logging import logger
from backy2.utils import TokenBucket
import fnmatch
import os
import queue
import threading
import time

def makedirs(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass



class DataBackend(_DataBackend):
    """ A DataBackend which stores in files. The files are stored in directories
    starting with the bytes of the generated uid. The depth of this structure
    is configurable via the DEPTH parameter, which defaults to 2. """

    DEPTH = 2
    SPLIT = 2
    SUFFIX = '.blob'
    WRITE_QUEUE_LENGTH = 10
    READ_QUEUE_LENGTH = 20

    last_exception = None


    def __init__(self, config, encryption_key):
        super().__init__(config, encryption_key)
        self.path = config.get('path')
        simultaneous_writes = config.getint('simultaneous_writes')
        simultaneous_reads = config.getint('simultaneous_reads', 1)
        self.write_queue_length = simultaneous_writes + self.WRITE_QUEUE_LENGTH
        self.read_queue_length = simultaneous_reads + self.READ_QUEUE_LENGTH

        bandwidth_read = config.getint('bandwidth_read', 0)
        bandwidth_write = config.getint('bandwidth_write', 0)

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling

        self._write_queue = queue.Queue(self.write_queue_length)
        self._read_queue = queue.Queue()
        self._read_data_queue = queue.Queue(self.read_queue_length)
        self._writer_threads = []
        self._reader_threads = []
        self.reader_thread_status = {}
        self.writer_thread_status = {}
        for i in range(simultaneous_writes):
            _writer_thread = threading.Thread(target=self._writer, args=(i,))
            _writer_thread.daemon = True
            _writer_thread.start()
            self._writer_threads.append(_writer_thread)
            self.writer_thread_status[i] = STATUS_NOTHING
        for i in range(simultaneous_reads):
            _reader_thread = threading.Thread(target=self._reader, args=(i,))
            _reader_thread.daemon = True
            _reader_thread.start()
            self._reader_threads.append(_reader_thread)
            self.reader_thread_status[i] = STATUS_NOTHING



    def _writer(self, id_=0):
        """ A threaded background writer """
        while True:
            entry = self._write_queue.get()
            if entry is None or self.last_exception:
                logger.debug("Writer {} finishing.".format(id_))
                break
            uid, enc_envkey, enc_version, enc_nonce, data, callback = entry

            path = os.path.join(self.path, self._path(uid))
            filename = self._filename(uid)
            self.writer_thread_status[id_] = STATUS_THROTTLING
            time.sleep(self.write_throttling.consume(len(data)))
            self.writer_thread_status[id_] = STATUS_NOTHING
            t1 = time.time()
            try:
                try:
                    self.writer_thread_status[id_] = STATUS_WRITING
                    with open(filename, 'wb') as f:
                        r = f.write(data)
                    self.writer_thread_status[id_] = STATUS_NOTHING
                except FileNotFoundError:
                    self.writer_thread_status[id_] = STATUS_WRITING
                    makedirs(path)
                    with open(filename, 'wb') as f:
                        r = f.write(data)
                    self.writer_thread_status[id_] = STATUS_NOTHING
                assert r == len(data)
            except Exception as e:
                self.last_exception = e
            else:
                t2 = time.time()
                if callback:
                    callback(uid, enc_envkey, enc_version, enc_nonce)
                self._write_queue.task_done()
                #logger.debug('Writer {} wrote data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, uid, t2-t1, self._write_queue.qsize()))


    def _reader(self, id_):
        """ A threaded background reader """
        while True:
            block = self._read_queue.get()
            if block is None:
                logger.debug("Reader {} finishing.".format(id_))
                break
            t1 = time.time()
            try:
                self.reader_thread_status[id_] = STATUS_READING
                data = self.read_raw(block)
                self.reader_thread_status[id_] = STATUS_NOTHING
                #except FileNotFoundError:
            except Exception as e:
                self.last_exception = e
            else:
                self._read_data_queue.put((block, data))
                t2 = time.time()
                self._read_queue.task_done()
                logger.debug('Reader {} read data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, block.uid, t2-t1, self._read_queue.qsize()))


    def _path(self, uid):
        """ Returns a generated path (depth = self.DEPTH) from a uid.
        Example uid=831bde887afc11e5b45aa44e314f9270 and depth=2, then
        it returns "83/1b".
        If depth is larger than available bytes, then available bytes
        are returned only as path."""

        parts = [uid[i:i+self.SPLIT] for i in range(0, len(uid), self.SPLIT)]
        return os.path.join(*parts[:self.DEPTH])


    def _filename(self, uid):
        path = os.path.join(self.path, self._path(uid))
        return os.path.join(path, uid + self.SUFFIX)


    def update(self, uid, data, offset=0):
        with open(self._filename(uid), 'r+b') as f:
            f.seek(offset)
            return f.write(data)


    def rm(self, uid):
        filename = self._filename(uid)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        os.unlink(filename)


    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """
        _no_del = []
        for uid in uids:
            try:
                self.rm(uid)
            except FileNotFoundError:
                _no_del.append(uid)
        return _no_del


    def read_raw(self, block):
        filename = self._filename(block.uid)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        data = open(filename, 'rb').read()
        time.sleep(self.read_throttling.consume(len(data)))
        return data


    def get_all_blob_uids(self, prefix=None):
        if prefix:
            raise RuntimeError('prefix is not supported on file backends.')
        matches = []
        for root, dirnames, filenames in os.walk(self.path):
            for filename in fnmatch.filter(filenames, '*.blob'):
                uid = filename.split('.')[0]
                matches.append(uid)
        return matches
