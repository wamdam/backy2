#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger
from backy2.utils import TokenBucket
import fnmatch
import hashlib
import os
import queue
import shortuuid
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

    _SUPPORTS_PARTIAL_READS = True
    _SUPPORTS_PARTIAL_WRITES = True


    def __init__(self, config):
        super().__init__(config)

        self.path = config.get('path')

    def _writer(self, id_=0):
        """ A threaded background writer """
        while True:
            entry = self._write_queue.get()
            if entry is None:
                logger.debug("Writer {} finishing.".format(id_))
                break
            uid, data = entry
            path = os.path.join(self.path, self._path(uid))
            filename = self._filename(uid)
            time.sleep(self.write_throttling.consume(len(data)))
            t1 = time.time()
            try:
                with open(filename, 'wb') as f:
                    r = f.write(data)
            except FileNotFoundError:
                makedirs(path)
                with open(filename, 'wb') as f:
                    r = f.write(data)
            t2 = time.time()
            assert r == len(data)
            self._write_queue.task_done()
            logger.debug('Writer {} wrote data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, uid, t2-t1, self._write_queue.qsize()))


    def _reader(self, id_):
        """ A threaded background reader """
        while True:
            d = self._read_queue.get()  # contains block, offset, length
            if d is None:
                logger.debug("Reader {} finishing.".format(id_))
                break
            block, offset, length = d
            t1 = time.time()
            try:
                data = self._read_raw(block.uid, offset, length)
            except FileNotFoundError:
                self._read_data_queue.put((block, offset, length, None))  # catch this!
            else:
                time.sleep(self.read_throttling.consume(len(data)))
                self._read_data_queue.put((block, offset, length, data))
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


    def save(self, data, _sync=False):
        uid = self._uid()
        self._write_queue.put((uid, data))
        if _sync:
            self._write_queue.join()
        return uid

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

    def read(self, block, sync=False, offset=0, length=None):
        self._read_queue.put((block, offset, length))
        if sync:
            rblock, offset, length, data = self.read_get()
            assert offset == offset
            assert length == length
            if rblock.id != block.id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            if data is None:
                raise FileNotFoundError('UID {} not found.'.format(block.uid))
            return data

    def read_get(self, qblock=True, qtimeout=None):
        block, offset, length, data = self._read_data_queue.get(block=qblock, timeout=qtimeout)
        self._read_data_queue.task_done()
        return block, offset, length, data

    def _read_raw(self, uid, offset=0, length=None):
        filename = self._filename(uid)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        if offset==0 and length is None:
            with open(filename, 'rb') as f:
                data = f.read()
            return data
        else:
            with open(filename, 'rb') as f:
                if offset:
                    f.seek(offset)
                if length:
                    data = f.read(length)
                else:
                    data = f.read()
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



