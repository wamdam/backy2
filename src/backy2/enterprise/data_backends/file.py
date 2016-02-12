#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.enterprise.data_backends import DataBackend as _DataBackend
from backy2.logging import logger
import fnmatch
import hashlib
import os
import queue
import threading
import time
import uuid


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

    _SUPPORTS_PARTIAL_READS = True
    _SUPPORTS_PARTIAL_WRITES = True


    def __init__(self, config):
        self.path = config.get('path')
        simultaneous_writes = config.getint('simultaneous_writes')
        self.write_queue_length = simultaneous_writes + self.WRITE_QUEUE_LENGTH
        self._queue = queue.Queue(self.write_queue_length)
        self._writer_threads = []
        for i in range(simultaneous_writes):
            _writer_thread = threading.Thread(target=self._writer, args=(i,))
            _writer_thread.daemon = True
            _writer_thread.start()
            self._writer_threads.append(_writer_thread)


    def _writer(self, id_=0):
        """ A threaded background writer """
        while True:
            entry = self._queue.get()
            if entry is None:
                logger.debug("Writer {} finishing.".format(id_))
                break
            uid, data = entry
            path = os.path.join(self.path, self._path(uid))
            filename = self._filename(uid)
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
            self._queue.task_done()
            logger.debug('Writer {} wrote data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, uid, t2-t1, self._queue.qsize()))


    def _uid(self):
        # a uuid always starts with the same bytes, so let's widen this
        return hashlib.md5(uuid.uuid1().bytes).hexdigest()


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
        self._queue.put((uid, data))
        if _sync:
            self._queue.join()
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


    def read(self, uid, offset=0, length=None):
        filename = self._filename(uid)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        if offset==0 and length is None:
            return open(filename, 'rb').read()
        else:
            with open(filename, 'rb') as f:
                if offset:
                    f.seek(offset)
                if length:
                    return f.read(length)
                else:
                    return f.read()



    def get_all_blob_uids(self):
        matches = []
        for root, dirnames, filenames in os.walk(self.path):
            for filename in fnmatch.filter(filenames, '*.blob'):
                uid = filename.split('.')[0]
                matches.append(uid)
        return matches


    def close(self):
        for _writer_thread in self._writer_threads:
            self._queue.put(None)  # ends the thread
        for _writer_thread in self._writer_threads:
            _writer_thread.join()



