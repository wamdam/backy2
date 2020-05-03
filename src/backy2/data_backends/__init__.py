#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import shortuuid
import hashlib
import binascii
from backy2.crypt import get_crypt

STATUS_NOTHING = 0
STATUS_READING = 1
STATUS_WRITING = 2
STATUS_THROTTLING = 3
STATUS_QUEUE = 4

class DataBackend():
    """ Holds BLOBs, never overwrites
    """

    def __init__(self, config, encryption_key, encryption_version=None):
        self.encryption_key = encryption_key
        if encryption_version == None:
            self.cc_latest = get_crypt()(key=encryption_key)
        else:
            self.cc_latest = get_crypt(version=encryption_version)(key=encryption_key)
        self.cc = {}


    def _cc_by_version(self, version):
        if version not in self.cc:
            self.cc[version] = get_crypt(version=version)(key=self.encryption_key)
        return self.cc[version]


    def _uid(self):
        # 32 chars are allowed and we need to spread the first few chars so
        # that blobs are distributed nicely. And want to avoid hash collisions.
        # So we create a real base57-encoded uuid (22 chars) and prefix it with
        # its own md5 hash[:10].
        suuid = shortuuid.uuid()
        hash = hashlib.md5(suuid.encode('ascii')).hexdigest()
        return hash[:10] + suuid


    def save(self, data, _sync=False, callback=None):
        """ Saves data, returns unique ID """
        if self.last_exception:
            raise self.last_exception
        uid = self._uid()

        # Important: It's important to call this from the main thread because
        # zstandard IS NOT THREAD SAFE as stated at https://pypi.org/project/zstandard/:
        # """ Unless specified otherwise, assume that no two methods of
        # ZstdCompressor instances can be called from multiple Python threads
        # simultaneously. In other words, assume instances are not thread safe
        # unless stated otherwise."""
        blob, enc_envkey, enc_nonce = self.cc_latest.encrypt(data)
        enc_version = self.cc_latest.VERSION

        self._write_queue.put((uid, enc_envkey, enc_version, enc_nonce, blob, callback))
        if _sync:
            self._write_queue.join()
        return uid


    def update(self, uid, data, offset=0):
        """ Updates data, returns written bytes.
        This is only available on *some* data backends.
        """
        raise NotImplementedError()


    def read(self, block, sync=False):
        """ Adds the read request to the read queue.
        If sync is True, returns b'<data>' or raises FileNotFoundError.
        Do not mix sync and non-sync reads in one program!
        With length==None, all known data is read for this uid.
        """
        self._read_queue.put(block)
        if sync:
            rblock, offset, length, data = self.read_get()
            if rblock.id != block.id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            if data is None:
                raise FileNotFoundError('UID {} not found.'.format(block.uid))
            return data


    def read_get(self, timeout=30):
        """ 
        Returns (block, offset, length, data) from the reader threads.
        """
        if self.last_exception:
            raise self.last_exception
        block, blob = self._read_data_queue.get(timeout=timeout)

        # Important: It's important to call this from the main thread because
        # zstandard IS NOT THREAD SAFE as stated at https://pypi.org/project/zstandard/:
        # """ Unless specified otherwise, assume that no two methods of
        # ZstdCompressor instances can be called from multiple Python threads
        # simultaneously. In other words, assume instances are not thread safe
        # unless stated otherwise."""
        cc = self._cc_by_version(block.enc_version)
        if block.enc_envkey:
            envelope_key = binascii.unhexlify(block.enc_envkey)
        else:
            envelope_key = b''
        data = cc.decrypt(blob, envelope_key)

        offset = 0
        length = len(data)
        self._read_data_queue.task_done()
        return block, offset, length, data


    def read_sync(self, block):
        """ Do a read_raw and decrypt it
        """
        cc = self._cc_by_version(block.enc_version)
        if block.enc_envkey:
            envelope_key = binascii.unhexlify(block.enc_envkey)
        else:
            envelope_key = None
        data = cc.decrypt(self.read_raw(block), envelope_key=envelope_key)
        return data


    def read_raw(self, block):
        """ Read a block in sync. Returns block's data.
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


    def queue_status(self):
        return {
            'rq_filled': self._read_data_queue.qsize() / self._read_data_queue.maxsize,  # 0..1
            'wq_filled': self._write_queue.qsize() / self._write_queue.maxsize,
        }


    def thread_status(self):
        return "DaBaR: N{} R{} T{} QL{}  DaBaW: N{} W{} T{} QL{}".format(
                len([t for t in self.reader_thread_status.values() if t==STATUS_NOTHING]),
                len([t for t in self.reader_thread_status.values() if t==STATUS_READING]),
                len([t for t in self.reader_thread_status.values() if t==STATUS_THROTTLING]),
                self._read_queue.qsize(),
                len([t for t in self.writer_thread_status.values() if t==STATUS_NOTHING]),
                len([t for t in self.writer_thread_status.values() if t==STATUS_WRITING]),
                len([t for t in self.writer_thread_status.values() if t==STATUS_THROTTLING]),
                self._write_queue.qsize(),
                )


    def close(self):
        for _writer_thread in self._writer_threads:
            self._write_queue.put(None)  # ends the thread
        for _writer_thread in self._writer_threads:
            _writer_thread.join()
        for _reader_thread in self._reader_threads:
            self._read_queue.put(None)  # ends the thread
        for _reader_thread in self._reader_threads:
            _reader_thread.join()
