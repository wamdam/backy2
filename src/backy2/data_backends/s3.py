#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger
from backy2.utils import TokenBucket
import boto.exception
import boto.s3.connection
import hashlib
import os
import queue
import shortuuid
import socket
import threading
import time


class DataBackend(_DataBackend):
    """ A DataBackend which stores in S3 compatible storages. The files are
    stored in a configurable bucket. """

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    _SUPPORTS_PARTIAL_READS = False
    _SUPPORTS_PARTIAL_WRITES = False
    fatal_error = None

    def __init__(self, config):
        aws_access_key_id = config.get('aws_access_key_id')
        aws_secret_access_key = config.get('aws_secret_access_key')
        host = config.get('host')
        port = config.getint('port')
        is_secure = config.getboolean('is_secure')
        bucket_name = config.get('bucket_name', 'backy2')
        simultaneous_writes = config.getint('simultaneous_writes', 1)
        simultaneous_reads = config.getint('simultaneous_reads', 1)
        calling_format=boto.s3.connection.OrdinaryCallingFormat()
        bandwidth_read = config.getint('bandwidth_read', 0)
        bandwidth_write = config.getint('bandwidth_write', 0)

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling

        self.conn = boto.connect_s3(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                host=host,
                port=port,
                is_secure=is_secure,
                calling_format=calling_format
            )
        # create our bucket
        try:
            self.bucket = self.conn.create_bucket(bucket_name)
        except boto.exception.S3CreateError:
            # exists...
            pass
        except OSError as e:
            # no route to host
            self.fatal_error = e
            logger.error('Fatal error, dying: {}'.format(e))
            print('Fatal error: {}'.format(e))
            exit(10)

        self.write_queue_length = simultaneous_writes + self.WRITE_QUEUE_LENGTH
        self.read_queue_length = simultaneous_reads + self.READ_QUEUE_LENGTH
        self._write_queue = queue.Queue(self.write_queue_length)
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
            if entry is None or self.fatal_error:
                logger.debug("Writer {} finishing.".format(id_))
                break
            uid, data = entry
            time.sleep(self.write_throttling.consume(len(data)))
            t1 = time.time()
            key = self.bucket.new_key(uid)
            try:
                r = key.set_contents_from_string(data)
            except (
                    OSError,
                    boto.exception.BotoServerError,
                    boto.exception.S3ResponseError,
                    ) as e:
                # OSError happens when the S3 host is gone (i.e. network died,
                # host down, ...). boto tries hard to recover, however after
                # several attempts it will give up and raise.
                # BotoServerError happens, when there is no server.
                # S3ResponseError sometimes happens, when the cluster is about
                # to shutdown. Hard to reproduce because the writer must write
                # in exactly this moment.
                # We let the backup job die here fataly.
                self.fatal_error = e
                logger.error('Fatal error, dying: {}'.format(e))
                #exit('Fatal error: {}'.format(e))  # this only raises SystemExit
                os._exit(11)
            t2 = time.time()
            assert r == len(data)
            self._write_queue.task_done()
            logger.debug('Writer {} wrote data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, uid, t2-t1, self._write_queue.qsize()))


    def _reader(self, id_):
        """ A threaded background reader """
        while True:
            block = self._read_queue.get()  # contains block
            if block is None or self.fatal_error:
                logger.debug("Reader {} finishing.".format(id_))
                break
            t1 = time.time()
            try:
                data = self.read_raw(block.uid)
            except FileNotFoundError:
                self._read_data_queue.put((block, None))  # catch this!
            else:
                self._read_data_queue.put((block, data))
                t2 = time.time()
                self._read_queue.task_done()
                logger.debug('Reader {} read data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, block.uid, t2-t1, self._read_queue.qsize()))


    def read_raw(self, block_uid):
        key = self.bucket.get_key(block_uid)
        if not key:
            raise FileNotFoundError('UID {} not found.'.format(block_uid))
        while True:
            try:
                data = key.get_contents_as_string()
            except socket.timeout:
                logger.error('Timeout while fetching from s3, trying again.')
                pass
            else:
                break
        time.sleep(self.read_throttling.consume(len(data)))
        return data


    def _uid(self):
        # 32 chars are allowed and we need to spread the first few chars so
        # that blobs are distributed nicely. And want to avoid hash collisions.
        # So we create a real base57-encoded uuid (22 chars) and prefix it with
        # its own md5 hash[:10].
        suuid = shortuuid.uuid()
        hash = hashlib.md5(suuid.encode('ascii')).hexdigest()
        return hash[:10] + suuid


    def save(self, data, _sync=False):
        if self.fatal_error:
            raise self.fatal_error
        uid = self._uid()
        self._write_queue.put((uid, data))
        if _sync:
            self._write_queue.join()
        return uid


    def rm(self, uid):
        key = self.bucket.get_key(uid)
        if not key:
            raise FileNotFoundError('UID {} not found.'.format(uid))
        self.bucket.delete_key(uid)


    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """
        errors = self.bucket.delete_keys(uids, quiet=True)
        if errors.errors:
            # unable to test this. ceph object gateway doesn't return errors.
            # raise FileNotFoundError('UIDS {} not found.'.format(errors.errors))
            return errors.errors  # TODO: which should be a list of uids.


    def read(self, block, sync=False):
        self._read_queue.put(block)
        if sync:
            rblock, offset, length, data = self.read_get()
            if rblock.id != block.id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            if data is None:
                raise FileNotFoundError('UID {} not found.'.format(block.uid))
            return data


    def read_get(self):
        block, data = self._read_data_queue.get()
        offset = 0
        length = len(data)
        self._read_data_queue.task_done()
        return block, offset, length, data


    def read_queue_size(self):
        return self._read_queue.qsize()


    def get_all_blob_uids(self, prefix=None):
        return [k.name for k in self.bucket.list(prefix)]


    def close(self):
        for _writer_thread in self._writer_threads:
            self._write_queue.put(None)  # ends the thread
        for _writer_thread in self._writer_threads:
            _writer_thread.join()
        for _reader_thread in self._reader_threads:
            self._read_queue.put(None)  # ends the thread
        for _reader_thread in self._reader_threads:
            _reader_thread.join()
        self.conn.close()


