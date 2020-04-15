#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger
from backy2.utils import TokenBucket

import boto3
from botocore.client import Config as BotoCoreClientConfig
from botocore.exceptions import ClientError
from botocore.handlers import set_list_objects_encoding_type_url

from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

import hashlib
import io
import os
import queue
import random
import shortuuid
import socket
import sys
import threading
import time

STATUS_NOTHING = 0
STATUS_READING = 1
STATUS_WRITING = 2
STATUS_THROTTLING = 3
STATUS_QUEUE = 4

_lock = threading.Lock()

class DataBackend(_DataBackend):
    """ A DataBackend which stores in S3 compatible storages. The files are
    stored in a configurable bucket. """

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    _SUPPORTS_PARTIAL_READS = False
    _SUPPORTS_PARTIAL_WRITES = False
    last_exception = None

    def __init__(self, config):
        self.aws_access_key_id = config.get('aws_access_key_id')
        if self.aws_access_key_id is None:
            aws_access_key_id_file = config.get('aws_access_key_id_file')
            with open(aws_access_key_id_file, 'r', encoding="ascii") as f:
                self.aws_access_key_id = f.read().rstrip()

        self.aws_secret_access_key = config.get('aws_secret_access_key')
        if self.aws_secret_access_key is None:
            aws_secret_access_key_file = config.get('aws_secret_access_key_file')
            with open(aws_secret_access_key_file, 'r', encoding="ascii") as f:
                self.aws_secret_access_key = f.read().rstrip()

        self.region_name = config.get('region_name', '')
        self.host = config.get('host', '')
        self.secure = config.getboolean('secure', False)
        self.bucket_name = config.get('bucket_name', '')

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
        self._read_queue = queue.Queue()
        self._read_data_queue = queue.Queue(self.read_queue_length)

        self.client = self._get_client()  # for read_raw, rm, ...

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


    def _writer_thread_status(self):
        _lock.acquire()
        sys.stdout.write("\r")
        for status in self.writer_thread_status.values():
            if status == STATUS_NOTHING:
                sys.stdout.write(' ')
            elif status == STATUS_READING:
                sys.stdout.write('R')
            elif status == STATUS_WRITING:
                sys.stdout.write('W')
            elif status == STATUS_THROTTLING:
                sys.stdout.write('T')
            elif status == STATUS_QUEUE:
                sys.stdout.write('=')
        sys.stdout.flush()
        #sys.stdout.write("\n")
        _lock.release()


    def _get_client(self):
        client = Minio(self.host,
                  access_key=self.aws_access_key_id,
                  secret_key=self.aws_secret_access_key,
                  secure=self.secure)
        return client


    def _writer(self, id_):
        """ A threaded background writer """
        #bucket = None
        client = None
        while True:
            self.writer_thread_status[id_] = STATUS_QUEUE
            #self._writer_thread_status()
            entry = self._write_queue.get()
            self.writer_thread_status[id_] = STATUS_NOTHING
            if entry is None or self.last_exception:
                logger.debug("Writer {} finishing.".format(id_))
                break
            if client is None:
                client = self._get_client()
            uid, data, callback = entry

            self.writer_thread_status[id_] = STATUS_THROTTLING
            time.sleep(self.write_throttling.consume(len(data)))
            self.writer_thread_status[id_] = STATUS_NOTHING

            try:
                self.writer_thread_status[id_] = STATUS_WRITING
                client.put_object(self.bucket_name, uid, io.BytesIO(data), len(data))
                #client.upload_fileobj(io.BytesIO(data), Key=uid, Bucket=self._bucket_name)
                self.writer_thread_status[id_] = STATUS_NOTHING
                #if random.random() > 0.9:
                #    raise ValueError("This is a test")
            except Exception as e:
                self.last_exception = e
            else:
                if callback:
                    callback(uid)
                self._write_queue.task_done()


    def _reader(self, id_):
        """ A threaded background reader """
        client = None
        while True:
            block = self._read_queue.get()  # contains block
            if block is None or self.last_exception:
                logger.debug("Reader {} finishing.".format(id_))
                break
            if client is None:
                client = self._get_client()
            t1 = time.time()
            try:
                self.reader_thread_status[id_] = STATUS_READING
                data = self.read_raw(block.uid, client)
                self.reader_thread_status[id_] = STATUS_NOTHING
                #except FileNotFoundError:
            except Exception as e:
                self.last_exception = e
            else:
                self._read_data_queue.put((block, data))
                t2 = time.time()
                self._read_queue.task_done()
                logger.debug('Reader {} read data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, block.uid, t2-t1, self._read_queue.qsize()))


    def read_raw(self, block_uid, _client=None):
        if not _client:
            _client = self._get_client()

        data = _client.get_object(self.bucket_name, block_uid).read()
        time.sleep(self.read_throttling.consume(len(data)))  # TODO: Need throttling in thread statistics!
        return data


    def _uid(self):
        # 32 chars are allowed and we need to spread the first few chars so
        # that blobs are distributed nicely. And want to avoid hash collisions.
        # So we create a real base57-encoded uuid (22 chars) and prefix it with
        # its own md5 hash[:10].
        suuid = shortuuid.uuid()
        hash = hashlib.md5(suuid.encode('ascii')).hexdigest()
        return hash[:10] + suuid


    def save(self, data, _sync=False, callback=None):
        if self.last_exception:
            raise self.last_exception
        uid = self._uid()
        self._write_queue.put((uid, data, callback))
        if _sync:
            self._write_queue.join()
        return uid


    def rm(self, uid):
        try:
            self.client.remove_object(self.bucket_name, uid)
        except ResponseError as e:
            raise


    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """
        try:
            for del_err in minioClient.remove_objects(self.bucket_name, uids):
                logger.error("S3 Object Deletion Error: {}".format(del_err))
        except ResponseError as err:
            raise


    def read(self, block, sync=False):
        self._read_queue.put(block)
        if sync:
            rblock, offset, length, data = self.read_get()
            if rblock.id != block.id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            if data is None:
                raise FileNotFoundError('UID {} not found.'.format(block.uid))
            return data


    def read_get(self, timeout=30):
        if self.last_exception:
            raise self.last_exception
        block, data = self._read_data_queue.get(timeout=timeout)
        offset = 0
        length = len(data)
        self._read_data_queue.task_done()
        return block, offset, length, data


    def read_queue_size(self):
        return self._read_queue.qsize()


    def get_all_blob_uids(self, prefix=None):
        objects = self.client.list_objects(self.bucket_name, prefix)
        return [o.object_name for o in objects]


    def queue_status(self):
        return {
            'rq_filled': self._read_data_queue.qsize() / self._read_data_queue.maxsize,  # 0..1
            'wq_filled': self._write_queue.qsize() / self._write_queue.maxsize,
        }


    def thread_status(self):
        return "DaBaR: N{} R{} QL{}  DaBaW: N{} W{} T{} QL{}".format(
                len([t for t in self.reader_thread_status.values() if t==STATUS_NOTHING]),
                len([t for t in self.reader_thread_status.values() if t==STATUS_READING]),
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


