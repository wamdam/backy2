#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.data_backends import DataBackend as _DataBackend
from backy2.data_backends import (STATUS_NOTHING, STATUS_READING, STATUS_WRITING, STATUS_THROTTLING, STATUS_QUEUE)
from backy2.logging import logger
from backy2.utils import TokenBucket

import boto3
from botocore.client import Config as BotoCoreClientConfig
from botocore.exceptions import ClientError
from botocore.handlers import set_list_objects_encoding_type_url

from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

import io
import os
import queue
import random
import socket
import sys
import threading
import time

class DataBackend(_DataBackend):
    """ A DataBackend which stores in S3 compatible storages. The files are
    stored in a configurable bucket. """

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    last_exception = None

    def __init__(self, config, encryption_key):
        super().__init__(config, encryption_key)
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
            entry = self._write_queue.get()
            self.writer_thread_status[id_] = STATUS_NOTHING
            if entry is None or self.last_exception:
                logger.debug("Writer {} finishing.".format(id_))
                break
            if client is None:
                client = self._get_client()
            uid, enc_envkey, enc_version, enc_nonce, data, callback = entry

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
                    callback(uid, enc_envkey, enc_version, enc_nonce)
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
                data = self.read_raw(block, client)
                self.reader_thread_status[id_] = STATUS_NOTHING
                #except FileNotFoundError:
            except Exception as e:
                self.last_exception = e
            else:
                self._read_data_queue.put((block, data))
                t2 = time.time()
                self._read_queue.task_done()
                logger.debug('Reader {} read data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, block.uid, t2-t1, self._read_queue.qsize()))


    def read_raw(self, block, _client=None):
        if not _client:
            _client = self._get_client()
        data = _client.get_object(self.bucket_name, block.uid).read()
        time.sleep(self.read_throttling.consume(len(data)))  # TODO: Need throttling in thread statistics!
        return data


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


    def get_all_blob_uids(self, prefix=None):
        objects = self.client.list_objects(self.bucket_name, prefix)
        return [o.object_name for o in objects]
