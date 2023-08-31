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
import hashlib
#import io
import os
import queue
import random
import socket
import sys
import threading
import time


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class DataBackend(_DataBackend):
    """ A DataBackend which stores in S3 compatible storages. The files are
    stored in a configurable bucket. """

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    last_exception = None

    def __init__(self, config, encryption_key, encryption_version=None):
        super().__init__(config, encryption_key, encryption_version)
        aws_access_key_id = config.get('aws_access_key_id')
        if aws_access_key_id is None:
            aws_access_key_id_file = config.get('aws_access_key_id_file')
            with open(aws_access_key_id_file, 'r', encoding="ascii") as f:
                aws_access_key_id = f.read().rstrip()

        aws_secret_access_key = config.get('aws_secret_access_key')
        if aws_secret_access_key is None:
            aws_secret_access_key_file = config.get('aws_secret_access_key_file')
            with open(aws_secret_access_key_file, 'r', encoding="ascii") as f:
                aws_secret_access_key = f.read().rstrip()

        region_name = config.get('region_name', '')
        endpoint_url = config.get('endpoint_url', '')
        use_ssl = config.get('use_ssl', '')
        self._bucket_name = config.get('bucket_name', '')
        addressing_style = config.get('addressing_style', '')
        signature_version = config.get('signature_version', '')
        self._disable_encoding_type = config.get('disable_encoding_type', '')

        simultaneous_writes = config.getint('simultaneous_writes', 1)
        simultaneous_reads = config.getint('simultaneous_reads', 1)
        bandwidth_read = config.getint('bandwidth_read', 0)
        bandwidth_write = config.getint('bandwidth_write', 0)

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling


        self._resource_config = {
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
        }

        if region_name:
            self._resource_config['region_name'] = region_name

        if endpoint_url:
            self._resource_config['endpoint_url'] = endpoint_url

        if use_ssl:
            self._resource_config['use_ssl'] = use_ssl

        resource_config = {}
        if addressing_style:
            resource_config['s3'] = {'addressing_style': addressing_style}

        if signature_version:
            resource_config['signature_version'] = signature_version

        # TODO
        #resource_config['max_pool_connections'] = 100
        #resource_config['parameter_validation'] = False
        #resource_config['use_accelerate_endpoint'] = True

        self._resource_config['config'] = BotoCoreClientConfig(**resource_config)


        self.write_queue_length = simultaneous_writes + self.WRITE_QUEUE_LENGTH
        self.read_queue_length = simultaneous_reads + self.READ_QUEUE_LENGTH
        self._write_queue = queue.Queue(self.write_queue_length)
        self._read_queue = queue.Queue()
        self._read_data_queue = queue.Queue(self.read_queue_length)

        self.bucket = self._get_bucket()  # for read_raw

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


    def _get_bucket(self):
        session = boto3.session.Session()
        if self._disable_encoding_type:
            session.events.unregister('before-parameter-build.s3.ListObjects', set_list_objects_encoding_type_url)
        resource = session.resource('s3', **self._resource_config)
        bucket = resource.Bucket(self._bucket_name)
        return bucket


    def _get_client(self):
        session = boto3.session.Session()
        if self._disable_encoding_type:
            session.events.unregister('before-parameter-build.s3.ListObjects', set_list_objects_encoding_type_url)
        client = session.client('s3', **self._resource_config)
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
                client.put_object(Body=data, Key=uid, Bucket=self._bucket_name)
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
        bucket = None
        while True:
            block = self._read_queue.get()  # contains block
            if block is None or self.last_exception:
                logger.debug("Reader {} finishing.".format(id_))
                break
            if bucket is None:
                bucket = self._get_bucket()
            t1 = time.time()
            try:
                self.reader_thread_status[id_] = STATUS_READING
                data = self.read_raw(block, bucket)
                self.reader_thread_status[id_] = STATUS_NOTHING
                #except FileNotFoundError:
            except Exception as e:
                self.last_exception = e
            else:
                self._read_data_queue.put((block, data))
                t2 = time.time()
                self._read_queue.task_done()
                logger.debug('Reader {} read data async. uid {} in {:.2f}s (Queue size is {})'.format(id_, block.uid, t2-t1, self._read_queue.qsize()))


    def read_raw(self, block, _bucket=None):
        if not _bucket:
            _bucket = self.bucket

        while True:
            obj = _bucket.Object(block.uid)
            try:
                data_dict = obj.get()
                data = data_dict['Body'].read()
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                    raise FileNotFoundError('Key {} not found.'.format(block.uid), block)
                else:
                    raise
            except socket.timeout:
                logger.error('Timeout while fetching from s3, trying again.')
                pass
            except OSError as e:
                # TODO: This is new and currently untested code. I'm not sure
                # why this happens in favour of socket.timeout and also if it
                # might be better to abort the whole restore/backup/scrub if
                # this happens, because I can't tell if the s3 lib is able to
                # recover from this situation and continue or not. We will see
                # this in the logs next time s3 is generating timeouts.
                logger.error('Timeout while fetching from s3 - error is "{}", trying again.'.format(str(e)))
                pass
            else:
                break
        time.sleep(self.read_throttling.consume(len(data)))  # TODO: Need throttling in thread statistics!
        return data


    def rm(self, uid):
        obj = self.bucket.Object(uid)
        try:
            obj.load()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                #raise FileNotFoundError('Key {} not found.'.format(uid)) from None
                logger.error('Unable to remove block: key {} not found.'.format(uid))
            else:
                raise
        else:
            obj.delete()


    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """
        # "The request contains a list of up to 1000 keys that you want to delete."
        no_deletes = []
        for chunk in chunks(uids, 1000):
            logger.debug("About to delete {} objects from the backend.".format(len(chunk)))
            objects = [{'Key': uid} for uid in chunk]
            response = self.bucket.delete_objects(
                Delete={
                    'Objects': objects,
                    #'Quiet': True|False
                },
                RequestPayer='requester',
            )
            # {'Deleted': [{'Key': 'a04ab9bcc0BK6vATCi95Bwb4Djriiy5B'},

            deleted_objects = [d['Key'] for d in response['Deleted']]
            not_found_objects = set(chunk) - set(deleted_objects)
            no_deletes.extend(not_found_objects)
            logger.debug("Deleted {} keys, {} were not found.".format(len(deleted_objects), len(not_found_objects)))
        return no_deletes


    def get_all_blob_uids(self, prefix=None):
        if prefix is None:
            objects_iterable = self.bucket.objects.all()
        else:
            objects_iterable = self.bucket.objects.filter(Prefix=prefix)

        return [o.key for o in objects_iterable]

