#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger
from backy2.utils import TokenBucket
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
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

        super().__init__(config)

        aws_access_key_id = config.get('aws_access_key_id')
        aws_secret_access_key = config.get('aws_secret_access_key')
        host = config.get('host')
        port = config.getint('port')
        is_secure = config.getboolean('is_secure')
        bucket_name = config.get('bucket_name', 'backy2')
        simultaneous_writes = config.getint('simultaneous_writes', 1)
        simultaneous_reads = config.getint('simultaneous_reads', 1)
        bandwidth_read = config.getint('bandwidth_read', 0)
        bandwidth_write = config.getint('bandwidth_write', 0)

        self.multi_delete = config.getboolean('multi_delete', True)

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling

        session = boto3.session.Session()

        self.conn = session.resource(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                endpoint_url='http{}://{}:{}'.format('s' if is_secure else '', host, port),
                config=Config(s3={'addressing_style': 'path'})
            )

        # create our bucket
        exists = True
        try:
            self.conn.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                # Doesn't exists...
                exists = False
            else:
                # e.g. 403 Forbidden -> ACL forbids access
                self.fatal_error = e
                logger.error('Fatal error, dying: {}'.format(e))
                print('Fatal error: {}'.format(e))
                exit(10)
        except OSError as e:
            # no route to host
            self.fatal_error = e
            logger.error('Fatal error, dying: {}'.format(e))
            print('Fatal error: {}'.format(e))
            exit(10)

        if not exists:
            try:
                self.conn.create_bucket(Bucket=bucket_name)
            except (
                    OSError,
                    ClientError,
            ) as e:
                self.fatal_error = e
                logger.error('Fatal error, dying: {}'.format(e))
                print('Fatal error: {}'.format(e))
                exit(10)

        self.bucket = self.conn.Bucket(bucket_name)

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
            object = self.bucket.Object(uid)

            data, metadata = self.compress(data)
            data, metadata_2 = self.encrypt(data)
            metadata.update(metadata_2)

            try:
                r = object.put(Metadata=metadata, Body=data)
            except (
                    OSError,
                    ClientError,
                    ) as e:
                # OSError happens when the S3 host is gone (i.e. network died,
                # host down, ...). boto tries hard to recover, however after
                # several attempts it will give up and raise.
                self.fatal_error = e
                logger.error('Fatal error, dying: {}'.format(e))
                #exit('Fatal error: {}'.format(e))  # this only raises SystemExit
                os._exit(11)
            t2 = time.time()
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
        object = self.bucket.Object(block_uid)
        while True:
            try:
                object = object.get()
                data = object['Body'].read()
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    raise FileNotFoundError('UID {} not found.'.format(block_uid))
                else:
                    raise e
            except socket.timeout:
                logger.error('Timeout while fetching from s3, trying again.')
                pass
            else:
                break
        time.sleep(self.read_throttling.consume(len(data)))

        data = self.decrypt(data, object['Metadata'])
        data = self.uncompress(data, object['Metadata'])

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
        # delete() always returns 204 even when key doesn't exist, so check for existence
        object = self.bucket.Object(uid)
        try:
            object.load()
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise FileNotFoundError('UID {} not found.'.format(uid))
            else:
                raise e
        else:
            object.delete()

    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """

        if self.multi_delete:
            # Amazon (at least) only handles 1000 deletes at a time
            # Split list into parts of at most 1000 elements
            uids_parts = [uids[i:i+1000] for i  in range(0, len(uids), 1000)]

            errors = []
            for part in uids_parts:
                response = self.conn.meta.client.delete_objects(
                    Bucket=self.bucket.name,
                    Delete={
                        'Objects': [{'Key': uid} for uid in part],
                        }
                )
                if 'Errors' in response:
                    errors += list(map(lambda object: object['Key'], response['Errors']))
        else:
            errors = []
            for uid in uids:
                try:
                    self.bucket.Object(uid).delete()
                except ClientError as e:
                    errors.append(uid)

        if len(errors) > 0:
            # unable to test this. ceph object gateway doesn't return errors.
            # raise FileNotFoundError('UIDS {} not found.'.format(errors))
            return errors

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
        if data is None:
            length = 0
        else:
            length = len(data)
        self._read_data_queue.task_done()
        return block, offset, length, data


    def read_queue_size(self):
        return self._read_queue.qsize()

    def get_all_blob_uids(self, prefix=None):
        if prefix is None:
            return [object.key for object in self.bucket.objects.filter()]
        else:
            return [object.key for object in self.bucket.objects.filter(Prefix=prefix)]

    def close(self):
        for _writer_thread in self._writer_threads:
            self._write_queue.put(None)  # ends the thread
        for _writer_thread in self._writer_threads:
            _writer_thread.join()
        for _reader_thread in self._reader_threads:
            self._read_queue.put(None)  # ends the thread
        for _reader_thread in self._reader_threads:
            _reader_thread.join()
