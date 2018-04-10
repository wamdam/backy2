#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger
from backy2.utils import TokenBucket
global b2
import b2
import b2.api
import b2.account_info
from b2.download_dest import DownloadDestBytes
import b2.file_version
from b2.exception import B2Error, FileNotPresent, UnknownError
import hashlib
import os
import queue
import shortuuid
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

        account_id = config.get('account_id')
        application_key = config.get('application_key')
        bucket_name = config.get('bucket_name')

        simultaneous_writes = config.getint('simultaneous_writes', 1)
        simultaneous_reads = config.getint('simultaneous_reads', 1)
        bandwidth_read = config.getint('bandwidth_read', 0)
        bandwidth_write = config.getint('bandwidth_write', 0)

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling

        self.service = b2.api.B2Api(b2.account_info.InMemoryAccountInfo())
        self.service.authorize_account('production', account_id, application_key)
        self.bucket = self.service.get_bucket_by_name(bucket_name)

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

            data, metadata = self.compress(data)
            data, metadata_2 = self.encrypt(data)
            metadata.update(metadata_2)

            try:
                self.bucket.upload_bytes(data, uid, file_infos=metadata)
            except B2Error as e:
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
        data_io = DownloadDestBytes()
        while True:
            try:
                r = self.bucket.download_file_by_name(block_uid, data_io)
            except B2Error as e:
                #if isinstance(e, FileNotPresent) or isinstance(e, UnknownError) and "404 not_found" in str(e):
                if isinstance(e, FileNotPresent):
                    raise FileNotFoundError('UID {} not found.'.format(block_uid))
                else:
                    raise e
            else:
                break
        data = data_io.get_bytes_written()
        time.sleep(self.read_throttling.consume(len(data)))

        data = self.decrypt(data, data_io.file_info)
        data = self.uncompress(data, data_io.file_info)

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

    def _file_info(self, uid):
        r = self.bucket.list_file_names(uid, 1)
        for entry in r['files']:
            file_version_info = b2.file_version.FileVersionInfoFactory.from_api_response(entry)
            if file_version_info.file_name == uid:
                return file_version_info

        raise FileNotFoundError('UID {} not found.'.format(uid))


    def rm(self, uid):
        try:
            file_version_info = self._file_info(uid)
            self.bucket.delete_file_version(file_version_info.id_, file_version_info.file_name)
        except B2Error as e:
            # Unfortunately
            #if isinstance(e, FileNotPresent) or isinstance(e, UnknownError) and "404 not_found" in str(e):
            if isinstance(e, FileNotPresent):
                raise FileNotFoundError('UID {} not found.'.format(uid))
            else:
                raise e

    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """
        errors = []
        for uid in uids:
            try:
                file_version_info = self._file_info(uid)
                self.bucket.delete_file_version(file_version_info.id_, file_version_info.file_name)
            except (B2Error, FileNotFoundError):
                errors.append(uid)

        if len(errors) > 0:
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
        if prefix:
            raise RuntimeError('prefix is not yet implemented for this backend')
        return [file_version_info.file_name
                for (file_version_info, folder_name) in self.bucket.ls()]

    def close(self):
        for _writer_thread in self._writer_threads:
            self._write_queue.put(None)  # ends the thread
        for _writer_thread in self._writer_threads:
            _writer_thread.join()
        for _reader_thread in self._reader_threads:
            self._read_queue.put(None)  # ends the thread
        for _reader_thread in self._reader_threads:
            _reader_thread.join()
