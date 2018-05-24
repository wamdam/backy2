#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import logging
import random
import time

import b2
import b2.api
import b2.file_version
from b2.account_info.exception import MissingAccountData
from b2.account_info.in_memory import InMemoryAccountInfo
from b2.account_info.sqlite_account_info import SqliteAccountInfo
from b2.download_dest import DownloadDestBytes
from b2.exception import B2Error, FileNotPresent, UnknownError
from backy2.data_backends import ReadCacheDataBackend
from backy2.logging import logger


class DataBackend(ReadCacheDataBackend):
    """ A DataBackend which stores its data in a BackBlaze (B2) file store."""

    NAME = 'b2'

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    def __init__(self, config):
        super().__init__(config)

        our_config = config.get('dataBackend.{}'.format(self.NAME), types=dict)
        account_id = config.get_from_dict(our_config, 'accountId', types=str)
        application_key = config.get_from_dict(our_config, 'applicationKey', types=str)
        bucket_name = config.get_from_dict(our_config, 'bucketName', types=str)

        account_info_file = config.get_from_dict(our_config, 'accountInfoFile', None, types=str)
        if account_info_file is not None:
            account_info = SqliteAccountInfo(file_name=account_info_file)
        else:
            account_info = InMemoryAccountInfo()

        b2.bucket.Bucket.MAX_UPLOAD_ATTEMPTS  = config.get_from_dict(our_config, 'uploadAttempts', types=int,
                                                                     check_func=lambda v: v >= 1,
                                                                     check_message='Must be a positive integer')

        self._write_object_attempts = config.get_from_dict(our_config, 'writeObjectAttempts', types=int,
                                                           check_func=lambda  v: v >= 1,
                                                           check_message='Must be a positive integer')

        self._read_object_attempts = config.get_from_dict(our_config, 'readObjectAttempts', types=int,
                                                           check_func=lambda  v: v >= 1,
                                                           check_message='Must be a positive integer')

        self.service = b2.api.B2Api(account_info)
        if account_info_file is not None:
            try:
                # This temporarily disables all logging as the b2 library does some very verbose logging
                # of the exception we're trying to catch here...
                logging.disable(logging.ERROR)
                _ = self.service.get_account_id()
                logging.disable(logging.NOTSET)
            except MissingAccountData:
                self.service.authorize_account('production', account_id, application_key)
        else:
            self.service.authorize_account('production', account_id, application_key)
            
        self.bucket = self.service.get_bucket_by_name(bucket_name)

    def _write_object(self, key, data):
        for i in range(self._write_object_attempts):
            try:
                self.bucket.upload_bytes(data, key)
            except B2Error:
                if i + 1 < self._write_object_attempts:
                    sleep_time = (2 ** (i + 1)) + (random.randint(0, 1000) / 1000)
                    logger.warning('Upload of object with key {} to B2 failed repeatedly, will try again in {:.2f} seconds.'
                                   .format(key, sleep_time))
                    time.sleep(sleep_time)
                    continue
                raise
            else:
                break

    def _read_object(self, key):
        for i in range(self._read_object_attempts):
            data_io = DownloadDestBytes()
            try:
                self.bucket.download_file_by_name(key, data_io)
            except B2Error as exception:
                # Currently FileNotPresent isn't always signaled correctly.
                # See: https://github.com/Backblaze/B2_Command_Line_Tool/pull/436
                if isinstance(exception, FileNotPresent) or isinstance(exception, UnknownError) and "404 not_found" in str(exception):
                #if isinstance(exception, FileNotPresent):
                    raise FileNotFoundError('UID {} not found.'.format(key)) from None
                else:
                    if i + 1 < self._read_object_attempts:
                        sleep_time = (2 ** (i + 1)) + (random.randint(0, 1000) / 1000)
                        logger.warning('Download of object with key {} to B2 failed, will try again in {:.2f} seconds.'
                                       .format(key, sleep_time))
                        time.sleep(sleep_time)
                        continue
                    raise
            else:
                break

        return data_io.get_bytes_written()

    def _file_info(self, key):
        r = self.bucket.list_file_names(key, 1)
        for entry in r['files']:
            file_version_info = b2.file_version.FileVersionInfoFactory.from_api_response(entry)
            if file_version_info.file_name == key:
                return file_version_info

        raise FileNotFoundError('Object {} not found.'.format(key))

    def _read_object_length(self, key):
        for i in range(self._read_object_attempts):
            try:
                file_version_info = self._file_info(key)
            except B2Error as exception:
                # Currently FileNotPresent isn't always signaled correctly.
                # See: https://github.com/Backblaze/B2_Command_Line_Tool/pull/436
                if isinstance(exception, FileNotPresent) or isinstance(exception, UnknownError) and "404 not_found" in str(exception):
                #if isinstance(exception, FileNotPresent):
                    raise FileNotFoundError('UID {} not found.'.format(key)) from None
                else:
                    if i + 1 < self._read_object_attempts:
                        sleep_time = (2 ** (i + 1)) + (random.randint(0, 1000) / 1000)
                        logger.warning('Object length request for key {} to B2 failed, will try again in {:.2f} seconds.'
                                       .format(key, sleep_time))
                        time.sleep(sleep_time)
                        continue
                    raise
            else:
                break

        return file_version_info.size

    def _rm_object(self, key):
        try:
            file_version_info = self._file_info(key)
            self.bucket.delete_file_version(file_version_info.id_, file_version_info.file_name)
        except B2Error as exception:
            # Currently FileNotPresent isn't always signaled correctly.
            # See: https://github.com/Backblaze/B2_Command_Line_Tool/pull/436
            if isinstance(exception, FileNotPresent) or isinstance(exception, UnknownError) and "404 not_found" in str(exception):
            #if isinstance(exception, FileNotPresent):
                raise FileNotFoundError('Object {} not found.'.format(key)) from None
            else:
                raise

    def _rm_many_objects(self, keys):
        """ Deletes many keys from the data backend and returns a list
        of keys that couldn't be deleted.
        """
        errors = []
        for key in keys:
            try:
                file_version_info = self._file_info(key)
                self.bucket.delete_file_version(file_version_info.id_, file_version_info.file_name)
            except (B2Error, FileNotFoundError):
                errors.append(key)
        return errors

    def _list_objects(self, prefix=''):
        return [file_version_info.file_name for (file_version_info, folder_name) in
                                                            self.bucket.ls(folder_to_list=prefix, recursive=True)]

