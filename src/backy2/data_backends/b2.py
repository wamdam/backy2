#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import logging

from b2.account_info.exception import MissingAccountData
from backy2.data_backends import DataBackend as _DataBackend

global b2
import b2
import b2.api
from b2.account_info.in_memory import InMemoryAccountInfo
from b2.account_info.sqlite_account_info import SqliteAccountInfo
from b2.download_dest import DownloadDestBytes
import b2.file_version
from b2.exception import B2Error, FileNotPresent


class DataBackend(_DataBackend):
    """ A DataBackend which stores its data in a BackBlaze (B2) file store."""

    NAME = 'b2'

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    SUPPORTS_PARTIAL_READS = False
    SUPPORTS_PARTIAL_WRITES = False
    SUPPORTS_METADATA = True

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

    def _write_raw(self, uid, data, metadata):
        self.bucket.upload_bytes(data, uid, file_infos=metadata)

    def _read_raw(self, uid, offset=0, length=None):
        data_io = DownloadDestBytes()
        try:
            self.bucket.download_file_by_name(uid, data_io)
        except B2Error as e:
            #if isinstance(e, FileNotPresent) or isinstance(e, UnknownError) and "404 not_found" in str(e):
            if isinstance(e, FileNotPresent):
                raise FileNotFoundError('UID {} not found.'.format(uid))
            else:
                raise e

        return data_io.get_bytes_written(), data_io.file_info

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

    def get_all_blob_uids(self, prefix=None):
        if prefix:
            raise RuntimeError('prefix is not yet implemented for this backend')
        return [file_version_info.file_name
                for (file_version_info, folder_name) in self.bucket.ls()]

