#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import socket

import boto.exception
import boto.s3.connection

from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger


class DataBackend(_DataBackend):
    """ A DataBackend which stores in S3 compatible storages. The files are
    stored in a configurable bucket. """

    NAME = 's3'

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    SUPPORTS_PARTIAL_READS = False
    SUPPORTS_PARTIAL_WRITES = False
    SUPPORTS_METADATA = False

    def __init__(self, config):

        super().__init__(config)

        our_config = config.get('dataBackend.{}'.format(self.NAME), types=dict)
        aws_access_key_id = config.get_from_dict(our_config, 'awsAccessKeyId', types=str)
        aws_secret_access_key = config.get_from_dict(our_config, 'awsSecretAccessKey', types=str)
        host = config.get_from_dict(our_config, 'host', types=str)
        port = config.get_from_dict(our_config, 'port', types=int)
        is_secure = config.get_from_dict(our_config, 'isSecure', types=bool)
        bucket_name = config.get_from_dict(our_config, 'bucketName', types=str)
        calling_format=boto.s3.connection.OrdinaryCallingFormat()

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
            self.conn.create_bucket(bucket_name)
        except boto.exception.S3CreateError:
            # exists...
            pass

        self.bucket = self.conn.get_bucket(bucket_name)

    def _write_raw(self, uid, data, metadata):
        key = self.bucket.new_key(uid)
        r = key.set_contents_from_string(data)
        assert r == len(data)
        # OSError happens when the S3 host is gone (i.e. network died,
        # host down, ...). boto tries hard to recover, however after
        # several attempts it will give up and raise.
        # BotoServerError happens, when there is no server.
        # S3ResponseError sometimes happens, when the cluster is about
        # to shutdown. Hard to reproduce because the writer must write
        # in exactly this moment.

    def _read_raw(self, uid, offset=0, length=None):
        key = self.bucket.get_key(uid)
        if not key:
            raise FileNotFoundError('UID {} not found.'.format(uid))
        while True:
            try:
                data = key.get_contents_as_string()
            except socket.timeout:
                logger.error('Timeout while fetching from s3, trying again.')
                pass
            else:
                break
        return data, {}

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

    def get_all_blob_uids(self, prefix=None):
        return [k.name for k in self.bucket.list(prefix)]

    def close(self):
        super().close()
        self.conn.close()


