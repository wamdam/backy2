#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import hashlib
import socket

import boto.exception
import boto.s3.connection

from backy2.data_backends import ReadCacheDataBackend
from backy2.logging import logger
from backy2.meta_backends.sql import BlockUid


class DataBackend(ReadCacheDataBackend):
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

    @staticmethod
    def _block_uid_to_key(block_uid):
        key_name = '{:016x}-{:016x}'.format(block_uid.left, block_uid.right)
        hash = hashlib.md5(key_name.encode('ascii')).hexdigest()
        return '{}/{}/{}-{}'.format(hash[0:2], hash[2:4], hash[:8], key_name)

    @staticmethod
    def _key_to_block_uid(key):
        if len(key) != 48:
            raise RuntimeError('Invalid key name {}'.format(key))
        return BlockUid(int(key[15:15 + 16], 16), int(key[32:32 + 16], 16))

    def _write_object(self, key, data, metadata):
        key_obj = self.bucket.new_key(key)
        r = key_obj.set_contents_from_string(data)
        assert r == len(data)
        # OSError happens when the S3 host is gone (i.e. network died,
        # host down, ...). boto tries hard to recover, however after
        # several attempts it will give up and raise.
        # BotoServerError happens, when there is no server.
        # S3ResponseError sometimes happens, when the cluster is about
        # to shutdown. Hard to reproduce because the writer must write
        # in exactly this moment.

    def _read_object(self, key, offset=0, length=None):
        key_obj = self.bucket.get_key(key)
        if not key_obj:
            raise FileNotFoundError('Key {} not found.'.format(key))
        while True:
            try:
                data = key_obj.get_contents_as_string()
            except socket.timeout:
                logger.error('Timeout while fetching from s3, trying again.')
                pass
            else:
                break
        return data, {}

    def _rm_object(self, key):
        key = self.bucket.get_key(key)
        if not key:
            raise FileNotFoundError('Key {} not found.'.format(key))
        self.bucket.delete_key(key)

    def _rm_many_objects(self, keys):
        """ Deletes many keys from the data backend and returns a list
        of keys that couldn't be deleted.
        """
        errors = self.bucket.delete_keys(keys, quiet=True)
        return errors.errors

    def _list_objects(self, prefix=None):
        return [k.name for k in self.bucket.list(prefix)]

    def close(self):
        super().close()
        self.conn.close()


