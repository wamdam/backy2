#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger
import boto.exception
import boto.s3.connection
import hashlib
import os
import time
import uuid


class DataBackend(_DataBackend):
    """ A DataBackend which stores in S3 compatible storages. The files are
    stored in a configurable bucket. """

    def __init__(self, config):
        aws_access_key_id = config.get('aws_access_key_id')
        aws_secret_access_key = config.get('aws_secret_access_key')
        host = config.get('host')
        port = config.getint('port')
        is_secure = config.getboolean('is_secure')
        bucket_name = config.get('bucket_name', 'backy2')
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
            self.bucket = self.conn.create_bucket(bucket_name)
        except boto.exception.S3CreateError:
            # exists...
            pass
        except OSError as e:
            # no route to host
            logger.error('Fatal error, dying: {}'.format(e))
            print('Fatal error: {}'.format(e))
            exit(12)


    def _uid(self):
        # a uuid always starts with the same bytes, so let's widen this
        return hashlib.md5(uuid.uuid1().bytes).hexdigest()


    def save(self, data):
        uid = self._uid()
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
            logger.error('Fatal error, dying: {}'.format(e))
            #exit('Fatal error: {}'.format(e))  # this only raises SystemExit
            os._exit(13)
        t2 = time.time()
        assert r == len(data)
        logger.debug('Wrote data uid {} in {:.2f}s'.format(uid, t2-t1))
        return uid


    def rm(self, uid):
        key = self.bucket.get_key(uid)
        if not key:
            raise FileNotFoundError('UID {} not found.'.format(uid))
        self.bucket.delete_key(uid)


    def read(self, uid):
        key = self.bucket.get_key(uid)
        if not key:
            raise FileNotFoundError('UID {} not found.'.format(uid))
        return key.get_contents_as_string()


    def get_all_blob_uids(self, prefix=None):
        return [k.name for k in self.bucket.list(prefix)]


    def close(self):
        self.conn.close()


