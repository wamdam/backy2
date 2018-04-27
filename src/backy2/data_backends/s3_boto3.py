#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import socket
import threading
import time
from itertools import islice

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger


class DataBackend(_DataBackend):
    """ A DataBackend which stores in S3 compatible storages. The files are
    stored in a configurable bucket. """

    NAME = 's3_boto3'

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    SUPPORTS_PARTIAL_READS = False
    SUPPORTS_PARTIAL_WRITES = False
    SUPPORTS_METADATA = True

    def __init__(self, config):

        our_config = config.get('dataBackend.{}'.format(self.NAME), types=dict)
        aws_access_key_id = config.get_from_dict(our_config, 'awsAccessKeyId', types=str)
        aws_secret_access_key = config.get_from_dict(our_config, 'awsSecretAccessKey', types=str)
        host = config.get_from_dict(our_config, 'host', types=str)
        port = config.get_from_dict(our_config, 'port', types=int)
        is_secure = config.get_from_dict(our_config, 'isSecure', types=bool)
        
        self._bucket_name = config.get_from_dict(our_config, 'bucketName', types=str)
        self.multi_delete = config.get_from_dict(our_config, 'multiDelete', types=bool)

        self._resource_config = {
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
            'endpoint_url': 'http{}://{}:{}'.format('s' if is_secure else '', host, port),
            'config': Config(s3={'addressing_style': 'path'})
        }

        self._local = threading.local()
        self._local.resource = boto3.session.Session().resource('s3', **self._resource_config)

        # create our bucket
        exists = True
        try:
            self._local.resource.meta.client.head_bucket(Bucket=self._bucket_name)
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
                self._local.resource.create_bucket(Bucket=self._bucket_name)
            except (
                    OSError,
                    ClientError,
            ) as e:
                self.fatal_error = e
                logger.error('Fatal error, dying: {}'.format(e))
                print('Fatal error: {}'.format(e))
                exit(10)

        self._local.bucket = self._local.resource.Bucket(self._bucket_name)

        super().__init__(config)

    def _init_connection(self):
        if not hasattr(self._local, 'resource'):
            logger.debug('Initializing S3 session and resource for {}'.format(threading.current_thread().name))
            self._local.resource = boto3.session.Session().resource('s3', **self._resource_config)
            self._local.bucket = self._local.resource.Bucket(self._bucket_name)

    def _write_raw(self, uid, data, metadata):
        self._init_connection()
        object = self._local.bucket.Object(uid)
        object.put(Body=data, Metadata=metadata)

    def _read_raw(self, uid, offset=0, length=None):
        self._init_connection()
        object = self._local.bucket.Object(uid)
        while True:
            try:
                object = object.get()
                data = object['Body'].read()
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    raise FileNotFoundError('UID {} not found.'.format(uid))
                else:
                    raise e
            except socket.timeout:
                logger.error('Timeout while fetching from s3, trying again.')
                pass
            else:
                break
        time.sleep(self.read_throttling.consume(len(data)))

        return data, object['Metadata']

    def rm(self, uid):
        self._init_connection()
        # delete() always returns 204 even when key doesn't exist, so check for existence
        object = self._local.bucket.Object(uid)
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
        self._init_connection()
        if self.multi_delete:
            # Amazon (at least) only handles 1000 deletes at a time
            # Split list into parts of at most 1000 elements
            uids_parts = [islice(uids, i, i+1000) for i  in range(0, len(uids), 1000)]

            errors = []
            for part in uids_parts:
                response = self._local.resource.meta.client.delete_objects(
                    Bucket=self._local.bucket.name,
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
                    self._local.bucket.Object(uid).delete()
                except ClientError as e:
                    errors.append(uid)

        if len(errors) > 0:
            # unable to test this. ceph object gateway doesn't return errors.
            # raise FileNotFoundError('UIDS {} not found.'.format(errors))
            return errors

    def get_all_blob_uids(self, prefix=None):
        self._init_connection()
        if prefix is None:
            return [object.key for object in self._local.bucket.objects.filter()]
        else:
            return [object.key for object in self._local.bucket.objects.filter(Prefix=prefix)]
