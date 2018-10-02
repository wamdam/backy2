#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import threading
from itertools import islice

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.handlers import set_list_objects_encoding_type_url

from benji.data_backends import ReadCacheDataBackend
from benji.logging import logger


class DataBackend(ReadCacheDataBackend):
    """ A DataBackend which stores in S3 compatible storages. The files are
    stored in a configurable bucket. """

    NAME = 's3'

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    def __init__(self, config):

        our_config = config.get('dataBackend.{}'.format(self.NAME), types=dict)
        aws_access_key_id = config.get_from_dict(our_config, 'awsAccessKeyId', types=str)
        aws_secret_access_key = config.get_from_dict(our_config, 'awsSecretAccessKey', types=str)
        region_name = config.get_from_dict(our_config, 'regionName', None, types=str)
        endpoint_url = config.get_from_dict(our_config, 'endpointUrl', None, types=str)
        use_ssl = config.get_from_dict(our_config, 'useSsl', None, types=bool)
        addressing_style = config.get_from_dict(our_config, 'addressingStyle', None, types=str)
        signature_version = config.get_from_dict(our_config, 'signatureVersion', None, types=str)

        self._bucket_name = config.get_from_dict(our_config, 'bucketName', types=str)
        self._multi_delete = config.get_from_dict(our_config, 'multiDelete', types=bool)
        self._disable_encoding_type = config.get_from_dict(our_config, 'disableEncodingType', types=bool)

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

        self._resource_config['config'] = Config(**resource_config)

        self._local = threading.local()
        self._init_connection()

        # create our bucket
        exists = True
        try:
            self._local.resource.meta.client.head_bucket(Bucket=self._bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket' or e.response['Error']['Code'] == '404':
                # Doesn't exists...
                exists = False
            else:
                raise

        if not exists:
            self._local.resource.create_bucket(Bucket=self._bucket_name)

        self._local.bucket = self._local.resource.Bucket(self._bucket_name)

        super().__init__(config)

    def _init_connection(self):
        if not hasattr(self._local, 'session'):
            logger.debug('Initializing S3 session and resource for {}'.format(threading.current_thread().name))
            self._local.session = boto3.session.Session()
            if self._disable_encoding_type:
                self._local.session.events.unregister('before-parameter-build.s3.ListObjects',
                                                      set_list_objects_encoding_type_url)
            self._local.resource = self._local.session.resource('s3', **self._resource_config)
            self._local.bucket = self._local.resource.Bucket(self._bucket_name)

    def _write_object(self, key, data):
        self._init_connection()
        object = self._local.bucket.Object(key)
        object.put(Body=data)

    def _read_object(self, key):
        self._init_connection()
        object = self._local.bucket.Object(key)
        try:
            data_dict = object.get()
            data = data_dict['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                raise FileNotFoundError('Key {} not found.'.format(key)) from None
            else:
                raise

        return data

    def _read_object_length(self, key):
        self._init_connection()
        object = self._local.bucket.Object(key)
        try:
            object.load()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                raise FileNotFoundError('Key {} not found.'.format(key)) from None
            else:
                raise

        return object.content_length

    def _rm_object(self, key):
        self._init_connection()
        # delete() always returns 204 even when key doesn't exist, so check for existence
        object = self._local.bucket.Object(key)
        try:
            object.load()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                raise FileNotFoundError('Key {} not found.'.format(key)) from None
            else:
                raise
        else:
            object.delete()

    def _rm_many_objects(self, keys):
        """ Deletes many keys from the data backend and returns a list
        of keys that couldn't be deleted.
        """
        self._init_connection()
        errors = []
        if self._multi_delete:
            # Amazon (at least) only handles 1000 deletes at a time
            # Split list into parts of at most 1000 elements
            keys_parts = [islice(keys, i, i + 1000) for i in range(0, len(keys), 1000)]
            for part in keys_parts:
                response = self._local.resource.meta.client.delete_objects(
                    Bucket=self._local.bucket.name, Delete={
                        'Objects': [{
                            'Key': key
                        } for key in part],
                    })
                if 'Errors' in response:
                    errors += list(map(lambda object: object['Key'], response['Errors']))
        else:
            for key in keys:
                try:
                    self._local.bucket.Object(key).delete()
                except ClientError:
                    errors.append(key)
        return errors

    def _list_objects(self, prefix=None):
        self._init_connection()
        if prefix is None:
            return [object.key for object in self._local.bucket.objects]
        else:
            return [object.key for object in self._local.bucket.objects.filter(Prefix=prefix)]
