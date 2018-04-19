#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import socket
import time

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from backy2.data_backends import DataBackend as _DataBackend
from backy2.logging import logger


class DataBackend(_DataBackend):
    """ A DataBackend which stores in S3 compatible storages. The files are
    stored in a configurable bucket. """

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    SUPPORTS_PARTIAL_READS = False
    SUPPORTS_PARTIAL_WRITES = False
    SUPPORTS_METADATA = True

    def __init__(self, config):

        super().__init__(config)

        aws_access_key_id = config.get('aws_access_key_id')
        aws_secret_access_key = config.get('aws_secret_access_key')
        host = config.get('host')
        port = config.getint('port')
        is_secure = config.getboolean('is_secure')
        bucket_name = config.get('bucket_name', 'backy2')

        self.multi_delete = config.getboolean('multi_delete', True)

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

    def _write_raw(self, uid, data, metadata):
        object = self.bucket.Object(uid)
        object.put(Body=data, Metadata=metadata)

    def _read_raw(self, uid, offset=0, length=None):
        object = self.bucket.Object(uid)
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

    def get_all_blob_uids(self, prefix=None):
        if prefix is None:
            return [object.key for object in self.bucket.objects.filter()]
        else:
            return [object.key for object in self.bucket.objects.filter(Prefix=prefix)]
