#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import boto.s3.connection

conn = boto.connect_s3(
    aws_access_key_id='',
    aws_secret_access_key='',
    host='127.0.0.1',
    port=10001,
    is_secure=False,
    calling_format=boto.s3.connection.OrdinaryCallingFormat()
    )

conn.create_bucket('test')

for bucket in conn.get_all_buckets():
    print("{name}\t{created}".format(
        name = bucket.name,
        created = bucket.creation_date,
        ))

bucket = conn.get_bucket('test')

key = bucket.new_key('hello.txt')

key.set_contents_from_string('Hello World!')
#12

for key in bucket.list():
    print("{name}\t{size}\t{modified}".format(
        name = key.name,
        size = key.size,
        modified = key.last_modified,
        ))

#hello.txt       12      2015-11-24T22:09:49.000Z

key = bucket.get_key('hello.txt')
key.get_contents_to_filename('/tmp/asd')
key.get_contents_as_string()
#Out[29]: b'Hello World!'

bucket.delete_key('hello.txt')


# also see http://docs.ceph.com/docs/v0.80/radosgw/s3/python/
