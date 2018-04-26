import unittest

from . import DatabackendTestCase


class test_s3_boto3(DatabackendTestCase, unittest.TestCase):
    CONFIG = """
        compression: backy2.data_backends.compression.zstd
        compression_default: zstd
        encryption: backy2.data_backends.encryption.aws_s3_cse
        encryption_materials: {{"MasterKey": "0000000000000000"}}
        encryption_default: aws_s3_cse
        configurationVersion: '0.1'
        logFile: /dev/stderr
        lockDirectory: {testpath}/lock
        hashFunction: blake2b,digest_size=32
        dataBackend:
          type: s3_boto3
          s3_boto3:
            awsAccessKeyId: minio
            awsSecretAccessKey: minio123
            host: 127.0.0.1
            port: 9901
            isSecure: False
            bucketName: backy2
            multiDelete: true
            
            compression:
              - name: zstd
                materials:
                  level: 1
                active: true
                  
            encryption:
              - name: aws_s3_cse
                materials:
                  masterKey: !!binary |
                    e/i1X4NsuT9k+FIVe2kd3vtHVkzZsbeYv35XQJeV8nA=
                active: true

          simultaneousWrites: 1
          simultaneousReads: 1
          bandwidthRead: 0
          bandwidthWrite: 0           
        """
if __name__ == '__main__':
    unittest.main()
