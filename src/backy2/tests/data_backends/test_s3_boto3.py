import unittest

from . import DatabackendTestCase


class test_s3_boto3(DatabackendTestCase, unittest.TestCase):
    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        lockDirectory: {testpath}/lock
        hashFunction: blake2b,digest_size=32
        dataBackend:
          type: s3_boto3
          s3_boto3:
            awsAccessKeyId: minio
            awsSecretAccessKey: minio123
            endpointUrl: http://127.0.0.1:9901/
            bucketName: backy2
            multiDelete: true
            addressingStyle: path
            disableEncodingType: true
            
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
                
            consistencyCheckWrites: True
          simultaneousWrites: 5
          simultaneousReads: 5
          bandwidthRead: 0
          bandwidthWrite: 0           
        """
if __name__ == '__main__':
    unittest.main()
