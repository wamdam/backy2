import unittest

from . import DatabackendTestCase


class test_s3_boto3(DatabackendTestCase, unittest.TestCase):
    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        hashFunction: blake2b,digest_size=32
        dataBackend:
          type: s3
          s3:
            awsAccessKeyId: minio
            awsSecretAccessKey: minio123
            endpointUrl: http://127.0.0.1:9901/
            bucketName: benji
            multiDelete: true
            addressingStyle: path
            disableEncodingType: true
            activeCompression: zstd
            activeEncryption: k1
            
          compression:
            - type: zstd
              materials:
                level: 1
                  
          encryption:
            - identifier: k1
              type: aes_256_gcm
              materials:
                masterKey: !!binary |
                  e/i1X4NsuT9k+FIVe2kd3vtHVkzZsbeYv35XQJeV8nA=
            - identifier: k2
              type: aes_256_gcm
              materials:
                kdfSalt: !!binary CPJlYMjRjfbXWOcqsE309A==
                kdfIterations: 20000
                password: "this is a very secret password"
                
          consistencyCheckWrites: True
          simultaneousWrites: 5
          simultaneousReads: 5
          bandwidthRead: 0
          bandwidthWrite: 0           
        """
if __name__ == '__main__':
    unittest.main()
