from aws_s3_cse import CSE


class Encryption:

    NAME = 'aws_s3_cse'

    def __init__(self, materials):
        if 'masterKey' not in materials:
            raise KeyError('Required key masterKey is missing in encryption materials.')

        self.delegate = CSE(master_key=materials['masterKey'])

    def encrypt(self, data):
        return self.delegate.encrypt_object(data)

    def decrypt(self, data, metadata):
        return self.delegate.decrypt_object(data, metadata)