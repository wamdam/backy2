from aws_s3_cse import CSE

class Encryption:

    NAME = 'aws_s3_cse'

    def __init__(self, materials):

        if 'MasterKey' not in materials:
            raise KeyError('required field MasterKey is missing in encryption materials')
        master_key = materials['MasterKey']

        self.delegate = CSE(MasterKey=master_key)

    def encrypt(self, data):
        return self.delegate.encrypt_object(data)

    def decrypt(self, data, metadata):
        return self.delegate.decrypt_object(data, metadata)