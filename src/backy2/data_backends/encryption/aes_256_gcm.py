import base64

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

from aes_keywrap import aes_wrap_key, aes_unwrap_key


# This implements envelope encryption with AES-256 in GCM mode. The envelope key is wrapped with AESWrap.
class Encryption:

    NAME = 'aes_256_gcm'

    def __init__(self, materials):
        if 'masterKey' not in materials:
            raise KeyError('Required key masterKey is missing in encryption materials.')

        if len(materials['masterKey']) != 32:
            raise ValueError('Key masterKey has the wrong length. It must be 32 bytes long.')

        self._master_key = materials['masterKey']

    def encrypt(self, data):
        envelope_key = get_random_bytes(32)
        envelope_iv = get_random_bytes(16)
        encryptor = AES.new(envelope_key, AES.MODE_GCM, nonce=envelope_iv)

        envelope_key = aes_wrap_key(self._master_key, envelope_key)

        materials = {
            'envelope_key': base64.b64encode(envelope_key).decode('ascii'),
            'iv': base64.b64encode(envelope_iv).decode('ascii'),
        }

        return encryptor.encrypt(data), materials

    def decrypt(self, data, materials):

        if 'envelope_key' not in materials:
            raise KeyError('Encryption materials are missing required key envelope_key.')
        envelope_key = materials['envelope_key']

        if 'iv' not in materials:
            raise KeyError('Encryption materials are missing required key iv.')
        iv = materials['iv']

        envelope_key = base64.b64decode(envelope_key)
        iv = base64.b64decode(iv)

        if len(iv) != 16:
            raise ValueError('Encryption materials IV iv has the wrong length of {}. It must be 16 bytes long.'
                             .format(len(iv)))

        envelope_key = aes_unwrap_key(self._master_key, envelope_key)
        if len(envelope_key) != 32:
            raise ValueError('Encryption materials key envelope_key the has wrong length of {}. It must be 32 bytes long.'
                     .format(len(envelope_key)))

        decryptor = AES.new(envelope_key, AES.MODE_GCM, nonce=iv)
        return decryptor.decrypt(data)

