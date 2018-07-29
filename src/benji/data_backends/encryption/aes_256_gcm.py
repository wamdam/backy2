import base64

try:
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
except ImportError:
    raise NotImplementedError('Python module "pycryptodome" is required to enable encryption support.')

from aes_keywrap import aes_wrap_key, aes_unwrap_key
# This implements envelope encryption with AES-256 in GCM mode. The envelope key is wrapped with AESWrap.
from benji.config import Config
from benji.data_backends.encryption.utils import derive_key


class Encryption:

    NAME = 'aes_256_gcm'

    def __init__(self, *, identifier, materials):
        master_key = Config.get_from_dict(materials, 'masterKey', None, types=bytes)
        if master_key is not None:
            if len(master_key) != 32:
                raise ValueError('Key masterKey has the wrong length. It must be 32 bytes long.')

            self._master_key = master_key
        else:
            kdfSalt = Config.get_from_dict(materials, 'kdfSalt', types=bytes)
            kdfIterations = Config.get_from_dict(materials, 'kdfIterations', types=int)
            password = Config.get_from_dict(materials, 'password', types=str)

            self._master_key = derive_key(salt=kdfSalt, iterations=kdfIterations, key_length=32, password=password)

        self._identifier = identifier

    @property
    def identifier(self):
        return self._identifier

    def encrypt(self, *, data):
        envelope_key = get_random_bytes(32)
        envelope_iv = get_random_bytes(16)
        encryptor = AES.new(envelope_key, AES.MODE_GCM, nonce=envelope_iv)

        envelope_key = aes_wrap_key(self._master_key, envelope_key)

        materials = {
            'envelope_key': base64.b64encode(envelope_key).decode('ascii'),
            'iv': base64.b64encode(envelope_iv).decode('ascii'),
        }

        return encryptor.encrypt(data), materials

    def decrypt(self, *, data, materials):

        for key in ['envelope_key', 'iv']:
            if key not in materials:
                raise KeyError('Encryption materials are missing required key {}.'.format(key))

        envelope_key = materials['envelope_key']
        iv = materials['iv']

        envelope_key = base64.b64decode(envelope_key)
        iv = base64.b64decode(iv)

        if len(iv) != 16:
            raise ValueError('Encryption materials IV iv has wrong length of {}. It must be 16 bytes long.'.format(
                len(iv)))

        envelope_key = aes_unwrap_key(self._master_key, envelope_key)
        if len(envelope_key) != 32:
            raise ValueError('Encryption materials key envelope_key has wrong length of {}. It must be 32 bytes long.'
                             .format(len(envelope_key)))

        decryptor = AES.new(envelope_key, AES.MODE_GCM, nonce=iv)
        return decryptor.decrypt(data)
