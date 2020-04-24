from Crypto.Cipher import AES
from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Random import get_random_bytes
from functools import partial
import binascii
import json
import zstandard


def get_crypt(version=1):  # Default will always be the latest version.
    """
    Example calls:
    >>> cc = get_crypt()(password='test'))
    >>> config = cc.get_configuration()
    >>> cc2 = get_crypt(version=1).from_configuration(password='test', configuration=config)

    Complete usage:
    >>> from backy2.crypt import get_crypt
    Backup:
    >>> cc = get_crypt()(password='from config')
    >>> blob = cc.encrypt(b'my block data')
    >>> # store blob
    >>> # store cc.get_configuration()
    >>> # store cc.VERSION

    Restore:
    >>> # load version, config from database
    >>> cc = get_crypt(version=1).from_configuration(password='from config', configuration=config)
    >>> # load blob
    >>> data = cc.decrypt(blob)
    """
    if version == 0:
        return NoCrypt
    if version == 1:
        return CryptV1


class CryptBase:
    def __init__(self, password):
        pass

    def get_configuration(self):
        """ Returns a configuration bytestring to be used later with get_crypt
        """
        return b''

    @classmethod
    def from_configuration(cls, password, configuration):
        return cls(password)

    def encrypt(self, data):
        return data

    def decrypt(self, blob):
        return blob


class NoCrypt(CryptBase):
    pass


class CryptV1(CryptBase):
    """ Initialize with a password and encrypt data. This lib also compresses
    data before it encrypts it.
    After encryption save the salt together with your data (salt is not secret)
    and initialize the class with this salt and the same password for decryption.
    The decrypt method also checks if the decryption with this salt and password
    succeeded by checking it to a digest, stored along with the data.

    The methods encrypt and decrypt are threadsafe.
    """
    VERSION = 1

    def __init__(self, password, salt=None, compression_level=1, iterations=241158):
        if salt is None:
            salt = get_random_bytes(16)
            self.salt = salt  # SAVE THIS!
        self.key = PBKDF2(password=password, salt=salt, dkLen=32, count=iterations, hmac_hash_module=SHA512)
        self.compression_level = compression_level
        self.iterations = iterations
        self.cctx = zstandard.ZstdCompressor(level=compression_level)  # zstandard.MAX_COMPRESSION_LEVEL
        self.dctx = zstandard.ZstdDecompressor()


    def get_configuration(self):
        """ Returns a configuration bytestring to be used later with get_crypt
        """
        return json.dumps({'version': 1,
                'salt': binascii.hexlify(self.salt).decode('ascii'),
                'compression_level': self.compression_level,
                'iterations': self.iterations,
                })


    @classmethod
    def from_configuration(cls, password, configuration):
        _c = json.loads(configuration)
        cc = cls(
                password=password,
                salt=binascii.unhexlify(_c['salt'].encode('ascii')),
                compression_level=_c['compression_level'],
                iterations=_c['iterations'],
                )
        return cc


    def _compress(self, data):
        return self.cctx.compress(data)


    def _decompress(self, compressed):
        return self.dctx.decompress(compressed)


    def _pack(self, data, nonce, digest):
        assert len(nonce) == 16
        assert len(digest) == 16
        return digest + nonce + data


    def _unpack(self, blob):
        assert len(blob) > 32
        # digest, nonce, data
        return blob[0:16], blob[16:32], blob[32:]


    def encrypt(self, data):
        # compress data, then encrypt it
        data = self._compress(data)
        encryptor = AES.new(self.key, AES.MODE_GCM)
        nonce = encryptor.nonce
        encrypted_data, digest = encryptor.encrypt_and_digest(data)
        return self._pack(encrypted_data, nonce, digest)


    def decrypt(self, blob):
        # decrypt data, then uncompress it
        digest, nonce, encrypted_data = self._unpack(blob)
        decryptor = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
        data = decryptor.decrypt_and_verify(encrypted_data, digest)
        return self._decompress(data)

