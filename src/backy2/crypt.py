from Crypto.Cipher import AES
from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Random import get_random_bytes
from functools import partial
from backy2.aes_keywrap import aes_wrap_key, aes_unwrap_key
from threading import Lock
import binascii
import json
import zstandard


def get_crypt(version=1):  # Default will always be the latest version.
    """
    Complete usage:
    >>> from backy2.crypt import get_crypt
    Backup:
    >>> cc = get_crypt()(key=b'\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad')
    >>> blob, envelope_key, nonce = cc.encrypt(b'my block data')
    >>> # store blob to disk (blob consists of data, nonce and digest. data is encrypted, nonce and digest are not secret)
    >>> # store envelope_key to blob's metadata (envelope_key is not secret)
    >>> # store cc.VERSION

    Restore:
    >>> # load version, config from database
    >>> cc2 = get_crypt(cc.VERSION)(key=b'\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad')
    >>> data = cc.decrypt(blob, envelope_key)

    Re-Key:
    >>> old_key = b'\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad\xde\xca\xfb\xad'
    >>> new_key = b'\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef\xde\xad\xbe\xef'
    >>> cc3 = get_crypt(cc.VERSION)(key=new_key)  # New key!
    >>> cc3.wrap_key(cc3.unwrap_key(envelope_key, old_key))
    """
    if version == 0:
        return NoCrypt
    if version == 1:
        return CryptV1


class CryptBase:
    VERSION = 0

    def __init__(self, key):
        pass

    def get_configuration(self):
        """ Returns a configuration bytestring to be used later with get_crypt
        """
        return b''

    @classmethod
    def from_configuration(cls, password, configuration):
        return cls(password)

    def encrypt(self, data):
        return data, None, None  # blob, envkey, nonce

    def decrypt(self, blob, envelope_key=b''):
        return blob


class NoCrypt(CryptBase):
    pass


# module wide lock because zstandard CANNOT be used from
# multiple threads simultaniously.
crypt_v1_zstandard_lock = Lock()

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

    def __init__(self, key, compression_level=1):
        if len(key) != 32:
            raise ValueError('You must provide a 32-byte long encryption-key in your configuration.')
        self.key = key
        self.compression_level = compression_level
        self.cctx = zstandard.ZstdCompressor(level=compression_level)  # zstandard.MAX_COMPRESSION_LEVEL
        self.dctx = zstandard.ZstdDecompressor()


    def _compress(self, data):
        with crypt_v1_zstandard_lock:
            return self.cctx.compress(data)


    def _decompress(self, compressed):
        with crypt_v1_zstandard_lock:
            return self.dctx.decompress(compressed)


    def _pack(self, data, nonce, digest):
        assert len(nonce) == 16
        assert len(digest) == 16
        return digest + nonce + data


    def _unpack(self, blob):
        assert len(blob) > 32
        # digest, nonce, data
        return blob[0:16], blob[16:32], blob[32:]


    def wrap_key(self, key):
        return aes_wrap_key(self.key, key)


    def unwrap_key(self, wrapped_key, master_key=None):
        if not master_key:
            master_key = self.key
        return aes_unwrap_key(master_key, wrapped_key)


    def encrypt(self, data, data_key=None, nonce=None):
        # compress data, then encrypt it
        data = self._compress(data)

        # encrypt data with a new key
        if data_key is None:  # for weak encryption in null data backend.
            data_key = get_random_bytes(32)
        if nonce is None:  # for weak encryption in null data backend.
            encryptor = AES.new(data_key, AES.MODE_GCM)
            nonce = encryptor.nonce
        else:
            encryptor = AES.new(data_key, AES.MODE_GCM, nonce=nonce)
        encrypted_data, digest = encryptor.encrypt_and_digest(data)

        envelope_key = self.wrap_key(data_key)

        # We return one blob with encrypted_data, nonce and digest
        # and the envelope_key which is the key the data was stored
        # with wrapped by the key from the config.

        return self._pack(encrypted_data, nonce, digest), envelope_key, nonce


    def decrypt(self, blob, envelope_key):
        # decrypt data, then uncompress it
        digest, nonce, encrypted_data = self._unpack(blob)

        data_key = self.unwrap_key(envelope_key)

        decryptor = AES.new(data_key, AES.MODE_GCM, nonce=nonce)
        data = decryptor.decrypt_and_verify(encrypted_data, digest)
        data = self._decompress(data)
        return data

