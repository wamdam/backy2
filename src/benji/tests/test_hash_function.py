import unittest

from benji.utils import parametrized_hash_function


class HashTestCase(unittest.TestCase):

    def test_sha512(self):
        hash_function = parametrized_hash_function('sha512')
        hash_function.update(b'test123')
        self.assertEqual('daef4953b9783365cad6615223720506cc46c5167cd16ab500fa597aa08ff964eb24fb19687f34d7665f778fcb6c5358fc0a5b81e1662cf90f73a2671c53f991', hash_function.hexdigest())

    def test_blake2_16(self):
        hash_function = parametrized_hash_function('blake2b,digest_size=16')
        hash_function.update(b'test123')
        self.assertEqual('6de7714a67685c8f448db98d3d1a307a', hash_function.hexdigest())

    def test_blake2_32(self):
        hash_function = parametrized_hash_function('blake2b,digest_size=32')
        hash_function.update(b'test123')
        self.assertEqual('90cccd774db0ac8c6ea2deff0e26fc52768a827c91c737a2e050668d8c39c224', hash_function.hexdigest())
