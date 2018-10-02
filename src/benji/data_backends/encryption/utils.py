#!/usr/bin/env python
# -*- encoding: utf-8 -*-
try:
    from Crypto.Hash import SHA512
    from Crypto.Protocol.KDF import PBKDF2
except ImportError:
    raise NotImplementedError('Python module "pycryptodome" is required to enable encryption support.')


def derive_key(*, password, salt, iterations, key_length):
    return PBKDF2(password=password, salt=salt, dkLen=key_length, count=iterations, hmac_hash_module=SHA512)
