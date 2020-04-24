# We include this version of aes_keywrap to prevent a Git dependency in
# setup.py. The more recent Python 3 compatible version from GitHub is
# not present on PyPi unfortunately.
#
# Source: https://github.com/kurtbrose/aes_keywrap
# MIT License
# Copyright (c) 2017 Kurt Rose
'''
Key wrapping and unwrapping as defined in RFC 3394.  
Also a padding mechanism that was used in openssl at one time.  
The purpose of this algorithm is to encrypt a key multiple times to add an extra layer of security.
'''
import struct
# TODO: dependency flexibility; make pip install aes_keywrap[cryptography], etc work
from typing import Tuple

from Crypto.Cipher import AES

QUAD = struct.Struct('>Q')


def aes_unwrap_key_and_iv(kek: bytes, wrapped: bytes) -> Tuple[bytes, int]:
    n = len(wrapped) // 8 - 1
    #NOTE: R[0] is never accessed, left in for consistency with RFC indices
    R = [b'\0'] + [wrapped[i * 8:i * 8 + 8] for i in range(1, n + 1)]
    A = QUAD.unpack(wrapped[:8])[0]
    decrypt = AES.new(kek, AES.MODE_ECB).decrypt
    for j in range(5, -1, -1):  #counting down
        for i in range(n, 0, -1):  #(n, n-1, ..., 1)
            ciphertext = QUAD.pack(A ^ (n * j + i)) + R[i]
            B = decrypt(ciphertext)
            A = QUAD.unpack(B[:8])[0]
            R[i] = B[8:]
    return b''.join(R[1:]), A


def aes_unwrap_key(kek: bytes, wrapped: bytes, iv: int = 0xa6a6a6a6a6a6a6a6) -> bytes:
    '''
    key wrapping as defined in RFC 3394
    http://www.ietf.org/rfc/rfc3394.txt
    '''
    key, key_iv = aes_unwrap_key_and_iv(kek, wrapped)
    if key_iv != iv:
        raise ValueError("Integrity Check Failed: " + hex(key_iv) + " (expected " + hex(iv) + ")")
    return key


def aes_wrap_key(kek: bytes, plaintext: bytes, iv: int = 0xa6a6a6a6a6a6a6a6) -> bytes:
    n = len(plaintext) // 8
    R = [b'\0'] + [plaintext[i * 8:i * 8 + 8] for i in range(0, n)]
    A = iv
    encrypt = AES.new(kek, AES.MODE_ECB).encrypt
    for j in range(6):
        for i in range(1, n + 1):
            B = encrypt(QUAD.pack(A) + R[i])
            A = QUAD.unpack(B[:8])[0] ^ (n * j + i)
            R[i] = B[8:]
    return QUAD.pack(A) + b''.join(R[1:])
