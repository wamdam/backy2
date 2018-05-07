#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#
# This is an example key derivation function for generating the master key needed by the AWS S3 client side encryption.
#

import argparse
import logging
from binascii import b2a_base64, a2b_base64

import pkg_resources
from Crypto.Hash import HMAC, SHA256
from Crypto.Protocol.KDF import PBKDF2

from backy2.logging import init_logging, logger

__version__ = pkg_resources.get_distribution('backy2').version

def derive_key(password, salt, rounds):
    derived_key = PBKDF2(password, salt, 32, rounds, prf=lambda p,s : HMAC.new(p,s, SHA256).digest())

    print("""
    dataBackend:
      encryption:
        - name: aws_s3_cse
          materials:
          masterKey: !!binary |
            {}
          active: true
    """.format(b2a_base64(derived_key, newline=False).decode('ascii')))

def main():
    parser = argparse.ArgumentParser(
        description='Key derivation tool for use with AWS S3 client-side encryption.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-V', '--version', action='store_true', help='Show version')
    parser.add_argument('-s', '--salt', nargs='?', default='mpJkkZqRm5GcpI2aLpyRpA==', help='BASE64 encoded salt')
    parser.add_argument('-r', '--rounds', nargs='?', type=int, default=65536, help='Number of rounds')
    parser.add_argument('password', help="Password string.")

    args = parser.parse_args()

    if args.version:
        print(__version__)
        exit(0)

    if not hasattr(args, 'password'):
        parser.print_usage()
        exit(1)

    init_logging(None, logging.WARN)

    try:
        derive_key(args.password, a2b_base64(args.salt), args.rounds)
        exit(0)
    except Exception as e:
        logger.error('Unexpected exception')
        logger.exception(e)
        exit(100)

if __name__ == '__main__':
    main()
