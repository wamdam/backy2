#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#
# This is an example key derivation function for generating the master key needed by the AWS S3 client side encryption.
#

import argparse
import getpass
import logging
import sys
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
        description="""
        This tool derives a high entropy key from the supplied password by using PBKDF2 with HMAC-SHA256. It outputs
        a configuration section which can be directly inserted into the configuration file under the dataBackend section.
        Currently the derived key is always 32 bytes long which makes it suitable for AES-256.
        """,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-V', '--version', action='store_true', help='show version')
    parser.add_argument('-s', '--salt', nargs='?', default='mpJkkZqRm5GcpI2aLpyRpA==', help='BASE64 encoded salt')
    parser.add_argument('-r', '--rounds', nargs='?', type=int, default=65536, help='number of rounds')
    parser.add_argument('-p', '--password-file', nargs='?', help='file containing the password')

    args = parser.parse_args()

    if args.version:
        print(__version__)
        exit(0)

    init_logging(None, logging.INFO)

    if args.password_file:
        logger.info('Reading password from file {}.'.format(args.password_file))
        with open(args.password_file, 'rb') as f:
            password = f.read()
    else:
        logger.info('Reading password from terminal.')
        password = getpass.getpass(prompt='Enter password: ')
        password_2 = getpass.getpass(prompt='Reenter password: ')
        if password != password_2:
            print('The two passwords don\'t match.', file=sys.stderr)
            exit(1)

    try:
        derive_key(password, a2b_base64(args.salt), args.rounds)
        exit(0)
    except Exception as exception:
        logger.error('Unexpected exception')
        logger.exception(exception)
        exit(100)

if __name__ == '__main__':
    main()
