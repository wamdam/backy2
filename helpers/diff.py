#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import argparse
import hashlib
import logging
import sys

class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self):
        pass


    def diff(self, file1, file2):
        block_size = 4*1024*1024
        with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
            block = 0
            while True:
                d1 = f1.read(block_size)
                d2 = f2.read(block_size)
                if d1 != d2:
                    print("Difference in block {}".format(block))
                    return
                block += 1


def main():
    parser = argparse.ArgumentParser(
        description='Diff two files and output where the first difference is (4MB block based)',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('file1')
    parser.add_argument('file2')
    parser.set_defaults(func='diff')


    args = parser.parse_args()
    commands = Commands()
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']

    try:
        func(**func_args)
        sys.exit(0)
    except Exception as e:
        raise e
        sys.exit(100)


if __name__ == '__main__':
    main()
