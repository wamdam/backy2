# -*- encoding: utf-8 -*-

#from prettytable import PrettyTable
import argparse
#import glob
#import datetime
import fileinput
#import math
#import hashlib
import logging
import json
#import random
import os
import sys


logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024*4096  # 4MB
CHUNK_STATUS_EXISTS = 0
CHUNK_STATUS_DESTROYED = 1
CHUNK_STATUS_NOTEXISTS = 2


def init_logging(logdir, console_level):  # pragma: no cover
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter('%(levelname)8s: %(message)s')),
    console.setLevel(console_level)
    #logger.addHandler(console)

    logfile = logging.FileHandler(os.path.join(logdir, 'backy.log'))
    logfile.setLevel(logging.INFO)
    logfile.setFormatter(logging.Formatter('%(asctime)s [%(process)d] %(message)s')),
    #logger.addHandler(logfile)

    logging.basicConfig(handlers = [console, logfile], level=logging.DEBUG)

    logger.info('$ ' + ' '.join(sys.argv))



def hints_from_rbd_diff(rbd_diff):
    """ Return the required offset:length tuples from a rbd json diff
    """
    data = json.loads(rbd_diff)
    return [(l['offset'], l['length'], True if l['exists']=='true' else False) for l in data]


class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self.path = path


    def backup(self, source, backupname, rbd):
        hints = None
        if rbd:
            data = ''.join([line for line in fileinput.input(rbd).readline()])
            hints = hints_from_rbd_diff(data)
            print(hints)
            # TODO: next
            # if '-' get from stdin
            # else read from file
            # convert rbd into hints

        backy = Backy(self.path, backupname, chunk_size=CHUNK_SIZE)
        backy.backup(source, hints)


    def restore(self, backupname, target, level):
        if level == '':
            level = None  # restore latest
        else:
            level = int(level)
        backy = Backy(self.path, backupname, chunk_size=CHUNK_SIZE)
        backy.restore(target, level)


    def scrub(self, backupname, level, source, percentile):
        if level == '':
            level = None  # restore latest
        else:
            level = int(level)
        if percentile:
            percentile = int(percentile)
        backy = Backy(self.path, backupname, chunk_size=CHUNK_SIZE)
        if source:
            backy.deep_scrub(source, level, percentile)
        else:
            backy.scrub(level)


    def ls(self, backupname):
        if not backupname:
            where = os.path.join(self.path)
            files = glob.glob(where + '/' + '*..index')
            backupnames = [f.split('..')[0].split('/')[-1] for f in files]
        else:
            backupnames = [backupname]
        for backupname in backupnames:
            Backy(self.path, backupname, chunk_size=CHUNK_SIZE).ls()


    def cleanup(self, backupname, keeplevels):
        keeplevels = int(keeplevels)
        if not backupname:
            where = os.path.join(self.path)
            files = glob.glob(where + '/' + '*..index')
            backupnames = [f.split('..')[0].split('/')[-1] for f in files]
        else:
            backupnames = [backupname]
        for backupname in backupnames:
            Backy(self.path, backupname, chunk_size=CHUNK_SIZE).cleanup(keeplevels)


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument(
        '-b', '--backupdir', default='.')

    subparsers = parser.add_subparsers()

    # BACKUP
    p = subparsers.add_parser(
        'backup',
        help="Perform a backup.")
    p.add_argument(
        'source',
        help='Source file')
    p.add_argument(
        'backupname',
        help='Destination file. Will be a copy of source.')
    p.add_argument('-r', '--rbd', default=None, help='Hints as rbd json format')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers.add_parser(
        'restore',
        help="Restore a given backup with level to a given target.")
    p.add_argument('-l', '--level', default='')
    p.add_argument('backupname')
    p.add_argument('target')
    p.set_defaults(func='restore')

    # SCRUB
    p = subparsers.add_parser(
        'scrub',
        help="Scrub a given backup and check for consistency.")
    p.add_argument('-l', '--level', default='')
    p.add_argument('-s', '--source', default=None,
        help="Source, optional. If given, check if source matches backup in addition to checksum tests.")
    p.add_argument('-p', '--percentile', default=100,
        help="Only check PERCENTILE percent of the blocks (value 0..100). Default: 100")
    p.add_argument('backupname')
    p.set_defaults(func='scrub')

    # CLEANUP
    p = subparsers.add_parser(
        'cleanup',
        help="Clean backup levels, only keep given number of newest levels.")
    p.add_argument('-l', '--keeplevels', default='7')
    p.add_argument('backupname', nargs='?', default="")
    p.set_defaults(func='cleanup')

    # LS
    p = subparsers.add_parser(
        'ls',
        help="List existing backups.")
    p.add_argument('backupname', nargs='?', default="")
    p.set_defaults(func='ls')

    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(0)

    if args.verbose:
        console_level = logging.DEBUG
    #elif args.func == 'scheduler':
        #console_level = logging.INFO
    else:
        console_level = logging.INFO
    init_logging(args.backupdir, console_level)

    commands = Commands(args.backupdir)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']
    del func_args['verbose']
    del func_args['backupdir']

    try:
        logger.debug('backup.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        logger.info('Backy complete.\n')
        sys.exit(0)
    except Exception as e:
        logger.error('Unexpected exception')
        logger.exception(e)
        logger.info('Backy failed.\n')
        sys.exit(1)


if __name__ == '__main__':
    main()
