# -*- encoding: utf-8 -*-

#from prettytable import PrettyTable
import argparse
import glob
import logging
import os
import sys


logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024*4096  # 4MB

def init_logging(backupdir, console_level):  # pragma: no cover

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter('%(levelname)8s: %(message)s')),
    console.setLevel(console_level)
    #logger.addHandler(console)

    logfile = logging.FileHandler(os.path.join(backupdir, 'backy.log'))
    logfile.setLevel(logging.INFO)
    logfile.setFormatter(logging.Formatter('%(asctime)s [%(process)d] %(message)s')),
    #logger.addHandler(logfile)

    logging.basicConfig(handlers = [console, logfile], level=logging.DEBUG)

    logger.info('$ ' + ' '.join(sys.argv))


class BackyException(Exception):
    pass


class BackyWriter():
    """TODO"""

    BASE = '{backupname}..'
    BASE_DATA = '{backupname}..data'
    BASE_ROLL = '{backupname}..roll'
    LEVEL_ROLL = '{backupname}..{level}.roll'
    LEVEL_INDEX = '{backupname}..{level}.index'
    LEVEL_DATA = '{backupname}..{level}.data'

    def __init__(self, path, backupname):
        if '.roll.' in backupname or \
               '.diff.' in backupname or \
               '.index.' in backupname or\
               '.data.' in backupname:
           raise BackyException('Reserved name found in backupname.')
        #self.path = path
        self.backupname = backupname
        self.base_datafile_path = os.path.join(path, self.BASE_DATA.format(backupname=backupname))
        self.base_rollfile_path = os.path.join(path, self.BASE_ROLL.format(backupname=backupname))
        _base = os.path.join(path, self.BASE.format(backupname=backupname))
        self.levels = [x[len(_base):] for x in glob.glob(_base + '*')]

        logger.debug('test')
        #import pdb; pdb.set_trace()


class Backy():
    """Backup, restore and scrub logic wrapper"""

    def __init__(self, path):
        self.path = path


    def backup(self, source, backupname, hints=[]):
        writer = BackyWriter(self.path, backupname)


    def restore(self, backupname, target, level):
        pass


    def scrub(self, backupname, level):
        pass



class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self.backy = Backy(path)
        pass


    def backup(self, source, backupname):
        hints = []  # TODO
        self.backy.backup(source, backupname, hints)
        #logger.info('Backup {} -> {}.'.format(source, backupname))


    def restore(self, backupname, target, level):
        self.backy.restore(backupname, target, level)
        #logger.info('Restore {} ({}) -> {}.'.format(level, backupname, target))


    def scrub(self, backupname, level):
        self.backy.scrub(backupname, level)
        #logger.info('scrub {} ({}).'.format(level, backupname))


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
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers.add_parser(
        'restore',
        help="Restore a given backup with level to a given target.")
    p.add_argument('-l', '--level', default='0')
    p.add_argument('backupname')
    p.add_argument('target')
    p.set_defaults(func='restore')

    # SCRUB
    p = subparsers.add_parser(
        'scrub',
        help="Scrub a given backup and check for consistency.")
    p.add_argument('-l', '--level', default='0')
    p.add_argument('backupname')
    p.set_defaults(func='scrub')

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
