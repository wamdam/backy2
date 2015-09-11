# -*- encoding: utf-8 -*-

#from prettytable import PrettyTable
import argparse
import logging
import os
import sys


logger = logging.getLogger(__name__)


def init_logging(backupdir, console_level):  # pragma: no cover
    logging.basicConfig(
        filename=os.path.join(backupdir, 'backy.log'),
        format='%(asctime)s [%(process)d] %(message)s',
        level=logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(console_level)
    logging.getLogger('').addHandler(console)

    logger.info('$ ' + ' '.join(sys.argv))


class Commands(object):
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        #self.backup = backy.backup.Backup(path)
        pass


    def backup(self, source, backupname):
        logger.info('Backup {} -> {}.'.format(source, backupname))


    def restore(self, level, backupname, target):
        logger.info('Restore {} ({}) -> {}.'.format(level, backupname, target))


    def scrub(self, level, backupname):
        logger.info('scrub {} ({}).'.format(level, backupname))


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
        logger.info('Backup complete.\n')
        sys.exit(0)
    except Exception as e:
        logger.error('Unexpected exception')
        logger.exception(e)
        logger.info('Backup failed.\n')
        sys.exit(1)


if __name__ == '__main__':
    main()
