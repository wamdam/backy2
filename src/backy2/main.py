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


class LevelWriter():
    """ Writes a level, i.e. a data-file and an index """

    def __init__(self, data_filename, index_filename):
        self.data_filename = data_filename
        self.index_filename = index_filename


    def __enter__(self):
        if not os.path.exists(self.data_filename):
            # touch them
            open(self.data_filename, 'wb').close()
            open(self.index_filename, 'wb').close()

        self.data = open(self.data_filename, 'r+b')
        self.index = open(self.index_filename, 'r+t')
        return self


    def __exit__(self, type, value, traceback):
        self.data.close()
        self.index.close()


class Backy():
    """TODO"""

    BASE = '{backupname}..'

    def __init__(self, path, backupname):
        if '.data.' in backupname or '.index.' in backupname:
           raise BackyException('Reserved name found in backupname.')
        #self.path = path
        self.backupname = backupname
        self.base = os.path.join(path, self.BASE.format(backupname=backupname))
        self.base_datafile_path = os.path.join(path, self.base + 'data')
        self.base_indexfile_path = os.path.join(path, self.base + 'index')

        logger.debug('Base data file:  {}'.format(self.base_datafile_path))
        logger.debug('Base index file: {}'.format(self.base_indexfile_path))
        logger.debug('Levels:          {}'.format(self.get_levels()))
        logger.debug('Next level:      {}'.format(self.next_level()))

        with LevelWriter('test1', 'test2') as lw:
            pass

        import pdb; pdb.set_trace()


    def get_levels(self, files=[]):
        """ Guess levels from the filenames """
        if not files:
            files = glob.glob(self.base + '*.*')
        levels = set([int(x.split('.')[-1]) for x in files])
        return sorted(levels)


    def next_level(self, files=[]):
        levels = self.get_levels(files)
        if not levels:
            return 0
        return levels[-1] + 1


    def backup(self, source, hints=[]):
        pass


    def restore(self, level, target):
        pass


    def scrub(self, level):
        pass



class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self.path = path


    def backup(self, source, backupname):
        hints = []  # TODO
        backy = Backy(self.path, backupname)
        backy.backup(source, hints)
        #logger.info('Backup {} -> {}.'.format(source, backupname))


    def restore(self, backupname, target, level):
        backy = Backy(self.path, backupname)
        backy.restore(level, target)
        #logger.info('Restore {} ({}) -> {}.'.format(level, backupname, target))


    def scrub(self, backupname, level):
        backy = Backy(self.path, backupname)
        backy.scrub(level)
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
