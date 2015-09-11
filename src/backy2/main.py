# -*- encoding: utf-8 -*-

#from prettytable import PrettyTable
import argparse
import glob
import hashlib
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


class Level():
    """ Writes and reads a single level, i.e. a data-file and an index.
    This is completely unaware of any chunk sizes. You may write any
    chunk size you want.
    So this is effectively a key value store.
    """

    def __init__(self, data_filename, index_filename, remove=False):
        self.data_filename = data_filename
        self.index_filename = index_filename
        self.remove = remove


    def __enter__(self):
        if not os.path.exists(self.data_filename):
            # touch them
            open(self.data_filename, 'wb').close()
            open(self.index_filename, 'wb').close()

        self.data = open(self.data_filename, 'r+b')
        self._read_index()
        return self


    def __exit__(self, type, value, traceback):
        self.data.close()
        self._write_index()
        if self.remove:
            os.unlink(self.data_filename)
            os.unlink(self.index_filename)


    def _read_index(self):
        self.index = {}
        for line in open(self.index_filename):
            _chunk_id, checksum, _offset, _length = line.strip().split('|')
            chunk_id = int(_chunk_id)
            offset = int(_offset)
            length = int(_length)
            self.index[chunk_id] = {'checksum': checksum, 'offset': offset, 'length': length}


    def _write_index(self):
        with open(self.index_filename, 'w') as f:
            for k in sorted(self.index.keys()):
                v = self.index[k]
                line = '|'.join([str(k), v['checksum'], str(v['offset']), str(v['length'])])
                f.write(line + '\n')


    def seek(self, chunk_id):
        here = self.data.tell()
        if chunk_id == -1:
            self.data.seek(0, 2)
        else:
            there = self.index[chunk_id]['offset']
            if here != there:
                self.data.seek(there)


    def tell(self):
        return self.data.tell()


    def write(self, chunk_id, data):
        assert len(data) <= CHUNK_SIZE
        checksum = hashlib.md5(data).hexdigest()
        if chunk_id in self.index:
            # size must match except that it's the last chunk.
            if self.index[chunk_id]['length'] != len(data) and chunk_id != max(self.index.keys()):
                raise BackyException('Unable to write chunk, size does not match.')
            self.seek(chunk_id)
        else:
            self.seek(-1)  # end of file
        self.index[chunk_id] = {'checksum': checksum, 'offset': self.tell(), 'length': len(data)}
        self.data.write(data)


    def read(self, chunk_id):
        self.seek(chunk_id)
        chunk = self.index[chunk_id]
        length = chunk['length']
        checksum = chunk['checksum']
        data = self.data.read(length)
        if not hashlib.md5(data).hexdigest() == checksum:
            logger.critical('Checksum for chunk {} does not match.'.format(chunk_id))
        return data


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

        with Level('test1', 'test2') as lw:
            #lw.write(10, b'asdasdasd')
            #lw.write(1, b'asd')
            #lw.write(20, b'asdasdasd')
            #lw.write(22, b'2222222222222222')
            #lw.write(22, b'3333333333333333')
            #lw.write(1, b'444')
            print(lw.read(22))


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
