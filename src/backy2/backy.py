# -*- encoding: utf-8 -*-

#from prettytable import PrettyTable
import argparse
import configparser
#import glob
import datetime
import fileinput
#import math
#import hashlib
import logging
import json
#import random
import sqlite3
import uuid
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


def makedirs(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


class MetaBackend():
    """ Holds meta data """

    def __init__(self, path):
        self.path = path


    def create_version(self, version_name, size):
        """ Creates a new version with a given name.
        size is the number of blocks this version will contain.
        Returns a uid for this version.
        """
        raise NotImplementedError()


    def get_version(self, uid):
        """ Returns a version as a dict """
        raise NotImplementedError()


    def set_block(self, id, version_uid, block_uid, checksum, size):
        """ Set a block to <id> for a version's uid (which must exist) and
        store it's uid (which points to the data BLOB).
        checksum is the block's checksum
        size is the block's size
        """
        raise NotImplementedError()


    def close(self):
        pass


class DataBackend():
    """ Holds BLOBs, never overwrites
    """

    def __init__(self, path):
        self.path = path


    def save(self, data):
        """ Saves data, returns unique ID """
        raise NotImplementedError()


    def read(self, uid):
        """ Returns b'<data>' or raises FileNotFoundError """
        raise NotImplementedError()


    def rm(self, uid):
        """ Deletes a block """
        raise NotImplementedError()


    def close(self):
        pass


class SQLiteBackend(MetaBackend):
    """ Stores meta data in a sqlite database """

    DBFILENAME = 'backy.sqlite'

    def __init__(self, path):
        MetaBackend.__init__(self, path)
        dbpath = os.path.join(self.path, self.DBFILENAME)

        def dict_factory(cursor, row):
            """ A row factory for sqlite3 which emulates a dict cursor. """
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        self.conn = sqlite3.connect(dbpath)
        self.conn.row_factory = dict_factory
        self.cursor = self.conn.cursor()
        self._create()


    def _uid(self):
        return str(uuid.uuid1())


    def _now(self):
        """ Returns datetime as isoformat (ex. 2015-10-25T10:43:03.823777+00:00) """
        return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()


    def _create(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS versions
             (uid text, date text, name text, size integer)''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS blocks
             (uid text, version_uid text, id integer, date text, checksum text, size integer)''')
        self.conn.commit()


    def create_version(self, version_name, size):
        uid = self._uid()
        now = self._now()
        self.cursor.execute('''
            INSERT INTO versions (uid, date, name, size) VALUES (?, ?, ?, ?)
            ''', (uid, now, version_name, size))
        self.conn.commit()
        return uid


    def get_version(self, uid):
        self.cursor.execute('''
            SELECT uid, date, name, size FROM versions WHERE uid=?
            ''', (uid,))
        version = self.cursor.fetchone()  # might raise
        if version is None:
            # not found
            raise KeyError('Version {} not found.'.format(uid))
        return version



    def set_block(self, id, version_uid, block_uid, checksum, size):
        now = self._now()
        self.cursor.execute('''
            INSERT INTO blocks (uid, version_uid, id, date, checksum, size) VALUES (?, ?, ?, ?, ?, ?)
            ''', (block_uid, version_uid, id, now, checksum, size))
        self.conn.commit()


    def close(self):
        self.conn.close()


class FileBackend(DataBackend):
    """ A DataBackend which stores in files. The files are stored in directories
    starting with the bytes of the generated uid. The depth of this structure
    is configurable via the DEPTH parameter, which defaults to 2. """

    DEPTH = 2
    SPLIT = 2

    def _uid(self):
        return uuid.uuid1().hex


    def _path(self, uid):
        """ Returns a generated path (depth = self.DEPTH) from a uid.
        Example uid=831bde887afc11e5b45aa44e314f9270 and depth=2, then
        it returns "83/1b".
        If depth is larger than available bytes, then available bytes
        are returned only as path."""

        parts = [uid[i:i+self.SPLIT] for i in range(0, len(uid), self.SPLIT)]
        return os.path.join(*parts[:self.DEPTH])


    def save(self, data):
        uid = self._uid()
        path = os.path.join(self.path, self._path(uid))
        makedirs(path)
        filename = os.path.join(path, uid)
        if os.path.exists(filename):
            raise ValueError('Found a file {} where this is impossible.'.format(filename))
        with open(filename, 'wb') as f:
            r = f.write(data)
            assert r == len(data)
        return uid


    def rm(self, uid):
        path = os.path.join(self.path, self._path(uid))
        filename = os.path.join(path, uid)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        os.unlink(filename)


    def read(self, uid):
        path = os.path.join(self.path, self._path(uid))
        filename = os.path.join(path, uid)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        return open(filename, 'rb').read()



class Backy():
    """
    """

    def __init__(self, path):
        self.path = path
        self.datapath = os.path.join(self.path, 'data')
        self.meta_backend = SQLiteBackend(self.datapath)
        self.data_backend = FileBackend(self.datapath)
        makedirs(self.datapath)


    def close(self):
        self.meta_backend.close()
        self.data_backend.close()



class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self.path = path


    def backup(self, source, backupname, rbd):
        backy = Backy(self.path)
        hints = None
        if rbd:
            data = ''.join([line for line in fileinput.input(rbd).readline()])
            hints = hints_from_rbd_diff(data)
        import pdb; pdb.set_trace()

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
