# -*- encoding: utf-8 -*-

#from prettytable import PrettyTable
from collections import defaultdict
import argparse
import glob
import math
import hashlib
import logging
import os
import sys


logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024*4096  # 4MB
CHUNK_STATUS_EXISTS = 0
CHUNK_STATUS_WIPED = 1

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


class ChunkNotFound(BackyException):
    pass


class ChunkWiped(BackyException):
    pass


class Chunk():
    checksum = ''
    offset = 0
    length = 0
    status = -1
    #data = b''


class Level():
    """ Writes and reads a single level, i.e. a data-file and an index.
    data size is variable.
    Levels are addable to each other. The result depends on the order of
    the addition.
    """

    def __init__(self, data_filename, index_filename, chunk_size, temporary=False):
        self.data_filename = data_filename
        self.index_filename = index_filename
        self.temporary = temporary
        self.chunk_size = chunk_size


    def open(self):
        if not os.path.exists(self.data_filename):
            # touch them
            open(self.data_filename, 'wb').close()
            open(self.index_filename, 'wb').close()

        self.data = open(self.data_filename, 'r+b')
        self._read_index()
        return self


    def close(self):
        self.data.close()
        self._write_index()
        if self.temporary:
            os.unlink(self.data_filename)
            os.unlink(self.index_filename)


    def __enter__(self):
        return self.open()


    def __exit__(self, type, value, traceback):
        self.close()


    def _read_index(self):
        self.index = defaultdict(Chunk)
        for line in open(self.index_filename):
            _chunk_id, checksum, _offset, _length, status = line.strip().split('|')
            chunk_id = int(_chunk_id)
            chunk = self.index[chunk_id]
            chunk.checksum = checksum
            chunk.offset = int(_offset)
            chunk.length = int(_length)
            chunk.status = int(status)


    def _write_index(self):
        with open(self.index_filename, 'w') as f:
            for k in sorted(self.index.keys()):
                v = self.index[k]
                line = '|'.join([str(k), v.checksum, str(v.offset), str(v.length), str(v.status)])
                f.write(line + '\n')


    def seek(self, chunk_id):
        here = self.data.tell()
        if chunk_id == -1:
            self.data.seek(0, 2)
        else:
            there = self.index[chunk_id].offset
            if here != there:
                self.data.seek(there)


    def _tell(self):
        """ Tell the real, i.e. byte position in the file """
        return self.data.tell()


    def max_chunk(self):
        """ Returns the max. known chunk in this level """
        return max(self.index.keys())


    def write(self, chunk_id, data):
        """ Write data to a given chunk ID.
        A write with no data means that the chunk was wiped.
        """
        checksum = hashlib.md5(data).hexdigest()
        if chunk_id in self.index:
            # size must match except that it's the last chunk.
            if self.index[chunk_id].length != len(data) and chunk_id != self.max_chunk():
                raise BackyException('Unable to write chunk, size does not match.')
            self.seek(chunk_id)
        else:
            self.seek(-1)  # end of file
        chunk = self.index[chunk_id]
        chunk.checksum = checksum
        chunk.offset = self._tell()
        chunk.length = len(data)
        if data:
            chunk.status = CHUNK_STATUS_EXISTS
        else:
            chunk.status = CHUNK_STATUS_WIPED
        self.data.write(data)


    def wipe(self, chunk_id):
        """ Mark data as wiped for chunk_id """
        self.write(chunk_id, b'')


    def read(self, chunk_id):
        """ Read a given chunk ID's data from the level and validate """
        if not chunk_id in self.index:
            raise ChunkNotFound()
        if self.index[chunk_id].status == CHUNK_STATUS_WIPED:
            raise ChunkWiped()

        self.seek(chunk_id)
        chunk = self.index[chunk_id]
        length = chunk.length
        checksum = chunk.checksum
        data = self.data.read(length)
        if not hashlib.md5(data).hexdigest() == checksum:
            logger.critical('Checksum for chunk {} does not match.'.format(chunk_id))
        return data


    def read_meta(self, chunk_id):
        """ Read a given chunk ID's meta data from the level """
        if not chunk_id in self.index:
            raise ChunkNotFound()
        if self.index[chunk_id].status == CHUNK_STATUS_WIPED:
            raise ChunkWiped()

        chunk = self.index[chunk_id]
        return chunk


    def truncate(self, chunk_id):
        """ Truncate the level after the given chunk_id.
        """
        if chunk_id not in self.index:
            raise BackyException('Cannot truncate to unknown chunk_id {}'.format(chunk_id))

        # truncate index
        _del = []
        for _chunk_id in self.index.keys():
            if _chunk_id > chunk_id:
                _del.append(chunk_id)
        for _chunk_id in _del:
            del(self.index[_chunk_id])

        # truncate data to index
        last_chunk = self.index[chunk_id]
        self.data.truncate(last_chunk.offset + last_chunk.length)



class Backy():
    """TODO"""

    BASE = '{backupname}..'

    def __init__(self, path, backupname, chunk_size):
        if '.data.' in backupname or '.index.' in backupname:
           raise BackyException('Reserved name found in backupname.')
        #self.path = path
        self.backupname = backupname
        self.base = os.path.join(path, self.BASE.format(backupname=backupname))
        self.chunk_size = chunk_size


    def get_levels(self, files=[]):
        """ Guess levels from the filenames """
        if not files:
            files = glob.glob(self.base + '*.*')
        levels = set([int(x.split('.')[-1]) for x in files])
        return sorted(levels)


    def get_next_level(self, files=[]):
        levels = self.get_levels(files)
        if not levels:
            return 0
        return levels[-1] + 1


    def backup(self, source, hints=[]):
        base_datafile_path = self.base + 'data'
        base_indexfile_path = self.base + 'index'

        next_level = self.get_next_level()

        next_datafile_path = self.base + 'data' + '.' + str(next_level)
        next_indexfile_path = self.base + 'index' + '.' + str(next_level)

        logger.debug('Base data file:  {}'.format(base_datafile_path))
        logger.debug('Base index file: {}'.format(base_indexfile_path))
        logger.debug('Levels:          {}'.format(self.get_levels()))
        logger.debug('Next level:      {}'.format(next_level))
        logger.debug('Next data file:  {}'.format(next_datafile_path))
        logger.debug('Next index file: {}'.format(next_indexfile_path))

        with Level(base_datafile_path, base_indexfile_path, self.chunk_size).open() as base_level, \
                Level(next_datafile_path, next_indexfile_path, self.chunk_size).open() as next_level, \
                open(source, 'rb') as source_file:

            # determine source size
            source_file.seek(0, 2)  # to the end
            source_size = source_file.tell()
            source_file.seek(0)
            num_chunks_src = math.ceil(source_size / self.chunk_size)
            for chunk_id in range(num_chunks_src):
                data = source_file.read(self.chunk_size)
                if not data:
                    # TODO: Shorten index to the current chunk_id
                    # and write the possible rest of data to the next level.
                    # That would happen when the base shrunk
                    break  # EOF

                data_checksum = hashlib.md5(data).hexdigest()
                try:
                    base_checksum = base_level.read_meta(chunk_id).checksum
                except (ChunkWiped, ChunkNotFound):
                    pass
                else:
                    if data_checksum == base_checksum:
                        continue  # base already matches

                # read from base
                try:
                    old_data = base_level.read(chunk_id)
                except ChunkNotFound:
                    next_level.wipe(chunk_id)
                except ChunkWiped:
                    pass
                else:
                    # write that to next level
                    next_level.write(chunk_id, old_data)
                # write data to base
                base_level.write(chunk_id, data)

            num_chunks_base = base_level.max_chunk() + 1
            # TODO: Check if chunk sizes match or at least if last chunk's length differs and truncate too
            if num_chunks_base > num_chunks_src:
                # write rest to next level and truncate base
                for chunk_id in range(num_chunks_src, num_chunks_base):
                    data = base_level.read(chunk_id)
                    next_level.write(chunk_id, data)

            max_chunk_src = num_chunks_src - 1  # chunk_ids count from 0
            base_level.truncate(max_chunk_src)


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
        backy = Backy(self.path, backupname, chunk_size=CHUNK_SIZE)
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
