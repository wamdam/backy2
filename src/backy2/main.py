# -*- encoding: utf-8 -*-

#from prettytable import PrettyTable
from collections import defaultdict
import argparse
import glob
import math
import hashlib
import logging
import random
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


class ChunkChecksumWrong(BackyException):
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

    def __init__(self, data_filename, index_filename, chunk_size):
        self.data_filename = data_filename
        self.index_filename = index_filename
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
        if self.index.keys():
            return max(self.index.keys())
        else:
            return -1


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


    @property
    def empty(self):
        return self.max_chunk() == 0


    def wipe(self, chunk_id):
        """ Mark data as wiped for chunk_id """
        self.write(chunk_id, b'')


    def read(self, chunk_id, raise_on_error=False):
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
            if raise_on_error:
                raise ChunkChecksumWrong('Checksum for chunk {} does not match'.format(chunk_id))
            else:
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


    def truncate(self, chunk_id=None):
        """ Truncate the level after the given chunk_id.
        If no chunk_id is given, truncate the last one according to the index.
        """
        if not chunk_id:
            chunk_id = self.max_chunk()
        if chunk_id not in self.index:
            raise BackyException('Cannot truncate to unknown chunk_id {}'.format(chunk_id))

        # truncate index
        _del = []
        for _chunk_id in self.index.keys():
            if _chunk_id > chunk_id:
                _del.append(_chunk_id)
        for _chunk_id in _del:
            del(self.index[_chunk_id])

        # truncate data to index
        last_chunk = self.index[chunk_id]
        logger.debug('Truncating base to {}'.format(last_chunk.offset + last_chunk.length))
        self.data.truncate(last_chunk.offset + last_chunk.length)


    def invalidate_chunk(self, chunk_id):
        chunk = self.index[chunk_id]
        chunk.checksum = ''


def chunks_from_hints(hints, chunk_size):
    """ Helper method """
    chunks = set()
    for offset, length in hints:
        start_chunk = offset // chunk_size  # integer division
        end_chunk = start_chunk + (length-1) // chunk_size
        for i in range(start_chunk, end_chunk+1):
            chunks.add(i)
    return chunks



class Backy():
    """ Handle several levels of backup with restore and scrubbing """

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


    def data_filename(self, level=None):
        if level is not None:
            return self.base + 'data.' + str(level)
        else:
            return self.base + 'data'


    def index_filename(self, level=None):
        if level is not None:
            return self.base + 'index.' + str(level)
        else:
            return self.base + 'index'


    def backup(self, source, hints=None):
        """ Create a backup from source.
        If hints are given, they must be tuples of (offset, length). Then, only
        data within hints will be backed up.
        Otherwise, the backup reads source and looks if checksums match with
        the target.
        """
        next_level_number = self.get_next_level()

        logger.debug('Base data file:  {}'.format(self.data_filename()))
        logger.debug('Base index file: {}'.format(self.index_filename()))
        logger.debug('Levels:          {}'.format(self.get_levels()))
        logger.debug('Next level:      {}'.format(next_level_number))
        logger.debug('Next data file:  {}'.format(self.data_filename(next_level_number)))
        logger.debug('Next index file: {}'.format(self.index_filename(next_level_number)))

        with Level(self.data_filename(), self.index_filename(), self.chunk_size) as base_level, \
                Level(self.data_filename(next_level_number), self.index_filename(next_level_number), self.chunk_size) as next_level, \
                open(source, 'rb') as source_file:

            # determine source size
            source_file.seek(0, 2)  # to the end
            source_size = source_file.tell()
            source_file.seek(0)
            num_chunks_src = math.ceil(source_size / self.chunk_size)

            # check if we must truncate
            num_chunks_base = base_level.max_chunk() + 1
            if num_chunks_base > num_chunks_src:
                # write rest to next level and truncate base
                for chunk_id in range(num_chunks_src, num_chunks_base):
                    data = base_level.read(chunk_id)
                    next_level.write(chunk_id, data)
            max_chunk_src = num_chunks_src - 1  # chunk_ids count from 0
            # need to do this twice. This time because last chunk must be last to get an different size.
            if base_level.max_chunk() > max_chunk_src:
                base_level.truncate(max_chunk_src)

            if hints:
                read_chunks = chunks_from_hints(hints, self.chunk_size)
            else:
                read_chunks = range(num_chunks_src)

            for chunk_id in sorted(read_chunks):
                source_file.seek(chunk_id * self.chunk_size)  # TODO: check if seek costs when it's == tell.
                data = source_file.read(self.chunk_size)
                if not data:
                    break  # EOF

                data_checksum = hashlib.md5(data).hexdigest()
                logger.debug('Read chunk {} (checksum {})'.format(chunk_id, data_checksum))
                try:
                    base_checksum = base_level.read_meta(chunk_id).checksum
                except (ChunkWiped, ChunkNotFound):
                    pass
                else:
                    if data_checksum == base_checksum:
                        logger.debug('Checksum matches for chunk {}'.format(chunk_id))
                        continue  # base already matches

                # read from base
                try:
                    old_data = base_level.read(chunk_id)
                    logger.debug('Read old data chunk {} (checksum {})'.format(chunk_id, base_checksum))
                    # TODO: Throw away buffers
                except ChunkNotFound:
                    next_level.wipe(chunk_id)
                except ChunkWiped:
                    pass
                else:
                    # write that to next level
                    # TODO: write into other thread
                    logger.debug('Wrote old data chunk {} to level {} (checksum {})'.format(chunk_id, next_level_number, base_checksum))
                    next_level.write(chunk_id, old_data)
                # write data to base
                # TODO: write into other thread
                logger.debug('Wrote new data chunk {} (checksum {})'.format(chunk_id, data_checksum))
                base_level.write(chunk_id, data)

            # need to do this again. This time because last chunk can have different size.
            base_level.truncate()

            logger.info('Successfully backed up level {} ({}:{})'.format(
                next_level_number,
                next_level.data_filename,
                next_level.index_filename,
                ))


    def restore(self, target, level=None):
        """ Restore a given level (or base if level is None) to target.
        warning: This overwrites data in target.
        """
        all_levels = self.get_levels()
        if level is not None and level not in all_levels:
            raise BackyException('Level {} not found.'.format(level))

        if level is None:
            to_restore_levels = []
        else:
            to_restore_levels = all_levels[all_levels.index(level):]

        levels = [Level(self.data_filename(l), self.index_filename(l), self.chunk_size).open() for l in to_restore_levels]
        levels.append(Level(self.data_filename(), self.index_filename(), self.chunk_size).open())

        # create a read list
        max_chunk = max([l.max_chunk() for l in levels if not l.empty])
        # TODO: Check if max_chunk is -1. No idea how this could happen however.
        num_chunks = max_chunk + 1
        read_list = []

        end = False
        for chunk_id in range(num_chunks):
            found = False
            for level in levels:
                if chunk_id in level.index:
                    try:
                        level.read_meta(chunk_id)
                    except ChunkWiped:
                        end = True
                    else:
                        if not end:
                            read_list.append((chunk_id, level))
                        else:
                            raise BackyException('Invalid backup found: Non-wiped chunk where it should be wiped: '
                                    'Chunk {} in level {}:{}'.format(chunk_id, level.index_filename, level.data_filename))
                    found = True
                    break
            if not found:
                raise BackyException('Chunk {} not found in any backup.'.format(chunk_id))

        # debug output
        for chunk_id, level in read_list:
            data_filename = level.data_filename
            #index_filename = level.index_filename
            chunk = level.read_meta(chunk_id)
            logger.debug('Restore {:12d} bytes from {:>20s}:{:<12d} with checksum {}'.format(
                chunk.length,
                data_filename,
                chunk.offset,
                chunk.checksum,
            ))

        with open(target, 'wb') as target:
            for chunk_id, level in read_list:
                data = level.read(chunk_id)
                target.write(data)

        for level in levels:
            level.close()


    def scrub(self, level=None, source=None):
        """ Scrub a level against its own checksums
        """
        all_levels = self.get_levels()
        if level is not None and level not in all_levels:
            raise BackyException('Level {} not found.'.format(level))

        checked = 0

        with Level(self.data_filename(level), self.index_filename(level), self.chunk_size) as level:
            for chunk_id in level.index:
                try:
                    level.read(chunk_id, raise_on_error=True)
                except ChunkChecksumWrong:
                    logger.critical('SCRUB: Checksum for chunk {} does not match.'.format(chunk_id))
                    level.invalidate_chunk(chunk_id)
                checked += 1
        logger.info("Deep scrub completed, {} chunks checked.".format(checked))


    def deep_scrub(self, source, level=None, percentile=100):
        """ Scrub a level against a source image.
        If percentile is given, only check percentile percent of the data blocks.
        Returns the number of checked chunks.
        """
        logger.info("Performing deep scrub with {}% chunk checks.".format(percentile))
        all_levels = self.get_levels()
        if level is not None and level not in all_levels:
            raise BackyException('Level {} not found.'.format(level))

        checked = 0

        with Level(self.data_filename(level), self.index_filename(level), self.chunk_size) as level, \
                open(source, 'rb') as source_file:

            source_file.seek(0, 2)  # to the end

            for chunk_id in level.index:
                if percentile < 100 and random.randint(1, 100) > percentile:
                    continue
                try:
                    backup_data = level.read(chunk_id, raise_on_error=True)
                except ChunkChecksumWrong:
                    logger.critical('SCRUB: Checksum for chunk {} does not match.'.format(chunk_id))
                    level.invalidate_chunk(chunk_id)
                # check source
                chunk = level.index[chunk_id]
                source_file.seek(chunk.offset)
                source_data = source_file.read(chunk.length)
                if backup_data != source_data:
                    logger.critical('SCRUB: Source data for chunk {} does not match.'.format(chunk_id))
                    level.invalidate_chunk(chunk_id)
                checked += 1
        logger.info("Deep scrub completed, {} chunks checked.".format(checked))
        return checked



class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self.path = path


    def backup(self, source, backupname, rbd):
        hints = None
        if rbd:
            # TODO: next
            # if '-' get from stdin
            # else read from file
            # convert rbd into hints
            pass

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
