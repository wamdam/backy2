# -*- encoding: utf-8 -*-

#from prettytable import PrettyTable
import argparse
import glob
import datetime
import math
import hashlib
import logging
import json
import random
import os
import sys


logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024*4096  # 4MB
CHUNK_STATUS_EXISTS = 0
CHUNK_STATUS_DESTROYED = 1
#CHUNK_STATUS_NOTEXISTS = 2

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


class BackyException(Exception):
    pass


class ChunkChecksumWrong(BackyException):
    pass


class ChunkNotFound(BackyException):
    pass


class Chunk():
    checksum = ''
    offset = 0
    length = 0
    status = -1
    #data = b''


class Index():
    """ A level index
    """
    def __init__(self, chunk_size):
        self.chunk_size = chunk_size
        self.size = 0           # size in bytes
        self._index = {}


    def write(self, filename):
        with open(filename, 'w') as f:
            f.write(str(self.size) + '\n')
            for k in sorted(self._index.keys()):
                v = self._index[k]
                line = '|'.join([str(k), v.checksum, str(v.offset), str(v.length), str(v.status)])
                f.write(line + '\n')


    def read(self, filename):
        with open(filename, 'r') as f:
            self.size = int(f.readline().strip())
            for line in f.readlines():
                _chunk_id, checksum, _offset, _length, status = line.strip().split('|')
                chunk_id = int(_chunk_id)
                chunk = Chunk()
                chunk.checksum = checksum
                chunk.offset = int(_offset)
                chunk.length = int(_length)
                chunk.status = int(status)
                self._index[chunk_id] = chunk
        return self


    def _next_offset(self):
        return len(self._index) * self.chunk_size


    def get(self, chunk_id):
        """ Get an existing chunk or create a new one
        """
        if self.has(chunk_id):
            return self._index[chunk_id]
        else:
            chunk = Chunk()
            chunk.offset = self._next_offset()
            self._index[chunk_id] = chunk
            return chunk


    def has(self, chunk_id):
        return chunk_id in self._index


    def remove(self, chunk_id):
        del(self._index[chunk_id])


    def chunk_ids(self):
        return self._index.keys()


class Level():
    """ Writes and reads a single level, i.e. a data-file and an index.
    data size is variable, but at max. chunk_size.
    """

    def __init__(self, data_filename, index_filename, chunk_size, mtime=None):
        self.data_filename = data_filename
        self.index_filename = index_filename
        self.chunk_size = chunk_size
        self.index = Index(self.chunk_size)
        self.mtime = mtime
        self._rm = False


    def open(self):
        if not os.path.exists(self.data_filename):
            open(self.data_filename, 'wb').close()
        self.data = open(self.data_filename, 'r+b')
        try:
            self.index.read(self.index_filename)
        except FileNotFoundError:
            pass
        return self


    def close(self):
        if not self._rm:
            self.data.close()
            self.index.write(self.index_filename)
            self.set_mtime(self.mtime)


    def __enter__(self):
        return self.open()


    def __exit__(self, type, value, traceback):
        self.close()


    def write(self, chunk_id, data):
        """ Write data to a given chunk ID.
        A write with no data means that the chunk was wiped.
        """
        if len(data) > self.chunk_size:
            raise BackyException('Unable to write chunk, size > chunk size.')
        checksum = hashlib.md5(data).hexdigest()
        chunk = self.index.get(chunk_id)
        self.data.seek(chunk.offset)
        chunk.checksum = checksum
        chunk.length = len(data)
        chunk.status = CHUNK_STATUS_EXISTS
        self.data.write(data)


    def read(self, chunk_id, raise_on_error=False):
        """ Read a given chunk ID's data from the level and validate """
        chunk = self.read_meta(chunk_id)   # raises ChunkNotFound
        self.data.seek(chunk.offset)
        length = chunk.length
        checksum = chunk.checksum
        data = self.data.read(length)
        if hashlib.md5(data).hexdigest() != checksum:
            self.invalidate_chunk(chunk_id)
            if raise_on_error:
                raise ChunkChecksumWrong('Checksum for chunk {} does not match'.format(chunk_id))
            else:
                logger.critical('Checksum for chunk {} does not match.'.format(chunk_id))
        return data


    def read_meta(self, chunk_id):
        """ Read a given chunk ID's meta data from the level """
        if not self.index.has(chunk_id):
            raise ChunkNotFound()
        return self.index.get(chunk_id)


    def set_size(self, size):
        if self.index.size > size:
            raise BackyException('Shrinking is unsupported. Make a new backup.')
        self.index.size = size


    @property
    def size(self):
        return self.index.size


    def level_size(self):
        """ Return the used disk space for this level """
        return os.path.getsize(self.data_filename)


    def invalidate_chunk(self, chunk_id):
        self.index.get(chunk_id).status = CHUNK_STATUS_DESTROYED


    def unexist_chunk(self, chunk_id):
        #self.index.get(chunk_id).status = CHUNK_STATUS_NOTEXISTS
        self.index.remove(chunk_id)


    def get_invalid_chunk_ids(self):
        c = set()
        for chunk_id in self.index.chunk_ids():
            chunk = self.index.get(chunk_id)
            if chunk.status == CHUNK_STATUS_DESTROYED:
                c.add(chunk_id)
        return c


    def set_mtime(self, mtime):
        if mtime is None:
            os.utime(self.index_filename, None)
            os.utime(self.data_filename, None)
        else:
            os.utime(self.index_filename, (mtime, mtime))
            os.utime(self.data_filename, (mtime, mtime))


    def get_mtime(self):
        return os.path.getmtime(self.index_filename)


    def rm(self):
        logger.debug('Deleting {}'.format(self.index_filename))
        os.unlink(self.index_filename)
        logger.debug('Deleting {}'.format(self.data_filename))
        os.unlink(self.data_filename)
        self._rm = True


def chunks_from_hints(hints, chunk_size):
    """ Helper method """
    chunks = set()
    for offset, length, exists in hints:
        start_chunk = offset // chunk_size  # integer division
        end_chunk = start_chunk + (length-1) // chunk_size
        for i in range(start_chunk, end_chunk+1):
            chunks.add(i)
    return chunks


def hints_from_rbd_diff(rbd_diff):
    """ Return the required offset:length tuples from a rbd json diff
    """
    data = json.loads(rbd_diff)
    return [(l['offset'], l['length'], True if l['exists']=='true' else False) for l in data]


class Backy():
    """ Handle several levels of backup with restore and scrubbing """

    BASE = '{backupname}..'

    def __init__(self, path, backupname, chunk_size):
        if '.data.' in backupname or '.index.' in backupname:
           raise BackyException('Reserved name found in backupname.')
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
        if level is None:
            return self.base + 'data'
        else:
            return self.base + 'data.' + str(level)


    def index_filename(self, level=None):
        if level is None:
            return self.base + 'index'
        else:
            return self.base + 'index.' + str(level)


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

            try:
                next_level.mtime = base_level.get_mtime()
            except FileNotFoundError:
                next_level.mtime = None

            next_level.set_size(base_level.size)  # store old size
            base_level.set_size(source_size)

            # Sanity check:
            # check hints for validity, i.e. too high offsets, ...
            if hints:
                max_offset = max([h[0]+h[1] for h in hints])
                if max_offset > source_size:
                    raise BackyException('Hints have higher offsets than source file.')

            if hints:
                # hint[2] is False for non-existing hints
                non_existing_hinted_chunks = chunks_from_hints([hint for hint in hints if not hint[2]], self.chunk_size)
                if non_existing_hinted_chunks:
                    logger.debug('Hints indicate to mark chunks as non-existent: {}'.format(','.join(map(str, non_existing_hinted_chunks))))
                for non_existing_hinted_chunk_id in non_existing_hinted_chunks:
                    base_level.unexist_chunk(non_existing_hinted_chunk_id)

                # hint[2] is True for existing hints
                hinted_chunks = chunks_from_hints([hint for hint in hints if hint[2]], self.chunk_size)
                # TODO: Test destroyed chunks reading
                destroyed_chunks = base_level.get_invalid_chunk_ids()  # always re-read destroyed chunks
                logger.debug('These destroyed chunks will be backed up again: {}'.format(','.join(map(str, destroyed_chunks))))
                logger.debug('Hints indicate to backup chunks {}'.format(','.join(map(str, hinted_chunks))))
                read_chunks = hinted_chunks.union(destroyed_chunks)
            else:
                read_chunks = range(num_chunks_src)

            for chunk_id in sorted(read_chunks):
                source_file.seek(chunk_id * self.chunk_size)  # TODO: check if seek costs when it's == tell.
                data = source_file.read(self.chunk_size)
                if not data:
                    raise BackyException('EOF reached on source when there should be data.')

                data_checksum = hashlib.md5(data).hexdigest()
                logger.debug('Read chunk {} (checksum {})'.format(chunk_id, data_checksum))
                try:
                    base_checksum = base_level.read_meta(chunk_id).checksum
                except ChunkNotFound:
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
                    # when base level doesn't know about this chunk, we also need
                    # no information in the next level.
                    pass
                else:
                    # write that to next level
                    # TODO: write into other thread
                    next_level.write(chunk_id, old_data)
                    logger.debug('Wrote old data chunk {} to level {} (checksum {})'.format(chunk_id, next_level_number, base_checksum))
                # write data to base
                # TODO: write into other thread
                base_level.write(chunk_id, data)
                logger.debug('Wrote new data chunk {} (checksum {})'.format(chunk_id, data_checksum))


            logger.info('Successfully backed up level {} ({}:{})'.format(
                next_level_number,
                next_level.data_filename,
                next_level.index_filename,
                ))


    def restore(self, target, level=None):
        """ Restore a given level (or base if level is None) to target.
        warning: This overwrites data in target.
        """
        logger.debug('Starting restore of level {} into {}'.format(level, target))
        all_levels = self.get_levels()
        if level is not None and level not in all_levels:
            raise BackyException('Level {} not found.'.format(level))

        if level is None:
            to_restore_levels = []
        else:
            to_restore_levels = all_levels[all_levels.index(level):]

        # old levels
        levels = [Level(self.data_filename(l), self.index_filename(l), self.chunk_size).open() for l in to_restore_levels]
        # base level
        levels.append(Level(self.data_filename(), self.index_filename(), self.chunk_size).open())

        # create a read list
        max_chunk = (levels[0].size - 1) // self.chunk_size
        num_chunks = max_chunk + 1
        read_list = []

        for chunk_id in range(num_chunks):
            found = False
            for level in levels:
                if level.index.has(chunk_id):
                    read_list.append((chunk_id, level))
                    found = True
                    break
            if not found:
                logger.debug('Chunk {} not found in any level. If the base backup is sparse, this is ok.'.format(chunk_id))

        # debug output
        for chunk_id, level in read_list:
            data_filename = level.data_filename
            chunk = level.read_meta(chunk_id)
            logger.debug('Restore {:12d} bytes from {:>20s}:{:<12d} with checksum {} and status {}'.format(
                chunk.length,
                data_filename,
                chunk.offset,
                chunk.checksum,
                {
                    CHUNK_STATUS_EXISTS:'CHUNK_STATUS_EXISTS',
                    CHUNK_STATUS_DESTROYED:'CHUNK_STATUS_DESTROYED'
                    }.get(chunk.status, ''),
            ))

        with open(target, 'wb') as target:
            last_write_position = 0
            for chunk_id, level in sorted(read_list):
                data = level.read(chunk_id)
                target.seek(chunk_id * self.chunk_size)
                target.write(data)
                last_write_position = target.tell()
            # if the last block was sparse, then the last write position is not at
            # the end of the to-be-restored file. So we seek there and write.
            if last_write_position < levels[0].size:
                target.seek(levels[0].size-1)
                target.write(b'\0')
                logger.debug('Fixed target size to {} bytes, as last block is sparse.'.format(target.tell()))

        for level in levels:
            level.close()

        # TODO: Test if same number of chunks really leads to index.size bytes.


    def scrub(self, level=None, percentile=100):
        """ Scrub a level against its own checksums
        """
        logger.info("Performing scrub with {}% chunk checks.".format(percentile))
        all_levels = self.get_levels()
        if level is not None and level not in all_levels:
            raise BackyException('Level {} not found.'.format(level))

        checked = 0

        with Level(self.data_filename(level), self.index_filename(level), self.chunk_size) as level:
            for chunk_id in level.index.chunk_ids():
                try:
                    level.read(chunk_id, raise_on_error=True)
                except ChunkChecksumWrong:
                    logger.critical('SCRUB: Checksum for chunk {} does not match.'.format(chunk_id))
                    level.invalidate_chunk(chunk_id)
                checked += 1
        logger.info("Scrub completed, {} chunks checked.".format(checked))


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

            for chunk_id in level.index.chunk_ids():
                if percentile < 100 and random.randint(1, 100) > percentile:
                    continue
                try:
                    backup_data = level.read(chunk_id, raise_on_error=True)
                except ChunkChecksumWrong:
                    logger.critical('SCRUB: Checksum for chunk {} does not match.'.format(chunk_id))
                    level.invalidate_chunk(chunk_id)
                else:
                    # check source
                    chunk = level.read_meta(chunk_id)
                    source_file.seek(chunk_id * self.chunk_size)
                    source_data = source_file.read(chunk.length)
                    if backup_data != source_data:
                        logger.critical('SCRUB: Source data for chunk {} does not match.'.format(chunk_id))
                        level.invalidate_chunk(chunk_id)
                checked += 1
        logger.info("Deep scrub completed, {} chunks checked.".format(checked))
        return checked


    def ls(self):
        levels = [(None, Level(self.data_filename(), self.index_filename(), self.chunk_size))]
        for level_number in self.get_levels():
            levels.append((level_number, Level(self.data_filename(level_number), self.index_filename(level_number), self.chunk_size)))

        sum_size = 0
        logger.info('Backup {}'.format(self.backupname))
        logger.info('----------------------------')
        for level_number, level in levels:
            level_size = level.level_size()
            sum_size += level_size
            size = level.size
            created = datetime.datetime.fromtimestamp(level.get_mtime()).strftime("%Y-%m-%d %H:%M:%S")
            logger.info('  Level {:>4} from {} (Size: {:6.1f}GB / {:6.1f}GB)  Restore: backy restore {} {} {{outfile}}'.format(
                'Base' if level_number is None else level_number,
                created,
                level_size/1024/1024/1024,
                size/1024/1024/1024,
                '-l {} '.format(level_number) if level_number is not None else '',
                self.backupname,
                ))

        logger.info("Statistics: Backup contains {} levels with a total size of {:.1f}GB".format(
            len(levels),
            sum_size/1024/1024/1024,
            ))
        logger.info('')


    def cleanup(self, keeplevels=7):
        """ Delete oldest levels, only keep keep_levels.
        Always keeps base.
        """
        levels = self.get_levels()
        delete_levels = levels[:-keeplevels]
        for level_number in delete_levels:
            with Level(self.data_filename(level_number), self.index_filename(level_number), self.chunk_size) as level:
                logger.info('Deleting level {} of backup {}.'.format(level_number, self.backupname))
                level.rm()


class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self.path = path


    def backup(self, source, backupname, rbd):
        hints = None
        if rbd:
            hints = hints_from_rbd_diff(open(rbd, 'r').read())
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
