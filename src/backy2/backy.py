# -*- encoding: utf-8 -*-

from prettytable import PrettyTable
from configparser import ConfigParser  # python 3.3
from functools import partial
from sqlalchemy import Column, Boolean, String, Integer, ForeignKey
from sqlalchemy import func, distinct
from sqlalchemy.types import DateTime
from sqlalchemy.orm import sessionmaker
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
import argparse
import datetime
import fnmatch
import fileinput
import math
import hashlib
import logging
import json
import random
import sqlite3
#import shutil
import uuid
import os
import sys


logger = logging.getLogger(__name__)

BLOCK_SIZE = 1024*4096  # 4MB
HASH_FUNCTION = hashlib.sha512

CFG = {
    'DB': {
        'type': 'sql',
        'engine': 'sqlite:////tmp/backy.sqlite',
        }
    }

Base = declarative_base()

class ConfigException(Exception):
    pass

class Config(dict):
    def __init__(self, base_config, conffile=None):
        if conffile:
            config = ConfigParser()
            config.read(conffile)
            sections = config.sections()
            difference = set(sections).difference(base_config.keys())
            if difference:
                raise ConfigException('Unknown config section(s): {}'.format(', '.join(difference)))
            for section in sections:
                items = config.items(section)
                _cfg = base_config[section]
                for item in items:
                    if item[0] not in _cfg:
                        raise ConfigException('Unknown setting "{}" in section "{}".'.format(item[0], section))
                    _cfg[item[0]] = item[1]
        for key, value in base_config.items():
            self[key] = value

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


def blocks_from_hints(hints, block_size):
    """ Helper method """
    blocks = set()
    for offset, length, exists in hints:
        start_block = math.floor(offset / block_size)
        end_block = math.ceil((offset + length) / block_size)
        for i in range(start_block, end_block):
            blocks.add(i)
    return blocks


def makedirs(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


class MetaBackend():
    """ Holds meta data """

    def __init__(self):
        pass

    def set_version(self, version_name, size, size_bytes):
        """ Creates a new version with a given name.
        size is the number of blocks this version will contain.
        Returns a uid for this version.
        """
        raise NotImplementedError()


    def set_version_invalid(self, uid):
        """ Mark a version as invalid """
        raise NotImplementedError()


    def set_version_valid(self, uid):
        """ Mark a version as valid """
        raise NotImplementedError()


    def get_version(self, uid):
        """ Returns a version as a dict """
        raise NotImplementedError()


    def get_versions(self):
        """ Returns a list of all versions """
        raise NotImplementedError()


    def set_block(self, id, version_uid, block_uid, checksum, size, _commit=True):
        """ Set a block to <id> for a version's uid (which must exist) and
        store it's uid (which points to the data BLOB).
        checksum is the block's checksum
        size is the block's size
        _commit is a hint if the transaction should be committed immediately.
        """
        raise NotImplementedError()


    def set_blocks_invalid(self, uid, checksum):
        """ Set blocks pointing to this block uid with the given checksum invalid.
        This happens, when a block is found invalid during read or scrub.
        """
        raise NotImplementedError()


    def get_block_by_checksum(self, checksum):
        """ Get a block by its checksum. This is useful for deduplication """
        raise NotImplementedError()


    def get_blocks_by_version(self, version_uid):
        """ Returns an ordered (by id asc) list of blocks for a version uid """
        raise NotImplementedError()


    def rm_version(self, version_uid):
        """ Remove a version from the meta data store """
        raise NotImplementedError()


    def get_all_block_uids(self):
        """ Get all block uids existing in the meta data store """
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


    def get_all_blob_uids(self):
        """ Get all existing blob uids """
        raise NotImplementedError()


    def close(self):
        pass


class Version(Base):
    __tablename__ = 'versions'
    uid = Column(String(36), primary_key=True)
    date = Column("date", DateTime , default=func.now(), nullable=False)
    name = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    size_bytes = Column(Integer, nullable=False)
    valid = Column(Integer, nullable=False)

    def __repr__(self):
       return "<Version(uid='%s', name='%s', date='%s')>" % (
                            self.uid, self.name, self.date)


class Block(Base):
    __tablename__ = 'blocks'
    uid = Column(String(32), nullable=True, index=True)
    version_uid = Column(String(36), ForeignKey('versions.uid'), primary_key=True, nullable=False)
    id = Column(Integer, primary_key=True, nullable=False)
    date = Column("date", DateTime , default=func.now(), nullable=False)
    checksum = Column(String(128), index=True, nullable=True)
    size = Column(Integer, nullable=True)
    valid = Column(Integer, nullable=False)

    def __repr__(self):
       return "<Block(id='%s', uid='%s', version_uid='%s')>" % (
                            self.id, self.uid, self.version_uid)


class SQLBackend(MetaBackend):
    """ Stores meta data in an sql database """

    def __init__(self, engine):
        MetaBackend.__init__(self)
        engine = sqlalchemy.create_engine(engine)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()


    def _uid(self):
        return str(uuid.uuid1())


    def _commit(self):
        self.session.commit()


    def set_version(self, version_name, size, size_bytes, valid):
        uid = self._uid()
        version = Version(
            uid=uid,
            name=version_name,
            size=size,
            size_bytes=size_bytes,
            valid=valid,
            )
        self.session.add(version)
        self.session.commit()
        return uid


    def set_version_invalid(self, uid):
        version = self.get_version(uid)
        version.valid = 0
        self.session.commit()
        logger.info('Marked version invalid (UID {})'.format(
            uid,
            ))


    def set_version_valid(self, uid):
        version = self.get_version(uid)
        version.valid = 1
        self.session.commit()
        logger.debug('Marked version valid (UID {})'.format(
            uid,
            ))


    def get_version(self, uid):
        version = self.session.query(Version).filter_by(uid=uid).first()
        if version is None:
            raise KeyError('Version {} not found.'.format(uid))
        return version


    def get_versions(self):
        return self.session.query(Version).order_by(Version.name, Version.date).all()


    def set_block(self, id, version_uid, block_uid, checksum, size, valid, _commit=True):
        valid = 1 if valid else 0
        block = self.session.query(Block).filter_by(id=id, version_uid=version_uid).first()
        if block:
            block.uid = block_uid
            block.checksum = checksum
            block.size = size
            block.valid = valid
        else:
            block = Block(
                id=id,
                version_uid=version_uid,
                uid=block_uid,
                checksum=checksum,
                size=size,
                valid=valid
                )
            self.session.add(block)
        if _commit:
            self.session.commit()


    def set_blocks_invalid(self, uid, checksum):
        _affected_version_uids = self.session.query(distinct(Block.version_uid)).filter_by(uid=uid, checksum=checksum).all()
        affected_version_uids = [v[0] for v in _affected_version_uids]
        self.session.query(Block).filter_by(uid=uid, checksum=checksum).update(valid=False)
        self.session.commit()
        logger.info('Marked block invalid (UID {}, Checksum {}. Affected versions: {}'.format(
            uid,
            checksum,
            ', '.join(affected_version_uids)
            ))
        for version_uid in affected_version_uids:
            self.set_version_invalid(version_uid)
        return affected_version_uids


    def get_block_by_checksum(self, checksum):
        return self.session.query(Block).filter_by(checksum=checksum).first()


    def get_blocks_by_version(self, version_uid):
        return self.session.query(Block).filter_by(version_uid=version_uid).all()


    def rm_version(self, version_uid):
        affected_blocks = self.session.query(Block).filter_by(version_uid=version_uid)
        num_blocks = affected_blocks.count()
        affected_blocks.delete()
        self.session.query(Version).filter_by(uid=version_uid).delete()
        self.session.commit()
        return num_blocks


    def get_all_block_uids(self):
        rows = self.session.query(distinct(Block.uid)).all()
        return [b[0] for b in rows]


    def close(self):
        self.session.close()


class FileBackend(DataBackend):
    """ A DataBackend which stores in files. The files are stored in directories
    starting with the bytes of the generated uid. The depth of this structure
    is configurable via the DEPTH parameter, which defaults to 2. """

    DEPTH = 2
    SPLIT = 2
    SUFFIX = '.blob'

    def _uid(self):
        # a uuid always starts with the same bytes, so let's widen this
        return hashlib.md5(uuid.uuid1().bytes).hexdigest()


    def _path(self, uid):
        """ Returns a generated path (depth = self.DEPTH) from a uid.
        Example uid=831bde887afc11e5b45aa44e314f9270 and depth=2, then
        it returns "83/1b".
        If depth is larger than available bytes, then available bytes
        are returned only as path."""

        parts = [uid[i:i+self.SPLIT] for i in range(0, len(uid), self.SPLIT)]
        return os.path.join(*parts[:self.DEPTH])


    def _filename(self, uid):
        path = os.path.join(self.path, self._path(uid))
        return os.path.join(path, uid + self.SUFFIX)


    def save(self, data):
        uid = self._uid()
        path = os.path.join(self.path, self._path(uid))
        makedirs(path)
        filename = self._filename(uid)
        if os.path.exists(filename):
            raise ValueError('Found a file {} where this is impossible.'.format(filename))
        with open(filename, 'wb') as f:
            r = f.write(data)
            assert r == len(data)
        return uid


    def rm(self, uid):
        filename = self._filename(uid)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        os.unlink(filename)


    def read(self, uid):
        filename = self._filename(uid)
        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        return open(filename, 'rb').read()


    def get_all_blob_uids(self):
        matches = []
        for root, dirnames, filenames in os.walk(self.path):
            for filename in fnmatch.filter(filenames, '*.blob'):
                uid = filename.split('.')[0]
                matches.append(uid)
        return matches



class Backy():
    """
    """

    def __init__(self, path, datapath='data', meta_backend=None, data_backend=None, block_size=BLOCK_SIZE):
        self.path = path
        real_datapath = os.path.join(self.path, datapath)
        makedirs(real_datapath)
        self.meta_backend = meta_backend if meta_backend else SQLBackend('sqlite:///{}/backy.sqlite'.format(real_datapath))
        self.data_backend = data_backend if data_backend else FileBackend(real_datapath)
        self.block_size = block_size


    def _prepare_version(self, name, size_bytes, from_version_uid=None):
        """ Prepares the metadata for a new version.
        If from_version_uid is given, this is taken as the base, otherwise
        a pure sparse version is created.
        """
        if from_version_uid:
            old_version = self.meta_backend.get_version(from_version_uid)  # raise if not exists
            if not old_version.valid:
                raise RuntimeError('You cannot base on an invalid version.')
            old_blocks = self.meta_backend.get_blocks_by_version(from_version_uid)
        else:
            old_blocks = None
        size = math.ceil(size_bytes / self.block_size)
        # we always start with invalid versions, then validate them after backup
        version_uid = self.meta_backend.set_version(name, size, size_bytes, 0)
        for id in range(size):
            if old_blocks:
                try:
                    old_block = old_blocks[id]
                except IndexError:
                    uid = None
                    checksum = None
                    block_size = self.block_size
                    valid = 1
                else:
                    assert old_block.id == id
                    uid = old_block.uid
                    checksum = old_block.checksum
                    block_size = old_block.size
                    valid = old_block.valid
            else:
                uid = None
                checksum = None
                block_size = self.block_size
                valid = 1

            # the last block can differ in size, so let's check
            _offset = id * self.block_size
            new_block_size = min(block_size, size_bytes - _offset)
            if new_block_size != block_size:
                # last block changed, so set back all info
                block_size = new_block_size
                uid = None
                checksum = None
                valid = 1

            self.meta_backend.set_block(
                id,
                version_uid,
                uid,
                checksum,
                block_size,
                valid,
                _commit=False)
        self.meta_backend._commit()
        #logger.info('New version: {}'.format(version_uid))
        return version_uid


    def ls(self):
        versions = self.meta_backend.get_versions()
        return versions


    def ls_version(self, version_uid):
        blocks = self.meta_backend.get_blocks_by_version(version_uid)
        return blocks


    def scrub(self, version_uid, source=None, percentile=100):
        """ Returns a boolean (state). If False, there were errors, if True
        all was ok
        """
        self.meta_backend.get_version(version_uid)  # raise if version not exists
        blocks = self.meta_backend.get_blocks_by_version(version_uid)
        if source:
            source_file = open(source, 'rb')
        else:
            source_file = None

        state = True
        for block in blocks:
            if block.uid:
                if percentile < 100 and random.randint(1, 100) > percentile:
                    logger.debug('Scrub of block {} (UID {}) skipped (percentile is {}).'.format(
                        block.id,
                        block.uid,
                        percentile,
                        ))
                    continue
                data = self.data_backend.read(block.uid)
                assert len(data) == block.size
                data_checksum = HASH_FUNCTION(data).hexdigest()
                if data_checksum != block.checksum:
                    logger.error('Checksum mismatch during scrub for block '
                        '{} (UID {}) (is: {} should-be: {}).'.format(
                            block.id,
                            block.uid,
                            data_checksum,
                            block.checksum,
                            ))
                    self.meta_backend.set_blocks_invalid(block.uid, block.checksum)
                    state = False
                else:
                    if source_file:
                        source_file.seek(block.id * self.block_size)
                        source_data = source_file.read(block.size)
                        if source_data != data:
                            logger.error('Source data has changed for block {} '
                                '(UID {}) (is: {} should-be: {}'.format(
                                    block.id,
                                    block.uid,
                                    HASH_FUNCTION(source_data).hexdigest(),
                                    data_checksum,
                                    ))
                            state = False
                    logger.debug('Scrub of block {} (UID {}) ok.'.format(
                        block.id,
                        block.uid,
                        ))
            else:
                logger.debug('Scrub of block {} (UID {}) skipped (sparse).'.format(
                    block.id,
                    block.uid,
                    ))
        return state


    def restore(self, version_uid, target, sparse=False):
        version = self.meta_backend.get_version(version_uid)  # raise if version not exists
        blocks = self.meta_backend.get_blocks_by_version(version_uid)
        with open(target, 'wb') as f:
            for block in blocks:
                f.seek(block.id * self.block_size)
                if block.uid:
                    data = self.data_backend.read(block.uid)
                    assert len(data) == block.size
                    data_checksum = HASH_FUNCTION(data).hexdigest()
                    written = f.write(data)
                    assert written == len(data)
                    if data_checksum != block.checksum:
                        logger.error('Checksum mismatch during restore for block '
                            '{} (is: {} should-be: {}, block-valid: {}). Block '
                            'restored is invalid. Continuing.'.format(
                                block.id,
                                data_checksum,
                                block.checksum,
                                block.valid,
                                ))
                        self.meta_backend.set_blocks_invalid(block.uid, block.checksum)
                    else:
                        logger.debug('Restored block {} successfully ({} bytes).'.format(
                            block.id,
                            block.size,
                            ))
                elif not sparse:
                    f.write(b'\0'*block.size)
                    logger.debug('Restored sparse block {} successfully ({} bytes).'.format(
                        block.id,
                        block.size,
                        ))
                else:
                    logger.debug('Ignored sparse block {}.'.format(
                        block.id,
                        ))
            if f.tell() != version.size_bytes:
                # write last byte with \0, because this can only happen when
                # the last block was left over in sparse mode.
                last_block = blocks[-1]
                f.seek(last_block.id * self.block_size + last_block.size - 1)
                f.write(b'\0')


    def rm(self, version_uid):
        self.meta_backend.get_version(version_uid)  # just to raise if not exists
        num_blocks = self.meta_backend.rm_version(version_uid)
        logger.info('Removed backup version {} with {} blocks.'.format(
            version_uid,
            num_blocks,
            ))


    def backup(self, name, source, hints, from_version):
        """ Create a backup from source.
        If hints are given, they must be tuples of (offset, length, exists)
        where offset and length are integers and exists is a boolean. Then, only
        data within hints will be backed up.
        Otherwise, the backup reads source and looks if checksums match with
        the target.
        """
        with open(source, 'rb') as source_file:
            # determine source size
            source_file.seek(0, 2)  # to the end
            source_size = source_file.tell()
            source_file.seek(0)
            size = math.ceil(source_size / self.block_size)

            # Sanity check: check hints for validity, i.e. too high offsets, ...
            if hints:
                max_offset = max([h[0]+h[1] for h in hints])
                if max_offset > source_size:
                    raise ValueError('Hints have higher offsets than source file.')

            if hints:
                sparse_blocks = blocks_from_hints([hint for hint in hints if not hint[2]], self.block_size)
                read_blocks = blocks_from_hints([hint for hint in hints if hint[2]], self.block_size)
            else:
                sparse_blocks = []
                read_blocks = range(size)
            sparse_blocks = set(sparse_blocks)
            read_blocks = set(read_blocks)

            try:
                version_uid = self._prepare_version(name, source_size, from_version)
            except RuntimeError as e:
                logger.error(str(e))
                logger.error('Backy exiting.')
                # TODO: Don't exit here, exit in Commands
                exit(1)
            blocks = self.meta_backend.get_blocks_by_version(version_uid)

            for block in blocks:
                if block.id in read_blocks or not block.valid:
                    source_file.seek(block.id * self.block_size)
                    data = source_file.read(self.block_size)
                    if not data:
                        raise RuntimeError('EOF reached on source when there should be data.')

                    data_checksum = HASH_FUNCTION(data).hexdigest()
                    if not block.valid:
                        logger.debug('Re-read block (bacause it was invalid) {} (checksum {})'.format(block.id, data_checksum))
                    else:
                        logger.debug('Read block {} (checksum {})'.format(block.id, data_checksum))

                    # dedup
                    existing_block = self.meta_backend.get_block_by_checksum(data_checksum)
                    if existing_block and existing_block.size == len(data):
                        self.meta_backend.set_block(block.id, version_uid, existing_block.uid, data_checksum, len(data), valid=1)
                        logger.debug('Found existing block for id {} with uid {})'.format
                                (block.id, existing_block.uid))
                    else:
                        block_uid = self.data_backend.save(data)
                        self.meta_backend.set_block(block.id, version_uid, block_uid, data_checksum, len(data), valid=1)
                        logger.debug('Wrote block {} (checksum {})'.format(block.id, data_checksum))
                elif block.id in sparse_blocks:
                    # This "elif" is very important. Because if the block is in read_blocks
                    # AND sparse_blocks, it *must* be read.
                    self.meta_backend.set_block(block.id, version_uid, None, None, block.size, valid=1)
                    logger.debug('Skipping block (sparse) {}'.format(block.id))
                else:
                    logger.debug('Keeping block {}'.format(block.id))
        self.meta_backend.set_version_valid(version_uid)
        logger.info('New version: {}'.format(version_uid))
        return version_uid


    def cleanup(self):
        """ Delete unreferenced blob UIDs """
        active_block_uids = set(self.meta_backend.get_all_block_uids())
        active_blob_uids = set(self.data_backend.get_all_blob_uids())
        remove_candidates = active_blob_uids.difference(active_block_uids)
        for remove_candidate in remove_candidates:
            logger.debug('Cleanup: Removing UID {}'.format(remove_candidate))
            self.data_backend.rm(remove_candidate)
        logger.info('Cleanup: Removed {} blobs'.format(len(remove_candidates)))


    def close(self):
        self.meta_backend.close()
        self.data_backend.close()



class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path, machine_output, config):
        self.path = path
        self.machine_output = machine_output
        self.config = config

        if config['DB']['type'] == 'sql':
            engine = config['DB']['engine']
            meta_backend = SQLBackend(engine)
        self.backy = partial(Backy, meta_backend=meta_backend)


    def backup(self, name, source, rbd, from_version):
        backy = self.backy(self.path)
        hints = None
        if rbd:
            data = ''.join([line for line in fileinput.input(rbd).readline()])
            hints = hints_from_rbd_diff(data)
        backy.backup(name, source, hints, from_version)


    def restore(self, version_uid, target, sparse):
        backy = self.backy(self.path)
        backy.restore(version_uid, target, sparse)


    def rm(self, version_uid):
        backy = self.backy(self.path)
        backy.rm(version_uid)


    def scrub(self, version_uid, source, percentile):
        if percentile:
            percentile = int(percentile)
        backy = self.backy(self.path)
        state = backy.scrub(version_uid, source, percentile)
        if not state:
            exit(1)


    def _ls_blocks_tbl_output(self, blocks):
        tbl = PrettyTable()
        tbl.field_names = ['id', 'date', 'uid', 'size', 'valid']
        for block in blocks:
            tbl.add_row([
                block.id,
                block.date,
                block.uid,
                block.size,
                int(block.valid),
                ])
        print(tbl)


    def _ls_blocks_machine_output(self, blocks):
        field_names = ['id', 'date', 'uid', 'size', 'valid']
        print(' '.join(field_names))
        for block in blocks:
            print(' '.join(map(str, [
                'block',
                block.id,
                block.date,
                block.uid,
                block.size,
                int(block.valid),
                ])))


    def _ls_versions_tbl_output(self, versions):
        tbl = PrettyTable()
        # TODO: number of invalid blocks, used disk space, shared disk space
        tbl.field_names = ['date', 'name', 'size', 'size_bytes', 'uid', 'version valid']
        for version in versions:
            tbl.add_row([
                version.date,
                version.name,
                version.size,
                version.size_bytes,
                version.uid,
                int(version.valid),
                ])
        print(tbl)


    def _ls_versions_machine_output(self, versions):
        field_names = ['date', 'size', 'size_bytes', 'uid', 'version valid', 'name']
        print(' '.join(field_names))
        for version in versions:
            print(' '.join(map(str, [
                'version',
                version.date,
                version.name,
                version.size,
                version.size_bytes,
                version.uid,
                int(version.valid),
                ])))


    def ls(self, version_uid):
        if version_uid:
            blocks = self.backy(self.path).ls_version(version_uid)
            if self.machine_output:
                self._ls_blocks_machine_output(blocks)
            else:
                self._ls_blocks_tbl_output(blocks)
        else:
            versions = self.backy(self.path).ls()
            if self.machine_output:
                self._ls_versions_machine_output(versions)
            else:
                self._ls_versions_tbl_output(versions)

    def cleanup(self):
        self.backy(self.path).cleanup()


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument(
        '-b', '--backupdir', default='.')
    parser.add_argument(
        '-m', '--machine-output', action='store_true', default=False)

    subparsers = parser.add_subparsers()

    # BACKUP
    p = subparsers.add_parser(
        'backup',
        help="Perform a backup.")
    p.add_argument(
        'source',
        help='Source file')
    p.add_argument(
        'name',
        help='Backup name')
    p.add_argument('-r', '--rbd', default=None, help='Hints as rbd json format')
    p.add_argument('-f', '--from-version', default=None, help='Use this version-uid as base')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers.add_parser(
        'restore',
        help="Restore a given backup with level to a given target.")
    p.add_argument('-s', '--sparse', action='store_true', help='Write restore file sparse (does not work with legacy devices)')
    p.add_argument('version_uid')
    p.add_argument('target')
    p.set_defaults(func='restore')

    # RM
    p = subparsers.add_parser(
        'rm',
        help="Remove a given backup version. This will only remove meta data and you will have to cleanup after this.")
    p.add_argument('version_uid')
    p.set_defaults(func='rm')

    # SCRUB
    p = subparsers.add_parser(
        'scrub',
        help="Scrub a given backup and check for consistency.")
    p.add_argument('-s', '--source', default=None,
        help="Source, optional. If given, check if source matches backup in addition to checksum tests.")
    p.add_argument('-p', '--percentile', default=100,
        help="Only check PERCENTILE percent of the blocks (value 0..100). Default: 100")
    p.add_argument('version_uid')
    p.set_defaults(func='scrub')

    # CLEANUP
    p = subparsers.add_parser(
        'cleanup',
        help="Clean unreferenced blobs.")
    p.set_defaults(func='cleanup')

    # LS
    p = subparsers.add_parser(
        'ls',
        help="List existing backups.")
    p.add_argument('version_uid', nargs='?', default=None, help='Show verbose blocks for this version')
    p.set_defaults(func='ls')

    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(0)

    here = os.path.dirname(os.path.abspath(__file__))
    conffilename = 'backy.cfg'
    conffiles = [
        os.path.join(args.backupdir, conffilename),
        os.path.join('/etc', conffilename),
        os.path.join('/etc', 'backy', conffilename),
        os.path.join(here, conffilename),
        os.path.join(here, '..', conffilename),
        os.path.join(here, '..', '..', conffilename),
        os.path.join(here, '..', '..', '..', conffilename),
        ]
    for conffile in conffiles:
        if args.verbose:
            print("Looking for {}... ".format(conffile), end="")
        if os.path.exists(conffile):
            if args.verbose:
                print("Found.")
            config = Config(CFG, conffile)
            break
        else:
            if args.verbose:
                print("")

    if args.verbose:
        console_level = logging.DEBUG
    #elif args.func == 'scheduler':
        #console_level = logging.INFO
    else:
        console_level = logging.INFO
    init_logging(args.backupdir, console_level)

    commands = Commands(args.backupdir, args.machine_output, config)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']
    del func_args['verbose']
    del func_args['backupdir']
    del func_args['machine_output']

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
