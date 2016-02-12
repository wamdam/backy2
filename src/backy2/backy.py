# -*- encoding: utf-8 -*-

from configparser import ConfigParser  # python 3.3
from functools import partial
from io import (StringIO, BytesIO)
from prettytable import PrettyTable
import argparse
import fileinput
import hashlib
import json
import logging
import math
import os
import random
import sys
import time

from backy2.readers.file import FileReader
from backy2.readers.rbd import RBDReader
from backy2.logging import logger, init_logging
from backy2.data_backends.file import FileBackend
from backy2.data_backends.s3 import S3Backend
from backy2.meta_backends.sql import SQLBackend

import pkg_resources
__version__ = pkg_resources.get_distribution('backy2').version
BLOCK_SIZE = 1024*4096  # 4MB
HASH_FUNCTION = hashlib.sha512

CFG = {
    'DEFAULTS': {
        'logfile': './backy.log',
        },
    'MetaBackend': {
        'type': 'sql',
        'engine': 'sqlite:////tmp/backy.sqlite',
        },
    'DataBackend': {
        'type': 'files',
        'path': '.',
        'aws_access_key_id': '',
        'aws_secret_access_key': '',
        'host': '',
        'port': '',
        'is_secure': '',
        'bucket_name': '',
        'simultaneous_writes': '1',
        },
    'Reader': {
        'type': 'file',
        'simultaneous_reads': '1',
        'ceph_conffile': '',
        },
    'NBD': {
        'cachedir': '/tmp',
        },
    }

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


class Backy():
    """
    """

    def __init__(self, meta_backend, data_backend, reader, block_size=BLOCK_SIZE):
        self.meta_backend = meta_backend
        self.data_backend = data_backend
        self.reader = reader
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
            new_block_size = min(self.block_size, size_bytes - _offset)
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


    def stats(self, version_uid=None):
        stats = self.meta_backend.get_stats(version_uid)
        return stats


    def scrub(self, version_uid, source=None, percentile=100):
        """ Returns a boolean (state). If False, there were errors, if True
        all was ok
        """
        self.meta_backend.get_version(version_uid)  # raise if version not exists
        blocks = self.meta_backend.get_blocks_by_version(version_uid)
        if source:
            self.reader.open(source)

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
                try:
                    data = self.data_backend.read(block.uid)
                except FileNotFoundError as e:
                    logger.error('Blob not found: {}'.format(str(e)))
                    self.meta_backend.set_blocks_invalid(block.uid, block.checksum)
                    state = False
                    continue
                if len(data) != block.size:
                    logger.error('Blob has wrong size: {} is: {} should be: {}'.format(
                        block.uid,
                        len(data),
                        block.size,
                        ))
                    self.meta_backend.set_blocks_invalid(block.uid, block.checksum)
                    state = False
                    continue
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
                    continue
                else:
                    if source:
                        source_data = self.reader.read(block, sync=True)
                        if source_data != data:
                            import pdb; pdb.set_trace()
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
        if state == True:
            self.meta_backend.set_version_valid(version_uid)
        if source:
            self.reader.close()  # wait for all readers

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
        stats = {
                'version_size_bytes': 0,
                'version_size_blocks': 0,
                'bytes_read': 0,
                'blocks_read': 0,
                'bytes_written': 0,
                'blocks_written': 0,
                'bytes_found_dedup': 0,
                'blocks_found_dedup': 0,
                'bytes_sparse': 0,
                'blocks_sparse': 0,
                'start_time': time.time(),
            }
        self.reader.open(source)
        source_size = self.reader.size()

        size = math.ceil(source_size / self.block_size)
        stats['version_size_bytes'] = source_size
        stats['version_size_blocks'] = size

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

        read_jobs = 0
        for block in blocks:
            if block.id in read_blocks or not block.valid:
                self.reader.read(block)  # adds a read job.
                read_jobs += 1
            elif block.id in sparse_blocks:
                # This "elif" is very important. Because if the block is in read_blocks
                # AND sparse_blocks, it *must* be read.
                self.meta_backend.set_block(block.id, version_uid, None, None, block.size, valid=1, _commit=False)
                stats['blocks_sparse'] += 1
                stats['bytes_sparse'] += block.size
                logger.debug('Skipping block (sparse) {}'.format(block.id))
            else:
                logger.debug('Keeping block {}'.format(block.id))

        # now use the readers and write
        for i in range(read_jobs):
            block, data, data_checksum = self.reader.get()

            stats['blocks_read'] += 1
            stats['bytes_read'] += len(data)

            # dedup
            existing_block = self.meta_backend.get_block_by_checksum(data_checksum)
            if existing_block and existing_block.size == len(data):
                self.meta_backend.set_block(block.id, version_uid, existing_block.uid, data_checksum, len(data), valid=1, _commit=False)
                stats['blocks_found_dedup'] += 1
                stats['bytes_found_dedup'] += len(data)
                logger.debug('Found existing block for id {} with uid {})'.format
                        (block.id, existing_block.uid))
            else:
                block_uid = self.data_backend.save(data)
                self.meta_backend.set_block(block.id, version_uid, block_uid, data_checksum, len(data), valid=1, _commit=False)
                stats['blocks_written'] += 1
                stats['bytes_written'] += len(data)
                logger.debug('Wrote block {} (checksum {}...)'.format(block.id, data_checksum[:16]))

        self.reader.close()  # wait for all readers
        self.data_backend.close()  # wait for all writers
        self.meta_backend.set_version_valid(version_uid)
        self.meta_backend.set_stats(
            version_uid=version_uid,
            version_name=name,
            version_size_bytes=stats['version_size_bytes'],
            version_size_blocks=stats['version_size_blocks'],
            bytes_read=stats['bytes_read'],
            blocks_read=stats['blocks_read'],
            bytes_written=stats['bytes_written'],
            blocks_written=stats['blocks_written'],
            bytes_found_dedup=stats['bytes_found_dedup'],
            blocks_found_dedup=stats['blocks_found_dedup'],
            bytes_sparse=stats['bytes_sparse'],
            blocks_sparse=stats['blocks_sparse'],
            duration_seconds=int(time.time() - stats['start_time']),
            )
        logger.info('New version: {}'.format(version_uid))
        return version_uid


    def cleanup(self):
        """ Delete unreferenced blob UIDs """
        active_blob_uids = set(self.data_backend.get_all_blob_uids())
        active_block_uids = set(self.meta_backend.get_all_block_uids())
        remove_candidates = active_blob_uids.difference(active_block_uids)
        for remove_candidate in remove_candidates:
            logger.debug('Cleanup: Removing UID {}'.format(remove_candidate))
            self.data_backend.rm(remove_candidate)
        logger.info('Cleanup: Removed {} blobs'.format(len(remove_candidates)))


    def close(self):
        self.meta_backend.close()
        self.data_backend.close()


    def export(self, version_uid, f):
        self.meta_backend.export(version_uid, f)
        return f


    def import_(self, f):
        self.meta_backend.import_(f)


class BackyStore():
    """ Makes backy storage look linear.
    Also has a COW method.
    """

    def __init__(self, backy, cachedir):
        self.backy = backy
        self.cachedir = cachedir
        self.blocks = {}  # block list cache by version
        self.block_cache = set()
        self.cow = {}  # contains version_uid: dict() of block id -> uid


    def get_versions(self):
        return self.backy.ls()


    def get_version(self, uid):
        return self.backy.meta_backend.get_version(uid)


    def _block_list(self, version_uid, offset, length):
        # get cached blocks data
        if not self.blocks.get(version_uid):
            self.blocks[version_uid] = self.backy.meta_backend.get_blocks_by_version(version_uid)
        blocks = self.blocks[version_uid]

        block_number = offset // self.backy.block_size
        block_offset = offset % self.backy.block_size

        read_list = []
        while True:
            try:
                block = blocks[block_number]
            except IndexError:
                # In case the backup file is not a multiple of 4096 in size,
                # we need to fake blocks to the end until it matches. That means,
                # that we return b'\0' until the block size is reached.
                # This is a nbd (or even block device) limitation
                block = None
                read_length = length
                read_list.append((None, 0, length))  # hint: return \0s
            else:
                assert block.id == block_number
                read_length = min(block.size-block_offset, length)
                read_list.append((block, block_offset, read_length))
            block_number += 1
            block_offset = 0
            length -= read_length
            assert length >= 0
            if length == 0:
                break

        return read_list


    def _read(self, block_uid, offset=0, length=None):
        if self.backy.data_backend._SUPPORTS_PARTIAL_READS:
            return self.backy.data_backend.read(block_uid, offset, length)
        else:
            if block_uid not in self.block_cache:
                data = self.backy.data_backend.read(block_uid)
                open(os.path.join(self.cachedir, block_uid), 'wb').write(data)
                self.block_cache.add(block_uid)
            with open(os.path.join(self.cachedir, block_uid), 'rb') as f:
                f.seek(offset)
                if length is None:
                    return f.read()
                else:
                    return f.read(length)


    def read(self, version_uid, offset, length):
        read_list = self._block_list(version_uid, offset, length)
        data = []
        for block, offset, length in read_list:
            if block is None:
                data.append(b'\0'*length)
            else:
                data.append(self._read(block.uid, offset, length))
        return b''.join(data)


    def get_cow_version(self, from_version):
        cow_version_uid = self.backy._prepare_version(
            'cow from {}'.format(from_version.uid),
            from_version.size_bytes, from_version.uid)
        self.cow[cow_version_uid] = {}  # contains version_uid: dict() of block id -> uid
        return cow_version_uid


    def _update(self, block_uid, data, offset=0):
        # update a given block_uid
        if self.backy.data_backend._SUPPORTS_PARTIAL_WRITES:
            return self.backy.data_backend.update(block_uid, data, offset)
        else:
            # update local copy
            with open(os.path.join(self.cachedir, block_uid), 'r+b') as f:
                f.seek(offset)
                return f.write(data)


    def _save(self, data):
        # update a given block_uid
        if self.backy.data_backend._SUPPORTS_PARTIAL_WRITES:
            return self.backy.data_backend.save(data, _sync=True)  # returns block uid
        else:
            new_uid = self.backy.data_backend._uid()
            with open(os.path.join(self.cachedir, new_uid), 'wb') as f:
                f.write(data)
            self.block_cache.add(new_uid)
            return new_uid


    def write(self, version_uid, offset, data):
        """ Copy on write backup writer """
        dataio = BytesIO(data)
        cow = self.cow[version_uid]
        write_list = self._block_list(version_uid, offset, len(data))
        for block, _offset, length in write_list:
            if block is None:
                logger.warning('Tried to save data beyond device (offset {})'.format(offset))
                continue  # raise? That'd be a write outside the device...
            if block.id in cow:
                # the block is already copied, so update it.
                block_uid = cow[block.id]
                self._update(block_uid, dataio.read(length), _offset)
                logger.debug('Updated cow changed block {} into {})'.format(block.id, block_uid))
            else:
                # read the block from the original, update it and write it back
                write_data = BytesIO(self.backy.data_backend.read(block.uid))
                write_data.seek(_offset)
                write_data.write(dataio.read(length))
                write_data.seek(0)
                block_uid = self._save(write_data.read())
                cow[block.id] = block_uid
                logger.debug('Wrote cow changed block {} into {})'.format(block.id, block_uid))


    def flush(self):
        # TODO: Maybe fixate partly?
        pass


    def fixate(self, cow_version_uid):
        # save blocks into version
        logger.info('Fixating version {}'.format(cow_version_uid))
        for block_id, block_uid in self.cow[cow_version_uid].items():
            logger.debug('Fixating block {} uid {}'.format(block_id, block_uid))
            data = self._read(block_uid)
            checksum = HASH_FUNCTION(data).hexdigest()
            if not self.backy.data_backend._SUPPORTS_PARTIAL_WRITES:
                # dump changed data
                new_uid = self.backy.data_backend.save(data, _sync=True)
                logger.debug('Stored block {} with local uid {} to uid {}'.format(block_id, block_uid, new_uid))
                block_uid = new_uid

            self.backy.meta_backend.set_block(block_id, cow_version_uid, block_uid, checksum, len(data), valid=1, _commit=False)
        self.backy.meta_backend.set_version_valid(cow_version_uid)
        self.backy.meta_backend._commit()
        # TODO: Delete COW blocks and also those from block_cache
        if self.backy.data_backend._SUPPORTS_PARTIAL_WRITES:
            for block_uid in self.block_cache:
                # TODO if this block is in the current version (and in no other?)
                # rm this block from cache
                # rm block uid from self.block_cache
                pass
            for block_id, block_uid in self.cow[cow_version_uid].items():
                # TODO: rm block_uid from cache
                pass
        else:
            # backends that support partial writes will be written to directly.
            # So there's no need to cleanup.
            pass
        del(self.cow[cow_version_uid])


class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, config):
        self.machine_output = machine_output
        self.config = config

        # configure meta backend
        if config['MetaBackend']['type'] == 'sql':
            engine = config['MetaBackend']['engine']
            meta_backend = SQLBackend(engine)
        else:
            raise NotImplementedError('MetaBackend type {} unsupported.'.format(config['MetaBackend']['type']))

        # configure file backend
        if config['DataBackend']['type'] == 'files':
            data_backend = FileBackend(
                    config['DataBackend']['path'],
                    simultaneous_writes=int(config['DataBackend']['simultaneous_writes']),
                    )
        elif config['DataBackend']['type'] == 's3':
            data_backend = S3Backend(
                    aws_access_key_id=config['DataBackend']['aws_access_key_id'],
                    aws_secret_access_key=config['DataBackend']['aws_secret_access_key'],
                    host=config['DataBackend']['host'],
                    port=int(config['DataBackend']['port']),
                    is_secure=True if config['DataBackend']['is_secure'] in ('True', 'true', '1') else False,
                    bucket_name=config['DataBackend']['bucket_name'],
                    simultaneous_writes=int(config['DataBackend']['simultaneous_writes']),
                    )

        if config['Reader']['type'] == 'file':
            reader = FileReader(
                    simultaneous_reads=int(config['Reader']['simultaneous_reads']),
                    block_size=BLOCK_SIZE,
                    hash_function=HASH_FUNCTION,
                    )
        elif config['Reader']['type'] == 'librbd':
            reader = RBDReader(
                    simultaneous_reads=int(config['Reader']['simultaneous_reads']),
                    ceph_conffile=config['Reader']['ceph_conffile'],
                    block_size=BLOCK_SIZE,
                    hash_function=HASH_FUNCTION,
                    )

        self.backy = partial(Backy,
                meta_backend=meta_backend,
                data_backend=data_backend,
                reader=reader,
                )


    def backup(self, name, source, rbd, from_version):
        backy = self.backy()
        hints = None
        if rbd:
            data = ''.join([line for line in fileinput.input(rbd).readline()])
            hints = hints_from_rbd_diff(data)
        backy.backup(name, source, hints, from_version)
        backy.close()


    def restore(self, version_uid, target, sparse):
        backy = self.backy()
        backy.restore(version_uid, target, sparse)
        backy.close()


    def rm(self, version_uid):
        backy = self.backy()
        backy.rm(version_uid)
        backy.close()


    def scrub(self, version_uid, source, percentile):
        if percentile:
            percentile = int(percentile)
        backy = self.backy()
        state = backy.scrub(version_uid, source, percentile)
        backy.close()
        if not state:
            exit(1)


    def _ls_blocks_tbl_output(self, blocks):
        tbl = PrettyTable()
        tbl.field_names = ['id', 'date', 'uid', 'size', 'valid']
        tbl.align['id'] = 'r'
        tbl.align['size'] = 'r'
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
        field_names = ['type', 'id', 'date', 'uid', 'size', 'valid']
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
        tbl.field_names = ['date', 'name', 'size', 'size_bytes', 'uid',
                'version valid']
        tbl.align['name'] = 'l'
        tbl.align['size'] = 'r'
        tbl.align['size_bytes'] = 'r'
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
        field_names = ['type', 'date', 'size', 'size_bytes', 'uid', 'version valid', 'name']
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


    def _stats_tbl_output(self, stats):
        tbl = PrettyTable()
        tbl.field_names = ['date', 'uid', 'name', 'size bytes', 'size blocks',
                'bytes read', 'blocks read', 'bytes written', 'blocks written',
                'bytes dedup', 'blocks dedup', 'bytes sparse', 'blocks sparse',
                'duration (s)']
        tbl.align['name'] = 'l'
        tbl.align['size bytes'] = 'r'
        tbl.align['size blocks'] = 'r'
        tbl.align['bytes read'] = 'r'
        tbl.align['blocks read'] = 'r'
        tbl.align['bytes written'] = 'r'
        tbl.align['blocks written'] = 'r'
        tbl.align['bytes dedup'] = 'r'
        tbl.align['blocks dedup'] = 'r'
        tbl.align['bytes sparse'] = 'r'
        tbl.align['blocks sparse'] = 'r'
        tbl.align['duration (s)'] = 'r'
        for stat in stats:
            tbl.add_row([
                stat.date,
                stat.version_uid,
                stat.version_name,
                stat.version_size_bytes,
                stat.version_size_blocks,
                stat.bytes_read,
                stat.blocks_read,
                stat.bytes_written,
                stat.blocks_written,
                stat.bytes_found_dedup,
                stat.blocks_found_dedup,
                stat.bytes_sparse,
                stat.blocks_sparse,
                stat.duration_seconds,
                ])
        print(tbl)


    def _stats_machine_output(self, stats):
        field_names = ['type', 'date', 'uid', 'name', 'size bytes', 'size blocks',
                'bytes read', 'blocks read', 'bytes written', 'blocks written',
                'bytes dedup', 'blocks dedup', 'bytes sparse', 'blocks sparse',
                'duration (s)']
        print(' '.join(field_names))
        for stat in stats:
            print(' '.join(map(str, [
                'statistics',
                stat.date,
                stat.version_uid,
                stat.version_name,
                stat.version_size_bytes,
                stat.version_size_blocks,
                stat.bytes_read,
                stat.blocks_read,
                stat.bytes_written,
                stat.blocks_written,
                stat.bytes_found_dedup,
                stat.blocks_found_dedup,
                stat.bytes_sparse,
                stat.blocks_sparse,
                stat.duration_seconds,
                ])))


    def ls(self, version_uid):
        backy = self.backy()
        if version_uid:
            blocks = backy.ls_version(version_uid)
            if self.machine_output:
                self._ls_blocks_machine_output(blocks)
            else:
                self._ls_blocks_tbl_output(blocks)
        else:
            versions = backy.ls()
            if self.machine_output:
                self._ls_versions_machine_output(versions)
            else:
                self._ls_versions_tbl_output(versions)
        backy.close()


    def stats(self, version_uid):
        backy = self.backy()
        stats = backy.stats(version_uid)
        if self.machine_output:
            self._stats_machine_output(stats)
        else:
            self._stats_tbl_output(stats)
        backy.close()


    def cleanup(self):
        backy = self.backy()
        backy.cleanup()
        backy.close()


    def export(self, version_uid, filename='-'):
        backy = self.backy()
        if filename == '-':
            f = StringIO()
            backy.export(version_uid, f)
            f.seek(0)
            print(f.read())
            f.close()
        else:
            with open(filename, 'w') as f:
                backy.export(version_uid, f)
        backy.close()


    def nbd(self, version_uid, bind_address, bind_port, read_only):
        from .enterprise.nbdserver import Server as NbdServer
        backy = self.backy()
        store = BackyStore(backy, cachedir=self.config['NBD']['cachedir'])
        addr = (bind_address, bind_port)
        server = NbdServer(addr, store, read_only)
        logger.info("Starting to serve nbd on %s:%s" % (addr[0], addr[1]))
        logger.info("You may now start")
        logger.info("  nbd-client -l %s -p %s" % (addr[0], addr[1]))
        logger.info("and then get the backup via")
        logger.info("  modprobe nbd")
        logger.info("  nbd-client -N <version> %s -p %s /dev/nbd0" % (addr[0], addr[1]))
        server.serve_forever()


    def import_(self, filename='-'):
        backy = self.backy()
        try:
            if filename=='-':
                backy.import_(sys.stdin)
            else:
                with open(filename, 'r') as f:
                    backy.import_(f)
        except KeyError as e:
            logger.error(str(e))
            exit(1)
        except ValueError as e:
            logger.error(str(e))
            exit(2)
        finally:
            backy.close()


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument(
        '-m', '--machine-output', action='store_true', default=False)
    parser.add_argument(
        '-V', '--version', action='store_true', help='Show version')

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

    # Export
    p = subparsers.add_parser(
        'export',
        help="Export the metadata of a backup uid into a file.")
    p.add_argument('version_uid')
    p.add_argument('filename', help="Export into this filename ('-' is for stdout)")
    p.set_defaults(func='export')

    # Import
    p = subparsers.add_parser(
        'import',
        help="Import the metadata of a backup from a file.")
    p.add_argument('filename', help="Read from this file ('-' is for stdin)")
    p.set_defaults(func='import_')

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

    # STATS
    p = subparsers.add_parser(
        'stats',
        help="Show statistics")
    p.add_argument('version_uid', nargs='?', default=None, help='Show statistics for this version')
    p.set_defaults(func='stats')

    # NBD
    p = subparsers.add_parser(
        'nbd',
        help="Start an nbd server")
    p.add_argument('version_uid', nargs='?', default=None, help='Start an nbd server for this version')
    p.add_argument('-a', '--bind-address', default='127.0.0.1',
            help="Bind to this ip address (default: 127.0.0.1)")
    p.add_argument('-p', '--bind-port', default=10809,
            help="Bind to this port (default: 10809)")
    p.add_argument(
        '-r', '--read-only', action='store_true', default=False,
        help='Read only if set, otherwise a copy on write backup is created.')
    p.set_defaults(func='nbd')

    args = parser.parse_args()

    if args.version:
        print(__version__)
        exit(0)

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(0)

    here = os.path.dirname(os.path.abspath(__file__))
    conffilename = 'backy.cfg'
    conffiles = [
        os.path.join('/etc', conffilename),
        os.path.join('/etc', 'backy', conffilename),
        conffilename,
        os.path.join('..', conffilename),
        os.path.join('..', '..', conffilename),
        os.path.join('..', '..', '..', conffilename),
        os.path.join(here, conffilename),
        os.path.join(here, '..', conffilename),
        os.path.join(here, '..', '..', conffilename),
        os.path.join(here, '..', '..', '..', conffilename),
        ]
    config = None

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
    if not config:
        logger.warn("Running without conffile. Consider adding one at /etc/backy.cfg")
        config = Config(CFG)

    if args.verbose:
        console_level = logging.DEBUG
    #elif args.func == 'scheduler':
        #console_level = logging.INFO
    else:
        console_level = logging.INFO
    init_logging(config['DEFAULTS']['logfile'], console_level)

    commands = Commands(args.machine_output, config)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']
    del func_args['verbose']
    del func_args['version']
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
