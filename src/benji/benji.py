# -*- encoding: utf-8 -*-

import datetime
import errno
import hashlib
import importlib
import math
import os
import random
import time
from concurrent.futures import CancelledError, TimeoutError
from io import StringIO, BytesIO
from urllib import parse

from benji.exception import InputDataError, InternalError, AlreadyLocked, UsageError, NoChange, ConfigurationError, \
    ScrubbingError
from benji.logging import logger
from benji.metadata import BlockUid, MetadataBackend
from benji.retentionfilter import RetentionFilter
from benji.utils import data_hexdigest, notify, parametrized_hash_function


def blocks_from_hints(hints, block_size):
    """ Helper method """
    sparse_blocks = set()
    read_blocks = set()
    for offset, length, exists in hints:
        start_block = offset // block_size
        end_block = (offset + length - 1) // block_size
        if exists:
            for i in range(start_block, end_block + 1):
                read_blocks.add(i)
        else:
            if offset % block_size > 0:
                # Start block is only partially sparse, make sure it is read
                read_blocks.add(start_block)

            if (offset + length) % block_size > 0:
                # End block is only partially sparse, make sure it is read
                read_blocks.add(end_block)

            for i in range(start_block, end_block + 1):
                sparse_blocks.add(i)

    return sparse_blocks, read_blocks


class Benji:

    def __init__(self, config, block_size=None, initdb=False, in_memory=False, _destroydb=False, _migratedb=True):

        self.config = config

        if block_size is None:
            self._block_size = config.get('blockSize', types=int)
        else:
            self._block_size = block_size

        self._hash_function = parametrized_hash_function(config.get('hashFunction', types=str))
        self._process_name = config.get('processName', types=str)

        from benji.data_backends import DataBackend
        name = config.get('dataBackend.type', types=str)
        try:
            self._data_backend_module = importlib.import_module('{}.{}'.format(DataBackend.PACKAGE_PREFIX, name))
        except ImportError:
            raise ConfigurationError('Data backend type {} not found.'.format(name))
        self._data_backend_object = None

        metadata_backend = MetadataBackend(config, in_memory=in_memory)
        if initdb or in_memory:
            metadata_backend.initdb(_destroydb=_destroydb, _migratedb=_migratedb)

        self._metadata_backend = metadata_backend.open(_migratedb=_migratedb)
        self._locking = self._metadata_backend.locking()

        notify(self._process_name)  # i.e. set process name without notification

        if self._locking.is_locked():
            raise AlreadyLocked('Another instance is already running.')

    # This implements a lazy creation of the data backend object so that any network connections
    # are only opened and expensive encryption calculations are only done when the object is really used.
    @property
    def _data_backend(self):
        if not self._data_backend_object:
            self._data_backend_object = self._data_backend_module.DataBackend(self.config)
        return self._data_backend_object

    def _clone_version(self, name, snapshot_name, size=None, base_version_uid=None):
        """ Prepares the metadata for a new version.
        If from_version_uid is given, this is taken as the base, otherwise
        a pure sparse version is created.
        """
        if base_version_uid:
            old_version = self._metadata_backend.get_version(base_version_uid)  # raise if not exists
            if not old_version.valid:
                raise UsageError('You cannot base new version on an invalid one.')
            if old_version.block_size != self._block_size:
                raise UsageError('You cannot base new version on an old version with a different block size.')
            old_blocks = self._metadata_backend.get_blocks_by_version(base_version_uid)
            if not size:
                size = old_version.size
        else:
            old_blocks = None
            if not size:
                raise InternalError('Size needs to be specified if there is no base version.')

        num_blocks = int(math.ceil(size / self._block_size))

        try:
            # we always start with invalid versions, then validate them after backup
            version = self._metadata_backend.create_version(
                version_name=name,
                snapshot_name=snapshot_name,
                size=size,
                block_size=self._block_size,
                valid=False,
                protected=True)
            self._locking.lock_version(version.uid, reason='Preparing version')
            self._metadata_backend.set_version(version.uid, protected=False)

            for id in range(num_blocks):
                if old_blocks:
                    try:
                        old_block = old_blocks[id]
                    except IndexError:
                        uid = None
                        checksum = None
                        block_size = self._block_size
                        valid = True
                    else:
                        assert old_block.id == id
                        uid = old_block.uid
                        checksum = old_block.checksum
                        block_size = old_block.size
                        valid = old_block.valid
                else:
                    uid = None
                    checksum = None
                    block_size = self._block_size
                    valid = True

                # the last block can differ in size, so let's check
                _offset = id * self._block_size
                new_block_size = min(self._block_size, size - _offset)
                if new_block_size != block_size:
                    # last block changed, so set back all info
                    block_size = new_block_size
                    uid = None
                    checksum = None
                    valid = False

                self._metadata_backend.set_block(id, version.uid, uid, checksum, block_size, valid, upsert=False)
                notify(self._process_name, 'Preparing version {} ({:.1f}%)'.format(version.uid.readable,
                                                                                   (id + 1) / num_blocks * 100))

            self._metadata_backend.commit()
        except:
            self._metadata_backend.rollback()
            if version:
                self._metadata_backend.set_version(version.uid, protected=False)
            if self._locking.is_version_locked(version.uid):
                self._locking.unlock_version(version.uid)
            raise
        finally:
            notify(self._process_name)

        return version

    def ls(self, version_uid=None, version_name=None, version_snapshot_name=None, version_tags=None):
        return self._metadata_backend.get_versions(
            version_uid=version_uid,
            version_name=version_name,
            version_snapshot_name=version_snapshot_name,
            version_tags=version_tags)

    def ls_version(self, version_uid):
        # don't lock here, this is not really error-prone.
        return self._metadata_backend.get_blocks_by_version(version_uid)

    def stats(self, version_uid=None, limit=None):
        return self._metadata_backend.get_stats(version_uid, limit)

    def _get_io_by_source(self, source, block_size):
        res = parse.urlparse(source)

        if res.params or res.query or res.fragment:
            raise UsageError('The supplied URL {} is invalid.'.format(source))

        scheme = res.scheme
        if not scheme:
            raise UsageError('The supplied URL {} is invalid. You must provide a scheme (e.g. file://).'.format(source))

        try:
            from benji.io import IO
            IOLib = importlib.import_module('{}.{}'.format(IO.PACKAGE_PREFIX, scheme))
        except ImportError:
            raise UsageError('IO scheme {} not supported.'.format(scheme))

        return IOLib.IO(
            config=self.config,
            block_size=block_size,
            hash_function=self._hash_function,
        )

    def scrub(self, version_uid, block_percentage=100):
        self._locking.lock_version(version_uid, reason='Scrubbing version')
        try:
            version = self._metadata_backend.get_version(version_uid)
            if not version.valid:
                raise ScrubbingError('Version {} is already marked as invalid.'.format(version_uid.readable))
            blocks = self._metadata_backend.get_blocks_by_version(version_uid)
        except:
            self._locking.unlock_version(version_uid)
            raise

        valid = True
        try:
            read_jobs = 0
            for i, block in enumerate(blocks):
                if block.uid:
                    if block_percentage < 100 and random.randint(1, 100) > block_percentage:
                        logger.debug('Scrub of block {} (UID {}) skipped (percentile is {}).'.format(
                            block.id, block.uid, block_percentage))
                    else:
                        self._data_backend.read(block.deref(), metadata_only=True)  # async queue
                        read_jobs += 1
                else:
                    logger.debug('Scrub of block {} (UID {}) skipped (sparse).'.format(block.id, block.uid))
                notify(
                    self._process_name, 'Preparing scrub of version {} ({:.1f}%)'.format(
                        version.uid.readable, (i + 1) / len(blocks) * 100))

            done_read_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent
            for entry in self._data_backend.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    logger.error('Data backend read failed: {}'.format(entry))
                    if isinstance(entry, (KeyError, ValueError)):
                        self._metadata_backend.set_blocks_invalid(block.uid)
                        valid = False
                        continue
                    else:
                        raise entry
                else:
                    block, data, metadata = entry

                try:
                    self._data_backend.check_block_metadata(block=block, data_length=None, metadata=metadata)
                except (KeyError, ValueError) as exception:
                    logger.error('Metadata check failed, block is invalid: {}'.format(exception))
                    self._metadata_backend.set_blocks_invalid(block.uid)
                    valid = False
                    continue
                except:
                    raise

                logger.debug('Scrub of block {} (UID {}) ok.'.format(block.id, block.uid))
                notify(self._process_name, 'Scrubbing version {} ({:.1f}%)'.format(version_uid.readable,
                                                                                   done_read_jobs / read_jobs * 100))
                if i % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Scrubbed {}/{} blocks ({:.1f}%)'.format(done_read_jobs, read_jobs,
                                                                         done_read_jobs / read_jobs * 100))
        except:
            raise
        finally:
            self._locking.unlock_version(version_uid)
            notify(self._process_name)

        if read_jobs != done_read_jobs:
            raise InternalError('Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'
                                .format(read_jobs, done_read_jobs))

        # A scrub (in contrast to a deep-scrub) can only ever mark a version as invalid. To mark it as valid
        # there is not enough information.
        if not valid:
            # version is set invalid by set_blocks_invalid.
            logger.error('Marked version {} as invalid because it has errors.'.format(version_uid.readable))

        if not valid:
            raise ScrubbingError('Scrub of version {} failed.'.format(version_uid.readable))

    def deep_scrub(self, version_uid, source=None, block_percentage=100):
        self._locking.lock_version(version_uid, reason='Deep scrubbing')
        try:
            version = self._metadata_backend.get_version(version_uid)
            if not version.valid and block_percentage < 100:
                raise ScrubbingError('Version {} is already marked as invalid.'.format(version_uid.readable))
            blocks = self._metadata_backend.get_blocks_by_version(version_uid)

            if source:
                io = self._get_io_by_source(source, version.block_size)
                io.open_r(source)
        except:
            self._locking.unlock_version(version_uid)
            raise

        valid = True
        try:
            old_use_read_cache = self._data_backend.use_read_cache(False)
            read_jobs = 0
            for i, block in enumerate(blocks):
                if block.uid:
                    if block_percentage < 100 and random.randint(1, 100) > block_percentage:
                        logger.debug('Deep scrub of block {} (UID {}) skipped (percentile is {}).'.format(
                            block.id, block.uid, block_percentage))
                    else:
                        self._data_backend.read(block.deref())  # async queue
                        read_jobs += 1
                else:
                    logger.debug('Deep scrub of block {} (UID {}) skipped (sparse).'.format(block.id, block.uid))
                notify(
                    self._process_name, 'Preparing deep scrub of version {} ({:.1f}%)'.format(
                        version.uid.readable, (i + 1) / len(blocks) * 100))

            done_read_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent
            for entry in self._data_backend.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    logger.error('Data backend read failed: {}'.format(entry))
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, (KeyError, ValueError)):
                        self._metadata_backend.set_blocks_invalid(block.uid)
                        valid = False
                        continue
                    else:
                        raise entry
                else:
                    block, data, metadata = entry

                try:
                    self._data_backend.check_block_metadata(block=block, data_length=len(data), metadata=metadata)
                except (KeyError, ValueError) as exception:
                    logger.error('Metadata check failed, block is invalid: {}'.format(exception))
                    self._metadata_backend.set_blocks_invalid(block.uid)
                    valid = False
                    continue
                except:
                    raise

                data_checksum = data_hexdigest(self._hash_function, data)
                if data_checksum != block.checksum:
                    logger.error('Checksum mismatch during deep scrub of block {} (UID {}) (is: {}... should-be: {}...).'
                                 .format(block.id, block.uid, data_checksum[:16], block.checksum[:16]))
                    self._metadata_backend.set_blocks_invalid(block.uid)
                    valid = False
                    continue

                if source:
                    source_data = io.read(block, sync=True)
                    if source_data != data:
                        logger.error('Source data has changed for block {} (UID {}) (is: {}... should-be: {}...). '
                                     'Won\'t set this block to invalid, because the source looks wrong.'.format(
                                         block, block.uid,
                                         data_hexdigest(self._hash_function, source_data)[:16], data_checksum[:16]))
                        valid = False
                        # We are not setting the block invalid here because
                        # when the block is there AND the checksum is good,
                        # then the source is invalid.

                logger.debug('Deep scrub of block {} (UID {}) ok.'.format(block.id, block.uid))
                notify(self._process_name, 'Deep scrubbing version {} ({:.1f}%)'.format(
                    version_uid.readable, (i + 1) / read_jobs * 100))
                if done_read_jobs % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Deep scrubbed {}/{} blocks ({:.1f}%)'.format(done_read_jobs, read_jobs,
                                                                              done_read_jobs / read_jobs * 100))
        except:
            self._locking.unlock_version(version_uid)
            raise
        finally:
            if source:
                io.close()
            # Restore old read cache setting
            self._data_backend.use_read_cache(old_use_read_cache)
            notify(self._process_name)

        if read_jobs != done_read_jobs:
            raise InternalError('Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'
                                .format(read_jobs, done_read_jobs))

        if valid:
            try:
                self._metadata_backend.set_version(version_uid, valid=True)
            except:
                self._locking.unlock_version(version_uid)
                raise
        else:
            # version is set invalid by set_blocks_invalid.
            # FIXME: This message might be misleading in the case where a source mismatch occurs where
            # FIXME: we set state to False but don't mark any blocks as invalid.
            logger.error('Marked version {} invalid because it has errors.'.format(version_uid.readable))

        self._locking.unlock_version(version_uid)

        if not valid:
            raise ScrubbingError('Deep scrub of version {} failed.'.format(version_uid.readable))

    def restore(self, version_uid, target, sparse=False, force=False):
        self._locking.lock_version(version_uid, reason='Restoring version')
        try:
            version = self._metadata_backend.get_version(version_uid)  # raise if version not exists
            notify(self._process_name, 'Restoring version {} to {}: Getting blocks'.format(
                version_uid.readable, target))
            blocks = self._metadata_backend.get_blocks_by_version(version_uid)

            io = self._get_io_by_source(target, version.block_size)
            io.open_w(target, version.size, force)
        except:
            self._locking.unlock_version(version_uid)
            raise

        try:
            read_jobs = 0
            for i, block in enumerate(blocks):
                if block.uid:
                    self._data_backend.read(block.deref())
                    read_jobs += 1
                elif not sparse:
                    io.write(block, b'\0' * block.size)
                    logger.debug('Restored sparse block {} successfully ({} bytes).'.format(block.id, block.size))
                else:
                    logger.debug('Ignored sparse block {}.'.format(block.id))
                if sparse:
                    notify(
                        self._process_name, 'Restoring version {} to {}: Queueing blocks to read ({:.1f}%)'.format(
                            version_uid.readable, target, (i + 1) / len(blocks) * 100))
                else:
                    notify(
                        self._process_name, 'Restoring version {} to {}: Sparse writing ({:.1f}%)'.format(
                            version_uid.readable, target, (i + 1) / len(blocks) * 100))

            done_read_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent
            for entry in self._data_backend.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    logger.error('Data backend read failed: {}'.format(entry))
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, (KeyError, ValueError)):
                        self._metadata_backend.set_blocks_invalid(block.uid)
                        continue
                    else:
                        raise entry
                else:
                    block, data, metadata = entry

                # Write what we have
                io.write(block, data)

                try:
                    self._data_backend.check_block_metadata(block=block, data_length=len(data), metadata=metadata)
                except (KeyError, ValueError) as exception:
                    logger.error('Metadata check failed, block is invalid: {}'.format(exception))
                    self._metadata_backend.set_blocks_invalid(block.uid)
                    continue
                except:
                    raise

                data_checksum = data_hexdigest(self._hash_function, data)
                if data_checksum != block.checksum:
                    logger.error('Checksum mismatch during restore for block {} (UID {}) (is: {}... should-be: {}..., '
                                 'block.valid: {}). Block restored is invalid.'.format(
                                     block.id, block.uid, data_checksum[:16], block.checksum[:16], block.valid))
                    self._metadata_backend.set_blocks_invalid(block.uid)
                else:
                    logger.debug('Restored block {} successfully ({} bytes).'.format(block.id, block.size))

                notify(
                    self._process_name, 'Restoring version {} to {} ({:.1f}%)'.format(
                        version_uid.readable, target, done_read_jobs / read_jobs * 100))
                if i % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Restored {}/{} blocks ({:.1f}%)'.format(done_read_jobs, read_jobs,
                                                                         done_read_jobs / read_jobs * 100))
        except:
            raise
        finally:
            io.close()
            self._locking.unlock_version(version_uid)
            notify(self._process_name)

        if read_jobs != done_read_jobs:
            raise InternalError('Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'
                                .format(read_jobs, done_read_jobs))

    def protect(self, version_uid):
        version = self._metadata_backend.get_version(version_uid)
        if version.protected:
            raise NoChange('Version {} is already protected.'.format(version_uid.readable))
        self._metadata_backend.set_version(version_uid, protected=True)

    def unprotect(self, version_uid):
        version = self._metadata_backend.get_version(version_uid)
        if not version.protected:
            raise NoChange('Version {} is not protected.'.format(version_uid.readable))
        self._metadata_backend.set_version(version_uid, protected=False)

    def rm(self, version_uid, force=True, disallow_rm_when_younger_than_days=0, keep_backend_metadata=False):
        with self._locking.with_version_lock(version_uid, reason='Removing version'):
            version = self._metadata_backend.get_version(version_uid)

            if version.protected:
                raise RuntimeError('Version {} is protected. Will not delete.'.format(version_uid.readable))

            if not force:
                # check if disallow_rm_when_younger_than_days allows deletion
                age_days = (datetime.datetime.now() - version.date).days
                if disallow_rm_when_younger_than_days > age_days:
                    raise RuntimeError('Version {} is too young. Will not delete.'.format(version_uid.readable))

            num_blocks = self._metadata_backend.rm_version(version_uid)

            if not keep_backend_metadata:
                try:
                    self._data_backend.rm_version(version_uid)
                    logger.info('Removed version {} metadata from backend storage.'.format(version_uid.readable))
                except FileNotFoundError:
                    logger.warning('Unable to remove version {} metadata from backend storage, the object wasn\'t found.'
                                   .format(version_uid.readable))
                    pass

            logger.info('Removed backup version {} with {} blocks.'.format(version_uid.readable, num_blocks))

    def rm_from_backend(self, version_uid):
        with self._locking.with_version_lock(version_uid, reason='Removing version from data backend'):
            self._data_backend.rm_version(version_uid)
        logger.info('Removed backup version {} metadata from backend storage.'.format(version_uid.readable))

    def backup(self, name, snapshot_name, source, hints=None, base_version_uid=None, tags=None):
        """ Create a backup from source.
        If hints are given, they must be tuples of (offset, length, exists)
        where offset and length are integers and exists is a boolean. Then, only
        data within hints will be backed up.
        Otherwise, the backup reads source and looks if checksums match with
        the target.
        """
        stats = {
            'bytes_read': 0,
            'bytes_written': 0,
            'bytes_dedup': 0,
            'bytes_sparse': 0,
            'start_time': time.time(),
        }
        io = self._get_io_by_source(source, self._block_size)
        io.open_r(source)
        source_size = io.size()

        num_blocks = int(math.ceil(source_size / self._block_size))

        if hints is not None:
            if len(hints) > 0:
                # Sanity check: check hints for validity, i.e. too high offsets, ...
                max_offset = max([h[0] + h[1] for h in hints])
                if max_offset > source_size:
                    raise InputDataError('Hints have higher offsets than source file.')

                sparse_blocks, read_blocks = blocks_from_hints(hints, self._block_size)
            else:
                # Two snapshots can be completely identical between one backup and next
                logger.warning('Hints are empty, assuming nothing has changed.')
                sparse_blocks = set()
                read_blocks = set()
        else:
            sparse_blocks = set()
            read_blocks = set(range(num_blocks))

        version = self._clone_version(name, snapshot_name, source_size, base_version_uid)
        self._locking.update_version_lock(version.uid, reason='Backup')
        blocks = self._metadata_backend.get_blocks_by_version(version.uid)

        if base_version_uid and hints is not None:
            # SANITY CHECK:
            # Check some blocks outside of hints if they are the same in the
            # from_version backup and in the current backup. If they
            # aren't, either hints are wrong (e.g. from a wrong snapshot diff)
            # or source doesn't match. In any case, the resulting backup won't
            # be good.
            logger.info('Starting sanity check with 0.1% of the ignored blocks. Reading...')
            ignore_blocks = sorted(set(range(num_blocks)) - read_blocks - sparse_blocks)
            # 0.1% but at least ten. If there are less than ten blocks check them all.
            num_check_blocks = max(min(len(ignore_blocks), 10), len(ignore_blocks) // 1000)
            # 50% from the start
            check_block_ids = ignore_blocks[:num_check_blocks // 2]
            # and 50% from random locations
            check_block_ids = set(check_block_ids + random.sample(ignore_blocks, num_check_blocks // 2))
            num_reading = 0
            for block in blocks:
                if block.id in check_block_ids and block.uid and block.valid:  # no uid = sparse block in backup. Can't check.
                    io.read(block.deref())
                    num_reading += 1
            for entry in io.read_get_completed():
                if isinstance(entry, Exception):
                    raise entry
                else:
                    source_block, source_data, source_data_checksum = entry

                # check metadata checksum with the newly read one
                if source_block.checksum != source_data_checksum:
                    logger.error("Source and backup don't match in regions outside of the ones indicated by the hints.")
                    logger.error("Looks like the hints don't match or the source is different.")
                    logger.error("Found wrong source data at block {}: offset {} with max. length {}".format(
                        source_block.id, source_block.id * self._block_size, self._block_size))
                    # remove version
                    self._metadata_backend.rm_version(version.uid)
                    raise InputDataError('Source changed in regions outside of ones indicated by the hints.')
            logger.info('Finished sanity check. Checked {} blocks {}.'.format(num_reading, check_block_ids))

        try:
            read_jobs = 0
            for i, block in enumerate(blocks):
                if block.id in read_blocks or not block.valid:
                    io.read(block.deref())  # adds a read job.
                    read_jobs += 1
                elif block.id in sparse_blocks:
                    # This "elif" is very important. Because if the block is in read_blocks
                    # AND sparse_blocks, it *must* be read.
                    self._metadata_backend.set_block(block.id, version.uid, None, None, block.size, valid=True)
                    stats['bytes_sparse'] += block.size
                    logger.debug('Skipping block (sparse) {}'.format(block.id))
                else:
                    # Block is already in database, no need to update it
                    logger.debug('Keeping block {}'.format(block.id))
                notify(
                    self._process_name, 'Backup version {} from {}: Queueing blocks to read ({:.1f}%)'.format(
                        version.uid.readable, source, (i + 1) / len(blocks) * 100))

            # precompute checksum of a sparse block
            sparse_block_checksum = data_hexdigest(self._hash_function, b'\0' * self._block_size)

            done_read_jobs = 0
            write_jobs = 0
            done_write_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent
            for entry in io.read_get_completed():
                if isinstance(entry, Exception):
                    raise entry
                else:
                    block, data, data_checksum = entry

                stats['bytes_read'] += len(data)

                # dedup
                existing_block = self._metadata_backend.get_block_by_checksum(data_checksum)
                if data_checksum == sparse_block_checksum and block.size == self._block_size:
                    # if the block is only \0, set it as a sparse block.
                    stats['bytes_sparse'] += block.size
                    logger.debug('Skipping block (detected sparse) {}'.format(block.id))
                    self._metadata_backend.set_block(block.id, version.uid, None, None, block.size, valid=True)
                # Don't try to detect sparse partial blocks as it counteracts the optimisation above
                #elif data == b'\0' * block.size:
                #    # if the block is only \0, set it as a sparse block.
                #    stats['blocks_sparse'] += 1
                #    stats['bytes_sparse'] += block.size
                #    logger.debug('Skipping block (detected sparse) {}'.format(block.id))
                #    self.metadata_backend.set_block(block.id, version_uid, None, None, block.size, valid=True)
                elif existing_block:
                    self._metadata_backend.set_block(
                        block.id,
                        version.uid,
                        existing_block.uid,
                        existing_block.checksum,
                        existing_block.size,
                        valid=True)
                    stats['bytes_dedup'] += len(data)
                    logger.debug('Found existing block for id {} with UID {}'.format(block.id, existing_block.uid))
                else:
                    block.uid = BlockUid(version.uid.int, block.id + 1)
                    block.checksum = data_checksum
                    self._data_backend.save(block, data)
                    write_jobs += 1
                    logger.debug('Queued block {} for write (checksum {}...)'.format(block.id, data_checksum[:16]))

                done_read_jobs += 1

                try:
                    for saved_block in self._data_backend.save_get_completed(timeout=0):
                        if isinstance(saved_block, Exception):
                            raise saved_block

                        self._metadata_backend.set_block(
                            saved_block.id,
                            saved_block.version_uid,
                            saved_block.uid,
                            saved_block.checksum,
                            saved_block.size,
                            valid=True)
                        done_write_jobs += 1
                        stats['bytes_written'] += saved_block.size
                except (TimeoutError, CancelledError):
                    pass

                notify(
                    self._process_name, 'Backup version {} from {} ({:.1f}%)'.format(
                        version.uid.readable, source, done_read_jobs / read_jobs * 100))
                if done_read_jobs % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Backed up {}/{} blocks ({:.1f}%)'.format(done_read_jobs, read_jobs,
                                                                          done_read_jobs / read_jobs * 100))

            try:
                for saved_block in self._data_backend.save_get_completed():
                    if isinstance(saved_block, Exception):
                        raise saved_block

                    self._metadata_backend.set_block(
                        saved_block.id,
                        saved_block.version_uid,
                        saved_block.uid,
                        saved_block.checksum,
                        saved_block.size,
                        valid=True)
                    done_write_jobs += 1
                    stats['bytes_written'] += saved_block.size
            except CancelledError:
                pass

        except:
            raise
        finally:
            # This will also cancel any outstanding read jobs
            io.close()
            self._metadata_backend.commit()

        if read_jobs != done_read_jobs:
            raise InternalError('Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'
                                .format(read_jobs, done_read_jobs))

        if write_jobs != done_write_jobs:
            raise InternalError(
                'Number of submitted and completed write jobs inconsistent (submitted: {}, completed {}).'.format(
                    write_jobs, done_write_jobs))

        self._metadata_backend.set_version(version.uid, valid=True)

        self.export_to_backend([version.uid], overwrite=True, locking=False)

        if tags:
            for tag in tags:
                self._metadata_backend.add_tag(version.uid, tag)

        logger.debug('Stats: {}'.format(stats))
        self._metadata_backend.set_stats(
            version_uid=version.uid,
            base_version_uid=base_version_uid,
            hints_supplied=hints is not None,
            version_date=version.date,
            version_name=name,
            version_snapshot_name=snapshot_name,
            version_size=source_size,
            version_block_size=self._block_size,
            bytes_read=stats['bytes_read'],
            bytes_written=stats['bytes_written'],
            bytes_dedup=stats['bytes_dedup'],
            bytes_sparse=stats['bytes_sparse'],
            duration_seconds=int(time.time() - stats['start_time']),
        )

        logger.info('New version: {} (Tags: [{}])'.format(version.uid, ','.join(tags if tags else [])))
        self._locking.unlock_version(version.uid)
        # It might be tempting to return a Version object here but this will only lead to SQLAlchemy errors
        return version.uid

    def cleanup_fast(self, dt=3600):
        with self._locking.with_lock(
                lock_name='cleanup-fast', reason='Cleanup (fast)',
                locked_msg='Another fast cleanup is already running.'):
            for uid_list in self._metadata_backend.get_delete_candidates(dt):
                logger.debug('Cleanup-fast: Deleting UIDs from data backend: {}'.format(uid_list))
                no_del_uids = self._data_backend.rm_many(uid_list)
                if no_del_uids:
                    logger.info('Cleanup-fast: Unable to delete these UIDs from data backend: {}'.format(uid_list))

    def cleanup_full(self):
        with self._locking.with_lock(
                reason='Cleanup (full)', locked_msg='Another instance has already taken the global lock.'):
            active_blob_uids = set(self._data_backend.list_blocks())
            active_block_uids = set(self._metadata_backend.get_all_block_uids())
            delete_candidates = active_blob_uids.difference(active_block_uids)
            for delete_candidate in delete_candidates:
                logger.debug('Cleanup: Removing UID {}'.format(delete_candidate))
                try:
                    self._data_backend.rm(delete_candidate)
                except FileNotFoundError:
                    continue
            logger.info('Cleanup: Removed {} blobs'.format(len(delete_candidates)))

    def add_tag(self, version_uid, name):
        self._metadata_backend.add_tag(version_uid, name)

    def rm_tag(self, version_uid, name):
        self._metadata_backend.rm_tag(version_uid, name)

    def close(self):
        if self._data_backend_object:
            self._data_backend.close()
        # Close metadata backend after data backend so that any open locks are held until all data backend jobs have
        # finished
        self._metadata_backend.close()

    def export(self, version_uids, f):
        try:
            locked_version_uids = []
            for version_uid in version_uids:
                self._locking.lock_version(version_uid, reason='Exporting version')
                locked_version_uids.append(version_uid)

            self._metadata_backend.export(version_uids, f)
            logger.info('Exported version {} metadata.'.format(version_uid.readable))
        finally:
            for version_uid in locked_version_uids:
                self._locking.unlock_version(version_uid)

    def export_to_backend(self, version_uids, overwrite=False, locking=True):
        try:
            locked_version_uids = []
            if locking:
                for version_uid in version_uids:
                    self._locking.lock_version(version_uid, reason='Exporting version to data backend')
                    locked_version_uids.append(version_uid)

            for version_uid in version_uids:
                with StringIO() as metadata_export:
                    self._metadata_backend.export([version_uid], metadata_export)
                    self._data_backend.save_version(version_uid, metadata_export.getvalue(), overwrite=overwrite)
                logger.info('Exported version {} metadata to backend storage.'.format(version_uid.readable))
        finally:
            for version_uid in locked_version_uids:
                self._locking.unlock_version(version_uid)

    def export_any(self, *args, **kwargs):
        return self._metadata_backend.export_any(*args, **kwargs)

    def import_(self, f):
        # TODO: Find a good way to lock here
        version_uids = self._metadata_backend.import_(f)
        for version_uid in version_uids:
            logger.info('Imported version {} metadata.'.format(version_uid.readable))

    def import_from_backend(self, version_uids):
        try:
            locked_version_uids = []
            for version_uid in version_uids:
                self._locking.lock_version(version_uid, reason='Importing version from data backend')
                locked_version_uids.append(version_uid)

            for version_uid in version_uids:
                metadata_import_data = self._data_backend.read_version(version_uid)
                with StringIO(metadata_import_data) as metadata_import:
                    self._metadata_backend.import_(metadata_import)
                logger.info('Imported version {} metadata from backend storage.'.format(version_uid.readable))
        finally:
            for version_uid in locked_version_uids:
                self._locking.unlock_version(version_uid)

    def enforce_retention_policy(self, version_name, rules_spec, dry_run=False, keep_backend_metadata=False):
        versions = self._metadata_backend.get_versions(version_name=version_name)

        dismissed_versions = RetentionFilter(rules_spec).filter(versions)

        if dismissed_versions:
            logger.info('Removing versions: {}.'.format(', '.join(
                map(lambda version: version.uid.readable, dismissed_versions))))
        else:
            logger.info('All versions are conforming to the retention policy.')

        if dry_run:
            logger.info('Dry run, won\'t remove anything.')
            return []

        for version in dismissed_versions:
            try:
                self.rm(version.uid, force=True, keep_backend_metadata=keep_backend_metadata)
            except AlreadyLocked:
                logger.warning('Version {} couldn\'t be deleted, it\'s currently locked.')

        return map(lambda version: version.uid, dismissed_versions)


# The reason for this class being here is that it accesses private attributes of class Benji
# and I don't want to make them all generally publicly available.
# Maybe they could inherit from the same base class in the future, but currently their
# functionality seems very different. So we just define that BenjiStore objects may access
# private attributes of Benji objects.
class BenjiStore:

    def __init__(self, benji_obj):
        self._benji_obj = benji_obj
        self._cachedir = self._benji_obj.config.get('nbd.cacheDirectory', types=str)
        self._blocks = {}  # block list cache by version
        self._block_cache = set()
        self._cow = {}  # contains version_uid: dict() of block id -> block

    def open(self, version, read_only):
        self._benji_obj._locking.lock_version(version.uid, reason='NBD')

    def close(self, version):
        self._benji_obj._locking.unlock_version(version.uid)

    def get_versions(self, version_uid=None, version_name=None, version_snapshot_name=None):
        return self._benji_obj._metadata_backend.get_versions(
            version_uid=version_uid, version_name=version_name, version_snapshot_name=version_snapshot_name)

    def _block_list(self, version, offset, length):
        # get cached blocks data
        if not self._blocks.get(version.uid):
            # Only work with dereferenced blocks
            self._blocks[version.uid] = [
                block.deref() for block in self._benji_obj._metadata_backend.get_blocks_by_version(version.uid)
            ]
        blocks = self._blocks[version.uid]

        block_number = offset // version.block_size
        block_offset = offset % version.block_size

        chunks = []
        while True:
            try:
                block = blocks[block_number]
            except IndexError:
                # We round up the size reported by the NBD server to a multiple of 4096 which is the maximum
                # block size supported by NBD. So we might need to fake up to 4095 bytes (of zeros) here.
                if length > 4095:
                    # Don't throw one of our own exceptions here as we need an exception with an errno value
                    # to communicate it back in the NBD response.
                    raise OSError(errno.EIO)
                read_length = min(block.size - block_offset, length)
                chunks.append((None, 0, read_length))  # hint: return \0s
            else:
                assert block.id == block_number
                read_length = min(block.size - block_offset, length)
                chunks.append((block, block_offset, read_length))
            block_number += 1
            block_offset = 0
            length -= read_length
            assert length >= 0
            if length == 0:
                break

        return chunks

    @staticmethod
    def _cache_filename(block_uid):
        filename = '{:016x}-{:016x}'.format(block_uid.left, block_uid.right)
        digest = hashlib.md5(filename.encode('ascii')).hexdigest()
        return '{}/{}/{}-{}'.format(digest[0:2], digest[2:4], digest[:8], filename)

    def _read(self, block, offset=0, length=None):
        if block.uid not in self._block_cache:
            data = self._benji_obj._data_backend.read(block, sync=True)
            filename = os.path.join(self._cachedir, self._cache_filename(block.uid))
            try:
                with open(filename, 'wb') as f:
                    f.write(data)
            except FileNotFoundError:
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                with open(filename, 'wb') as f:
                    f.write(data)
            self._block_cache.add(block.uid)
        with open(os.path.join(self._cachedir, self._cache_filename(block.uid)), 'rb') as f:
            f.seek(offset)
            if length is None:
                return f.read()
            else:
                return f.read(length)

    def read(self, version, cow_version, offset, length):
        if cow_version:
            cow = self._cow[cow_version.uid.int]
        else:
            cow = None
        read_list = self._block_list(version, offset, length)
        data = []
        for block, offset, length in read_list:
            # If block is in COW version, read it from there
            if block is not None and cow is not None and block.id in cow:
                block = cow[block.id]
                logger.debug('Reading block {}:{}:{} from COW version.'.format(block, offset, length))
            else:
                logger.debug('Reading block {}:{}:{}.'.format(block, offset, length))

            if block is None:
                logger.warning('Tried to read data beyond device (offset {}).'.format(offset))
                data.append(b'\0' * length)
            elif not block.uid:  # sparse block
                data.append(b'\0' * length)
            else:
                data.append(self._read(block, offset, length))
        return b''.join(data)

    def get_cow_version(self, base_version):
        #_clone_version(self, name, snapshot_name, size=None, from_version_uid=None):
        cow_version = self._benji_obj._clone_version(
            name=base_version.name,
            snapshot_name='nbd-cow-{}-{}'.format(
                base_version.uid.readable, datetime.datetime.now().isoformat(timespec='seconds')),
            base_version_uid=base_version.uid)
        self._benji_obj._locking.update_version_lock(cow_version.uid, reason='NBD COW')
        self._cow[cow_version.uid.int] = {}  # contains version_uid: dict() of block id -> uid
        return cow_version

    def _save(self, block, data):
        filename = os.path.join(self._cachedir, self._cache_filename(block.uid))
        try:
            with open(filename, 'wb') as f:
                f.write(data)
        except FileNotFoundError:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb') as f:
                f.write(data)
        self._block_cache.add(block.uid)

    def _rm(self, block):
        filename = os.path.join(self._cachedir, self._cache_filename(block.uid))
        os.unlink(filename)

    def write(self, cow_version, offset, data):
        """ Copy on write backup writer """
        dataio = BytesIO(data)
        cow = self._cow[cow_version.uid.int]
        write_list = self._block_list(cow_version, offset, len(data))
        for block, _offset, length in write_list:
            if block is None:
                logger.warning('Tried to save data beyond device, it will be lost (offset {}).'.format(offset))
                continue
            if block.id in cow:
                # the block is already copied, so update it.
                with open(os.path.join(self._cachedir, self._cache_filename(block.uid)), 'r+b') as f:
                    f.seek(offset)
                logger.debug('COW: Updated block {}'.format(block.id))
            else:
                # read the block from the original, update it and write it back
                if block.uid:
                    write_data = BytesIO(self._benji_obj._data_backend.read(block, sync=True))
                    write_data.seek(_offset)
                    write_data.write(dataio.read(length))
                    write_data.seek(0)
                else:  # was a sparse block
                    write_data = BytesIO(data)
                # Save a copy of the changed data and record the changed block UID
                block.uid = BlockUid(cow_version.uid.int, block.id + 1)
                block.checksum = None
                self._save(block, write_data.read())
                cow[block.id] = block
                logger.debug('COW: Wrote block {} into {}'.format(block.id, block.uid))

    def flush(self, cow_version):
        # TODO: Maybe fixate partly?
        pass

    def fixate(self, cow_version):
        # save blocks into version
        logger.info('Fixating version {} with {} blocks, please wait!'.format(
            cow_version.uid, len(self._cow[cow_version.uid.int].items())))

        for block in self._cow[cow_version.uid.int].values():
            logger.debug('Fixating block {} uid {}'.format(block.id, block.uid))
            data = self._read(block)

            # dump changed data
            self._benji_obj._data_backend.save(block, data, sync=True)
            logger.debug('Stored block {} uid {}'.format(block.id, block.uid))

            # TODO: Add deduplication (maybe share code with backup?), detect sparse blocks?
            checksum = data_hexdigest(self._benji_obj._hash_function, data)
            self._benji_obj._metadata_backend.set_block(
                block.id, cow_version.uid, block.uid, checksum, len(data), valid=True)

        self._benji_obj._metadata_backend.commit()
        self._benji_obj._metadata_backend.set_version(cow_version.uid, valid=True, protected=True)
        self._benji_obj.export_to_backend([cow_version.uid], overwrite=True, locking=False)
        logger.info('Fixation done. Deleting temporary data, please wait!')
        # TODO: Delete COW blocks and also those from block_cache
        for block_uid in self._block_cache:
            # TODO if this block is in the current version (and in no other?)
            # rm this block from cache
            # rm block uid from self._block_cache
            pass
        for block_id, block in self._cow[cow_version.uid.int].items():
            pass
        del (self._cow[cow_version.uid.int])
        self._benji_obj._locking.unlock_version(cow_version.uid)
        logger.info('Finished.')
