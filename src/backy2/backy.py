# -*- encoding: utf-8 -*-

import datetime
import importlib
import math
import random
import time
from concurrent.futures import CancelledError, TimeoutError
from io import StringIO
from urllib import parse

from dateutil.relativedelta import relativedelta

from backy2.exception import InputDataError, InternalError, AlreadyLocked, UsageError, NoChange, ConfigurationError
from backy2.logging import logger
from backy2.meta_backend import BlockUid, MetaBackend
from backy2.utils import data_hexdigest, notify, parametrized_hash_function


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


class Backy:

    def __init__(self, config, block_size=None, initdb=False, _destroydb=False, _migratedb=True):

        self.config = config

        if block_size is None:
            self._block_size = config.get('blockSize', types=int)
        else:
            self._block_size = block_size

        self._hash_function = parametrized_hash_function(config.get('hashFunction', types=str))
        self._process_name = config.get('processName', types=str)
        self._export_metadata = config.get('exportMetadata', types=bool)

        from backy2.data_backends import DataBackend
        name = config.get('dataBackend.type', types=str)
        try:
            self._data_backend_module = importlib.import_module('{}.{}'.format(DataBackend.PACKAGE_PREFIX, name))
        except ImportError:
            raise ConfigurationError('Data backend type {} not found.'.format(name))
        self._data_backend_object = None

        meta_backend = MetaBackend(config)
        if initdb:
            meta_backend.initdb(_destroydb=_destroydb, _migratedb=_migratedb)

        self._meta_backend = meta_backend.open(_migratedb=_migratedb)
        self._locking = self._meta_backend.locking()

        notify(self._process_name)  # i.e. set process name without notification

        if self._locking.is_locked():
            raise AlreadyLocked('Another process is already running.')

    # This implements a lazy creation of the data backend object so that any network connections
    # are only opened and expensive encryption calculations are only done when the object is really used.
    @property
    def _data_backend(self):
        if not self._data_backend_object:
            self._data_backend_object = self._data_backend_module.DataBackend(self.config)
        return self._data_backend_object

    def _prepare_version(self, name, snapshot_name, size=None, from_version_uid=None):
        """ Prepares the metadata for a new version.
        If from_version_uid is given, this is taken as the base, otherwise
        a pure sparse version is created.
        """
        if from_version_uid:
            old_version = self._meta_backend.get_version(from_version_uid)  # raise if not exists
            if not old_version.valid:
                raise UsageError('You cannot base new version on an invalid one.')
            if old_version.block_size != self._block_size:
                raise UsageError('You cannot base new version on an old version with a different block size.')
            old_blocks = self._meta_backend.get_blocks_by_version(from_version_uid)
            if not size:
                size = old_version.size
        else:
            old_blocks = None
            if not size:
                raise InternalError('Size needs to be specified if there is no base version.')

        num_blocks = int(math.ceil(size / self._block_size))
        # we always start with invalid versions, then validate them after backup
        version = self._meta_backend.set_version(
            version_name=name,
            snapshot_name=snapshot_name,
            size=size,
            block_size=self._block_size,
            valid=False)
        if not self._locking.lock(lock_name=version.uid.readable, reason='Preparing version'):
            raise AlreadyLocked('Version {} is locked.'.format(version.uid))
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

            self._meta_backend.set_block(id, version.uid, uid, checksum, block_size, valid, upsert=False)
            notify(self._process_name, 'Preparing version {} ({:.1f}%)'.format(version.uid.readable, (id + 1) / num_blocks * 100))
        self._meta_backend.commit()
        notify(self._process_name)
        self._locking.unlock(lock_name=version.uid.readable)
        return version

    def clone_version(self, name, snapshot_name, from_version_uid):
        return self._prepare_version(name, snapshot_name, None, from_version_uid)

    def ls(self):
        return self._meta_backend.get_versions()

    def ls_version(self, version_uid):
        # don't lock here, this is not really error-prone.
        return self._meta_backend.get_blocks_by_version(version_uid)

    def stats(self, version_uid=None, limit=None):
        return self._meta_backend.get_stats(version_uid, limit)

    def get_io_by_source(self, source, block_size):
        res = parse.urlparse(source)

        if res.params or res.query or res.fragment:
            raise UsageError('The supplied URL {} is invalid.'.format(source))

        scheme = res.scheme
        if not scheme:
            raise UsageError('The supplied URL {} is invalid. You must provide a scheme (e.g. file://).'
                             .format(source))

        try:
            from backy2.io import IO
            IOLib = importlib.import_module('{}.{}'.format(IO.PACKAGE_PREFIX, scheme))
        except ImportError:
            raise UsageError('IO scheme {} not supported.'.format(scheme))

        return IOLib.IO(
                config=self.config,
                block_size=block_size,
                hash_function=self._hash_function,
                )

    def scrub(self, version_uid, percentile=100):
        """ Returns a boolean (state). If False, there were errors, if True
        all was ok
        """
        if not self._locking.lock(lock_name=version_uid.readable, reason='Scrubbing version'):
            raise AlreadyLocked('Version {} is locked.'.format(version_uid.readable))

        version = self._meta_backend.get_version(version_uid)
        blocks = self._meta_backend.get_blocks_by_version(version_uid)

        state = True
        try:
            read_jobs = 0
            for i, block in enumerate(blocks):
                if block.uid:
                    if percentile < 100 and random.randint(1, 100) > percentile:
                        logger.debug('Scrub of block {} (UID {}) skipped (percentile is {}).'
                                     .format(block.id, block.uid, percentile))
                    else:
                        self._data_backend.read(block.deref(), metadata_only=True)  # async queue
                        read_jobs += 1
                else:
                    logger.debug('Scrub of block {} (UID {}) skipped (sparse).'.format(block.id, block.uid))
                notify(self._process_name, 'Preparing scrub of version {} ({:.1f}%)'
                       .format(version.uid.readable, (i + 1) / len(blocks) * 100))

            done_read_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent
            for entry in self._data_backend.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    logger.error('Data backend read failed: {}'.format(entry))
                    if isinstance(entry, (KeyError, ValueError)):
                        self._meta_backend.set_blocks_invalid(block.uid, block.checksum)
                        state = False
                        continue
                    else:
                        raise entry
                else:
                    block, data, metadata = entry

                try:
                    self._data_backend.check_block_metadata(block=block, data_length=None, metadata=metadata)
                except (KeyError, ValueError) as exception:
                    logger.error('Metadata check failed, block is invalid: {}'.format(exception))
                    self._meta_backend.set_blocks_invalid(block.uid, block.checksum)
                    state = False
                    continue
                except:
                    raise

                logger.debug('Scrub of block {} (UID {}) ok.'.format(block.id, block.uid))
                notify(self._process_name, 'Scrubbing version {} ({:.1f}%)'.format(version_uid.readable, done_read_jobs / read_jobs * 100))
                if i % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Scrubbed {}/{} blocks ({:.1f}%)'
                                .format(done_read_jobs, read_jobs, done_read_jobs / read_jobs * 100))
        except:
            raise

        if read_jobs != done_read_jobs:
            raise InternalError('Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'
                                .format(read_jobs, done_read_jobs))

        if state == True:
            self._meta_backend.set_version_valid(version_uid)
        else:
            # version is set invalid by set_blocks_invalid.
            logger.error('Marked version {} invalid because it has errors.'.format(version_uid.readable))

        self._locking.unlock(lock_name=version_uid.readable)
        notify(self._process_name)
        return state

    def deep_scrub(self, version_uid, source=None, percentile=100):
        """ Returns a boolean (state). If False, there were errors, if True
        all was ok
        """
        if not self._locking.lock(lock_name=version_uid.readable, reason='Scrubbing version'):
            raise AlreadyLocked('Version {} is locked.'.format(version_uid.readable))

        version = self._meta_backend.get_version(version_uid)
        blocks = self._meta_backend.get_blocks_by_version(version_uid)

        if source:
            io = self.get_io_by_source(source, version.block_size)
            io.open_r(source)

        state = True
        try:
            old_use_read_cache = self._data_backend.use_read_cache(False)
            read_jobs = 0
            for i, block in enumerate(blocks):
                if block.uid:
                    if percentile < 100 and random.randint(1, 100) > percentile:
                        logger.debug('Deep scrub of block {} (UID {}) skipped (percentile is {}).'
                            .format(block.id, block.uid, percentile))
                    else:
                        self._data_backend.read(block.deref())  # async queue
                        read_jobs += 1
                else:
                    logger.debug('Deep scrub of block {} (UID {}) skipped (sparse).'.format(block.id, block.uid))
                notify(self._process_name, 'Preparing deep scrub of version {} ({:.1f}%)'.format(version.uid.readable, (i + 1) / len(blocks) * 100))

            done_read_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent
            for entry in self._data_backend.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    logger.error('Data backend read failed: {}'.format(entry))
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, (KeyError, ValueError)):
                        self._meta_backend.set_blocks_invalid(block.uid, block.checksum)
                        state = False
                        continue
                    else:
                        raise entry
                else:
                    block, data, metadata = entry

                try:
                    self._data_backend.check_block_metadata(block=block, data_length=len(data), metadata=metadata)
                except (KeyError, ValueError) as exception:
                    logger.error('Metadata check failed, block is invalid: {}'.format(exception))
                    self._meta_backend.set_blocks_invalid(block.uid, block.checksum)
                    state = False
                    continue
                except:
                    raise

                data_checksum = data_hexdigest(self._hash_function, data)
                if data_checksum != block.checksum:
                    logger.error('Checksum mismatch during deep scrub for block {} (UID {}) (is: {} should-be: {}).'
                                 .format(block.id, block.uid, data_checksum, block.checksum))
                    self._meta_backend.set_blocks_invalid(block.uid, block.checksum)
                    state = False
                    continue

                if source:
                    source_data = io.read(block, sync=True)
                    if source_data != data:
                        logger.error('Source data has changed for block {} (UID {}) (is: {} should-be: {}). Won\'t set '
                                     'this block to invalid, because the source looks wrong.'
                                     .format(block,
                                             block.uid,
                                             data_hexdigest(self._hash_function, source_data),
                                             data_checksum))
                        state = False
                        # We are not setting the block invalid here because
                        # when the block is there AND the checksum is good,
                        # then the source is invalid.

                logger.debug('Deep scrub of block {} (UID {}) ok.'.format(block.id, block.uid))
                notify(self._process_name, 'Deep scrubbing version {} ({:.1f}%)'
                       .format(version_uid.readable, (i + 1) / read_jobs * 100))
                if done_read_jobs % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Deep scrubbed {}/{} blocks ({:.1f}%)'.format(done_read_jobs, read_jobs,  done_read_jobs / read_jobs * 100))
        except:
            raise
        finally:
            if source:
                io.close()

        if read_jobs != done_read_jobs:
            raise InternalError('Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'
                                .format(read_jobs, done_read_jobs))

        # Restore old read cache setting
        self._data_backend.use_read_cache(old_use_read_cache)

        if state == True:
            self._meta_backend.set_version_valid(version_uid)
        else:
            # version is set invalid by set_blocks_invalid.
            # FIXME: This message might be misleading in the case where a source mismatch occurs where
            # FIXME: we set state to False but don't mark any blocks as invalid.
            logger.error('Marked version {} invalid because it has errors.'.format(version_uid.readable))

        self._locking.unlock(lock_name=version_uid.readable)
        notify(self._process_name)
        return state

    def restore(self, version_uid, target, sparse=False, force=False):
        if not self._locking.lock(lock_name=version_uid.readable, reason='Restoring version'):
            raise AlreadyLocked('Version {} is locked.'.format(version_uid.readable))

        version = self._meta_backend.get_version(version_uid)  # raise if version not exists
        notify(self._process_name, 'Restoring version {} to {}: Getting blocks'.format(version_uid.readable, target))
        blocks = self._meta_backend.get_blocks_by_version(version_uid)

        io = self.get_io_by_source(target, version.block_size)
        io.open_w(target, version.size, force)

        try:
            read_jobs = 0
            for i, block in enumerate(blocks):
                if block.uid:
                    self._data_backend.read(block.deref())
                    read_jobs += 1
                elif not sparse:
                    io.write(block, b'\0'*block.size)
                    logger.debug('Restored sparse block {} successfully ({} bytes).'.format(block.id, block.size))
                else:
                    logger.debug('Ignored sparse block {}.'.format(block.id))
                if sparse:
                    notify(self._process_name, 'Restoring version {} to {}: Queueing blocks to read ({:.1f}%)'
                           .format(version_uid.readable, target, (i + 1) / len(blocks) * 100))
                else:
                    notify(self._process_name, 'Restoring version {} to {}: Sparse writing ({:.1f}%)'
                           .format(version_uid.readable, target, (i + 1) / len(blocks) * 100))

            done_read_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent
            for entry in self._data_backend.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    logger.error('Data backend read failed: {}'.format(entry))
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, (KeyError, ValueError)):
                        self._meta_backend.set_blocks_invalid(block.uid, block.checksum)
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
                    self._meta_backend.set_blocks_invalid(block.uid, block.checksum)
                    continue
                except:
                    raise

                data_checksum = data_hexdigest(self._hash_function, data)
                if data_checksum != block.checksum:
                    logger.error('Checksum mismatch during restore for block {} (UID {}) (is: {} should-be: {}, '
                                 'block.valid: {}). Block restored is invalid.'
                                 .format(block.id, block.uid, data_checksum, block.checksum, block.valid))
                    self._meta_backend.set_blocks_invalid(block.uid, block.checksum)
                else:
                    logger.debug('Restored block {} successfully ({} bytes).'.format(block.id, block.size))

                notify(self._process_name, 'Restoring version {} to {} ({:.1f}%)'
                                .format(version_uid.readable, target, done_read_jobs / read_jobs * 100))
                if i % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Restored {}/{} blocks ({:.1f}%)'
                                .format(done_read_jobs, read_jobs, done_read_jobs / read_jobs * 100))
        except:
            raise
        finally:
            io.close()

        if read_jobs != done_read_jobs:
            raise InternalError('Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'
                                .format(read_jobs, done_read_jobs))

        self._locking.unlock(lock_name=version_uid.readable)

    def protect(self, version_uid):
        version = self._meta_backend.get_version(version_uid)
        if version.protected:
            raise NoChange('Version {} is already protected.'.format(version_uid.readable))
        self._meta_backend.protect_version(version_uid)

    def unprotect(self, version_uid):
        version = self._meta_backend.get_version(version_uid)
        if not version.protected:
            raise NoChange('Version {} is not protected.'.format(version_uid.readable))
        self._meta_backend.unprotect_version(version_uid)

    def rm(self, version_uid, force=True, disallow_rm_when_younger_than_days=0, keep_backend_metadata=False):
        if not self._locking.lock(lock_name=version_uid.readable, reason='Removing version'):
            raise AlreadyLocked('Version {} is locked.'.format(version_uid.readable))
        try:
            version = self._meta_backend.get_version(version_uid)

            if version.protected:
                raise RuntimeError('Version {} is protected. Will not delete.'.format(version_uid.readable))

            if not force:
                # check if disallow_rm_when_younger_than_days allows deletion
                age_days = (datetime.datetime.now() - version.date).days
                if disallow_rm_when_younger_than_days > age_days:
                    raise RuntimeError('Version {} is too young. Will not delete.'.format(version_uid.readable))

            num_blocks = self._meta_backend.rm_version(version_uid)

            if not keep_backend_metadata:
                try:
                    self._data_backend.rm_version(version_uid)
                    logger.info('Removed version {} metadata from backend storage.'.format(version_uid.readable))
                except FileNotFoundError:
                    logger.warning('Unable to remove version {} metadata from backend storage, the object wasn\'t found.'
                                .format(version_uid.readable))
                    pass

            logger.info('Removed backup version {} with {} blocks.'.format(version_uid.readable, num_blocks))
        finally:
            self._locking.unlock(lock_name=version_uid.readable)

    def rm_from_backend(self, version_uid):
        if not self._locking.lock(lock_name=version_uid.readable, reason='Removing version from backend storage'):
            raise AlreadyLocked('Version {} is locked.'.format(version_uid.readable))
        try:
            self._data_backend.rm_version(version_uid)
            logger.info('Removed backup version {} metadata from backend storage.'.format(version_uid.readable))
        finally:
            self._locking.unlock(lock_name=version_uid.readable)

    def _generate_auto_tags(self, version_name):
        """ Generates automatic tag suggestions by looking up versions with
        the same name and comparing their dates.
        This algorithm will
        - give the tag 'b_daily' if the last b_daily tagged version for this name is > 0 days ago
        - give the tag 'b_weekly' if the last b_weekly tagged version for this name is > 6 days ago
        - give the tag 'b_monthly' if the last b_monthly tagged version for this name is > 1 month ago
        """
        versions = self._meta_backend.get_versions(version_name=version_name)
        versions = [{'date': v.date.date(), 'tags': [t.name for t in v.tags]} for v in versions]

        for version in versions:
            b_daily = [v for v in versions if 'b_daily' in v['tags']]
            b_weekly = [v for v in versions if 'b_weekly' in v['tags']]
            b_monthly = [v for v in versions if 'b_monthly' in v['tags']]
        b_daily_last = max([v['date'] for v in b_daily]) if b_daily else None
        b_weekly_last = max([v['date'] for v in b_weekly]) if b_weekly else None
        b_monthly_last = max([v['date'] for v in b_monthly]) if b_monthly else None

        tags = []
        today = datetime.date.today()
        if not b_daily_last or \
                (today - b_daily_last).days > 0:
            tags.append('b_daily')
        if not b_weekly_last or \
                (today - b_weekly_last).days // 7 > 0:
            tags.append('b_weekly')
        if not b_monthly_last or \
                relativedelta(today, b_monthly_last).months + 12 * relativedelta(today, b_monthly_last).years > 0:
            tags.append('b_monthly')

        return tags

    def backup(self, name, snapshot_name, source, hints, from_version_uid, tag=None):
        """ Create a backup from source.
        If hints are given, they must be tuples of (offset, length, exists)
        where offset and length are integers and exists is a boolean. Then, only
        data within hints will be backed up.
        Otherwise, the backup reads source and looks if checksums match with
        the target.
        """
        stats = {
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
        io = self.get_io_by_source(source, self._block_size)
        io.open_r(source)
        source_size = io.size()

        num_blocks = int(math.ceil(source_size / self._block_size))

        if hints is not None and len(hints) > 0:
            # Sanity check: check hints for validity, i.e. too high offsets, ...
            max_offset = max([h[0]+h[1] for h in hints])
            if max_offset > source_size:
                raise InputDataError('Hints have higher offsets than source file.')

            sparse_blocks, read_blocks = blocks_from_hints(hints, self._block_size)
        else:
            sparse_blocks = set()
            read_blocks = set(range(num_blocks))

        version = self._prepare_version(name, snapshot_name, source_size, from_version_uid)

        if not self._locking.lock(lock_name=version.uid.readable, reason='Backup'):
            raise AlreadyLocked('Version {} is locked.'.format(version.uid))

        blocks = self._meta_backend.get_blocks_by_version(version.uid)

        if from_version_uid and hints:
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
                        source_block.id,
                        source_block.id * self._block_size,
                        self._block_size
                        ))
                    # remove version
                    self._meta_backend.rm_version(version.uid)
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
                    self._meta_backend.set_block(block.id, version.uid, None, None, block.size, valid=True)
                    stats['blocks_sparse'] += 1
                    stats['bytes_sparse'] += block.size
                    logger.debug('Skipping block (sparse) {}'.format(block.id))
                else:
                    # Block is already in database, no need to update it
                    logger.debug('Keeping block {}'.format(block.id))
                notify(self._process_name, 'Backup version {} from {}: Queueing blocks to read ({:.1f}%)'.format(version.uid.readable, source, (i + 1) / len(blocks) * 100))

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

                stats['blocks_read'] += 1
                stats['bytes_read'] += len(data)

                # dedup
                existing_block = self._meta_backend.get_block_by_checksum(data_checksum)
                if data_checksum == sparse_block_checksum and block.size == self._block_size:
                    # if the block is only \0, set it as a sparse block.
                    stats['blocks_sparse'] += 1
                    stats['bytes_sparse'] += block.size
                    logger.debug('Skipping block (detected sparse) {}'.format(block.id))
                    self._meta_backend.set_block(block.id, version.uid, None, None, block.size, valid=True)
                # Don't try to detect sparse partial blocks as it counteracts the optimisation above
                #elif data == b'\0' * block.size:
                #    # if the block is only \0, set it as a sparse block.
                #    stats['blocks_sparse'] += 1
                #    stats['bytes_sparse'] += block.size
                #    logger.debug('Skipping block (detected sparse) {}'.format(block.id))
                #    self.meta_backend.set_block(block.id, version_uid, None, None, block.size, valid=True)
                elif existing_block:
                    self._meta_backend.set_block(block.id, version.uid, existing_block.uid, existing_block.checksum, existing_block.size, valid=True)
                    stats['blocks_found_dedup'] += 1
                    stats['bytes_found_dedup'] += len(data)
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

                        self._meta_backend.set_block(saved_block.id, saved_block.version_uid, saved_block.uid,
                                                     saved_block.checksum, saved_block.size, valid=True)
                        done_write_jobs += 1
                        stats['blocks_written'] += 1
                        stats['bytes_written'] += saved_block.size
                except (TimeoutError, CancelledError):
                    pass

                notify(self._process_name, 'Backup version {} from {} ({:.1f}%)'.format(version.uid.readable, source, done_read_jobs / read_jobs * 100))
                if done_read_jobs % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Backed up {}/{} blocks ({:.1f}%)'.format(done_read_jobs, read_jobs,  done_read_jobs / read_jobs * 100))

            try:
                for saved_block in self._data_backend.save_get_completed():
                    if isinstance(saved_block, Exception):
                        raise saved_block

                    self._meta_backend.set_block(saved_block.id, saved_block.version_uid, saved_block.uid,
                                                 saved_block.checksum, saved_block.size, valid=True)
                    done_write_jobs += 1
                    stats['blocks_written'] += 1
                    stats['bytes_written'] += saved_block.size
            except CancelledError:
                pass

        except:
            raise
        finally:
            # This will also cancel any outstanding read jobs
            io.close()

        if read_jobs != done_read_jobs:
            raise InternalError('Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'
                                .format(read_jobs, done_read_jobs))

        if  write_jobs != done_write_jobs:
            raise InternalError('Number of submitted and completed write jobs inconsistent (submitted: {}, completed {}).'
                                .format(write_jobs, done_write_jobs))

        self._meta_backend.set_version_valid(version.uid)

        if self._export_metadata:
            self.export_to_backend([version.uid], overwrite=True, locking=False)

        if tag is not None:
            if isinstance(tag, list):
                tags = tag
            else:
                tags = [tag]
        else:
            tags = self._generate_auto_tags(name)
        for tag in tags:
            self._meta_backend.add_tag(version.uid, tag)

        logger.debug('Stats: {}'.format(stats))
        self._meta_backend.set_stats(
            version_uid=version.uid,
            version_name=name,
            version_snapshot_name=snapshot_name,
            version_size=source_size,
            version_block_size=self._block_size,
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

        logger.info('New version: {} (Tags: [{}])'.format(version.uid, ','.join(tags)))
        self._locking.unlock(lock_name=version.uid.readable)
        return version.uid


    def cleanup_fast(self, dt=3600):
        """ Delete unreferenced blob UIDs """
        if not self._locking.lock(lock_name='cleanup-fast', reason='Cleanup fast'):
            raise AlreadyLocked('Another backy cleanup is running.')
        try:
            for uid_list in self._meta_backend.get_delete_candidates(dt):
                logger.debug('Cleanup-fast: Deleting UIDs from data backend: {}'.format(uid_list))
                no_del_uids = self._data_backend.rm_many(uid_list)
                if no_del_uids:
                    logger.info('Cleanup-fast: Unable to delete these UIDs from data backend: {}'.format(uid_list))
        finally:
            self._locking.unlock(lock_name='cleanup-fast')

    def cleanup_full(self):
        """ Delete unreferenced blob UIDs starting with <prefix> """
        # in this mode, we compare all existing uids in data and meta.
        # make sure, no other backy will start
        if not self._locking.lock(reason='Cleanup full'):
            raise AlreadyLocked('Other backy instances are running.')
        try:
            active_blob_uids = set(self._data_backend.list_blocks())
            active_block_uids = set(self._meta_backend.get_all_block_uids())
            delete_candidates = active_blob_uids.difference(active_block_uids)
            for delete_candidate in delete_candidates:
                logger.debug('Cleanup: Removing UID {}'.format(delete_candidate))
                try:
                    self._data_backend.rm(delete_candidate)
                except FileNotFoundError:
                    continue
            logger.info('Cleanup: Removed {} blobs'.format(len(delete_candidates)))
        finally:
            self._locking.unlock()

    def add_tag(self, version_uid, name):
        self._meta_backend.add_tag(version_uid, name)

    def rm_tag(self, version_uid, name):
        self._meta_backend.rm_tag(version_uid, name)

    def close(self):
        if self._data_backend_object:
            self._data_backend.close()
        # Close meta backend after data backend so that any open locks are held until all data backend jobs have
        # finished
        self._meta_backend.close()

    def export(self, version_uids, f):
        try:
            locked_version_uids = []
            for version_uid in version_uids:
                if not self._locking.lock(lock_name=version_uid.readable, reason='Exporting version(s)'):
                    raise AlreadyLocked('Version {} is locked.'.format(version_uid.readable))
                locked_version_uids.append(version_uid)

            self._meta_backend.export(version_uids, f)
            logger.info('Exported version {} metadata.'.format(version_uid.readable))
        finally:
            for version_uid in locked_version_uids:
                self._locking.unlock(lock_name=version_uid.readable)

    def export_to_backend(self, version_uids, overwrite=False, locking=True):
        try:
            locked_version_uids = []
            if locking:
                for version_uid in version_uids:
                    if not self._locking.lock(lock_name=version_uid.readable, reason='Exporting verion(s) to backend storage'):
                        raise AlreadyLocked('Version {} is locked.'.format(version_uid.readable))
                    locked_version_uids.append(version_uid)

            for version_uid in version_uids:
                with StringIO() as metadata_export:
                    self._meta_backend.export([version_uid], metadata_export)
                    self._data_backend.save_version(version_uid, metadata_export.getvalue(), overwrite=overwrite)
                logger.info('Exported version {} metadata to backend storage.'.format(version_uid.readable))
        finally:
            for version_uid in locked_version_uids:
                self._locking.unlock(lock_name=version_uid.readable)

    def import_(self, f):
        # TODO: Find a good way to lock here
        version_uids = self._meta_backend.import_(f)
        for version_uid in version_uids:
            logger.info('Imported version {} metadata.'.format(version_uid.readable))

    def import_from_backend(self, version_uids):
        try:
            locked_version_uids = []
            for version_uid in version_uids:
                if not self._locking.lock(lock_name=version_uid.readable, reason='Importing version(s) from backend storage'):
                    raise AlreadyLocked('Version {} is locked.'.format(version_uid.readable))
                locked_version_uids.append(version_uid)

            for version_uid in version_uids:
                metadata_import_data = self._data_backend.read_version(version_uid)
                with StringIO(metadata_import_data) as metadata_import:
                    self._meta_backend.import_(metadata_import)
                logger.info('Imported version {} metadata from backend storage.'.format(version_uid.readable))
        finally:
            for version_uid in locked_version_uids:
                self._locking.unlock(lock_name=version_uid.readable)
