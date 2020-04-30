# -*- encoding: utf-8 -*-

from backy2 import notify
from backy2.crypt import get_crypt
from backy2.logging import logger
from backy2.locking import Locking
from backy2.locking import find_other_procs
from backy2.utils import grouper
from backy2.utils import status
from backy2.utils import MinSequential
from dateutil.relativedelta import relativedelta
from urllib import parse
import binascii
import datetime
import importlib
import math
import queue
import random
import time
import sys


def blocks_from_hints(hints, block_size):
    """ Helper method """
    blocks = set()
    for offset, length, exists in hints:
        start_block = math.floor(offset / block_size)
        end_block = math.ceil((offset + length) / block_size)
        for i in range(start_block, end_block):
            blocks.add(i)
    return blocks


class LockError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class Backy():
    """
    """

    def __init__(self, meta_backend, data_backend, config, block_size=None,
            hash_function=None, lock_dir=None, process_name='backy2',
            initdb=False, dedup=True):
        if block_size is None:
            block_size = 1024*4096  # 4MB
        if hash_function is None:
            import hashlib
            hash_function = hashlib.sha512
        if initdb:
            meta_backend.initdb()
        self.meta_backend = meta_backend.open()
        self.data_backend = data_backend
        self.config = config
        self.block_size = block_size
        self.hash_function = hash_function
        self.locking = Locking(lock_dir)
        self.process_name = process_name
        self.dedup = dedup

        notify(process_name)  # i.e. set process name without notification

        if not self.locking.lock('backy'):
            raise LockError('A backy is running which requires exclusive access.')
        self.locking.unlock('backy')


    def ls(self):
        versions = self.meta_backend.get_versions()
        return versions


    def ls_version(self, version_uid):
        # don't lock here, this is not really error-prone.
        blocks = self.meta_backend.get_blocks_by_version(version_uid).all()
        return blocks


    def stats(self, version_uid=None, limit=None):
        stats = self.meta_backend.get_stats(version_uid, limit)
        return stats


    def get_io_by_source(self, source):
        res = parse.urlparse(source)
        if res.params or res.query or res.fragment:
            raise ValueError('Invalid URL.')
        scheme = res.scheme
        if not scheme:
            raise ValueError('Invalid URL. You must provide the type (e.g. file://)')
        # import io with name == scheme
        # and pass config section io_<scheme>
        IOLib = importlib.import_module('backy2.io.{}'.format(scheme))
        config = self.config(section='io_{}'.format(scheme))
        return IOLib.IO(
                config=config,
                block_size=self.block_size,
                hash_function=self.hash_function,
                )


    def du(self, version_uid):
        """ Returns disk usage statistics for a version.
        """
        if not self.locking.lock(version_uid):
            raise LockError('Version {} is locked.'.format(version_uid))
        self.meta_backend.get_version(version_uid)  # raise if version not exists
        return self.meta_backend.du(version_uid)


    def scrub(self, version_uid, source=None, percentile=100):
        """ Returns a boolean (state). If False, there were errors, if True
        all was ok
        """
        if not self.locking.lock(version_uid):
            raise LockError('Version {} is locked.'.format(version_uid))
        self.locking.unlock(version_uid)  # No need to keep it locked.

        stats = {
                'source_bytes_read': 0,
                'source_blocks_read': 0,
                'bytes_read': 0,
                'blocks_read': 0,
            }

        version = self.meta_backend.get_version(version_uid)  # raise if version not exists
        blocks = self.meta_backend.get_blocks_by_version(version_uid)

        # check if the backup is at least complete
        if blocks.count() != version.size:
            logger.error("Version is incomplete.")
            self.meta_backend.set_version_invalid(version_uid)
            return

        if source:
            io = self.get_io_by_source(source)
            io.open_r(source)

        state = True

        notify(self.process_name, 'Preparing Scrub of version {}'.format(version_uid))
        # prepare
        read_jobs = 0
        for block in blocks.yield_per(1000):
            if block.uid:
                if percentile < 100 and random.randint(1, 100) > percentile:
                    logger.debug('Scrub of block {} (UID {}) skipped (percentile is {}).'.format(
                        block.id,
                        block.uid,
                        percentile,
                        ))
                else:
                    self.data_backend.read(block.deref())  # async queue
                    read_jobs += 1
            else:
                logger.debug('Scrub of block {} (UID {}) skipped (sparse).'.format(
                    block.id,
                    block.uid,
                    ))

        # and read
        _log_every_jobs = read_jobs // 200 + 1  # about every half percent
        _log_jobs_counter = 0
        t1 = time.time()
        t_last_run = 0
        for i in range(read_jobs):
            _log_jobs_counter -= 1
            try:
                while True:
                    try:
                        block, offset, length, data = self.data_backend.read_get(timeout=1)
                    except queue.Empty:  # timeout occured
                        continue
                    else:
                        break
            except Exception as e:
                # log e
                logger.error("Exception during reading from the data backend: {}".format(str(e)))
                raise  # use if you want to debug.
                # exit with error
                sys.exit(6)
            if data is None:
                logger.error('Blob not found: {}'.format(str(block)))
                self.meta_backend.set_blocks_invalid(block.uid, block.checksum)
                state = False
                continue
            stats['blocks_read'] += 1
            stats['bytes_read'] += len(data)
            if len(data) != block.size:
                logger.error('Blob has wrong size: {} is: {} should be: {}'.format(
                    block.uid,
                    len(data),
                    block.size,
                    ))
                self.meta_backend.set_blocks_invalid(block.uid, block.checksum)
                state = False
                continue
            data_checksum = self.hash_function(data).hexdigest()
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

            if source:
                source_data = io.read(block.id, sync=True)  # TODO: This is still sync, but how could we do better (easily)?
                stats['source_blocks_read'] += 1
                stats['source_bytes_read'] += len(source_data)
                if source_data != data:
                    logger.error('Source data has changed for block {} '
                        '(UID {}) (is: {} should-be: {}). NOT setting '
                        'this block invalid, because the source looks '
                        'wrong.'.format(
                            block.id,
                            block.uid,
                            self.hash_function(source_data).hexdigest(),
                            data_checksum,
                            ))
                    state = False
                    # We are not setting the block invalid here because
                    # when the block is there AND the checksum is good,
                    # then the source is invalid.
            logger.debug('Scrub of block {} (UID {}) ok.'.format(
                block.id,
                block.uid,
                ))

            if time.time() - t_last_run >= 1:
                # TODO: Log source io status
                t_last_run = time.time()
                t2 = time.time()
                dt = t2-t1
                logger.debug(self.data_backend.thread_status())

                db_queue_status = self.data_backend.queue_status()
                _status = status(
                    'Scrubbing {} ({})'.format(version.name, version_uid),
                    db_queue_status['rq_filled']*100,
                    0,
                    (i + 1) / read_jobs * 100,
                    stats['bytes_read'] / dt,
                    round(read_jobs / (i+1) * dt - dt),
                    )
                notify(self.process_name, _status)
                if _log_jobs_counter <= 0:
                    _log_jobs_counter = _log_every_jobs
                    logger.info(_status)

        if state == True:
            self.meta_backend.set_version_valid(version_uid)
            logger.info('Marked version valid: {}'.format(version_uid))
        else:
            # version is set invalid by set_blocks_invalid.
            logger.error('Marked version invalid because it has errors: {}'.format(version_uid))
        if source:
            io.close()  # wait for all io

        notify(self.process_name)
        return state


    def restore(self, version_uid, target, sparse=False, force=False, continue_from=0):
        # See if the version is locked, i.e. currently in backup
        if not self.locking.lock(version_uid):
            raise LockError('Version {} is locked.'.format(version_uid))
        self.locking.unlock(version_uid)  # no need to keep it locked

        stats = {
                'bytes_read': 0,
                'blocks_read': 0,
                'bytes_written': 0,
                'blocks_written': 0,
                'bytes_throughput': 0,
                'blocks_throughput': 0,
                'bytes_sparse': 0,
                'blocks_sparse': 0,
            }

        version = self.meta_backend.get_version(version_uid)  # raise if version does not exist
        if continue_from:
            notify(self.process_name, 'Restoring Version {} from block id'.format(version_uid, continue_from))
        else:
            notify(self.process_name, 'Restoring Version {}'.format(version_uid))
        blocks = self.meta_backend.get_blocks_by_version(version_uid)
        num_blocks = blocks.count()

        io = self.get_io_by_source(target)
        io.open_w(target, version.size_bytes, force)

        read_jobs = 0
        _log_every_jobs = num_blocks // 200 + 1  # about every half percent
        _log_jobs_counter = 0
        t1 = time.time()
        t_last_run = 0
        num_blocks = blocks.count()
        for i, block in enumerate(blocks.yield_per(1000)):
            if block.id < continue_from:
                continue
            _log_jobs_counter -= 1
            if block.uid:
                self.data_backend.read(block.deref())  # adds a read job
                read_jobs += 1
            elif not sparse:
                io.write(block, b'\0'*block.size)
                stats['blocks_written'] += 1
                stats['bytes_written'] += block.size
                stats['blocks_throughput'] += 1
                stats['bytes_throughput'] += block.size
                logger.debug('Restored sparse block {} successfully ({} bytes).'.format(
                    block.id,
                    block.size,
                    ))
            else:
                stats['blocks_sparse'] += 1
                stats['bytes_sparse'] += block.size
                logger.debug('Ignored sparse block {}.'.format(
                    block.id,
                    ))

            if time.time() - t_last_run >= 1:
                t_last_run = time.time()
                t2 = time.time()
                dt = t2-t1
                logger.debug(io.thread_status() + " " + self.data_backend.thread_status())

                io_queue_status = io.queue_status()
                db_queue_status = self.data_backend.queue_status()
                _status = status(
                    'Restore phase 1/2 (sparse) to {}'.format(target),
                    db_queue_status['rq_filled']*100,
                    io_queue_status['wq_filled']*100,
                    (i + 1) / num_blocks * 100,
                    stats['bytes_throughput'] / dt,
                    round(num_blocks / (i+1) * dt - dt),
                    )
                notify(self.process_name, _status)
                if _log_jobs_counter <= 0:
                    _log_jobs_counter = _log_every_jobs
                    logger.info(_status)

        stats = {
                'bytes_read': 0,
                'blocks_read': 0,
                'bytes_written': 0,
                'blocks_written': 0,
                'bytes_sparse': 0,
                'blocks_sparse': 0,
                'bytes_throughput': 0,
                'blocks_throughput': 0,
            }
        _log_every_jobs = read_jobs // 200 + 1  # about every half percent
        _log_jobs_counter = 0
        t1 = time.time()
        t_last_run = 0
        min_sequential_block_id = MinSequential(continue_from)  # for finding the minimum block-ID until which we have restored ALL blocks
        for i in range(read_jobs):
            _log_jobs_counter -= 1
            try:
                while True:
                    try:
                        block, offset, length, data = self.data_backend.read_get(timeout=.1)
                    except queue.Empty:  # timeout occured
                        continue
                    else:
                        break
            except Exception as e:
                # TODO (restore): write information for continue
                logger.error("Exception during reading from the data backend: {}".format(str(e)))
                #raise  # Enable for debugging
                sys.exit(6)
            assert len(data) == block.size
            stats['blocks_read'] += 1
            stats['bytes_read'] += block.size

            data_checksum = self.hash_function(data).hexdigest()
            def callback(local_block_id):
                def f():
                    min_sequential_block_id.put(local_block_id)
                    stats['blocks_written'] += 1
                    stats['bytes_written'] += block.size
                    stats['blocks_throughput'] += 1
                    stats['bytes_throughput'] += block.size
                return f
            io.write(block, data, callback(block.id))

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

            if time.time() - t_last_run >= 1:
                t_last_run = time.time()
                t2 = time.time()
                dt = t2-t1
                logger.debug(io.thread_status() + " " + self.data_backend.thread_status())

                io_queue_status = io.queue_status()
                db_queue_status = self.data_backend.queue_status()
                _status = status(
                    'Restore phase 2/2 (data) to {}'.format(target),
                    db_queue_status['rq_filled']*100,
                    io_queue_status['wq_filled']*100,
                    (i + 1) / read_jobs * 100,
                    stats['bytes_throughput'] / dt,
                    round(read_jobs / (i+1) * dt - dt),
                    'Last ID: {}'.format(min_sequential_block_id.get()),
                    )
                notify(self.process_name, _status)
                if _log_jobs_counter <= 0:
                    _log_jobs_counter = _log_every_jobs
                    logger.info(_status)


        self.locking.unlock(version_uid)
        io.close()


    def protect(self, version_uid):
        version = self.meta_backend.get_version(version_uid)
        if version.protected:
            raise ValueError('Version {} is already protected.'.format(version_uid))
        self.meta_backend.protect_version(version_uid)


    def unprotect(self, version_uid):
        version = self.meta_backend.get_version(version_uid)
        if not version.protected:
            raise ValueError('Version {} is not protected.'.format(version_uid))
        self.meta_backend.unprotect_version(version_uid)


    def rm(self, version_uid, force=True, disallow_rm_when_younger_than_days=0):
        if not self.locking.lock(version_uid):
            raise LockError('Version {} is locked.'.format(version_uid))
        version = self.meta_backend.get_version(version_uid)
        if version.protected:
            raise ValueError('Version {} is protected. Will not delete.'.format(version_uid))
        if not force:
            # check if disallow_rm_when_younger_than_days allows deletion
            age_days = (datetime.datetime.utcnow() - version.date).days
            if disallow_rm_when_younger_than_days > age_days:
                raise LockError('Version {} is too young. Will not delete.'.format(version_uid))

        num_blocks = self.meta_backend.rm_version(version_uid)
        logger.info('Removed backup version {} with {} blocks.'.format(
            version_uid,
            num_blocks,
            ))
        self.locking.unlock(version_uid)


    def get_sla_breaches(self, name, scheduler, interval, keep, sla):
        """
        Get SLA breaches for version name and tag_name (scheduler)
        """
        # version's name must match and also the scheduler's name must be in tags.
        # They're already sorted by date so the newest is at the end of the list.
        _last_versions_for_name_and_scheduler = [v for v in self.ls() if v.valid and v.name == name and scheduler in [t.name for t in v.tags]]
        sla_breaches = []  # name: list of breaches

        # Check SLA for number of versions to keep for this scheduler
        if len(_last_versions_for_name_and_scheduler) < keep:
            sla_breaches.append('{}: Too few backups. Found {}, should be {}.'.format(
                scheduler,
                len(_last_versions_for_name_and_scheduler),
                keep,
            ))

        if len(_last_versions_for_name_and_scheduler) > keep + 2:  # allow two more during delete time
            sla_breaches.append('{}: Too many backups. Found {}, should be {}.'.format(
                scheduler,
                len(_last_versions_for_name_and_scheduler),
                keep,
            ))

        # Check SLA for age of newest backup
        if _last_versions_for_name_and_scheduler and _last_versions_for_name_and_scheduler[-1].date + interval + sla < datetime.datetime.utcnow():
            sla_breaches.append('{}: Latest backup is too old. Version {} has date {}, new backup due since {}.'.format(
                scheduler,
                _last_versions_for_name_and_scheduler[-1].uid,
                _last_versions_for_name_and_scheduler[-1].date.strftime('%Y-%m-%d %H:%M:%S'),
                (_last_versions_for_name_and_scheduler[-1].date + interval + sla).strftime('%Y-%m-%d %H:%M:%S'),
            ))



        # Check SLA for delta time between versions for this scheduler
        _last_version = 0
        for version in _last_versions_for_name_and_scheduler:
            if _last_version == 0:
                _last_version = version.date
                continue
            if version.date < _last_version + interval - sla or version.date > _last_version + interval + sla:
                sla_breaches.append('{}: Version {} is not in SLA range. It was created at {} and shoud be between {} and {}.'.format(
                    scheduler,
                    version.uid,
                    version.date.strftime('%Y-%m-%d %H:%M:%S'),
                    (_last_version + interval - sla).strftime('%Y-%m-%d %H:%M:%S'),
                    (_last_version + interval + sla).strftime('%Y-%m-%d %H:%M:%S'),
                ))
            _last_version = version.date

        # Check if oldest backup is not older than allowed
        _oldest_allowed = datetime.datetime.utcnow() - keep*interval - sla - relativedelta(days=1)  # always allow 1 day lazy delete time.
        if _last_versions_for_name_and_scheduler and _last_versions_for_name_and_scheduler[0].date < _oldest_allowed:
            sla_breaches.append('{}: Backup too old. Found version_uid {} with backup date {}. Oldest allowed date is {}.'.format(
                scheduler,
                _last_versions_for_name_and_scheduler[0].uid,
                _last_versions_for_name_and_scheduler[0].date.strftime('%Y-%m-%d %H:%M:%S'),
                _oldest_allowed.strftime('%Y-%m-%d %H:%M:%S'),
            ))

        return sla_breaches


    def get_due_backups(self, name, scheduler, interval, keep, sla):
        """
        Returns True if a backup is due for a given scheduler
        """
        RANGE = datetime.timedelta(seconds=30)  # be unsharp when searching and also find backups that will be due in RANGE seconds.
        if keep == 0:
            return False
        _last_versions_for_name_and_scheduler = [v for v in self.ls() if v.valid and v.name == name and scheduler in [t.name for t in v.tags]]
        # Check if now is the time to create a backup for this name and scheduler.
        if not _last_versions_for_name_and_scheduler:  # no backups exist, so require one
            return True
        elif datetime.datetime.utcnow() > (_last_versions_for_name_and_scheduler[-1].date + interval - RANGE):   # no backup within interval exists, so require one
            return True
        return False


    def _generate_auto_tags(self, version_name):
        """ Generates automatic tag suggestions by looking up versions with
        the same name and comparing their dates.
        This algorithm will
        - give the tag 'b_daily' if the last b_daily tagged version for this name is > 0 days ago
        - give the tag 'b_weekly' if the last b_weekly tagged version for this name is > 6 days ago
        - give the tag 'b_monthly' if the last b_monthly tagged version for this name is > 1 month ago
        """
        all_versions = self.meta_backend.get_versions()
        versions = [{'date': v.date.date(), 'tags': [t.name for t in v.tags]} for v in all_versions if v.name == version_name]

        b_daily = None
        b_weekly = None
        b_monthly = None
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


    def backup(self, name, snapshot_name, source, hints, from_version, tag=None, expire=None, continue_version=None):
        """ Create a backup from source.
        If hints are given, they must be tuples of (offset, length, exists)
        where offset and length are integers and exists is a boolean. Then, only
        data within hints will be backed up.
        Otherwise, the backup reads source and looks if checksums match with
        the target.
        If continue_version is given, this version will be continued, i.e.
        existing blocks will not be read again.
        """
        stats = {
                'version_size_bytes': 0,
                'version_size_blocks': 0,
                'bytes_checked': 0,
                'blocks_checked': 0,
                'bytes_read': 0,
                'blocks_read': 0,
                'bytes_written': 0,
                'blocks_written': 0,
                'bytes_found_dedup': 0,
                'blocks_found_dedup': 0,
                'bytes_sparse': 0,
                'blocks_sparse': 0,
                'bytes_throughput': 0,
                'blocks_throughput': 0,
                'start_time': time.time(),
            }
        io = self.get_io_by_source(source)
        io.open_r(source)
        source_size = io.size()

        size = math.ceil(source_size / self.block_size)
        stats['version_size_bytes'] = source_size
        stats['version_size_blocks'] = size

        # Sanity check: check hints for validity, i.e. too high offsets, ...
        if hints is not None and len(hints) > 0:
            max_offset = max([h[0]+h[1] for h in hints])
            if max_offset > source_size:
                raise ValueError('Hints have higher offsets than source file.')

        # Find out which blocks to read
        if hints is not None:
            sparse_blocks = blocks_from_hints([hint for hint in hints if not hint[2]], self.block_size)
            read_blocks = blocks_from_hints([hint for hint in hints if hint[2]], self.block_size)
        else:
            sparse_blocks = []
            read_blocks = range(size)
        sparse_blocks = set(sparse_blocks)
        read_blocks = set(read_blocks)

        # Validity check
        if from_version:
            old_version = self.meta_backend.get_version(from_version)  # raise if not exists
            if not old_version.valid:
                raise RuntimeError('You cannot base on an invalid version.')

        existing_block_ids = set()
        if continue_version:
            version_uid = continue_version
            _v = self.meta_backend.get_version(version_uid)  # raise if version does not exist
            if _v.size_bytes != source_size:
                raise ValueError('Version to continue backup from has a different size than the source. Cannot continue.')
            if _v.valid:
                raise ValueError('You cannot continue a valid version.')
            # reduce read_blocks and sparse_blocks by existing blocks
            existing_block_ids = set(self.meta_backend.get_block_ids_by_version(version_uid))
            read_blocks = read_blocks - existing_block_ids
            sparse_blocks = sparse_blocks - existing_block_ids
        else:
            # Create new version
            version_uid = self.meta_backend.set_version(name, snapshot_name, size, source_size, 0)  # initially marked invalid
            if not self.locking.lock(version_uid):
                raise LockError('Version {} is locked.'.format(version_uid))

        # Sanity check:
        # Check some blocks outside of hints if they are the same in the
        # from_version backup and in the current backup. If they
        # don't, either hints are wrong (e.g. from a wrong snapshot diff)
        # or source doesn't match. In any case, the resulting backup won't
        # be good.
        check_block_ids = set()
        if from_version and hints:
            ignore_blocks = list(set(range(size)) - read_blocks - sparse_blocks)
            random.shuffle(ignore_blocks)
            num_check_blocks = 10
            # 50% from the start
            check_block_ids = ignore_blocks[:num_check_blocks//2]
            # and 50% from random locations
            check_block_ids = set(check_block_ids + random.sample(ignore_blocks, num_check_blocks//2))

        # Find blocks to base on
        if from_version:
            # Make sure we're based on a valid version.
            old_blocks = iter(self.meta_backend.get_blocks_by_version(from_version).yield_per(1000))
        else:
            old_blocks = iter([])

        # Create read jobs
        _log_every_jobs = size // 200 + 1  # about every half percent
        _log_jobs_counter = 0
        t1 = time.time()
        t_last_run = 0
        for block_id in range(size):
            _log_jobs_counter -= 1
            # Create a block, either based on an old one (from_version) or a fresh one
            _have_old_block = False
            try:
                old_block = next(old_blocks)
            except StopIteration:  # No old block found, we create a fresh one
                block_uid = None
                checksum = None
                block_size = self.block_size
                enc_envkey = ''
                enc_nonce = None
                enc_version = 0
                valid = 1
            else:  # Old block found, maybe base on that one
                assert old_block.id == block_id
                block_uid = old_block.uid
                checksum = old_block.checksum
                block_size = old_block.size
                valid = old_block.valid
                enc_envkey = old_block.enc_envkey
                enc_nonce = old_block.enc_nonce
                enc_version = old_block.enc_version
                _have_old_block = True
            # the last block can differ in size, so let's check
            _offset = block_id * self.block_size
            new_block_size = min(self.block_size, source_size - _offset)
            if new_block_size != block_size:
                # last block changed, so set back all info
                block_size = new_block_size
                block_uid = None
                checksum = None
                enc_envkey = ''
                enc_nonce = None
                enc_version = 0
                valid = 1
                _have_old_block = False

            # Build list of blocks to be read or skipped
            # Read (read_blocks, check_block_ids or block is invalid) or not?
            if block_id in read_blocks:
                logger.debug('Block {}: Reading'.format(block_id))
                io.read(block_id, read=True)
            elif block_id in check_block_ids and _have_old_block and checksum:
                logger.debug('Block {}: Reading / checking'.format(block_id))
                io.read(block_id, read=True, metadata={'check': True, 'checksum': checksum, 'block_size': block_size, 'enc_envkey': enc_envkey, 'enc_nonce': 'enc_nonce', 'enc_version': enc_version})
            elif not valid:
                logger.debug('Block {}: Reading because not valid'.format(block_id))
                io.read(block_id, read=True)
                assert _have_old_block
            elif block_id in sparse_blocks:
                logger.debug('Block {}: Sparse'.format(block_id))
                # Sparse blocks have uid and checksum None.
                io.read(block_id, read=False, metadata={'block_uid': None, 'checksum': None, 'block_size': block_size, 'enc_envkey': '', 'enc_version': 0, 'enc_nonce': None})
            elif block_id in existing_block_ids:
                logger.debug('Block {}: Exists in continued version'.format(block_id))
                io.read(block_id, read=False, metadata={'skip': True})
            else:
                logger.debug('Block {}: Fresh empty or existing'.format(block_id))
                io.read(block_id, read=False, metadata={'block_uid': block_uid, 'checksum': checksum, 'block_size': block_size, 'enc_envkey': enc_envkey, 'enc_nonce': enc_nonce, 'enc_version': enc_version})

            # log and process output
            if time.time() - t_last_run >= 1:
                t_last_run = time.time()
                t2 = time.time()
                dt = t2-t1
                logger.debug(io.thread_status() + " " + self.data_backend.thread_status())

                io_queue_status = io.queue_status()
                db_queue_status = self.data_backend.queue_status()
                _status = status(
                    'Backing up (1/2: Prep) {}'.format(source),
                    0,
                    0,
                    (block_id + 1) / size * 100,
                    0.0,
                    round(size / (block_id+1) * dt - dt),
                    )
                notify(self.process_name, _status)
                if _log_jobs_counter <= 0:
                    _log_jobs_counter = _log_every_jobs
                    logger.info(_status)


        # now use the readers and write
        _log_every_jobs = size // 200 + 1  # about every half percent
        _log_jobs_counter = 0
        t1 = time.time()
        t_last_run = 0

        _written_blocks_queue = queue.Queue()  # contains ONLY blocks that have been written to the data backend.

        # consume the read jobs
        for i in range(size):
            _log_jobs_counter -= 1
            block_id, data, data_checksum, metadata = io.get()

            if data:
                block_size = len(data)
                stats['blocks_read'] += 1
                stats['bytes_read'] += block_size
                stats['blocks_throughput'] += 1
                stats['bytes_throughput'] += block_size

                existing_block = None
                if self.dedup:
                    existing_block = self.meta_backend.get_block_by_checksum(data_checksum)

                if data == b'\0' * block_size:
                    block_uid = None
                    data_checksum = None
                    _written_blocks_queue.put((block_id, version_uid, block_uid, data_checksum, block_size, None, 0, None))
                elif existing_block and existing_block.size == block_size:
                    block_uid = existing_block.uid
                    _written_blocks_queue.put((block_id,
                        version_uid,
                        block_uid,
                        data_checksum,
                        block_size,
                        #existing_block.enc_envkey.encode('ascii'),
                        binascii.unhexlify(existing_block.enc_envkey),
                        existing_block.enc_version,
                        #existing_block.enc_nonce.encode('ascii')))
                        binascii.unhexlify(existing_block.enc_nonce),
                        ))
                else:
                    # This is the whole reason for _written_blocks_queue. We must first write the block to
                    # the backup data store before we write it to the database. Otherwise we can't support
                    # backup continuation reliably.
                    def callback(local_block_id, local_version_uid, local_data_checksum, local_block_size):
                        def f(_block_uid, enc_envkey, enc_version, enc_nonce):
                            _written_blocks_queue.put((
                                local_block_id,
                                local_version_uid,
                                _block_uid,
                                local_data_checksum,
                                local_block_size,
                                enc_envkey,
                                enc_version,
                                enc_nonce
                                ))
                        return f
                    block_uid = self.data_backend.save(data, callback=callback(block_id, version_uid, data_checksum, block_size))  # this will re-raise an exception from a worker thread

                    stats['blocks_written'] += 1
                    stats['bytes_written'] += block_size

                if metadata and 'check' in metadata:
                    # Perform sanity check
                    if not metadata['checksum'] == data_checksum or not metadata['block_size'] == block_size:
                        logger.error("Source and backup don't match in regions outside of the hints.")
                        logger.error("Looks like the hints don't match or the source is different.")
                        logger.error("Found wrong source data at block {}: offset {} with max. length {}".format(
                            block_id,
                            block_id * self.block_size,
                            block_size
                            ))
                        # remove version
                        self.meta_backend.rm_version(version_uid)
                        sys.exit(5)
                    stats['blocks_checked'] += 1
                    stats['bytes_checked'] += block_size
            else:
                # No data means that this block is from the previous version or is empty as of the hints, so just store metadata.
                # Except it's a skipped block from a continued version.
                if not 'skip' in metadata:
                    block_uid = metadata['block_uid']
                    data_checksum = metadata['checksum']
                    block_size = metadata['block_size']
                    enc_envkey = metadata['enc_envkey']
                    if enc_envkey:
                        enc_envkey = binascii.unhexlify(enc_envkey)
                    enc_version = metadata['enc_version']
                    enc_nonce = metadata['enc_nonce']
                    if enc_nonce:
                        enc_nonce = binascii.unhexlify(enc_nonce)
                    _written_blocks_queue.put((
                        block_id,
                        version_uid,
                        block_uid,
                        data_checksum,
                        block_size,
                        enc_envkey,
                        enc_version,
                        enc_nonce
                        ))
                    if metadata['block_uid'] is None:
                        stats['blocks_sparse'] += 1
                        stats['bytes_sparse'] += block_size
                    else:
                        stats['blocks_found_dedup'] += 1
                        stats['bytes_found_dedup'] += block_size

            # Set the blocks from the _written_blocks_queue
            while True:
                try:
                    q_block_id, q_version_uid, q_block_uid, q_data_checksum, q_block_size, q_enc_envkey, q_enc_version, q_enc_nonce = _written_blocks_queue.get(block=False)
                except queue.Empty:
                    break
                else:
                    self.meta_backend.set_block(q_block_id,
                        q_version_uid,
                        q_block_uid,
                        q_data_checksum,
                        q_block_size,
                        valid=1,
                        enc_envkey=q_enc_envkey,
                        enc_version=q_enc_version,
                        enc_nonce=q_enc_nonce,
                        _commit=True,
                        )

            # log and process output
            if time.time() - t_last_run >= 1:
                t_last_run = time.time()
                t2 = time.time()
                dt = t2-t1
                logger.debug(io.thread_status() + " " + self.data_backend.thread_status())

                io_queue_status = io.queue_status()
                db_queue_status = self.data_backend.queue_status()
                _status = status(
                    'Backing up (2/2: Data) {}'.format(source),
                    io_queue_status['rq_filled']*100,
                    db_queue_status['wq_filled']*100,
                    (i + 1) / size * 100,
                    stats['bytes_throughput'] / dt,
                    round(size / (i+1) * dt - dt),
                    )
                notify(self.process_name, _status)
                if _log_jobs_counter <= 0:
                    _log_jobs_counter = _log_every_jobs
                    logger.info(_status)

        # check if there are any exceptions left
        if self.data_backend.last_exception:
            logger.error("Exception during saving to the data backend: {}".format(str(self.data_backend.last_exception)))
        else:
            io.close()  # wait for all readers
            self.data_backend.close()  # wait for all writers

        # Set the rest of the blocks from the _written_blocks_queue
        while True:
            try:
                q_block_id, q_version_uid, q_block_uid, q_data_checksum, q_block_size, q_enc_envkey, q_enc_version, q_enc_nonce = _written_blocks_queue.get(block=False)
            except queue.Empty:
                break
            else:
                self.meta_backend.set_block(q_block_id,
                    q_version_uid,
                    q_block_uid,
                    q_data_checksum,
                    q_block_size,
                    valid=1,
                    enc_envkey=q_enc_envkey,
                    enc_version=q_enc_version,
                    enc_nonce=q_enc_nonce,
                    _commit=True)

        tags = []
        if tag is not None:
            if isinstance(tag, list):
                tags = tag
            else:
                tags.append(tag)
        else:
            if not continue_version:
                tags = self._generate_auto_tags(name)
        for tag in tags:
            self.meta_backend.add_tag(version_uid, tag)

        if expire:
            self.meta_backend.expire_version(version_uid, expire)

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

        if self.data_backend.last_exception:
            logger.info('New invalid version: {} (Tags: [{}])'.format(version_uid, ','.join(tags)))
        else:
            self.meta_backend.set_version_valid(version_uid)
            logger.info('New version: {} (Tags: [{}])'.format(version_uid, ','.join(tags)))

        self.meta_backend._commit()
        self.locking.unlock(version_uid)

        if self.data_backend.last_exception:
            sys.exit(6)  # i.e. kill all the remaining workers

        return version_uid


    def rekey(self, oldkey):
        cc = self.data_backend.cc_latest
        for block in self.meta_backend.get_blocks():
            if block.enc_version == cc.VERSION:
                print("Re-Keying block {}".format(block.uid))
                newkey = cc.wrap_key(cc.unwrap_key(binascii.unhexlify(block.enc_envkey), binascii.unhexlify(oldkey)))
                self.meta_backend.set_block_enc_envkey(block, newkey)
            else:
                print("Ignoring block {} (wrong encryption version {})".format(block.uid, block.enc_version))
        self.meta_backend.session.commit()  # ONLY at the very end. One transaction for them all so that interruptions don't matter.


    def cleanup_fast(self, dt=3600):
        """ Delete unreferenced blob UIDs """
        if not self.locking.lock('backy-cleanup-fast'):
            raise LockError('Another backy cleanup is running.')

        for uid_list in self.meta_backend.get_delete_candidates(dt):
            logger.debug('Cleanup-fast: Deleting UIDs from data backend: {}'.format(uid_list))
            no_del_uids = []
            no_del_uids = self.data_backend.rm_many(uid_list)
            if no_del_uids:
                logger.info('Cleanup-fast: Unable to delete these UIDs from data backend: {}'.format(uid_list))
        self.locking.unlock('backy-cleanup-fast')


    def cleanup_full(self, prefix=None):
        """ Delete unreferenced blob UIDs starting with <prefix> """
        # in this mode, we compare all existing uids in data and meta.
        # make sure, no other backy will start
        if not self.locking.lock('backy'):
            self.locking.unlock('backy')
            raise LockError('Other backy instances are running.')
        # make sure, no other backy is running
        if len(find_other_procs(self.process_name)) > 1:
            raise LockError('Other backy instances are running.')
        active_blob_uids = set(self.data_backend.get_all_blob_uids(prefix))
        active_block_uids = set(self.meta_backend.get_all_block_uids(prefix))
        delete_candidates = active_blob_uids.difference(active_block_uids)
        for delete_candidate in delete_candidates:
            logger.debug('Cleanup: Removing UID {}'.format(delete_candidate))
            try:
                self.data_backend.rm(delete_candidate)
            except FileNotFoundError:
                continue
        logger.info('Cleanup: Removed {} blobs'.format(len(delete_candidates)))
        self.locking.unlock('backy')


    def add_tag(self, version_uid, name):
        self.meta_backend.add_tag(version_uid, name)


    def remove_tag(self, version_uid, name):
        self.meta_backend.remove_tag(version_uid, name)


    def expire_version(self, version_uid, expire):
        self.meta_backend.expire_version(version_uid, expire)


    def close(self):
        self.meta_backend.close()
        self.data_backend.close()


    def export(self, version_uid, f):
        self.meta_backend.export(version_uid, f)
        return f


    def import_(self, f):
        self.meta_backend.import_(f)
