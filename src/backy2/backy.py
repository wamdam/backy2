# -*- encoding: utf-8 -*-

import math
from backy2.logging import logger
import random
import time

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

    def __init__(self, meta_backend, data_backend, reader, block_size, hash_function):
        self.meta_backend = meta_backend
        self.data_backend = data_backend
        self.reader = reader
        self.block_size = block_size
        self.hash_function = hash_function


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
                else:
                    if source:
                        source_data = self.reader.read(block, sync=True)
                        if source_data != data:
                            import pdb; pdb.set_trace()
                            logger.error('Source data has changed for block {} '
                                '(UID {}) (is: {} should-be: {}'.format(
                                    block.id,
                                    block.uid,
                                    self.hash_function(source_data).hexdigest(),
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
                    data_checksum = self.hash_function(data).hexdigest()
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


