#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.io.lib import rados  # XXX use default rados lib?
from backy2.io.lib import rbd    # XXX use default rbd lib?
from backy2.logging import logger
from backy2.io import IO as _IO
from functools import reduce
from operator import or_
import queue
import re
import threading
import time

class IO(_IO):
    simultaneous_reads = 10
    pool_name = None
    image_name = None
    snapshot_name = None
    mode = None
    _writer = None

    def __init__(self, config, block_size, hash_function):
        self.simultaneous_reads = config.getint('simultaneous_reads')
        ceph_conffile = config.get('ceph_conffile')
        self.block_size = block_size
        self.hash_function = hash_function
        self._reader_threads = []
        self._inqueue = queue.Queue()  # infinite size for all the blocks
        self._outqueue = queue.Queue(self.simultaneous_reads)
        self.cluster = rados.Rados(conffile=ceph_conffile)
        self.cluster.connect()
        # create a bitwise or'd list of the configured features
        self.new_image_features = reduce(or_, [getattr(rbd, feature) for feature in config.getlist('new_image_features')])


    def open_r(self, io_name):
        # io_name has the form rbd://pool/imagename@snapshotname or rbd://pool/imagename
        self.mode = 'r'
        self.io_name = io_name
        img_name = re.match('^rbd://([^/]+)/([^@]+)@?(.+)?$', io_name)
        if not img_name:
            raise RuntimeError('Not a valid io name: {} . Need pool/imagename or pool/imagename@snapshotname'.format(io_name))
        self.pool_name, self.image_name, self.snapshot_name = img_name.groups()
        # try opening it and quit if that's not possible.
        try:
            ioctx = self.cluster.open_ioctx(self.pool_name)
        except rados.ObjectNotFound:
            logger.error('Pool not found: {}'.format(self.pool_name))
            exit('Error opening backup source.')

        try:
            rbd.Image(ioctx, self.image_name, self.snapshot_name, read_only=True)
        except rbd.ImageNotFound:
            logger.error('Image/Snapshot not found: {}@{}'.format(self.image_name, self.snapshot_name))
            exit('Error opening backup source.')

        for i in range(self.simultaneous_reads):
            _reader_thread = threading.Thread(target=self._reader, args=(i,))
            _reader_thread.daemon = True
            _reader_thread.start()
            self._reader_threads.append(_reader_thread)


    def open_w(self, io_name, size=None, force=False):
        """ size is bytes
        """
        self.mode = 'w'
        # io_name has the form rbd://pool/imagename@snapshotname or rbd://pool/imagename
        self.io_name = io_name
        img_name = re.match('^rbd://([^/]+)/([^@]+)$', io_name)
        if not img_name:
            raise RuntimeError('Not a valid io name: {} . Need pool/imagename'.format(io_name))
        self.pool_name, self.image_name = img_name.groups()
        # try opening it and quit if that's not possible.
        try:
            ioctx = self.cluster.open_ioctx(self.pool_name)
        except rados.ObjectNotFound:
            logger.error('Pool not found: {}'.format(self.pool_name))
            exit('Error opening backup source.')

        try:
            rbd.Image(ioctx, self.image_name)
        except rbd.ImageNotFound:
            rbd.RBD().create(ioctx, self.image_name, size, old_format=False, features=self.new_image_features)
        else:
            if not force:
                logger.error('Image already exists: {}'.format(self.image_name))
                exit('Error opening restore target.')
            else:
                if size < self.size():
                    logger.error('Target size is too small. Has {}b, need {}b.'.format(self.size(), size))
                    exit('Error opening restore target.')


    def size(self):
        ioctx = self.cluster.open_ioctx(self.pool_name)
        with rbd.Image(ioctx, self.image_name, self.snapshot_name, read_only=True) as image:
            size = image.size()
        return size


    def _reader(self, id_):
        """ self._inqueue contains Blocks.
        self._outqueue contains (block, data, data_checksum)
        """
        ioctx = self.cluster.open_ioctx(self.pool_name)
        with rbd.Image(ioctx, self.image_name, self.snapshot_name, read_only=True) as image:
            while True:
                block = self._inqueue.get()
                if block is None:
                    logger.debug("IO {} finishing.".format(id_))
                    self._outqueue.put(None)  # also let the outqueue end
                    break
                offset = block.id * self.block_size
                t1 = time.time()
                data = image.read(offset, self.block_size, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
                t2 = time.time()
                # throw away cache
                if not data:
                    raise RuntimeError('EOF reached on source when there should be data.')

                data_checksum = self.hash_function(data).hexdigest()
                if not block.valid:
                    logger.debug('IO {} re-read block (because it was invalid) {} (checksum {})'.format(id_, block.id, data_checksum))
                else:
                    logger.debug('IO {} read block {} (checksum {}...) in {:.2f}s) '
                        '(Inqueue size: {}, Outqueue size: {})'.format(
                            id_,
                            block.id,
                            data_checksum[:16],
                            t2-t1,
                            self._inqueue.qsize(),
                            self._outqueue.qsize()
                            ))

                self._outqueue.put((block, data, data_checksum))
                self._inqueue.task_done()


    def read(self, block, sync=False):
        """ Adds a read job """
        self._inqueue.put(block)
        if sync:
            rblock, data, data_checksum = self.get()
            if rblock.id != block.id:
                raise RuntimeError('Do not mix threaded reading with sync reading!')
            return data


    def get(self):
        d = self._outqueue.get()
        self._outqueue.task_done()
        return d


    def write(self, block, data):
        # print("Writing block {} with {} bytes of data".format(block.id, len(data)))
        if not self._writer:
            ioctx = self.cluster.open_ioctx(self.pool_name)
            self._writer = rbd.Image(ioctx, self.image_name)

        offset = block.id * self.block_size
        written = self._writer.write(data, offset, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
        assert written == len(data)


    def close(self):
        if self.mode == 'r':
            for _reader_thread in self._reader_threads:
                self._inqueue.put(None)  # ends the threads
            for _reader_thread in self._reader_threads:
                _reader_thread.join()
        elif self.mode == 'w':
            self._writer.close()

