#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.enterprise import rados
from backy2.enterprise import rbd
from backy2.logging import logger
from backy2.enterprise.readers import Reader as _Reader
import queue
import re
import threading
import time

class Reader(_Reader):
    simultaneous_reads = 10
    pool_name = None
    image_name = None
    snapshot_name = None

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


    def open(self, source):
        self.source = source  # pool/imagename@snapshotname or pool/imagename
        img_name = re.match('^([^/]+)/([^@]+)@?(.+)?$', source)
        if not img_name:
            raise RuntimeError('Not a source: {} . Need pool/imagename or pool/imagename@snapshotname'.format(source))
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
                    logger.debug("Reader {} finishing.".format(id_))
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
                    logger.debug('Reader {} re-read block (because it was invalid) {} (checksum {})'.format(id_, block.id, data_checksum))
                else:
                    logger.debug('Reader {} read block {} (checksum {}...) in {:.2f}s) '
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


    def close(self):
        for _reader_thread in self._reader_threads:
            self._inqueue.put(None)  # ends the threads
        for _reader_thread in self._reader_threads:
            _reader_thread.join()


