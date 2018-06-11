#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import re
import threading
import time

import rados
import rbd

from benji.exception import UsageError, ConfigurationError
from benji.io import IO as _IO
from benji.logging import logger
from benji.utils import data_hexdigest


class IO(_IO):

    NAME = 'rbd'

    def __init__(self, config, block_size, hash_function):
        super().__init__(config, block_size, hash_function)

        our_config = config.get('io.{}'.format(self.NAME), types=dict)
        ceph_conffile = config.get_from_dict(our_config, 'cephConfigFile', types=str)
        self._cluster = rados.Rados(conffile=ceph_conffile)
        self._cluster.connect()
        # create a bitwise or'd list of the configured features
        self._new_image_features = 0
        for feature in config.get_from_dict(our_config, 'newImageFeatures', types=list):
            try:
                self._new_image_features = self._new_image_features | getattr(rbd, feature)
            except AttributeError:
                raise ConfigurationError('io.{}.newImageFeatures: Unknown image feature {}.'.format(self.NAME, feature))

        self._writer = None

    def open_r(self, io_name):
        # io_name has the form rbd://pool/imagename@snapshotname or rbd://pool/imagename
        super().open_r(io_name)

        self.io_name = io_name
        img_name = re.match('^rbd://([^/]+)/([^@]+)@?(.+)?$', io_name)
        if not img_name:
            raise UsageError(
                'Not a valid io name: {} . Need pool/imagename or pool/imagename@snapshotname.'.format(io_name))
        self.pool_name, self.image_name, self.snapshot_name = img_name.groups()
        # try opening it and quit if that's not possible.
        try:
            ioctx = self._cluster.open_ioctx(self.pool_name)
        except rados.ObjectNotFound:
            raise FileNotFoundError('Pool not found: {}'.format(self.pool_name)) from None

        try:
            rbd.Image(ioctx, self.image_name, self.snapshot_name, read_only=True)
        except rbd.ImageNotFound:
            raise FileNotFoundError('Image or snapshot not found: {}'.format(self.io_name)) from None

    def open_w(self, io_name, size=None, force=False):
        # io_name has the form rbd://pool/imagename@snapshotname or rbd://pool/imagename
        self.io_name = io_name
        img_name = re.match('^rbd://([^/]+)/([^@]+)$', io_name)
        if not img_name:
            raise UsageError('Not a valid io name: {} . Need pool/imagename.'.format(io_name))
        self.pool_name, self.image_name = img_name.groups()
        # try opening it and quit if that's not possible.
        try:
            ioctx = self._cluster.open_ioctx(self.pool_name)
        except rados.ObjectNotFound:
            raise FileNotFoundError('Pool not found: {}'.format(self.pool_name)) from None

        try:
            rbd.Image(ioctx, self.image_name)
        except rbd.ImageNotFound:
            rbd.RBD().create(ioctx, self.image_name, size, old_format=False, features=self._new_image_features)
        else:
            if not force:
                raise FileExistsError('Restore target {} already exists. Force the restore if you want to overwrite it.'
                                      .format(self.io_name))
            else:
                if size < self.size():
                    raise IOError(
                        'Restore target {} is too small. Its size is {} bytes, but we need {} bytes for the restore.'
                        .format(self.io_name, self.size(), size))

    def size(self):
        ioctx = self._cluster.open_ioctx(self.pool_name)
        with rbd.Image(ioctx, self.image_name, self.snapshot_name, read_only=True) as image:
            size = image.size()
        return size

    def _read(self, block):
        ioctx = self._cluster.open_ioctx(self.pool_name)
        with rbd.Image(ioctx, self.image_name, self.snapshot_name, read_only=True) as image:
            offset = block.id * self._block_size
            t1 = time.time()
            data = image.read(offset, block.size, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
            t2 = time.time()

        if not data:
            raise EOFError('EOF reached on source when there should be data.')

        data_checksum = data_hexdigest(self._hash_function, data)
        logger.debug('{} read block {} (checksum {}...) in {:.2f}s'.format(
            threading.current_thread().name,
            block.id,
            data_checksum[:16],
            t2 - t1,
        ))

        return block, data, data_checksum

    def write(self, block, data):
        if not self._writer:
            ioctx = self._cluster.open_ioctx(self.pool_name)
            self._writer = rbd.Image(ioctx, self.image_name)

        offset = block.id * self._block_size
        written = self._writer.write(data, offset, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
        assert written == len(data)

    def close(self):
        super().close()
        if self._writer:
            self._writer.close()
