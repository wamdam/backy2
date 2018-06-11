#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os
import re
import threading
import time

from benji.exception import UsageError
from benji.io import IO as _IO
from benji.logging import logger
from benji.utils import data_hexdigest


class IO(_IO):

    NAME = 'file'

    def __init__(self, config, block_size, hash_function):
        super().__init__(config, block_size, hash_function)

        self._writer = None

    def open_r(self, io_name):
        super().open_r(io_name)

        _s = re.match('^file://(.+)$', io_name)
        if not _s:
            raise UsageError('Not a valid io name: {} . Need a file path, e.g. file:///somepath/file.'.format(io_name))
        self.io_name = _s.groups()[0]

    def open_w(self, io_name, size=None, force=False):
        _s = re.match('^file://(.+)$', io_name)
        if not _s:
            raise UsageError('Not a valid io name: {} . Need a file path, e.g. file:///somepath/file.'.format(io_name))
        self.io_name = _s.groups()[0]

        if os.path.exists(self.io_name):
            if not force:
                raise FileExistsError('Restore target {} already exists. Force the restore if you want to overwrite it.'
                                      .format(self.io_name))
            else:
                if size < self.size():
                    raise IOError(
                        'Restore target {} is too small. Its size is {} bytes, but we need {} bytes for the restore.'
                        .format(self.io_name, self.size(), size))
        else:
            # create the file
            with open(self.io_name, 'wb') as f:
                f.seek(size - 1)
                f.write(b'\0')

    def size(self):
        with open(self.io_name, 'rb') as source_file:
            source_file.seek(0, 2)  # to the end
            source_size = source_file.tell()
        return source_size

    def _read(self, block):
        with open(self.io_name, 'rb') as source_file:
            offset = block.id * self._block_size
            t1 = time.time()
            source_file.seek(offset)
            data = source_file.read(block.size)
            t2 = time.time()
            # throw away cache
            os.posix_fadvise(source_file.fileno(), offset, block.size, os.POSIX_FADV_DONTNEED)

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
            self._writer = open(self.io_name, 'rb+')

        offset = block.id * self._block_size
        self._writer.seek(offset)
        written = self._writer.write(data)
        os.posix_fadvise(self._writer.fileno(), offset, len(data), os.POSIX_FADV_DONTNEED)
        assert written == len(data)

    def close(self):
        super().close()
        if self._writer:
            self._writer.close()
