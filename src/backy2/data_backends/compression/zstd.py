import threading

import zstandard

from backy2.config import Config


class Compression:

    NAME = 'zstd'
    DEFAULT_LEVEL = 3

    def __init__(self, *, materials):
        self.level = Config.get_from_dict(materials,
                                          'level',
                                          self.DEFAULT_LEVEL,
                                          types=int,
                                          check_func=lambda v: v >= 1 and v <= zstandard.MAX_COMPRESSION_LEVEL,
                                          check_message='Option level must be between 1 and {} (inclusive)'
                                                            .format(zstandard.MAX_COMPRESSION_LEVEL)
                                          )

        self.compressors = {}
        self.decompressors = {}

    def _get_compressor(self):
        thread_id = threading.get_ident()

        if thread_id in self.compressors:
            return self.compressors[thread_id]

        cctx = zstandard.ZstdCompressor(level=self.level,
                                        write_checksum=False, # We have our own checksum
                                        write_content_size=False) # We know the uncompressed size

        self.compressors[thread_id]= cctx
        return cctx

    def _get_decompressor(self, dict_id=0):
        thread_id = threading.get_ident()

        if thread_id in self.decompressors:
            return self.decompressors[thread_id]

        dctx = zstandard.ZstdDecompressor()

        self.decompressors[thread_id]= dctx
        return dctx

    def compress(self, *, data):
        return self._get_compressor().compress(data), {}

    def uncompress(self, *, data, materials, original_size):
        return self._get_decompressor().decompress(data, max_output_size=original_size)
