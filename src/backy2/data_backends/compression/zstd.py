import zstd

class Compression:

    NAME = 'zstd'
    DEFAULT_LEVEL = 1

    def __init__(self, materials):

        if 'level' in materials:
            self.level = materials['level']
        else:
            self.level = self.DEFAULT_LEVEL

    def compress(self, data):
        return zstd.compress(data, self.level), {}

    def uncompress(self, data, materials):
        return zstd.uncompress(data)
