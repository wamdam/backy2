# noinspection PyUnresolvedReferences
from sparsebitfield import SparseBitfield

from benji.metadata import BlockUidBase


class BlockUidHistory:

    def __init__(self):
        self._history = {}

    def add(self, block_uid):
        history = self._history
        if block_uid.left not in history:
            history[block_uid.left] = SparseBitfield()
        history[block_uid.left].add(block_uid.right)

    def __contains__(self, block_uid):
        history = self._history
        if not isinstance(block_uid, BlockUidBase):
            raise TypeError('Called with wrong type {}.'.format(type(block_uid)))
        if block_uid.left not in history:
            return False
        return block_uid.right in history[block_uid.left]
