import random
import unittest

from benji.blockuidhistory import BlockUidHistory
from benji.metadata import BlockUid


class BlockUidHistoryTestCase(unittest.TestCase):

    def test_contains(self):
        history = BlockUidHistory()
        blocks_in = set()
        blocks_out = set()
        block_exists = set()
        for i in range(0, 10000):
            block = BlockUid(random.randint(1, 10000000), random.randint(1, 100000000))
            if block in block_exists:
                continue
            block_exists.add(block)
            if random.randint(1, 100) > 50:
                blocks_in.add(block)
                history.add(block)
                self.assertTrue(block in history)
            else:
                blocks_out.add(block)
                self.assertFalse(block in history)
        if len(block_exists) <= 10:
            history_lst = []
            for left, sbf in history._history.items():
                for right in sbf:
                    history_lst.append(BlockUid(left, right))
            print('All blocks        : {}'.format(sorted(block_exists)))
            print('History           : {}'.format(sorted(history_lst)))
            print('History (expected): {}'.format(sorted(blocks_in)))
            print('Not in history    : {}'.format(sorted(blocks_out)))
        for block in blocks_in:
            self.assertTrue(block in history)
        for block in blocks_out:
            self.assertFalse(block in history)