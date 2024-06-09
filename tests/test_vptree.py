from __future__ import annotations

import logging
import time
import unittest
from typing import TYPE_CHECKING

from hydrusvideodeduplicator.client import HVDClient
from hydrusvideodeduplicator.dedup import HydrusVideoDeduplicator

if TYPE_CHECKING:
    pass

class TestVptree(unittest.TestCase):
    log = logging.getLogger(__name__)
    log.setLevel(logging.WARNING)
    logging.basicConfig()

    def setUp(self):
        pass

    def test_temp(self):
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main(module="test_vptree")
