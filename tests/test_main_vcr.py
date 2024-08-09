from __future__ import annotations

import json
import logging
import os
import unittest
import zipfile
from typing import TYPE_CHECKING

import vcr
from hydrusvideodeduplicator.__main__ import main

from .check_testdb import check_testdb_exists

if TYPE_CHECKING:
    pass

import uuid
from pathlib import Path
from tempfile import TemporaryDirectory


def somedbdir():
    return str(uuid.uuid4().hex)


CASSETTES_DIR = Path(__file__).parent / "testdb/fixtures/vcr_cassettes"


def unzip_all(zip_dir: Path, extract_to: Path):
    for item in os.listdir(zip_dir):
        if item.endswith(".zip"):
            zip_file_path = os.path.join(zip_dir, item)
            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(extract_to)
                print(f"Extracted: {item}")


class TestMainVcr(unittest.TestCase):
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)
    logging.basicConfig()

    def setUp(self):
        check_testdb_exists()
        # Unzip all the cassettes
        # TODO: Check if the corresponding yaml exists and don't overwrite to save disk writes.
        unzip_all(CASSETTES_DIR, CASSETTES_DIR)

    def test_main(self):
        GENERATE_CASSETTE_RUN = False
        record_mode = "all" if GENERATE_CASSETTE_RUN else "none"  # see vcrpy docs
        cassette_file = CASSETTES_DIR / "main.yaml"
        if not GENERATE_CASSETTE_RUN:
            self.assertTrue(
                cassette_file.exists(),
                f"Cassette file: {cassette_file} does not exist. Need to generate it or fix something.",
            )
        else:
            cassette_file.unlink(missing_ok=True)
        with vcr.use_cassette(cassette_file, record_mode=record_mode) as cass:
            if not GENERATE_CASSETTE_RUN:
                expected_pair_count = int(
                    json.loads(cass.responses[-1]["body"]["string"])["potential_duplicates_count"]
                )
                # sanity check
                self.assertGreater(
                    expected_pair_count,
                    0,
                    "Cassette potential duplicates count is not >0. Something is wrong with the vcr potential duplicates count.",  # noqa: E501
                )

            with TemporaryDirectory() as tmpdir:
                db_dir = Path(tmpdir) / somedbdir()

                num_similar_pairs = main(
                    "3b3cf10cc13862818ea95ddecfe434bed0828fb319b1ff56413917b471b566ab",
                    "https://localhost:45869",
                    job_count=1,  # TODO: vcr doesn't work with more than 1 jobcount
                    dedup_database_dir=db_dir,
                )

                if not GENERATE_CASSETTE_RUN:
                    self.assertEqual(
                        num_similar_pairs, expected_pair_count, "Number of similar files found is unexpected."
                    )
                    self.log.info(f"{num_similar_pairs} similar file pairs found in main test.")
                else:
                    self.assertFalse(
                        True, "Cassette generated. Change GENERATE_CASSETTE_RUN to False and rerun the test."
                    )


if __name__ == "__main__":
    unittest.main(module="test_main_vcr")
