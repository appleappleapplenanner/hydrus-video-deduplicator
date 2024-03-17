"""

These tests use clips from the Big Buck Bunny movie and Sintel movie,
which are licensed under Creative Commons Attribution 3.0
(https://creativecommons.org/licenses/by/3.0/).
(c) copyright 2008, Blender Foundation / www.bigbuckbunny.org
(c) copyright Blender Foundation | durian.blender.org
Blender Foundation | www.blender.org

"""

from __future__ import annotations

import logging
import unittest
from pathlib import Path
from typing import TYPE_CHECKING

from hydrusvideodeduplicator.vpdqpy.vpdqpy import Vpdq, VpdqHash

if TYPE_CHECKING:
    from collections.abc import Iterable


class TestVpdq(unittest.TestCase):
    log = logging.getLogger(__name__)
    log.setLevel(logging.WARNING)
    logging.basicConfig()

    def setUp(self):
        all_vids_dir = Path(__file__).parent / "testdb" / "videos"
        self.video_hashes_dir = Path(__file__).parent / "testdb" / "video hashes"
        assert all_vids_dir.is_dir()
        assert self.video_hashes_dir.is_dir()

        # Similarity videos should be checked for similarity.
        # They should be similar to other videos in the same group, but not to videos in other groups.
        # They are in separate folders for organizational purposes.
        similarity_vids_dirs = ["big_buck_bunny", "sintel"]
        self.similarity_vids: list[Path] = []
        for vids_dir in similarity_vids_dirs:
            self.similarity_vids.extend(Path(all_vids_dir / vids_dir).glob("*"))

        # Strange videos should hash but not be checked for similarity.
        # They're used to test that the program doesn't crash
        # when it encounters a video with odd characteristics like extremely short length
        # or a video that has tiny dimensions, etc. The more of these added the better.
        # They shouldn't be compared because they might be similar to other videos, but not all of them in a group.
        strange_vids_dir = "strange"
        self.strange_vids: list[Path] = Path(all_vids_dir / strange_vids_dir).glob("*")

    # This should be run with a known good version of Vpdq to generate known good hashes.
    # If VPDQ changes, these hashes will need to be updated this needs
    # to be run once with overwrite = True to generate new good hashes.
    #
    # Hashes after changes to Vpdq should be byte-for-byte identical
    # to the hashes generated by the current version of Vpdq.
    def generate_known_good_hashes(self, vids: Iterable[Path], overwrite: bool = False):
        vids_to_be_hashed = []
        for vid in vids:
            hash_file = self.video_hashes_dir / Path(f"{vid.name}.txt")
            if not hash_file.exists() or overwrite:
                vids_to_be_hashed.append(vid)

        vids_hashes = self.calc_hashes(vids_to_be_hashed)
        for vid in vids_hashes.items():
            with open(self.video_hashes_dir / f"{vid[0].name}.txt", "w") as hashes_file:
                hashes_file.write(Vpdq.vpdq_to_json(vid[1]))

    # Return if two videos are supposed to be similar
    # This uses the prefix SXX where XX is an arbitrary group number.
    # If two videos have the same SXX they should be similar,
    # if they don't they should NOT be similar.
    def similar_group(self, vid1: Path, vid2: Path) -> bool:
        # If either video doesn't have a group, they're not similar
        if vid1.name.split("_")[0][0] != "S" or vid2.name.split("_")[0][0] != "S":
            return False

        vid1_group = vid1.name.split("_")[0]
        vid2_group = vid2.name.split("_")[0]
        return vid1_group == vid2_group

    # Hash videos
    # Several other functions call this, so it needs to be correct and catch all bad hashes.
    def calc_hashes(self, vids: list[Path]) -> dict[Path, VpdqHash]:
        vids_hashes = {}
        for vid in vids:
            perceptual_hash = Vpdq.computeHash(vid)
            vids_hashes[vid] = perceptual_hash
            self.assertTrue(len(perceptual_hash) > 0)

            perceptual_hash_json = Vpdq.vpdq_to_json(perceptual_hash)
            self.assertTrue(perceptual_hash_json != "[]")
        return vids_hashes

    # Hash all videos. They should all have hashes.
    def test_hashing(self):
        self.calc_hashes(self.similarity_vids)
        self.calc_hashes(self.strange_vids)

    # Hash videos and test if they are identical to the known good hashes.
    # Change overwrite to true to generate new hashes, then rerun, but it should always be committed as overwrite=False
    def test_hashing_identical(self):
        vids = self.similarity_vids
        self.generate_known_good_hashes(vids=vids, overwrite=False)
        vids_hashes = self.calc_hashes(vids)

        for phash_path, phash in vids_hashes.items():
            with open(self.video_hashes_dir / f"{phash_path.name}.txt", "r") as hashes_file:
                phash_str = Vpdq.vpdq_to_json(phash)
                expected_hash = hashes_file.readline()
                self.log.error(phash_path.name)  # Needs to be error to show up in log
                similar, similarity = Vpdq.is_similar(phash, Vpdq.json_to_vpdq(expected_hash))
                self.assertTrue((0.0 <= similarity) and (similarity <= 100.0))
                self.assertEqual(
                    expected_hash,
                    phash_str,
                    msg=f"Hashes not identical for file {phash_path.name}. \n {expected_hash} \n {phash_str}",
                )

    # Compare similar videos. They should be similar if they're in the same similarity group.
    def test_compare_similarity_true(self):
        vids_hashes = self.calc_hashes(self.similarity_vids)
        for vid1 in vids_hashes.items():
            for vid2 in vids_hashes.items():
                if vid1[0] == vid2[0]:
                    continue

                similar, similarity = Vpdq.is_similar(vid1[1], vid2[1])
                self.assertTrue((0.0 <= similarity) and (similarity <= 100.0))

                with self.subTest(msg=f"Similar: {similar}", vid1=vid1[0].name, vid2=vid2[0].name):
                    if self.similar_group(vid1[0], vid2[0]):
                        self.assertTrue(similar, msg=f"{vid1[1]}, \n {vid2[1]}")
                    else:
                        self.assertFalse(similar, msg=f"{vid1[1]}, \n {vid2[1]}")


if __name__ == "__main__":
    unittest.main(module="test_vpdqpy")
