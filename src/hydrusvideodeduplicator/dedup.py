from __future__ import annotations

import logging
from collections import namedtuple
from typing import TYPE_CHECKING

from rich import print
from tqdm import tqdm

if TYPE_CHECKING:
    from collections.abc import Sequence

import hydrusvideodeduplicator.hydrus_api as hydrus_api

from .client import HVDClient
from .db import DedupeDB
from .hashing import (
    compute_phash,
    decode_phash_from_str,
    encode_phash_to_str,
    get_phash_similarity,
)


class HydrusVideoDeduplicator:
    hydlog = logging.getLogger("hvd")
    hydlog.setLevel(logging.INFO)
    threshold: float = 75.0
    _DEBUG = False

    def __init__(
        self,
        client: HVDClient,
        verify_connection: bool = True,
        job_count: int = -2,
    ):
        self.client = client
        if verify_connection:
            self.client.verify_api_connection()
        self.job_count = job_count

    def deduplicate(
        self,
        overwrite: bool = False,
        custom_query: Sequence[str] | None = None,
        skip_hashing: bool = False,
    ) -> None:
        """
        Run all deduplicate functions:
        1. Retrieve video hashes
        2. Calculate perceptual hashes
        3. Find potential duplicates
        """

        # Add perceptual hashes to video files
        # system:filetype tags are really inconsistent
        search_tags = [
            'system:filetype=video, gif, apng',
            'system:has duration',
            'system:file service is not currently in trash',
        ]

        if custom_query is not None:
            # Remove whitespace and empty strings
            custom_query = [x for x in custom_query if x.strip()]
            if len(custom_query) > 0:
                search_tags.extend(custom_query)
                print(f"[yellow] Custom Query: {custom_query}")

        if skip_hashing:
            print("[yellow] Skipping perceptual hashing")
        else:
            video_hashes = list(self.client.get_video_hashes(search_tags))
            self.add_perceptual_hashes_to_db(overwrite=overwrite, video_hashes=video_hashes)

        self._find_potential_duplicates()

        self.hydlog.info("Deduplication done.")

    def fetch_and_hash_file(self, video_hash: str) -> tuple | None:
        """Retrieves the video from Hydrus and calculates its perceptual hash"""
        try:
            video_response = self.client.client.get_file(hash_=video_hash)
        except hydrus_api.HydrusAPIException:
            print("[red] Failed to get video from Hydrus.")
            self.hydlog.error("Error getting video from Hydrus.")
            return None

        # Calculate perceptual_hash
        try:
            phash = compute_phash(video_response.content)
            phash_str: str = encode_phash_to_str(phash)
        except Exception as exc:
            print("[red] Failed to calculate a perceptual hash.")
            self.hydlog.exception(exc)
            self.hydlog.error(f"Errored file hash: {video_hash}")
            return None
        else:
            assert phash_str and phash_str != "[]"
            PHashedVideo = namedtuple("PHashedVideo", "video_hash perceptual_hash")
            return PHashedVideo(video_hash, phash_str)

    def add_perceptual_hashes_to_db(self, overwrite: bool, video_hashes: Sequence[str]) -> None:
        """
        Retrieves the video from Hydrus,
        calculates the perceptual hash,
        and then add it to the database.
        """

        cur = DedupeDB.create_cursor()

        new_video_hashes = []
        if overwrite:
            new_video_hashes = video_hashes
            print(f"[yellow] Overwriting {len(video_hashes)} existing hashes.")
        else:
            # Filter existing hashes
            for video_hash in video_hashes:
                # Get the hash_id from the video hash (if it exists).
                hash_id = DedupeDB.get_hash_id_from_hash(video_hash)
                # If the file isn't in files, then we want to hash it, because it won't be in perceptual_hash_map either.
                if not hash_id:
                    new_video_hashes.append(video_hash)
                else:
                    # If the file isn't in perceptual_hash_map, then we want to hash it.
                    res = cur.execute(
                        """
                        SELECT phash_id FROM perceptual_hashes
                        WHERE (SELECT phash_id FROM perceptual_hash_map WHERE hash_id = :hash_id);
                        """,
                        {"hash_id": hash_id},
                    ).fetchone()
                    if res is None or (len(res) == 0):
                        new_video_hashes.append(video_hash)

            print(f"[blue] Found {len(new_video_hashes)} videos to process")

            videos_phashed_count = 0
            try:
                self.hydlog.info("Starting perceptual hash processing")

                with tqdm(total=len(new_video_hashes), dynamic_ncols=True, unit="video", colour="BLUE") as pbar:
                    for video_hash in new_video_hashes:
                        result = self.fetch_and_hash_file(video_hash)
                        if result is None:
                            continue
                        video_hash, perceptual_hash = result

                        # TODO: Both of these inserts should be in a single transaction.

                        # Insert the file into the files table.
                        cur.execute(
                            "INSERT OR IGNORE INTO files (hash) VALUES (:hash);",
                            {"hash": video_hash},
                        )

                        # Insert the phash into the perceptual_hashes table.
                        # If it already exists, there's nothing to do, so ignore.
                        # It's possible to already exist if two files have the same perceptual hash.
                        cur.execute(
                            "INSERT OR IGNORE INTO perceptual_hashes (phash) VALUES (:phash);",
                            {"phash": perceptual_hash},
                        )

                        # Get the hash_id from the video hash.
                        hash_id = DedupeDB.get_hash_id_from_hash(video_hash)

                        DedupeDB.associate_perceptual_hash(hash_id, perceptual_hash)

                        videos_phashed_count += 1
                        DedupeDB.get_connection().commit()
                        pbar.update(1)

            except KeyboardInterrupt:
                print("[yellow] Perceptual hash processing was interrupted!")

            else:
                print("[green] Finished perceptual hash processing.")

            finally:
                print(f"[green] Added {videos_phashed_count} new videos to the database.")

    def compare_videos(self, video1_hash: str, video2_hash: str, video1_phash: str, video2_phash: str) -> None:
        """Compare videos and mark them as potential duplicates in Hydrus if they are similar."""
        hash_a = decode_phash_from_str(video1_phash)
        hash_b = decode_phash_from_str(video2_phash)
        similarity = get_phash_similarity(hash_a, hash_b)

        if similarity >= self.threshold:
            if self._DEBUG:
                # Getting the file names will be VERY slow because of the API call
                # file_names = get_file_names_hydrus(self.client.client, [video1_hash, video2_hash])
                # self.hydlog.info(f"Duplicates filenames: {file_names}")
                self.hydlog.info(f"\"Similar {similarity}%: {video1_hash}\" and \"{video2_hash}\"")

            self.mark_videos_as_duplicates(video1_hash, video2_hash)

    def mark_videos_as_duplicates(self, video1_hash: str, video2_hash: str):
        """Mark a pair of videos as duplicates in Hydrus."""
        new_relationship = {
            "hash_a": video1_hash,
            "hash_b": video2_hash,
            "relationship": int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
            "do_default_content_merge": True,
        }

        self.client.client.set_file_relationships([new_relationship])

    def _find_potential_duplicates(
        self,
    ) -> None:
        """Find potential duplicates in the database and mark them in Hydrus."""
        # Number of potential duplicates before adding more. Just for user info.
        pre_dedupe_count = self.client.get_potential_duplicate_count_hydrus()

        cur = DedupeDB.create_cursor()
        cur.execute(
            """
            SELECT count(hash_id) FROM perceptual_hash_map
            WHERE hash_id NOT IN deleted_files
            """
        )
        total_perceptual_hashes = cur.fetchone()[0]

        try:
            with tqdm(
                dynamic_ncols=True,
                total=total_perceptual_hashes,
                desc="Finding duplicates",
                unit="video",
                colour="BLUE",
            ) as pbar:
                cur.execute(
                    """
                    SELECT phash_id, hash_id FROM perceptual_hash_map
                    WHERE hash_id NOT IN deleted_files
                    ORDER BY hash_id ASC;
                    """
                )
                while (row := cur.fetchone()) is not None:
                    phash_id, hash_id = row
                    pbar.update(1)

                    cur_b = DedupeDB.create_cursor()
                    # Avoid O(n^2) comparisons. Just compare to the ones not already compared to.
                    cur_b.execute(
                        """
                        SELECT phash_id, hash_id FROM perceptual_hash_map
                        WHERE hash_id > :hash_id AND hash_id NOT IN deleted_files
                        ORDER BY hash_id ASC;
                        """,
                        {"hash_id": hash_id},
                    )
                    while (row_b := cur_b.fetchone()) is not None:
                        phash_id_b, hash_id_b = row_b

                        # Don't compare to self
                        if hash_id != hash_id_b:
                            hash_a = DedupeDB.get_hash_from_hash_id(hash_id)
                            hash_b = DedupeDB.get_hash_from_hash_id(hash_id_b)
                            phash_a = DedupeDB.get_phash_from_phash_id(phash_id)
                            phash_b = DedupeDB.get_phash_from_phash_id(phash_id_b)

                            self.compare_videos(hash_a, hash_b, phash_a, phash_b)

                        # TODO: Should this be committed less frequently?
                        DedupeDB.get_connection().commit()

        except KeyboardInterrupt:
            print("[yellow] Duplicate search was interrupted!")

        # Statistics for user
        post_dedupe_count = self.client.get_potential_duplicate_count_hydrus()
        new_dedupes_count = post_dedupe_count - pre_dedupe_count
        if new_dedupes_count > 0:
            print(f"[green] {new_dedupes_count} new potential duplicates marked for processing!")
        else:
            print("[green] No new potential duplicates found.")
