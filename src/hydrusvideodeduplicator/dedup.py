from __future__ import annotations

import logging
import os
from collections import namedtuple
from itertools import islice
from typing import TYPE_CHECKING

from joblib import Parallel, delayed
from rich import print
from sqlitedict import SqliteDict
from tqdm import tqdm

if TYPE_CHECKING:
    from collections.abc import Sequence

import hydrusvideodeduplicator.hydrus_api as hydrus_api

from .client import HVDClient
from .db import DedupeDB
from .hashing import (
    compute_phash,
    get_phash_similarity,
)
from .page_logger import HydrusPageLogger


class FailedVideo:
    def __init__(self, video_hash: str):
        self.video_hash = video_hash


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
        failed_page_name: str | None = None,
    ):
        self.client = client
        if verify_connection:
            self.client.verify_api_connection()
        self.job_count = job_count
        self.page_logger = None if failed_page_name is None else HydrusPageLogger(self.client, failed_page_name)

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
            "system:filetype=video, gif, apng",
            "system:has duration",
            "system:file service is not currently in trash",
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

    def fetch_and_hash_file(self, video_hash: str) -> tuple | FailedVideo:
        """Retrieves the video from Hydrus and calculates its perceptual hash"""
        try:
            video_response = self.client.client.get_file(hash_=video_hash)
        except hydrus_api.HydrusAPIException:
            print("[red] Failed to get video from Hydrus.")
            self.hydlog.error("Error getting video from Hydrus.")
            return FailedVideo(video_hash)

        # Calculate perceptual_hash
        try:
            phash = compute_phash(video_response.content)
        except Exception as exc:
            print("[red] Failed to calculate a perceptual hash.")
            self.hydlog.exception(exc)
            self.hydlog.error(f"Errored file hash: {video_hash}")
            return FailedVideo(video_hash)
        else:
            # TODO: removed some error checking here, was that required?
            PHashedVideo = namedtuple("PHashedVideo", "video_hash perceptual_hash")
            return PHashedVideo(video_hash, phash)

    def add_perceptual_hashes_to_db(self, overwrite: bool, video_hashes: Sequence[str]) -> None:
        """
        Retrieves the video from Hydrus,
        calculates the perceptual hash,
        and then add it to the database.
        """

        DedupeDB.create_db_dir()

        with SqliteDict(
            str(DedupeDB.get_db_file_path()), tablename="videos", flag="c", autocommit=True, outer_stack=False
        ) as hashdb:
            dbsize = os.path.getsize(DedupeDB.get_db_file_path())

            # Cache len(hashdb) because it's O(n) to get the length.
            if (dblen := len(hashdb)) > 0:
                self.hydlog.info(f"Database found of length {dblen}, size {dbsize} bytes")
            else:
                self.hydlog.info(f"Database not found. Creating one at {DedupeDB.get_db_file_path()}")

            if overwrite:
                new_video_hashes = video_hashes
                print(f"[yellow] Overwriting {dblen} existing hashes.")
            else:
                # Filter existing hashes
                new_video_hashes = [
                    video_hash
                    for video_hash in video_hashes
                    if video_hash not in hashdb or "perceptual_hash_raw" not in hashdb[video_hash]
                ]

            print(f"[blue] Found {len(new_video_hashes)} videos to process")

            success_hash_count = 0
            failed_hash_count = 0
            try:
                self.hydlog.info("Starting perceptual hash processing")
                with tqdm(total=len(new_video_hashes), dynamic_ncols=True, unit="video", colour="BLUE") as pbar:
                    with Parallel(n_jobs=self.job_count, return_as="generator_unordered") as parallel:
                        result_generator = parallel(
                            delayed(self.fetch_and_hash_file)(video_hash) for video_hash in new_video_hashes
                        )
                        for result in result_generator:
                            if isinstance(result, FailedVideo):
                                if self.page_logger:
                                    # TODO: Is this thread-safe as is?
                                    # Joblib throws a pickling error if trying to use lock to make it so.
                                    self.page_logger.add_failed_video(result.video_hash)
                                failed_hash_count += 1
                                pbar.update(1)
                                continue
                            video_hash = result.video_hash
                            perceptual_hash = result.perceptual_hash
                            row = hashdb.get(video_hash, {})
                            row["perceptual_hash_raw"] = perceptual_hash
                            hashdb[video_hash] = row

                            success_hash_count += 1
                            pbar.update(1)

            except KeyboardInterrupt:
                print("[yellow] Perceptual hash processing was interrupted!")

            else:
                print("[green] Finished perceptual hash processing.")

            finally:
                if failed_hash_count > 0:
                    print(f"[yellow] Perceptual hash processing had {failed_hash_count} failed files.")
                    if self.page_logger is None:
                        print(
                            "\nTip: You can see what files failed directly in Hydrus by "
                            "creating a page with the name 'failed' and "
                            "running the program with '--failed-page-name=failed'\n"
                        )
                print(f"[green] Added {success_hash_count} new videos to the database.")

    # TODO: re-add type hints
    def compare_videos(self, video1_hash: str, video2_hash: str, video1_phash, video2_phash) -> None:
        """Compare videos and mark them as potential duplicates in Hydrus if they are similar."""
        # TODO: remove these assignments
        hash_a = video1_phash
        hash_b = video2_phash
        similarity = get_phash_similarity(hash_a, hash_b)

        if similarity >= self.threshold:
            if self._DEBUG:
                # Getting the file names will be VERY slow because of the API call
                # file_names = get_file_names_hydrus(self.client.client, [video1_hash, video2_hash])
                # self.hydlog.info(f"Duplicates filenames: {file_names}")
                self.hydlog.info(f'"Similar {similarity}%: {video1_hash}" and "{video2_hash}"')

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
        if not DedupeDB.is_db_accessible(verbose=True):
            print("[red] Could not search for duplicates.")
            return

        # Number of potential duplicates before adding more. Just for user info.
        pre_dedupe_count = self.client.get_potential_duplicate_count_hydrus()

        video_counter = 0
        with SqliteDict(
            str(DedupeDB.get_db_file_path()), tablename="videos", flag="c", autocommit=True, outer_stack=False
        ) as videos_table:
            current_hash = None
            try:
                # Make a copy of the video hashes here so we can preserve their order because SqliteDict row order
                # changes during writes for the farthest search index. This is a bandaid solution.
                # This assumes SqliteDict row order is preserved when opened and closed, even if it's not preserved
                # while modifying elements.
                video_hashes = [video_hash for video_hash in videos_table]
                total = len(video_hashes)

                with tqdm(
                    dynamic_ncols=True, total=total, desc="Finding duplicates", unit="video", colour="BLUE"
                ) as pbar:
                    # -1 is all cores, -2 is all cores but one
                    with Parallel(n_jobs=self.job_count) as parallel:
                        for i, video1_hash in enumerate(video_hashes):
                            current_hash = video1_hash
                            video_counter += 1
                            pbar.update(1)

                            row = videos_table[video1_hash]

                            # We only care about combinations of pairs, not permutations,
                            # so start at the next unique comparison.
                            start_index = i + 1

                            # Start at the last furthest searched position in the database for each element.
                            # This way you only have to start searching at that place instead of at i+1, if it exists
                            if "farthest_search_index" in row:
                                start_index = row["farthest_search_index"]

                            assert start_index <= total
                            if start_index == total:
                                # This file has already been searched for dupes against all other videos in the DB
                                continue

                            parallel(
                                delayed(self.compare_videos)(
                                    video1_hash,
                                    video2_hash,
                                    row["perceptual_hash_raw"],
                                    videos_table[video2_hash]["perceptual_hash_raw"],
                                )
                                for video2_hash in islice(video_hashes, start_index, None)
                            )

                            # Video has now been compared against all other videos for dupes,
                            # so update farthest_search_index to the current length of the table
                            row["farthest_search_index"] = total
                            videos_table[video1_hash] = row

            except KeyboardInterrupt:
                print("[yellow] Duplicate search was interrupted!")
            else:
                # current_hash can be None if Hydrus DB has no files...
                if current_hash is not None:
                    # Set the last element farthest_search_index to the end of the
                    # table since it won't get hashed because of the islice optimization
                    row = videos_table[current_hash]
                    row["farthest_search_index"] = total
                    videos_table[current_hash] = row

        # Statistics for user
        post_dedupe_count = self.client.get_potential_duplicate_count_hydrus()
        new_dedupes_count = post_dedupe_count - pre_dedupe_count
        if new_dedupes_count > 0:
            print(f"[green] {new_dedupes_count} new potential duplicates marked for processing!")
        else:
            print("[green] No new potential duplicates found.")
