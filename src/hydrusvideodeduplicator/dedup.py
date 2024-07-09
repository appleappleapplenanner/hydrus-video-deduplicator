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
    decode_phash_from_str,
    encode_phash_to_str,
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

        if not DedupeDB.is_db_accessible(verbose=True):
            print("[red] Could not search for duplicates.")
        else:
            # Number of potential duplicates before adding more. Just for user info.
            pre_dedupe_count = self.client.get_potential_duplicate_count_hydrus()

            DedupeDB.populate_video_ids()
            self.find_potential_duplicates()

            # Statistics for user
            post_dedupe_count = self.client.get_potential_duplicate_count_hydrus()
            new_dedupes_count = post_dedupe_count - pre_dedupe_count
            if new_dedupes_count > 0:
                print(f"[green] {new_dedupes_count} new potential duplicate pairs marked for manual processing!")
            else:
                print("[green] No new potential duplicate pairs found.")

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
            phash_str: str = encode_phash_to_str(phash)
        except Exception as exc:
            print("[red] Failed to calculate a perceptual hash.")
            self.hydlog.exception(exc)
            self.hydlog.error(f"Errored file hash: {video_hash}")
            return FailedVideo(video_hash)
        else:
            # "just in case" error checking
            if phash_str is None or phash_str == "[]":
                return FailedVideo(video_hash)

            PHashedVideo = namedtuple("PHashedVideo", "video_hash perceptual_hash")
            return PHashedVideo(video_hash, phash_str)

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
                    if video_hash not in hashdb or "perceptual_hash" not in hashdb[video_hash]
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
                            row["perceptual_hash"] = perceptual_hash
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

    def find_potential_duplicates(
        self,
    ) -> None:
        """Find potential duplicates in the database and mark them in Hydrus."""
        with SqliteDict(
            str(DedupeDB.get_db_file_path()), tablename="video_id", flag="r", outer_stack=False
        ) as video_id_table:
            with SqliteDict(
                str(DedupeDB.get_db_file_path()), tablename="videos", flag="c", autocommit=True, outer_stack=False
            ) as hashdb:
                current_hash = None
                try:
                    total = len(hashdb)

                    with tqdm(
                        dynamic_ncols=True, total=total, desc="Finding duplicates", unit="video", colour="BLUE"
                    ) as pbar:
                        # -1 is all cores, -2 is all cores but one
                        with Parallel(n_jobs=self.job_count) as parallel:
                            for i, video1_hash in enumerate(video_id_table):
                                current_hash = video1_hash
                                pbar.update(1)

                                row = hashdb[video1_hash]

                                # Store last furthest searched position in the database for each element
                                # This way you only have to start searching at that place instead of at i+1 if it exists
                                farthest_search_index = i
                                if "farthest_search_index" in row:
                                    farthest_search_index = row["farthest_search_index"]

                                assert farthest_search_index <= total
                                if farthest_search_index == total:
                                    # This file has already been searched for dupes against all other videos in the DB
                                    continue

                                parallel(
                                    delayed(self.compare_videos)(
                                        video1_hash,
                                        video2_hash,
                                        row["perceptual_hash"],
                                        hashdb[video2_hash]["perceptual_hash"],
                                    )
                                    for video2_hash in islice(video_id_table, farthest_search_index + 1, None)
                                )

                                # Video has now been compared against all other videos for dupes,
                                # so update farthest_search_index to the current length of the table
                                row["farthest_search_index"] = total
                                hashdb[video1_hash] = row

                except KeyboardInterrupt:
                    print("[yellow] Duplicate search was interrupted!")
                else:
                    # current_hash can be None if Hydrus DB has no files...
                    if current_hash is not None:
                        # TODO: Do we still need this? I don't think so.
                        # Set the last element farthest_search_index to the end of the
                        # table since it won't get hashed because of the islice optimization
                        row = hashdb[current_hash]
                        row["farthest_search_index"] = total
                        hashdb[current_hash] = row
