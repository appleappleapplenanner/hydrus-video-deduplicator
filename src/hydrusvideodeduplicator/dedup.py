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
    from collections.abc import Generator, Iterable, Sequence
    from typing import Any

import hydrusvideodeduplicator.hydrus_api as hydrus_api

from .client import HVDClient
from .config import DEDUP_DATABASE_DIR, DEDUP_DATABASE_FILE
from .dedup_util import database_accessible
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
            video_hashes = list(self.client.retrieve_video_hashes(search_tags))
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

        # Create database folder
        try:
            os.makedirs(DEDUP_DATABASE_DIR, exist_ok=False)
            # Exception before this log if directory already exists
            self.hydlog.info(f"Created DB dir {DEDUP_DATABASE_DIR}")
        except OSError:
            pass

        with SqliteDict(
            str(DEDUP_DATABASE_FILE), tablename="videos", flag="c", autocommit=True, outer_stack=False
        ) as hashdb:
            dbsize = os.path.getsize(DEDUP_DATABASE_FILE)

            # Cache len(hashdb) because it's O(n) to get the length.
            if (dblen := len(hashdb)) > 0:
                self.hydlog.info(f"Database found of length {dblen}, size {dbsize} bytes")
            else:
                self.hydlog.info(f"Database not found. Creating one at {DEDUP_DATABASE_FILE}")

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

            hash_count = 0
            try:
                self.hydlog.info("Starting perceptual hash processing")

                with tqdm(total=len(new_video_hashes), dynamic_ncols=True, unit="video", colour="BLUE") as pbar:
                    # Change to return_as='unordered_generator' when joblib supports it! (should be soon)
                    with Parallel(n_jobs=self.job_count, return_as='generator') as parallel:
                        result_generator = parallel(
                            delayed(self.fetch_and_hash_file)(video_hash) for video_hash in new_video_hashes
                        )
                        for result in result_generator:
                            if result is None:
                                continue
                            video_hash = result.video_hash
                            perceptual_hash = result.perceptual_hash
                            row = hashdb.get(video_hash, {})
                            row["perceptual_hash"] = perceptual_hash
                            hashdb[video_hash] = row

                            hash_count += 1
                            pbar.update(1)

            except KeyboardInterrupt:
                print("[yellow] Perceptual hash processing was interrupted!")

            else:
                print("[green] Finished perceptual hash processing.")

            finally:
                print(f"[green] Added {hash_count} new videos to the database.")

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

            new_relationship = {
                "hash_a": str(video1_hash),
                "hash_b": str(video2_hash),
                "relationship": int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
                "do_default_content_merge": True,
            }

            self.client.client.set_file_relationships([new_relationship])

    @staticmethod
    def clear_search_cache() -> None:
        """Delete cache search index value for each video in database"""
        if not database_accessible(DEDUP_DATABASE_FILE, tablename="videos"):
            return

        with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c") as hashdb:
            for key in hashdb:
                row = hashdb[key]
                if "farthest_search_index" in row:
                    del row["farthest_search_index"]
                    hashdb[key] = row
                    hashdb.commit()
        print("[green] Cleared search cache.")

    def _find_potential_duplicates(
        self,
    ) -> None:
        """Find potential duplicates in the database and mark them in Hydrus."""
        if not database_accessible(DEDUP_DATABASE_FILE, tablename="videos", verbose=True):
            print("[red] Could not search for duplicates.")
            return

        # Number of potential duplicates before adding more. Just for user info.
        pre_dedupe_count = self.client.get_potential_duplicate_count_hydrus()

        video_counter = 0
        with SqliteDict(
            str(DEDUP_DATABASE_FILE), tablename="videos", flag="c", autocommit=True, outer_stack=False
        ) as hashdb:
            try:
                total = len(hashdb)

                with tqdm(
                    dynamic_ncols=True, total=total, desc="Finding duplicates", unit="video", colour="BLUE"
                ) as pbar:
                    # -1 is all cores, -2 is all cores but one
                    with Parallel(n_jobs=self.job_count) as parallel:
                        for i, video1_hash in enumerate(hashdb):
                            video_counter += 1
                            pbar.update(1)

                            row = hashdb[video1_hash]

                            # Store last furthest searched position in the database for each element
                            # This way you only have to start searching at that place instead of at i+1 if it exists
                            farthest_search_index = row.setdefault("farthest_search_index", i + 1)

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
                                for video2_hash in islice(hashdb, row["farthest_search_index"], None)
                            )

                            # Video has now been compared against all other videos for dupes,
                            # so update farthest_search_index to the current length of the table
                            row["farthest_search_index"] = total
                            hashdb[video1_hash] = row

            except KeyboardInterrupt:
                print("[yellow] Duplicate search was interrupted!")
            else:
                # Set the last element farthest_search_index to the end of the
                # table since it won't get hashed because of the islice optimization
                row = hashdb[video1_hash]
                row["farthest_search_index"] = total
                hashdb[video1_hash] = row

        # Statistics for user
        post_dedupe_count = self.client.get_potential_duplicate_count_hydrus()
        new_dedupes_count = post_dedupe_count - pre_dedupe_count
        if new_dedupes_count > 0:
            print(f"[green] {new_dedupes_count} new potential duplicates marked for processing!")
        else:
            print("[green] No new potential duplicates found.")

    @staticmethod
    def batched(iterable: Iterable, batch_size: int) -> Generator[tuple, Any, None]:
        """
        Batch data into tuples of length batch_size. The last batch may be shorter."
        batched('ABCDEFG', 3) --> ABC DEF G
        DO NOT use this for iterating over the database. Use batched_and_save_db instead.
        """
        assert batch_size >= 1
        it = iter(iterable)
        while batch := tuple(islice(it, batch_size)):
            yield batch

    @staticmethod
    def batched_and_save_db(
        db: SqliteDict,
        batch_size: int = 1,
        chunk_size: int = 1,
    ) -> Generator[dict[str, dict[str, Any]], Any, None]:
        """
        Batch rows of into rows of length n and save changes after each batch or after chunk_size batches.
        """
        assert batch_size >= 1 and chunk_size >= 1
        it = iter(db.items())
        chunk_counter = 0
        while batch_items := dict(islice(it, batch_size)):
            yield batch_items
            chunk_counter += 1

            # Save changes after chunk_size batches
            if chunk_counter % chunk_size == 0:
                db.commit()

    def is_files_deleted_hydrus(self, file_hashes: Iterable[str]) -> dict[str, bool]:
        """
        Check if files are trashed or deleted in Hydrus
        Returns a dictionary of hash : trashed_or_not
        """
        videos_metadata = self.client.client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)[
            "metadata"
        ]

        result = {}
        for video_metadata in videos_metadata:
            # This should never happen, but it shouldn't break the program if it does
            if "hash" not in video_metadata:
                logging.error("Hash not found for potentially trashed file.")
                continue
            video_hash = video_metadata["hash"]
            is_deleted: bool = video_metadata.get("is_deleted", False)
            result[video_hash] = is_deleted
        return result

    @staticmethod
    def update_search_cache(new_total: int | None = None) -> None:
        """
        Update the search cache to clamp the farthest_search_index to the current length of the database
        """
        assert new_total is None or new_total >= 0

        if not database_accessible(DEDUP_DATABASE_FILE, tablename="videos"):
            return

        BATCH_SIZE = 256
        with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c", outer_stack=False) as hashdb:
            if new_total is None:
                new_total = len(hashdb)
            for batched_items in HydrusVideoDeduplicator.batched_and_save_db(hashdb, BATCH_SIZE):
                for item in batched_items.items():
                    row = hashdb[item[0]]
                    if 'farthest_search_index' in row and row['farthest_search_index'] > new_total:
                        row['farthest_search_index'] = new_total
                        hashdb[item[0]] = row

    def clear_trashed_files_from_db(self) -> None:
        """
        Delete trashed and deleted files from Hydrus from the database
        """
        if not database_accessible(DEDUP_DATABASE_FILE, tablename="videos"):
            return

        try:
            with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c", outer_stack=False) as hashdb:
                # This is EXPENSIVE. sqlitedict gets len by iterating over the entire database!
                if (total := len(hashdb)) < 1:
                    return

                delete_count = 0
                print(f"[blue] Database found with {total} videos already hashed.")
                try:
                    with tqdm(
                        dynamic_ncols=True,
                        total=total,
                        desc="Clearing trashed videos",
                        unit="video",
                        colour="BLUE",
                    ) as pbar:
                        BATCH_SIZE = 32
                        for batched_items in self.batched_and_save_db(hashdb, BATCH_SIZE):
                            is_trashed_result = self.is_files_deleted_hydrus(batched_items.keys())
                            for result in is_trashed_result.items():
                                video_hash = result[0]
                                is_trashed = result[1]
                                if is_trashed is True:
                                    del hashdb[video_hash]
                                    delete_count += 1
                            pbar.update(min(BATCH_SIZE, total - pbar.n))
                except Exception as exc:
                    print("[red] Failed to clear trashed videos cache.")
                    print(exc)
                    self.hydlog.error(exc)
                finally:
                    if delete_count > 0:
                        print(f"Cleared {delete_count} trashed videos from the database.")
                    self.update_search_cache(total - delete_count)

        except OSError as exc:
            self.hydlog.info(exc)
