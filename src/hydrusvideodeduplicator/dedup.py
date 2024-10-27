from __future__ import annotations

import datetime
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from joblib import Parallel, delayed
from rich import print
from tqdm import tqdm

if TYPE_CHECKING:
    from collections.abc import Sequence

    FileHash = str

import hydrusvideodeduplicator.hydrus_api as hydrus_api

from .client import HVDClient
from .db import DedupeDB, vptree
from .hashing import (
    compute_phash,
    encode_phash_to_str,
)
from .page_logger import HydrusPageLogger


@dataclass
class PerceptuallyHashedFile:
    """Class for perceptually hashed files."""

    file_hash: FileHash
    perceptual_hash: str


@dataclass
class FailedPerceptuallyHashedFile:
    """Class for failed perceptually hashed files."""

    file_hash: FileHash
    exc: Exception | str


class HydrusApiException(Exception):
    """Wrapper around hydrus_api.HydrusAPIException to avoid some coupling with hydrus_api outside of FileHasher."""


class FailedPerceptualHashException(Exception):
    """Exception for when files are failed to be perceptually hashed."""

    def __init__(self, file_hash: FileHash, other_exc: str = ""):
        super().__init__()
        self.file_hash = file_hash
        self.other_exc = other_exc


class FileHasher:
    """
    A class to fetch a file from Hydrus and phash it.

    Note: This class was created because HydrusVideoDeduplicator is not pickleable by
    joblib due to the sqlite db member variable.
    """

    def __init__(self, client: HVDClient):
        self.client = client

    def _fetch_file(self, file_hash: str):
        try:
            response = self.client.client.get_file(hash_=file_hash)
        except hydrus_api.HydrusAPIException as exc:
            raise HydrusApiException(exc)
        return response.content

    def _phash_file(self, file: bytes) -> str:
        try:
            phash = compute_phash(file)
            phash_str: str = encode_phash_to_str(phash)
        except Exception as exc:
            raise FailedPerceptualHashException("", str(exc))

        # sanity check
        if phash_str is None or phash_str == "[]" or phash_str == "":
            raise FailedPerceptualHashException("", "phash_str was None or empty or [].")

        return phash_str

    def fetch_and_phash_file(self, file_hash: str) -> PerceptuallyHashedFile | FailedPerceptuallyHashedFile:
        """
        Retrieves the file from Hydrus and calculates its perceptual hash and returns the result.

        Returns FailedPerceptuallyHashedFile with the failed video hash if there are any errors.
        """
        try:
            file = self._fetch_file(file_hash)
        except HydrusApiException as exc:
            # Add a delay before turning so that if there is some transient issue
            # the next file to be hashed won't also immediately error.
            # This is a hack. There should probably be some counter in the result generator
            # for the number of failures before hashing is stopped entirely.
            time.sleep(3)
            return FailedPerceptuallyHashedFile(file_hash, exc)

        try:
            phash = self._phash_file(file)
        except FailedPerceptualHashException as exc:
            # Note: Joblib can't serialize unless this exc is a str. It may be because
            # it may contain exceptions from other modules like pyav or something.
            return FailedPerceptuallyHashedFile(file_hash, str(exc))

        return PerceptuallyHashedFile(file_hash, phash)


@dataclass
class PerceptualHashingStats:
    success_hash_count: int = 0
    failed_from_api_errors_count: int = 0
    failed_from_phash_count: int = 0


class CancelledPerceptualHashException(Exception):
    """Exception for when perceptual hashing is cancelled."""

    def __init__(self, stats: PerceptualHashingStats):
        super().__init__()
        self.stats = stats


class HydrusVideoDeduplicator:
    hydlog = logging.getLogger("hvd")
    hydlog.setLevel(logging.INFO)
    threshold: float = 75.0
    _DEBUG = False

    def __init__(
        self,
        db: DedupeDB.DedupeDb,
        client: HVDClient,
        job_count: int = -2,
        failed_page_name: str | None = None,
        custom_query: Sequence[str] | None = None,
    ):
        self.db = db
        self.client = client
        self.job_count = job_count
        self.page_logger = None if failed_page_name is None else HydrusPageLogger(self.client, failed_page_name)
        self.search_tags = self.get_search_tags(custom_query)

    def get_search_tags(self, custom_query: Sequence[str] | None) -> list[str]:
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
        return search_tags

    def deduplicate(
        self,
        skip_hashing: bool,
    ) -> int:
        """
        Run all deduplicate functions.

        Dedupe Algorithm:
        1. Perceptually hash the videos.
        2. Insert the perceptual hashes into the vptree
        3. Search for similar videos in the vptree.
        4. Mark the similar videos as potential duplicates in Hydrus.

        Returns the number of similar pairs found during searching.
        """
        num_similar_pairs = 0

        if skip_hashing:
            print("[yellow] Skipping perceptual hashing")
        else:
            begin_time = datetime.datetime.now()
            print(
                f"{begin_time.isoformat(timespec='seconds')} | Requesting hashes from Hydrus (may take a bit, depending on db size and search filters)...",  # noqa: E501
            )
            end_time = datetime.datetime.now()
            print(
                f"{begin_time.isoformat(timespec='seconds')} | Done. Took {(end_time - begin_time).total_seconds()}s."
            )

            video_hashes = list(self.client.get_video_hashes(self.search_tags))
            video_hashes = self.filter_unhashed(video_hashes)
            print(f"[blue] Found {len(video_hashes)} eligible files to perceptually hash.")
            self.hydlog.info("Starting perceptual hash processing")
            self.db.begin_transaction()
            with self.db.conn:
                try:
                    stats = self.add_perceptual_hashes_to_db(video_hashes)
                except CancelledPerceptualHashException as exc:
                    # Interrupted, but on purpose.
                    stats = exc.stats
                    print("[yellow] Perceptual hash processing was interrupted! Progress was saved.")
                else:
                    print("[green] Finished perceptual hash processing.")
                finally:
                    # Print some useful stats and info for users
                    total_failures = stats.failed_from_api_errors_count + stats.failed_from_phash_count
                    if total_failures > 0:
                        print(f"[yellow] Perceptual hash processing had {total_failures} total failed files.")

                        if stats.failed_from_api_errors_count > 0:
                            print(
                                f"[yellow] {stats.failed_from_api_errors_count} failures were due to API errors. Ensure Hydrus is running and accessible before trying again."  # noqa: E501
                            )

                        if stats.failed_from_phash_count > 0:
                            print(
                                f"[yellow] {stats.failed_from_phash_count} failures were from an error during perceptual hashing. Are the files corrupted?"  # noqa: E501
                            )
                            print(
                                "\nTip: You could have seen which files failed directly in Hydrus by "
                                "creating a Hydrus page with the name 'failed' and "
                                "running the program with '--failed-page-name=failed'\n"
                            )
                    print(f"[green] Added {stats.success_hash_count} new perceptual hashes to the database.")

        # Insert the perceptual hashed files into the vptree.
        self.db.begin_transaction()
        with self.db.conn:
            try:
                self.process_phashed_file_queue()
            except KeyboardInterrupt:
                print("[yellow] Building the search tree was interrupted! Progress was saved.")
            else:
                print("[green] Finished fully building the search tree.")

        self.db.begin_transaction()
        with self.db.conn:
            try:
                self.run_maintenance()
            except KeyboardInterrupt:
                print("[yellow] Maintenance was interrupted!")
            else:
                print("[green] Finished maintenance.")

        # Number of potential duplicates before adding more.
        # This is just to print info for the user.
        # Note: This will be inaccurate if the user searches for duplicates in the Hydrus client
        #       while this is running.
        pre_dedupe_count = self.client.get_potential_duplicate_count_hydrus()

        self.db.begin_transaction()
        with self.db.conn:
            try:
                num_similar_pairs = self.find_potential_duplicates()
            except KeyboardInterrupt:
                print("[yellow] Searching for duplicates was interrupted! Progress was saved.")

        # Statistics for user
        post_dedupe_count = self.client.get_potential_duplicate_count_hydrus()
        new_dedupes_count = post_dedupe_count - pre_dedupe_count
        if new_dedupes_count > 0:
            print(f"[green] {new_dedupes_count} new potential duplicate pairs marked for manual processing!")
        else:
            print("[green] No new potential duplicate pairs found.")

        self.hydlog.info(f"{num_similar_pairs} similar file pairs found.")
        self.hydlog.info("Deduplication done.")

        return num_similar_pairs

    def filter_unhashed(self, file_hashes: list[FileHash]) -> list[FileHash]:
        """
        Get only the files that have not been perceptually hashed in the db from a list of files.
        """
        all_phashed_files = self.db.get_phashed_files()
        return [file_hash for file_hash in file_hashes if file_hash not in all_phashed_files]

    def add_perceptual_hashes_to_db(self, video_hashes: Sequence[str]) -> PerceptualHashingStats:
        """
        Retrieves the video from Hydrus,
        calculates the perceptual hash,
        and then add it to the database.
        """
        stats = PerceptualHashingStats()
        try:
            with tqdm(
                total=len(video_hashes),
                desc="Perceptually hashing files",
                dynamic_ncols=True,
                unit="file",
                colour="BLUE",
            ) as pbar:
                filehasher = FileHasher(self.client)
                with Parallel(n_jobs=self.job_count, return_as="generator_unordered") as parallel:
                    # Note: joblib actually copies the entire filehasher into a new process, including the client.
                    result_generator = parallel(
                        delayed(filehasher.fetch_and_phash_file)(video_hash) for video_hash in video_hashes
                    )
                    for result in result_generator:
                        if isinstance(result, FailedPerceptuallyHashedFile):
                            # We only want to add the failure to the page if the file was the actual cause of failure.
                            if isinstance(result.exc, HydrusApiException):
                                stats.failed_from_api_errors_count += 1
                                print("[red] Hydrus API error during perceptual hashing:")
                                print(f"{result.exc}")
                            else:
                                stats.failed_from_phash_count += 1
                                print("[red] Failed to perceptually hash a file.")
                                print(f"Failed file SHA256 hash: {result.file_hash}")
                                print(f"{result.exc}")
                                if self.page_logger:
                                    self.page_logger.add_failed_video(result.file_hash)
                        else:
                            stats.success_hash_count += 1
                            self.db.add_to_phashed_files_queue(result.file_hash, result.perceptual_hash)

                        pbar.update(1)
        except KeyboardInterrupt:
            raise CancelledPerceptualHashException(stats)
        return stats

    def mark_videos_as_duplicates(self, video1_hash: str, video2_hash: str):
        """Mark a pair of videos as duplicates in Hydrus."""
        new_relationship = {
            "hash_a": video1_hash,
            "hash_b": video2_hash,
            "relationship": int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
            "do_default_content_merge": True,
        }

        self.client.client.set_file_relationships([new_relationship])

    def process_phashed_file_queue(self):
        """
        Process the files in the phashed files queue.
        This inserts the queue entries into their respective tables and then inserts the file into the vptree.
        """
        results = self.db.execute("SELECT file_hash, phash FROM phashed_file_queue").fetchall()
        for file_hash, perceptual_hash in tqdm(
            results, dynamic_ncols=True, total=len(results), desc="Building search tree", unit="file", colour="BLUE"
        ):
            self.db.add_file(file_hash)
            self.db.add_perceptual_hash(perceptual_hash)
            self.db.associate_file_with_perceptual_hash(file_hash, perceptual_hash)
            self.db.execute(
                "DELETE FROM phashed_file_queue WHERE file_hash = :file_hash AND phash = :phash",
                {"file_hash": file_hash, "phash": perceptual_hash},
            )

    def run_maintenance(self):
        """Run maintenance, if needed."""
        tree = vptree.VpTreeManager(self.db)
        search_threshold = vptree.fix_vpdq_similarity((self.threshold))
        assert search_threshold > 0 and isinstance(search_threshold, int)

        if tree.maintenance_due(search_threshold):
            # TODO: Do further testing on this.
            print("[blue] Running search tree maintenance...")
            tree.maintain_tree()

    def find_potential_duplicates(
        self,
    ) -> int:
        """
        Find potential duplicates in the database and mark them as such in Hydrus.

        Returns the number of similar file pairs found.
        """
        # TODO: Should we turn the inside of this function into a generator? It might make testing super easy.
        tree = vptree.VpTreeManager(self.db)
        search_threshold = vptree.fix_vpdq_similarity((self.threshold))
        assert search_threshold > 0 and isinstance(search_threshold, int)

        files = self.db.execute(
            "SELECT hash_id FROM shape_search_cache WHERE searched_distance is NULL or searched_distance < :threshold",
            {"threshold": search_threshold},
        ).fetchall()

        num_similar_pairs = 0
        with tqdm(
            dynamic_ncols=True, total=len(files), desc="Finding potential duplicates", unit="file", colour="BLUE"
        ) as pbar:
            for hash_id in files:
                hash_id = hash_id[0]
                result = tree.search_file(hash_id, max_hamming_distance=search_threshold)
                file_hash_a = self.db.get_file_hash(hash_id)
                for similar_hash_id, distance in result:
                    file_hash_b = self.db.get_file_hash(similar_hash_id)
                    if hash_id != similar_hash_id:
                        # self.hydlog.info(f'Similar files found. "{file_hash_a}" and "{file_hash_b}"')
                        self.mark_videos_as_duplicates(file_hash_a, file_hash_b)
                        num_similar_pairs += 1

                # TODO:
                # Do we need to add the below line here? See _PerceptualHashesSearchForPotentialDuplicates in Hydrus.
                # group_of_hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM shape_search_cache WHERE searched_distance IS NULL or searched_distance < ?;', ( search_distance, ) ).fetchmany( 10 ) )   # noqa: E501
                # Update the search cache
                self.db.execute(
                    "UPDATE shape_search_cache SET searched_distance = ? WHERE hash_id = ?;",
                    (search_threshold, hash_id),
                )

                pbar.update(1)
        return num_similar_pairs // 2
