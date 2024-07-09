from __future__ import annotations

import logging
import os
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING

from rich import print
from sqlitedict import SqliteDict
from tqdm import tqdm

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable
    from typing import Any, TypeAlias

    FileServiceKeys: TypeAlias = list[str]
    FileHashes: TypeAlias = Iterable[str]

    from hydrusvideodeduplicator.client import HVDClient

from hydrusvideodeduplicator.config import DEDUP_DATABASE_DIR, DEDUP_DATABASE_FILE

dedupedblog = logging.getLogger("hvd")
dedupedblog.setLevel(logging.INFO)


def database_accessible(db_file: Path | str, tablename: str, verbose: bool = False):
    try:
        with SqliteDict(str(db_file), tablename=tablename, flag="r"):
            return True
    except OSError:
        if verbose:
            print("[red] Database does not exist.")
    except RuntimeError:  # SqliteDict error when trying to create a table for a DB in read-only mode
        if verbose:
            print("[red] Database does not exist.")
    except Exception as exc:
        if verbose:
            print(f"[red] Could not access database. Exception: {exc}")
    return False


def is_db_accessible(verbose: bool = False) -> bool:
    """
    Check DB exists and is accessible.

    Return DB exists and is accessible.
    """
    return database_accessible(get_db_file_path(), tablename="videos", verbose=verbose)


def clear_search_cache() -> None:
    """Delete cache search index value for each video in database"""
    if not is_db_accessible():
        return

    with SqliteDict(str(get_db_file_path()), tablename="videos", flag="c") as hashdb:
        for key in hashdb:
            row = hashdb[key]
            if "farthest_search_index" in row:
                del row["farthest_search_index"]
                hashdb[key] = row
                hashdb.commit()
    print("[green] Cleared search cache.")


def update_search_cache(new_total: int | None = None) -> None:
    """
    Update the search cache to clamp the farthest_search_index to the current length of the database.
    """
    assert new_total is None or new_total >= 0

    if not is_db_accessible():
        return

    BATCH_SIZE = 256
    with SqliteDict(str(get_db_file_path()), tablename="videos", flag="c", outer_stack=False) as hashdb:
        if new_total is None:
            new_total = len(hashdb)
        for batched_items in batched_and_save_db(hashdb, BATCH_SIZE):
            for video_hash, _ in batched_items.items():
                row = hashdb[video_hash]
                if "farthest_search_index" in row and row["farthest_search_index"] > new_total:
                    row["farthest_search_index"] = new_total
                    hashdb[video_hash] = row


def batched_and_save_db(
    db: SqliteDict,
    batch_size: int = 1,
    chunk_size: int = 1,
) -> Generator[dict[str, dict[str, Any]], Any, None]:
    """
    Batch rows into rows of length n and save changes after each batch or after chunk_size batches.
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


def are_files_deleted_hydrus(client: HVDClient, file_hashes: FileHashes) -> dict[str, bool]:
    """
    Check if files are trashed or deleted in Hydrus

    Returns a dictionary of {hash, trashed_or_not}
    """
    videos_metadata = client.client.get_file_metadata(hashes=file_hashes, only_return_basic_information=False)[
        "metadata"
    ]

    result: dict[str, bool] = {}
    for video_metadata in videos_metadata:
        # This should never happen, but it shouldn't break the program if it does
        if "hash" not in video_metadata:
            logging.error("Hash not found for potentially trashed file.")
            continue
        video_hash = video_metadata["hash"]
        is_deleted: bool = video_metadata.get("is_deleted", False)
        result[video_hash] = is_deleted

    return result


def clear_trashed_files_from_db(client: HVDClient) -> None:
    """
    Delete trashed and deleted Hydrus files from the database.
    """
    if not is_db_accessible():
        return

    try:
        with SqliteDict(str(get_db_file_path()), tablename="videos", flag="c", outer_stack=False) as hashdb:
            # This is EXPENSIVE. sqlitedict gets len by iterating over the entire database!
            if (total := len(hashdb)) < 1:
                return

            delete_count = 0
            print(f"[blue] Database found with {total} videos already hashed.")
            try:
                with tqdm(
                    dynamic_ncols=True,
                    total=total,
                    desc="Searching for trashed videos",
                    unit="video",
                    colour="BLUE",
                ) as pbar:
                    BATCH_SIZE = 32
                    for batched_items in batched_and_save_db(hashdb, BATCH_SIZE):
                        is_trashed_result = are_files_deleted_hydrus(client, batched_items.keys())
                        for video_hash, is_trashed in is_trashed_result.items():
                            if is_trashed is True:
                                del hashdb[video_hash]
                                delete_count += 1
                        pbar.update(min(BATCH_SIZE, total - pbar.n))
            except Exception as exc:
                print("[red] Failed to clear trashed videos cache.")
                print(exc)
                dedupedblog.error(exc)
            finally:
                if delete_count > 0:
                    print(f"Cleared {delete_count} trashed videos from the database.")
                update_search_cache(total - delete_count)

    except OSError as exc:
        dedupedblog.info(exc)


def populate_video_ids() -> None:
    """Add unique id to videos id table from all videos in the videos table."""
    with SqliteDict(str(get_db_file_path()), tablename="video_id", flag="c", outer_stack=False) as video_id_table:
        with SqliteDict(str(get_db_file_path()), tablename="videos", flag="r", outer_stack=False) as videos_table:
            for i, video_hash in enumerate(videos_table):
                if video_hash not in video_id_table:
                    video_id_table[video_hash] = i

        video_id_table.commit()


def create_db_dir() -> None:
    """
    Create database folder if it does not exist.
    """
    try:
        os.makedirs(DEDUP_DATABASE_DIR, exist_ok=False)
        # Exception before this log if directory already exists
        dedupedblog.info(f"Created DB dir {DEDUP_DATABASE_DIR}")
    except OSError:
        pass


def get_db_file_path() -> Path:
    """
    Get database file path.

    Return the database file path.
    """
    return DEDUP_DATABASE_FILE
