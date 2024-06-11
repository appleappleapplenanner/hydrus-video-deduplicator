from __future__ import annotations

import logging
import os
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING

from tqdm import tqdm
from rich import print
from sqlitedict import SqliteDict
import sqlite3

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


def update_search_cache(new_total: int | None = None) -> None:
    """
    Update the search cache to clamp the farthest_search_index to the current length of the database.
    """
    assert new_total is None or new_total >= 0

    if not is_db_accessible():
        return

    BATCH_SIZE = 256
    with SqliteDict(str(DEDUP_DATABASE_FILE), tablename="videos", flag="c", outer_stack=False) as hashdb:
        if new_total is None:
            new_total = len(hashdb)
        for batched_items in batched_and_save_db(hashdb, BATCH_SIZE):
            for video_hash, _ in batched_items.items():
                row = hashdb[video_hash]
                if 'farthest_search_index' in row and row['farthest_search_index'] > new_total:
                    row['farthest_search_index'] = new_total
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


def add_trashed_files_to_db(client: HVDClient) -> None:
    """
    Get trashed files and mark them as trashed in the database.
    """
    cur = create_cursor()
    file_count = get_files_count()
    print(f"[blue] Database found with {file_count} videos already hashed.")
    delete_count = 0
    undelete_count = 0
    with tqdm(
        dynamic_ncols=True,
        total=file_count,
        desc="Searching for trashed videos",
        unit="video",
        colour="BLUE",
    ) as pbar:
        # Check if known files are deleted in Hydrus. If they are, mark them as deleted in the db.
        cur.execute("SELECT hash FROM files")
        while (row := cur.fetchone()) is not None:
            video_hash = row[0]
            # TODO: Batch requests for efficiency.
            is_trashed_res = are_files_deleted_hydrus(client, [video_hash])
            # This statement is a bit contrived but we want to make sure the result is actually a bool
            is_trashed_in_hydrus = True if is_trashed_res[video_hash] is True else False

            trash_cur = create_cursor()
            hash_id = get_hash_id_from_hash(video_hash)
            # If the file is trashed in hydrus, add it to the trashed table.
            # If its not, delete it from the trashed table.
            res = trash_cur.execute(
                "SELECT hash_id FROM deleted_files WHERE hash_id = :hash_id;", {"hash_id": hash_id}
            ).fetchone()
            is_already_trashed = res is not None and len(res) == 1
            if is_trashed_in_hydrus:
                if not is_already_trashed:
                    trash_cur.execute(
                        """
                        INSERT INTO deleted_files (hash_id) VALUES (:hash_id)
                        """,
                        {"hash_id": hash_id},
                    )
                    delete_count += 1
            else:
                if is_already_trashed:
                    trash_cur.execute(
                        """
                        DELETE FROM deleted_files WHERE hash_id=:hash_id
                        """,
                        {"hash_id": hash_id},
                    )
                    undelete_count += 1

            pbar.update(1)

        get_connection().commit()

    if delete_count > 0:
        print(f"Marked {delete_count} deleted videos as trashed.")
    if undelete_count > 0:
        print(f"Marked {undelete_count} undeleted videos as not trashed.")


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


_db_connection: sqlite3.Connection


def connect_to_db() -> None:
    DB_PATH = Path("testing.db")
    if Path.exists(Path(DB_PATH)):
        print("DB already exists. Wiping.")
        # Path.unlink(DB_PATH)
    con = sqlite3.connect(DB_PATH)
    global _db_connection
    _db_connection = con


def get_connection():
    return _db_connection


def create_cursor():
    return get_connection().cursor()


def create_tables() -> None:
    cur = create_cursor()
    # The new files table is analogous to Hydrus client.master.md hashes table.
    # hash_id is the video id.
    # This table is the comparable to the old videos table where hash is the key column
    cur.execute("CREATE TABLE IF NOT EXISTS files(hash_id INTEGER PRIMARY KEY, hash BLOB_BYTES UNIQUE)")
    cur.execute("CREATE TABLE IF NOT EXISTS perceptual_hashes(phash_id INTEGER PRIMARY KEY, phash BLOB_BYTES UNIQUE)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS
        perceptual_hash_map(phash_id INTEGER, hash_id INTEGER, PRIMARY KEY (phash_id, hash_id))
        """
    )
    cur.execute("CREATE TABLE IF NOT EXISTS deleted_files(hash_id INTEGER PRIMARY KEY)")
    cur.execute("CREATE TABLE IF NOT EXISTS version(version TEXT)")


def set_version(version: str) -> None:
    """Set the version in the database."""
    cur = create_cursor()
    cur.execute("DELETE FROM version")
    cur.execute("INSERT INTO version (version) VALUES (:version)", {"version": version})


def get_row_count(table: str) -> int:
    """Get the number of rows in a table."""
    cur = create_cursor()
    # Please don't SQL inject this. You can't use named parameters for table.
    if not table.isascii():
        raise ValueError
    cur.execute(f"SELECT count(*) FROM {table}")
    return cur.fetchone()[0]


def get_files_count() -> int:
    """Get the number of files in the DB."""
    cur = create_cursor()
    cur.execute("SELECT count(hash_id) FROM files")
    return cur.fetchone()[0]


def get_hash_from_hash_id(hash_id: int) -> int | None:
    """Get the hash from the hash_id. Return None if it's not found."""
    cur = create_cursor()
    cur.execute("SELECT hash FROM files WHERE hash_id = :hash_id;", {"hash_id": hash_id})
    res = cur.fetchone()
    if res is None or (len(res) == 0):
        return None
    return res[0]


def get_hash_id_from_hash(video_hash: str) -> str | None:
    """Get the hash_id from the video hash. Return None if it's not found."""
    cur = create_cursor()
    cur.execute("SELECT hash_id FROM files WHERE hash = :hash;", {"hash": video_hash})
    res = cur.fetchone()
    if res is None or (len(res) == 0):
        return None
    return res[0]


def get_phash_from_phash_id(phash_id: int) -> str | None:
    """Get the perceptual hash from the phash_id. Return None if it's not found."""
    cur = create_cursor()
    cur.execute("SELECT phash FROM perceptual_hashes WHERE phash_id = :phash_id;", {"phash_id": phash_id})
    res = cur.fetchone()
    if res is None or (len(res) == 0):
        return None
    return res[0]


def get_phash_from_hash_id(hash_id: int) -> str | None:
    """Get the perceptual hash from the hash_id. Return None if it's not found."""
    hash_id = get_phash_id_from_hash_id(hash_id)
    if hash_id is None:
        return None
    return get_phash_from_phash_id(hash_id)


def get_phash_id_from_hash_id(hash_id: int) -> int | None:
    """Get the phash_id from the hash_id. Return None if not found."""
    cur = create_cursor()
    cur.execute("SELECT phash_id FROM perceptual_hash_map WHERE hash_id = :hash_id;", {"hash_id": hash_id})
    res = cur.fetchone()
    if res is None or (len(res) == 0):
        return None
    return res[0]


def get_phash_id_from_phash(perceptual_hash) -> int | None:
    """Get the phash_id from the perceptual_hash. Return None if not found."""
    cur = create_cursor()
    cur.execute("SELECT phash_id FROM perceptual_hashes WHERE phash = :phash;", {"phash": perceptual_hash})
    res = cur.fetchone()
    if res is None or (len(res) == 0):
        return None
    return res[0]


def associate_perceptual_hash(hash_id: int, perceptual_hash) -> None:
    """Associate a hash_id with a perceptual_hash in the perceptual_hash_map."""
    phash_id = get_phash_id_from_phash(perceptual_hash)
    cur = create_cursor()
    # phash_id is updated if there's a collision. A collision will happen if dedupe is set to overwrite.
    # This may leave a dangling perceptual hash, but cleaning them up later is possible since it is
    # possible to check if anything points to it.
    cur.execute(
        """
        INSERT INTO perceptual_hash_map (phash_id, hash_id) VALUES (:phash_id, :hash_id)
        ON CONFLICT (phash_id, hash_id) DO UPDATE SET phash_id = :phash_id;
        """,
        {"phash_id": phash_id, "hash_id": hash_id},
    )
    cur.execute("SELECT phash_id, hash_id FROM perceptual_hash_map;")


def disassociate_perceptual_hash(hash_id: int, phash_id: int) -> None:
    """Disassociate a hash_id and phash_id in the perceptual_hash_map."""
    cur = create_cursor()
    cur.execute(
        "DELETE FROM perceptual_hash_map WHERE hash_id = :hash_id AND phash_id = :phash_id;",
        {"hash_id": hash_id, "phash_id": phash_id},
    )
