import json
from collections import deque
from rich import print
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

from sqlitedict import SqliteDict
from .config import PDQ_DATABASE_FILE, PDQ_TABLE_NAME, PDQ_FLUSH_COUNT
from .dedup_util import get_potential_duplicate_count_hydrus
import hydrusvideodeduplicator.hydrus_api as hydrus_api


class Relationship(dict):
    def __init__(
        self,
        hash_a: str,
        hash_b: str,
        relationship_type: int = int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
        do_default_content_merge: bool = True,
    ):
        super().__init__(
            self,
            hash_a=hash_a,
            hash_b=hash_b,
            relationship=relationship_type,
            do_default_content_merge=do_default_content_merge,
        )


class PotentialDuplicatesQueue:
    def __init__(self, client: hydrus_api, file_service_keys: list[str], flush_count: int = int(PDQ_FLUSH_COUNT)):
        self._queue = deque()
        self._client = client
        self._file_service_keys = file_service_keys
        self._flush_count = flush_count
        self.populate_from_db()  # Load any remaining dupes in the db in case there are leftovers from prior runs

    def add(self, relationship: Relationship) -> None:
        self._queue.append(relationship)

        if self._flush_count != 0 and len(self._queue) >= self._flush_count:
            self.flush_to_db()

    def flush_to_db(self, clear_db_contents_first: bool = False) -> None:
        flag = "w" if clear_db_contents_first else "c"
        relationship = None

        try:
            with self._get_pdq_db_conn(flag) as pdq_db:
                while self._queue:
                    relationship = self._queue.pop()
                    pdq_db[self.get_pdq_table_key(relationship)] = relationship
        except Exception as e:
            if relationship is not None:
                # Make sure the last popped relationship isn't lost in case of an error
                self._queue.append(relationship)
            raise e

    def populate_from_db(self) -> None:
        old_flush_count = self._flush_count
        try:
            # temporarily turn off flushing so we aren't writing rows as add them to the queue
            self._flush_count = 0
            with self._get_pdq_db_conn('c') as pdq_db:  # Use 'c' here in case db not yet created
                for relationship in pdq_db.values():
                    self._queue.append(relationship)
        finally:
            # turn flushing back on
            self._flush_count = old_flush_count

    def flush_to_client_api(self) -> None:
        payload = list()
        try:
            self.populate_from_db()  # Fetch everything we've flushed away
            initial_pd_count = get_potential_duplicate_count_hydrus(self._client, self._file_service_keys)

            while self._queue:
                payload.append(self._queue.pop())
                if len(payload) >= self._flush_count:
                    self._client.set_file_relationships(payload)
                    payload.clear()

            self._client.set_file_relationships(payload)  # send the last batch
            final_pd_count = get_potential_duplicate_count_hydrus(self._client, self._file_service_keys)
            new_pd_count = final_pd_count - initial_pd_count
            print(f"[green] Finished sending potential duplicates to Hydrus. {new_pd_count} new potential dupes added.")
        except Exception as e:
            print(
                "[yellow] Error while sending dupe relationships to hydrus! Dupe relationships saved to db - "
                "retry by re-running with --only-send-queued-dupes argument"
            )
            # preserve any popped dupes in the payload by adding them back to the queue
            for relationship in payload:
                self._queue.append(relationship)
            raise e
        finally:
            # overwrite the contents of the db here to remove any relationships successfully sent to the client
            self.flush_to_db(True)

    def get_pdq_size(self) -> int:
        return len(self._queue)

    @staticmethod
    def _get_pdq_db_conn(flag: str = 'c') -> SqliteDict:
        return SqliteDict(
            str(PDQ_DATABASE_FILE),
            tablename=str(PDQ_TABLE_NAME),
            flag=flag,
            autocommit=True,
            outer_stack=False,
            encode=json.dumps,
            decode=json.loads,
        )

    @staticmethod
    def get_pdq_table_key(r: Relationship) -> str:
        """
        This function ensures that the key for any given relationship with the same two videos is always the same ,
        regardless of which is 'hash_a' and which is 'hash_b'
        """
        if r['hash_a'] > r['hash_b']:
            return r['hash_a'] + r['hash_b']
        elif r['hash_a'] < r['hash_b']:
            return r['hash_b'] + r['hash_a']
        else:
            raise Exception(
                "hash_a is the same as hash_b, which means we're comparing a file with itself - something's not "
                "right here."
            )
