import json
import os
from pathlib import Path
from platform import uname

from dotenv import load_dotenv
from platformdirs import PlatformDirs


class InvalidEnvironmentVariable(Exception):
    """Raise for when environment variables are invalid."""

    def __init__(self, msg):
        super().__init__(msg)
        print("Exiting due to invalid environment variable.")


# Validates that a env var is a valid JSON array
# Program will exit if invalid
# Returns the parsed json array as a list
def validate_json_array_env_var(env_var: str | None, err_msg: str) -> list | None:
    if env_var is None:
        return None

    try:
        env_var_list = json.loads(env_var)
        if not isinstance(env_var_list, list):
            raise InvalidEnvironmentVariable(f"ERROR: {err_msg}")
    except json.decoder.JSONDecodeError as exc:
        raise InvalidEnvironmentVariable(f"ERROR: {err_msg}") from exc

    return env_var_list


load_dotenv()
HYDRUS_API_KEY = os.getenv("HYDRUS_API_KEY")


def in_wsl() -> bool:
    return 'microsoft-standard' in uname().release


_DEFAULT_IP = "localhost"
_DEFAULT_PORT = "45869"
# If you're in WSL you probably want to connect to your Windows Hydrus Client by default
if in_wsl():
    from socket import gethostname

    _DEFAULT_IP = f"{gethostname()}.local"

HYDRUS_API_URL = os.getenv("HYDRUS_API_URL", f"https://{_DEFAULT_IP}:{_DEFAULT_PORT}")

# ~/.local/share/hydrusvideodeduplicator/ on Linux
_DEDUP_DATABASE_DIR_ENV = PlatformDirs("hydrusvideodeduplicator").user_data_dir
_DEDUP_DATABASE_DIR_ENV = os.getenv("DEDUP_DATABASE_DIR", _DEDUP_DATABASE_DIR_ENV)
DEDUP_DATABASE_DIR = Path(_DEDUP_DATABASE_DIR_ENV)

_DEDUP_DATABASE_NAME_ENV = os.getenv("DEDUP_DATABASE_NAME", "videohashes")
DEDUP_DATABASE_FILE = Path(DEDUP_DATABASE_DIR, f"{_DEDUP_DATABASE_NAME_ENV}.sqlite")

REQUESTS_CA_BUNDLE = os.getenv("REQUESTS_CA_BUNDLE")

_PDQ_DATABASE_NAME = os.getenv("DEDUP_DATABASE_NAME", "pdq")
PDQ_DATABASE_FILE = Path(DEDUP_DATABASE_DIR, f"{_PDQ_DATABASE_NAME}.sqlite")
PDQ_TABLE_NAME = "potential_duplicates_queue"

USE_POTENTIAL_DUPES_QUEUE = True if os.getenv("USE_POTENTIAL_DUPES_QUEUE") else False
PDQ_FLUSH_COUNT = os.getenv("PDQ_FLUSH_COUNT", 16)
ONLY_SEND_QUEUED_DUPES = True if os.getenv("ONLY_SEND_QUEUED_DUPES") else False

# Optional query for selecting files to process
_HYDRUS_QUERY_ENV = os.getenv("HYDRUS_QUERY")
HYDRUS_QUERY = validate_json_array_env_var(_HYDRUS_QUERY_ENV, err_msg="Ensure HYDRUS_QUERY is a JSON formatted array.")

# Optional service key of local file service/s to fetch files from
_HYDRUS_LOCAL_FILE_SERVICE_KEYS_ENV = os.getenv("HYDRUS_LOCAL_FILE_SERVICE_KEYS")
HYDRUS_LOCAL_FILE_SERVICE_KEYS = validate_json_array_env_var(
    _HYDRUS_LOCAL_FILE_SERVICE_KEYS_ENV, err_msg="Ensure HYDRUS_LOCAL_FILE_SERVICE_KEYS is a JSON formatted array"
)
