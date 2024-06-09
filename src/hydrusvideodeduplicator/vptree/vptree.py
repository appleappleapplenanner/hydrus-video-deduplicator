from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable
    from typing import Any

log = logging.getLogger("vptree")
log.setLevel(logging.INFO)
