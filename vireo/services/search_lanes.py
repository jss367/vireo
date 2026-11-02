"""Latest-wins cancellation for reads the page has already superseded.

Browse re-issues its grid, calendar and summary reads on every search
change, and on a large catalog one metadata search can keep SQLite busy for
seconds. The page already ignores a response once it has sent a newer
request for the same loader, but ignoring it does not stop the work, so
without cancellation quick typing stacks several full searches on the
server.

A request that opts in names a *lane* (one page instance plus one loader)
and its *sequence* (that loader's own stale-response counter). When a newer
request claims the lane, SQLite work still running for the older one is
interrupted through a progress handler. So a request is cancelled exactly
when the page would have discarded its response, and never by another
window or another loader.
"""

from __future__ import annotations

import re
import sqlite3
import threading
from collections import OrderedDict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from db import Database

LANE_HEADER = "X-Vireo-Search-Lane"
SEQ_HEADER = "X-Vireo-Search-Seq"
SUPERSEDED_CODE = "search_superseded"

_LANE_PATTERN = re.compile(r"[A-Za-z0-9._:-]{1,96}")
# Lanes are per page load; old ones are forgotten oldest-first. Forgetting a
# lane only means a late straggler on it runs to completion.
MAX_LANES = 1024
# SQLite VM instructions between supersession checks: well under a
# millisecond of work, against a lock-guarded dict lookup per check.
PROGRESS_INSTRUCTIONS = 20_000


class SearchLanes:
    """The newest sequence number claimed on each lane."""

    def __init__(self, max_lanes: int = MAX_LANES) -> None:
        self._lock = threading.Lock()
        self._latest: OrderedDict[str, int] = OrderedDict()
        self._max_lanes = max_lanes

    def claim(self, lane: str, seq: int) -> Callable[[], bool]:
        """Record a request for ``seq`` on ``lane``.

        Returns a callable reporting whether a newer request has claimed the
        lane since. A request that arrives after a newer one is superseded
        from the start.
        """
        with self._lock:
            latest = self._latest.get(lane)
            if latest is None or seq > latest:
                self._latest[lane] = seq
            self._latest.move_to_end(lane)
            while len(self._latest) > self._max_lanes:
                self._latest.popitem(last=False)

        def superseded() -> bool:
            with self._lock:
                latest = self._latest.get(lane)
            return latest is not None and latest > seq

        return superseded


# Lane names embed a random per-page-load id, so one table serves every app
# in the process, like a module-level lock.
SEARCH_LANES = SearchLanes()


def parse_lane(lane: str | None, seq: str | None) -> tuple[str, int] | None:
    """Validate the two header values; ``None`` when either is absent or bad."""
    if not lane or not seq or not _LANE_PATTERN.fullmatch(lane):
        return None
    if not seq.isdigit() or len(seq) > 15:
        return None
    return lane, int(seq)


def cancel_when_superseded(
    conn: Database | sqlite3.Connection, superseded: Callable[[], bool],
) -> None:
    """Interrupt ``conn``'s running statement once ``superseded()`` is true.

    ``conn`` is a ``Database`` or a ``sqlite3.Connection``: anything with
    ``set_progress_handler``.
    """
    conn.set_progress_handler(
        lambda: 1 if superseded() else 0, PROGRESS_INSTRUCTIONS,
    )


def is_superseded_interrupt(
    exc: BaseException, superseded: Callable[[], bool] | None,
) -> bool:
    """Whether ``exc`` is SQLite abandoning work for a superseded request."""
    return (
        superseded is not None
        and isinstance(exc, sqlite3.OperationalError)
        and "interrupted" in str(exc).lower()
        and superseded()
    )
