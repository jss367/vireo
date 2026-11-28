"""Reuse derived catalog reads across requests until another connection commits."""

from __future__ import annotations

import sqlite3
import threading
from collections import OrderedDict
from collections.abc import Hashable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from services.service_types import JsonDict


class CatalogReadCache:
    """Bounded per-app cache invalidated by any committed catalog write."""

    def __init__(self, capacity: int = 8) -> None:
        self._capacity = capacity
        self._lock = threading.Lock()
        self._observer: sqlite3.Connection | None = None
        self._file: tuple[int, int] | None = None
        self._entries: OrderedDict[Hashable, JsonDict] = OrderedDict()
        self._version = -1
        self._closed = False

    def close(self) -> None:
        """Release the catalog and prevent in-flight reads from reopening it."""
        with self._lock:
            self._closed = True
            self._entries.clear()
            if self._observer is not None:
                self._observer.close()
                self._observer = None

    def _revision(self, path: Path) -> tuple[int, int, int]:
        # data_version is comparable only on the same connection. This
        # read-only observer never holds a transaction across requests.
        stat = path.stat()
        identity = (stat.st_dev, stat.st_ino)
        if self._observer is None or identity != self._file:
            observer = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, check_same_thread=False)
            if self._observer is not None:
                self._observer.close()
            self._observer = observer
            self._file = identity
            self._entries.clear()
        version = self._observer.execute("PRAGMA data_version").fetchone()[0]
        if self._version != version:
            self._entries.clear()
            self._version = version
        return (*identity, version)

    def _available_revision(self, path: Path) -> tuple[int, int, int] | None:
        if self._closed:
            return None
        try:
            return self._revision(path)
        except (OSError, sqlite3.Error):
            # The ordinary read still owns error handling if the extra
            # read-only connection cannot be opened during catalog recovery.
            return None

    def read(self, path: Path, key: Hashable, compute: Callable[[], JsonDict]) -> JsonDict:
        with self._lock:
            revision = self._available_revision(path)
            if revision is not None and key in self._entries:
                self._entries.move_to_end(key)
                return self._entries[key]
        # Other scopes and superseding requests can proceed while this
        # request computes its aggregates on its own SQLite snapshot.
        result = compute()
        with self._lock:
            if revision is not None and revision == self._available_revision(path):
                self._entries[key] = result
                if len(self._entries) > self._capacity:
                    self._entries.popitem(last=False)
        return result
