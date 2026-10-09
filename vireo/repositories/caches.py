"""Persistence for the on-disk caches: the preview LRU and offline originals.

It also reads the "do not adopt this preview" markers in
``preview_cache_invalidations``; ``preview_cache`` still creates that table
lazily and writes it.

The caches are catalog-wide (keyed by photo id, not by workspace), so the
repository takes no workspace id. The lock-retry helpers
(``execute_with_retry`` / ``commit_with_retry``) live in ``db``; the façade
passes them in so this module imports no ``db`` code and a monkeypatch of
``db.commit_with_retry`` still reaches the offline-original writes.

Callers reach it as ``db.caches`` (a fresh repository per access, see
``Database.caches``); there are no forwarding wrappers on ``Database``.
"""

import sqlite3
from collections.abc import Callable, Iterable, Sequence


class CachesRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        execute_with_retry: Callable[..., sqlite3.Cursor],
        commit_with_retry: Callable[[sqlite3.Connection], None],
    ) -> None:
        self.conn = conn
        self.execute_with_retry = execute_with_retry
        self.commit_with_retry = commit_with_retry

    # -- preview_cache LRU ---------------------------------------------------

    def preview_insert(self, photo_id: int, size: int, bytes_: int) -> None:
        """Insert or replace a preview_cache entry. last_access_at = now()."""
        import time
        self.conn.execute(
            "INSERT OR REPLACE INTO preview_cache "
            "(photo_id, size, bytes, last_access_at) VALUES (?, ?, ?, ?)",
            (photo_id, size, bytes_, time.time()),
        )
        self.conn.commit()

    def preview_touch(self, photo_id: int, size: int) -> None:
        """Update last_access_at for an existing entry. No-op if missing."""
        import time
        self.conn.execute(
            "UPDATE preview_cache SET last_access_at=? WHERE photo_id=? AND size=?",
            (time.time(), photo_id, size),
        )
        self.conn.commit()

    def preview_delete(self, photo_id: int, size: int) -> None:
        """Delete a preview_cache entry (caller removes the file)."""
        self.conn.execute(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            (photo_id, size),
        )
        self.conn.commit()

    def preview_total_bytes(self) -> int:
        """Return total bytes tracked across ordinary and paired previews."""
        row = self.conn.execute(
            "SELECT (SELECT COALESCE(SUM(bytes), 0) FROM preview_cache) + "
            "(SELECT COALESCE(SUM(bytes), 0) FROM paired_preview_cache) AS total"
        ).fetchone()
        return row["total"]

    def preview_entry_count(self) -> int:
        """Number of tracked ordinary plus paired preview entries."""
        return self.conn.execute(
            "SELECT (SELECT COUNT(*) FROM preview_cache) + "
            "(SELECT COUNT(*) FROM paired_preview_cache) AS c"
        ).fetchone()["c"]

    def preview_average_bytes(self) -> float | None:
        """Mean size of the non-empty ordinary and paired entries, or None if there are none."""
        return self.conn.execute(
            "SELECT AVG(bytes) AS a FROM ("
            "SELECT bytes FROM preview_cache UNION ALL "
            "SELECT bytes FROM paired_preview_cache) WHERE bytes > 0"
        ).fetchone()["a"]

    def preview_delete_all_except(self, keep_keys: Sequence[tuple[int, int]]) -> None:
        """Delete every ordinary entry except the ``(photo_id, size)`` pairs in ``keep_keys``.

        Paired entries are untouched. Does not commit. The kept keys are
        staged in a temp table, 400 pairs (800 bind parameters) per insert,
        so the delete is not a giant ``NOT IN`` list past SQLite's variable
        limit; the table is dropped even if a statement fails. With no kept
        keys every ordinary entry is deleted.
        """
        if keep_keys:
            self.conn.execute(
                "CREATE TEMP TABLE _pc_failed (photo_id INTEGER, size INTEGER)"
            )
            try:
                CHUNK = 400
                for i in range(0, len(keep_keys), CHUNK):
                    batch = keep_keys[i:i + CHUNK]
                    placeholders = ",".join(["(?,?)"] * len(batch))
                    flat = [v for pair in batch for v in pair]
                    self.conn.execute(
                        f"INSERT INTO _pc_failed (photo_id, size) VALUES {placeholders}",
                        flat,
                    )
                self.conn.execute(
                    "DELETE FROM preview_cache WHERE (photo_id, size) NOT IN "
                    "(SELECT photo_id, size FROM _pc_failed)"
                )
            finally:
                self.conn.execute("DROP TABLE _pc_failed")
        else:
            self.conn.execute("DELETE FROM preview_cache")

    def preview_oldest_first(self) -> list[sqlite3.Row]:
        """Return all rows ordered by last_access_at ascending (oldest first)."""
        return self.conn.execute(
            "SELECT photo_id, size, bytes, last_access_at FROM preview_cache "
            "ORDER BY last_access_at ASC"
        ).fetchall()

    def preview_get(self, photo_id: int, size: int) -> sqlite3.Row | None:
        """Return the row for (photo_id, size), or None."""
        return self.conn.execute(
            "SELECT photo_id, size, bytes, last_access_at FROM preview_cache "
            "WHERE photo_id=? AND size=?",
            (photo_id, size),
        ).fetchone()

    def preview_invalidated(self, photo_id: int, size: int) -> bool:
        """True when ``(photo_id, size)`` carries a "do not adopt" marker.

        Reads ``preview_cache_invalidations``, which the caller creates first
        (``preview_cache.ensure_preview_cache_invalidations_table``).
        """
        row = self.conn.execute(
            "SELECT 1 FROM preview_cache_invalidations "
            "WHERE photo_id=? AND size=?",
            (photo_id, size),
        ).fetchone()
        return row is not None

    def paired_preview_insert(self, photo_id: int, filename: str, bytes_: int) -> None:
        """Join the publisher's transaction; the filename includes source state."""
        import time
        self.conn.execute(
            "INSERT OR REPLACE INTO paired_preview_cache "
            "(filename, photo_id, bytes, last_access_at) VALUES (?, ?, ?, ?)",
            (filename, photo_id, bytes_, time.time()),
        )

    def paired_preview_get(self, filename: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM paired_preview_cache WHERE filename=?", (filename,),
        ).fetchone()

    def paired_preview_touch(self, filename: str) -> None:
        import time
        self.conn.execute(
            "UPDATE paired_preview_cache SET last_access_at=? WHERE filename=?",
            (time.time(), filename),
        )
        self.conn.commit()

    def paired_preview_oldest_first(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM paired_preview_cache ORDER BY last_access_at",
        ).fetchall()

    def paired_preview_delete(self, filename: str) -> None:
        """Delete one paired_preview_cache entry (caller removes the file)."""
        self.conn.execute(
            "DELETE FROM paired_preview_cache WHERE filename=?", (filename,),
        )
        self.conn.commit()

    def preview_delete_entries(
        self, preview_keys: Iterable[tuple[int, int]], paired_filenames: Iterable[str],
    ) -> None:
        """Delete ordinary entries by (photo_id, size) and paired ones by filename, then commit."""
        self.conn.executemany(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            list(preview_keys),
        )
        self.conn.executemany(
            "DELETE FROM paired_preview_cache WHERE filename=?",
            [(filename,) for filename in paired_filenames],
        )
        self.conn.commit()

    def preview_clear_all(self) -> None:
        """Delete every ordinary and paired preview entry (caller removes the files)."""
        self.conn.execute("DELETE FROM preview_cache")
        self.conn.execute("DELETE FROM paired_preview_cache")
        self.conn.commit()

    # -- offline original cache ----------------------------------------------

    def offline_original_upsert(
        self,
        photo_id: int,
        original_path: str | None,
        xmp_path: str | None,
        companion_path: str | None,
        bytes_: int,
        source_size: int | None,
        source_mtime: float | None,
        cached_at: float,
        status: str,
        error: str | None = None,
    ) -> None:
        self.execute_with_retry(
            self.conn,
            """INSERT OR REPLACE INTO offline_originals
               (photo_id, original_path, xmp_path, companion_path, bytes,
                source_size, source_mtime, cached_at, status, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                photo_id,
                original_path,
                xmp_path,
                companion_path,
                bytes_,
                source_size,
                source_mtime,
                cached_at,
                status,
                error,
            ),
        )
        self.commit_with_retry(self.conn)

    def offline_original_get(self, photo_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT photo_id, original_path, xmp_path, companion_path, bytes,
                      source_size, source_mtime, cached_at, status, error
               FROM offline_originals WHERE photo_id=?""",
            (photo_id,),
        ).fetchone()

    def offline_original_delete(self, photo_id: int, _commit: bool = True) -> None:
        self.execute_with_retry(
            self.conn,
            "DELETE FROM offline_originals WHERE photo_id=?",
            (photo_id,),
        )
        if _commit:
            self.commit_with_retry(self.conn)

    def offline_original_total_bytes(self) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(bytes), 0) AS total FROM offline_originals "
            "WHERE status='cached'"
        ).fetchone()
        return row["total"]

    def offline_original_cached_count(self) -> int:
        """Number of offline originals with ``status='cached'``."""
        return self.conn.execute(
            "SELECT COUNT(*) AS c FROM offline_originals WHERE status='cached'"
        ).fetchone()["c"]

    def clear_preview_invalidation(self, photo_id: int, size: int | str) -> None:
        self.conn.execute(
            "DELETE FROM preview_cache_invalidations WHERE photo_id=? AND size=?",
            (photo_id, size),
        )

    def clear_thumbnail_path(self, pid: int) -> None:
        self.conn.execute(
            "UPDATE photos SET thumb_path = NULL WHERE id = ?", (pid,),
        )

    def delete_preview_rows(self, removed_preview_rows: Sequence[tuple[int, int]]) -> None:
        self.conn.executemany(
            "DELETE FROM preview_cache WHERE photo_id = ? AND size = ?",
            removed_preview_rows,
        )

    def preview_sizes(self, pid: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT size FROM preview_cache WHERE photo_id = ?",
            (pid,),
        ).fetchall()
