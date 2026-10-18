"""Catalog SQL behind card cleanup (freeing space on an import source).

``card_cleanup`` deletes a card file only after a fresh catalog read finds a
verified archive copy with the same content hash. This repository holds that
SQL: the photo rows sharing one ``file_hash`` (with their folder path), the
one-pass hash index the scan builds, and the scoped hash-verification verdict
that certifies (or flags) a never-checked archive row only if it is still the
row that was read.

Everything here is catalog-wide (photos and folders are global), so nothing
resolves a workspace. The verdict write never commits: ``card_cleanup``
commits with ``db.commit()`` after each verified hash, exactly where it
always did. The filesystem checks, hashing and every safety gate stay in
``card_cleanup``.

Callers reach it as ``db.card_cleanup`` (a fresh repository per access, see
``Database.card_cleanup``); there are no forwarding wrappers on ``Database``.
"""

import sqlite3
from collections.abc import Mapping
from typing import Any

_ROWS_BY_HASH_SQL = """
    SELECT p.id, p.folder_id, p.filename, p.file_size, p.file_mtime,
           p.hash_status,
           f.path AS folder_path
    FROM photos p LEFT JOIN folders f ON f.id = p.folder_id
    WHERE p.file_hash = ?
"""


class CardCleanupRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def rows_by_hash(self, file_hash: str) -> list[sqlite3.Row]:
        """Every photo row whose ``file_hash`` is ``file_hash``, with its folder path.

        Each row carries ``id``, ``folder_id``, ``filename``, ``file_size``,
        ``file_mtime``, ``hash_status`` and ``folder_path`` (None when the
        folder row is gone: a ``LEFT JOIN``).
        """
        return self.conn.execute(_ROWS_BY_HASH_SQL, (file_hash,)).fetchall()

    def hashed_photo_rows(self) -> list[sqlite3.Row]:
        """Every photo with a stored hash, in one pass, for the scan's hash index.

        Rows carry ``filename``, ``file_size``, ``file_mtime``,
        ``hash_status``, ``file_hash`` and ``folder_path`` (``LEFT JOIN``).
        """
        return self.conn.execute("""
        SELECT p.filename, p.file_size, p.file_mtime, p.hash_status,
               p.file_hash, f.path AS folder_path
        FROM photos p LEFT JOIN folders f ON f.id = p.folder_id
        WHERE p.file_hash IS NOT NULL
    """).fetchall()

    def record_unchecked_hash_verdict(
            self, row: sqlite3.Row | Mapping[str, Any], expected_hash: str, status: str,
            now: str, file_size: int | None = None,
            file_mtime: float | None = None) -> bool:
        """Set ``hash_status`` (and optionally the size/mtime baseline) on ``row``'s photo.

        Only while the photo still has ``row``'s id, folder, filename,
        size and mtime, ``file_hash = expected_hash`` and a NULL
        ``hash_status``; True when exactly one row changed. ``now`` is the
        ``hash_checked_at`` stamp. Does not commit.
        """
        assignments = "hash_status = ?, hash_checked_at = ?"
        values: list[Any] = [status, now]
        if file_size is not None and file_mtime is not None:
            assignments += ", file_size = ?, file_mtime = ?"
            values.extend([file_size, file_mtime])
        values.extend([
            row["id"], expected_hash, row["folder_id"], row["filename"],
            row["file_size"], row["file_mtime"],
        ])
        cursor = self.conn.execute(
            f"UPDATE photos SET {assignments} "
            "WHERE id = ? AND file_hash = ? AND hash_status IS NULL "
            "AND folder_id IS ? AND filename = ? "
            "AND file_size IS ? AND file_mtime IS ?",
            values,
        )
        return cursor.rowcount == 1
