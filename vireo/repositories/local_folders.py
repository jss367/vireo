"""Persistence for folder-level local copies (``local_folders``).

A ``local_folders`` row is one folder subtree staged to local disk ("Work
Locally"), keyed by its root folder and catalog-wide: every workspace that
sees the folder shares the copy. ``services/local_folder.py`` still runs the
staging, sync and discard SQL itself; this repository holds the reads the
``/api/workspaces/active/local-folders`` routes run, starting with the
per-root residency signature the blocker status reports.

Callers reach it as ``db.local_folders`` (a fresh repository per access, see
``Database.local_folders``); there are no forwarding wrappers on
``Database``.
"""

import sqlite3
from collections.abc import Iterable


class LocalFolderRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def state_rows(self, root_folder_ids: Iterable[int]) -> list[sqlite3.Row]:
        """Rows (``root_folder_id``, ``state``, ``activated_at``, ``created_at``).

        One statement over every id (no chunking), ordered by root folder id.
        Ids without a local copy are absent; no ids reads nothing.
        """
        root_folder_ids = list(root_folder_ids)
        if not root_folder_ids:
            return []
        placeholders = ",".join("?" for _ in root_folder_ids)
        return self.conn.execute(
            f"""SELECT root_folder_id, state, activated_at, created_at
                FROM local_folders
                WHERE root_folder_id IN ({placeholders})
                ORDER BY root_folder_id""",
            tuple(root_folder_ids),
        ).fetchall()
