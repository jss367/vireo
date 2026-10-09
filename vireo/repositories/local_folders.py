"""Persistence for folder-level local copies (``local_folders``).

A ``local_folders`` row is one folder subtree staged to local disk ("Work
Locally"), keyed by its root folder and catalog-wide: every workspace that
sees the folder shares the copy. This repository owns staging, sync and discard
persistence, including the catalog path changes shared by folder- and workspace-local copies. Services
retain filesystem operations, locks and transaction boundaries; these methods
never commit. Workspace ids are explicit where a read needs visibility.

Callers reach it as ``db.local_folders`` (a fresh repository per access, see
``Database.local_folders``); there are no forwarding wrappers on
``Database``.
"""

import sqlite3
from collections.abc import Iterable, Mapping
from typing import Any


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

    def delete_mappings(self, root_folder_id: int) -> None:
        self.conn.execute("DELETE FROM local_folder_mappings WHERE root_folder_id=?", (root_folder_id,))

    def delete_state(self, root_folder_id: int) -> None:
        self.conn.execute("DELETE FROM local_folders WHERE root_folder_id=?", (root_folder_id,))

    def get_state(self, root_folder_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT root_folder_id, state, created_at, activated_at
               FROM local_folders WHERE root_folder_id=?""",
            (root_folder_id,),
        ).fetchone()

    def mappings(self, root_folder_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT root_folder_id, folder_id, source_path, local_path,
                      original_status, is_root
               FROM local_folder_mappings
               WHERE root_folder_id=?
               ORDER BY is_root DESC, source_path""",
            (root_folder_id,),
        ).fetchall()

    def root_mapping(self, root_folder_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT root_folder_id, folder_id, source_path, local_path,
                      original_status, is_root
               FROM local_folder_mappings
               WHERE root_folder_id=? AND is_root=1""",
            (root_folder_id,),
        ).fetchone()

    def root_for_folder(self, folder_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT root_folder_id FROM local_folder_mappings WHERE folder_id=?",
            (folder_id,),
        ).fetchone()

    def source_roots(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT root_folder_id, source_path FROM local_folder_mappings WHERE is_root=1"
        ).fetchall()

    def ordered_source_roots(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT root_folder_id, source_path FROM local_folder_mappings "
            "WHERE is_root=1 ORDER BY root_folder_id"
        ).fetchall()

    def workspace_has_root(self, workspace_id: int, root_folder_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT 1
               FROM workspace_folders wf
               JOIN local_folder_mappings lfm ON lfm.folder_id = wf.folder_id
               WHERE wf.workspace_id=? AND lfm.root_folder_id=?
               LIMIT 1""",
            (int(workspace_id), int(root_folder_id)),
        ).fetchone()

    def workspace_root_ids(self, workspace_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT DISTINCT lfm.root_folder_id
               FROM workspace_folders wf
               JOIN local_folder_mappings lfm ON lfm.folder_id = wf.folder_id
               WHERE wf.workspace_id=?
               ORDER BY lfm.root_folder_id""",
            (workspace_id,),
        ).fetchall()

    def visible_photo_count(self, workspace_id: int, root_folder_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT COUNT(*) AS photo_count
               FROM photos p
               JOIN photo_workspace_visibility wf
                 ON wf.photo_id = p.id AND wf.workspace_id = ?
               JOIN local_folder_mappings lfm ON lfm.folder_id = p.folder_id
               WHERE lfm.root_folder_id = ?""",
            (int(workspace_id), int(root_folder_id)),
        ).fetchone()

    def affected_workspace_ids(self, root_folder_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT DISTINCT wf.workspace_id
               FROM local_folder_mappings lfm
               JOIN workspace_visible_folders wf ON wf.folder_id = lfm.folder_id
               WHERE lfm.root_folder_id=?
               ORDER BY wf.workspace_id""",
            (root_folder_id,),
        ).fetchall()

    def local_roots(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT root_folder_id, local_path FROM local_folder_mappings WHERE is_root=1"
        ).fetchall()

    def catalog_rows_by_path(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT id, path, status FROM folders ORDER BY path").fetchall()

    def set_catalog_path(self, path: str, folder_id: int) -> None:
        self.conn.execute(
            "UPDATE folders SET path=? WHERE id=?",
            (path, folder_id),
        )

    def restore_catalog_folder(self, mapping: Mapping[str, Any]) -> None:
        self.conn.execute(
            "UPDATE folders SET path=?, status=? WHERE id=?",
            (mapping["source_path"], mapping["original_status"], mapping["folder_id"]),
        )

    def mark_syncing(self, root_folder_id: int) -> None:
        self.conn.execute(
            "UPDATE local_folders SET state='syncing' WHERE root_folder_id=?", (root_folder_id,)
        )

    def create_staging(self, root_folder_id: int, created_at: float) -> None:
        self.conn.execute(
            "INSERT INTO local_folders (root_folder_id, state, created_at) VALUES (?, 'staging', ?)",
            (root_folder_id, created_at),
        )

    def activate(self, root_folder_id: int, activated_at: float) -> None:
        self.conn.execute(
            "UPDATE local_folders SET state='active', activated_at=? WHERE root_folder_id=?",
            (activated_at, root_folder_id),
        )

    def catalog_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT id, path, status FROM folders").fetchall()

    def catalog_paths(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT path FROM folders").fetchall()

    def add_mapping(self, root_folder_id: int, folder: Mapping[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO local_folder_mappings
               (root_folder_id, folder_id, source_path, local_path,
                original_status, is_root)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                root_folder_id,
                folder["folder_id"],
                folder["source_path"],
                folder["local_path"],
                folder["status"],
                1 if folder["is_root"] else 0,
            ),
        )

    def rebase_folder_if_unchanged(self, folder: Mapping[str, Any]) -> None:
        self.conn.execute(
            "UPDATE folders SET path=? WHERE id=? AND path=?",
            (folder["local_path"], folder["folder_id"], folder["source_path"]),
        )

    def path_conflict(self, path: str, folder_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT id FROM folders WHERE path=? AND id != ?",
            (path, folder_id),
        ).fetchone()

    def source_paths(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT source_path FROM local_folder_mappings"
        ).fetchall()

    def last_change_count(self) -> int:
        """Rows changed by the last write on the shared connection."""
        return self.conn.execute("SELECT changes()").fetchone()[0]
