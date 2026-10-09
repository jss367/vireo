"""Persistence for workspace-local copies and their catalog mappings.

Services own filesystem work, locks and transaction boundaries. All methods
use explicit workspace ids, share the Database connection, and never commit.
"""

import sqlite3
from collections.abc import Mapping
from typing import Any


class LocalWorkspaceRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def source_roots(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT workspace_id, source_path FROM local_workspace_folders WHERE is_root=1"
        ).fetchall()

    def source_paths(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT source_path FROM local_workspace_folders"
        ).fetchall()

    def delete_mappings(self, workspace_id: int) -> None:
        self.conn.execute("DELETE FROM local_workspace_folders WHERE workspace_id=?", (workspace_id,))

    def delete_state(self, workspace_id: int) -> None:
        self.conn.execute("DELETE FROM local_workspaces WHERE workspace_id=?", (workspace_id,))

    def get_state(self, workspace_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT workspace_id, state, created_at, activated_at FROM local_workspaces WHERE workspace_id=?",
            (workspace_id,),
        ).fetchone()

    def workspace_for_folder(self, folder_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT lwf.workspace_id
               FROM local_workspace_folders lwf
               JOIN local_workspaces lw ON lw.workspace_id = lwf.workspace_id
               WHERE lwf.folder_id = ?
               LIMIT 1""",
            (folder_id,),
        ).fetchone()

    def mappings(self, workspace_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT folder_id, source_path, local_path, original_status, is_root, root_index
               FROM local_workspace_folders WHERE workspace_id=?
               ORDER BY is_root DESC, root_index, folder_id""",
            (workspace_id,),
        ).fetchall()

    def other_source_roots(self, workspace_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT source_path FROM local_workspace_folders WHERE workspace_id != ? AND is_root = 1",
            (workspace_id,),
        ).fetchall()

    def shared_folder_path(self, workspace_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT f.path
               FROM workspace_folders current_wf
               JOIN workspace_visible_folders other_wf
                 ON other_wf.folder_id = current_wf.folder_id
                AND other_wf.workspace_id != current_wf.workspace_id
               JOIN folders f ON f.id = current_wf.folder_id
               WHERE current_wf.workspace_id = ?
               LIMIT 1""",
            (workspace_id,),
        ).fetchone()

    def other_workspace_root_paths(self, workspace_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT f.path
               FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id != ? AND wf.is_root = 1""",
            (workspace_id,),
        ).fetchall()

    def mark_syncing(self, workspace_id: int) -> None:
        self.conn.execute(
            "UPDATE local_workspaces SET state='syncing' WHERE workspace_id=?", (workspace_id,)
        )

    def create_staging(self, workspace_id: int, created_at: float) -> None:
        self.conn.execute(
            "INSERT INTO local_workspaces (workspace_id, state, created_at) VALUES (?, 'staging', ?)",
            (workspace_id, created_at),
        )

    def activate(self, workspace_id: int, activated_at: float) -> None:
        self.conn.execute(
            "UPDATE local_workspaces SET state='active', activated_at=? WHERE workspace_id=?",
            (activated_at, workspace_id),
        )

    def add_mapping(self, workspace_id: int, folder: Mapping[str, Any], root_ids: Mapping[int, int]) -> None:
        self.conn.execute(
            """INSERT INTO local_workspace_folders
               (workspace_id, folder_id, source_path, local_path, original_status, is_root, root_index)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                workspace_id,
                folder["folder_id"],
                folder["source_path"],
                folder["local_path"],
                folder["status"],
                1 if folder["folder_id"] in root_ids else 0,
                root_ids.get(folder["folder_id"]),
            ),
        )
