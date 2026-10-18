"""Catalog SQL for cleaning up the source folder of a completed move.

``move_cleanup`` reviews what a folder move left behind in its original
folder and, when that folder is empty, retires its catalog rows and removes
it. This repository holds the catalog side: whether a folder still holds
cataloged photos, the Work Locally source and local paths that protect a
folder from cleanup, the recursive roots other workspaces still link, every
folder's stored path, and retiring an emptied folder's rows.

``move_cleanup`` owns the filesystem checks, the order of the checks and the
transaction: it opens the writer lock with ``db.begin_immediate()``, and it
commits or rolls back with ``db.commit()`` / ``db.rollback()`` around the
``rmdir``. Nothing here commits. Workspace membership reads that already
exist (``list_workspaces_for_folder``, ``removed_ids``) stay on
``db.workspace_folders``. Nothing here is scoped to the active workspace;
the caller passes the workspace to exclude explicitly.

Callers reach it as ``db.source_cleanup`` (a fresh repository per access, see
``Database.source_cleanup``); there are no forwarding wrappers on
``Database``.
"""

import sqlite3


class SourceCleanupRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def folder_has_photos(self, folder_id: int) -> bool:
        """True when any photo row (in any workspace) is filed under ``folder_id``."""
        return self.conn.execute(
            "SELECT 1 FROM photos WHERE folder_id = ? LIMIT 1", (folder_id,)
        ).fetchone() is not None

    def local_copy_paths(self) -> sqlite3.Cursor:
        """``source_path`` / ``local_path`` of every Work Locally mapping.

        Workspace-scoped (``local_workspace_folders``) and folder-scoped
        (``local_folder_mappings``) copies alike, as an unconsumed cursor.
        """
        return self.conn.execute(
            "SELECT source_path, local_path FROM local_workspace_folders "
            "UNION SELECT source_path, local_path FROM local_folder_mappings"
        )

    def other_workspace_roots(self, workspace_id: int | None) -> sqlite3.Cursor:
        """``workspace_id`` / ``path`` of every recursive root outside ``workspace_id``.

        ``IS NOT`` keeps every workspace's roots when ``workspace_id`` is
        ``None``. Returned as an unconsumed cursor.
        """
        return self.conn.execute(
            "SELECT wf.workspace_id, f.path FROM workspace_folders wf "
            "JOIN folders f ON f.id = wf.folder_id "
            "WHERE wf.is_root = 1 AND wf.workspace_id IS NOT ?", (workspace_id,),
        )

    def folder_paths(self) -> sqlite3.Cursor:
        """``id`` / ``path`` of every folder row, any workspace or status, as a cursor."""
        return self.conn.execute("SELECT id, path FROM folders")

    def retire_folder(self, folder_id: int, path: str) -> None:
        """Delete an emptied source folder's row and links, and forget it as a move source.

        Drops its ``workspace_folders`` links, then the ``folders`` row, then
        clears ``last_move_source_folder_path`` on photos that recorded
        ``path``, in that order. Does not commit.
        """
        self.conn.execute("DELETE FROM workspace_folders WHERE folder_id = ?", (folder_id,))
        self.conn.execute("DELETE FROM folders WHERE id = ?", (folder_id,))
        self.conn.execute("UPDATE photos SET last_move_source_folder_path = NULL "
                          "WHERE last_move_source_folder_path = ?", (path,))
