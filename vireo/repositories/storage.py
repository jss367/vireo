"""Catalog-wide inspection for managed storage; no filesystem mutations."""

import sqlite3


class StorageRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def local_copy_paths(self) -> list[str]:
        return [row[0] for row in self.conn.execute(
            "SELECT local_path FROM local_folder_mappings WHERE is_root=1 "
            "UNION SELECT local_path FROM local_workspace_folders WHERE is_root=1"
        )]

    def source_folder_paths(self) -> list[str]:
        return [row[0] for row in self.conn.execute("SELECT path FROM folders")]

    def retained_source(self, photo_id: int) -> dict | None:
        row = self.conn.execute(
            """SELECT p.id, p.filename, p.file_size, p.file_mtime, p.file_hash,
                      p.companion_path, f.path AS folder_path
               FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?""", (photo_id,),
        ).fetchone()
        return dict(row) if row else None

    def grouping_history(self, *, limit: int = 200) -> dict:
        summary = self.conn.execute(
            """SELECT COUNT(*) AS count,
                      COALESCE(SUM(p.bytes), 0) AS bytes,
                      COALESCE(SUM(CASE WHEN h.undone THEN p.bytes ELSE 0 END), 0) AS redo_bytes
               FROM edit_history_payloads p JOIN edit_history h ON h.id=p.edit_id"""
        ).fetchone()
        entries = self.conn.execute(
            """SELECT p.edit_id, h.workspace_id, w.name AS workspace,
                      h.description, h.created_at, h.undone,
                      p.bytes
               FROM edit_history_payloads p JOIN edit_history h ON h.id=p.edit_id
               JOIN workspaces w ON w.id=h.workspace_id
               ORDER BY p.edit_id DESC LIMIT ?""", (limit,)
        ).fetchall()
        return {**dict(summary), "entries": [dict(row) for row in entries],
                "truncated": summary["count"] > len(entries)}

    def grouping_payload_ids(self) -> list[int]:
        return [row[0] for row in self.conn.execute("SELECT edit_id FROM edit_history_payloads")]

    def latest_maintenance(self) -> dict | None:
        row = self.conn.execute(
            """SELECT id, status, started_at, finished_at, summary
               FROM job_history WHERE type IN ('retained-cache-cleanup', 'retained-cache-maintenance')
               ORDER BY started_at DESC LIMIT 1"""
        ).fetchone()
        return dict(row) if row else None
