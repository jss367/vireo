"""Persistence for library audits: audit-run records and hash integrity.

``Database`` owns the active-workspace state; this repository owns the SQL.
Methods that act on the active workspace (audit runs, the integrity
queries) resolve it through ``workspace_id_fn`` (``Database._ws_id``) before
running any SQL, so with no workspace active they raise ``RuntimeError``
having touched nothing; building the repository never resolves it. The
hash-check verdict write is catalog-wide, takes the photo id as an argument
and never resolves a workspace. So are the repair helpers the audit module
runs: removing confirmed orphan rows (through ``photo_row_deletion``, never
committing; ``audit.remove_orphans`` commits), the cataloged names in one
folder that ``import_untracked`` compares before and after its scan, and one
photo's filename and folder path for the orphan recheck.

Callers reach it as ``db.audit`` (a fresh repository per access, see
``Database.audit``); there are no forwarding wrappers on ``Database``.
"""

import json
import sqlite3
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any

from repositories.photo_row_deletion import photo_row_deletion


class AuditRepository:
    def __init__(self, conn: sqlite3.Connection,
                 workspace_id_fn: Callable[[], int], *,
                 chunk_size: int = 800) -> None:
        self.conn = conn
        self.workspace_id_fn = workspace_id_fn
        self.chunk_size = chunk_size

    # -- audit runs ----------------------------------------------------------

    def record_run(self, check_name: str, problem_count: int) -> None:
        """Record that an audit check ran now and what it found.

        One row per (workspace, check); re-running a check overwrites its
        previous row. The audit summary reads these to decide whether the
        archive can honestly be called intact.
        """
        workspace_id = self.workspace_id_fn()
        self.conn.execute(
            "INSERT OR REPLACE INTO audit_runs "
            "(workspace_id, check_name, ran_at, problem_count) "
            "VALUES (?, ?, ?, ?)",
            (workspace_id, check_name, datetime.now().isoformat(),
             int(problem_count)),
        )

    def get_runs(self) -> dict[str, dict[str, Any]]:
        """Return {check_name: {ran_at, problem_count}} for this workspace."""
        workspace_id = self.workspace_id_fn()
        rows = self.conn.execute(
            "SELECT check_name, ran_at, problem_count FROM audit_runs "
            "WHERE workspace_id = ?",
            (workspace_id,),
        ).fetchall()
        return {
            r["check_name"]: {
                "ran_at": r["ran_at"],
                "problem_count": r["problem_count"],
            }
            for r in rows
        }

    # -- hash integrity ------------------------------------------------------

    def get_integrity_photos(self) -> list[dict[str, Any]]:
        """Return workspace photos with the fields hash verification needs."""
        workspace_id = self.workspace_id_fn()
        rows = self.conn.execute(
            """SELECT p.id, p.filename, p.file_hash, p.file_mtime,
                      p.hash_status, p.hash_checked_at, f.path AS folder_path
               FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                    AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')
               ORDER BY p.id""",
            (workspace_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_integrity_flagged(self) -> list[dict[str, Any]]:
        """Return workspace photos whose last hash check found a problem."""
        workspace_id = self.workspace_id_fn()
        rows = self.conn.execute(
            """SELECT p.id AS photo_id, p.filename, p.hash_status,
                      p.hash_checked_at, f.path AS folder_path
               FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                    AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')
               WHERE p.hash_status IN ('modified', 'corrupt', 'unreadable')
               ORDER BY p.hash_status, p.filename""",
            (workspace_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_integrity_stats(self) -> dict[str, int]:
        """Return hash-verification coverage for the active workspace.

        ``unchecked`` is load-bearing for the summary banner: photos added
        after the last verify run have hash_checked_at NULL, so a green
        light can't silently cover files that were never re-hashed.
        """
        workspace_id = self.workspace_id_fn()
        row = self.conn.execute(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN p.hash_checked_at IS NOT NULL
                          THEN 1 ELSE 0 END) AS checked,
                      SUM(CASE WHEN p.hash_status IN
                          ('modified', 'corrupt', 'unreadable')
                          THEN 1 ELSE 0 END) AS flagged
               FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                    AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')""",
            (workspace_id,),
        ).fetchone()
        total = row["total"] or 0
        checked = row["checked"] or 0
        return {
            "total": total,
            "checked": checked,
            "unchecked": total - checked,
            "flagged": row["flagged"] or 0,
        }

    def update_photo_hash_check(self, photo_id: int, status: str,
                                file_hash: str | None = None,
                                clear_file_hash: bool = False) -> None:
        """Record a hash-verification verdict for one photo (no commit).

        When ``file_hash`` is given the stored baseline is replaced too
        (first-time baselining, or the user accepting an external edit).
        Set ``clear_file_hash=True`` to explicitly NULL the stored hash:
        used for zero-byte files so ``EMPTY_FILE_SHA256`` never lands in
        the ``file_hash`` column (it would otherwise collide as an exact
        duplicate of every other empty placeholder).
        """
        if clear_file_hash and file_hash is not None:
            raise ValueError(
                "clear_file_hash and file_hash are mutually exclusive"
            )
        now = datetime.now().isoformat()
        if clear_file_hash:
            self.conn.execute(
                "UPDATE photos SET hash_status = ?, hash_checked_at = ?, "
                "file_hash = NULL WHERE id = ?",
                (status, now, photo_id),
            )
        elif file_hash is not None:
            self.conn.execute(
                "UPDATE photos SET hash_status = ?, hash_checked_at = ?, "
                "file_hash = ? WHERE id = ?",
                (status, now, file_hash, photo_id),
            )
        else:
            self.conn.execute(
                "UPDATE photos SET hash_status = ?, hash_checked_at = ? "
                "WHERE id = ?",
                (status, now, photo_id),
            )

    # -- repairs ---------------------------------------------------------------

    def drift_history_state(self, photo_id: int) -> str:
        """Small audit snapshot of the metadata and queue a drift repair changes."""
        workspace_id = self.workspace_id_fn()
        keywords = [dict(row) for row in self.conn.execute(
            "SELECT pk.keyword_id, k.name, pk.source FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? ORDER BY pk.keyword_id", (photo_id,),
        )]
        pending = [dict(row) for row in self.conn.execute(
            "SELECT id, change_type, value FROM pending_changes "
            "WHERE photo_id = ? AND workspace_id = ? ORDER BY id",
            (photo_id, workspace_id),
        )]
        photo = self.conn.execute(
            "SELECT xmp_mtime FROM photos WHERE id = ?", (photo_id,),
        ).fetchone()
        return json.dumps({
            'keywords': keywords, 'pending_changes': pending,
            'xmp_mtime': photo['xmp_mtime'] if photo else None,
        }, sort_keys=True)

    def remove_orphan_rows(self, photo_ids: Sequence[int]) -> None:
        """Delete the catalog rows of orphaned photos, in the caller's transaction.

        For each photo, drops its ``photo_keywords``, embedded-keyword offers
        and ``pending_changes`` first, then deletes the photo rows through
        ``photo_row_deletion`` so static collections drop them too. Catalog
        wide (the caller confirms the ids); does not commit.
        """
        with photo_row_deletion(self.conn) as photo_rows:
            for pid in photo_ids:
                self.conn.execute("DELETE FROM photo_keywords WHERE photo_id = ?", (pid,))
                self.conn.execute(
                    "DELETE FROM photo_embedded_keyword_offered WHERE photo_id = ?", (pid,),
                )
                self.conn.execute("DELETE FROM pending_changes WHERE photo_id = ?", (pid,))
            photo_rows.delete(dict.fromkeys(photo_ids))

    def cataloged_names_in_folder(self, folder_path: str) -> list[sqlite3.Row]:
        """Filenames and companion paths of the photos in the folder stored at ``folder_path``.

        One column per row: each photo's ``filename``, plus the
        ``companion_path`` of those that have one (``UNION``, so duplicates
        collapse). Catalog-wide.
        """
        return self.conn.execute(
            "SELECT p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE f.path=? "
            "UNION SELECT p.companion_path FROM photos p JOIN folders f ON f.id=p.folder_id "
            "WHERE f.path=? AND p.companion_path IS NOT NULL",
            (folder_path, folder_path),
        ).fetchall()

    def photo_location(self, photo_id: int) -> sqlite3.Row | None:
        """``filename`` and folder ``path`` of one photo, or None (no row, or no folder row)."""
        return self.conn.execute(
            "SELECT p.filename, f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
            (photo_id,),
        ).fetchone()
