"""Persistence for library audits: audit-run records and hash integrity.

``Database`` owns the active-workspace state; this repository owns the SQL.
Methods that act on the active workspace (audit runs, the integrity
queries) use ``self.workspace_id``, which the façade resolves with
``Database._ws_id()`` when it builds the repository. The hash-check verdict
write is catalog-wide and takes the photo id as an argument, matching the
``Database`` method it backs.
"""

from datetime import datetime


class AuditRepository:
    def __init__(self, conn, workspace_id, *, chunk_size=800):
        self.conn = conn
        self.workspace_id = workspace_id
        self.chunk_size = chunk_size

    # -- audit runs ----------------------------------------------------------

    def record_run(self, check_name, problem_count):
        """Record that an audit check ran now and what it found.

        One row per (workspace, check); re-running a check overwrites its
        previous row. The audit summary reads these to decide whether the
        archive can honestly be called intact.
        """
        self.conn.execute(
            "INSERT OR REPLACE INTO audit_runs "
            "(workspace_id, check_name, ran_at, problem_count) "
            "VALUES (?, ?, ?, ?)",
            (self.workspace_id, check_name, datetime.now().isoformat(),
             int(problem_count)),
        )
        self.conn.commit()

    def get_runs(self):
        """Return {check_name: {ran_at, problem_count}} for this workspace."""
        rows = self.conn.execute(
            "SELECT check_name, ran_at, problem_count FROM audit_runs "
            "WHERE workspace_id = ?",
            (self.workspace_id,),
        ).fetchall()
        return {
            r["check_name"]: {
                "ran_at": r["ran_at"],
                "problem_count": r["problem_count"],
            }
            for r in rows
        }

    # -- hash integrity ------------------------------------------------------

    def get_integrity_photos(self):
        """Return workspace photos with the fields hash verification needs."""
        rows = self.conn.execute(
            """SELECT p.id, p.filename, p.file_hash, p.file_mtime,
                      p.hash_status, p.hash_checked_at, f.path AS folder_path
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')
               ORDER BY p.id""",
            (self.workspace_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_integrity_flagged(self):
        """Return workspace photos whose last hash check found a problem."""
        rows = self.conn.execute(
            """SELECT p.id AS photo_id, p.filename, p.hash_status,
                      p.hash_checked_at, f.path AS folder_path
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')
               WHERE p.hash_status IN ('modified', 'corrupt', 'unreadable')
               ORDER BY p.hash_status, p.filename""",
            (self.workspace_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_integrity_stats(self):
        """Return hash-verification coverage for the active workspace.

        ``unchecked`` is load-bearing for the summary banner: photos added
        after the last verify run have hash_checked_at NULL, so a green
        light can't silently cover files that were never re-hashed.
        """
        row = self.conn.execute(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN p.hash_checked_at IS NOT NULL
                          THEN 1 ELSE 0 END) AS checked,
                      SUM(CASE WHEN p.hash_status IN
                          ('modified', 'corrupt', 'unreadable')
                          THEN 1 ELSE 0 END) AS flagged
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')""",
            (self.workspace_id,),
        ).fetchone()
        total = row["total"] or 0
        checked = row["checked"] or 0
        return {
            "total": total,
            "checked": checked,
            "unchecked": total - checked,
            "flagged": row["flagged"] or 0,
        }

    def update_photo_hash_check(self, photo_id, status, file_hash=None,
                                commit=True, clear_file_hash=False):
        """Record a hash-verification verdict for one photo.

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
        if commit:
            self.conn.commit()
