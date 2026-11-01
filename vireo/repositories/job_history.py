"""Job-route reads and writes of finished jobs in ``job_history``.

The table is part of the canonical schema (``canonical_schema.
FORMERLY_LAZY_OBJECTS``, with its workspace index and ownership trigger);
``jobs.JobRunner`` runs its own writes on its own connection (the checkpoint,
the final record, the startup reconciliation and the history listing). This repository holds what the job
routes do through the request's ``Database``: the newest completed result of
a job type (the Duplicates page restoring its last scan), the lookup of one
record by id, and the result rewrite that records a follow-up action on a
finished job. Job ids are globally unique and the restore reads are
catalog-wide on purpose (a duplicate scan covers every photo), so nothing here
is scoped to a workspace; callers that care compare the row's
``workspace_id`` themselves.

Callers reach it as ``db.job_history`` (a fresh repository per access, see
``Database.job_history``); there are no forwarding wrappers on ``Database``.
"""

import sqlite3
from collections.abc import Sequence


class JobHistoryRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def catalog_file(self) -> str:
        """The file SQLite opened as this connection's main database.

        ``PRAGMA database_list``'s path for ``main``: what ``JobRunner``
        reopens on its own connections for checkpoints and the final record.
        """
        return self.conn.execute("PRAGMA database_list").fetchone()[2]

    def unfinished(self) -> list[sqlite3.Row]:
        """Every ``running`` or ``queued`` row, with the columns the startup sweep rewrites."""
        return self.conn.execute(
            "SELECT id, status, result, tree, progress, error_count "
            "FROM job_history WHERE status IN ('running', 'queued')",
        ).fetchall()

    def mark_interrupted(
        self,
        job_id: str,
        finished_at: str,
        result_json: str,
        error_count: int,
        tree: str | None,
        summary: str,
        progress_json: str | None,
    ) -> None:
        """Fail one row the restart cut short. Does not commit (the sweep commits once)."""
        self.conn.execute(
            "UPDATE job_history "
            "SET status='failed', finished_at=?, result=?, error_count=?, "
            "    tree=?, summary=?, progress=? "
            "WHERE id = ?",
            (
                finished_at,
                result_json,
                error_count,
                tree,
                summary,
                progress_json,
                job_id,
            ),
        )

    def terminal(
        self, workspace_id: int | None, statuses: Sequence[str], limit: int,
        origin: str | None = None,
    ) -> list[sqlite3.Row]:
        """Up to ``limit`` newest-first rows in ``statuses``.

        Of one workspace or (``None``) every one. A workspace's rows include
        the global ones (``workspace_id`` NULL: storage maintenance, model
        downloads), which belong to every workspace; leaving them out hid
        Vireo's own maintenance from the Jobs page entirely.

        ``origin`` (``user`` / ``system``) narrows the window to one origin.
        ``limit`` is always the total row cap; a caller that shows one
        origin asks for it here, so newer rows of the other origin cannot
        fill the window and hide older retained ones (pruning keeps the
        two quotas apart for the same reason).
        """
        placeholders = ",".join(["?"] * len(statuses))
        clauses = [f"status IN ({placeholders})"]
        params: list[object] = list(statuses)
        if workspace_id is not None:
            clauses.append("(workspace_id = ? OR workspace_id IS NULL)")
            params.append(workspace_id)
        if origin is not None:
            clauses.append("origin = ?")
            params.append(origin)
        return self.conn.execute(
            f"SELECT * FROM job_history WHERE {' AND '.join(clauses)} "
            f"ORDER BY started_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()

    def analysis_retry_results(self, workspace_id: int) -> list[sqlite3.Row]:
        """Recent bounded selected-analysis attempts, including persisted partial outcomes."""
        return self.conn.execute(
            "SELECT id, result FROM job_history WHERE type='analysis-retry' AND workspace_id=? "
            "AND result IS NOT NULL ORDER BY started_at DESC LIMIT 50", (workspace_id,),
        ).fetchall()

    def last_completed_with_result(self, job_type: str) -> sqlite3.Row | None:
        """Newest completed ``job_type`` row that stored a result, or None.

        The row carries ``id``, ``started_at``, ``finished_at`` and the raw
        ``result`` JSON text; "newest" is the latest ``finished_at``.
        """
        return self.conn.execute(
            """SELECT id, started_at, finished_at, result
                 FROM job_history
                WHERE type = ?
                  AND status = 'completed'
                  AND result IS NOT NULL
                ORDER BY finished_at DESC
                LIMIT 1""",
            (job_type,),
        ).fetchone()

    def get(self, job_id: str) -> sqlite3.Row | None:
        """The full ``job_history`` row for ``job_id``, or None."""
        return self.conn.execute(
            "SELECT * FROM job_history WHERE id = ?", (job_id,)
        ).fetchone()

    def set_result(self, job_id: str, result_json: str) -> None:
        """Replace the stored ``result`` JSON of ``job_id`` (no commit)."""
        self.conn.execute(
            "UPDATE job_history SET result = ? WHERE id = ?", (result_json, job_id),
        )

    def parent_import_row(self, parent_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT type, status, workspace_id, config, result, started_at "
            "FROM job_history WHERE id = ?",
            (parent_id,),
        ).fetchone()

    def in_place_restart(self, parent_id: str) -> sqlite3.Row | None:
        """Find the run that took over an in-place import, including live runs."""
        return self.conn.execute(
            "SELECT id FROM job_history WHERE type = 'import-in-place' "
            "AND json_valid(config) AND json_extract(config, '$.restarted_from') = ? LIMIT 1",
            (parent_id,),
        ).fetchone()

    def chained_job_row(self, parent_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT 1 FROM job_history "
            "WHERE parent_job_id = ? "
            "  AND COALESCE(json_extract(result, '$.never_started'), 0) = 0 "
            "  AND NOT (type='pipeline' AND status='cancelled' AND duration IS NULL "
            "           AND (result IS NULL OR result='null')) "
            "LIMIT 1",
            (parent_id,),
        ).fetchone()

    def pipeline_resume_rows(self, workspace_id: int | None, process_ids: Sequence[str]) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in process_ids)
        return self.conn.execute(
            f"SELECT id, type, status, started_at, duration, config, result FROM job_history "
            f"WHERE type='pipeline' AND workspace_id IS ? AND id IN ({placeholders})",
            (workspace_id, *process_ids),
        ).fetchall()

    def terminal_import_lineage(self, workspace_id: int | None, root: str, parent_id: str, runner_ids: Sequence[str]) -> list[sqlite3.Row]:
        """Terminal import ancestry, including runner jobs not yet persisted."""
        runner_seeds = "".join(" UNION SELECT ?" for _ in runner_ids)
        return self.conn.execute(
            "WITH RECURSIVE lineage(id) AS ("
            " SELECT id FROM job_history WHERE type='import' AND workspace_id IS ?"
            " AND (id IN (?, ?) OR json_extract(config, '$.root_import_job_id') = ?"
            " OR json_extract(config, '$.parent_import_job_id') = ?)"
            + runner_seeds +
            " UNION SELECT child.id FROM job_history child JOIN lineage"
            " ON json_extract(child.config, '$.parent_import_job_id') = lineage.id"
            " WHERE child.type='import' AND child.workspace_id IS ?"
            ") SELECT id, type, status, started_at, config, result FROM job_history"
            " WHERE id IN (SELECT id FROM lineage)"
            " AND status IN ('completed', 'failed', 'cancelled')",
            (workspace_id, root, parent_id, root, parent_id,
             *runner_ids, workspace_id),
        ).fetchall()
