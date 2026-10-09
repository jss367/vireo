"""Job-route reads and writes of finished jobs in ``job_history``.

``jobs.JobRunner`` creates and migrates the table on its own connection and
still runs its own writes (the checkpoint, the final record, the startup
reconciliation and the history listing). This repository holds what the job
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
        """Replace the stored ``result`` JSON of ``job_id`` and commit.

        The commit goes through the connection's own ``commit``, so it stays
        a no-op while ``Database._commits_held`` holds commits (undo/redo
        replay), like every other repository write.
        """
        self.conn.execute(
            "UPDATE job_history SET result = ? WHERE id = ?", (result_json, job_id),
        )
        self.conn.commit()

    def parent_import_row(self, parent_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT type, status, workspace_id, config, result "
            "FROM job_history WHERE id = ?",
            (parent_id,),
        ).fetchone()

    def chained_job_row(self, parent_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT 1 FROM job_history "
            "WHERE json_extract(config, '$.chained_from') = ? "
            "  AND COALESCE(json_extract(result, '$.never_started'), 0) = 0 "
            "LIMIT 1",
            (parent_id,),
        ).fetchone()

    def pipeline_resume_rows(self, workspace_id: int | None, process_ids: Sequence[str]) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in process_ids)
        return self.conn.execute(
            f"SELECT id, type, status, started_at, config, result FROM job_history "
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
