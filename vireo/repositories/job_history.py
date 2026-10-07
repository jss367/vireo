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
"""


class JobHistoryRepository:
    def __init__(self, conn):
        self.conn = conn

    def last_completed_with_result(self, job_type):
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

    def get(self, job_id):
        """The full ``job_history`` row for ``job_id``, or None."""
        return self.conn.execute(
            "SELECT * FROM job_history WHERE id = ?", (job_id,)
        ).fetchone()

    def set_result(self, job_id, result_json):
        """Replace the stored ``result`` JSON of ``job_id`` and commit."""
        self.conn.execute(
            "UPDATE job_history SET result = ? WHERE id = ?", (result_json, job_id),
        )
        self.conn.commit()
