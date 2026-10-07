"""Reads of finished jobs in ``job_history``, for routes that restore a result.

``JobRunner`` (``jobs.py``) creates, migrates and writes ``job_history`` on
its own connection; this repository only reads it, through the request's
``Database``. Job rows carry the triggering workspace's id, but the reads
here are catalog-wide on purpose (a duplicate scan covers every photo), so
the repository takes no workspace id. ``Database`` keeps
``get_last_completed_job`` as a thin wrapper over this class.
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
