"""The runner's ``job_history`` rows: startup sweep, live rows, final rows.

Every write here opens its own SQLite connection, so worker threads never
share the caller's ``Database`` handle; no runner lock is held around any of
them. ``JobHistory.db_path`` is None for a runner built without a database,
and the runner checks it before each write, as it always has.
"""

import json
import logging
import sqlite3
import time
from datetime import datetime

from job_runner.outcome import attach_result_text, build_summary, error_total

# These lines were logged by ``jobs.JobRunner``; keep the logger name the log
# and its readers know them by.
log = logging.getLogger("jobs")

INTERRUPTED_BY_RESTART = "Interrupted by Vireo restart"

TERMINAL_STATUSES = ("completed", "failed", "cancelled")

# Finished operations kept per workspace and origin. An operation is a root
# job plus every follow-up it started (``parent_job_id``), pruned together so
# a grouped Jobs page never shows a chained run whose import was dropped.
# Vireo's own work keeps a separate quota, so recurring checks cannot evict
# the user's history.
HISTORY_OPERATIONS_PER_ORIGIN = 100

_PRUNE_OPERATIONS_SQL = """
WITH RECURSIVE
ws_rows AS (
    SELECT id, parent_job_id, origin, started_at FROM job_history
     WHERE workspace_id IS ?
),
lineage(id, root) AS (
    SELECT id, id FROM ws_rows
     WHERE parent_job_id IS NULL
        OR parent_job_id NOT IN (SELECT id FROM ws_rows)
    UNION
    SELECT child.id, lineage.root FROM job_history child
      JOIN lineage ON child.parent_job_id = lineage.id
),
open_roots AS (
    SELECT DISTINCT lineage.root FROM lineage
      JOIN job_history j ON j.id = lineage.id
     WHERE j.status IS NULL
        OR j.status NOT IN ('completed', 'failed', 'cancelled')
),
ranked AS (
    SELECT id, ROW_NUMBER() OVER (
               PARTITION BY origin ORDER BY started_at DESC
           ) AS rank
      FROM ws_rows
     WHERE id IN (SELECT root FROM lineage)
       AND id NOT IN (SELECT root FROM open_roots)
)
DELETE FROM job_history WHERE id IN (
    SELECT lineage.id FROM lineage JOIN ranked ON ranked.id = lineage.root
     WHERE ranked.rank > ?
)
"""


class JobHistoryUnavailable(RuntimeError):
    """The ``job_history`` rows could not be read from the catalog.

    Raised instead of returning ``[]``: an empty list reads as "no jobs have
    run", which is not what happened. The message is user-facing.
    """


def _is_noop(result):
    """Whether a work function reported that its run changed nothing."""
    return isinstance(result, dict) and result.get("noop") is True


def _load_json(blob):
    """Decode a job_history JSON column, tolerating NULL and junk."""
    if not blob or not isinstance(blob, str):
        return None
    try:
        return json.loads(blob)
    except (json.JSONDecodeError, TypeError):
        return None


class JobHistory:
    """Reads and writes the runner's ``job_history`` rows.

    Owns ``db_path`` (set once at construction). Holds no lock: callers
    reach it outside the runner lock, except ``write_checkpoints``, which
    the checkpointer calls under its own ``write_lock``.
    """

    def __init__(self, db_path=None):
        self.db_path = db_path

    def startup_sweep(self, db):
        """Reconcile job_history with the fact that we just started.

        On a clean shutdown a job thread either finishes or is cancelled
        and the row is updated accordingly. On a crash or kill, threads
        die without persisting — so rows with ``status='running'`` from
        a prior process are orphans. Mark them ``'failed'``. Queued rows
        from a prior process likewise lose their in-process work closure;
        mark them ``'failed'`` too so they don't linger forever waiting
        for a slot.

        Running rows carry whatever the checkpoint thread last wrote
        (step tree, progress, partial result). Keep all of it: the user
        wants to know what the job got done before the restart, not just
        that it was interrupted. Steps still marked running are flipped
        to failed with the interruption as their error; completed steps
        keep their summaries; the checkpoint time becomes ``finished_at``
        (the last moment the job was known to be alive) and is exposed as
        ``result.last_progress_at`` so the UI can say when progress was
        last recorded — or that none was.

        Future PR: rebuild work closures from the ``config`` blob on
        startup so queued runs survive restart.
        """
        now = datetime.now().isoformat()
        msg = INTERRUPTED_BY_RESTART
        rows = db.job_history.unfinished()
        for row in rows:
            was_queued = row["status"] == "queued"
            result = _load_json(row["result"])
            if not isinstance(result, dict):
                result = {}
            tree = _load_json(row["tree"])
            if not isinstance(tree, list):
                tree = []
            progress = _load_json(row["progress"])
            if not isinstance(progress, dict):
                progress = {}
            last_progress_at = progress.pop("checkpoint_at", None)
            for step in tree:
                if isinstance(step, dict) and step.get("status") == "running":
                    step["status"] = "failed"
                    step["error"] = step.get("error") or msg
                    step["error_count"] = max(
                        int(step.get("error_count") or 0), 1,
                    )
            result.update({
                "error": msg,
                "interrupted": True,
                "last_progress_at": last_progress_at,
            })
            if was_queued:
                # Durable marker so callers can distinguish a queued row
                # the sweep failed from one that actually started (e.g.
                # a chained pipeline swept before any slot promoted it,
                # which _chained_job_exists must not treat as proof the
                # import handed its photos to processing).
                result["never_started"] = True
            db.job_history.mark_interrupted(
                row["id"],
                last_progress_at or now,
                json.dumps(result, default=str),
                int(row["error_count"] or 0) + 1,
                json.dumps(tree) if tree else row["tree"],
                self.interrupted_summary(msg, was_queued, tree, progress),
                json.dumps(progress, default=str) if progress else None,
            )
        db.commit()

    @staticmethod
    def interrupted_summary(msg, was_queued, tree, progress):
        """One-line history summary for a job the restart cut short."""
        if was_queued:
            return f"{msg} before it started"
        if tree:
            done = sum(
                1 for s in tree
                if isinstance(s, dict) and s.get("status") == "completed"
            )
            noun = "step" if len(tree) == 1 else "steps"
            return f"{msg} after {done} of {len(tree)} {noun}"
        current = progress.get("current") or 0
        total = progress.get("total") or 0
        if isinstance(current, (int, float)) and isinstance(total, (int, float)):
            if total:
                return f"{msg} at {int(current):,} of {int(total):,}"
            if current:
                return f"{msg} at {int(current):,}"
        return msg

    # ------------------------------------------------------------------
    # Live checkpoints
    # ------------------------------------------------------------------

    @staticmethod
    def insert_running_row(conn, job):
        """Insert the job's history row as running, if it has none yet.

        ``OR IGNORE`` so a pipeline promoted from ``queued`` (whose row
        already exists) and a job whose final row already landed are
        both left alone.
        """
        conn.execute(
            "INSERT OR IGNORE INTO job_history "
            "(id, type, status, started_at, config, workspace_id, error_count, "
            " origin, parent_job_id, reason) "
            "VALUES (?, ?, 'running', ?, ?, ?, 0, ?, ?, ?)",
            (
                job["id"],
                job["type"],
                job["started_at"],
                json.dumps(job.get("config") or {}, default=str),
                job.get("workspace_id"),
                job.get("origin") or "user",
                job.get("parent_job_id"),
                job.get("reason"),
            ),
        )

    def record_job_started(self, job):
        """Write the running row before any work happens.

        Bookkeeping must never break the job: lock contention is logged
        and skipped (the checkpoint thread retries the insert on its next
        tick).
        """

        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            self.insert_running_row(conn, job)
            conn.commit()
        except sqlite3.Error as exc:
            log.warning(
                "Could not record start of job %s in history: %s",
                job["id"], exc,
            )
        finally:
            if conn is not None:
                conn.close()

    def write_checkpoints(self, snapshots):
        """Write checkpoint snapshots in one transaction.

        The UPDATE is guarded by ``status='running'`` so a checkpoint that
        races the final ``persist_job`` write can never revert a terminal
        row back to running. Lock contention (a worker mid-transaction on
        its own connection) is skipped, not waited out: the next tick
        tries again.
        """

        now_iso = datetime.now().isoformat()
        now = time.time()
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            written = 0
            for job in snapshots:
                progress = dict(job.get("progress") or {})
                progress["checkpoint_at"] = now_iso
                elapsed = now - (job.get("_start_time") or now)
                result = job.get("result")
                self.insert_running_row(conn, job)
                cur = conn.execute(
                    "UPDATE job_history "
                    "SET duration=?, result=COALESCE(?, result), "
                    "    error_count=?, tree=?, "
                    "    summary=?, progress=?, resource_wait_seconds=?, "
                    "    resource_wait_count=? "
                    "WHERE id = ? AND status = 'running'",
                    (
                        round(elapsed, 1),
                        json.dumps(result, default=str)
                        if result is not None else None,
                        # Mirror ``persist_job``: a bounded-sample ``errors``
                        # list publishes the real count as ``errors_total``,
                        # so the checkpointed ``error_count`` must read that
                        # too — otherwise an interrupted job's startup-sweep
                        # ``+1`` lands on the capped sample size, not the
                        # true total.
                        error_total(job),
                        json.dumps(job.get("steps") or [], default=str),
                        build_summary(job),
                        json.dumps(progress, default=str),
                        job.get("resource_wait_seconds", 0.0),
                        job.get("resource_wait_count", 0),
                        job["id"],
                    ),
                )
                written += cur.rowcount
            conn.commit()
            return written
        except sqlite3.OperationalError as exc:
            log.debug("Skipped job checkpoint: %s", exc)
            return 0
        finally:
            if conn is not None:
                conn.close()

    # ------------------------------------------------------------------
    # Queued pipeline rows
    # ------------------------------------------------------------------

    def insert_queued_row(self, job_id, now_iso, config_blob, workspace_id,
                          lineage=None):
        """Persist a pipeline's ``queued`` row.

        ``lineage`` is the resolved ``origin`` / ``parent_job_id`` /
        ``reason`` fields. Uses a thread-local connection so we don't share
        the caller's DB handle across thread boundaries.
        """
        lineage = lineage or {}
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            conn.execute(
                "INSERT INTO job_history "
                "(id, type, status, started_at, config, workspace_id, "
                " error_count, origin, parent_job_id, reason) "
                "VALUES (?, 'pipeline', 'queued', ?, ?, ?, 0, ?, ?, ?)",
                (
                    job_id, now_iso, json.dumps(config_blob), workspace_id,
                    lineage.get("origin") or "user",
                    lineage.get("parent_job_id"),
                    lineage.get("reason"),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def flip_queued_row_to_running(self, job_id):
        """Flip the job's history row from queued to running.

        Returns False when the row was no longer queued.
        """
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            cur = conn.execute(
                "UPDATE job_history SET status='running' "
                "WHERE id = ? AND status = 'queued'",
                (job_id,),
            )
            promoted = cur.rowcount == 1
            conn.commit()
        finally:
            conn.close()
        return promoted

    def cancel_queued_history_row(self, job_id, cancelled_at, db_timeout):
        """Flip a queued job's history row to cancelled.

        Returns False when the row was no longer queued.
        """
        conn = sqlite3.connect(self.db_path, timeout=max(0.0, db_timeout))
        try:
            cur = conn.execute(
                "UPDATE job_history "
                "SET status='cancelled', finished_at=?, result=? "
                "WHERE id = ? AND status = 'queued'",
                (cancelled_at, json.dumps({"cancelled": True, "never_started": True}), job_id),
            )
            conn.commit()
            cancelled = cur.rowcount == 1
        finally:
            conn.close()
        return cancelled

    # ------------------------------------------------------------------
    # Final rows
    # ------------------------------------------------------------------

    @staticmethod
    def final_result(job):
        """The result a finished job's row stores, with its primary error."""
        result_data = job["result"]
        if job["status"] == "failed" and job["errors"]:
            # Preserve a structured result (e.g. the pipeline's stages dict)
            # when the work function stashed one before raising. Otherwise
            # fall back to a minimal {"error": ...} payload so the history
            # row still carries something useful.
            # Use the pre-selected fatal error when available (pipeline jobs
            # set _fatal_error to a "[stage] Fatal: …" message, which is the
            # true failure cause). Fall back to errors[0] for non-pipeline
            # jobs or edge cases where _fatal_error wasn't set.
            primary_error = job.get("_fatal_error") or job["errors"][0]
            if isinstance(result_data, dict):
                result_data = {**result_data, "error": primary_error}
                if job.get("_partial_failure"):
                    result_data["partial_failure"] = error_total(job)
            else:
                result_data = {"error": primary_error}
        return result_data

    def persist_job(self, job, duration):
        """Persist job to history table using a thread-local connection.

        A job Vireo started on its own (``origin='system'``) is compared with
        the previous run of the same task first; see ``system_disposition``.
        """
        if not self.db_path:
            return

        result_data = self.final_result(job)
        summary = build_summary(job, result_data)
        config_json = json.dumps(job["config"])
        for attempt in range(3):
            conn = None
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                action, previous = self.system_disposition(
                    conn, job, result_data, summary, config_json,
                )
                if action == "drop":
                    # Routine: the check ran and found nothing to do. Its
                    # running row goes; a crash would have left it failed.
                    conn.execute("DELETE FROM job_history WHERE id = ?", (job["id"],))
                    conn.commit()
                    return
                repeat_count, repeat_since = 1, None
                if action == "collapse":
                    repeat_count = int(previous["repeat_count"] or 1) + 1
                    repeat_since = previous["repeat_since"] or previous["started_at"]
                    self.replace_row(conn, previous["id"], job["id"])
                conn.execute(
                    """INSERT OR REPLACE INTO job_history
                       (id, type, status, started_at, finished_at, duration,
                        result, error_count, config, workspace_id, tree, summary,
                        resource_wait_seconds, resource_wait_count,
                        origin, parent_job_id, reason, repeat_count, repeat_since)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                               ?, ?, ?, ?, ?)""",
                    (
                        job["id"], job["type"], job["status"],
                        job["started_at"], job["finished_at"],
                        round(duration, 1), json.dumps(result_data),
                        error_total(job), config_json, job.get("workspace_id"),
                        json.dumps(job.get("steps", [])), summary,
                        job.get("resource_wait_seconds", 0.0),
                        job.get("resource_wait_count", 0),
                        job.get("origin") or "user", job.get("parent_job_id"),
                        job.get("reason"), repeat_count, repeat_since,
                    ),
                )
                self.prune_operations(conn, job.get("workspace_id"))
                conn.commit()
                return
            except sqlite3.OperationalError:
                if attempt < 2:
                    time.sleep(2)
                else:
                    log.warning(
                        "Failed to persist job history for %s after 3 attempts",
                        job["id"],
                    )
            finally:
                # Close on every path — a failed execute/commit previously
                # leaked the connection (one per retry).
                if conn is not None:
                    conn.close()

    @staticmethod
    def system_disposition(conn, job, result_data, summary, config_json):
        """How a finished job's row relates to the previous run of its task.

        Returns ``(action, previous_row)``, ``action`` one of:

        - ``"keep"``: write a new row. Every job the user caused, every
          cancelled run, and any system outcome that differs from the last.
        - ``"collapse"``: the previous run of the same system task (same
          type, workspace and config) ended the same way (failed again, or
          succeeded with the same summary). Its row is replaced by this one,
          carrying the repeat count and when the streak began, so an
          unmounted NAS failing every check is one row, not hundreds.
        - ``"drop"``: a successful system run that changed nothing
          (``result["noop"]``) after a run that also changed nothing, or
          the first such run. A no-op that follows a failure or a run that
          found something is kept: it records that the state changed.
        """
        if job.get("origin") != "system" or job["status"] not in ("completed", "failed"):
            return "keep", None
        conn.row_factory = sqlite3.Row
        previous = conn.execute(
            "SELECT id, status, summary, result, started_at, repeat_count, "
            "       repeat_since "
            "FROM job_history "
            "WHERE type = ? AND workspace_id IS ? AND config = ? "
            "  AND origin = 'system' AND id != ? AND started_at <= ? "
            "  AND status IN ('completed', 'failed', 'cancelled') "
            "ORDER BY started_at DESC LIMIT 1",
            (job["type"], job.get("workspace_id"), config_json, job["id"],
             job["started_at"]),
        ).fetchone()
        conn.row_factory = None
        if job["status"] == "completed" and _is_noop(result_data):
            if previous is None or (
                previous["status"] == "completed"
                and _is_noop(_load_json(previous["result"]))
            ):
                return "drop", None
            return "keep", None
        if previous is not None and previous["status"] == job["status"] and (
            job["status"] == "failed" or previous["summary"] == summary
        ):
            return "collapse", previous
        return "keep", None

    @staticmethod
    def replace_row(conn, old_id, new_id):
        """Delete ``old_id``'s row; follow-ups it started now name ``new_id``."""
        conn.execute(
            "UPDATE job_history SET parent_job_id = ? WHERE parent_job_id = ?",
            (new_id, old_id),
        )
        conn.execute("DELETE FROM job_history WHERE id = ?", (old_id,))

    @staticmethod
    def prune_operations(conn, workspace_id):
        """Keep the newest finished operations of each origin in a workspace.

        Operations with any row still queued or running are never pruned:
        a queued pipeline waiting behind a busy slot can sit in the table
        for a long time, and if its row got pruned by an unrelated job
        completing, the next promotion attempt would see rowcount==0 on its
        conditional UPDATE and treat that as a cancel, silently dropping
        the run.
        """
        conn.execute(
            _PRUNE_OPERATIONS_SQL,
            (workspace_id, HISTORY_OPERATIONS_PER_ORIGIN),
        )

    def origin_of(self, job_id):
        """The recorded origin of ``job_id``, or None when there is no row."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            row = conn.execute(
                "SELECT origin FROM job_history WHERE id = ?", (job_id,),
            ).fetchone()
            return row[0] if row else None
        except sqlite3.Error as exc:
            log.warning("Could not read the origin of job %s: %s", job_id, exc)
            return None
        finally:
            if conn is not None:
                conn.close()

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_history(self, db, limit=10):
        """Get recent job history from the database.

        Only TERMINAL rows (completed/failed/cancelled) are returned —
        ``queued`` and ``running`` rows represent live state and surface
        through ``list_jobs()`` / ``get()`` so the UI can render and
        cancel them. Including them in history would make queued runs
        show up under "last run" / Jobs-page history with no cancel
        affordance, which is exactly the wrong UX.

        Args:
            db: Database instance (must be from the calling thread)
            limit: max number of rows

        Raises:
            JobHistoryUnavailable: the rows could not be read.
        """
        try:
            ws_id = db.active_workspace_id
            terminal = ("completed", "failed", "cancelled")
            rows = db.job_history.terminal(ws_id, terminal, limit)
            result = []
            for r in rows:
                d = dict(r)
                for field in ("tree", "result", "config", "progress"):
                    if d.get(field) and isinstance(d[field], str):
                        try:
                            d[field] = json.loads(d[field])
                        except (json.JSONDecodeError, TypeError):
                            pass
                # The Jobs page reads ``tree.length`` and iterates with
                # ``forEach``; a stored ``tree`` that decoded to anything
                # other than a list (``{"length": 1}``, a bare int) would
                # abort rendering of the whole history pane. Iterating a
                # dict visits its keys without raising, so the per-row
                # guard below never fires on that shape — drop it here.
                if d.get("tree") is not None and not isinstance(d["tree"], list):
                    log.warning(
                        "Dropping malformed tree in history row %r", d.get("id"),
                    )
                    d["tree"] = []
                try:
                    attach_result_text(d)
                except (TypeError, ValueError, AttributeError, LookupError):
                    # A stored field decoded to an unexpected shape (e.g. a
                    # ``tree`` that is not a list, which ``attach_result_text``
                    # iterates). Describer bugs already fall back inside
                    # ``describe_result``; keep this handler for the stored
                    # row itself so one bad row cannot poison the whole
                    # response. The row still ships with its raw fields.
                    log.warning(
                        "Could not render job history row %r", d.get("id"),
                        exc_info=True,
                    )
                result.append(d)
            return result
        except sqlite3.Error as exc:
            # Stored JSON is already tolerated per field above, and describer
            # bugs fall back to the generic summary in ``describe_result``; a
            # failure here is the catalog itself (locked, corrupt, missing
            # table). Say so rather than answering "no jobs have run".
            log.warning("Could not read job history", exc_info=True)
            raise JobHistoryUnavailable(
                f"Vireo could not read the job history from its catalog ({exc})."
            ) from exc
