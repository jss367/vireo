"""Background job runner with SSE progress streaming and log broadcasting."""

import json
import logging
import queue
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime

import power
from job_contract import failure_event
from job_summaries import describe_result
from resource_ledger import (
    bind_resource_cancel_check,
    bind_resource_owner,
    get_resource_ledger,
)

log = logging.getLogger(__name__)

# Job types that must NOT hold an idle-sleep assertion. Everything else
# does, including types added later: over-protecting a short job costs a
# few seconds of battery, while under-protecting a long one costs the
# whole run (issue #1397 — a 2h16m import suspended twelve minutes in,
# whose network share did not survive the sleep/DarkWake cycling).
#
# Ephemeral jobs are excluded wholesale — they are transient background
# work surfaced for transparency, and losing one to sleep is harmless.
_NO_SLEEP_ASSERTION_JOB_TYPES = frozenset({
    "missing_originals_scan",
    "working_copy_backfill",
    "thumb_path_backfill",
    "verify-models",
})

# How long to keep completed/failed jobs in memory before eviction (seconds)
_JOB_RETENTION_SECS = 3600  # 1 hour

_PROMOTION_RETRY_DELAY_SECS = 0.1
# How often live jobs checkpoint their step tree and progress into
# job_history. A crash or kill (the desktop app quits Flask without a
# graceful shutdown) never reaches _persist_job, so without checkpoints
# the startup sweep can only say "interrupted" — not what got done.
CHECKPOINT_INTERVAL_SECS = 2.0
INTERRUPTED_BY_RESTART = "Interrupted by Vireo restart"
_LIVE_STATUSES = ("running", "pausing", "paused")


def _load_json(blob):
    """Decode a job_history JSON column, tolerating NULL and junk."""
    if not blob or not isinstance(blob, str):
        return None
    try:
        return json.loads(blob)
    except (json.JSONDecodeError, TypeError):
        return None


class _TrackedJobThread(threading.Thread):
    """A normal job thread that reports when its target has fully returned."""

    def __init__(self, *args, on_finish, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_finish = on_finish

    def run(self):
        try:
            super().run()
        finally:
            self._on_finish(self)


# Maximum number of pipeline jobs allowed to run concurrently. Bumped
# to 2 in Step 6 of the concurrency rollout (Steps 1-3 added the
# ModelCache + GPU/regroup locks that make this safe; Steps 4-5 added
# the queue + UI). The cap is intentionally low: the gains from
# overlapping two pipelines come from one's GPU phase running while
# another's I/O / CPU phases progress. A third pipeline would mostly
# queue at the GPU lock without adding throughput.
# See docs/plans/2026-05-26-pipeline-concurrency-design.md.
SLOT_CAP = 2


class WorkspaceBusyError(RuntimeError):
    """A transfer and another job cannot share the workspace's originals."""


class JobRunner:
    """Runs long operations in background threads with progress tracking.

    Args:
        db: optional Database instance for persisting job history
    """

    def __init__(self, db=None):
        self._jobs = {}
        self._events = {}  # job_id -> deque of events
        self._subscribers = {}  # job_id -> list of queues
        self._lock = threading.Lock()
        self._pause_condition = threading.Condition(self._lock)
        self._cancelled = set()  # job ids that have been cancelled
        # Cooperative pause requests. Pausable work reaches these through
        # is_cancelled(), which doubles as its existing safe-point callback.
        # Keeping the wait in the runner means scan/import call sites do not
        # need a second callback threaded through every layer.
        self._pause_requested = set()
        # job ids past an uninterruptible commit point (e.g. the
        # local-processing archive move). Once a job is in here, any
        # late ``cancel_job`` call is a no-op so the terminal status can
        # no longer be flipped to "cancelled" by a Stop press that
        # landed after the commit finished but before _run_job recorded
        # the result. Cleared by _prune_finished_jobs alongside
        # _cancelled.
        self._uncancellable = set()
        # Held while any sleep-blocking job runs; see issue #1397. Resolved
        # through the module rather than bound at import so tests (and any
        # future platform override) can substitute the inhibitor.
        self.sleep_blocker = power.SleepBlocker(
            start_inhibitor=lambda reason: power.start_platform_inhibitor(
                reason
            ),
        )
        self._db_path = None
        # Pending pipeline work, keyed by job_id. Populated by
        # ``enqueue_pipeline`` and consumed by ``_try_promote_queued``
        # when a slot opens. The work_fn closure can't be persisted
        # cross-process, so a process restart will see queued rows in
        # job_history without a matching entry here; the startup sweep
        # promotes such rows to 'failed'.
        self._queued_pipelines = {}  # job_id -> dict(work_fn, config, ...)
        self._pipeline_admissions = {}  # workspace -> enqueues persisting outside the lock
        self._workspace_mutations = {}  # workspace -> synchronous API requests
        self._exclusive_workspace_mutations = set()
        # Monotonic suffix so two enqueues landing in the same
        # millisecond don't collide on the PRIMARY KEY.
        self._enqueue_counter = 0
        self._promotion_retry_scheduled = False
        # Periodic checkpointing of live jobs into job_history. Started
        # lazily by the first persisted job and exits on its own once no
        # persisted job is live, so idle runners (and the thousands the
        # test suite creates) own no thread.
        self._checkpoint_thread = None
        self._checkpoint_wakeup = threading.Event()
        # Worker ownership is explicit.  Daemon threads alone are not a
        # lifecycle: short-lived create_app() callers (especially tests) must
        # be able to cancel and join their work before tearing down databases
        # and process-wide monkeypatches.
        self._worker_threads = set()
        self._shutting_down = False
        if db:
            self._db_path = db.conn.execute("PRAGMA database_list").fetchone()[2]
            self._ensure_history_table(db)
            self._startup_sweep(db)

    def _start_worker_thread(self, job, work_fn):
        """Start and retain a worker until its complete cleanup finishes.

        Returns False when shutdown won the race with a caller that had
        already registered a job but had not started its thread yet.
        """
        def forget_thread(thread):
            with self._lock:
                self._worker_threads.discard(thread)

        thread = _TrackedJobThread(
            target=self._run_job,
            args=(job, work_fn),
            on_finish=forget_thread,
            daemon=True,
            name=f"vireo-job-{job['id']}",
        )
        with self._lock:
            if self._shutting_down:
                return False
            self._worker_threads.add(thread)
            # Start while still holding the ownership lock. shutdown() can
            # therefore never take a snapshot between registration and start.
            thread.start()
        return True

    def _discard_unstarted_job(self, job_id):
        """Remove a job whose worker lost the race with shutdown()."""
        with self._lock:
            self._jobs.pop(job_id, None)
            self._events.pop(job_id, None)
            self._subscribers.pop(job_id, None)
            self._cancelled.discard(job_id)
            self._pause_requested.discard(job_id)
            self._uncancellable.discard(job_id)

    def shutdown(self, timeout=10.0):
        """Stop accepting work, request cancellation, and join workers.

        Cancellation is cooperative: work functions must reach an
        ``is_cancelled``/``cancellation_requested`` checkpoint before their
        thread can exit. Returns False when one or more workers remain alive
        after *timeout*, allowing callers such as pytest fixtures to fail at
        the test that leaked the work instead of contaminating the next one.
        """
        if timeout < 0:
            raise ValueError("timeout must be non-negative")

        # Start the deadline clock before queued cancellation so database-lock
        # waits and worker joins share the caller's shutdown budget.
        deadline = time.monotonic() + timeout
        with self._pause_condition:
            self._shutting_down = True
            queued_ids = list(self._queued_pipelines)
            for job_id, job in self._jobs.items():
                if (
                    job.get("status") in ("running", "pausing", "paused")
                    and job_id not in self._uncancellable
                ):
                    self._cancelled.add(job_id)
                    self._pause_requested.discard(job_id)
            # A paused worker must wake before it can observe cancellation.
            self._pause_condition.notify_all()
            checkpoint_thread = self._checkpoint_thread
        # The checkpoint thread exits on its next wake once it sees
        # _shutting_down; wake it now so it doesn't hold the deadline.
        self._checkpoint_wakeup.set()
        if checkpoint_thread is not None and checkpoint_thread.is_alive():
            checkpoint_thread.join(timeout=max(0.0, deadline - time.monotonic()))

        # Queued jobs have persisted state to transition as well as in-memory
        # contexts to remove. Do that outside the runner lock, bounding each
        # SQLite lock wait by the time left for the whole shutdown.
        queued_cancelled = True
        for index, job_id in enumerate(queued_ids):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                queued_cancelled = False
                log.warning(
                    "JobRunner shutdown timed out cancelling queued jobs; "
                    "%d queued job(s) left uncancelled",
                    len(queued_ids) - index,
                )
                break
            try:
                self.cancel_job(
                    job_id,
                    promote_after_cancel=False,
                    db_timeout=remaining,
                )
            except Exception:
                queued_cancelled = False
                log.exception(
                    "Failed to cancel queued job %s during shutdown", job_id,
                )

        current = threading.current_thread()
        while True:
            with self._lock:
                threads = [
                    thread for thread in self._worker_threads
                    if thread is not current and thread.is_alive()
                ]
            if not threads:
                return queued_cancelled
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                log.warning(
                    "JobRunner shutdown timed out with %d worker(s) alive: %s",
                    len(threads), ", ".join(thread.name for thread in threads),
                )
                return False
            threads[0].join(timeout=remaining)

    def _ensure_history_table(self, db):
        db.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_history (
                id          TEXT PRIMARY KEY,
                type        TEXT,
                status      TEXT,
                started_at  TEXT,
                finished_at TEXT,
                duration    REAL,
                result      TEXT,
                error_count INTEGER DEFAULT 0,
                config      TEXT,
                workspace_id INTEGER,
                resource_wait_seconds REAL DEFAULT 0,
                resource_wait_count INTEGER DEFAULT 0
            )
            """
        )
        # Migration: add workspace_id to existing job_history tables
        try:
            db.conn.execute("SELECT workspace_id FROM job_history LIMIT 0")
        except Exception:
            db.conn.execute(
                "ALTER TABLE job_history ADD COLUMN workspace_id INTEGER"
            )
        db.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_history_workspace ON job_history(workspace_id)"
        )
        # Migration: add tree column
        try:
            db.conn.execute("SELECT tree FROM job_history LIMIT 0")
        except Exception:
            db.conn.execute("ALTER TABLE job_history ADD COLUMN tree TEXT")
        # Migration: add summary column
        try:
            db.conn.execute("SELECT summary FROM job_history LIMIT 0")
        except Exception:
            db.conn.execute("ALTER TABLE job_history ADD COLUMN summary TEXT DEFAULT ''")
        for column, definition in (
            ("resource_wait_seconds", "REAL DEFAULT 0"),
            ("resource_wait_count", "INTEGER DEFAULT 0"),
            # Last checkpointed job["progress"] (plus checkpoint_at) for
            # live rows; the startup sweep folds it into interrupted rows.
            ("progress", "TEXT"),
        ):
            try:
                db.conn.execute(f"SELECT {column} FROM job_history LIMIT 0")
            except Exception:
                db.conn.execute(
                    f"ALTER TABLE job_history ADD COLUMN {column} {definition}"
                )

    def _startup_sweep(self, db):
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
        rows = db.conn.execute(
            "SELECT id, status, result, tree, progress, error_count "
            "FROM job_history WHERE status IN ('running', 'queued')",
        ).fetchall()
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
            db.conn.execute(
                "UPDATE job_history "
                "SET status='failed', finished_at=?, result=?, error_count=?, "
                "    tree=?, summary=?, progress=? "
                "WHERE id = ?",
                (
                    last_progress_at or now,
                    json.dumps(result, default=str),
                    int(row["error_count"] or 0) + 1,
                    json.dumps(tree) if tree else row["tree"],
                    self._interrupted_summary(msg, was_queued, tree, progress),
                    json.dumps(progress, default=str) if progress else None,
                    row["id"],
                ),
            )
        db.conn.commit()

    @staticmethod
    def _interrupted_summary(msg, was_queued, tree, progress):
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
    def _insert_running_row(conn, job):
        """Insert the job's history row as running, if it has none yet.

        ``OR IGNORE`` so a pipeline promoted from ``queued`` (whose row
        already exists) and a job whose final row already landed are
        both left alone.
        """
        conn.execute(
            "INSERT OR IGNORE INTO job_history "
            "(id, type, status, started_at, config, workspace_id, error_count) "
            "VALUES (?, ?, 'running', ?, ?, ?, 0)",
            (
                job["id"],
                job["type"],
                job["started_at"],
                json.dumps(job.get("config") or {}, default=str),
                job.get("workspace_id"),
            ),
        )

    def _record_job_started(self, job):
        """Write the running row before any work happens.

        Bookkeeping must never break the job: lock contention is logged
        and skipped (the checkpoint thread retries the insert on its next
        tick).
        """
        import sqlite3

        conn = None
        try:
            conn = sqlite3.connect(self._db_path, timeout=5)
            self._insert_running_row(conn, job)
            conn.commit()
        except sqlite3.Error as exc:
            log.warning(
                "Could not record start of job %s in history: %s",
                job["id"], exc,
            )
        finally:
            if conn is not None:
                conn.close()

    def _live_persisted_jobs_locked(self):
        """Jobs worth checkpointing. Must be called with self._lock held."""
        return [
            j for j in self._jobs.values()
            if j["status"] in _LIVE_STATUSES
            and not j.get("ephemeral")
            and not j.get("_persisted")
        ]

    def _ensure_checkpoint_thread(self):
        """Start the checkpoint thread unless one is already running."""
        if not self._db_path:
            return
        with self._lock:
            if self._shutting_down:
                return
            thread = self._checkpoint_thread
            if thread is not None and thread.is_alive():
                return
            self._checkpoint_wakeup.clear()
            thread = threading.Thread(
                target=self._checkpoint_loop,
                daemon=True,
                name="vireo-job-checkpoints",
            )
            self._checkpoint_thread = thread
            thread.start()

    def _checkpoint_loop(self):
        while True:
            self._checkpoint_wakeup.wait(CHECKPOINT_INTERVAL_SECS)
            with self._lock:
                if self._shutting_down or not self._live_persisted_jobs_locked():
                    # Nothing left to checkpoint; the next persisted job
                    # start spawns a fresh thread.
                    self._checkpoint_thread = None
                    return
            try:
                self.checkpoint_live_jobs()
            except Exception:
                log.exception("Job checkpoint failed")

    def checkpoint_live_jobs(self):
        """Persist a snapshot of every live, non-ephemeral job.

        Writes the step tree, progress, partial result and elapsed time
        onto the job's ``running`` history row so a crash leaves a record
        of what the job got done. Returns the number of rows updated.
        Public so tests (and callers that just did something worth
        recording) can checkpoint deterministically instead of waiting
        for the timer.
        """
        if not self._db_path:
            return 0
        with self._lock:
            snapshots = [
                self._snapshot_job(j)
                for j in self._live_persisted_jobs_locked()
            ]
        if not snapshots:
            return 0
        return self._write_checkpoints(snapshots)

    def _write_checkpoints(self, snapshots):
        """Write checkpoint snapshots in one transaction.

        The UPDATE is guarded by ``status='running'`` so a checkpoint that
        races the final ``_persist_job`` write can never revert a terminal
        row back to running. Lock contention (a worker mid-transaction on
        its own connection) is skipped, not waited out: the next tick
        tries again.
        """
        import sqlite3

        now_iso = datetime.now().isoformat()
        now = time.time()
        conn = None
        try:
            conn = sqlite3.connect(self._db_path, timeout=5)
            written = 0
            for job in snapshots:
                progress = dict(job.get("progress") or {})
                progress["checkpoint_at"] = now_iso
                elapsed = now - (job.get("_start_time") or now)
                result = job.get("result")
                self._insert_running_row(conn, job)
                cur = conn.execute(
                    "UPDATE job_history "
                    "SET duration=?, result=?, error_count=?, tree=?, "
                    "    summary=?, progress=?, resource_wait_seconds=?, "
                    "    resource_wait_count=? "
                    "WHERE id = ? AND status = 'running'",
                    (
                        round(elapsed, 1),
                        json.dumps(result, default=str)
                        if result is not None else None,
                        len(job.get("errors") or []),
                        json.dumps(job.get("steps") or [], default=str),
                        self._build_summary(job),
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

    def enqueue_pipeline(self, work_fn, config=None, workspace_id=None,
                         runtime_warning=None):
        """Admit and persist a pipeline, then attempt promotion into a free slot."""
        # Cover the persistence window before the queued context is published.
        # A transfer must not slip between checking admission and publishing it.
        with self._lock:
            self._check_workspace_admission_locked(workspace_id)
            self._pipeline_admissions[workspace_id] = self._pipeline_admissions.get(workspace_id, 0) + 1
        try:
            return self._enqueue_pipeline_admitted(work_fn, config, workspace_id, runtime_warning)
        finally:
            with self._lock:
                self._pipeline_admissions[workspace_id] -= 1
                if not self._pipeline_admissions[workspace_id]:
                    del self._pipeline_admissions[workspace_id]

    def _enqueue_pipeline_admitted(self, work_fn, config=None, workspace_id=None,
                                   runtime_warning=None):
        """Enqueue a pipeline job and attempt promotion before returning.

        Unlike ``start`` (which spawns a worker thread synchronously),
        ``enqueue_pipeline`` persists the job to ``job_history`` with
        ``status='queued'``, stashes the work closure in-process, and
        then asks the scheduler to promote it. Promotion is attempted, not
        guaranteed: the job stays queued when no slot is free, when queue
        ordering promotes an older candidate first, or when the SQLite
        ``queued -> running`` flip fails and a retry is scheduled. Callers
        must not assume the work thread is running on return.

        Returns the job id.
        """
        with self._lock:
            if self._shutting_down:
                raise RuntimeError("JobRunner is shut down")
            self._enqueue_counter += 1
            seq = self._enqueue_counter
        job_id = f"pipeline-{int(time.time() * 1000)}-{seq}"
        now_iso = datetime.now().isoformat()
        config_blob = config or {}

        # Persist the queued row using a thread-local connection so we
        # don't share the caller's DB handle across thread boundaries.
        if self._db_path:
            import sqlite3
            conn = sqlite3.connect(self._db_path, timeout=30)
            try:
                conn.execute(
                    "INSERT INTO job_history "
                    "(id, type, status, started_at, config, workspace_id, "
                    " error_count) "
                    "VALUES (?, 'pipeline', 'queued', ?, ?, ?, 0)",
                    (job_id, now_iso, json.dumps(config_blob), workspace_id),
                )
                conn.commit()
            finally:
                conn.close()

        with self._lock:
            self._queued_pipelines[job_id] = {
                "work_fn": work_fn,
                "config": config_blob,
                "workspace_id": workspace_id,
                "runtime_warning": runtime_warning,
                "started_at": now_iso,
            }
            shutdown_raced = self._shutting_down

        # The database insert happens outside the runner lock. shutdown() may
        # begin in that window after the initial admission check; publish a
        # terminal cancellation instead of leaving a queued row behind after
        # the runner has already reported itself drained.
        if shutdown_raced:
            self.cancel_job(job_id, promote_after_cancel=False)
            raise RuntimeError("JobRunner is shut down")

        # Promote eagerly so a free slot is filled before we return.
        self._try_promote_queued()
        return job_id

    def _try_promote_queued(self):
        """Promote the oldest queued pipeline if a slot is open.

        Single-pass: count active and in-flight promotions under
        ``self._lock`` and mark the oldest queued context as promoting, then
        release the lock before the conditional SQLite UPDATE. If a Cancel
        lands first, rowcount==0 and promotion quietly gives up.
        """
        with self._lock:
            if self._shutting_down:
                return
            active = sum(
                1 for j in self._jobs.values()
                if (
                    j["type"] == "pipeline"
                    and j["status"] in ("running", "pausing", "paused")
                )
            )
            if active >= SLOT_CAP:
                return
            promoting = sum(
                1 for ctx in self._queued_pipelines.values()
                if ctx.get("_promoting")
            )
            if active + promoting >= SLOT_CAP:
                return
            candidates = sorted(
                (
                    item for item in self._queued_pipelines.items()
                    if not item[1].get("_promoting")
                ),
                key=lambda kv: kv[1]["started_at"],
            )
            if not candidates:
                return
            job_id, ctx = candidates[0]
            ctx["_promoting"] = True

        promoted = True
        try:
            # Atomic queued->running flip. If a concurrent cancel beat us
            # to it, rowcount is 0 and the row stays in its final state.
            if self._db_path:
                import sqlite3
                conn = sqlite3.connect(self._db_path, timeout=30)
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
        except Exception:
            with self._lock:
                if self._queued_pipelines.get(job_id) is ctx:
                    ctx.pop("_promoting", None)
                    self._schedule_promotion_retry_locked()
            log.exception("Failed to promote queued pipeline %s", job_id)
            return

        retry_promotion = False
        record_terminal = False
        with self._lock:
            if self._queued_pipelines.get(job_id) is not ctx:
                retry_promotion = True
            else:
                ctx.pop("_promoting", None)
                if not promoted:
                    # The row was modified elsewhere (cancelled).
                    self._queued_pipelines.pop(job_id, None)
                    record_terminal = True
                    retry_promotion = True
                else:
                    # Move from queue context into the live jobs dict.
                    del self._queued_pipelines[job_id]
                    job = {
                        "id": job_id,
                        "type": "pipeline",
                        "status": "running",
                        "started_at": ctx["started_at"],
                        "finished_at": None,
                        "progress": {"current": 0, "total": 0, "current_file": ""},
                        "result": None,
                        "errors": [],
                        "config": ctx["config"],
                        "workspace_id": ctx["workspace_id"],
                        "steps": [],
                        "ephemeral": False,
                        "counts_for_badge": True,
                        "pausable": True,
                        "runtime_warning": ctx["runtime_warning"],
                        # Pre-seeded for iteration safety — see start().
                        "_start_time": time.time(),
                        "_ended_at": None,
                        "_persisted": False,
                        "_fatal_error": None,
                    }
                    self._prune_finished_jobs()
                    self._jobs[job_id] = job
                    self._events[job_id] = deque(maxlen=1000)
                    # setdefault, NOT assignment: clients can subscribe to the
                    # SSE stream while the pipeline is still queued. Replacing
                    # the list at promotion time would silently drop those
                    # waiters' queues.
                    self._subscribers.setdefault(job_id, [])
                    work_fn = ctx["work_fn"]
                    if job_id in self._cancelled:
                        def work_fn(job):
                            return None

        if record_terminal:
            self._record_terminal_queued_pipeline(job_id, ctx, status="cancelled")
            self.push_event(
                job_id,
                "complete",
                {
                    "status": "cancelled",
                    "result": None,
                    "duration": 0.0,
                    "errors": [],
                },
            )
        if retry_promotion:
            self._try_promote_queued()
            return

        log.info("Job %s started type=pipeline", job["id"])
        if not self._start_worker_thread(job, work_fn):
            # shutdown() marked the installed job for cancellation while it
            # sat between promotion and thread start. Record the terminal row
            # here because this promoted pipeline was already persisted as
            # running before its in-memory worker registration existed.
            now = datetime.now().isoformat()
            with self._lock:
                job["status"] = "cancelled"
                job["finished_at"] = now
                job["_ended_at"] = time.time()
                self._cancelled.discard(job["id"])
            self.push_event(job["id"], "complete", {
                "job_id": job["id"],
                "job_type": job["type"],
                "status": "cancelled",
                "result": None,
                "duration": 0.0,
                "errors": [],
            })
            if self._db_path:
                self._persist_job(job, 0.0)
            job["_persisted"] = True

    def _schedule_promotion_retry_locked(self):
        """Retry queue promotion after a transient DB failure.

        Must be called with self._lock held.
        """
        if self._promotion_retry_scheduled or self._shutting_down:
            return
        self._promotion_retry_scheduled = True

        def retry():
            time.sleep(_PROMOTION_RETRY_DELAY_SECS)
            with self._lock:
                self._promotion_retry_scheduled = False
                if self._shutting_down:
                    return
            self._try_promote_queued()

        def forget_thread(thread):
            with self._lock:
                self._worker_threads.discard(thread)

        thread = _TrackedJobThread(
            target=retry,
            on_finish=forget_thread,
            daemon=True,
            name="vireo-promotion-retry",
        )
        self._worker_threads.add(thread)
        thread.start()

    def _record_terminal_queued_pipeline(self, job_id, ctx, status="cancelled"):
        """Keep a terminal queued pipeline in the normal in-memory lifecycle."""
        finished_at = datetime.now().isoformat()
        job = {
            "id": job_id,
            "type": "pipeline",
            "status": status,
            "started_at": ctx["started_at"],
            "finished_at": finished_at,
            "progress": {"current": 0, "total": 0, "current_file": ""},
            "result": None,
            "errors": [],
            "config": ctx["config"],
            "workspace_id": ctx["workspace_id"],
            "steps": [],
            "ephemeral": False,
            "counts_for_badge": True,
            "pausable": False,
            "runtime_warning": ctx.get("runtime_warning"),
            "_ended_at": time.time(),
            "_persisted": True,
        }
        with self._lock:
            self._prune_finished_jobs()
            self._jobs[job_id] = job
            self._events.setdefault(job_id, deque(maxlen=1000))
            self._subscribers.setdefault(job_id, [])

    def start(self, job_type, work_fn, config=None, workspace_id=None,
              ephemeral=False, runtime_warning=None, counts_for_badge=True,
              pausable=False, blocks_local_transitions=True,
              workspace_transfer_batch=None):
        """Start a background job.

        Args:
            job_type: string like 'scan', 'thumbnails', 'import', 'sync'
            work_fn: callable(job_dict) that does the work. Can update
                     job['progress'] and return a result dict.
            config: optional dict of job configuration (persisted to history)
            workspace_id: optional workspace id to associate with this job
            ephemeral: if True, the job runs and streams events normally but
                       is never written to ``job_history``. Use for transient
                       background work surfaced to the user for transparency
                       (e.g. the new-images filesystem walk) — it is fine to
                       lose the record on process restart and we don't want
                       it to clutter the history list.
            runtime_warning: optional user-facing warning metadata to expose
                       while the job is running.
            counts_for_badge: if False, the job remains visible in job lists
                       but does not contribute to app/Dock attention badges.
            pausable: if True, pause_job() may suspend the worker the next
                      time it calls is_cancelled(). Only set this for work
                      that checks cancellation at safe boundaries.
            blocks_local_transitions: if False, Work Locally stage/sync/discard
                      actions may proceed while this job runs. Reserve this
                      for observational jobs whose results are safely dropped
                      when a local transition invalidates their cache.
            workspace_transfer_batch: identifies automatic moves whose workers
                      call wait_for_workspace_transfer before touching originals.

        Returns:
            job_id string
        """
        job, work_fn = self._register_and_prepare(
            job_type, work_fn,
            config=config, workspace_id=workspace_id,
            ephemeral=ephemeral, runtime_warning=runtime_warning,
            counts_for_badge=counts_for_badge, pausable=pausable,
            blocks_local_transitions=blocks_local_transitions,
            workspace_transfer_batch=workspace_transfer_batch,
        )
        log.info("Job %s started type=%s", job["id"], job_type)
        if not self._start_worker_thread(job, work_fn):
            self._discard_unstarted_job(job["id"])
            raise RuntimeError("JobRunner is shut down")
        return job["id"]

    def _make_job_dict(self, job_id, job_type, *, config, workspace_id,
                       ephemeral, counts_for_badge, pausable,
                       blocks_local_transitions, runtime_warning,
                       singleton_key=None, now=None):
        return {
            "id": job_id,
            "type": job_type,
            "status": "running",
            "started_at": now or datetime.now().isoformat(),
            "finished_at": None,
            "progress": {"current": 0, "total": 0, "current_file": ""},
            "result": None,
            "errors": [],
            "config": config or {},
            "workspace_id": workspace_id,
            "steps": [],
            "ephemeral": ephemeral,
            "counts_for_badge": counts_for_badge,
            "pausable": bool(pausable),
            "blocks_local_transitions": bool(blocks_local_transitions),
            "exclusive_workspace": False,
            "workspace_transfer_batch": None,
            "runtime_warning": runtime_warning,
            "singleton_key": singleton_key,
            "resource_wait_seconds": 0.0,
            "resource_wait_count": 0,
            # Pre-seeded so later writes from worker threads update an
            # existing key instead of inserting a new one: key insertion
            # while a request handler iterates the same dict (jsonify of
            # /api/jobs) raises "dictionary changed size during iteration";
            # same-key updates don't resize the dict.
            "_start_time": time.time(),
            "_ended_at": None,
            "_persisted": False,
            "_fatal_error": None,
        }

    def _register_and_prepare(self, job_type, work_fn, *, config, workspace_id,
                              ephemeral, runtime_warning, counts_for_badge,
                              pausable, blocks_local_transitions,
                              singleton_key=None, workspace_transfer_batch=None):
        """Allocate an id and register the job dict under the runner lock.

        Returns the (job, work_fn) pair the caller should hand to a thread.
        Kept separate from start()/start_singleton so both paths share the
        same registration semantics.
        """
        # Monotonic suffix (shared with enqueue_pipeline) so two same-type
        # starts in the same millisecond can't collide — a collision makes
        # the second registration overwrite the first in _jobs/_events and
        # clobber its history row.
        with self._lock:
            if self._shutting_down:
                raise RuntimeError("JobRunner is shut down")
            self._check_workspace_admission_locked(workspace_id, blocks_local_transitions)
            self._enqueue_counter += 1
            seq = self._enqueue_counter
            job_id = f"{job_type}-{int(time.time() * 1000)}-{seq}"
            job = self._make_job_dict(
                job_id, job_type,
                config=config, workspace_id=workspace_id,
                ephemeral=ephemeral, counts_for_badge=counts_for_badge,
                pausable=pausable,
                blocks_local_transitions=blocks_local_transitions,
                runtime_warning=runtime_warning,
                singleton_key=singleton_key,
            )
            self._prune_finished_jobs()
            job["workspace_transfer_batch"] = workspace_transfer_batch
            self._jobs[job_id] = job
            self._events[job_id] = deque(maxlen=1000)
            self._subscribers[job_id] = []
        return job, work_fn

    @contextmanager
    def workspace_mutation(self, workspace_id, *, exclusive=False):
        """Reserve synchronous mutations against transfers for their full duration."""
        with self._lock:
            self._check_workspace_admission_locked(workspace_id, exclusive=exclusive)
            self._workspace_mutations[workspace_id] = self._workspace_mutations.get(workspace_id, 0) + 1
            if exclusive:
                self._exclusive_workspace_mutations.add(workspace_id)
        try:
            yield
        finally:
            with self._lock:
                self._workspace_mutations[workspace_id] -= 1
                if not self._workspace_mutations[workspace_id]:
                    del self._workspace_mutations[workspace_id]
                if exclusive:
                    self._exclusive_workspace_mutations.discard(workspace_id)

    def wait_for_workspace_transfer(self, job_id):
        """Reserve an automatic transfer batch after its producing jobs finish.

        All batch members register while their parent is still live. The first
        move claims every live sibling, so the reservation survives between
        serialized moves. Other waiting batches do not deadlock each other;
        registration order chooses the next batch once existing work drains.
        """
        with self._pause_condition:
            while True:
                job = self._jobs[job_id]
                if job_id in self._cancelled or self._shutting_down:
                    return False
                workspace_id = job["workspace_id"]
                batch = job.get("workspace_transfer_batch")
                if not batch:
                    raise ValueError("Automatic transfer batch is required")
                live = [j for j in self._jobs.values()
                        if j.get("workspace_id") == workspace_id
                        and j.get("status") in ("running", "queued", "pausing", "paused")
                        and j.get("blocks_local_transitions", True)]
                first_batch = next((j.get("workspace_transfer_batch") for j in live
                                    if j.get("workspace_transfer_batch") and j.get("exclusive_workspace")), None)
                if first_batch is None:
                    first_batch = next((j.get("workspace_transfer_batch") for j in live
                                        if j.get("workspace_transfer_batch")), None)
                busy = any(j.get("workspace_transfer_batch") != batch
                           and (not j.get("workspace_transfer_batch") or j.get("exclusive_workspace"))
                           for j in live)
                if (not busy and first_batch == batch
                        and not self._pipeline_admissions.get(workspace_id)
                        and not self._workspace_mutations.get(workspace_id)
                        and not any(c.get("workspace_id") == workspace_id for c in self._queued_pipelines.values())):
                    for sibling in live:
                        if sibling.get("workspace_transfer_batch") == batch:
                            sibling["exclusive_workspace"] = True
                    return True
                self._pause_condition.wait(timeout=0.1)

    def _check_workspace_admission_locked(self, workspace_id, blocking=True, exclusive=False):
        """Check both sides of a transfer reservation under the registration lock."""
        if not blocking:
            return
        if workspace_id in self._exclusive_workspace_mutations:
            raise WorkspaceBusyError("Wait for the workspace operation to finish before starting another job or change")
        active = [j for j in self._jobs.values()
                  if j.get("workspace_id") == workspace_id
                  and j.get("status") in ("running", "queued", "pausing", "paused")
                  and j.get("blocks_local_transitions", True)]
        if any(j.get("exclusive_workspace") for j in active):
            raise WorkspaceBusyError("Wait for the NAS transfer to finish before starting another job in this workspace")
        if exclusive and (active or self._pipeline_admissions.get(workspace_id)
                          or self._workspace_mutations.get(workspace_id)
                          or any(c.get("workspace_id") == workspace_id for c in self._queued_pipelines.values())):
            raise WorkspaceBusyError("Wait for running jobs or changes in this workspace to finish")

    def _find_singleton_locked(self, job_type, singleton_key):
        """Find an active singleton job by (job_type, singleton_key).

        Must be called with self._lock held. Returns (job_id, job_dict) or
        (None, None). "Active" means running/queued/pausing/paused — a
        completed/failed/cancelled job releases the slot.

        A ``singleton_key`` of None is not a valid key: without this guard,
        every plain ``start()`` job (which stores singleton_key=None) would
        match a lookup for the same job_type with key=None.
        """
        if singleton_key is None:
            return None, None
        for jid, j in self._jobs.items():
            if (
                j.get("type") == job_type
                and j.get("singleton_key") == singleton_key
                and j.get("status") in ("running", "queued", "pausing", "paused")
                and jid not in self._cancelled
            ):
                return jid, j
        return None, None

    def start_singleton(self, job_type, work_fn, *, singleton_key,
                        config=None, workspace_id=None, ephemeral=False,
                        runtime_warning=None, counts_for_badge=True,
                        pausable=False, blocks_local_transitions=True,
                        exclusive_workspace=False):
        """Start a job unless one with the same (type, singleton_key) is active.

        The existence check AND the new-job registration happen under a
        single ``self._lock`` acquisition so two concurrent callers cannot
        both see "no existing job" and both start a worker — a race that
        the earlier check-then-start pattern still had even with an
        explicit ``list_jobs()`` guard.

        Returns ``(job_id, joined_existing, existing_snapshot)``:
        - ``joined_existing`` True means work_fn was NOT invoked; job_id is
          the id of the already-active singleton and ``existing_snapshot``
          is a read-only copy of its state (so callers can inspect its
          stored config to decide whether joining is actually appropriate).
        - ``joined_existing`` False means a fresh job was registered and
          its worker thread was started; ``existing_snapshot`` is None.
        """
        # Compute the joined snapshot / register a new job under one lock
        # acquisition so the check-and-start is genuinely atomic.
        with self._lock:
            if self._shutting_down:
                raise RuntimeError("JobRunner is shut down")
            existing_id, existing = self._find_singleton_locked(
                job_type, singleton_key,
            )
            if existing_id is not None:
                return existing_id, True, self._snapshot_job(existing)
            self._check_workspace_admission_locked(workspace_id, blocks_local_transitions, exclusive_workspace)
            # No active singleton — register inline while still holding the
            # lock so a second caller arriving between our check and our
            # registration cannot slip in and register its own.
            self._enqueue_counter += 1
            seq = self._enqueue_counter
            job_id = f"{job_type}-{int(time.time() * 1000)}-{seq}"
            job = self._make_job_dict(
                job_id, job_type,
                config=config, workspace_id=workspace_id,
                ephemeral=ephemeral, counts_for_badge=counts_for_badge,
                pausable=pausable,
                blocks_local_transitions=blocks_local_transitions,
                runtime_warning=runtime_warning,
                singleton_key=singleton_key,
            )
            job["exclusive_workspace"] = bool(exclusive_workspace)
            self._prune_finished_jobs()
            self._jobs[job_id] = job
            self._events[job_id] = deque(maxlen=1000)
            self._subscribers[job_id] = []

        log.info(
            "Job %s started type=%s singleton_key=%s",
            job_id, job_type, singleton_key,
        )
        if not self._start_worker_thread(job, work_fn):
            self._discard_unstarted_job(job_id)
            raise RuntimeError("JobRunner is shut down")
        return job_id, False, None

    def _prune_finished_jobs(self):
        """Remove completed/failed jobs older than _JOB_RETENTION_SECS.

        Must be called with self._lock held.
        """
        now = time.time()
        to_remove = []
        for jid, j in self._jobs.items():
            if (
                j["status"] in ("completed", "failed", "cancelled")
                and j.get("_ended_at")
                and now - j["_ended_at"] > _JOB_RETENTION_SECS
            ):
                to_remove.append(jid)
        for jid in to_remove:
            del self._jobs[jid]
            self._events.pop(jid, None)
            self._subscribers.pop(jid, None)
            self._cancelled.discard(jid)
            self._pause_requested.discard(jid)
            self._uncancellable.discard(jid)

    def _blocks_sleep(self, job):
        """Whether this job should keep the machine awake while it runs."""
        if job.get("ephemeral"):
            return False
        return job.get("type") not in _NO_SLEEP_ASSERTION_JOB_TYPES

    def _run_job(self, job, work_fn):
        start_time = time.time()
        if self._db_path and not job.get("ephemeral"):
            # Row exists from the first moment so a kill at any point
            # leaves a history entry; checkpoints then keep it current.
            self._record_job_started(job)
            self._ensure_checkpoint_thread()
        holds_sleep_assertion = self._blocks_sleep(job)
        if holds_sleep_assertion:
            # Acquired before the work starts and released in the outer
            # finally, so a crash, cancel, or early return can't strand
            # the inhibitor and hold the machine awake indefinitely.
            self.sleep_blocker.acquire()
        try:
            job_id = job["id"]
            # Bind the job's cancellation probe so resource-ledger waits
            # (including CPU inference acquisitions on the ``cpu_ml`` lane)
            # wake promptly when the job is cancelled instead of blocking
            # until the current holder releases. Without this a cancel
            # request could not preempt a queued classify/detect/mask/embed
            # worker, and if the current native inference stalled the
            # cancelled worker could outlive ``JobRunner.shutdown()``.
            def _job_cancel_probe(_job_id=job_id):
                return self.cancellation_requested(_job_id)
            with bind_resource_owner(job_id), bind_resource_cancel_check(
                _job_cancel_probe,
            ):
                result = work_fn(job)
            # The worker has returned and released its resources. Honor a
            # pause accepted during its final unit before publishing completion.
            # Waiting and finalizing share the pause/cancel lock, so neither
            # request can be accepted and then silently overtaken by completion.
            with self._pause_condition:
                job_id = job["id"]
                while (
                    job.get("pausable")
                    and job_id in self._pause_requested
                    and job_id not in self._cancelled
                    and job_id not in self._uncancellable
                ):
                    if job.get("status") != "paused":
                        self._publish_status_locked(job, "paused")
                    self._pause_condition.wait()
                self._pause_requested.discard(job_id)
                if job_id in self._cancelled:
                    job["status"] = "cancelled"
                    self._cancelled.discard(job_id)
                else:
                    job["status"] = "completed"
                    # A work function can return normally yet still have
                    # failed (e.g. move-folder returns {"moved": 0, "errors":
                    # [...]} when rsync times out). When the result opts into
                    # the convention by carrying an "ok" key, honor it: fold
                    # its errors into the job's tally so error_count is
                    # accurate, and demote a falsy "ok" to "failed" so the
                    # history doesn't read "completed, 0 errors" for a run
                    # that accomplished nothing.
                    if isinstance(result, dict) and "ok" in result:
                        for err in (result.get("errors") or []):
                            err_str = str(err)
                            if err_str not in job["errors"]:
                                job["errors"].append(err_str)
                        if result["ok"] is False:
                            job["status"] = "failed"
                job["result"] = result
            if job["status"] == "failed":
                log.warning(
                    "Job %s reported failure via result: %s",
                    job["id"], "; ".join(job["errors"]) or "(no detail)",
                )
        except Exception as e:
            # Cancellation takes precedence over failure: if the user cancelled
            # while the work function was raising (e.g. a stage crash happened
            # during shutdown), honor the cancel rather than recording a
            # misleading "failed" status.
            with self._lock:
                job_id = job["id"]
                self._pause_requested.discard(job_id)
                if job_id in self._cancelled:
                    job["status"] = "cancelled"
                    self._cancelled.discard(job_id)
                else:
                    job["status"] = "failed"
                    # Avoid duplicating an error the work function already
                    # recorded. Pipelines capture stage errors directly into
                    # job["errors"] and then re-raise with the same message,
                    # so a naive append here would double-count them and
                    # inflate error_count in the persisted history.
                    err_str = str(e)
                    if err_str not in job["errors"]:
                        job["errors"].append(err_str)
            if job["status"] == "failed":
                log.exception("Job %s failed", job["id"])
        finally:
            # First thing in the finally, deliberately: everything below
            # (history persistence, SSE publish, pipeline promotion) can
            # raise, and a stranded inhibitor would hold the machine awake
            # until Vireo exits.
            if holds_sleep_assertion:
                self.sleep_blocker.release()
            resource_timing = get_resource_ledger().owner_timing(
                job["id"], remove=True,
            )
            with self._lock:
                job["resource_wait_seconds"] = resource_timing["wait_seconds"]
                job["resource_wait_count"] = resource_timing["wait_count"]
            elapsed = time.time() - start_time
            job["finished_at"] = datetime.now().isoformat()
            job["_ended_at"] = time.time()
            phase = (job.get("progress") or {}).get("phase")
            failure = None
            if job["status"] == "failed":
                failure = failure_event(
                    job["errors"][-1] if job["errors"] else "Job failed",
                    phase=phase,
                )
            self.push_event(
                job["id"],
                "complete",
                {
                    "job_id": job["id"],
                    "job_type": job["type"],
                    "status": job["status"],
                    "phase": phase,
                    "result": job["result"],
                    "duration": round(elapsed, 1),
                    "resource_wait_seconds": job["resource_wait_seconds"],
                    "resource_wait_count": job["resource_wait_count"],
                    "errors": job["errors"],
                    "failure": failure,
                },
            )
            log.info(
                "Job %s finished type=%s status=%s duration=%.1fs phase=%s",
                job["id"], job["type"], job["status"], elapsed, phase or "-",
            )
            if self._db_path and not job.get("ephemeral"):
                self._persist_job(job, elapsed)
            # Mark the in-memory job dict as fully persisted so test code
            # can synchronize with `job_history` reads. Ephemeral jobs are
            # also flagged so callers waiting on this don't hang.
            job["_persisted"] = True
            # A pipeline slot just opened — let any queued pipeline take
            # its turn. Non-pipeline jobs (scan, thumbnails, etc.) also
            # call through here but the queue check is cheap and the
            # method is a no-op when nothing is queued.
            if job["type"] == "pipeline":
                self._try_promote_queued()

    def _persist_job(self, job, duration):
        """Persist job to history table using a thread-local connection."""
        if not self._db_path:
            return

        import sqlite3

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
            else:
                result_data = {"error": primary_error}

        tree_json = json.dumps(job.get("steps", []))
        summary = self._build_summary(job, result_data)

        params = (
            job["id"],
            job["type"],
            job["status"],
            job["started_at"],
            job["finished_at"],
            round(duration, 1),
            json.dumps(result_data),
            len(job["errors"]),
            json.dumps(job["config"]),
            job.get("workspace_id"),
            tree_json,
            summary,
            job.get("resource_wait_seconds", 0.0),
            job.get("resource_wait_count", 0),
        )

        for attempt in range(3):
            conn = None
            try:
                conn = sqlite3.connect(self._db_path, timeout=30)
                conn.execute(
                    """INSERT OR REPLACE INTO job_history
                       (id, type, status, started_at, finished_at, duration,
                        result, error_count, config, workspace_id, tree, summary,
                        resource_wait_seconds, resource_wait_count)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    params,
                )
                ws_id = job.get("workspace_id")
                if ws_id is not None:
                    # Keep separate 100-row terminal histories for discovery
                    # probes and other jobs, so periodic checks cannot evict
                    # the user's pipeline/import history. Non-terminal rows
                    # stay outside both quotas. Excluding these rows is
                    # load-bearing: a queued pipeline waiting behind a
                    # busy slot can sit in the table for a long time;
                    # if its row got pruned by an unrelated job
                    # completing, the next promotion attempt would see
                    # rowcount==0 on its conditional UPDATE and treat
                    # that as a cancel, silently dropping the run.
                    for discovery in (False, True):
                        conn.execute(
                            """DELETE FROM job_history
                           WHERE workspace_id = ?
                             AND status IN ('completed', 'failed', 'cancelled')
                             AND (type IN ('new_images_walk', 'missing_originals_scan')) = ?
                             AND id NOT IN (
                               SELECT id FROM job_history
                               WHERE workspace_id = ?
                                 AND status IN ('completed', 'failed', 'cancelled')
                                 AND (type IN ('new_images_walk', 'missing_originals_scan')) = ?
                               ORDER BY started_at DESC LIMIT 100
                           )""",
                            (ws_id, discovery, ws_id, discovery),
                        )
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

    def _build_summary(self, job, result=None):
        """Build a one-line summary from job steps or result.

        ``result`` is the payload being persisted (which for a failed job
        already carries the fatal ``error``); it defaults to the job's own
        result so failed runs summarize with their error instead of a bare
        "<Type> failed".
        """
        steps = job.get("steps", [])
        if steps:
            parts = []
            for s in steps:
                if s.get("summary"):
                    parts.append(s["summary"])
            if parts:
                return ", ".join(parts)

        if result is None:
            result = job.get("result")
        if result and isinstance(result, dict):
            described = describe_result(job["type"], result, job.get("config"))
            if described["summary"]:
                return described["summary"]

        # Final fallback: title-case the job type (e.g. "duplicate-scan" →
        # "Duplicate Scan") so the summary line is presentable to the user.
        pretty_type = " ".join(
            w.capitalize() for w in job["type"].replace("_", " ").replace("-", " ").split()
        )
        return f"{pretty_type} {job['status']}"

    def _synthesize_queued_view(self, job_id, ctx):
        """Render a queued pipeline's in-memory context as a job-shaped dict.

        Queued pipelines aren't in ``self._jobs`` yet — they live in
        ``self._queued_pipelines`` until the scheduler promotes them.
        ``get()`` and ``list_jobs()`` both need to surface them in the
        same shape as a live job so callers (UI, SSE, the navbar's
        active-jobs polling) can render and cancel them uniformly.
        """
        return {
            "id": job_id,
            "type": "pipeline",
            "status": "queued",
            "started_at": ctx["started_at"],
            "finished_at": None,
            "progress": {"current": 0, "total": 0, "current_file": ""},
            "result": None,
            "errors": [],
            "config": dict(ctx["config"]),
            "workspace_id": ctx["workspace_id"],
            "steps": [],
            "ephemeral": False,
            "counts_for_badge": True,
            "pausable": False,
            "runtime_warning": ctx.get("runtime_warning"),
            "resource_wait_seconds": 0.0,
            "resource_wait_count": 0,
        }

    @staticmethod
    def _snapshot_job(job):
        """Copy a job dict for callers outside the lock.

        The top-level copy alone isn't enough: handlers jsonify the nested
        progress/steps containers while worker threads mutate them, and a
        key insertion during that iteration raises RuntimeError. Snapshot
        the nested mutable containers under the lock too.
        """
        snap = dict(job)
        snap["progress"] = dict(job.get("progress") or {})
        snap["steps"] = [dict(s) for s in (job.get("steps") or [])]
        snap["errors"] = list(job.get("errors") or [])
        if job.get("status") in ("running", "pausing", "paused"):
            timing = get_resource_ledger().owner_timing(job["id"])
            snap["resource_wait_seconds"] = timing["wait_seconds"]
            snap["resource_wait_count"] = timing["wait_count"]
        elif job.get("status") in ("completed", "failed", "cancelled"):
            JobRunner._attach_result_text(snap)
        return snap

    @staticmethod
    def _attach_result_text(job):
        """Add ``summary`` / ``result_details`` prose derived from ``result``.

        The Jobs page shows these instead of the raw result dict, so a
        finished job reads as "28 photos removed from Vireo, 28 files
        moved to Trash" rather than ``{"deleted": 28, "trashed": 28, ...}``.

        A stored ``summary`` is kept only when it came from step summaries
        or from the job itself (``result["summary"]``). Anything else was
        generated from the result dict, and history rows written before
        the prose describer exist still carry the old ``ok: True,
        deleted: 28`` form, so those are recomputed from the result here
        instead of requiring a backfill.
        """
        result = job.get("result")
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except (json.JSONDecodeError, TypeError):
                result = None
        # A worker that stored a partial result and then raised leaves the
        # in-memory job with the fatal error in _fatal_error/errors, not in
        # result (only the persisted history row gets it merged in). Fold
        # it in here so live snapshots of failed jobs describe the failure
        # too, not just the partial counts.
        if job.get("status") == "failed" and not (
            isinstance(result, dict) and result.get("error")
        ):
            fatal = job.get("_fatal_error") or (job.get("errors") or [None])[0]
            if fatal:
                result = {**result, "error": fatal} if isinstance(result, dict) else {"error": fatal}
        described = describe_result(job.get("type") or "", result, job.get("config"))
        steps = job.get("steps") or job.get("tree") or []
        if isinstance(steps, str):
            try:
                steps = json.loads(steps)
            except (json.JSONDecodeError, TypeError):
                steps = []
        from_steps = any(
            isinstance(s, dict) and s.get("summary") for s in steps
        )
        # Interrupted rows get a summary from the startup sweep that says
        # how far the job got; that beats the plain error text the
        # describer would produce, so treat it as authored too.
        authored = isinstance(result, dict) and bool(
            result.get("summary") or result.get("interrupted")
        )
        if described["summary"] and not (from_steps or authored) or not job.get("summary"):
            job["summary"] = described["summary"]
        job["result_details"] = described["details"]
        if described["error"] and not job.get("error"):
            job["error"] = described["error"]
        return job

    def get(self, job_id):
        """Get a job by id. Returns a copy so callers don't mutate (or race
        with) shared state."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                return self._snapshot_job(job)
            ctx = self._queued_pipelines.get(job_id)
            if ctx is None:
                return None
            return self._synthesize_queued_view(job_id, ctx)

    def list_jobs(self):
        """List all tracked jobs (active, queued, and recently completed).

        Includes synthetic queued-pipeline entries so the navbar and
        /jobs page can render and cancel them; otherwise a queued run
        disappears from the UI between enqueue and promotion. Returns
        snapshots, not live dicts — see _snapshot_job.
        """
        with self._lock:
            jobs = [self._snapshot_job(j) for j in self._jobs.values()]
            for job_id, ctx in self._queued_pipelines.items():
                jobs.append(self._synthesize_queued_view(job_id, ctx))
            return jobs

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
        """
        try:
            ws_id = db._active_workspace_id
            terminal = ("completed", "failed", "cancelled")
            placeholders = ",".join(["?"] * len(terminal))
            if ws_id is not None:
                rows = db.conn.execute(
                    f"SELECT * FROM job_history "
                    f"WHERE workspace_id = ? AND status IN ({placeholders}) "
                    f"ORDER BY started_at DESC LIMIT ?",
                    (ws_id, *terminal, limit),
                ).fetchall()
            else:
                rows = db.conn.execute(
                    f"SELECT * FROM job_history "
                    f"WHERE status IN ({placeholders}) "
                    f"ORDER BY started_at DESC LIMIT ?",
                    (*terminal, limit),
                ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                for field in ("tree", "result", "config", "progress"):
                    if d.get(field) and isinstance(d[field], str):
                        try:
                            d[field] = json.loads(d[field])
                        except (json.JSONDecodeError, TypeError):
                            pass
                self._attach_result_text(d)
                result.append(d)
            return result
        except Exception:
            return []

    def push_event(self, job_id, event_type, data):
        """Push an event to the job's event stream."""
        is_critical = event_type in ("complete", "error")
        with self._lock:
            if event_type == "progress":
                job = self._jobs.get(job_id)
                if job is not None:
                    # Mirror latest progress fields onto job["progress"] so
                    # clients polling /api/jobs or /api/jobs/history see the
                    # current phase/current_file without needing SSE.
                    prog = job.setdefault(
                        "progress",
                        {"current": 0, "total": 0, "current_file": ""},
                    )
                    for key, value in data.items():
                        if key == "steps":
                            continue
                        prog[key] = value
                if job and job.get("steps"):
                    data = dict(data)
                    data["steps"] = [dict(s) for s in job["steps"]]
            event = {"type": event_type, "data": data, "time": time.time()}
            if job_id in self._events:
                self._events[job_id].append(event)
            # Snapshot subscriber list so we can deliver outside the lock
            subscribers = list(self._subscribers.get(job_id, []))

        # Deliver to subscribers outside the lock to avoid blocking
        for q in subscribers:
            if is_critical:
                # Critical events must not be dropped
                try:
                    q.put(event, timeout=5)
                except queue.Full:
                    log.warning(
                        "Failed to deliver critical '%s' event for job %s "
                        "after 5s — subscriber queue full",
                        event_type, job_id,
                    )
            else:
                try:
                    q.put_nowait(event)
                except queue.Full:
                    log.debug(
                        "Dropped '%s' event for job %s — subscriber queue full",
                        event_type, job_id,
                    )

    def _publish_status_locked(self, job, status):
        """Change *job* status and publish its event as one locked action.

        The caller must hold ``self._lock`` (directly or through
        ``self._pause_condition``). Keeping the state change, buffered event,
        and non-blocking subscriber delivery under that same lock prevents
        competing pause, resume, and completion paths from publishing stale
        status events out of order.
        """
        job_id = job["id"]
        job["status"] = status
        event = {
            "type": "status",
            "data": {"job_id": job_id, "status": status},
            "time": time.time(),
        }
        if job_id in self._events:
            self._events[job_id].append(event)
        for subscriber in self._subscribers.get(job_id, []):
            try:
                subscriber.put_nowait(event)
            except queue.Full:
                log.debug(
                    "Dropped 'status' event for job %s — subscriber queue full",
                    job_id,
                )

    def get_events(self, job_id):
        """Get all buffered events for a job."""
        with self._lock:
            return list(self._events.get(job_id, []))

    def subscribe(self, job_id):
        """Subscribe to a job's event stream. Returns a queue."""
        q = queue.Queue(maxsize=200)
        with self._lock:
            if job_id not in self._subscribers:
                self._subscribers[job_id] = []
            self._subscribers[job_id].append(q)
        return q

    def unsubscribe(self, job_id, q):
        """Unsubscribe from a job's event stream."""
        with self._lock:
            subs = self._subscribers.get(job_id, [])
            if q in subs:
                subs.remove(q)

    def set_steps(self, job_id, steps):
        """Define the execution plan for a job.

        Args:
            job_id: job identifier
            steps: list of dicts with at least 'id' and 'label' keys
        """
        full_steps = []
        for s in steps:
            full_steps.append({
                "id": s["id"],
                "label": s["label"],
                "status": "pending",
                "progress": {"current": 0, "total": 0},
                "started_at": None,
                "finished_at": None,
                "duration": None,
                "summary": None,
                "error": None,
                "error_count": 0,
            })
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job["steps"] = full_steps

    def append_step(self, job_id, step_id, label, *, status="completed",
                    summary=None, error=None, error_count=0):
        """Append an already-terminal step to a job's plan after the fact.

        For completion hooks that learn of extra work — or its failure —
        only when the job ends (the after-process NAS-move handoff). A
        stepped job's panel view renders ONLY the step tree: per-step
        summary/error/error_count are its sole error surface (global
        ``job.errors`` shows only for failed jobs), so a hook outcome that
        lives just in ``job.result`` would be invisible to the user.

        Appends under the runner lock — worker-thread mutation of
        ``job["steps"]`` outside it races ``_snapshot_job``'s copy.
        """
        now = datetime.now().isoformat()
        step = {
            "id": step_id,
            "label": label,
            "status": status,
            "progress": {"current": 0, "total": 0},
            "started_at": now,
            "finished_at": now,
            "duration": 0.0,
            "summary": summary,
            "error": error,
            "error_count": error_count,
        }
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job.setdefault("steps", []).append(step)

    def update_step(self, job_id, step_id, **kwargs):
        """Update a step's fields (status, progress, summary, error).

        Automatically sets started_at/finished_at/duration timestamps.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or "steps" not in job:
                return
            for step in job["steps"]:
                if step["id"] == step_id:
                    new_status = kwargs.get("status")
                    if new_status == "running" and step["status"] == "pending":
                        step["started_at"] = datetime.now().isoformat()
                    # "cancelled" is terminal too — classify/pipeline steps
                    # report it on user cancel; without it here those steps
                    # persist with no finished_at/duration.
                    if new_status in ("completed", "failed", "cancelled") and step["started_at"]:
                        step["finished_at"] = datetime.now().isoformat()
                        start = datetime.fromisoformat(step["started_at"])
                        end = datetime.fromisoformat(step["finished_at"])
                        step["duration"] = round((end - start).total_seconds(), 1)
                    for key in (
                        "status", "summary", "error", "error_count",
                        "progress", "current_file", "source_index",
                        "label_source",
                    ):
                        if key in kwargs:
                            step[key] = kwargs[key]
                    break

    def cancel_job(
        self,
        job_id,
        expected_status=None,
        promote_after_cancel=True,
        db_timeout=30.0,
    ):
        """Request cancellation of a running OR queued job.

        For running jobs: the work function should periodically check
        ``runner.is_cancelled(job_id)`` and exit early if True. The
        terminal status flip happens in ``_run_job``.

        For queued pipelines: atomically transition the persisted row
        to ``status='cancelled'`` and remove the in-memory context.
        If promotion already flipped the row to ``running`` but has not
        installed the job in ``_jobs`` yet, preserve the cancellation
        request so the promoted worker exits as cancelled.

        Args:
            job_id: id to cancel.
            expected_status: optional status guard. When set, cancellation only
                proceeds if the latest in-memory state still has this status.
            promote_after_cancel: if True, immediately re-check the queue after
                removing a queued job. Bulk queued cancellation sets this False
                so the whole snapshot can be cancelled before another queued job
                is promoted.
            db_timeout: maximum time to wait for a locked history database.

        Returns True if the job was found and either marked for
        cancellation (running) or transitioned to cancelled (queued).
        """
        emit_complete = False
        retry_promotion = False
        queued_cancel = None
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None and job["status"] in (
                "running", "pausing", "paused",
            ):
                if expected_status and expected_status != job["status"]:
                    return False
                # Stop is a no-op once the job has entered an
                # uninterruptible commit step (e.g. the local-processing
                # archive move). Without this guard a Stop landing after
                # clear_cancellation() but before _run_job's terminal
                # check would re-add the job to _cancelled and record
                # the run as "cancelled" even though the archive
                # already committed to disk.
                if job_id in self._uncancellable:
                    return False
                self._cancelled.add(job_id)
                self._pause_requested.discard(job_id)
                # A paused worker is sleeping on this condition. Wake it so
                # its checkpoint can observe cancellation and return.
                self._pause_condition.notify_all()
                return True
            if job is not None:
                return False
            # Queued case: still in _queued_pipelines, not _jobs yet.
            if job_id in self._queued_pipelines:
                if expected_status and expected_status != "queued":
                    return False
                cancelled_at = datetime.now().isoformat()
                queued_cancel = self._queued_pipelines[job_id]
            else:
                return False

        cancelled = True
        if self._db_path:
            import sqlite3
            conn = sqlite3.connect(self._db_path, timeout=max(0.0, db_timeout))
            try:
                cur = conn.execute(
                    "UPDATE job_history "
                    "SET status='cancelled', finished_at=? "
                    "WHERE id = ? AND status = 'queued'",
                    (cancelled_at, job_id),
                )
                conn.commit()
                cancelled = cur.rowcount == 1
            finally:
                conn.close()

        with self._lock:
            # rowcount==0 means a concurrent promotion beat us — the row is
            # now 'running' and the worker will see the cancellation flag.
            if not cancelled:
                job = self._jobs.get(job_id)
                if (
                    job is not None
                    and job["status"] in ("running", "pausing", "paused")
                    and (
                        expected_status is None
                        or expected_status == job["status"]
                    )
                ):
                    # Same uncancellable guard as the first running-job
                    # branch above; the post-commit Stop race exists on
                    # this path too when promotion beat the queued
                    # cancel.
                    if job_id in self._uncancellable:
                        return False
                    self._cancelled.add(job_id)
                    self._pause_requested.discard(job_id)
                    self._pause_condition.notify_all()
                    return True
                if self._queued_pipelines.get(job_id) is queued_cancel:
                    self._cancelled.add(job_id)
                    return True
                return False
            if self._queued_pipelines.get(job_id) is queued_cancel:
                self._queued_pipelines.pop(job_id, None)
                retry_promotion = True
            emit_complete = True
        if emit_complete:
            self._record_terminal_queued_pipeline(
                job_id, queued_cancel, status="cancelled",
            )
        # Emit the terminal SSE event AFTER releasing the lock — clients
        # subscribed to /api/jobs/<id>/stream while the job was queued
        # need a ``complete`` event with status='cancelled' so they
        # close cleanly. Without this they'd see ``get(job_id) is None``
        # on the next keepalive and report the job as ``expired``.
        if emit_complete:
            self.push_event(
                job_id,
                "complete",
                {
                    "status": "cancelled",
                    "result": None,
                    "duration": 0.0,
                    "errors": [],
                },
            )
        if retry_promotion and promote_after_cancel:
            self._try_promote_queued()
        return True

    def cancel_queued_jobs(self, workspace_id=None):
        """Cancel queued pipelines, optionally scoped to one workspace."""
        with self._lock:
            job_ids = [
                job_id
                for job_id, ctx in self._queued_pipelines.items()
                if workspace_id is None or ctx.get("workspace_id") == workspace_id
            ]
        cancelled = []
        for job_id in job_ids:
            if self.cancel_job(
                job_id,
                promote_after_cancel=False,
            ):
                cancelled.append(job_id)
        if cancelled:
            self._try_promote_queued()
        return cancelled

    def pause_job(self, job_id):
        """Request a cooperative pause at the job's next safe checkpoint.

        The public state moves to ``pausing`` immediately. The worker changes
        it to ``paused`` only at a cooperative checkpoint or after the worker
        returns, so the UI never claims an in-flight ExifTool/copy batch has
        already stopped.
        """
        with self._pause_condition:
            job = self._jobs.get(job_id)
            if (
                job is None
                or job.get("status") != "running"
                or not job.get("pausable")
                or job_id in self._uncancellable
                or job_id in self._cancelled
            ):
                return False
            self._pause_requested.add(job_id)
            self._publish_status_locked(job, "pausing")
        return True

    def pause_requested(self, job_id):
        """Return whether *job_id* currently has a pending pause request.

        Pipeline jobs use this non-blocking probe to coordinate several worker
        threads.  A single worker reaching a checkpoint is not enough to call
        the whole pipeline paused; the pipeline publishes ``paused`` only once
        every active participant has reached a safe boundary.
        """
        with self._lock:
            return (
                job_id in self._pause_requested
                and job_id not in self._cancelled
            )

    def mark_paused(self, job_id):
        """Publish ``paused`` if a live pause request still applies.

        This is separate from :meth:`wait_if_paused` so multi-worker jobs can
        wait for all of their workers before claiming that work has stopped.
        """
        with self._pause_condition:
            job = self._jobs.get(job_id)
            if (
                job is None
                or not job.get("pausable")
                or job_id not in self._pause_requested
                or job_id in self._cancelled
                or job.get("status") not in ("pausing", "paused")
            ):
                return False
            if job.get("status") != "paused":
                self._publish_status_locked(job, "paused")
            return True

    def wait_if_paused(self, job_id, *, publish_paused=False):
        """Wait for a pause request to clear, then report cancellation.

        ``publish_paused`` is appropriate for ordinary single-worker jobs.
        Coordinated jobs pass ``False`` and call :meth:`mark_paused` only after
        all active workers are parked.
        """
        while True:
            with self._pause_condition:
                if job_id in self._cancelled:
                    return True
                job = self._jobs.get(job_id)
                if (
                    job is None
                    or not job.get("pausable")
                    or job_id not in self._pause_requested
                ):
                    return False
                if publish_paused and job.get("status") != "paused":
                    self._publish_status_locked(job, "paused")
                self._pause_condition.wait()

    def resume_job(self, job_id):
        """Resume a pausing or paused job."""
        with self._pause_condition:
            job = self._jobs.get(job_id)
            if (
                job is None
                or job.get("status") not in ("pausing", "paused")
                or job_id not in self._pause_requested
                or job_id in self._cancelled
            ):
                return False
            self._pause_requested.discard(job_id)
            self._publish_status_locked(job, "running")
            self._pause_condition.notify_all()
        return True

    def is_cancelled(self, job_id):
        """Wait through a cooperative pause, then report cancellation.

        Existing scan/import work already calls this method at boundaries
        where it is safe to stop. For jobs explicitly started as pausable,
        those same boundaries are also safe places to sleep without losing
        the work function's in-memory state.
        """
        return self.wait_if_paused(job_id, publish_paused=True)

    def cancellation_requested(self, job_id):
        """Report cancellation without waiting on a pause request.

        Transactional loops use this only after their pause-safe boundary.
        They can still roll back promptly on Cancel without sleeping while a
        database write transaction is open.
        """
        with self._lock:
            return job_id in self._cancelled

    def begin_uncancellable(self, job_id):
        """Honor a pending pause, then atomically enter an uninterruptible phase.

        Returns False when a cancellation is already pending, leaving that flag
        intact so ``_run_job`` can record the job as cancelled. Once this
        returns True, later pause and cancellation requests are ignored until
        the job reaches a terminal state. Pausable callers must invoke this at
        a safe checkpoint before acquiring resources for the commit.
        """
        with self._pause_condition:
            while job_id in self._pause_requested and job_id not in self._cancelled:
                job = self._jobs.get(job_id)
                if job is None:
                    return False
                if job.get("status") != "paused":
                    self._publish_status_locked(job, "paused")
                self._pause_condition.wait()
            if job_id in self._cancelled:
                return False
            self._uncancellable.add(job_id)
            return True

    def clear_cancellation(self, job_id):
        """Consume any pending cancellation flag for ``job_id`` and
        mark the job uncancellable.

        Used by stages that have entered an uninterruptible commit step
        (e.g. the local-processing archive move) where a Stop press
        cannot be honored without leaving a partial published artifact.
        Without consuming the flag, ``_run_job``'s atomic terminal check
        would record the job as "cancelled" even though the commit
        succeeded — confusing the user about whether the archive landed.

        The flag is consumed AND the job is added to ``_uncancellable``
        so a Stop press that lands after this call but before
        ``_run_job`` flips to a terminal status can't re-add the
        cancellation flag and override the committed result.
        """
        with self._lock:
            self._cancelled.discard(job_id)
            self._uncancellable.add(job_id)


class LogBroadcaster(logging.Handler):
    """Captures log records and broadcasts to SSE subscribers.

    Maintains a ring buffer of recent records and a list of subscriber queues.
    """

    def __init__(self, buffer_size=500):
        super().__init__()
        self._buffer = deque(maxlen=buffer_size)
        self._subscribers = []
        self._lock = threading.Lock()
        self._installed = False

    def install(self):
        """Install this handler on the root logger."""
        if not self._installed:
            root = logging.getLogger()
            root.addHandler(self)
            self._installed = True

    def uninstall(self):
        """Remove this handler from the root logger."""
        if self._installed:
            root = logging.getLogger()
            root.removeHandler(self)
            self._installed = False

    def emit(self, record):
        """Called by the logging framework for each log record."""
        message = record.getMessage()
        # Include traceback if present
        if record.exc_info and record.exc_info[1] is not None:
            import traceback

            tb = "".join(traceback.format_exception(*record.exc_info))
            message = message + "\n" + tb
        entry = {
            "time": record.created,
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }
        with self._lock:
            self._buffer.append(entry)
            for q in self._subscribers:
                try:
                    q.put_nowait(entry)
                except queue.Full:
                    pass

    def get_recent(self, count=100):
        """Get the most recent log entries from the ring buffer."""
        with self._lock:
            items = list(self._buffer)
        return items[-count:]

    def subscribe(self):
        """Subscribe to the log stream. Returns a queue."""
        q = queue.Queue(maxsize=500)
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q):
        """Unsubscribe from the log stream."""
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)
