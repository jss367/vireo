"""Background job runner with SSE progress streaming and log broadcasting."""

import json
import logging
import queue
import threading
import time
from collections import deque
from datetime import datetime

from job_contract import failure_event

log = logging.getLogger(__name__)

# How long to keep completed/failed jobs in memory before eviction (seconds)
_JOB_RETENTION_SECS = 3600  # 1 hour

_PROMOTION_RETRY_DELAY_SECS = 0.1


# Maximum number of pipeline jobs allowed to run concurrently. Bumped
# to 2 in Step 6 of the concurrency rollout (Steps 1-3 added the
# ModelCache + GPU/regroup locks that make this safe; Steps 4-5 added
# the queue + UI). The cap is intentionally low: the gains from
# overlapping two pipelines come from one's GPU phase running while
# another's I/O / CPU phases progress. A third pipeline would mostly
# queue at the GPU lock without adding throughput.
# See docs/plans/2026-05-26-pipeline-concurrency-design.md.
SLOT_CAP = 2


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
        self._cancelled = set()  # job ids that have been cancelled
        # job ids past an uninterruptible commit point (e.g. the
        # local-processing archive move). Once a job is in here, any
        # late ``cancel_job`` call is a no-op so the terminal status can
        # no longer be flipped to "cancelled" by a Stop press that
        # landed after the commit finished but before _run_job recorded
        # the result. Cleared by _prune_finished_jobs alongside
        # _cancelled.
        self._uncancellable = set()
        self._db_path = None
        # Pending pipeline work, keyed by job_id. Populated by
        # ``enqueue_pipeline`` and consumed by ``_try_promote_queued``
        # when a slot opens. The work_fn closure can't be persisted
        # cross-process, so a process restart will see queued rows in
        # job_history without a matching entry here; the startup sweep
        # promotes such rows to 'failed'.
        self._queued_pipelines = {}  # job_id -> dict(work_fn, config, ...)
        # Monotonic suffix so two enqueues landing in the same
        # millisecond don't collide on the PRIMARY KEY.
        self._enqueue_counter = 0
        self._promotion_retry_scheduled = False
        if db:
            self._db_path = db.conn.execute("PRAGMA database_list").fetchone()[2]
            self._ensure_history_table(db)
            self._startup_sweep(db)

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
                workspace_id INTEGER
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

    def _startup_sweep(self, db):
        """Reconcile job_history with the fact that we just started.

        On a clean shutdown a job thread either finishes or is cancelled
        and the row is updated accordingly. On a crash or kill, threads
        die without persisting — so rows with ``status='running'`` from
        a prior process are orphans. Mark them ``'failed'``. Queued rows
        from a prior process likewise lose their in-process work closure;
        mark them ``'failed'`` too so they don't linger forever waiting
        for a slot.

        Future PR: rebuild work closures from the ``config`` blob on
        startup so queued runs survive restart. For this PR we just
        clear the rot.
        """
        now = datetime.now().isoformat()
        msg = "Interrupted by Vireo restart"
        for status in ("running", "queued"):
            rows = db.conn.execute(
                "SELECT id FROM job_history WHERE status = ?", (status,),
            ).fetchall()
            if not rows:
                continue
            payload = json.dumps({"error": msg})
            for row in rows:
                db.conn.execute(
                    "UPDATE job_history "
                    "SET status='failed', finished_at=?, result=?, error_count=1 "
                    "WHERE id = ?",
                    (now, payload, row["id"]),
                )
        db.conn.commit()

    def enqueue_pipeline(self, work_fn, config=None, workspace_id=None,
                         runtime_warning=None):
        """Enqueue a pipeline job. Promotes immediately when a slot is free.

        Unlike ``start`` (which spawns a worker thread synchronously),
        ``enqueue_pipeline`` persists the job to ``job_history`` with
        ``status='queued'``, stashes the work closure in-process, and
        then asks the scheduler to promote it. With ``SLOT_CAP=1`` and
        no other pipelines active, promotion happens before this method
        returns and the work thread is already running.

        Returns the job id.
        """
        with self._lock:
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
            active = sum(
                1 for j in self._jobs.values()
                if j["type"] == "pipeline" and j["status"] == "running"
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

        thread = threading.Thread(
            target=self._run_job, args=(job, work_fn), daemon=True,
        )
        log.info("Job %s started type=pipeline", job["id"])
        thread.start()

    def _schedule_promotion_retry_locked(self):
        """Retry queue promotion after a transient DB failure.

        Must be called with self._lock held.
        """
        if self._promotion_retry_scheduled:
            return
        self._promotion_retry_scheduled = True

        def retry():
            time.sleep(_PROMOTION_RETRY_DELAY_SECS)
            with self._lock:
                self._promotion_retry_scheduled = False
            self._try_promote_queued()

        thread = threading.Thread(target=retry, daemon=True)
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
              ephemeral=False, runtime_warning=None, counts_for_badge=True):
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

        Returns:
            job_id string
        """
        # Monotonic suffix (shared with enqueue_pipeline) so two same-type
        # starts in the same millisecond can't collide — a collision makes
        # the second registration overwrite the first in _jobs/_events and
        # clobber its history row.
        with self._lock:
            self._enqueue_counter += 1
            seq = self._enqueue_counter
        job_id = f"{job_type}-{int(time.time() * 1000)}-{seq}"
        now = datetime.now().isoformat()

        job = {
            "id": job_id,
            "type": job_type,
            "status": "running",
            "started_at": now,
            "finished_at": None,
            "progress": {"current": 0, "total": 0, "current_file": ""},
            "result": None,
            "errors": [],
            "config": config or {},
            "workspace_id": workspace_id,
            "steps": [],
            "ephemeral": ephemeral,
            "counts_for_badge": counts_for_badge,
            "runtime_warning": runtime_warning,
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

        with self._lock:
            self._prune_finished_jobs()
            self._jobs[job_id] = job
            self._events[job_id] = deque(maxlen=1000)
            self._subscribers[job_id] = []

        thread = threading.Thread(
            target=self._run_job, args=(job, work_fn), daemon=True
        )
        log.info("Job %s started type=%s", job_id, job_type)
        thread.start()
        return job_id

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
            self._uncancellable.discard(jid)

    def _run_job(self, job, work_fn):
        start_time = time.time()
        try:
            result = work_fn(job)
            # Atomically check cancellation and set final status under the
            # same lock acquisition to prevent a race where cancel_job()
            # returns True but the job still finishes as "completed".
            with self._lock:
                job_id = job["id"]
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
        summary = self._build_summary(job)

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
        )

        for attempt in range(3):
            conn = None
            try:
                conn = sqlite3.connect(self._db_path, timeout=30)
                conn.execute(
                    """INSERT OR REPLACE INTO job_history
                       (id, type, status, started_at, finished_at, duration,
                        result, error_count, config, workspace_id, tree, summary)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    params,
                )
                ws_id = job.get("workspace_id")
                if ws_id is not None:
                    # Retention: keep the 100 most-recent TERMINAL rows
                    # per workspace. Excluding non-terminal rows is
                    # load-bearing: a queued pipeline waiting behind a
                    # busy slot can sit in the table for a long time;
                    # if its row got pruned by an unrelated job
                    # completing, the next promotion attempt would see
                    # rowcount==0 on its conditional UPDATE and treat
                    # that as a cancel, silently dropping the run.
                    conn.execute(
                        """DELETE FROM job_history
                           WHERE workspace_id = ?
                             AND status IN ('completed', 'failed', 'cancelled')
                             AND id NOT IN (
                               SELECT id FROM job_history
                               WHERE workspace_id = ?
                                 AND status IN ('completed', 'failed', 'cancelled')
                               ORDER BY started_at DESC LIMIT 100
                           )""",
                        (ws_id, ws_id),
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

    def _build_summary(self, job):
        """Build a one-line summary from job steps or result."""
        steps = job.get("steps", [])
        if steps:
            parts = []
            for s in steps:
                if s.get("summary"):
                    parts.append(s["summary"])
            if parts:
                return ", ".join(parts)

        result = job.get("result")
        if result and isinstance(result, dict):
            if result.get("summary"):
                return result["summary"]
            parts = []
            for k, v in result.items():
                if isinstance(v, dict):
                    continue
                parts.append(f"{k}: {v}")
            if parts:
                return ", ".join(parts[:3])

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
            "runtime_warning": ctx.get("runtime_warning"),
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
        return snap

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
                for field in ("tree", "result", "config"):
                    if d.get(field) and isinstance(d[field], str):
                        try:
                            d[field] = json.loads(d[field])
                        except (json.JSONDecodeError, TypeError):
                            pass
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
                    for key in ("status", "summary", "error", "error_count", "progress", "current_file"):
                        if key in kwargs:
                            step[key] = kwargs[key]
                    break

    def cancel_job(self, job_id, expected_status=None, promote_after_cancel=True):
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

        Returns True if the job was found and either marked for
        cancellation (running) or transitioned to cancelled (queued).
        """
        emit_complete = False
        retry_promotion = False
        queued_cancel = None
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None and job["status"] == "running":
                if expected_status and expected_status != "running":
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
            conn = sqlite3.connect(self._db_path, timeout=30)
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
                    and job["status"] == "running"
                    and (expected_status is None or expected_status == "running")
                ):
                    # Same uncancellable guard as the first running-job
                    # branch above; the post-commit Stop race exists on
                    # this path too when promotion beat the queued
                    # cancel.
                    if job_id in self._uncancellable:
                        return False
                    self._cancelled.add(job_id)
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

    def is_cancelled(self, job_id):
        """Check whether a job has been marked for cancellation."""
        with self._lock:
            return job_id in self._cancelled

    def begin_uncancellable(self, job_id):
        """Atomically enter an uninterruptible phase if not cancelled.

        Returns False when a cancellation is already pending, leaving that flag
        intact so ``_run_job`` can record the job as cancelled. Once this
        returns True, later ``cancel_job`` calls are ignored until the job
        reaches a terminal state.
        """
        with self._lock:
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
