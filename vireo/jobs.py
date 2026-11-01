"""Background job runner with SSE progress streaming and log broadcasting."""

import logging
import queue
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime

import power
from job_contract import failure_event
from job_runner.checkpoints import JobCheckpointer
from job_runner.control import JobControl
from job_runner.events import EventStreams
from job_runner.history import INTERRUPTED_BY_RESTART as INTERRUPTED_BY_RESTART
from job_runner.history import JobHistory
from job_runner.lineage import JobLineage as JobLineage
from job_runner.lineage import resolve_lineage
from job_runner.pipeline_queue import SLOT_CAP as SLOT_CAP
from job_runner.pipeline_queue import PipelineQueue
from job_runner.reservations import WorkspaceBusyError as WorkspaceBusyError
from job_runner.reservations import WorkspaceReservations
from job_runner.reservations import describe_job as describe_job
from job_runner.reservations import describe_jobs as describe_jobs
from job_runner.reservations import format_duration as format_duration
from job_runner.steps import StepTree
from job_runner.views import (
    new_job_dict,
    queued_lineage,
    snapshot_job,
    synthesize_queued_view,
)
from job_runner.workers import WorkerThreads
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
    "preview-cache-maintenance",
    "label-list-species-ids",
    "exif_search_backfill",
    "verify-models",
})

# Job types that never read or write catalog rows keyed by a photo, folder,
# or file path, and therefore do not block a Work Locally stage/sync/discard.
#
# That block exists for exactly one hazard: a local transition rebases
# ``folders.path`` for a whole tree inside one transaction, so a job that
# captured photo ids and absolute paths before the rebase can commit rows
# against a layout that no longer exists. Jobs listed here operate on files
# under ``~/.vireo`` (models, label lists, embedding caches) or on
# identity-keyed rows that survive a rebase untouched, so blocking them buys
# nothing and strands the user behind a download that has no bearing on the
# folder they want to work on.
#
# Deliberately NOT listed:
#  - ``download-taxonomy``: identity-keyed writes, but it holds one open
#    write transaction across hundreds of thousands of taxa inserts, which a
#    transition's ``BEGIN IMMEDIATE`` would have to wait out or fail against.
#  - anything that scans, imports, moves, renders, classifies or exports
#    photos — all of those carry pre-rebase paths in worker memory.
#
# The default is to block, so a job type added later is safe until someone
# makes the call to list it here.
CATALOG_INDEPENDENT_JOB_TYPES = frozenset({
    "download-darktable",
    "download-megadetector",
    "download-model",
    "fetch-labels",
    "label-list-species-ids",
    "new_images_walk",
    "precompute-embeddings",
    "verify-models",
})


def _resolve_blocking(job_type, blocks_local_transitions):
    """Resolve an explicit flag, falling back to the job type's policy."""
    if blocks_local_transitions is None:
        return job_type_blocks_local_transitions(job_type)
    return bool(blocks_local_transitions)


def job_type_blocks_local_transitions(job_type):
    """Whether ``job_type`` blocks Work Locally transitions by default.

    Call sites may still pass ``blocks_local_transitions`` explicitly — used
    by the pipeline-model downloads, whose job type embeds the model id
    (``download-sam2``) and so cannot be enumerated here.
    """
    return str(job_type) not in CATALOG_INDEPENDENT_JOB_TYPES


# How long to keep completed/failed jobs in memory before eviction (seconds)
_JOB_RETENTION_SECS = 3600  # 1 hour


class JobRunner:
    """Runs long operations in background threads with progress tracking.

    Args:
        db: optional Database instance for persisting job history
    """

    def __init__(self, db=None):
        self._jobs = {}
        self._lock = threading.RLock()
        self._pause_condition = threading.Condition(self._lock)
        # Held while any sleep-blocking job runs; see issue #1397. Resolved
        # through the module rather than bound at import so tests (and any
        # future platform override) can substitute the inhibitor.
        self.sleep_blocker = power.SleepBlocker(
            start_inhibitor=lambda reason: power.start_platform_inhibitor(
                reason
            ),
        )
        # Monotonic suffix so two enqueues landing in the same
        # millisecond don't collide on the PRIMARY KEY.
        self._enqueue_counter = 0
        self._workers = WorkerThreads(self._lock, lambda: self._run_job)
        self._streams = EventStreams(self._lock, self._jobs)
        self._steps = StepTree(self._lock, self._jobs)
        self._control = JobControl(
            self._lock, self._pause_condition, self._jobs, self._streams,
        )
        self._queue = PipelineQueue(
            self._lock, self._jobs, self._workers,
            promote=lambda: self._try_promote_queued(),
        )
        self._reservations = WorkspaceReservations(
            self._lock, self._pause_condition, self._jobs, self._queue,
            self._control, self._workers,
        )
        self._history = JobHistory(db.job_history.catalog_file() if db else None)
        self._checkpoints = JobCheckpointer(
            self._lock, self._jobs, self._history, self._workers,
            checkpoint=lambda: self.checkpoint_live_jobs(),
        )
        if db:
            # job_history is part of the canonical schema (canonical_schema.
            # FORMERLY_LAZY_OBJECTS); the runner only reconciles its rows.
            self._history.startup_sweep(db)

    def _discard_unstarted_job(self, job_id):
        """Remove a job whose worker lost the race with shutdown()."""
        with self._lock:
            self._jobs.pop(job_id, None)
            self._streams.drop_locked(job_id)
            self._control.forget_locked(job_id)

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
            self._workers.begin_shutdown_locked()
            queued_ids = list(self._queue.queued)
            self._control.cancel_live_locked()
            # A paused worker must wake before it can observe cancellation.
            self._pause_condition.notify_all()
            checkpoint_thread = self._checkpoints.thread
        self._checkpoints.stop(checkpoint_thread, deadline)

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
            except Exception:  # broad: shutdown must try every queued job even if one cancel fails
                queued_cancelled = False
                log.exception(
                    "Failed to cancel queued job %s during shutdown", job_id,
                )

        if not self._workers.join(deadline):
            return False
        return queued_cancelled

    def checkpoint_live_jobs(self):
        """Persist a snapshot of every live, non-ephemeral job now.

        See ``JobCheckpointer.checkpoint_live_jobs``. Returns the number of
        rows updated.
        """
        return self._checkpoints.checkpoint_live_jobs()

    def flush_partial_result(self, job, attempts=10, cancel_check=None):
        """Write ``job["partial_result"]`` to its history row now.

        See ``JobCheckpointer.flush_partial_result``.
        """
        return self._checkpoints.flush_partial_result(
            job, attempts=attempts, cancel_check=cancel_check,
        )

    def enqueue_pipeline(self, work_fn, config=None, workspace_id=None,
                         runtime_warning=None, workspace_paths=None, lineage=None):
        """Admit and persist a pipeline, then attempt promotion into a free slot.

        ``workspace_paths`` are the realpath'd folders of every photo the
        pipeline processes; see ``start``. ``lineage`` (a ``JobLineage``)
        names the job this run follows and who caused it; see
        ``job_runner.lineage``.
        """
        resolved = self._resolve_lineage(lineage)
        # Cover the persistence window before the queued context is published.
        # A transfer must not slip between checking admission and publishing it.
        with self._lock:
            self._reservations.check_admission_locked(workspace_id, paths=workspace_paths)
            self._queue.admit_locked(workspace_id)
        try:
            return self._enqueue_pipeline_admitted(
                work_fn, config, workspace_id, runtime_warning, resolved,
            )
        finally:
            self._queue.release_admission(workspace_id)

    def _resolve_lineage(self, lineage):
        """Resolve ``lineage`` to job-dict fields, inheriting the parent's origin.

        The parent is looked up among live and queued jobs first, then in
        history (a chained job usually starts as its parent finishes). Never
        called with the runner lock held: the history read opens SQLite.
        """
        parent_id = lineage.parent_job_id if lineage is not None else None
        parent_origin = None
        if parent_id:
            with self._lock:
                parent = self._jobs.get(parent_id)
                if parent is not None:
                    parent_origin = parent.get("origin")
                elif parent_id in self._queue.queued:
                    parent_origin = queued_lineage(self._queue.queued[parent_id])["origin"]
            if parent_origin is None and self._history.db_path:
                parent_origin = self._history.origin_of(parent_id)
        return resolve_lineage(lineage, parent_origin)

    def _enqueue_pipeline_admitted(self, work_fn, config=None, workspace_id=None,
                                   runtime_warning=None, lineage=None):
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
            if self._workers.shutting_down:
                raise RuntimeError("JobRunner is shut down")
            self._enqueue_counter += 1
            seq = self._enqueue_counter
        job_id = f"pipeline-{int(time.time() * 1000)}-{seq}"
        now_iso = datetime.now().isoformat()
        config_blob = config or {}

        lineage = lineage or resolve_lineage(None, None)
        if self._history.db_path:
            self._history.insert_queued_row(
                job_id, now_iso, config_blob, workspace_id, lineage,
            )

        with self._lock:
            self._queue.add_locked(job_id, {
                "work_fn": work_fn,
                "config": config_blob,
                "workspace_id": workspace_id,
                "runtime_warning": runtime_warning,
                "started_at": now_iso,
                **lineage,
            })
            shutdown_raced = self._workers.shutting_down

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
            claimed = self._queue.claim_locked()
            if claimed is None:
                return
            job_id, ctx = claimed

        promoted = True
        try:
            # Atomic queued->running flip. If a concurrent cancel beat us
            # to it, rowcount is 0 and the row stays in its final state.
            if self._history.db_path:
                promoted = self._history.flip_queued_row_to_running(job_id)
        except Exception:  # broad: any promotion failure unclaims the job for a retry instead of stranding it
            with self._lock:
                if self._queue.unclaim_locked(job_id, ctx):
                    self._queue.schedule_retry_locked()
            log.exception("Failed to promote queued pipeline %s", job_id)
            return

        retry_promotion = False
        record_terminal = False
        with self._lock:
            if not self._queue.unclaim_locked(job_id, ctx):
                retry_promotion = True
            else:
                if not promoted:
                    # The row was modified elsewhere (cancelled).
                    self._queue.discard_locked(job_id)
                    record_terminal = True
                    retry_promotion = True
                else:
                    job, work_fn = self._install_promoted_pipeline_locked(
                        job_id, ctx,
                    )

        if record_terminal:
            self._record_terminal_queued_pipeline(job_id, ctx, status="cancelled")
            self.push_event(
                job_id,
                "complete",
                {
                    "status": "cancelled",
                    "result": {"cancelled": True, "never_started": True},
                    "duration": 0.0,
                    "errors": [],
                },
            )
        if retry_promotion:
            self._try_promote_queued()
            return

        log.info("Job %s started type=pipeline", job["id"])
        if not self._workers.start(job, work_fn):
            self._cancel_unstarted_promoted_pipeline(job)

    def _install_promoted_pipeline_locked(self, job_id, ctx):
        """Move a promoted pipeline from its queue context into the live jobs.

        Returns ``(job, work_fn)``. Must be called with self._lock held.
        """
        # Move from queue context into the live jobs dict.
        self._queue.remove_locked(job_id)
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
            "pause_unavailable_reason": None,
            "cancellable": True,
            "control_unavailable_reason": None,
            "runtime_warning": ctx["runtime_warning"],
            **queued_lineage(ctx),
            # Pre-seeded for iteration safety — see start().
            "_start_time": time.time(),
            "_ended_at": None,
            "_persisted": False,
            "_fatal_error": None,
        }
        self._prune_finished_jobs()
        self._jobs[job_id] = job
        self._streams.open_promoted_locked(job_id)
        work_fn = ctx["work_fn"]
        if job_id in self._control.cancelled:
            def work_fn(job):
                return {"cancelled": True, "never_started": True}
        return job, work_fn

    def _cancel_unstarted_promoted_pipeline(self, job):
        """Record a promoted pipeline whose worker thread never started.

        shutdown() marked the installed job for cancellation while it
        sat between promotion and thread start. Record the terminal row
        here because this promoted pipeline was already persisted as
        running before its in-memory worker registration existed.
        """
        now = datetime.now().isoformat()
        with self._lock:
            job["status"] = "cancelled"
            job["result"] = {"cancelled": True, "never_started": True}
            job["finished_at"] = now
            job["_ended_at"] = time.time()
            self._control.consume_cancel_locked(job["id"])
        self.push_event(job["id"], "complete", {
            "job_id": job["id"],
            "job_type": job["type"],
            "status": "cancelled",
            "result": {"cancelled": True, "never_started": True},
            "duration": 0.0,
            "errors": [],
        })
        if self._history.db_path:
            self._persist_final(job, 0.0)
        job["_persisted"] = True

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
            "result": {"cancelled": True, "never_started": True},
            "errors": [],
            "config": ctx["config"],
            "workspace_id": ctx["workspace_id"],
            "steps": [],
            "ephemeral": False,
            "counts_for_badge": True,
            "pausable": False,
            "runtime_warning": ctx.get("runtime_warning"),
            **queued_lineage(ctx),
            "_ended_at": time.time(),
            "_persisted": True,
        }
        with self._lock:
            self._prune_finished_jobs()
            self._jobs[job_id] = job
            self._streams.ensure_locked(job_id)

    @contextmanager
    def admission_guard(self):
        """Keep a cheap admission check and registration atomic with finalization.

        The condition uses the same reentrant lock as start/list_jobs and
        terminal status transitions. Never perform file I/O inside this guard.
        """
        with self._pause_condition:
            yield

    def start(self, job_type, work_fn, config=None, workspace_id=None,
              ephemeral=False, runtime_warning=None, counts_for_badge=True,
              pausable=None, blocks_local_transitions=None,
              workspace_transfer_batch=None, workspace_paths=None, lineage=None):
        """Start a background job.

        Args:
            job_type: string like 'scan', 'thumbnails', 'import', 'sync'
            work_fn: callable(job_dict) that does the work. Can update
                     job['progress'] and return a result dict.
            config: optional dict of job configuration (persisted to history)
            workspace_id: optional workspace id to associate with this job
            ephemeral: if True, the job runs and streams events normally but
                       is never written to ``job_history`` and holds no
                       sleep assertion. Not for Vireo's own background work:
                       start that with ``lineage=JobLineage.system(...)``,
                       whose row survives a crash and whose no-op runs are
                       dropped from history instead (see
                       ``JobHistory.system_disposition``).
            runtime_warning: optional user-facing warning metadata to expose
                       while the job is running.
            counts_for_badge: if False, the job remains visible in job lists
                       but does not contribute to app/Dock attention badges.
            pausable: defaults to the job type's audited pause policy.
                      Unknown types must explicitly declare this option.
                      If True, pause_job() may suspend the worker the next
                      time it calls is_cancelled(). Only set this for work
                      that checks cancellation at safe boundaries.
            blocks_local_transitions: if False, Work Locally stage/sync/discard
                      actions may proceed while this job runs. Reserve this
                      for jobs that carry no pre-rebase photo/folder paths.
                      Defaults to the job type's policy — see
                      CATALOG_INDEPENDENT_JOB_TYPES — so the answer to "does
                      this block Work Locally?" lives in one place rather
                      than being re-decided at every launcher.
            workspace_transfer_batch: identifies automatic moves whose workers
                      call wait_for_workspace_transfer before touching originals.
            workspace_paths: the realpath'd folder trees the job reads and
                      writes. A job that declares them may run beside a NAS
                      transfer whose trees they do not overlap; one that does
                      not is refused while any transfer runs in its workspace.
                      Only declare trees that bound everything the job
                      touches, its catalog writes included.
            lineage: a ``JobLineage``: whether Vireo started this on its own
                      (and why), or which job it follows. Default: the
                      user's own job.

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
            workspace_paths=workspace_paths,
            lineage=lineage,
        )
        log.info(
            "Job %s started type=%s origin=%s", job["id"], job_type, job["origin"],
        )
        if not self._workers.start(job, work_fn):
            self._discard_unstarted_job(job["id"])
            raise RuntimeError("JobRunner is shut down")
        return job["id"]

    def _register_and_prepare(self, job_type, work_fn, *, config, workspace_id,
                              ephemeral, runtime_warning, counts_for_badge,
                              pausable, blocks_local_transitions,
                              workspace_transfer_batch=None,
                              workspace_paths=None, lineage=None):
        """Allocate an id and register the job dict under the runner lock.

        Returns the (job, work_fn) pair the caller should hand to a thread.
        ``start_singleton`` registers inline instead, under the same lock
        acquisition as its existence check.
        """
        # Monotonic suffix (shared with enqueue_pipeline) so two same-type
        # starts in the same millisecond can't collide — a collision makes
        # the second registration overwrite the first in _jobs/_events and
        # clobber its history row.
        blocks_local_transitions = _resolve_blocking(
            job_type, blocks_local_transitions,
        )
        resolved = self._resolve_lineage(lineage)
        with self._lock:
            if self._workers.shutting_down:
                raise RuntimeError("JobRunner is shut down")
            # An automatic transfer waits for the running one before it
            # touches anything, so it registers now and queues behind it.
            self._reservations.check_admission_locked(
                workspace_id, blocks_local_transitions, paths=workspace_paths,
                defer_transfer_check=workspace_transfer_batch is not None,
            )
            self._enqueue_counter += 1
            seq = self._enqueue_counter
            job_id = f"{job_type}-{int(time.time() * 1000)}-{seq}"
            job = new_job_dict(
                job_id, job_type,
                config=config, workspace_id=workspace_id,
                ephemeral=ephemeral, counts_for_badge=counts_for_badge,
                pausable=pausable,
                blocks_local_transitions=blocks_local_transitions,
                runtime_warning=runtime_warning,
                workspace_paths=workspace_paths,
                lineage=resolved,
            )
            self._prune_finished_jobs()
            job["workspace_transfer_batch"] = workspace_transfer_batch
            self._jobs[job_id] = job
            self._streams.open_locked(job_id)
        return job, work_fn

    def workspace_mutation(self, workspace_id, *, exclusive=False, label=None,
                           defer_transfer_check=False):
        """Reserve synchronous mutations against transfers for their full duration.

        A context manager; see ``WorkspaceReservations.workspace_mutation``.
        """
        return self._reservations.workspace_mutation(
            workspace_id, exclusive=exclusive, label=label,
            defer_transfer_check=defer_transfer_check,
        )

    def wait_for_workspace_transfer(self, job_id):
        """Reserve an automatic transfer batch after its producing jobs finish.

        See ``WorkspaceReservations.wait_for_workspace_transfer``.
        """
        return self._reservations.wait_for_workspace_transfer(job_id)

    def storage_transfer_active(self):
        """Whether a transfer currently needs priority over background I/O.

        Include queued and paused transfers: their intermediate folder layout
        must not be mistaken for newly discovered or missing originals.
        """
        with self._lock:
            return any(
                (job.get("exclusive_workspace") or job.get("type") in (
                    "send-to-nas", "move-folder", "move-photos",
                ))
                and job.get("status") in ("queued", "running", "pausing", "paused")
                for job in self._jobs.values()
            )

    def wait_for_background_io(self, job_id=None):
        """Yield at a background worker's safe point until transfers finish.

        Only independent discovery and global housekeeping may park here:
        a transfer must never be waiting for the caller's workspace reservation.
        No filesystem or cache locks should be held across this wait.
        """
        with self._pause_condition:
            job = self._jobs.get(job_id)
            previous_phase = None
            waiting = False
            try:
                while self.storage_transfer_active():
                    if self._workers.shutting_down or job_id in self._control.cancelled:
                        raise RuntimeError("Background work cancelled")
                    if job is not None and not waiting:
                        previous_phase = job["progress"].get("phase", "")
                        self.push_event(job_id, "progress", {
                            "phase": "Waiting for photo transfer",
                        })
                        waiting = True
                    self._pause_condition.wait(timeout=0.25)
                if self._workers.shutting_down or job_id in self._control.cancelled:
                    raise RuntimeError("Background work cancelled")
            finally:
                if waiting:
                    self.push_event(job_id, "progress", {"phase": previous_phase})

    def _find_singleton_locked(self, job_type, singleton_key):
        """Find an active singleton job by (job_type, singleton_key).

        Must be called with self._lock held. Returns (job_id, job_dict) or
        (None, None). "Active" means running/queued/pausing/paused — a
        completed/failed/cancelled job releases the slot. A cancellation
        request alone does not: its worker may still be reading or writing.

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
            ):
                return jid, j
        return None, None

    def start_singleton(self, job_type, work_fn, *, singleton_key,
                        config=None, workspace_id=None,
                        runtime_warning=None, counts_for_badge=True,
                        pausable=None, blocks_local_transitions=None,
                        exclusive_workspace=False, workspace_paths=None,
                        lineage=None):
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

        An ``exclusive_workspace`` job is a NAS transfer; its
        ``workspace_paths`` are the trees it moves, and only jobs whose own
        declared trees miss them may start while it runs (see ``start``).
        """
        resolved = self._resolve_lineage(lineage)
        # Compute the joined snapshot / register a new job under one lock
        # acquisition so the check-and-start is genuinely atomic.
        with self._lock:
            if self._workers.shutting_down:
                raise RuntimeError("JobRunner is shut down")
            existing_id, existing = self._find_singleton_locked(
                job_type, singleton_key,
            )
            if existing_id is not None:
                return existing_id, True, snapshot_job(existing)
            blocks_local_transitions = _resolve_blocking(
                job_type, blocks_local_transitions,
            )
            self._reservations.check_admission_locked(
                workspace_id, blocks_local_transitions, exclusive_workspace,
                paths=None if exclusive_workspace else workspace_paths,
            )
            # No active singleton — register inline while still holding the
            # lock so a second caller arriving between our check and our
            # registration cannot slip in and register its own.
            self._enqueue_counter += 1
            seq = self._enqueue_counter
            job_id = f"{job_type}-{int(time.time() * 1000)}-{seq}"
            job = new_job_dict(
                job_id, job_type,
                config=config, workspace_id=workspace_id,
                ephemeral=False, counts_for_badge=counts_for_badge,
                pausable=pausable,
                blocks_local_transitions=blocks_local_transitions,
                runtime_warning=runtime_warning,
                singleton_key=singleton_key,
                workspace_paths=workspace_paths,
                lineage=resolved,
            )
            job["exclusive_workspace"] = bool(exclusive_workspace)
            self._prune_finished_jobs()
            self._jobs[job_id] = job
            self._streams.open_locked(job_id)

        log.info(
            "Job %s started type=%s singleton_key=%s",
            job_id, job_type, singleton_key,
        )
        if not self._workers.start(job, work_fn):
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
            self._streams.drop_locked(jid)
            self._control.forget_locked(jid)

    def _blocks_sleep(self, job):
        """Whether this job should keep the machine awake while it runs."""
        if job.get("ephemeral"):
            return False
        return job.get("type") not in _NO_SLEEP_ASSERTION_JOB_TYPES

    def _run_job(self, job, work_fn):
        start_time = time.time()
        if self._history.db_path and not job.get("ephemeral"):
            # Row exists from the first moment so a kill at any point
            # leaves a history entry; checkpoints then keep it current.
            self._history.record_job_started(job)
            self._checkpoints.ensure_thread()
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
            self._control.settle_returned(job, result)
            if job["status"] == "failed":
                log.warning(
                    "Job %s reported failure via result: %s",
                    job["id"], "; ".join(job["errors"]) or "(no detail)",
                )
        except Exception as e:  # broad: worker-thread top level; every failure becomes the job's failed status
            # Cancellation takes precedence over failure: if the user cancelled
            # while the work function was raising (e.g. a stage crash happened
            # during shutdown), honor the cancel rather than recording a
            # misleading "failed" status.
            with self._lock:
                self._control.settle_raised_locked(job, e)
            if job["status"] == "failed":
                log.exception("Job %s failed", job["id"])
        finally:
            # First thing in the finally, deliberately: everything below
            # (history persistence, SSE publish, pipeline promotion) can
            # raise, and a stranded inhibitor would hold the machine awake
            # until Vireo exits.
            if holds_sleep_assertion:
                self.sleep_blocker.release()
            self._publish_job_completion(job, start_time)

    def _publish_job_completion(self, job, start_time):
        """Stamp a finished job, publish its completion, and persist it."""
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
        if self._history.db_path and not job.get("ephemeral"):
            self._persist_final(job, elapsed)
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

    def _persist_final(self, job, duration):
        """Write a finished job's row; a system job's under ``write_lock``.

        A checkpoint snapshots the live jobs and writes them later, with
        ``INSERT OR IGNORE`` for a row that is missing. A system job's final
        write may delete its own row (a no-op run); between those two steps
        that would let the stale snapshot re-create it as ``running``, and
        the next startup would fail a run that succeeded. Holding
        ``write_lock`` makes the two serial: a checkpoint either finishes
        before the final write, or snapshots after the job has left the live
        statuses. The user's jobs always keep their row, so their final
        writes stay off the lock and never queue behind one another.
        """
        if job.get("origin") != "system":
            self._history.persist_job(job, duration)
            return
        with self._checkpoints.write_lock:
            self._history.persist_job(job, duration)

    def get(self, job_id):
        """Get a job by id. Returns a copy so callers don't mutate (or race
        with) shared state."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                return snapshot_job(job)
            ctx = self._queue.queued.get(job_id)
            if ctx is None:
                return None
            return synthesize_queued_view(job_id, ctx)

    def list_jobs(self):
        """List all tracked jobs (active, queued, and recently completed).

        Includes synthetic queued-pipeline entries so the navbar and
        /jobs page can render and cancel them; otherwise a queued run
        disappears from the UI between enqueue and promotion. Returns
        snapshots, not live dicts — see job_runner.views.snapshot_job.
        """
        with self._lock:
            jobs = [snapshot_job(j) for j in self._jobs.values()]
            for job_id, ctx in self._queue.queued.items():
                jobs.append(synthesize_queued_view(job_id, ctx))
            return jobs

    def get_history(self, db, limit=10, origin=None):
        """Recent terminal job history rows; see ``JobHistory.get_history``.

        Args:
            db: Database instance (must be from the calling thread)
            limit: max number of rows
        """
        return self._history.get_history(db, limit, origin)

    def push_event(self, job_id, event_type, data):
        """Push an event to the job's event stream."""
        self._streams.push(job_id, event_type, data)

    def get_events(self, job_id):
        """Get all buffered events for a job."""
        return self._streams.get_events(job_id)

    def subscribe(self, job_id):
        """Subscribe to a job's event stream. Returns a queue."""
        return self._streams.subscribe(job_id)

    def unsubscribe(self, job_id, q):
        """Unsubscribe from a job's event stream."""
        self._streams.unsubscribe(job_id, q)

    def set_steps(self, job_id, steps):
        """Define the execution plan for a job; see ``StepTree.set_steps``."""
        self._steps.set_steps(job_id, steps)

    def append_step(self, job_id, step_id, label, *, status="completed",
                    summary=None, error=None, error_count=0):
        """Append an already-terminal step; see ``StepTree.append_step``."""
        self._steps.append_step(
            job_id, step_id, label, status=status,
            summary=summary, error=error, error_count=error_count,
        )

    def update_step(self, job_id, step_id, **kwargs):
        """Update a step's fields; see ``StepTree.update_step``."""
        self._steps.update_step(job_id, step_id, **kwargs)

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
                return self._control.request_running_cancel_locked(job_id)
            if job is not None:
                return False
            # Queued case: still in the pipeline queue, not _jobs yet.
            if job_id in self._queue.queued:
                if expected_status and expected_status != "queued":
                    return False
                cancelled_at = datetime.now().isoformat()
                queued_cancel = self._queue.queued[job_id]
            else:
                return False

        cancelled = True
        if self._history.db_path:
            cancelled = self._history.cancel_queued_history_row(
                job_id, cancelled_at, db_timeout,
            )

        with self._lock:
            # rowcount==0 means a concurrent promotion beat us — the row is
            # now 'running' and the worker will see the cancellation flag.
            if not cancelled:
                return self._cancel_after_lost_queue_race_locked(
                    job_id, expected_status, queued_cancel,
                )
            if self._queue.is_current_locked(job_id, queued_cancel):
                self._queue.discard_locked(job_id)
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
                    "result": {"cancelled": True, "never_started": True},
                    "duration": 0.0,
                    "errors": [],
                },
            )
        if retry_promotion and promote_after_cancel:
            self._try_promote_queued()
        return True

    def _cancel_after_lost_queue_race_locked(
        self, job_id, expected_status, queued_cancel,
    ):
        """Cancel a queued job whose row a concurrent promotion already took.

        Must be called with self._lock held.
        """
        job = self._jobs.get(job_id)
        if (
            job is not None
            and job["status"] in ("running", "pausing", "paused")
            and (
                expected_status is None
                or expected_status == job["status"]
            )
        ):
            # Same uncancellable guard as cancel_job's running-job
            # branch; the post-commit Stop race exists on
            # this path too when promotion beat the queued
            # cancel.
            return self._control.request_running_cancel_locked(job_id)
        if self._queue.is_current_locked(job_id, queued_cancel):
            self._control.flag_cancelled_locked(job_id)
            return True
        return False

    def cancel_queued_jobs(self, workspace_id=None):
        """Cancel queued pipelines, optionally scoped to one workspace."""
        with self._lock:
            job_ids = [
                job_id
                for job_id, ctx in self._queue.queued.items()
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
        """Request a cooperative pause; see ``JobControl.pause_job``."""
        return self._control.pause_job(job_id)

    def pause_requested(self, job_id):
        """Whether *job_id* has a pending pause; see ``JobControl.is_pause_requested``."""
        return self._control.is_pause_requested(job_id)

    def mark_paused(self, job_id):
        """Publish ``paused`` if a pause still applies; see ``JobControl.mark_paused``."""
        return self._control.mark_paused(job_id)

    def wait_if_paused(self, job_id, *, publish_paused=False):
        """Wait out a pause, then report cancellation; see ``JobControl.wait_if_paused``."""
        return self._control.wait_if_paused(job_id, publish_paused=publish_paused)

    def pause_elapsed_seconds(self, job_id):
        """Cumulative pause duration for *job_id*; see ``JobControl.pause_elapsed_seconds``."""
        return self._control.pause_elapsed_seconds(job_id)

    def resume_job(self, job_id):
        """Resume a pausing or paused job."""
        return self._control.resume_job(job_id)

    def is_cancelled(self, job_id):
        """Wait through a cooperative pause, then report cancellation.

        Existing scan/import work already calls this method at boundaries
        where it is safe to stop. For jobs explicitly started as pausable,
        those same boundaries are also safe places to sleep without losing
        the work function's in-memory state.
        """
        return self.wait_if_paused(job_id, publish_paused=True)

    def cancellation_requested(self, job_id):
        """Report cancellation without waiting; see ``JobControl.cancellation_requested``."""
        return self._control.cancellation_requested(job_id)

    def begin_uncancellable(self, job_id, *, wait_for_pause=True):
        """Honor a pending pause, then enter an uninterruptible phase.

        See ``JobControl.begin_uncancellable``.
        """
        return self._control.begin_uncancellable(job_id, wait_for_pause=wait_for_pause)

    def clear_cancellation(self, job_id):
        """Consume a pending cancel and mark the job uncancellable.

        See ``JobControl.clear_cancellation``.
        """
        self._control.clear_cancellation(job_id)


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
