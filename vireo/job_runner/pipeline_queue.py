"""Pipelines waiting for a slot, and the in-flight enqueues not yet published."""

import time

# Maximum number of pipeline jobs allowed to run concurrently. Bumped
# to 2 in Step 6 of the concurrency rollout (Steps 1-3 added the
# ModelCache + GPU/regroup locks that make this safe; Steps 4-5 added
# the queue + UI). The cap is intentionally low: the gains from
# overlapping two pipelines come from one's GPU phase running while
# another's I/O / CPU phases progress. A third pipeline would mostly
# queue at the GPU lock without adding throughput.
# See docs/plans/2026-05-26-pipeline-concurrency-design.md.
SLOT_CAP = 2

_PROMOTION_RETRY_DELAY_SECS = 0.1


class PipelineQueue:
    """Owns the queued pipeline contexts, admissions and the retry flag.

    ``queued``, ``admissions`` and ``retry_scheduled`` are guarded by the
    runner lock; every ``*_locked`` method must be called with it held.
    The runner moves a context out of here and into its job registry when
    it promotes one; this class only decides which context goes next.
    """

    def __init__(self, lock, jobs, workers, promote):
        self._lock = lock
        self._jobs = jobs
        self._workers = workers
        self._promote = promote
        # Pending pipeline work, keyed by job_id. Populated by
        # ``enqueue_pipeline`` and consumed by ``_try_promote_queued``
        # when a slot opens. The work_fn closure can't be persisted
        # cross-process, so a process restart will see queued rows in
        # job_history without a matching entry here; the startup sweep
        # promotes such rows to 'failed'.
        self.queued = {}  # job_id -> dict(work_fn, config, ...)
        self.admissions = {}  # workspace -> enqueues persisting outside the lock
        self.retry_scheduled = False

    def admit_locked(self, workspace_id):
        """Count an enqueue whose queued row is about to be persisted."""
        self.admissions[workspace_id] = self.admissions.get(workspace_id, 0) + 1

    def release_admission(self, workspace_id):
        """Stop counting an enqueue once its context is published (or failed)."""
        with self._lock:
            self.admissions[workspace_id] -= 1
            if not self.admissions[workspace_id]:
                del self.admissions[workspace_id]

    def add_locked(self, job_id, ctx):
        """Publish a queued pipeline's context."""
        self.queued[job_id] = ctx

    def remove_locked(self, job_id):
        """Remove a promoted pipeline's context (KeyError if absent)."""
        del self.queued[job_id]

    def discard_locked(self, job_id):
        """Remove ``job_id``'s context if it is still here."""
        self.queued.pop(job_id, None)

    def is_current_locked(self, job_id, ctx):
        """Whether ``ctx`` is still the published context for ``job_id``."""
        return self.queued.get(job_id) is ctx

    def unclaim_locked(self, job_id, ctx):
        """Clear ``ctx``'s promoting mark; False if ``ctx`` is no longer queued."""
        if self.queued.get(job_id) is not ctx:
            return False
        ctx.pop("_promoting", None)
        return True

    def claim_locked(self):
        """Mark the oldest queued pipeline as promoting if a slot is open.

        Returns ``(job_id, ctx)``, or None when shutting down, no slot is
        free, or nothing is waiting. Must be called with self._lock held.
        """
        if self._workers.shutting_down:
            return None
        active = sum(
            1 for j in self._jobs.values()
            if (
                j["type"] == "pipeline"
                and j["status"] in ("running", "pausing", "paused")
            )
        )
        if active >= SLOT_CAP:
            return None
        promoting = sum(
            1 for ctx in self.queued.values()
            if ctx.get("_promoting")
        )
        if active + promoting >= SLOT_CAP:
            return None
        candidates = sorted(
            (
                item for item in self.queued.items()
                if not item[1].get("_promoting")
            ),
            key=lambda kv: kv[1]["started_at"],
        )
        if not candidates:
            return None
        job_id, ctx = candidates[0]
        ctx["_promoting"] = True
        return job_id, ctx

    def schedule_retry_locked(self):
        """Retry queue promotion after a transient DB failure.

        Must be called with self._lock held.
        """
        if self.retry_scheduled or self._workers.shutting_down:
            return
        self.retry_scheduled = True

        def retry():
            time.sleep(_PROMOTION_RETRY_DELAY_SECS)
            with self._lock:
                self.retry_scheduled = False
                if self._workers.shutting_down:
                    return
            self._promote()

        self._workers.start_promotion_retry_locked(retry)
