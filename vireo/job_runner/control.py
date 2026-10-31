"""Cooperative cancel and pause: the flags workers poll and the waits they park in."""

import time

from job_runner.outcome import apply_result_verdict


class JobControl:
    """Owns the cancel, pause and uncancellable flags of the runner's jobs.

    ``cancelled``, ``pause_requested`` and ``uncancellable`` (sets of job
    ids) are guarded by the runner lock; waits park on ``_pause_condition``,
    which is built on that same lock. Reads the runner's job registry and
    publishes status changes through ``EventStreams``.
    """

    def __init__(self, lock, pause_condition, jobs, streams, clock=None):
        self._lock = lock
        self._pause_condition = pause_condition
        self._jobs = jobs
        self._streams = streams
        self._clock = clock or time.monotonic
        self.cancelled = set()  # job ids that have been cancelled
        # Cooperative pause requests. Pausable work reaches these through
        # is_cancelled(), which doubles as its existing safe-point callback.
        # Keeping the wait in the runner means scan/import call sites do not
        # need a second callback threaded through every layer.
        self.pause_requested = set()
        # job ids past an uninterruptible commit point (e.g. the
        # local-processing archive move). Once a job is in here, any
        # late ``cancel_job`` call is a no-op so the terminal status can
        # no longer be flipped to "cancelled" by a Stop press that
        # landed after the commit finished but before _run_job recorded
        # the result. Cleared by _prune_finished_jobs alongside
        # cancelled.
        self.uncancellable = set()
        # Cumulative seconds each live job has spent parked inside
        # ``wait_if_paused``. ETA estimators subtract this from their
        # elapsed-time measurements so a pause does not inflate the next
        # computed rate: without it, an item that straddles a one-hour pause
        # would present as taking an hour, poisoning the per-item rate for
        # the rest of the pass. Cleared alongside the other flags in
        # ``forget_locked``.
        self._pause_elapsed = {}

    # ------------------------------------------------------------------
    # Lifecycle bookkeeping (runner lock held)
    # ------------------------------------------------------------------

    def forget_locked(self, job_id):
        """Drop every flag for ``job_id``. Must be called with the lock held."""
        self.cancelled.discard(job_id)
        self.pause_requested.discard(job_id)
        self.uncancellable.discard(job_id)
        self._pause_elapsed.pop(job_id, None)

    def cancel_live_locked(self):
        """Flag every live, cancellable job for cancellation (shutdown).

        Must be called with the lock held. The caller wakes paused workers.
        """
        for job_id, job in self._jobs.items():
            if (
                job.get("status") in ("running", "pausing", "paused")
                and job_id not in self.uncancellable
            ):
                self.cancelled.add(job_id)
                self.pause_requested.discard(job_id)

    def flag_cancelled_locked(self, job_id):
        """Record a cancel for a job not yet installed as live.

        Must be called with the lock held.
        """
        self.cancelled.add(job_id)

    def consume_cancel_locked(self, job_id):
        """Clear a cancel the caller has already honored.

        Must be called with the lock held.
        """
        self.cancelled.discard(job_id)

    def request_running_cancel_locked(self, job_id):
        """Flag a running job for cancellation; False if it can't be stopped.

        Must be called with self._lock held.
        """
        # Stop is a no-op once the job has entered an
        # uninterruptible commit step (e.g. the local-processing
        # archive move). Without this guard a Stop landing after
        # clear_cancellation() but before _run_job's terminal
        # check would re-add the job to cancelled and record
        # the run as "cancelled" even though the archive
        # already committed to disk.
        if job_id in self.uncancellable:
            return False
        self.cancelled.add(job_id)
        self.pause_requested.discard(job_id)
        # A paused worker is sleeping on this condition. Wake it so
        # its checkpoint can observe cancellation and return.
        self._pause_condition.notify_all()
        return True

    # ------------------------------------------------------------------
    # Terminal status
    # ------------------------------------------------------------------

    def settle_returned(self, job, result):
        """Settle a job whose work function returned ``result``."""
        # The worker has returned and released its resources. Honor a
        # pause accepted during its final unit before publishing completion.
        # Waiting and finalizing share the pause/cancel lock, so neither
        # request can be accepted and then silently overtaken by completion.
        with self._pause_condition:
            job_id = job["id"]
            while (
                job.get("pausable")
                and job_id in self.pause_requested
                and job_id not in self.cancelled
                and job_id not in self.uncancellable
            ):
                if job.get("status") != "paused":
                    self._streams.publish_status_locked(job, "paused")
                self._pause_condition.wait()
            self.pause_requested.discard(job_id)
            if job_id in self.cancelled:
                job["status"] = "cancelled"
                self.cancelled.discard(job_id)
            else:
                apply_result_verdict(job, result)
            job["result"] = result

    def settle_raised_locked(self, job, e):
        """Settle the status of a job whose work function raised ``e``.

        Must be called with self._lock held.
        """
        job_id = job["id"]
        self.pause_requested.discard(job_id)
        if job_id in self.cancelled:
            job["status"] = "cancelled"
            self.cancelled.discard(job_id)
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

    # ------------------------------------------------------------------
    # Pause / resume / cancel probes
    # ------------------------------------------------------------------

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
                or job_id in self.uncancellable
                or job_id in self.cancelled
            ):
                return False
            self.pause_requested.add(job_id)
            self._streams.publish_status_locked(job, "pausing")
        return True

    def is_pause_requested(self, job_id):
        """Return whether *job_id* currently has a pending pause request.

        Pipeline jobs use this non-blocking probe to coordinate several worker
        threads.  A single worker reaching a checkpoint is not enough to call
        the whole pipeline paused; the pipeline publishes ``paused`` only once
        every active participant has reached a safe boundary.
        """
        with self._lock:
            return (
                job_id in self.pause_requested
                and job_id not in self.cancelled
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
                or job_id not in self.pause_requested
                or job_id in self.cancelled
                or job.get("status") not in ("pausing", "paused")
            ):
                return False
            if job.get("status") != "paused":
                self._streams.publish_status_locked(job, "paused")
            return True

    def wait_if_paused(self, job_id, *, publish_paused=False):
        """Wait for a pause request to clear, then report cancellation.

        ``publish_paused`` is appropriate for ordinary single-worker jobs.
        Coordinated jobs pass ``False`` and call :meth:`mark_paused` only after
        all active workers are parked.
        """
        while True:
            with self._pause_condition:
                if job_id in self.cancelled:
                    return True
                job = self._jobs.get(job_id)
                if (
                    job is None
                    or not job.get("pausable")
                    or job_id not in self.pause_requested
                ):
                    return False
                if publish_paused and job.get("status") != "paused":
                    self._streams.publish_status_locked(job, "paused")
                wait_start = self._clock()
                self._pause_condition.wait()
                self._pause_elapsed[job_id] = (
                    self._pause_elapsed.get(job_id, 0.0)
                    + max(0.0, self._clock() - wait_start)
                )

    def pause_elapsed_seconds(self, job_id):
        """Return cumulative seconds *job_id* has spent parked in a pause.

        Reset to zero when the job is forgotten. Returns 0.0 for a job that
        has never paused or is unknown. ETA estimators subtract this from
        their wall-clock elapsed so a pause does not inflate the per-item
        rate they publish on the next status callback.
        """
        with self._lock:
            return self._pause_elapsed.get(job_id, 0.0)

    def resume_job(self, job_id):
        """Resume a pausing or paused job."""
        with self._pause_condition:
            job = self._jobs.get(job_id)
            if (
                job is None
                or job.get("status") not in ("pausing", "paused")
                or job_id not in self.pause_requested
                or job_id in self.cancelled
            ):
                return False
            self.pause_requested.discard(job_id)
            self._streams.publish_status_locked(job, "running")
            self._pause_condition.notify_all()
        return True

    def cancellation_requested(self, job_id):
        """Report cancellation without waiting on a pause request.

        Transactional loops use this only after their pause-safe boundary.
        They can still roll back promptly on Cancel without sleeping while a
        database write transaction is open.
        """
        with self._lock:
            return job_id in self.cancelled

    def begin_uncancellable(self, job_id, *, wait_for_pause=True):
        """Honor a pending pause, then atomically enter an uninterruptible phase.

        Returns False when a cancellation is already pending, leaving that flag
        intact so ``_run_job`` can record the job as cancelled. Once this
        returns True, later pause and cancellation requests are ignored until
        the job reaches a terminal state. Pausable callers must invoke this at
        a safe checkpoint before acquiring resources for the commit.

        With ``wait_for_pause=False``, a pending pause returns False without
        parking. Verifiers use this to repeat their checks after Resume
        instead of committing with evidence from before an indefinite pause.
        """
        with self._pause_condition:
            if not wait_for_pause and job_id in self.pause_requested:
                return False
            while job_id in self.pause_requested and job_id not in self.cancelled:
                job = self._jobs.get(job_id)
                if job is None:
                    return False
                if job.get("status") != "paused":
                    self._streams.publish_status_locked(job, "paused")
                self._pause_condition.wait()
            if job_id in self.cancelled:
                return False
            self.uncancellable.add(job_id)
            self._publish_commit_locked(job_id)
            return True

    def _publish_commit_locked(self, job_id):
        """Expose the same control restrictions the backend enforces."""
        job = self._jobs.get(job_id)
        if job is not None:
            job["cancellable"] = False
            job["control_unavailable_reason"] = (
                "Finishing safely. Pause and cancel are unavailable during this step."
            )
            self._streams.publish_status_locked(job, job["status"])

    def clear_cancellation(self, job_id):
        """Consume any pending cancellation flag for ``job_id`` and
        mark the job uncancellable.

        Used by stages that have entered an uninterruptible commit step
        (e.g. the local-processing archive move) where a Stop press
        cannot be honored without leaving a partial published artifact.
        Without consuming the flag, ``_run_job``'s atomic terminal check
        would record the job as "cancelled" even though the commit
        succeeded — confusing the user about whether the archive landed.

        The flag is consumed AND the job is added to ``uncancellable``
        so a Stop press that lands after this call but before
        ``_run_job`` flips to a terminal status can't re-add the
        cancellation flag and override the committed result.
        """
        with self._lock:
            self.cancelled.discard(job_id)
            self.uncancellable.add(job_id)
            self._publish_commit_locked(job_id)
