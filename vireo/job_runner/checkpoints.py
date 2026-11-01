"""Periodic checkpoints of live jobs into ``job_history``."""

import logging
import threading
import time

from job_runner.views import snapshot_job

# These lines were logged by ``jobs.JobRunner``; keep the logger name the log
# and its readers know them by.
log = logging.getLogger("jobs")

# How often live jobs checkpoint their step tree and progress into
# job_history. A crash or kill (the desktop app quits Flask without a
# graceful shutdown) never reaches persist_job, so without checkpoints
# the startup sweep can only say "interrupted" — not what got done.
CHECKPOINT_INTERVAL_SECS = 2.0
_LIVE_STATUSES = ("running", "pausing", "paused")


class JobCheckpointer:
    """Owns the checkpoint thread, its wake-up event and ``write_lock``.

    ``write_lock`` sits above the runner lock in lock order; besides the
    checkpoint path only ``JobRunner._persist_final`` acquires it, around a
    finished system job's final row (taking no runner lock inside). ``thread`` is guarded by the runner lock. Reads the
    runner's job registry; writes go through ``JobHistory``.
    """

    def __init__(self, lock, jobs, history, workers, *, checkpoint):
        self._lock = lock
        self._jobs = jobs
        self._history = history
        self._workers = workers
        # Timer/flush honor the runner hook; its default delegator calls
        # checkpoint_live_jobs below, which remains the implementation.
        self._checkpoint = checkpoint
        # Periodic checkpointing of live jobs into job_history. Started
        # lazily by the first persisted job and exits on its own once no
        # persisted job is live, so idle runners (and the thousands the
        # test suite creates) own no thread.
        self.thread = None
        self.wakeup = threading.Event()
        # Serializes the whole snapshot+write path so a late-finishing
        # older snapshot (the 2s timer's) cannot overwrite the row with
        # a scope that a fresher snapshot (a worker's synchronous flush
        # right after a batch commit) already persisted. Without this
        # the two callers can each snapshot under ``_lock`` and then
        # race in ``write_checkpoints``, leaving the interrupted row
        # holding stale ``photo_ids`` if Vireo crashes in that window.
        # Held around the whole checkpoint sequence, above ``_lock`` in
        # lock order. A system job's final row write takes it too, so a
        # stale snapshot can never re-create a row the final write deleted.
        self.write_lock = threading.Lock()

    def live_persisted_jobs_locked(self):
        """Jobs worth checkpointing. Must be called with self._lock held."""
        return [
            j for j in self._jobs.values()
            if j["status"] in _LIVE_STATUSES
            and not j.get("ephemeral")
            and not j.get("_persisted")
        ]

    def ensure_thread(self):
        """Start the checkpoint thread unless one is already running."""
        if not self._history.db_path:
            return
        with self._lock:
            if self._workers.shutting_down:
                return
            thread = self.thread
            if thread is not None and thread.is_alive():
                return
            self.wakeup.clear()
            thread = threading.Thread(
                target=self._loop,
                daemon=True,
                name="vireo-job-checkpoints",
            )
            self.thread = thread
            thread.start()

    def _loop(self):
        while True:
            self.wakeup.wait(CHECKPOINT_INTERVAL_SECS)
            with self._lock:
                if self._workers.shutting_down or not self.live_persisted_jobs_locked():
                    # Nothing left to checkpoint; the next persisted job
                    # start spawns a fresh thread.
                    self.thread = None
                    return
            try:
                self._checkpoint()
            except Exception:  # broad: the checkpoint loop must survive one failed write and try again next tick
                log.exception("Job checkpoint failed")

    def stop(self, thread, deadline):
        """Wake ``thread`` (read under the runner lock) and join it by ``deadline``."""
        # The checkpoint thread exits on its next wake once it sees
        # shutting_down; wake it now so it doesn't hold the deadline.
        self.wakeup.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(0.0, deadline - time.monotonic()))

    def checkpoint_live_jobs(self):
        """Persist a snapshot of every live, non-ephemeral job.

        Writes the step tree, progress, partial result and elapsed time
        onto the job's ``running`` history row so a crash leaves a record
        of what the job got done. Until the job has a real ``result``, a
        worker's ``job["partial_result"]`` stands in for it, so the
        startup sweep keeps it on the interrupted row. Returns the number
        of rows updated. Public so tests (and callers that just did something worth
        recording) can checkpoint deterministically instead of waiting
        for the timer.

        The whole snapshot+write path runs under
        ``write_lock``: without it, a worker's synchronous
        flush after a batch commit (scope N+1) can race the 2s timer
        that started earlier with scope N — both snapshot under
        ``_lock``, both release it, and whichever ``write_checkpoints``
        finishes last wins. A late-finishing N would overwrite N+1 on
        disk and, on a crash inside that window, leave the interrupted
        row missing the latest cataloged photo ids.
        """
        if not self._history.db_path:
            return 0
        with self.write_lock:
            with self._lock:
                snapshots = []
                for j in self.live_persisted_jobs_locked():
                    snap = snapshot_job(j)
                    # A worker publishes ``partial_result`` (a fresh dict
                    # per update) for what a crash must not lose. Write
                    # it only when it changed: it can be large, and the
                    # row keeps the last written copy between writes.
                    partial = j.get("partial_result")
                    if (
                        snap.get("result") is None
                        and partial is not None
                        and partial is not j.get("_checkpointed_partial")
                    ):
                        snap["result"] = partial
                        snap["_partial_result"] = partial
                    snapshots.append(snap)
            if not snapshots:
                return 0
            written = self._history.write_checkpoints(snapshots)
            if written:
                with self._lock:
                    for snap in snapshots:
                        live = self._jobs.get(snap["id"])
                        if live is not None and "_partial_result" in snap:
                            live["_checkpointed_partial"] = snap["_partial_result"]
            return written

    def flush_partial_result(self, job, attempts=10, cancel_check=None):
        """Write ``job["partial_result"]`` to its history row now.

        For a worker that must not proceed until the row holds its latest
        partial result. A checkpoint skips a write it can't get the lock
        for, so retry with a short backoff. Returns True once the row holds
        it, or at once when this runner keeps no history for the job.

        ``cancel_check``, when passed, is polled before each attempt so a
        Stop pressed while SQLite stays write-locked can end the wait
        without sitting through the full retry window (each internal
        checkpoint call blocks up to the 5s SQLite busy timeout).
        """
        if not self._history.db_path or job.get("ephemeral"):
            return True
        for attempt in range(attempts):
            if cancel_check is not None and cancel_check():
                return False
            self._checkpoint()
            if job.get("_checkpointed_partial") is job.get("partial_result"):
                return True
            time.sleep(0.2 * (attempt + 1))
        return False
