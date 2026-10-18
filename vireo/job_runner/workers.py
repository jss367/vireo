"""The runner's threads and its shutting-down flag."""

import logging
import threading
import time

# These lines were logged by ``jobs.JobRunner``; keep the logger name the log
# and its readers know them by.
log = logging.getLogger("jobs")


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


class WorkerThreads:
    """Owns the runner's worker threads and its ``shutting_down`` flag.

    Worker ownership is explicit.  Daemon threads alone are not a
    lifecycle: short-lived create_app() callers (especially tests) must
    be able to cancel and join their work before tearing down databases
    and process-wide monkeypatches.

    Both attributes are guarded by the runner lock it is given; other
    collaborators read ``shutting_down`` under that lock.
    """

    def __init__(self, lock, run_job_provider):
        # ``run_job_provider`` is a zero-arg callable resolved every time
        # a worker thread is started so a runner instrumented after
        # construction (a fixture or subclass that assigns ``_run_job``)
        # still has its replacement honored — matching the pre-extraction
        # ``_start_worker_thread`` that read ``self._run_job`` each time.
        self._lock = lock
        self._run_job_provider = run_job_provider
        self.threads = set()
        self.shutting_down = False

    def _forget(self, thread):
        with self._lock:
            self.threads.discard(thread)

    def start(self, job, work_fn):
        """Start and retain a worker until its complete cleanup finishes.

        Returns False when shutdown won the race with a caller that had
        already registered a job but had not started its thread yet.
        """
        thread = _TrackedJobThread(
            target=self._run_job_provider(),
            args=(job, work_fn),
            on_finish=self._forget,
            daemon=True,
            name=f"vireo-job-{job['id']}",
        )
        with self._lock:
            if self.shutting_down:
                return False
            self.threads.add(thread)
            # Start while still holding the ownership lock. shutdown() can
            # therefore never take a snapshot between registration and start.
            thread.start()
        return True

    def start_promotion_retry_locked(self, retry):
        """Start the promotion-retry thread. Caller holds the runner lock."""
        thread = _TrackedJobThread(
            target=retry,
            on_finish=self._forget,
            daemon=True,
            name="vireo-promotion-retry",
        )
        self.threads.add(thread)
        thread.start()

    def begin_shutdown_locked(self):
        """Stop accepting work. Caller holds the runner lock."""
        self.shutting_down = True

    def join(self, deadline):
        """Join every worker but the calling thread before ``deadline``.

        Returns False, after logging the survivors, when one or more are
        still alive at the deadline.
        """
        current = threading.current_thread()
        while True:
            with self._lock:
                threads = [
                    thread for thread in self.threads
                    if thread is not current and thread.is_alive()
                ]
            if not threads:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                log.warning(
                    "JobRunner shutdown timed out with %d worker(s) alive: %s",
                    len(threads), ", ".join(thread.name for thread in threads),
                )
                return False
            threads[0].join(timeout=remaining)
