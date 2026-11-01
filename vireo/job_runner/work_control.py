"""Shared cooperative checkpoints for file work and external processes."""

from contextlib import contextmanager, nullcontext


class JobCancelled(Exception):
    """Unwind a worker at a safe boundary after cancellation."""


class JobNotStarted(JobCancelled):
    """Cancellation before acquiring a worker's transfer slot."""


class WorkControl:
    """Only park after all active work has stopped and resources are released.

    ``pause_context`` can temporarily release a transfer serialization lock.
    ``generation`` lets a verifier discard evidence gathered before a pause.
    """

    def __init__(self, runner, job_id, pause_context=nullcontext, commit_context=nullcontext):
        self.runner = runner
        self.job_id = job_id
        self.pause_context = pause_context
        self.commit_context = commit_context
        self.generation = 0

    def stop_requested(self):
        """Nonblocking probe suitable for a subprocess watchdog."""
        return (self.runner.cancellation_requested(self.job_id)
                or self.runner.pause_requested(self.job_id))

    def checkpoint(self):
        """Wait with no active subprocess, transaction, or shared lock."""
        if self.runner.pause_requested(self.job_id):
            self.generation += 1
            with self.pause_context():
                cancelled = self.runner.is_cancelled(self.job_id)
        else:
            cancelled = self.runner.cancellation_requested(self.job_id)
        if cancelled:
            raise JobCancelled("Job cancelled at a safe checkpoint")

    def try_commit(self):
        """Atomically enter the final commit, or require a fresh checkpoint.

        Never park here: callers must repeat verification if a pending pause
        wins the race against this boundary.
        """
        return self.runner.begin_uncancellable(self.job_id, wait_for_pause=False)


class SerializedWork:
    """Reserve one transfer slot, releasing it for the duration of a pause."""

    def __init__(self, runner, job_id, lock=None):
        self.runner = runner
        self.job_id = job_id
        self.lock = lock
        self.held = False
        self.control = WorkControl(runner, job_id, self._pause, self._commit_scope)

    def _acquire(self, *, checkpoint):
        if self.lock is None:
            return
        while True:
            if checkpoint:
                self.control.checkpoint()
            if self.runner.cancellation_requested(self.job_id):
                raise JobCancelled("Transfer cancelled at a safe checkpoint")
            if self.lock.acquire(timeout=0.1):
                self.held = True
                return

    def _release(self):
        if self.held:
            self.lock.release()
            self.held = False

    @contextmanager
    def _commit_scope(self):
        """Guard verification and commit, unless an outer scope already does."""
        if self.held:
            yield
            return
        try:
            self._acquire(checkpoint=True)
            self.control.checkpoint()
            yield
        finally:
            self._release()

    @contextmanager
    def _pause(self):
        was_held = self.held
        self._release()
        try:
            yield
        finally:
            if was_held and not self.runner.cancellation_requested(self.job_id):
                self._acquire(checkpoint=False)

    def __enter__(self):
        try:
            self._acquire(checkpoint=True)
            self.control.checkpoint()
            return self.control
        except JobCancelled as exc:
            self._release()
            raise JobNotStarted("Cancelled before transfer started") from exc

    def __exit__(self, *exc):
        self._release()
