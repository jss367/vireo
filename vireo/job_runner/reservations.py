"""Workspace reservations: transfers versus every other job and change.

A NAS transfer moves the originals under two folder trees (where they are and
where they land), so nothing that might touch those trees may run while it
does: no job that carries photo/folder paths, no queued pipeline, and no
synchronous request that writes the catalog. A job that declares the trees it
reads and writes (``workspace_paths``: an import's sources and the folder it
copies into, a chained Process run's photo folders) is admitted beside a
transfer when none of them overlaps the transfer's trees; anything that does
not declare them is refused, because it could reach any folder. A new
transfer still waits for every other job in the workspace. This module
decides admission on both sides and names whatever holds a workspace when it
refuses.
"""

import logging
import os
import sys
import threading
import time
import traceback
from contextlib import contextmanager

# These lines were logged by ``jobs.JobRunner``; keep the logger name the log
# and its readers know them by.
log = logging.getLogger("jobs")


class WorkspaceBusyError(RuntimeError):
    """A transfer and another job cannot share the workspace's originals."""


# A synchronous request that has held its workspace reservation this long is
# hung, not slow: a busy rejection says so and logs the holder's stack.
STALE_WORKSPACE_MUTATION_SECS = 60


def format_duration(seconds):
    """Short human duration for status text: "12 s", "4 min", "3 h 2 min"."""
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} h {minutes} min" if minutes else f"{hours} h"


def describe_job(job):
    """Name a job for a busy message, e.g. "the export job (running for 4 min)"."""
    kind = str(job.get("type") or "unknown").replace("-", " ").replace("_", " ")
    kind = " ".join("NAS" if word.lower() == "nas" else word for word in kind.split())
    status = job.get("status") or "running"
    started = job.get("_start_time")
    if status == "running" and isinstance(started, (int, float)):
        status = f"running for {format_duration(time.time() - started)}"
    return f"the {kind} job ({status})"


def _join_blockers(items):
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def describe_jobs(jobs):
    """``describe_job`` for each job, joined into one phrase."""
    return _join_blockers([describe_job(job) for job in jobs])


def _tree_key(path):
    # Case-folded on every platform: treating two trees as overlapping when
    # a case-sensitive filesystem would keep them apart only refuses a job,
    # and the check runs under the runner lock, so it must not probe disk.
    return os.path.normpath(path).casefold().rstrip(os.sep) + os.sep


def paths_overlap(a, b):
    """True if realpath'd ``a`` and ``b`` are the same tree or one holds the other."""
    a_key, b_key = _tree_key(a), _tree_key(b)
    return a_key.startswith(b_key) or b_key.startswith(a_key)


def transfer_conflict(transfers, paths):
    """The first (transfer, transfer path) whose tree overlaps one of ``paths``.

    A transfer that declared no trees conflicts with everything, and so does
    a job that declared none (``paths`` empty or None): either could reach
    any folder. Returns None when the job can run beside every transfer.
    """
    for transfer in transfers:
        reserved = transfer.get("workspace_paths")
        if not reserved or not paths:
            return transfer, None
        for tree in reserved:
            if any(paths_overlap(tree, path) for path in paths):
                return transfer, tree
    return None


class WorkspaceReservations:
    """Owns the synchronous workspace reservations (API requests).

    ``mutations`` (workspace -> holders) and ``exclusive`` (workspace ->
    holder) are guarded by the runner lock; transfer waits park on
    ``_pause_condition``, which is built on that same lock. Reads the job
    registry, the pipeline queue's contexts and admissions, the cancel
    flags and the shutting-down flag.
    """

    def __init__(self, lock, pause_condition, jobs, pipeline_queue, control, workers):
        self._lock = lock
        self._pause_condition = pause_condition
        self._jobs = jobs
        self._queue = pipeline_queue
        self._control = control
        self._workers = workers
        # workspace -> holders of synchronous reservations (API requests).
        # Each holder records what it is, when it began and which thread holds
        # it, so a rejected transfer can name it instead of "something".
        self.mutations = {}
        self.exclusive = {}  # workspace -> holder

    @contextmanager
    def workspace_mutation(self, workspace_id, *, exclusive=False, label=None,
                           defer_transfer_check=False):
        """Reserve synchronous mutations against transfers for their full duration.

        ``label`` names the holder in the message a blocked job gets back.
        ``defer_transfer_check`` admits the request beside a running transfer
        because the job it starts declares its folder trees and is checked
        against the transfer's when it registers. The reservation still keeps
        a new transfer and a workspace deletion out while the request runs.
        """
        holder = {
            "label": label or "a change in this workspace",
            "started": time.monotonic(),
            "thread": threading.get_ident(),
        }
        with self._lock:
            self.check_admission_locked(workspace_id, exclusive=exclusive,
                                        defer_transfer_check=defer_transfer_check)
            self.mutations.setdefault(workspace_id, []).append(holder)
            if exclusive:
                self.exclusive[workspace_id] = holder
        try:
            yield
        finally:
            with self._lock:
                holders = [h for h in self.mutations[workspace_id] if h is not holder]
                if holders:
                    self.mutations[workspace_id] = holders
                else:
                    del self.mutations[workspace_id]
                if self.exclusive.get(workspace_id) is holder:
                    del self.exclusive[workspace_id]
            held = time.monotonic() - holder["started"]
            if held >= STALE_WORKSPACE_MUTATION_SECS:
                log.warning("Workspace %s reservation for %s was held for %s",
                            workspace_id, holder["label"], format_duration(held))

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
                if job_id in self._control.cancelled or self._workers.shutting_down:
                    return False
                # No filesystem work or reservation is held yet. Publish a
                # genuine pause even while waiting for the producing jobs.
                if self._control.wait_if_paused(job_id, publish_paused=True):
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
                        and not self._queue.admissions.get(workspace_id)
                        and not self.mutations.get(workspace_id)
                        and not any(c.get("workspace_id") == workspace_id for c in self._queue.queued.values())):
                    for sibling in live:
                        if sibling.get("workspace_transfer_batch") == batch:
                            sibling["exclusive_workspace"] = True
                    return True
                self._pause_condition.wait(timeout=0.1)

    def check_admission_locked(self, workspace_id, blocking=True, exclusive=False,
                               paths=None, defer_transfer_check=False):
        """Check both sides of a transfer reservation under the registration lock.

        ``paths`` are the realpath'd folder trees a job declared; it runs
        beside a transfer only when none overlaps the transfer's trees.
        ``defer_transfer_check`` skips the transfer check for an admission
        that cannot touch originals yet: an automatic transfer, which waits in
        ``wait_for_workspace_transfer`` before moving anything, and a request
        whose job is checked when it registers.

        A rejection names what holds the workspace: the user cannot see a
        synchronous reservation anywhere else, so "wait for running jobs"
        with no job running leaves them nothing to wait for.
        """
        if not blocking:
            return
        exclusive_holder = self.exclusive.get(workspace_id)
        if exclusive_holder is not None:
            self._raise_workspace_busy_locked(
                workspace_id,
                "Wait for {blockers} to finish before starting another job or change in this workspace.",
                [], [exclusive_holder],
            )
        active = [j for j in self._jobs.values()
                  if j.get("workspace_id") == workspace_id
                  and j.get("status") in ("running", "queued", "pausing", "paused")
                  and j.get("blocks_local_transitions", True)]
        transfers = [j for j in active if j.get("exclusive_workspace")]
        conflict = None if defer_transfer_check else transfer_conflict(transfers, paths)
        if conflict is not None:
            transfer, tree = conflict
            if tree is None:
                template = ("Wait for the NAS transfer ({blockers}) to finish before starting "
                            "another job in this workspace.")
            else:
                # The folder is the user's to compare with what they chose;
                # braces in it must survive the template formatting.
                folder = tree.replace("{", "{{").replace("}", "}}")
                template = ("Wait for the NAS transfer ({blockers}) to finish: it is moving "
                            f"{folder}, which this job also uses.")
            self._raise_workspace_busy_locked(workspace_id, template, [transfer], [])
        if not exclusive:
            return
        queued = sum(1 for c in self._queue.queued.values() if c.get("workspace_id") == workspace_id)
        admitting = self._queue.admissions.get(workspace_id, 0)
        holders = self.mutations.get(workspace_id, [])
        if active or queued or admitting or holders:
            extra = []
            if queued:
                extra.append("a queued pipeline" if queued == 1 else f"{queued} queued pipelines")
            if admitting:
                extra.append("a pipeline being added to the queue")
            self._raise_workspace_busy_locked(
                workspace_id,
                "This workspace is busy with {blockers}. Wait for {it} to finish, then try again.",
                active, holders, extra,
            )

    def _raise_workspace_busy_locked(self, workspace_id, template, jobs, holders, extra=()):
        """Raise ``WorkspaceBusyError`` naming every blocker in ``template``.

        A request that has held its reservation past
        ``STALE_WORKSPACE_MUTATION_SECS`` is called out as long-running, and
        its thread's stack goes to the log so the hang can be diagnosed
        without attaching a debugger to the running app.
        """
        now = time.monotonic()
        stale = [h for h in holders if now - h["started"] >= STALE_WORKSPACE_MUTATION_SECS]
        parts = [describe_job(j) for j in jobs]
        parts += [f"{h['label']} (running for {format_duration(now - h['started'])})" for h in holders]
        parts += list(extra)
        message = template.format(blockers=_join_blockers(parts), it="it" if len(parts) == 1 else "them")
        if stale:
            message += " A request is taking longer than usual; it may still be working."
        (log.warning if stale else log.info)("Workspace %s busy: %s", workspace_id, message)
        frames = sys._current_frames() if stale else {}
        for holder in stale:
            frame = frames.get(holder["thread"])
            if frame is None:
                log.warning("Holder of %s (thread %s) has exited without releasing its reservation",
                            holder["label"], holder["thread"])
            else:
                log.warning("Holder of %s (thread %s) is at:\n%s", holder["label"], holder["thread"],
                            "".join(traceback.format_stack(frame)))
        raise WorkspaceBusyError(message)
