"""Step lists for jobs that run ``move.move_folder``.

``move_folder`` reports one phase at a time through its progress callback
(``progress_cb(current, total, filename, phase)``). ``MoveSteps`` turns that
stream into the job runner's step tree, so the jobs page lists every phase
of a transfer with its own count, bar and timer. A single "Overall" bar sat
full for the whole byte-for-byte verify of a large NAS transfer and read as
a hang.
"""

import threading
import time

# move_folder's phase name -> step id. A phase renamed in move.py must be
# renamed here too, or its progress stops reaching the step list.
FOLDER_MOVE_PHASES = {
    "Checking destination": "check",
    "Copying files": "copy",
    "Checking timestamps": "timestamps",
    "Verifying copy": "verify",
    "Updating catalog": "catalog",
    "Removing originals": "cleanup",
}

_TERMINAL = ("completed", "failed", "cancelled")

# Minimum gap between two progress events for the same step. The copy and
# verify phases call back once per file; on a fast local tree that would
# flood the SSE stream.
_EVENT_INTERVAL = 0.25


def folder_move_steps(*, remote, verify_contents):
    """The steps ``move_folder`` runs through, in order.

    A remote move has no timestamp pass (``plan_mtime_corrections`` only runs
    for local destinations), so it gets no step for one.
    """
    steps = [
        {"id": "check", "label": "Check destination"},
        {"id": "copy", "label": "Copy files"},
    ]
    if not remote:
        steps.append({"id": "timestamps", "label": "Check timestamps"})
    steps.extend([
        {"id": "verify", "label": (
            "Verify copy (compare every byte)" if verify_contents and not remote
            else "Verify copy")},
        {"id": "catalog", "label": "Update catalog"},
        {"id": "cleanup", "label": "Remove originals"},
    ])
    return steps


class MoveSteps:
    """Drive a job's step tree from ``move_folder``'s progress phases.

    ``phases`` maps each phase name the work reports to a step id. A phase
    with no entry (a wait message, ``"Done"``) leaves the tree alone. Steps
    advance in list order: entering a step completes every earlier one still
    open.
    """

    def __init__(self, runner, job, steps, phases=FOLDER_MOVE_PHASES):
        self._runner = runner
        self._job_id = job["id"]
        self._order = [step["id"] for step in steps]
        self._status = {step_id: "pending" for step_id in self._order}
        self._phases = phases
        self._active = None
        self._last_event = 0.0
        self._lock = threading.Lock()
        runner.set_steps(self._job_id, steps)

    def start(self):
        """Open the first step, for setup the work does before its first
        progress callback."""
        with self._lock:
            self._enter(self._order[0])

    def report(self, current, total, filename, phase):
        """Record one progress callback. Returns True when the caller should
        push a progress event now, False when the event can be skipped."""
        step_id = self._phases.get(phase)
        if step_id is None:
            return True
        with self._lock:
            changed = self._enter(step_id)
            self._runner.update_step(
                self._job_id, step_id,
                progress={"current": current, "total": total},
                current_file=filename,
            )
            now = time.monotonic()
            due = (changed or now - self._last_event >= _EVENT_INTERVAL
                   or (total and current >= total))
            if due:
                self._last_event = now
            return due

    def finish(self, step_id=None, *, summary=None, error=None):
        """Complete the open steps. A ``step_id`` gets ``summary`` and
        ``error`` (a warning on a completed step)."""
        with self._lock:
            if self._active is not None:
                self._close(self._active, "completed")
            if step_id is not None:
                fields = {}
                if summary:
                    fields["summary"] = summary
                if error:
                    fields.update(error=error, error_count=1)
                if fields:
                    self._runner.update_step(self._job_id, step_id, **fields)

    def fail(self, message, *, status="failed"):
        """Mark the step that was running (or the next one due) failed, or
        ``status``."""
        with self._lock:
            step_id = self._active or next(
                (s for s in self._order if self._status[s] == "pending"), None)
            if step_id is None:
                return
            if self._status[step_id] == "pending":
                self._set_status(step_id, "running")
            self._set_status(step_id, status, error=message, error_count=1)
            self._active = None

    def _enter(self, step_id):
        if step_id == self._active:
            return False
        if self._status.get(step_id) in _TERMINAL:
            return False
        for earlier in self._order[:self._order.index(step_id)]:
            if self._status[earlier] not in _TERMINAL:
                self._close(earlier, "completed")
        self._set_status(step_id, "running")
        self._active = step_id
        return True

    def _close(self, step_id, status):
        if self._status[step_id] in _TERMINAL:
            return
        self._set_status(step_id, status)
        if self._active == step_id:
            self._active = None

    def _set_status(self, step_id, status, **fields):
        self._status[step_id] = status
        self._runner.update_step(self._job_id, step_id, status=status, **fields)
