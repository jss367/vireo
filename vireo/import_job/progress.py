"""Import progress: the ETA estimator and the progress emitter."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from job_contract import progress_event

if TYPE_CHECKING:
    from collections.abc import Callable

    from import_job.import_types import Emit
    from import_job.state import _ImportRunState
    from jobs import JobRunner
    from scanner.scan_types import StatusCallback
    from services.service_types import JobDict

# Every ETA key an import publishes. ``eta_phase`` names the phase the
# estimate describes (``"copy"`` for the card-to-archive copy,
# ``"working_copies"`` for the deferred working-copy pass) so a consumer
# never reads one phase's finished estimate as the other's.
_IMPORT_ETA_PROGRESS_KEYS = (
    "eta_phase", "eta_state", "eta_settled", "eta_seconds", "eta_rate_per_min",
)

# The working-copy pass reports ``estimating`` until this much time has
# passed since its first status callback and at least one working copy has
# completed. One second is short enough that a card of small JPEG
# extractions gets an estimate almost at once, and long enough that the
# first rate is not computed from a few milliseconds of timer resolution.
_WORKING_COPY_ETA_MIN_ELAPSED = 1.0


# Batch-scoped truth counters for the remote path: how many files of the
# current per-batch rsync have actually crossed the network, next to the
# ordinary ``current``/``total`` which advances while files are merely
# inspected and queued. Uses the same sub-phase progress keys the scanner
# emits (``phase_current``/``phase_total``/``phase_label``) so the bottom
# panel's ``bpActiveProgress`` renders the batch counter without changes.
# Cleared on every ordinary emit so they never outlive their batch.
_IMPORT_TRANSFER_PROGRESS_KEYS = (
    "phase_current", "phase_total", "phase_label",
)


class _ImportEtaEstimator:
    """Estimate import time from completed batches that landed new files.

    The ordinary job progress counter advances while a batch is being
    prepared.  For remote imports that means it can jump by 200 and then sit
    unchanged while rsync and the restricted catalog scan do the expensive
    work.  A lifetime ``current / elapsed`` rate therefore treats prepared
    files -- and fast duplicate-only batches -- as completed transfers.

    Keep an independent settled count and separate quick duplicate-only
    batches from work that actually increased ``copied``. Until the first
    relevant batch completes the honest answer is "estimating". Later
    estimates use an EWMA biased toward the newest completed batch, which
    adapts when file sizes or archive speed change without swinging on every
    individual file.
    """

    def __init__(self, clock: Callable[[], float] | None = None, expected_new: int | None = None) -> None:
        self._clock = clock or time.monotonic
        self._expected_new = expected_new
        self._batch_started_at = None
        self._batch_start_settled = 0
        self._batch_start_copied = 0
        self._settled = 0
        self._copied = 0
        self._seconds_per_file = None
        self._duplicate_seconds_per_file = None

    @staticmethod
    def _smooth(previous: float | None, measured: float) -> float:
        if previous is None:
            return measured
        return 0.4 * previous + 0.6 * measured

    def note_importing(self, copied: int) -> None:
        """Start timing the current batch on its first per-file event."""
        if self._batch_started_at is None:
            self._batch_started_at = self._clock()
            self._batch_start_settled = self._settled
            self._batch_start_copied = copied

    def note_batch_complete(self, current: int, copied: int) -> None:
        """Settle a batch and, when it landed files, learn its duration."""
        current = max(self._settled, int(current or 0))
        completed = current - self._batch_start_settled

        if (
            self._batch_started_at is not None
            and completed > 0
        ):
            elapsed = max(0.0, self._clock() - self._batch_started_at)
            if elapsed > 0:
                copied_in_batch = max(0, copied - self._batch_start_copied)
                duplicates_in_batch = max(0, completed - copied_in_batch)
                if copied_in_batch == 0:
                    measured = elapsed / completed
                    self._duplicate_seconds_per_file = self._smooth(
                        self._duplicate_seconds_per_file, measured,
                    )
                else:
                    # Measure expensive work against files actually copied,
                    # even when no preview supplied an expected-new count. A
                    # boundary batch with 199 quick duplicates and one slow
                    # transfer must not look like 200 fast transfers. Remove
                    # the duplicate time learned from earlier pure batches
                    # when available; otherwise retaining it here makes the
                    # first estimate conservative instead of optimistic.
                    duplicate_time = (
                        duplicates_in_batch
                        * (self._duplicate_seconds_per_file or 0.0)
                    )
                    measured = max(
                        elapsed - duplicate_time,
                        0.001 * copied_in_batch,
                    ) / copied_in_batch
                    self._seconds_per_file = self._smooth(
                        self._seconds_per_file, measured,
                    )

        self._settled = current
        self._copied = copied
        self._batch_started_at = None
        self._batch_start_settled = current
        self._batch_start_copied = copied

    def fields(self, total: int) -> dict[str, Any]:
        """Return JSON-safe telemetry for the progress event and step."""
        result = {
            "eta_phase": "copy",
            "eta_state": (
                "ready" if self._seconds_per_file is not None
                else "estimating"
            ),
            "eta_settled": self._settled,
        }
        remaining = max(0, int(total or 0) - self._settled)
        if self._expected_new is not None:
            remaining_new = min(
                remaining, max(0, self._expected_new - self._copied),
            )
            remaining_duplicates = max(0, remaining - remaining_new)
            new_rate = self._seconds_per_file
            duplicate_rate = self._duplicate_seconds_per_file or new_rate
            ready = (
                (remaining_new == 0 or new_rate is not None)
                and (remaining_duplicates == 0 or duplicate_rate is not None)
            )
            result["eta_state"] = "ready" if ready else "estimating"
            if ready:
                eta_seconds = (
                    remaining_new * (new_rate or 0.0)
                    + remaining_duplicates * (duplicate_rate or 0.0)
                )
                result["eta_seconds"] = round(eta_seconds, 1)
                if eta_seconds > 0 and remaining > 0:
                    result["eta_rate_per_min"] = round(
                        60.0 * remaining / eta_seconds, 1,
                    )
            return result

        if self._seconds_per_file is not None:
            result["eta_seconds"] = round(
                remaining * self._seconds_per_file, 1,
            )
            result["eta_rate_per_min"] = round(
                60.0 / self._seconds_per_file, 1,
            )
        return result


class _WorkingCopyEtaEstimator:
    """Estimate the deferred working-copy pass from its own progress.

    The pass starts after every original has landed, when the copy phase's
    estimate already reads zero seconds remaining; carrying that forward
    would present a finished phase's ETA while thousands of working copies
    are still pending. Time the pass from its first status callback and
    divide by the working copies completed so far. Until
    ``_WORKING_COPY_ETA_MIN_ELAPSED`` has passed and one working copy has
    completed the honest answer is "estimating".

    The extractor's cancel check parks inside the runner's
    ``wait_if_paused`` on Pause, so an item that straddles a pause would
    otherwise look like it took the entire pause to extract and poison the
    per-item rate for the rest of the pass. ``paused_elapsed``, when
    supplied, returns the job's cumulative pause duration; the estimator
    subtracts the delta since timing started from its wall-clock elapsed so
    rate and ETA reflect active processing time only.
    """

    def __init__(
        self,
        clock: Callable[[], float] | None = None,
        paused_elapsed: Callable[[], float] | None = None,
    ) -> None:
        self._clock = clock or time.monotonic
        self._paused_elapsed = paused_elapsed
        self._started_at: float | None = None
        self._paused_at_start: float = 0.0

    def fields(self, current: int | None, total: int | None) -> dict[str, Any]:
        """Return JSON-safe telemetry for the progress event and step."""
        now = self._clock()
        if self._started_at is None:
            self._started_at = now
            if self._paused_elapsed is not None:
                self._paused_at_start = self._paused_elapsed()
        completed = max(0, int(current or 0))
        remaining = max(0, int(total or 0) - completed)
        result: dict[str, Any] = {
            "eta_phase": "working_copies",
            "eta_state": "estimating",
            "eta_settled": completed,
        }
        if remaining == 0:
            result["eta_state"] = "ready"
            result["eta_seconds"] = 0.0
            return result
        pause_delta = 0.0
        if self._paused_elapsed is not None:
            pause_delta = max(0.0, self._paused_elapsed() - self._paused_at_start)
        elapsed = max(0.0, now - self._started_at - pause_delta)
        if completed == 0 or elapsed < _WORKING_COPY_ETA_MIN_ELAPSED:
            return result
        seconds_per_item = elapsed / completed
        result["eta_state"] = "ready"
        result["eta_seconds"] = round(remaining * seconds_per_item, 1)
        result["eta_rate_per_min"] = round(60.0 / seconds_per_item, 1)
        return result


def _make_working_copy_status(
    job: JobDict, runner: JobRunner, state: _ImportRunState, eta: _WorkingCopyEtaEstimator | None = None,
) -> StatusCallback:
    """Build the status callback for the deferred working-copy pass.

    The callback exposes the pass as sub-phase progress
    (``phase_current`` / ``phase_total`` / ``phase_label``) without
    replacing the import's file totals, and replaces the copy phase's ETA
    keys with the pass's own estimate: the copy phase is over by then, so
    its ``eta_seconds: 0`` would otherwise be published as this phase's.
    """
    if eta is None:
        eta = _WorkingCopyEtaEstimator(
            paused_elapsed=lambda: runner.pause_elapsed_seconds(job["id"]),
        )
    eta_fields: dict[str, Any] = {}

    def _working_copy_status(
        message: str, phase_current: int | None = None, phase_total: int | None = None,
        phase_label: str | None = None,
    ) -> None:
        nonlocal eta_fields
        if phase_total is not None:
            eta_fields = eta.fields(phase_current, phase_total)
        extra = {
            "phase_current": phase_current,
            "phase_total": phase_total,
            "phase_label": phase_label or "Generating working copies",
            **eta_fields,
        }
        for key in _IMPORT_ETA_PROGRESS_KEYS:
            job["progress"].pop(key, None)
        job["progress"].update(extra)
        job["progress"]["current_file"] = message
        runner.update_step(
            job["id"], "import",
            current_file=message,
            progress={
                "current": job["progress"].get("current", 0),
                "total": job["progress"].get("total", 0),
                **extra,
            },
        )
        runner.push_event(
            job["id"], "progress",
            progress_event(
                extra["phase_label"],
                job["progress"].get("current", 0),
                job["progress"].get("total", 0),
                message,
                folders={
                    rel: dict(counts)
                    for rel, counts in state.folder_counts.items()
                },
                **extra,
            ),
        )
    return _working_copy_status


def _make_emitter(job: JobDict, runner: JobRunner, state: _ImportRunState, eta: _ImportEtaEstimator) -> Emit:
    """Build the per-run progress emitter shared by both import paths.

    ``state.folder_counts``: live per-folder counters, mutated by the
    copy loop via _counts() and snapshotted onto every progress event
    so the Import page can render truthful per-folder progress mid-run
    (not just from the terminal result).
    """
    def _emit(phase: str, current: int, total: int, current_file: str = "", *, is_importing: bool = False) -> None:
        eta_fields = {}
        if total > 0:
            if is_importing:
                eta.note_importing(state.copied)
            else:
                eta.note_batch_complete(current, state.copied)
            eta_fields = eta.fields(total)
        job["progress"]["current"] = current
        job["progress"]["total"] = total
        job["progress"]["current_file"] = current_file
        # ONE body serves both transports because clearing the transfer
        # keys is a PROVEN no-op on the local one: those keys are only
        # ever written by ``_RsyncTransport._emit_transfer``
        # and never seeded by JobRunner —
        # every job "progress" dict it builds is exactly
        # ``{current, total, current_file}`` (jobs.py:552/626/1042).
        # push_event mirroring cannot introduce them locally either,
        # since local progress events never carry them.
        for key in _IMPORT_ETA_PROGRESS_KEYS + _IMPORT_TRANSFER_PROGRESS_KEYS:
            job["progress"].pop(key, None)
        job["progress"].update(eta_fields)
        runner.update_step(
            job["id"], "import",
            current_file=current_file,
            progress={
                "current": current, "total": total, **eta_fields,
            },
        )
        runner.push_event(
            job["id"], "progress",
            progress_event(
                phase, current, total, current_file,
                # Snapshot (counts dicts mutate as the loop advances; SSE
                # consumers must see the state at emit time). Both paths
                # send the folder table — spec decision 1.
                folders={
                    rel: dict(counts) for rel, counts in state.folder_counts.items()
                },
                **eta_fields,
            ),
        )
    return _emit
