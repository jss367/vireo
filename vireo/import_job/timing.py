"""Where an import run spent its time.

An import does four kinds of work, and on a slow destination their shares
differ by an order of magnitude: finding the files on the card, copying
them (and, on the local transport, reading every copy back to verify it),
cataloging each batch (the scan hashes each new file at the destination
again), and building working copies after the last batch. The job has one
step, so without these numbers a slow import reads as one long "Copy &
catalog" with no way to tell which part was slow.

``timed`` adds a phase's wall-clock seconds to ``state.phase_seconds``.
Copying and cataloging alternate per batch, so each phase is a sum over the
run, not one interval. ``state.transferred_bytes`` counts the bytes of every
file the transport landed, so the copy phase has a rate.

``timing_result`` is the result's ``timing`` block (raw numbers) and
``timing_notice`` the sentence the Jobs page and the Import page show.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

from job_runner.reservations import format_duration
from local_processing import format_bytes

if TYPE_CHECKING:
    from collections.abc import Iterator

    from import_job.state import _ImportRunState
    from services.service_types import JsonDict

FIND = "find"
COPY = "copy"
CATALOG = "catalog"
WORKING_COPIES = "working_copies"

# Reporting order, with the words each phase is described by.
_PHASES = (
    (FIND, "finding photos"),
    (COPY, "copying"),
    (CATALOG, "cataloging"),
    (WORKING_COPIES, "working copies"),
)


@contextmanager
def timed(state: _ImportRunState, phase: str) -> Iterator[None]:
    """Add the block's wall-clock time to ``phase``, however it exits."""
    started = time.monotonic()
    try:
        yield
    finally:
        state.phase_seconds[phase] = (
            state.phase_seconds.get(phase, 0.0) + time.monotonic() - started
        )


def timing_result(state: _ImportRunState) -> JsonDict:
    """The result's ``timing`` block: seconds per phase and bytes copied.

    Only phases the run entered appear, so a run stopped during discovery
    reports ``find`` alone.
    """
    return {
        "seconds": {
            phase: round(state.phase_seconds[phase], 1)
            for phase, _words in _PHASES
            if phase in state.phase_seconds
        },
        "transferred_bytes": state.transferred_bytes,
    }


def _copy_rate(state: _ImportRunState) -> str:
    """The copy phase's size and rate, or '' when nothing was copied.

    The rate divides the bytes landed by the whole copy phase. Where the
    transport verifies each copy (the local transport reads every one back),
    that time is in the phase, so the rate is the pace of the import, not
    the speed of the connection; the notice names the phase "copying and
    verifying" for that reason.
    """
    seconds = state.phase_seconds.get(COPY, 0.0)
    if state.transferred_bytes <= 0 or seconds <= 0:
        return ""
    rate = format_bytes(int(state.transferred_bytes / seconds))
    return f" ({format_bytes(state.transferred_bytes)} at {rate}/s)"


def timing_notice(state: _ImportRunState, *, verified: bool) -> str | None:
    """One sentence saying where the run's time went, or None before any phase ran.

    ``verified`` is whether the transport independently verified every
    copy (``transport.attests_bytes``), which the copy phase's time includes.
    """
    parts = []
    for phase, words in _PHASES:
        if phase not in state.phase_seconds:
            continue
        part = f"{words} {format_duration(state.phase_seconds[phase])}"
        if phase == COPY:
            if verified:
                part = f"copying and verifying {format_duration(state.phase_seconds[phase])}"
            part += _copy_rate(state)
        parts.append(part)
    if not parts:
        return None
    return "Time spent: " + "; ".join(parts) + "."
