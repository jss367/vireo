"""Import planning: discovery, selection, and batching by destination."""

import logging
from typing import NamedTuple

from import_dedup import CatalogIndex, DuplicateChecker
from import_job.file_selection import _apply_selection
from import_job.siblings import _companion_key
from import_job.snapshots import _capture_source_snapshots
from ingest import (
    _source_file_timestamps,
    build_destination_path,
    discover_source_files,
)

log = logging.getLogger(__name__)


# Batch unit (Task 2.0 Q4): files sharing a destination folder, chunked so
# one scan call never covers more than this many fresh files. Copy, verify,
# scan, and hash stamping all commit at batch boundaries, so every stopping
# point (cancel, crash, yanked card) leaves a valid catalog.
IMPORT_BATCH_SIZE = 200


class _ImportPlan(NamedTuple):
    """Everything the pre-loop planning phase produces for one run.

    Destructure BY NAME, never by position — same rationale as
    ``_Selection``: most fields are same-typed, so a transposed
    positional unpack would still run, still type-check, and still pass
    the local/remote parity test (which compares the two paths to each
    other and cannot see a transposition applied to both). ``files``
    and ``timestamps`` are already consumed by the batching step inside
    ``_plan_import``; they ride along for PR 7's orchestrator.
    """

    files: list
    discovered: int
    source_snapshots: dict
    include_paths: set | None
    queued: int
    deselected: int
    vanished_paths: set
    appeared: int
    checker: object  # DuplicateChecker | None
    timestamps: dict
    batches: list


def _plan_import(db, params, emit, state):
    """Discovery → source snapshots → selection → checker → batching.

    ORDERING IS LOAD-BEARING: ``_capture_source_snapshots`` runs before
    ``_apply_selection`` and before any copy work — see the block
    comment at the snapshot call.
    """
    # --- Discover ---------------------------------------------------
    emit("Discovering files", 0, 0)
    files = _discover_import_files(params, state)
    discovered = len(files)
    # Snapshot the discovered source metadata NOW — before selection filters
    # the copy set, before any copy work, and before duplicate hashing. The
    # retry-side signature check re-enumerates each source in full and
    # compares the current signature to what the parent recorded; capturing
    # the snapshot AFTER ``_apply_selection`` would mean a per-file import
    # only ever stored a signature over its selected subset, and the retry's
    # full-enumeration signature would never match — an unchanged card
    # would be rejected as drifted, and correcting that by having retry
    # also filter would leave nothing gating a card whose deselected files
    # were replaced or removed. Pre-selection capture keeps both sides on
    # the same enumeration. Capturing before copy also matters so a card
    # ejected or momentarily unreadable mid-run doesn't backfill ``-1``
    # sizes for files we successfully enumerated — reinserting the card
    # and retrying is a common recovery workflow, and the retry-side
    # signature check must have a snapshot taken from the source as
    # observed at run start to accept it.
    source_snapshots = _capture_source_snapshots(files, params.sources)

    # Selection: filter the copy set and measure drift. Shared by both
    # copy paths — see ``_apply_selection`` for why each condition is
    # shaped the way it is. Destructured BY NAME, not by position: four of
    # the six fields are plain ints and two are sets, so a positional unpack
    # that transposed a same-typed pair (``queued``/``deselected``,
    # ``queued``/``appeared``) would still run, still type-check, and still
    # pass the local/remote parity test — which compares the two paths to
    # each other and so cannot see a transposition applied to both.
    _sel = _apply_selection(files, params)
    files = _sel.files
    include_paths = _sel.include_paths
    queued = _sel.queued
    deselected = _sel.deselected
    vanished_paths = _sel.vanished_paths
    appeared = _sel.appeared

    checker = None
    if params.skip_duplicates:
        checker = DuplicateChecker(
            CatalogIndex.from_db(db), verify_by_hash=params.verify_by_hash,
        )
        checker.prepare(files)

    # Folder-planning timestamps: EXIF first (reusing the checker's batched
    # reads in metadata mode), file mtime fallback.
    timestamps = _source_file_timestamps(
        files,
        capture_times=(
            {str(f): checker.capture_time(f) for f in files}
            if checker is not None and not checker.verify_by_hash
            else None
        ),
    )

    batches = _batch_by_destination(files, timestamps, params)

    return _ImportPlan(
        files=files,
        discovered=discovered,
        source_snapshots=source_snapshots,
        include_paths=include_paths,
        queued=queued,
        deselected=deselected,
        vanished_paths=vanished_paths,
        appeared=appeared,
        checker=checker,
        timestamps=timestamps,
        batches=batches,
    )


def _discover_import_files(params, state):
    """Enumerate every source; enumeration errors land on ``state``."""
    # Enumeration errors (permission denied, macOS TCC block on a
    # removable volume, unreadable subtree) get silently swallowed by
    # os.walk-style callbacks by default. If we ignored them, discovered
    # would just be smaller than reality and safe_to_format could still
    # flip green over a card whose contents were never actually visited.
    # Track them explicitly: each is a bucket-of-its-own failure entry
    # tied to the source path where it occurred.
    files = []

    def _discovery_onerror(exc):
        state.discovery_errors.append(exc)
        log.warning("Import discovery error: %s", exc)

    for src in params.sources:
        files.extend(discover_source_files(
            src, params.file_types, recursive=params.recursive,
            onerror=_discovery_onerror,
        ))
    return files


def _batch_by_destination(files, timestamps, params):
    """Group ``files`` into ``(rel, batch)`` pairs, in template-folder order."""
    # Group by destination (template) folder, template order, then chunk.
    # Chunking is companion-aware: each ``_ImportBatchState`` holds its own
    # ``companion_siblings`` and ``companion_slots``, so a same-stem RAW/JPEG
    # pair split across two batches loses both signals and the collision
    # walk can suffix one half while the other keeps the shared stem —
    # the scan then pairs the JPEG with an unrelated same-stem RAW at the
    # destination. Keeping companion groups intact in the same batch is
    # enough because such a group is 2–3 files (RAW + JPEG + occasional
    # sidecar); a group large enough to exceed ``IMPORT_BATCH_SIZE`` on
    # its own gets split as before (unavoidable, and the same-stem
    # collision walk still adopts the anchor within one batch).
    groups = {}
    for f in files:
        rel = build_destination_path(
            timestamps.get(f), params.folder_template, f,
        ) or "."
        groups.setdefault(rel, []).append(f)
    batches = []
    for rel in sorted(groups):
        group = groups[rel]
        by_companion = {}
        for f in group:
            by_companion.setdefault(_companion_key(f), []).append(f)
        batch = []
        for companion in by_companion.values():
            if len(companion) > IMPORT_BATCH_SIZE:
                # Degenerate: a single companion group is bigger than a
                # batch on its own. Preserve the historical chunking so
                # this doesn't blow batch size unbounded; same-stem
                # anchoring still works within each sub-chunk.
                if batch:
                    batches.append((rel, batch))
                    batch = []
                for i in range(0, len(companion), IMPORT_BATCH_SIZE):
                    batches.append((rel, companion[i:i + IMPORT_BATCH_SIZE]))
                continue
            if batch and len(batch) + len(companion) > IMPORT_BATCH_SIZE:
                batches.append((rel, batch))
                batch = []
            batch.extend(companion)
        if batch:
            batches.append((rel, batch))
    return batches
