"""The per-batch import loop shared by both transports."""

from __future__ import annotations

import functools
import os
from typing import TYPE_CHECKING

from import_job.cataloging import (
    _catalog_scan_and_prescan,
    _fill_wc_overrides,
    _invalidate_changed_and_sweep,
    _stamp_landed_and_validate_catalog,
)
from import_job.destination import _reject_source_backed_dest
from import_job.duplicate_gate import _GATE_CANCELLED, _GATE_SKIPPED, _duplicate_gate
from import_job.preflight import (
    _batch_preflight,
    _drop_queued_transfers,
    _rollback_on_mount_loss,
)
from import_job.resume import _publish_resume_scope, _record_landed_files
from import_job.siblings import _companion_siblings
from import_job.state import _fail, _ImportBatchState, _reclassify_landed_failed
from import_job.transports import _ENQ_CANCELLED
from import_job.twins import _link_twins_and_emit

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from db import Database
    from import_job.destination import _DestContext
    from import_job.import_types import Emit, ImportBatch, ScanFunction, StopCheck
    from import_job.planning import _ImportPlan
    from import_job.state import ImportParams, _ImportRunState
    from import_job.transports import _LocalTransport, _RsyncTransport
    from jobs import JobRunner
    from services.service_types import JobDict


def _make_stop_check(runner: JobRunner, job: JobDict) -> StopCheck:
    """Build the nonblocking stop probe shared by both import paths.

    Threaded through every destination-side hash read so a Stop can
    interrupt a read blocked on a dead mount (see _hash_dest_file).
    Nonblocking probe — ``is_cancelled`` would park in
    ``wait_if_paused`` for a pausable import, freezing the watchdog
    loop itself and stopping the 120s stall timer from running while
    the daemon reader can keep touching the archive even though the
    UI says the job is paused. Mirrors the rsync watchdog's use of
    ``cancellation_requested`` for the same reason.
    """
    def _stop_requested() -> bool:
        return runner.cancellation_requested(job["id"])
    return _stop_requested


class _ImportBatchLoop:
    """The per-batch copy -> verify -> catalog loop of one import run.

    Holds the run-wide bindings the loop threads through every batch;
    the mutable run-wide ledger itself stays on ``state``.

    Every discovered file ends in exactly one terminal bucket on
    ``state``. ``state.verified``: count of files independently
    checksum-verified (every copy when the transport attests bytes).
    ``state.imported_photo_ids``: photo rows this run created or
    landed bytes into: hash-stamped fresh copies, adopted
    (crash-recovery) landings whose pre-existing row now belongs to
    this run, and RAW primaries that adopted a landed companion JPEG.
    The after-import chaining hook scopes its process job to exactly
    these (cataloged-twin duplicate skips are excluded — a
    duplicates-only import chains to "no new photos", not an empty
    run).

    Working-copy extraction is DEFERRED to the end of the whole import
    run (not per-batch). Rationale: a folder that receives more than
    ``IMPORT_BATCH_SIZE`` files splits across multiple batches; a
    RAW+JPEG companion pair can then straddle a batch boundary — the
    RAW lands in batch N and its JPEG in batch N+1. Per-batch
    extraction would run on the RAW before scan()'s
    ``_pair_raw_jpeg_companions`` sees the JPEG, so the RAW's row
    still has ``companion_path IS NULL``. The extractor then reads the
    RAW itself (RAW-decode-first); if that fails or produces a
    low-quality fallback, ``working_copy_failed_at`` is set and the
    candidate predicate would gate future retries — the JPEG landing
    in the next batch never re-triggers extraction while the card-side
    JPEG is still available. Deferring to end-of-run means every
    companion in the run has landed and been paired before
    ``_extract_working_copies`` decides which source to read.
    ``state.wc_source_paths``: dest_path -> (card_src_path,
    expected_size, expected_mtime_ns).
    The identity tuple is captured at land time (before any WC
    extraction pass), so the deferred ``_extract_working_copies`` call
    can verify the override still holds the exact bytes we copied —
    not just any same-sized file that happens to be at the same path.
    A rewrite or a reused-mount collision differ in mtime and get
    rejected; extraction falls back to the verified archive copy.
    ``state.wc_dest_folders`` is the extraction's exact-match scope.
    ``state.run_dest_folders``: intra-run duplicate destinations —
    token -> dest folder where the identity landed this run (mirrors
    ingest's batch_dest_folders).
    ``state.run_verified_hashes``: byte-proven verified hash for each
    intra-run token. A ('hash', h)
    token's own value IS the proof; a ('key', …) token carries no bytes,
    so accepting a later key match against a run twin requires hashing
    the current source and comparing against this recorded value (two
    different files with the same filename+size+capture-second across
    cards would otherwise be counted as skipped_duplicate without ever
    verifying bytes — safe_to_format green, second card is only copy).
    ``state.linked_dup_dirs``: dup-twin dirs already scanned+linked.

    ``state.mount_ever_lost``: sticky across the rest of the run once
    a mounted → unmounted transition is observed. The per-batch
    rollback below undoes ``dup_skips``, ``landed``, and queued
    ``to_transfer`` entries, but not the identities the same batch
    already installed in the job-wide ``checker`` (and in
    ``run_dest_folders`` / ``run_verified_hashes``) via
    ``_record_checker`` — and ``DuplicateChecker`` exposes no removal
    API, so those entries cannot be surgically undone. If the share
    remounts before a later batch, a same-content card file would hit
    the intra-run fast path and be counted as a duplicate of a landing
    whose archive claim was rolled back. Refusing every remaining
    batch keeps the stale intra-run cache from ever being consulted.
    See PR #1400 review (Codex P2 r3688614624).

    Dup-folder linking runs in a SEPARATE ``scan(restrict_dirs=…)`` call
    after the duplicate skip; its exception was previously logged and
    swallowed, leaving safe_to_format true while the imported
    duplicates never became visible in the active workspace. Track it
    explicitly (``state.dup_link_failed``) so safe_to_format reflects
    "workspace can actually see these bytes" and not just "the bytes
    are somewhere on disk".
    """

    def __init__(
        self, *, job: JobDict, runner: JobRunner, db: Database, workspace_id: int,
        params: ImportParams, ctx: _DestContext, transport: _LocalTransport | _RsyncTransport,
        state: _ImportRunState, emit: Emit, plan: _ImportPlan, scan: ScanFunction,
        unmounted_since_baseline: Callable[[dict[str, bool]], str | None],
        missing_root_check: Callable[[], str | None],
    ) -> None:
        self._job = job
        self._runner = runner
        self._db = db
        self._workspace_id = workspace_id
        self._params = params
        self._ctx = ctx
        self._transport = transport
        self._state = state
        self._emit = emit
        self._queued = plan.queued
        self._checker = plan.checker
        self._source_snapshots = plan.source_snapshots
        self._scan = scan
        self._unmounted_since_baseline = unmounted_since_baseline
        self._missing_root_check = missing_root_check
        self._stop_requested = _make_stop_check(runner, job)
        # Whether each ``copied`` booking also incremented ``verified``:
        # the local transport hash-verifies every copy
        # (``copy_and_hash_verify`` reads the landed bytes back); the rsync
        # transport books ``verified`` only when ``params.verify_by_hash``
        # made the independent card->NAS check — so
        # ``_reclassify_landed_failed`` must undo ``verified`` exactly when
        # the transport attests bytes. Bound once: the rsync property is
        # constant for the run.
        self.attests_bytes = transport.attests_bytes

    def _is_cancelled(self) -> bool:
        return self._runner.is_cancelled(self._job["id"])

    def run(self, batches: list[ImportBatch]) -> None:
        """Copy, verify and catalog every batch until done or stopped."""
        for rel, batch in batches:
            if self._is_cancelled():
                self._state.cancelled = True
                break
            if self._run_batch(rel, batch):
                break

    def _run_batch(self, rel: str, batch: list[Path]) -> bool:
        """Import one destination batch; return True to stop the run."""
        state = self._state
        # Shared guard chain (ordering is the 2026-07-30 incident
        # ordering — see ``_batch_preflight``); ``None`` means the whole
        # batch was refused and booked failed.
        dest_folder = _batch_preflight(
            state, self._emit, self._db, rel=rel, batch=batch,
            queued=self._queued, ctx=self._ctx,
            missing_root_check=self._missing_root_check,
        )
        if dest_folder is None:
            return False

        # Field rationale lives on ``_ImportBatchState``; the rsync-only
        # trio stays empty on the local transport.
        batch_st = _ImportBatchState(
            rel=rel, dest_folder=dest_folder,
            companion_siblings=_companion_siblings(batch),
        )

        for source_file in batch:
            if not self._feed_file(batch_st, rel, dest_folder, source_file):
                break

        self._recheck_mount_and_roll_back(batch_st, rel)

        # Transfer the queued batch — the transport owns the whole
        # phase, including the ``to_transfer``/``cancelled`` guard.
        # Called unconditionally: a no-op on the local transport (copies
        # land inside ``enqueue``; ``to_transfer`` is always empty) and
        # whenever nothing was queued or Stop was observed.
        self._transport.flush_batch(
            state, batch_st, rel=rel, checker=self._checker)

        self._catalog_batch(batch_st, rel)

        _link_twins_and_emit(
            state, batch_st, self._db, self._workspace_id, self._emit, rel,
            self._queued,
        )
        self._publish_batch_scope(batch_st)
        return state.cancelled

    def _feed_file(self, batch_st: _ImportBatchState, rel: str, dest_folder: str, source_file: Path) -> bool:
        """Gate and enqueue one card file; return False to stop the batch."""
        state = self._state
        if self._is_cancelled():
            state.cancelled = True
            return False
        if not batch_st.mount_lost:
            batch_st.mount_lost = self._unmounted_since_baseline(
                self._ctx.mount_baseline)
        if batch_st.mount_lost:
            state.emitted += 1
            _fail(
                state, rel, source_file,
                f"archive mount root {batch_st.mount_lost} detached while this "
                "batch was in progress (the directory persists but "
                "the share is gone, so neither further writes nor a "
                "duplicate match against it can be trusted)",
            )
            return True
        state.emitted += 1
        self._emit(
            f"{rel}: importing", state.emitted, self._queued, source_file.name,
            is_importing=True,
        )

        verdict = _duplicate_gate(
            state, batch_st, source_file=source_file, rel=rel,
            checker=self._checker, db=self._db, params=self._params,
            ctx=self._ctx, stop_requested=self._stop_requested,
        )
        if verdict is _GATE_CANCELLED:
            # Stop interrupted a destination read inside the gate —
            # nothing further this batch may touch the mount.
            return False
        if verdict is _GATE_SKIPPED:
            return True

        # Per-file dest-safety guard — spec decision 12. See
        # ``_reject_source_backed_dest`` for the geometry it catches
        # beyond the folder-level guard, and for why it FAILS the
        # file where the shared collision walk (which probes its own
        # candidates with the same ``_is_source_backed_dest`` since
        # spec decision 13) merely advances past one. (The
        # primary-name join here is also computed inside each
        # transport's ``enqueue``; the guard only needs the path,
        # not a shared binding.)
        if _reject_source_backed_dest(
            state, self._ctx, rel=rel, source_file=source_file,
            dest_file=os.path.join(dest_folder, source_file.name),
        ):
            return True

        verdict = self._transport.enqueue(
            state, batch_st, source_file=source_file, rel=rel,
            checker=self._checker, stop_requested=self._stop_requested,
        )
        # _ENQ_CANCELLED: Stop was observed inside the transport — either
        # a destination read was interrupted mid-flight (the transport
        # also set ``dest_read_cancelled``, and nothing further this
        # batch may touch the mount) or the walk saw a plain Stop
        # between candidate probes (mount healthy; the batch's earlier
        # landings still get probed and cataloged below). Either way,
        # stop feeding files.
        # _ENQ_HANDLED / _ENQ_QUEUED: the file reached a terminal
        # bucket (or an adopted landing) inside the transport, or its
        # bytes were queued for ``flush_batch`` — next file either way.
        return verdict is not _ENQ_CANCELLED

    def _recheck_mount_and_roll_back(self, batch_st: _ImportBatchState, rel: str) -> None:
        """Probe the mount after the last file; roll the batch back if lost.

        The per-file probe runs BEFORE each copy, so it cannot see a
        detach that happens during the last (or only) file — there is no
        next iteration to catch it. Probe once more here, after the loop
        and before anything is cataloged, so the whole batch including
        its final file is covered. See PR #1396 review (Codex P1
        r3687456172).

        This is the last probe that can help: a detach after this point
        races the catalog scan itself, which is irreducible — no probe
        can make a network filesystem stay mounted across a write. What
        it does guarantee is that nothing is booked as archived without
        a mount check on both sides of every copy in the batch.
        """
        # Skip the probe ONLY when the per-file loop above broke because
        # a destination-side hash was interrupted mid-read
        # (``dest_read_cancelled``): that signal means the mount itself
        # is misbehaving, so probing it here would block for the mount's
        # own timeout and put the job right back in the long
        # "cancelling" state this fix set out to avoid.
        #
        # Do NOT skip on a plain-Stop ``cancelled`` (observed by
        # ``runner.is_cancelled`` at the top of the source-file loop).
        # An earlier file in this same batch may have been copied or
        # adopted before the user hit Stop, and if the archive mount
        # dropped during that just-finished operation the only remaining
        # chance to notice — and to roll ``landed`` / ``dup_skips`` back
        # so the catalog block below doesn't scan a local shadow — is
        # this probe. See PR #1423 review (Codex P2 r3716581282,
        # r3716581283).
        if not batch_st.mount_lost and not batch_st.dest_read_cancelled:
            batch_st.mount_lost = self._unmounted_since_baseline(
                self._ctx.mount_baseline)

        # A detach invalidates this batch's duplicate skips, adoptions,
        # and copies alike — see ``_rollback_on_mount_loss`` for the
        # per-bucket rationale and the load-bearing rollback order.
        # (At this point ``landed`` holds copies on the local transport
        # but only adoptions on rsync — fresh transfers append inside
        # ``flush_batch`` below.)
        if batch_st.mount_lost:
            _rollback_on_mount_loss(
                self._state, batch_st, self.attests_bytes,
                # Books failures for queued-but-untransferred files and
                # empties the queue; a no-op on the local transport
                # (copies land in ``enqueue``, so ``to_transfer`` is
                # always empty).
                extra_rollback=functools.partial(
                    _drop_queued_transfers, self._state, batch_st, rel),
                db=self._db,
                workspace_id=self._workspace_id,
            )

    def _catalog_batch(self, batch_st: _ImportBatchState, rel: str) -> None:
        """Record, catalog, validate and stamp this batch's landings.

        Runs even when cancelled mid-batch: what landed on disk must be
        cataloged before we stop, so every stopping point is a valid
        catalog state. Bounded by the batch size, so no cancel_check is
        passed — it runs to completion.

        Skip when a destination-side hash in the per-file loop above
        cancelled mid-read. That signal means the mount is misbehaving,
        and ``scan()`` here — plus the ``_rehash_dest_or_none``
        re-checks below — would touch the same wedged mount and pin the
        job in "cancelling" for the mount's own timeout. Already-copied
        landings are picked up by the next run's crash-recovery
        adoption (byte-identical files match by hash and count as
        ``skipped_duplicate``). A plain user Stop on a healthy mount
        leaves ``dest_read_cancelled`` False, so partially-landed
        batches keep cataloging like before. See PR #1423 review
        (Codex P2 r3716433824, r3716433830).
        """
        state = self._state
        db = self._db
        params = self._params
        attests_bytes = self.attests_bytes
        self._record_landings_or_fail(batch_st, rel)
        if not batch_st.landed or batch_st.dest_read_cancelled:
            return
        pre_scan_hashes = _catalog_scan_and_prescan(
            state, batch_st, db, params, self._scan, self._ctx.destination,
            rel, attests_bytes, runner=self._runner, job=self._job,
        )

        # RAW rows whose derived caches need invalidation because a
        # newly-landed JPEG became (or already was) their companion.
        # Pair-scan merges the JPEG's identity into the RAW row and
        # deletes the JPEG's own photos row, so the JPEG's landed
        # entry has ``row is None`` and never enters the
        # ``pre_scan_hashes`` diff loop below. But the RAW's
        # ``working_copy_path``/thumb/preview may have been built
        # from stale companion bytes (JPEG was deleted then this
        # import restored it with different content, or the RAW was
        # standalone before and pairing now changes preview
        # strategy), and the deferred ``_extract_working_copies``
        # skips rows whose ``working_copy_path IS NOT NULL``. Without
        # invalidation the UI keeps serving derived files for the
        # previous companion state. Collected regardless of origin —
        # adoption (``origin == "skipped_duplicate"``) only proves
        # the JPEG bytes were already at the archive path, NOT that
        # the RAW row already carried ``companion_path`` for this
        # JPEG (see the companion accept branch in
        # ``_stamp_landed_and_validate_catalog``). See PR #1107
        # review.
        raw_companion_invalidations = _stamp_landed_and_validate_catalog(
            state, batch_st, db, params, rel,
            attests_bytes=attests_bytes,
            dest_noun=self._transport.dest_noun,
            stop_requested=self._stop_requested,
        )

        # Invalidate derived caches for any landed row whose bytes
        # differ from what was there pre-scan. The batch scan passes
        # ``vireo_dir`` through, so scanner's own
        # ``_invalidate_derived_caches`` already fires on rows it
        # detects as content-changed; this loop is defense-in-depth
        # for legacy rows and codepath changes the scanner misses
        # (see the ``pre_scan_hashes`` capture comment above), and
        # is idempotent with scanner's call. Without it, imports
        # that restore a replaced-then-deleted archive file could
        # leave stale ``working_copy_path``/thumb/preview files
        # pointing at the previous bytes, and the deferred
        # end-of-run ``_extract_working_copies`` skips rows whose
        # ``working_copy_path`` is already set — so the WC never
        # rebuilds against the new archive bytes. See PR #1107 review.
        _invalidate_changed_and_sweep(
            state, batch_st, db, params, pre_scan_hashes,
            raw_companion_invalidations,
        )

        # Accumulate the card-source mapping for the deferred
        # end-of-run ``_extract_working_copies`` call. Extraction
        # cannot run here per-batch: a RAW+JPEG companion pair that
        # straddles a batch boundary would still be unpaired at this
        # point, and the extractor would read the RAW before scan()
        # in a later batch pairs the JPEG — poisoning the row with a
        # failure marker or low-quality WC that the candidate
        # predicate then skips.
        _fill_wc_overrides(state, batch_st, params)

    def _record_landings_or_fail(self, batch_st: _ImportBatchState, rel: str) -> None:
        """Record this batch's landings for a resume before cataloging them.

        A Stop while waiting to record them books them failed and empties
        ``landed``, so ``_catalog_batch`` skips the catalog.
        """
        state = self._state
        if (
            batch_st.landed and not batch_st.dest_read_cancelled
            and not _record_landed_files(
                self._job, self._db, state, batch_st, self._source_snapshots,
                self._runner,
            )
        ):
            # Stopped while waiting to record the landings. Never catalog
            # them unrecorded: book them failed (the bytes are in the
            # destination, and a retry adopts them) and skip the catalog.
            state.cancelled = True
            for entry in batch_st.landed:
                _reclassify_landed_failed(
                    state, rel, entry,
                    "import stopped before this file could be cataloged; "
                    "it is in the destination, and a retry catalogs it",
                    self.attests_bytes,
                )
            batch_st.landed = []

    def _publish_batch_scope(self, batch_st: _ImportBatchState) -> None:
        """Publish the end-of-batch resume scope.

        A rejected landing is removed from ``state.landed_files`` in
        memory, but its pre-catalog identity is already on the history
        row (``_record_landed_files`` above required a flush). If this
        end-of-batch publish is only best-effort, a crash or transient
        checkpoint failure between now and the next required flush
        would leave the rejected entry on disk — recovery could then
        match its unchanged bad bytes by size/mtime (a merged JPEG
        companion has no cataloged hash to compare) and fold the RAW
        row into the resumed tag/GPS and processing scopes. Wait for
        the removal to land before continuing.
        """
        need_durable = bool(batch_st.reclassified_landed_paths)
        if not _publish_resume_scope(
            self._job, self._db, self._state, self._source_snapshots,
            runner=self._runner, require_flush=need_durable,
        ) and need_durable:
            self._state.cancelled = True
