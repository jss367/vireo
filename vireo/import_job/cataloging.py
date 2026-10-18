"""Per-batch cataloging: the preservation scan, hash stamping and
validation of landed files, derived-cache invalidation, and the
deferred working-copy extraction.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from import_job.copy_verify import DestReadCancelled, _rehash_dest_or_none
from import_job.progress import _IMPORT_ETA_PROGRESS_KEYS
from import_job.state import _reclassify_landed_failed
from job_contract import progress_event
from scanner import EMPTY_FILE_SHA256, ScanControl, ScanOptions

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

    from db import Database
    from import_job.import_types import ScanFunction, StopCheck
    from import_job.state import ImportParams, _ImportBatchState, _ImportRunState, _LandedFile
    from jobs import JobRunner
    from services.service_types import JobDict

log = logging.getLogger(__name__)


def _invalidate_new_images(db: Database, root: str) -> None:
    """Invalidate the /new-images cache for ``root`` after a restricted scan.

    Lazy import so import_job.py stays independent of new_images at
    module-load time (mirrors how pipeline_job.py handles it). A failure
    here must never fail the import — the bytes are on disk and cataloged;
    the cache will re-warm on its next miss.
    """
    try:
        from new_images import invalidate_new_images_after_scan
        invalidate_new_images_after_scan(db, root)
    except Exception:
        log.exception(
            "Failed to invalidate new-images cache for %s", root,
        )


def _catalog_scan_and_prescan(
    state: _ImportRunState, batch_st: _ImportBatchState, db: Database, params: ImportParams,
    scan: ScanFunction, destination: str, rel: str, attests_bytes: bool, *,
    runner: JobRunner | None = None, job: JobDict | None = None,
) -> dict[str, str | None]:
    """Run the restricted per-batch catalog scan for everything in
    ``batch_st.landed`` (fresh copies/transfers AND adoptions), capturing
    each landed path's pre-scan photo-row hash first so the caller's
    derived-cache diff loop can compare. Shared verbatim by both import
    paths; the caller owns the ``if batch_st.landed and not
    batch_st.dest_read_cancelled:`` gate. Returns ``pre_scan_hashes``.
    On scan failure every landed entry is reclassified to failed and
    ``batch_st.landed`` is cleared.

    ``runner`` and ``job``, when supplied, wire the job's PAUSE probe
    into the per-batch ``scan()`` call so a Pause on a pausable import
    (``/api/jobs/import-photos``) unwinds the scanner's process pool
    and drops its CPU lease at the pause checkpoint instead of holding
    permits until the batch finishes.

    Deliberately does NOT wire a cancel probe: the caller's contract
    (run_import_job comment at ~lines 4422-4425) is that this scan is
    the preservation catalog for what already landed on disk, and it
    must run to completion even after Stop so those files reach a
    valid catalog state. Passing a cancel_check that already-fired
    would let ``scan()`` raise ``ScanCancelled`` and the surrounding
    handler would reclassify every landed entry as failed even though
    the bytes remain on disk uncataloged.
    """
    # ``landed`` covers collision-loop adoptions too (origin
    # "skipped_duplicate"), so their photo rows get created by the
    # restricted scan. The explicit file set is also what prevents a
    # duplicate-only import from falling back to a whole-directory
    # discovery walk. See PR #1113 review.
    landed_paths = {entry.dest_path for entry in batch_st.landed}
    # Capture the pre-scan (photo_id, file_hash) for every landed
    # dest_path. Scanner's own ``_invalidate_derived_caches``
    # fires on content-changed rows during the batch scan below
    # (now that ``vireo_dir`` is passed through so pairing keeps
    # its cache context), but the manual invalidation loop below
    # remains as defense-in-depth for the batch-scan's
    # ``skip_working_copies=True`` path: the deferred end-of-run
    # ``_extract_working_copies`` still skips rows with
    # ``working_copy_path IS NOT NULL``, so any stale WC pointer
    # left behind by scanner's own path (e.g. a codepath change,
    # or a legacy row scanner declines to invalidate) would
    # otherwise persist. Idempotent with scanner's call. See PR
    # #1107 review.
    pre_scan_hashes = {}
    for entry in batch_st.landed:
        dest_path = entry.dest_path
        row = db.imports.photo_hash_row(
            os.path.dirname(dest_path),
            os.path.basename(dest_path),
        )
        if row is not None:
            pre_scan_hashes[dest_path] = row["file_hash"]
    try:
        _run_preservation_scan(
            scan, destination, db, batch_st, landed_paths, params,
            runner=runner, job=job,
        )
    except Exception as e:  # scan failure fails the whole batch
        log.warning("Catalog scan failed for import batch %s", rel, exc_info=True)
        # Each entry was already booked into copied or
        # skipped_duplicate — reclassify (roll back origin, add
        # to failed) so the ledger never double-counts.
        for entry in batch_st.landed:
            _reclassify_landed_failed(
                state, rel, entry, f"catalog scan failed: {e}",
                attests_bytes,
            )
        batch_st.landed = []
    else:
        # Restricted scan committed new photo rows and
        # created/linked ``workspace_folders`` entries under
        # ``dest_folder``; the /api/workspaces/active/new-images
        # endpoint serves a cached filesystem diff that will
        # otherwise report the just-imported files as new until
        # the cache expires or another full scan runs. Mirrors
        # api_job_scan / api_job_import_full / pipeline_job.
        _invalidate_new_images(db, batch_st.dest_folder)
    return pre_scan_hashes


def _run_preservation_scan(
    scan: ScanFunction, destination: str, db: Database, batch_st: _ImportBatchState,
    landed_paths: set[str], params: ImportParams, *, runner: JobRunner | None, job: JobDict | None,
) -> None:
    """The restricted per-batch ``scan()`` behind
    ``_catalog_scan_and_prescan``: runs to completion even after Stop,
    with only the PAUSE probe wired (see the comments below).
    """
    # ``vireo_dir`` / ``thumb_cache_dir`` are threaded through
    # so ``_pair_raw_jpeg_companions`` has cache context: when
    # a newly imported RAW pairs with an already-cataloged
    # JPEG that carries an edit recipe with local-mask
    # snapshots, pairing only moves those snapshots to the
    # RAW primary when ``vireo_dir`` is set — passing ``None``
    # silently loses the local pass. ``skip_working_copies``
    # keeps the per-batch WC extraction deferred to the
    # end-of-run pass (per-batch extraction would race
    # RAW+JPEG pairing across batch boundaries). See PR
    # #1107 review.
    #
    # Deliberately NOT incremental: a landed path is not
    # necessarily uncataloged — a stale row can survive for a
    # file deleted off the archive, and if the replacement
    # bytes land at that path carrying an mtime equal to the
    # stale row's, the incremental fast path skips it without
    # comparing size or content. ``file_hash`` then keeps the
    # stale value, the post-scan cross-check compares the
    # copy-time hash against it, and a file that transferred
    # fine is reported failed — with retries unable to refresh
    # the row. ``restrict_files`` already narrows these batches
    # to a handful of paths, so incremental buys nothing here
    # anyway. See PR #1398 review.
    # Deliberately no cancel_check: the caller's contract is that
    # this scan runs to completion even after Stop so already-landed
    # files reach a valid catalog state (run_import_job comment at
    # ~lines 4422-4425). Passing an already-fired probe would let
    # ``scan()`` raise ``ScanCancelled``, and the surrounding
    # handler would reclassify every landed entry as failed while
    # the bytes remain on disk uncataloged.
    #
    # Simply omitting ``cancel_check`` is not enough: ``JobRunner``
    # binds ``cancellation_requested`` as the process-wide resource
    # cancel probe, so scanner's ``_claim_worker_count`` still
    # resolves that probe through ``ResourceLedger.acquire`` and
    # raises ``ResourceWaitCancelled`` even when permits are
    # immediately available. Suppress the binding just for this
    # scan by re-binding to ``None`` — same shape as classify_job's
    # preservation pass in 4be1fa9b / ccd44bc4. ``JobRunner`` still
    # owns hard shutdown via its own deadline. The pause probe is
    # kept, so a user Pause still unwinds the scan pool cleanly.
    #
    # ``cancel_check`` is a parking callback rather than ``None``; see
    # ``_preservation_scan_probes``.
    from resource_ledger import bind_resource_cancel_check
    scan_pause_check, scan_park_only_check = _preservation_scan_probes(
        runner, job,
    )
    with bind_resource_cancel_check(None):
        scan(
            destination, db,
            options=ScanOptions(
                restrict_dirs=[batch_st.dest_folder],
                restrict_files=landed_paths,
                vireo_dir=params.vireo_dir,
                thumb_cache_dir=params.thumb_cache_dir,
                skip_working_copies=True,
            ),
            control=ScanControl(
                cancel_check=scan_park_only_check,
                pause_check=scan_pause_check,
                # Preservation contract: this scan runs to completion
                # regardless of cancel. Passing an always-False pure
                # cancel probe lets the scanner use ``_check_cancelled_no_park``
                # inside ``_claim_worker_count`` — without it a Pause
                # arriving in the race window between the pause probe
                # and the parking ``scan_park_only_check`` would park
                # ``wait_if_paused`` while the hashing pool and CPU
                # permits are still held, blocking unrelated work
                # until Resume.
                cancel_only_check=lambda: False,
            ),
        )


def _preservation_scan_probes(
    runner: JobRunner | None, job: JobDict | None,
) -> tuple[Callable[[], bool] | None, Callable[[], bool] | None]:
    """``(pause_check, park_only_check)`` for the preservation scan.

    Both are None without a runner and job. ``park_only_check`` is the
    scan's ``cancel_check``: a parking callback rather than ``None`` so the
    job actually transitions to ``paused`` when a user pauses mid-scan.
    ``pause_check`` alone is non-blocking: scanner releases its pool and
    then polls ``pause_check`` every 50 ms, leaving the job stuck in
    ``pausing``. The parking callback invokes
    ``wait_if_paused(publish_paused=True)`` to park AND publish the paused
    status, then returns ``False`` unconditionally — the parking
    side-effect matters, the ``cancelled`` return value must be ignored to
    keep the preservation contract intact.
    """
    if runner is None or job is None:
        return None, None
    job_id = job["id"]

    def scan_pause_check() -> bool:
        return runner.pause_requested(job_id)

    def scan_park_only_check() -> bool:
        # Park via wait_if_paused so the job publishes ``paused``
        # while the scanner is safely at its pause checkpoint.
        # Discard the cancellation return value — the preservation
        # contract requires this scan to run to completion.
        runner.wait_if_paused(job_id, publish_paused=True)
        return False

    return scan_pause_check, scan_park_only_check


def _stamp_landed_and_validate_catalog(
    state: _ImportRunState, batch_st: _ImportBatchState, db: Database, params: ImportParams,
    rel: str, *, attests_bytes: bool, dest_noun: str, stop_requested: StopCheck,
) -> set[int]:
    """Cross-check every landed file against what scan() cataloged and
    stamp the integrity verdicts; returns the RAW photo ids whose
    derived caches need invalidation because a landed JPEG became (or
    already was) their companion (the caller passes them to
    ``_invalidate_changed_and_sweep``).

    ``entry.verified_hash`` is the hash this import recorded when the
    bytes landed (copy-time hash locally, card-side hash remotely).
    Catalog integrity is checked in BOTH verify modes; the wording of
    the failure reasons says "recorded", not "verified", because a
    no-verify remote run computed the card-side hash but verified
    nothing destination-side.

    Invariants (spec PR 6b — do not regress):

    - The ``hash_status='ok'`` stamps run ONLY behind ``attests_bytes``
      (``True`` locally, where every copy is hash-verified;
      ``params.verify_by_hash`` remotely) — the stamp is the
      independent byte-attestation, not just "scan and import agree".
    - ``state.imported_photo_ids.add`` runs on EVERY accept path and
      stays OUTSIDE that gate: the after-import chaining hook builds
      its process job collection from these ids even on no-verify runs.
    - The scan-NULL backfill (spec decision 11) excludes
      ``EMPTY_FILE_SHA256``: zero-byte accepts pass ``file_hash=None``
      (``update_photo_hash_check``'s status-only arm), keeping scan's
      NULL — backfilling EMPTY would recreate the every-empty-file
      collision the zero-byte convention exists to prevent.

    ``dest_noun`` ("archive" locally, "mount" remotely) parameterizes
    the user-visible failure reasons; ``stop_requested`` is the
    caller's cancellation poll, threaded into the destination re-reads.
    """
    raw_companion_invalidations = set()
    for entry in batch_st.landed:
        dest_path = entry.dest_path
        row = db.imports.photo_hash_row(
            os.path.dirname(dest_path),
            os.path.basename(dest_path),
        )
        if row is None:
            if not _validate_rowless_landing(
                state, batch_st, db, rel, entry,
                raw_companion_invalidations,
                attests_bytes=attests_bytes, dest_noun=dest_noun,
                stop_requested=stop_requested,
            ):
                break
            continue
        if not _stamp_cataloged_landing(
            state, batch_st, db, rel, entry, row,
            attests_bytes=attests_bytes, dest_noun=dest_noun,
            stop_requested=stop_requested,
        ):
            break
    return raw_companion_invalidations


def _validate_rowless_landing(
    state: _ImportRunState, batch_st: _ImportBatchState, db: Database, rel: str, entry: _LandedFile,
    raw_companion_invalidations: set[int], *, attests_bytes: bool, dest_noun: str,
    stop_requested: StopCheck,
) -> bool:
    """Validate a landed file that scan() left without a photo row of
    its own; returns False when a Stop interrupted the destination
    re-read (``state.cancelled`` is set and the stamping loop ends).
    """
    dest_path = entry.dest_path
    verified_hash = entry.verified_hash
    # RAW+JPEG pairing merges the JPEG's photo row into
    # the RAW primary (companion_path) and deletes the
    # JPEG's own row, so a landed JPEG whose sibling RAW
    # was scanned in the same batch legitimately has no
    # row of its own. Look it up as another row's
    # companion_path before deciding "not cataloged".
    # See PR #1107/#1113 reviews.
    companion = db.imports.photo_with_companion(
        os.path.dirname(dest_path),
        os.path.basename(dest_path),
    )
    if companion is None:
        _reclassify_landed_failed(
            state, rel, entry,
            "not cataloged after scan (no photo row)",
            attests_bytes,
        )
        batch_st.reclassified_landed_paths.add(dest_path)
        return True
    # The paired JPEG's own row is gone by design,
    # so ``_stamp_cataloged_landing`` can't cross-check its
    # bytes. Re-read the destination file here in
    # BOTH verify modes — paired JPEGs need the same
    # stale-destination catalog-integrity guard or
    # after-import processing would enqueue against
    # the wrong companion. ``None`` from the re-read
    # unambiguously means unreadable:
    # ``verified_hash`` is never None (zero-byte
    # normalizes to ``EMPTY_FILE_SHA256`` at
    # ``_LandedFile`` construction), so an empty
    # companion whose file vanished cannot be
    # confused with a legitimately empty one. See
    # PR #1107/#1113/#1437 reviews.
    try:
        actual = _rehash_dest_or_none(
            dest_path, stop_requested)
    except DestReadCancelled:
        state.cancelled = True
        return False
    if actual is not None and actual == verified_hash:
        # Invalidate the RAW's derived caches
        # regardless of origin: adoption only proves
        # the JPEG bytes were already at the dest
        # path, NOT that the RAW row already carried
        # ``companion_path`` for this JPEG. A prior
        # partial run or backfill may have left the
        # RAW as RAW-only, and the deferred
        # end-of-run ``_extract_working_copies``
        # skips rows whose ``working_copy_path IS
        # NOT NULL`` — a stale RAW-only cache would
        # persist past this import otherwise. See
        # PR #1107 review.
        raw_companion_invalidations.add(
            companion["id"],
        )
        # The landed JPEG's bytes are represented on
        # the RAW primary — that row is what the
        # chaining hook should process.
        state.imported_photo_ids.add(companion["id"])
        return True
    _reclassify_landed_failed(
        state, rel, entry,
        f"paired companion {dest_noun} bytes could "
        "not be read"
        if actual is None else
        f"paired companion {dest_noun} bytes do not "
        "match the hash this import recorded "
        f"({dest_noun} base is likely stale or "
        "misconfigured)",
        attests_bytes,
    )
    batch_st.reclassified_landed_paths.add(dest_path)
    return True


def _stamp_cataloged_landing(
    state: _ImportRunState, batch_st: _ImportBatchState, db: Database, rel: str, entry: _LandedFile,
    row: sqlite3.Row, *, attests_bytes: bool, dest_noun: str, stop_requested: StopCheck,
) -> bool:
    """Cross-check a landed file's own photo ``row`` and stamp it; returns
    False when a Stop interrupted the destination re-read
    (``state.cancelled`` is set and the stamping loop ends).
    """
    dest_path = entry.dest_path
    verified_hash = entry.verified_hash
    if row["file_hash"] == verified_hash:
        if attests_bytes:
            db.audit.update_photo_hash_check(
                row["id"], "ok", commit=False,
            )
        state.imported_photo_ids.add(row["id"])
    elif row["file_hash"] is None:
        # scan() legitimately writes NULL for zero-byte
        # files (the convention keeps ``EMPTY_FILE_SHA256``
        # out of ``photos.file_hash``) and can leave NULL
        # when its own read failed (large files, prior
        # partial scan, or a suppressed read error). Don't
        # stamp blind either way: re-read the destination
        # file as the last-line check so a file that
        # vanished, changed, or became unreadable between
        # scan and stamping fails instead of keeping
        # ``hash_status='ok'`` with ``safe_to_format``
        # green. See PR #1107/#1113/#1437 reviews.
        try:
            actual = _rehash_dest_or_none(
                dest_path, stop_requested)
        except DestReadCancelled:
            state.cancelled = True
            return False
        if actual is not None and actual == verified_hash:
            # Backfill the row hash scan couldn't write
            # (spec decision 11) — except the zero-byte
            # case, where ``file_hash=None`` hits
            # ``update_photo_hash_check``'s status-only arm
            # and the row keeps scan's NULL (backfilling
            # EMPTY would recreate the every-empty-file
            # collision the convention exists to prevent).
            if attests_bytes:
                db.audit.update_photo_hash_check(
                    row["id"], "ok",
                    file_hash=(
                        verified_hash
                        if verified_hash != EMPTY_FILE_SHA256
                        else None
                    ),
                    commit=False,
                )
            state.imported_photo_ids.add(row["id"])
        else:
            _reclassify_landed_failed(
                state, rel, entry,
                f"scan wrote no {dest_noun} row hash and a "
                f"re-read of the {dest_noun} file disagrees "
                "with the hash this import recorded "
                f"({dest_noun} file is likely stale, "
                "unreadable, or misconfigured)",
                attests_bytes,
            )
            batch_st.reclassified_landed_paths.add(dest_path)
    else:
        _reclassify_landed_failed(
            state, rel, entry,
            f"scanned {dest_noun} row hash does not match "
            "the hash this import recorded "
            f"({dest_noun} base is likely stale or "
            "misconfigured)",
            attests_bytes,
        )
        batch_st.reclassified_landed_paths.add(dest_path)
    return True


def _invalidate_changed_and_sweep(
    state: _ImportRunState, batch_st: _ImportBatchState, db: Database, params: ImportParams,
    pre_scan_hashes: dict[str, str | None], raw_companion_invalidations: set[int],
) -> None:
    """Invalidate derived caches for landed rows whose content identity
    changed (pre-scan hash differs from the attested hash) and for RAW
    rows that gained a companion this batch, then commit the batch and
    sweep untracked preview files. Shared verbatim by both import paths.
    """
    invalidated_photo_ids = set()
    if params.vireo_dir:
        from scanner import _invalidate_derived_caches
        for entry in batch_st.landed:
            dest_path = entry.dest_path
            if dest_path in batch_st.reclassified_landed_paths:
                continue
            if dest_path not in pre_scan_hashes:
                # No pre-scan row (fresh insert) — no derived
                # caches exist for this photo yet.
                continue
            # A pre-scan row existed. Its ``file_hash`` may be
            # ``NULL`` (legacy row, or a prior scan that couldn't
            # read the file), and such a row can still carry
            # ``working_copy_path``/thumb/preview caches from
            # earlier processing. Scanner's own content-change
            # path treats ``NULL -> concrete hash`` as an
            # invalidating transition (see scanner.scan()'s
            # ``content_identity_changed`` block); mirror that
            # here so restoring a deleted archive file whose
            # legacy row lost its hash still clears the stale
            # derived caches. See PR #1107 review.
            pre_hash = pre_scan_hashes[dest_path]
            verified_hash = entry.verified_hash
            if pre_hash == verified_hash:
                continue
            row = db.imports.photo_hash_row(
                os.path.dirname(dest_path),
                os.path.basename(dest_path),
            )
            if row is None:
                continue
            _invalidate_derived_caches(
                db, params.vireo_dir, row["id"],
                thumb_cache_dir=params.thumb_cache_dir,
            )
            invalidated_photo_ids.add(row["id"])

        # RAW rows whose companion JPEG we just landed fresh —
        # covered by the same untracked-preview sweep below so
        # orphaned preview files from the prior companion state
        # don't get lazy-adopted on the next request.
        for raw_id in raw_companion_invalidations:
            _invalidate_derived_caches(
                db, params.vireo_dir, raw_id,
                thumb_cache_dir=params.thumb_cache_dir,
            )
            invalidated_photo_ids.add(raw_id)

    db.commit()

    if invalidated_photo_ids:
        # Mirror scanner.scan()'s post-loop untracked-preview
        # sweep: orphan preview files with no preview_cache row
        # would be lazy-adopted on the next request and served as
        # stale bytes for the just-replaced archive file.
        from scanner import _sweep_untracked_previews_for_photos
        _sweep_untracked_previews_for_photos(
            db, params.vireo_dir, invalidated_photo_ids,
        )


def _fill_wc_overrides(state: _ImportRunState, batch_st: _ImportBatchState, params: ImportParams) -> None:
    """Record the card-side working-copy source override for every landed
    entry the post-scan checks did not reclassify. Shared verbatim by
    both import paths.
    """
    # Accumulate the card-source mapping for the deferred
    # end-of-run ``_extract_working_copies`` call. Extraction
    # cannot run here per-batch: a RAW+JPEG companion pair that
    # straddles a batch boundary would still be unpaired at this
    # point, and the extractor would read the RAW before scan()
    # in a later batch pairs the JPEG — poisoning the row with a
    # failure marker or low-quality WC that the candidate
    # predicate then skips.
    if params.vireo_dir:
        for entry in batch_st.landed:
            dest_path = entry.dest_path
            if dest_path in batch_st.reclassified_landed_paths:
                # Reclassified to failed by the post-scan cross-checks
                # (missing row, or archive/mount-vs-attested hash
                # mismatch). Skipping the card override lets the WC
                # extractor fall back to whatever the destination
                # currently holds — matching the catalog's view —
                # instead of caching a WC of bytes the ledger no
                # longer vouches for.
                continue
            src_path = entry.source_path
            exp_size = entry.src_size
            exp_mtime_ns = entry.src_mtime_ns
            state.wc_source_paths[dest_path] = (
                src_path, exp_size, exp_mtime_ns,
            )
        state.wc_dest_folders.add(batch_st.dest_folder)


def _extract_deferred_working_copies(
    state: _ImportRunState, params: ImportParams, runner: JobRunner, job: JobDict, db: Database,
) -> None:
    """One working-copy extraction pass over every folder this run
    touched, after all batches have landed and been paired. Shared
    verbatim by both import paths.
    """
    # Reads card-side bytes for any dest_path present in
    # ``wc_source_paths``; anything else (crash-recovery adopted files
    # whose card is gone, later backfill retries) falls back to the
    # cataloged archive path. Per-row failures mark the photo for the
    # scanner's later backfill and never fail the import.
    #
    # If the run was already cancelled at a batch boundary, skip the pass
    # entirely — otherwise Stop appears hung for minutes on large RAW
    # imports while the extractor decodes what the user asked us to
    # abort. During the pass, poll ``runner.is_cancelled`` so cancellation
    # aborts extraction row-by-row too.
    if params.vireo_dir and state.wc_dest_folders and not state.cancelled:
        from scanner import _extract_working_copies

        def _working_copy_status(
            message: str, phase_current: int | None = None, phase_total: int | None = None,
            phase_label: str | None = None,
        ) -> None:
            """Expose deferred extraction without replacing import totals."""
            extra = {
                "phase_current": phase_current,
                "phase_total": phase_total,
                "phase_label": phase_label or "Generating working copies",
                **{
                    key: job["progress"][key]
                    for key in _IMPORT_ETA_PROGRESS_KEYS
                    if key in job["progress"]
                },
            }
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

        try:
            _extract_working_copies(
                db, params.vireo_dir,
                scope=[(d, "exact") for d in sorted(state.wc_dest_folders)],
                source_paths=state.wc_source_paths,
                status_callback=_working_copy_status,
                cancel_check=lambda: runner.is_cancelled(job["id"]),
            )
        except Exception:
            log.exception(
                "Working-copy extraction failed for %s",
                sorted(state.wc_dest_folders),
            )
        if runner.is_cancelled(job["id"]):
            state.cancelled = True
