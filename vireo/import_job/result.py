"""The import's final result and its safe-to-format verdict."""

from __future__ import annotations

from typing import TYPE_CHECKING

from image_loader import SUPPORTED_EXTENSIONS
from import_job.file_selection import (
    _append_selection_unsafe,
    _selection_blocks_format,
    _selection_summary,
)
from import_job.snapshots import _capture_photo_fingerprints
from scanner.working_copies import import_working_copy_notice

if TYPE_CHECKING:
    from db import Database
    from import_job.import_types import SourceSnapshot
    from import_job.planning import _ImportPlan
    from import_job.state import ImportParams, _ImportRunState
    from jobs import JobRunner
    from services.service_types import JobDict, JsonDict


def _finalize_import(job: JobDict, runner: JobRunner, db: Database, state: _ImportRunState, params: ImportParams, *,
                     discovered: int, include_paths: set[str] | None, source_snapshots: dict[str, SourceSnapshot],
                     deselected: int, vanished_paths: set[str], appeared: int,
                     remote_unverified: bool = False) -> JsonDict:
    """Terminal status, safety verdicts, and the shared 19-key result dict.

    ``remote_unverified`` is the one transport-required term: the remote
    path passes ``not params.verify_by_hash`` (see the honesty-gate
    comment in ``_append_run_unsafe_files``); the local path hash-verifies every
    copy, so it calls with the default ``False`` and every
    ``remote_unverified`` term degenerates to a no-op.
    """
    status = "cancelled" if state.cancelled else (
        "failed" if state.failed else "completed"
    )
    summary = _selection_summary(
        params, include_paths, discovered=discovered, copied=state.copied,
        skipped_duplicate=state.skipped_duplicate, failed=state.failed,
    ) + _working_copy_summary_suffix(state)
    sidecar_note = state.sidecar_failures.note()
    if sidecar_note:
        summary += f", {sidecar_note}"
    # Cancellation takes precedence over the sidecar-failure status: Stop
    # accepted after the preservation scan recorded an unreadable sidecar
    # settles the job as cancelled, so the step must report cancelled too
    # — not a contradictory failed step inside a cancelled job. Matches
    # the scan step in services/scan_work.py and services/import_in_place.py.
    if status == "cancelled":
        step_status = "cancelled"
    elif status == "failed" or sidecar_note:
        step_status = "failed"
    else:
        step_status = "completed"
    runner.update_step(
        job["id"], "import",
        status=step_status,
        summary=summary,
    )

    _append_run_unsafe_files(
        state, discovered=discovered, deselected=deselected,
        vanished_paths=vanished_paths, appeared=appeared,
        remote_unverified=remote_unverified,
    )

    # Safe to format iff every discovered file reached a verified
    # terminal bucket: hash-verified fresh copy, or duplicate whose bytes
    # verifiably exist (hash-backed match, or key match re-hashed against
    # its cataloged twin), AND every source was walked cleanly, AND every
    # duplicate-only batch's direct workspace link succeeded (otherwise the
    # imported duplicates are on disk but not visible in the workspace),
    # AND the run enumerated the card's full supported-file set. Any
    # narrowing of the walk falls into ``partial_scope``: a narrowed
    # ``file_types`` ("raw", "jpeg", or a custom extension list) leaves
    # the un-selected supported photos on the card entirely unseen, and
    # ``recursive=False`` skips every subdirectory of every source root.
    # In both cases ``discovered`` covers only a subset of what the card
    # actually holds, so the naive ``copied + skipped_duplicate ==
    # discovered`` check would go green even though the card still holds
    # files the pill is expected to cover. A cancelled run leaves
    # unprocessed files, so it is never safe. This pill means exactly
    # what it says.
    partial_scope = _import_partial_scope(params)
    safe_to_format = (
        not state.cancelled
        and state.failed == 0
        and not state.discovery_errors
        and not state.dup_link_failed
        and not partial_scope
        and not remote_unverified
        and state.unverified_duplicate == 0
        and (state.copied + state.skipped_duplicate) == discovered
        and not _selection_blocks_format(
            deselected=deselected, vanished_paths=vanished_paths)
    )
    # ``unverified_duplicate`` requires ``not verify_by_hash``, which is
    # exactly ``remote_unverified``, so this block is unreachable on the
    # remote path today. The selection condition is wired anyway: the mutual
    # exclusion is incidental, and a change to either flag would silently
    # reopen the hole.
    unverified_duplicates_only = (
        state.unverified_duplicate > 0
        and not state.cancelled
        and state.failed == 0
        and not state.discovery_errors
        and not state.dup_link_failed
        and not partial_scope
        and not remote_unverified
        and (state.copied + state.skipped_duplicate) == discovered
        and not _selection_blocks_format(
            deselected=deselected, vanished_paths=vanished_paths)
    )
    return _import_result(
        db, state, params, discovered=discovered,
        source_snapshots=source_snapshots, safe_to_format=safe_to_format,
        unverified_duplicates_only=unverified_duplicates_only,
        vanished_paths=vanished_paths, appeared=appeared,
    )


def _working_copy_summary_suffix(state: _ImportRunState) -> str:
    """The import step's note on working copies it could not make, or ''.

    The step summary is the line the Jobs page lists the import under, so
    the shortfall shows there too; the result's notice has the details.
    """
    parts = []
    if state.wc_failed_paths:
        n = len(state.wc_failed_paths)
        parts.append(f"{n:,} photo{'' if n == 1 else 's'} without a working copy")
    if state.wc_pass_error:
        parts.append("working copies stopped early")
    return "".join(f", {part}" for part in parts)


def _append_run_unsafe_files(state: _ImportRunState, *, discovered: int, deselected: int,
                             vanished_paths: set[str], appeared: int, remote_unverified: bool) -> None:
    """Append the run-level ``unsafe_files`` entries, in render order:
    discovery errors, the remote honesty gate, likely duplicates, then
    selection drift.
    """
    # Discovery/enumeration errors must flip safe_to_format off — a
    # permission-denied subtree yields no files (``discovered`` shrinks),
    # so a naive check of ``copied + skipped_duplicate == discovered``
    # would still pass and the UI would tell the user it's safe to format
    # a card whose contents were never verified. Surface each error into
    # ``unsafe_files`` (path = the enumeration failure's own filename when
    # available, otherwise ``<discovery>``) so the caller can show what
    # went unseen.
    for exc in state.discovery_errors:
        state.unsafe_files.append({
            "path": str(getattr(exc, "filename", None) or "<discovery>"),
            "reason": f"source enumeration failed: {exc}",
        })

    # Honesty gate: a remote import is only safe to format when every
    # discovered file was INDEPENDENTLY hash-confirmed at the destination —
    # which only happens on the checksum-verification path. Without
    # verify_by_hash the transfer relied on rsync's own integrity checking,
    # which we do not surface as a format-the-card guarantee. Report exactly
    # that with the plan's reason string.
    if remote_unverified and discovered > 0:
        state.unsafe_files.append({
            "path": "<remote>",
            "reason": "enable verify_by_hash for remote verification",
        })
    if state.unverified_duplicate:
        state.unsafe_files.append({
            "path": "Likely duplicates",
            "reason": (
                f"{state.unverified_duplicate} matched by filename, byte size, "
                "and capture time but were not compared byte-for-byte"
            ),
        })
    # Selection drift entries.
    _append_selection_unsafe(
        state.unsafe_files, deselected=deselected, vanished_paths=vanished_paths,
        appeared=appeared,
    )


def _import_partial_scope(params: ImportParams) -> bool:
    """True when the run's walk covers less than the card's full
    supported-file set (see ``safe_to_format`` in ``_finalize_import``).
    """
    # A list-form ``file_types`` whose members cover every
    # ``SUPPORTED_EXTENSIONS`` entry is NOT actually filtered — the
    # pipeline UI's ``getIngestFileTypes()`` returns exactly this shape
    # when the user checks every box, and ``discover_source_files``
    # walks it identically to ``"both"``. Treating it as partial would
    # leave ``safe_to_format`` permanently false over an unfiltered
    # import. Normalize to leading-dot lowercase to match how
    # SUPPORTED_EXTENSIONS is stored; unknown extensions in the list
    # are ignored (they can't be in SUPPORTED_EXTENSIONS regardless).
    # See PR #1107 review.
    partial_scope = not params.recursive
    if params.file_types != "both":
        if isinstance(params.file_types, list):
            normalized_types = {
                ("." + e.lower().lstrip("."))
                for e in params.file_types
                if isinstance(e, str) and e
            }
            partial_scope = partial_scope or not SUPPORTED_EXTENSIONS.issubset(
                normalized_types,
            )
        else:
            partial_scope = True
    return partial_scope


def _with_working_copy_notice(result: JsonDict, state: _ImportRunState) -> JsonDict:
    """Add the working-copy notice when the deferred pass fell short.

    A notice, not a failure: ``ok``, ``errors`` and the card-safety verdict
    describe the originals, which were imported either way. Marking the
    import failed would skip after-import processing and offer a Retry that
    has nothing to copy. Absent when every working copy was made.
    """
    notice = import_working_copy_notice(state.wc_failed_paths, state.wc_pass_error)
    if notice:
        result["working_copy_notice"] = notice
    if state.wc_failed_paths:
        result["working_copy_failures"] = list(state.wc_failed_paths)
    return result


def _import_result(
    db: Database, state: _ImportRunState, params: ImportParams, *, discovered: int,
    source_snapshots: dict[str, SourceSnapshot], safe_to_format: bool,
    unverified_duplicates_only: bool, vanished_paths: set[str], appeared: int,
) -> JsonDict:
    """The import's result dict, shared by both transports."""
    return _with_working_copy_notice(_import_result_fields(
        db, state, params, discovered=discovered,
        source_snapshots=source_snapshots, safe_to_format=safe_to_format,
        unverified_duplicates_only=unverified_duplicates_only,
        vanished_paths=vanished_paths, appeared=appeared,
    ), state)


def _import_result_fields(
    db: Database, state: _ImportRunState, params: ImportParams, *, discovered: int,
    source_snapshots: dict[str, SourceSnapshot], safe_to_format: bool,
    unverified_duplicates_only: bool, vanished_paths: set[str], appeared: int,
) -> JsonDict:
    """Every key of the import's result except the working-copy notice."""
    photos_ok = (
        state.failed == 0
        and not state.discovery_errors
        and not state.dup_link_failed
    )
    sidecar_failures = list(state.sidecar_failures.messages)
    return {
        "discovered": discovered,
        "copied": state.copied,
        "verified": state.verified,
        # Photo rows the after-import chaining hook should process.
        # Duplicate-only imports intentionally return an empty list so
        # ``_chain_after_import`` skips into its "no new photos" branch
        # instead of enqueueing an empty process run — same convention as
        # the local path. Without this the remote import always missed
        # after-import processing. See PR #1113 review.
        "photo_ids": sorted(state.imported_photo_ids),
        # Photos an interrupted parent landed that its carry list lacked
        # (``_recover_parent_landings``); the caller carries them too, into
        # tags, the collection and processing.
        "recovered_photo_ids": sorted(
            state.recovered_photo_ids
            - state.imported_photo_ids
            - set(params.carry_photo_ids or ()),
        ),
        # See ``_record_landed_files``; a resume of a later interruption
        # in this retry chain inherits them through its parent's config.
        "landed_files": dict(state.landed_files),
        # Every row found at the interrupted parent's recorded landings,
        # including ones this run adopted into ``photo_ids`` (duplicate
        # skipping off); the caller uses it to tag only what is owed.
        "parent_landing_ids": sorted(state.recovered_photo_ids),
        # Stable-identity map so a recovery retry can verify each carried
        # ID still points at the same file. Without this the retry
        # authorizes any current photo row that happens to share an ID
        # with something the parent landed — an especially real risk
        # after users delete recent imports (SQLite reuses the freed
        # IDs on the next insert). Includes ``recovered_photo_ids``
        # (rows a resume recovered by path from the interrupted
        # parent): a subsequent retry inherits those ids too, and
        # ``_validate_parent_import_job`` needs an expected fingerprint
        # for each — otherwise the ``expected is None`` path accepts an
        # unrelated row whose id SQLite reused after a delete.
        "photo_fingerprints": _capture_photo_fingerprints(
            db, state.imported_photo_ids | state.recovered_photo_ids,
        ),
        # Per-source signature over the discovered file set so a
        # recovery retry can detect a source whose contents changed
        # between the failed run and the retry — e.g. a different SD
        # card mounted at the same path, or new photos added to the
        # same card. Captured at DISCOVERY time (see the ``source_snapshots``
        # capture in ``_plan_import``); recording it here instead
        # would let a mid-copy card ejection stamp ``-1`` sizes and
        # refuse a legitimate reinsert-and-retry recovery.
        "source_snapshots": source_snapshots,
        "skipped_duplicate": state.skipped_duplicate,
        "unverified_duplicate": state.unverified_duplicate,
        "unverified_duplicates_only": unverified_duplicates_only,
        "failed": state.failed,
        "safe_to_format": safe_to_format,
        "unsafe_files": state.unsafe_files,
        "folders": state.folder_counts,
        "cancelled": state.cancelled,
        "discovery_errors": len(state.discovery_errors),
        # Selection drift, for the caller's readout. ``files_appeared`` is a
        # clamped net delta (card size minus previewed count), so it reads 0
        # — never negative — when more files vanished than arrived.
        "files_appeared": appeared,
        "files_vanished": len(vanished_paths),
        # JobRunner's mixed-outcome convention: a run with any failed
        # file, unseen source subtree, workspace-link failure, or sidecar
        # whose keywords were not imported is recorded "failed" (with
        # per-file / per-operation reasons), never "completed".
        "ok": photos_ok and not sidecar_failures,
        # Every photo was imported, sidecar keywords aside. After-import
        # processing keys on this: the photos are cataloged either way.
        "photos_ok": photos_ok,
        "errors": [
            f"{u['path']}: {u['reason']}" for u in state.unsafe_files
        ] + sidecar_failures,
        "sidecar_failures": sidecar_failures,
    }


def _finalize_planned(
    job: JobDict, runner: JobRunner, db: Database, state: _ImportRunState, params: ImportParams,
    plan: _ImportPlan, *, remote_unverified: bool,
) -> JsonDict:
    """``_finalize_import`` for a run that got through ``_plan_import``."""
    return _finalize_import(
        job, runner, db, state, params,
        discovered=plan.discovered, include_paths=plan.include_paths,
        source_snapshots=plan.source_snapshots, deselected=plan.deselected,
        vanished_paths=plan.vanished_paths, appeared=plan.appeared,
        remote_unverified=remote_unverified,
    )
