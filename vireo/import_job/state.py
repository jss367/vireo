"""Import parameters, run and batch state, and the counter bookkeeping
every phase books files through.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from scanner.keywords import SidecarFailures

if TYPE_CHECKING:
    from collections.abc import Collection
    from pathlib import Path

    from import_dedup import DuplicateChecker
    from import_job.import_types import (
        CompanionKey,
        DupToken,
        LandedIdentity,
        QueuedTransfer,
        UnsafeFile,
        WorkingCopySources,
    )

log = logging.getLogger(__name__)


@dataclass
class ImportParams:
    """Parameters for an import job run."""

    sources: list[str]
    destination: str
    folder_template: str = "%Y/%Y-%m-%d"
    file_types: str = "both"
    skip_duplicates: bool = True
    verify_by_hash: bool = False
    # Fast interactive-import mode: accept a cataloged duplicate candidate
    # from the metadata-first checker without re-reading both complete files.
    # The result reports these separately and never claims the card is safe
    # to format until they have been byte-verified.
    trust_likely_duplicates: bool = False
    recursive: bool = True
    # Per-file selection (copy mode only — see the import-file-selection spec).
    # ``include_paths`` is NOT the set of checked boxes: it is
    # ``previewed - user-deselected`` and deliberately still contains files the
    # UI rendered as unchecked duplicates, so the duplicate checker can see,
    # skip and COUNT them. Dropping them here makes them land in no ledger
    # bucket and falsely reports a fully-archived card as unsafe to format.
    include_paths: Collection[str] | None = None
    # Size of the previewed set and the count the UI showed as checked. Both
    # are transport for values the job cannot reconstruct; ``previewed_count``
    # additionally gates a card-safety condition, so it is not just reporting.
    previewed_count: int | None = None
    checked_count: int | None = None
    # After-import process strategy name. Stored in the job config for the
    # PR 3 chaining hook; unused by the import job itself.
    after_import: str | None = None
    # Remote (SSH) archive destination. When set, the card is
    # rsynced to ``remote_path/subpath`` over SSH instead of copied locally,
    # and photos are cataloged at ``mount_path/subpath`` (which ``destination``
    # is set to). The dict shape (built by ``/api/jobs/import-photos`` from
    # ``config.get_remote_target`` + ``build_remote_move_spec``):
    #   {"rsync_bin": str,
    #    "remote": <build_remote_move_spec dict: host/user/port/ssh_key/
    #               bwlimit_kbps/rsync_bin/ssh_dest_base/mount_dest_base>,
    #    "ssh_base": remote_path/subpath (NAS-side),
    #    "mount_base": mount_path/subpath (== destination)}
    # ``None`` selects the local copy path.
    remote_target: dict[str, Any] | None = None
    # Vireo data dir for working-copy extraction (Task 2.5). None skips
    # extraction (tests, or callers that defer to the scanner backfill).
    vireo_dir: str | None = None
    # Configured thumbnail cache directory (``--thumb-dir``). Independently
    # configurable from ``vireo_dir``: defaulting to ``vireo_dir/thumbnails``
    # silently misses the real cache when they diverge, so an import that
    # replaces bytes at an existing archive path would clear working copies
    # and previews but leave a stale thumbnail served by the UI. Callers with
    # the configured value (Flask ``/api/jobs/import-photos``) should pass
    # it; ``None`` falls back to the default location downstream.
    thumb_cache_dir: str | None = None
    # Photo IDs a recovery retry carries from its parent import (already
    # validated against the parent's scope). The run finishes their
    # working copies, which an interrupted parent may never have reached.
    carry_photo_ids: list[int] | None = None
    # Set only when resuming an interrupted import: the files the
    # interrupted run (and any interrupted run it resumed) recorded as
    # landed before cataloging them, ``{dest_path: [size, mtime_ns]}``. A
    # photo row whose file is still that landing is that run's, whether or
    # not its checkpoint got to record the photo id. See
    # ``_recover_parent_landings``.
    recover_landed_files: dict[str, LandedIdentity] | None = None


@dataclass(frozen=True)
class _LandedFile:
    """One file this batch landed (fresh copy/transfer) or adopted.

    ``verified_hash`` is the hash the import attests is at ``dest_path``
    (copy-time hash locally; card-side hash remotely). Never None: the
    ledger convention for zero-byte files is ``EMPTY_FILE_SHA256``,
    normalized at the construction sites (``checker.content_hash``
    returns None for size-0; scan's photo-row convention is NULL —
    consumers that compare against row hashes normalize EMPTY → None
    themselves). ``origin`` is "copied" or "skipped_duplicate"
    (adoption) and drives rollback accounting in
    ``_reclassify_landed_failed``.
    """
    dest_path: str
    verified_hash: str
    source_path: str
    origin: str
    src_size: int | None
    src_mtime_ns: int | None


@dataclass
class _ImportRunState:
    """Run-scoped mutable state shared by the import batch loop and its
    helpers. One instance per job run; batch-scoped state (``landed``,
    ``dup_skips``, collision maps, …) deliberately stays in function
    locals until the PR 6 phase extraction. ``log_label`` keeps the two
    paths' failure log lines byte-identical after the helper hoist.
    """
    log_label: str
    copied: int = 0
    verified: int = 0
    skipped_duplicate: int = 0
    unverified_duplicate: int = 0
    failed: int = 0
    emitted: int = 0
    cancelled: bool = False
    # None or a mount-root path string; consumers are truthiness-only
    # plus one f-string interpolation of the path into a failure reason.
    mount_ever_lost: str | None = None
    dup_link_failed: bool = False
    unsafe_files: list[UnsafeFile] = field(default_factory=list)
    folder_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    discovery_errors: list[OSError] = field(default_factory=list)
    imported_photo_ids: set[int] = field(default_factory=set)
    # Fingerprints of ``imported_photo_ids`` captured so far for the
    # resume scope (see ``_publish_resume_scope``).
    resume_fingerprints: dict[str, str] = field(default_factory=dict)
    # Rows an interrupted parent landed (see ``_recover_parent_landings``).
    recovered_photo_ids: set[int] = field(default_factory=set)
    # Files this run landed, ``{dest_path: [size, mtime_ns]}``, recorded
    # (and flushed to the history row) before each batch is cataloged, so
    # a resume can tell this run's rows from anyone else's. See
    # ``_record_landed_files``.
    landed_files: dict[str, LandedIdentity] = field(default_factory=dict)
    linked_dup_dirs: set[str] = field(default_factory=set)
    run_dest_folders: dict[DupToken, str] = field(default_factory=dict)
    run_verified_hashes: dict[DupToken, str | None] = field(default_factory=dict)
    wc_source_paths: WorkingCopySources = field(default_factory=dict)
    wc_dest_folders: set[str] = field(default_factory=set)
    # What the deferred working-copy pass could not do, for the result's
    # notice: the photos it failed (cataloged paths) and the error that
    # stopped the whole pass, if one did.
    wc_failed_paths: list[str] = field(default_factory=list)
    wc_pass_error: str | None = None
    # One line per sidecar whose keywords a batch scan could not import
    # (``ScanHooks.sidecar_error``). The photo was imported but is missing
    # those keywords: a failed item for the job's verdict, not for the card
    # (``safe_to_format`` is about the originals).
    sidecar_failures: SidecarFailures = field(default_factory=SidecarFailures)


@dataclass
class _ImportBatchState:
    """Batch-scoped mutable state for one ``(rel, batch)`` iteration.

    One instance per batch; ``dest_folder`` is filled in after
    ``_batch_preflight`` succeeds. The last three fields are remote-only
    (the rsync transport's queue/collision bookkeeping) and stay empty
    on the local path until PR 7 merges the loops.
    """
    rel: str
    dest_folder: str
    # ``_LandedFile`` entries for this batch's landed files — fresh
    # copies/transfers (``origin="copied"``; the remote path appends
    # after per-file verification in its rsync block) plus
    # byte-identical files already present at the destination (the
    # crash-recovery adoption path, ``origin="skipped_duplicate"``).
    # ``source_path`` feeds working-copy extraction so it reads local
    # card bytes, never the just-written archive copy; on the remote
    # path ``verified_hash`` is the card-side src_hash so the
    # catalog-stamping loop can cross-check the scanned MOUNT row
    # against the bytes we confirmed. Populated starting from the
    # per-file loop because the adoption branch inside it appends here.
    landed: list[_LandedFile] = field(default_factory=list)
    # Twin folders (under destination) whose bytes we RE-HASHED this run
    # and confirmed against source hashes — safe to scan/link into the
    # active workspace after this batch's fresh-scan runs.
    dup_dirs: set[str] = field(default_factory=set)
    # Accepted duplicate skips for this batch —
    # (source_file, counted_unverified). A skip asserts the archive
    # already holds these bytes, which stops being true the moment the
    # share detaches: the twin it matched may be a shadow file on the
    # persistent mount stub left by an earlier failed import. These
    # never enter ``landed``, so they need their own rollback — without
    # it a duplicate-only batch satisfies copied + skipped_duplicate ==
    # discovered and reports safe_to_format True over an archive
    # holding none of the bytes.
    dup_skips: list[tuple[Path, bool]] = field(default_factory=list)
    # dest_paths the post-scan cross-checks reclassified from
    # copied/skipped_duplicate to failed. The entries stay in ``landed``
    # (mutating a list during its own iteration is error-prone), so the
    # downstream readers — the derived-cache diff loop and the
    # working-copy override fill — skip these paths instead of acting
    # on bytes the ledger no longer vouches for: otherwise the deferred
    # ``_extract_working_copies`` would read card-side bytes for a
    # photo whose catalog row is missing (JPEG-pair miss aside) or
    # whose archive bytes no longer match what we copied, and cache a
    # working copy that doesn't correspond to what the rest of the app
    # sees at the archive path.
    reclassified_landed_paths: set[str] = field(default_factory=set)
    # Sticky once tripped: a batch can hold hundreds of files (the
    # 2026-07-26 folder held 200), and when it is the only batch there
    # is no later batch boundary to catch a detach. Probing per file
    # keeps the blast radius at one file instead of a whole folder; one
    # ismount call is nothing next to copying and hashing a RAW. The
    # remote copy is one rsync per batch rather than a per-file write,
    # but the duplicate/adoption decisions happen per file and each one
    # reads the mount — so the mount has to be re-checked at that
    # granularity there too.
    mount_lost: str | None = None
    # Sticky signal that a destination-side hash in the per-file loop
    # was cancelled mid-read (``DestReadCancelled``). Any such cancel
    # is evidence the mount is misbehaving, so the post-loop catalog
    # block MUST skip its ``scan()`` / ``_hash_dest_file`` /
    # ``_rehash_dest_or_none`` calls on the same paths — they would hit
    # the same wedged mount and pin the job in "cancelling" for the
    # mount's own timeout. A plain user Stop on a healthy mount leaves
    # this False (the catalog runs normally so partially-landed batches
    # stay cataloged the way ``test_cancel_leaves_valid_partial_catalog``
    # expects).
    dest_read_cancelled: bool = False
    # --- Remote-only trio (unused on the local path until PR 7) ------
    # Files queued for the batch rsync:
    # (source_file, dest_basename, src_hash, src_size, src_mtime_ns).
    to_transfer: list[QueuedTransfer] = field(default_factory=list)
    # dest basename -> src_hash, for intra-batch same-basename collision
    # resolution (FIX 2). Populated as files are queued/skipped.
    claimed_basenames: dict[str, str | None] = field(default_factory=dict)
    # src_hash set, for intra-batch same-content different-basename
    # dedup: the local path calls ``_record_checker`` inside its own
    # batch loop (right after each copy_and_hash_verify), so a byte-
    # identical second file in the same loop sees the first landing
    # via ``run_dest_folders`` and is skipped. The remote path
    # decouples "decide to copy" (this batch loop) from "actually
    # copied" (the post-loop rsync), so a byte-identical second file
    # would otherwise sail past the empty ``_seen_hashes`` and get
    # queued/rsynced/cataloged again. Track queued src hashes here
    # to catch that case at enqueue time — the file that was already
    # queued backs this skip.
    queued_src_hashes: dict[str, str] = field(default_factory=dict)
    # Same-stem card files (IMG_0001.CR3 + IMG_0001.JPG) must keep one
    # shared stem at the destination, or a collision rename splits the
    # pair (IMG_0001_1.CR3 beside IMG_0001.JPG) and the scan's companion
    # pairing matches the JPEG to an unrelated RAW that owns the original
    # stem. ``companion_siblings``: companion key -> this batch's source
    # files sharing it (only groups of 2+). ``companion_slots``: companion
    # key -> the numeric suffix (0 = original name) the group's first
    # landed member took, which later members try first.
    companion_siblings: dict[CompanionKey, list[Path]] = field(default_factory=dict)
    companion_slots: dict[CompanionKey, int] = field(default_factory=dict)
    # Workspace-photo grants this batch newly inserted via
    # ``photo_visibility.grant_verified_twins_tracked``, with the folders it promoted
    # from ``status='missing'`` to ``'ok'`` in the same call. A mount-loss
    # rollback reverts both — the twins the grants rested on may be shadow
    # files on the detached mount stub, so a failed batch must not change
    # workspace visibility or folder reachability. Pre-existing grants
    # from an earlier batch (or an earlier run) are deliberately excluded
    # by the tracked grant path. See ``_rollback_on_mount_loss``.
    dup_granted_photo_ids: list[int] = field(default_factory=list)
    dup_promoted_folder_ids: list[int] = field(default_factory=list)


def _counts(state: _ImportRunState, rel: str) -> dict[str, int]:
    return state.folder_counts.setdefault(
        rel, {"copied": 0, "skipped_duplicate": 0, "failed": 0},
    )


def _fail(state: _ImportRunState, rel: str, source_file: str | Path, reason: str) -> None:
    state.failed += 1
    _counts(state, rel)["failed"] += 1
    state.unsafe_files.append({"path": str(source_file), "reason": reason})
    log.warning("%s failed for %s: %s", state.log_label, source_file, reason)


def _reclassify_landed_failed(state: _ImportRunState, rel: str, entry: _LandedFile, reason: str,
                              attests_bytes: bool) -> None:
    """Move a landed file's count from copied/skipped_duplicate to failed.

    A landed entry has already been booked into ``copied`` (fresh copy)
    or ``skipped_duplicate`` (crash-recovery adopt) at the moment its
    bytes were verified on disk. When a later step in the batch pass
    (scan itself failing, a missing catalog row after scan, or a
    hash mismatch against what scan re-hashed) forces this file into
    the ``failed`` bucket, the origin count must be rolled back —
    otherwise the exactly-one-terminal-bucket invariant breaks and
    ``copied + skipped_duplicate + failed`` exceeds ``discovered``.

    ``attests_bytes``: whether each ``copied`` booking
    also incremented ``verified``, so the rollback must undo both.
    (Named for the transport attribute it becomes; the attribute
    itself lands with the transport classes later in PR 7.)
    """
    dest_path = entry.dest_path
    origin = entry.origin
    if origin == "copied":
        state.copied -= 1
        if attests_bytes:
            state.verified -= 1
        _counts(state, rel)["copied"] -= 1
    elif origin == "skipped_duplicate":
        state.skipped_duplicate -= 1
        _counts(state, rel)["skipped_duplicate"] -= 1
    # A rejected landing is not this run's; keep it out of what a resume
    # may recover (see ``_record_landed_files``).
    state.landed_files.pop(os.path.normpath(dest_path), None)
    _fail(state, rel, dest_path, reason)


def _record_checker(
    state: _ImportRunState, checker: DuplicateChecker | None, source_file: Path, dest_folder: str,
    file_hash: str | None,
) -> None:
    """Register a landed/adopted file's identity with the intra-run checker.

    Without this the checker only sees the pre-run catalog, so a later
    byte-identical card file with a different basename in the same run
    is copied/cataloged again instead of being recognized as an
    intra-run duplicate: every caller passes the
    landed/adopted ``dest_folder`` and verified ``file_hash``, recorded
    unconditionally per token. ``DuplicateChecker.record``
    re-``os.stat``s the source path — on removable media yanked after
    this file's bytes were verified, that raises ``OSError`` and would
    kill the whole background job. Swallow it: the run keeps its
    already-verified landings and only loses the intra-run dedupe
    optimization.
    """
    if checker is None:
        return
    try:
        tokens = checker.record(source_file)
    except OSError as e:
        log.warning(
            "Duplicate-checker record() failed for %s (dest %s): %s",
            source_file, dest_folder, e,
        )
        return
    for tok in tokens:
        state.run_dest_folders[tok] = dest_folder
        state.run_verified_hashes[tok] = file_hash


def _fail_whole_batch(state: _ImportRunState, rel: str, batch: list[Path], reason: str) -> None:
    """Book every file of a batch ``_batch_preflight`` refused as failed.

    Each file also counts as emitted: ``emitted`` otherwise only advances
    inside the per-file loop, which a refused batch skips, and a progress
    bar frozen at the last copied file reads as "still working" while the
    rest of the card is quietly failing (spec decision 2). The caller
    emits the refusal's progress phase.
    """
    for source_file in batch:
        state.emitted += 1
        _fail(state, rel, source_file, reason)
