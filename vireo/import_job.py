"""Import job: copy card -> archive directly, hash-verify, catalog incrementally.

Implements the import half of the import/process split (design doc
2026-07-04-import-process-split-design.md). The core invariant:

    A photo row is created only when its file verifiably exists at its
    final archive path.

Files are copied per destination-folder batch, each copy is verified by
content hash before promotion, and each batch is cataloged via the
scanner's restricted-scan path immediately after it lands. A run that
dies at any point leaves a valid partial catalog; a retry's duplicate
gate skips exactly the files that landed and copies the rest. The caller
may supply managed temporary storage for a process-before-archive run,
then chain a verified move to the final destination. This job always
catalogs the supplied destination; failed runs keep those local originals.

Reconnaissance notes (Task 2.0, verified 2026-07-04):

1. ``scanner.scan()`` computes and writes ``photos.file_hash`` itself
   (``_compute_file_features`` hashes every new/changed file). It does
   NOT write ``hash_status``/``hash_checked_at`` — those belong to the
   integrity-audit vocabulary, so the import job stamps them after each
   batch's scan, gated on the scan-computed hash matching the hash this
   job verified at copy time (a free cross-check: a mismatch means the
   destination changed between copy and scan and the file is bucketed
   as failed instead of silently trusted).
2. ``CatalogIndex`` retains only identity sets (hashes/keys/sizes), not
   paths. Key-match twin resolution for the safe-to-format ledger uses
   a direct DB query on (filename, file_size) + stored_metadata_key
   equality, joining folders for the archive path.
3. Cancellation mirrors the scan job: the work function polls
   ``runner.is_cancelled(job_id)`` at batch boundaries and passes
   ``cancel_check`` into ``scan()``; the runner flips the job status to
   "cancelled" when the work function returns after a Stop. The remote
   path additionally passes ``cancel_check`` into the per-batch rsync so
   Stop kills the subprocess mid-transfer instead of waiting out the
   batch — the interrupted batch is cancelled work (nothing failed,
   nothing cataloged), recovered like a mid-batch crash. Destination-side
   hash reads (duplicate gate, crash-recovery adopt, post-scan re-checks)
   go through ``_hash_dest_file``, which watches the same Stop signal and
   a stall watchdog — a read blocked on a dead SMB mount can otherwise
   pin the worker for the mount's own multi-minute timeout per file while
   cancellation goes unobserved.
4. Batch unit: files grouped by destination (template) folder,
   processed in template order, chunked to at most
   ``IMPORT_BATCH_SIZE`` files per scan call. Restricted scans only
   enumerate the files the batch actually landed, so per-batch scan cost
   tracks the batch, not the archive tree. Duplicate-matched folders are
   linked to the active workspace directly from their existing catalog
   rows. They are deliberately NOT scanned as part of the import: even an
   incremental scan must enumerate/stat every entry, which can block a
   zero-copy import for hours on SMB. Uncataloged strays and ``partial``
   folder health remain visible for the explicit folder-rescan workflow;
   archive repair is not part of the import's critical path.
"""

import contextlib
import errno
import functools
import hashlib
import json
import logging
import os
import posixpath
import shutil
import stat as stat_mod
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

# POSIX advisory lock used by the hardlinkless-FS promote fallback (see
# copy_and_hash_verify below). Unavailable on Windows; Vireo targets
# macOS/Linux so this import normally succeeds. If it fails, the
# fallback promote path degrades gracefully to the previous
# check-then-rename behavior (documented in that block).
try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

from db import Database
from image_loader import SUPPORTED_EXTENSIONS
from import_dedup import (
    CatalogIndex,
    DuplicateChecker,
    compute_file_hash,
    stored_metadata_key,
)
from ingest import (
    _source_file_timestamps,
    build_destination_path,
    discover_source_files,
)
from job_contract import progress_event

# Shared with the pipeline readiness copy so "1 file" / "2 files" reads the
# same way everywhere the UI states a count. pipeline_plan imports only
# stdlib at module level, so this does not cycle back through import_job.
from pipeline_plan import _plural
from scanner import EMPTY_FILE_SHA256

log = logging.getLogger(__name__)


def _invalidate_new_images(db, root):
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


# Batch unit (Task 2.0 Q4): files sharing a destination folder, chunked so
# one scan call never covers more than this many fresh files. Copy, verify,
# scan, and hash stamping all commit at batch boundaries, so every stopping
# point (cancel, crash, yanked card) leaves a valid catalog.
IMPORT_BATCH_SIZE = 200
_IMPORT_ETA_PROGRESS_KEYS = (
    "eta_state", "eta_settled", "eta_seconds", "eta_rate_per_min",
)

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

    def __init__(self, clock=None, expected_new=None):
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
    def _smooth(previous, measured):
        if previous is None:
            return measured
        return 0.4 * previous + 0.6 * measured

    def note_importing(self, copied):
        """Start timing the current batch on its first per-file event."""
        if self._batch_started_at is None:
            self._batch_started_at = self._clock()
            self._batch_start_settled = self._settled
            self._batch_start_copied = copied

    def note_batch_complete(self, current, copied):
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

    def fields(self, total):
        """Return JSON-safe telemetry for the progress event and step."""
        result = {
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


def _capture_source_snapshots(files, sources):
    """Return ``{source_str: {"count": N, "signature": HEX}}`` per source.

    Persisted in the import result so a recovery retry can verify each
    source's file set still matches what the parent ran against. Without
    this check, retrying against ``/mnt/card`` after the card was
    ejected and a different one mounted at the same path would silently
    import every file on the new card (they have no prior catalog
    entry, so ``skip_duplicates`` doesn't gate them). Files added to
    the same card between the failed run and the retry produce the
    same mismatch — the "Retry failed files" button should not import
    files the user's original import never saw.

    Signature is a sha256 over the sorted ``(relative_posix_path,
    size, mtime_ns)`` list of files under each source root; a healthy
    file reports its ``st_size`` and ``st_mtime_ns``, an unreadable
    file renders both as ``-1`` so a stat failure produces a distinct
    signature from a successful stat at the same path. ``st_mtime_ns``
    is included alongside size so a same-size in-place replacement
    (edit that preserves byte count, or a rewritten SD-card file at
    the identical path and length) is caught by the drift check —
    ``size`` alone would treat it as unchanged and let the retry copy
    the new bytes as if they were the parent's failed files. Files
    not under a source root (empty in practice — discovery only yields
    paths under the source) are skipped.

    Called from the import job at DISCOVERY time, before any copy work
    starts, so the persisted snapshot reflects the source as the
    parent first observed it. Snapshotting at completion time instead
    would let a card ejected or momentarily unreadable mid-copy record
    ``-1`` for successfully-discovered files, then refuse the natural
    "reinsert the card and retry" recovery even though the source is
    unchanged.
    """
    if not sources:
        return {}
    snapshots = {}
    for src in sources:
        src_path = Path(src)
        src_str = str(src)
        # Deduplicate by relative path so overlapping sources (a case the
        # Import page explicitly supports — e.g. selecting both ``/card``
        # and ``/card/DCIM``) don't double-count nested files. The combined
        # ``files`` list contains each nested file twice (once from each
        # enumeration), so a plain append would put two identical entries
        # in this source's snapshot; the retry re-enumerates each source
        # alone and produces one entry per file, so the parent's doubled
        # snapshot would refuse an unchanged card. Keying by ``rel.as_posix()``
        # collapses the twin discoveries into the single-source view retry
        # validation reconstructs. See PR #1387 Codex review.
        entries_by_rel = {}
        for f in files:
            try:
                rel = f.relative_to(src_path)
            except ValueError:
                continue
            rel_str = rel.as_posix()
            if rel_str in entries_by_rel:
                continue
            try:
                st = f.stat()
                size = st.st_size
                mtime_ns = st.st_mtime_ns
            except OSError:
                size = -1
                mtime_ns = -1
            entries_by_rel[rel_str] = (rel_str, size, mtime_ns)
        entries = sorted(entries_by_rel.values())
        payload = json.dumps(entries, separators=(",", ":"))
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        snapshots[src_str] = {"count": len(entries), "signature": digest}
    return snapshots


def _fingerprint_for_row(row):
    """Format one photo row into a stable identity string.

    Format: ``folder_path/filename|s=SIZE|h=HASH``. Path alone is not
    enough: SQLite is free to reuse a freed ``photos.id`` on the next
    insert, so a delete-then-import cycle can put an unrelated file at
    the same path under the same numeric ID. ``file_size`` (always
    populated by the copy pass) and ``file_hash`` (populated by
    ``scanner.scan`` after the copy verifies) together identify the
    file's bytes; a genuine retry sees identical values, a same-path
    imposter almost never does. Missing hash/size render as empty
    segments so the format stays comparable between parent-capture time
    and retry-verify time even when the scanner hasn't stamped a hash
    (both sides observe the same NULL and match). Returns ``None`` when
    the row is missing the path fields the retry needs to compare
    against.
    """
    folder_path = row["folder_path"] or ""
    filename = row["filename"] or ""
    if not folder_path or not filename:
        return None
    size = row["file_size"]
    file_hash = row["file_hash"] or ""
    size_str = "" if size is None else str(size)
    return f"{folder_path}/{filename}|s={size_str}|h={file_hash}"


def _capture_photo_fingerprints(db, photo_ids):
    """Capture ``{id: fingerprint_string}`` for each landed photo.

    Persisted in the import result so a retry can verify each carried ID
    is still the same file. ``photos.id`` is an ``INTEGER PRIMARY KEY``
    without ``AUTOINCREMENT``, so a delete-then-import cycle can reuse
    an ID for an unrelated photo; without a stable-identity check the
    retry's after-import chain (and any ``after_process_move``) would
    silently pick up that unrelated row. See ``_fingerprint_for_row``
    for the string format. Keys are stringified for JSON storage.
    """
    if not photo_ids:
        return {}
    fingerprints = {}
    for chunk in _chunks(sorted(int(pid) for pid in photo_ids)):
        placeholders = ",".join("?" for _ in chunk)
        rows = db.conn.execute(
            f"""SELECT p.id AS id,
                       f.path AS folder_path,
                       p.filename AS filename,
                       p.file_size AS file_size,
                       p.file_hash AS file_hash
                FROM photos p
                JOIN folders f ON f.id = p.folder_id
                WHERE p.id IN ({placeholders})""",
            list(chunk),
        ).fetchall()
        for row in rows:
            fp = _fingerprint_for_row(row)
            if fp is None:
                continue
            fingerprints[str(row["id"])] = fp
    return fingerprints


def _chunks(items, size=500):
    """Yield ``items`` split into lists of up to ``size`` elements.

    Kept module-private so callers here don't reach into ``db._chunks``.
    500 keeps well under SQLite's default 999 bound-parameter cap while
    holding fingerprint round-trips to one per few hundred photos.
    """
    buf = []
    for item in items:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


# Case-folded matching is unconditional on darwin/win32 (the OS enforces
# case-insensitive filesystems). On Linux we probe each source's actual
# filesystem: a FAT/exFAT/NTFS-mounted SD card is case-insensitive even
# under a case-sensitive ext4 parent, so a platform-wide check would miss
# a differently-cased twin path there. See PR #1107 review.
_CASE_INSENSITIVE_PLATFORM = sys.platform in ("darwin", "win32")


def _fs_is_case_insensitive(path):
    """Probe whether the filesystem at ``path`` treats case as insensitive.

    List an entry inside ``path`` and check whether accessing it under a
    case-swapped name resolves to the same inode. Probing *inside* the
    directory (rather than swapping characters in ``path`` itself) is
    essential when a case-insensitive mount sits under a case-sensitive
    parent — a FAT/exFAT SD card mounted at ``/mnt/Card`` on Linux under
    an ext4 root: the ext4 ``/mnt`` cannot resolve ``/Mnt`` or a
    differently-cased ``Card`` entry (mount-point dentries live in the
    parent FS), so swapping characters in the ``path`` string always
    reports case-sensitive regardless of the card's own semantics.

    Any inconclusive result (unlistable, empty, no alpha-containing entry
    — Nikon-style ``100``/``101``/``102`` roots — or a stat error while
    comparing) returns True so the caller falls back to case-fold,
    mirroring the ``/api/jobs/import-photos`` route guard. False on
    inconclusive would let a differently-cased catalog twin under a
    source pass duplicate acceptance (or a differently-cased twin folder
    under the destination skip workspace linking), and
    ``safe_to_format`` could then go green without a visible off-card
    copy. See PR #1107 review.
    """
    try:
        entries = os.listdir(path)
    except OSError:
        return True
    for name in entries:
        for i, c in enumerate(name):
            if c.isalpha():
                swapped = name[:i] + c.swapcase() + name[i + 1:]
                if swapped == name:
                    continue
                original_full = os.path.join(path, name)
                probe_full = os.path.join(path, swapped)
                if not os.path.exists(probe_full):
                    return False
                try:
                    return os.path.samefile(original_full, probe_full)
                except OSError:
                    return True
    return True


def _build_source_root_guard(sources):
    """Return ``path_under_any_source(path) -> bool`` for the given roots.

    Shared by both the local and remote duplicate gates to reject
    cataloged twins that live under the card being imported. A stale scan
    of a mounted card can leave a photos row whose ``folder_path`` IS the
    card; re-hashing that twin just re-reads the very card file being
    imported, so accepting it as duplicate proof would flip
    ``safe_to_format`` green while the card holds the only bytes. Only an
    off-card twin can back a duplicate skip. See PR #1107 review.
    """
    def _norm(s):
        try:
            real = os.path.realpath(s)
        except OSError:
            real = str(s)
        ci = _CASE_INSENSITIVE_PLATFORM or _fs_is_case_insensitive(real)
        return (real.casefold() if ci else real).rstrip(os.sep), ci

    roots = [_norm(s) for s in sources]

    def path_under_any_source(path):
        try:
            real = os.path.realpath(path)
        except OSError:
            real = str(path)
        real_folded = real.casefold()
        for root, ci in roots:
            if not root:
                continue
            cmp = real_folded if ci else real
            if cmp == root or cmp.startswith(root + os.sep):
                return True
        return False

    return path_under_any_source


@dataclass
class ImportParams:
    """Parameters for an import job run."""

    sources: list
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
    include_paths: set | None = None
    # Size of the previewed set and the count the UI showed as checked. Both
    # are transport for values the job cannot reconstruct; ``previewed_count``
    # additionally gates a card-safety condition, so it is not just reporting.
    previewed_count: int | None = None
    checked_count: int | None = None
    # After-import process strategy name. Stored in the job config for the
    # PR 3 chaining hook; unused by the import job itself.
    after_import: str | None = None
    # Remote (SSH) archive destination (Task 2.7). When set, the card is
    # rsynced to ``remote_path/subpath`` over SSH instead of copied locally,
    # and photos are cataloged at ``mount_path/subpath`` (which ``destination``
    # is set to). The dict shape (built by ``/api/jobs/import-photos`` from
    # ``config.get_remote_target`` + ``build_remote_move_spec``):
    #   {"rsync_bin": str,
    #    "remote": <build_remote_move_spec dict: host/user/port/ssh_key/
    #               bwlimit_kbps/rsync_bin/ssh_dest_base/mount_dest_base>,
    #    "ssh_base": remote_path/subpath (NAS-side),
    #    "mount_base": mount_path/subpath (== destination)}
    # ``None`` keeps the local copy path unchanged.
    remote_target: dict | None = None
    # Vireo data dir for working-copy extraction (Task 2.5). None skips
    # extraction (tests, or callers that defer to the scanner backfill).
    vireo_dir: str | None = None
    # Configured thumbnail cache directory (``--thumb-dir``). Independently
    # configurable from ``vireo_dir``: defaulting to ``vireo_dir/thumbnails``
    # silently misses the real cache when they diverge, so an import that
    # replaces bytes at an existing archive path would clear working copies
    # and previews but leave a stale thumbnail served by the UI. Callers with
    # the configured value (Flask ``/api/jobs/import-photos``) should pass
    # it; ``None`` falls back to the default location downstream. See PR
    # #1107 review.
    thumb_cache_dir: str | None = None


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
    unsafe_files: list = field(default_factory=list)  # [{path, reason}]
    folder_counts: dict = field(default_factory=dict)
    discovery_errors: list = field(default_factory=list)
    imported_photo_ids: set = field(default_factory=set)
    linked_dup_dirs: set = field(default_factory=set)
    run_dest_folders: dict = field(default_factory=dict)
    run_verified_hashes: dict = field(default_factory=dict)
    wc_source_paths: dict = field(default_factory=dict)
    wc_dest_folders: set = field(default_factory=set)


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
    landed: list = field(default_factory=list)
    # Twin folders (under destination) whose bytes we RE-HASHED this run
    # and confirmed against source hashes — safe to scan/link into the
    # active workspace after this batch's fresh-scan runs. See PR #1113
    # review.
    dup_dirs: set = field(default_factory=set)
    # Accepted duplicate skips for this batch —
    # (source_file, counted_unverified). A skip asserts the archive
    # already holds these bytes, which stops being true the moment the
    # share detaches: the twin it matched may be a shadow file on the
    # persistent mount stub left by an earlier failed import. These
    # never enter ``landed``, so they need their own rollback — without
    # it a duplicate-only batch satisfies copied + skipped_duplicate ==
    # discovered and reports safe_to_format True over an archive
    # holding none of the bytes. See PR #1396 review (Codex P1
    # r3687506040; remote fold r3688498501 / r3688501706).
    dup_skips: list = field(default_factory=list)
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
    # sees at the archive path. See PR #1107 review.
    reclassified_landed_paths: set = field(default_factory=set)
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
    # mount's own timeout, exactly the failure mode this PR set out to
    # eliminate. A plain user Stop on a healthy mount leaves this False
    # (the catalog runs normally so partially-landed batches stay
    # cataloged the way ``test_cancel_leaves_valid_partial_catalog``
    # expects). See PR #1423 review (Codex P2 r3716433824 /
    # r3716433830).
    dest_read_cancelled: bool = False
    # --- Remote-only trio (unused on the local path until PR 7) ------
    # Files queued for the batch rsync:
    # (source_file, dest_basename, src_hash, src_size, src_mtime_ns).
    to_transfer: list = field(default_factory=list)
    # dest basename -> src_hash, for intra-batch same-basename collision
    # resolution (FIX 2). Populated as files are queued/skipped.
    claimed_basenames: dict = field(default_factory=dict)
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
    # queued backs this skip. See PR #1113 review.
    queued_src_hashes: dict = field(default_factory=dict)


def _counts(state, rel):
    return state.folder_counts.setdefault(
        rel, {"copied": 0, "skipped_duplicate": 0, "failed": 0},
    )


def _fail(state, rel, source_file, reason):
    state.failed += 1
    _counts(state, rel)["failed"] += 1
    state.unsafe_files.append({"path": str(source_file), "reason": reason})
    log.warning("%s failed for %s: %s", state.log_label, source_file, reason)


def _reclassify_landed_failed(state, rel, entry, reason,
                              attests_bytes):
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
    _fail(state, rel, dest_path, reason)


def _record_checker(state, checker, source_file, dest_folder, file_hash):
    """Register a landed/adopted file's identity with the intra-run checker.

    Without this the checker only sees the pre-run catalog, so a later
    byte-identical card file with a different basename in the same run
    is copied/cataloged again instead of being recognized as an
    intra-run duplicate (spec decision 5): every caller passes the
    landed/adopted ``dest_folder`` and verified ``file_hash``, recorded
    unconditionally per token. ``DuplicateChecker.record``
    re-``os.stat``s the source path — on removable media yanked after
    this file's bytes were verified, that raises ``OSError`` and would
    kill the whole background job. Swallow it: the run keeps its
    already-verified landings and only loses the intra-run dedupe
    optimization. See PR #1113 review.
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


def _batch_preflight(state, emit, db, *, rel, batch, queued, ctx,
                     missing_root_check):
    """Run the per-batch guard chain; return ``dest_folder`` or ``None``.

    ``None`` means the whole batch was refused — every file booked
    failed with a specific reason and progress emitted — and the caller
    must ``continue`` to the next batch.

    ORDER IS LOAD-BEARING (the 2026-07-30 incident ordering): sticky
    mount-loss refusal → dest-under-source → missing mount root → stale
    mount → makedirs → folder-status promotion. Each step assumes the
    ones above it already passed — in particular ``os.makedirs`` must
    not run until every mount check has, or it recreates the vacated
    mount point as a local shadow directory. Extracted as ONE function,
    never five, so the sequence cannot be partially reused out of order.
    """
    # Function-level import: pipeline_job imports from this module.
    from pipeline_job import _unmounted_since_baseline

    # A detach observed in an earlier batch is sticky for the rest of
    # the run: the intra-run duplicate cache and the job-wide checker
    # hold identities for files whose archive claim was rolled back,
    # and consulting them against a remounted share would count fresh
    # card files as duplicates of copies that never completed. Refuse
    # every remaining file rather than risk a stale-cache hit.
    # See PR #1400 review (Codex P2 r3688614624).
    if state.mount_ever_lost:
        for source_file in batch:
            state.emitted += 1
            _fail(
                state, rel, source_file,
                f"archive mount root {state.mount_ever_lost} detached "
                "earlier in this import; the intra-run duplicate "
                "cache still holds identities for files whose "
                "archive claim was rolled back, so no further batch "
                "can be trusted to consult it",
            )
        emit(
            f"{rel}: archive unmounted", state.emitted, queued,
        )
        return None

    # Normalize so the "/" strftime puts in ``rel`` (e.g. "2026/07-03")
    # lines up with what scanner stores. Scanner wraps paths in
    # ``Path(...)`` before writing the folder row and building its
    # restrict_files set, which on Windows rewrites mid-path "/" to
    # "\\"; a raw os.path.join here would leave copied files invisible
    # to the restricted scan and unfindable in the post-scan lookup.
    dest_folder = (
        os.path.normpath(os.path.join(ctx.destination, rel))
        if rel != "." else ctx.destination
    )
    # Reject the whole batch before creating any directories on the
    # card. The per-file guard (``_reject_source_backed_dest``, both
    # paths) already refuses ``dest_file``
    # under a source, but that check runs AFTER ``os.makedirs``, so
    # a rejected unsafe import would still create the archive
    # directory tree on the source (or raise on read-only media,
    # killing the background job instead of returning a controlled
    # unsafe result). This mirror at the batch boundary keeps the
    # failure quiet and preserves the ``ok`` field for the API.
    # See PR #1107 review.
    #
    # On the remote path the same guard also protects the duplicate
    # gate: when the mount base is an ancestor of a selected source and
    # the folder template maps back into that source folder,
    # ``dest_folder`` (and therefore every ``cand_mount`` under it)
    # resolves inside a source root. The per-file collision walk would
    # hash those source-backed candidates, byte-match them against the
    # card, and count them as ``skipped_duplicate`` — with
    # ``verify_by_hash=true`` that would let ``safe_to_format`` go
    # green over a card whose bytes never crossed the network. See PR
    # #1113 review. (Since spec decision 13 the shared walk refuses
    # source-backed candidates on its own, so that is now defense in
    # depth rather than the only guard — but this batch-level refusal
    # is still the one that keeps ``os.makedirs`` off the card, and it
    # is still the honest verdict for a geometry where EVERY candidate
    # is poisoned rather than just one entry.)
    if ctx.path_under_any_source(dest_folder):
        for source_file in batch:
            # Count these as emitted so the progress bar reflects the
            # rejected batch instead of freezing at the last copied
            # file — spec decision 2.
            state.emitted += 1
            _fail(
                state, rel, source_file,
                "destination folder resolves inside a source directory "
                "(dest_folder would be created under the card being "
                "imported); formatting the card would erase the archive "
                "copy",
            )
        emit(
            f"{rel}: {_counts(state, rel)['copied']} copied · "
            f"{_counts(state, rel)['skipped_duplicate']} already present",
            state.emitted, queued,
        )
        return None
    # Mount-root check: if the archive's mount root has gone (share
    # never attached, or unmounted mid-run), refuse the batch instead
    # of letting ``os.makedirs`` recreate the vacated mount point as a
    # plain local directory. That shadow tree would take the card's
    # bytes onto the internal disk, look like a successful import, and
    # then hide the moment the real share remounts over it. Probed per
    # batch rather than once up front: a card import runs for hours
    # against a network archive, and the share can drop *during* the
    # run — a Tailscale/SMB archive did exactly that on 2026-07-30
    # (unmounted two hours into an import, after which ``os.makedirs``
    # walked straight into the vacated mount point), and only a
    # root-owned ``/Volumes`` turned the shadow write into a crash
    # instead of silent data misplacement. A start-of-job preflight
    # cannot see a mid-run drop; the probe is a couple of
    # ``os.path.lexists`` calls on the mount root, so paying it once
    # per destination folder is free next to the copy work.
    #
    # On the remote (SSH) path the stakes are the same shape: the SSH
    # rsync still writes to the NAS, but the subsequent scan reads the
    # fresh local shadow and leaves the import uncataloged/failed;
    # worse, on macOS/Linux that shadow root can also prevent the real
    # share from remounting at the configured path. ``missing_root_check``
    # wraps the pipeline path's ``_missing_archive_mount_root`` helper
    # (only fires for the ``/Volumes/X``, ``/mnt/X``, and
    # ``/media/user/X`` shapes that denote removable/network mount
    # roots). See PR #1113 review.
    missing_mount_root = missing_root_check()
    if missing_mount_root:
        for source_file in batch:
            # Count these as emitted: ``emitted`` otherwise only
            # advances inside the per-file loop, which this branch
            # skips, and a progress bar frozen at the last copied
            # file reads as "still working" while the rest of the
            # card is quietly failing.
            state.emitted += 1
            _fail(
                state, rel, source_file,
                f"archive mount root {missing_mount_root} is not "
                "available (destination drive is not mounted; refusing "
                "to create a shadow directory tree under it, which "
                "would prevent the real share from remounting)",
            )
        # Specific refusal phase — spec decision 3.
        emit(
            f"{rel}: archive unavailable", state.emitted, queued,
        )
        return None
    # The check above only sees a mount point that VANISHED. Linux
    # keeps ``/mnt/<name>`` as an empty directory after the share
    # detaches, so there the destination still "exists" and
    # ``os.makedirs`` below would happily build the archive tree on
    # the system disk and copy the card into it — bytes that vanish
    # under the real share the moment it remounts, after
    # safe_to_format may already have gone green. Catch that by
    # comparing against the baseline taken at job start: only a
    # mounted → unmounted transition counts, so an ordinary local
    # directory that merely looks mount-shaped is never refused.
    # Matters more on the remote path, not less: rsync keeps pushing
    # to the NAS while the batch scan reads a local shadow, so the
    # import lands bytes remotely and catalogs nothing.
    stale_mount_root = _unmounted_since_baseline(ctx.mount_baseline)
    if stale_mount_root:
        for source_file in batch:
            state.emitted += 1
            _fail(
                state, rel, source_file,
                f"archive mount root {stale_mount_root} is no longer "
                "mounted (it was at the start of this import; the "
                "directory persists but the share has detached, so "
                "copying here would write to the local disk under a "
                "stale mount point)",
            )
        emit(
            f"{rel}: archive unmounted", state.emitted, queued,
        )
        return None
    # A stale-but-registered mount (ismount still true while reads
    # fail), a read-only or permission-changed parent, or a full
    # disk all still surface here — and an uncaught OSError out of
    # this call tears down the whole background job, which is exactly
    # how the 2026-07-30 import died after two hours. Whatever the
    # cause, book it as a per-file failure and keep the card marked
    # unsafe to format instead of crashing the run.
    try:
        os.makedirs(dest_folder, exist_ok=True)
    except OSError as e:
        for source_file in batch:
            state.emitted += 1
            _fail(
                state, rel, source_file,
                f"could not create destination folder {dest_folder}: {e}",
            )
        emit(
            f"{rel}: destination unavailable", state.emitted, queued,
        )
        return None

    # Promote any pre-existing folder row for this destination out of
    # ``'missing'``. Standalone scans run ``check_folder_health()`` as
    # their preflight, so a reattached archive drive transitions
    # ``missing`` → ``ok`` before its files become visible in the
    # workspace again. The import path calls ``scanner.scan()``
    # directly, and scan's success stamp only clears ``'partial'``
    # (see ``_update_folder_status(only_from_partial=True)``), so a
    # folder row still marked ``'missing'`` (e.g. from a prior health
    # check when the NAS mount was absent) would keep the archive
    # drive's photos filtered out of workspace queries even after this
    # import successfully lands and hash-stamps files into it, and
    # safe_to_format could go green over folders the UI won't show.
    # We just makedirs'd ``dest_folder`` so the path definitely exists;
    # any row still labelled ``'missing'`` is stale. Preserve
    # ``'partial'`` (a real prior-scan signal that the folder needs a
    # rescan). See PR #1107 / PR #1113 reviews.
    db.conn.execute(
        "UPDATE folders SET status = 'ok' "
        "WHERE path = ? AND status = 'missing'",
        (dest_folder,),
    )
    db.conn.commit()

    return dest_folder


def _is_source_backed_dest(ctx, source_file, dest_path):
    """True when ``dest_path`` IS the source file or resolves under any
    source root — i.e. writing to it, or adopting it, would attest bytes
    the card still owns.

    ONE pair of probes, shared by the orchestrator's per-file guard (spec
    decision 12, which fails the file) and the collision walk's candidate
    check (decision 13, which advances past it). They disagreed on nothing
    before; keeping them one function is what stops them from ever
    starting to. Note that both probes stat/realpath the destination, so
    the walk pays them only for candidates that actually exist.

    KNOWN-REMAINING: a candidate hardlinked to a DIFFERENT card file has
    no path- or inode-visible tie to *this* source — ``samefile`` is False
    and its realpath is its own path — so neither probe sees it. Catching
    that needs ``st_dev`` comparison against the source roots; out of
    scope for PR 7b and recorded in spec row 13.
    """
    # Reject the source-under-destination overlap where the folder
    # template maps the source right back to its own directory
    # (e.g. source ``/archive/2026/2026-07-05``, destination
    # ``/archive``, template ``%Y/%Y-%m-%d`` → dest_path IS the
    # source file). The API rejects destinations INSIDE any source;
    # this catches the opposite direction, where the destination is
    # a legal ancestor but the template resolves back to the source
    # directory. Without this the collision walk's adopt branch hashes
    # the source against itself, records it as ``skipped_duplicate``,
    # and safe_to_format goes green — deleting/formatting the
    # source then erases the only copy. See PR #1107 review.
    try:
        same_file = (
            os.path.exists(dest_path)
            and os.path.samefile(str(source_file), dest_path)
        )
    except OSError:
        # Fall back to normalized-path equality when samefile can't
        # stat (e.g. the destination is a stale entry). Prefer a
        # false positive here over a false negative that lets the
        # adopt branch loop back onto the source itself.
        same_file = (
            os.path.normpath(str(source_file))
            == os.path.normpath(dest_path)
        )
    if same_file:
        return True
    # Also reject any dest_path (not just an exact self-copy) that
    # resolves under any source root. Example: source
    # ``/Volumes/Card/DCIM``, destination ``/Volumes/Card``,
    # template ``DCIM/Archive/%Y`` — dest_path lands at
    # ``/Volumes/Card/DCIM/Archive/2026/<name>``, which is NOT the
    # source file (samefile is False) but is still inside the card.
    # A copy there is counted as ``copied``, safe_to_format can go
    # green, and formatting the card erases the "archive" copy too.
    # ``path_under_any_source`` realpaths its argument, so this is
    # also what catches a candidate that is a SYMLINK into the card.
    # See PR #1107 review.
    return ctx.path_under_any_source(dest_path)


def _reject_source_backed_dest(state, ctx, *, rel, source_file, dest_file):
    """Per-file dest-safety guard (spec decision 12): fail a ``dest_file``
    that IS the source file or resolves under any source root. Returns
    True when the file was rejected (booked failed; the caller skips it).

    Complements ``_batch_preflight``'s folder-level guard, which realpaths
    only ``dest_folder`` and so cannot see ``os.path.samefile`` identity
    (hard-links/symlinks to the same inode) or a ``dest_file`` that is
    itself a symlink resolving back into the card. Runs on BOTH
    transports: on the remote path this geometry is exactly the
    safe_to_format-over-unbacked-bytes hazard the batch guard's own
    comment describes — before the port the collision walk hashed the
    source-backed dest, byte-matched it, and adopted it.

    SUFFIX candidates are handled elsewhere, and differently. This guard
    only sees the PRIMARY dest name; the collision walk probes each
    candidate it visits with the same ``_is_source_backed_dest`` and
    ADVANCES past a source-backed one rather than failing the file (spec
    decision 13, closed in PR 7b — before that it hashed such a candidate,
    byte-matched the card against itself, and adopted it). The two
    dispositions differ deliberately: this guard's geometry is a folder
    template that resolves back into the card, which poisons every suffix
    too, so there is no safe slot to advance to; a single symlinked entry
    at ``name_1.ext`` poisons only itself. Do not "unify" them.
    """
    if _is_source_backed_dest(ctx, source_file, dest_file):
        _fail(
            state, rel, source_file,
            "destination file resolves inside a source directory "
            "(dest_file would live under the card being imported); "
            "formatting the card would erase the archive copy",
        )
        return True
    return False


def _rollback_on_mount_loss(state, batch_st, attests_bytes,
                            extra_rollback=None):
    """Undo this batch's bookings after a mount-loss detection.

    The caller keeps the final post-loop probe at its own call site and
    calls this only when ``batch_st.mount_lost`` is set. A detach
    invalidates every accepted "already present" claim in this batch —
    the twin each one matched may be a shadow file on the mount stub
    rather than a real object on the archive — and everything landed
    before the detach may sit in the local shadow too.

    ORDER IS LOAD-BEARING: dup_skips rollback → ``extra_rollback`` (both
    paths pass it — it drops the queued-but-untransferred ``to_transfer``
    entries; a no-op locally) → landed rollback →
    ``state.mount_ever_lost`` LAST.
    The run-wide sticky flag asserts "this batch's bookings were rolled
    back", so it must not trip until every rollback above has run — an
    exception partway through must not leave later batches refused
    while this batch's counts still stand.
    """
    # Accepted duplicate skips rest on a twin that may live in the
    # local shadow rather than on the share, so a detach invalidates
    # the "already present" claim exactly as it invalidates a copy.
    # These never enter ``landed``, so they need their own rollback:
    # without it a duplicate-only batch reports every file safely
    # accounted for and the card looks safe to erase over an archive
    # holding none of the bytes. ``dup_dirs`` is dropped for the same
    # reason — linking those folders would pull shadow paths into the
    # workspace. See PR #1396 review (Codex P1 r3687506040).
    for skipped_file, counted_unverified in batch_st.dup_skips:
        state.skipped_duplicate -= 1
        _counts(state, batch_st.rel)["skipped_duplicate"] -= 1
        if counted_unverified:
            state.unverified_duplicate -= 1
        _fail(
            state, batch_st.rel, skipped_file,
            f"archive mount root {batch_st.mount_lost} detached mid-batch; "
            "the duplicate this file matched cannot be confirmed "
            "to be on the archive rather than in a local shadow",
        )
    batch_st.dup_skips = []
    batch_st.dup_dirs = set()
    if extra_rollback is not None:
        extra_rollback()
    # Anything that landed BEFORE the detach is sitting in the local
    # shadow, not on the archive. Roll those out of copied/
    # skipped_duplicate into failed via the origin-switching helper
    # and drop them: cataloging them would record archive paths for
    # bytes that vanish when the real share remounts, and leaving them
    # booked as copied could let safe_to_format go green over a card
    # that is still the only copy. Emptying ``landed`` also skips the
    # catalog scan for this batch.
    for entry in batch_st.landed:
        _reclassify_landed_failed(
            state, batch_st.rel, entry,
            f"archive mount root {batch_st.mount_lost} detached "
            "mid-batch; this file landed in a local shadow "
            "of the archive, not on the share",
            attests_bytes,
        )
    batch_st.landed = []
    # Trip the run-wide sticky flag so every remaining batch is
    # refused at the top of the loop rather than allowed to consult
    # the intra-run duplicate cache (which still holds identities
    # for the files just rolled back above; the checker has no
    # removal API). LAST — see the docstring. See PR #1400 review
    # (Codex P2 r3688614624).
    state.mount_ever_lost = batch_st.mount_lost


def _drop_queued_transfers(state, batch_st, rel):
    # Queued-but-untransferred files: nothing landed, so no
    # counter rollback — book the failure and drop the queue
    # so the rsync below has nothing to send.
    #
    # (Formerly a nested def whose default-arg binding of
    # ``batch_st`` / ``rel`` silenced ruff B023; module-level
    # explicit parameters replace that binding.)
    for queued_file, _dest_basename, _queued_hash, _sz, _mt \
            in batch_st.to_transfer:
        _fail(
            state, rel, queued_file,
            f"archive mount root {batch_st.mount_lost} detached before "
            "this file was transferred",
        )
    batch_st.to_transfer = []


# Verdicts returned by _duplicate_gate. The CALLER maps them onto the
# per-file loop's control flow, and the mapping is load-bearing:
# _GATE_CANCELLED must map to ``break`` (Stop interrupted a destination
# read — every further step this batch would touch the same, possibly
# dead, mount; mapping it to ``continue`` would reintroduce the
# wedged-mount cancellation pin fixed in PR #1423). _GATE_SKIPPED maps
# to ``continue`` (the file was counted as a duplicate skip or failed
# its duplicate check); _GATE_PROCEED falls through to the
# collision/adopt/transfer path.
_GATE_SKIPPED = "skipped"
_GATE_PROCEED = "proceed"
_GATE_CANCELLED = "cancelled"


def _duplicate_gate(state, batch_st, *, source_file, rel, checker, db,
                    params, ctx, stop_requested):
    """Decide whether ``source_file`` is a duplicate of cataloged or
    intra-run bytes, doing the byte-verification reads that decision
    requires. Shared verbatim by both import paths (the 2026-08-08
    phase map verified the two nested copies identical); returns one of
    the ``_GATE_*`` verdicts — see the constants above for the caller's
    mapping contract.
    """
    if checker is None:
        return _GATE_PROCEED
    try:
        token = checker.match(source_file)
    except OSError as e:
        _fail(state, rel, source_file, f"duplicate check failed: {e}")
        return _GATE_SKIPPED
    if token is None:
        return _GATE_PROCEED
    if (
        params.trust_likely_duplicates
        and not params.verify_by_hash
    ):
        likely_rows = _likely_twin_rows(
            db, token, source_file, ctx.path_under_any_source,
        )
        if likely_rows:
            state.skipped_duplicate += 1
            state.unverified_duplicate += 1
            _counts(state, rel)["skipped_duplicate"] += 1
            batch_st.dup_skips.append((source_file, True))
            batch_st.dup_dirs.update(_linkable_twin_dirs(
                likely_rows, ctx.path_under_destination,
            ))
            return _GATE_SKIPPED
    accept = False
    # verified_twin_rows records only the twin(s) whose
    # bytes we actually hashed on disk this run and
    # matched against the source. Both 'hash' and 'key'
    # tokens can carry stale rows: 'key' is a filename+
    # size+capture-second bucket where individual rows
    # may hold unrelated bytes, and 'hash' shares the
    # token's stored file_hash by construction but that
    # column reflects the LAST scan — an archive file
    # deleted or overwritten between scans leaves a stale
    # hash row. Linking any twin folder we did not
    # re-hash would pull unrelated/missing archive folders
    # into the active workspace on a duplicate-only
    # import. See PR #1107 review.
    verified_twin_rows = []
    if token[0] == "hash":
        twin_rows = _hash_twin_rows(db, token[1])
        src_hash = token[1]
    else:
        twin_rows = _key_twin_rows(db, token[1])
        # Hash the current source so a key match can be
        # confirmed against a cataloged (or intra-run)
        # twin's actual bytes. Reading a removable-media
        # source can fail (card yanked mid-check, I/O
        # error) — same as checker.match() and the copy
        # path, that must fail JUST this source rather
        # than escape and kill the whole background job.
        try:
            src_hash = checker.content_hash(source_file)
        except OSError as e:
            _fail(
                state, rel, source_file,
                f"duplicate check failed: {e}",
            )
            return _GATE_SKIPPED
    # An intra-run token is byte-proven by this session's
    # own copy_and_hash_verify — safe to skip without
    # hitting the archive, but ONLY when the token itself
    # carries bytes (``('hash', …)`` — the hash IS the
    # proof) or the current source's bytes match the run
    # twin's verified hash (``('key', …)`` — the metadata
    # key proves nothing about bytes; two different files
    # with the same filename+size+capture-second across
    # cards would otherwise be counted as skipped without
    # ever being byte-compared). Any other match
    # (catalog-side hash OR metadata-only key) is
    # stale-suspect: the photos.file_hash row could
    # describe an archive file that was deleted or
    # modified since the last scan, so a duplicate skip
    # must be backed by a cataloged twin that STILL holds
    # those bytes on disk. Without this, a stale hash row
    # would let the card be counted as skipped_duplicate
    # and safe_to_format go green while the card is the
    # only remaining copy of the bytes.
    if token in state.run_dest_folders:
        if token[0] == "hash":
            accept = True
        else:
            run_hash = state.run_verified_hashes.get(token)
            if (
                src_hash is not None
                and run_hash is not None
                and src_hash == run_hash
            ):
                accept = True
    if not accept:
        for twin in twin_rows:
            twin_path = os.path.join(
                twin["folder_path"], twin["filename"],
            )
            # A cataloged twin under any import source
            # root is (or may be) the card file being
            # imported this run — a stale scan of the
            # mounted card left a photos row whose path
            # IS the card. Hashing it just re-reads the
            # source, which proves nothing about an
            # archive copy; accepting it as duplicate
            # proof would flip safe_to_format green
            # while the card holds the only bytes. Only
            # an off-card twin can back a duplicate
            # skip. See PR #1107 review.
            if ctx.path_under_any_source(twin_path):
                continue
            try:
                twin_hash = _hash_dest_file(
                    twin_path, stop_requested)
            except DestReadCancelled:
                state.cancelled = True
                batch_st.dest_read_cancelled = True
                break
            except OSError:
                continue
            if twin_hash is not None and twin_hash == src_hash:
                accept = True
                # Keep scanning to collect every
                # byte-verified twin — for both 'hash'
                # and 'key' tokens. Breaking at the
                # first match (or falling back to the
                # full twin_rows for 'hash') risks
                # linking a stale/off-destination twin:
                # _linkable_twin_dirs then either drops
                # a legitimate destination twin (leaving
                # the imported photo invisible in the
                # active workspace) or pulls an
                # unrelated folder in (if the catalog's
                # stored hash row no longer describes
                # the on-disk bytes). See PR #1107
                # review.
                verified_twin_rows.append(twin)
    if state.cancelled:
        # Stop interrupted a twin hash above. Don't let this file fall
        # through to the adopt/copy path — every further step touches
        # the same (possibly dead) mount.
        return _GATE_CANCELLED
    if accept:
        state.skipped_duplicate += 1
        _counts(state, rel)["skipped_duplicate"] += 1
        batch_st.dup_skips.append((source_file, False))
        # verified_twin_rows carries only twins whose
        # bytes we re-hashed and matched this run — the
        # only rows whose folders are safe to link. For
        # a 'hash' token, other twin_rows entries share
        # the token's stored hash by construction but
        # that column can be stale (the archive file
        # changed or was deleted between scans); for a
        # 'key' token, other twin_rows entries share
        # only filename+size+capture-second and may
        # hold unrelated bytes. Linking either category
        # would pull unrelated/missing archive folders
        # into the active workspace on a duplicate-only
        # import. verified_twin_rows is empty when the
        # intra-run branch accepted above (run_dest is
        # added separately below). See PR #1107 review.
        batch_st.dup_dirs.update(
            _linkable_twin_dirs(
                verified_twin_rows, ctx.path_under_destination,
            ),
        )
        run_dest = state.run_dest_folders.get(token)
        if run_dest is not None:
            batch_st.dup_dirs.add(run_dest)
        return _GATE_SKIPPED
    # No byte-identical twin remains on disk — the card file is a
    # distinct photo; import it normally.
    return _GATE_PROCEED


def _catalog_scan_and_prescan(state, batch_st, db, params, scan, destination,
                              rel, attests_bytes, *, runner=None, job=None):
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
        row = db.conn.execute(
            """SELECT p.id, p.file_hash FROM photos p
               JOIN folders f ON f.id = p.folder_id
               WHERE f.path = ? AND p.filename = ?""",
            (
                os.path.dirname(dest_path),
                os.path.basename(dest_path),
            ),
        ).fetchone()
        if row is not None:
            pre_scan_hashes[dest_path] = row["file_hash"]
    try:
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
        # ``cancel_check`` is set to a parking callback rather than
        # ``None`` so the job actually transitions to ``paused`` when a
        # user pauses mid-scan. ``pause_check`` alone is non-blocking:
        # scanner releases its pool and then polls ``pause_check``
        # every 50 ms, leaving the job stuck in ``pausing``. The
        # parking callback invokes ``wait_if_paused(publish_paused=True)``
        # to park AND publish the paused status, then returns ``False``
        # unconditionally — the parking side-effect matters, the
        # ``cancelled`` return value must be ignored to keep the
        # preservation contract intact.
        from resource_ledger import bind_resource_cancel_check
        scan_pause_check = None
        scan_park_only_check = None
        if runner is not None and job is not None:
            job_id = job["id"]

            def scan_pause_check():
                return runner.pause_requested(job_id)

            def scan_park_only_check():
                # Park via wait_if_paused so the job publishes ``paused``
                # while the scanner is safely at its pause checkpoint.
                # Discard the cancellation return value — the preservation
                # contract requires this scan to run to completion.
                runner.wait_if_paused(job_id, publish_paused=True)
                return False
        with bind_resource_cancel_check(None):
            scan(
                destination, db,
                restrict_dirs=[batch_st.dest_folder],
                restrict_files=landed_paths,
                vireo_dir=params.vireo_dir,
                thumb_cache_dir=params.thumb_cache_dir,
                skip_working_copies=True,
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
            )
    except Exception as e:  # scan failure fails the whole batch
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



def _stamp_landed_and_validate_catalog(state, batch_st, db, params, rel, *,
                                       attests_bytes, dest_noun,
                                       stop_requested):
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
        verified_hash = entry.verified_hash
        row = db.conn.execute(
            """SELECT p.id, p.file_hash FROM photos p
               JOIN folders f ON f.id = p.folder_id
               WHERE f.path = ? AND p.filename = ?""",
            (os.path.dirname(dest_path),
             os.path.basename(dest_path)),
        ).fetchone()
        if row is None:
            # RAW+JPEG pairing merges the JPEG's photo row into
            # the RAW primary (companion_path) and deletes the
            # JPEG's own row, so a landed JPEG whose sibling RAW
            # was scanned in the same batch legitimately has no
            # row of its own. Look it up as another row's
            # companion_path before deciding "not cataloged".
            # See PR #1107/#1113 reviews.
            companion = db.conn.execute(
                """SELECT p.id FROM photos p
                   JOIN folders f ON f.id = p.folder_id
                   WHERE f.path = ? AND p.companion_path = ?""",
                (os.path.dirname(dest_path),
                 os.path.basename(dest_path)),
            ).fetchone()
            if companion is not None:
                # The paired JPEG's own row is gone by design,
                # so the row branch below can't cross-check its
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
                    break
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
                    continue
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
                continue
            _reclassify_landed_failed(
                state, rel, entry,
                "not cataloged after scan (no photo row)",
                attests_bytes,
            )
            batch_st.reclassified_landed_paths.add(dest_path)
            continue
        if row["file_hash"] == verified_hash:
            if attests_bytes:
                db.update_photo_hash_check(
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
                break
            if actual is not None and actual == verified_hash:
                # Backfill the row hash scan couldn't write
                # (spec decision 11) — except the zero-byte
                # case, where ``file_hash=None`` hits
                # ``update_photo_hash_check``'s status-only arm
                # and the row keeps scan's NULL (backfilling
                # EMPTY would recreate the every-empty-file
                # collision the convention exists to prevent).
                if attests_bytes:
                    db.update_photo_hash_check(
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
    return raw_companion_invalidations


def _invalidate_changed_and_sweep(state, batch_st, db, params,
                                  pre_scan_hashes,
                                  raw_companion_invalidations):
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
            row = db.conn.execute(
                """SELECT p.id FROM photos p
                   JOIN folders f ON f.id = p.folder_id
                   WHERE f.path = ? AND p.filename = ?""",
                (
                    os.path.dirname(dest_path),
                    os.path.basename(dest_path),
                ),
            ).fetchone()
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

    db.conn.commit()

    if invalidated_photo_ids:
        # Mirror scanner.scan()'s post-loop untracked-preview
        # sweep: orphan preview files with no preview_cache row
        # would be lazy-adopted on the next request and served as
        # stale bytes for the just-replaced archive file.
        from scanner import _sweep_untracked_previews_for_photos
        _sweep_untracked_previews_for_photos(
            db, params.vireo_dir, invalidated_photo_ids,
        )


def _fill_wc_overrides(state, batch_st, params):
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


def _link_twins_and_emit(state, batch_st, db, workspace_id, emit, rel,
                         queued):
    """Link verified duplicate-twin folders into the workspace and emit
    the end-of-batch progress line. Shared verbatim by both import
    paths; the caller keeps the ``if state.cancelled: break``.
    """
    # A verified duplicate skip's twin folder may live elsewhere under
    # the archive (older date-layout, ``unsorted``, etc.). The twins
    # already have catalog rows and _linkable_twin_dirs has checked
    # that their folders exist under this destination — link those
    # rows directly. A broad incremental scan would still
    # enumerate/stat every file in the matched folders before the
    # import could finish (over SMB that can turn a zero-copy import
    # into an hours-long metadata walk); uncataloged-stray repair
    # belongs to the explicit folder-rescan workflow instead.
    new_dup_dirs = batch_st.dup_dirs - state.linked_dup_dirs
    if new_dup_dirs:
        linked, failures = _link_duplicate_twin_dirs(
            db, workspace_id, new_dup_dirs,
        )
        state.linked_dup_dirs.update(linked)
        if failures:
            state.dup_link_failed = True
            for d, detail in failures.items():
                state.unsafe_files.append({
                    "path": d,
                    "reason": (
                        "duplicate-folder workspace link failed: "
                        f"{detail}"
                    ),
                })
    emit(
        f"{rel}: {_counts(state, rel)['copied']} copied · "
        f"{_counts(state, rel)['skipped_duplicate']} already present",
        state.emitted, queued,
    )


def _extract_deferred_working_copies(state, params, runner, job, db):
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
            message, phase_current=None, phase_total=None, phase_label=None,
        ):
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


def _make_stop_check(runner, job):
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
    def _stop_requested():
        return runner.cancellation_requested(job["id"])
    return _stop_requested


@dataclass(frozen=True)
class _DestContext:
    """Normalized destination plus the run-scoped guards derived from it.

    Built once per run by ``_build_destination_context``; both import
    paths consume the same bundle. ``fold_basename`` backs the remote
    path's intra-batch collision map and is unused — free and harmless —
    on the local path until PR 7 merges the loops.
    """
    destination: str
    mount_baseline: dict  # mount-root candidate -> was-live-at-run-start
    path_under_any_source: object  # callable(path) -> bool
    path_under_destination: object  # callable(path) -> bool
    fold_basename: object  # callable(name) -> str


def _build_destination_context(db, params):
    """Normalize the destination and derive the run-scoped guards.

    ORDERING IS LOAD-BEARING: normalize first, capture the live-mount
    baseline immediately after (before the caller runs discovery,
    catalog-index construction or timestamp extraction), then build the
    source/destination predicates against the normalized path. This
    function preserves that order by construction — see the block
    comments below for why each step sits where it does.
    """
    # Function-level import mirrors the two import-path bodies: pipeline_job
    # imports from this module, so a module-level import would be a cycle.
    from pipeline_job import (
        _archive_mount_baseline,
        _load_known_mount_roots,
        _record_known_mount_roots,
    )

    # Normalize once — the raw destination string is passed as ``root`` to
    # both the copy layout (``os.path.normpath(os.path.join(destination,
    # rel))``) and to ``scan(root, …, restrict_dirs=[dest_folder])``.
    # ``scanner._ensure_folder`` stops walking the folder chain when the
    # parent equals the scan root string; a destination like
    # ``/photos/tmp/../archive`` copies into the normalized
    # ``/photos/archive/…`` but the restricted scan root would remain the
    # dot-segment form, so the recursion never reaches root and the scan
    # loses those files (copied bytes then bucket as catalog failures).
    #
    # Also resolve symlinks (``realpath``) so a destination like ``/photos``
    # symlinked at ``/Volumes/Photos`` matches cataloged twin folders whose
    # ``folders.path`` was scanned under the real archive root — otherwise
    # duplicate matching compares paths from prior catalog scans against
    # this destination. Sources are already ``realpath``-resolved (see
    # ``_norm_source``); doing the same to destination keeps the two sides
    # symmetric. See PR #1107 review.
    #
    # On the remote (SSH) path ``params.destination`` is the catalog/mount
    # base — the route sets it to mount_path/subpath — and the same
    # normalization applies.
    try:
        destination = os.path.realpath(os.path.normpath(str(params.destination)))
    except OSError:
        destination = os.path.normpath(str(params.destination))

    # Which of the destination's mount-root candidates are live mounts.
    # Captured HERE — immediately after normalization, before discovery,
    # catalog-index construction or timestamp extraction — because all of
    # those are slow against a network archive (the 2026-07-30 incident
    # spent eight minutes just enumerating the destination). A share that
    # detached during that window would be recorded as already-unmounted,
    # the mounted → unmounted transition would never fire, and the guard
    # would be silently disarmed for the rest of the run. See PR #1396
    # review (Codex P1 r3687336684).
    #
    # Keying on the transition (rather than "is it mounted?") is what lets
    # an ordinary local directory at a mount-shaped path stay usable: it
    # is False here and stays False, so it never trips the check.
    #
    # ``known_mount_roots`` seeds True from a persistent record of
    # mount roots ever observed live. Without it, a share that detached
    # BEFORE the run started (baseline is False from the outset) escapes
    # the guard: no True → False transition can fire against a False
    # baseline, so the persistent ``/mnt/<name>`` stub still passes the
    # per-batch check and copies land on the local disk (on the remote
    # path, rsync would still push to the NAS while the per-batch
    # mount-side scan reads a fresh local shadow). Cross-run history
    # closes that hole; a hand-made local dir is never observed as a
    # live mount and stays out of the known-set. See PR #1396 review
    # (Codex P1 r3687401636).
    known_mount_roots = _load_known_mount_roots(db)
    mount_baseline = _archive_mount_baseline(destination, known_mount_roots)
    _record_known_mount_roots(db, mount_baseline)

    # Reject cataloged twins that live under the card being imported. The
    # /api/jobs/import-photos route already refuses destinations that sit
    # inside a source (formatting the card would erase the archive copy),
    # but the duplicate acceptance loop separately trusts any cataloged
    # twin whose bytes hash to ``src_hash`` — including a stale row for a
    # previously scanned mounted card. That twin's re-hash just re-reads
    # the very card file being imported, so accepting it as duplicate
    # proof would flip ``safe_to_format`` green over a card whose bytes
    # never made it to the archive (remotely: whose bytes never crossed
    # the network). Shared by both import paths. See PR #1107 review.
    _path_under_any_source = _build_source_root_guard(params.sources)

    # Destination containment for cataloged twin folders. Used to scope
    # ``_linkable_twin_dirs`` to twins under the destination/mount base —
    # an off-destination twin in some other library root is none of this
    # import's business. ``destination`` is already ``realpath``-resolved
    # above so a symlinked destination like ``/photos`` ->
    # ``/Volumes/Photos`` matches twin folders cataloged under
    # ``/Volumes/Photos/…``. Case-different spellings on case-insensitive
    # mounts (SMB, FAT, HFS+/APFS/exFAT) still need explicit
    # case-folding: ``realpath`` on APFS preserves the case the user
    # gave. Probe the destination's own filesystem (walking up to the
    # closest existing ancestor when the destination itself hasn't been
    # created yet); default to case-insensitive on inconclusive results
    # so a differently-cased twin folder under the destination is still
    # linked to the workspace — otherwise ``safe_to_format`` can go
    # green while the imported photo stays invisible in the active
    # workspace. See PR #1107 review.
    def _probe_dir_case_insensitive(path):
        p = os.path.normpath(path)
        while True:
            if os.path.isdir(p):
                return _fs_is_case_insensitive(p)
            parent = os.path.dirname(p)
            if parent == p:
                return True
            p = parent

    _dest_ci = _CASE_INSENSITIVE_PLATFORM or _probe_dir_case_insensitive(destination)
    _dest_root_norm = (
        destination.casefold() if _dest_ci else destination
    ).rstrip(os.sep)

    def _path_under_destination(path):
        if not _dest_root_norm:
            return False
        try:
            real = os.path.realpath(path)
        except OSError:
            real = str(path)
        cmp = (real.casefold() if _dest_ci else real).rstrip(os.sep)
        return cmp == _dest_root_norm or cmp.startswith(_dest_root_norm + os.sep)

    # Case-insensitive destinations (macOS APFS/HFS+, SMB, FAT/exFAT)
    # collapse basenames that differ only by case onto the same on-disk
    # file. The intra-batch collision map ``claimed_basenames`` keys by
    # basename, so keying it case-foldedly there makes a second file whose
    # basename differs from an earlier queued file's only by case (e.g.
    # ``IMG_0001.JPG`` then ``img_0001.jpg``) collide and advance through
    # numeric suffixes, instead of being sent to the same effective
    # receiver path where ``--ignore-existing`` would silently drop it and
    # the later catalog/hash validation would fail. See PR #1113 review.
    def _fold_basename(name):
        return name.casefold() if _dest_ci else name

    return _DestContext(
        destination=destination,
        mount_baseline=mount_baseline,
        path_under_any_source=_path_under_any_source,
        path_under_destination=_path_under_destination,
        fold_basename=_fold_basename,
    )


def _make_emitter(job, runner, state, eta):
    """Build the per-run progress emitter shared by both import paths.

    ``state.folder_counts``: live per-folder counters, mutated by the
    copy loop via _counts() and snapshotted onto every progress event
    so the Import page can render truthful per-folder progress mid-run
    (not just from the terminal result).
    """
    def _emit(phase, current, total, current_file="", *, is_importing=False):
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
    # Enumeration errors (permission denied, macOS TCC block on a
    # removable volume, unreadable subtree) get silently swallowed by
    # os.walk-style callbacks by default. If we ignored them, discovered
    # would just be smaller than reality and safe_to_format could still
    # flip green over a card whose contents were never actually visited.
    # Track them explicitly: each is a bucket-of-its-own failure entry
    # tied to the source path where it occurred.
    emit("Discovering files", 0, 0)
    files = []

    def _discovery_onerror(exc):
        state.discovery_errors.append(exc)
        log.warning("Import discovery error: %s", exc)

    for src in params.sources:
        files.extend(discover_source_files(
            src, params.file_types, recursive=params.recursive,
            onerror=_discovery_onerror,
        ))
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

    # Group by destination (template) folder, template order, then chunk.
    groups = {}
    for f in files:
        rel = build_destination_path(
            timestamps.get(f), params.folder_template,
        ) or "."
        groups.setdefault(rel, []).append(f)
    batches = []
    for rel in sorted(groups):
        group = groups[rel]
        for i in range(0, len(group), IMPORT_BATCH_SIZE):
            batches.append((rel, group[i:i + IMPORT_BATCH_SIZE]))

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


def _finalize_import(job, runner, db, state, params, *,
                     discovered, include_paths, source_snapshots,
                     deselected, vanished_paths, appeared,
                     remote_unverified=False):
    """Terminal status, safety verdicts, and the shared 19-key result dict.

    ``remote_unverified`` is the one transport-required term: the remote
    path passes ``not params.verify_by_hash`` (see the honesty-gate
    comment at its append below); the local path hash-verifies every
    copy, so it calls with the default ``False`` and every
    ``remote_unverified`` term degenerates to a no-op.
    """
    status = "cancelled" if state.cancelled else (
        "failed" if state.failed else "completed"
    )
    summary = _selection_summary(
        params, include_paths, discovered=discovered, copied=state.copied,
        skipped_duplicate=state.skipped_duplicate, failed=state.failed,
    )
    runner.update_step(
        job["id"], "import",
        status="failed" if status == "failed" else "completed",
        summary=summary,
    )

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
    #
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
    result = {
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
        # Stable-identity map so a recovery retry can verify each carried
        # ID still points at the same file. Without this the retry
        # authorizes any current photo row that happens to share an ID
        # with something the parent landed — an especially real risk
        # after users delete recent imports (SQLite reuses the freed
        # IDs on the next insert).
        "photo_fingerprints": _capture_photo_fingerprints(
            db, state.imported_photo_ids,
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
        # file, unseen source subtree, or workspace-link failure is
        # recorded "failed" (with per-file / per-operation reasons),
        # never "completed".
        "ok": (
            state.failed == 0
            and not state.discovery_errors
            and not state.dup_link_failed
        ),
        "errors": [f"{u['path']}: {u['reason']}" for u in state.unsafe_files],
    }
    return result


def copy_and_hash_verify(src, dst, *, src_hash=None):
    """Copy ``src`` to ``dst`` and verify the landed bytes by content hash.

    The copy goes to a hidden sibling temp path first; only a copy whose
    hash matches the source is promoted into ``dst``. Promotion prefers a
    no-overwrite ``os.link`` (atomic on POSIX same-FS) over ``os.replace``
    — imports have no pipeline-slot lock, so two concurrent jobs targeting
    the same destination/date folder with the same filename can both pass
    their pre-copy collision check before either promotes; ``os.replace``
    would silently overwrite the first job's already-verified archive
    copy, and its ``safe_to_format`` would still report green after the
    bytes it verified are gone. A raced promote is surfaced as a copy
    failure instead.

    When the destination filesystem does not support hard links (exFAT/FAT,
    some SMB/NFS mounts — os.link raises OSError with EPERM/ENOTSUP/
    EOPNOTSUPP), fall back to a check-then-rename promotion serialized on
    a directory-level ``fcntl.flock`` of the destination folder. That
    preserves both crash-safety (no zero-byte placeholder file) and
    no-overwrite race protection against concurrent imports targeting the
    same destination/date folder — the fallback block below documents the
    tradeoffs. Imports do not fail across every file on FAT-family
    archives or hardlinkless NAS shares.

    On mismatch (or race) the temp copy is removed and any pre-existing
    ``dst`` is left untouched.

    Args:
        src: source file path (e.g. on the card)
        dst: final destination path in the archive
        src_hash: optional already-computed source hash (e.g. the
            DuplicateChecker's cached value) to avoid re-reading the
            source.

    Returns:
        (True, file_hash) on verified success, (False, None) on failure.
    """
    dst_dir = os.path.dirname(dst)
    if dst_dir:
        os.makedirs(dst_dir, exist_ok=True)
    tmp = os.path.join(
        dst_dir, f".{os.path.basename(dst)}.{uuid.uuid4().hex}.tmp"
    )
    try:
        shutil.copy2(src, tmp)
        copied_hash = compute_file_hash(tmp)
        expected = src_hash if src_hash is not None else compute_file_hash(src)
        if copied_hash is None or expected is None or copied_hash != expected:
            log.warning(
                "Hash verification failed for %s -> %s (expected %s, got %s)",
                src, dst, expected, copied_hash,
            )
            return (False, None)
        # Atomic no-overwrite promote: os.link raises FileExistsError if
        # ``dst`` was created between the caller's collision check and
        # this instant. tmp lives in the same directory as dst, so link
        # stays same-filesystem and portable across the NAS mounts that
        # are the real archive target.
        try:
            os.link(tmp, dst)
        except FileExistsError:
            log.warning(
                "Destination raced during copy (concurrent import?): %s",
                dst,
            )
            return (False, None)
        except OSError as link_err:
            # Hard links unsupported on this destination filesystem
            # (FAT/exFAT return EPERM; some SMB/NFS mounts return
            # ENOTSUP/EOPNOTSUPP; Windows exFAT can return EACCES).
            # Without a fallback promotion path every file on
            # hardlinkless archives buckets as a copy failure and
            # imports are unusable on those destinations.
            #
            # Fall back to existence-check + os.rename, wrapped in a
            # directory-level POSIX advisory lock. The verified temp
            # stays hidden until it moves atomically over to ``dst``.
            # Do NOT reserve the final path as an O_EXCL placeholder
            # before renaming — a crash between placeholder creation
            # and os.replace would leave a zero-byte stray at the
            # intended archive name, and a retry treats that
            # placeholder as "existing archive file", suffixes the
            # real photo to ``name_1.ext``, and orphans the empty
            # file. That violates the crash-recovery invariant that a
            # dead run leaves only valid archive copies or hidden
            # temps.
            #
            # A bare check-then-rename loses a concurrent-import race:
            # two hardlinkless-FS jobs targeting the same
            # destination/date folder could both pass exists() before
            # either rename(), and the later rename would silently
            # overwrite the first job's already-verified archive copy
            # (its ``safe_to_format`` would still report green after
            # its bytes are gone). Serialize the critical section on
            # an exclusive ``fcntl.flock`` of the destination
            # directory: FD-scoped, so a crash releases it
            # automatically — no placeholder cleanup burden, and the
            # zero-byte crash-safety invariant is preserved. On mounts
            # where flock silently no-ops (some remote FSes mounted
            # ``nolock``) we degrade to the previous check-then-rename
            # behavior; per-workspace/per-date destinations make
            # overlapping runs unusual there. See PR #1107 review.
            if not _fs_lacks_hardlinks(link_err):
                raise
            log.info(
                "os.link unsupported on %s (%s); using rename fallback",
                dst_dir, link_err,
            )
            lock_fd = None
            try:
                try:
                    lock_fd = os.open(dst_dir, os.O_RDONLY)
                except OSError:
                    lock_fd = None
                if lock_fd is not None and fcntl is not None:
                    with contextlib.suppress(OSError):
                        fcntl.flock(lock_fd, fcntl.LOCK_EX)
                if os.path.exists(dst):
                    log.warning(
                        "Destination raced during copy "
                        "(concurrent import?): %s",
                        dst,
                    )
                    return (False, None)
                try:
                    os.rename(tmp, dst)
                except OSError as rep_err:
                    log.warning(
                        "Fallback promote failed for %s -> %s: %s",
                        src, dst, rep_err,
                    )
                    return (False, None)
                tmp = None
                return (True, copied_hash)
            finally:
                if lock_fd is not None:
                    with contextlib.suppress(OSError):
                        os.close(lock_fd)
        os.unlink(tmp)
        tmp = None
        return (True, copied_hash)
    except OSError as e:
        log.warning("Copy failed for %s -> %s: %s", src, dst, e)
        return (False, None)
    finally:
        if tmp is not None:
            with contextlib.suppress(OSError):
                os.unlink(tmp)


# errno values that mean "this filesystem doesn't support hard links",
# not "the operation was denied for some other reason". Kept narrow so a
# genuine permission error on a link-supporting FS still surfaces as a
# copy failure instead of silently falling back to the placeholder path.
# EPERM is the canonical Linux answer for FAT/exFAT; ENOTSUP/EOPNOTSUPP
# come from various BSD-family kernels and userspace filesystems; EACCES
# has been observed on Windows exFAT via WSL. EXDEV (cross-device link)
# also lands here — same-directory tmp should never trip it, but treating
# it as "hard link not usable here" and using the O_EXCL fallback is
# strictly safer than propagating.
_HARDLINK_UNSUPPORTED_ERRNOS = frozenset(
    e for e in (
        getattr(errno, "EPERM", None),
        getattr(errno, "ENOTSUP", None),
        getattr(errno, "EOPNOTSUPP", None),
        getattr(errno, "EACCES", None),
        getattr(errno, "EXDEV", None),
    ) if e is not None
)


def _fs_lacks_hardlinks(err):
    """True when ``err`` from os.link indicates a hardlinkless target FS."""
    return getattr(err, "errno", None) in _HARDLINK_UNSUPPORTED_ERRNOS


# Seconds a destination-side hash read may go without producing a single
# chunk before it is declared stalled. Same philosophy as the rsync stall
# watchdog in move.py: bound silence, not total runtime — a slow but moving
# mount read never trips this, a wedged SMB session does.
DEST_HASH_STALL_TIMEOUT = 120.0


class DestReadCancelled(OSError):
    """Stop arrived while a destination-side read was in flight.

    Subclasses OSError so any call site without explicit cancel handling
    treats it as an ordinary unreadable-file result (safe by default),
    while the sites that would otherwise record a failure catch it first
    and convert it into the job's normal cancelled exit.
    """


def _hash_dest_file(path, cancel_check, *,
                    stall_timeout=DEST_HASH_STALL_TIMEOUT):
    """SHA-256 a destination/mount-side file without letting a sick
    network mount hold cancellation hostage.

    ``compute_file_hash`` is a plain blocking read: against a stale SMB
    mount a single file can pin the worker for the mount's own timeout
    (tens of minutes on macOS) per read while Stop goes unobserved —
    cancellation is only polled between files, so the job sits in
    "cancelling" for hours. Run the chunked read in a watcher-supervised
    daemon thread instead:

    - ``cancel_check()`` returns True → ``DestReadCancelled`` within a
      second, even mid-read.
    - no chunk lands for ``stall_timeout`` → plain ``OSError``, the same
      shape an unreadable file already produces at every call site.

    The abandoned worker cannot be killed — the read is stuck in an
    uninterruptible kernel call — so it is orphaned: a daemon thread that
    exits when the mount finally returns, never blocking shutdown.
    """
    if cancel_check():
        # The open() itself blocks on a dead mount; don't even touch it.
        raise DestReadCancelled(f"import cancelled before reading {path}")

    result = {}
    activity = {"t": time.monotonic()}
    done = threading.Event()

    def _worker():
        h = hashlib.sha256()
        try:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    activity["t"] = time.monotonic()
                    if not chunk:
                        break
                    h.update(chunk)
            result["hash"] = h.hexdigest()
        except BaseException as exc:  # surfaced on the caller's thread
            result["exc"] = exc
        finally:
            done.set()

    worker = threading.Thread(
        target=_worker, daemon=True, name="import-dest-hash",
    )
    worker.start()
    while not done.wait(0.5):
        if cancel_check():
            raise DestReadCancelled(
                f"import cancelled while reading {path}")
        if time.monotonic() - activity["t"] > stall_timeout:
            raise OSError(
                f"read of {path} stalled (no data for "
                f"{stall_timeout:.0f}s; the mount is likely dead)")
    if "exc" in result:
        raise result["exc"]
    return result["hash"]


def _rehash_dest_or_none(path, stop_requested):
    """Re-hash a landed destination file, returning None on read failure.

    The stamping loop's last-line check that the bytes currently at the
    destination path still match the hash this import recorded —
    necessary any time the scan-side hash is missing (paired-JPEG row
    deletion) or NULL (zero-byte convention aside, a NULL means the
    scan-side read failed between landing and scan). ``None``
    unambiguously means unreadable: ``_LandedFile.verified_hash`` is
    never None, so callers comparing against it cannot confuse a read
    failure with a legitimately empty file. Re-raises
    ``DestReadCancelled`` from a stop request.
    """
    try:
        return _hash_dest_file(path, stop_requested)
    except DestReadCancelled:
        raise
    except OSError:
        return None


def _key_twin_rows(db, key):
    """Catalog rows whose stored identity equals a source metadata key.

    ``CatalogIndex`` retains only identity sets, so the safe-to-format
    second pass resolves a key match's cataloged twin(s) here. Prefilter
    by file_size (cheap, indexedable) and compare the full
    ``stored_metadata_key`` in Python — SQL LOWER() is ASCII-only and
    must not stand in for casefold().
    """
    rows = db.conn.execute(
        """SELECT p.id, p.filename, p.file_size, p.timestamp, p.file_hash,
                  f.path AS folder_path, f.status AS folder_status
           FROM photos p JOIN folders f ON f.id = p.folder_id
           WHERE p.file_size = ?""",
        (key[1],),
    ).fetchall()
    return [
        r for r in rows
        if stored_metadata_key(r["filename"], r["file_size"], r["timestamp"]) == key
    ]


def _hash_twin_rows(db, file_hash):
    return db.conn.execute(
        """SELECT p.id, p.filename, p.file_size, f.path AS folder_path,
                  f.status AS folder_status
           FROM photos p JOIN folders f ON f.id = p.folder_id
           WHERE p.file_hash = ?""",
        (file_hash,),
    ).fetchall()


def _likely_twin_rows(db, token, source_file, path_under_source):
    """Return live off-source catalog twins that plausibly back ``token``.

    This is the intentionally fast duplicate mode: the checker has already
    matched filename + byte size + capture time (or, for metadata-poor files,
    a stored hash). We only stat the proposed archive twin to make sure it
    still exists off the card at the expected size; we do not read either
    complete file. Callers must report the resulting skip as unverified.
    """
    rows = (
        _hash_twin_rows(db, token[1])
        if token[0] == "hash"
        else _key_twin_rows(db, token[1])
    )
    try:
        source_size = os.path.getsize(str(source_file))
    except OSError:
        return []
    likely = []
    for row in rows:
        twin_path = os.path.join(row["folder_path"], row["filename"])
        if path_under_source(twin_path):
            continue
        try:
            if os.path.getsize(twin_path) != source_size:
                continue
        except OSError:
            continue
        likely.append(row)
    return likely


def _linkable_twin_dirs(rows, under_destination):
    """Destination-scoped folders holding a duplicate's cataloged twin.

    Only folders under the import destination are linked after a
    duplicate skip (a twin in some other library root is none of this
    import's business). Mirrors ingest()'s dup_token_folders guards:
    path under destination and still a real directory on disk. Status
    ``ok``/``partial`` is trusted as-is; ``missing`` is accepted too when
    the path is still a real directory (a reattached archive drive whose
    row hasn't been refreshed yet), and ``run_import_job`` promotes it
    to ``ok`` as part of the direct link — otherwise a duplicate-only
    batch that matches a missing-marked twin folder would drop it from
    ``dup_dirs``, safe_to_format could go green, and the imported
    duplicates would stay filtered out of workspace queries.

    ``under_destination(path)`` compares resolved/case-normalized paths
    (built in ``run_import_job`` from the destination's own filesystem
    semantics). A lexical prefix check would drop a twin folder when the
    destination is a symlink to the twin's on-disk archive root, or
    spelled with different case on a case-insensitive mount — dropping
    the twin means the direct workspace link never runs and the imported
    duplicate stays filtered out of the active workspace while
    safe_to_format still flips green. See PR #1107 review.
    """
    dirs = set()
    for r in rows:
        folder_path = r["folder_path"]
        if r["folder_status"] not in ("ok", "partial", "missing"):
            continue
        if not under_destination(folder_path):
            continue
        if not os.path.isdir(folder_path):
            continue
        dirs.add(folder_path)
    return dirs


def _selection_blocks_format(*, deselected, vanished_paths):
    """True when the user's selection means the card is NOT fully archived.

    Deliberately separate from the ``(copied + skipped_duplicate) ==
    discovered`` ledger check. That equality catches these cases *usually*,
    and three data-loss bugs in review came from trusting "usually":
      - the selection filter applied above ``discovered`` instead of below
        it, so a partial import still balanced the equality;
      - a vanished selected file blocked ``safe_to_format`` but not
        ``unverified_duplicates_only``, whose amber pill's stated remedy
        (re-run with ``verify_by_hash``) then turns it green;
      - deselect X, then X also vanishes -> discovered shrinks too, equality
        balances, nothing is wrong arithmetically, and a file the user
        excluded is reported as archived.
    Do not delete either condition because the other "already covers it".

    ``deselected != 0`` rather than ``> 0``: a negative count means the
    caller previewed fewer files than it selected, which is self-inconsistent
    input, and this module fails closed on inconsistency.

    Intended as the single home for selection-based card-safety conditions:
    every card-safety verdict on every copy path should call this so a new
    condition added here reaches all of them. All four verdicts do —
    ``safe_to_format`` and ``unverified_duplicates_only``, for both
    transports behind ``run_import_job``.
    Do not inline the condition at a call site.
    """
    return deselected != 0 or bool(vanished_paths)


class _Selection(NamedTuple):
    """What the per-file selection did to one import run.

    ``files`` is the copy set after filtering; ``queued`` is its length as
    the progress denominator; the rest is drift, feeding the card-safety
    verdict, the ``unsafe_files`` lines, and the caller's readout.

    Both copy paths destructure this BY NAME (``sel.queued``), never by
    position. Four of the six fields are plain ints and two are sets, so a
    positional unpack that transposed a same-typed pair would still run and
    still type-check, and the local/remote parity test would not see it: that
    test compares the two paths to *each other*, so a transposition applied
    to both is invisible to it. (Measured, not assumed — swapping
    ``queued``/``deselected`` or ``queued``/``appeared`` in a positional
    unpack survives parity; only a cross-type swap like
    ``files``/``include_paths`` fails it.) Reordering these fields is
    therefore safe today; keep it that way by not reintroducing a positional
    unpack.
    """

    files: list
    include_paths: set | None
    queued: int
    deselected: int
    vanished_paths: set
    appeared: int


def _apply_selection(files, params):
    """Filter the discovered files by the user's selection and measure drift.

    THE single home for the pre-copy-loop selection block, called by the
    ``run_import_job`` orchestrator for both transports. It used to be
    duplicated across the two pre-merge copy paths, and the copies'
    comments had diverged on the very commit that created them.

    ``files`` is the raw discovery result; ``discovered`` (its length) is
    the caller's, and stays the card-safety denominator.
    """
    # Coerce ONCE, above the filter, and use this set everywhere below.
    # ``ImportParams.include_paths`` is typed ``set | None`` but a JSON
    # payload deserializes to a list, and the drift math below does set
    # arithmetic on it (``- discovered_paths``), which raises TypeError on a
    # list before a single file is copied or rsynced. Deduping here also
    # keeps ``len(include_paths)`` agreeing with the set the filter actually
    # used, so a payload with repeats can't skew ``deselected``.
    include_paths = (
        set(params.include_paths) if params.include_paths is not None else None
    )
    # Snapshot BEFORE filtering — drift is measured against what the card
    # actually holds, and computing it post-filter makes files-appeared zero.
    # NOTE: this is a set while the caller's ``discovered`` is a raw list
    # length, so overlapping sources (``/card`` plus ``/card/DCIM``)
    # enumerate a file twice and make ``len(discovered_paths) <
    # discovered``. Do not assume the two agree.
    discovered_paths = {str(f) for f in files}
    if include_paths is not None:
        # Matching is exact string equality against ``str(f)`` as produced by
        # ``discover_source_files``. The caller's paths come from that same
        # enumeration over the same raw source strings (the ``path`` field of
        # ``/api/import/folder-preview``), and NEITHER side resolves symlinks
        # or otherwise normalizes. Realpath-ing ``params.sources`` here would
        # silently empty this filter and copy (or rsync) nothing. Covered by
        # ``test_selection_filter_matches_unresolved_paths_*``, which import
        # through a symlinked directory — an ordinary ``tmp_path`` card is
        # already fully resolved and cannot tell the two apart.
        files = [f for f in files if str(f) in include_paths]
    # Progress denominator. Deliberately NOT ``discovered``: that counts the
    # whole card, so a half-deselected import would run to completion with
    # the bar stalled near 50% — a finished job that looks hung. ``queued``
    # is the work actually enqueued; ``discovered`` keeps backing the
    # card-safety verdict. Two denominators, on purpose.
    queued = len(files)

    # Selection drift. Computed against the pre-filter snapshot.
    deselected = 0
    vanished_paths = set()
    appeared = 0
    if include_paths is not None:
        # Deliberately NOT gated on ``previewed_count``. A selected file the
        # card no longer holds is drift whether or not the caller told us how
        # big the preview was, the ledger equality cannot see it (discovered
        # shrinks in step with copied), and ``include_paths`` without
        # ``previewed_count`` is constructible today — both fields are
        # independently optional on ``ImportParams``. Gating this would fail
        # OPEN on exactly the case this code exists to close.
        vanished_paths = include_paths - discovered_paths
    if include_paths is not None and params.previewed_count is not None:
        # ``deselected`` is compared with ``!= 0``, not ``> 0`` (see
        # ``_selection_blocks_format``): a negative value means the caller
        # previewed fewer files than it selected, which is self-inconsistent,
        # and this module fails closed on inconsistency rather than reading
        # it as "nothing deselected".
        deselected = params.previewed_count - len(include_paths)
        # The clamp conflates "no new files" with "the card holds fewer
        # files than were previewed". That second case is not lost: some
        # previewed file must be missing, and it is caught either by
        # ``vanished_paths`` (if it was selected) or by a positive
        # ``deselected`` (if it was not, since ``include_paths`` is then a
        # proper subset of the previewed set). NOT by a negative
        # ``deselected`` — previewed=3 with all three selected and one
        # vanished gives ``deselected == 0``, and only ``vanished_paths``
        # blocks it. ``appeared`` is a report count, so a negative would be
        # meaningless.
        appeared = max(0, len(discovered_paths) - params.previewed_count)

    return _Selection(
        files=files, include_paths=include_paths, queued=queued,
        deselected=deselected, vanished_paths=vanished_paths,
        appeared=appeared,
    )


def _selection_summary(params, include_paths, *, discovered, copied,
                       skipped_duplicate, failed):
    """The import step's summary line. Shared by both copy paths.

    Two forms; the discovered total appears exactly once in each.

      no selection: "5 copied, 0 already present, 0 failed of
                     5 discovered"   (byte-identical to pre-selection)
      selection:    "1 selected of 3 discovered, 1 copied,
                     0 already present, 0 failed"

    Dropping the tail from the no-selection form to make room for the
    prefix would leave every user who never touches a checkbox with counts
    and no total to read them against.

    The gate is conjunctive, and must stay that way: ``include_paths`` and
    ``checked_count`` are independently optional on ``ImportParams``, and it
    is ``include_paths`` alone that decides whether the copy set was
    filtered. Keying this on ``checked_count`` by itself would let
    ``ImportParams(sources=..., checked_count=3)`` — no ``include_paths``,
    so the whole card is copied — report "3 selected of 5 discovered",
    claiming a selection for a run where none was applied.

    The selected figure is ``checked_count``, NOT ``len(include_paths)``:
    that set also carries files the user left unchecked because they were
    flagged duplicates, so it would overstate what was chosen.

    Counts are keyword-only: four ints in a row is a swap waiting to happen,
    and a swapped ``copied``/``failed`` reads as a clean run.
    """
    if include_paths is not None and params.checked_count is not None:
        return (f"{params.checked_count} selected of {discovered} "
                f"discovered, {copied} copied, "
                f"{skipped_duplicate} already present, {failed} failed")
    return (f"{copied} copied, {skipped_duplicate} already present, "
            f"{failed} failed of {discovered} discovered")


def _append_selection_unsafe(unsafe_files, *, deselected, vanished_paths,
                             appeared):
    """Append the selection-drift lines to ``unsafe_files``, in place.

    Shared by both copy paths, and the reporting counterpart to
    ``_selection_blocks_format``: every selection signal that can flip the
    pill red appends a line here, because ``renderResult`` HIDES the unsafe
    list when it is empty — otherwise the user gets "Do NOT format the card
    yet" with no stated reason. That property holds for the selection
    signals only; it is NOT a general invariant of the callers today. Known
    pre-existing gap: ``partial_scope`` (``recursive=False``, or a narrowed
    ``file_types``) flips ``safe_to_format`` False and appends nothing, so
    that path still renders a bare red pill. Out of scope here; don't read
    this helper as evidence it was fixed.

    These lines *attribute* the gap between what was previewed and what was
    copied; they do not claim to enumerate it. Deselect one file, have
    another vanish, and have a third arrive after the preview, and the
    counts no longer map one-to-one onto the files still card-only — do not
    reword these into a claim that the list is exhaustive.

    Both branches of ``deselected`` are covered on purpose:
    ``_selection_blocks_format`` blocks on ``deselected != 0``, so a
    negative count (the payload previewed fewer files than it selected)
    turns the pill red too. A lone ``if deselected > 0`` would leave exactly
    that red pill bare. The negative branch does not quote the number: the
    payload is self-inconsistent, so the count is precisely the thing that
    cannot be trusted. A negative ``deselected`` also cannot occur alone —
    ``len(include_paths) > previewed_count`` forces either a selected path
    the card no longer holds (``vanished_paths``) or a card holding more
    than was previewed (``appeared``) — so its line always ships alongside
    another.

    ``appeared`` likewise only ever renders under a red pill: a balanced
    ledger with ``appeared > 0`` forces ``deselected <= 0`` (negative
    blocks; zero would make ``appeared`` zero too).

    Counts are pluralized (``_plural`` + verb agreement) because 1 is the
    single most likely real case — one frame deselected, one file gone.
    """
    if deselected > 0:
        unsafe_files.append({
            "path": "Deselected files",
            "reason": (
                f"{deselected} file{_plural(deselected)} you deselected "
                f"{'was' if deselected == 1 else 'were'} not copied"
            ),
        })
    if deselected < 0:
        unsafe_files.append({
            "path": "Selection count mismatch",
            "reason": ("your preview reported fewer files than were "
                       "selected, so how many files went uncopied could not "
                       "be determined — re-preview before formatting"),
        })
    if vanished_paths:
        vanished_count = len(vanished_paths)
        unsafe_files.append({
            "path": "Files missing at import time",
            "reason": (
                f"{vanished_count} file{_plural(vanished_count)} "
                f"{'was' if vanished_count == 1 else 'were'} in scope but "
                "had disappeared from the source when the import ran"
            ),
        })
    if appeared > 0:
        unsafe_files.append({
            "path": "Files added after preview",
            "reason": (
                f"at least {appeared} file{_plural(appeared)} arrived after "
                f"your preview and {'was' if appeared == 1 else 'were'} not "
                f"imported — re-preview to include "
                f"{'it' if appeared == 1 else 'them'}"
            ),
        })


def _link_duplicate_twin_dirs(db, workspace_id, dup_dirs):
    """Link cataloged duplicate folders without walking archive storage.

    ``_linkable_twin_dirs`` has already proved each path is a cataloged
    folder under the destination and is currently a real directory. A
    filesystem scan adds no evidence about the duplicate bytes themselves;
    it only used to be an indirect way to create ``workspace_folders`` rows.
    Doing that directly keeps duplicate-only imports independent of SMB
    directory-enumeration latency.

    A reattached folder whose stale status is ``missing`` is promoted after
    the existence check. ``partial`` is intentionally preserved: only an
    explicit successful rescan may claim that an incomplete scan is repaired.
    Returns ``(linked_paths, failures)`` so callers can keep
    ``safe_to_format`` honest if the database link itself fails.
    """
    linked = set()
    failures = {}
    for folder_path in sorted(dup_dirs):
        try:
            folder_row = db.conn.execute(
                "SELECT id, status FROM folders WHERE path = ?",
                (folder_path,),
            ).fetchone()
            if folder_row is None:
                raise RuntimeError("folder row not found")
            if folder_row["status"] == "missing":
                db.conn.execute(
                    "UPDATE folders SET status = 'ok' WHERE id = ? "
                    "AND status = 'missing'",
                    (folder_row["id"],),
                )
            db.add_workspace_folder(
                workspace_id, folder_row["id"], is_root=True,
            )
        except Exception as exc:
            db.conn.rollback()
            failures[folder_path] = str(exc)
            log.exception(
                "Linking duplicate-matched folder failed: %s", folder_path,
            )
            continue
        linked.add(folder_path)
        _invalidate_new_images(db, folder_path)
    return linked, failures


# Verdicts returned by the transports' ``enqueue``. The CALLER maps them
# onto the per-file loop's control flow, and the mapping is load-bearing:
# _ENQ_CANCELLED must map to ``break`` (Stop interrupted a destination
# read inside the transport — every further step this batch would touch
# the same, possibly dead, mount; mapping it to ``continue`` would
# reintroduce the wedged-mount cancellation pin fixed in PR #1423) — the
# same contract as ``_GATE_CANCELLED``. _ENQ_HANDLED maps to ``continue``
# (the file reached a terminal bucket or an adopted landing inside the
# transport); _ENQ_QUEUED also advances to the next file — the bytes move
# later, in ``flush_batch``.
_ENQ_HANDLED = "handled"
_ENQ_QUEUED = "queued"
_ENQ_CANCELLED = "cancelled"


def _book_copied(state, rel, attests_bytes):
    """Book one landed copy into the run counters.

    The two pre-merge sites incremented in different statement orders
    (local: copied → verified → folder count; rsync: copied → folder
    count → verified-if-attested). The statements are independent
    increments with no reads between them, so one order is picked here:
    copied → folder count → verified when the transport attests bytes
    (``attests_bytes=True`` reproduces the local transport's
    unconditional increment).
    """
    state.copied += 1
    _counts(state, rel)["copied"] += 1
    if attests_bytes:
        state.verified += 1


def _book_adoption(state, batch_st, checker, *, rel, source_file, dest_path,
                   verified_hash, record_hash, src_size, src_mtime_ns):
    """Book one crash-recovery adoption: count the duplicate skip, fold
    the file into ``landed`` with origin "skipped_duplicate" (see the
    rsync call site's rationale comment for why adoptions ride
    ``landed`` and deliberately NOT ``dup_skips``), and register the
    identity with the intra-run checker.

    ``verified_hash`` is the ledger-normalized hash (never None — the
    zero-byte convention is ``EMPTY_FILE_SHA256``); ``record_hash`` is
    the RAW source hash the checker's intra-batch maps rely on (still
    None for a zero-byte source on the rsync transport).
    """
    state.skipped_duplicate += 1
    _counts(state, rel)["skipped_duplicate"] += 1
    batch_st.landed.append(_LandedFile(
        dest_path=dest_path,
        verified_hash=verified_hash,
        source_path=str(source_file),
        origin="skipped_duplicate",
        src_size=src_size,
        src_mtime_ns=src_mtime_ns,
    ))
    _record_checker(
        state, checker, source_file, batch_st.dest_folder, record_hash,
    )


def _landed_copied(source_file, *, dest_path, verified_hash, src_size,
                   src_mtime_ns):
    """The ``origin="copied"`` ledger entry, shared by both transports.

    ``verified_hash`` arrives ledger-normalized (``EMPTY_FILE_SHA256``
    for zero-byte, never None) — see the call sites' normalization
    comments for the raw-vs-normalized split.
    """
    return _LandedFile(
        dest_path=dest_path,
        verified_hash=verified_hash,
        source_path=str(source_file),
        origin="copied",
        src_size=src_size,
        src_mtime_ns=src_mtime_ns,
    )


# Verdicts from ``_resolve_dest_collision``. _WALK_HANDLED means the file
# reached a terminal bucket inside the walk (a crash-recovery adoption or
# an intra-batch duplicate skip) and is fully booked — the caller returns
# _ENQ_HANDLED. _WALK_PLACED carries the destination BASENAME the caller
# must copy/queue the bytes to. Cancellation is NOT a verdict: the walk
# lets ``DestReadCancelled`` propagate to each transport's existing
# handler, which is the only place that knows how to book it.
_WALK_HANDLED = "handled"
_WALK_PLACED = "placed"
# A plain Stop observed BETWEEN candidate probes. Deliberately distinct
# from ``DestReadCancelled``, which the walk still lets propagate: that
# exception means a destination READ was interrupted mid-flight, i.e. the
# mount may be wedged, and its handlers set ``dest_read_cancelled``, which
# suppresses both the post-loop mount probe and the batch's catalog pass.
# A healthy Stop observed between probes is no evidence of a sick mount,
# and suppressing the catalog pass for it would leave already-copied files
# uncataloged until some later run adopted them. See the catalog block's
# "A plain user Stop on a healthy mount leaves dest_read_cancelled False"
# comment, and the Codex review of PR #1450.
_WALK_CANCELLED = "cancelled"


def _resolve_dest_collision(state, batch_st, ctx, *, source_file, rel,
                            checker, src_hash_fn, src_size, src_mtime_ns,
                            claims, stop_requested):
    """Walk the primary basename and its numeric suffixes to an adoption,
    an intra-batch duplicate skip, or a free destination basename.

    ONE walk for both transports (PR 7b; PR 7 adaptation 2 left a
    hand-mirrored copy inside each of them). The two remaining
    transport-shaped inputs are parameters rather than branches:

    ``src_hash_fn``: zero-arg callable returning the RAW source hash —
    ``None`` means a checker'd zero-byte source, the convention the
    intra-batch maps rely on. Memoized-lazy on the local transport (which
    may never need a source hash at all), precomputed on rsync (which
    already hashed eagerly for its ``queued_src_hashes`` dedup). Called
    only where a hash is genuinely required, and always AFTER the
    destination read, so a Stop mid-read is observed before the card is
    touched.

    ``claims``: the rsync transport's batch-scoped ``claimed_basenames``
    reservation map, or ``None`` on the local transport — where the
    filesystem itself is the reservation, because bytes land inside
    ``enqueue`` rather than at the batch's flush. That asymmetry is
    inherent to deferred transfer, not drift: for a same-basename
    byte-identical sibling the local path adopts the copy it just made
    while rsync dup-skips against the claim.

    ``DestReadCancelled`` (and any source-read ``OSError`` from
    ``src_hash_fn``) propagate to the caller's existing handlers. An
    unreadable *candidate*, by contrast, is advanced past — it is a
    destination artifact, not a source problem.

    Returns ``(_WALK_HANDLED, None)`` or ``(_WALK_PLACED, basename)``.
    """
    stem, suffix = os.path.splitext(source_file.name)
    counter = 0
    while True:
        if counter and stop_requested():
            # This iteration was reached by ADVANCING past a candidate,
            # and every advance path — claimed sibling, dangling entry,
            # source-backed entry, unreadable candidate, size mismatch,
            # hash mismatch — can run without calling _hash_dest_file,
            # which is the walk's only other cancellation poll. Without
            # this a Stop goes unobserved for the whole collision chain
            # while the walk keeps probing a (possibly slow, possibly
            # dying) network destination. Codex review of PR #1450;
            # before flip B the rsync walk got this for free by hashing
            # every existing candidate, and the local walk — which has
            # always size-gated — never had it at all.
            #
            # Reported as a VERDICT, not as ``DestReadCancelled``: no
            # destination read was interrupted here, so this must not
            # set ``dest_read_cancelled`` and suppress the batch's mount
            # probe and catalog pass. See ``_WALK_CANCELLED``.
            #
            # Gated on ``counter`` deliberately: the no-collision fast
            # path stays unpolled, so a Stop racing the orchestrator's
            # loop-top check cannot flip an otherwise-clean copy into a
            # cancellation.
            return _WALK_CANCELLED, None
        candidate = (source_file.name if counter == 0
                     else f"{stem}_{counter}{suffix}")
        cand_path = os.path.join(batch_st.dest_folder, candidate)
        if claims is not None:
            candidate_key = ctx.fold_basename(candidate)
            if candidate_key in claims:
                # Claimed earlier in this batch (a same-basename sibling
                # already queued). If that sibling has our exact bytes,
                # skip as an intra-batch duplicate; otherwise advance.
                # Gated on ``checker``: with ``skip_duplicates=False``
                # both files must be queued under distinct suffixes
                # instead of the second being counted as
                # ``skipped_duplicate`` — which would otherwise let a
                # verified run report ``safe_to_format=True`` because
                # ``copied + skipped_duplicate == discovered`` without an
                # off-card row for it. See PR #1113 review.
                if checker is not None and claims[candidate_key] == src_hash_fn():
                    state.skipped_duplicate += 1
                    _counts(state, rel)["skipped_duplicate"] += 1
                    batch_st.dup_skips.append((source_file, False))
                    _record_checker(state, checker, source_file,
                                    batch_st.dest_folder, src_hash_fn())
                    return _WALK_HANDLED, None
                counter += 1
                continue
        if os.path.lexists(cand_path):
            # ``lexists``, not ``exists``: a DANGLING symlink is a
            # directory entry that this walk must not treat as a free
            # slot. ``exists`` follows the link and reports False, so the
            # name got picked — and then ``copy_and_hash_verify``'s
            # no-overwrite ``os.link`` promote hit the entry that does
            # exist, raised FileExistsError, and failed a healthy card
            # file with the misleading reason "copy verification failed".
            # (It does not write THROUGH the link: the copy goes to a
            # hidden sibling temp and is promoted by link/rename, neither
            # of which follows a symlink. Verified before the fix; see
            # spec row 13.) Nothing to adopt either — there are no bytes
            # behind it — so advance.
            #
            # ONE stat serves as both the existence probe and the
            # metadata read: a dangling symlink is ``lexists`` but raises
            # ENOENT here, so the single ``except OSError`` covers the
            # dangling case AND a candidate that cannot be stat'd, and
            # both advance. Merged deliberately — a separate
            # ``os.path.exists`` would be a second round trip per
            # candidate on a network archive, and it made the
            # unreadable-candidate branch untestable by hiding which call
            # a test had to intercept (Codex review of PR #1450).
            try:
                cand_stat = os.stat(cand_path)
            except OSError:
                # Unreadable candidate (or a dangling symlink): advance
                # rather than fail the source file (PR 7b flip C).
                # Deliberately not folded into the zero-byte test below —
                # a failed stat must never read as "empty".
                counter += 1
                continue
            # Never hash, and never adopt, a candidate that is really
            # source media (spec decision 13). The hazard is precise: the
            # walk would read the CARD's own bytes back through the link,
            # byte-match them against themselves, book
            # ``skipped_duplicate``, and let ``safe_to_format`` go green
            # over bytes that exist nowhere but the card. Unlike the
            # orchestrator's primary-name guard this ADVANCES rather than
            # failing the file: one poisoned entry does not poison the
            # sibling slots, so the photo can still be imported safely.
            # (At counter 0 the orchestrator's guard has already rejected
            # this geometry with the same two probes, so this can only
            # fire from counter 1 on — it is kept unconditional so the
            # walk does not depend on its caller's ordering.)
            if _is_source_backed_dest(ctx, source_file, cand_path):
                log.warning(
                    "%s: destination candidate %s resolves to source "
                    "media; skipping it (never adopt bytes the card "
                    "still owns)", state.log_label, cand_path,
                )
                counter += 1
                continue
            # Already on disk. Byte-identical -> adopt; different ->
            # advance to the next suffix. Adopting matters because a
            # crash-interrupted retry may already have written THIS
            # source's bytes under an earlier suffix: an earlier run
            # copied a colliding different file to ``name.ext``, put this
            # source's bytes at ``name_1.ext``, then died before its
            # scan. Advancing past ``name_1.ext`` without hashing it
            # would re-copy identical bytes to ``name_2.ext`` and leave
            # two archive copies of one source photo. See PR #1107
            # review.
            if not stat_mod.S_ISREG(cand_stat.st_mode):
                # FIFOs, device nodes and sockets all stat as size 0, so
                # the zero-byte branch below would adopt one as this
                # source's archive copy without ever reading it. (Today
                # that ends as a spurious failure rather than a silent
                # lie — the post-scan validation finds no photo row and
                # reclassifies the file to failed — but failing a healthy
                # card file over a stray FIFO is still wrong.) The
                # size-MATCHED branch needs no such guard: it reads the
                # candidate, and a blocked FIFO read becomes an OSError
                # advance via _hash_dest_file's watchdog. A directory at
                # the candidate name lands here too, and advancing is
                # equally right for it. CodeRabbit review of PR #1450.
                counter += 1
                continue
            cand_size = cand_stat.st_size
            if src_size == 0 and cand_size == 0:
                # Zero-byte twin: identical by definition, but kept out
                # of the duplicate-identity index (see ingest), so it is
                # an adopted landing rather than a cataloged twin — a
                # crash may have left these bytes on disk before any
                # folder/photo row was committed. Size, not hash: a
                # checker'd zero-byte source hashes to None by
                # convention and could never match. Adopts at ANY
                # candidate position on both transports (PR 7b flip A).
                #
                # Re-stat the SOURCE first. ``src_size`` was captured by
                # ``enqueue``'s opening stat, and this is the one adopt
                # branch that reads nothing — the size-matched branch
                # below re-reads the source through ``src_hash_fn`` and
                # would notice a change. A source still being written to
                # the card can therefore be empty at that stat and have
                # real bytes by now, and adopting an empty destination
                # for it books ``skipped_duplicate`` over bytes that
                # exist only on the card: the destination validates fine
                # (it IS empty), and a verified run reports
                # ``safe_to_format=True``. Formatting then loses them.
                # If it is no longer empty, this candidate simply is not
                # a match — advance, and the copy/transfer that follows
                # moves the real bytes. An OSError here means we cannot
                # confirm emptiness, so advance for the same reason (the
                # copy then fails with the real error). Codex review of
                # PR #1450 (P1); the local primary-name case predates
                # PR 7b, flip A widened it to the other positions.
                try:
                    still_empty = source_file.stat().st_size == 0
                except OSError:
                    still_empty = False
                if not still_empty:
                    counter += 1
                    continue
                adopt = (cand_path, EMPTY_FILE_SHA256, EMPTY_FILE_SHA256)
            elif cand_size == src_size:
                # Size pre-check: equal hashes imply equal sizes, so a
                # mismatch rules out a byte match without reading the
                # file. The archive is often a network mount.
                try:
                    cand_hash = _hash_dest_file(cand_path, stop_requested)
                except DestReadCancelled:
                    # MUST come first: DestReadCancelled subclasses
                    # OSError, so the advance handler below would
                    # otherwise absorb a Stop and let the run keep
                    # touching the wedged destination (PR #1423).
                    raise
                except OSError:
                    cand_hash = None
                src_hash = src_hash_fn()
                if (cand_hash is not None and src_hash is not None
                        and cand_hash == src_hash):
                    # Byte-identical file already at the destination
                    # (e.g. a previous run died between copy and
                    # catalog). Treat as landed: catalog + stamp it
                    # rather than skipping — the designed self-heal for
                    # crash-shaped interruptions. ``src_hash`` is
                    # non-None here by the gate, so it serves as both
                    # the ledger and the raw record hash.
                    adopt = (cand_path, src_hash, src_hash)
                else:
                    adopt = None
            else:
                adopt = None
            if adopt is None:
                counter += 1
                continue
            dest_path, verified_hash, record_hash = adopt
            if claims is not None:
                # Claim with the RAW hash (``None`` for a checker'd
                # zero-byte source) so a later same-basename sibling
                # compares against the same convention the queued path
                # uses.
                claims[ctx.fold_basename(candidate)] = src_hash_fn()
            # ``_book_adoption`` folds the file into ``landed`` with
            # origin "skipped_duplicate" — deliberately NOT ``dup_skips``
            # — so the restricted scan catalogs it and the rollback
            # decrements the right counter. See its docstring.
            _book_adoption(
                state, batch_st, checker, rel=rel,
                source_file=source_file, dest_path=dest_path,
                verified_hash=verified_hash, record_hash=record_hash,
                src_size=src_size, src_mtime_ns=src_mtime_ns,
            )
            return _WALK_HANDLED, None
        return _WALK_PLACED, candidate


class _LocalTransport:
    """Byte transport for a locally mounted archive destination.

    Every file is copied AND hash-verified inside ``enqueue``
    (``copy_and_hash_verify`` reads the landed bytes back), so every
    ``copied`` booking also increments ``verified`` — ``attests_bytes``
    is unconditionally True. Nothing is deferred to ``flush_batch``.
    """

    attests_bytes = True
    dest_noun = "archive"
    log_label = "Import"

    def __init__(self, params, ctx):
        self._params = params
        self._ctx = ctx

    def enqueue(self, state, batch_st, *, source_file, rel, checker,
                stop_requested):
        # Local-rebinding shim: the body below is the local path's
        # per-file middle (moved verbatim in PR 7; its collision walk is
        # shared with the rsync transport since PR 7b).
        dest_folder = batch_st.dest_folder
        _stop_requested = stop_requested
        # Capture card-side (size, mtime_ns) BEFORE the copy so the
        # deferred working-copy pass can identity-check the card
        # override at extraction time. Byte-identical files have the
        # same size AND mtime; a rewrite between now and the end-of-
        # run extractor bumps mtime, and a remounted different card
        # at the same path has an unrelated mtime for its coincidence
        # of same-sized file. Without this the size-only check would
        # accept a same-size collision and cache a working copy for
        # the wrong bytes. Stat errors are the same class as
        # copy_and_hash_verify's OSError handling — fail just this
        # source rather than escape and kill the whole background job.
        try:
            src_stat = source_file.stat()
        except OSError as e:
            _fail(state, rel, source_file, str(e))
            return _ENQ_HANDLED
        src_size = src_stat.st_size
        src_mtime_ns = src_stat.st_mtime_ns
        try:
            # Source hash is potentially needed by the shared collision
            # walk (once per hashed candidate) and by the
            # copy_and_hash_verify src_hash arg. Compute lazily and cache
            # in a small closure so nothing hashes the card twice — and
            # so a run with no collisions never hashes the card at all.
            _sh_cache = [False, None]

            def _src_hash_cached(
                _sh_cache=_sh_cache, source_file=source_file,
            ):
                if not _sh_cache[0]:
                    _sh_cache[0] = True
                    _sh_cache[1] = (
                        checker.content_hash(source_file)
                        if checker is not None
                        else compute_file_hash(str(source_file))
                    )
                return _sh_cache[1]

            # Destination basename + collision handling, shared with
            # the rsync transport (``_resolve_dest_collision``). No
            # reservation map: bytes land inside this method, so the
            # filesystem itself is the reservation.
            verdict, dest_basename = _resolve_dest_collision(
                state, batch_st, self._ctx,
                source_file=source_file, rel=rel, checker=checker,
                src_hash_fn=_src_hash_cached,
                src_size=src_size, src_mtime_ns=src_mtime_ns,
                claims=None, stop_requested=_stop_requested,
            )
            if verdict is _WALK_HANDLED:
                return _ENQ_HANDLED
            if verdict is _WALK_CANCELLED:
                # Plain Stop between candidate probes. Book the run as
                # cancelled but leave ``dest_read_cancelled`` alone —
                # the mount is healthy, so this batch's earlier landings
                # must still be probed and cataloged.
                state.cancelled = True
                return _ENQ_CANCELLED
            dest_file = os.path.join(dest_folder, dest_basename)

            src_hash = (
                checker.content_hash(source_file)
                if checker is not None else None
            )
            ok, file_hash = copy_and_hash_verify(
                str(source_file), dest_file, src_hash=src_hash,
            )
        except DestReadCancelled:
            # Stop arrived mid-read against the destination (adopt/
            # collision hashing above). The file is neither copied nor
            # failed — it stays on the card for the next run, like any
            # file the cancel got to before its batch.
            state.cancelled = True
            batch_st.dest_read_cancelled = True
            return _ENQ_CANCELLED
        except OSError as e:
            _fail(state, rel, source_file, str(e))
            return _ENQ_HANDLED
        if not ok:
            _fail(
                state, rel, source_file,
                "copy verification failed (destination bytes do not "
                "match the source)",
            )
            return _ENQ_HANDLED
        _book_copied(state, rel, self.attests_bytes)
        batch_st.landed.append(_landed_copied(
            source_file, dest_path=dest_file,
            # ``copy_and_hash_verify`` returns the destination-read hash
            # (EMPTY_FILE_SHA256 for zero-byte) — already ledger-normalized.
            verified_hash=file_hash,
            src_size=src_size, src_mtime_ns=src_mtime_ns,
        ))
        _record_checker(state, checker, source_file, dest_folder, file_hash)
        return _ENQ_HANDLED

    def flush_batch(self, state, batch_st, *, rel, checker):
        # Copies land (and are verified) inside ``enqueue``; nothing is
        # deferred, so there is never anything to flush —
        # ``batch_st.to_transfer`` is always empty on this transport. The
        # orchestrator calls this unconditionally on both transports.
        return


class _RsyncTransport:
    """Byte transport for a remote (SSH) archive destination (Task 2.7).

    ``enqueue`` reserves a destination basename and queues the file on
    ``batch_st.to_transfer``; ``flush_batch`` moves the queued bytes with
    the per-batch rsync (flat batch + per-file renamed transfers +
    optional ``remote_verify_files``) and books the outcomes. Transfers
    go to ``remote_path/subpath/<rel>`` over SSH (``move.py`` plumbing);
    photos are cataloged at ``mount_path/subpath/<rel>`` —
    ``params.destination`` is the local mount base, so ``scan()`` walks
    the just-rsynced files exactly as it would a local copy. The mount
    is also what makes the shared duplicate gate honest here: a
    duplicate skip is only honest when the cataloged twin's bytes are
    confirmed at the destination, and the twin's file is locally
    readable through the mount, so the gate's on-disk re-hash contract
    applies unchanged.

    Verification honesty contract: rsync's own transfer integrity by
    default; a ``--checksum`` dry-run (``move.remote_verify_files``)
    only when ``params.verify_by_hash``. Catalog rows get
    ``hash_status='ok'`` + ``hash_checked_at`` ONLY on the checksum
    path; otherwise both stay NULL (no invented status values).
    Consequently a remote import without ``verify_by_hash`` honestly
    reports ``safe_to_format=False`` with the reason
    ``"enable verify_by_hash for remote verification"`` — the card is
    off-loaded but its landing wasn't independently hash-confirmed.
    Each ``_LandedFile`` carries the card-side hash so the shared
    stamping loop can cross-check the scanned MOUNT row against the
    hash confirmed on the NAS: without that carry-through, a stale or
    misconfigured mount base already containing ``<folder>/<filename>``
    would let scan() populate the row from unrelated bytes (or an
    unmounted base would leave no catalog trail at all) while blind
    stamping flipped ``safe_to_format`` green over storage this run
    never touched. See PR #1113 review.

    All ``move.py`` seams are resolved at call time as
    ``move_mod.<name>(...)`` so the tests' monkeypatch surface on the
    ``move`` module keeps working unchanged.
    """

    dest_noun = "mount"
    log_label = "Remote import"

    def __init__(self, job, runner, params, ctx):
        self._job = job
        self._runner = runner
        self._params = params
        self._ctx = ctx
        # Preamble moved verbatim from the pre-merge remote import
        # function (PR 7 transport merge).
        rt = params.remote_target
        remote = rt["remote"]                 # build_remote_move_spec dict
        rsync_bin = rt.get("rsync_bin") or remote.get("rsync_bin")
        ssh_base = rt["ssh_base"]             # remote_path/subpath (NAS side)

        import move as move_mod

        self._remote = remote
        self._rsync_bin = rsync_bin
        self._ssh_base = ssh_base
        self._move_mod = move_mod

    @property
    def attests_bytes(self):
        # A landed file carries a destination-attested hash only when
        # ``params.verify_by_hash`` made the independent card->NAS check
        # (rsync alone attests nothing).
        return self._params.verify_by_hash

    def _emit_transfer(self, state, rel, transfer_current, transfer_total,
                       current_file):
        """Report one actually-transferred file of the current batch rsync.

        Leaves ``current``/``total`` (and the ETA fields derived from them)
        exactly as the last ordinary ``_emit`` set them: this is the truth
        channel next to the prepared-files counter, not a second driver of
        it. ``_emit`` clears the transfer keys, so they exist only while a
        batch is on the wire.

        (``state`` is a parameter rather than a capture: the pre-move
        closure captured the enclosing run's ``state`` for
        ``state.folder_counts``; a method constructed before the run state
        exists takes it per call instead.)
        """
        job = self._job
        runner = self._runner
        extra = {
            "phase_current": transfer_current,
            "phase_total": transfer_total,
            "phase_label": "Transferring batch",
            **{k: job["progress"][k] for k in _IMPORT_ETA_PROGRESS_KEYS
               if k in job["progress"]},
        }
        job["progress"]["phase_current"] = transfer_current
        job["progress"]["phase_total"] = transfer_total
        job["progress"]["phase_label"] = "Transferring batch"
        job["progress"]["current_file"] = current_file
        runner.update_step(
            job["id"], "import",
            current_file=current_file,
            progress={
                "current": job["progress"]["current"],
                "total": job["progress"]["total"], **extra,
            },
        )
        runner.push_event(
            job["id"], "progress",
            progress_event(
                f"{rel}: transferring",
                job["progress"]["current"], job["progress"]["total"],
                current_file,
                # The Import page re-renders the folder table from each
                # event, so a transfer event without the snapshot would
                # blank the table for the whole batch transfer. (``rel_``
                # to avoid confusion with this function's ``rel``
                # parameter.)
                folders={
                    rel_: dict(counts)
                    for rel_, counts in state.folder_counts.items()
                },
                **extra,
            ),
        )

    def enqueue(self, state, batch_st, *, source_file, rel, checker,
                stop_requested):
        # Local-rebinding shim: the body below is the remote path's
        # per-file middle, moved verbatim; these bindings keep its lines
        # byte-identical to the pre-move text.
        dest_folder = batch_st.dest_folder
        _fold_basename = self._ctx.fold_basename
        _stop_requested = stop_requested
        # Working-copy identity, captured at decision time — before
        # any bytes move — so a source that changes mid-transfer
        # cannot look clean to the working-copy identity check.
        # Mirrors the local path, which stats before it hashes (and,
        # like it, fails the file when the source cannot be stat'd —
        # stat-first also means a vanished source costs one syscall,
        # not a full hash read that gets thrown away). Spec
        # decision 7.
        try:
            st = source_file.stat()
            src_size, src_mtime_ns = st.st_size, st.st_mtime_ns
        except OSError as e:
            _fail(state, rel, source_file, str(e))
            return _ENQ_HANDLED
        try:
            src_hash = (
                checker.content_hash(source_file)
                if checker is not None
                else compute_file_hash(str(source_file))
            )
        except OSError as e:
            _fail(state, rel, source_file, str(e))
            return _ENQ_HANDLED
        # Intra-batch same-content dedup: an earlier file THIS BATCH
        # queued a byte-identical source under a different basename
        # (e.g. ``DSC_0001.jpg`` and ``DSC_0002.jpg`` with the same
        # bytes). The batch's rsync hasn't happened yet, so the DB
        # twin lookup and ``checker.match()`` above can't see it —
        # skip here to match the local path's post-copy behaviour.
        # Gated on ``checker`` (``skip_duplicates=True``): the local
        # path only skips same-content/different-name twins through
        # ``DuplicateChecker``, so when the user disabled duplicate
        # skipping both files must be rsynced/cataloged under
        # distinct names instead of the second one being silently
        # counted as ``skipped_duplicate`` (which would otherwise let
        # a verified run report ``safe_to_format=True`` because
        # ``copied + skipped_duplicate == discovered`` without an
        # off-card row for the second file). See PR #1113 review.
        if (
            checker is not None
            and src_hash is not None
            and src_hash in batch_st.queued_src_hashes
        ):
            state.skipped_duplicate += 1
            _counts(state, rel)["skipped_duplicate"] += 1
            batch_st.dup_skips.append((source_file, False))
            _record_checker(state, checker, source_file, dest_folder, src_hash)
            return _ENQ_HANDLED
        # Collision parity (FIX 2): rsync lands files flat by basename,
        # so two different card files with the same basename in one batch
        # would clobber on the NAS. Assign a distinct dest basename per
        # colliding file, mirroring ingest()/the local path: a byte-
        # identical file already at the destination (a prior run's copy,
        # or an earlier card file this batch) is a skip; a different one
        # advances through numeric suffixes. ``claimed_basenames`` tracks
        # names taken by earlier files IN THIS BATCH; the mount is
        # locally readable so already-landed bytes are checked on disk.
        # The walk itself is shared with the local transport (PR 7b);
        # ``src_hash`` is already computed above for the intra-batch
        # dedup, so the callable is a constant here rather than lazy.
        try:
            verdict, dest_basename = _resolve_dest_collision(
                state, batch_st, self._ctx,
                source_file=source_file, rel=rel, checker=checker,
                src_hash_fn=lambda: src_hash,
                src_size=src_size, src_mtime_ns=src_mtime_ns,
                claims=batch_st.claimed_basenames,
                stop_requested=_stop_requested,
            )
        except DestReadCancelled:
            # Stop arrived mid-read against a candidate on the (possibly
            # dead) mount. Neither this file nor the rest of the batch
            # may take another step: every one of them touches the same
            # mount, and advancing would pin cancelling for the mount's
            # own timeout. The file stays on the card for the next run.
            # (Before PR 7b this was caught per-candidate inside the loop
            # and re-checked after it; the walk propagates instead, so
            # both transports book a dest-read cancel in exactly one
            # place. See PR #1423.)
            state.cancelled = True
            batch_st.dest_read_cancelled = True
            return _ENQ_CANCELLED
        if verdict is _WALK_HANDLED:
            return _ENQ_HANDLED
        if verdict is _WALK_CANCELLED:
            # Plain Stop between candidate probes — see the local
            # transport's twin. Not a dest-read cancel: the mount is
            # healthy, so this batch's earlier adoptions must still be
            # probed and cataloged.
            state.cancelled = True
            return _ENQ_CANCELLED
        batch_st.claimed_basenames[_fold_basename(dest_basename)] = src_hash
        # Only track queued source hashes when duplicate skipping is
        # enabled — the intra-batch dedup that consults this map is
        # gated on ``checker`` above, so populating it with
        # ``skip_duplicates=False`` would just be dead state.
        if checker is not None and src_hash is not None:
            batch_st.queued_src_hashes[src_hash] = dest_folder
        batch_st.to_transfer.append(
            (source_file, dest_basename, src_hash,
             src_size, src_mtime_ns))
        return _ENQ_QUEUED

    def flush_batch(self, state, batch_st, *, rel, checker):
        # Local-rebinding shim: the body below is the remote path's
        # transfer phase, moved verbatim (including its
        # ``to_transfer``/``cancelled`` guard); these bindings keep its
        # lines byte-identical to the pre-move text. ``_emit_transfer``
        # is partially applied with ``state`` so its call sites keep the
        # pre-move argument layout.
        job = self._job
        runner = self._runner
        params = self._params
        remote = self._remote
        rsync_bin = self._rsync_bin
        ssh_base = self._ssh_base
        move_mod = self._move_mod
        attests_bytes = self.attests_bytes
        dest_folder = batch_st.dest_folder
        _emit_transfer = functools.partial(self._emit_transfer, state)
        ssh_dest = (
            posixpath.join(ssh_base, *rel.split("/")) if rel != "."
            else ssh_base
        )
        # Honor cancellation before any network transfer starts. The break
        # inside the per-file queue-building loop above sets ``cancelled``
        # and exits the loop, but ``to_transfer`` still holds files that
        # were queued (decided but not yet sent). Without this guard the
        # rsync block below would start copying a partial batch after Stop
        # was requested. Queued files that never rsync stay on the card
        # and will be picked up by the next run; adopted ``landed``
        # entries for files already visible on the mount are still
        # cataloged by the batch-scan block below. See PR #1113
        # review.
        if batch_st.to_transfer and not state.cancelled:
            # ``--ignore-existing`` protects against basename-race overwrites:
            # two remote import jobs (or a job racing another writer) that
            # both passed the earlier mount-side os.path.exists check for
            # DSC_0001.jpg would otherwise both rsync to the same NAS name
            # with plain ``rsync -a``, and the second writer would clobber
            # the first's already-verified bytes. ``--ignore-existing`` tells
            # rsync's receiver to skip files that already exist there, so
            # the first landing's bytes stay put. On the verify path, the
            # subsequent ``rsync -an --checksum`` step then detects the
            # mismatch between the second writer's card bytes and the
            # first-writer bytes on the NAS and fails that specific file
            # honestly; without verification the honesty gate already
            # reports safe_to_format=False for the whole run, so a masked
            # race can't quietly flip the pill green. Crash-recovery already
            # avoids re-transferring files it saw on the mount (hash match
            # -> skip; hash mismatch -> advance to a suffix that doesn't
            # exist), so no legitimate flow relies on rsync overwriting an
            # existing destination file. See PR #1113 review.
            extra_args = [
                "-e", move_mod._ssh_rsh_string(remote),
                "--partial-dir=.rsync-partial",
                "--ignore-existing",
                # ``--copy-links`` dereferences symlinked source files so
                # the NAS receives their referenced bytes instead of a
                # symlink. The base command is ``rsync -a``, which preserves
                # symlinks; without ``-L`` a curated card folder that
                # symlinks to ``/Volumes/Card/DCIM/IMG_0001.JPG`` would send
                # the symlink itself to the NAS. With ``verify_by_hash``
                # the mount-side scan follows the symlink through the local
                # mount, so ``safe_to_format`` can still go green — and
                # then formatting/unmounting the card breaks the archived
                # copy. Card-local symlinks aren't legitimately preserved
                # by an archive of the card contents, and the local path's
                # ingest reads the referenced file bytes too. See PR #1113
                # review.
                "--copy-links",
            ]
            if remote.get("bwlimit_kbps"):
                extra_args.append(f"--bwlimit={int(remote['bwlimit_kbps'])}")
            rsync_target = move_mod.rsync_dest_spec(remote, ssh_dest)
            # rsync creates the leaf itself but not intermediate parents.
            ok_mkdir, mkdir_detail = move_mod._remote_mkdir_p(remote, ssh_dest)
            if not ok_mkdir:
                for sf, _bn, _sh, _sz, _mt in batch_st.to_transfer:
                    _fail(state, rel, sf,
                          f"remote mkdir failed for {ssh_dest}: {mkdir_detail}")
            else:
                # Split into the flat fast path (dest basename == card
                # basename) and collision-renamed files (transferred and
                # verified individually to an explicit NAS filename, since a
                # flat --files-from list to one dir can't rename).
                flat = [
                    (sf, bn, sh, sz, mt)
                    for sf, bn, sh, sz, mt in batch_st.to_transfer
                    if bn == sf.name
                ]
                renamed = [
                    (sf, bn, sh, sz, mt)
                    for sf, bn, sh, sz, mt in batch_st.to_transfer
                    if bn != sf.name
                ]

                def _do_rsync(src_specs, target, dest_is_dir, extra_args,
                              progress_cb=None):
                    try:
                        rc, stderr, timed_out = move_mod._run_rsync_streamed(
                            None, target, [], len(src_specs), progress_cb,
                            rsync_bin=rsync_bin, extra_args=extra_args,
                            src_specs=src_specs,
                            src_specs_dest_is_dir=dest_is_dir,
                            # Stop must reach the subprocess: a slow batch
                            # can otherwise pin "cancelling" for hours,
                            # since cancellation is only observed at file/
                            # batch boundaries the transfer never yields.
                            # Non-blocking probe: this fires from the rsync
                            # watchdog thread, and ``is_cancelled`` would
                            # park it inside ``wait_if_paused`` on Pause,
                            # disabling both stall detection and Stop until
                            # the user resumes. Pause is handled at the
                            # existing batch-boundary check below.
                            cancel_check=lambda: runner.cancellation_requested(
                                job["id"]),
                        )
                        return rc, stderr, timed_out
                    except OSError as exc:
                        return 1, str(exc), False

                def _rsync_cancelled(rc):
                    # A Stop mid-transfer kills the subprocess via the
                    # cancel_check above and surfaces as a nonzero exit.
                    # That is cancelled work, not a pile of per-file
                    # failures: queued files stay on the card for the next
                    # run, and whatever rsync landed before the kill is
                    # adopted by crash-recovery like any mid-batch stop
                    # (--partial-dir preserves the interrupted file).
                    # Non-blocking probe matches the watchdog above; the
                    # batch-boundary ``is_cancelled`` further down is where
                    # pause is actually observed.
                    return rc != 0 and runner.cancellation_requested(
                        job["id"])

                transferred = []   # (sf, dest_basename, src_hash, src_size, src_mtime_ns, nas_full_path)
                batch_size = len(batch_st.to_transfer)
                # Flat batch: one rsync into the dir. rsync names each file
                # as it lands; forward that as the batch's honest
                # actually-crossed-the-network counter next to the
                # prepared-files counter (which already reads ``emitted``).
                if flat:
                    rc, stderr, timed_out = _do_rsync(
                        [str(sf) for sf, _bn, _sh, _sz, _mt in flat],
                        rsync_target, True,
                        extra_args,
                        progress_cb=lambda done, _tot, name, _label,
                        _rel=rel, _bs=batch_size:
                            _emit_transfer(_rel, done, _bs, name))
                    if timed_out:
                        for sf, _bn, _sh, _sz, _mt in flat:
                            _fail(state, rel, sf, "rsync stalled (no progress)")
                    elif _rsync_cancelled(rc):
                        state.cancelled = True
                    elif rc != 0:
                        for sf, _bn, _sh, _sz, _mt in flat:
                            _fail(state, rel, sf, f"rsync failed: {stderr.strip()}")
                    else:
                        for sf, bn, sh, sz, mt in flat:
                            transferred.append((
                                sf, bn, sh, sz, mt,
                                posixpath.join(ssh_dest, bn)))
                # Renamed files: one rsync each to the explicit NAS file
                # path (rsync <card> user@host:/dir/DSC_0001_1.jpg).
                for sf, bn, sh, sz, mt in renamed:
                    if state.cancelled or runner.is_cancelled(job["id"]):
                        state.cancelled = True
                        break
                    nas_full = posixpath.join(ssh_dest, bn)
                    rc, stderr, timed_out = _do_rsync(
                        [str(sf)],
                        move_mod.rsync_dest_spec(remote, nas_full), False,
                        extra_args)
                    if timed_out:
                        _fail(state, rel, sf, "rsync stalled (no progress)")
                    elif _rsync_cancelled(rc):
                        state.cancelled = True
                        break
                    elif rc != 0:
                        _fail(state, rel, sf, f"rsync failed: {stderr.strip()}")
                    else:
                        transferred.append((sf, bn, sh, sz, mt, nas_full))
                        # ``len(transferred)`` counts only files that truly
                        # landed, so a failed flat batch can't inflate the
                        # renamed files' transfer counter.
                        _emit_transfer(rel, len(transferred), batch_size, bn)

                for sf, bn, src_hash, sz, mt, nas_full in transferred:
                    dest_path = os.path.join(dest_folder, bn)
                    # Independent verification (Task 2.7 FIX 1): card -> NAS,
                    # opt-in behind ``verify_by_hash`` (it reads every NAS
                    # byte; same knob the local path uses). This compares the
                    # actual CARD file against its NAS counterpart — the only
                    # check that confirms the card's bytes landed intact;
                    # comparing the SMB mount view against the NAS would be
                    # near-tautological (same physical storage). By default
                    # the transfer relies on rsync's own integrity checking
                    # and the run reports ``safe_to_format=False`` (honesty
                    # gate below) because no independent hash was made.
                    if params.verify_by_hash:
                        if bn == sf.name:
                            v = move_mod.remote_verify_files(
                                rsync_bin, [str(sf)], rsync_target,
                                remote, dest_is_dir=True)
                        else:
                            # Collision-renamed: verify against the explicit
                            # NAS name (file->file), not the card basename.
                            v = move_mod.remote_verify_files(
                                rsync_bin, [str(sf)],
                                move_mod.rsync_dest_spec(remote, nas_full),
                                remote, dest_is_dir=False)
                        if v is not None:
                            name, detail = v
                            reason = (
                                f"remote verification failed "
                                f"({detail or name})"
                                if name == "__ERROR__"
                                else f"remote verification: '{name}' missing "
                                     f"or differs at destination"
                            )
                            _fail(state, rel, sf, reason)
                            continue
                    _book_copied(state, rel, attests_bytes)
                    batch_st.landed.append(_landed_copied(
                        sf, dest_path=dest_path,
                        # checker.content_hash returns None for size-0;
                        # the ledger convention is EMPTY, scan's row
                        # convention is NULL — normalize here, once,
                        # instead of in every consumer. Do NOT
                        # normalize ``src_hash`` itself: the
                        # intra-batch dedup maps
                        # (claimed_basenames/queued_src_hashes), the
                        # adopt gate, and ``_record_checker`` rely on
                        # the None convention.
                        verified_hash=src_hash
                        if src_hash is not None
                        else EMPTY_FILE_SHA256,
                        src_size=sz, src_mtime_ns=mt,
                    ))
                    _record_checker(state, checker, sf, dest_folder, src_hash)


def run_import_job(job, runner, db_path, workspace_id, params):
    """Copy card(s) -> archive, hash-verify, and catalog incrementally.

    THE import orchestrator, for both destinations: picks the byte
    transport from ``params.remote_target`` (``_LocalTransport`` /
    ``_RsyncTransport``) and runs the single batch loop over it — every
    non-transport behavior lands here exactly once.

    Returns the result dict (counts + per-folder breakdown). The catalog
    is committed per batch; cancellation and crashes leave every already-
    verified file cataloged and nothing else.
    """
    from pipeline_job import (
        _missing_archive_mount_root,
        _unmounted_since_baseline,
    )
    from scanner import scan

    db = Database(db_path)
    db.set_active_workspace(workspace_id)

    # Normalized destination + mount baseline + guards. See
    # ``_build_destination_context`` — the ordering inside it
    # (normalize → baseline → guards) is load-bearing.
    ctx = _build_destination_context(db, params)
    destination = ctx.destination
    mount_baseline = ctx.mount_baseline

    # Byte transport — the ONLY transport-divergent code. Local: every
    # file is copied and hash-verified inside ``enqueue``. Rsync (SSH):
    # ``enqueue`` reserves and queues; ``flush_batch`` moves the batch's
    # bytes. Everything below runs identically over either.
    transport = (
        _RsyncTransport(job, runner, params, ctx)
        if params.remote_target is not None
        else _LocalTransport(params, ctx)
    )

    runner.set_steps(job["id"], [
        {"id": "import", "label": "Copy & catalog"},
    ])
    runner.update_step(job["id"], "import", status="running")

    state = _ImportRunState(log_label=transport.log_label)
    eta = _ImportEtaEstimator(
        expected_new=(params.checked_count if params.skip_duplicates else None),
    )

    _emit = _make_emitter(job, runner, state, eta)

    # --- Discover → select → batch (shared planning phase) ----------
    # Destructured BY NAME (see ``_ImportPlan``: same-typed field
    # transpositions survive a positional unpack and the parity test).
    # ``plan.files``/``plan.timestamps`` are consumed inside the planner;
    # the loop below works from ``batches``.
    plan = _plan_import(db, params, _emit, state)
    discovered = plan.discovered
    source_snapshots = plan.source_snapshots
    include_paths = plan.include_paths
    queued = plan.queued
    deselected = plan.deselected
    vanished_paths = plan.vanished_paths
    appeared = plan.appeared
    checker = plan.checker
    batches = plan.batches

    # --- Ledger -----------------------------------------------------
    # Every discovered file ends in exactly one terminal bucket on
    # ``state``. ``state.verified``: count of files independently
    # checksum-verified (every copy when the transport attests bytes).
    # ``state.imported_photo_ids``: photo rows this run created or
    # landed bytes into: hash-stamped fresh copies, adopted
    # (crash-recovery) landings whose pre-existing row now belongs to
    # this run, and RAW primaries that adopted a landed companion JPEG.
    # The after-import chaining hook scopes its process job to exactly
    # these (cataloged-twin duplicate skips are excluded — a
    # duplicates-only import chains to "no new photos", not an empty
    # run).

    _stop_requested = _make_stop_check(runner, job)

    # Working-copy extraction is DEFERRED to the end of the whole import
    # run (not per-batch). Rationale: a folder that receives more than
    # ``IMPORT_BATCH_SIZE`` files splits across multiple batches; a
    # RAW+JPEG companion pair can then straddle a batch boundary — the
    # RAW lands in batch N and its JPEG in batch N+1. Per-batch
    # extraction would run on the RAW before scan()'s
    # ``_pair_raw_jpeg_companions`` sees the JPEG, so the RAW's row
    # still has ``companion_path IS NULL``. The extractor then reads the
    # RAW itself (RAW-decode-first); if that fails or produces a
    # low-quality fallback, ``working_copy_failed_at`` is set and the
    # candidate predicate would gate future retries — the JPEG landing
    # in the next batch never re-triggers extraction while the card-side
    # JPEG is still available. Deferring to end-of-run means every
    # companion in the run has landed and been paired before
    # ``_extract_working_copies`` decides which source to read.
    # ``state.wc_source_paths``: dest_path -> (card_src_path,
    # expected_size, expected_mtime_ns).
    # The identity tuple is captured at land time (before any WC
    # extraction pass), so the deferred ``_extract_working_copies`` call
    # can verify the override still holds the exact bytes we copied —
    # not just any same-sized file that happens to be at the same path.
    # A rewrite or a reused-mount collision differ in mtime and get
    # rejected; extraction falls back to the verified archive copy.
    # ``state.wc_dest_folders`` is the extraction's exact-match scope.
    # ``state.run_dest_folders``: intra-run duplicate destinations —
    # token -> dest folder where the identity landed this run (mirrors
    # ingest's batch_dest_folders).
    # ``state.run_verified_hashes``: byte-proven verified hash for each
    # intra-run token. A ('hash', h)
    # token's own value IS the proof; a ('key', …) token carries no bytes,
    # so accepting a later key match against a run twin requires hashing
    # the current source and comparing against this recorded value (two
    # different files with the same filename+size+capture-second across
    # cards would otherwise be counted as skipped_duplicate without ever
    # verifying bytes — safe_to_format green, second card is only copy).
    # ``state.linked_dup_dirs``: dup-twin dirs already scanned+linked.

    # ``state.mount_ever_lost``: sticky across the rest of the run once
    # a mounted → unmounted transition is observed. The per-batch
    # rollback below undoes ``dup_skips``, ``landed``, and queued
    # ``to_transfer`` entries, but not the identities the same batch
    # already installed in the job-wide ``checker`` (and in
    # ``run_dest_folders`` / ``run_verified_hashes``) via
    # ``_record_checker`` — and ``DuplicateChecker`` exposes no removal
    # API, so those entries cannot be surgically undone. If the share
    # remounts before a later batch, a same-content card file would hit
    # the intra-run fast path and be counted as a duplicate of a landing
    # whose archive claim was rolled back. Refusing every remaining
    # batch keeps the stale intra-run cache from ever being consulted.
    # See PR #1400 review (Codex P2 r3688614624).

    # Dup-folder linking runs in a SEPARATE ``scan(restrict_dirs=…)`` call
    # after the duplicate skip; its exception was previously logged and
    # swallowed, leaving safe_to_format true while the imported
    # duplicates never became visible in the active workspace. Track it
    # explicitly (``state.dup_link_failed``) so safe_to_format reflects
    # "workspace can actually see these bytes" and not just "the bytes
    # are somewhere on disk".

    # Whether each ``copied`` booking also incremented ``verified``:
    # the local transport hash-verifies every copy
    # (``copy_and_hash_verify`` reads the landed bytes back); the rsync
    # transport books ``verified`` only when ``params.verify_by_hash``
    # made the independent card->NAS check — so
    # ``_reclassify_landed_failed`` must undo ``verified`` exactly when
    # the transport attests bytes. Bound once: the rsync property is
    # constant for the run.
    attests_bytes = transport.attests_bytes

    for rel, batch in batches:
        if runner.is_cancelled(job["id"]):
            state.cancelled = True
            break

        # Shared guard chain (ordering is the 2026-07-30 incident
        # ordering — see ``_batch_preflight``); ``None`` means the whole
        # batch was refused and booked failed.
        dest_folder = _batch_preflight(
            state, _emit, db, rel=rel, batch=batch, queued=queued, ctx=ctx,
            missing_root_check=lambda: _missing_archive_mount_root(ctx.destination),
        )
        if dest_folder is None:
            continue

        # Field rationale lives on ``_ImportBatchState``; the rsync-only
        # trio stays empty on the local transport.
        batch_st = _ImportBatchState(rel=rel, dest_folder=dest_folder)

        for source_file in batch:
            if runner.is_cancelled(job["id"]):
                state.cancelled = True
                break
            if not batch_st.mount_lost:
                batch_st.mount_lost = _unmounted_since_baseline(mount_baseline)
            if batch_st.mount_lost:
                state.emitted += 1
                _fail(
                    state, rel, source_file,
                    f"archive mount root {batch_st.mount_lost} detached while this "
                    "batch was in progress (the directory persists but "
                    "the share is gone, so neither further writes nor a "
                    "duplicate match against it can be trusted)",
                )
                continue
            state.emitted += 1
            _emit(
                f"{rel}: importing", state.emitted, queued, source_file.name,
                is_importing=True,
            )

            verdict = _duplicate_gate(
                state, batch_st, source_file=source_file, rel=rel,
                checker=checker, db=db, params=params, ctx=ctx,
                stop_requested=_stop_requested,
            )
            if verdict is _GATE_CANCELLED:
                # Stop interrupted a destination read inside the gate —
                # nothing further this batch may touch the mount.
                break
            if verdict is _GATE_SKIPPED:
                continue

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
                state, ctx, rel=rel, source_file=source_file,
                dest_file=os.path.join(dest_folder, source_file.name),
            ):
                continue

            verdict = transport.enqueue(
                state, batch_st, source_file=source_file, rel=rel,
                checker=checker, stop_requested=_stop_requested,
            )
            if verdict is _ENQ_CANCELLED:
                # Stop was observed inside the transport — either a
                # destination read was interrupted mid-flight (the
                # transport also set ``dest_read_cancelled``, and nothing
                # further this batch may touch the mount) or the walk saw
                # a plain Stop between candidate probes (mount healthy;
                # the batch's earlier landings still get probed and
                # cataloged below). Either way, stop feeding files.
                break
            # _ENQ_HANDLED / _ENQ_QUEUED: the file reached a terminal
            # bucket (or an adopted landing) inside the transport, or its
            # bytes were queued for ``flush_batch`` — next file either way.

        # The per-file probe runs BEFORE each copy, so it cannot see a
        # detach that happens during the last (or only) file — there is no
        # next iteration to catch it. Probe once more here, after the loop
        # and before anything is cataloged, so the whole batch including
        # its final file is covered. See PR #1396 review (Codex P1
        # r3687456172).
        #
        # This is the last probe that can help: a detach after this point
        # races the catalog scan itself, which is irreducible — no probe
        # can make a network filesystem stay mounted across a write. What
        # it does guarantee is that nothing is booked as archived without
        # a mount check on both sides of every copy in the batch.
        #
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
            batch_st.mount_lost = _unmounted_since_baseline(mount_baseline)

        # A detach invalidates this batch's duplicate skips, adoptions,
        # and copies alike — see ``_rollback_on_mount_loss`` for the
        # per-bucket rationale and the load-bearing rollback order.
        # (At this point ``landed`` holds copies on the local transport
        # but only adoptions on rsync — fresh transfers append inside
        # ``flush_batch`` below.)
        if batch_st.mount_lost:
            _rollback_on_mount_loss(
                state, batch_st, attests_bytes,
                # Books failures for queued-but-untransferred files and
                # empties the queue; a no-op on the local transport
                # (copies land in ``enqueue``, so ``to_transfer`` is
                # always empty).
                extra_rollback=functools.partial(
                    _drop_queued_transfers, state, batch_st, rel),
            )

        # Transfer the queued batch — the transport owns the whole
        # phase, including the ``to_transfer``/``cancelled`` guard.
        # Called unconditionally: a no-op on the local transport (copies
        # land inside ``enqueue``; ``to_transfer`` is always empty) and
        # whenever nothing was queued or Stop was observed.
        transport.flush_batch(state, batch_st, rel=rel, checker=checker)

        # --- Catalog this batch (even when cancelled mid-batch: what
        # landed on disk must be cataloged before we stop, so every
        # stopping point is a valid catalog state). Bounded by the batch
        # size, so no cancel_check is passed — it runs to completion.
        #
        # Skip when a destination-side hash in the per-file loop above
        # cancelled mid-read. That signal means the mount is misbehaving,
        # and ``scan()`` here — plus the ``_rehash_dest_or_none``
        # re-checks below — would touch the same wedged mount and pin the
        # job in "cancelling" for the mount's own timeout. Already-copied
        # landings are picked up by the next run's crash-recovery
        # adoption (byte-identical files match by hash and count as
        # ``skipped_duplicate``). A plain user Stop on a healthy mount
        # leaves ``dest_read_cancelled`` False, so partially-landed
        # batches keep cataloging like before. See PR #1423 review
        # (Codex P2 r3716433824, r3716433830).
        if batch_st.landed and not batch_st.dest_read_cancelled:
            pre_scan_hashes = _catalog_scan_and_prescan(
                state, batch_st, db, params, scan, destination, rel,
                attests_bytes, runner=runner, job=job,
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
                dest_noun=transport.dest_noun,
                stop_requested=_stop_requested,
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

        _link_twins_and_emit(
            state, batch_st, db, workspace_id, _emit, rel, queued,
        )
        if state.cancelled:
            break

    _extract_deferred_working_copies(state, params, runner, job, db)

    # ``remote_unverified`` is the honesty gate — only a transport that
    # independently confirms bytes at the destination may leave it
    # False; see the append inside ``_finalize_import``. Degenerates to
    # False on the local transport (every copy is hash-verified),
    # exactly the parameter's old default there. ``attests_bytes`` is
    # the run-constant binding of ``transport.attests_bytes`` above.
    return _finalize_import(
        job, runner, db, state, params,
        discovered=discovered, include_paths=include_paths,
        source_snapshots=source_snapshots, deselected=deselected,
        vanished_paths=vanished_paths, appeared=appeared,
        remote_unverified=not attests_bytes,
    )
