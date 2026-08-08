"""Import job: copy card -> archive directly, hash-verify, catalog incrementally.

Implements the import half of the import/process split (design doc
2026-07-04-import-process-split-design.md). The core invariant:

    A photo row is created only when its file verifiably exists at its
    final archive path.

Files are copied per destination-folder batch, each copy is verified by
content hash before promotion, and each batch is cataloged via the
scanner's restricted-scan path immediately after it lands. A run that
dies at any point leaves a valid partial catalog; a retry's duplicate
gate skips exactly the files that landed and copies the rest. There is
no staging tree and no unwind step (`_deindex_staging` has no
equivalent here).

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
import hashlib
import json
import logging
import os
import posixpath
import shutil
import sys
import threading
import time
import uuid
from dataclasses import dataclass
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


@dataclass
class _LandedFile:
    """One file this batch landed (fresh copy/transfer) or adopted.

    ``verified_hash`` is the hash the import attests is at ``dest_path``
    (copy-time hash locally; card-side hash remotely). ``origin`` is
    "copied" or "skipped_duplicate" (adoption) and drives rollback
    accounting in ``_reclassify_landed_failed``.
    """
    dest_path: str
    verified_hash: str | None
    source_path: str
    origin: str
    src_size: int | None
    src_mtime_ns: int | None


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
    ``safe_to_format`` and ``unverified_duplicates_only``, on both the local
    (``run_import_job``) and remote (``_run_remote_import_job``) copy paths.
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

    THE single home for the pre-copy-loop selection block: both copy paths
    (``run_import_job`` and ``_run_remote_import_job``) call this, so a
    change reaches both. It used to be duplicated, and the copies' comments
    had diverged on the very commit that created them.

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


def _run_remote_import_job(job, runner, db, workspace_id, params):
    """Import to a remote (SSH) archive destination (Task 2.7).

    Groups the card into destination-folder batches exactly like the local
    path, but transfers each batch with a single per-batch rsync to
    ``remote_path/subpath/<rel>`` over SSH (``move.py`` plumbing) instead of
    ``copy_and_hash_verify``. Photos are cataloged at
    ``mount_path/subpath/<rel>`` — ``params.destination`` is the local mount
    base, so ``scan()`` walks the just-rsynced files exactly as it would a
    local copy.

    Verification: rsync's own transfer integrity by default; a ``--checksum``
    dry-run (``move._remote_verify_complete``) only when
    ``params.verify_by_hash``. Catalog rows get ``hash_status='ok'`` +
    ``hash_checked_at`` ONLY on the checksum path; otherwise both stay NULL
    (no invented status values). Consequently a remote import without
    ``verify_by_hash`` honestly reports ``safe_to_format=False`` with the
    reason ``"enable verify_by_hash for remote verification"`` — the card is
    off-loaded but its landing wasn't independently hash-confirmed.
    """
    from pipeline_job import (
        _archive_mount_baseline,
        _load_known_mount_roots,
        _missing_archive_mount_root,
        _record_known_mount_roots,
        _unmounted_since_baseline,
    )
    from scanner import scan

    rt = params.remote_target
    remote = rt["remote"]                 # build_remote_move_spec dict
    rsync_bin = rt.get("rsync_bin") or remote.get("rsync_bin")
    ssh_base = rt["ssh_base"]             # remote_path/subpath (NAS side)
    # The catalog/mount base is params.destination (the route sets it to
    # mount_path/subpath). Normalize identically to the local path.
    try:
        destination = os.path.realpath(os.path.normpath(str(params.destination)))
    except OSError:
        destination = os.path.normpath(str(params.destination))

    # Live-mount baseline for the destination, captured before discovery
    # for the same reason as the local path: a detach during the slow
    # pre-copy phases would otherwise be baked in as "never mounted" and
    # disarm the guard. See ``_archive_mount_baseline`` and PR #1396
    # review (Codex P1 r3687336684).
    #
    # Cross-run mount history: seed the baseline True for mount roots we
    # previously observed live, then persist any candidate we see live
    # this run. Without this, an SMB share that detached BEFORE the run
    # started never earns a True → False transition; rsync would still
    # push to the NAS while the per-batch mount-side scan reads a fresh
    # local shadow. See PR #1396 review (Codex P1 r3687401636).
    known_mount_roots = _load_known_mount_roots(db)
    mount_baseline = _archive_mount_baseline(destination, known_mount_roots)
    _record_known_mount_roots(db, mount_baseline)

    # Reject cataloged twins that live under the card being imported: a stale
    # scan of the mounted card can leave a photos row whose ``folder_path``
    # IS the card, and re-hashing it just re-reads the very source we're
    # supposed to be copying off — which would count the file as
    # ``skipped_duplicate`` and, when ``verify_by_hash`` is on, still let
    # ``safe_to_format`` go green over a card whose bytes never crossed the
    # network. Mirrors the local path's ``_path_under_any_source`` filter.
    _path_under_any_source = _build_source_root_guard(params.sources)

    # Destination containment for cataloged twin folders. Used to scope
    # ``_linkable_twin_dirs`` to twins under the mount base — an off-
    # destination twin in some other library root is none of this import's
    # business. Case-fold on inconclusive/insensitive filesystems (SMB, FAT,
    # HFS+/APFS) so a differently-cased twin under the mount still matches;
    # otherwise a duplicate-only remote import could report
    # ``safe_to_format=True`` while the twin's folder never gets linked
    # into the active workspace. Mirrors the local path.
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

    import move as move_mod

    runner.set_steps(job["id"], [
        {"id": "import", "label": "Copy & catalog"},
    ])
    runner.update_step(job["id"], "import", status="running")

    copied = 0
    eta = _ImportEtaEstimator(
        expected_new=(params.checked_count if params.skip_duplicates else None),
    )

    # Live per-folder counters, mutated by the copy loop via _counts() and
    # snapshotted onto every progress event so the Import page can render
    # truthful per-folder progress mid-run. Declared before _emit so the
    # discovery-phase emits see an empty-but-present dict. Mirrors the
    # local path.
    folder_counts = {}

    def _emit(phase, current, total, current_file="", *, is_importing=False):
        eta_fields = {}
        if total > 0:
            if is_importing:
                eta.note_importing(copied)
            else:
                eta.note_batch_complete(current, copied)
            eta_fields = eta.fields(total)
        job["progress"]["current"] = current
        job["progress"]["total"] = total
        job["progress"]["current_file"] = current_file
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
                # consumers must see the state at emit time). Mirrors the
                # local path — spec decision 1.
                folders={
                    rel: dict(counts) for rel, counts in folder_counts.items()
                },
                **eta_fields,
            ),
        )

    def _emit_transfer(rel, transfer_current, transfer_total, current_file):
        """Report one actually-transferred file of the current batch rsync.

        Leaves ``current``/``total`` (and the ETA fields derived from them)
        exactly as the last ordinary ``_emit`` set them: this is the truth
        channel next to the prepared-files counter, not a second driver of
        it. ``_emit`` clears the transfer keys, so they exist only while a
        batch is on the wire.
        """
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
                    for rel_, counts in folder_counts.items()
                },
                **extra,
            ),
        )

    # --- Discover (same enumeration-error handling as the local path) ---
    _emit("Discovering files", 0, 0)
    files = []
    discovery_errors = []

    def _discovery_onerror(exc):
        discovery_errors.append(exc)
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

    # Selection: filter the copy set and measure drift. Shared with the local
    # path — see ``_apply_selection`` for why each condition is shaped the
    # way it is. Destructured BY NAME, not by position: four of the six
    # fields are plain ints and two are sets, so a positional unpack that
    # transposed a same-typed pair (``queued``/``deselected``,
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

    timestamps = _source_file_timestamps(
        files,
        capture_times=(
            {str(f): checker.capture_time(f) for f in files}
            if checker is not None and not checker.verify_by_hash
            else None
        ),
    )

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

    # --- Ledger ---------------------------------------------------------
    verified = 0            # count of files independently checksum-verified
    skipped_duplicate = 0
    unverified_duplicate = 0
    failed = 0
    unsafe_files = []
    emitted = 0
    cancelled = False

    def _stop_requested():
        # Threaded through every destination-side hash read so a Stop can
        # interrupt a read blocked on a dead mount (see _hash_dest_file).
        # Nonblocking probe — ``is_cancelled`` would park in
        # ``wait_if_paused`` for a pausable import, freezing the watchdog
        # loop itself and stopping the 120s stall timer from running while
        # the daemon reader can keep touching the archive even though the
        # UI says the job is paused. Mirrors the rsync watchdog's use of
        # ``cancellation_requested`` for the same reason.
        return runner.cancellation_requested(job["id"])

    wc_source_paths = {}
    wc_dest_folders = set()
    # Photo rows this run created or landed bytes into: fresh copies whose
    # mount row was cataloged, adopted duplicates whose pre-existing mount
    # row now belongs to this run, verified cataloged-twin skips, and RAW
    # primaries that adopted a landed JPEG companion. The after-import
    # chaining hook scopes its process job to exactly these; without it a
    # successful remote import falls into the "no new photos" branch and
    # the requested process job never runs.
    imported_photo_ids = set()
    # Dup-twin dirs already linked across batches.
    linked_dup_dirs = set()
    # A duplicate-only batch's workspace visibility depends on the direct
    # DB link; if it fails, safe_to_format must remain false.
    dup_link_failed = False

    def _counts(rel):
        return folder_counts.setdefault(
            rel, {"copied": 0, "skipped_duplicate": 0, "failed": 0},
        )

    def _fail(rel, source_file, reason):
        nonlocal failed
        failed += 1
        _counts(rel)["failed"] += 1
        unsafe_files.append({"path": str(source_file), "reason": reason})
        log.warning("Remote import failed for %s: %s", source_file, reason)

    def _reclassify_landed_failed(rel, entry, reason):
        """Move a landed file's count from copied/skipped_duplicate to failed.

        A landed entry has already been booked into ``copied`` (fresh copy)
        or ``skipped_duplicate`` (crash-recovery adopt) at the moment its
        bytes were verified on disk. When a later step in the batch pass
        (scan itself failing, a missing catalog row after scan, or a
        hash mismatch against what scan re-hashed) forces this file into
        the ``failed`` bucket, the origin count must be rolled back —
        otherwise the exactly-one-terminal-bucket invariant breaks and
        ``copied + skipped_duplicate + failed`` exceeds ``discovered``.
        """
        nonlocal copied, verified, skipped_duplicate
        dest_path = entry.dest_path
        origin = entry.origin
        if origin == "copied":
            copied -= 1
            if verified_counted_for_copies:
                verified -= 1
            _counts(rel)["copied"] -= 1
        elif origin == "skipped_duplicate":
            skipped_duplicate -= 1
            _counts(rel)["skipped_duplicate"] -= 1
        _fail(rel, dest_path, reason)

    # Intra-run bookkeeping so a second byte-identical card file (with a
    # different basename) in this run is recognized as a duplicate before
    # ``scan()`` runs — the DB twin lookup can't help until the batch's
    # ``scan()`` has cataloged the first landing. Mirrors the local path.
    run_dest_folders = {}
    run_verified_hashes = {}

    # Sticky across the rest of the run once a mounted → unmounted
    # transition is observed. The per-batch rollback below undoes
    # ``to_transfer`` / ``landed`` (adoptions) / ``dup_skips`` / ``dup_dirs``
    # but not the identities the same batch already installed in the
    # job-wide ``checker`` (and in ``run_dest_folders`` /
    # ``run_verified_hashes``) via ``_record_checker`` — and
    # ``DuplicateChecker`` exposes no removal API, so those entries
    # cannot be surgically undone. If the share remounts before a later
    # batch (another date group, or after the 200-file batch boundary),
    # a same-content card file in that later batch would hit the intra-
    # run fast path at line 1237 and be counted as a duplicate of an
    # adopted or queued file whose archive claim was just rolled back —
    # so it is not transferred even though no backing archive copy
    # exists, and ``copied + skipped_duplicate == discovered`` could
    # again make ``safe_to_format`` go green over a card that is still
    # the only real copy. Refusing every remaining batch once a detach
    # has been observed keeps the stale intra-run cache from ever being
    # consulted. See PR #1400 review (Codex P2 r3688614624).
    mount_ever_lost = None

    # Mount-root check (Task 2.7 late follow-up): when a saved remote
    # target's local mount root is not mounted (for example ``/Volumes/NAS``
    # or ``/mnt/NAS`` is absent because the share isn't attached), a naive
    # ``os.makedirs(dest_folder, exist_ok=True)`` in the batch loop below
    # would create the whole mount tree as an empty local shadow directory
    # on the internal disk. The SSH rsync still writes to the NAS, but the
    # subsequent scan reads the fresh local shadow and leaves the import
    # uncataloged/failed; worse, on macOS/Linux that shadow root can also
    # prevent the real share from remounting at the configured path. Fail
    # the batch's files with a clear reason instead. Reuses the
    # pipeline path's ``_missing_archive_mount_root`` helper (only fires
    # for the ``/Volumes/X``, ``/mnt/X``, and ``/media/user/X`` shapes that
    # denote removable/network mount roots).
    #
    # Re-probed per batch rather than once up front: a card import runs
    # for hours against a network archive, and the share can drop *during*
    # the run (a Tailscale/SMB archive unmounted two hours into an import
    # on 2026-07-30, after which ``os.makedirs`` walked straight into the
    # vacated mount point). A start-of-job preflight cannot see that. The
    # probe is a couple of ``os.path.lexists`` calls on the mount root, so
    # paying it once per destination folder is free next to the copy work.
    # See PR #1113 review.
    def _missing_mount_root():
        return _missing_archive_mount_root(destination)

    def _record_checker(source_file, dest_folder, file_hash):
        """Register a landed/adopted file's identity with the intra-run checker.

        Without this the checker only sees the pre-run catalog, so a later
        byte-identical card file with a different basename in the same run
        is rsynced/cataloged again instead of being recognized as an
        intra-run duplicate. Same shape as the local path's
        ``_record_checker`` (spec decision 5, toward a single shared
        helper): every caller passes the landed/adopted ``dest_folder``
        and verified ``file_hash``, recorded unconditionally per token.
        ``DuplicateChecker.record`` re-``os.stat``s the source path — swallow
        OSError (removable media yanked mid-run) so the run keeps its
        already-verified landings and only loses the intra-run dedupe
        optimization. See PR #1113 review.
        """
        if checker is None:
            return
        try:
            tokens = checker.record(source_file)
        except OSError as e:
            log.warning(
                "Duplicate-checker record() failed for %s: %s",
                source_file, e,
            )
            return
        for tok in tokens:
            run_dest_folders[tok] = dest_folder
            run_verified_hashes[tok] = file_hash

    for rel, batch in batches:
        if runner.is_cancelled(job["id"]):
            cancelled = True
            break

        # A detach observed in an earlier batch is sticky: the intra-run
        # duplicate cache and the job-wide checker hold identities for
        # files whose archive claim was rolled back, and consulting them
        # against a remounted share would count fresh card files as
        # duplicates of transfers that never happened. Fail every
        # remaining file in the run rather than risk a stale-cache hit.
        # See PR #1400 review (Codex P2 r3688614624).
        if mount_ever_lost:
            for source_file in batch:
                emitted += 1
                _fail(
                    rel, source_file,
                    f"archive mount root {mount_ever_lost} detached "
                    "earlier in this import; the intra-run duplicate "
                    "cache still holds identities for files whose "
                    "archive claim was rolled back, so no further batch "
                    "can be trusted to consult it",
                )
            _emit(
                f"{rel}: archive unmounted", emitted, queued,
            )
            continue

        dest_folder = (
            os.path.normpath(os.path.join(destination, rel))
            if rel != "." else destination
        )
        ssh_dest = (
            posixpath.join(ssh_base, *rel.split("/")) if rel != "."
            else ssh_base
        )
        # Reject the whole batch before creating any directories on the card
        # or hashing candidate files below. When the mount base is an
        # ancestor of a selected source and the folder template maps back
        # into that source folder, ``dest_folder`` (and therefore every
        # ``cand_mount`` under it) resolves inside a source root. The
        # per-file collision loop below would hash those source-backed
        # ``cand_mount`` files, byte-match them against the card, and count
        # them as ``skipped_duplicate`` — with ``verify_by_hash=true`` that
        # would let ``safe_to_format`` go green over a card whose bytes
        # never crossed the network. Mirrors the local path's batch-level
        # dest-under-source guard (formatting the card would erase the
        # archive copy). See PR #1113 review.
        if _path_under_any_source(dest_folder):
            for source_file in batch:
                emitted += 1
                _fail(
                    rel, source_file,
                    "destination folder resolves inside a source directory "
                    "(dest_folder would be created under the card being "
                    "imported); formatting the card would erase the archive "
                    "copy",
                )
            _emit(
                f"{rel}: {_counts(rel)['copied']} copied · "
                f"{_counts(rel)['skipped_duplicate']} already present",
                emitted, queued,
            )
            continue
        # Mount-root check (see the ``_missing_mount_root`` helper near the
        # top of this function). Same shape as the dest-under-source guard:
        # fail every file in the batch with a specific reason and skip the
        # batch instead of letting ``os.makedirs`` create a local shadow of
        # the unmounted destination. Re-probed here, per batch, so a share
        # that drops mid-run is caught at the next batch boundary rather
        # than only at job start. See PR #1113 review.
        missing_mount_root = _missing_mount_root()
        if missing_mount_root:
            for source_file in batch:
                emitted += 1
                _fail(
                    rel, source_file,
                    f"archive mount root {missing_mount_root} is not "
                    "available (destination drive is not mounted; refusing "
                    "to create a shadow directory tree under it, which "
                    "would prevent the real share from remounting)",
                )
            # Specific refusal phase — mirrors the local path; spec
            # decision 3.
            _emit(
                f"{rel}: archive unavailable", emitted, queued,
            )
            continue
        # Persistent-mount-point case (Linux ``/mnt/<name>`` survives the
        # unmount), which the vanished-root check above structurally
        # cannot see. Same baseline-transition logic as the local path so
        # an ordinary directory at a mount-shaped path is never refused.
        # Matters more here, not less: rsync keeps pushing to the NAS
        # while the batch scan reads a local shadow, so the import lands
        # bytes remotely and catalogs nothing.
        stale_mount_root = _unmounted_since_baseline(mount_baseline)
        if stale_mount_root:
            for source_file in batch:
                emitted += 1
                _fail(
                    rel, source_file,
                    f"archive mount root {stale_mount_root} is no longer "
                    "mounted (it was at the start of this import; the "
                    "directory persists but the share has detached, so "
                    "cataloging here would read a local shadow of the "
                    "archive)",
                )
            _emit(
                f"{rel}: archive unmounted", emitted, queued,
            )
            continue
        # Mirrors the local path: the checks above only catch a mount
        # point that vanished or detached, so a stale-but-registered
        # mount, a read-only parent, or a permission change still reaches
        # this call. An uncaught OSError here kills the background job;
        # book it per file and let the run finish with an honest result.
        try:
            os.makedirs(dest_folder, exist_ok=True)
        except OSError as e:
            for source_file in batch:
                emitted += 1
                _fail(
                    rel, source_file,
                    f"could not create destination folder {dest_folder}: {e}",
                )
            _emit(
                f"{rel}: destination unavailable", emitted, queued,
            )
            continue

        # Promote any pre-existing folder row for this destination out of
        # ``'missing'``. Mirrors the local path (see
        # ``UPDATE folders SET status = 'ok' ... status = 'missing'``
        # after ``os.makedirs`` there): ``scanner.scan()`` only clears
        # ``'partial'`` on success, and workspace/photo queries hide rows
        # under ``missing`` folders, so a folder row still labelled
        # ``'missing'`` (from a prior health check when the NAS mount was
        # absent) would keep the just-imported photos invisible in the
        # workspace even after this run lands and hash-stamps them —
        # ``safe_to_format`` could go green over folders the UI won't
        # show. We just makedirs'd the folder so the path exists;
        # preserve ``'partial'`` (a real prior-scan needs-rescan signal).
        # See PR #1113 review.
        db.conn.execute(
            "UPDATE folders SET status = 'ok' "
            "WHERE path = ? AND status = 'missing'",
            (dest_folder,),
        )
        db.conn.commit()

        # Duplicate gate. A remote duplicate skip is only honest when the
        # cataloged twin's bytes are confirmed at the destination; the local
        # path re-hashes the twin's archive file. On the mount that file is
        # locally readable, so reuse the same on-disk re-hash contract.
        to_transfer = []   # (source_file, dest_basename, src_hash, src_size, src_mtime_ns)
        # Twin folders (under destination) whose bytes we RE-HASHED this run
        # and confirmed against source hashes — safe to scan/link into the
        # active workspace after this batch's fresh-scan runs. Mirrors the
        # local path's ``dup_dirs`` per-batch accumulator. See PR #1113 review.
        dup_dirs = set()
        # Files this batch landed: fresh rsync transfers (origin
        # "copied", appended after per-file verification in the rsync
        # block below) AND mount paths already on disk that the
        # collision walk adopted as ``skipped_duplicate`` (origin
        # "skipped_duplicate", retry / crash-recovery adopt).
        # ``_LandedFile`` entries; ``verified_hash`` is the card-side
        # src_hash so the catalog-stamping loop can cross-check the
        # scanned MOUNT row against the bytes we confirmed. Declared
        # BEFORE the per-file loop because the adoption branch inside
        # it appends here.
        landed = []
        # dest basename -> src_hash, for intra-batch same-basename collision
        # resolution (FIX 2). Populated as files are queued/skipped.
        claimed_basenames = {}
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
        queued_src_hashes = {}
        # Accepted duplicate skips for this batch —
        # (source_file, counted_unverified). A skip asserts the archive
        # already holds these bytes, which a detach invalidates: the twin
        # it matched may be a shadow file on the persistent mount stub
        # left by an earlier failed import. These never enter ``landed``,
        # so they need their own rollback or a
        # duplicate-only batch reports every file accounted for and the
        # card looks safe to erase. Mirrors the local path's ``dup_skips``.
        # See PR #1396 review (Codex P1 r3688498501 / r3688501706).
        dup_skips = []
        # Whether each ``copied`` booking also incremented ``verified``:
        # the remote path books ``verified`` only when
        # ``params.verify_by_hash`` made the independent card->NAS check,
        # so ``_reclassify_landed_failed`` must undo ``verified`` only in
        # that case. (The local path hash-verifies every copy and sets
        # this to True. PR 7's transport ``attests_bytes`` absorbs this
        # flag.)
        verified_counted_for_copies = params.verify_by_hash
        # dest_paths the post-scan cross-checks reclassified from
        # ``copied`` to ``failed``. The entries stay in ``landed``
        # (mutating a list during its own iteration is error-prone), so
        # the downstream readers — the derived-cache diff loop and the
        # working-copy override fill — skip these paths instead of
        # acting on bytes the ledger no longer vouches for. Mirrors the
        # local path's ``reclassified_landed_paths``.
        reclassified_landed_paths = set()
        # Sticky once tripped. The remote copy is one rsync per batch
        # rather than a per-file write, but the duplicate/adoption
        # decisions above happen per file and each one reads the mount —
        # so the mount has to be re-checked at that granularity too.
        mount_lost = None
        # Sticky signal that a destination-side hash in the per-file loop
        # below was cancelled mid-read (``DestReadCancelled``). Any such
        # cancel is evidence the mount is misbehaving, so the post-loop
        # catalog block MUST skip its ``scan()`` / ``_hash_dest_file``
        # calls on the same paths — they would hit the same wedged mount
        # and pin the job in "cancelling" for the mount's own timeout,
        # exactly the failure mode this PR set out to eliminate. A plain
        # user Stop on a healthy mount leaves this False (the catalog
        # runs normally so partially-landed batches stay cataloged the
        # way ``test_cancel_leaves_valid_partial_catalog`` expects).
        # See PR #1423 review (Codex P2 r3716433824).
        dest_read_cancelled = False
        for source_file in batch:
            if runner.is_cancelled(job["id"]):
                cancelled = True
                break
            if not mount_lost:
                mount_lost = _unmounted_since_baseline(mount_baseline)
            if mount_lost:
                emitted += 1
                _fail(
                    rel, source_file,
                    f"archive mount root {mount_lost} detached while this "
                    "batch was being prepared (the directory persists but "
                    "the share is gone, so neither a transfer nor a "
                    "duplicate match against it can be trusted)",
                )
                continue
            emitted += 1
            _emit(
                f"{rel}: importing", emitted, queued, source_file.name,
                is_importing=True,
            )
            if checker is not None:
                try:
                    token = checker.match(source_file)
                except OSError as e:
                    _fail(rel, source_file, f"duplicate check failed: {e}")
                    continue
                if token is not None:
                    if (
                        params.trust_likely_duplicates
                        and not params.verify_by_hash
                    ):
                        likely_rows = _likely_twin_rows(
                            db, token, source_file, _path_under_any_source,
                        )
                        if likely_rows:
                            skipped_duplicate += 1
                            unverified_duplicate += 1
                            _counts(rel)["skipped_duplicate"] += 1
                            dup_skips.append((source_file, True))
                            dup_dirs.update(_linkable_twin_dirs(
                                likely_rows, _path_under_destination,
                            ))
                            continue
                    # Confirm against a cataloged twin's on-disk bytes (mount
                    # side is locally readable). Only a byte-verified twin
                    # backs a skip; otherwise import the file normally.
                    if token[0] == "hash":
                        twin_rows = _hash_twin_rows(db, token[1])
                        src_hash = token[1]
                    else:
                        twin_rows = _key_twin_rows(db, token[1])
                        try:
                            src_hash = checker.content_hash(source_file)
                        except OSError as e:
                            _fail(rel, source_file,
                                  f"duplicate check failed: {e}")
                            continue
                    accept = False
                    # Intra-run fast path: an earlier file in this run
                    # already landed for this identity, so this file's
                    # bytes are byte-proven by the earlier landing without
                    # hitting the archive. ``('hash', …)`` tokens carry
                    # the bytes as their identity; ``('key', …)`` tokens
                    # need the source's fresh hash to match the run twin's
                    # verified hash before accepting (two different files
                    # with the same filename+size+capture-second must not
                    # dedupe on metadata alone). Without this, the DB
                    # twin lookup below sees only the pre-``scan()``
                    # catalog and a byte-identical second file gets
                    # rsynced/cataloged again. Mirrors the local path.
                    # See PR #1113 review.
                    if token in run_dest_folders:
                        if token[0] == "hash":
                            accept = True
                        else:
                            run_hash = run_verified_hashes.get(token)
                            if (
                                src_hash is not None
                                and run_hash is not None
                                and src_hash == run_hash
                            ):
                                accept = True
                    # Twins whose bytes we actually hashed this run and
                    # matched against the source. Only these back a
                    # duplicate-folder link (a stale/off-destination twin's
                    # folder must not be pulled into the active workspace).
                    verified_twin_rows = []
                    if not accept:
                        for twin in twin_rows:
                            twin_path = os.path.join(
                                twin["folder_path"], twin["filename"],
                            )
                            # A twin cataloged under any import source root
                            # is (or may be) the card file being imported
                            # this run — re-hashing it just re-reads the
                            # source, proving nothing about an off-card
                            # copy. Accepting it would count the file as
                            # skipped_duplicate and (with verify_by_hash)
                            # let safe_to_format go green while the card
                            # holds the only bytes. Mirrors the local
                            # path's filter. See PR #1113 review.
                            if _path_under_any_source(twin_path):
                                continue
                            try:
                                twin_hash = _hash_dest_file(
                                    twin_path, _stop_requested)
                            except DestReadCancelled:
                                cancelled = True
                                dest_read_cancelled = True
                                break
                            except OSError:
                                continue
                            if twin_hash is not None and twin_hash == src_hash:
                                accept = True
                                # Keep scanning to collect every byte-
                                # verified twin's folder — an older run
                                # may have written the same identity into a
                                # different folder layout (e.g.
                                # ``unsorted`` or a different date
                                # template), and only the folder we
                                # RE-HASHED this run is safe to link.
                                # Mirrors the local path's collect-then-
                                # link pattern. See PR #1113 review.
                                verified_twin_rows.append(twin)
                    if cancelled:
                        # Stop interrupted a twin hash above. Don't let
                        # this file fall through to the collision checks
                        # and the transfer queue — every further step
                        # touches the same (possibly dead) mount.
                        break
                    if accept:
                        skipped_duplicate += 1
                        _counts(rel)["skipped_duplicate"] += 1
                        dup_skips.append((source_file, False))
                        # Preserve the verified twin folders so the follow-
                        # up direct link can pull them into the active
                        # workspace. Without this a verified duplicate-only
                        # remote import whose twins live in a different
                        # folder than this run's template output would skip
                        # rsync AND leave the twin folder unlinked while
                        # safe_to_format still went green. See PR #1113
                        # review.
                        dup_dirs.update(
                            _linkable_twin_dirs(
                                verified_twin_rows,
                                _path_under_destination,
                            ),
                        )
                        # An intra-run twin's dest_folder isn't cataloged
                        # yet (scan runs after the batch loop) but WILL be
                        # by this run's own batch scan, so add it to
                        # dup_dirs so a duplicate-only follow-up batch
                        # still finds it visible. Mirrors the local path.
                        run_dest = run_dest_folders.get(token)
                        if run_dest is not None:
                            dup_dirs.add(run_dest)
                        continue
            # Collision parity (FIX 2): rsync lands files flat by basename,
            # so two different card files with the same basename in one batch
            # would clobber on the NAS. Assign a distinct dest basename per
            # colliding file, mirroring ingest()/the local path: a byte-
            # identical file already at the destination (a prior run's copy,
            # or an earlier card file this batch) is a skip; a different one
            # advances through numeric suffixes. ``claimed_basenames`` tracks
            # names taken by earlier files IN THIS BATCH; the mount is
            # locally readable so already-landed bytes are checked on disk.
            dest_basename = source_file.name
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
                _fail(rel, source_file, str(e))
                continue
            try:
                src_hash = (
                    checker.content_hash(source_file)
                    if checker is not None
                    else compute_file_hash(str(source_file))
                )
            except OSError as e:
                _fail(rel, source_file, str(e))
                continue
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
                and src_hash in queued_src_hashes
            ):
                skipped_duplicate += 1
                _counts(rel)["skipped_duplicate"] += 1
                dup_skips.append((source_file, False))
                _record_checker(source_file, dest_folder, src_hash)
                continue
            stem, suffix = os.path.splitext(source_file.name)
            counter = 0
            adopted = False
            while True:
                candidate = (
                    source_file.name if counter == 0
                    else f"{stem}_{counter}{suffix}"
                )
                cand_mount = os.path.join(dest_folder, candidate)
                candidate_key = _fold_basename(candidate)
                if candidate_key in claimed_basenames:
                    # Claimed earlier in this batch (a same-basename sibling
                    # already queued). If that sibling has our exact bytes,
                    # skip as an intra-batch duplicate; otherwise advance.
                    # Gated on ``checker`` for the same reason as the
                    # different-basename intra-batch dedup above: when
                    # ``skip_duplicates=False``, both files must be
                    # queued under distinct suffixes instead of the
                    # second one being counted as ``skipped_duplicate``.
                    if (
                        checker is not None
                        and claimed_basenames[candidate_key] == src_hash
                    ):
                        skipped_duplicate += 1
                        _counts(rel)["skipped_duplicate"] += 1
                        dup_skips.append((source_file, False))
                        _record_checker(source_file, dest_folder, src_hash)
                        adopted = True
                        break
                    counter += 1
                    continue
                if os.path.exists(cand_mount):
                    # Already on disk (crash-recovery/resume). Byte-identical
                    # -> skip; different -> advance to the next suffix.
                    try:
                        on_disk = _hash_dest_file(
                            cand_mount, _stop_requested)
                    except DestReadCancelled:
                        # Stop arrived mid-read against a candidate on the
                        # (possibly dead) mount. Advancing to the next
                        # suffix would immediately call os.path.exists /
                        # getsize / _hash_dest_file on the same mount and
                        # can pin cancelling for the mount's own timeout,
                        # exactly like the twin-hash branch above. Exit the
                        # candidate loop; the outer check below then exits
                        # the source-file loop so nothing else in this
                        # batch touches the mount. The interrupted file
                        # stays on the card for the next run.
                        cancelled = True
                        dest_read_cancelled = True
                        break
                    except OSError:
                        on_disk = None
                    if on_disk is not None and on_disk == src_hash:
                        skipped_duplicate += 1
                        _counts(rel)["skipped_duplicate"] += 1
                        claimed_basenames[candidate_key] = src_hash
                        # Fold the adoption into ``landed`` (origin
                        # "skipped_duplicate") so the restricted scan
                        # below picks the mount path up — without this
                        # entry the scan's explicit file set would skip
                        # the adopted-but-uncataloged file and
                        # ``copied + skipped_duplicate == discovered``
                        # could still let a verified run report
                        # ``safe_to_format=True`` with no photo row —
                        # and so the post-scan stamping loop re-checks
                        # that the mount bytes still equal
                        # ``verified_hash`` before leaving the skip
                        # counted. Rollback goes through
                        # ``_reclassify_landed_failed``, whose origin
                        # switch decrements ``skipped_duplicate``, NOT
                        # ``copied``. Deliberately NOT also booked into
                        # ``dup_skips``: the mount-lost block rolls back
                        # both ledgers, and double-booking would
                        # decrement ``skipped_duplicate`` twice (the
                        # mount-detach-after-adoption pins hold it at
                        # exactly 0). See PR #1113 review.
                        landed.append(_LandedFile(
                            dest_path=cand_mount,
                            verified_hash=src_hash,
                            source_path=str(source_file),
                            origin="skipped_duplicate",
                            src_size=src_size,
                            src_mtime_ns=src_mtime_ns,
                        ))
                        _record_checker(source_file, dest_folder, src_hash)
                        adopted = True
                        break
                    counter += 1
                    continue
                dest_basename = candidate
                break
            if cancelled:
                # Stop interrupted a collision hash above (mirrors the
                # twin-hash branch's post-loop check). Don't let this file
                # (or the rest of the batch) fall through to the queue /
                # rsync path — every further step touches the same
                # (possibly dead) mount.
                break
            if adopted:
                continue
            claimed_basenames[_fold_basename(dest_basename)] = src_hash
            # Only track queued source hashes when duplicate skipping is
            # enabled — the intra-batch dedup that consults this map is
            # gated on ``checker`` above, so populating it with
            # ``skip_duplicates=False`` would just be dead state.
            if checker is not None and src_hash is not None:
                queued_src_hashes[src_hash] = dest_folder
            to_transfer.append(
                (source_file, dest_basename, src_hash,
                 src_size, src_mtime_ns))

        # --- Per-batch rsync -------------------------------------------
        # landed carries the card-side src_hash so the catalog-stamping loop
        # below can cross-check the scanned MOUNT row's file_hash against the
        # hash confirmed on the NAS. Without that carry-through, a stale/
        # misconfigured mount base that happens to already contain
        # ``<folder>/<filename>`` for a name we transferred would let scan()
        # populate the row from unrelated bytes while remote_verify_files
        # confirmed the NAS bytes — and blind hash_status='ok' stamping would
        # flip safe_to_format green over storage we never touched. See PR
        # #1113 review.
        # The per-file probe runs before each file is decided, so it
        # cannot see a detach that happens while the last (or only) file
        # is being considered. Probe once more here — before the transfer
        # and before anything is cataloged — so the whole batch is
        # covered. As on the local path this is the last probe that can
        # help: a detach after it races the transfer and catalog scan,
        # which no amount of probing can prevent.
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
        # An earlier file in this same batch may have been accepted as an
        # adopted/duplicate claim before the user hit Stop, and if the
        # share detached between that mount read and the Stop the only
        # remaining chance to notice — and to roll ``dup_skips`` /
        # ``landed`` (adoptions) back so the catalog block below doesn't
        # trust a local shadow — is this probe. See PR #1423 review
        # (Codex P2 r3716581282).
        if not mount_lost and not dest_read_cancelled:
            mount_lost = _unmounted_since_baseline(mount_baseline)

        # A detach invalidates every accepted "already present" claim in
        # this batch: the twin each one matched may be a shadow file on
        # the mount stub rather than a real object on the NAS. Roll them
        # back into failed, and drop the queued transfers and adoptions
        # too — with the mount gone we can neither verify what the NAS
        # holds nor trust the mount-side paths we were about to catalog.
        if mount_lost:
            for skipped_file, counted_unverified in dup_skips:
                skipped_duplicate -= 1
                _counts(rel)["skipped_duplicate"] -= 1
                if counted_unverified:
                    unverified_duplicate -= 1
                _fail(
                    rel, skipped_file,
                    f"archive mount root {mount_lost} detached mid-batch; "
                    "the duplicate this file matched cannot be confirmed "
                    "to be on the archive rather than in a local shadow",
                )
            dup_skips = []
            dup_dirs = set()
            for queued_file, _dest_basename, _queued_hash, _sz, _mt \
                    in to_transfer:
                _fail(
                    rel, queued_file,
                    f"archive mount root {mount_lost} detached before this "
                    "file was transferred",
                )
            to_transfer = []
            # Anything adopted BEFORE the detach rests on mount bytes
            # that may be a local shadow of the archive, not the share
            # itself (only adoptions can be in ``landed`` here — fresh
            # transfers append after the rsync below). Roll them out of
            # ``skipped_duplicate`` into failed via the origin-switching
            # helper and drop them: cataloging them would record archive
            # paths for bytes that vanish when the real share remounts.
            # Mirrors the local path's mount-lost ``landed`` rollback.
            if landed:
                for entry in landed:
                    _reclassify_landed_failed(
                        rel, entry,
                        f"archive mount root {mount_lost} detached "
                        "mid-batch; this file landed in a local shadow "
                        "of the archive, not on the share",
                    )
                landed = []
            # Trip the run-wide sticky flag so every remaining batch is
            # refused at the top of the loop rather than allowed to
            # consult the intra-run duplicate cache (which still holds
            # identities for the files just rolled back above; the
            # checker has no removal API). See PR #1400 review (Codex
            # P2 r3688614624).
            mount_ever_lost = mount_lost

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
        if to_transfer and not cancelled:
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
                for sf, _bn, _sh, _sz, _mt in to_transfer:
                    _fail(rel, sf,
                          f"remote mkdir failed for {ssh_dest}: {mkdir_detail}")
            else:
                # Split into the flat fast path (dest basename == card
                # basename) and collision-renamed files (transferred and
                # verified individually to an explicit NAS filename, since a
                # flat --files-from list to one dir can't rename).
                flat = [
                    (sf, bn, sh, sz, mt)
                    for sf, bn, sh, sz, mt in to_transfer
                    if bn == sf.name
                ]
                renamed = [
                    (sf, bn, sh, sz, mt)
                    for sf, bn, sh, sz, mt in to_transfer
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
                batch_size = len(to_transfer)
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
                            _fail(rel, sf, "rsync stalled (no progress)")
                    elif _rsync_cancelled(rc):
                        cancelled = True
                    elif rc != 0:
                        for sf, _bn, _sh, _sz, _mt in flat:
                            _fail(rel, sf, f"rsync failed: {stderr.strip()}")
                    else:
                        for sf, bn, sh, sz, mt in flat:
                            transferred.append((
                                sf, bn, sh, sz, mt,
                                posixpath.join(ssh_dest, bn)))
                # Renamed files: one rsync each to the explicit NAS file
                # path (rsync <card> user@host:/dir/DSC_0001_1.jpg).
                for sf, bn, sh, sz, mt in renamed:
                    if cancelled or runner.is_cancelled(job["id"]):
                        cancelled = True
                        break
                    nas_full = posixpath.join(ssh_dest, bn)
                    rc, stderr, timed_out = _do_rsync(
                        [str(sf)],
                        move_mod.rsync_dest_spec(remote, nas_full), False,
                        extra_args)
                    if timed_out:
                        _fail(rel, sf, "rsync stalled (no progress)")
                    elif _rsync_cancelled(rc):
                        cancelled = True
                        break
                    elif rc != 0:
                        _fail(rel, sf, f"rsync failed: {stderr.strip()}")
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
                            _fail(rel, sf, reason)
                            continue
                    copied += 1
                    _counts(rel)["copied"] += 1
                    if params.verify_by_hash:
                        verified += 1
                    landed.append(_LandedFile(
                        dest_path=dest_path,
                        verified_hash=src_hash,
                        source_path=str(sf),
                        origin="copied",
                        src_size=sz,
                        src_mtime_ns=mt,
                    ))
                    _record_checker(sf, dest_folder, src_hash)

        # --- Catalog this batch ----------------------------------------
        # Scan only files that were freshly transferred or adopted from an
        # uncataloged mount-side collision. Cataloged duplicate twins are
        # linked directly below; scanning a duplicate-only destination would
        # enumerate/stat the entire mounted NAS folder for no new rows.
        #
        # Skip when a destination-side hash in the per-file loop above
        # cancelled mid-read. That signal means the mount is misbehaving,
        # and ``scan()`` here — plus the ``_hash_dest_file`` re-checks below
        # for landed and adopted paths — would touch the same mounted
        # directory and pin the job in "cancelling" for the mount's own
        # timeout. Already-rsync'd landings and adopted-on-disk paths are
        # picked up by the next run's crash-recovery adoption (byte-
        # identical files match by hash and count as ``skipped_duplicate``).
        # A plain user Stop on a healthy mount leaves
        # ``dest_read_cancelled`` False, so partially-landed batches keep
        # cataloging like before. See PR #1423 review (Codex P2
        # r3716433824).
        if landed and not dest_read_cancelled:
            # ``landed`` covers collision-loop adoptions too (origin
            # "skipped_duplicate"), so their photo rows get created by
            # the restricted scan. The explicit file set is also what
            # prevents a duplicate-only import from falling back to a
            # whole-directory discovery walk. See PR #1113 review.
            landed_paths = {entry.dest_path for entry in landed}
            # Pre-scan snapshot of any photo row already cataloged at a
            # path this batch will scan (fresh transfers AND adoptions —
            # both live in ``landed``). Compared after the
            # scan to invalidate derived caches for rows whose content
            # identity changed. Defense-in-depth next to the scanner's
            # own ``content_identity_changed`` invalidation, for rows/
            # codepaths the scanner misses (legacy NULL-hash rows).
            # Mirrors the local path — spec decision 6.
            pre_scan_hashes = {}
            for sp in landed_paths:
                row = db.conn.execute(
                    """SELECT p.id, p.file_hash FROM photos p
                       JOIN folders f ON f.id = p.folder_id
                       WHERE f.path = ? AND p.filename = ?""",
                    (os.path.dirname(sp), os.path.basename(sp)),
                ).fetchone()
                if row is not None:
                    pre_scan_hashes[sp] = row["file_hash"]
            try:
                # A landed path is not necessarily uncataloged: a stale row
                # can survive for a file deleted off the archive, and if
                # the replacement bytes land at that path carrying an mtime
                # equal to the stale row's, the incremental fast path skips
                # it without comparing size or content. ``file_hash`` then
                # keeps the stale value, the post-scan cross-check below
                # compares the copy-time hash against it, and a file that
                # transferred fine is reported failed — with retries unable
                # to refresh the row. ``restrict_files`` already narrows
                # those batches to a handful of paths, so incremental buys
                # nothing there anyway. See PR #1398 review.
                scan(
                    destination, db,
                    restrict_dirs=[dest_folder],
                    restrict_files=landed_paths,
                    vireo_dir=params.vireo_dir,
                    thumb_cache_dir=params.thumb_cache_dir,
                    skip_working_copies=True,
                )
            except Exception as e:
                # Each entry was already booked into copied or
                # skipped_duplicate — reclassify (roll back origin, add
                # to failed) so the ledger never double-counts.
                for entry in landed:
                    _reclassify_landed_failed(
                        rel, entry, f"catalog scan failed: {e}",
                    )
                landed = []
            else:
                _invalidate_new_images(db, dest_folder)

            # Catalog-row presence is required on BOTH paths: the route's
            # copy-and-catalog contract says every landed byte becomes a
            # photo row (directly or via a companion_path on a sibling
            # row), and a landed file with no row and no companion row
            # after scan is failed rather than silently left counted as
            # ``copied`` — otherwise a remote import into an unmounted/
            # misconfigured mount base would report copied/ok (or
            # copied/NULL, no-verify) with no catalog trail. The
            # RAW+JPEG-pair case (scan merges the JPEG row into the RAW
            # primary via ``companion_path`` and deletes the JPEG row) is
            # handled explicitly below so a legitimate paired JPEG is
            # accepted instead of failed. Hash stamping
            # (``hash_status='ok'``) still runs ONLY on the checksum-
            # verification path — without verify_by_hash the rows keep
            # NULL hash_status/hash_checked_at (scan may set file_hash,
            # but we don't claim an integrity verdict we didn't
            # independently make).
            #
            # RAW rows that gained a JPEG companion this batch. The
            # scan's pair-merge only invalidates the RAW's derived
            # caches when an edit recipe transfers, so a RAW whose
            # working copy / thumb / previews were rendered RAW-only
            # keeps serving them after pairing. Collect every such RAW
            # id (transferred AND adopted JPEGs — adoption only proves
            # the JPEG bytes pre-existed on the mount, not that the RAW
            # already carried companion_path) and invalidate below.
            # Mirrors the local path — spec decision 6.
            raw_companion_invalidations = set()
            for entry in list(landed):
                dest_path = entry.dest_path
                src_hash = entry.verified_hash
                row = db.conn.execute(
                    """SELECT p.id, p.file_hash FROM photos p
                       JOIN folders f ON f.id = p.folder_id
                       WHERE f.path = ? AND p.filename = ?""",
                    (os.path.dirname(dest_path),
                     os.path.basename(dest_path)),
                ).fetchone()
                if row is not None:
                    # Cross-check the scanned MOUNT row's hash against the
                    # source hash (the bytes we intended to land). This
                    # runs even without ``verify_by_hash`` because catalog
                    # integrity is a separate concern from the format
                    # honesty gate: a stale/misconfigured mount that
                    # happens to already contain ``<folder>/<filename>``
                    # for a name we ``--ignore-existing``-transferred, or
                    # a receiver-side race that left a different file at
                    # that path, would otherwise be cataloged against
                    # unrelated bytes while ``safe_to_format=False``
                    # (correct on the format side, but the workspace
                    # catalog now points at the wrong photo). The
                    # ``hash_status='ok'`` stamp still runs ONLY behind
                    # ``verify_by_hash`` — that stamp is the independent
                    # card→NAS attestation, not just "scan and source
                    # agree on the mount view". Mirrors the local path's
                    # cross-check against ``verified_hash``. See PR #1113
                    # review.
                    #
                    # Normalize zero-byte convention on both sides:
                    # scan() writes NULL for zero-byte files;
                    # ``checker.content_hash`` returns None; a
                    # checker-less ``compute_file_hash`` returns
                    # ``EMPTY_FILE_SHA256``. Treat all three as
                    # equivalent so an empty card file matches its
                    # empty catalog row.
                    scan_h = row["file_hash"]
                    if scan_h == EMPTY_FILE_SHA256:
                        scan_h = None
                    src_h_norm = (
                        None if src_hash == EMPTY_FILE_SHA256
                        else src_hash
                    )
                    # scan() can legitimately leave ``file_hash`` NULL
                    # (large files, prior partial scan, tests that stub
                    # the hash step, or a read/permission failure that
                    # scanner suppresses). A missing scan hash doesn't
                    # prove anything on its own, but silently accepting
                    # would let a stale/unreadable mount stamp ``ok``
                    # under ``verify_by_hash`` — the NAS checksum only
                    # proves card bytes reached the SSH target, not that
                    # the cataloged mount path holds those bytes. Rehash
                    # the mount file directly as the last-line check;
                    # mirrors the local path's ``_rehash_dest_or_none``
                    # fallback. See PR #1113 review.
                    if scan_h is None and src_h_norm is not None:
                        try:
                            mount_hash = _hash_dest_file(
                                dest_path, _stop_requested)
                        except DestReadCancelled:
                            cancelled = True
                            break
                        except OSError:
                            mount_hash = None
                        mount_norm = (
                            None if mount_hash == EMPTY_FILE_SHA256
                            else mount_hash
                        )
                        if mount_norm is None or mount_norm != src_h_norm:
                            _reclassify_landed_failed(
                                rel, entry,
                                "scan wrote no mount row hash and a re-"
                                "read of the mount file "
                                + ("disagrees with the source hash"
                                   if not params.verify_by_hash else
                                   "disagrees with the hash verified "
                                   "on the NAS")
                                + " (mount base is likely stale, "
                                "unreadable, or misconfigured)",
                            )
                            reclassified_landed_paths.add(entry.dest_path)
                            continue
                    elif scan_h is not None and scan_h != src_h_norm:
                        _reclassify_landed_failed(
                            rel, entry,
                            "scanned mount row hash does not match "
                            "the source hash (mount base is likely "
                            "stale or misconfigured)"
                            if not params.verify_by_hash else
                            "scanned mount row hash does not match "
                            "the hash verified on the NAS (mount "
                            "base is likely stale or misconfigured)",
                        )
                        reclassified_landed_paths.add(entry.dest_path)
                        continue
                    if params.verify_by_hash:
                        db.update_photo_hash_check(
                            row["id"], "ok", commit=False,
                        )
                    # Fresh mount row this run stamped as valid — the
                    # after-import chaining hook builds its process job
                    # collection from these ids.
                    imported_photo_ids.add(row["id"])
                else:
                    # RAW+JPEG pairing merges the JPEG's photo row into
                    # the RAW primary (companion_path) and deletes the
                    # JPEG's own row, so a landed JPEG whose sibling RAW
                    # was scanned in the same batch legitimately has no
                    # row of its own. Look it up as another row's
                    # companion_path before deciding "not cataloged".
                    # When verifying, cross-check the mount JPEG bytes
                    # against the hash confirmed card->NAS (same
                    # stale-mount guard the non-companion branch runs
                    # above). Mirrors the local path's companion lookup.
                    # See PR #1113 review.
                    companion = db.conn.execute(
                        """SELECT p.id FROM photos p
                           JOIN folders f ON f.id = p.folder_id
                           WHERE f.path = ? AND p.companion_path = ?""",
                        (os.path.dirname(dest_path),
                         os.path.basename(dest_path)),
                    ).fetchone()
                    if companion is not None:
                        # The paired JPEG's own photo row is gone by
                        # design (pair-scan merges it into the RAW
                        # primary), so the non-companion branch above
                        # can't cross-check its bytes for us. Hash the
                        # mount JPEG here regardless of
                        # ``verify_by_hash`` — the non-companion branch
                        # compares ``scan_h`` vs ``src_h_norm`` even in
                        # no-verify mode as a stale-mount catalog-
                        # integrity guard, and paired JPEGs need the
                        # same protection or a stale/misconfigured
                        # mount with a same-named but different JPEG
                        # would enqueue after-import processing against
                        # the wrong companion. Only the
                        # ``verified``/``hash_status`` accounting is
                        # gated behind ``verify_by_hash``. See PR #1113
                        # review.
                        try:
                            mount_hash = _hash_dest_file(
                                dest_path, _stop_requested)
                        except DestReadCancelled:
                            cancelled = True
                            break
                        except OSError:
                            mount_hash = None
                        src_h_norm = (
                            None if src_hash == EMPTY_FILE_SHA256
                            else src_hash
                        )
                        mount_norm = (
                            None if mount_hash == EMPTY_FILE_SHA256
                            else mount_hash
                        )
                        if mount_norm != src_h_norm:
                            _reclassify_landed_failed(
                                rel, entry,
                                "paired companion mount bytes do "
                                "not match the source hash (mount "
                                "base is likely stale or "
                                "misconfigured)"
                                if not params.verify_by_hash else
                                "paired companion mount bytes do "
                                "not match the hash verified on "
                                "the NAS (mount base is likely "
                                "stale or misconfigured)",
                            )
                            reclassified_landed_paths.add(entry.dest_path)
                            continue
                        # JPEG bytes are represented by the RAW row's
                        # companion_path — accept as landed; leave the
                        # copied/verified counters alone. The RAW row is
                        # what the chaining hook should process, so its
                        # id joins ``imported_photo_ids``. See PR #1113
                        # review.
                        # Collected for the post-validation invalidation
                        # loop — see the raw_companion_invalidations decl.
                        raw_companion_invalidations.add(companion["id"])
                        imported_photo_ids.add(companion["id"])
                        continue
                    _reclassify_landed_failed(
                        rel, entry,
                        "not cataloged after scan (no photo row)",
                    )
                    reclassified_landed_paths.add(entry.dest_path)
            # Invalidate derived caches for any landed/adopted row whose
            # bytes differ from what was there pre-scan. The batch scan
            # passes ``vireo_dir`` through, so scanner's own
            # ``_invalidate_derived_caches`` already fires on rows it
            # detects as content-changed; this loop is defense-in-depth
            # for legacy rows and codepath changes the scanner misses
            # (see the ``pre_scan_hashes`` capture comment above), and
            # is idempotent with scanner's call. Without it, imports
            # that restore a replaced-then-deleted mount file could
            # leave stale ``working_copy_path``/thumb/preview files
            # pointing at the previous bytes, and the deferred
            # end-of-run ``_extract_working_copies`` skips rows whose
            # ``working_copy_path`` is already set — so the WC never
            # rebuilds against the new mount bytes. Mirrors the local
            # path — spec decision 6.
            invalidated_photo_ids = set()
            if params.vireo_dir:
                from scanner import _invalidate_derived_caches
                # Surviving entries only: landed entries (fresh
                # transfers and adoptions alike) that the post-scan
                # cross-checks reclassified to failed are skipped via
                # ``reclassified_landed_paths``. Copy-time hash:
                # ``landed`` carries it as ``verified_hash``.
                changed_candidates = [
                    (entry.dest_path, entry.verified_hash)
                    for entry in landed
                    if entry.dest_path not in reclassified_landed_paths
                ]
                for cand_path, copy_hash in changed_candidates:
                    if cand_path not in pre_scan_hashes:
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
                    # here so restoring a deleted mount file whose
                    # legacy row lost its hash still clears the stale
                    # derived caches. Compared raw, matching the local
                    # path's diff loop exactly — deliberately NOT
                    # normalizing the zero-byte convention (scan writes
                    # NULL, copy-time hashing yields EMPTY_FILE_SHA256),
                    # so a pre-existing zero-byte row is spuriously
                    # invalidated. Harmless, and shared with local so
                    # PR 5 unifies identical behavior.
                    pre_hash = pre_scan_hashes[cand_path]
                    if pre_hash == copy_hash:
                        continue
                    row = db.conn.execute(
                        """SELECT p.id FROM photos p
                           JOIN folders f ON f.id = p.folder_id
                           WHERE f.path = ? AND p.filename = ?""",
                        (
                            os.path.dirname(cand_path),
                            os.path.basename(cand_path),
                        ),
                    ).fetchone()
                    if row is None:
                        continue
                    _invalidate_derived_caches(
                        db, params.vireo_dir, row["id"],
                        thumb_cache_dir=params.thumb_cache_dir,
                    )
                    invalidated_photo_ids.add(row["id"])

                # RAW rows whose companion JPEG we just landed —
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
                # stale bytes for the just-replaced mount file.
                from scanner import _sweep_untracked_previews_for_photos
                _sweep_untracked_previews_for_photos(
                    db, params.vireo_dir, invalidated_photo_ids,
                )

            if params.vireo_dir:
                for entry in landed:
                    if entry.dest_path in reclassified_landed_paths:
                        # Reclassified to failed by the post-scan
                        # cross-checks above (missing row or mount-vs-
                        # source hash mismatch). Skipping the card
                        # override lets the WC extractor fall back to
                        # whatever the mount currently holds — matching
                        # the catalog's view — instead of caching a WC
                        # of bytes the ledger no longer vouches for.
                        continue
                    wc_source_paths[entry.dest_path] = (
                        entry.source_path, entry.src_size,
                        entry.src_mtime_ns,
                    )
                wc_dest_folders.add(dest_folder)

        # --- Link verified duplicate-twin folders ----------------------
        # A verified duplicate skip's twin folder may live elsewhere under
        # the archive (older date-layout, ``unsorted``, etc.). Link its
        # existing catalog row directly instead of scanning the folder: an
        # incremental scan still enumerates/stats every NAS entry and can
        # turn a zero-copy import into an hours-long metadata walk.
        new_dup_dirs = dup_dirs - linked_dup_dirs
        if new_dup_dirs:
            linked, failures = _link_duplicate_twin_dirs(
                db, workspace_id, new_dup_dirs,
            )
            linked_dup_dirs.update(linked)
            if failures:
                dup_link_failed = True
                for d, detail in failures.items():
                    unsafe_files.append({
                        "path": d,
                        "reason": (
                            "duplicate-folder workspace link failed: "
                            f"{detail}"
                        ),
                    })
        _emit(
            f"{rel}: {_counts(rel)['copied']} copied · "
            f"{_counts(rel)['skipped_duplicate']} already present",
            emitted, queued,
        )
        if cancelled:
            break

    # --- Deferred working-copy extraction ------------------------------
    if params.vireo_dir and wc_dest_folders and not cancelled:
        from scanner import _extract_working_copies

        try:
            _extract_working_copies(
                db, params.vireo_dir,
                scope=[(d, "exact") for d in sorted(wc_dest_folders)],
                source_paths=wc_source_paths,
                cancel_check=lambda: runner.is_cancelled(job["id"]),
            )
        except Exception:
            log.exception(
                "Working-copy extraction failed for %s",
                sorted(wc_dest_folders),
            )
        if runner.is_cancelled(job["id"]):
            cancelled = True

    status = "cancelled" if cancelled else (
        "failed" if failed else "completed"
    )
    summary = _selection_summary(
        params, include_paths, discovered=discovered, copied=copied,
        skipped_duplicate=skipped_duplicate, failed=failed,
    )
    runner.update_step(
        job["id"], "import",
        status="failed" if status == "failed" else "completed",
        summary=summary,
    )

    for exc in discovery_errors:
        unsafe_files.append({
            "path": str(getattr(exc, "filename", None) or "<discovery>"),
            "reason": f"source enumeration failed: {exc}",
        })

    # Scope narrowing (same rules as the local path).
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
    remote_unverified = not params.verify_by_hash
    if remote_unverified and discovered > 0:
        unsafe_files.append({
            "path": "<remote>",
            "reason": "enable verify_by_hash for remote verification",
        })
    if unverified_duplicate:
        unsafe_files.append({
            "path": "Likely duplicates",
            "reason": (
                f"{unverified_duplicate} matched by filename, byte size, "
                "and capture time but were not compared byte-for-byte"
            ),
        })
    # Selection drift entries. Shared with the local path.
    _append_selection_unsafe(
        unsafe_files, deselected=deselected, vanished_paths=vanished_paths,
        appeared=appeared,
    )
    safe_to_format = (
        not cancelled
        and failed == 0
        and not discovery_errors
        and not dup_link_failed
        and not partial_scope
        and not remote_unverified
        and unverified_duplicate == 0
        and (copied + skipped_duplicate) == discovered
        and not _selection_blocks_format(
            deselected=deselected, vanished_paths=vanished_paths)
    )
    # ``unverified_duplicate`` requires ``not verify_by_hash``, which is
    # exactly ``remote_unverified``, so this block is unreachable on the
    # remote path today. The selection condition is wired anyway: the mutual
    # exclusion is incidental, and a change to either flag would silently
    # reopen the hole.
    unverified_duplicates_only = (
        unverified_duplicate > 0
        and not cancelled
        and failed == 0
        and not discovery_errors
        and not dup_link_failed
        and not partial_scope
        and not remote_unverified
        and (copied + skipped_duplicate) == discovered
        and not _selection_blocks_format(
            deselected=deselected, vanished_paths=vanished_paths)
    )
    result = {
        "discovered": discovered,
        "copied": copied,
        "verified": verified,
        # Photo rows the after-import chaining hook should process.
        # Duplicate-only imports intentionally return an empty list so
        # ``_chain_after_import`` skips into its "no new photos" branch
        # instead of enqueueing an empty process run — same convention as
        # the local path. Without this the remote import always missed
        # after-import processing. See PR #1113 review.
        "photo_ids": sorted(imported_photo_ids),
        # Stable-identity map so a recovery retry can verify each carried
        # ID still points at the same file. Without this the retry
        # authorizes any current photo row that happens to share an ID
        # with something the parent landed — an especially real risk
        # after users delete recent imports (SQLite reuses the freed
        # IDs on the next insert).
        "photo_fingerprints": _capture_photo_fingerprints(
            db, imported_photo_ids,
        ),
        # Per-source signature over the discovered file set so a
        # recovery retry can detect a source whose contents changed
        # between the failed run and the retry — e.g. a different SD
        # card mounted at the same path, or new photos added to the
        # same card. Captured at DISCOVERY time (see the ``source_snapshots``
        # assignment above the copy loop); recording it here instead
        # would let a mid-copy card ejection stamp ``-1`` sizes and
        # refuse a legitimate reinsert-and-retry recovery.
        "source_snapshots": source_snapshots,
        "skipped_duplicate": skipped_duplicate,
        "unverified_duplicate": unverified_duplicate,
        "unverified_duplicates_only": unverified_duplicates_only,
        "failed": failed,
        "safe_to_format": safe_to_format,
        "unsafe_files": unsafe_files,
        "folders": folder_counts,
        "cancelled": cancelled,
        "discovery_errors": len(discovery_errors),
        # Selection drift, for the caller's readout. ``files_appeared`` is a
        # clamped net delta (card size minus previewed count), so it reads 0
        # — never negative — when more files vanished than arrived.
        "files_appeared": appeared,
        "files_vanished": len(vanished_paths),
        "ok": (failed == 0 and not discovery_errors and not dup_link_failed),
        "errors": [f"{u['path']}: {u['reason']}" for u in unsafe_files],
    }
    return result


def run_import_job(job, runner, db_path, workspace_id, params):
    """Copy card(s) -> archive, hash-verify, and catalog incrementally.

    Returns the result dict (counts + per-folder breakdown). The catalog
    is committed per batch; cancellation and crashes leave every already-
    verified file cataloged and nothing else.
    """
    from pipeline_job import (
        _archive_mount_baseline,
        _load_known_mount_roots,
        _missing_archive_mount_root,
        _record_known_mount_roots,
        _unmounted_since_baseline,
    )
    from scanner import scan

    db = Database(db_path)
    db.set_active_workspace(workspace_id)

    if params.remote_target is not None:
        # Remote (SSH) archive: card -> remote_path/subpath over rsync,
        # cataloged at mount_path/subpath (== params.destination). Kept in a
        # separate function so the local copy path stays byte-for-byte
        # unchanged. See Task 2.7.
        return _run_remote_import_job(job, runner, db, workspace_id, params)

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
    # ``known_mounted_roots`` seeds True from a persistent record of
    # mount roots ever observed live. Without it, a share that detached
    # BEFORE the run started (baseline is False from the outset) escapes
    # the guard: no True → False transition can fire against a False
    # baseline, so the persistent ``/mnt/<name>`` stub still passes the
    # per-batch check and copies land on the local disk. Cross-run
    # history closes that hole; a hand-made local dir is never observed
    # as a live mount and stays out of the known-set. See PR #1396
    # review (Codex P1 r3687401636).
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
    # never made it to the archive. The guard is shared with the remote
    # (SSH) path via the module-level factory. See PR #1107 review.
    _path_under_any_source = _build_source_root_guard(params.sources)

    # Destination containment for cataloged twin folders. ``destination``
    # is already ``realpath``-resolved above so a symlinked destination
    # like ``/photos`` -> ``/Volumes/Photos`` matches twin folders
    # cataloged under ``/Volumes/Photos/…``. Case-different spellings on
    # case-insensitive mounts (HFS+/APFS/exFAT) still need explicit
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

    runner.set_steps(job["id"], [
        {"id": "import", "label": "Copy & catalog"},
    ])
    runner.update_step(job["id"], "import", status="running")

    copied = 0
    eta = _ImportEtaEstimator(
        expected_new=(params.checked_count if params.skip_duplicates else None),
    )

    # Live per-folder counters, mutated by the copy loop via _counts() and
    # snapshotted onto every progress event so the Import page can render
    # truthful per-folder progress mid-run (not just from the terminal
    # result). Initialized before _emit so the discovery-phase emits see
    # an empty-but-present dict.
    folder_counts = {}

    def _emit(phase, current, total, current_file="", *, is_importing=False):
        eta_fields = {}
        if total > 0:
            if is_importing:
                eta.note_importing(copied)
            else:
                eta.note_batch_complete(current, copied)
            eta_fields = eta.fields(total)
        job["progress"]["current"] = current
        job["progress"]["total"] = total
        job["progress"]["current_file"] = current_file
        for key in _IMPORT_ETA_PROGRESS_KEYS:
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
                # consumers must see the state at emit time).
                folders={
                    rel: dict(counts) for rel, counts in folder_counts.items()
                },
                **eta_fields,
            ),
        )

    # --- Discover ---------------------------------------------------
    # Enumeration errors (permission denied, macOS TCC block on a
    # removable volume, unreadable subtree) get silently swallowed by
    # os.walk-style callbacks by default. If we ignored them, discovered
    # would just be smaller than reality and safe_to_format could still
    # flip green over a card whose contents were never actually visited.
    # Track them explicitly: each is a bucket-of-its-own failure entry
    # tied to the source path where it occurred.
    _emit("Discovering files", 0, 0)
    files = []
    discovery_errors = []

    def _discovery_onerror(exc):
        discovery_errors.append(exc)
        log.warning("Import discovery error: %s", exc)

    for src in params.sources:
        files.extend(discover_source_files(
            src, params.file_types, recursive=params.recursive,
            onerror=_discovery_onerror,
        ))
    discovered = len(files)
    # Snapshot the discovered source metadata NOW — before selection filters
    # the copy set, before any copy work, and before duplicate hashing. See
    # the matching block in ``run_import_job`` for the full rationale: retry
    # re-enumerates each source in full and would refuse an unchanged card
    # if the parent's stored signature only covered the selected subset,
    # and pre-copy capture keeps a mid-run ejection from backfilling ``-1``
    # sizes over a snapshot that would otherwise refuse the natural
    # reinsert-and-retry recovery.
    source_snapshots = _capture_source_snapshots(files, params.sources)

    # Selection: filter the copy set and measure drift. Shared with the
    # remote path — see ``_apply_selection`` for why each condition is
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

    # --- Ledger -----------------------------------------------------
    # Every discovered file ends in exactly one terminal bucket.
    verified = 0
    skipped_duplicate = 0
    unverified_duplicate = 0
    failed = 0
    unsafe_files = []          # [{path, reason}] — failed copies etc.
    # (folder_counts is initialized above _emit — see there.)
    # Photo rows this run created or landed bytes into: hash-stamped fresh
    # copies plus RAW primaries that adopted a landed companion JPEG.
    # The after-import chaining hook scopes its process job to exactly
    # these (duplicates are excluded — a duplicates-only import chains to
    # "no new photos", not an empty run).
    imported_photo_ids = set()
    emitted = 0
    cancelled = False

    def _stop_requested():
        # Threaded through every destination-side hash read so a Stop can
        # interrupt a read blocked on a dead mount (see _hash_dest_file).
        # Nonblocking probe — ``is_cancelled`` would park in
        # ``wait_if_paused`` for a pausable import, freezing the watchdog
        # loop itself and stopping the 120s stall timer from running while
        # the daemon reader can keep touching the archive even though the
        # UI says the job is paused. Mirrors the rsync watchdog's use of
        # ``cancellation_requested`` for the same reason.
        return runner.cancellation_requested(job["id"])

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
    # dest_path -> (card_src_path, expected_size, expected_mtime_ns).
    # The identity tuple is captured at land time (before any WC
    # extraction pass), so the deferred ``_extract_working_copies`` call
    # can verify the override still holds the exact bytes we copied —
    # not just any same-sized file that happens to be at the same path.
    # A rewrite or a reused-mount collision differ in mtime and get
    # rejected; extraction falls back to the verified archive copy.
    wc_source_paths = {}
    wc_dest_folders = set()  # exact-match scope for extraction
    # Intra-run duplicate destinations: token -> dest folder where the
    # identity landed this run (mirrors ingest's batch_dest_folders).
    run_dest_folders = {}
    # Byte-proven verified hash for each intra-run token. A ('hash', h)
    # token's own value IS the proof; a ('key', …) token carries no bytes,
    # so accepting a later key match against a run twin requires hashing
    # the current source and comparing against this recorded value (two
    # different files with the same filename+size+capture-second across
    # cards would otherwise be counted as skipped_duplicate without ever
    # verifying bytes — safe_to_format green, second card is only copy).
    run_verified_hashes = {}
    linked_dup_dirs = set()    # dup-twin dirs already scanned+linked

    # Sticky across the rest of the run once a mounted → unmounted
    # transition is observed. The per-batch rollback below undoes
    # ``dup_skips`` and ``landed`` but not the identities the same batch
    # already installed in the job-wide ``checker`` (and in
    # ``run_dest_folders`` / ``run_verified_hashes``) via
    # ``_record_checker`` — and ``DuplicateChecker`` exposes no removal
    # API, so those entries cannot be surgically undone. If the share
    # remounts before a later batch, a same-content card file would hit
    # the intra-run fast path and be counted as a duplicate of a landing
    # whose archive claim was rolled back. Refusing every remaining
    # batch keeps the stale intra-run cache from ever being consulted.
    # Same rationale as the remote path (see PR #1400 review, Codex P2
    # r3688614624).
    mount_ever_lost = None

    def _counts(rel):
        return folder_counts.setdefault(
            rel, {"copied": 0, "skipped_duplicate": 0, "failed": 0},
        )

    def _fail(rel, source_file, reason):
        nonlocal failed
        failed += 1
        _counts(rel)["failed"] += 1
        unsafe_files.append({"path": str(source_file), "reason": reason})
        log.warning("Import failed for %s: %s", source_file, reason)

    def _reclassify_landed_failed(rel, entry, reason):
        """Move a landed file's count from copied/skipped_duplicate to failed.

        A landed entry has already been booked into ``copied`` (fresh copy)
        or ``skipped_duplicate`` (crash-recovery adopt) at the moment its
        bytes were verified on disk. When a later step in the batch pass
        (scan itself failing, a missing catalog row after scan, or a
        hash mismatch against what scan re-hashed) forces this file into
        the ``failed`` bucket, the origin count must be rolled back —
        otherwise the exactly-one-terminal-bucket invariant breaks and
        ``copied + skipped_duplicate + failed`` exceeds ``discovered``.
        """
        nonlocal copied, verified, skipped_duplicate
        dest_path = entry.dest_path
        origin = entry.origin
        if origin == "copied":
            copied -= 1
            if verified_counted_for_copies:
                verified -= 1
            _counts(rel)["copied"] -= 1
        elif origin == "skipped_duplicate":
            skipped_duplicate -= 1
            _counts(rel)["skipped_duplicate"] -= 1
        _fail(rel, dest_path, reason)

    def _record_checker(source_file, dest_folder, file_hash):
        """Register a landed file's identity with the intra-run checker.

        ``DuplicateChecker.record`` re-``os.stat``s the source path — on
        removable media that was yanked just after ``copy_and_hash_verify``
        succeeded, that raises ``OSError`` and would kill the whole
        background job even though this file's bytes are already
        verified at ``dest_folder``. Swallow the error, keep the copy in
        the ledger, and accept that later intra-run tokens for this
        file's identity won't dedupe: the archive is intact, the run
        just loses a small cache-hit optimization.
        """
        if checker is None:
            return
        try:
            tokens = checker.record(source_file)
        except OSError as e:
            log.warning(
                "Duplicate-checker record() failed for %s after landing "
                "at %s: %s",
                source_file, dest_folder, e,
            )
            return
        for tok in tokens:
            run_dest_folders[tok] = dest_folder
            run_verified_hashes[tok] = file_hash

    # Dup-folder linking runs in a SEPARATE ``scan(restrict_dirs=…)`` call
    # after the duplicate skip; its exception was previously logged and
    # swallowed, leaving safe_to_format true while the imported
    # duplicates never became visible in the active workspace. Track it
    # explicitly so safe_to_format reflects "workspace can actually see
    # these bytes" and not just "the bytes are somewhere on disk".
    dup_link_failed = False

    for rel, batch in batches:
        if runner.is_cancelled(job["id"]):
            cancelled = True
            break

        # A detach observed in an earlier batch is sticky for the rest
        # of the run: the intra-run duplicate cache and the job-wide
        # checker hold identities for files whose landing was rolled
        # back, and consulting them against a remounted share would
        # count fresh card files as duplicates of copies that never
        # completed. Refuse every remaining file rather than risk a
        # stale-cache hit. See PR #1400 review (Codex P2 r3688614624).
        if mount_ever_lost:
            for source_file in batch:
                emitted += 1
                _fail(
                    rel, source_file,
                    f"archive mount root {mount_ever_lost} detached "
                    "earlier in this import; the intra-run duplicate "
                    "cache still holds identities for files whose "
                    "landing was rolled back, so no further batch can "
                    "be trusted to consult it",
                )
            _emit(
                f"{rel}: archive unmounted", emitted, queued,
            )
            continue

        # Normalize so the "/" strftime puts in ``rel`` (e.g. "2026/07-03")
        # lines up with what scanner stores. Scanner wraps paths in
        # ``Path(...)`` before writing the folder row and building its
        # restrict_files set, which on Windows rewrites mid-path "/" to
        # "\\"; a raw os.path.join here would leave copied files invisible
        # to the restricted scan and unfindable in the post-scan lookup.
        dest_folder = (
            os.path.normpath(os.path.join(destination, rel))
            if rel != "." else destination
        )
        # Reject the whole batch before creating any directories on the
        # card. The per-file loop below already refuses ``dest_file``
        # under a source, but that check runs AFTER ``os.makedirs``, so
        # a rejected unsafe import would still create the archive
        # directory tree on the source (or raise on read-only media,
        # killing the background job instead of returning a controlled
        # unsafe result). This mirror at the batch boundary keeps the
        # failure quiet and preserves the ``ok`` field for the API.
        # See PR #1107 review.
        if _path_under_any_source(dest_folder):
            for source_file in batch:
                # Count these as emitted so the progress bar reflects the
                # rejected batch instead of freezing at the last copied
                # file. Mirrors the remote guard — spec decision 2.
                emitted += 1
                _fail(
                    rel, source_file,
                    "destination folder resolves inside a source directory "
                    "(dest_folder would be created under the card being "
                    "imported); formatting the card would erase the archive "
                    "copy",
                )
            _emit(
                f"{rel}: {_counts(rel)['copied']} copied · "
                f"{_counts(rel)['skipped_duplicate']} already present",
                emitted, queued,
            )
            continue
        # Same guard the remote path applies (see ``_missing_mount_root``
        # in ``_run_remote_import_job``): if the archive's mount root has
        # gone (share never attached, or unmounted mid-run), refuse the
        # batch instead of letting ``os.makedirs`` recreate the vacated
        # mount point as a plain local directory. That shadow tree would
        # take the card's bytes onto the internal disk, look like a
        # successful import, and then hide the moment the real share
        # remounts over it. Probed per batch so a share that drops during
        # a multi-hour card import is caught at the next batch boundary —
        # a Tailscale/SMB archive did exactly that on 2026-07-30, and only
        # a root-owned ``/Volumes`` turned the shadow write into a crash
        # instead of silent data misplacement.
        missing_mount_root = _missing_archive_mount_root(destination)
        if missing_mount_root:
            for source_file in batch:
                # Count these as emitted: ``emitted`` otherwise only
                # advances inside the per-file loop below, which this
                # branch skips, and a progress bar frozen at the last
                # copied file reads as "still working" while the rest of
                # the card is quietly failing.
                emitted += 1
                _fail(
                    rel, source_file,
                    f"archive mount root {missing_mount_root} is not "
                    "available (destination drive is not mounted; refusing "
                    "to create a shadow directory tree under it, which "
                    "would prevent the real share from remounting)",
                )
            _emit(
                f"{rel}: archive unavailable", emitted, queued,
            )
            continue
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
        stale_mount_root = _unmounted_since_baseline(mount_baseline)
        if stale_mount_root:
            for source_file in batch:
                emitted += 1
                _fail(
                    rel, source_file,
                    f"archive mount root {stale_mount_root} is no longer "
                    "mounted (it was at the start of this import; the "
                    "directory persists but the share has detached, so "
                    "copying here would write to the local disk under a "
                    "stale mount point)",
                )
            _emit(
                f"{rel}: archive unmounted", emitted, queued,
            )
            continue
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
                emitted += 1
                _fail(
                    rel, source_file,
                    f"could not create destination folder {dest_folder}: {e}",
                )
            _emit(
                f"{rel}: destination unavailable", emitted, queued,
            )
            continue

        # Promote any pre-existing folder row for this destination out of
        # ``'missing'``. Standalone scans run ``check_folder_health()`` as
        # their preflight, so a reattached archive drive transitions
        # ``missing`` → ``ok`` before its files become visible in the
        # workspace again. The import path calls ``scanner.scan()``
        # directly, and scan's success stamp only clears ``'partial'``
        # (see ``_update_folder_status(only_from_partial=True)``), so a
        # folder row still marked ``'missing'`` would keep the archive
        # drive's photos filtered out of workspace queries even after this
        # import successfully lands and hash-stamps files into it, and
        # safe_to_format could go green over folders the UI won't show.
        # We just makedirs'd ``dest_folder`` so the path definitely exists;
        # any row still labelled ``'missing'`` is stale. Preserve
        # ``'partial'`` (a real prior-scan signal that the folder needs a
        # rescan). See PR #1107 review.
        db.conn.execute(
            "UPDATE folders SET status = 'ok' "
            "WHERE path = ? AND status = 'missing'",
            (dest_folder,),
        )
        db.conn.commit()

        # ``_LandedFile`` entries for this batch's landed files — fresh
        # copies (``origin="copied"``) plus byte-identical files already
        # present at the destination (the crash-recovery adoption path,
        # ``origin="skipped_duplicate"``). ``source_path`` feeds
        # working-copy extraction so it reads local card bytes, never the
        # just-written archive copy.
        landed = []
        # Whether each ``copied`` booking also incremented ``verified``:
        # the local path hash-verifies every copy (``copy_and_hash_verify``
        # reads the landed bytes back), so ``_reclassify_landed_failed``
        # must always undo both. (The remote path books ``verified`` only
        # under ``params.verify_by_hash`` and sets this accordingly. PR
        # 7's transport ``attests_bytes`` absorbs this flag.)
        verified_counted_for_copies = True
        dup_dirs = set()
        # Duplicate skips accepted in this batch that never enter
        # ``landed`` — (source_file, counted_unverified). An accepted skip
        # asserts "the archive already holds these bytes", which stops
        # being true the moment the share detaches: the twin it matched
        # may be in the local shadow. A duplicate-only batch would
        # otherwise satisfy copied + skipped_duplicate == discovered and
        # report safe_to_format True over an archive holding nothing.
        # See PR #1396 review (Codex P1 r3687506040).
        dup_skips = []
        # Sticky once tripped: a batch can hold hundreds of files (the
        # 2026-07-26 folder held 200), and when it is the only batch there
        # is no later batch boundary to catch a detach. Probing per file
        # keeps the blast radius at one file instead of a whole folder.
        # One ismount call is nothing next to copying and hashing a RAW.
        mount_lost = None
        # Sticky signal that a destination-side hash in the per-file loop
        # below was cancelled mid-read (``DestReadCancelled``). Any such
        # cancel is evidence the mount is misbehaving, so the post-loop
        # catalog block MUST skip its ``scan()`` / ``_rehash_dest_or_none``
        # calls on the same paths — they would hit the same wedged mount
        # and pin the job in "cancelling" for the mount's own timeout,
        # exactly the failure mode this PR set out to eliminate. A plain
        # user Stop on a healthy mount leaves this False (the catalog
        # runs normally so partially-landed batches stay cataloged the
        # way ``test_cancel_leaves_valid_partial_catalog`` expects).
        # Mirrors the remote path. See PR #1423 review (Codex P2
        # r3716433830).
        dest_read_cancelled = False

        for source_file in batch:
            if runner.is_cancelled(job["id"]):
                cancelled = True
                break
            if not mount_lost:
                mount_lost = _unmounted_since_baseline(mount_baseline)
            if mount_lost:
                emitted += 1
                _fail(
                    rel, source_file,
                    f"archive mount root {mount_lost} detached while this "
                    "batch was copying (the directory persists but the "
                    "share is gone, so further writes would land on the "
                    "local disk under a stale mount point)",
                )
                continue
            emitted += 1
            _emit(
                f"{rel}: importing", emitted, queued, source_file.name,
                is_importing=True,
            )

            # Duplicate gate.
            if checker is not None:
                try:
                    token = checker.match(source_file)
                except OSError as e:
                    _fail(rel, source_file, f"duplicate check failed: {e}")
                    continue
                if token is not None:
                    if (
                        params.trust_likely_duplicates
                        and not params.verify_by_hash
                    ):
                        likely_rows = _likely_twin_rows(
                            db, token, source_file, _path_under_any_source,
                        )
                        if likely_rows:
                            skipped_duplicate += 1
                            unverified_duplicate += 1
                            _counts(rel)["skipped_duplicate"] += 1
                            dup_skips.append((source_file, True))
                            dup_dirs.update(_linkable_twin_dirs(
                                likely_rows, _path_under_destination,
                            ))
                            continue
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
                                rel, source_file,
                                f"duplicate check failed: {e}",
                            )
                            continue
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
                    if token in run_dest_folders:
                        if token[0] == "hash":
                            accept = True
                        else:
                            run_hash = run_verified_hashes.get(token)
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
                            if _path_under_any_source(twin_path):
                                continue
                            try:
                                twin_hash = _hash_dest_file(
                                    twin_path, _stop_requested)
                            except DestReadCancelled:
                                cancelled = True
                                dest_read_cancelled = True
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
                    if cancelled:
                        # Stop interrupted a twin hash above. Don't let
                        # this file fall through to the adopt/copy path —
                        # every further step touches the same (possibly
                        # dead) mount.
                        break
                    if accept:
                        skipped_duplicate += 1
                        _counts(rel)["skipped_duplicate"] += 1
                        dup_skips.append((source_file, False))
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
                        dup_dirs.update(
                            _linkable_twin_dirs(
                                verified_twin_rows, _path_under_destination,
                            ),
                        )
                        run_dest = run_dest_folders.get(token)
                        if run_dest is not None:
                            dup_dirs.add(run_dest)
                        continue
                    # No byte-identical twin remains on disk — the card
                    # file is a distinct photo; import it normally.

            # Destination path + collision handling (mirrors ingest()).
            dest_file = os.path.join(dest_folder, source_file.name)
            # Reject the source-under-destination overlap where the folder
            # template maps the source right back to its own directory
            # (e.g. source ``/archive/2026/2026-07-05``, destination
            # ``/archive``, template ``%Y/%Y-%m-%d`` → dest_file IS the
            # source file). The API rejects destinations INSIDE any source;
            # this catches the opposite direction, where the destination is
            # a legal ancestor but the template resolves back to the source
            # directory. Without this the adopt branch below hashes the
            # source against itself, records it as ``skipped_duplicate``,
            # and safe_to_format goes green — deleting/formatting the
            # source then erases the only copy. See PR #1107 review.
            try:
                same_file = (
                    os.path.exists(dest_file)
                    and os.path.samefile(str(source_file), dest_file)
                )
            except OSError:
                # Fall back to normalized-path equality when samefile can't
                # stat (e.g. the destination is a stale entry). Prefer a
                # false positive here (fail this file) over a false
                # negative that lets the adopt branch loop back onto the
                # source itself.
                same_file = (
                    os.path.normpath(str(source_file))
                    == os.path.normpath(dest_file)
                )
            # Also reject any dest_file (not just an exact self-copy) that
            # resolves under any source root. Example: source
            # ``/Volumes/Card/DCIM``, destination ``/Volumes/Card``,
            # template ``DCIM/Archive/%Y`` — dest_file lands at
            # ``/Volumes/Card/DCIM/Archive/2026/<name>``, which is NOT the
            # source file (samefile is False) but is still inside the card.
            # A copy there is counted as ``copied``, safe_to_format can go
            # green, and formatting the card erases the "archive" copy too.
            # See PR #1107 review.
            dest_under_source = _path_under_any_source(dest_file)
            if same_file or dest_under_source:
                _fail(
                    rel, source_file,
                    "destination file resolves inside a source directory "
                    "(dest_file would live under the card being imported); "
                    "formatting the card would erase the archive copy",
                )
                continue
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
                _fail(rel, source_file, str(e))
                continue
            src_size = src_stat.st_size
            src_mtime_ns = src_stat.st_mtime_ns
            try:
                # Source hash is potentially needed by three checks below
                # (primary-name adopt, per-suffix candidate adopt, and the
                # copy_and_hash_verify src_hash arg). Compute lazily and
                # cache in a small closure so nothing hashes the card
                # twice.
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

                adopted_dest = None  # (path, hash) when byte-identical twin found

                if os.path.exists(dest_file):
                    dest_size = os.path.getsize(dest_file)
                    if src_size == 0 and dest_size == 0:
                        # Zero-byte twin: identical by definition, but kept
                        # out of the duplicate-identity index (see ingest).
                        # Treat it as an adopted landing, not a cataloged
                        # twin: a crash may have left these bytes on disk
                        # before any folder/photo row was committed. The
                        # exact-file batch scan below catalogs that recovery
                        # case without walking the rest of the directory.
                        adopted_dest = (dest_file, EMPTY_FILE_SHA256)
                    elif src_size == dest_size:
                        dest_hash = _hash_dest_file(
                            dest_file, _stop_requested)
                        src_h = _src_hash_cached()
                        if src_h is not None and src_h == dest_hash:
                            # Byte-identical file already at the destination
                            # (e.g. a previous run died between copy and
                            # catalog). Treat as landed: catalog + stamp it
                            # rather than skipping — this is the designed
                            # self-heal for crash-shaped interruptions.
                            adopted_dest = (dest_file, src_h)
                    if adopted_dest is None:
                        # Different content, same primary name — advance
                        # through numeric suffixes. But a crash-interrupted
                        # retry may already have written THIS source's bytes
                        # under an earlier suffix: an earlier run copied a
                        # colliding different file to ``name.ext`` and put
                        # this source's bytes at ``name_1.ext``, then died
                        # before its scan. Advancing past ``name_1.ext``
                        # without hashing it would re-copy identical bytes
                        # to ``name_2.ext`` and leave two archive copies of
                        # one source photo. Hash-match every existing
                        # suffix candidate and adopt on a match; on no
                        # match, land at the next free suffix. See PR #1107
                        # review.
                        stem, suffix = os.path.splitext(source_file.name)
                        counter = 1
                        while True:
                            candidate = os.path.join(
                                dest_folder, f"{stem}_{counter}{suffix}",
                            )
                            if not os.path.exists(candidate):
                                dest_file = candidate
                                break
                            try:
                                cand_size = os.path.getsize(candidate)
                            except OSError:
                                cand_size = -1
                            if cand_size == src_size:
                                cand_hash = _hash_dest_file(
                                    candidate, _stop_requested)
                                src_h = _src_hash_cached()
                                if (
                                    cand_hash is not None
                                    and src_h is not None
                                    and cand_hash == src_h
                                ):
                                    adopted_dest = (candidate, src_h)
                                    break
                            counter += 1

                if adopted_dest is not None:
                    dest_file, adopt_hash = adopted_dest
                    skipped_duplicate += 1
                    _counts(rel)["skipped_duplicate"] += 1
                    landed.append(
                        _LandedFile(
                            dest_path=dest_file,
                            verified_hash=adopt_hash,
                            source_path=str(source_file),
                            origin="skipped_duplicate",
                            src_size=src_size,
                            src_mtime_ns=src_mtime_ns,
                        ),
                    )
                    _record_checker(source_file, dest_folder, adopt_hash)
                    continue

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
                cancelled = True
                dest_read_cancelled = True
                break
            except OSError as e:
                _fail(rel, source_file, str(e))
                continue
            if not ok:
                _fail(
                    rel, source_file,
                    "copy verification failed (destination bytes do not "
                    "match the source)",
                )
                continue
            copied += 1
            verified += 1
            _counts(rel)["copied"] += 1
            landed.append(
                _LandedFile(
                    dest_path=dest_file,
                    verified_hash=file_hash,
                    source_path=str(source_file),
                    origin="copied",
                    src_size=src_size,
                    src_mtime_ns=src_mtime_ns,
                ),
            )
            _record_checker(source_file, dest_folder, file_hash)

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
        # this probe. Mirrors the remote path's gate above. See PR #1423
        # review (Codex P2 r3716581283).
        if not mount_lost and not dest_read_cancelled:
            mount_lost = _unmounted_since_baseline(mount_baseline)

        # Accepted duplicate skips rest on a twin that may live in the
        # local shadow rather than on the share, so a detach invalidates
        # the "already present" claim exactly as it invalidates a copy.
        # These never enter ``landed``, so they need their own rollback:
        # without it a duplicate-only batch reports every file safely
        # accounted for and the card looks safe to erase over an archive
        # holding none of the bytes. ``dup_dirs`` is dropped for the same
        # reason — linking those folders would pull shadow paths into the
        # workspace. See PR #1396 review (Codex P1 r3687506040).
        if mount_lost and dup_skips:
            for skipped_file, counted_unverified in dup_skips:
                skipped_duplicate -= 1
                _counts(rel)["skipped_duplicate"] -= 1
                if counted_unverified:
                    unverified_duplicate -= 1
                _fail(
                    rel, skipped_file,
                    f"archive mount root {mount_lost} detached mid-batch; "
                    "the duplicate this file matched cannot be confirmed "
                    "to be on the archive rather than in a local shadow",
                )
            dup_skips = []
            dup_dirs = set()

        # Anything that landed BEFORE the detach is sitting in the local
        # shadow, not on the archive. Roll those out of copied/
        # skipped_duplicate into failed and drop them: cataloging them
        # would record archive paths for bytes that vanish when the real
        # share remounts, and leaving them booked as copied could let
        # safe_to_format go green over a card that is still the only
        # copy. Emptying ``landed`` also skips the catalog scan below.
        if mount_lost and landed:
            for entry in landed:
                _reclassify_landed_failed(
                    rel, entry,
                    f"archive mount root {mount_lost} detached mid-batch; "
                    "this file landed in a local shadow of the archive, "
                    "not on the share",
                )
            landed = []

        # Trip the run-wide sticky flag so every remaining batch is
        # refused at the top of the loop rather than allowed to consult
        # the intra-run duplicate cache (which still holds identities
        # for the files just rolled back above; the checker has no
        # removal API). See PR #1400 review (Codex P2 r3688614624).
        if mount_lost:
            mount_ever_lost = mount_lost

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
        # batches keep cataloging like before. Mirrors the remote path.
        # See PR #1423 review (Codex P2 r3716433830).
        if landed and not dest_read_cancelled:
            landed_paths = {entry.dest_path for entry in landed}
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
            for entry in landed:
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
                # end-of-run pass below (per-batch extraction would race
                # RAW+JPEG pairing across batch boundaries). See PR
                # #1107 review.
                scan(
                    destination, db,
                    restrict_dirs=[dest_folder],
                    restrict_files=landed_paths,
                    vireo_dir=params.vireo_dir,
                    thumb_cache_dir=params.thumb_cache_dir,
                    skip_working_copies=True,
                )
            except Exception as e:  # scan failure fails the whole batch
                # Each entry was already booked into copied or
                # skipped_duplicate — reclassify (roll back origin, add
                # to failed) so the ledger never double-counts.
                for entry in landed:
                    _reclassify_landed_failed(
                        rel, entry, f"catalog scan failed: {e}",
                    )
                landed = []
            else:
                # Restricted scan committed new photo rows and
                # created/linked ``workspace_folders`` entries under
                # ``dest_folder``; the /api/workspaces/active/new-images
                # endpoint serves a cached filesystem diff that will
                # otherwise report the just-imported files as new until
                # the cache expires or another full scan runs. Mirrors
                # api_job_scan / api_job_import_full / pipeline_job.
                _invalidate_new_images(db, dest_folder)

            # dest_paths that hash-stamping reclassified from
            # copied/skipped_duplicate to failed. The entries stay in
            # ``landed`` (mutating a list during its own iteration is
            # error-prone), so we filter them out of the working-copy
            # override map below — otherwise the deferred
            # ``_extract_working_copies`` would read card-side bytes for
            # a photo whose catalog row is missing (JPEG-pair miss aside)
            # or whose archive bytes no longer match what we copied, and
            # cache a working copy that doesn't correspond to what the
            # rest of the app sees at the archive path. See PR #1107 review.
            reclassified_landed_paths = set()

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
            # JPEG (see the accept branch below). See PR #1107 review.
            raw_companion_invalidations = set()

            def _rehash_dest_or_none(path):
                """Re-hash the archive file, returning None on read failure.

                Used as the last-line check that the bytes currently at the
                archive path still match what ``copy_and_hash_verify()``
                landed — necessary any time the scan-side hash is missing
                (paired-JPEG row deletion) or NULL (scanner hashed the empty
                zero-byte convention aside, a NULL means the archive read
                failed between promote and scan). Without it, mutation of
                the archive file between promote and scan would still be
                accepted as success.
                """
                try:
                    return _hash_dest_file(path, _stop_requested)
                except DestReadCancelled:
                    raise
                except OSError:
                    return None

            # Stamp the verified hashes in the integrity-audit vocabulary,
            # cross-checked against what scan() stored.
            for entry in landed:
                dest_path = entry.dest_path
                verified_hash = entry.verified_hash
                row = db.conn.execute(
                    """SELECT p.id, p.file_hash FROM photos p
                       JOIN folders f ON f.id = p.folder_id
                       WHERE f.path = ? AND p.filename = ?""",
                    (os.path.dirname(dest_path), os.path.basename(dest_path)),
                ).fetchone()
                if row is None:
                    # RAW+JPEG pairing merges the JPEG's photo row into the
                    # RAW primary (companion_path); the JPEG's own row is
                    # gone by design and the bytes are represented on the
                    # RAW. But the pair lookup can't tell us the JPEG's
                    # archive bytes are still the ones we verified — the
                    # archive file could have been rewritten or corrupted
                    # between promote and the restricted scan. Re-read the
                    # archive path and require its hash to still equal
                    # ``verified_hash`` before counting the JPEG landed;
                    # otherwise reclassify to failed. See PR #1107 review.
                    companion = db.conn.execute(
                        """SELECT p.id FROM photos p
                           JOIN folders f ON f.id = p.folder_id
                           WHERE f.path = ? AND p.companion_path = ?""",
                        (
                            os.path.dirname(dest_path),
                            os.path.basename(dest_path),
                        ),
                    ).fetchone()
                    if companion is not None:
                        try:
                            actual = _rehash_dest_or_none(dest_path)
                        except DestReadCancelled:
                            cancelled = True
                            break
                        if actual is not None and actual == verified_hash:
                            # Landed JPEG paired with an existing RAW
                            # row. Invalidate the RAW's derived caches
                            # regardless of origin: adoption
                            # (``skipped_duplicate``) only proves the
                            # JPEG bytes were already at the archive
                            # path, NOT that the RAW row already carried
                            # ``companion_path`` for this JPEG. A prior
                            # partial run or backfill may have left the
                            # RAW as RAW-only (with a
                            # ``working_copy_path`` or
                            # ``working_copy_failed_at`` built without
                            # knowing this companion existed); the
                            # deferred end-of-run
                            # ``_extract_working_copies`` skips RAWs
                            # whose ``working_copy_path IS NOT NULL``,
                            # so a stale RAW-only cache would persist
                            # past this import and the UI would keep
                            # serving derived files for the pre-pair
                            # state. Fresh-copy JPEGs need this too
                            # (RAW may have been standalone or paired
                            # with a since-deleted companion). See PR
                            # #1107 review.
                            raw_companion_invalidations.add(
                                companion["id"],
                            )
                            # The landed JPEG's bytes are now represented
                            # on the RAW primary — that row is what the
                            # chaining hook should process.
                            imported_photo_ids.add(companion["id"])
                            continue
                        _reclassify_landed_failed(
                            rel, entry,
                            "paired companion archive bytes no longer "
                            "match the copy-time hash",
                        )
                        reclassified_landed_paths.add(dest_path)
                        continue
                    _reclassify_landed_failed(
                        rel, entry, "not cataloged after scan",
                    )
                    reclassified_landed_paths.add(dest_path)
                    continue
                if row["file_hash"] == verified_hash:
                    db.update_photo_hash_check(
                        row["id"], "ok", commit=False,
                    )
                    imported_photo_ids.add(row["id"])
                elif row["file_hash"] is None:
                    if verified_hash == EMPTY_FILE_SHA256:
                        # Zero-byte convention: EMPTY_FILE_SHA256 never
                        # lands in file_hash (it would collide with every
                        # other empty file). Status only.
                        db.update_photo_hash_check(
                            row["id"], "ok", commit=False,
                        )
                        imported_photo_ids.add(row["id"])
                    else:
                        # Non-empty file with NULL file_hash after scan
                        # means scanner._compute_file_features couldn't
                        # read the archive file (unreadable between
                        # promote and scan). Trusting the copy-time hash
                        # here would flip ``safe_to_format`` green for
                        # bytes we can't currently verify on disk. Re-
                        # hash the archive path from here as a last check
                        # — if that also fails or disagrees with our
                        # copy-time hash, reclassify to failed instead of
                        # stamping a stale value. See PR #1107 review.
                        try:
                            actual = _rehash_dest_or_none(dest_path)
                        except DestReadCancelled:
                            cancelled = True
                            break
                        if actual is not None and actual == verified_hash:
                            db.update_photo_hash_check(
                                row["id"], "ok", file_hash=verified_hash,
                                commit=False,
                            )
                            imported_photo_ids.add(row["id"])
                        else:
                            _reclassify_landed_failed(
                                rel, entry,
                                "archive file unhashable after copy "
                                "verification (scan wrote no hash and "
                                "re-hash disagrees)",
                            )
                            reclassified_landed_paths.add(dest_path)
                else:
                    _reclassify_landed_failed(
                        rel, entry,
                        "destination changed between copy verification and "
                        "catalog scan (hash mismatch)",
                    )
                    reclassified_landed_paths.add(dest_path)

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
            invalidated_photo_ids = set()
            if params.vireo_dir:
                from scanner import _invalidate_derived_caches
                for entry in landed:
                    dest_path = entry.dest_path
                    if dest_path in reclassified_landed_paths:
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

            # Accumulate the card-source mapping for the deferred
            # end-of-run ``_extract_working_copies`` call. Extraction
            # cannot run here per-batch: a RAW+JPEG companion pair that
            # straddles a batch boundary would still be unpaired at this
            # point, and the extractor would read the RAW before scan()
            # in a later batch pairs the JPEG — poisoning the row with a
            # failure marker or low-quality WC that the candidate
            # predicate then skips.
            if params.vireo_dir:
                for entry in landed:
                    dest_path = entry.dest_path
                    if dest_path in reclassified_landed_paths:
                        # Reclassified to failed by hash stamping above
                        # (missing row or archive-vs-copy hash mismatch).
                        # Skipping the card override lets the WC extractor
                        # fall back to whatever the archive currently
                        # holds — matching the catalog's view — instead
                        # of caching a WC of bytes the archive no longer
                        # has.
                        continue
                    src_path = entry.source_path
                    exp_size = entry.src_size
                    exp_mtime_ns = entry.src_mtime_ns
                    wc_source_paths[dest_path] = (
                        src_path, exp_size, exp_mtime_ns,
                    )
                wc_dest_folders.add(dest_folder)

        # --- Link duplicate-twin folders -------------------------------
        # The twins already have catalog rows and _linkable_twin_dirs has
        # checked that their folders exist under this destination. Link
        # those rows directly. A broad incremental scan would still
        # enumerate/stat every file in the matched NAS folders before the
        # import could finish; uncataloged-stray repair belongs to the
        # explicit folder-rescan workflow instead.
        new_dup_dirs = dup_dirs - linked_dup_dirs
        if new_dup_dirs:
            linked, failures = _link_duplicate_twin_dirs(
                db, workspace_id, new_dup_dirs,
            )
            linked_dup_dirs.update(linked)
            if failures:
                dup_link_failed = True
                for d, detail in failures.items():
                    unsafe_files.append({
                        "path": d,
                        "reason": (
                            "duplicate-folder workspace link failed: "
                            f"{detail}"
                        ),
                    })
        _emit(
            f"{rel}: {_counts(rel)['copied']} copied · "
            f"{_counts(rel)['skipped_duplicate']} already present",
            emitted, queued,
        )

        if cancelled:
            break

    # --- Deferred working-copy extraction ---------------------------
    # One extraction pass over every folder this run touched, after all
    # batches have landed and been paired. Reads card-side bytes for any
    # dest_path present in ``wc_source_paths``; anything else (crash-
    # recovery adopted files whose card is gone, later backfill retries)
    # falls back to the cataloged archive path. Per-row failures mark the
    # photo for the scanner's later backfill and never fail the import.
    #
    # If the run was already cancelled at a batch boundary, skip the pass
    # entirely — otherwise Stop appears hung for minutes on large RAW
    # imports while the extractor decodes what the user asked us to
    # abort. During the pass, poll ``runner.is_cancelled`` so cancellation
    # aborts extraction row-by-row too.
    if params.vireo_dir and wc_dest_folders and not cancelled:
        from scanner import _extract_working_copies

        try:
            _extract_working_copies(
                db, params.vireo_dir,
                scope=[(d, "exact") for d in sorted(wc_dest_folders)],
                source_paths=wc_source_paths,
                cancel_check=lambda: runner.is_cancelled(job["id"]),
            )
        except Exception:
            log.exception(
                "Working-copy extraction failed for %s",
                sorted(wc_dest_folders),
            )
        if runner.is_cancelled(job["id"]):
            cancelled = True

    status = "cancelled" if cancelled else (
        "failed" if failed else "completed"
    )
    summary = _selection_summary(
        params, include_paths, discovered=discovered, copied=copied,
        skipped_duplicate=skipped_duplicate, failed=failed,
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
    for exc in discovery_errors:
        unsafe_files.append({
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
    if unverified_duplicate:
        unsafe_files.append({
            "path": "Likely duplicates",
            "reason": (
                f"{unverified_duplicate} matched by filename, byte size, "
                "and capture time but were not compared byte-for-byte"
            ),
        })
    # Selection drift entries. Shared with the remote path.
    _append_selection_unsafe(
        unsafe_files, deselected=deselected, vanished_paths=vanished_paths,
        appeared=appeared,
    )
    safe_to_format = (
        not cancelled
        and failed == 0
        and not discovery_errors
        and not dup_link_failed
        and not partial_scope
        and unverified_duplicate == 0
        and (copied + skipped_duplicate) == discovered
        and not _selection_blocks_format(
            deselected=deselected, vanished_paths=vanished_paths)
    )
    unverified_duplicates_only = (
        unverified_duplicate > 0
        and not cancelled
        and failed == 0
        and not discovery_errors
        and not dup_link_failed
        and not partial_scope
        and (copied + skipped_duplicate) == discovered
        and not _selection_blocks_format(
            deselected=deselected, vanished_paths=vanished_paths)
    )
    result = {
        "discovered": discovered,
        "copied": copied,
        "verified": verified,
        "photo_ids": sorted(imported_photo_ids),
        # Stable-identity map so a recovery retry can verify each carried
        # ID still points at the same file. Without this the retry
        # authorizes any current photo row that happens to share an ID
        # with something the parent landed — an especially real risk
        # after users delete recent imports (SQLite reuses the freed
        # IDs on the next insert).
        "photo_fingerprints": _capture_photo_fingerprints(
            db, imported_photo_ids,
        ),
        # Per-source signature over the discovered file set so a
        # recovery retry can detect a source whose contents changed
        # between the failed run and the retry — e.g. a different SD
        # card mounted at the same path, or new photos added to the
        # same card. Captured at DISCOVERY time (see the ``source_snapshots``
        # assignment above the copy loop); recording it here instead
        # would let a mid-copy card ejection stamp ``-1`` sizes and
        # refuse a legitimate reinsert-and-retry recovery.
        "source_snapshots": source_snapshots,
        "skipped_duplicate": skipped_duplicate,
        "unverified_duplicate": unverified_duplicate,
        "unverified_duplicates_only": unverified_duplicates_only,
        "failed": failed,
        "safe_to_format": safe_to_format,
        "unsafe_files": unsafe_files,
        "folders": folder_counts,
        "cancelled": cancelled,
        "discovery_errors": len(discovery_errors),
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
            failed == 0
            and not discovery_errors
            and not dup_link_failed
        ),
        "errors": [f"{u['path']}: {u['reason']}" for u in unsafe_files],
    }
    return result
