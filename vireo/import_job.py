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
   "cancelled" when the work function returns after a Stop.
4. Batch unit: files grouped by destination (template) folder,
   processed in template order, chunked to at most
   ``IMPORT_BATCH_SIZE`` files per scan call. Restricted scans only
   enumerate the restricted dirs, so per-batch scan cost tracks the
   batch, not the archive tree. One deliberate deviation from the plan
   text: duplicate-matched folders are scanned in a SEPARATE
   ``scan(restrict_dirs=…)`` call without ``restrict_files`` —
   ``_ensure_folder`` only fires for *discovered* files, so folding
   duplicate dirs into the fresh-copy call (whose ``restrict_files``
   excludes their files) would create/link nothing for duplicate-only
   imports.
"""

import contextlib
import errno
import logging
import os
import posixpath
import shutil
import sys
import uuid
from dataclasses import dataclass

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
    recursive: bool = True
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
        """SELECT p.id, p.filename, f.path AS folder_path,
                  f.status AS folder_status
           FROM photos p JOIN folders f ON f.id = p.folder_id
           WHERE p.file_hash = ?""",
        (file_hash,),
    ).fetchall()


def _linkable_twin_dirs(rows, under_destination):
    """Destination-scoped folders holding a duplicate's cataloged twin.

    Only folders under the import destination are scanned/linked after a
    duplicate skip (a twin in some other library root is none of this
    import's business). Mirrors ingest()'s dup_token_folders guards:
    path under destination and still a real directory on disk. Status
    ``ok``/``partial`` is trusted as-is; ``missing`` is accepted too when
    the path is still a real directory (a reattached archive drive whose
    row hasn't been refreshed yet), and ``run_import_job`` promotes it
    to ``ok`` before the dup-link scan runs — otherwise a duplicate-only
    batch that matches a missing-marked twin folder would drop it from
    ``dup_dirs``, safe_to_format could go green, and the imported
    duplicates would stay filtered out of workspace queries.

    ``under_destination(path)`` compares resolved/case-normalized paths
    (built in ``run_import_job`` from the destination's own filesystem
    semantics). A lexical prefix check would drop a twin folder when the
    destination is a symlink to the twin's on-disk archive root, or
    spelled with different case on a case-insensitive mount — dropping
    the twin means the duplicate-link scan never runs and the imported
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

    # Reject cataloged twins that live under the card being imported: a stale
    # scan of the mounted card can leave a photos row whose ``folder_path``
    # IS the card, and re-hashing it just re-reads the very source we're
    # supposed to be copying off — which would count the file as
    # ``skipped_duplicate`` and, when ``verify_by_hash`` is on, still let
    # ``safe_to_format`` go green over a card whose bytes never crossed the
    # network. Mirrors the local path's ``_path_under_any_source`` filter.
    _path_under_any_source = _build_source_root_guard(params.sources)

    import move as move_mod

    runner.set_steps(job["id"], [
        {"id": "import", "label": "Copy & catalog"},
    ])
    runner.update_step(job["id"], "import", status="running")

    def _emit(phase, current, total, current_file=""):
        job["progress"]["current"] = current
        job["progress"]["total"] = total
        job["progress"]["current_file"] = current_file
        runner.update_step(
            job["id"], "import",
            current_file=current_file,
            progress={"current": current, "total": total},
        )
        runner.push_event(job["id"], "progress", {
            "phase": phase, "current": current, "total": total,
            "current_file": current_file,
        })

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
    copied = 0
    verified = 0            # count of files independently checksum-verified
    skipped_duplicate = 0
    failed = 0
    unsafe_files = []
    folder_counts = {}
    emitted = 0
    cancelled = False
    wc_source_paths = {}
    wc_dest_folders = set()

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

    for rel, batch in batches:
        if runner.is_cancelled(job["id"]):
            cancelled = True
            break

        dest_folder = (
            os.path.normpath(os.path.join(destination, rel))
            if rel != "." else destination
        )
        ssh_dest = (
            posixpath.join(ssh_base, *rel.split("/")) if rel != "."
            else ssh_base
        )
        os.makedirs(dest_folder, exist_ok=True)

        # Duplicate gate. A remote duplicate skip is only honest when the
        # cataloged twin's bytes are confirmed at the destination; the local
        # path re-hashes the twin's archive file. On the mount that file is
        # locally readable, so reuse the same on-disk re-hash contract.
        to_transfer = []           # (source_file, dest_basename, src_hash)
        dup_skipped = 0
        # dest basename -> src_hash, for intra-batch same-basename collision
        # resolution (FIX 2). Populated as files are queued/skipped.
        claimed_basenames = {}
        for source_file in batch:
            if runner.is_cancelled(job["id"]):
                cancelled = True
                break
            emitted += 1
            _emit(f"{rel}: importing", emitted, discovered, source_file.name)
            if checker is not None:
                try:
                    token = checker.match(source_file)
                except OSError as e:
                    _fail(rel, source_file, f"duplicate check failed: {e}")
                    continue
                if token is not None:
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
                    for twin in twin_rows:
                        twin_path = os.path.join(
                            twin["folder_path"], twin["filename"],
                        )
                        # A twin cataloged under any import source root is
                        # (or may be) the card file being imported this run
                        # — re-hashing it just re-reads the source, proving
                        # nothing about an off-card copy. Accepting it would
                        # count the file as skipped_duplicate and (with
                        # verify_by_hash) let safe_to_format go green while
                        # the card holds the only bytes. Mirrors the local
                        # path's filter. See PR #1113 review.
                        if _path_under_any_source(twin_path):
                            continue
                        try:
                            twin_hash = compute_file_hash(twin_path)
                        except OSError:
                            continue
                        if twin_hash is not None and twin_hash == src_hash:
                            accept = True
                            break
                    if accept:
                        skipped_duplicate += 1
                        dup_skipped += 1
                        _counts(rel)["skipped_duplicate"] += 1
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
            try:
                src_hash = (
                    checker.content_hash(source_file)
                    if checker is not None
                    else compute_file_hash(str(source_file))
                )
            except OSError as e:
                _fail(rel, source_file, str(e))
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
                if candidate in claimed_basenames:
                    # Claimed earlier in this batch (a same-basename sibling
                    # already queued). If that sibling has our exact bytes,
                    # skip as an intra-batch duplicate; otherwise advance.
                    if claimed_basenames[candidate] == src_hash:
                        skipped_duplicate += 1
                        dup_skipped += 1
                        _counts(rel)["skipped_duplicate"] += 1
                        adopted = True
                        break
                    counter += 1
                    continue
                if os.path.exists(cand_mount):
                    # Already on disk (crash-recovery/resume). Byte-identical
                    # -> skip; different -> advance to the next suffix.
                    try:
                        on_disk = compute_file_hash(cand_mount)
                    except OSError:
                        on_disk = None
                    if on_disk is not None and on_disk == src_hash:
                        skipped_duplicate += 1
                        dup_skipped += 1
                        _counts(rel)["skipped_duplicate"] += 1
                        claimed_basenames[candidate] = src_hash
                        adopted = True
                        break
                    counter += 1
                    continue
                dest_basename = candidate
                break
            if adopted:
                continue
            claimed_basenames[dest_basename] = src_hash
            to_transfer.append((source_file, dest_basename, src_hash))

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
        landed = []   # (dest_path, card_source, src_hash, src_size, src_mtime_ns)
        if to_transfer:
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
            ]
            if remote.get("bwlimit_kbps"):
                extra_args.append(f"--bwlimit={int(remote['bwlimit_kbps'])}")
            rsync_target = move_mod.rsync_dest_spec(remote, ssh_dest)
            # rsync creates the leaf itself but not intermediate parents.
            ok_mkdir, mkdir_detail = move_mod._remote_mkdir_p(remote, ssh_dest)
            if not ok_mkdir:
                for sf, _bn, _sh in to_transfer:
                    _fail(rel, sf,
                          f"remote mkdir failed for {ssh_dest}: {mkdir_detail}")
            else:
                # Split into the flat fast path (dest basename == card
                # basename) and collision-renamed files (transferred and
                # verified individually to an explicit NAS filename, since a
                # flat --files-from list to one dir can't rename).
                flat = [
                    (sf, bn, sh) for sf, bn, sh in to_transfer
                    if bn == sf.name
                ]
                renamed = [
                    (sf, bn, sh) for sf, bn, sh in to_transfer
                    if bn != sf.name
                ]

                def _do_rsync(src_specs, target, dest_is_dir, extra_args):
                    try:
                        rc, stderr, timed_out = move_mod._run_rsync_streamed(
                            None, target, [], len(src_specs), None,
                            rsync_bin=rsync_bin, extra_args=extra_args,
                            src_specs=src_specs,
                            src_specs_dest_is_dir=dest_is_dir,
                        )
                        return rc, stderr, timed_out
                    except OSError as exc:
                        return 1, str(exc), False

                transferred = []   # (sf, dest_basename, src_hash, nas_full_path)
                # Flat batch: one rsync into the dir.
                if flat:
                    rc, stderr, timed_out = _do_rsync(
                        [str(sf) for sf, _bn, _sh in flat], rsync_target, True,
                        extra_args)
                    if timed_out:
                        for sf, _bn, _sh in flat:
                            _fail(rel, sf, "rsync stalled (no progress)")
                    elif rc != 0:
                        for sf, _bn, _sh in flat:
                            _fail(rel, sf, f"rsync failed: {stderr.strip()}")
                    else:
                        for sf, bn, sh in flat:
                            transferred.append((
                                sf, bn, sh, posixpath.join(ssh_dest, bn)))
                # Renamed files: one rsync each to the explicit NAS file
                # path (rsync <card> user@host:/dir/DSC_0001_1.jpg).
                for sf, bn, sh in renamed:
                    nas_full = posixpath.join(ssh_dest, bn)
                    rc, stderr, timed_out = _do_rsync(
                        [str(sf)],
                        move_mod.rsync_dest_spec(remote, nas_full), False,
                        extra_args)
                    if timed_out:
                        _fail(rel, sf, "rsync stalled (no progress)")
                    elif rc != 0:
                        _fail(rel, sf, f"rsync failed: {stderr.strip()}")
                    else:
                        transferred.append((sf, bn, sh, nas_full))

                for sf, bn, src_hash, nas_full in transferred:
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
                    try:
                        st = sf.stat()
                        sz, mt = st.st_size, st.st_mtime_ns
                    except OSError:
                        sz, mt = None, None
                    copied += 1
                    _counts(rel)["copied"] += 1
                    if params.verify_by_hash:
                        verified += 1
                    landed.append((dest_path, str(sf), src_hash, sz, mt))

        # --- Catalog this batch ----------------------------------------
        # Fresh copies AND duplicate skips both need the mount folder
        # cataloged+linked (a duplicate-only batch would otherwise leave the
        # mount folder invisible). scan() over the mount is the same call the
        # local path makes; the mount is locally walkable.
        if landed or dup_skipped:
            landed_paths = {entry[0] for entry in landed}
            try:
                scan(
                    destination, db,
                    restrict_dirs=[dest_folder],
                    restrict_files=(landed_paths or None),
                    vireo_dir=params.vireo_dir,
                    thumb_cache_dir=params.thumb_cache_dir,
                    skip_working_copies=True,
                )
            except Exception as e:
                for dest_path, _sf, _sh, _sz, _mt in landed:
                    # Roll back the copied count and fail.
                    copied -= 1
                    _counts(rel)["copied"] -= 1
                    if params.verify_by_hash:
                        verified -= 1
                    _fail(rel, dest_path, f"catalog scan failed: {e}")
                landed = []
            else:
                _invalidate_new_images(db, dest_folder)

            # Catalog-row presence is required on BOTH paths: the route's
            # copy-and-catalog contract says every landed byte becomes a
            # photo row, and a landed file with no row after scan is failed
            # rather than silently left counted as ``copied`` — otherwise a
            # remote import into an unmounted/misconfigured mount base would
            # report copied/ok (or copied/NULL, no-verify) with no catalog
            # trail. (A RAW+JPEG pair whose JPEG row was merged into the RAW
            # is the main legitimate "no row" case; the RAW carries the
            # bytes, so failing the JPEG here is conservative but keeps the
            # pill honest.) Hash stamping (``hash_status='ok'``) still runs
            # ONLY on the checksum-verification path — without
            # verify_by_hash the rows keep NULL hash_status/hash_checked_at
            # (scan may set file_hash, but we don't claim an integrity
            # verdict we didn't independently make).
            for dest_path, _sf, src_hash, _sz, _mt in list(landed):
                row = db.conn.execute(
                    """SELECT p.id, p.file_hash FROM photos p
                       JOIN folders f ON f.id = p.folder_id
                       WHERE f.path = ? AND p.filename = ?""",
                    (os.path.dirname(dest_path),
                     os.path.basename(dest_path)),
                ).fetchone()
                if row is not None:
                    if params.verify_by_hash:
                        # Cross-check the scanned MOUNT row's hash against
                        # the source hash that remote_verify_files just
                        # confirmed at the NAS. remote_verify_files ran
                        # card -> ssh_base (the NAS); scan() reads whatever
                        # is under the mount base. If the mount is stale
                        # or misconfigured but happens to already contain
                        # the same <folder>/<filename> we transferred, scan
                        # populates ``file_hash`` from the mount's (wrong)
                        # bytes. Stamping ``hash_status='ok'`` on that row
                        # would let ``safe_to_format`` go green over
                        # storage we never actually touched. Require the
                        # scanned row's file_hash to match the verified
                        # source hash; otherwise fail this file. Mirrors
                        # the local path's cross-check against
                        # ``verified_hash``. See PR #1113 review.
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
                        if scan_h != src_h_norm:
                            copied -= 1
                            verified -= 1
                            _counts(rel)["copied"] -= 1
                            _fail(
                                rel, dest_path,
                                "scanned mount row hash does not match "
                                "the hash verified on the NAS (mount "
                                "base is likely stale or misconfigured)",
                            )
                            landed = [
                                e for e in landed if e[0] != dest_path
                            ]
                            continue
                        db.update_photo_hash_check(
                            row["id"], "ok", commit=False,
                        )
                else:
                    copied -= 1
                    if params.verify_by_hash:
                        verified -= 1
                    _counts(rel)["copied"] -= 1
                    _fail(rel, dest_path,
                          "not cataloged after scan (no photo row)")
                    landed = [
                        e for e in landed if e[0] != dest_path
                    ]
            db.conn.commit()

            if params.vireo_dir:
                for dest_path, sf, _sh, sz, mt in landed:
                    wc_source_paths[dest_path] = (sf, sz, mt)
                wc_dest_folders.add(dest_folder)

        _emit(
            f"{rel}: {_counts(rel)['copied']} copied · "
            f"{_counts(rel)['skipped_duplicate']} already present",
            emitted, discovered,
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
    runner.update_step(
        job["id"], "import",
        status="failed" if status == "failed" else "completed",
        summary=(
            f"{copied} copied, {skipped_duplicate} already present, "
            f"{failed} failed of {discovered} discovered"
        ),
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
    safe_to_format = (
        not cancelled
        and failed == 0
        and not discovery_errors
        and not partial_scope
        and not remote_unverified
        and (copied + skipped_duplicate) == discovered
    )
    result = {
        "discovered": discovered,
        "copied": copied,
        "verified": verified,
        "skipped_duplicate": skipped_duplicate,
        "failed": failed,
        "safe_to_format": safe_to_format,
        "unsafe_files": unsafe_files,
        "folders": folder_counts,
        "cancelled": cancelled,
        "discovery_errors": len(discovery_errors),
        "ok": (failed == 0 and not discovery_errors),
        "errors": [f"{u['path']}: {u['reason']}" for u in unsafe_files],
    }
    return result


def run_import_job(job, runner, db_path, workspace_id, params):
    """Copy card(s) -> archive, hash-verify, and catalog incrementally.

    Returns the result dict (counts + per-folder breakdown). The catalog
    is committed per batch; cancellation and crashes leave every already-
    verified file cataloged and nothing else.
    """
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
    # a duplicate-only import's dup-link scan is called with the symlink
    # path as root while its ``restrict_dirs`` hold the real path, and
    # ``_ensure_folder``'s walk never reaches root (dead recurse). Sources
    # are already ``realpath``-resolved (see ``_norm_source``); doing the
    # same to destination keeps the two sides symmetric. See PR #1107
    # review.
    try:
        destination = os.path.realpath(os.path.normpath(str(params.destination)))
    except OSError:
        destination = os.path.normpath(str(params.destination))

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

    def _emit(phase, current, total, current_file=""):
        job["progress"]["current"] = current
        job["progress"]["total"] = total
        job["progress"]["current_file"] = current_file
        runner.update_step(
            job["id"], "import",
            current_file=current_file,
            progress={"current": current, "total": total},
        )
        runner.push_event(job["id"], "progress", {
            "phase": phase,
            "current": current,
            "total": total,
            "current_file": current_file,
        })

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
    copied = 0
    verified = 0
    skipped_duplicate = 0
    failed = 0
    unsafe_files = []          # [{path, reason}] — failed copies etc.
    folder_counts = {}         # rel folder -> counts for the PR 3 UI
    emitted = 0
    cancelled = False

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
        dest_path = entry[0]
        origin = entry[3]
        if origin == "copied":
            copied -= 1
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
                _fail(
                    rel, source_file,
                    "destination folder resolves inside a source directory "
                    "(dest_folder would be created under the card being "
                    "imported); formatting the card would erase the archive "
                    "copy",
                )
            continue
        os.makedirs(dest_folder, exist_ok=True)

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

        # (dest_path, verified_hash, card_source) for this batch's
        # landed files — fresh copies plus byte-identical files already
        # present at the destination (the crash-recovery path). The card
        # source feeds working-copy extraction so it reads local card
        # bytes, never the just-written archive copy.
        landed = []
        dup_dirs = set()

        for source_file in batch:
            if runner.is_cancelled(job["id"]):
                cancelled = True
                break
            emitted += 1
            _emit(
                f"{rel}: importing", emitted, discovered, source_file.name,
            )

            # Duplicate gate.
            if checker is not None:
                try:
                    token = checker.match(source_file)
                except OSError as e:
                    _fail(rel, source_file, f"duplicate check failed: {e}")
                    continue
                if token is not None:
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
                                twin_hash = compute_file_hash(twin_path)
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
                    if accept:
                        skipped_duplicate += 1
                        _counts(rel)["skipped_duplicate"] += 1
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
                        skipped_duplicate += 1
                        _counts(rel)["skipped_duplicate"] += 1
                        dup_dirs.add(dest_folder)
                        continue
                    if src_size == dest_size:
                        dest_hash = compute_file_hash(dest_file)
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
                                cand_hash = compute_file_hash(candidate)
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
                        (dest_file, adopt_hash, str(source_file),
                         "skipped_duplicate",
                         src_size, src_mtime_ns),
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
                (dest_file, file_hash, str(source_file), "copied",
                 src_size, src_mtime_ns),
            )
            _record_checker(source_file, dest_folder, file_hash)

        # --- Catalog this batch (even when cancelled mid-batch: what
        # landed on disk must be cataloged before we stop, so every
        # stopping point is a valid catalog state). Bounded by the batch
        # size, so no cancel_check is passed — it runs to completion.
        if landed:
            landed_paths = {entry[0] for entry in landed}
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
                dest_path = entry[0]
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
            # previous companion state. Skip when the JPEG was adopted
            # (``origin == "skipped_duplicate"``): its bytes were already
            # at the archive path before this run, so any RAW cache
            # derived from them is by construction consistent. See PR
            # #1107 review.
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
                    return compute_file_hash(path)
                except OSError:
                    return None

            # Stamp the verified hashes in the integrity-audit vocabulary,
            # cross-checked against what scan() stored.
            for entry in landed:
                dest_path = entry[0]
                verified_hash = entry[1]
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
                        actual = _rehash_dest_or_none(dest_path)
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
                elif row["file_hash"] is None:
                    if verified_hash == EMPTY_FILE_SHA256:
                        # Zero-byte convention: EMPTY_FILE_SHA256 never
                        # lands in file_hash (it would collide with every
                        # other empty file). Status only.
                        db.update_photo_hash_check(
                            row["id"], "ok", commit=False,
                        )
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
                        actual = _rehash_dest_or_none(dest_path)
                        if actual is not None and actual == verified_hash:
                            db.update_photo_hash_check(
                                row["id"], "ok", file_hash=verified_hash,
                                commit=False,
                            )
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
            # differ from what was there pre-scan. The batch scan ran with
            # ``vireo_dir=None`` (per-batch WC extraction would race
            # RAW+JPEG pairing across batch boundaries), so scanner's own
            # ``_invalidate_derived_caches`` on content change was
            # bypassed. Do it here for the same set of rows the scanner
            # would have caught: existing rows whose new hash differs from
            # the pre-scan hash. Without this, imports that restore a
            # replaced-then-deleted archive file leave stale
            # ``working_copy_path``/thumb/preview files pointing at the
            # previous bytes, and the deferred end-of-run
            # ``_extract_working_copies`` skips rows whose
            # ``working_copy_path`` is already set — so the WC never
            # rebuilds against the new archive bytes. See PR #1107 review.
            invalidated_photo_ids = set()
            if params.vireo_dir:
                from scanner import _invalidate_derived_caches
                for entry in landed:
                    dest_path = entry[0]
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
                    verified_hash = entry[1]
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
                    dest_path = entry[0]
                    if dest_path in reclassified_landed_paths:
                        # Reclassified to failed by hash stamping above
                        # (missing row or archive-vs-copy hash mismatch).
                        # Skipping the card override lets the WC extractor
                        # fall back to whatever the archive currently
                        # holds — matching the catalog's view — instead
                        # of caching a WC of bytes the archive no longer
                        # has.
                        continue
                    src_path = entry[2]
                    exp_size = entry[4]
                    exp_mtime_ns = entry[5]
                    wc_source_paths[dest_path] = (
                        src_path, exp_size, exp_mtime_ns,
                    )
                wc_dest_folders.add(dest_folder)

        # --- Link duplicate-twin folders. Separate scan call WITHOUT
        # restrict_files: scan only creates/links folder rows for files it
        # discovers, so folding these dirs into the restricted call above
        # would link nothing for duplicate-only batches. No restrict on
        # files also means any uncataloged strays in these dirs get
        # self-healed into the catalog.
        new_dup_dirs = dup_dirs - linked_dup_dirs
        if new_dup_dirs:
            # Twin folders may be cataloged through a symlink alias while
            # ``destination`` is realpath-normalized above. ``_path_under_
            # destination`` accepts them via realpath, but scanner's
            # ``_ensure_folder`` walks parents *lexically* until they equal
            # the scan root — a restrict_dir ``/alias/2026-07-05`` scanned
            # under root ``/real/archive`` would recurse to ``/alias`` →
            # ``/`` → ``/`` … and hit Python's recursion limit before the
            # workspace_folders link is ever created. Split by whether the
            # twin's folder path sits lexically under destination: link
            # the alias-spelled ones directly (their folder row and the
            # twin's photos already exist — no self-heal scan needed for
            # workspace visibility), scan only the lexically-under-
            # destination ones. See PR #1107 review.
            lex_dup_dirs = set()
            alias_dup_dirs = set()
            for d in new_dup_dirs:
                d_cmp = (
                    d.casefold() if _dest_ci else d
                ).rstrip(os.sep)
                if (
                    d_cmp == _dest_root_norm
                    or d_cmp.startswith(_dest_root_norm + os.sep)
                ):
                    lex_dup_dirs.add(d)
                else:
                    alias_dup_dirs.add(d)

            # Mirror the fresh-batch ``dest_folder`` promotion (see the
            # UPDATE at the top of this loop): a twin folder that survived
            # ``_linkable_twin_dirs`` is real on disk (``os.path.isdir``
            # passed), and if its row is still ``'missing'`` — a
            # reattached archive drive whose row hasn't been refreshed —
            # ``scanner.scan()``'s success stamp only clears ``'partial'``.
            # Without promoting here, safe_to_format could go green while
            # the duplicate twin's folder stays ``'missing'`` and its
            # photos are filtered out of workspace queries. Preserve
            # ``'partial'`` (a real prior-scan signal). See PR #1107 review.
            db.conn.executemany(
                "UPDATE folders SET status = 'ok' "
                "WHERE path = ? AND status = 'missing'",
                [(d,) for d in sorted(new_dup_dirs)],
            )
            db.conn.commit()

            # Link alias-spelled twin folders directly to the active
            # workspace: the folder row and its photos already exist (they
            # were cataloged by whatever original scan used the alias
            # spelling), so we just need the ``workspace_folders`` entry.
            # Passing them through ``scan(root=destination)`` would blow
            # the stack in ``_ensure_folder`` as described above.
            for d in sorted(alias_dup_dirs):
                folder_row = db.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (d,),
                ).fetchone()
                if folder_row is None:
                    # Shouldn't happen — _linkable_twin_dirs pulled this
                    # from folders in the first place — but if it does,
                    # count it as a link failure so safe_to_format stays
                    # honest.
                    log.warning(
                        "Alias-spelled dup dir %s vanished from folders "
                        "between _linkable_twin_dirs and the direct "
                        "workspace link", d,
                    )
                    dup_link_failed = True
                    unsafe_files.append({
                        "path": d,
                        "reason": (
                            "duplicate-folder workspace link failed: "
                            "folder row not found"
                        ),
                    })
                    continue
                db.add_workspace_folder(
                    workspace_id, folder_row["id"], is_root=True,
                )
                linked_dup_dirs.add(d)
                _invalidate_new_images(db, d)
            db.conn.commit()

            if lex_dup_dirs:
                try:
                    # Same rationale as the fresh-batch scan above: pass
                    # ``vireo_dir`` / ``thumb_cache_dir`` so pairing keeps
                    # cache context, and defer per-batch WC extraction to
                    # the end-of-run pass. See PR #1107 review.
                    scan(
                        destination, db,
                        restrict_dirs=sorted(lex_dup_dirs),
                        vireo_dir=params.vireo_dir,
                        thumb_cache_dir=params.thumb_cache_dir,
                        skip_working_copies=True,
                        cancel_check=lambda: runner.is_cancelled(job["id"]),
                    )
                    linked_dup_dirs.update(lex_dup_dirs)
                    # Duplicate-link scan created/linked workspace_folders
                    # rows for each duplicate twin's folder; the same
                    # cached-diff staleness applies (see the fresh-batch
                    # scan branch above). Invalidate every touched dup dir.
                    for d in sorted(lex_dup_dirs):
                        _invalidate_new_images(db, d)
                except Exception as e:
                    # A duplicate-only batch's ONLY workspace-visibility
                    # step is this scan — swallowing the error would leave
                    # safe_to_format green while the imported duplicates
                    # are invisible in the active workspace. Record it and
                    # force safe_to_format false; the file(s) are on disk
                    # (import succeeded), but the operation as a whole is
                    # not safe.
                    #
                    # scanner.scan raises ``RuntimeError("scan cancelled")``
                    # at cancellation checkpoints when ``cancel_check``
                    # returns truthy. Match on BOTH the sentinel message
                    # AND the runner's cancelled state before routing to
                    # the cancellation branch — a bare RuntimeError catch
                    # would also swallow real runtime failures (a
                    # ``RecursionError`` inherits from RuntimeError, or a
                    # library-level RuntimeError bubbling out of the scan)
                    # and report the job as cancelled even though nothing
                    # was cancelled and the workspace-link step actually
                    # failed. Mirrors pipeline_job.py's convention. See
                    # PR #1107 review.
                    if (
                        isinstance(e, RuntimeError)
                        and str(e) == "scan cancelled"
                        and runner.is_cancelled(job["id"])
                    ):
                        cancelled = True
                    else:
                        log.exception(
                            "Linking duplicate-matched folders failed: %s",
                            sorted(lex_dup_dirs),
                        )
                        dup_link_failed = True
                        for d in sorted(lex_dup_dirs):
                            unsafe_files.append({
                                "path": d,
                                "reason": (
                                    f"duplicate-folder link scan failed: {e}"
                                ),
                            })

        _emit(
            f"{rel}: {_counts(rel)['copied']} copied · "
            f"{_counts(rel)['skipped_duplicate']} already present",
            emitted, discovered,
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
    runner.update_step(
        job["id"], "import",
        status="failed" if status == "failed" else "completed",
        summary=(
            f"{copied} copied, {skipped_duplicate} already present, "
            f"{failed} failed of {discovered} discovered"
        ),
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
    # duplicate-only batch's workspace-link scan succeeded (otherwise the
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
    safe_to_format = (
        not cancelled
        and failed == 0
        and not discovery_errors
        and not dup_link_failed
        and not partial_scope
        and (copied + skipped_duplicate) == discovered
    )
    result = {
        "discovered": discovered,
        "copied": copied,
        "verified": verified,
        "skipped_duplicate": skipped_duplicate,
        "failed": failed,
        "safe_to_format": safe_to_format,
        "unsafe_files": unsafe_files,
        "folders": folder_counts,
        "cancelled": cancelled,
        "discovery_errors": len(discovery_errors),
        # JobRunner's mixed-outcome convention: a run with any failed
        # file, unseen source subtree, or workspace-link scan failure is
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
