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
import shutil
import sys
import uuid
from dataclasses import dataclass

from db import Database
from import_dedup import (
    CatalogIndex,
    DuplicateChecker,
    compute_file_hash,
    stored_metadata_key,
)
from ingest import (
    _path_under_root,
    _source_file_timestamps,
    build_destination_path,
    discover_source_files,
)
from scanner import EMPTY_FILE_SHA256

log = logging.getLogger(__name__)

# Batch unit (Task 2.0 Q4): files sharing a destination folder, chunked so
# one scan call never covers more than this many fresh files. Copy, verify,
# scan, and hash stamping all commit at batch boundaries, so every stopping
# point (cancel, crash, yanked card) leaves a valid catalog.
IMPORT_BATCH_SIZE = 200


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
    # Vireo data dir for working-copy extraction (Task 2.5). None skips
    # extraction (tests, or callers that defer to the scanner backfill).
    vireo_dir: str | None = None


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
    EOPNOTSUPP), fall back to ``O_CREAT|O_EXCL`` to atomically claim the
    destination path as an empty placeholder, then ``os.replace`` the
    verified temp file over it. That preserves no-overwrite race
    protection without requiring hardlink support, so imports do not fail
    across every file on FAT-family archives or hardlinkless NAS shares.

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
            # ENOTSUP/EOPNOTSUPP; Windows exFAT can return EACCES). Fall
            # back to an atomic no-overwrite reserve-and-replace: O_EXCL
            # atomically claims ``dst`` as an empty placeholder (races
            # with concurrent imports fail here just like os.link would),
            # then os.replace atomically swaps the verified temp into
            # place. Without this fallback every file on hardlinkless
            # archives buckets as a copy failure and imports are
            # unusable on those destinations. See PR #1107 review.
            if not _fs_lacks_hardlinks(link_err):
                raise
            log.info(
                "os.link unsupported on %s (%s); using O_EXCL fallback",
                dst_dir, link_err,
            )
            try:
                fd = os.open(
                    dst,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o644,
                )
            except FileExistsError:
                log.warning(
                    "Destination raced during copy "
                    "(concurrent import?): %s",
                    dst,
                )
                return (False, None)
            os.close(fd)
            try:
                os.replace(tmp, dst)
            except OSError as rep_err:
                log.warning(
                    "Fallback promote failed for %s -> %s: %s",
                    src, dst, rep_err,
                )
                # Best-effort remove the empty placeholder we created.
                with contextlib.suppress(OSError):
                    os.unlink(dst)
                return (False, None)
            tmp = None
            return (True, copied_hash)
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


def _linkable_twin_dirs(rows, destination):
    """Destination-scoped folders holding a duplicate's cataloged twin.

    Only folders under the import destination are scanned/linked after a
    duplicate skip (a twin in some other library root is none of this
    import's business). Mirrors ingest()'s dup_token_folders guards:
    folder status ok/partial, path under destination, and still a real
    directory on disk.
    """
    dirs = set()
    for r in rows:
        folder_path = r["folder_path"]
        if r["folder_status"] not in ("ok", "partial"):
            continue
        if not _path_under_root(folder_path, destination):
            continue
        if not os.path.isdir(folder_path):
            continue
        dirs.add(folder_path)
    return dirs


def run_import_job(job, runner, db_path, workspace_id, params):
    """Copy card(s) -> archive, hash-verify, and catalog incrementally.

    Returns the result dict (counts + per-folder breakdown). The catalog
    is committed per batch; cancellation and crashes leave every already-
    verified file cataloged and nothing else.
    """
    from scanner import scan

    db = Database(db_path)
    db.set_active_workspace(workspace_id)

    # Normalize once — the raw destination string is passed as ``root`` to
    # both the copy layout (``os.path.normpath(os.path.join(destination,
    # rel))``) and to ``scan(root, …, restrict_dirs=[dest_folder])``.
    # ``scanner._ensure_folder`` stops walking the folder chain when the
    # parent equals the scan root string; a destination like
    # ``/photos/tmp/../archive`` copies into the normalized
    # ``/photos/archive/…`` but the restricted scan root would remain the
    # dot-segment form, so the recursion never reaches root and the scan
    # loses those files (copied bytes then bucket as catalog failures).
    destination = os.path.normpath(str(params.destination))

    # Normalized realpaths of every import source root, used to reject
    # cataloged twins that live under the card being imported. The
    # /api/jobs/import-photos route already refuses destinations that sit
    # inside a source (formatting the card would erase the archive copy),
    # but the duplicate acceptance loop separately trusts any cataloged
    # twin whose bytes hash to ``src_hash`` — including a stale row for a
    # previously scanned mounted card. That twin's re-hash just re-reads
    # the very card file being imported, so accepting it as duplicate
    # proof would flip ``safe_to_format`` green over a card whose bytes
    # never made it to the archive. Case-fold on darwin/win32 so a
    # differently-cased spelling of the source path can't slip a twin
    # through the containment check; ext4/xfs on Linux really do
    # distinguish case. See PR #1107 review.
    _case_insensitive_platform = sys.platform in ("darwin", "win32")

    def _casenorm_path(p):
        return p.casefold() if _case_insensitive_platform else p

    def _norm_source(s):
        try:
            real = os.path.realpath(s)
        except OSError:
            real = str(s)
        return _casenorm_path(real).rstrip(os.sep)

    source_roots = [_norm_source(s) for s in params.sources]

    def _path_under_any_source(path):
        try:
            real = os.path.realpath(path)
        except OSError:
            real = str(path)
        cmp = _casenorm_path(real)
        for root in source_roots:
            if not root:
                continue
            if cmp == root or cmp.startswith(root + os.sep):
                return True
        return False

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
        os.makedirs(dest_folder, exist_ok=True)

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
                                break
                    if accept:
                        skipped_duplicate += 1
                        _counts(rel)["skipped_duplicate"] += 1
                        dup_dirs.update(
                            _linkable_twin_dirs(twin_rows, destination),
                        )
                        run_dest = run_dest_folders.get(token)
                        if run_dest is not None:
                            dup_dirs.add(run_dest)
                        continue
                    # No byte-identical twin remains on disk — the card
                    # file is a distinct photo; import it normally.

            # Destination path + collision handling (mirrors ingest()).
            dest_file = os.path.join(dest_folder, source_file.name)
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

                def _src_hash_cached():
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
            try:
                scan(
                    destination, db,
                    restrict_dirs=[dest_folder],
                    restrict_files=landed_paths,
                    vireo_dir=None,
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
            db.conn.commit()

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
            try:
                scan(
                    destination, db,
                    restrict_dirs=sorted(new_dup_dirs),
                    vireo_dir=None,
                    cancel_check=lambda: runner.is_cancelled(job["id"]),
                )
                linked_dup_dirs.update(new_dup_dirs)
            except RuntimeError:
                cancelled = True
            except Exception as e:
                # A duplicate-only batch's ONLY workspace-visibility step
                # is this scan — swallowing the error would leave
                # safe_to_format green while the imported duplicates are
                # invisible in the active workspace. Record it and force
                # safe_to_format false; the file(s) are on disk (import
                # succeeded), but the operation as a whole is not safe.
                log.exception(
                    "Linking duplicate-matched folders failed: %s",
                    sorted(new_dup_dirs),
                )
                dup_link_failed = True
                for d in sorted(new_dup_dirs):
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
    if params.vireo_dir and wc_dest_folders:
        from scanner import _extract_working_copies

        try:
            _extract_working_copies(
                db, params.vireo_dir,
                scope=[(d, "exact") for d in sorted(wc_dest_folders)],
                source_paths=wc_source_paths,
            )
        except Exception:
            log.exception(
                "Working-copy extraction failed for %s",
                sorted(wc_dest_folders),
            )

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
    # imported duplicates are on disk but not visible in the workspace).
    # A cancelled run leaves unprocessed files, so it is never safe.
    # This pill means exactly what it says.
    safe_to_format = (
        not cancelled
        and failed == 0
        and not discovery_errors
        and not dup_link_failed
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
