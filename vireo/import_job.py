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
import logging
import os
import shutil
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
    hash matches the source is promoted into ``dst`` (``os.replace``).
    On mismatch the temp copy is removed and any pre-existing ``dst`` is
    left untouched.

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
        os.replace(tmp, dst)
        tmp = None
        return (True, copied_hash)
    except OSError as e:
        log.warning("Copy failed for %s -> %s: %s", src, dst, e)
        return (False, None)
    finally:
        if tmp is not None:
            with contextlib.suppress(OSError):
                os.unlink(tmp)


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

    destination = str(params.destination)

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
    _emit("Discovering files", 0, 0)
    files = []
    for src in params.sources:
        files.extend(discover_source_files(
            src, params.file_types, recursive=params.recursive,
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
            try:
                if os.path.exists(dest_file):
                    src_size = source_file.stat().st_size
                    dest_size = os.path.getsize(dest_file)
                    if src_size == 0 and dest_size == 0:
                        # Zero-byte twin: identical by definition, but kept
                        # out of the duplicate-identity index (see ingest).
                        skipped_duplicate += 1
                        _counts(rel)["skipped_duplicate"] += 1
                        dup_dirs.add(dest_folder)
                        continue
                    if src_size == dest_size:
                        src_hash = (
                            checker.content_hash(source_file)
                            if checker is not None
                            else compute_file_hash(str(source_file))
                        )
                        dest_hash = compute_file_hash(dest_file)
                        if src_hash is not None and src_hash == dest_hash:
                            # Byte-identical file already at the destination
                            # (e.g. a previous run died between copy and
                            # catalog). Treat as landed: catalog + stamp it
                            # rather than skipping — this is the designed
                            # self-heal for crash-shaped interruptions.
                            skipped_duplicate += 1
                            _counts(rel)["skipped_duplicate"] += 1
                            landed.append(
                                (dest_file, src_hash, str(source_file)),
                            )
                            if checker is not None:
                                for tok in checker.record(source_file):
                                    run_dest_folders[tok] = dest_folder
                                    run_verified_hashes[tok] = src_hash
                            continue
                    # Different content, same name — numeric suffix.
                    stem, suffix = os.path.splitext(source_file.name)
                    counter = 1
                    while os.path.exists(dest_file):
                        dest_file = os.path.join(
                            dest_folder, f"{stem}_{counter}{suffix}",
                        )
                        counter += 1

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
            landed.append((dest_file, file_hash, str(source_file)))
            if checker is not None:
                for tok in checker.record(source_file):
                    run_dest_folders[tok] = dest_folder
                    run_verified_hashes[tok] = file_hash

        # --- Catalog this batch (even when cancelled mid-batch: what
        # landed on disk must be cataloged before we stop, so every
        # stopping point is a valid catalog state). Bounded by the batch
        # size, so no cancel_check is passed — it runs to completion.
        if landed:
            landed_paths = {p for p, _, _ in landed}
            try:
                scan(
                    destination, db,
                    restrict_dirs=[dest_folder],
                    restrict_files=landed_paths,
                    vireo_dir=None,
                )
            except Exception as e:  # scan failure fails the whole batch
                for path, _, _ in landed:
                    _fail(rel, path, f"catalog scan failed: {e}")
                landed = []

            # Stamp the verified hashes in the integrity-audit vocabulary,
            # cross-checked against what scan() stored.
            for dest_path, verified_hash, _src in landed:
                row = db.conn.execute(
                    """SELECT p.id, p.file_hash FROM photos p
                       JOIN folders f ON f.id = p.folder_id
                       WHERE f.path = ? AND p.filename = ?""",
                    (os.path.dirname(dest_path), os.path.basename(dest_path)),
                ).fetchone()
                if row is None:
                    # RAW+JPEG pairing merges the JPEG's photo row into the
                    # RAW primary (companion_path); the JPEG's bytes are
                    # verified and represented — that's a success, not a
                    # missing catalog row.
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
                        continue
                    _fail(rel, dest_path, "not cataloged after scan")
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
                        db.update_photo_hash_check(
                            row["id"], "ok", file_hash=verified_hash,
                            commit=False,
                        )
                else:
                    _fail(
                        rel, dest_path,
                        "destination changed between copy verification and "
                        "catalog scan (hash mismatch)",
                    )
            db.conn.commit()

            # Extract working copies reading the CARD copy (fast local
            # bytes) instead of the just-written archive copy. Scoped to
            # this batch's destination folder; scan() ran with
            # vireo_dir=None precisely so extraction happens here with
            # the card-side source mapping. Per-row failures mark the
            # photo for the scanner's later backfill and never fail the
            # import.
            if params.vireo_dir:
                from scanner import _extract_working_copies

                try:
                    _extract_working_copies(
                        db, params.vireo_dir,
                        scope=[(dest_folder, "exact")],
                        source_paths={
                            dest: src for dest, _, src in landed
                        },
                    )
                except Exception:
                    log.exception(
                        "Working-copy extraction failed for %s", dest_folder,
                    )

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
            except Exception:
                log.exception(
                    "Linking duplicate-matched folders failed: %s",
                    sorted(new_dup_dirs),
                )

        _emit(
            f"{rel}: {_counts(rel)['copied']} copied · "
            f"{_counts(rel)['skipped_duplicate']} already present",
            emitted, discovered,
        )

        if cancelled:
            break

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

    # Safe to format iff every discovered file reached a verified
    # terminal bucket: hash-verified fresh copy, or duplicate whose bytes
    # verifiably exist (hash-backed match, or key match re-hashed against
    # its cataloged twin). A cancelled run leaves unprocessed files, so it
    # is never safe. This pill means exactly what it says.
    safe_to_format = (
        not cancelled
        and failed == 0
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
        # JobRunner's mixed-outcome convention: a run with any failed file
        # is recorded "failed" (with per-file reasons), never "completed".
        "ok": failed == 0,
        "errors": [f"{u['path']}: {u['reason']}" for u in unsafe_files],
    }
    return result
