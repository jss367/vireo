"""Audit system: detect drift, orphans, untracked files, stray sidecars,
and silent file corruption (bit rot)."""

import logging
import os

from image_loader import (
    SUPPORTED_EXTENSIONS,
    is_excluded_scan_path,
    safe_scan_walk,
)
from xmp import read_keywords

log = logging.getLogger(__name__)

# Every check the summary banner aggregates. The banner only shows the
# green "archive intact" light when ALL of these have run and found
# nothing — a check that never ran is reported as unverified, not clean.
AUDIT_CHECKS = ("drift", "orphans", "untracked", "sidecars", "integrity")


def check_drift(db):
    """Find photos where DB and XMP sidecar disagree.

    Checks both directions:
    - XMP modified externally (keywords in XMP not in DB)
    - DB modified by Vireo (keywords in DB not in XMP, pending sync)

    Returns:
        list of {photo_id, filename, folder_path, field, db_value, xmp_value,
                 added_in_xmp, removed_in_xmp, direction}
    """
    photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    drifts = []

    for photo in photos:
        folder_path = folders.get(photo["folder_id"], "")
        base = os.path.splitext(photo["filename"])[0]
        xmp_path = os.path.join(folder_path, base + ".xmp")

        db_keywords = {k["name"] for k in db.get_photo_keywords(photo["id"])}

        if not os.path.exists(xmp_path):
            # No XMP file — if DB has keywords, that's a pending sync
            if db_keywords:
                drifts.append(
                    {
                        "photo_id": photo["id"],
                        "filename": photo["filename"],
                        "folder_path": folder_path,
                        "field": "keywords",
                        "db_value": sorted(db_keywords),
                        "xmp_value": [],
                        "added_in_xmp": [],
                        "removed_in_xmp": sorted(db_keywords),
                        "direction": "db_ahead",
                    }
                )
            continue

        xmp_keywords = read_keywords(xmp_path)

        # Compare case-insensitively: resolve_drift('use_xmp') reconciles
        # via sync_from_xmp, which treats keywords differing only by case as
        # already in sync, so a case-only difference reported here would be
        # permanently unresolvable. Reported values keep the actual strings.
        db_by_lower = {k.lower(): k for k in db_keywords}
        xmp_by_lower = {k.lower(): k for k in xmp_keywords}

        if set(xmp_by_lower) != set(db_by_lower):
            added_in_xmp = {
                kw for low, kw in xmp_by_lower.items() if low not in db_by_lower
            }
            removed_in_xmp = {
                kw for low, kw in db_by_lower.items() if low not in xmp_by_lower
            }

            # Determine direction
            if added_in_xmp and not removed_in_xmp:
                direction = "xmp_ahead"
            elif removed_in_xmp and not added_in_xmp:
                direction = "db_ahead"
            else:
                direction = "both"

            drifts.append(
                {
                    "photo_id": photo["id"],
                    "filename": photo["filename"],
                    "folder_path": folder_path,
                    "field": "keywords",
                    "db_value": sorted(db_keywords),
                    "xmp_value": sorted(xmp_keywords),
                    "added_in_xmp": sorted(added_in_xmp),
                    "removed_in_xmp": sorted(removed_in_xmp),
                    "direction": direction,
                }
            )

    log.info("Drift check: %d discrepancies found", len(drifts))
    return drifts


def check_orphans(db):
    """Find DB entries where the file no longer exists on disk.

    Returns:
        list of {photo_id, filename, folder_path}
    """
    photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    orphans = []

    for photo in photos:
        folder_path = folders.get(photo["folder_id"], "")
        file_path = os.path.join(folder_path, photo["filename"])

        if not os.path.exists(file_path):
            orphans.append(
                {
                    "photo_id": photo["id"],
                    "filename": photo["filename"],
                    "folder_path": folder_path,
                }
            )

    log.info("Orphan check: %d orphaned entries found", len(orphans))
    return orphans


def check_untracked(db, root_paths):
    """Find files on disk not in the database.

    Args:
        db: Database instance
        root_paths: list of root directory paths to scan

    Returns:
        list of {path, folder}
    """
    # Build set of known file paths
    photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    known_paths = set()
    for photo in photos:
        folder_path = folders.get(photo["folder_id"], "")
        known_paths.add(os.path.join(folder_path, photo["filename"]))

    untracked = []
    for root in root_paths:
        # prune_scan_dirs filters only children; if the root is, or sits
        # inside, an excluded bundle (e.g. a stale folder row pointing at
        # ``.../Photos Library.photoslibrary/originals``), os.walk would
        # still open it. Reject the whole subtree before we touch it so
        # the audit can't trip the macOS TCC prompt either. This must run
        # BEFORE ``os.path.isdir`` — isdir follows symlinks and stat's the
        # target, so for a directly selected bundle (or a symlink to one)
        # the existence test alone is enough to trip TCC.
        if is_excluded_scan_path(root):
            continue
        if not os.path.isdir(root):
            continue
        # safe_scan_walk replaces os.walk + prune_scan_dirs so we never
        # stat-follow a symlinked excluded bundle (e.g. a child like
        # ``LibraryAlias -> Photos Library.photoslibrary``) — the os.walk
        # classification call alone would re-trip the macOS TCC prompt.
        # rglob offered no way to stop descending; this walker does.
        for dirpath, _dirnames, filenames in safe_scan_walk(root):
            for name in filenames:
                if name.startswith(".") or os.path.splitext(name)[1].lower() not in SUPPORTED_EXTENSIONS:
                    continue
                full = os.path.join(dirpath, name)
                if full in known_paths:
                    continue
                # safe_scan_walk surfaces dangling symlinks (and other
                # non-regular entries) in ``filenames``; the prior
                # Path.rglob path gated on f.is_file(). scanner.scan skips
                # non-files via os.path.isfile, so flagging one here would
                # raise an untracked-image warning a rescan can never
                # clear. os.path.isfile follows symlinks and returns False
                # (not raise) for dangling targets.
                if not os.path.isfile(full):
                    continue
                untracked.append({"path": full, "folder": dirpath})

    log.info("Untracked check: %d untracked files found", len(untracked))
    return untracked


def check_stray_sidecars(root_paths):
    """Find .xmp sidecar files with no corresponding image file on disk.

    Matches both sidecar naming styles: ``bird.xmp`` next to ``bird.jpg``
    (Vireo/Lightroom) and ``bird.jpg.xmp`` (darktable). A sidecar whose
    image exists but isn't in the DB is the untracked check's problem,
    not a stray — import would re-attach it. Comparison is
    case-insensitive so ``BIRD.JPG`` matches ``bird.xmp``.

    Returns:
        list of {path, folder}
    """
    strays = []
    for root in root_paths:
        # Reject excluded bundles before ``os.path.isdir`` — isdir follows
        # symlinks and stat's the target, which is enough to trip the macOS
        # TCC prompt for a directly selected bundle or a symlink to one.
        if is_excluded_scan_path(root):
            continue
        if not os.path.isdir(root):
            continue
        # See check_untracked_files for why safe_scan_walk supersedes
        # os.walk + prune_scan_dirs here.
        for dirpath, _dirnames, filenames in safe_scan_walk(root):
            image_names = set()
            xmps = []
            for name in filenames:
                if name.startswith("."):
                    continue
                # safe_scan_walk classifies entries with follow_symlinks=False,
                # so a symlink to a directory named like an image/sidecar (e.g.
                # ``Albums/ghost.xmp -> RealAlbum``) lands in ``filenames``
                # instead of ``dirnames``. Without this guard the loop would
                # treat the link as a real sidecar or image — bogus stray
                # reports plus a delete that would unlink a real directory.
                # Matches the os.path.isfile guard in check_untracked_files.
                if not os.path.isfile(os.path.join(dirpath, name)):
                    continue
                stem, ext = os.path.splitext(name)
                if ext.lower() == ".xmp":
                    xmps.append(name)
                elif ext.lower() in SUPPORTED_EXTENSIONS:
                    # Both forms so "bird.xmp" and "bird.jpg.xmp" match
                    image_names.add(name.lower())
                    image_names.add(stem.lower())
            for x in xmps:
                base = os.path.splitext(x)[0].lower()
                if base not in image_names:
                    strays.append(
                        {"path": os.path.join(dirpath, x), "folder": dirpath}
                    )

    log.info("Stray sidecar check: %d stray sidecars found", len(strays))
    return strays


def _sidecar_has_image(xmp_path):
    """True if any image file next to ``xmp_path`` matches its base name."""
    dirpath = os.path.dirname(xmp_path)
    base = os.path.splitext(os.path.basename(xmp_path))[0].lower()
    try:
        names = os.listdir(dirpath)
    except OSError:
        return False
    for name in names:
        stem, ext = os.path.splitext(name)
        if ext.lower() in SUPPORTED_EXTENSIONS and (
            name.lower() == base or stem.lower() == base
        ):
            return True
    return False


def _is_under_roots(path, real_roots):
    """True if ``path`` resolves to a location inside one of the roots.

    ``real_roots`` must already be realpath'd. The trailing-separator
    prefix check prevents ``/roota-evil`` from matching root ``/roota``.
    """
    real = os.path.realpath(path)
    return any(
        real.startswith(root.rstrip(os.sep) + os.sep) for root in real_roots
    )


def delete_stray_sidecars(paths, allowed_roots):
    """Delete sidecar files, re-verifying each is still a stray.

    Each path must end in .xmp and must still have no matching image
    file beside it at deletion time — the list the client holds may be
    stale (the user could have restored the photo since the check ran),
    and a sidecar with a living image is data, not litter.

    ``allowed_roots`` confines deletions to the audited folders: the
    client-supplied list is untrusted input, so any path that does not
    resolve to a location under one of the roots (the same workspace
    root folders the sidecars check scans) is refused. Both sides are
    realpath'd so symlinks can't smuggle a path outside the library.

    Returns the number of files actually deleted.
    """
    real_roots = [os.path.realpath(r) for r in allowed_roots]
    deleted = 0
    for p in paths:
        if os.path.splitext(p)[1].lower() != ".xmp":
            continue
        if not _is_under_roots(p, real_roots):
            log.warning(
                "Refusing to delete sidecar outside library roots: %s", p
            )
            continue
        if not os.path.isfile(p):
            continue
        if _sidecar_has_image(p):
            continue
        try:
            os.unlink(p)
            deleted += 1
        except OSError:
            log.exception("Failed to delete stray sidecar %s", p)
    log.info("Deleted %d stray sidecars", deleted)
    return deleted


def verify_hashes(db, progress_cb=None, should_cancel=None):
    """Re-hash every workspace photo and compare against the stored SHA-256.

    Verdicts per photo (stored in photos.hash_status):
    - ``ok``: content matches the stored hash.
    - ``modified``: content differs AND the file's mtime moved — the file
      was edited outside Vireo since the last scan; a rescan refreshes it.
    - ``corrupt``: content differs but the mtime is unchanged — nothing
      legitimately wrote the file, which is the bit-rot signature.
      Restore from backup, then re-verify or accept.
    - ``unreadable``: the file exists but could not be read.

    Photos with no stored hash (imported before hashing existed) are
    baselined: the current content hash is stored and counted separately
    so the run summary doesn't claim they were "verified" against history.
    Missing files are not hashed — they're the orphans check's territory,
    and since this walk covers exactly the population and existence
    predicate check_orphans uses, a completed run also records the
    orphans result. Without that, deleting a file and re-running only
    this check would leave a stale clean orphans verdict standing and
    the summary could go green right after the verifier saw a missing
    file.

    Args:
        db: Database instance (workspace must be active)
        progress_cb: optional callable(current, total, filename)
        should_cancel: optional callable() -> bool, checked per file

    Returns stats dict: {checked, ok, baselined, modified, corrupt,
    unreadable, missing, cancelled}
    """
    from scanner import compute_file_hash

    photos = db.get_integrity_photos()
    total = len(photos)
    stats = {
        "checked": 0, "ok": 0, "baselined": 0, "modified": 0,
        "corrupt": 0, "unreadable": 0, "missing": 0, "cancelled": False,
    }

    for i, photo in enumerate(photos):
        if should_cancel and should_cancel():
            stats["cancelled"] = True
            break
        if progress_cb:
            progress_cb(i + 1, total, photo["filename"])

        path = os.path.join(photo["folder_path"], photo["filename"])
        if not os.path.exists(path):
            stats["missing"] += 1
            continue

        try:
            actual = compute_file_hash(path)
        except OSError:
            db.update_photo_hash_check(photo["id"], "unreadable",
                                       commit=False)
            stats["checked"] += 1
            stats["unreadable"] += 1
            continue

        stats["checked"] += 1
        if not photo["file_hash"]:
            db.update_photo_hash_check(photo["id"], "ok", file_hash=actual,
                                       commit=False)
            stats["baselined"] += 1
        elif actual == photo["file_hash"]:
            db.update_photo_hash_check(photo["id"], "ok", commit=False)
            stats["ok"] += 1
        else:
            try:
                disk_mtime = os.path.getmtime(path)
            except OSError:
                disk_mtime = None
            db_mtime = photo["file_mtime"]
            # 1s tolerance: FAT/exFAT mtimes have 2s resolution and copies
            # can round; a sub-second wobble is not evidence of an edit.
            if (
                disk_mtime is not None
                and db_mtime is not None
                and abs(disk_mtime - db_mtime) > 1.0
            ):
                status = "modified"
            else:
                status = "corrupt"
            db.update_photo_hash_check(photo["id"], status, commit=False)
            stats[status] += 1

        if (i + 1) % 100 == 0:
            db.conn.commit()

    db.conn.commit()

    # A cancelled run verified only a prefix of the library — recording it
    # would let the summary banner claim coverage that doesn't exist.
    if not stats["cancelled"]:
        problems = stats["modified"] + stats["corrupt"] + stats["unreadable"]
        db.record_audit_run("integrity", problems)
        # This walk just applied the orphans check's exact predicate to
        # its exact population, so record that result too. Missing files
        # stay under the orphans check (where the UI offers the right
        # remediation) instead of inflating the integrity count, and a
        # stale clean orphans verdict can't keep the banner green after
        # this run saw a missing file.
        db.record_audit_run("orphans", stats["missing"])

    log.info(
        "Hash verification: %d checked, %d ok, %d baselined, %d modified, "
        "%d corrupt, %d unreadable, %d missing%s",
        stats["checked"], stats["ok"], stats["baselined"], stats["modified"],
        stats["corrupt"], stats["unreadable"], stats["missing"],
        " (cancelled)" if stats["cancelled"] else "",
    )
    return stats


def check_integrity(db):
    """Return the current integrity state without re-hashing anything.

    Reads the verdicts stored by the last verify_hashes run plus coverage
    stats, so the UI can show flagged files (and how stale the check is)
    without paying for a full re-hash.
    """
    return {
        "flagged": db.get_integrity_flagged(),
        "stats": db.get_integrity_stats(),
    }


def accept_current_hash(db, photo_ids):
    """Accept a file's current content as the new baseline hash.

    For files flagged 'modified' (or 'corrupt' if the user decides the
    change is legitimate): re-hash from disk, store as the new baseline,
    and clear the flag. The DB's file_mtime is left alone so the scanner's
    own change detection still reprocesses the file on the next scan.

    Returns the number of photos updated.
    """
    from scanner import compute_file_hash

    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    accepted = 0
    for pid in photo_ids:
        photo = db.get_photo(pid)
        if not photo:
            continue
        folder_path = folders.get(photo["folder_id"], "")
        path = os.path.join(folder_path, photo["filename"])
        try:
            new_hash = compute_file_hash(path)
        except OSError:
            log.warning("Cannot accept hash for unreadable file %s", path)
            continue
        db.update_photo_hash_check(pid, "ok", file_hash=new_hash)
        accepted += 1
    log.info("Accepted current hash for %d photos", accepted)
    return accepted


def build_summary(db):
    """Aggregate every audit check into one archive-integrity verdict.

    Statuses:
    - ``intact``: all checks ran, none found problems, and every photo
      has been hash-verified at least once.
    - ``stale``: checks are clean but some photos (e.g. new imports)
      have never been hash-verified.
    - ``problems``: at least one check found something, or a workspace
      folder is marked missing.
    - ``unverified``: at least one check has never run.

    Missing folders force ``problems`` regardless of check results:
    every scoped query the checks run on excludes folders flagged
    ``missing``, so without this an entire offline folder of photos
    would silently drop out of every check and the banner could show
    green over a gone library. The payload carries ``missing_folders``
    and ``missing_folder_photos`` so the UI can say exactly that.

    Per-check entries carry ran_at so the UI can show how old each
    verdict is — the green light means "verified, at these times", never
    "no evidence of problems".
    """
    runs = db.get_audit_runs()
    stats = db.get_integrity_stats()
    missing_folders = db.get_missing_folders()

    checks = {}
    for name in ("drift", "orphans", "untracked", "sidecars"):
        checks[name] = runs.get(name)

    integrity_run = runs.get("integrity")
    if integrity_run:
        # problem_count comes live from the photos table, not the recorded
        # row, so accepting a hash updates the banner without a re-run.
        checks["integrity"] = {
            "ran_at": integrity_run["ran_at"],
            "problem_count": stats["flagged"],
        }
    else:
        checks["integrity"] = None

    ran = [c for c in checks.values() if c is not None]
    problem_count = sum(c["problem_count"] for c in ran)

    if missing_folders:
        # A missing folder is direct evidence of a problem, even when
        # checks haven't all run — it outranks "unverified".
        status = "problems"
    elif len(ran) < len(checks):
        status = "unverified"
    elif problem_count > 0:
        status = "problems"
    elif stats["unchecked"] > 0:
        status = "stale"
    else:
        status = "intact"

    return {
        "status": status,
        "problem_count": problem_count,
        "missing_folders": len(missing_folders),
        "missing_folder_photos": sum(
            f["photo_count"] for f in missing_folders
        ),
        "checks": checks,
        "integrity": stats,
    }


def resolve_drift(db, photo_id, direction):
    """Resolve a drift for a single photo.

    Args:
        db: Database instance
        photo_id: photo to resolve
        direction: 'use_db' queues XMP write, 'use_xmp' updates DB from XMP
    """
    if direction == "use_db":
        # Queue all current DB keywords as pending writes
        keywords = db.get_photo_keywords(photo_id)
        for kw in keywords:
            db.queue_change(photo_id, "keyword_add", kw["name"])
    elif direction == "use_xmp":
        from sync import sync_from_xmp

        sync_from_xmp(db, [photo_id])


def remove_orphans(db, photo_ids):
    """Delete DB entries for orphaned photos.

    Args:
        db: Database instance
        photo_ids: list of photo ids to remove
    """
    for pid in photo_ids:
        db.conn.execute("DELETE FROM photo_keywords WHERE photo_id = ?", (pid,))
        db.conn.execute("DELETE FROM pending_changes WHERE photo_id = ?", (pid,))
        db.conn.execute("DELETE FROM photos WHERE id = ?", (pid,))
    db.conn.commit()
    db.update_folder_counts()
    log.info("Removed %d orphan entries", len(photo_ids))


def import_untracked(db, paths, vireo_dir=None, thumb_cache_dir=None):
    """Import untracked files into the database by scanning them.

    Args:
        db: Database instance
        paths: list of file paths to import
        vireo_dir: path to the vireo data directory (parent of
            ``working/`` and ``previews/``). Required for derived-cache
            invalidation and working-copy extraction to fire — scanner
            can't guess it because ``--db`` and ``--thumb-dir`` are
            independently configurable. When omitted, a rescan that
            detects a content change will leave stale caches in place.
        thumb_cache_dir: configured thumbnail cache directory. Forwarded
            to the scanner so invalidation targets the real cache even
            when ``--thumb-dir`` points outside ``vireo_dir/thumbnails``.
    """
    from new_images import invalidate_new_images_after_scan
    from scanner import scan

    # Group by parent directory
    dirs = set(os.path.dirname(p) for p in paths)
    for d in dirs:
        try:
            scan(d, db, incremental=True,
                 vireo_dir=vireo_dir,
                 thumb_cache_dir=thumb_cache_dir)
        finally:
            # scanner.scan commits photo rows incrementally, so even a
            # mid-scan failure can leave DB state that invalidates cached
            # new-image counts. Mirrors the try/finally in pipeline_job
            # and the api_job_scan / api_job_import_full handlers.
            try:
                invalidate_new_images_after_scan(db, d)
            except Exception:
                log.exception(
                    "Failed to invalidate new-images cache for %s", d
                )
