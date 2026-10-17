"""Local original-file cache for offline viewing."""

import contextlib
import os
import shutil
import sqlite3
import stat
import tempfile
import threading
import time
import weakref

from file_replace import replace_file

# Weak values release idle locks without losing a lock held by a waiter.
_original_locks = weakref.WeakValueDictionary()
_original_locks_guard = threading.Lock()


def original_preparation_guard(vireo_dir, photo_id):
    """Serialize a photo's preparation and offline-cache writes through cleanup.

    A database transaction alone cannot guard files published before their
    cache row is inserted. Reentrant so a preparation job can hold this across
    copying, rendering, and cleanup while cache_photo_original uses it too.
    """
    key = (os.path.realpath(vireo_dir), int(photo_id))
    with _original_locks_guard:
        lock = _original_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            _original_locks[key] = lock
        return lock


def photo_source_matches(selected, current):
    """Whether a catalog row still describes the selected source asset."""
    return selected is not None and current is not None and all(
        selected[key] == current[key]
        for key in ("folder_id", "filename", "file_size", "file_mtime", "companion_path")
    )


def cleanup_preparation_offline_files(vireo_dir, photo_id):
    """Remove offline assets while holding original_preparation_guard.

    Unlike full photo-deletion cleanup, this never touches artifacts owned
    by independent thumbnail, preview, mask, or interactive render producers.
    """
    import glob

    for family in ("originals", "xmp", "companions"):
        for path in glob.glob(os.path.join(vireo_dir, "offline", family, f"{photo_id}.*")):
            _unlink_cached_rel(vireo_dir, os.path.relpath(path, vireo_dir))


def _copy_atomic(src, dst):
    """Copy ``src`` to ``dst`` via a unique sibling temp file."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        dir=os.path.dirname(dst),
        prefix=os.path.basename(dst) + ".",
        suffix=".tmp",
    )
    os.close(fd)
    try:
        shutil.copy2(src, tmp)
        replace_file(tmp, dst)
    finally:
        if os.path.exists(tmp):
            with contextlib.suppress(OSError):
                os.unlink(tmp)


def _offline_rel_path(kind, photo_id, suffix):
    return os.path.join("offline", kind, f"{int(photo_id)}{suffix}")


def _xmp_source_for(folder_path, filename):
    # Match both cases — other code paths (see app.py sidecar cleanup)
    # treat `.XMP` as a valid sidecar, so case-sensitive filesystems would
    # otherwise drop uppercase sidecars from the offline cache.
    stem = os.path.splitext(filename)[0]
    lower = os.path.join(folder_path, stem + ".xmp")
    upper = os.path.join(folder_path, stem + ".XMP")
    if os.path.isfile(upper) and not os.path.isfile(lower):
        return upper
    return lower


def _same_file_stat(src, dst):
    if not src or not dst:
        return False
    try:
        src_st = os.stat(src)
        dst_st = os.stat(dst)
    except OSError:
        return False
    return src_st.st_size == dst_st.st_size and src_st.st_mtime == dst_st.st_mtime


def _unlink_cached_rel(vireo_dir, rel_path):
    if not rel_path:
        return
    with contextlib.suppress(OSError):
        os.unlink(os.path.join(vireo_dir, rel_path))


def offline_original_abs(vireo_dir, row):
    if not row or not row["original_path"]:
        return None
    return os.path.join(vireo_dir, row["original_path"])


def cached_original_for_photo(db, photo_id, vireo_dir):
    """Return the cached original path if present on disk, else None."""
    row = db.caches.offline_original_get(photo_id)
    path = offline_original_abs(vireo_dir, row)
    if path and os.path.isfile(path):
        return path
    return None


def resolve_original_path(
    db, photo, vireo_dir, folders, *, prefer_cached=False,
):
    """Return (path, used_offline_cache) for a photo's original.

    The source-of-truth file wins when it is available unless ``prefer_cached``
    is true and the managed copy still matches the catalogued source size and
    mtime. Full-resolution inspection uses that opt-in so a photo explicitly
    prepared by the user is read from local storage instead of returning to a
    slow removable or network volume. Other callers retain the historical
    source-first behavior.
    """
    folder_id = photo["folder_id"]
    offline_row = db.caches.offline_original_get(photo["id"])
    cached = offline_original_abs(vireo_dir, offline_row)
    source_path = (
        os.path.join(folders[folder_id], photo["filename"])
        if folder_id in folders
        else ""
    )
    source_is_available = os.path.isfile(source_path)
    primary_is_current = (
        _same_file_stat(source_path, cached)
        if source_is_available
        else bool(
            offline_row
            and offline_row["source_size"] == (photo["file_size"] or 0)
            and offline_row["source_mtime"] == photo["file_mtime"]
        )
    )
    companion_is_current = True
    if photo["companion_path"] and folder_id in folders:
        source_original = os.path.join(
            folders[folder_id], photo["filename"],
        )
        source_companion = os.path.join(
            folders[folder_id], photo["companion_path"],
        )
        if os.path.isfile(source_original):
            cached_companion = (
                os.path.join(vireo_dir, offline_row["companion_path"])
                if offline_row and offline_row["companion_path"]
                else None
            )
            companion_is_current = _same_file_stat(
                source_companion, cached_companion,
            )
    cached_is_current = bool(
        offline_row
        and offline_row["status"] == "cached"
        and cached
        and os.path.isfile(cached)
        and primary_is_current
        and companion_is_current
    )
    if prefer_cached and cached_is_current:
        return cached, True
    if source_is_available:
        return source_path, False
    if cached and os.path.isfile(cached):
        return cached, True
    return source_path, False


def cache_photo_original(db, photo, vireo_dir, folders):
    """Copy one photo's original and sidecar metadata into the offline cache."""
    with original_preparation_guard(vireo_dir, photo["id"]):
        return _cache_photo_original(db, photo, vireo_dir, folders)


def _releasable_cached_paths(base, photo_id, photo, folder, row):
    """Return ``[(cached_path, size)]`` safe to unlink for one photo.

    Raises ``ValueError`` (or ``OSError``) naming why a managed copy must be
    kept: a path outside the managed cache, a source that is not an
    independent regular file, or a source and cache that no longer match.
    ``base`` is the resolved Vireo data directory.
    """
    paths = []
    # Sidecars are snapshots, not edit targets. A newer source XMP is
    # allowed, but a missing source sidecar must not lose its last copy.
    sources = {
        "originals": os.path.join(folder["path"], photo["filename"]),
        "xmp": _xmp_source_for(folder["path"], photo["filename"]),
        "companions": os.path.join(folder["path"], photo["companion_path"])
        if photo["companion_path"] else None,
    }
    for family, column in (("xmp", "xmp_path"), ("companions", "companion_path"),
                           ("originals", "original_path")):
        relative = row[column]
        if not relative:
            continue
        path = os.path.join(base, relative)
        parent = os.path.join(base, "offline", family)
        if (os.path.dirname(path) != parent
                or os.path.realpath(parent) != parent
                or not os.path.basename(path).startswith(f"{photo_id}.")
                or os.path.islink(path)):
            raise ValueError("Cached path is outside the managed originals cache")
        source = sources[family]
        source_real = os.path.normcase(os.path.realpath(source)) if source else ""
        offline_root = os.path.normcase(os.path.join(base, "offline"))
        # A drive-letter change on Windows is a valid independent
        # source; commonpath would raise for that ordinary case.
        if not source or source_real == offline_root or source_real.startswith(offline_root + os.sep):
            raise ValueError("Source does not have an independent original")
        with open(source, "rb") as handle:
            source_stat = os.fstat(handle.fileno())
        if not stat.S_ISREG(source_stat.st_mode):
            raise ValueError("Source is not a regular file")
        if family == "originals" and (
            source_stat.st_size != row["source_size"]
            or source_stat.st_mtime != row["source_mtime"]
        ):
            raise ValueError("Source original has changed since it was cached")
        try:
            cached_stat = os.stat(path)
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(cached_stat.st_mode):
            raise ValueError("Cached path is not a regular file")
        if family != "xmp" and (
            source_stat.st_size != cached_stat.st_size
            or source_stat.st_mtime != cached_stat.st_mtime
        ):
            raise ValueError("Source and cached image no longer match")
        paths.append((path, cached_stat.st_size))
    return paths


def release_photo_original(
    db, photo_id, vireo_dir, *, verify_workspace=True, selected_photo=None,
):
    """Release only managed copies whose live sources are still accessible.

    Share preparation's per-photo lock, then recheck catalog identity under a
    writer transaction before unlinking. Network probes happen before taking
    that transaction. Other generated assets and all catalog metadata survive.
    """
    freed = 0
    with original_preparation_guard(vireo_dir, photo_id):
        photo = db.get_photo(photo_id, verify_workspace=verify_workspace)
        row = db.caches.offline_original_get(photo_id)
        if photo is None or row is None:
            return {"status": "not_cached", "bytes": 0}
        # A row left by a failed cache attempt (``missing_source`` or any
        # other non-``cached`` status) has no managed copy to release.
        # Storage counts only ``status='cached'`` rows, so the explicit
        # photo-selection entrypoint matches the bulk scope filter and
        # reports these as not cached instead of inflating the released
        # count with zero-byte "releases".
        if row["status"] != "cached":
            return {"status": "not_cached", "bytes": 0}
        if selected_photo is not None and not photo_source_matches(selected_photo, photo):
            return {"status": "kept", "bytes": 0, "reason": "Photo changed since release was requested"}
        folder = db.get_folder(photo["folder_id"])
        if folder is None:
            return {"status": "kept", "bytes": 0, "reason": "Source folder is unavailable"}
        base = os.path.realpath(vireo_dir)
        try:
            paths = _releasable_cached_paths(base, photo_id, photo, folder, row)
        except (OSError, ValueError) as exc:
            return {"status": "kept", "bytes": 0, "reason": str(exc)}

        try:
            db.begin_immediate()
            with db.transaction():
                current = db.get_photo(photo_id, verify_workspace=verify_workspace)
                cached = db.caches.offline_original_get(photo_id)
                current_folder = db.get_folder(photo["folder_id"])
                if (not photo_source_matches(photo, current)
                        or current_folder is None or current_folder["path"] != folder["path"]
                        or cached is None or dict(cached) != dict(row)):
                    return {"status": "kept", "bytes": 0, "reason": "Photo changed during release"}
                for path, size in paths:
                    try:
                        os.unlink(path)
                    except FileNotFoundError:
                        continue
                    freed += size
                db.caches.offline_original_delete(photo_id, _commit=False)
        except (OSError, sqlite3.Error) as exc:
            return {"status": "failed", "bytes": freed, "reason": str(exc)}
        return {"status": "released", "bytes": freed}


def _missing_source_result(
    db, photo, existing, existing_path, cache_intact, now, *, error, path,
):
    """Keep an intact cache, or record that the photo's source is missing."""
    if cache_intact:
        return {
            "status": "skipped",
            "bytes": existing["bytes"],
            "path": existing_path,
        }
    db.caches.offline_original_upsert(
        photo["id"],
        original_path=None,
        xmp_path=None,
        companion_path=None,
        bytes_=0,
        source_size=photo["file_size"] or 0,
        source_mtime=photo["file_mtime"],
        cached_at=now,
        status="missing_source",
        error=error,
    )
    return {"status": "missing_source", "bytes": 0, "path": path}


def _sidecar_sources(folder_path, photo):
    """Locate a photo's XMP and companion sources and their cache paths.

    Returns ``(xmp_src, xmp_src_exists, xmp_rel_expected, companion_src,
    companion_src_exists, companion_rel_expected)``.
    """
    xmp_src = _xmp_source_for(folder_path, photo["filename"])
    xmp_src_exists = os.path.isfile(xmp_src)
    companion_src = None
    companion = photo["companion_path"]
    if companion:
        companion_src = os.path.join(folder_path, companion)
    companion_src_exists = bool(companion_src) and os.path.isfile(companion_src)

    xmp_rel_expected = (
        _offline_rel_path("xmp", photo["id"], ".xmp") if xmp_src_exists else None
    )
    companion_rel_expected = None
    if companion_src_exists:
        companion_ext = os.path.splitext(companion)[1] or ".jpg"
        companion_rel_expected = _offline_rel_path(
            "companions", photo["id"], companion_ext.lower()
        )
    return (
        xmp_src, xmp_src_exists, xmp_rel_expected,
        companion_src, companion_src_exists, companion_rel_expected,
    )


def _cached_sidecar_fresh(vireo_dir, existing, column, src, src_exists, rel_expected):
    """Whether the cached copy in ``existing[column]`` still matches ``src``."""
    existing_abs = (
        os.path.join(vireo_dir, existing[column])
        if existing and existing[column]
        else None
    )
    return (
        (not src_exists and existing_abs is None)
        or (
            src_exists
            and existing
            and existing[column] == rel_expected
            and _same_file_stat(src, existing_abs)
        )
    )


def _cache_sidecar(vireo_dir, existing, column, src, src_exists, rel_expected):
    """Copy one sidecar into the cache and drop a superseded cached copy.

    Returns ``(rel_path_or_None, copied_bytes)``.
    """
    rel = None
    copied_bytes = 0
    if src_exists:
        rel = rel_expected
        dst = os.path.join(vireo_dir, rel)
        _copy_atomic(src, dst)
        copied_bytes = os.path.getsize(dst)
    if existing and existing[column] and existing[column] != rel:
        _unlink_cached_rel(vireo_dir, existing[column])
    return rel, copied_bytes


def _cache_photo_original(db, photo, vireo_dir, folders):
    folder_id = photo["folder_id"]
    now = time.time()
    existing = db.caches.offline_original_get(photo["id"])
    existing_path = offline_original_abs(vireo_dir, existing)
    # If a prior run cached this photo and the cached file is still on disk,
    # treat it as the source-of-record for offline access when the live source
    # is unavailable — don't clobber the DB record (which would also disable
    # `resolve_original_path`'s fallback to the cached copy).
    cache_intact = bool(
        existing
        and existing["status"] == "cached"
        and existing_path
        and os.path.isfile(existing_path)
    )

    if folder_id not in folders:
        return _missing_source_result(
            db, photo, existing, existing_path, cache_intact, now,
            error=f"unknown folder_id {folder_id}", path="",
        )

    folder_path = folders[folder_id]
    source_path = os.path.join(folder_path, photo["filename"])

    if not os.path.isfile(source_path):
        return _missing_source_result(
            db, photo, existing, existing_path, cache_intact, now,
            error=f"source file missing: {source_path}", path=source_path,
        )

    st = os.stat(source_path)
    (
        xmp_src, xmp_src_exists, xmp_rel_expected,
        companion_src, companion_src_exists, companion_rel_expected,
    ) = _sidecar_sources(folder_path, photo)

    xmp_fresh = _cached_sidecar_fresh(
        vireo_dir, existing, "xmp_path", xmp_src, xmp_src_exists, xmp_rel_expected,
    )
    companion_fresh = _cached_sidecar_fresh(
        vireo_dir, existing, "companion_path",
        companion_src, companion_src_exists, companion_rel_expected,
    )

    if (
        existing
        and existing["status"] == "cached"
        and existing_path
        and os.path.isfile(existing_path)
        and existing["source_size"] == st.st_size
        and existing["source_mtime"] == st.st_mtime
        and xmp_fresh
        and companion_fresh
    ):
        return {
            "status": "skipped",
            "bytes": existing["bytes"],
            "path": existing_path,
        }

    ext = os.path.splitext(photo["filename"])[1] or photo["extension"] or ".jpg"
    original_rel = _offline_rel_path("originals", photo["id"], ext.lower())
    original_abs = os.path.join(vireo_dir, original_rel)
    _copy_atomic(source_path, original_abs)

    copied_bytes = os.path.getsize(original_abs)
    xmp_rel, xmp_bytes = _cache_sidecar(
        vireo_dir, existing, "xmp_path", xmp_src, xmp_src_exists, xmp_rel_expected,
    )
    copied_bytes += xmp_bytes
    companion_rel, companion_bytes = _cache_sidecar(
        vireo_dir, existing, "companion_path",
        companion_src, companion_src_exists, companion_rel_expected,
    )
    copied_bytes += companion_bytes

    db.caches.offline_original_upsert(
        photo["id"],
        original_path=original_rel,
        xmp_path=xmp_rel,
        companion_path=companion_rel,
        bytes_=copied_bytes,
        source_size=st.st_size,
        source_mtime=st.st_mtime,
        cached_at=now,
        status="cached",
        error=None,
    )
    return {"status": "cached", "bytes": copied_bytes, "path": original_abs}
