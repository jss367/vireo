"""Local original-file cache for offline viewing."""

import contextlib
import os
import shutil
import tempfile
import time


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
        os.replace(tmp, dst)
    finally:
        if os.path.exists(tmp):
            with contextlib.suppress(OSError):
                os.unlink(tmp)


def _offline_rel_path(kind, photo_id, suffix):
    return os.path.join("offline", kind, f"{int(photo_id)}{suffix}")


def _xmp_source_for(folder_path, filename):
    stem = os.path.splitext(filename)[0]
    return os.path.join(folder_path, stem + ".xmp")


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
    row = db.offline_original_get(photo_id)
    path = offline_original_abs(vireo_dir, row)
    if path and os.path.isfile(path):
        return path
    return None


def resolve_original_path(db, photo, vireo_dir, folders):
    """Return (path, used_offline_cache) for a photo's original.

    The source-of-truth file wins when it is available. The offline copy is a
    fallback for temporarily unavailable volumes.
    """
    folder_id = photo["folder_id"]
    if folder_id in folders:
        source_path = os.path.join(folders[folder_id], photo["filename"])
        if os.path.isfile(source_path):
            return source_path, False
    else:
        source_path = ""
    cached = cached_original_for_photo(db, photo["id"], vireo_dir)
    if cached:
        return cached, True
    return source_path, False


def cache_photo_original(db, photo, vireo_dir, folders):
    """Copy one photo's original and sidecar metadata into the offline cache."""
    folder_id = photo["folder_id"]
    now = time.time()
    existing = db.offline_original_get(photo["id"])
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
        if cache_intact:
            return {
                "status": "skipped",
                "bytes": existing["bytes"],
                "path": existing_path,
            }
        db.offline_original_upsert(
            photo["id"],
            original_path=None,
            xmp_path=None,
            companion_path=None,
            bytes_=0,
            source_size=photo["file_size"] or 0,
            source_mtime=photo["file_mtime"],
            cached_at=now,
            status="missing_source",
            error=f"unknown folder_id {folder_id}",
        )
        return {"status": "missing_source", "bytes": 0, "path": ""}

    folder_path = folders[folder_id]
    source_path = os.path.join(folder_path, photo["filename"])

    if not os.path.isfile(source_path):
        if cache_intact:
            return {
                "status": "skipped",
                "bytes": existing["bytes"],
                "path": existing_path,
            }
        db.offline_original_upsert(
            photo["id"],
            original_path=None,
            xmp_path=None,
            companion_path=None,
            bytes_=0,
            source_size=photo["file_size"] or 0,
            source_mtime=photo["file_mtime"],
            cached_at=now,
            status="missing_source",
            error=f"source file missing: {source_path}",
        )
        return {"status": "missing_source", "bytes": 0, "path": source_path}

    st = os.stat(source_path)
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

    existing_xmp_abs = (
        os.path.join(vireo_dir, existing["xmp_path"])
        if existing and existing["xmp_path"]
        else None
    )
    existing_companion_abs = (
        os.path.join(vireo_dir, existing["companion_path"])
        if existing and existing["companion_path"]
        else None
    )
    xmp_fresh = (
        (not xmp_src_exists and existing_xmp_abs is None)
        or (
            xmp_src_exists
            and existing
            and existing["xmp_path"] == xmp_rel_expected
            and _same_file_stat(xmp_src, existing_xmp_abs)
        )
    )
    companion_fresh = (
        (not companion_src_exists and existing_companion_abs is None)
        or (
            companion_src_exists
            and existing
            and existing["companion_path"] == companion_rel_expected
            and _same_file_stat(companion_src, existing_companion_abs)
        )
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
    xmp_rel = None
    if xmp_src_exists:
        xmp_rel = xmp_rel_expected
        xmp_abs = os.path.join(vireo_dir, xmp_rel)
        _copy_atomic(xmp_src, xmp_abs)
        copied_bytes += os.path.getsize(xmp_abs)
    if existing and existing["xmp_path"] and existing["xmp_path"] != xmp_rel:
        _unlink_cached_rel(vireo_dir, existing["xmp_path"])

    companion_rel = None
    if companion_src_exists:
        companion_rel = companion_rel_expected
        companion_abs = os.path.join(vireo_dir, companion_rel)
        _copy_atomic(companion_src, companion_abs)
        copied_bytes += os.path.getsize(companion_abs)
    if (
        existing
        and existing["companion_path"]
        and existing["companion_path"] != companion_rel
    ):
        _unlink_cached_rel(vireo_dir, existing["companion_path"])

    db.offline_original_upsert(
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
