"""Local original-file cache for offline viewing."""

import os
import shutil
import time


def _copy_atomic(src, dst):
    """Copy ``src`` to ``dst`` via a sibling temp file."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    tmp = dst + ".tmp"
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def _offline_rel_path(kind, photo_id, suffix):
    return os.path.join("offline", kind, f"{int(photo_id)}{suffix}")


def _xmp_source_for(folder_path, filename):
    stem = os.path.splitext(filename)[0]
    return os.path.join(folder_path, stem + ".xmp")


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
    folder_path = folders.get(photo["folder_id"], "")
    source_path = os.path.join(folder_path, photo["filename"])
    if os.path.isfile(source_path):
        return source_path, False
    cached = cached_original_for_photo(db, photo["id"], vireo_dir)
    if cached:
        return cached, True
    return source_path, False


def cache_photo_original(db, photo, vireo_dir, folders):
    """Copy one photo's original and sidecar metadata into the offline cache."""
    folder_path = folders.get(photo["folder_id"], "")
    source_path = os.path.join(folder_path, photo["filename"])
    now = time.time()

    if not os.path.isfile(source_path):
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
    existing = db.offline_original_get(photo["id"])
    existing_path = offline_original_abs(vireo_dir, existing)
    if (
        existing
        and existing["status"] == "cached"
        and existing_path
        and os.path.isfile(existing_path)
        and existing["source_size"] == st.st_size
        and existing["source_mtime"] == st.st_mtime
    ):
        return {"status": "skipped", "bytes": existing["bytes"], "path": existing_path}

    ext = os.path.splitext(photo["filename"])[1] or photo["extension"] or ".jpg"
    original_rel = _offline_rel_path("originals", photo["id"], ext.lower())
    original_abs = os.path.join(vireo_dir, original_rel)
    _copy_atomic(source_path, original_abs)

    copied_bytes = os.path.getsize(original_abs)
    xmp_rel = None
    xmp_src = _xmp_source_for(folder_path, photo["filename"])
    if os.path.isfile(xmp_src):
        xmp_rel = _offline_rel_path("xmp", photo["id"], ".xmp")
        _copy_atomic(xmp_src, os.path.join(vireo_dir, xmp_rel))

    companion_rel = None
    companion = photo["companion_path"]
    if companion:
        companion_src = os.path.join(folder_path, companion)
        if os.path.isfile(companion_src):
            companion_ext = os.path.splitext(companion)[1] or ".jpg"
            companion_rel = _offline_rel_path(
                "companions", photo["id"], companion_ext.lower()
            )
            _copy_atomic(companion_src, os.path.join(vireo_dir, companion_rel))
            copied_bytes += os.path.getsize(os.path.join(vireo_dir, companion_rel))

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
