"""Shared helpers for the preview_cache LRU.

The eviction pass is called from three places: the Flask request path
(``_serve_preview``), the startup migration in ``create_app``, and the
pipeline job's preview stage. Keeping the logic here avoids having the
pipeline import from ``app`` (which would be circular) or duplicating
the loop in two modules.
"""

import glob as _glob
import logging
import os

log = logging.getLogger(__name__)


def cleanup_cached_files_for_deleted_photos(
    thumb_cache_dir, files, progress_callback=None,
):
    """Remove thumbnail, preview, and working-copy files for deleted photos.

    ``files`` is the list returned by ``db.delete_photos`` /
    ``db.delete_folder``. The FK cascade drops preview_cache rows when
    photos are deleted, but the on-disk files stay unless we unlink
    them here — otherwise they leak into untracked bytes that eviction
    can't see, and on SQLite a retry that reuses one of the just-freed
    photo IDs would treat the stale ``{photo_id}.jpg`` as a valid
    thumbnail and skip regenerating it.

    Note: if an unlink fails (e.g. file locked on Windows), the file
    remains on disk as an orphan because the cascade has already removed
    the preview_cache row. "Clear cache" in Settings recovers by globbing
    the directory.
    """
    vireo_dir = os.path.dirname(thumb_cache_dir)
    preview_dir = os.path.join(vireo_dir, "previews")
    working_dir = os.path.join(vireo_dir, "working")
    originals_dir = os.path.join(vireo_dir, "originals")
    # Offline-cache layout: offline/{originals,xmp,companions}/{pid}{ext}.
    # The FK cascade drops the offline_originals row when the photo is
    # deleted, so we lose the exact stored paths — glob by photo id to
    # cover any source extension and any sidecar/companion that was
    # copied alongside it.
    offline_dirs = [
        os.path.join(vireo_dir, "offline", "originals"),
        os.path.join(vireo_dir, "offline", "xmp"),
        os.path.join(vireo_dir, "offline", "companions"),
    ]
    total = len(files)
    for idx, f in enumerate(files, start=1):
        pid = f["photo_id"]
        # {id}.jpg lives in these dirs as a legacy full preview, thumbnail,
        # working copy, or prepared full-resolution render. {id}_{size}.jpg
        # is used for sized preview variants.
        for d in [thumb_cache_dir, preview_dir, working_dir, originals_dir]:
            cached = os.path.join(d, f"{pid}.jpg")
            if os.path.isfile(cached):
                try:
                    os.remove(cached)
                except OSError as e:
                    log.warning(
                        "Failed to remove cached file %s after photo "
                        "delete — will be reclaimed by Clear Cache: %s",
                        cached, e,
                    )
        for prepared_render in _glob.glob(
            os.path.join(originals_dir, f"{pid}_*.jpg")
        ):
            try:
                os.remove(prepared_render)
            except OSError as e:
                log.warning(
                    "Failed to remove cached file %s after photo delete — "
                    "will be reclaimed by Clear Cache: %s",
                    prepared_render, e,
                )
        for variant in _glob.glob(os.path.join(preview_dir, f"{pid}_*.jpg")):
            try:
                os.remove(variant)
            except OSError as e:
                log.warning(
                    "Failed to remove preview variant %s after photo "
                    "delete — will be reclaimed by Clear Cache: %s",
                    variant, e,
                )
        for d in offline_dirs:
            for orphan in _glob.glob(os.path.join(d, f"{pid}.*")):
                try:
                    os.remove(orphan)
                except OSError as e:
                    log.warning(
                        "Failed to remove offline cache file %s after "
                        "photo delete — will be reclaimed by Clear "
                        "Cache: %s",
                        orphan, e,
                    )
        if progress_callback:
            progress_callback(idx, total, f.get("filename") or str(pid))


def evict_if_over_quota(db, vireo_dir):
    """Evict oldest preview_cache entries until under preview_cache_max_mb.

    Walks rows in ascending ``last_access_at`` order, removes files and
    rows, and stops as soon as total <= quota. Self-healing: if a file
    is already missing, the ghost row is still deleted. If ``unlink``
    fails for any other OS reason the row is *left in place* so the
    bytes stay accounted for and a future pass can retry; otherwise the
    accounting under-reports and eviction stops targeting the leaked
    bytes.

    Deletes are batched into one transaction to avoid hundreds of
    fsyncs when the quota is shrunk dramatically.
    """
    import config as cfg

    quota_mb = cfg.load().get("preview_cache_max_mb", 20480)
    max_bytes = int(quota_mb) * 1024 * 1024
    total = db.preview_cache_total_bytes()
    if total <= max_bytes:
        return

    preview_dir = os.path.join(vireo_dir, "previews")
    to_delete = []
    freed_bytes = 0
    for row in db.preview_cache_oldest_first():
        if total <= max_bytes:
            break
        path = os.path.join(
            preview_dir, f"{row['photo_id']}_{row['size']}.jpg"
        )
        removed = True
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as e:
            log.warning("Failed to remove preview cache file %s: %s", path, e)
            removed = False
        if removed:
            to_delete.append((row["photo_id"], row["size"]))
            total -= row["bytes"]
            freed_bytes += row["bytes"]

    if to_delete:
        db.conn.executemany(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            to_delete,
        )
        db.conn.commit()
        log.info(
            "Preview cache eviction: removed %d entries, freed %.1f MB",
            len(to_delete), freed_bytes / 1024 / 1024,
        )


def reconcile_preview_cache(db, vireo_dir):
    """Drop preview_cache rows whose on-disk file is missing.

    Counterpart to ``evict_if_over_quota``'s self-heal: that path only
    cleans up ghost rows when the cache is *over* quota. If the cache
    accounting drifts while *under* quota — e.g. files deleted by an
    external process, or a previous eviction pass that removed files
    after the row's ``last_access_at`` was recently touched — the
    table keeps reporting ``total_bytes`` for files that no longer
    exist, and eviction stays asleep. That's invisible to the user
    until the next pipeline run regenerates everything from RAW
    because none of the cache files actually exist.

    Run at startup so a stale table can't poison the rest of the
    session. Returns the number of rows dropped.
    """
    preview_dir = os.path.join(vireo_dir, "previews")
    to_delete = []
    for row in db.preview_cache_oldest_first():
        path = os.path.join(
            preview_dir, f"{row['photo_id']}_{row['size']}.jpg"
        )
        if not os.path.exists(path):
            to_delete.append((row["photo_id"], row["size"]))

    if to_delete:
        db.conn.executemany(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            to_delete,
        )
        db.conn.commit()
        log.info(
            "Preview cache reconcile: dropped %d ghost rows (files missing on disk)",
            len(to_delete),
        )
    return len(to_delete)
