"""Generate and manage local thumbnail cache for the photo browser."""

import logging
import os

from image_loader import get_canonical_image_path, load_image

log = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = os.path.expanduser("~/.vireo/thumbnails")
THUMB_SIZE = 400


def generate_thumbnail(photo_id, source_path, cache_dir, size=THUMB_SIZE, quality=85):
    """Generate a JPEG thumbnail for a photo.

    Args:
        photo_id: database photo id (used as filename)
        source_path: path to the original image file
        cache_dir: directory to store thumbnails
        size: max dimension in pixels
        quality: JPEG quality (1-95)

    Returns:
        path to the thumbnail file, or None on failure
    """
    thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")

    if os.path.exists(thumb_path):
        return thumb_path

    img = load_image(source_path, max_size=size)
    if img is None:
        log.warning("Could not load image for thumbnail: %s", source_path)
        return None

    os.makedirs(cache_dir, exist_ok=True)
    img.save(thumb_path, "JPEG", quality=quality)
    return thumb_path


def get_thumb_path(photo_id, cache_dir):
    """Return the thumbnail path if it exists, None otherwise."""
    thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
    if os.path.exists(thumb_path):
        return thumb_path
    return None


def generate_all(db, cache_dir, progress_callback=None, config=None, vireo_dir=None):
    """Generate thumbnails for photos that don't have one yet.

    Only processes photos missing thumbnails, so re-running is fast.

    Args:
        db: Database instance
        cache_dir: thumbnail cache directory
        progress_callback: optional callable(current, total)
        config: optional config dict; if None, loads from disk
        vireo_dir: optional path to ~/.vireo/; when set, working copies
            are preferred over original files as thumbnail source
    """
    import config as cfg
    user_cfg = config or cfg.load()
    thumb_size = user_cfg.get("thumbnail_size", THUMB_SIZE)
    thumb_quality = user_cfg.get("thumbnail_quality", 85)

    os.makedirs(cache_dir, exist_ok=True)

    photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}

    # Filter to only photos needing thumbnails
    needed = []
    for photo in photos:
        thumb_path = os.path.join(cache_dir, f"{photo['id']}.jpg")
        if not os.path.exists(thumb_path):
            needed.append(photo)

    total = len(needed)
    skipped = len(photos) - total

    if total == 0:
        log.info("All %d thumbnails up to date", skipped)
        result = {"generated": 0, "skipped": skipped, "failed": 0}
        result["summary"] = format_summary(result)
        return result

    log.info("Generating %d thumbnails (%d already cached)", total, skipped)

    generated = 0
    failed = 0
    for i, photo in enumerate(needed):
        # Resolve source via the single canonical-path helper when we
        # have a vireo_dir; fall back to a raw folder+filename join for
        # callers that don't pass it.
        if vireo_dir:
            source_path = get_canonical_image_path(photo, vireo_dir, folders)
        else:
            folder_path = folders.get(photo["folder_id"], "")
            source_path = os.path.join(folder_path, photo["filename"])
        if generate_thumbnail(photo["id"], source_path, cache_dir, size=thumb_size, quality=thumb_quality) is not None:
            generated += 1
            # Record on-disk presence in the photos table so the dashboard's
            # coverage query (`thumb_path IS NOT NULL`) reflects this run.
            # Stored value is the bare filename ({id}.jpg) so the column
            # stays correct even if the thumbnail cache dir is later moved.
            db.conn.execute(
                "UPDATE photos SET thumb_path=? WHERE id=?",
                (f"{photo['id']}.jpg", photo["id"]),
            )
        else:
            failed += 1

        if progress_callback:
            progress_callback(i + 1, total)

    db.conn.commit()
    if failed:
        log.warning("Thumbnail generation: %d of %d failed", failed, total)
    result = {"generated": generated, "skipped": skipped, "failed": failed}
    result["summary"] = format_summary(result)
    return result


def thumb_path_backfill_candidate_count(db, cache_dir):
    """Count photos that ``backfill_thumb_paths`` would actually update.

    A photo is a candidate when its ``thumb_path`` does not match disk
    reality:

    * ``thumb_path IS NULL`` but ``<cache_dir>/<id>.jpg`` exists, or
    * ``thumb_path IS NOT NULL`` but the file no longer exists (drift after
      a manual cache wipe).

    Used by the startup gate in ``app.py`` to skip spawning a backfill job
    when nothing needs work, and after a backfill run for "remaining"
    reporting.
    """
    rows = db.conn.execute(
        "SELECT id, thumb_path FROM photos"
    ).fetchall()
    candidates = 0
    for row in rows:
        photo_id = row["id"]
        on_disk = os.path.exists(os.path.join(cache_dir, f"{photo_id}.jpg"))
        if row["thumb_path"] is None and on_disk or row["thumb_path"] is not None and not on_disk:
            candidates += 1
    return candidates


def backfill_thumb_paths(db, cache_dir, progress_callback=None,
                         status_callback=None, cancel_check=None):
    """Library-wide self-healing pass that aligns ``photos.thumb_path`` with
    on-disk reality.

    Two corrections:

    * For each photo with ``thumb_path IS NULL`` whose JPEG exists on disk,
      set ``thumb_path = '<id>.jpg'`` so the dashboard's coverage query
      reports it. This is the path that fired in production: thumbnails
      generated before the column was wired up reported 0 forever.
    * For each photo whose ``thumb_path`` is set but the file is gone,
      clear the column — otherwise the dashboard claims coverage that no
      longer exists after a manual cache wipe.

    Returns a summary dict ``{"set": N, "cleared": M, "remaining": K}``.

    Sequential by design — each row is a stat() plus at most one tiny
    UPDATE, so single-threaded throughput is fine even on tens of
    thousands of photos.
    """
    rows = db.conn.execute(
        "SELECT id, thumb_path FROM photos"
    ).fetchall()
    total = len(rows)
    if status_callback:
        status_callback(f"Reconciling thumb_path for {total:,} photos")

    set_count = 0
    cleared_count = 0
    BATCH = 500
    pending_set = []
    pending_clear = []

    def _flush():
        if pending_set:
            db.conn.executemany(
                "UPDATE photos SET thumb_path=? WHERE id=?",
                pending_set,
            )
            pending_set.clear()
        if pending_clear:
            db.conn.executemany(
                "UPDATE photos SET thumb_path=NULL WHERE id=?",
                pending_clear,
            )
            pending_clear.clear()
        db.conn.commit()

    for i, row in enumerate(rows):
        if cancel_check is not None and cancel_check():
            break
        photo_id = row["id"]
        on_disk = os.path.exists(os.path.join(cache_dir, f"{photo_id}.jpg"))
        if row["thumb_path"] is None and on_disk:
            pending_set.append((f"{photo_id}.jpg", photo_id))
            set_count += 1
        elif row["thumb_path"] is not None and not on_disk:
            pending_clear.append((photo_id,))
            cleared_count += 1

        if (len(pending_set) + len(pending_clear)) >= BATCH:
            _flush()
        if progress_callback is not None:
            progress_callback(i + 1, total)

    _flush()

    log.info(
        "thumb_path backfill: set=%d cleared=%d (of %d photos)",
        set_count, cleared_count, total,
    )
    return {
        "set": set_count,
        "cleared": cleared_count,
        "remaining": thumb_path_backfill_candidate_count(db, cache_dir),
    }


def format_summary(result):
    """Build a human-friendly one-line summary from a thumbnail result dict."""
    generated = result.get("generated", 0)
    skipped = result.get("skipped", 0)
    failed = result.get("failed", 0)

    parts = []
    if generated:
        parts.append(f"{generated} new")
    if skipped:
        parts.append(f"{skipped} already cached")
    if failed:
        parts.append(f"{failed} failed")
    if not parts:
        return "0 thumbnails"
    return ", ".join(parts)
