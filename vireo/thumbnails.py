"""Generate and manage local thumbnail cache for the photo browser."""

import logging
import os

from image_loader import load_image

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
        # Prefer working copy for source
        source_path = None
        if vireo_dir and photo["working_copy_path"]:
            wc = os.path.join(vireo_dir, photo["working_copy_path"])
            if os.path.exists(wc):
                source_path = wc
        if source_path is None:
            folder_path = folders.get(photo["folder_id"], "")
            source_path = os.path.join(folder_path, photo["filename"])
        if generate_thumbnail(photo["id"], source_path, cache_dir, size=thumb_size, quality=thumb_quality) is not None:
            generated += 1
        else:
            failed += 1

        if progress_callback:
            progress_callback(i + 1, total)

    if failed:
        log.warning("Thumbnail generation: %d of %d failed", failed, total)
    result = {"generated": generated, "skipped": skipped, "failed": failed}
    result["summary"] = format_summary(result)
    return result


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
