"""Generate and manage local thumbnail cache for the photo browser."""

import logging
import os

from image_loader import load_image

log = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = os.path.expanduser("~/.spotter/thumbnails")
THUMB_SIZE = 400


def generate_thumbnail(photo_id, source_path, cache_dir, size=THUMB_SIZE):
    """Generate a JPEG thumbnail for a photo.

    Args:
        photo_id: database photo id (used as filename)
        source_path: path to the original image file
        cache_dir: directory to store thumbnails
        size: max dimension in pixels

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
    img.save(thumb_path, "JPEG", quality=85)
    return thumb_path


def get_thumb_path(photo_id, cache_dir):
    """Return the thumbnail path if it exists, None otherwise."""
    thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
    if os.path.exists(thumb_path):
        return thumb_path
    return None


def generate_all(db, cache_dir, progress_callback=None):
    """Generate thumbnails for all photos that don't have one yet.

    Args:
        db: Database instance
        cache_dir: thumbnail cache directory
        progress_callback: optional callable(current, total)
    """
    os.makedirs(cache_dir, exist_ok=True)

    photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}

    total = len(photos)
    failed = 0
    for i, photo in enumerate(photos):
        thumb_path = os.path.join(cache_dir, f"{photo['id']}.jpg")
        if not os.path.exists(thumb_path):
            folder_path = folders.get(photo["folder_id"], "")
            source_path = os.path.join(folder_path, photo["filename"])
            if generate_thumbnail(photo["id"], source_path, cache_dir) is None:
                failed += 1

        if progress_callback:
            progress_callback(i + 1, total)

    if failed:
        log.warning("Thumbnail generation: %d of %d failed", failed, total)
    return {"generated": total - failed, "failed": failed}
