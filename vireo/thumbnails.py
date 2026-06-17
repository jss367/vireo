"""Generate and manage local thumbnail cache for the photo browser."""

import contextlib
import json
import logging
import os
import tempfile
from datetime import UTC, datetime

from image_loader import RAW_EXTENSIONS, get_canonical_image_path, load_image

log = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = os.path.expanduser("~/.vireo/thumbnails")
THUMB_SIZE = 400
_EXIF_ORIENTATION_TAG = 274


def _rendered_recipe_long_edge(width, height, recipe):
    rotation = (recipe or {}).get("rotation", 0)
    if rotation in (90, 270):
        width, height = height, width
    crop = (recipe or {}).get("crop") if recipe else None
    if crop:
        return max(float(crop["w"]) * width, float(crop["h"]) * height)
    return max(width, height)


def _photo_value(photo, key):
    try:
        return photo[key]
    except (KeyError, IndexError, TypeError):
        if hasattr(photo, "get"):
            return photo.get(key)
    return None


def _exif_orientation(exif_data):
    if not exif_data:
        return None
    if isinstance(exif_data, str):
        try:
            metadata = json.loads(exif_data)
        except (TypeError, ValueError):
            return None
    elif isinstance(exif_data, dict):
        metadata = exif_data
    else:
        return None
    if not isinstance(metadata, dict):
        return None
    for group in ("EXIF", "IFD0", "TIFF", "File"):
        values = metadata.get(group)
        if isinstance(values, dict) and "Orientation" in values:
            return values["Orientation"]
    return metadata.get("Orientation")


def _orientation_swaps_axes(orientation):
    if orientation is None or isinstance(orientation, bool):
        return False
    if isinstance(orientation, (int, float)):
        return int(orientation) in (5, 6, 7, 8)
    text = str(orientation).strip().lower()
    if not text:
        return False
    try:
        return int(text) in (5, 6, 7, 8)
    except ValueError:
        return "90" in text or "270" in text


def _recipe_source_dimensions(photo):
    try:
        width = int(_photo_value(photo, "width") or 0)
        height = int(_photo_value(photo, "height") or 0)
    except (TypeError, ValueError):
        return 0, 0
    if (
        width > 0
        and height > 0
        and _orientation_swaps_axes(_exif_orientation(_photo_value(photo, "exif_data")))
    ):
        return height, width
    return width, height


def _image_size_after_exif_orientation(img):
    width, height = img.size
    orientation = None
    with contextlib.suppress(Exception):
        orientation = img.getexif().get(_EXIF_ORIENTATION_TAG)
    if _orientation_swaps_axes(orientation):
        return height, width
    return width, height


def _path_satisfies_recipe_render(path, photo, recipe, max_size):
    original_w, original_h = _recipe_source_dimensions(photo)
    if original_w <= 0 or original_h <= 0:
        return False
    try:
        from PIL import Image as _PILImage
        with _PILImage.open(path) as img:
            width, height = _image_size_after_exif_orientation(img)
    except Exception:
        return False
    original_render_long = _rendered_recipe_long_edge(
        original_w, original_h, recipe,
    )
    required_long = min(max_size, original_render_long) if max_size else original_render_long
    return _rendered_recipe_long_edge(width, height, recipe) >= required_long


def _recipe_source_path(photo, recipe, max_size, vireo_dir, folders):
    if not vireo_dir or not recipe:
        if vireo_dir:
            return get_canonical_image_path(photo, vireo_dir, folders)
        return os.path.join(folders.get(photo["folder_id"], ""), photo["filename"])

    canonical = get_canonical_image_path(photo, vireo_dir, folders)
    wc_rel = photo["working_copy_path"]
    if not recipe.get("crop") and canonical and wc_rel:
        wc_path = wc_rel if os.path.isabs(wc_rel) else os.path.join(vireo_dir, wc_rel)
        if os.path.abspath(canonical) == os.path.abspath(wc_path):
            return canonical
    if recipe.get("crop") and wc_rel:
        wc_path = os.path.join(vireo_dir, wc_rel)
        if (
            os.path.exists(wc_path)
            and _path_satisfies_recipe_render(wc_path, photo, recipe, max_size)
        ):
            return canonical

    folder_path = folders.get(photo["folder_id"])
    if not folder_path:
        if wc_rel:
            wc_path = os.path.join(vireo_dir, wc_rel)
            if os.path.exists(wc_path):
                return wc_path
        return ""
    companion_path = photo["companion_path"]
    if companion_path:
        companion = os.path.join(folder_path, companion_path)
        if (
            os.path.exists(companion)
            and _path_satisfies_recipe_render(companion, photo, recipe, max_size)
        ):
            return companion
    original = os.path.join(folder_path, photo["filename"])
    if not os.path.exists(original) and wc_rel:
        wc_path = os.path.join(vireo_dir, wc_rel)
        if os.path.exists(wc_path):
            return wc_path
    return original


def _has_current_raw_failure(photo, source_path):
    if os.path.splitext(source_path or "")[1].lower() not in RAW_EXTENSIONS:
        return False
    if os.path.splitext(_photo_value(photo, "filename") or "")[1].lower() not in RAW_EXTENSIONS:
        return False
    if _photo_value(photo, "working_copy_failed_source") not in (None, "source"):
        return False
    failed_at = _photo_value(photo, "working_copy_failed_at")
    failed_mtime = _photo_value(photo, "working_copy_failed_mtime")
    file_mtime = _photo_value(photo, "file_mtime")
    if not failed_at or failed_mtime is None or file_mtime is None:
        return False
    try:
        if float(failed_mtime) != float(file_mtime):
            return False
    except (TypeError, ValueError):
        return False
    try:
        failed_s = str(failed_at).strip()
        if failed_s.endswith("Z"):
            failed_s = failed_s[:-1] + "+00:00"
        failed_dt = datetime.fromisoformat(failed_s)
        if failed_dt.tzinfo is None:
            failed_dt = failed_dt.replace(tzinfo=UTC)
    except (TypeError, ValueError):
        return False
    age = (datetime.now(UTC) - failed_dt.astimezone(UTC)).total_seconds()
    return age < 24 * 60 * 60


def generate_thumbnail(
    photo_id, source_path, cache_dir, size=THUMB_SIZE, quality=85, recipe=None,
):
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

    load_max_size = None if recipe and recipe.get("crop") else size
    img = load_image(source_path, max_size=load_max_size)
    if img is None:
        log.warning("Could not load image for thumbnail: %s", source_path)
        return None
    if recipe:
        from image_edits import apply_recipe_to_loaded_image
        img = apply_recipe_to_loaded_image(img, recipe, max_size=size)

    os.makedirs(cache_dir, exist_ok=True)
    # Atomic write: two concurrent jobs (or two iterations of the same job
    # racing on a freshly added photo) can both see the missing thumb and
    # call img.save() on the same path, producing a half-written or
    # interleaved JPEG. Write to a sibling temp file then os.replace().
    fd, tmp_path = tempfile.mkstemp(
        prefix=f".{photo_id}.", suffix=".jpg.tmp", dir=cache_dir
    )
    os.close(fd)
    try:
        img.save(tmp_path, "JPEG", quality=quality)
        os.replace(tmp_path, thumb_path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
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
        recipe = db.get_photo_edit_recipe(photo["id"])
        source_photo = db.get_photo(photo["id"]) if recipe and vireo_dir else photo
        if source_photo is None:
            source_photo = photo
        source_path = _recipe_source_path(
            source_photo, recipe, thumb_size, vireo_dir, folders,
        )
        if recipe and _has_current_raw_failure(source_photo, source_path):
            failed += 1
            continue
        # generate_thumbnail decodes the source image (slow for RAW). Run
        # it before any UPDATE so no transaction is open while it runs;
        # then commit per photo to release the writer lock between
        # iterations and avoid blocking concurrent jobs (a parallel scan's
        # add_photo INSERT) past the 30s busy_timeout.
        recipe_kwargs = {"recipe": recipe} if recipe else {}
        if generate_thumbnail(
            photo["id"],
            source_path,
            cache_dir,
            size=thumb_size,
            quality=thumb_quality,
            **recipe_kwargs,
        ) is not None:
            generated += 1
            # Record on-disk presence in the photos table so the dashboard's
            # coverage query (`thumb_path IS NOT NULL`) reflects this run.
            # Stored value is the bare filename ({id}.jpg) so the column
            # stays correct even if the thumbnail cache dir is later moved.
            db.conn.execute(
                "UPDATE photos SET thumb_path=? WHERE id=?",
                (f"{photo['id']}.jpg", photo["id"]),
            )
            db.conn.commit()
        else:
            failed += 1

        if progress_callback:
            progress_callback(i + 1, total)

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
