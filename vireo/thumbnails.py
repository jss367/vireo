"""Generate and manage local thumbnail cache for the photo browser."""

import contextlib
import logging
import os
import tempfile
from datetime import UTC, datetime

from image_loader import (
    RAW_DECODE_PRESERVE_HIGHLIGHTS,
    RAW_EXTENSIONS,
    load_image,
)
from render_source import (
    photo_value as _photo_value,
)
from render_source import (
    recipe_render_source,
    thumbnail_source_dimensions_are_acceptable,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from render_source import (
    scaled_recipe_source_dimensions as _scaled_recipe_source_dimensions,
)
from render_source import (
    working_copy_path_if_satisfies as _working_copy_path_if_satisfies,
)

log = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = os.path.expanduser("~/.vireo/thumbnails")
THUMB_SIZE = 400
def _recipe_source_path(photo, recipe, max_size, vireo_dir, folders):
    """Thin wrapper around the shared resolver, returning just the path.

    Thumbnail callers don't need the ``using_working_copy`` flag, so the
    second element of :func:`render_source.recipe_render_source` is dropped.
    """
    return recipe_render_source(photo, recipe, max_size, vireo_dir, folders)[0]


def _has_current_raw_failure(photo, source_path):
    """Whether this RAW row carries an explicit `source` failure marker.

    Only an explicit ``working_copy_failed_source == "source"`` marker
    routes RAW thumbnails to the companion JPEG. Treating legacy NULL
    markers the same way as an explicit source failure made thumbnail
    selection diverge from preview/export, which use only the explicit
    semantics (see ``_has_current_working_copy_failure`` in ``app.py``
    and ``pipeline_job.py``).
    """
    if os.path.splitext(source_path or "")[1].lower() not in RAW_EXTENSIONS:
        return False
    if os.path.splitext(_photo_value(photo, "filename") or "")[1].lower() not in RAW_EXTENSIONS:
        return False
    if _photo_value(photo, "working_copy_failed_source") != "source":
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


def _retry_thumbnail_with_companion(
    db, photo, source_path, cache_dir, size, quality, recipe, folder_path,
):
    if not photo or not folder_path:
        return None
    companion_rel = _photo_value(photo, "companion_path")
    if not companion_rel:
        return None
    companion_abs = os.path.join(folder_path, companion_rel)
    if (
        not os.path.exists(companion_abs)
        or os.path.abspath(companion_abs) == os.path.abspath(source_path)
    ):
        return None
    photo_id = _photo_value(photo, "id")
    log.info(
        "Thumbnail RAW decode failed for photo %s; falling back to companion JPEG",
        photo_id,
    )
    file_mtime = _photo_value(photo, "file_mtime")
    if file_mtime is not None and photo_id is not None and hasattr(db, "conn"):
        with contextlib.suppress(Exception):
            db.conn.execute(
                "UPDATE photos SET"
                " working_copy_failed_at=datetime('now'),"
                " working_copy_failed_mtime=?,"
                " working_copy_failed_source='source'"
                " WHERE id=?",
                (file_mtime, photo_id),
            )
            db.conn.commit()
    recipe_kwargs = {"recipe": recipe} if recipe else {}
    if recipe:
        recipe_kwargs["native_size"] = _recipe_source_dimensions(photo)
    return generate_thumbnail(
        photo_id,
        companion_abs,
        cache_dir,
        size=size,
        quality=quality,
        **recipe_kwargs,
    )


def _retry_thumbnail_with_working_copy(
    db, photo, source_path, cache_dir, size, quality, recipe, vireo_dir,
):
    if not photo or not recipe or not vireo_dir:
        return None
    if os.path.splitext(source_path or "")[1].lower() not in RAW_EXTENSIONS:
        return None
    wc_path = _working_copy_path_if_satisfies(
        photo, recipe, size, vireo_dir, thumbnail_tolerance=True,
    )
    if not wc_path or os.path.abspath(wc_path) == os.path.abspath(source_path):
        return None
    photo_id = _photo_value(photo, "id")
    log.info(
        "Thumbnail RAW decode failed for photo %s; falling back to JPEG "
        "working copy",
        photo_id,
    )
    file_mtime = _photo_value(photo, "file_mtime")
    if file_mtime is not None and photo_id is not None and hasattr(db, "conn"):
        with contextlib.suppress(Exception):
            db.conn.execute(
                "UPDATE photos SET"
                " working_copy_failed_at=datetime('now'),"
                " working_copy_failed_mtime=?,"
                " working_copy_failed_source='source'"
                " WHERE id=?",
                (file_mtime, photo_id),
            )
            db.conn.commit()
    recipe_kwargs = {"recipe": recipe, "native_size": _recipe_source_dimensions(photo)}
    return generate_thumbnail(
        photo_id,
        wc_path,
        cache_dir,
        size=size,
        quality=quality,
        **recipe_kwargs,
    )


def generate_thumbnail(
    photo_id, source_path, cache_dir, size=THUMB_SIZE, quality=85, recipe=None,
    raw_decode=None, min_source_size=None, native_size=None, cache_name=None,
):
    """Generate a JPEG thumbnail for a photo.

    Args:
        photo_id: database photo id (used as filename)
        source_path: path to the original image file
        cache_dir: directory to store thumbnails
        size: max dimension in pixels
        quality: JPEG quality (1-95)
        raw_decode: optional RAW decode mode forwarded to ``load_image``.
            Defaults to ``None``, which uses ``load_image``'s default
            (RAW_DECODE_JPEG_FIRST). Pass
            ``RAW_DECODE_PRESERVE_HIGHLIGHTS`` when regenerating an
            edited RAW thumbnail so the demosaic matches the preview /
            export pipeline instead of falling back to the embedded
            camera JPEG's clipped highlights.
        min_source_size: optional ``(width, height)`` for the image loaded
            before applying ``recipe``. If the RAW loader falls back to an
            embedded preview smaller than either axis, generation returns
            ``None`` so callers can retry a full-size companion JPEG.
        native_size: optional orientation-corrected native ``(width, height)``
            of the photo (see ``render_source.recipe_source_dimensions``),
            used to scale the recipe's detail pass (sharpen/NR kernels) to
            this render's resolution.
        cache_name: optional cache filename. The default remains
            ``{photo_id}.jpg``; paired-source views use a distinct filename
            so switching between RAW and JPEG can never return pixels cached
            for the other source.

    Returns:
        path to the thumbnail file, or None on failure
    """
    thumb_path = os.path.join(cache_dir, cache_name or f"{photo_id}.jpg")

    if os.path.exists(thumb_path):
        return thumb_path

    load_max_size = None if recipe and recipe.get("crop") else size
    load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
    img = load_image(source_path, max_size=load_max_size, **load_kwargs)
    if img is None:
        log.warning("Could not load image for thumbnail: %s", source_path)
        return None
    if min_source_size:
        expected_w, expected_h = min_source_size
        if not thumbnail_source_dimensions_are_acceptable(
            img.size[0], img.size[1], expected_w, expected_h,
        ):
            log.info(
                "Thumbnail source for photo %s is undersized (%dx%d, "
                "expected %dx%d): %s",
                photo_id, img.size[0], img.size[1],
                expected_w, expected_h, source_path,
            )
            img.close()
            return None
    if recipe:
        import local_masks
        from image_edits import apply_recipe_to_loaded_image
        # cache_dir is <vireo_dir>/thumbnails, the same root derivation the
        # app uses (dirname of THUMB_CACHE_DIR), so snapshots resolve here
        # without threading vireo_dir through every caller.
        img = apply_recipe_to_loaded_image(
            img, recipe, max_size=size, native_size=native_size,
            local_mask=local_masks.load_snapshot(
                os.path.dirname(os.path.abspath(cache_dir)), photo_id, recipe,
            ),
        )

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
        if recipe:
            recipe_kwargs["native_size"] = _recipe_source_dimensions(
                source_photo
            )
        # Derive the decode mode from the primary photo's extension so
        # edited RAWs that fall through to the original source path are
        # demosaiced with highlight preservation, matching the preview /
        # export pipeline rather than the default JPEG-first decode.
        raw_decode_kwargs = {}
        if recipe and os.path.splitext(
            source_photo["filename"]
        )[1].lower() in RAW_EXTENSIONS:
            raw_decode_kwargs["raw_decode"] = RAW_DECODE_PRESERVE_HIGHLIGHTS
        min_source_size = None
        if (
            recipe
            and os.path.splitext(source_path)[1].lower() in RAW_EXTENSIONS
        ):
            load_max_size = None if recipe.get("crop") else thumb_size
            min_source_size = _scaled_recipe_source_dimensions(
                source_photo, load_max_size,
            )
        result_path = generate_thumbnail(
            photo["id"],
            source_path,
            cache_dir,
            size=thumb_size,
            quality=thumb_quality,
            min_source_size=min_source_size,
            **recipe_kwargs,
            **raw_decode_kwargs,
        )
        if (
            result_path is None
            and recipe
            and os.path.splitext(source_path)[1].lower() in RAW_EXTENSIONS
        ):
            result_path = _retry_thumbnail_with_companion(
                db,
                source_photo,
                source_path,
                cache_dir,
                thumb_size,
                thumb_quality,
                recipe,
                folders.get(source_photo["folder_id"]),
            )
        if (
            result_path is None
            and recipe
            and os.path.splitext(source_path)[1].lower() in RAW_EXTENSIONS
        ):
            result_path = _retry_thumbnail_with_working_copy(
                db,
                source_photo,
                source_path,
                cache_dir,
                thumb_size,
                thumb_quality,
                recipe,
                vireo_dir,
            )
        if result_path is not None:
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
