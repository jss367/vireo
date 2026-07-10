"""Single source of truth for edit-render source selection.

For an edited photo, several flows (preview, preview-warmer, edit-preview,
edited-original serving, thumbnail self-heal, export, external-editor and
iNaturalist hand-off, and the scanner's working-copy extraction) must answer
the same questions:

* Which file do we actually decode — the RAW primary, the companion JPEG, the
  working copy, or the original? (:func:`recipe_render_source`)
* After decoding, is the result big enough, or did libraw hand us a small
  embedded preview that a full-size companion JPEG should replace?
  (:func:`is_undersized` / :func:`companion_image_can_replace_raw_result`)
* Has this RAW already failed extraction for the current source mtime, so a
  request thread should not block on the same slow decode again?
  (:func:`has_current_working_copy_failure` / :func:`record_working_copy_failure`)

Historically each flow carried its own copy of these helpers across
``app.py``, ``pipeline_job.py``, ``thumbnails.py``, ``export.py`` and
``scanner.py``. The copies drifted (long-edge-only vs both-axis dimension
checks, ``+1px`` vs ``*0.99`` tolerances, EXIF-orientation axis swaps applied
in some copies but not others), so a fix in one flow never reached the
others. This module holds the canonical implementations; the per-flow modules
import them so a single change covers every flow.
"""

import contextlib
import json
import logging
import os
from datetime import UTC, datetime

from exif_orientation import orientation_swaps_axes
from image_loader import RAW_EXTENSIONS, get_canonical_image_path

log = logging.getLogger(__name__)

# Stored failure markers expire after this long so transient I/O / environment
# failures recover even when the source file itself is unchanged. Must match
# scanner.py's ``_FAILURE_RETRY_AFTER`` window.
WORKING_COPY_FAILURE_RETRY_SECONDS = 24 * 60 * 60

# PIL's numeric tag id for EXIF ``Orientation``.
EXIF_ORIENTATION_TAG = 274

# Sentinel so ``recipe_source_dimensions`` can distinguish "read exif_data from
# the photo row" from an explicit ``exif_data=None`` passed by export.py.
_UNSET = object()


def photo_value(photo, key):
    """Read ``key`` from a sqlite Row or a plain dict, returning None if absent."""
    try:
        return photo[key]
    except (KeyError, IndexError, TypeError):
        if hasattr(photo, "get"):
            return photo.get(key)
    return None


def exif_orientation(exif_data):
    """Return the EXIF ``Orientation`` value from stored metadata, or None.

    ``exif_data`` may be a JSON string (as stored on the photo row) or an
    already-parsed dict. Looks in the common metadata groups ExifTool emits
    before falling back to a top-level ``Orientation`` key.
    """
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


def recipe_source_dimensions(photo, exif_data=_UNSET):
    """Return the photo's (width, height) as ``load_image`` sees them.

    Stored width/height are the unrotated sensor axes; this normalizes them to
    display orientation (swapping for EXIF orientations 5-8) so callers compare
    against the orientation-corrected pixels ``load_image`` returns. Pass
    ``exif_data`` explicitly when it isn't on the photo row (export.py loads it
    separately); otherwise it's read from ``photo['exif_data']``.
    """
    try:
        width = int(photo_value(photo, "width") or 0)
        height = int(photo_value(photo, "height") or 0)
    except (TypeError, ValueError):
        return 0, 0
    if width <= 0 or height <= 0:
        return 0, 0
    if exif_data is _UNSET:
        exif_data = photo_value(photo, "exif_data")
    if orientation_swaps_axes(exif_orientation(exif_data)):
        return height, width
    return width, height


def scaled_recipe_source_dimensions(photo, max_size=None, exif_data=_UNSET):
    """Like :func:`recipe_source_dimensions` but scaled down to ``max_size``.

    Mirrors ``load_image``'s long-edge downscale so callers can compare a
    loaded image against the dimensions a same-``max_size`` decode would yield.
    """
    width, height = recipe_source_dimensions(photo, exif_data)
    if width <= 0 or height <= 0:
        return 0, 0
    if max_size:
        long_edge = max(width, height)
        if long_edge > max_size:
            scale = max_size / long_edge
            width = round(width * scale)
            height = round(height * scale)
    return width, height


def rendered_recipe_dimensions(width, height, recipe):
    """Return the rendered ``(width, height)`` after right-angle rotation and crop.

    Kept in floats to match the multiplicative crop shape callers use, and to
    let :func:`working_copy_satisfies_recipe_render` compare each axis
    separately — a long-edge-only compare accepts a truncated short edge
    (e.g. 6000x3376 for a 6000x4000 source) which drops content.
    """
    rotation = (recipe or {}).get("rotation", 0)
    if rotation in (90, 270):
        width, height = height, width
    crop = (recipe or {}).get("crop") if recipe else None
    if crop:
        return float(crop["w"]) * width, float(crop["h"]) * height
    return float(width), float(height)


def rendered_recipe_long_edge(width, height, recipe):
    """Return the rendered long edge after right-angle rotation and crop."""
    w, h = rendered_recipe_dimensions(width, height, recipe)
    return max(w, h)


def image_size_after_exif_orientation(img):
    """Return an opened image's (width, height) after EXIF transpose."""
    width, height = img.size
    orientation = None
    with contextlib.suppress(Exception):
        orientation = img.getexif().get(EXIF_ORIENTATION_TAG)
    if orientation_swaps_axes(orientation):
        return height, width
    return width, height


def is_undersized(width, height, expected_w, expected_h, *, abs_slack=1, rel_slack=0.0):
    """Return True when (width, height) falls short of the expected size.

    Compares *both* axes — a long-edge-only check accepts e.g. a 6000x3376
    embedded preview for a 6000x4000 source and silently drops short-edge
    content. The tolerance is explicit so callers can pick the slack their
    comparison needs without forking the logic:

    * ``abs_slack`` (default 1px) absorbs rounding between a RAW decoder's
      output and the stored dimensions — used by the request-path checks that
      compare a decoded preview against a full-size companion.
    * ``rel_slack`` (e.g. 0.01) absorbs libraw emitting the active image area a
      few pixels narrower than the full sensor — used by the scanner's
      RAW working-copy size gate so a valid extraction isn't marked failed.

    The more lenient (smaller) of the two thresholds wins so passing both is
    safe. Returns False when the expected size is unknown (<= 0).
    """
    if expected_w <= 0 or expected_h <= 0:
        return False
    thr_w = min(expected_w - abs_slack, expected_w * (1.0 - rel_slack))
    thr_h = min(expected_h - abs_slack, expected_h * (1.0 - rel_slack))
    return width < thr_w or height < thr_h


def image_is_smaller_than_expected(img, expected_w, expected_h, *, abs_slack=1, rel_slack=0.0):
    """:func:`is_undersized` applied to an opened image's size."""
    return is_undersized(
        img.size[0], img.size[1], expected_w, expected_h,
        abs_slack=abs_slack, rel_slack=rel_slack,
    )


def companion_image_can_replace_raw_result(
    companion_img, current_img, expected_w, expected_h,
):
    """Return True when the companion JPEG should replace a RAW decode result.

    When the expected size is known, the companion qualifies if it meets that
    size on both axes. Otherwise it must cover the current decode on both axes.
    """
    if companion_img is None:
        return False
    if expected_w > 0 and expected_h > 0:
        return not image_is_smaller_than_expected(
            companion_img, expected_w, expected_h,
        )
    if current_img is None:
        return True
    return (
        companion_img.size[0] >= current_img.size[0]
        and companion_img.size[1] >= current_img.size[1]
    )


def working_copy_satisfies_recipe_render(
    photo, recipe, max_size, vireo_dir, *, rel_slack=0.0,
):
    """Return True when the working copy is large enough for this recipe render.

    The working copy qualifies when its rendered dimensions (after the recipe's
    rotation/crop) cover the rendered original scaled to ``max_size`` on both
    axes — for a typical capped request that's just ``max_size`` on the long
    edge with the short edge scaled proportionally, but a native-resolution
    request (the editor's 100% zoom) needs the full original.

    Both axes are compared. A long-edge-only check accepts a working copy whose
    short edge is truncated (e.g. a failed RAW decode's 6000x3376 embedded
    preview for a 6000x4000 source) and silently drops that lost short-edge
    content into the cached edit render.
    """
    wc_rel = photo_value(photo, "working_copy_path")
    if not wc_rel:
        return False
    wc_path = os.path.join(vireo_dir, wc_rel)
    if not os.path.exists(wc_path):
        return False
    try:
        from PIL import Image as _PILImage
        with _PILImage.open(wc_path) as wc_img:
            wc_w, wc_h = image_size_after_exif_orientation(wc_img)
    except Exception:
        return False
    original_w, original_h = recipe_source_dimensions(photo)
    if original_w <= 0 or original_h <= 0:
        return False
    orig_render_w, orig_render_h = rendered_recipe_dimensions(
        original_w, original_h, recipe,
    )
    orig_render_long = max(orig_render_w, orig_render_h)
    if max_size and orig_render_long > max_size:
        scale = max_size / orig_render_long
        required_w = orig_render_w * scale
        required_h = orig_render_h * scale
    else:
        required_w = orig_render_w
        required_h = orig_render_h
    wc_render_w, wc_render_h = rendered_recipe_dimensions(wc_w, wc_h, recipe)
    # ``load_image(..., max_size=max_size)`` caps the long edge at ``max_size``
    # and scales the short edge proportionally. Compare what it would actually
    # produce from this working copy, not the raw WC render dims: a 6000x3376
    # embedded preview for a 6000x4000 source clears an unscaled compare
    # against 1024x683 (both axes exceed it), but a max_size=1024 render of
    # that WC is 1024x576 — its short edge falls short and the truncated
    # preview would otherwise get cached through the failed-RAW fallback.
    if max_size:
        wc_render_long = max(wc_render_w, wc_render_h)
        if wc_render_long > max_size:
            wc_scale = max_size / wc_render_long
            wc_render_w = wc_render_w * wc_scale
            wc_render_h = wc_render_h * wc_scale
    return not is_undersized(
        wc_render_w, wc_render_h, required_w, required_h,
        abs_slack=0, rel_slack=rel_slack,
    )


def path_satisfies_recipe_render(path, photo, recipe, max_size):
    """Return True when the file at ``path`` is large enough for this render."""
    original_w, original_h = recipe_source_dimensions(photo)
    if original_w <= 0 or original_h <= 0:
        return False
    try:
        from PIL import Image as _PILImage
        with _PILImage.open(path) as img:
            width, height = image_size_after_exif_orientation(img)
    except Exception:
        return False
    original_render_long = rendered_recipe_long_edge(original_w, original_h, recipe)
    required_long = (
        min(max_size, original_render_long) if max_size else original_render_long
    )
    return rendered_recipe_long_edge(width, height, recipe) >= required_long


def recipe_render_source(photo, recipe, max_size, vireo_dir, folders):
    """Pick the file to decode for an edited render. Returns (path, using_wc).

    ``using_wc`` is True when the returned path is the photo's working copy —
    callers use it to skip the RAW-failure gate (a working copy is already a
    decoded JPEG, not a fresh RAW decode that could block).

    Selection rules:

    * No recipe: the canonical path (working copy if present, else original).
    * RAW primaries with a recipe never short-circuit to the working copy —
      legacy working copies predate the highlight-preserving RAW decode and
      ``EDIT_MATH_VERSION``'s migration only purges preview/thumb caches, not
      working copies, so reusing one would apply the recipe to clipped bytes.
    * Non-RAW primaries may use the working copy when it satisfies the render.
    * The companion JPEG is used for a RAW primary only when the RAW source is
      offline or already marked failed for the current mtime; otherwise the RAW
      is decoded with highlight preservation.
    * The working copy is the last fallback when the original is offline.
    """
    if not vireo_dir:
        folder_path = folders.get(photo_value(photo, "folder_id"), "")
        return os.path.join(folder_path, photo_value(photo, "filename") or ""), False

    def _is_working_copy_path(path):
        wc_rel = photo_value(photo, "working_copy_path")
        if not path or not wc_rel:
            return False
        wc_path = (
            wc_rel if os.path.isabs(wc_rel) else os.path.join(vireo_dir, wc_rel)
        )
        return os.path.abspath(path) == os.path.abspath(wc_path)

    canonical = get_canonical_image_path(photo, vireo_dir, folders)
    if not recipe:
        return canonical, _is_working_copy_path(canonical)

    primary_is_raw = (
        os.path.splitext(photo_value(photo, "filename"))[1].lower() in RAW_EXTENSIONS
    )

    if not primary_is_raw and working_copy_satisfies_recipe_render(
        photo, recipe, max_size, vireo_dir,
    ):
        return canonical, _is_working_copy_path(canonical)

    folder_path = folders.get(photo_value(photo, "folder_id"))
    wc_rel = photo_value(photo, "working_copy_path")
    if not folder_path:
        if wc_rel:
            wc_path = os.path.join(vireo_dir, wc_rel)
            if os.path.exists(wc_path):
                return wc_path, True
        return "", False

    original_abs = os.path.join(folder_path, photo_value(photo, "filename"))
    source_failure_current = primary_is_raw and has_current_working_copy_failure(
        photo,
        vireo_dir,
        trust_existing_working_copy=False,
        live_source_path=original_abs,
        folder_path=folder_path,
    )
    companion_path = photo_value(photo, "companion_path")
    allow_companion = (
        not primary_is_raw
        or not os.path.exists(original_abs)
        or source_failure_current
    )
    if companion_path and allow_companion:
        companion = os.path.join(folder_path, companion_path)
        if os.path.exists(companion) and path_satisfies_recipe_render(
            companion, photo, recipe, max_size,
        ):
            return companion, True
    if source_failure_current and working_copy_satisfies_recipe_render(
        photo, recipe, max_size, vireo_dir, rel_slack=0.01,
    ):
        wc_path = os.path.join(vireo_dir, wc_rel)
        if os.path.exists(wc_path):
            return wc_path, True
    if not os.path.exists(original_abs) and wc_rel:
        wc_path = os.path.join(vireo_dir, wc_rel)
        if os.path.exists(wc_path):
            return wc_path, True
    return original_abs, False


def has_current_working_copy_failure(
    photo, vireo_dir=None, trust_existing_working_copy=True,
    live_source_path=None, folder_path=None,
):
    """Return True when this RAW already failed working-copy extraction.

    Missing thumbnail/preview requests normally self-heal by decoding the
    source. For RAW rows whose working-copy extraction already failed at the
    same source mtime, retrying that decode in a request thread can block the
    UI for minutes and then fail the same way. A present working copy is still
    authoritative for thumbnail/preview routes, but callers that have already
    rejected that copy as insufficient can opt into honoring the marker anyway.
    A stale ``working_copy_path`` whose file was deleted should not bypass a
    fresh failure marker. If a RAW+JPEG companion pair has a companion-source
    marker while both the companion and RAW source are currently available,
    allow request paths to try the RAW: scanner.py prefers companions for
    working-copy extraction, so that marker may not describe a RAW failure.
    Match scanner.py's stale-failure contract: a file replacement changes
    ``file_mtime`` and failures older than 24 hours are allowed to retry.
    """
    working_copy_path = photo_value(photo, "working_copy_path")
    if working_copy_path and trust_existing_working_copy:
        if not vireo_dir:
            return False
        wc_abs = (
            working_copy_path if os.path.isabs(working_copy_path)
            else os.path.join(vireo_dir, working_copy_path)
        )
        if os.path.exists(wc_abs):
            return False

    filename = photo_value(photo, "filename") or ""
    if os.path.splitext(filename)[1].lower() not in RAW_EXTENSIONS:
        return False

    companion_path = photo_value(photo, "companion_path")
    if companion_path and live_source_path and folder_path:
        companion_abs = os.path.join(folder_path, companion_path)
        failed_source = photo_value(photo, "working_copy_failed_source")
        if (
            failed_source != "source"
            and os.path.exists(live_source_path)
            and os.path.exists(companion_abs)
        ):
            return False

    failed_at = photo_value(photo, "working_copy_failed_at")
    failed_mtime = photo_value(photo, "working_copy_failed_mtime")
    file_mtime = photo_value(photo, "file_mtime")
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
    return age < WORKING_COPY_FAILURE_RETRY_SECONDS


def record_working_copy_failure(db, photo, source_path=None):
    """Persist a RAW extraction failure marker after a request-time retry.

    When :func:`has_current_working_copy_failure` returns False because the
    stored marker is older than ``WORKING_COPY_FAILURE_RETRY_SECONDS``, request
    paths are allowed to retry the slow RAW decode. If that retry still fails,
    refresh ``working_copy_failed_at``/``working_copy_failed_mtime`` so the next
    thumbnail/preview/original request fails fast again instead of repeating the
    expensive decode until the scanner runs. Mirrors the SQL the scanner writes
    on failure. No-op for non-RAW rows, rows without a recorded
    ``file_mtime``/``id``, source paths that are currently unavailable/offline,
    or source paths that aren't the original RAW (e.g. a corrupt working-copy
    JPEG) — a non-RAW decode failure isn't a RAW extraction failure and must not
    stamp the RAW marker. Recorded with ``working_copy_failed_source='source'``
    so companion-source bypasses do not ignore fresh RAW failures.
    """
    filename = photo_value(photo, "filename") or ""
    if os.path.splitext(filename)[1].lower() not in RAW_EXTENSIONS:
        return
    if source_path is not None:
        if os.path.splitext(source_path)[1].lower() not in RAW_EXTENSIONS:
            return
        if not os.path.exists(source_path):
            return

    file_mtime = photo_value(photo, "file_mtime")
    photo_id = photo_value(photo, "id")
    if file_mtime is None or photo_id is None:
        return

    try:
        db.conn.execute(
            "UPDATE photos SET working_copy_failed_at=datetime('now'),"
            " working_copy_failed_mtime=?,"
            " working_copy_failed_source='source'"
            " WHERE id=?",
            (file_mtime, photo_id),
        )
        db.conn.commit()
    except Exception:
        log.debug(
            "Could not refresh working_copy_failed marker for photo %s",
            photo_id, exc_info=True,
        )
