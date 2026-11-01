"""Non-destructive photo edit recipes and rendering helpers."""

from __future__ import annotations

from edit_recipe import (
    _ADJUSTMENT_RANGES,
    _DETAIL_KEYS,
    _OPTICS_KEYS,
    _PRESENCE_KEYS,
    SHARPEN_RADIUS_DEFAULT,
    normalize_recipe,
)
from PIL import Image

try:
    from .float_image import FloatImage
    from .optics import apply_optics, distort_image
    from .render_fingerprint import record_render_source
except ImportError:
    from float_image import FloatImage
    from optics import apply_optics, distort_image
    from render_fingerprint import record_render_source

# Bump whenever the rendering math in this module or `tone.py` changes
# in a way that produces different output bytes for the same recipe. Cached
# previews/thumbnails are keyed by (photo_id, size) — they have no recipe hash,
# so without this version a deploy that changes the math keeps serving the old
# bytes until each recipe is touched again. `app._migrate_edit_math_render_caches`
# reads `db_meta["edit_math_version"]` at startup and purges stale renders when
# it lags behind this constant.
#
# History:
#   1 — original gamma-encoded sRGB multiply with hard clip at white.
#   2 — linear-light pipeline with gated highlight shoulder.
#   3 — expanded tone controls: highlights/shadows/whites/blacks/vibrance.
#   4 — RAW edit renders demosaic with auto-bright off + highlight blending.
#   5 — monotonic, linear-luminance range curves with chroma preservation.
#   6 — scene-linear RAW decode and floating-point render/export buffers.
#   7 — edge-aware Shadows/Highlights with local contrast preservation.
#   8 — camera-aware denoising.
#   9 — manual lens corrections and edge-aware defringing.
#  10 — Shadows/Highlights as scene-linear local exposure before the shoulder.
EDIT_MATH_VERSION = 10

# Tile budget for the tone pass, in pixels. Bounds peak memory on
# full-resolution originals/exports; overridable in tests to force many tiles.
_ADJUST_TILE_PIXELS = 4_000_000
_ADVANCED_COLOR_TILE_PIXELS = 1_000_000


def _adjust_rows_per_tile(width, *, spatial, color_ops):
    """Rows per strip of the tiled tone pass."""
    # Bound working memory on full-resolution originals/exports (45MP+).
    # Every operation is per-pixel (Shadows/Highlights sample a base built
    # once over the whole frame), so strips need no overlap. Shadows/
    # Highlights, HSL conversion and curve indexing keep several additional
    # float planes alive; smaller strips bound their peak memory.
    tile_budget = _ADJUST_TILE_PIXELS
    if spatial or color_ops:
        tile_budget = min(tile_budget, _ADVANCED_COLOR_TILE_PIXELS)
    return max(1, tile_budget // max(1, width))


def _promote_to_rgb(img):
    """Convert ``img`` to RGB, or RGBA when it carries any transparency."""
    has_alpha = "A" in img.getbands() or "transparency" in img.info
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGBA" if has_alpha else "RGB")
    elif img.mode == "RGB" and "transparency" in img.info:
        img = img.convert("RGBA")
    return img


def _adjusted_image(output, img, floating):
    """Wrap the tone pass's output buffer in the source's image type."""
    if floating:
        return FloatImage(output, encoding="srgb")
    if img.mode == "RGBA":
        return Image.fromarray(output, "RGBA")
    return Image.fromarray(output, "RGB")


def _apply_adjustments(
    img, adjustments, local_weight=None, local_subject=None,
    local_background=None, range_sigma=0.0,
):
    """Apply tonal adjustments to a PIL image via the linear tone pipeline.

    Promotes the image to RGB(A), runs the shared tone pipeline in strips,
    and merges alpha back unchanged. ``range_sigma`` is the Shadows/Highlights
    base blur in this image's pixels; the base is built once over the whole
    frame, so every strip samples what a whole-frame render would.
    """
    import numpy as np

    try:
        from .tone import LocalAdjustments, RangeBase, ToneAdjustments, apply_adjustments
    except ImportError:
        from tone import LocalAdjustments, RangeBase, ToneAdjustments, apply_adjustments

    img = _promote_to_rgb(img)

    controls = ToneAdjustments.from_mapping(adjustments)

    spatial = controls.highlights or controls.shadows or (
        local_weight is not None and any(
            region.get("shadows") or region.get("highlights")
            for region in (local_subject or {}, local_background or {})
        )
    )

    floating = isinstance(img, FloatImage)
    src = np.asarray(img)  # view onto the source buffer
    height, width = src.shape[:2]
    channels = src.shape[2]
    input_linear = floating and img.encoding == "linear"
    output = np.empty((height, width, channels), dtype=np.float32 if floating else np.uint8)
    range_base = (
        RangeBase.build(
            src, range_sigma, exposure=controls.exposure,
            white_balance=controls.white_balance, input_linear=input_linear,
            value_scale=1.0 if floating else 1.0 / 255.0,
        )
        if spatial and range_sigma else None
    )

    rows_per_tile = _adjust_rows_per_tile(
        width, spatial=spatial,
        color_ops=(
            controls.tone_curve or controls.point_curves or controls.point_color
            or controls.hsl or controls.color_grading
        ),
    )
    for top in range(0, height, rows_per_tile):
        bottom = min(top + rows_per_tile, height)
        tile = src[top:bottom].astype(np.float32)
        if not floating:
            tile /= 255.0
        adj = apply_adjustments(
            tile[..., :3],
            controls,
            local=(
                LocalAdjustments(
                    weight=local_weight[top:bottom],
                    subject=local_subject,
                    background=local_background,
                )
                if local_weight is not None else None
            ),
            input_linear=input_linear,
            range_base=range_base,
            origin=(top, 0),
        )
        output[top:bottom, :, :3] = (
            adj if floating else np.clip(adj * 255.0 + 0.5, 0, 255).astype(np.uint8)
        )
        if channels == 4:
            # Alpha passes through unchanged (round-trip uint8->float->uint8 is
            # identity for 8-bit values).
            output[top:bottom, :, 3] = src[top:bottom, :, 3]

    return _adjusted_image(output, img, floating)


def _apply_geometry(image, normalized, lens_profile=None):
    """Apply the recipe's geometry ops (rotate/flip/straighten/crop).

    Used for both the photo and the local-adjustment mask so their pixels
    stay aligned through every transform.
    """
    result = distort_image(image, (normalized.get("adjustments") or {}).get("distortion", 0), lens_profile)

    rotation = normalized.get("rotation", 0)
    if rotation:
        # PIL rotates counter-clockwise; photo-editor rotation controls are
        # conventionally clockwise.
        result = result.rotate(-rotation, expand=True)

    flip = normalized.get("flip") or {}
    if flip.get("horizontal"):
        result = result.transpose(Image.Transpose.FLIP_LEFT_RIGHT)
    if flip.get("vertical"):
        result = result.transpose(Image.Transpose.FLIP_TOP_BOTTOM)

    straighten = normalized.get("straighten", 0)
    if straighten:
        result = result.rotate(
            -straighten,
            resample=Image.Resampling.BICUBIC,
            expand=False,
        )

    crop = normalized.get("crop")
    if crop:
        iw, ih = result.size
        left = int(round(crop["x"] * iw))
        top = int(round(crop["y"] * ih))
        right = int(round((crop["x"] + crop["w"]) * iw))
        bottom = int(round((crop["y"] + crop["h"]) * ih))
        right = max(left + 1, min(iw, right))
        bottom = max(top + 1, min(ih, bottom))
        result = result.crop((left, top, right, bottom))

    return result


_LOCAL_TONE_KEYS = frozenset(
    {"exposure", "highlights", "shadows", "contrast", "saturation"}
)


def _local_region_deltas(local, keys):
    """Split a normalized local section into (subject, background) dicts
    restricted to ``keys``."""
    subject = {}
    background = {}
    for entry in (local or {}).get("regions") or []:
        target = subject if entry["region"] == "subject" else background
        for name, value in entry["adjustments"].items():
            if name in keys:
                target[name] = value
    return subject, background


def _fit_mask_to_source(mask, size):
    """Uniform-scale a snapshot mask onto the loaded source, or None.

    None means the mask cannot be trusted to line up (aspect disagreement
    beyond tolerance — e.g. an embedded-preview-crop RAW) and the local pass
    must be disabled rather than misaligned.
    """
    try:
        from .local_masks import ASPECT_TOLERANCE
    except ImportError:
        from local_masks import ASPECT_TOLERANCE

    mask_w, mask_h = mask.size
    target_w, target_h = size
    if min(mask_w, mask_h, target_w, target_h) <= 0:
        return None
    target_ar = target_w / target_h
    if abs(mask_w / mask_h - target_ar) / target_ar > ASPECT_TOLERANCE:
        return None
    if mask.size != size:
        mask = mask.resize(size, Image.Resampling.BILINEAR)
    return mask


def _feathered_weight(mask_img, sigma):
    """Mask image -> [0,1] float weight map, Gaussian-feathered by sigma."""
    import numpy as np

    arr = np.asarray(mask_img, dtype=np.float32) / 255.0
    if sigma > 0.3:
        try:
            from .detail import _gaussian_blur
        except ImportError:
            from detail import _gaussian_blur
        arr = _gaussian_blur(arr, sigma)
    return np.clip(arr, 0.0, 1.0)


def _apply_recipe_impl(
    img, normalized, local_mask, native_size, detail_scale=None, lens_profile=None,
):
    """Geometry + weighted tone. Returns (image, geometry-transformed mask).

    The returned mask is None whenever the local pass is disabled (no local
    section, no usable mask) — callers must then skip local detail too, so a
    degraded render fails toward "no local edits", never toward applying a
    region edit to the whole frame.

    ``detail_scale`` overrides the scale used to convert ``local.mask.feather``
    (native pixels) into a blur sigma for the tone pass. Callers that also
    override the scale for the detail pass (e.g. the edit-preview endpoint,
    which strips crop from the recipe but wants the saved-render scale) must
    pass the same override here — otherwise the tone falloff uses the scale
    computed from the crop-stripped recipe while detail uses the saved-render
    scale, and preview mask geometry disagrees with the saved output.
    """
    local = normalized.get("local")
    fitted = None
    if local and local_mask is not None:
        fitted = _fit_mask_to_source(local_mask, img.size)
        if fitted is None:
            import logging
            logging.getLogger(__name__).warning(
                "Local-adjustment mask does not fit the render source "
                "(size %s vs mask %s); local pass disabled for this render",
                img.size, local_mask.size,
            )

    # Float geometry returns new buffers/views and the tone pass allocates its
    # output; avoid copying a 500+ MB source only to immediately replace it.
    adjustments = normalized.get("adjustments") or {}
    corrected = apply_optics(img, adjustments, lens_profile)
    result = _apply_geometry(corrected if isinstance(corrected, FloatImage) else corrected.copy(), normalized, lens_profile)
    mask_geo = _apply_geometry(fitted, normalized, lens_profile) if fitted is not None else None

    tone_adjustments = {
        k: v for k, v in adjustments.items()
        if k not in _DETAIL_KEYS | _PRESENCE_KEYS | _OPTICS_KEYS
    }
    subject_tone, background_tone = (
        _local_region_deltas(local, _LOCAL_TONE_KEYS)
        if mask_geo is not None else ({}, {})
    )
    # Tone runs at loaded resolution, before the final output resize. The
    # Shadows/Highlights base scales with the loaded full frame (before crop),
    # never with the detail override, which describes output pixels.
    range_sigma = _range_sigma_for(img)
    scale = (
        detail_scale
        if detail_scale is not None
        else detail_render_scale(result.size, native_size, normalized)
    )
    if subject_tone or background_tone:
        feather = (local["mask"].get("feather") or 0.0) * scale
        weight = _feathered_weight(mask_geo, feather)
        result = _apply_adjustments(
            result, tone_adjustments,
            local_weight=weight,
            local_subject=subject_tone,
            local_background=background_tone,
            range_sigma=range_sigma,
        )
    elif tone_adjustments or isinstance(result, FloatImage):
        result = _apply_adjustments(result, tone_adjustments, range_sigma=range_sigma)

    return result, mask_geo


def _range_sigma_for(source):
    """Shadows/Highlights base blur, in pixels of the loaded ``source`` frame."""
    try:
        from .tone import range_sigma
    except ImportError:
        from tone import range_sigma
    return range_sigma(max(source.size))


def apply_recipe(img, recipe, local_mask=None, native_size=None):
    """Apply a normalized edit recipe to a PIL image and return a new image.

    ``local_mask`` is the recipe's edit-mask snapshot (PIL 'L', source
    working space — see local_masks.load_snapshot); without it any local
    regions in the recipe are skipped. Presence and detail passes (global
    and local) run in :func:`apply_recipe_to_loaded_image`, not here.
    """
    normalized = normalize_recipe(recipe)
    if normalized is None:
        return _apply_adjustments(img, {}) if isinstance(img, FloatImage) else img
    result, _ = _apply_recipe_impl(img, normalized, local_mask, native_size)
    return result


def detail_render_scale(rendered_size, native_size, recipe):
    """Return output pixels per native pixel for a recipe render.

    ``rendered_size`` is the final image's (width, height); ``native_size`` is
    the photo's orientation-corrected native (width, height), or None when
    unknown (scale falls back to 1.0 — apply detail as authored). The scale
    compares long edges against what the recipe would render at native
    resolution: right-angle rotation swaps the axes a crop applies to, crop
    shrinks them, and straighten keeps dimensions.
    """
    if not native_size:
        return 1.0
    try:
        native_w, native_h = (float(v) for v in native_size)
    except (TypeError, ValueError):
        return 1.0
    if native_w <= 0 or native_h <= 0:
        return 1.0
    if (recipe or {}).get("rotation") in (90, 270):
        native_w, native_h = native_h, native_w
    crop = (recipe or {}).get("crop")
    if crop:
        native_long = max(
            float(crop["w"]) * native_w, float(crop["h"]) * native_h
        )
    else:
        native_long = max(native_w, native_h)
    if native_long <= 0:
        return 1.0
    return max(rendered_size) / native_long


def _combine_detail(adjustments, region):
    """Effective detail params for one region: global + delta, clamped.

    ``sharpen_radius`` is an absolute override (a kernel size, not a
    strength), falling back to the global radius.
    """
    def clamped(name):
        lo, hi = _ADJUSTMENT_RANGES[name]
        total = float(adjustments.get(name, 0.0)) + float(region.get(name, 0.0))
        return min(hi, max(lo, total))

    return {
        "sharpen": clamped("sharpen"),
        "sharpen_radius": float(
            region.get(
                "sharpen_radius",
                adjustments.get("sharpen_radius", SHARPEN_RADIUS_DEFAULT),
            )
        ),
        "noise_reduction": clamped("noise_reduction"),
    }


def _camera_denoise_kwargs(adjustments, camera_metadata):
    """``apply_detail`` keywords for camera-aware denoising, or ``{}``."""
    if adjustments.get("denoise_mode") == "camera":
        try:
            from .camera_denoise import resolve_profile
        except ImportError:
            from camera_denoise import resolve_profile
        return {"denoise_mode": "camera", "noise_profile": resolve_profile(camera_metadata)}
    return {}


def _apply_local_detail(
    result, adjustments, local, mask_geo, subject_detail, background_detail,
    scale, denoise_kwargs,
):
    """Run the subject and background detail branches and blend them."""
    import numpy as np

    try:
        from .detail import apply_detail
    except ImportError:
        from detail import apply_detail

    subject_params = _combine_detail(adjustments, subject_detail)
    background_params = _combine_detail(adjustments, background_detail)
    if subject_params == background_params:
        if subject_params["sharpen"] or subject_params["noise_reduction"]:
            result = apply_detail(result, scale=scale, **subject_params, **denoise_kwargs)
        return result
    subject_out = apply_detail(result, scale=scale, **subject_params, **denoise_kwargs)
    background_out = apply_detail(result, scale=scale, **background_params, **denoise_kwargs)
    feather = (local["mask"].get("feather") or 0.0) * scale
    weight = _feathered_weight(
        mask_geo.resize(subject_out.size, Image.Resampling.BILINEAR),
        feather,
    )[..., None]
    subject_arr = np.asarray(subject_out).astype(np.float32)
    background_arr = np.asarray(background_out).astype(np.float32)
    blended = subject_arr.copy()
    blended[..., :3] = (
        subject_arr[..., :3] * weight
        + background_arr[..., :3] * (1.0 - weight)
    )
    if isinstance(subject_out, FloatImage):
        return FloatImage(blended, encoding="srgb")
    out8 = np.clip(blended + 0.5, 0, 255).astype(np.uint8)
    if out8.shape[-1] == 4:
        # Alpha passes through unchanged (identical in both branches).
        out8[..., 3] = np.asarray(subject_out)[..., 3]
    return Image.fromarray(out8, subject_out.mode)


def apply_recipe_to_loaded_image(
    img, recipe, max_size=None, native_size=None, detail_scale=None,
    local_mask=None, camera_metadata=None,
):
    """Apply edits, constrain the long edge, then run presence and detail.

    Detail ops (sharpen/NR) are neighborhood filters authored in native
    pixels, so they run last — at output resolution, with kernels scaled by
    ``detail_render_scale`` — approximating the full-resolution render
    downscaled. Callers that know the photo's native dimensions pass them via
    ``native_size`` (see render_source.recipe_source_dimensions); callers
    rendering a recipe with local regions pass the loaded snapshot via
    ``local_mask`` (see local_masks.load_snapshot) — without it any local
    regions are skipped entirely.

    ``camera_metadata`` is the destination photo row (or promoted camera
    fields/grouped EXIF), kept separate from the reusable recipe. Camera-aware
    denoising resolves camera and ISO on each render; missing metadata uses
    the image-only estimate.

    ``detail_scale`` overrides the scale computed from this call's recipe.
    Use it when rendering a modified recipe (e.g. the edit-preview endpoint
    strips crop to show the whole frame) so the detail pass still matches
    what the unmodified recipe's saved render would produce — otherwise a
    tighter crop scales sharpen/NR up in the saved output but not in the
    preview, and the two disagree for cropped detail edits.

    Local detail runs as a two-branch blend: the detail pass executes once
    per distinct (global + region delta) parameter set on the tone output,
    and the outputs blend by the feathered weight map. A single pass can't
    represent two regions (NR mutates the image before sharpening reads
    it), and composing each branch with the global baseline guarantees
    whole-photo sharpen/NR is never dropped when a local delta is added.
    """
    normalized = normalize_recipe(recipe)
    lens_profile = _lens_profile_for_image(img, normalized, camera_metadata)
    if normalized is None:
        result = _apply_adjustments(img, {}) if isinstance(img, FloatImage) else img
        mask_geo = None
    else:
        result, mask_geo = _apply_recipe_impl(
            img, normalized, local_mask, native_size,
            detail_scale=detail_scale,
            lens_profile=lens_profile,
        )
    if max_size and max_size > 0 and max(result.size) > max_size:
        result.thumbnail((max_size, max_size), resample=Image.Resampling.LANCZOS)

    adjustments = (normalized or {}).get("adjustments") or {}
    local = (normalized or {}).get("local")
    scale = (
        detail_scale
        if detail_scale is not None
        else detail_render_scale(result.size, native_size, normalized)
    )
    denoise_kwargs = _camera_denoise_kwargs(adjustments, camera_metadata)

    # Global presence runs before the two local detail branches so neither
    # branch can discard it when subject/background sharpening is active.
    presence = {key: adjustments[key] for key in _PRESENCE_KEYS if adjustments.get(key)}
    if presence:
        try:
            from .presence import apply_presence
        except ImportError:
            from presence import apply_presence
        result = apply_presence(result, scale=scale, **presence)

    result = _apply_render_detail(result, adjustments, local, mask_geo, scale, denoise_kwargs)
    if local and local.get("background_denoise") and mask_geo is not None:
        from background_denoise import apply_background_denoise
        weight = local_weight_map(
            local_mask, img.size, normalized, native_size, detail_scale=scale,
            render_size=result.size, lens_profile=lens_profile,
        )
        if weight is not None:
            result = apply_background_denoise(
                result, weight, local["background_denoise"], scale, camera_metadata,
            )
    return record_render_source(result, img, normalized)


def _apply_render_detail(result, adjustments, local, mask_geo, scale, denoise_kwargs):
    """Apply existing global/local detail before optional background smoothing."""
    subject_detail, background_detail = (
        _local_region_deltas(local, _DETAIL_KEYS)
        if (local and mask_geo is not None) else ({}, {})
    )
    if subject_detail or background_detail:
        return _apply_local_detail(
            result, adjustments, local, mask_geo, subject_detail,
            background_detail, scale, denoise_kwargs,
        )

    sharpen = adjustments.get("sharpen", 0.0)
    noise_reduction = adjustments.get("noise_reduction", 0.0)
    if sharpen or noise_reduction:
        try:
            from .detail import apply_detail
        except ImportError:
            from detail import apply_detail

        result = apply_detail(
            result,
            sharpen=sharpen,
            sharpen_radius=adjustments.get(
                "sharpen_radius", SHARPEN_RADIUS_DEFAULT
            ),
            noise_reduction=noise_reduction,
            scale=scale,
            **denoise_kwargs,
        )
    return result


def local_weight_map(
    local_mask, source_size, recipe, native_size=None, detail_scale=None,
    feather=None, render_size=None, camera_metadata=None, lens_profile=None,
):
    """The local-adjustment weight map exactly as the renderer computes it.

    For overlay/preview consumers: fit the snapshot to the render source,
    ride the recipe's geometry, feather at this resolution. Returns a float
    [0,1] array, or None when the recipe has no local section or the mask
    cannot be trusted to line up (same disable conditions as rendering).

    ``detail_scale`` overrides the scale used to convert the recipe's
    feather amount into pixels. Use it when the caller has already
    stripped crop from ``recipe`` to align with an uncropped preview
    (the edit-mask-preview endpoint does this) so the feather still
    matches the halo the saved cropped render will actually produce —
    otherwise the overlay recomputes the scale from the crop-stripped
    recipe and disagrees with what /edit-preview passes to
    :func:`apply_recipe_to_loaded_image`.

    ``feather`` previews a mask the recipe does not reference yet (the
    editor's Show Mask before any local slider moves): when given, it
    replaces the recipe's ``local.mask.feather`` and the recipe needs no
    local section. With background smoothing enabled this returns its
    subject-protection weight, including filled holes and the outward
    transition. ``render_size`` resizes geometry before computing that
    transition, matching the final detail pass. ``lens_profile`` supplies
    the image render's already-resolved calibration; otherwise metadata
    resolves it. Callers rendering from JPEGs omit both to avoid correcting
    the mask when the image itself skipped automatic lens correction.
    """
    normalized = normalize_recipe(recipe) or {}
    local = normalized.get("local")
    if local_mask is None or (not local and feather is None):
        return None
    protected = bool(local and local.get("background_denoise"))
    if protected:
        from background_denoise import protection_mask
        local_mask = protection_mask(local_mask, corrected=local["mask"].get("corrected", False))
    fitted = _fit_mask_to_source(local_mask, source_size)
    if fitted is None:
        return None
    if lens_profile is None:
        lens_profile = _resolve_lens_profile(normalized, camera_metadata)
    mask_geo = _apply_geometry(fitted, normalized, lens_profile)
    if render_size and mask_geo.size != render_size:
        mask_geo = mask_geo.resize(render_size, Image.Resampling.BILINEAR)
    scale = (
        detail_scale
        if detail_scale is not None
        else detail_render_scale(mask_geo.size, native_size, normalized)
    )
    if feather is None:
        feather = local["mask"].get("feather") or 0.0
    if protected:
        from background_denoise import protection_weight
        return protection_weight(mask_geo, scale, feather)
    return _feathered_weight(mask_geo, feather * scale)


def _resolve_lens_profile(recipe, metadata):
    if not ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        return None
    try:
        from .lens_profiles import resolve_profile
    except ImportError:
        from lens_profiles import resolve_profile
    profile = resolve_profile(metadata)
    return profile if profile["matched"] else None


def _lens_profile_for_image(image, recipe, metadata):
    # A RAW catalog row can render from an embedded/companion JPEG or working
    # copy. Those may already be corrected: never apply the calibration twice.
    if not isinstance(image, FloatImage) or image.encoding != "linear":
        return None
    return _resolve_lens_profile(recipe, metadata)
