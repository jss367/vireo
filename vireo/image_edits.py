"""Non-destructive photo edit recipes and rendering helpers."""

from __future__ import annotations

import copy
import json
import math
import re

from PIL import Image

SCHEMA_VERSION = 1

# Bump whenever the per-pixel rendering math in this module or `tone.py` changes
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
EDIT_MATH_VERSION = 4

_ADJUSTMENT_RANGES = {
    "exposure": (-5.0, 5.0),
    "highlights": (-100.0, 100.0),
    "shadows": (-100.0, 100.0),
    "whites": (-100.0, 100.0),
    "blacks": (-100.0, 100.0),
    "contrast": (-100.0, 100.0),
    "vibrance": (-100.0, 100.0),
    "saturation": (-100.0, 100.0),
    # Detail ops (see detail.py). Zero is a no-op like every other adjustment;
    # sharpen_radius is validated separately because its no-op is absence, not 0.
    "sharpen": (0.0, 100.0),
    "noise_reduction": (0.0, 100.0),
}

SHARPEN_RADIUS_RANGE = (0.5, 3.0)
SHARPEN_RADIUS_DEFAULT = 1.0

# Adjustment keys handled by the neighborhood pass in detail.py, not the
# per-pixel tone pipeline. A recipe containing only these must not run the
# tone pass at all (so a detail-only edit stays byte-exact outside detail).
_DETAIL_KEYS = frozenset({"sharpen", "sharpen_radius", "noise_reduction"})

# Local (mask-weighted) adjustments — see
# docs/plans/2026-07-03-local-adjustments-design.md. Region values are deltas
# on top of the global adjustments and share the global ranges; the v1 set
# deliberately excludes whites/blacks/vibrance/white-balance.
_LOCAL_REGION_NAMES = ("subject", "background")
_LOCAL_ADJUSTMENT_KEYS = frozenset({
    "exposure", "highlights", "shadows", "contrast", "saturation",
    "sharpen", "noise_reduction",
})
LOCAL_FEATHER_RANGE = (0.0, 200.0)
_LOCAL_MASK_REF_RE = re.compile(r"^[0-9a-f]{12}$")

_WHITE_BALANCE_RANGES = {
    "temperature": (-100.0, 100.0),
    "tint": (-100.0, 100.0),
}


class RecipeError(ValueError):
    """Raised when an edit recipe is malformed or unsupported."""


def normalize_recipe(recipe):
    """Validate and canonicalize a non-destructive image edit recipe.

    The crop rectangle uses normalized coordinates in the image space after
    rotation, flips, and straightening have been applied. Rotation is limited
    to right angles; straightening is a small arbitrary clockwise angle applied
    in-place so crop coordinates remain stable.
    """
    if recipe in (None, "", {}):
        return None
    if isinstance(recipe, str):
        try:
            recipe = json.loads(recipe)
        except (TypeError, ValueError) as exc:
            raise RecipeError("recipe must be valid JSON") from exc
    if not isinstance(recipe, dict):
        raise RecipeError("recipe must be an object")

    out = {"version": SCHEMA_VERSION}

    rotation = recipe.get("rotation", 0)
    if isinstance(rotation, bool) or not isinstance(rotation, int | float):
        raise RecipeError("rotation must be one of 0, 90, 180, or 270")
    if isinstance(rotation, float) and not rotation.is_integer():
        raise RecipeError("rotation must be one of 0, 90, 180, or 270")
    rotation = int(rotation)
    if rotation not in (0, 90, 180, 270):
        raise RecipeError("rotation must be one of 0, 90, 180, or 270")
    if rotation:
        out["rotation"] = rotation

    straighten = recipe.get("straighten", 0)
    if straighten in (None, ""):
        straighten = 0
    if isinstance(straighten, bool) or not isinstance(straighten, int | float):
        raise RecipeError("straighten must be numeric")
    straighten = float(straighten)
    if not math.isfinite(straighten) or straighten < -45.0 or straighten > 45.0:
        raise RecipeError("straighten must be between -45 and 45 degrees")
    if abs(straighten) > 1e-9:
        out["straighten"] = round(straighten, 4)

    flip = recipe.get("flip")
    if flip is None:
        flip = {}
    if not isinstance(flip, dict):
        raise RecipeError("flip must be an object")
    normalized_flip = {}
    for axis in ("horizontal", "vertical"):
        if axis not in flip:
            continue
        value = flip[axis]
        if not isinstance(value, bool):
            raise RecipeError(f"flip.{axis} must be a boolean")
        if value:
            normalized_flip[axis] = True
    if normalized_flip:
        out["flip"] = normalized_flip

    crop = recipe.get("crop")
    if crop is not None:
        if not isinstance(crop, dict):
            raise RecipeError("crop must be an object")
        raw_vals = []
        try:
            raw_vals = [crop["x"], crop["y"], crop["w"], crop["h"]]
        except (KeyError, TypeError, ValueError) as exc:
            raise RecipeError("crop must include numeric x, y, w, and h") from exc
        if any(isinstance(v, bool) for v in raw_vals):
            raise RecipeError("crop must include numeric x, y, w, and h")
        try:
            x, y, w, h = (float(v) for v in raw_vals)
        except (TypeError, ValueError) as exc:
            raise RecipeError("crop must include numeric x, y, w, and h") from exc
        vals = (x, y, w, h)
        if not all(math.isfinite(v) for v in vals):
            raise RecipeError("crop values must be finite")
        if x < 0 or y < 0 or w <= 0 or h <= 0 or x + w > 1 or y + h > 1:
            raise RecipeError("crop must fit inside normalized image bounds")
        # Treat an effectively full-frame crop as no-op.
        if not (
            abs(x) < 1e-9 and abs(y) < 1e-9
            and abs(w - 1) < 1e-9 and abs(h - 1) < 1e-9
        ):
            x = round(x, 6)
            y = round(y, 6)
            w = round(w, 6)
            h = round(h, 6)
            if x < 0 or y < 0 or w <= 0 or h <= 0 or x + w > 1 or y + h > 1:
                raise RecipeError("crop must fit inside normalized image bounds")
            out["crop"] = {
                "x": x,
                "y": y,
                "w": w,
                "h": h,
            }

    adjustments = recipe.get("adjustments")
    if adjustments is None:
        adjustments = {}
    if not isinstance(adjustments, dict):
        raise RecipeError("adjustments must be an object")
    normalized_adjustments = {}
    for name, (lo, hi) in _ADJUSTMENT_RANGES.items():
        raw = adjustments.get(name)
        if raw in (None, ""):
            continue
        if isinstance(raw, bool) or not isinstance(raw, int | float):
            raise RecipeError(f"{name} adjustment must be numeric")
        val = float(raw)
        if not math.isfinite(val) or val < lo or val > hi:
            raise RecipeError(f"{name} adjustment must be between {lo:g} and {hi:g}")
        if abs(val) > 1e-9:
            normalized_adjustments[name] = round(val, 6)

    # The USM radius only means something while sharpening is on, and its
    # default (1.0) is canonicalized to absence so an untouched radius slider
    # never dirties a recipe.
    if normalized_adjustments.get("sharpen"):
        raw_radius = adjustments.get("sharpen_radius")
        if raw_radius not in (None, ""):
            if isinstance(raw_radius, bool) or not isinstance(
                raw_radius, int | float
            ):
                raise RecipeError("sharpen_radius adjustment must be numeric")
            radius = float(raw_radius)
            lo, hi = SHARPEN_RADIUS_RANGE
            if not math.isfinite(radius) or radius < lo or radius > hi:
                raise RecipeError(
                    f"sharpen_radius adjustment must be between {lo:g} and {hi:g}"
                )
            if abs(radius - SHARPEN_RADIUS_DEFAULT) > 1e-9:
                normalized_adjustments["sharpen_radius"] = round(radius, 6)

    white_balance = adjustments.get("white_balance")
    if white_balance is None:
        white_balance = {
            key: adjustments[key]
            for key in _WHITE_BALANCE_RANGES
            if key in adjustments
        }
    if white_balance in (None, "", {}):
        white_balance = {}
    if not isinstance(white_balance, dict):
        raise RecipeError("white_balance adjustment must be an object")
    normalized_wb = {}
    for name, (lo, hi) in _WHITE_BALANCE_RANGES.items():
        raw = white_balance.get(name)
        if raw in (None, ""):
            continue
        if isinstance(raw, bool) or not isinstance(raw, int | float):
            raise RecipeError(f"white_balance.{name} adjustment must be numeric")
        val = float(raw)
        if not math.isfinite(val) or val < lo or val > hi:
            raise RecipeError(
                f"white_balance.{name} adjustment must be between {lo:g} and {hi:g}"
            )
        if abs(val) > 1e-9:
            normalized_wb[name] = round(val, 6)
    if normalized_wb:
        normalized_adjustments["white_balance"] = normalized_wb

    if normalized_adjustments:
        out["adjustments"] = normalized_adjustments

    local = recipe.get("local")
    if local not in (None, "", {}):
        normalized_local = _normalize_local(local)
        if normalized_local is not None:
            out["local"] = normalized_local

    return out if len(out) > 1 else None


def _normalize_local(local):
    """Validate and canonicalize the local (mask-weighted) section.

    Returns None when every region normalizes away — the shared mask never
    persists without at least one active region referencing it.
    """
    if not isinstance(local, dict):
        raise RecipeError("local must be an object")

    regions_in = local.get("regions")
    if regions_in in (None, ""):
        regions_in = []
    if not isinstance(regions_in, list):
        raise RecipeError("local.regions must be an array")

    normalized_regions = []
    seen = set()
    for entry in regions_in:
        if not isinstance(entry, dict):
            raise RecipeError("local.regions entries must be objects")
        region = entry.get("region")
        if region not in _LOCAL_REGION_NAMES:
            raise RecipeError("local region must be 'subject' or 'background'")
        if region in seen:
            raise RecipeError(f"duplicate local region '{region}'")
        seen.add(region)

        adjustments_in = entry.get("adjustments")
        if adjustments_in is None:
            adjustments_in = {}
        if not isinstance(adjustments_in, dict):
            raise RecipeError("local adjustments must be an object")
        normalized_adj = {}
        for name, raw in adjustments_in.items():
            if raw in (None, ""):
                continue
            if name == "sharpen_radius":
                # A region radius is an absolute override of the effective
                # branch radius (which may come from global sharpen), so it
                # is kept even without a region sharpen delta and its 1.0
                # value is meaningful — no default-drop like the global key.
                if isinstance(raw, bool) or not isinstance(raw, int | float):
                    raise RecipeError(
                        "sharpen_radius adjustment must be numeric"
                    )
                radius = float(raw)
                lo, hi = SHARPEN_RADIUS_RANGE
                if not math.isfinite(radius) or radius < lo or radius > hi:
                    raise RecipeError(
                        f"sharpen_radius adjustment must be between "
                        f"{lo:g} and {hi:g}"
                    )
                normalized_adj[name] = round(radius, 6)
                continue
            if name not in _LOCAL_ADJUSTMENT_KEYS:
                raise RecipeError(
                    f"{name} adjustment is not supported in local regions"
                )
            if isinstance(raw, bool) or not isinstance(raw, int | float):
                raise RecipeError(f"{name} adjustment must be numeric")
            val = float(raw)
            lo, hi = _ADJUSTMENT_RANGES[name]
            if not math.isfinite(val) or val < lo or val > hi:
                raise RecipeError(
                    f"{name} adjustment must be between {lo:g} and {hi:g}"
                )
            if abs(val) > 1e-9:
                normalized_adj[name] = round(val, 6)
        if normalized_adj:
            normalized_regions.append(
                {"region": region, "adjustments": normalized_adj}
            )

    if not normalized_regions:
        return None

    mask_in = local.get("mask")
    if not isinstance(mask_in, dict):
        raise RecipeError("local.mask is required when regions are present")
    ref = mask_in.get("ref")
    if not isinstance(ref, str) or not _LOCAL_MASK_REF_RE.match(ref):
        raise RecipeError(
            "local.mask.ref must be a 12-character lowercase hex id"
        )
    digest = mask_in.get("source_digest")
    if not isinstance(digest, str) or not digest.strip() or len(digest) > 128:
        raise RecipeError("local.mask.source_digest is required")
    normalized_mask = {"ref": ref, "source_digest": digest}

    feather = mask_in.get("feather", 0)
    if feather in (None, ""):
        feather = 0
    if isinstance(feather, bool) or not isinstance(feather, int | float):
        raise RecipeError("local.mask.feather must be numeric")
    feather = float(feather)
    lo, hi = LOCAL_FEATHER_RANGE
    if not math.isfinite(feather) or feather < lo or feather > hi:
        raise RecipeError(
            f"local.mask.feather must be between {lo:g} and {hi:g}"
        )
    if abs(feather) > 1e-9:
        normalized_mask["feather"] = round(feather, 4)

    normalized_regions.sort(key=lambda entry: entry["region"])
    return {"mask": normalized_mask, "regions": normalized_regions}


def recipe_to_json(recipe):
    """Return canonical JSON for a normalized recipe, or None for no-op."""
    normalized = normalize_recipe(recipe)
    if normalized is None:
        return None
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


# Row-tile budget for the tone pass, in pixels. Bounds peak memory on
# full-resolution originals/exports; overridable in tests to force many tiles.
_ADJUST_TILE_PIXELS = 4_000_000


def _apply_adjustments(img, adjustments):
    """Apply tonal adjustments to a PIL image via the linear tone pipeline.

    Bridges PIL <-> numpy: promotes the image to RGB(A), runs the shared
    per-pixel pipeline in :mod:`tone` (linear-light exposure/white balance with
    a highlight shoulder, then display-space tonal/color controls), and merges
    any alpha channel back unchanged.
    """
    import numpy as np

    try:
        from .tone import apply_adjustments
    except ImportError:
        from tone import apply_adjustments

    has_alpha = "A" in img.getbands() or "transparency" in img.info
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGBA" if has_alpha else "RGB")
    elif img.mode == "RGB" and "transparency" in img.info:
        img = img.convert("RGBA")

    exposure = adjustments.get("exposure", 0.0)
    white_balance = adjustments.get("white_balance")
    highlights = adjustments.get("highlights", 0.0)
    shadows = adjustments.get("shadows", 0.0)
    whites = adjustments.get("whites", 0.0)
    blacks = adjustments.get("blacks", 0.0)
    contrast = adjustments.get("contrast", 0.0)
    vibrance = adjustments.get("vibrance", 0.0)
    saturation = adjustments.get("saturation", 0.0)

    src = np.asarray(img)  # uint8, view onto the PIL buffer (no copy)
    height, width = src.shape[:2]
    channels = src.shape[2]
    out8 = np.empty((height, width, channels), dtype=np.uint8)

    # Process in row blocks so peak memory stays bounded on full-resolution
    # originals/exports (45MP+). The tone pass is strictly per-pixel, so tiling
    # is numerically identical to a single whole-frame pass. ~4M pixels per tile
    # keeps the transient float arrays to a few hundred MB.
    rows_per_tile = max(1, _ADJUST_TILE_PIXELS // max(1, width))
    for top in range(0, height, rows_per_tile):
        bottom = min(top + rows_per_tile, height)
        tile = src[top:bottom].astype(np.float32) / 255.0
        adj = apply_adjustments(
            tile[..., :3],
            exposure=exposure,
            white_balance=white_balance,
            highlights=highlights,
            shadows=shadows,
            whites=whites,
            blacks=blacks,
            contrast=contrast,
            vibrance=vibrance,
            saturation=saturation,
        )
        out8[top:bottom, :, :3] = np.clip(adj * 255.0 + 0.5, 0, 255).astype(
            np.uint8
        )
        if channels == 4:
            # Alpha passes through unchanged (round-trip uint8->float->uint8 is
            # identity for 8-bit values).
            out8[top:bottom, :, 3] = src[top:bottom, :, 3]

    if img.mode == "RGBA":
        return Image.fromarray(out8, "RGBA")
    return Image.fromarray(out8, "RGB")


def apply_recipe(img, recipe):
    """Apply a normalized edit recipe to a PIL image and return a new image."""
    normalized = normalize_recipe(recipe)
    if normalized is None:
        return img

    result = img.copy()

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

    adjustments = normalized.get("adjustments") or {}
    tone_adjustments = {
        k: v for k, v in adjustments.items() if k not in _DETAIL_KEYS
    }
    if tone_adjustments:
        result = _apply_adjustments(result, tone_adjustments)

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


def apply_recipe_to_loaded_image(
    img, recipe, max_size=None, native_size=None, detail_scale=None,
):
    """Apply edits, constrain the long edge, then run the detail pass.

    Detail ops (sharpen/NR) are neighborhood filters authored in native
    pixels, so they run last — at output resolution, with kernels scaled by
    ``detail_render_scale`` — approximating the full-resolution render
    downscaled. Callers that know the photo's native dimensions pass them via
    ``native_size`` (see render_source.recipe_source_dimensions).

    ``detail_scale`` overrides the scale computed from this call's recipe.
    Use it when rendering a modified recipe (e.g. the edit-preview endpoint
    strips crop to show the whole frame) so the detail pass still matches
    what the unmodified recipe's saved render would produce — otherwise a
    tighter crop scales sharpen/NR up in the saved output but not in the
    preview, and the two disagree for cropped detail edits.
    """
    normalized = normalize_recipe(recipe)
    result = apply_recipe(img, normalized)
    if max_size and max_size > 0 and max(result.size) > max_size:
        result.thumbnail((max_size, max_size), resample=Image.Resampling.LANCZOS)

    adjustments = (normalized or {}).get("adjustments") or {}
    sharpen = adjustments.get("sharpen", 0.0)
    noise_reduction = adjustments.get("noise_reduction", 0.0)
    if sharpen or noise_reduction:
        try:
            from .detail import apply_detail
        except ImportError:
            from detail import apply_detail

        scale = (
            detail_scale
            if detail_scale is not None
            else detail_render_scale(result.size, native_size, normalized)
        )
        result = apply_detail(
            result,
            sharpen=sharpen,
            sharpen_radius=adjustments.get(
                "sharpen_radius", SHARPEN_RADIUS_DEFAULT
            ),
            noise_reduction=noise_reduction,
            scale=scale,
        )
    return result


def copy_recipe(recipe):
    """Return a detached normalized recipe dict for API responses."""
    normalized = normalize_recipe(recipe)
    return copy.deepcopy(normalized)
