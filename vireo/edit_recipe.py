"""The edit-recipe schema: validation, normalization and canonical JSON.

A recipe is the non-destructive edit a photo carries (exposure, tone, color,
crop, local adjustments). This module owns its shape and value ranges and
nothing else: no pixels, no disk. ``image_edits`` renders recipes; the data
layer (``repositories/edits.py``) normalizes them before storing.
"""

from __future__ import annotations

import copy
import json
import math
import re
from typing import Any

SCHEMA_VERSION = 1

_ADJUSTMENT_RANGES = {
    "exposure": (-5.0, 5.0),
    "highlights": (-100.0, 100.0),
    "shadows": (-100.0, 100.0),
    "whites": (-100.0, 100.0),
    "blacks": (-100.0, 100.0),
    "contrast": (-100.0, 100.0),
    "vibrance": (-100.0, 100.0),
    "saturation": (-100.0, 100.0),
    "texture": (-100.0, 100.0),
    "clarity": (-100.0, 100.0),
    "dehaze": (-100.0, 100.0),
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
_DETAIL_KEYS = frozenset({"sharpen", "sharpen_radius", "noise_reduction", "denoise_mode"})
# Global spatial controls run in presence.py before local detail branches.
_PRESENCE_KEYS = frozenset({"texture", "clarity", "dehaze"})

# Local (mask-weighted) adjustments — see
# docs/plans/2026-07-03-local-adjustments-design.md. Region values are deltas
# on top of the global adjustments and share the global ranges; the v1 set
# deliberately excludes whites/blacks/vibrance/white-balance.
_LOCAL_REGION_NAMES = ("subject", "background")
_LOCAL_ADJUSTMENT_KEYS = frozenset({
    "exposure", "highlights", "shadows", "contrast", "saturation",
    "sharpen", "noise_reduction",
})
# Region values are deltas layered on top of the global adjustments, so
# sharpen and noise_reduction — whose globals are [0, 100] — need to accept
# negative deltas here (e.g. background sharpen -70 against global sharpen 70
# zeroes sharpen in the background). ``_combine_detail`` clamps the sum back
# into the global range before rendering.
_LOCAL_ADJUSTMENT_RANGES = {
    **_ADJUSTMENT_RANGES,
    "sharpen": (-100.0, 100.0),
    "noise_reduction": (-100.0, 100.0),
}
LOCAL_FEATHER_RANGE = (0.0, 200.0)
_LOCAL_MASK_REF_RE = re.compile(r"^[0-9a-f]{12}$")

_WHITE_BALANCE_RANGES = {
    "temperature": (-100.0, 100.0),
    "tint": (-100.0, 100.0),
}

# Advanced global colour controls. Tone-curve values are output levels at
# fixed input positions (0, 25, 50, 75, 100 percent), which makes the neutral
# curve explicit and lets the renderer use a deterministic five-point curve.
TONE_CURVE_DEFAULTS = {
    "black": 0.0,
    "shadows": 25.0,
    "midtones": 50.0,
    "highlights": 75.0,
    "white": 100.0,
}
HSL_COLOR_NAMES = (
    "red", "orange", "yellow", "green", "aqua", "blue", "purple", "magenta",
)
_HSL_KEYS = ("hue", "saturation", "luminance")
COLOR_GRADING_ZONES = ("shadows", "midtones", "highlights")


class RecipeError(ValueError):
    """Raised when an edit recipe is malformed or unsupported."""


def _number(value, path, lo, hi):
    """Validate a finite numeric recipe leaf and return a rounded float."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise RecipeError(f"{path} must be numeric")
    value = float(value)
    if not math.isfinite(value) or value < lo or value > hi:
        raise RecipeError(f"{path} must be between {lo:g} and {hi:g}")
    return round(value, 6)


def _normalize_tone_curve(value):
    if value in (None, "", {}):
        return None
    if not isinstance(value, dict):
        raise RecipeError("tone_curve adjustment must be an object")
    unknown = set(value) - set(TONE_CURVE_DEFAULTS)
    if unknown:
        raise RecipeError(f"unsupported tone_curve point '{sorted(unknown)[0]}'")
    out = {}
    for name, default in TONE_CURVE_DEFAULTS.items():
        if name not in value or value[name] in (None, ""):
            continue
        point = _number(value[name], f"tone_curve.{name}", 0.0, 100.0)
        if abs(point - default) > 1e-9:
            out[name] = point
    return out or None


def _normalize_point_curves(value):
    """Optional display-space composite and channel curves, in percent."""
    if value in (None, "", {}):
        return None
    if not isinstance(value, dict) or set(value) - {"rgb", "red", "green", "blue"}:
        raise RecipeError("point_curves must contain only rgb, red, green, or blue curves")
    out = {}
    for channel, points in value.items():
        if not isinstance(points, list) or not 2 <= len(points) <= 32:
            raise RecipeError("point_curves require 2 to 32 points per channel")
        normalized = []
        for point in points:
            if not isinstance(point, list) or len(point) != 2:
                raise RecipeError("point_curves points must be [input, output] pairs")
            normalized.append([
                _number(point[0], "curve input", 0, 100),
                _number(point[1], "curve output", 0, 100),
            ])
        if normalized[0][0] != 0 or normalized[-1][0] != 100:
            raise RecipeError("point_curves must start at input 0 and end at input 100")
        if any(b[0] - a[0] < 0.01 - 1e-9 for a, b in zip(normalized, normalized[1:], strict=False)):
            raise RecipeError("point_curves inputs must increase by at least 0.01")
        if any(abs(x - y) > 1e-6 for x, y in normalized):
            out[channel] = normalized
    return out or None


_POINT_COLOR_RANGES = {
    "hue_range": (1.0, 180.0, 30.0),
    "saturation_range": (1.0, 100.0, 100.0),
    "luminance_range": (1.0, 100.0, 100.0),
    "hue": (-180.0, 180.0, 0.0),
    "saturation": (-100.0, 100.0, 0.0),
    "luminance": (-100.0, 100.0, 0.0),
}


def _normalize_point_color(value):
    """Keep unadjusted color samples, but reject the renderer's zero-weight range."""
    if value is None or value == []:
        return None
    if not isinstance(value, list) or len(value) > 8:
        raise RecipeError("point_color must be a list of up to 8 samples")
    out = []
    for item in value:
        if not isinstance(item, dict) or set(item) - ({"sample"} | set(_POINT_COLOR_RANGES)):
            raise RecipeError("unsupported point_color sample controls")
        sample = item.get("sample")
        if not isinstance(sample, list) or len(sample) != 3:
            raise RecipeError("point_color sample must be [hue, saturation, luminance]")
        normalized = {"sample": [
            _number(sample[0], "sample hue", 0, 360) % 360,
            _number(sample[1], "sample saturation", 0, 100),
            _number(sample[2], "sample luminance", 0, 100),
        ]}
        if normalized["sample"][1] <= 1.0:
            raise RecipeError("point_color sample saturation must be greater than 1%")
        if not 0.0 < normalized["sample"][2] < 100.0:
            raise RecipeError("point_color sample luminance must be between 0% and 100%, exclusive")
        for key, (lo, hi, default) in _POINT_COLOR_RANGES.items():
            amount = _number(item.get(key, default), f"point_color.{key}", lo, hi)
            if abs(amount - default) > 1e-6:
                normalized[key] = amount
        out.append(normalized)
    return out or None


def _normalize_hsl(value):
    if value in (None, "", {}):
        return None
    if not isinstance(value, dict):
        raise RecipeError("hsl adjustment must be an object")
    unknown = set(value) - set(HSL_COLOR_NAMES)
    if unknown:
        raise RecipeError(f"unsupported hsl color '{sorted(unknown)[0]}'")
    out = {}
    for color in HSL_COLOR_NAMES:
        section = value.get(color)
        if section in (None, "", {}):
            continue
        if not isinstance(section, dict):
            raise RecipeError(f"hsl.{color} must be an object")
        unknown_keys = set(section) - set(_HSL_KEYS)
        if unknown_keys:
            raise RecipeError(
                f"unsupported hsl.{color} control '{sorted(unknown_keys)[0]}'"
            )
        normalized = {}
        for name in _HSL_KEYS:
            if name not in section or section[name] in (None, ""):
                continue
            amount = _number(section[name], f"hsl.{color}.{name}", -100.0, 100.0)
            if abs(amount) > 1e-9:
                normalized[name] = amount
        if normalized:
            out[color] = normalized
    return out or None


def _normalize_color_grading(value):
    if value in (None, "", {}):
        return None
    if not isinstance(value, dict):
        raise RecipeError("color_grading adjustment must be an object")
    supported = set(COLOR_GRADING_ZONES) | {"balance"}
    unknown = set(value) - supported
    if unknown:
        raise RecipeError(f"unsupported color_grading control '{sorted(unknown)[0]}'")
    raw_balance = value.get("balance", 0.0)
    balance = _number(
        0.0 if raw_balance in (None, "") else raw_balance,
        "color_grading.balance", -100.0, 100.0,
    )
    out = {}
    for zone in COLOR_GRADING_ZONES:
        section = value.get(zone)
        if section in (None, "", {}):
            continue
        if not isinstance(section, dict):
            raise RecipeError(f"color_grading.{zone} must be an object")
        unknown_keys = set(section) - {"hue", "saturation"}
        if unknown_keys:
            raise RecipeError(
                f"unsupported color_grading.{zone} control "
                f"'{sorted(unknown_keys)[0]}'"
            )
        raw_hue = section.get("hue", 0.0)
        raw_saturation = section.get("saturation", 0.0)
        hue = _number(
            0.0 if raw_hue in (None, "") else raw_hue,
            f"color_grading.{zone}.hue", 0.0, 360.0,
        )
        saturation = _number(
            0.0 if raw_saturation in (None, "") else raw_saturation,
            f"color_grading.{zone}.saturation", 0.0, 100.0,
        )
        # Hue has no visible meaning without saturation. Dropping the whole
        # zone keeps neutral recipes canonical and prevents needless renders.
        if saturation > 1e-9:
            out[zone] = {
                "hue": 0.0 if abs(hue - 360.0) < 1e-9 else hue,
                "saturation": saturation,
            }
    if out and abs(balance) > 1e-9:
        out["balance"] = balance
    return out or None


def _normalize_ranged(values, ranges, label):
    """Validate every ``ranges`` key present in ``values``; drop zeros.

    ``label`` is a format string receiving ``name`` for error messages.
    Returns a dict of the non-zero rounded values.
    """
    out = {}
    for name, (lo, hi) in ranges.items():
        raw = values.get(name)
        if raw in (None, ""):
            continue
        val = _number(raw, label.format(name=name), lo, hi)
        if abs(val) > 1e-9:
            out[name] = val
    return out


def _crop_in_bounds(x, y, w, h):
    return x >= 0 and y >= 0 and w > 0 and h > 0 and x + w <= 1 and y + h <= 1


def _normalize_crop(crop):
    """Validate a normalized crop rectangle; return None for absent/full-frame."""
    if crop is None:
        return None
    if not isinstance(crop, dict):
        raise RecipeError("crop must be an object")
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
    if not all(math.isfinite(v) for v in (x, y, w, h)):
        raise RecipeError("crop values must be finite")
    if not _crop_in_bounds(x, y, w, h):
        raise RecipeError("crop must fit inside normalized image bounds")
    # Treat an effectively full-frame crop as no-op.
    if (
        abs(x) < 1e-9 and abs(y) < 1e-9
        and abs(w - 1) < 1e-9 and abs(h - 1) < 1e-9
    ):
        return None
    x, y, w, h = (round(v, 6) for v in (x, y, w, h))
    if not _crop_in_bounds(x, y, w, h):
        raise RecipeError("crop must fit inside normalized image bounds")
    return {"x": x, "y": y, "w": w, "h": h}


def _normalize_geometry(recipe, out):
    """Validate rotation, straighten and flip into ``out``, in that order."""
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


def _normalize_white_balance(adjustments):
    """The canonical white-balance block (nested or legacy flat keys)."""
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
    normalized_wb = _normalize_ranged(
        white_balance, _WHITE_BALANCE_RANGES, "white_balance.{name} adjustment",
    )
    return normalized_wb


def _normalize_adjustments(adjustments):
    """Canonicalize the ``adjustments`` object of a recipe."""
    normalized_adjustments = _normalize_ranged(
        adjustments, _ADJUSTMENT_RANGES, "{name} adjustment",
    )
    denoise_mode = adjustments.get("denoise_mode", "standard")
    if denoise_mode not in ("standard", "camera"):
        raise RecipeError("denoise_mode must be standard or camera")
    if denoise_mode == "camera":
        normalized_adjustments["denoise_mode"] = denoise_mode

    # The USM radius only means something while sharpening is on, and its
    # default (1.0) is canonicalized to absence so an untouched radius slider
    # never dirties a recipe.
    if normalized_adjustments.get("sharpen"):
        raw_radius = adjustments.get("sharpen_radius")
        if raw_radius not in (None, ""):
            radius = _number(
                raw_radius, "sharpen_radius adjustment", *SHARPEN_RADIUS_RANGE
            )
            if abs(radius - SHARPEN_RADIUS_DEFAULT) > 1e-9:
                normalized_adjustments["sharpen_radius"] = radius

    normalized_wb = _normalize_white_balance(adjustments)
    if normalized_wb:
        normalized_adjustments["white_balance"] = normalized_wb

    tone_curve = _normalize_tone_curve(adjustments.get("tone_curve"))
    if tone_curve:
        normalized_adjustments["tone_curve"] = tone_curve
    point_curves = _normalize_point_curves(adjustments.get("point_curves"))
    if point_curves:
        normalized_adjustments["point_curves"] = point_curves
    point_color = _normalize_point_color(adjustments.get("point_color"))
    if point_color:
        normalized_adjustments["point_color"] = point_color
    hsl = _normalize_hsl(adjustments.get("hsl"))
    if hsl:
        normalized_adjustments["hsl"] = hsl
    color_grading = _normalize_color_grading(adjustments.get("color_grading"))
    if color_grading:
        normalized_adjustments["color_grading"] = color_grading
    return normalized_adjustments


def normalize_recipe(recipe) -> dict[str, Any] | None:
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

    _normalize_geometry(recipe, out)

    crop = _normalize_crop(recipe.get("crop"))
    if crop is not None:
        out["crop"] = crop

    adjustments = recipe.get("adjustments")
    if adjustments is None:
        adjustments = {}
    if not isinstance(adjustments, dict):
        raise RecipeError("adjustments must be an object")
    normalized_adjustments = _normalize_adjustments(adjustments)
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

    Retain manually corrected masks even before a region is adjusted; other
    masks normalize away when no region references them.
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
                normalized_adj[name] = _number(
                    raw, "sharpen_radius adjustment", *SHARPEN_RADIUS_RANGE
                )
                continue
            if name not in _LOCAL_ADJUSTMENT_KEYS:
                raise RecipeError(
                    f"{name} adjustment is not supported in local regions"
                )
            val = _number(
                raw, f"{name} adjustment", *_LOCAL_ADJUSTMENT_RANGES[name]
            )
            if abs(val) > 1e-9:
                normalized_adj[name] = val
        if normalized_adj:
            normalized_regions.append(
                {"region": region, "adjustments": normalized_adj}
            )

    mask_in = local.get("mask")
    corrected = isinstance(mask_in, dict) and mask_in.get("corrected") is True
    if not normalized_regions and not corrected:
        return None

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
    if corrected:
        normalized_mask["corrected"] = True

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


def recipe_to_json(recipe) -> str | None:
    """Return canonical JSON for a normalized recipe, or None for no-op."""
    normalized = normalize_recipe(recipe)
    if normalized is None:
        return None
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def copy_recipe(recipe):
    """Return a detached normalized recipe dict for API responses."""
    normalized = normalize_recipe(recipe)
    return copy.deepcopy(normalized)
