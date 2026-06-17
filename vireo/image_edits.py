"""Non-destructive photo edit recipes and rendering helpers."""

from __future__ import annotations

import copy
import json
import math

from PIL import Image, ImageEnhance

SCHEMA_VERSION = 1

_ADJUSTMENT_RANGES = {
    "exposure": (-5.0, 5.0),
    "brightness": (-100.0, 100.0),
    "contrast": (-100.0, 100.0),
    "saturation": (-100.0, 100.0),
}


class RecipeError(ValueError):
    """Raised when an edit recipe is malformed or unsupported."""


def normalize_recipe(recipe):
    """Validate and canonicalize a non-destructive image edit recipe.

    The crop rectangle uses normalized coordinates in the image space after
    rotation and flips have been applied. Rotation is limited to right angles;
    arbitrary straightening can be added to this schema without changing the
    storage model.
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
    if isinstance(rotation, bool) or not isinstance(rotation, (int, float)):
        raise RecipeError("rotation must be one of 0, 90, 180, or 270")
    if isinstance(rotation, float) and not rotation.is_integer():
        raise RecipeError("rotation must be one of 0, 90, 180, or 270")
    rotation = int(rotation)
    if rotation not in (0, 90, 180, 270):
        raise RecipeError("rotation must be one of 0, 90, 180, or 270")
    if rotation:
        out["rotation"] = rotation

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
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise RecipeError(f"{name} adjustment must be numeric")
        val = float(raw)
        if not math.isfinite(val) or val < lo or val > hi:
            raise RecipeError(f"{name} adjustment must be between {lo:g} and {hi:g}")
        if abs(val) > 1e-9:
            normalized_adjustments[name] = round(val, 6)
    if normalized_adjustments:
        out["adjustments"] = normalized_adjustments

    return out if len(out) > 1 else None


def recipe_to_json(recipe):
    """Return canonical JSON for a normalized recipe, or None for no-op."""
    normalized = normalize_recipe(recipe)
    if normalized is None:
        return None
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


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
    if "exposure" in adjustments:
        result = ImageEnhance.Brightness(result).enhance(2 ** adjustments["exposure"])
    if "brightness" in adjustments:
        result = ImageEnhance.Brightness(result).enhance(
            max(0.0, 1.0 + adjustments["brightness"] / 100.0)
        )
    if "contrast" in adjustments:
        result = ImageEnhance.Contrast(result).enhance(
            max(0.0, 1.0 + adjustments["contrast"] / 100.0)
        )
    if "saturation" in adjustments:
        result = ImageEnhance.Color(result).enhance(
            max(0.0, 1.0 + adjustments["saturation"] / 100.0)
        )

    return result


def apply_recipe_to_loaded_image(img, recipe, max_size=None):
    """Apply edits, then optionally constrain the long edge."""
    result = apply_recipe(img, recipe)
    if max_size and max_size > 0 and max(result.size) > max_size:
        result.thumbnail((max_size, max_size), resample=Image.Resampling.LANCZOS)
    return result


def copy_recipe(recipe):
    """Return a detached normalized recipe dict for API responses."""
    normalized = normalize_recipe(recipe)
    return copy.deepcopy(normalized)
