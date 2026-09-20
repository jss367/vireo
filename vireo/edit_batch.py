"""Field-scoped development edits shared by batch editing and presets."""

import copy
import json

from image_edits import (
    _ADJUSTMENT_RANGES,
    RecipeError,
    _number,
    normalize_recipe,
)

# Explicit paths make a missing source value mean "reset this setting", while
# settings outside the selection always retain the destination's value.
FIELDS = []
for name, bounds in _ADJUSTMENT_RANGES.items():
    FIELDS.append({
        "path": f"adjustments.{name}",
        "label": name.replace("_", " ").capitalize(),
        "group": "Adjustments", "min": bounds[0], "max": bounds[1],
        "step": 0.1 if name == "exposure" else 1, "default": 0,
    })
FIELDS += [
    {"path": "adjustments.denoise_mode", "label": "Denoise method", "group": "Adjustments"},
    {"path": "adjustments.sharpen_radius", "label": "Sharpening radius", "group": "Adjustments",
     "min": 0.5, "max": 3, "step": 0.1, "default": 1},
    *({"path": f"adjustments.white_balance.{name}", "label": name.capitalize(), "group": "White balance",
       "min": -100, "max": 100, "step": 1, "default": 0} for name in ("temperature", "tint")),
    *({"path": f"adjustments.{name}", "label": label, "group": "Color"} for name, label in (
        ("tone_curve", "Tone curve"), ("point_curves", "Point curves"),
        ("hsl", "Color mixer"), ("point_color", "Point Color"), ("color_grading", "Color grading"),
    )),
    *({"path": name, "label": label, "group": "Geometry"} for name, label in (
        ("rotation", "Rotation"), ("flip", "Flips"), ("straighten", "Straighten"), ("crop", "Crop"),
    )),
    {"path": "local", "label": "Subject and background", "group": "Local adjustments"},
]
FIELD_MAP = {field["path"]: field for field in FIELDS}


def validate_fields(fields):
    if not isinstance(fields, list) or not fields:
        raise RecipeError("Select at least one setting")
    if any(not isinstance(path, str) or path not in FIELD_MAP for path in fields):
        raise RecipeError("Unsupported development setting")
    return list(dict.fromkeys(fields))


def get_value(recipe, path, default=None):
    value = recipe or {}
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return default
        value = value[part]
    return value


def put_value(recipe, path, value):
    parts = path.split(".")
    node = recipe
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    if value is None:
        node.pop(parts[-1], None)
    else:
        node[parts[-1]] = copy.deepcopy(value)


def validate_operation(recipe, fields=None, mode="replace"):
    if not isinstance(recipe, dict):
        raise RecipeError("recipe must be a JSON object")
    if mode not in ("replace", "merge", "relative"):
        raise RecipeError("mode must be replace, merge, or relative")
    if fields is not None:
        fields = validate_fields(fields)
    if mode != "replace" and fields is None:
        raise RecipeError("Selected settings are required")
    if mode == "relative":
        for path in fields:
            field = FIELD_MAP[path]
            if "min" not in field:
                raise RecipeError("Relative edits require numeric adjustments")
            # A delta can span the entire slider range, including negative
            # sharpening and noise reduction. Validate before touching photos.
            span = field["max"] - field["min"]
            _number(get_value(recipe, path), path, -span, span)
    else:
        normalize_recipe(recipe)
        for path in fields or []:
            field = FIELD_MAP[path]
            value = get_value(recipe, path)
            if "min" in field and value is not None:
                _number(value, path, field["min"], field["max"])
    return fields


def compose_recipe(current, source, fields=None, mode="replace"):
    """Compose without rebinding masks; the caller owns target mask access."""
    fields = validate_operation(source, fields, mode)
    if fields is None:
        return normalize_recipe(source)
    result = copy.deepcopy(current or {})
    normalized = source if mode == "relative" else (normalize_recipe(source) or {})
    for path in fields:
        value = get_value(normalized, path)
        if path == "adjustments.sharpen_radius" and mode != "relative":
            # Radius is meaningful on a sharpened destination even when the
            # source patch contains no sharpen strength of its own.
            value = get_value(source, path)
            if value is not None:
                value = _number(value, path, 0.5, 3)
        if mode == "relative":
            field = FIELD_MAP[path]
            value += get_value(current, path, field["default"])
            value = max(field["min"], min(field["max"], value))
        put_value(result, path, value)
    return normalize_recipe(result)


def _restore_preset_radius(normalized, source, fields):
    # A partial preset can own radius without owning sharpen strength. Full
    # photo normalization drops an inactive radius, but a preset must keep it
    # for destinations which already have sharpening enabled.
    path = "adjustments.sharpen_radius"
    if path in fields:
        radius = get_value(source, path)
        if radius is not None:
            radius = _number(radius, path, 0.5, 3)
            if radius != 1:
                put_value(normalized, path, radius)
    return normalized


def encode_preset(recipe, fields):
    fields = validate_fields(fields)
    selected = compose_recipe({}, recipe, fields, "merge") or {}
    _restore_preset_radius(selected, recipe, fields)
    if selected.get("local"):
        # Presets carry region values, never a reference to a source image's
        # snapshot. A valid sentinel keeps the normal recipe schema reusable;
        # application always replaces it with the target's own snapshot.
        selected["local"]["mask"].update(ref="000000000000", source_digest="preset")
    return json.dumps({"recipe": selected, "fields": fields}, separators=(",", ":"))


def decode_preset(raw):
    stored = json.loads(raw) if isinstance(raw, str) else raw
    if isinstance(stored, dict) and "fields" in stored:
        fields = validate_fields(stored["fields"])
        recipe = _restore_preset_radius(normalize_recipe(stored["recipe"]) or {}, stored["recipe"], fields)
        return recipe, fields
    return normalize_recipe(stored), None
