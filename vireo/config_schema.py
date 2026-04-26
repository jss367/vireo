"""Schema metadata for Vireo config keys.

Parallel to ``config.DEFAULTS`` — provides type, category, scope, label, and
description per leaf key. Used by the schema-rendered settings UI to generate
widgets and validate values. Drift between ``DEFAULTS`` and ``SCHEMA`` is
enforced by ``vireo/tests/test_config_schema.py``.
"""

import math

# Prefixes whose leaf keys are intentionally NOT covered by SCHEMA.
#   - setup_complete: internal state flag (not a user-facing setting)
#   - ingest.recent_destinations: auto-populated MRU list, not a knob
#   - keyboard_shortcuts.*: managed by a dedicated curated UI section
EXCLUDED = ("setup_complete", "ingest.recent_destinations", "keyboard_shortcuts")


CATEGORIES = (
    "Detection",
    "Pipeline",
    "Culling",
    "Display",
    "Working copy",
    "Preview",
    "Ingest",
    "Paths",
    "Integrations",
    "Behavior",
)


# Each entry: type, category, scope, label, desc (required) + optional
# min/max/step/enum/enum_labels/items_enum.
SCHEMA = {
    # --- Detection / classification --------------------------------------
    "classification_threshold": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Detection", "scope": "both",
        "label": "Classification threshold",
        "desc": "Minimum confidence for a species prediction to be kept.",
    },
    "detector_confidence": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Detection", "scope": "both",
        "label": "Detector confidence",
        "desc": "Minimum confidence for a wildlife detection bounding box.",
    },
    "detection_padding": {
        # scope=global: only consumers are cfg.load().get(...) in app.py;
        # promote to "both" only after wiring those reads to
        # db.get_effective_config(cfg.load()).
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Detection", "scope": "global",
        "label": "Detection padding",
        "desc": "Padding around detected bounding boxes before classification (fraction of bbox size).",
    },
    "top_k_predictions": {
        "type": "int", "min": 1, "max": 50,
        "category": "Detection", "scope": "both",
        "label": "Top-K predictions",
        "desc": "Number of top species predictions to keep per detection.",
    },
    "redundancy_threshold": {
        # scope=global: culling.analyze_for_culling reads cfg.load() (not
        # db.get_effective_config), so a workspace override here would be
        # silently ignored.
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Detection", "scope": "global",
        "label": "Redundancy threshold",
        "desc": "Cosine-similarity threshold for collapsing near-duplicate detections.",
    },

    # --- Behavior / general ----------------------------------------------
    "grouping_window_seconds": {
        "type": "int", "min": 0, "max": 3600,
        "category": "Behavior", "scope": "both",
        "label": "Grouping window (seconds)",
        "desc": "Time gap that separates one shoot/encounter from the next.",
    },
    "similarity_threshold": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Behavior", "scope": "both",
        "label": "Similarity threshold",
        "desc": "Cosine-similarity threshold above which two photos are visual duplicates.",
    },
    "max_edit_history": {
        # scope=global: Database._prune_edit_history reads cfg.get(...) so a
        # workspace override would not change actual pruning behavior.
        "type": "int", "min": 0, "max": 100000,
        "category": "Behavior", "scope": "global",
        "label": "Edit history limit",
        "desc": "Maximum number of undo-able edits kept per workspace.",
    },
    "keyword_case": {
        # scope=global: Database.add_keyword reads cfg.get("keyword_case"), so
        # a workspace override would not change keyword capitalization.
        "type": "enum",
        "enum": ["auto", "title", "lower"],
        "enum_labels": {
            "auto": "Auto-detect",
            "title": "Title Case (House Finch)",
            "lower": "Sentence case (House finch)",
        },
        "category": "Behavior", "scope": "global",
        "label": "Keyword case",
        "desc": "Capitalization convention for species keywords.",
    },
    "scan_workers": {
        "type": "int", "min": 0, "max": 64,
        "category": "Behavior", "scope": "global",
        "label": "Scan workers",
        "desc": "Worker thread count for scans (0 = auto).",
    },

    # --- Culling ----------------------------------------------------------
    # NOTE: scope is "global" — the cull workflow (cull.html init via
    # /api/config, culling.analyze_for_culling default-fill via cfg.load())
    # reads only the global config, so a workspace override here would be
    # silently ignored. Promote to "both" only after wiring those consumers
    # to db.get_effective_config(cfg.load()).
    "cull_time_window": {
        "type": "int", "min": 0, "max": 3600,
        "category": "Culling", "scope": "global",
        "label": "Cull time window (seconds)",
        "desc": "Burst window for the cull workflow — photos within this gap are eligible for grouping.",
    },
    "cull_phash_threshold": {
        "type": "int", "min": 0, "max": 64,
        "category": "Culling", "scope": "global",
        "label": "Cull pHash threshold",
        "desc": "Maximum perceptual-hash distance for two photos to be considered visually similar.",
    },

    # --- Display ----------------------------------------------------------
    "photos_per_page": {
        "type": "int", "min": 10, "max": 500,
        "category": "Display", "scope": "global",
        "label": "Photos per page",
        "desc": "Page size in browse views.",
    },
    "thumbnail_size": {
        "type": "int", "min": 100, "max": 1024,
        "category": "Display", "scope": "global",
        "label": "Thumbnail size (px)",
        "desc": "Maximum dimension for thumbnails (used in browse grid).",
    },
    "thumbnail_quality": {
        "type": "int", "min": 1, "max": 100,
        "category": "Display", "scope": "global",
        "label": "Thumbnail quality",
        "desc": "JPEG quality for thumbnails (1–100).",
    },
    "browse_thumb_default": {
        "type": "int", "min": 80, "max": 600,
        "category": "Display", "scope": "global",
        "label": "Default browse thumbnail size",
        "desc": "Initial slider position for thumbnail size in the browse view.",
    },
    "browse_card_fields": {
        "type": "list_string",
        "items_enum": [
            "filename", "rating", "flag", "sharpness", "species",
            "dimensions", "file_size", "capture_date", "extension",
            "quality_score",
        ],
        "category": "Display", "scope": "global",
        "label": "Browse card fields",
        "desc": "Which metadata badges appear on each photo card in browse.",
    },

    # --- Working copy -----------------------------------------------------
    "working_copy_max_size": {
        "type": "int", "min": 512, "max": 16384,
        "category": "Working copy", "scope": "global",
        "label": "Working copy max size (px)",
        "desc": "Largest dimension of the JPEG working copy extracted from RAW files.",
    },
    "working_copy_quality": {
        "type": "int", "min": 1, "max": 100,
        "category": "Working copy", "scope": "global",
        "label": "Working copy quality",
        "desc": "JPEG quality for the working copy (1–100).",
    },

    # --- Preview ----------------------------------------------------------
    "preview_max_size": {
        "type": "int", "min": 512, "max": 8192,
        "category": "Preview", "scope": "global",
        "label": "Preview max size (px)",
        "desc": "Largest dimension for inline preview images.",
    },
    "preview_quality": {
        "type": "int", "min": 1, "max": 100,
        "category": "Preview", "scope": "global",
        "label": "Preview quality",
        "desc": "JPEG quality for inline previews (1–100).",
    },
    "preview_cache_max_mb": {
        "type": "int", "min": 0, "max": 65536,
        "category": "Preview", "scope": "global",
        "label": "Preview cache max (MB)",
        "desc": "On-disk cache budget for generated previews.",
    },

    # --- Paths / external tools ------------------------------------------
    "scan_roots": {
        "type": "list_string",
        "category": "Paths", "scope": "global",
        "label": "Scan roots",
        "desc": "Top-level folders Vireo scans for photos.",
    },
    "darktable_bin": {
        "type": "path",
        "category": "Paths", "scope": "global",
        "label": "darktable-cli path",
        "desc": "Absolute path to the darktable-cli binary.",
    },
    "external_editor": {
        "type": "path",
        "category": "Paths", "scope": "global",
        "label": "External editor",
        "desc": "Absolute path to an external image editor (used by 'Open in editor').",
    },
    "darktable_style": {
        "type": "string",
        "category": "Paths", "scope": "global",
        "label": "Darktable style",
        "desc": "Darktable style name to apply when developing.",
    },
    "darktable_output_format": {
        "type": "enum",
        "enum": ["jpg", "tiff"],
        "enum_labels": {"jpg": "JPEG", "tiff": "TIFF"},
        "category": "Paths", "scope": "global",
        "label": "Darktable output format",
        "desc": "File format for developed photos.",
    },
    "darktable_output_dir": {
        "type": "path",
        "category": "Paths", "scope": "global",
        "label": "Darktable output directory",
        "desc": 'Where developed photos are saved. Empty = create a "developed" subfolder next to originals.',
    },

    # --- Integrations -----------------------------------------------------
    "inat_token": {
        "type": "secret",
        "category": "Integrations", "scope": "global",
        "label": "iNaturalist token",
        "desc": "API token for iNaturalist (used for taxonomy / observations).",
    },
    "hf_token": {
        "type": "secret",
        "category": "Integrations", "scope": "global",
        "label": "Hugging Face token",
        "desc": "API token for Hugging Face (used for model downloads).",
    },
    "report_url": {
        "type": "string",
        "category": "Integrations", "scope": "global",
        "label": "Report URL",
        "desc": "Endpoint for anonymous bug/feedback reports.",
    },

    # --- Ingest -----------------------------------------------------------
    "ingest.folder_template": {
        "type": "string",
        "category": "Ingest", "scope": "both",
        "label": "Ingest folder template",
        "desc": "Strftime template for ingest destination subfolders (e.g. %Y/%Y-%m-%d).",
    },
    "ingest.skip_duplicates": {
        "type": "bool",
        "category": "Ingest", "scope": "both",
        "label": "Skip duplicates on ingest",
        "desc": "Skip files whose hash already exists in the database.",
    },
    "ingest.file_types": {
        "type": "enum",
        "enum": ["both", "raw", "jpg"],
        "enum_labels": {"both": "RAW + JPEG", "raw": "RAW only", "jpg": "JPEG only"},
        "category": "Ingest", "scope": "both",
        "label": "Ingest file types",
        "desc": "Which file types to import.",
    },

    # --- Pipeline (scoring weights & rejection thresholds) ---------------
    "pipeline.w_focus": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Score weight: focus",
        "desc": "Weight of focus quality in the photo score.",
    },
    "pipeline.w_exposure": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Score weight: exposure",
        "desc": "Weight of exposure quality in the photo score.",
    },
    "pipeline.w_composition": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Score weight: composition",
        "desc": "Weight of composition quality in the photo score.",
    },
    "pipeline.w_area": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Score weight: subject area",
        "desc": "Weight of subject size in frame.",
    },
    "pipeline.w_noise": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Score weight: noise",
        "desc": "Weight of noise penalty in the photo score.",
    },
    "pipeline.reject_crop_complete": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Reject: complete crop",
        "desc": "Reject threshold for complete-crop detection.",
    },
    "pipeline.reject_focus": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Reject: focus",
        "desc": "Reject threshold for focus failure.",
    },
    "pipeline.reject_clip_high": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Reject: highlight clipping",
        "desc": "Reject threshold for blown highlights.",
    },
    "pipeline.reject_composite": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Reject: composite",
        "desc": "Combined reject threshold.",
    },
    "pipeline.miss_enabled": {
        "type": "bool",
        "category": "Pipeline", "scope": "both",
        "label": "Miss-detection enabled",
        "desc": 'Enable detection of "missed" subject shots.',
    },
    "pipeline.miss_det_confidence": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Miss: detection confidence",
        "desc": "Confidence gate for miss detection.",
    },
    "pipeline.miss_det_confidence_burst": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Miss: burst confidence",
        "desc": "Looser confidence inside a burst.",
    },
    "pipeline.miss_bbox_area_min": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.001,
        "category": "Pipeline", "scope": "both",
        "label": "Miss: min bbox area",
        "desc": "Minimum bbox area as fraction of frame.",
    },
    "pipeline.miss_bbox_area_min_singleton": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.001,
        "category": "Pipeline", "scope": "both",
        "label": "Miss: min bbox area (singleton)",
        "desc": "Minimum bbox area for non-burst frames.",
    },
    "pipeline.miss_oof_ratio": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Miss: out-of-focus ratio",
        "desc": "Out-of-focus area ratio threshold.",
    },
    "pipeline.eye_detect_enabled": {
        "type": "bool",
        "category": "Pipeline", "scope": "both",
        "label": "Eye-focus detection enabled",
        "desc": "Enable eye-focus detection.",
    },
    "pipeline.eye_classifier_conf_gate": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Eye: classifier confidence",
        "desc": "Classifier confidence gate for eye-focus.",
    },
    "pipeline.eye_detection_conf_gate": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Eye: detection confidence",
        "desc": "Detection confidence gate for eye-focus.",
    },
    "pipeline.eye_window_k": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Eye: window k",
        "desc": "Window size factor for eye-focus measurement.",
    },
    "pipeline.reject_eye_focus": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Reject: eye focus",
        "desc": "Reject threshold for eye out-of-focus.",
    },
    "pipeline.burst_time_gap": {
        "type": "float", "min": 0.0, "max": 600.0, "step": 0.5,
        "category": "Pipeline", "scope": "both",
        "label": "Burst: time gap (s)",
        "desc": "Maximum time gap between frames in a burst.",
    },
    "pipeline.burst_phash_threshold": {
        "type": "int", "min": 0, "max": 64,
        "category": "Pipeline", "scope": "both",
        "label": "Burst: pHash threshold",
        "desc": "pHash distance threshold for burst grouping.",
    },
    "pipeline.burst_embedding_threshold": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Burst: embedding threshold",
        "desc": "Embedding similarity threshold for burst grouping.",
    },
    "pipeline.burst_lambda": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Burst: lambda",
        "desc": "MMR-style diversity weight inside a burst.",
    },
    "pipeline.burst_max_keep": {
        "type": "int", "min": 1, "max": 100,
        "category": "Pipeline", "scope": "both",
        "label": "Burst: max keep",
        "desc": "Maximum frames to keep per burst.",
    },
    "pipeline.encounter_lambda": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Encounter: lambda",
        "desc": "MMR-style diversity weight across an encounter.",
    },
    "pipeline.encounter_max_keep": {
        "type": "int", "min": 1, "max": 100,
        "category": "Pipeline", "scope": "both",
        "label": "Encounter: max keep",
        "desc": "Maximum frames to keep per encounter.",
    },
    "pipeline.w_time": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Diversity weight: time",
        "desc": "Weight of time component in diversity score.",
    },
    "pipeline.w_subj": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Diversity weight: subject",
        "desc": "Weight of subject component in diversity score.",
    },
    "pipeline.w_global": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Diversity weight: global",
        "desc": "Weight of global-embedding component in diversity score.",
    },
    "pipeline.w_species": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Diversity weight: species",
        "desc": "Weight of species component in diversity score.",
    },
    "pipeline.w_meta": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Diversity weight: metadata",
        "desc": "Weight of metadata component in diversity score.",
    },
    "pipeline.hard_cut_time": {
        "type": "float", "min": 0.0, "max": 86400.0, "step": 1.0,
        "category": "Pipeline", "scope": "both",
        "label": "Encounter: hard-cut time (s)",
        "desc": "Time gap that always splits an encounter.",
    },
    "pipeline.hard_cut_score": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Encounter: hard-cut score",
        "desc": "Similarity score that always splits an encounter.",
    },
    "pipeline.soft_cut_score": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Encounter: soft-cut score",
        "desc": "Similarity below which encounters are likely to split.",
    },
    "pipeline.merge_score": {
        "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
        "category": "Pipeline", "scope": "both",
        "label": "Encounter: merge score",
        "desc": "Similarity above which adjacent encounters merge.",
    },
    "pipeline.merge_max_gap": {
        "type": "float", "min": 0.0, "max": 86400.0, "step": 1.0,
        "category": "Pipeline", "scope": "both",
        "label": "Encounter: merge max gap (s)",
        "desc": "Maximum time gap that still allows a merge.",
    },
    "pipeline.extract_full_metadata": {
        "type": "bool",
        "category": "Pipeline", "scope": "both",
        "label": "Extract full metadata",
        "desc": "Read full EXIF/XMP during pipeline scan (slower but more complete).",
    },
}


# ---------------------------------------------------------------------------
# Dotted-path helpers
# ---------------------------------------------------------------------------


def is_excluded(dotted_key):
    """True if dotted_key is intentionally excluded from SCHEMA coverage."""
    return any(dotted_key == p or dotted_key.startswith(p + ".") for p in EXCLUDED)


def schema_parent_prefixes():
    """Return the set of dotted prefixes that are parents of any SCHEMA key.

    For example, with ``pipeline.w_focus`` and ``ingest.folder_template`` in
    SCHEMA, this returns ``{"pipeline", "ingest"}``. Used to detect malformed
    imports that replace a schema-backed subtree with a non-object value.
    """
    out = set()
    for k in SCHEMA:
        parts = k.split(".")
        for i in range(1, len(parts)):
            out.add(".".join(parts[:i]))
    return out


def flatten(d, prefix=""):
    """Flatten a nested dict to a single dict keyed by dotted paths."""
    out = {}
    for k, v in d.items():
        path = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten(v, path))
        else:
            out[path] = v
    return out


def get_dotted(d, key, default=None):
    """Look up a value at a dotted path. Returns ``default`` if any segment is missing."""
    cur = d
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def set_dotted(d, key, value):
    """Set a value at a dotted path, creating intermediate dicts as needed.

    Replaces non-dict intermediates (so set_dotted({"a": 1}, "a.b", 2) succeeds).
    """
    parts = key.split(".")
    cur = d
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


def delete_dotted(d, key):
    """Remove a leaf at a dotted path. Returns True if something was removed."""
    parts = key.split(".")
    cur = d
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            return False
        cur = cur[part]
    return cur.pop(parts[-1], None) is not None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class ValidationError(ValueError):
    """Raised by ``validate_value`` on invalid input."""


_TRUE_STRINGS = ("true", "1", "yes", "on")
_FALSE_STRINGS = ("false", "0", "no", "off", "")


def _coerce(raw, kind):
    if kind == "bool":
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        if isinstance(raw, str):
            low = raw.strip().lower()
            if low in _TRUE_STRINGS:
                return True
            if low in _FALSE_STRINGS:
                return False
        raise ValidationError(f"cannot coerce {raw!r} to bool")
    if kind == "int":
        if isinstance(raw, bool):
            return int(raw)
        # JSON numeric values arrive as float when they have a decimal point.
        # Reject non-integral floats so {"key": 1.9} doesn't silently become 1.
        if isinstance(raw, float):
            if not math.isfinite(raw) or not raw.is_integer():
                raise ValidationError(
                    f"cannot coerce {raw!r} to int: not an integral value"
                )
            return int(raw)
        try:
            if isinstance(raw, str):
                return int(raw.strip())
            return int(raw)
        except (TypeError, ValueError) as e:
            raise ValidationError(f"cannot coerce {raw!r} to int: {e}") from e
    if kind == "float":
        if isinstance(raw, bool):
            return float(raw)
        try:
            value = float(raw)
        except (TypeError, ValueError) as e:
            raise ValidationError(f"cannot coerce {raw!r} to float: {e}") from e
        # Reject NaN and ±Inf — bound checks (`< min`, `> max`) skip NaN, so
        # a payload like {"value": "nan"} would otherwise pass validation and
        # poison downstream comparisons.
        if not math.isfinite(value):
            raise ValidationError(f"cannot coerce {raw!r} to float: not a finite number")
        return value
    if kind in ("string", "secret", "path", "enum"):
        if raw is None:
            return ""
        return str(raw)
    if kind == "list_string":
        if not isinstance(raw, list):
            raise ValidationError(f"expected list, got {type(raw).__name__}")
        return [str(x) for x in raw]
    raise ValidationError(f"unknown type kind {kind!r}")


def validate_value(key, raw):
    """Validate ``raw`` against ``SCHEMA[key]``.

    Returns the coerced value. Raises :class:`ValidationError` on any failure
    (unknown key, type mismatch, out-of-range, unknown enum value, etc.).
    """
    if key not in SCHEMA:
        raise ValidationError(f"unknown setting {key!r}")
    spec = SCHEMA[key]
    kind = spec["type"]
    value = _coerce(raw, kind)

    if kind in ("int", "float"):
        if "min" in spec and value < spec["min"]:
            raise ValidationError(f"{key} must be >= {spec['min']} (got {value})")
        if "max" in spec and value > spec["max"]:
            raise ValidationError(f"{key} must be <= {spec['max']} (got {value})")

    if kind == "enum" and value not in spec["enum"]:
        raise ValidationError(
            f"{key} must be one of {spec['enum']} (got {value!r})"
        )

    if kind == "list_string" and "items_enum" in spec:
        for item in value:
            if item not in spec["items_enum"]:
                raise ValidationError(
                    f"{key}: item {item!r} not in {spec['items_enum']}"
                )

    return value
