"""User configuration for Vireo (persisted to ~/.vireo/config.json)."""

import json
import logging
import os

log = logging.getLogger(__name__)

CONFIG_PATH = os.path.expanduser("~/.vireo/config.json")

DEFAULTS = {
    "classification_threshold": 0.4,
    "grouping_window_seconds": 10,
    "similarity_threshold": 0.85,
    "preview_max_size": 1920,
    "keyword_case": "auto",
    "max_edit_history": 1000,
    "inat_token": "",
    "hf_token": "",
    "scan_roots": [],
    "setup_complete": False,
    "darktable_bin": "",
    "external_editor": "",
    "darktable_style": "",
    "darktable_output_format": "jpg",
    "darktable_output_dir": "",
    # --- Display ---
    "photos_per_page": 50,
    "thumbnail_size": 400,
    "thumbnail_quality": 85,
    "preview_quality": 90,
    "browse_thumb_default": 220,
    # --- Detection ---
    "detector_confidence": 0.2,
    "detection_padding": 0.2,
    "top_k_predictions": 5,
    "redundancy_threshold": 0.88,
    # --- Culling defaults ---
    "cull_time_window": 60,
    "cull_phash_threshold": 19,
    # --- Pipeline (nested — flows through effective_cfg.get("pipeline")) ---
    "pipeline": {
        "w_focus": 0.45,
        "w_exposure": 0.20,
        "w_composition": 0.15,
        "w_area": 0.10,
        "w_noise": 0.10,
        "reject_crop_complete": 0.60,
        "reject_focus": 0.35,
        "reject_clip_high": 0.30,
        "reject_composite": 0.40,
        "burst_time_gap": 3.0,
        "burst_phash_threshold": 12,
        "burst_embedding_threshold": 0.80,
        "burst_lambda": 0.85,
        "burst_max_keep": 3,
        "encounter_lambda": 0.70,
        "encounter_max_keep": 5,
        "w_time": 0.35,
        "w_subj": 0.35,
        "w_global": 0.15,
        "w_species": 0.10,
        "w_meta": 0.05,
        "hard_cut_time": 180.0,
        "hard_cut_score": 0.42,
        "soft_cut_score": 0.52,
        "merge_score": 0.62,
        "merge_max_gap": 60.0,
        "extract_full_metadata": True,
    },
    # --- Ingest (import from external source) ---
    "ingest": {
        "folder_template": "%Y/%m-%d",
        "skip_duplicates": True,
        "file_types": "both",
    },
    "keyboard_shortcuts": {
        "navigation": {
            "import": "i",
            "pipeline": "p",
            "pipeline_review": "e",
            "review": "r",
            "cull": "c",
            "browse": "b",
            "map": "m",
            "variants": "v",
            "dashboard": "d",
            "audit": "a",
            "compare": "o",
            "workspace": "w",
            "shortcuts": "/",
            "settings": ",",
            "keywords": "k",
        },
        "review": {
            "accept": "a",
            "skip": "s",
        },
        "browse": {
            "rate_0": "0",
            "rate_1": "1",
            "rate_2": "2",
            "rate_3": "3",
            "rate_4": "4",
            "rate_5": "5",
            "flag": "p",
            "reject": "x",
            "unflag": "u",
            "undo": "ctrl+z",
            "select_all": "ctrl+a",
            "zoom": "z",
        },
    },
}


def _deep_merge(base, override):
    """Merge override into base recursively so nested dicts are merged, not replaced."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load():
    """Load config, returning defaults for any missing keys."""
    config = dict(DEFAULTS)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                config = _deep_merge(config, json.load(f))
        except Exception:
            log.warning("Failed to read config, using defaults")
    return config


def save(config):
    """Save config to disk."""
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


def get(key):
    """Get a single config value."""
    return load().get(key, DEFAULTS.get(key))


def set(key, value):
    """Set a single config value."""
    config = load()
    config[key] = value
    save(config)
