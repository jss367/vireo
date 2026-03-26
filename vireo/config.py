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
    "darktable_bin": "",
    "darktable_style": "",
    "darktable_output_format": "jpg",
    "darktable_output_dir": "",
    "keyboard_shortcuts": {
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
