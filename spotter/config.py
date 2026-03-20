"""User configuration for Spotter (persisted to ~/.spotter/config.json)."""

import json
import logging
import os

log = logging.getLogger(__name__)

CONFIG_PATH = os.path.expanduser("~/.spotter/config.json")

DEFAULTS = {
    "classification_threshold": 0.4,
    "grouping_window_seconds": 10,
    "similarity_threshold": 0.85,
    "preview_max_size": 1920,
    "hf_token": "",
    "scan_roots": [],
}


def load():
    """Load config, returning defaults for any missing keys."""
    config = dict(DEFAULTS)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                config.update(json.load(f))
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
