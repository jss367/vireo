"""Canonical JSON digests and source input identities for portable model runs."""

import hashlib
import json
import math

_HEX64 = frozenset("0123456789abcdef")


class CacheFormatError(ValueError):
    """Raised when an artifact or bundle is invalid or unsupported."""


def is_sha256(value):
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in _HEX64 for char in value)
    )


def normalize_json(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CacheFormatError("NaN and infinity are not valid cache data")
        return 0.0 if value == 0 else value
    if isinstance(value, list):
        return [normalize_json(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise CacheFormatError("cache object keys must be strings")
        return {key: normalize_json(item) for key, item in value.items()}
    raise CacheFormatError(f"unsupported cache value: {type(value).__name__}")


def canonical_bytes(value):
    """Return the exact canonical JSON bytes used for all cache digests."""
    normalized = normalize_json(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def fingerprint(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def source_input(photo_sha256, recipe, *, focus=None):
    """Build the versioned source identity used by the experimental v1 cache.

    The model runtime identity owns decode/preprocess behavior.  The input
    identity owns the exact source bytes and named source-selection recipe.
    """
    if not is_sha256(photo_sha256):
        raise CacheFormatError("photo_sha256 must be a lowercase SHA-256")
    block = {
        "recipe": recipe,
        "sources": [{"role": "original", "sha256": photo_sha256}],
    }
    if focus is not None:
        block["focus_area"] = list(focus)
    return block, fingerprint(block)


def detector_input(photo_sha256, focus):
    recipe = "vireo-detector-source-v1" if focus is None else "vireo-detector-focus-v2"
    return source_input(photo_sha256, recipe, focus=focus)


def catalog_detector_input(photo_sha256, detector_model, focus_area):
    focus = json.loads(focus_area) if focus_area and detector_model == "megadetector-v6" else None
    return detector_input(photo_sha256, focus)
