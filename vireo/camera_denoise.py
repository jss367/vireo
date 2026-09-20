"""Camera/ISO-guided denoising of developed RGB images.

The bundled darktable measurements describe *linear sensor* variance, not
variance in a developed JPEG. They supply a bounded prior; noise measured in
the actual render determines the filter strength. This avoids treating RAW
coefficients as display-space noise or trusting ISO alone after resizing,
in-camera processing, or tone edits. Unknown cameras use image estimates.
"""

from __future__ import annotations

import json
import logging
import math
import re
from functools import lru_cache
from pathlib import Path

import numpy as np
from PIL import Image

_DATA = Path(__file__).parent / "data" / "denoise" / "noiseprofiles.json"
_TILE_PIXELS = 1_000_000
_MAKERS = {
    "nikoncorporation": "nikon", "canoninc": "canon",
    "sonycorporation": "sony", "olympusimagingcorp": "olympus",
    "olympuscorporation": "olympus", "omsystem": "omdigitalsolutions",
    "ricohimagingcompanyltd": "pentax",
}


def _key(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


def _identity(make, model):
    make = _MAKERS.get(_key(make), _key(make))
    model = _key(model)
    # EXIF often repeats the maker (e.g. NIKON Z 8 versus Z 8).
    if make and model.startswith(make):
        model = model[len(make):]
    return make, model


def _value(photo, key):
    try:
        return photo[key]
    except (KeyError, IndexError, TypeError):
        return None


def camera_metadata(photo=None, exif_data=None):
    """Read promoted columns or grouped EXIF, including sqlite Row inputs."""
    if exif_data is None:
        exif_data = _value(photo, "exif_data") or _value(photo, "metadata")
    if isinstance(exif_data, str):
        try:
            exif_data = json.loads(exif_data)
        except (ValueError, TypeError):
            exif_data = None
    exif = {}
    if isinstance(exif_data, dict):
        exif.update(exif_data)
        for group in ("TIFF", "IFD0", "ExifIFD", "EXIF"):
            if isinstance(exif_data.get(group), dict):
                exif.update(exif_data[group])
    make = _value(photo, "camera_make") or exif.get("Make") or ""
    model = _value(photo, "camera_model") or exif.get("Model") or ""
    raw_iso = _value(photo, "iso") or exif.get("ISO") or exif.get("PhotographicSensitivity")
    try:
        iso = float(raw_iso) if not isinstance(raw_iso, bool) else 0.0
    except (ValueError, TypeError, OverflowError):
        iso = 0.0
    return {
        "camera_make": str(make), "camera_model": str(model),
        "iso": iso if math.isfinite(iso) and iso > 0 else None,
    }


@lru_cache(maxsize=1)
def _profiles():
    try:
        data = json.loads(_DATA.read_text(encoding="utf-8"))
        result = {}
        for maker in data["noiseprofiles"]:
            for model in maker["models"]:
                profiles = []
                for profile in model["profiles"]:
                    a, b = profile["a"], profile["b"]
                    if (profile["iso"] > 0 and len(a) == len(b) == 3
                            and all(math.isfinite(v) and v > 0 for v in a)
                            and all(math.isfinite(v) for v in b)):
                        profiles.append(profile)
                if profiles:
                    result[_identity(maker["maker"], model["model"])] = sorted(
                        profiles, key=lambda p: p["iso"],
                    )
        return result
    except (OSError, ValueError, KeyError, TypeError):
        logging.getLogger(__name__).warning("Camera noise measurements unavailable; using image estimates")
        return {}


def resolve_profile(photo=None, exif_data=None):
    """Resolve exact camera identity and linearly interpolate measured ISO.

    Outside the measured range use the closest endpoint, never extrapolate
    across unknown sensor gain modes. No fuzzy model matching: a Z 6 II must
    not inherit Z 6 or Z 6 III measurements.
    """
    metadata = camera_metadata(photo, exif_data)
    result = {**metadata, "source": "image", "match": None}
    iso = metadata["iso"]
    profiles = _profiles().get(_identity(metadata["camera_make"], metadata["camera_model"]))
    if not profiles or iso is None:
        return result
    lower = max((p for p in profiles if p["iso"] <= iso), key=lambda p: p["iso"], default=profiles[0])
    upper = min((p for p in profiles if p["iso"] >= iso), key=lambda p: p["iso"], default=profiles[-1])
    t = (iso - lower["iso"]) / (upper["iso"] - lower["iso"]) if upper["iso"] != lower["iso"] else 0.0
    result.update({
        "source": "camera", "profile_iso": [lower["iso"], upper["iso"]],
        "match": "interpolated" if lower != upper else ("exact" if iso == lower["iso"] else "nearest"),
        "a": [(1 - t) * x + t * y for x, y in zip(lower["a"], upper["a"], strict=True)],
        "b": [(1 - t) * x + t * y for x, y in zip(lower["b"], upper["b"], strict=True)],
    })
    return result


def _estimate_noise(rgb):
    """Robust channel noise estimates from bounded, native-pixel patches.

    A 2x2 diagonal Haar residual rejects constant regions and linear ramps.
    MAD / 0.67449 estimates Gaussian sigma. Lower-quartile patch estimates
    limit contamination from texture; clipped/flat patches are excluded.
    Sampling patches (not resizing the image) preserves the noise amplitude.
    """
    import cv2

    height, width = rgb.shape[:2]
    estimates = []
    for y in np.unique(np.linspace(0, max(0, height - 64), min(8, max(1, height // 32))).astype(int)):
        for x in np.unique(np.linspace(0, max(0, width - 64), min(8, max(1, width // 32))).astype(int)):
            patch = cv2.cvtColor(np.ascontiguousarray(rgb[y:y + 64, x:x + 64]), cv2.COLOR_RGB2YCrCb).astype(np.float32)
            h, w = patch.shape[:2]
            h, w = h // 2 * 2, w // 2 * 2
            if min(h, w) < 4:
                continue
            p = patch[:h, :w]
            residual = (p[::2, ::2] - p[1::2, ::2] - p[::2, 1::2] + p[1::2, 1::2]) / 2
            mad = np.median(np.abs(residual - np.median(residual, axis=(0, 1))), axis=(0, 1)) / 0.67449
            if 8 < np.median(p[..., 0]) < 247:
                estimates.append(mad)
    if not estimates:
        return np.zeros(3, dtype=np.float32)
    return np.percentile(estimates, 25, axis=0).astype(np.float32)


def _filter_strength(rgb, profile, scale):
    measured = _estimate_noise(rgb)
    if profile and profile.get("source") == "camera":
        # Approximate mid-grey sRGB amplitude as a *prior*, not a claim that
        # sensor RGB equals display RGB. Actual render measurements bound its
        # effect to +/-20%, including tone, JPEG processing and downsampling.
        variance = max(0.0, profile["a"][1] * 0.18 + profile["b"][1])
        slope = (1.055 / 2.4) * 0.18 ** (1 / 2.4 - 1)
        prior = math.sqrt(variance) * slope * 255 * min(1.0, max(0.01, scale))
        ratio = prior / (prior + max(1.0, float(measured[0])))
        measured *= np.float32(0.8 + 0.4 * ratio)
    return np.clip(measured, 0, 35)


def apply_camera_denoise(img, amount, *, profile=None, scale=1.0):
    """Apply tiled non-local means, preserving alpha and a zero-amount no-op."""
    if amount <= 0:
        return img
    import cv2

    has_alpha = "A" in img.getbands() or "transparency" in img.info
    src = np.asarray(img.convert("RGBA" if has_alpha else "RGB"))
    strength = _filter_strength(src[..., :3], profile, scale)
    if float(np.max(strength)) < 0.5:
        return img
    blend = min(1.0, amount / 100.0)
    # Both the search radius and patch radius contribute to the halo.
    template, search = 7, 21
    halo = template // 2 + search // 2
    height, width = src.shape[:2]
    rows = max(1, _TILE_PIXELS // max(1, width))
    out = src.copy()
    for top in range(0, height, rows):
        bottom = min(height, top + rows)
        start, end = max(0, top - halo), min(height, bottom + halo)
        rgb = np.ascontiguousarray(src[start:end, :, :3])
        ycc = cv2.cvtColor(rgb, cv2.COLOR_RGB2YCrCb)
        filtered = ycc.copy()
        for channel in range(3):
            if strength[channel] >= 0.5:
                filtered[..., channel] = cv2.fastNlMeansDenoising(
                    ycc[..., channel], None, float(strength[channel] * 0.85), template, search,
                )
        denoised = cv2.cvtColor(filtered, cv2.COLOR_YCrCb2RGB)
        mixed = rgb.astype(np.float32) + (denoised.astype(np.float32) - rgb) * blend
        out[top:bottom, :, :3] = np.clip(mixed[top - start:bottom - start] + 0.5, 0, 255).astype(np.uint8)
    return Image.fromarray(out)
