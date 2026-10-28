"""Cache identities for camera/lens-dependent edits outside the saved recipe."""

import hashlib
import json
import os
import time

from PIL import Image

_FALLBACK_RETRY_SECONDS = 60

try:
    from . import camera_denoise, lens_profiles
except ImportError:
    import camera_denoise
    import lens_profiles


def _source_kind(photo, source_path, image):
    """Prefer actual decode provenance; use selected source intent for cache reads."""
    if image is not None:
        return image.info.get("vireo_lens_source", "rendered")
    extension = os.path.splitext(source_path)[1].lower() if source_path is not None else lens_profiles.lens_metadata(photo)["extension"]
    return "linear" if extension in lens_profiles._RAW_EXTENSIONS else "rendered"


def render_cache_fields(photo, recipe, source_path=None, *, image=None):
    fields = camera_denoise.render_cache_fields(photo, recipe)
    lens_key = lens_profiles.render_cache_key(photo, recipe)
    if lens_key:
        fields["lens_profile"] = lens_key
        fields["lens_source"] = _source_kind(photo, source_path, image)
    return fields


def record_render_source(result, source, recipe):
    """Retain decode provenance after tone/detail convert RAW pixels to sRGB."""
    if ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        try:
            from .float_image import FloatImage
        except ImportError:
            from float_image import FloatImage
        result.info["vireo_lens_source"] = (
            "linear" if isinstance(source, FloatImage) and source.encoding == "linear" else "rendered"
        )
    return result


def _optical_marker(fields):
    value = json.dumps(fields, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"vireo-render-inputs:{digest}".encode("ascii")


def _source_stamp(path):
    """Opaque source health identity; cached JPEGs never disclose local paths."""
    if path is None:
        return ""
    try:
        stat = os.stat(path)
        state = [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_mode]
    except OSError:
        state = None
    value = json.dumps([os.path.abspath(path), state], separators=(",", ":"))
    return hashlib.sha256(value.encode()).hexdigest()


def cache_save_options(photo, recipe, source_path=None, *, image=None):
    if not ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        return camera_denoise.cache_save_options(photo, recipe)
    fields = render_cache_fields(photo, recipe, source_path, image=image)
    marker = _optical_marker(fields)
    if image is not None and fields["lens_source"] == "rendered" and lens_profiles.lens_metadata(photo)["extension"] in lens_profiles._RAW_EXTENSIONS:
        marker += f"|{_source_stamp(source_path)}|{time.time():.6f}".encode("ascii")
    return {"comment": marker}


def cache_matches(path, photo, recipe, source_path=None, *, allow_fallback=False):
    if not ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        return camera_denoise.cache_matches(path, photo, recipe)
    try:
        with Image.open(path) as image:
            marker = image.info.get("comment")
        fields = render_cache_fields(photo, recipe, source_path)
        raw = lens_profiles.lens_metadata(photo)["extension"] in lens_profiles._RAW_EXTENSIONS
        if marker == _optical_marker({**fields, "lens_source": "linear"} if raw else fields):
            return True
        return _fallback_matches(marker, fields, source_path, allow_fallback)
    except (OSError, ValueError):
        return False


def _fallback_matches(marker, fields, source_path, allow_fallback):
    if not isinstance(marker, bytes):
        return False
    parts = marker.split(b"|")
    if len(parts) != 3 or parts[0] != _optical_marker({**fields, "lens_source": "rendered"}):
        return False
    age = time.time() - float(parts[2])
    if not 0 <= age < float("inf"):
        return False
    if allow_fallback:
        return age <= _FALLBACK_RETRY_SECONDS
    if source_path is None or not parts[1] or parts[1].decode("ascii") != _source_stamp(source_path):
        return False
    if os.path.splitext(source_path)[1].lower() not in lens_profiles._RAW_EXTENSIONS or not os.path.isfile(source_path):
        return True  # Still the same explicitly rendered/offline source.
    # Stable unsupported RAWs reuse their fallback briefly; transient decode
    # failures are retried without decoding on every hover or bulk pass.
    return age <= _FALLBACK_RETRY_SECONDS
