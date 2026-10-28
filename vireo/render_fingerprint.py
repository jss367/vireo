"""Cache identities for camera/lens-dependent edits outside the saved recipe."""

import hashlib
import json
import os

from PIL import Image

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


def cache_save_options(photo, recipe, source_path=None, *, image=None):
    if not ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        return camera_denoise.cache_save_options(photo, recipe)
    return {"comment": _optical_marker(render_cache_fields(photo, recipe, source_path, image=image))}


def cache_matches(path, photo, recipe, source_path=None, *, allow_fallback=False):
    if not ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        return camera_denoise.cache_matches(path, photo, recipe)
    try:
        with Image.open(path) as image:
            marker = image.info.get("comment")
        fields = render_cache_fields(photo, recipe, source_path)
        if marker == _optical_marker(fields):
            return True
        # Equal-key flight waiters may consume the producer's fresh fallback
        # once. Later cache requests must retry the RAW before reusing it.
        return allow_fallback and marker == _optical_marker({**fields, "lens_source": "rendered"})
    except (OSError, ValueError):
        return False
