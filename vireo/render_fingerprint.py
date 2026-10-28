"""Cache identities for camera/lens-dependent edits outside the saved recipe."""

import hashlib
import json
import os

from PIL import Image

try:
    from . import camera_denoise, lens_profiles
    from .image_loader import RAW_EXTENSIONS
except ImportError:
    import camera_denoise
    import lens_profiles
    from image_loader import RAW_EXTENSIONS


def _lens_profile_actually_applied(recipe, source_path):
    """Lens profile only applies to decoded RAW; a fallback source skips it.

    When ``source_path`` is unknown (``None``) we assume the recipe's intent
    was honored: this preserves the pre-source-aware marker for callers that
    cannot name the rendered source. A known non-RAW source overrides that.
    """
    if not ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        return False
    if source_path is None:
        return True
    return os.path.splitext(source_path)[1].lower() in RAW_EXTENSIONS


def render_cache_fields(photo, recipe, source_path=None):
    fields = camera_denoise.render_cache_fields(photo, recipe)
    if _lens_profile_actually_applied(recipe, source_path):
        lens_key = lens_profiles.render_cache_key(photo, recipe)
        if lens_key:
            fields["lens_profile"] = lens_key
    return fields


def cache_save_options(photo, recipe, source_path=None):
    if not _lens_profile_actually_applied(recipe, source_path):
        return camera_denoise.cache_save_options(photo, recipe)
    fields = json.dumps(render_cache_fields(photo, recipe, source_path),
                        sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(fields.encode("utf-8")).hexdigest()
    return {"comment": f"vireo-render-inputs:{digest}".encode("ascii")}


def cache_matches(path, photo, recipe, source_path=None):
    if not _lens_profile_actually_applied(recipe, source_path):
        return camera_denoise.cache_matches(path, photo, recipe)
    try:
        with Image.open(path) as image:
            expected = cache_save_options(photo, recipe, source_path)["comment"]
            return image.info.get("comment") == expected
    except (OSError, ValueError):
        return False
