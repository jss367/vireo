"""Cache identities for camera/lens-dependent edits outside the saved recipe."""

import hashlib
import json

from PIL import Image

try:
    from . import camera_denoise, lens_profiles
except ImportError:
    import camera_denoise
    import lens_profiles


def render_cache_fields(photo, recipe):
    fields = camera_denoise.render_cache_fields(photo, recipe)
    lens_key = lens_profiles.render_cache_key(photo, recipe)
    if lens_key:
        fields["lens_profile"] = lens_key
    return fields


def cache_save_options(photo, recipe):
    if not ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        return camera_denoise.cache_save_options(photo, recipe)
    fields = json.dumps(render_cache_fields(photo, recipe), sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(fields.encode("utf-8")).hexdigest()
    return {"comment": f"vireo-render-inputs:{digest}".encode("ascii")}


def cache_matches(path, photo, recipe):
    if not ((recipe or {}).get("adjustments") or {}).get("lens_profile"):
        return camera_denoise.cache_matches(path, photo, recipe)
    try:
        with Image.open(path) as image:
            return image.info.get("comment") == cache_save_options(photo, recipe)["comment"]
    except (OSError, ValueError):
        return False
