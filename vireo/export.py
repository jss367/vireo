"""Photo export with resize, quality control, and template-based naming."""

import logging
import os
import re

log = logging.getLogger(__name__)

# Characters not allowed in filenames (covers Windows + macOS + Linux)
_UNSAFE_RE = re.compile(r'[<>:"/|?*\\]')


def sanitize_filename(name):
    """Replace filesystem-unsafe characters with underscores."""
    return _UNSAFE_RE.sub("_", name)


def resolve_template(template, photo, species=None, seq=1):
    """Resolve a naming template against photo metadata.

    Args:
        template: naming template with {variable} placeholders
        photo: dict with filename, timestamp, rating, folder_name
        species: species name string or None (falls back to "unknown")
        seq: sequence number (1-based)

    Returns:
        Resolved path string (may contain '/' for subdirectories)
    """
    stem = os.path.splitext(photo["filename"])[0]
    ts = photo.get("timestamp") or ""

    if ts:
        date_part = ts[:10]
        time_part = ts[11:19].replace(":", "") if len(ts) >= 19 else "000000"
    else:
        date_part = "unknown-date"
        time_part = "000000"

    species_name = species or "unknown"

    replacements = {
        "original": stem,
        "date": date_part,
        "datetime": f"{date_part}_{time_part}",
        "species": sanitize_filename(species_name),
        "rating": str(photo.get("rating") or 0),
        "seq": f"{seq:03d}",
        "folder": sanitize_filename(photo.get("folder_name") or ""),
    }

    result = template
    for key, value in replacements.items():
        result = result.replace("{" + key + "}", value)

    return result
