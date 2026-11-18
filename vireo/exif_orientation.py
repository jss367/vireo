"""Shared EXIF orientation helpers.

Centralizes the ``Orientation`` tag interpretation used by thumbnails,
exports, the scanner, the Flask app, and the pipeline. Keeping the
predicate here prevents the per-file copies from drifting (e.g. accepting
``int|float`` numerics, swallowing booleans, parsing string variants like
``"90 cw"``).
"""


import json


def exif_orientation(exif_data):
    """Return the EXIF ``Orientation`` value from stored metadata, or None.

    ``exif_data`` may be a JSON string (as stored on the photo row) or an
    already-parsed dict. Looks in the common metadata groups ExifTool emits
    before falling back to a top-level ``Orientation`` key.
    """
    if not exif_data:
        return None
    if isinstance(exif_data, str):
        try:
            metadata = json.loads(exif_data)
        except (TypeError, ValueError):
            return None
    elif isinstance(exif_data, dict):
        metadata = exif_data
    else:
        return None
    if not isinstance(metadata, dict):
        return None
    for group in ("EXIF", "IFD0", "TIFF", "File"):
        values = metadata.get(group)
        if isinstance(values, dict) and "Orientation" in values:
            return values["Orientation"]
    return metadata.get("Orientation")


def orientation_swaps_axes(orientation):
    """Return True for EXIF Orientation values that rotate by 90°/270°.

    Accepts integers, floats, and the string forms ExifTool emits.
    Values 5-8 swap the rendered axes; values 1-4 do not.
    """
    if orientation is None or isinstance(orientation, bool):
        return False
    if isinstance(orientation, int | float):
        return int(orientation) in (5, 6, 7, 8)
    text = str(orientation).strip().lower()
    if not text:
        return False
    try:
        return int(text) in (5, 6, 7, 8)
    except ValueError:
        return "90" in text or "270" in text
