"""Shared EXIF orientation helpers.

Centralizes the ``Orientation`` tag interpretation used by thumbnails,
exports, the scanner, the Flask app, and the pipeline. Keeping the
predicate here prevents the per-file copies from drifting (e.g. accepting
``int|float`` numerics, swallowing booleans, parsing string variants like
``"90 cw"``).
"""


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
