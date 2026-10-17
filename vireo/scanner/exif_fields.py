"""Dimensions and capture time read from ExifTool's metadata groups."""

import logging
from datetime import datetime

from exif_orientation import orientation_swaps_axes as _orientation_swaps_axes
from image_loader import RAW_EXTENSIONS
from render_source import exif_orientation as _exif_orientation_from_data

log = logging.getLogger(__name__)


def _scaled_dimensions(width, height, max_size):
    try:
        width = int(width or 0)
        height = int(height or 0)
    except (TypeError, ValueError):
        return 0, 0
    if width <= 0 or height <= 0:
        return 0, 0
    if max_size and max_size > 0:
        long_edge = max(width, height)
        if long_edge > max_size:
            scale = max_size / long_edge
            width = round(width * scale)
            height = round(height * scale)
    return width, height


def _oriented_dimensions(width, height, exif_data):
    """Return (width, height) rotated to match what extract_working_copy writes.

    Stored ``width``/``height`` come straight from the sensor/file metadata,
    so for portrait shots taken on a landscape sensor they're the unrotated
    axes (e.g. 6000x4000 with EXIF Orientation 6). The request-path helpers
    in thumbnails/app/export/pipeline normalize these to display orientation
    before comparing against rendered pixels; scanner's RAW-undersize check
    must do the same or it sees the orientation-normalized JPEG written by
    ``extract_working_copy`` (4000x6000) as catastrophically undersized vs.
    the raw 6000x4000 and falls back to the companion JPEG.
    """
    if _orientation_swaps_axes(_exif_orientation_from_data(exif_data)):
        return height, width
    return width, height


def _extract_dimensions(exif_group, file_group, extension=None):
    """Extract width and height from ExifTool metadata groups.

    For standard images (JPEG, PNG, etc.):
    1. EXIF:ExifImageWidth / EXIF:ExifImageHeight
    2. EXIF:ImageWidth / EXIF:ImageHeight
    3. File:ImageWidth / File:ImageHeight

    For RAW files (NEF, CR2, ARW, etc.), ExifImageWidth/Height contains the
    embedded JPEG thumbnail dimensions (e.g. 160x120), not the actual image.
    Priority for RAW:
    1. File:ImageWidth / File:ImageHeight (actual decoded dimensions)
    2. EXIF:ImageWidth / EXIF:ImageHeight
    """
    is_raw = extension and extension.lower() in RAW_EXTENSIONS

    if is_raw:
        width = file_group.get("ImageWidth")
        if width is None:
            width = exif_group.get("ImageWidth")
        height = file_group.get("ImageHeight")
        if height is None:
            height = exif_group.get("ImageHeight")
    else:
        width = exif_group.get("ExifImageWidth")
        if width is None:
            width = exif_group.get("ImageWidth")
        if width is None:
            width = file_group.get("ImageWidth")
        height = exif_group.get("ExifImageHeight")
        if height is None:
            height = exif_group.get("ImageHeight")
        if height is None:
            height = file_group.get("ImageHeight")

    if width is not None:
        width = int(width)
    if height is not None:
        height = int(height)
    return width, height


def _extract_timestamp(exif_group):
    """Extract and normalize timestamp from ExifTool EXIF group.

    Checks EXIF:DateTimeOriginal first, then EXIF:CreateDate.
    If SubSecTimeOriginal (or SubSecTime) is present and numeric,
    it is included as fractional seconds for sub-second precision.
    Returns ISO format string or None.
    """
    dto = exif_group.get("DateTimeOriginal") or exif_group.get("CreateDate")
    if not dto:
        return None
    try:
        dt = datetime.strptime(str(dto), "%Y:%m:%d %H:%M:%S")
        # Attempt to add sub-second precision
        subsec = exif_group.get("SubSecTimeOriginal") or exif_group.get("SubSecTime")
        if subsec is not None:
            subsec_str = str(subsec).strip()
            if subsec_str.isdigit():
                # Pad or truncate to 6 digits (microseconds)
                us_str = subsec_str[:6].ljust(6, "0")
                dt = dt.replace(microsecond=int(us_str))
        return dt.isoformat()
    except (ValueError, TypeError):
        log.debug("Unparseable EXIF timestamp dropped: %r", dto)
        return None
