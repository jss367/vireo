"""Smart ingest: copy and organize photos from external source to destination."""

import logging
from pathlib import Path

from image_loader import IMAGE_EXTENSIONS, RAW_EXTENSIONS, SUPPORTED_EXTENSIONS

log = logging.getLogger(__name__)


def build_destination_path(exif_timestamp, template="%Y/%m/%d"):
    """Build relative destination folder path from EXIF timestamp.

    Args:
        exif_timestamp: datetime object from EXIF, or None
        template: strftime format string for folder structure

    Returns:
        Relative path string, or "unsorted" if no timestamp
    """
    if exif_timestamp is None:
        return "unsorted"
    return exif_timestamp.strftime(template)


def discover_source_files(source_dir, file_types="both"):
    """Discover image files in source directory.

    Args:
        source_dir: path to source directory (e.g., SD card mount)
        file_types: "raw", "jpeg", or "both"

    Returns:
        Sorted list of Path objects for matching files
    """
    source_path = Path(source_dir)
    if not source_path.is_dir():
        return []

    if file_types == "raw":
        allowed = RAW_EXTENSIONS
    elif file_types == "jpeg":
        allowed = IMAGE_EXTENSIONS
    else:
        allowed = SUPPORTED_EXTENSIONS

    return sorted(
        f
        for f in source_path.rglob("*")
        if f.is_file()
        and f.suffix.lower() in allowed
        and not f.name.startswith(".")
    )
