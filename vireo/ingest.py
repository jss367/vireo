"""Smart ingest: copy and organize photos from external source to destination."""

import logging

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
