"""Scan folders, discover photos, read metadata, populate database."""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import imagehash
from image_loader import SUPPORTED_EXTENSIONS
from metadata import extract_metadata
from PIL import Image
from xmp import read_hierarchical_keywords, read_keywords

log = logging.getLogger(__name__)


def _import_keywords_for_photo(db, photo_id, xmp_path_str):
    """Read flat and hierarchical keywords from XMP and populate the database."""
    flat_keywords = read_keywords(xmp_path_str)
    hier_keywords = read_hierarchical_keywords(xmp_path_str)

    # Build hierarchy from lr:hierarchicalSubject
    # e.g., 'Birds|Raptors|Black kite' creates Birds -> Raptors -> Black kite
    for hier in hier_keywords:
        parts = hier.split("|")
        parent_id = None
        for part in parts:
            kid = db.add_keyword(part, parent_id=parent_id)
            parent_id = kid
        # Tag with the leaf keyword
        db.tag_photo(photo_id, parent_id)

    # Also add any flat keywords not already covered by hierarchy
    existing_kw_names = {k["name"] for k in db.get_photo_keywords(photo_id)}
    for kw in flat_keywords:
        if kw not in existing_kw_names:
            kid = db.add_keyword(kw)
            db.tag_photo(photo_id, kid)


def _extract_dimensions(exif_group, file_group):
    """Extract width and height from ExifTool metadata groups.

    Priority:
    1. EXIF:ExifImageWidth / EXIF:ExifImageHeight (actual image dimensions for JPEGs)
    2. EXIF:ImageWidth / EXIF:ImageHeight
    3. File:ImageWidth / File:ImageHeight
    """
    width = (
        exif_group.get("ExifImageWidth")
        or exif_group.get("ImageWidth")
        or file_group.get("ImageWidth")
    )
    height = (
        exif_group.get("ExifImageHeight")
        or exif_group.get("ImageHeight")
        or file_group.get("ImageHeight")
    )
    if width is not None:
        width = int(width)
    if height is not None:
        height = int(height)
    return width, height


def _extract_timestamp(exif_group):
    """Extract and normalize timestamp from ExifTool EXIF group.

    Checks EXIF:DateTimeOriginal first, then EXIF:CreateDate.
    Returns ISO format string or None.
    """
    dto = exif_group.get("DateTimeOriginal") or exif_group.get("CreateDate")
    if not dto:
        return None
    try:
        dt = datetime.strptime(str(dto), "%Y:%m:%d %H:%M:%S")
        return dt.isoformat()
    except (ValueError, TypeError):
        return str(dto)


def scan(root, db, progress_callback=None, incremental=False):
    """Walk a folder tree, discover photos, read metadata, populate database.

    Args:
        root: path to the root folder to scan
        db: Database instance
        progress_callback: optional callable(current, total) for progress reporting
        incremental: if True, skip files unchanged since last scan
    """
    root_path = Path(root)
    if not root_path.is_dir():
        log.warning("Root path does not exist or is not a directory: %s", root)
        return

    # Discover all image files
    image_files = sorted(
        f
        for f in root_path.rglob("*")
        if f.is_file()
        and f.suffix.lower() in SUPPORTED_EXTENSIONS
        and not f.name.startswith(".")
    )

    total = len(image_files)
    log.info("Found %d images in %s", total, root)

    # Batch extract metadata via ExifTool for all files
    all_paths = [str(f) for f in image_files]
    metadata_map = extract_metadata(all_paths)

    # Build existing photo lookup for incremental mode
    existing_photos = {}
    if incremental:
        all_photos = db.get_photos(per_page=999999)
        for p in all_photos:
            # Key by folder_id + filename won't work easily, so use a second lookup
            existing_photos[p["id"]] = p
        # Build a path-based lookup: we need folder path + filename
        existing_by_path = {}
        folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
        for p in all_photos:
            folder_path = folders.get(p["folder_id"], "")
            full_path = os.path.join(folder_path, p["filename"])
            existing_by_path[full_path] = p

    # Build folder cache: path -> folder_id
    folder_cache = {}

    def _ensure_folder(folder_path):
        """Ensure a folder and all its parents exist in the DB. Returns folder_id."""
        folder_str = str(folder_path)
        if folder_str in folder_cache:
            return folder_cache[folder_str]

        parent_id = None
        if folder_path != root_path:
            parent_id = _ensure_folder(folder_path.parent)

        folder_id = db.add_folder(
            path=folder_str,
            name=folder_path.name,
            parent_id=parent_id,
        )
        folder_cache[folder_str] = folder_id
        return folder_id

    for i, image_path in enumerate(image_files):
        folder_id = _ensure_folder(image_path.parent)

        # File stats
        stat = image_path.stat()
        file_size = stat.st_size
        file_mtime = stat.st_mtime

        # XMP sidecar
        xmp_path = image_path.with_suffix(".xmp")
        xmp_mtime = None
        if xmp_path.exists():
            xmp_mtime = xmp_path.stat().st_mtime

        # Incremental: check if file needs processing
        if incremental:
            full_path_str = str(image_path)
            existing = existing_by_path.get(full_path_str)
            if existing:
                file_unchanged = existing["file_mtime"] == file_mtime
                xmp_unchanged = existing["xmp_mtime"] == xmp_mtime

                if file_unchanged and xmp_unchanged:
                    if progress_callback:
                        progress_callback(i + 1, total)
                    continue

                # XMP changed: re-import keywords
                if not xmp_unchanged and xmp_mtime is not None:
                    _import_keywords_for_photo(db, existing["id"], str(xmp_path))
                    db.conn.execute(
                        "UPDATE photos SET xmp_mtime = ? WHERE id = ?",
                        (xmp_mtime, existing["id"]),
                    )
                    db.conn.commit()

                if file_unchanged:
                    if progress_callback:
                        progress_callback(i + 1, total)
                    continue

        # Get pre-extracted metadata for this file
        file_meta = metadata_map.get(str(image_path), {})
        file_group = file_meta.get("File", {})
        exif_group = file_meta.get("EXIF", {})
        composite = file_meta.get("Composite", {})

        # Dimensions from ExifTool (works for all file types including RAW)
        width, height = _extract_dimensions(exif_group, file_group)

        # Timestamp from ExifTool
        timestamp = _extract_timestamp(exif_group)

        # Focal length
        focal_length = exif_group.get("FocalLength")
        if focal_length is not None:
            focal_length = float(focal_length)

        # Burst ID (ImageUniqueID)
        burst_id = exif_group.get("ImageUniqueID")
        if burst_id:
            burst_id = str(burst_id)

        # GPS coordinates — ExifTool with -n gives decimal degrees directly
        latitude = composite.get("GPSLatitude") or exif_group.get("GPSLatitude")
        longitude = composite.get("GPSLongitude") or exif_group.get("GPSLongitude")

        # Compute perceptual hash (computed, not from EXIF)
        phash = None
        try:
            with Image.open(str(image_path)) as img:
                phash = str(imagehash.phash(img))
        except Exception:
            log.debug("Could not compute pHash for %s", image_path)

        photo_id = db.add_photo(
            folder_id=folder_id,
            filename=image_path.name,
            extension=image_path.suffix.lower(),
            file_size=file_size,
            file_mtime=file_mtime,
            xmp_mtime=xmp_mtime,
            timestamp=timestamp,
            width=width,
            height=height,
        )

        # Store GPS, pHash, focal length, burst ID, and exif_data
        updates = []
        update_params = []
        if latitude is not None:
            updates.extend(["latitude=?", "longitude=?"])
            update_params.extend([latitude, longitude])
        if phash is not None:
            updates.append("phash=?")
            update_params.append(phash)
        if focal_length is not None:
            updates.append("focal_length=?")
            update_params.append(focal_length)
        if burst_id is not None:
            updates.append("burst_id=?")
            update_params.append(burst_id)
        if file_meta:
            updates.append("exif_data=?")
            update_params.append(json.dumps(file_meta))
        if updates:
            update_params.append(photo_id)
            db.conn.execute(
                f"UPDATE photos SET {', '.join(updates)} WHERE id=?",
                update_params,
            )
            db.conn.commit()

        # Import XMP keywords if sidecar exists
        if xmp_path.exists():
            _import_keywords_for_photo(db, photo_id, str(xmp_path))

        if progress_callback:
            progress_callback(i + 1, total)

    db.update_folder_counts()
    log.info("Scan complete: %d photos indexed", total)
