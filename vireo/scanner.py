"""Scan folders, discover photos, read metadata, populate database."""

import logging
import os
from pathlib import Path
from xml.etree import ElementTree as ET

from PIL import Image

from compare import read_xmp_keywords
from image_loader import IMAGE_EXTENSIONS, SUPPORTED_EXTENSIONS
from grouping import read_exif_timestamp

log = logging.getLogger(__name__)

NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_LR = "http://ns.adobe.com/lightroom/1.0/"


def _read_hierarchical_keywords(xmp_path):
    """Read lr:hierarchicalSubject from an XMP sidecar.

    Returns a list of pipe-delimited hierarchy strings, e.g. ['Birds|Raptors|Black kite'].
    """
    path = Path(xmp_path)
    if not path.exists():
        return []

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        return []

    root = tree.getroot()
    results = []
    for li in root.findall(
        f".//{{{NS_LR}}}hierarchicalSubject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"
    ):
        if li.text:
            results.append(li.text)
    return results


def _import_keywords_for_photo(db, photo_id, xmp_path_str):
    """Read flat and hierarchical keywords from XMP and populate the database."""
    flat_keywords = read_xmp_keywords(xmp_path_str)
    hier_keywords = _read_hierarchical_keywords(xmp_path_str)

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

        # Read dimensions
        width, height = None, None
        try:
            ext = image_path.suffix.lower()
            if ext in SUPPORTED_EXTENSIONS and ext not in IMAGE_EXTENSIONS:
                # RAW file — PIL only reads the thumbnail, use rawpy
                import rawpy

                with rawpy.imread(str(image_path)) as raw:
                    sizes = raw.sizes
                    width, height = sizes.width, sizes.height
            else:
                with Image.open(str(image_path)) as img:
                    width, height = img.size
        except Exception:
            log.debug("Could not read dimensions from %s", image_path)

        # Read EXIF timestamp
        timestamp = None
        try:
            dt = read_exif_timestamp(str(image_path))
            if dt:
                timestamp = dt.isoformat()
        except Exception:
            log.debug("Could not read EXIF timestamp from %s", image_path)

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

        # Import XMP keywords if sidecar exists
        if xmp_path.exists():
            _import_keywords_for_photo(db, photo_id, str(xmp_path))

        if progress_callback:
            progress_callback(i + 1, total)

    db.update_folder_counts()
    log.info("Scan complete: %d photos indexed", total)
