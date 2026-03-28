"""Scan folders, discover photos, read metadata, populate database."""

import hashlib
import logging
import os
from pathlib import Path

import imagehash
from grouping import read_exif_timestamp
from image_loader import IMAGE_EXTENSIONS, SUPPORTED_EXTENSIONS
from PIL import Image
from xmp import read_hierarchical_keywords, read_keywords

log = logging.getLogger(__name__)


def compute_file_hash(file_path, chunk_size=65536):
    """Compute SHA-256 hash of a file. Returns hex digest string."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


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


def _pair_raw_jpeg_companions(db):
    """Find raw+JPEG pairs in the same folder and merge them.

    When both IMG_001.cr3 and IMG_001.jpg exist in the same folder,
    keep the raw as the primary photo and set companion_path to the JPEG filename.
    Delete the duplicate JPEG-only photo record.
    """
    raw_exts = {".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"}
    jpeg_exts = {".jpg", ".jpeg"}

    rows = db.conn.execute(
        "SELECT id, folder_id, filename, extension FROM photos ORDER BY folder_id, filename"
    ).fetchall()

    # Group by folder_id + base name (without extension)
    from collections import defaultdict
    groups = defaultdict(list)
    for row in rows:
        base = os.path.splitext(row["filename"])[0]
        groups[(row["folder_id"], base)].append(dict(row))

    for (_folder_id, _base), members in groups.items():
        if len(members) < 2:
            continue

        raws = [m for m in members if m["extension"] in raw_exts]
        jpegs = [m for m in members if m["extension"] in jpeg_exts]

        if not raws or not jpegs:
            continue

        # Use first raw as primary, first JPEG as companion
        primary = raws[0]
        companion = jpegs[0]

        db.conn.execute(
            "UPDATE photos SET companion_path = ? WHERE id = ?",
            (companion["filename"], primary["id"]),
        )
        # Remove the duplicate JPEG record
        db.conn.execute("DELETE FROM photos WHERE id = ?", (companion["id"],))

    db.conn.commit()


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

        # Compute perceptual hash
        phash = None
        try:
            with Image.open(str(image_path)) as img:
                phash = str(imagehash.phash(img))
        except Exception:
            log.debug("Could not compute pHash for %s", image_path)

        # Read EXIF timestamp
        timestamp = None
        try:
            dt = read_exif_timestamp(str(image_path))
            if dt:
                timestamp = dt.isoformat()
        except Exception:
            log.debug("Could not read EXIF timestamp from %s", image_path)

        # Read EXIF focal length and burst/sequence ID
        focal_length = None
        burst_id = None
        try:
            with Image.open(str(image_path)) as img:
                exif = img.getexif()
                # FocalLength is EXIF tag 0x920A
                fl = exif.get(0x920A)
                if fl is not None:
                    focal_length = float(fl)
                # Check EXIF IFD for FocalLength if not in main IFD
                if focal_length is None:
                    exif_ifd = exif.get_ifd(0x8769)
                    if exif_ifd:
                        fl = exif_ifd.get(0x920A)
                        if fl is not None:
                            focal_length = float(fl)
                # BurstMode / SequenceNumber from MakerNote varies by camera
                # Try ImageUniqueID (0xA420) as a fallback burst grouping key
                exif_ifd = exif.get_ifd(0x8769)
                if exif_ifd:
                    uid = exif_ifd.get(0xA420)  # ImageUniqueID
                    if uid:
                        burst_id = str(uid)
        except Exception:
            pass

        # Read GPS coordinates
        latitude, longitude = None, None
        try:
            with Image.open(str(image_path)) as img:
                gps_info = img.getexif().get_ifd(0x8825)
                if gps_info:
                    lat = gps_info.get(2)  # GPSLatitude
                    lat_ref = gps_info.get(1)  # N or S
                    lng = gps_info.get(4)  # GPSLongitude
                    lng_ref = gps_info.get(3)  # E or W
                    if lat and lng:
                        latitude = lat[0] + lat[1] / 60 + lat[2] / 3600
                        longitude = lng[0] + lng[1] / 60 + lng[2] / 3600
                        if lat_ref == "S":
                            latitude = -latitude
                        if lng_ref == "W":
                            longitude = -longitude
        except Exception:
            pass

        # Compute file hash for duplicate detection
        file_hash = None
        try:
            file_hash = compute_file_hash(str(image_path))
        except Exception:
            log.debug("Could not compute file hash for %s", image_path)

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

        # Store GPS, pHash, focal length, and burst ID if found
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
        if file_hash is not None:
            updates.append("file_hash=?")
            update_params.append(file_hash)
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

    # Pair raw+JPEG companions: raw is primary, JPEG becomes companion_path
    _pair_raw_jpeg_companions(db)

    db.update_folder_counts()
    log.info("Scan complete: %d photos indexed", total)
