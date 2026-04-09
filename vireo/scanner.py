"""Scan folders, discover photos, read metadata, populate database."""

import hashlib
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import imagehash
from image_loader import RAW_EXTENSIONS, SUPPORTED_EXTENSIONS, extract_working_copy
from metadata import extract_metadata
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
    Returns ISO format string or None.
    """
    dto = exif_group.get("DateTimeOriginal") or exif_group.get("CreateDate")
    if not dto:
        return None
    try:
        dt = datetime.strptime(str(dto), "%Y:%m:%d %H:%M:%S")
        return dt.isoformat()
    except (ValueError, TypeError):
        log.debug("Unparseable EXIF timestamp dropped: %r", dto)
        return None


def _pair_raw_jpeg_companions(db):
    """Find raw+JPEG pairs in the same folder and merge them.

    When both IMG_001.cr3 and IMG_001.jpg exist in the same folder,
    keep the raw as the primary photo and set companion_path to the JPEG filename.
    Delete the duplicate JPEG-only photo record.
    """
    raw_exts = {".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"}
    jpeg_exts = {".jpg", ".jpeg"}

    rows = db.conn.execute(
        "SELECT id, folder_id, filename, extension FROM photos"
        " WHERE companion_path IS NULL"
        " OR (companion_path IS NOT NULL AND extension IN"
        " ('.nef','.cr2','.cr3','.arw','.raf','.dng','.rw2','.orf'))"
        " ORDER BY folder_id, filename"
    ).fetchall()

    # Group by folder_id + base name (without extension)
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

        # Transfer metadata from companion to primary if primary lacks it
        transfer_cols = "timestamp, rating, flag, latitude, longitude, exif_data, focal_length, width, height"
        primary_full = db.conn.execute(
            f"SELECT {transfer_cols} FROM photos WHERE id = ?",
            (primary["id"],),
        ).fetchone()
        companion_full = db.conn.execute(
            f"SELECT {transfer_cols} FROM photos WHERE id = ?",
            (companion["id"],),
        ).fetchone()

        updates = []
        params = []
        if not primary_full["timestamp"] and companion_full["timestamp"]:
            updates.append("timestamp = ?")
            params.append(companion_full["timestamp"])
        if primary_full["rating"] == 0 and companion_full["rating"] != 0:
            updates.append("rating = ?")
            params.append(companion_full["rating"])
        if primary_full["flag"] == "none" and companion_full["flag"] != "none":
            updates.append("flag = ?")
            params.append(companion_full["flag"])
        if primary_full["latitude"] is None and companion_full["latitude"] is not None:
            updates.extend(["latitude = ?", "longitude = ?"])
            params.extend([companion_full["latitude"], companion_full["longitude"]])
        if not primary_full["exif_data"] and companion_full["exif_data"]:
            updates.append("exif_data = ?")
            params.append(companion_full["exif_data"])
        if primary_full["focal_length"] is None and companion_full["focal_length"] is not None:
            updates.append("focal_length = ?")
            params.append(companion_full["focal_length"])
        if not primary_full["width"] and companion_full["width"]:
            updates.extend(["width = ?", "height = ?"])
            params.extend([companion_full["width"], companion_full["height"]])
        if updates:
            params.append(primary["id"])
            db.conn.execute(
                f"UPDATE photos SET {', '.join(updates)} WHERE id = ?", params
            )

        # Transfer keywords from companion to primary
        companion_keywords = db.conn.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?",
            (companion["id"],),
        ).fetchall()
        for kw in companion_keywords:
            db.conn.execute(
                "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
                (primary["id"], kw["keyword_id"]),
            )

        db.conn.execute(
            "UPDATE photos SET companion_path = ? WHERE id = ?",
            (companion["filename"], primary["id"]),
        )

        # Transfer detections (and their cascaded predictions) from companion to primary.
        # Detections are linked to photos; predictions cascade through detection_id.
        db.conn.execute(
            "UPDATE detections SET photo_id = ? WHERE photo_id = ?",
            (primary["id"], companion["id"]),
        )

        # Transfer pending_changes from companion to primary
        db.conn.execute(
            "UPDATE pending_changes SET photo_id = ? WHERE photo_id = ?",
            (primary["id"], companion["id"]),
        )
        # Transfer iNaturalist submissions: deduplicate on (photo_id, observation_id)
        # before reassigning to avoid UNIQUE constraint violation.
        db.conn.execute(
            """DELETE FROM inat_submissions
               WHERE photo_id = ? AND observation_id IN (
                   SELECT observation_id FROM inat_submissions WHERE photo_id = ?
               )""",
            (companion["id"], primary["id"]),
        )
        db.conn.execute(
            "UPDATE inat_submissions SET photo_id = ? WHERE photo_id = ?",
            (primary["id"], companion["id"]),
        )
        # Remove keyword associations then the duplicate JPEG record
        db.conn.execute("DELETE FROM photo_keywords WHERE photo_id = ?", (companion["id"],))
        db.conn.execute("DELETE FROM photos WHERE id = ?", (companion["id"],))

    db.conn.commit()


def _extract_working_copies(db, vireo_dir, progress_callback=None, status_callback=None):
    """Extract working copies for all RAW photos missing one.

    For each RAW photo without a working_copy_path, extract a JPEG working
    copy into ``<vireo_dir>/working/<photo_id>.jpg``.  When the photo has a
    companion JPEG (RAW+JPEG pair), the companion is used as the extraction
    source because the in-camera JPEG is higher quality than extracting from
    the RAW.
    """
    import config as cfg

    user_cfg = cfg.load()
    wc_max_size = user_cfg.get("working_copy_max_size", 4096)
    wc_quality = user_cfg.get("working_copy_quality", 92)

    rows = db.conn.execute(
        "SELECT p.id, p.filename, p.companion_path, p.working_copy_path, "
        "f.path AS folder_path "
        "FROM photos p JOIN folders f ON f.id = p.folder_id "
        "WHERE p.extension IN ({}) AND p.working_copy_path IS NULL".format(
            ",".join("?" for _ in RAW_EXTENSIONS)
        ),
        list(RAW_EXTENSIONS),
    ).fetchall()

    if not rows:
        return

    if status_callback:
        status_callback(f"Extracting {len(rows)} working copies...")

    for _i, row in enumerate(rows):
        wc_rel = f"working/{row['id']}.jpg"
        wc_abs = os.path.join(vireo_dir, wc_rel)

        # Prefer companion JPEG if available
        source = os.path.join(row["folder_path"], row["filename"])
        if row["companion_path"]:
            companion = os.path.join(row["folder_path"], row["companion_path"])
            if os.path.isfile(companion):
                source = companion

        if extract_working_copy(source, wc_abs, max_size=wc_max_size, quality=wc_quality):
            db.conn.execute(
                "UPDATE photos SET working_copy_path=? WHERE id=?",
                (wc_rel, row["id"]),
            )

    db.conn.commit()


def scan(root, db, progress_callback=None, incremental=False, extract_full_metadata=True, photo_callback=None, skip_paths=None, status_callback=None, recursive=True, restrict_dirs=None, vireo_dir=None):
    """Walk a folder tree, discover photos, read metadata, populate database.

    Args:
        root: path to the root folder to scan
        db: Database instance
        progress_callback: optional callable(current, total) for progress reporting
        incremental: if True, skip files unchanged since last scan
        extract_full_metadata: if True, store full ExifTool JSON in exif_data column
        photo_callback: optional callable(photo_id, path_str) called after each photo is committed
        skip_paths: optional set of absolute path strings to exclude from scanning
        status_callback: optional callable(message) for phase status updates
        recursive: if True (default), scan subfolders; if False, only scan root directory
        restrict_dirs: optional list of directory paths to scan instead of the
            full tree. When provided, only files in these directories are
            discovered (non-recursively), but ``root`` is still used as the
            folder hierarchy root so parent links are preserved correctly.
        vireo_dir: optional path to the vireo data directory (e.g. ``~/.vireo``).
            When provided, working copies are extracted for RAW photos after
            companion pairing.
    """
    root_path = Path(root)
    if not root_path.is_dir():
        log.warning("Root path does not exist or is not a directory: %s", root)
        return

    # Discover all image files (incremental enumeration for progress reporting)
    log.info("Discovering files in %s ...", root)
    if status_callback:
        status_callback("Discovering files...")
    image_files = []
    if restrict_dirs is not None:
        # Only enumerate files in the specified directories (non-recursive).
        # root is still used as the folder hierarchy root for _ensure_folder.
        for d in restrict_dirs:
            dp = Path(d)
            if dp.is_dir():
                for f in dp.iterdir():
                    if (f.is_file()
                            and f.suffix.lower() in SUPPORTED_EXTENSIONS
                            and not f.name.startswith(".")
                            and (skip_paths is None or str(f) not in skip_paths)):
                        image_files.append(f)
    else:
        candidates = root_path.rglob("*") if recursive else root_path.iterdir()
        for checked, f in enumerate(candidates, 1):
            if checked % 500 == 0 and status_callback:
                status_callback(f"Discovering files... ({len(image_files)} found)")
            if (f.is_file()
                    and f.suffix.lower() in SUPPORTED_EXTENSIONS
                    and not f.name.startswith(".")
                    and (skip_paths is None or str(f) not in skip_paths)):
                image_files.append(f)
    image_files.sort()

    total = len(image_files)
    log.info("Found %d images in %s", total, root)
    if progress_callback:
        progress_callback(0, total)

    # Build existing photo lookup for incremental mode
    existing_photos = {}
    exif_extracted = set()  # photo IDs where ExifTool has already run
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
        # Track which photos have had ExifTool metadata extracted (exif_data
        # is non-NULL). Photos with NULL exif_data need re-extraction.
        for row in db.conn.execute("SELECT id FROM photos WHERE exif_data IS NOT NULL"):
            exif_extracted.add(row["id"])

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

    # First pass: determine which files need full processing (for incremental mode).
    # Handle XMP-only changes inline; collect files needing metadata extraction.
    files_to_process = []
    processed_count = 0
    for image_path in image_files:
        stat = image_path.stat()
        file_mtime = stat.st_mtime
        xmp_path = image_path.with_suffix(".xmp")
        xmp_mtime = xmp_path.stat().st_mtime if xmp_path.exists() else None

        if incremental:
            full_path_str = str(image_path)
            existing = existing_by_path.get(full_path_str)
            if existing:
                file_unchanged = existing["file_mtime"] == file_mtime
                xmp_unchanged = existing["xmp_mtime"] == xmp_mtime
                # Re-process if ExifTool never ran for this photo (both
                # timestamp and exif_data are NULL). Photos with genuinely
                # missing timestamps (screenshots, exports) will have
                # exif_data set after one extraction attempt.
                metadata_missing = (
                    existing["timestamp"] is None
                    and existing["id"] not in exif_extracted
                )

                if file_unchanged and xmp_unchanged and not metadata_missing:
                    processed_count += 1
                    if photo_callback:
                        photo_callback(existing["id"], full_path_str)
                    if progress_callback:
                        progress_callback(processed_count, total)
                    continue

                # XMP changed: re-import keywords
                if not xmp_unchanged and xmp_mtime is not None:
                    _import_keywords_for_photo(db, existing["id"], str(xmp_path))
                    db.conn.execute(
                        "UPDATE photos SET xmp_mtime = ? WHERE id = ?",
                        (xmp_mtime, existing["id"]),
                    )
                    db.conn.commit()

                if file_unchanged and not metadata_missing:
                    processed_count += 1
                    if photo_callback:
                        photo_callback(existing["id"], full_path_str)
                    if progress_callback:
                        progress_callback(processed_count, total)
                    continue

        files_to_process.append(image_path)

    # Batch extract metadata via ExifTool only for files that need processing
    paths_to_extract = [str(ip) for ip in files_to_process]
    if paths_to_extract and status_callback:
        status_callback(f"Extracting metadata ({len(paths_to_extract)} files)...")
    metadata_map = extract_metadata(paths_to_extract) if paths_to_extract else {}

    for image_path in files_to_process:
        folder_id = _ensure_folder(image_path.parent)

        # File stats
        stat = image_path.stat()
        file_size = stat.st_size
        file_mtime = stat.st_mtime

        # XMP sidecar
        xmp_path = image_path.with_suffix(".xmp")
        xmp_mtime = xmp_path.stat().st_mtime if xmp_path.exists() else None

        # Get pre-extracted metadata for this file
        file_meta = metadata_map.get(str(image_path), {})
        file_group = file_meta.get("File", {})
        exif_group = file_meta.get("EXIF", {})
        composite = file_meta.get("Composite", {})

        # Dimensions from ExifTool (works for all file types including RAW)
        width, height = _extract_dimensions(exif_group, file_group, extension=image_path.suffix.lower())

        # Fallback if ExifTool didn't provide dimensions
        if width is None or height is None:
            ext = image_path.suffix.lower()
            if ext in RAW_EXTENSIONS:
                try:
                    import rawpy

                    with rawpy.imread(str(image_path)) as raw:
                        width = raw.sizes.width
                        height = raw.sizes.height
                except Exception:
                    log.debug("Could not read RAW dimensions from %s", image_path)
            else:
                try:
                    with Image.open(str(image_path)) as img:
                        width, height = img.size
                except Exception:
                    log.debug("Could not read dimensions from %s", image_path)

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
        latitude = composite.get("GPSLatitude")
        if latitude is None:
            latitude = exif_group.get("GPSLatitude")
        longitude = composite.get("GPSLongitude")
        if longitude is None:
            longitude = exif_group.get("GPSLongitude")

        # Compute perceptual hash (computed, not from EXIF)
        phash = None
        try:
            with Image.open(str(image_path)) as img:
                phash = str(imagehash.phash(img))
        except Exception:
            log.debug("Could not compute pHash for %s", image_path)

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

        # Update metadata columns (also fixes existing photos that were
        # inserted before ExifTool metadata was available)
        updates = []
        update_params = []
        if timestamp is not None:
            updates.append("timestamp=?")
            update_params.append(timestamp)
        if width is not None:
            updates.append("width=?")
            update_params.append(width)
        if height is not None:
            updates.append("height=?")
            update_params.append(height)
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
        if file_meta and extract_full_metadata:
            updates.append("exif_data=?")
            update_params.append(json.dumps(file_meta))
        elif file_meta:
            # Store minimal marker so we know ExifTool ran (even when
            # extract_full_metadata is off) — prevents perpetual retry
            updates.append("exif_data=COALESCE(exif_data, ?)")
            update_params.append("{}")
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

        if photo_callback:
            photo_callback(photo_id, str(image_path))

        processed_count += 1
        if progress_callback:
            progress_callback(processed_count, total)

    # Pair raw+JPEG companions: raw is primary, JPEG becomes companion_path.
    # Wrap post-processing so folder counts are always updated, even on failure.
    # On exception, roll back any uncommitted partial writes before updating
    # counts — otherwise update_folder_counts()'s commit would persist
    # half-applied pairing or working-copy records.
    try:
        _pair_raw_jpeg_companions(db)

        # Extract working copies for RAW photos (after pairing so companion is known)
        if vireo_dir:
            _extract_working_copies(db, vireo_dir, progress_callback, status_callback)
    except Exception:
        db.conn.rollback()
        raise
    finally:
        db.update_folder_counts()
    log.info("Scan complete: %d photos indexed", total)
