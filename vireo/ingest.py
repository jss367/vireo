"""Smart ingest: copy and organize photos from external source to destination."""

import contextlib
import logging
import shutil
from datetime import datetime
from pathlib import Path

from grouping import read_exif_timestamp
from image_loader import IMAGE_EXTENSIONS, RAW_EXTENSIONS, SUPPORTED_EXTENSIONS
from scanner import compute_file_hash

log = logging.getLogger(__name__)


def build_destination_path(exif_timestamp, template="%Y/%Y-%m-%d"):
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


def preview_destination(sources, destination, folder_template="%Y/%Y-%m-%d",
                        file_types="both", recursive=True):
    """Dry-run preview of destination folder structure.

    Scans source files, reads EXIF timestamps, and groups them by the
    folder template without copying anything.

    Returns:
        dict with folders list, total_photos, total_folders,
        new_folders, existing_folders
    """
    all_files = []
    for src in sources:
        all_files.extend(discover_source_files(src, file_types, recursive=recursive))

    folder_counts = {}
    for source_file in all_files:
        exif_dt = None
        with contextlib.suppress(OSError, ValueError):
            exif_dt = read_exif_timestamp(str(source_file))
        if exif_dt is None:
            with contextlib.suppress(OSError, ValueError, OverflowError):
                exif_dt = datetime.fromtimestamp(source_file.stat().st_mtime)

        rel_folder = build_destination_path(exif_dt, folder_template)
        if not rel_folder:
            rel_folder = "."
        folder_counts[rel_folder] = folder_counts.get(rel_folder, 0) + 1

    dest_path = Path(destination)
    folders = []
    for path in sorted(folder_counts):
        check_path = dest_path if path == "." else dest_path / path
        folders.append({
            "path": path,
            "count": folder_counts[path],
            "exists": check_path.is_dir(),
        })

    new_count = sum(1 for f in folders if not f["exists"])
    existing_count = sum(1 for f in folders if f["exists"])

    return {
        "folders": folders,
        "total_photos": len(all_files),
        "total_folders": len(folders),
        "new_folders": new_count,
        "existing_folders": existing_count,
    }


def discover_source_files(source_dir, file_types="both", recursive=True):
    """Discover image files in source directory.

    Args:
        source_dir: path to source directory (e.g., SD card mount)
        file_types: "raw", "jpeg", "both", or a list of extensions
            (e.g. [".jpg", ".nef"])
        recursive: if True (default), scan subfolders; if False, only scan root

    Returns:
        Sorted list of Path objects for matching files
    """
    source_path = Path(source_dir)
    if not source_path.is_dir():
        return []

    if isinstance(file_types, list):
        allowed = {ext.lower() for ext in file_types}
    elif file_types == "raw":
        allowed = RAW_EXTENSIONS
    elif file_types == "jpeg":
        allowed = IMAGE_EXTENSIONS
    else:
        allowed = SUPPORTED_EXTENSIONS

    candidates = source_path.rglob("*") if recursive else source_path.iterdir()
    return sorted(
        f
        for f in candidates
        if f.is_file()
        and f.suffix.lower() in allowed
        and not f.name.startswith(".")
    )


def ingest(
    source_dir,
    destination_dir,
    db,
    file_types="both",
    folder_template="%Y/%Y-%m-%d",
    skip_duplicates=True,
    progress_callback=None,
    extra_known_hashes=None,
    skip_paths=None,
    recursive=True,
):
    """Copy and organize photos from source to destination.

    Args:
        source_dir: path to source (e.g., /Volumes/SD_CARD)
        destination_dir: path to destination (e.g., /Volumes/NAS/Photos)
        db: Database instance (used for duplicate hash lookup)
        file_types: "raw", "jpeg", or "both"
        folder_template: strftime format for destination subfolder
        skip_duplicates: if True, skip files whose hash matches existing file
        progress_callback: optional callable(current, total, filename)
        extra_known_hashes: optional set of hashes to treat as known in
            addition to those already in the DB.  Pass a shared mutable set
            when calling ingest() in a loop so that files copied by earlier
            iterations are treated as duplicates by later ones even though
            they have not been scanned into the DB yet.

    Returns:
        dict with counts: copied, skipped_duplicate, failed, total
    """
    files = discover_source_files(source_dir, file_types, recursive=recursive)
    if skip_paths:
        files = [f for f in files if str(f) not in skip_paths]
    total = len(files)

    # Load known hashes from database for duplicate detection and merge with
    # any hashes accumulated by previous ingest() calls in the same session.
    known_hashes = set()
    if skip_duplicates:
        rows = db.conn.execute(
            "SELECT file_hash FROM photos WHERE file_hash IS NOT NULL"
        ).fetchall()
        known_hashes = {r["file_hash"] for r in rows}
        if extra_known_hashes:
            known_hashes |= extra_known_hashes

    copied = 0
    skipped_duplicate = 0
    failed = 0
    copied_paths = []

    for i, source_file in enumerate(files):
        try:
            # Compute hash for duplicate detection
            file_hash = compute_file_hash(str(source_file))

            if skip_duplicates and file_hash in known_hashes:
                skipped_duplicate += 1
                if progress_callback:
                    progress_callback(i + 1, total, source_file.name)
                continue

            # Determine destination folder from EXIF date
            exif_dt = None
            try:
                exif_dt = read_exif_timestamp(str(source_file))
            except (OSError, ValueError):
                log.debug("Could not read EXIF timestamp from %s", source_file)
            if exif_dt is None:
                # Fall back to file modification time
                with contextlib.suppress(OSError, ValueError, OverflowError):
                    exif_dt = datetime.fromtimestamp(source_file.stat().st_mtime)

            rel_folder = build_destination_path(exif_dt, folder_template)
            dest_folder = Path(destination_dir) / rel_folder
            dest_folder.mkdir(parents=True, exist_ok=True)

            dest_file = dest_folder / source_file.name

            # Handle filename collision (different file, same name)
            if dest_file.exists():
                dest_hash = compute_file_hash(str(dest_file))
                if file_hash == dest_hash:
                    # Exact same file already there
                    skipped_duplicate += 1
                    known_hashes.add(file_hash)
                    if progress_callback:
                        progress_callback(i + 1, total, source_file.name)
                    continue
                # Different file, same name — add numeric suffix
                stem = dest_file.stem
                suffix = dest_file.suffix
                counter = 1
                while dest_file.exists():
                    dest_file = dest_folder / f"{stem}_{counter}{suffix}"
                    counter += 1

            shutil.copy2(str(source_file), str(dest_file))
            known_hashes.add(file_hash)
            copied_paths.append(str(dest_file))
            copied += 1

        except Exception as e:
            log.warning("Failed to ingest %s: %s", source_file, e)
            failed += 1

        if progress_callback:
            progress_callback(i + 1, total, source_file.name)

    return {
        "copied": copied,
        "skipped_duplicate": skipped_duplicate,
        "failed": failed,
        "total": total,
        "copied_paths": copied_paths,
    }
