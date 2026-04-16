"""Audit system: detect drift, orphans, and untracked files."""

import logging
import os
from pathlib import Path

from image_loader import SUPPORTED_EXTENSIONS
from xmp import read_keywords

log = logging.getLogger(__name__)


def check_drift(db):
    """Find photos where DB and XMP sidecar disagree.

    Checks both directions:
    - XMP modified externally (keywords in XMP not in DB)
    - DB modified by Vireo (keywords in DB not in XMP, pending sync)

    Returns:
        list of {photo_id, filename, folder_path, field, db_value, xmp_value,
                 added_in_xmp, removed_in_xmp, direction}
    """
    photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    drifts = []

    for photo in photos:
        folder_path = folders.get(photo["folder_id"], "")
        base = os.path.splitext(photo["filename"])[0]
        xmp_path = os.path.join(folder_path, base + ".xmp")

        db_keywords = {k["name"] for k in db.get_photo_keywords(photo["id"])}

        if not os.path.exists(xmp_path):
            # No XMP file — if DB has keywords, that's a pending sync
            if db_keywords:
                drifts.append(
                    {
                        "photo_id": photo["id"],
                        "filename": photo["filename"],
                        "folder_path": folder_path,
                        "field": "keywords",
                        "db_value": sorted(db_keywords),
                        "xmp_value": [],
                        "added_in_xmp": [],
                        "removed_in_xmp": sorted(db_keywords),
                        "direction": "db_ahead",
                    }
                )
            continue

        xmp_keywords = read_keywords(xmp_path)

        if xmp_keywords != db_keywords:
            added_in_xmp = xmp_keywords - db_keywords
            removed_in_xmp = db_keywords - xmp_keywords

            # Determine direction
            if added_in_xmp and not removed_in_xmp:
                direction = "xmp_ahead"
            elif removed_in_xmp and not added_in_xmp:
                direction = "db_ahead"
            else:
                direction = "both"

            drifts.append(
                {
                    "photo_id": photo["id"],
                    "filename": photo["filename"],
                    "folder_path": folder_path,
                    "field": "keywords",
                    "db_value": sorted(db_keywords),
                    "xmp_value": sorted(xmp_keywords),
                    "added_in_xmp": sorted(added_in_xmp),
                    "removed_in_xmp": sorted(removed_in_xmp),
                    "direction": direction,
                }
            )

    log.info("Drift check: %d discrepancies found", len(drifts))
    return drifts


def check_orphans(db):
    """Find DB entries where the file no longer exists on disk.

    Returns:
        list of {photo_id, filename, folder_path}
    """
    photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    orphans = []

    for photo in photos:
        folder_path = folders.get(photo["folder_id"], "")
        file_path = os.path.join(folder_path, photo["filename"])

        if not os.path.exists(file_path):
            orphans.append(
                {
                    "photo_id": photo["id"],
                    "filename": photo["filename"],
                    "folder_path": folder_path,
                }
            )

    log.info("Orphan check: %d orphaned entries found", len(orphans))
    return orphans


def check_untracked(db, root_paths):
    """Find files on disk not in the database.

    Args:
        db: Database instance
        root_paths: list of root directory paths to scan

    Returns:
        list of {path, folder}
    """
    # Build set of known file paths
    photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    known_paths = set()
    for photo in photos:
        folder_path = folders.get(photo["folder_id"], "")
        known_paths.add(os.path.join(folder_path, photo["filename"]))

    untracked = []
    for root in root_paths:
        root_path = Path(root)
        if not root_path.is_dir():
            continue
        for f in root_path.rglob("*"):
            if (
                f.is_file()
                and f.suffix.lower() in SUPPORTED_EXTENSIONS
                and not f.name.startswith(".")
                and str(f) not in known_paths
            ):
                untracked.append(
                    {
                        "path": str(f),
                        "folder": str(f.parent),
                    }
                )

    log.info("Untracked check: %d untracked files found", len(untracked))
    return untracked


def resolve_drift(db, photo_id, direction):
    """Resolve a drift for a single photo.

    Args:
        db: Database instance
        photo_id: photo to resolve
        direction: 'use_db' queues XMP write, 'use_xmp' updates DB from XMP
    """
    if direction == "use_db":
        # Queue all current DB keywords as pending writes
        keywords = db.get_photo_keywords(photo_id)
        for kw in keywords:
            db.queue_change(photo_id, "keyword_add", kw["name"])
    elif direction == "use_xmp":
        from sync import sync_from_xmp

        sync_from_xmp(db, [photo_id])


def remove_orphans(db, photo_ids):
    """Delete DB entries for orphaned photos.

    Args:
        db: Database instance
        photo_ids: list of photo ids to remove
    """
    for pid in photo_ids:
        db.conn.execute("DELETE FROM photo_keywords WHERE photo_id = ?", (pid,))
        db.conn.execute("DELETE FROM pending_changes WHERE photo_id = ?", (pid,))
        db.conn.execute("DELETE FROM photos WHERE id = ?", (pid,))
    db.conn.commit()
    db.update_folder_counts()
    log.info("Removed %d orphan entries", len(photo_ids))


def import_untracked(db, paths):
    """Import untracked files into the database by scanning them.

    Args:
        db: Database instance
        paths: list of file paths to import
    """
    from new_images import invalidate_new_images_after_scan
    from scanner import scan

    # Group by parent directory
    dirs = set(os.path.dirname(p) for p in paths)
    for d in dirs:
        try:
            scan(d, db, incremental=True)
        finally:
            # scanner.scan commits photo rows incrementally, so even a
            # mid-scan failure can leave DB state that invalidates cached
            # new-image counts. Mirrors the try/finally in pipeline_job
            # and the api_job_scan / api_job_import_full handlers.
            try:
                invalidate_new_images_after_scan(db, d)
            except Exception:
                log.exception(
                    "Failed to invalidate new-images cache for %s", d
                )
