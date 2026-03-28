"""Sync engine: reconcile database and XMP sidecars."""

import logging
import os
from collections import defaultdict

from xmp import read_keywords, write_sidecar, remove_keywords, write_rating

log = logging.getLogger(__name__)


def _get_xmp_path_for_photo(db, photo_id):
    """Determine the XMP sidecar path for a photo."""
    photo = db.get_photo(photo_id)
    if not photo:
        return None
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    folder_path = folders.get(photo["folder_id"], "")
    base = os.path.splitext(photo["filename"])[0]
    return os.path.join(folder_path, base + ".xmp")


def sync_to_xmp(db, progress_callback=None):
    """Write pending changes to XMP sidecars.

    Args:
        db: Database instance
        progress_callback: optional callable(current, total)

    Returns:
        dict with synced, failed, failures counts
    """
    changes = db.get_pending_changes()
    if not changes:
        return {"synced": 0, "failed": 0, "failures": []}

    # Group changes by photo_id
    by_photo = defaultdict(list)
    for c in changes:
        by_photo[c["photo_id"]].append(c)

    synced = 0
    failed = 0
    failures = []
    synced_ids = []

    total = len(by_photo)
    for i, (photo_id, photo_changes) in enumerate(by_photo.items()):
        xmp_path = _get_xmp_path_for_photo(db, photo_id)
        if not xmp_path:
            failed += 1
            failures.append({"photo_id": photo_id, "error": "photo not found in DB"})
            continue

        # Check if the folder exists (NAS might be offline)
        folder = os.path.dirname(xmp_path)
        if not os.path.isdir(folder):
            failed += 1
            failures.append(
                {"photo_id": photo_id, "error": f"folder not accessible: {folder}"}
            )
            continue

        try:
            # Collect keyword adds/removes and rating/flag changes
            keywords_to_add = set()
            keywords_to_remove = set()
            new_rating = None
            supported_ids = []
            unsupported_changes = []

            for c in photo_changes:
                if c["change_type"] == "keyword_add":
                    keywords_to_add.add(c["value"])
                    supported_ids.append(c["id"])
                elif c["change_type"] == "keyword_remove":
                    keywords_to_remove.add(c["value"])
                    supported_ids.append(c["id"])
                elif c["change_type"] == "rating":
                    new_rating = int(c["value"])
                    supported_ids.append(c["id"])
                elif c["change_type"] == "flag":
                    unsupported_changes.append(c)

            # Write keywords
            if keywords_to_add:
                write_sidecar(
                    xmp_path, flat_keywords=keywords_to_add, hierarchical_keywords=set()
                )

            # Handle keyword removals: remove matching li elements from bags
            if keywords_to_remove:
                remove_keywords(xmp_path, keywords_to_remove)

            # Write rating
            if new_rating is not None:
                write_rating(xmp_path, new_rating)

            if supported_ids:
                synced += 1
                synced_ids.extend(supported_ids)

            for c in unsupported_changes:
                failed += 1
                failures.append(
                    {
                        "photo_id": photo_id,
                        "change_id": c["id"],
                        "error": f"unsupported change type: {c['change_type']}",
                    }
                )

        except Exception as e:
            failed += 1
            failures.append({"photo_id": photo_id, "error": str(e)})
            log.warning("Failed to sync photo %d: %s", photo_id, e)

        if progress_callback:
            progress_callback(i + 1, total)

    # Clear successfully synced changes
    if synced_ids:
        db.clear_pending(synced_ids)

    log.info("Sync complete: %d synced, %d failed", synced, failed)
    return {"synced": synced, "failed": failed, "failures": failures}


def sync_from_xmp(db, photo_ids):
    """Re-read XMP sidecars and update database keywords.

    Args:
        db: Database instance
        photo_ids: list of photo ids to re-sync
    """
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}

    for photo_id in photo_ids:
        photo = db.get_photo(photo_id)
        if not photo:
            continue

        folder_path = folders.get(photo["folder_id"], "")
        base = os.path.splitext(photo["filename"])[0]
        xmp_path = os.path.join(folder_path, base + ".xmp")

        if not os.path.exists(xmp_path):
            continue

        # Read current XMP keywords
        xmp_keywords = read_keywords(xmp_path)
        xmp_keywords_lower = {kw.lower(): kw for kw in xmp_keywords}

        # Get current DB keywords
        db_keywords = db.get_photo_keywords(photo_id)
        db_keywords_lower = {k["name"].lower(): k for k in db_keywords}

        # Reconcile DB keyword associations to match the current XMP file.
        for kw_lower, kw_name in xmp_keywords_lower.items():
            if kw_lower in db_keywords_lower:
                continue
            kid = db.add_keyword(kw_name)
            db.tag_photo(photo_id, kid)

        for kw in db_keywords:
            if kw["name"].lower() not in xmp_keywords_lower:
                db.untag_photo(photo_id, kw["id"])

        # Update xmp_mtime
        xmp_mtime = os.path.getmtime(xmp_path)
        db.conn.execute(
            "UPDATE photos SET xmp_mtime = ? WHERE id = ?", (xmp_mtime, photo_id)
        )
        db.conn.commit()

        log.info(
            "Synced XMP -> DB for photo %d: %d keywords", photo_id, len(xmp_keywords)
        )
