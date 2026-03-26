"""Sync engine: reconcile database and XMP sidecars."""

import logging
import os
from collections import defaultdict
from xml.etree import ElementTree as ET

from compare import read_xmp_keywords
from xmp_writer import write_xmp_sidecar

log = logging.getLogger(__name__)

NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_DC = "http://purl.org/dc/elements/1.1/"
NS_LR = "http://ns.adobe.com/lightroom/1.0/"
NS_XMP = "http://ns.adobe.com/xap/1.0/"

# Register xmp namespace so writes preserve it
ET.register_namespace("xmp", NS_XMP)


def _get_xmp_path_for_photo(db, photo_id):
    """Determine the XMP sidecar path for a photo."""
    photo = db.get_photo(photo_id)
    if not photo:
        return None
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    folder_path = folders.get(photo["folder_id"], "")
    base = os.path.splitext(photo["filename"])[0]
    return os.path.join(folder_path, base + ".xmp")


def _write_rating_to_xmp(xmp_path, rating):
    """Write xmp:Rating attribute to an XMP sidecar."""
    from pathlib import Path

    path = Path(xmp_path)

    if path.exists():
        try:
            tree = ET.parse(path)
            root = tree.getroot()
        except ET.ParseError:
            return
    else:
        return  # Don't create XMP just for rating

    # Find rdf:Description and set xmp:Rating attribute
    desc = root.find(f".//{{{NS_RDF}}}Description")
    if desc is not None:
        desc.set(f"{{{NS_XMP}}}Rating", str(rating))
        ET.indent(tree, space="  ")
        tree.write(xmp_path, xml_declaration=True, encoding="unicode")


def _remove_keywords_from_xmp(xmp_path, keywords_to_remove):
    """Remove keywords from dc:subject and lr:hierarchicalSubject in an XMP file.

    Args:
        xmp_path: path to the .xmp sidecar
        keywords_to_remove: set of keyword strings to remove
    """
    from pathlib import Path

    path = Path(xmp_path)
    if not path.exists():
        return

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        log.warning("Corrupt XMP file, cannot remove keywords: %s", xmp_path)
        return

    root = tree.getroot()
    remove_lower = {kw.lower() for kw in keywords_to_remove}
    removed = []

    # Remove from dc:subject bag
    for bag in root.findall(f".//{{{NS_DC}}}subject/{{{NS_RDF}}}Bag"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            if li.text and li.text.lower() in remove_lower:
                removed.append(li.text)
                bag.remove(li)

    # Remove from lr:hierarchicalSubject bag (matches if any segment matches)
    for bag in root.findall(f".//{{{NS_LR}}}hierarchicalSubject/{{{NS_RDF}}}Bag"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            if li.text:
                # Hierarchical keywords use pipe-delimited paths like "Animals|Birds|Hawk"
                segments = {s.lower() for s in li.text.split("|")}
                if segments & remove_lower:
                    removed.append(li.text)
                    bag.remove(li)

    if removed:
        ET.indent(tree, space="  ")
        tree.write(xmp_path, xml_declaration=True, encoding="unicode")
        log.info("Removed keywords from %s: %s", xmp_path, removed)


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
                write_xmp_sidecar(
                    xmp_path, flat_keywords=keywords_to_add, hierarchical_keywords=set()
                )

            # Handle keyword removals: remove matching li elements from bags
            if keywords_to_remove:
                _remove_keywords_from_xmp(xmp_path, keywords_to_remove)

            # Write rating
            if new_rating is not None:
                _write_rating_to_xmp(xmp_path, new_rating)

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
        xmp_keywords = read_xmp_keywords(xmp_path)

        # Get current DB keywords
        db_keywords = db.get_photo_keywords(photo_id)
        db_kw_names = {k["name"] for k in db_keywords}

        # Reconcile DB keyword associations to match the current XMP file.
        for kw in xmp_keywords - db_kw_names:
            kid = db.add_keyword(kw)
            db.tag_photo(photo_id, kid)

        for kw in db_keywords:
            if kw["name"] not in xmp_keywords:
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
