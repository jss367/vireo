"""Import keywords from Lightroom .lrcat catalogs into the Vireo database."""

import logging
import os
from collections import defaultdict
from pathlib import Path

from catalog_reader import read_catalog
from xmp_writer import write_xmp_sidecar

log = logging.getLogger(__name__)


def preview_catalog(catalog_path, db):
    """Preview what a single catalog contains and how it maps to files on disk.

    Args:
        catalog_path: path to .lrcat file
        db: Database instance (for checking existing data)

    Returns:
        dict with total_files, matched_files, unmatched_files, keyword_count
    """
    data = read_catalog(catalog_path)

    total = len(data)
    matched = 0
    unmatched = 0
    all_keywords = set()

    for file_path, kw_data in data.items():
        if Path(file_path).exists():
            matched += 1
        else:
            unmatched += 1
        all_keywords.update(kw_data["flat_keywords"])

    return {
        "catalog": os.path.basename(catalog_path),
        "total_files": total,
        "matched_files": matched,
        "unmatched_files": unmatched,
        "keyword_count": len(all_keywords),
    }


def preview_import(catalog_paths, db):
    """Preview importing multiple catalogs, detecting conflicts.

    Args:
        catalog_paths: list of paths to .lrcat files
        db: Database instance

    Returns:
        dict with catalogs (list of previews), conflict_count, conflicts (list)
    """
    catalogs = []
    merged = {}  # file_path -> {keywords_by_catalog: {cat_name: set}}

    for cat_path in catalog_paths:
        try:
            preview = preview_catalog(cat_path, db)
            catalogs.append(preview)

            data = read_catalog(cat_path)
            cat_name = Path(cat_path).stem

            for file_path, kw_data in data.items():
                if file_path not in merged:
                    merged[file_path] = {"keywords_by_catalog": {}}
                merged[file_path]["keywords_by_catalog"][cat_name] = kw_data[
                    "flat_keywords"
                ]
        except Exception:
            log.exception("Failed to read catalog: %s", cat_path)

    # Detect conflicts: files in multiple catalogs with different keywords
    conflicts = []
    for file_path, info in merged.items():
        if len(info["keywords_by_catalog"]) > 1:
            conflicts.append(
                {
                    "file_path": file_path,
                    "keywords_by_catalog": {
                        cat: sorted(kws)
                        for cat, kws in info["keywords_by_catalog"].items()
                    },
                }
            )

    return {
        "catalogs": catalogs,
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
    }


def execute_import(
    catalog_paths, db, write_xmp=False, strategy="merge_all", progress_callback=None
):
    """Import keywords from catalogs into the Vireo database.

    Args:
        catalog_paths: list of paths to .lrcat files
        db: Database instance
        write_xmp: if True, also write XMP sidecars
        strategy: conflict resolution ('merge_all', 'prefer_first', 'prefer_last')
        progress_callback: optional callable(current, total)

    Returns:
        dict with imported, skipped, failed counts
    """
    # Build path -> DB photo lookup
    photos_by_path = {}
    all_photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    for p in all_photos:
        folder_path = folders.get(p["folder_id"], "")
        full_path = os.path.join(folder_path, p["filename"])
        photos_by_path[full_path] = p

    # Merge catalog data
    merged = {}  # file_path -> {flat_keywords, hierarchical_keywords}
    for idx, cat_path in enumerate(catalog_paths):
        try:
            data = read_catalog(cat_path)
        except Exception:
            log.exception("Failed to read catalog: %s", cat_path)
            continue

        for file_path, kw_data in data.items():
            if file_path not in merged:
                merged[file_path] = {
                    "flat_keywords": set(),
                    "hierarchical_keywords": set(),
                }

            if strategy == "merge_all":
                merged[file_path]["flat_keywords"].update(kw_data["flat_keywords"])
                merged[file_path]["hierarchical_keywords"].update(
                    kw_data["hierarchical_keywords"]
                )
            elif strategy == "prefer_first" and not merged[file_path]["flat_keywords"]:
                merged[file_path]["flat_keywords"] = kw_data["flat_keywords"]
                merged[file_path]["hierarchical_keywords"] = kw_data[
                    "hierarchical_keywords"
                ]
            elif strategy == "prefer_last":
                merged[file_path]["flat_keywords"] = kw_data["flat_keywords"]
                merged[file_path]["hierarchical_keywords"] = kw_data[
                    "hierarchical_keywords"
                ]

    imported = 0
    skipped = 0
    failed = 0
    total = len(merged)

    for i, (file_path, kw_data) in enumerate(merged.items()):
        # Find matching photo in DB
        photo = photos_by_path.get(file_path)
        if not photo:
            skipped += 1
            if progress_callback:
                progress_callback(i + 1, total)
            continue

        try:
            # Import keywords into DB
            for kw_name in kw_data["flat_keywords"]:
                kid = db.add_keyword(kw_name)
                db.tag_photo(photo["id"], kid)

            # Import hierarchical keywords
            for hier in kw_data["hierarchical_keywords"]:
                parts = hier.split("|")
                parent_id = None
                for part in parts:
                    kid = db.add_keyword(part, parent_id=parent_id)
                    parent_id = kid
                db.tag_photo(photo["id"], parent_id)

            # Write XMP if requested
            if write_xmp and Path(file_path).exists():
                xmp_path = str(Path(file_path).with_suffix(".xmp"))
                write_xmp_sidecar(
                    xmp_path,
                    flat_keywords=kw_data["flat_keywords"],
                    hierarchical_keywords=kw_data["hierarchical_keywords"],
                )

            imported += 1
        except Exception:
            failed += 1
            log.warning("Failed to import keywords for %s", file_path, exc_info=True)

        if progress_callback:
            progress_callback(i + 1, total)

    log.info(
        "Import complete: %d imported, %d skipped, %d failed", imported, skipped, failed
    )
    return {
        "imported": imported,
        "skipped": skipped,
        "failed": failed,
    }
