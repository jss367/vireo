"""Import keywords from Lightroom .lrcat catalogs into the Vireo database."""

import logging
import os
import sqlite3
from pathlib import Path

from catalog import read_catalog
from keyword_identity import validate_import_locations
from keyword_normalization import keyword_match_key, normalize_keyword_display
from xmp import write_sidecar

log = logging.getLogger(__name__)


def _path_lookup_key(path):
    """Key a file path so the catalog and scanned spellings of it collide.

    ``os.path.normpath`` already reconciles the separator difference between
    Lightroom's forward-slashed paths and the backslashed ones ``os.walk``
    produces. Case is the other half of that on Windows: a catalog holding
    ``D:/Pictures`` and a scan rooted at ``d:\\Pictures`` name the same file.
    ``os.path.normcase`` folds both, and is the identity on POSIX, where case
    is significant and a backslash is a legal filename character.
    """
    return os.path.normcase(os.path.normpath(path))


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
    merged = {}  # lookup key -> {file_path, keywords_by_catalog: {cat_name: set}}

    for cat_path in catalog_paths:
        try:
            preview = preview_catalog(cat_path, db)
            catalogs.append(preview)

            data = read_catalog(cat_path)
            cat_name = Path(cat_path).stem

            for file_path, kw_data in data.items():
                key = _path_lookup_key(file_path)
                if key not in merged:
                    merged[key] = {"file_path": file_path, "keywords_by_catalog": {}}
                merged[key]["keywords_by_catalog"][cat_name] = kw_data[
                    "flat_keywords"
                ]
        except Exception:
            log.exception("Failed to read catalog: %s", cat_path)

    # Detect conflicts: files in multiple catalogs with different keywords
    conflicts = []
    for info in merged.values():
        if len(info["keywords_by_catalog"]) > 1:
            conflicts.append(
                {
                    "file_path": info["file_path"],
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
    catalog_paths, db, write_xmp=False, strategy="merge_all", progress_callback=None,
    pause_callback=None,
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
    # Build path -> DB photo lookup. Both sides go through _path_lookup_key
    # so a catalog path and a scanned path that name the same file agree.
    photos_by_path = {}
    all_photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    for p in all_photos:
        if pause_callback:
            pause_callback()
        folder_path = folders.get(p["folder_id"], "")
        full_path = os.path.join(folder_path, p["filename"])
        photos_by_path[_path_lookup_key(full_path)] = p

    # Merge catalog data
    merged = {}  # lookup key -> {path, flat_keywords, hierarchical_keywords}
    for idx, cat_path in enumerate(catalog_paths):
        if pause_callback:
            pause_callback()
        try:
            data = read_catalog(cat_path, pause_callback=pause_callback)
        except (OSError, sqlite3.Error):
            log.exception("Failed to read catalog: %s", cat_path)
            continue

        for raw_file_path, kw_data in data.items():
            if pause_callback:
                pause_callback()
            # Group on the same key the photo lookup uses. Two catalogs that
            # spell one photo differently are one entry, so the conflict
            # strategy decides between them instead of both being imported in
            # turn onto the photo they both resolve to. The entry keeps an
            # original spelling for the sidecar path.
            key = _path_lookup_key(raw_file_path)
            if key not in merged:
                merged[key] = {
                    "path": os.path.normpath(raw_file_path),
                    "flat_keywords": set(),
                    "hierarchical_keywords": set(),
                }

            if strategy == "merge_all":
                merged[key]["flat_keywords"].update(kw_data["flat_keywords"])
                merged[key]["hierarchical_keywords"].update(
                    kw_data["hierarchical_keywords"]
                )
            elif strategy == "prefer_first" and not merged[key]["flat_keywords"] or strategy == "prefer_last":
                merged[key]["path"] = os.path.normpath(raw_file_path)
                merged[key]["flat_keywords"] = kw_data["flat_keywords"]
                merged[key]["hierarchical_keywords"] = kw_data[
                    "hierarchical_keywords"
                ]

    imported = 0
    skipped = 0
    failed = 0
    total = len(merged)

    for i, (key, kw_data) in enumerate(merged.items()):
        if pause_callback:
            db.conn.commit()
            pause_callback()
        file_path = kw_data["path"]
        # Find matching photo in DB
        photo = photos_by_path.get(key)
        if not photo:
            skipped += 1
            if progress_callback:
                progress_callback(i + 1, total)
            continue

        try:
            # Import keywords into DB. Skip entries that normalize to `""`
            # (a lone smart quote, whitespace) — add_keyword() rejects those
            # after this PR, and the surrounding try/except would otherwise
            # count the whole photo as failed rather than just dropping the
            # malformed keyword.
            validate_import_locations(db, photo['id'], kw_data['flat_keywords'], kw_data['hierarchical_keywords'])
            # Import hierarchical keywords. Skip an entry whose chain
            # contains any segment that normalizes to `""` — the resulting
            # `add_keyword()` call would raise and the whole hierarchical
            # tree for this photo would be lost.
            hierarchy_leaf_keys = set()
            for hier in kw_data["hierarchical_keywords"]:
                parts = hier.split("|")
                if any(not keyword_match_key(part) for part in parts):
                    continue
                parent_id = None
                for index, part in enumerate(parts):
                    kid = db.add_keyword(part, parent_id=parent_id, _resolve_alias=index == len(parts) - 1)
                    parent_id = kid
                db.tag_photo(photo["id"], parent_id, source="manual")
                hierarchy_leaf_keys.add(keyword_match_key(parts[-1]))

            # Hierarchical leaves already represent their flat spelling.
            existing_keys = {keyword_match_key(k["name"])
                             for k in db.get_photo_keywords(photo["id"])} | hierarchy_leaf_keys
            for kw_name in kw_data["flat_keywords"]:
                if not keyword_match_key(kw_name) or keyword_match_key(kw_name) in existing_keys:
                    continue
                kid = db.add_keyword(kw_name, _resolve_alias=True)
                # Lightroom catalog metadata was explicitly authored outside
                # Vireo. Keep that provenance on the association even when
                # write_xmp=False leaves no sidecar or pending change.
                db.tag_photo(photo["id"], kid, source="manual")
                existing_keys.add(keyword_match_key(kw_name))

            # Write XMP if requested. Build normalized keyword sets so the
            # sidecar matches what we stored/tagged in the DB above:
            # entries that normalize to `""` are dropped (they were also
            # skipped by add_keyword), and edge-quote variants are written
            # in their clean form. Without this, the sidecar can carry a
            # `‘apapane` <rdf:li> while the DB row is clean `apapane`, and
            # a later XMP import/prune diff would tag the two as
            # different keywords.
            if write_xmp and Path(file_path).exists():
                xmp_flat = {
                    normalize_keyword_display(kw)
                    for kw in kw_data["flat_keywords"]
                    if keyword_match_key(kw)
                }
                xmp_hier = set()
                for hier in kw_data["hierarchical_keywords"]:
                    parts = hier.split("|")
                    if any(not keyword_match_key(part) for part in parts):
                        continue
                    xmp_hier.add(
                        "|".join(normalize_keyword_display(part) for part in parts)
                    )
                xmp_path = str(Path(file_path).with_suffix(".xmp"))
                write_sidecar(
                    xmp_path,
                    flat_keywords=xmp_flat,
                    hierarchical_keywords=xmp_hier,
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
