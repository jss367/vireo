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


_AMBIGUOUS = object()


def _fold(path):
    """Case-folded key for an already-normalized path. Identity on POSIX."""
    return os.path.normcase(path)


def _same_file(one, other):
    """Whether two spellings of a path name a single file.

    Only reachable where ``os.path.normcase`` folds anything, i.e. Windows: a
    case-insensitive directory holds one file under both spellings, while a
    directory with per-directory case sensitivity enabled can hold two. Ask the
    filesystem. When it cannot answer — an offline volume, a catalog naming
    files that are not on this machine — read the spellings as one file, which
    is overwhelmingly what they are.
    """
    try:
        return os.path.samefile(one, other)
    except OSError:
        return True


class _PhotoPathIndex:
    """Find the photo a catalog path names, exact spelling first.

    Lightroom records its own spelling, so on Windows a catalog names
    ``D:/Pictures/a.jpg`` for the photo the scanner stored as
    ``d:\\Pictures\\a.jpg`` — without folding case and separator, a real
    import matches nothing at all. But folding alone would collapse ``Bird.jpg``
    and ``bird.jpg``, which can coexist in a directory with per-directory case
    sensitivity enabled. So an exact path wins whenever there is one, and a
    folded key resolves only when it names exactly one photo.
    """

    def __init__(self):
        self._exact = {}
        self._folded = {}

    def add(self, path, photo):
        path = os.path.normpath(path)
        self._exact[path] = photo
        key = _fold(path)
        claimed, _ = self._folded.get(key, (path, None))
        self._folded[key] = (path, photo) if claimed == path else (None, _AMBIGUOUS)

    def get(self, path):
        path = os.path.normpath(path)
        if path in self._exact:
            return self._exact[path]
        _, photo = self._folded.get(_fold(path), (None, None))
        return None if photo is _AMBIGUOUS else photo


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
    merged = {}  # normalized path -> {file_path, keywords_by_catalog: {cat_name: set}}
    spellings = {}  # folded key -> normalized paths already grouped under it

    for cat_path in catalog_paths:
        try:
            preview = preview_catalog(cat_path, db)
            catalogs.append(preview)

            data = read_catalog(cat_path)
            cat_name = Path(cat_path).stem

            for file_path, kw_data in data.items():
                # Two catalogs can spell one file differently, and the user
                # needs to see that as the conflict it is rather than as two
                # untroubled singletons. Group those together — but only when
                # they really are one file, so a case-sensitive directory's
                # Bird.jpg and bird.jpg stay the two files they are.
                key = os.path.normpath(file_path)
                siblings = spellings.setdefault(_fold(key), [])
                if key not in merged:
                    key = next((s for s in siblings if _same_file(s, key)), key)
                if key not in merged:
                    siblings.append(key)
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
    # Build path -> DB photo lookup.
    photos_by_path = _PhotoPathIndex()
    all_photos = db.get_photos(per_page=999999)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    for p in all_photos:
        if pause_callback:
            pause_callback()
        folder_path = folders.get(p["folder_id"], "")
        photos_by_path.add(os.path.join(folder_path, p["filename"]), p)

    # Merge catalog data
    merged = {}  # group key -> {path, photo, flat_keywords, hierarchical_keywords}
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
            # Group on the photo the entry resolves to. Two catalogs that
            # spell one photo differently are then a single entry, so the
            # conflict strategy decides between them instead of both being
            # imported in turn onto the photo they both land on. Entries that
            # match no photo keep their own spelling; they are only counted.
            # The entry carries an original path for the sidecar write.
            path = os.path.normpath(raw_file_path)
            photo = photos_by_path.get(path)
            key = ("photo", photo["id"]) if photo else ("path", path)
            if key not in merged:
                merged[key] = {
                    "path": path,
                    "photo": photo,
                    "flat_keywords": set(),
                    "hierarchical_keywords": set(),
                }

            if strategy == "merge_all":
                merged[key]["flat_keywords"].update(kw_data["flat_keywords"])
                merged[key]["hierarchical_keywords"].update(
                    kw_data["hierarchical_keywords"]
                )
            elif strategy == "prefer_first" and not merged[key]["flat_keywords"] or strategy == "prefer_last":
                merged[key]["path"] = path
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
        photo = kw_data["photo"]
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
