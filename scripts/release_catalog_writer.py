"""Write the release-catalog dataset with one release's own code.

``release_catalogs.py`` runs this file in a child process whose import path
starts with a release's ``vireo/`` directory, so every write below goes
through that release's ``Database`` and services, not today's. It writes the
same small dataset every release can express (a folder, four photos, flags,
ratings, keywords, a detection and an active subject mask per masked photo),
then reads the catalog back with ``snapshot`` and prints that as JSON. The
upgrade test takes the same snapshot after current code has opened the
catalog and requires the two to match, so a migration that drops or changes
any of it fails.

Internal APIs move between releases (``Database.add_photo`` became
``services.photo_ingest.add_photo``, ``upsert_photo_mask`` became
``masks_features.upsert_mask``, ...), so each write tries the newest
location first and falls back to the older one. When a release offers none
of the candidates the writer fails rather than writing rows by hand: a
catalog whose rows this script inserted itself would no longer be one the
release made.

Usage (from ``release_catalogs.py``): ``python release_catalog_writer.py
<release vireo dir> <catalog path> <snapshot path>``. ``snapshot`` uses only the standard
library, so the test imports it from this file.
"""

from __future__ import annotations

import importlib
import inspect
import json
import os
import sqlite3
import sys

FOLDER_PATH = "/vireo-release-fixture/2026-10-04 Lake Murray"
MASK_DIR = "/vireo-release-fixture/masks"
MASK_VARIANT = "sam2-small"
DETECTOR_MODEL = "megadetector-v6"

# Each photo: filename, flag, rating, keywords as (name, is_species), and
# the detection box when the photo carries a subject mask.
PHOTOS = [
    {
        "filename": "DSC_0001.NEF", "flag": "flagged", "rating": 5,
        "keywords": [("Northern Cardinal", True), ("backyard", False)],
        "box": {"x": 0.2, "y": 0.25, "w": 0.4, "h": 0.5},
    },
    {
        "filename": "DSC_0002.NEF", "flag": "rejected", "rating": 1,
        "keywords": [("Northern Cardinal", True)],
        "box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
    },
    {
        "filename": "DSC_0003.JPG", "flag": None, "rating": 3,
        "keywords": [("Red-tailed Hawk", True), ("backyard", False)],
        "box": None,
    },
    {
        "filename": "DSC_0004.JPG", "flag": None, "rating": 0,
        "keywords": [],
        "box": None,
    },
]


class MissingApi(RuntimeError):
    """None of the candidate write paths exists in this release."""


def _module(name):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


def _attr(obj, dotted):
    for part in dotted.split("."):
        if obj is None:
            return None
        try:
            obj = getattr(obj, part)
        except AttributeError:
            return None
    return obj


def _first(what, *candidates):
    """Return the first ``(owner, dotted attribute)`` that resolves."""
    for owner, dotted in candidates:
        found = _attr(owner, dotted)
        if found is not None:
            return found
    raise MissingApi(f"no write path for {what}")


def _commit(db):
    commit = getattr(db, "commit", None) or db.conn.commit
    commit()


def _transaction(db):
    """The release's unit of work, or a plain commit for older releases."""
    transaction = getattr(db, "transaction", None)
    if transaction is not None:
        return transaction()

    class _Commit:
        def __enter__(self):
            return None

        def __exit__(self, exc_type, *_):
            if exc_type is None:
                _commit(db)
            return False

    return _Commit()


def _init_schema(db_path):
    """Create the catalog the way the release's startup does."""
    schema = _module("schema")
    ensure_schema = getattr(schema, "ensure_schema", None)
    if ensure_schema is not None:
        ensure_schema(db_path)


def _add_folder(db, services):
    name = os.path.basename(FOLDER_PATH)
    folders = services.get("workspace_folders")
    if folders is not None and hasattr(folders, "add_folder"):
        return folders.add_folder(db, FOLDER_PATH, name=name)
    return _first("folders", (db, "add_folder"))(FOLDER_PATH, name=name)


def _add_photo(db, services, folder_id, index, photo):
    stem, ext = os.path.splitext(photo["filename"])
    args = (
        folder_id, photo["filename"], ext.lower(), 25_000_000 + index,
        1_759_600_000.0 + index,
    )
    kwargs = {
        "timestamp": f"2026-10-04T08:1{index}:00",
        "width": 8256, "height": 5504,
        "file_hash": f"release-fixture-{stem.lower()}",
    }
    ingest = services.get("photo_ingest")
    if ingest is not None and hasattr(ingest, "add_photo"):
        return ingest.add_photo(db, *args, **kwargs)
    return _first("photos", (db, "add_photo"))(*args, **kwargs)


def _review(db, services):
    service = _first("flags and ratings", (services.get("photo_review"), "PhotoReviewService"))
    return service(db)


def _add_keyword(db, name, is_species):
    add = _first("keywords", (db, "keywords.add"), (db, "add_keyword"))
    return add(name, is_species=is_species)


def _tag(db, services, photo_id, keyword_id):
    tagging = services.get("keyword_tagging")
    if tagging is not None and hasattr(tagging, "tag_photo"):
        tagging.tag_photo(db, photo_id, keyword_id)
    else:
        _first("keyword tags", (db, "tag_photo"))(photo_id, keyword_id)


def _save_detection(db, photo_id, box):
    save = _first("detections", (db, "detections.save"), (db, "save_detections"))
    detection = {"box": box, "confidence": 0.93, "category": "animal"}
    return save(photo_id, [detection], DETECTOR_MODEL, "legacy")


def _sync_primary(db, photo_id):
    """Project the primary subject when the release keeps one per photo."""
    subjects = _module("subjects")
    sync_primary = getattr(subjects, "sync_primary", None)
    if sync_primary is not None:
        sync_primary(db, photo_id, min_conf=0.2)


def _save_mask(db, services, photo_id, box):
    path = f"{MASK_DIR}/{photo_id}_{MASK_VARIANT}.png"
    prompt = (box["x"], box["y"], box["w"], box["h"])
    upsert = _first("masks", (db, "masks_features.upsert_mask"), (db, "upsert_photo_mask"))
    # The prompt box became one ``prompt`` argument after being four.
    if "prompt" in inspect.signature(upsert).parameters:
        upsert(photo_id, MASK_VARIANT, path, DETECTOR_MODEL, prompt)
    else:
        upsert(photo_id, MASK_VARIANT, path, DETECTOR_MODEL, *prompt)
    subject_masks = services.get("subject_masks")
    if subject_masks is not None and hasattr(subject_masks, "set_active_mask_variant"):
        subject_masks.set_active_mask_variant(db, photo_id, MASK_VARIANT)
    else:
        _first("active masks", (db, "set_active_mask_variant"))(photo_id, MASK_VARIANT)
    return path


def write_catalog(db_path):
    """Write the dataset through the release's code and return its snapshot."""
    _init_schema(db_path)
    from db import Database

    services = {
        name: _module(f"services.{name}")
        for name in ("workspace_folders", "photo_ingest", "photo_review",
                     "keyword_tagging", "subject_masks")
    }
    with Database(db_path) as db:
        with _transaction(db):
            folder_id = _add_folder(db, services)
        for index, photo in enumerate(PHOTOS, start=1):
            with _transaction(db):
                photo_id = _add_photo(db, services, folder_id, index, photo)
            review = _review(db, services)
            if photo["flag"]:
                review.set_flag(photo_id, photo["flag"])
            if photo["rating"]:
                review.set_rating(photo_id, photo["rating"])
            with _transaction(db):
                for name, is_species in photo["keywords"]:
                    _tag(db, services, photo_id, _add_keyword(db, name, is_species))
            if photo["box"] is not None:
                with _transaction(db):
                    _save_detection(db, photo_id, photo["box"])
                    _sync_primary(db, photo_id)
                with _transaction(db):
                    _save_mask(db, services, photo_id, photo["box"])
        _commit(db)
    with sqlite3.connect(db_path) as conn:
        state = snapshot(conn)
    _check_written(state)
    return state


def snapshot(conn):
    """What a user would miss if an upgrade lost it, read with plain SQL.

    Reads only columns every release since the fixtures began has kept
    (``photos.flag``/``rating``/``mask_path``, ``photo_keywords``,
    ``photo_masks``, ``workspace_folders``), so the same query describes the
    catalog before and after current code migrates it.
    """
    conn.row_factory = sqlite3.Row
    photos = {}
    rows = conn.execute(
        "SELECT p.id, p.filename, p.flag, p.rating, p.mask_path FROM photos p "
        "JOIN folders f ON f.id = p.folder_id WHERE f.path = ? ORDER BY p.filename",
        (FOLDER_PATH,),
    ).fetchall()
    for row in rows:
        keywords = sorted(r[0] for r in conn.execute(
            "SELECT k.name FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ?", (row["id"],)))
        masks = [dict(r) for r in conn.execute(
            "SELECT variant, path, detector_model FROM photo_masks "
            "WHERE photo_id = ? ORDER BY variant", (row["id"],))]
        photos[row["filename"]] = {
            "flag": row["flag"] or "none",
            "rating": row["rating"] or 0,
            "keywords": keywords,
            "masks": masks,
            "active_mask": row["mask_path"],
        }
    workspaces = sorted(r[0] for r in conn.execute(
        "SELECT w.name FROM workspace_folders wf JOIN workspaces w ON w.id = wf.workspace_id "
        "JOIN folders f ON f.id = wf.folder_id WHERE f.path = ?", (FOLDER_PATH,)))
    return {
        "schema_version": conn.execute("PRAGMA user_version").fetchone()[0],
        "folder_workspaces": workspaces,
        "photos": photos,
    }


def _check_written(state):
    """Fail when the release silently skipped part of the dataset."""
    for photo in PHOTOS:
        got = state["photos"].get(photo["filename"])
        if got is None:
            raise RuntimeError(f"{photo['filename']} was not written")
        expected_masks = 0 if photo["box"] is None else 1
        problems = [
            got["flag"] != (photo["flag"] or "none") and "flag",
            got["rating"] != photo["rating"] and "rating",
            len(got["keywords"]) != len(photo["keywords"]) and "keywords",
            len(got["masks"]) != expected_masks and "masks",
            (got["active_mask"] is not None) != bool(expected_masks) and "active mask",
        ]
        problems = [p for p in problems if p]
        if problems:
            raise RuntimeError(f"{photo['filename']}: {', '.join(problems)} not written: {got}")
    if not state["folder_workspaces"]:
        raise RuntimeError("the folder is in no workspace")


def main(argv):
    release_dir, db_path, snapshot_path = argv[1:4]
    # Import the release's modules, never today's: drop this script's own
    # directory from the path and put the release first.
    here = os.path.dirname(os.path.abspath(__file__))
    sys.path[:] = [release_dir] + [p for p in sys.path if os.path.abspath(p or ".") != here]
    os.chdir(release_dir)
    state = write_catalog(db_path)
    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
        f.write("\n")


if __name__ == "__main__":
    main(sys.argv)
