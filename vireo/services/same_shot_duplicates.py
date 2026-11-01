"""Review-only camera-exposure candidates, independent of exact-byte cleanup.

Metadata edits change a whole-file hash. Camera identity and shutter count
can still connect the two versions. These are clues, never permission to
reject a photo, skip an import or delete bytes. Stored perceptual hashes
provide additional visual evidence, not proof of identical originals.

A shutter counter can also repeat: a camera service can set it back, after
which new exposures reuse counts already in the catalog. Those pairs are
different photos, and their images and capture times both disagree. Neither
signal alone separates them from real copies: an edited derivative (a
denoised DNG, an exported JPEG) has a different visual hash but the same
capture time, and a capture-time correction changes the time but not the
image. A group where both disagree is reported as
``images_and_times_differ`` and listed after the likely copies.
"""
from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING

from services.same_shot_reviews import attach_same_shot_reviews
from services.service_types import JobDict, JsonDict

if TYPE_CHECKING:
    import sqlite3

    from db import Database

ShotIdentity = tuple[str, str, str, int]
_PHASH = re.compile(r"[0-9a-f]{16}", re.IGNORECASE)
_UNKNOWN = {"", "unknown", "none", "null", "n/a", "not available", "0"}
# Copies of one exposure keep its capture time to the second; derivative
# files can round it, so allow a minute before calling two times different.
_SAME_CAPTURE_SECONDS = 60
_VISUAL_ORDER = {"matching": 0, "unavailable": 1, "different": 2}


def _text(value: object) -> str:
    return str(value).strip().casefold() if isinstance(value, (str, int)) and not isinstance(value, bool) else ""


def shot_identity(row: sqlite3.Row) -> ShotIdentity | None:
    """Accept body serials and an explicit positive ShutterCount only.

    Do not substitute lens serials, image/file numbers, or dates. Conflicting
    body serials or counters are ambiguous and cannot form a review group.
    """
    try:
        metadata = json.loads(row["exif_data"] or "{}")
    except (ValueError, TypeError):
        return None
    if not isinstance(metadata, dict):
        return None
    groups = [metadata.get(name, {}) for name in ("EXIF", "MakerNotes")]
    groups = [group for group in groups if isinstance(group, dict)]
    serials = {_text(group[tag]) for group in groups
               for tag in ("SerialNumber", "BodySerialNumber", "CameraSerialNumber") if tag in group}
    serials -= _UNKNOWN
    serials = {serial for serial in serials if serial.strip("0 -")}
    counts = {_text(group["ShutterCount"]) for group in groups if "ShutterCount" in group}
    if len(serials) != 1 or len(counts) != 1:
        return None
    count = counts.pop()
    if not count.isascii() or not count.isdecimal() or len(count) > 18 or int(count) <= 0:
        return None
    exif = metadata.get("EXIF", {})
    exif = exif if isinstance(exif, dict) else {}
    make = _text(row["camera_make"] or exif.get("Make"))
    model = _text(row["camera_model"] or exif.get("Model"))
    if make in _UNKNOWN or model in _UNKNOWN:
        return None
    return make, model, serials.pop(), int(count)


def _photo(row: sqlite3.Row) -> JsonDict:
    """Do not stat a possibly sleeping NAS or claim that its files exist."""
    phash = row["phash"]
    return {
        "id": row["id"], "filename": row["filename"],
        "path": os.path.join(row["folder_path"], row["filename"]),
        "file_hash": row["file_hash"], "file_size": row["file_size"],
        "rating": row["rating"] or 0, "timestamp": row["timestamp"],
        "phash": phash.lower() if isinstance(phash, str) and _PHASH.fullmatch(phash) else None,
    }


def _capture_times_differ(photos: list[JsonDict]) -> bool:
    """True only when every time parses and two are more than a minute apart."""
    times = []
    for photo in photos:
        try:
            times.append(datetime.fromisoformat(photo["timestamp"]).replace(tzinfo=None))
        except (TypeError, ValueError):
            return False
    return (max(times) - min(times)).total_seconds() > _SAME_CAPTURE_SECONDS


def same_shot_sort_key(group: JsonDict) -> tuple[bool, int, list[object]]:
    """Likely copies first, the strongest visual evidence first among them."""
    return (bool(group.get("images_and_times_differ")),
            _VISUAL_ORDER.get(group["visual_evidence"], 1), group["identity"])


def _group(identity: ShotIdentity, photos: list[JsonDict]) -> JsonDict | None:
    if len(photos) < 2 or len({p["file_hash"] for p in photos}) < 2:
        return None  # The exact-byte workflow already covers identical hashes.
    hashes = {p["phash"] for p in photos if p["phash"] is not None}
    visual = "different" if len(hashes) > 1 else (
        "matching" if len(hashes) == 1 and all(p["phash"] for p in photos) else "unavailable"
    )
    make, model, serial, count = identity
    return {
        "identity": list(identity), "camera": f"{make} {model}".upper(),
        "serial_number": serial, "shutter_count": count,
        "visual_evidence": visual, "photos": sorted(photos, key=lambda p: p["path"]),
        "images_and_times_differ": visual == "different" and _capture_times_differ(photos),
    }


def _attach_photo_context(db: Database, groups: list[JsonDict]) -> None:
    photos = [photo for group in groups for photo in group["photos"]]
    ids = [photo["id"] for photo in photos]
    names = db.duplicates.workspace_names(ids)
    recipes = db.edits.get_photo_recipes(ids)
    for photo in photos:
        photo["workspaces"] = names.get(photo["id"], [])
        photo["edit_recipe"] = recipes.get(photo["id"])
    attach_same_shot_reviews(db, groups)


def scan_same_shot_candidates(
    db: Database, job: JobDict, cancel_check: Callable[[], bool] | None = None,
) -> JsonDict:
    """Tally identities first, then materialize only duplicate groups.

    Pass 1 holds just an id per eligible photo. Pass 2 reads the small
    subset that formed groups, so scan-time memory never scales with the
    whole catalog of singleton eligible photos.
    """
    seen_once: dict[ShotIdentity, int] = {}
    duplicated_ids: dict[ShotIdentity, list[int]] = defaultdict(list)
    examined = eligible = 0
    job["progress"] = {"phase": "Checking camera identities for possible same-shot copies", "current": 0}
    for row in db.duplicates.same_shot_rows():
        if cancel_check and cancel_check():
            break
        examined += 1
        identity = shot_identity(row)
        if identity:
            eligible += 1
            if identity in duplicated_ids:
                duplicated_ids[identity].append(row["id"])
            elif identity in seen_once:
                duplicated_ids[identity] = [seen_once.pop(identity), row["id"]]
            else:
                seen_once[identity] = row["id"]
        job["progress"]["current"] = examined
        job["progress"]["phase"] = f"Checking camera identities: {examined:,} catalog photos examined"
    seen_once.clear()
    by_identity: dict[ShotIdentity, list[JsonDict]] = defaultdict(list)
    if duplicated_ids and not (cancel_check and cancel_check()):
        wanted = sorted({i for ids in duplicated_ids.values() for i in ids})
        for row in db.duplicates.same_shot_rows(wanted):
            if cancel_check and cancel_check():
                break
            identity = shot_identity(row)
            if identity in duplicated_ids:
                by_identity[identity].append(_photo(row))
    groups = [group for identity, photos in by_identity.items() if (group := _group(identity, photos))]
    groups.sort(key=same_shot_sort_key)
    _attach_photo_context(db, groups)
    return {
        "same_shot_scanned": True, "same_shot_groups": groups,
        "same_shot_eligible_photos": eligible, "same_shot_examined_photos": examined,
    }


def revalidate_same_shot_groups(db: Database, groups: list[JsonDict]) -> list[JsonDict]:
    """Drop changed/reused ids and refresh evidence without reading photo files."""
    ids = sorted({photo["id"] for group in groups for photo in group["photos"]})
    rows = {row["id"]: row for row in db.duplicates.same_shot_rows(ids)}
    current = []
    for group in groups:
        photos = []
        identity = tuple(group["identity"])
        for saved in group["photos"]:
            row = rows.get(saved["id"])
            if row is None or shot_identity(row) != identity:
                continue
            photo = _photo(row)
            if (photo["path"], photo["file_hash"]) == (saved["path"], saved["file_hash"]):
                photos.append(photo)
        if refreshed := _group(identity, photos):
            current.append(refreshed)
    current.sort(key=same_shot_sort_key)
    _attach_photo_context(db, current)
    return current


def _current_group_contains(db: Database, photo_id: int, group: JsonDict) -> bool:
    groups = revalidate_same_shot_groups(db, [group])
    return any(photo["id"] == photo_id for current in groups for photo in current["photos"])


def is_duplicate_review_member(
    db: Database, photo_id: int, live_jobs: Callable[[], list[JobDict]] | None = None,
) -> bool:
    """Require current exact, saved, or just-completed in-memory review membership.

    JobRunner publishes completion before saving history. Permit the live
    completed result during that window too, with the same identity checks,
    so the initial thumbnails and Reveal do not race history persistence.
    """
    if db.duplicates.is_group_member(photo_id):
        return True
    saved = db.duplicates.saved_same_shot_group(photo_id)
    if saved is not None and _current_group_contains(db, photo_id, json.loads(saved)):
        return True
    if live_jobs:
        for job in live_jobs():
            if job.get("type") != "duplicate-scan" or job.get("status") != "completed" or job.get("_persisted"):
                continue
            for group in (job.get("result") or {}).get("same_shot_groups", []):
                if any(photo["id"] == photo_id for photo in group["photos"]) and _current_group_contains(db, photo_id, group):
                    return True
    return False
