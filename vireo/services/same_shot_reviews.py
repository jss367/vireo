"""Remember human review of a specific catalog snapshot, never a deletion verdict."""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

from services.service_types import JsonDict

if TYPE_CHECKING:
    from db import Database


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def attach_same_shot_reviews(db: Database, groups: list[JsonDict]) -> None:
    """Invalidate a decision when membership, file identity or visible evidence changes."""
    fields = ("id", "file_hash", "path", "file_size", "timestamp", "phash", "edit_recipe")
    for group in groups:
        group["group_key"] = _digest(group["identity"])
        snapshot = [{field: photo.get(field) for field in fields}
                    for photo in sorted(group["photos"], key=lambda photo: photo["id"])]
        group["review_key"] = _digest([group["identity"], snapshot])
        group["review_decision"] = None
        group["reviewed_at"] = None
    reviews = db.duplicates.same_shot_reviews([group["group_key"] for group in groups])
    for group in groups:
        saved = reviews.get(group["group_key"])
        if saved and saved["review_key"] == group["review_key"]:
            group["review_decision"] = saved["decision"]
            group["reviewed_at"] = saved["updated_at"]


def record_same_shot_review(
    db: Database, photo_ids: list[int], review_key: str, decision: str | None,
) -> JsonDict | None:
    """Return the refreshed group, or None for a stale/invalid client snapshot.

    Only catalog data is read; this note makes no claim about file availability
    or identical pixels. BEGIN IMMEDIATE holds the snapshot stable through commit.
    """
    from services.same_shot_duplicates import _attach_photo_context, _group, _photo, shot_identity

    with db.duplicates.transaction():
        db.duplicates.begin_review_write()
        rows = list(db.duplicates.same_shot_rows(photo_ids))
        identities = {shot_identity(row) for row in rows}
        if len(rows) != len(photo_ids) or len(identities) != 1 or None in identities:
            return None
        identity = identities.pop()
        if identity is None:
            return None
        group = _group(identity, [_photo(row) for row in rows])
        if group is None:
            return None
        _attach_photo_context(db, [group])
        if group["review_key"] != review_key:
            return None
        db.duplicates.save_same_shot_review(group["group_key"], review_key, decision)
        attach_same_shot_reviews(db, [group])
        return group
