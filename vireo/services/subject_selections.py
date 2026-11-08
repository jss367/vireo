"""Reversible primary choices and reviewer boxes, with subject evidence invalidation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from services.subject_verdicts import SubjectVerdictError, _photo_free_of_analysis
from subjects import REVIEWER_MODEL, retained, select_reviewer_box, sync_primary

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from db import Database
    from services.service_types import JsonDict

ACTION_TYPE = "subject_selection"


def _snapshot(db: Database, photo_id: int) -> str:
    choice = db.subjects.choice(photo_id)
    boxes = [{"box": {k: row["box_" + k] for k in "xywh"}, "verdict": row["subject_verdict"]}
             for row in db.detections.get(photo_id, 0, REVIEWER_MODEL, include_rejected=True)]
    return json.dumps({"choice": choice["detection_id"] if choice else None, "boxes": boxes}, sort_keys=True)


def set_selection(
    db: Database, photo_id: int, body: JsonDict, *, reviewer_box: bool = False,
    source: str | None = None, reason: str | None = None,
) -> JsonDict:
    """Save one correction and its before/after history atomically; refuse busy photos."""
    with db.transaction(immediate=True), _photo_free_of_analysis(photo_id):
        if db.photos.get(photo_id, verify_workspace=True) is None:
            raise SubjectVerdictError("Photo not found in the active workspace", 404)
        before = _snapshot(db, photo_id)
        if reviewer_box:
            select_reviewer_box(db, photo_id, body.get("box"))
        else:
            detection_id = body["detection_id"]
            if detection_id is not None and (type(detection_id) is not int
                    or not any(d["id"] == detection_id for d in retained(db, photo_id))):
                raise SubjectVerdictError("Choose a retained animal detection from this photo")
            sync_primary(db, photo_id)
            if detection_id is None:
                db.subjects.clear_choice(photo_id)
            else:
                db.subjects.set_choice(photo_id, detection_id)
            sync_primary(db, photo_id)
        after = _snapshot(db, photo_id)
        result: JsonDict = {"changed": before != after}
        if before != after:
            result["edit_id"] = db.record_edit(
                ACTION_TYPE, "Selected animal region" if reviewer_box else "Changed primary subject", after,
                [{"photo_id": photo_id, "old_value": before, "new_value": after}], source=source, reason=reason,
            )
    return result


def replay_history(db: Database, items: Sequence[Mapping], *, undo: bool) -> None:
    """Restore geometry and choice, discarding derived evidence rather than reviving stale scores."""
    for item in items:
        photo_id = int(item["photo_id"])
        state = json.loads(item["old_value" if undo else "new_value"])
        with _photo_free_of_analysis(photo_id):
            current = _snapshot(db, photo_id)
            if json.loads(current)["boxes"] != state["boxes"]:
                db.detections.clear(photo_id, REVIEWER_MODEL)
                ids = db.detections.upsert_rows(photo_id, REVIEWER_MODEL, [
                    {"box": entry["box"], "confidence": 1.0, "category": "animal"}
                    for entry in state["boxes"]], "reviewer-v1")
                for detection_id, entry in zip(ids, state["boxes"], strict=True):
                    db.detections.set_subject_verdict(detection_id, entry["verdict"])
            db.subjects.clear_primary_features(photo_id)
            if state["choice"] is None:
                db.subjects.clear_choice(photo_id)
            else:
                db.subjects.set_choice(photo_id, state["choice"])
            sync_primary(db, photo_id)
