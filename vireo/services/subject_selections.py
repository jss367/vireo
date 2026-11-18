"""Reversible primary choices and reviewer boxes, with subject evidence invalidation."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from detection_id import detection_id as legacy_detection_id
from services.detection_store import delete_detections_by_ids, get_detections
from services.subject_verdicts import SubjectVerdictError, _photo_free_of_analysis
from subjects import (
    REVIEWER_MODEL,
    retained,
    reviewer_revision,
    select_reviewer_box,
    sync_primary,
    validate_reviewer_box,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from db import Database
    from services.service_types import JsonDict

ACTION_TYPE = "subject_selection"


def _region_state(row: sqlite3.Row) -> JsonDict:
    return {
        "id": row["id"], "box": {k: row["box_" + k] for k in "xywh"},
        "verdict": row["subject_verdict"], "runtime": row["runtime_fingerprint"],
    }


def _snapshot(db: Database, photo_id: int) -> str:
    choice = db.subjects.choice(photo_id)
    rows = get_detections(db, photo_id, 0, REVIEWER_MODEL, include_rejected=True)
    boxes = [_region_state(row) for row in sorted(rows, key=lambda row: row["id"])]
    return json.dumps({"choice": choice["detection_id"] if choice else None, "boxes": boxes}, sort_keys=True)


def _write_region(db: Database, photo_id: int, region_id: int, old: JsonDict | None, new: JsonDict | None) -> None:
    """Change a single region and retire only evidence derived from its old input."""
    if old == new:
        return
    if new is not None:
        for row in get_detections(db, photo_id, 0, REVIEWER_MODEL, include_rejected=True):
            if row["id"] != region_id and all(row["box_" + k] == new["box"][k] for k in "xywh"):
                raise SubjectVerdictError("Another reviewer region already has this box", 409)
    if old is None:
        assert new is not None
        if db.detections.row(region_id) is not None:
            raise SubjectVerdictError("Region identity is already in use", 409)
        db.detections.insert_reviewer_region(photo_id, region_id, new["box"], new["runtime"], new["verdict"])
        return
    primary = db.subjects.primary_state(photo_id)
    if primary is None or primary["detection_id"] == region_id:
        db.subjects.clear_primary_features(photo_id)
    db.masks_features.clear_detection_embeddings(photo_id, region_id)
    db.masks_features.retire_prompt_masks(photo_id, REVIEWER_MODEL, old["box"])
    if new is None:
        delete_detections_by_ids(db, [region_id])
    else:
        db.detections.update_reviewer_region(region_id, new["box"], new["runtime"], new["verdict"])


def _save_box(db: Database, photo_id: int, body: JsonDict) -> str:
    from services.subject_frames import verified_reviewer_runtime

    operation = body.get("operation", "add")
    if operation not in ("add", "edit", "remove"):
        raise ValueError("Choose add, edit or remove")
    runtime = "reviewer-v1"
    if operation != "remove":
        validate_reviewer_box(body.get("box"))
        if "frame" in body and "frame_token" not in body:
            raise ValueError("Reload the original frame before confirming the reviewer box")
        if "frame_token" in body:
            runtime = verified_reviewer_runtime(db, photo_id, body["frame_token"])
    if operation == "add":
        select_reviewer_box(db, photo_id, body["box"], frame_runtime=runtime)
        return "Added animal region"
    region_id = body.get("detection_id")
    if type(region_id) is not int or not 0 <= region_id < 2**53:
        raise ValueError("Choose a reviewer region from this photo")
    row = db.detections.row(region_id)
    if (row is None or row["photo_id"] != photo_id or row["detector_model"] != REVIEWER_MODEL
            or body.get("revision") != reviewer_revision(row)):
        raise SubjectVerdictError("Region changed. Reload the subjects and try again.", 409)
    old = _region_state(row)
    new = dict(old, box=body["box"], runtime=runtime) if operation == "edit" else None
    _write_region(db, photo_id, region_id, old, new)
    choice = db.subjects.choice(photo_id)
    if new is None and choice and choice["detection_id"] == region_id:
        db.subjects.clear_choice(photo_id)
    sync_primary(db, photo_id)
    return "Edited animal region" if new else "Removed animal region"


def set_selection(
    db: Database, photo_id: int, body: JsonDict, *, reviewer_box: bool = False,
    source: str | None = None, reason: str | None = None,
) -> JsonDict:
    """Save one correction and its before/after history atomically; refuse busy photos."""
    with db.transaction(immediate=True), _photo_free_of_analysis(photo_id):
        if db.photos.get(photo_id, verify_workspace=True) is None:
            raise SubjectVerdictError("Photo not found in the active workspace", 404)
        before = _snapshot(db, photo_id)
        if reviewer_box and body.get("operation") == "remove":
            region_id = body.get("detection_id")
            if type(region_id) is int and 0 <= region_id < 2**53:
                state = json.loads(before)
                state["removed_evidence"] = {"region_id": region_id, "rows": db.predictions.region_removal_state(region_id)}
                before = json.dumps(state, sort_keys=True)
        if reviewer_box:
            description = _save_box(db, photo_id, body)
        else:
            description = "Changed primary subject"
            detection_id = body["detection_id"]
            candidates = retained(db, photo_id)
            primary_id = candidates[0]["id"] if candidates else None
            if "expected_primary_detection_id" in body and body["expected_primary_detection_id"] != primary_id:
                raise SubjectVerdictError("Primary subject changed. Reload the subjects and try again.", 409)
            if detection_id is not None and (type(detection_id) is not int
                    or not any(d["id"] == detection_id for d in candidates)):
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
                ACTION_TYPE, description, after,
                [{"photo_id": photo_id, "old_value": before, "new_value": after}], source=source, reason=reason,
            )
    return result


def _regions(photo_id: int, state: JsonDict) -> dict[int, JsonDict]:
    regions = {}
    for entry in state["boxes"]:
        entry.setdefault("runtime", "reviewer-v1")
        if "id" not in entry:
            entry["id"] = legacy_detection_id(photo_id, REVIEWER_MODEL,
                (entry["box"]["x"], entry["box"]["y"], entry["box"]["w"], entry["box"]["h"]), "animal")
        regions[entry["id"]] = entry
    return regions


def _replay_region(db: Database, photo_id: int, region_id: int, old: JsonDict | None,
                   new: JsonDict | None, expected: JsonDict, desired: JsonDict) -> None:
    archive = expected.get("removed_evidence", {})
    if new is None:
        evidence = db.predictions.region_removal_state(region_id)
        if archive.get("region_id") == region_id:
            if evidence != archive["rows"]:
                raise SubjectVerdictError("Species decisions changed since this removal. Reload before changing the region.", 409)
        elif (evidence["prediction_history_instances"]
                or any(row["status"] != "pending" for row in evidence["prediction_review"])
                or any(row["decision"] != "unreviewed" for row in evidence["detection_evidence_reviews"])):
            raise SubjectVerdictError("This history entry cannot preserve the region's newer decisions. Remove it explicitly instead.", 409)
    _write_region(db, photo_id, region_id, old, new)
    archive = desired.get("removed_evidence", {})
    if old is None and new is not None and archive.get("region_id") == region_id:
        try:
            db.predictions.restore_region_removal_state(archive["rows"])
        except sqlite3.IntegrityError as exc:
            raise SubjectVerdictError("Saved species decisions conflict with current catalog identities", 409) from exc


def replay_history(db: Database, items: Sequence[Mapping], *, undo: bool) -> None:
    for item in items:
        photo_id = int(item["photo_id"])
        with _photo_free_of_analysis(photo_id):
            current = json.loads(_snapshot(db, photo_id))
            expected = json.loads(item["new_value" if undo else "old_value"])
            desired = json.loads(item["old_value" if undo else "new_value"])
            live, before, after = (_regions(photo_id, state) for state in (current, expected, desired))
            for region_id in before.keys() | after.keys():
                old, new = before.get(region_id), after.get(region_id)
                if old != new:
                    if live.get(region_id) != old:
                        raise SubjectVerdictError("Region changed since this edit", 409)
                    _replay_region(db, photo_id, region_id, old, new, expected, desired)
            if expected["choice"] != desired["choice"]:
                if current["choice"] != expected["choice"]:
                    raise SubjectVerdictError("Primary subject changed since this edit", 409)
                if desired["choice"] is None:
                    db.subjects.clear_choice(photo_id)
                else:
                    db.subjects.set_choice(photo_id, desired["choice"])
            sync_primary(db, photo_id)
