"""The reviewer's verdict on a detector region: reject it as background, or retain it again.

A detector proposes boxes and a classifier ranks species inside every one of
them, so a stump or a pine branch can carry a strong "Mule Deer" with no
animal in it. ``reject_region`` records that the user looked and saw no
animal: it writes ``detections.subject_verdict = 'not_animal'``, a
catalog-wide fact about the pixels (a stump is a stump in every workspace),
and from then on the region is not retained anywhere. Every reader that
applies the detector-confidence floor also requires a NULL verdict, so the
box and its predictions leave Review, species evidence, encounter species
labels and primary-subject selection together, and the photo's next box
becomes its primary. Nothing is deleted: the predictions, masks and review
rows stay, so restoring the region (``verdict=None``) brings them back
without reclassifying, and the verdict is one Undo step.

Only the detector's own rerun can retire the row, and the content-addressed
upsert never writes this column, so a rerun that finds the same box keeps
the verdict. Changing the verdict also refreshes the photo's evidence in the
pipeline results cache (``pipeline.refresh_cache_predictions_for_photos``),
which recomputes its encounter's species label from the remaining regions
and touches no other photo.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

from pipeline_locks import acquire_photo_mask
from subjects import sync_primary

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Mapping, Sequence

    from db import Database
    from services.service_types import JsonDict

VERDICTS = frozenset({"not_animal"})
ACTION_TYPE = "subject_verdict"
SOURCE_MAX_LENGTH = 80
REASON_MAX_LENGTH = 300


class SubjectVerdictError(ValueError):
    """A verdict the route must refuse, with the status it answers with."""

    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


def validate_verdict(value: object) -> str | None:
    """The verdict a request body carries: ``'not_animal'`` or None (retain)."""
    if value is None:
        return None
    if not isinstance(value, str) or value not in VERDICTS:
        raise SubjectVerdictError("verdict must be 'not_animal' or null")
    return value


def validate_provenance(body: JsonDict) -> tuple[str | None, str | None]:
    """Optional ``source`` (who decided) and ``reason`` (why), bounded."""
    values: list[str | None] = []
    for key, limit in (("source", SOURCE_MAX_LENGTH), ("reason", REASON_MAX_LENGTH)):
        value = body.get(key)
        if value is None:
            values.append(None)
            continue
        if not isinstance(value, str) or not value.strip() or len(value) > limit:
            raise SubjectVerdictError(f"{key} must be a non-empty string of at most {limit} characters")
        values.append(value.strip())
    return values[0], values[1]


def _description(verdict: str | None, source: str | None, reason: str | None) -> str:
    text = "Rejected region as not an animal" if verdict else "Restored region as a subject"
    if source and reason:
        return f"{text} — {source}: {reason}"
    if source:
        return f"{text} — by {source}"
    if reason:
        return f"{text} — {reason}"
    return text


def _state(detection_id: int, verdict: str | None) -> str:
    return json.dumps({"detection_id": detection_id, "verdict": verdict}, sort_keys=True)


def _apply(db: Database, photo_id: int, detection_id: int, verdict: str | None) -> None:
    """Write the verdict and re-project the photo's primary subject (no commit)."""
    db.detections.set_subject_verdict(detection_id, verdict)
    sync_primary(db, photo_id)


def set_subject_verdict(
    db: Database, detection_id: int, verdict: str | None, *,
    source: str | None = None, reason: str | None = None,
) -> JsonDict:
    """Record ``verdict`` on one region as one Undo step; return the new state.

    Raises ``SubjectVerdictError`` (404) when the detection is not in the
    active workspace, (400) for a full-image classification, which has no
    region to reject, and (409) while subject analysis holds the photo.
    An unchanged verdict writes nothing and reports ``changed: False``.
    """
    row = db.detections.row(detection_id)
    if row is None or db.photos.get(row["photo_id"], verify_workspace=True) is None:
        raise SubjectVerdictError("Detection not found in the active workspace", 404)
    if row["detector_model"] == "full-image":
        raise SubjectVerdictError("A full-image classification has no region to reject")
    photo_id = int(row["photo_id"])
    previous = row["subject_verdict"]
    result: JsonDict = {"photo_id": photo_id, "detection_id": detection_id,
                        "verdict": verdict, "previous_verdict": previous, "changed": previous != verdict}
    if previous == verdict:
        return result
    lock = acquire_photo_mask(photo_id)
    if not lock.acquire(blocking=False):
        raise SubjectVerdictError(
            "Subject analysis is running for this photo. Try again when it finishes.", 409,
        )
    try:
        # One unit of work: the verdict, the primary re-projection and the
        # history row commit together (record_edit's own commit and prune
        # run inside the unit, where a legacy commit is a no-op).
        with db.transaction(immediate=True):
            _apply(db, photo_id, detection_id, verdict)
            result["edit_id"] = db.record_edit(
                ACTION_TYPE, _description(verdict, source, reason), _state(detection_id, verdict),
                [{"photo_id": photo_id, "old_value": _state(detection_id, previous),
                  "new_value": _state(detection_id, verdict)}],
                source=source, reason=reason,
            )
    finally:
        lock.release()
    return result


def replay_history(
    db: Database, items: Sequence[sqlite3.Row | Mapping[str, object]], *, undo: bool,
) -> None:
    """Put every item's region back to its recorded verdict (undo) or forward (redo).

    Runs inside the caller's transaction. A region the detector has since
    retired is skipped: there is nothing to restore and the photo's primary
    was already re-projected when the row went.
    """
    key = "old_value" if undo else "new_value"
    for item in items:
        state = json.loads(str(item[key]))
        detection_id = int(state["detection_id"])
        if db.detections.row(detection_id) is None:
            continue
        photo_id = int(str(item["photo_id"]))
        with acquire_photo_mask(photo_id):
            _apply(db, photo_id, detection_id, state["verdict"])


def refresh_pipeline_cache(db: Database, db_path: str, photo_ids: Sequence[int]) -> bool:
    """Rewrite the photos' cached evidence; True unless the refresh failed.

    The catalog write already committed, so a cache that cannot be rewritten
    is reported (the caller tells the user Process Review may be stale)
    rather than failing the verdict.
    """
    from pipeline import refresh_cache_predictions_for_photos

    try:
        refresh_cache_predictions_for_photos(
            db, os.path.dirname(db_path), db.require_workspace_id(), list(photo_ids),
        )
    except (OSError, ValueError, KeyError, TypeError, RuntimeError):
        import logging

        logging.getLogger(__name__).exception(
            "Could not refresh pipeline cache evidence after a subject verdict on %s", list(photo_ids),
        )
        return False
    return True
