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
from contextlib import contextmanager
from typing import TYPE_CHECKING

from pipeline_locks import acquire_photo_mask
from subjects import sync_primary

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator, Mapping, Sequence

    from db import Database
    from services.service_types import JsonDict

VERDICTS = frozenset({"not_animal"})
ACTION_TYPE = "subject_verdict"
ANALYSIS_BUSY_MESSAGE = "Subject analysis is running for this photo. Try again when it finishes."
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


@contextmanager
def _photo_free_of_analysis(photo_id: int) -> Iterator[None]:
    """Hold the photo's mask lock for a verdict write, or refuse at once (409).

    Subject analysis (``subjects.analyze_photo``, the features stage, the
    analysis retry) holds this lock across its own read-then-write of the
    photo's primary state, so a verdict written meanwhile could be projected
    over by a primary computed from the catalog as it stood before. Every
    verdict write therefore takes the lock too, and never waits for it: the
    analysis side takes the photo lock first and SQLite's writer lock second,
    while a verdict writer already holds the writer lock (``/api/undo`` and
    ``/api/redo`` replay under ``under_prediction_decision_lock``;
    ``set_subject_verdict`` opens its immediate transaction first), so
    blocking here would be the lock-order inversion that stalls both sides
    until the busy timeout. A held lock is a 409 the caller's transaction
    rolls back on, as ``PUT /api/photos/<id>/primary-subject`` answers.
    """
    lock = acquire_photo_mask(photo_id)
    if not lock.acquire(blocking=False):
        raise SubjectVerdictError(ANALYSIS_BUSY_MESSAGE, 409)
    try:
        yield
    finally:
        lock.release()


def _apply(db: Database, photo_id: int, detection_id: int, verdict: str | None) -> None:
    """Write the verdict and re-project the photo's primary subject (no commit)."""
    with _photo_free_of_analysis(photo_id):
        db.detections.set_subject_verdict(detection_id, verdict)
        sync_primary(db, photo_id)


def _locked_region(db: Database, detection_id: int) -> tuple[int, str | None]:
    """The region's photo id and current verdict, read under the writer lock.

    Called inside ``set_subject_verdict``'s immediate transaction, so the
    row, its workspace visibility and the prior verdict are the state the
    write is serialized after: two overlapping requests cannot both read the
    same prior verdict, and a detach committed after this check cannot slip
    in before the write. Raises ``SubjectVerdictError`` (404) when the
    detection is not in the active workspace and (400) for a full-image
    classification, which has no region to reject.
    """
    row = db.detections.row(detection_id)
    if row is None or db.photos.get(row["photo_id"], verify_workspace=True) is None:
        raise SubjectVerdictError("Detection not found in the active workspace", 404)
    if row["detector_model"] == "full-image":
        raise SubjectVerdictError("A full-image classification has no region to reject")
    return int(row["photo_id"]), row["subject_verdict"]


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
    # One unit of work, taken before anything is read: the detection row,
    # its workspace visibility and the prior verdict are read under the
    # writer lock (``_locked_region``), so the history row's old value is
    # the verdict this write actually replaces, and the verdict, the primary
    # re-projection and the history row commit together (record_edit's own
    # commit and prune run inside the unit, where a legacy commit is a
    # no-op). The writer lock comes first and the photo lock inside
    # ``_apply`` second, the one order every verdict writer uses; every
    # ``SubjectVerdictError`` raised inside rolls the unit back.
    with db.transaction(immediate=True):
        photo_id, previous = _locked_region(db, detection_id)
        result: JsonDict = {"photo_id": photo_id, "detection_id": detection_id,
                            "verdict": verdict, "previous_verdict": previous,
                            "changed": previous != verdict}
        if previous == verdict:
            return result
        _apply(db, photo_id, detection_id, verdict)
        result["edit_id"] = db.record_edit(
            ACTION_TYPE, _description(verdict, source, reason), _state(detection_id, verdict),
            [{"photo_id": photo_id, "old_value": _state(detection_id, previous),
              "new_value": _state(detection_id, verdict)}],
            source=source, reason=reason,
        )
    return result


def replay_history(
    db: Database, items: Sequence[sqlite3.Row | Mapping[str, object]], *, undo: bool,
) -> None:
    """Put every item's region back to its recorded verdict (undo) or forward (redo).

    Runs inside the caller's transaction. A region the detector has since
    retired is skipped: there is nothing to restore and the photo's primary
    was already re-projected when the row went.

    Raises ``SubjectVerdictError`` (409) when subject analysis holds one of
    the photos, so the caller's transaction rolls back and nothing is
    replayed; it never waits for that lock (``_photo_free_of_analysis``).
    """
    key = "old_value" if undo else "new_value"
    for item in items:
        state = json.loads(str(item[key]))
        detection_id = int(state["detection_id"])
        if db.detections.row(detection_id) is None:
            continue
        photo_id = int(str(item["photo_id"]))
        _apply(db, photo_id, detection_id, state["verdict"])


def refresh_pipeline_cache(db: Database, db_path: str, photo_ids: Sequence[int]) -> bool:
    """Rewrite every affected workspace's cached evidence; True unless any refresh failed.

    The verdict is catalog-wide, so a photo visible in multiple workspaces
    needs each workspace's cache rewritten with its own effective
    configuration or Process Review there would still list the rejected
    region's species. Each rewrite runs under ``acquire_workspace_regroup``
    so a regroup, detach, or species confirmation on the same workspace
    cannot interleave with the load-modify-dump and silently publish a
    stale snapshot. The catalog write already committed, so a cache that
    cannot be rewritten is reported (the caller tells the user Process
    Review may be stale) rather than failing the verdict.
    """
    from pipeline import refresh_cache_predictions_for_photos
    from pipeline_locks import acquire_workspace_regroup

    cache_dir = os.path.dirname(db_path)
    photos_by_workspace: dict[int, list[int]] = {}
    for pid in dict.fromkeys(photo_ids):
        for ws_id in db.photo_visibility.workspace_ids_for_photo(pid):
            photos_by_workspace.setdefault(ws_id, []).append(pid)

    original_workspace = db.active_workspace_id
    ok = True
    try:
        for ws_id in sorted(photos_by_workspace):
            ws_photos = photos_by_workspace[ws_id]
            try:
                # ``load_photo_features`` reads ``db.require_workspace_id``
                # and the active workspace's effective config, so switch to
                # each affected workspace before loading its features.
                db.set_active_workspace(ws_id)
                with acquire_workspace_regroup(ws_id):
                    refresh_cache_predictions_for_photos(
                        db, cache_dir, ws_id, ws_photos,
                    )
            except (OSError, ValueError, KeyError, TypeError, RuntimeError):
                import logging

                logging.getLogger(__name__).exception(
                    "Could not refresh pipeline cache evidence after a subject "
                    "verdict on %s in workspace %s", ws_photos, ws_id,
                )
                ok = False
    finally:
        db.set_active_workspace(original_workspace)
    return ok
