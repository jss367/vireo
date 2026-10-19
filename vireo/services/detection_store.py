"""Detection reads and writes that compose more than the detections table.

Writes pair a detection batch with its ``detector_runs`` row (and the
model-runs pin check), or re-sync each touched photo's primary subject after a
delete. Reads resolve the workspace-effective detector or classifier floor
(through ``Database.get_effective_config``) when the caller passes none; the
detections table itself is global, so only the floor is workspace-specific.
The single-table writes (``db.detections.save``, ...) are reached through the
``db.detections`` accessor directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import config as cfg

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable, Mapping, Sequence

    from db import Database


def write_detection_batch(
    db: Database,
    photo_id: int,
    detector_model: str | None,
    detections: Sequence[Mapping[str, Any]],
    runtime_fingerprint: str = "legacy",
    input_fingerprint: str | None = None,
    force_runtime_replace: bool = False,
) -> list[int]:
    """Atomically replace detections and record the detector_runs row.

    Combines ``db.detections.save`` and ``record_detector_run`` under a single
    transaction so readers never observe a torn state where one table
    reflects the new run and the other still reflects the old one.
    Callers in the detection write path (e.g. `_detect_batch`) should
    prefer this over invoking the two methods separately.

    Args:
        photo_id: the photo
        detector_model: required, e.g. "megadetector-v6"
        detections: list of dicts {box: {x,y,w,h}, confidence, category}.
            An empty list records an empty-scene run (box_count=0) and
            clears any prior detection rows for the same (photo, model).
    Returns:
        list of new detection IDs (empty if detections was empty).
    """
    if detector_model is None:
        raise ValueError("detector_model is required")
    # The pin check comes from the model-runs domain, so a patched
    # ``model_runs.detector_run_is_pinned`` still applies inside the transaction.
    return db.detections.write_batch(
        photo_id,
        detector_model,
        detections,
        runtime_fingerprint,
        input_fingerprint,
        force_runtime_replace,
        is_pinned=db.model_runs.detector_run_is_pinned,
    )


def get_detections(
    db: Database, photo_id: int, min_conf: float | None = None,
    detector_model: str | None = None,
) -> list[sqlite3.Row]:
    """Return all boxes for a photo above `min_conf`, globally.

    The detections table is global (no workspace_id). Threshold filtering
    happens at read time so raw boxes stay cached across workspaces.

    Args:
        photo_id: the photo
        min_conf: confidence floor. ``None`` pulls ``detector_confidence``
            from the active workspace's effective config (default 0.2).
            ``0`` returns raw rows with no filtering.
        detector_model: optional — filter to a single detector model.
    """
    floor: float = (
        min_conf if min_conf is not None
        else db.get_effective_config(cfg.load()).get("detector_confidence", 0.2)
    )
    return db.detections.get(photo_id, floor, detector_model)


def get_detections_for_photos(
    db: Database, photo_ids: Iterable[int], min_conf: float | None = None,
    detector_model: str | None = None,
) -> dict[int, list[dict[str, Any]]]:
    """Return {photo_id: [det_dict, ...]} for a batch of photos.

    Each det_dict has keys: id, x, y, w, h, confidence, category, and
    detector_model. Lists put the chosen primary first, then quality and
    detector confidence. The detections
    table is global — threshold filtering happens at read time. Photos
    with no detections above ``min_conf`` are omitted from the result.

    Args:
        photo_ids: iterable of photo ids
        min_conf: confidence floor. ``None`` resolves to the active
            workspace's effective ``detector_confidence`` (default 0.2).
            ``0`` returns raw rows.
        detector_model: optional — filter to a single detector model.
    """
    if not photo_ids:
        return {}
    floor: float = (
        min_conf if min_conf is not None
        else db.get_effective_config(cfg.load()).get("detector_confidence", 0.2)
    )
    return db.detections.get_for_photos(photo_ids, floor, detector_model)


def get_predictions_for_detection(
    db: Database, detection_id: int, min_classifier_conf: float | None = None,
    classifier_model: str | None = None, labels_fingerprint: str | None = None,
) -> list[sqlite3.Row]:
    """Return cached classifier predictions for a single detection.

    Reads the global ``predictions`` table. Review status (accepted /
    rejected / pending) lives in ``prediction_review`` and is joined in
    by callers that need workspace-scoped review state.

    Args:
        detection_id: the detection whose predictions to fetch.
        min_classifier_conf: confidence floor. ``None`` resolves to the
            active workspace's effective ``classifier_confidence`` (0.0
            if unset). ``0`` returns all rows.
        classifier_model: optional — filter to a single classifier model.
        labels_fingerprint: optional — filter to predictions produced
            against a specific label set.
    """
    floor: float = (
        min_classifier_conf if min_classifier_conf is not None
        else db.get_effective_config(cfg.load()).get("classifier_confidence", 0.0)
    )
    return db.detections.get_predictions(
        detection_id, floor, classifier_model, labels_fingerprint,
    )


def delete_detections_by_ids(db: Database, detection_ids: Iterable[int]) -> None:
    """Delete specific detection rows by primary key.

    Cascades to predictions via the FK constraint.  Does nothing if
    the list is empty.  Used by reclassify to purge only the stale
    rows for photos that have just been re-detected, without touching
    detection rows that belong to models not included in the current run.

    IDs are deleted in chunks of at most 900 to stay safely under
    SQLite's default bound-parameter limit (SQLITE_LIMIT_VARIABLE_NUMBER,
    typically 999 in production builds).
    """
    if not detection_ids:
        return
    repo = db.detections
    affected_subject_photos = repo.delete_by_ids(detection_ids)
    if affected_subject_photos:
        from subjects import sync_primary
        for photo_id in affected_subject_photos:
            sync_primary(db, photo_id)
    repo.commit()


def upsert_photo_embedding(
    db: Database, photo_id: int, model: str, embedding_bytes: bytes,
    variant: str = '', verify_workspace: bool = False,
) -> None:
    """Store an embedding blob for (photo_id, model, variant).

    Replaces any existing row with the same primary key. ``model`` is
    required because the storage philosophy keeps a per-model cache; a
    missing model name has no key in the table.

    Args:
        verify_workspace: when True, raises ValueError if the photo is
            not in the active workspace. Defaults to False because this
            method is typically called from background classify jobs that
            already iterate only over workspace-scoped photos.
    """
    if verify_workspace:
        db._verify_photo_in_workspace(photo_id)
    db.masks_features.upsert_embedding(
        photo_id, model, embedding_bytes, variant,
    )
