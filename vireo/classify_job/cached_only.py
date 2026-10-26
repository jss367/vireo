"""Finalizing a classify run from cached predictions alone.

Used when every photo's classifier runs are already cached (a bundle import
or an earlier run), so no model has to load.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, cast

from classify_job.cache_coverage import _cache_run_filter
from classify_job.common import (
    _load_job_taxonomy,
    _photo_timestamp,
    taxonomy_step_summary,
    taxonomy_step_warning,
    with_taxonomy_notice,
)
from classify_job.label_source import _describe_cached_label_source, describe_label_source
from classify_job.store_predictions import _cached_prediction_taxonomy, _store_grouped_predictions
from config import config_value
from services import detection_store

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable, Mapping, Sequence

    from classify_job.classify_types import DetectionMap, LabelSetMeta, ModelSpec, RawResult
    from classify_job.common import ClassifyParams
    from db import Database
    from jobs import JobRunner
    from services.service_types import JobDict, JsonDict
    from taxonomy import Taxonomy


def _finalize_remaining_steps(
    runner: JobRunner, job_id: str, step_ids: Iterable[str], status: str, summary: str,
) -> None:
    """Flip not-yet-run step rows to a terminal status before an early return.

    _persist_job stores whatever statuses are in the step tree at job end;
    a row left "pending" with no finished_at renders as an indeterminate
    spinner forever on the jobs page. Early-return paths must call this for
    every step they are about to skip.
    """
    for step_id in step_ids:
        runner.update_step(job_id, step_id, status=status, summary=summary)


def _cached_only_reused_count(
    db: Database, photo_ids: Sequence[int], classifier_model: str | None,
    labels_fingerprint: str | None, detector_confidence: float,
) -> int:
    """Count covered cached work, including measured no-match outcomes.

    The caller's coverage/identity gate already proved these rows share the
    requested identity. Read that same stored-output gate again rather than
    inventing a prediction for a classifier that matched nothing.
    """
    from db import _chunks

    run_filter = _cache_run_filter(
        db, photo_ids, classifier_model, labels_fingerprint, None, None, "no-tax",
    )
    return sum(
        db.model_runs.cache_coverage_counts(chunk, run_filter, detector_confidence)[1]
        for chunk in _chunks(photo_ids)
    )

def _finalize_cached_only(
    thread_db: Database, runner: JobRunner, job: JobDict, params: ClassifyParams, photos: list[sqlite3.Row],
    classifier_model: str | None, labels_fingerprint: str | None,
    peek_model: ModelSpec | None = None, peek_labels: list[str] | None = None, peek_use_tol: bool = False,
    peek_label_metas: list[LabelSetMeta] | None = None, peek_succeeded: bool = False,
    detector_confidence: float = 0.0,
) -> JsonDict:
    """Reconcile imported classifier results without loading a model.

    ``_all_photos_cache_satisfied`` proves every detection already has a
    matching classifier_run row, so no inference is needed. But those
    rows still carry ``category='new'`` from the materialize path and
    lack burst grouping metadata — the ordinary finalize path
    (``_store_grouped_predictions``) performs auto-match reconciliation
    against destination XMP/taxonomy and stamps group_id / vote counts.
    Skipping it silently leaves imported predictions unreconciled, which
    is why the earlier early-return "success" was actually a bug.

    ``peek_*`` — the labels/mode/metadata resolved earlier in
    ``run_classify_job`` before this shortcut fired. ``peek_succeeded`` is
    True when ``_load_labels`` actually returned (as opposed to raising and
    the caller falling back to model-only cache filtering). Used only to
    publish the ``label_source`` line on the classify step, so a re-run
    against a fully cached collection still records which lists produced
    its predictions — otherwise the classify history row is unlabeled,
    exactly the state this feature set out to eliminate.

    Returns the count dict ``run_classify_job`` returns to the runner.
    """

    total = len(photos)

    runner.update_step(job["id"], "load_taxonomy", status="running")
    tax = _load_job_taxonomy(job)
    runner.update_step(
        job["id"], "load_taxonomy",
        status="completed",
        summary=taxonomy_step_summary(job, tax),
        **taxonomy_step_warning(job),
    )
    _finalize_remaining_steps(
        runner, job["id"], ["load_model", "detect"],
        status="completed", summary="Reused cached results",
    )

    # Name the label space on the classify step BEFORE the reconcile pass.
    # Without this, a cached-only run finishes with no label_source line and
    # its history row looks the same as a pre-feature run.
    label_source_text = _cached_only_label_source(
        thread_db, params, photos, classifier_model, labels_fingerprint,
        peek_model, peek_labels, peek_use_tol, peek_label_metas,
        peek_succeeded, detector_confidence,
    )
    if label_source_text:
        runner.update_step(
            job["id"], "classify", label_source=label_source_text,
        )

    folders = {f["id"]: f["path"] for f in thread_db.folders.tree()}
    photo_ids = [p["id"] for p in photos]
    detection_map = _cached_only_detection_map(thread_db, photo_ids)

    runner.update_step(job["id"], "classify", status="running")

    raw_results, resolved_model, resolved_fingerprint = (
        _cached_only_raw_results(
            thread_db, photos, folders, detection_map,
            classifier_model, labels_fingerprint, detector_confidence,
        )
    )
    reused_results = _cached_only_reused_count(
        thread_db, photo_ids, classifier_model, labels_fingerprint, detector_confidence,
    )
    runner.update_step(
        job["id"], "classify",
        status="completed",
        summary=f"{reused_results} cached" if reused_results else "No cached results",
    )

    group_result = _finalize_cached_results(
        thread_db, runner, job, params, tax, total,
        raw_results, resolved_model, resolved_fingerprint,
    )
    return with_taxonomy_notice(job, {
        "total": total,
        "predictions_stored": group_result["predictions_stored"],
        "burst_groups": group_result["burst_groups"],
        "already_classified": reused_results,
        "already_labeled": group_result["already_labeled"],
        "detected": 0,
        "failed": 0,
    })


def _finalize_cached_results(
    thread_db: Database, runner: JobRunner, job: JobDict, params: ClassifyParams, tax: Taxonomy | None, total: int,
    raw_results: list[RawResult], resolved_model: str | None, resolved_fingerprint: str | None,
) -> dict[str, int]:
    """Run the finalize step over reused results; returns the group result."""
    runner.update_step(job["id"], "finalize", status="running")
    runner.push_event(
        job["id"], "progress",
        {
            "current": total,
            "total": total,
            "current_file": "Grouping bursts and computing consensus...",
            "rate": 0,
            "phase": "Finalizing results",
        },
    )
    group_result = _store_grouped_predictions(
        raw_results=raw_results,
        job_id=job["id"],
        # ``resolved_model`` is None only when no identity was resolved,
        # which leaves ``raw_results`` empty and stores nothing.
        model_name=cast("str", resolved_model),
        grouping_window=params.grouping_window,
        similarity_threshold=params.similarity_threshold,
        tax=tax,
        db=thread_db,
        labels_fingerprint=resolved_fingerprint or "legacy",
    )
    runner.update_step(
        job["id"], "finalize",
        status="completed", summary=_finalize_summary(group_result),
    )
    return group_result


def _cached_only_label_source(
    thread_db: Database, params: ClassifyParams, photos: list[sqlite3.Row],
    classifier_model: str | None, labels_fingerprint: str | None, peek_model: ModelSpec | None,
    peek_labels: list[str] | None, peek_use_tol: bool, peek_label_metas: list[LabelSetMeta] | None,
    peek_succeeded: bool, detector_confidence: float,
) -> str | None:
    """The ``label_source`` line for a cached-only classify step, or None.

    ``class_count`` is unavailable here — no classifier is constructed — so
    the two count-dependent branches (Tree of Life exact count, timm exact
    count) degrade to their generic wording; that is acceptable because the
    regional-list branch carries its count in ``labels``.
    """
    if peek_succeeded:
        peek_model_type = (peek_model or {}).get("model_type", "bioclip")
        return describe_label_source(
            params, thread_db,
            labels=peek_labels,
            use_tol=peek_use_tol,
            model_type=peek_model_type,
            label_metas=peek_label_metas,
        )
    # ``_load_labels`` raised (e.g. an imported cache on a fresh install
    # whose selected labels or Tree-of-Life artifacts aren't present),
    # so we don't have a live label space to name. The identity gate in
    # ``_all_photos_cache_satisfied`` guarantees a single (model, fp)
    # pair covers every reused row — recover its provenance from
    # ``labels_fingerprints`` so the classify history row still says
    # which list produced the predictions instead of being unlabeled,
    # which was exactly the pre-feature state.
    return _describe_cached_label_source(
        thread_db,
        [p["id"] for p in photos],
        classifier_model=classifier_model,
        labels_fingerprint=labels_fingerprint,
        detector_confidence=detector_confidence,
    )


def _cached_only_detection_map(thread_db: Database, photo_ids: list[int]) -> DetectionMap:
    """The classifiable detections a cached-only finalize reconciles.

    Returns ``{photo_id: [detection, ...]}``: animal detections at the
    workspace threshold plus every synthetic full-image anchor.
    """
    # Apply the workspace-effective detector_confidence so imported bundles
    # from a machine with a lower threshold don't surface subjects the
    # destination workspace would hide.  The normal (non-cache) classify path
    # filters detections with the same threshold before grouping/review.
    import config as cfg
    effective_cfg = thread_db.get_effective_config(cfg.load())
    det_conf_threshold = config_value(effective_cfg, "detector_confidence")
    detection_map = detection_store.get_detections_for_photos(
        thread_db, photo_ids, min_conf=det_conf_threshold,
    )
    detection_map = {
        photo_id: [
            detection for detection in detections
            if detection.get("detector_model") != "full-image"
            and (detection.get("category") or "animal") == "animal"
        ]
        for photo_id, detections in detection_map.items()
    }
    # Synthetic full-image anchors intentionally have confidence 0, so the
    # positive MegaDetector threshold above hides them. They are nevertheless
    # classifier inputs and must participate in cached-only reconciliation.
    # Fetch them independently at min_conf=0 and merge them back.
    full_image_map = detection_store.get_detections_for_photos(
        thread_db, photo_ids, min_conf=0, detector_model="full-image",
    )
    for photo_id, detections in full_image_map.items():
        existing = detection_map.setdefault(photo_id, [])
        existing_ids = {detection["id"] for detection in existing}
        existing.extend(
            detection for detection in detections
            if detection["id"] not in existing_ids
        )
    return detection_map


def _cached_only_identity(
    thread_db: Database, photo_ids: Sequence[int], classifier_model: str | None,
    labels_fingerprint: str | None, detector_confidence: float,
) -> tuple[str, str] | None:
    """Resolve the single identity accepted by the caller's coverage gate.

    Predictions outside that gate (for example RAW recipes) cannot choose
    the identity for normal-image reconstruction, even if they score higher.
    A vanished or newly mixed identity safely produces no reconstructed rows.
    """
    from db import _chunks

    run_filter = _cache_run_filter(
        thread_db, photo_ids, classifier_model, labels_fingerprint, None, None, "no-tax",
    )
    identities = set()
    for chunk in _chunks(photo_ids):
        identities.update(
            (row["m"], row["fp"])
            for row in thread_db.model_runs.covering_classifier_identities(
                chunk, run_filter, detector_confidence,
            )
        )
        if len(identities) > 1:
            return None
    return next(iter(identities)) if identities else None


def _cached_only_raw_results(
    thread_db: Database, photos: list[sqlite3.Row], folders: dict[int, str], detection_map: DetectionMap,
    classifier_model: str | None, labels_fingerprint: str | None, detector_confidence: float = 0.0,
) -> tuple[list[RawResult], str | None, str | None]:
    """Rebuild raw classifier results from each detection's cached top-1.

    Returns ``(raw_results, resolved_model, resolved_fingerprint)``; one
    raw result per reused prediction.
    """
    identity = _cached_only_identity(
        thread_db, [photo["id"] for photo in photos],
        classifier_model, labels_fingerprint, detector_confidence,
    )
    if identity is None:
        return [], classifier_model, labels_fingerprint
    resolved_model, resolved_fingerprint = identity
    raw_results = []
    for photo in photos:
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        timestamp = _photo_timestamp(photo)
        for detection in detection_map.get(photo["id"], []):
            cached = detection_store.get_predictions_for_detection(
                thread_db, detection["id"],
                classifier_model=resolved_model,
                labels_fingerprint=resolved_fingerprint,
                min_classifier_conf=0,
            )
            if not cached:
                continue
            top = cached[0]
            raw_results.append({
                "photo": photo,
                "detection_id": detection["id"],
                "folder_path": folder_path,
                "image_path": image_path,
                "prediction": top["species"],
                "confidence": top["confidence"],
                "timestamp": timestamp,
                "filename": photo["filename"],
                "embedding": None,
                "taxonomy": _cached_prediction_taxonomy(top),
                "alternatives": [],
                "_existing": True,
            })
    return raw_results, resolved_model, resolved_fingerprint


def _finalize_summary(group_result: Mapping[str, int]) -> str:
    """The finalize step's summary line for a grouped-store result."""
    finalize_parts = [f"{group_result['predictions_stored']} predictions"]
    if group_result["burst_groups"]:
        finalize_parts.append(f"{group_result['burst_groups']} burst groups")
    if group_result["already_labeled"]:
        finalize_parts.append(f"{group_result['already_labeled']} already labeled")
    return ", ".join(finalize_parts)
