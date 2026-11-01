"""Bounded selected-photo analysis recovery; never writes photographer decisions."""

from __future__ import annotations

import io
import json
import logging
import math
import os
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Protocol

from config import config_value
from services.service_types import JobDict, JsonDict
from subject_readiness import REQUIRED_QUALITY_FIELDS, subject_analysis_readiness

if TYPE_CHECKING:
    from db import Database

log = logging.getLogger(__name__)
MAX_SELECTED_PHOTOS = 200


class RetryContext(Protocol):
    def thread_db(self) -> Database: ...
    def checkpoint(self, job: JobDict) -> None: ...


def selected_ids(db: Database, values: object) -> list[int]:
    if (not isinstance(values, list) or not 1 <= len(values) <= MAX_SELECTED_PHOTOS
            or any(type(pid) is not int or not 0 < pid <= 2**63 - 1 for pid in values)):
        raise ValueError(f"Choose 1–{MAX_SELECTED_PHOTOS} positive integer photo IDs")
    ids = list(dict.fromkeys(values))
    if db.active_workspace_id is None:
        raise ValueError("Choose an active workspace before retrying analysis")
    visible = db.pipeline_features.present_workspace_photo_ids(db.require_workspace_id(), ids)
    if set(ids) != visible:
        raise ValueError("Every selected photo must be visible in the active workspace")
    return ids


def _current(db: Database, ids: list[int], effective: JsonDict) -> list[JsonDict]:
    from pipeline import load_photo_features
    context = _context_ids(db, ids, effective)
    wanted = set(ids)
    return [photo for photo in load_photo_features(db, photo_ids=context, config=effective, effective_config=effective)
            if photo["id"] in wanted]


def _current_photo(db: Database, pid: int, effective: JsonDict) -> JsonDict:
    photos = _current(db, [pid], effective)
    if not photos:
        raise ValueError("Photo is no longer visible in this workspace")
    return photos[0]


def _context_ids(db: Database, ids: list[int], effective: JsonDict) -> list[int]:
    """Read neighboring rescue anchors without inferring on unselected photos.

    The feature loader rescues a selected frame from its neighbors in two ways:
    ordinary weak-box runs at the workspace's weak floor, and full-image
    dropout runs around merely positive boxes (``matching_full_image_runs``).
    Both kinds of anchor must be loaded, or the same frame reads as "no
    qualifying subject" in a small request and "segmentation incomplete" in
    the request that happens to include its encounter.
    """
    from services.detection_store import get_detections_for_photos
    from weak_detections import FULL_IMAGE_RESCUE_WEAK_FLOOR, rescue_candidate_runs
    options = effective.get("pipeline", {})
    if not options.get("weak_detection_rescue_enabled", True):
        return ids
    with db.transaction(join=True):
        db.pipeline_features.stage_scope_photo_ids(ids)
    rows = db.pipeline_features.selected_folder_context(db.require_workspace_id())
    weak_confidence = config_value(options, "weak_detection_confidence", section="pipeline")
    detections = get_detections_for_photos(db, [row["id"] for row in rows],
        min_conf=min(weak_confidence, FULL_IMAGE_RESCUE_WEAK_FLOOR), detector_model="megadetector-v6")
    runs = rescue_candidate_runs(rows, detections, detector_confidence=config_value(effective, "detector_confidence"),
        weak_confidence=weak_confidence, max_gap=config_value(options, "burst_time_gap", section="pipeline"))
    context = set(ids)
    for run in runs:
        if context.intersection(run["photo_ids"]):
            context.update([*run["photo_ids"], run["left_photo_id"], run["right_photo_id"]])
    return sorted(context)


def required_stages(photo: JsonDict) -> list[str]:
    readiness = subject_analysis_readiness(photo)
    if readiness["eligible"]:
        return []
    reasons = readiness["reasons"]
    detection = any(reason.startswith("detection_") or reason == "no_qualifying_subject" for reason in reasons)
    segmentation = photo.get("analysis_provenance", {}).get("segmentation", {})
    if not detection and segmentation.get("status") == "complete" and segmentation.get("current") is True:
        return ["quality"]
    return (["detection"] if detection else []) + ["segmentation", "quality"]


def explain_selected(db: Database, photo_ids: list[int], effective: JsonDict) -> JsonDict:
    ids = selected_ids(db, photo_ids)
    previous = _last_attempts(db, ids)
    photos = []
    for photo in _current(db, ids, effective):
        readiness = subject_analysis_readiness(photo)
        reasons = readiness["reasons"]
        if readiness["eligible"]:
            explanation = "Subject analysis is complete for current inputs and settings."
        elif "no_qualifying_subject" in reasons:
            explanation = ("Detection completed without a qualifying animal at the current threshold. "
                           "Retry will run detection again; it may still leave this photo in REVIEW.")
        elif any(reason.startswith("detection_") for reason in reasons):
            explanation = "Subject detection is unavailable or stale. The historical cause may be unknown."
        elif any(reason.startswith("missing_subject_") or reason == "segmentation_incomplete" for reason in reasons):
            explanation = "Subject analysis is incomplete — segmentation or quality measurements are missing. Historical cause unknown."
        else:
            explanation = "Subject analysis is incomplete — current segmentation or quality measurements are unavailable."
        attempt = previous.get(photo["id"])
        if not readiness["eligible"] and attempt and attempt.get("status") == "failed":
            explanation += f" Last retry failed during {attempt['stage']}: {attempt['error']}"
        photos.append({"photo_id": photo["id"], "filename": photo["filename"], **readiness,
                       "explanation": explanation, "required_stages": required_stages(photo),
                       "last_attempt": attempt, "provenance": photo.get("analysis_provenance", {})})
    return {"workspace_id": db.require_workspace_id(), "photos": photos,
            "count": len(photos), "max_selected_photos": MAX_SELECTED_PHOTOS}


def _last_attempts(db: Database, ids: list[int]) -> dict[int, JsonDict]:
    attempts = {}
    for row in db.job_history.analysis_retry_results(db.require_workspace_id()):
        try:
            result = json.loads(row["result"] or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        for outcome in result.get("photos", []):
            pid = outcome.get("photo_id")
            if pid in ids and pid not in attempts:
                attempts[pid] = {**outcome, "job_id": row["id"]}
    return attempts


def _source(db: Database, pid: int) -> tuple[JsonDict, str, tuple[int, int]]:
    from subject_ranking import source_path
    photo = db.photos.get(pid, verify_workspace=True)
    if photo is None:
        raise ValueError("Photo is no longer visible in this workspace")
    path = source_path(db, photo)
    stat = os.stat(path)
    return dict(photo), path, (stat.st_size, stat.st_mtime_ns)


def _unchanged(db: Database, pid: int, path: str, identity: tuple[int, int]) -> None:
    _, current_path, current_identity = _source(db, pid)
    if os.path.realpath(current_path) != os.path.realpath(path) or current_identity != identity:
        raise ValueError("Photo source changed during analysis; retry with the current source")


@contextmanager
def _photo_lock(pid: int, checkpoint: Callable[[], None]) -> Iterator[None]:
    from pipeline_locks import acquire_photo_mask
    lock = acquire_photo_mask(pid)
    while not lock.acquire(timeout=.2):
        checkpoint()
    try:
        checkpoint()
        yield
    finally:
        lock.release()


def retry_detection(db: Database, pid: int, checkpoint: Callable[[], None],
                    *, on_published: Callable[[], None] | None = None) -> None:
    from computation_cache import megadetector_runtime_fingerprint, source_input
    from detector import detect_animals, ensure_megadetector_weights
    from services.detection_store import write_detection_batch

    _, path, identity = _source(db, pid)
    checkpoint()
    runtime = megadetector_runtime_fingerprint()
    if runtime is None:
        ensure_megadetector_weights()  # first-use installation precedes the input snapshot
        checkpoint()
        runtime = megadetector_runtime_fingerprint()
    if runtime is None:
        raise ValueError("Detector runtime identity is unavailable after model preparation")
    checkpoint()
    detections = detect_animals(path)  # intentionally bypass completed/incomplete detector caches
    if detections is None:
        raise ValueError("Subject detection could not decode or analyze this photo")
    checkpoint()
    source = db.model_runs.detector_source_identity(pid)
    input_fp = None
    if source and source["file_hash"] and not source["companion_path"]:
        input_fp = source_input(source["file_hash"], "vireo-detector-source-v1")[1]
    with _photo_lock(pid, checkpoint):
        _unchanged(db, pid, path, identity)
        if megadetector_runtime_fingerprint() != runtime:
            raise ValueError("Detector runtime changed during analysis; retry with the current model")
        write_detection_batch(db, pid, "megadetector-v6", detections, runtime, input_fp)
        if on_published:
            on_published()
        # The normal writer protects reviewed detector outputs. Never force away
        # those decisions merely to make recovery appear successful.
        stored = db.model_runs.detector_run_identity(pid, "megadetector-v6")
        if (stored is None or stored["runtime_fingerprint"] != runtime
                or stored["input_fingerprint"] != input_fp):
            raise ValueError("A reviewed detector result was retained for different inputs; review its subject choice before retrying")


def retry_features(db: Database, pid: int, effective: JsonDict, cache_dir: str,
                   checkpoint: Callable[[], None], *, quality_only: bool = False) -> None:
    from subject_analysis import analyze_detection

    photo, path, identity = _source(db, pid)
    checkpoint()
    with _photo_lock(pid, checkpoint):
        _unchanged(db, pid, path, identity)
        detection = _selected_detection(db, pid, effective)
        mask_path = db.masks_features.get_mask(pid, effective.get("pipeline", {}).get("sam2_variant") or "sam2-small")
        analysis = analyze_detection(db, photo, detection, effective, checkpoint, force=True,
                          existing_mask_path=mask_path["path"] if quality_only and mask_path else None,
                          defer_publication=True)
        if not isinstance(analysis, dict):
            raise ValueError("No current subject analysis was published")
        _unchanged(db, pid, path, identity)
        current = _selected_detection(db, pid, effective)
        if current is None or current["id"] != detection["id"]:
            raise ValueError("Primary subject changed during analysis; retry the chosen subject")
        _publish_features(db, pid, detection, analysis, effective, cache_dir, checkpoint,
                          lambda: _unchanged(db, pid, path, identity),
                          existing_mask_path=mask_path["path"] if quality_only and mask_path else None)


def _selected_detection(db: Database, pid: int, effective: JsonDict) -> JsonDict:
    from subject_ranking import SubjectComparison, resolve_subject
    from subjects import retained
    detection, _ = resolve_subject(db, pid, SubjectComparison())
    if detection:
        return detection
    current = _current_photo(db, pid, effective)
    if current.get("subject_uncertain") and current.get("detection_box"):
        box = current["detection_box"]
        candidate = next((d for d in retained(db, pid, min_conf=0)
                          if all(d["box_" + key] == box[key] for key in "xywh")), None)
        if candidate:
            chosen, _ = resolve_subject(db, pid, SubjectComparison(subject_overrides={pid: candidate["id"]}))
            if chosen:
                return chosen
    raise ValueError("Current qualifying subject is no longer available; retry after reviewing the subject")


def _publish_features(db: Database, pid: int, detection: JsonDict, analysis: JsonDict,
                      effective: JsonDict, cache_dir: str, checkpoint: Callable[[], None],
                      validate_source: Callable[[], None], *, existing_mask_path: str | None = None) -> None:
    import numpy as np
    from masking import save_mask
    from PIL import Image
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    from pipeline_job import _StagedMaskFile
    from repositories.masks_features import MaskQuality, PipelineFeatures
    from subject_analysis import publish_analysis
    from weak_detections import rescue_floor

    features = analysis["features"]
    if any(not isinstance(features.get(field), (int, float)) or isinstance(features.get(field), bool)
           or not math.isfinite(features[field]) for field in REQUIRED_QUALITY_FIELDS):
        raise ValueError("Required quality measurements are still incomplete")
    variant = effective.get("pipeline", {}).get("sam2_variant") or "sam2-small"
    stage = None
    if existing_mask_path is None:
        with Image.open(io.BytesIO(analysis["mask_png"])) as image:
            mask = np.asarray(image).astype(bool)
        previous = db.masks_features.get_mask(pid, variant)
        stage = _StagedMaskFile.create(mask, os.path.join(cache_dir, "masks"), pid, variant, save_mask,
                                      previous_path=previous["path"] if previous else None)
    mask_path = stage.final_path if stage else existing_mask_path
    if mask_path is None:
        raise ValueError("Current subject mask is unavailable")
    try:
        checkpoint()
        if stage:
            stage.install()
        db.begin_immediate()
        try:
            selected_ids(db, [pid])
            validate_source()
            if _selected_detection(db, pid, effective)["id"] != detection["id"]:
                raise ValueError("Primary subject changed before publication; retry the chosen subject")
            if existing_mask_path:
                previous = db.masks_features.get_mask(pid, variant)
                if previous is None or previous["path"] != existing_mask_path:
                    raise ValueError("Subject mask changed before quality publication; retry the current mask")
            db.masks_features.upsert_mask(pid, variant, mask_path, detection["detector_model"],
                tuple(detection["box_" + key] for key in "xywh"),
                MaskQuality(**{field: features.get(field) for field in MaskQuality.__dataclass_fields__}))
            current = _current_photo(db, pid, effective)
            weak_floor = (rescue_floor(current, config_value(effective.get("pipeline", {}), "weak_detection_confidence", section="pipeline"))
                          if current.get("subject_uncertain") else None)
            db.masks_features.set_active_variant(pid, variant, config_value(effective, "detector_confidence"), weak_rescue_min_conf=weak_floor)
            db.masks_features.update_pipeline_features(pid, PipelineFeatures(
                **{field: features.get(field) for field in ("eye_x", "eye_y", "eye_conf", "eye_tenengrad")},
                eye_kp_fingerprint=(EYE_KP_FINGERPRINT_VERSION
                    if features.get("eye_status") in ("measured", "not_located") else None)))
            if existing_mask_path is None:
                db.masks_features.update_embeddings(pid, None, None, variant=None)
            photo = db.photos.get(pid, verify_workspace=True)
            publish_analysis(db, photo, detection, analysis, effective)
            db.commit_with_retry()
        except (OSError, ValueError, RuntimeError, sqlite3.Error):
            db.rollback()
            raise
    finally:
        if db.in_transaction:
            db.rollback()
        if stage:
            current_mask = db.masks_features.get_mask(pid, variant)
            if current_mask is not None and current_mask["path"] == stage.final_path:
                stage.finish()
            else:
                stage.restore()


def run_retry(ctx: RetryContext, job: JobDict, photo_ids: list[int], effective: JsonDict, cache_dir: str,
              *, results_dir: str | None = None) -> JsonDict:
    """Execute only the immutable selection; partial failures stay visible and retryable."""
    from resource_ledger import bind_resource_cancel_check

    def cancellation_probe() -> bool:
        ctx.checkpoint(job)
        return False

    with bind_resource_cancel_check(cancellation_probe):
        return _run_retry(ctx, job, photo_ids, effective, cache_dir,
                          results_dir if results_dir is not None else cache_dir)


def _run_retry(ctx: RetryContext, job: JobDict, photo_ids: list[int], effective: JsonDict, cache_dir: str,
               results_dir: str) -> JsonDict:
    db = ctx.thread_db()
    result: JsonDict = {"ok": True, "photos": [], "errors": [], "requested_photo_ids": photo_ids,
                       "review_refresh": {"status": "not_needed", "photo_ids": []}}
    published: set[int] = set()
    refreshed = False
    job["result"] = result
    job["progress"]["total"] = len(photo_ids)
    def checkpoint() -> None:
        ctx.checkpoint(job)
    try:
        selected_ids(db, photo_ids)
        for index, pid in enumerate(photo_ids):
            checkpoint()
            stage = "readiness"
            try:
                selected_ids(db, [pid])
                photo = _current_photo(db, pid, effective)
                stages = required_stages(photo)
                job["progress"].update(current=index, current_file=photo["filename"], phase="Checking analysis")
                ran = []
                if "detection" in stages:
                    stage = "detection"
                    job["progress"]["phase"] = "Retrying subject detection"
                    retry_detection(db, pid, checkpoint, on_published=lambda photo_id=pid: published.add(photo_id))
                    ran.append("detection")
                    photo = _current_photo(db, pid, effective)
                    stages = required_stages(photo)
                no_subject = "no_qualifying_subject" in subject_analysis_readiness(photo)["reasons"]
                if stages and not no_subject:
                    stage = "quality" if stages == ["quality"] else "segmentation and quality"
                    job["progress"]["phase"] = f"Retrying {stage} measurements"
                    retry_features(db, pid, effective, cache_dir, checkpoint, quality_only=stages == ["quality"])
                    published.add(pid)
                    ran.extend(stage_name for stage_name in stages if stage_name != "detection")
                after = subject_analysis_readiness(_current_photo(db, pid, effective))
                result["photos"].append({"photo_id": pid, "status": "complete" if after["eligible"] else "review",
                                         "stages_run": ran, "analysis": after})
            except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
                checkpoint()  # resource cancellation must unwind as cancellation, not a photo failure
                log.exception("Selected analysis retry failed for photo %s during %s", pid, stage)
                outcome = {"photo_id": pid, "status": "failed", "stage": stage, "error": str(exc)}
                result["photos"].append(outcome)
                result["errors"].append(outcome)
                result["ok"] = False
            job["progress"]["current"] = index + 1
        job["progress"]["phase"] = "Refreshing affected review suggestions"
        try:
            _refresh_review(db, effective, results_dir, photo_ids, checkpoint)
        except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
            result["ok"] = False
            result["review_refresh"] = {"status": "failed", "photo_ids": photo_ids, "error": str(error)}
            raise
        refreshed = True
        result["review_refresh"] = {"status": "complete", "photo_ids": photo_ids}
        remaining = [pid for pid in photo_ids if db.photos.get(pid, verify_workspace=True)]
        result["readiness"] = explain_selected(db, remaining, effective) if remaining else {"photos": [], "count": 0}
        return result
    finally:
        try:
            if published and not refreshed:
                _refresh_interrupted(db, effective, results_dir, sorted(published), result)
        finally:
            db.close()


def _refresh_interrupted(db: Database, effective: JsonDict, results_dir: str,
                         photo_ids: list[int], result: JsonDict) -> None:
    from pipeline import load_results_raw
    from pipeline_locks import acquire_workspace_regroup
    from services.review_refresh import queue_refresh, refresh_pending

    state: JsonDict = {"status": "pending", "photo_ids": photo_ids}
    result["review_refresh"] = state  # visible even if durable marker publication fails
    try:
        queue_refresh(results_dir, db.require_workspace_id(), photo_ids)
    except (OSError, ValueError, RuntimeError) as error:
        state.update(status="failed", error=str(error))
        result["ok"] = False
        log.exception("Could not persist interrupted analysis review refresh")
        return
    lock = acquire_workspace_regroup(db.require_workspace_id())
    if not lock.acquire(blocking=False):
        state["status"] = "deferred"
        return
    try:
        data = load_results_raw(results_dir, db.require_workspace_id())
        db.begin_immediate()
        try:
            refresh_pending(db, data, results_dir, effective)
            db.commit()
            state["status"] = "complete"
        finally:
            if db.in_transaction:
                db.rollback()
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
        state.update(status="deferred", error=str(error))
        log.exception("Interrupted analysis review refresh will retry on next read")
    finally:
        lock.release()


def _refresh_review(db: Database, effective: JsonDict, results_dir: str, photo_ids: list[int],
                    checkpoint: Callable[[], None]) -> None:
    from pipeline import load_results_raw, save_results_raw
    from pipeline_locks import acquire_workspace_regroup
    from services.saved_review import refresh_selected_suggestions

    lock = acquire_workspace_regroup(db.require_workspace_id())
    while not lock.acquire(timeout=.2):
        checkpoint()
    try:
        checkpoint()
        data = load_results_raw(results_dir, db.require_workspace_id())
        if data is not None:
            db.begin_immediate()
            try:
                refresh_selected_suggestions(db, data, photo_ids, effective)
                save_results_raw(data, results_dir, db.require_workspace_id())
                db.commit()
            finally:
                if db.in_transaction:
                    db.rollback()
    finally:
        lock.release()
