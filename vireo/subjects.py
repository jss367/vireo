"""Cached, non-destructive analysis and primary selection for retained animals.

Subject scores compare boxes in the same image; they are not the encounter
percentile scores used by culling. Suggestions never modify an edit recipe.
"""
import hashlib
import json
import math
import os

import numpy as np
from services import detection_store

ANALYSIS_VERSION = 1
REVIEWER_MODEL = "reviewer-selected"


def retained(db, photo_id, min_conf=None, *, detector_model=None):
    detections = (dict(d) for d in detection_store.get_detections(
        db, photo_id, min_conf=min_conf, detector_model=detector_model))
    return [d for d in detections
            if d.get("category", "animal") == "animal" and d.get("detector_model") != "full-image"]


def suggested_crop(detection, padding=0.15):
    x, y, w, h = (float(detection[f"box_{k}"]) for k in "xywh")
    left, top = max(0, x - w * padding), max(0, y - h * padding)
    right, bottom = min(1, x + w * (1 + padding)), min(1, y + h * (1 + padding))
    if not all(math.isfinite(v) for v in (left, top, right, bottom)) or right <= left or bottom <= top:
        raise ValueError("Invalid subject box")
    return {"x": left, "y": top, "w": right - left, "h": bottom - top}


def analyze_image(image, detections):
    """Decode once, measure every box in the same coordinate space/resolution."""
    from scipy.ndimage import sobel
    from scoring import exposure_score

    rgb = np.asarray(image.convert("RGB"), dtype=np.float64) / 255
    gray = np.asarray(image.convert("L"), dtype=np.float64)
    gradients = sobel(gray, axis=0) ** 2 + sobel(gray, axis=1) ** 2
    linear = np.where(rgb <= 0.04045, rgb / 12.92, ((rgb + 0.055) / 1.055) ** 2.4)
    luminance = linear @ np.array([0.2126, 0.7152, 0.0722])
    height, width = gray.shape
    results = []
    for detection in detections:
        crop = suggested_crop(detection)
        x, y, w, h = (float(detection[f"box_{k}"]) for k in "xywh")
        left, top = max(0, int(x * width)), max(0, int(y * height))
        right, bottom = min(width, math.ceil((x + w) * width)), min(height, math.ceil((y + h) * height))
        if right <= left or bottom <= top:
            continue
        pixels = gray[top:bottom, left:right]
        sharpness = float(gradients[top:bottom, left:right].mean())
        high, low, median = float((pixels > 250).mean()), float((pixels < 5).mean()), float(np.median(pixels))
        area = (right - left) * (bottom - top) / (width * height)
        # Absolute measures avoid changing a subject's score when a sibling
        # detection is removed. This score is intentionally independent of
        # detector/classifier confidence and suggested exposure correction.
        focus = min(1, math.log1p(sharpness) / 12)
        visibility = 0.5 if min(x, y, 1 - x - w, 1 - y - h) <= 0.005 else 1
        score = 0.55 * focus + 0.25 * exposure_score(high, low, median) + 0.1 * min(1, math.sqrt(area) * 2) + 0.1 * visibility
        subject_luma = float(np.median(luminance[top:bottom, left:right]))
        ev = float(np.clip(math.log2(0.18 / max(subject_luma, 0.001)), -2, 2))
        # Protect bright pixels within the chosen subject when lifting it.
        if ev > 0:
            bright = float(np.quantile(linear[top:bottom, left:right], 0.99))
            ev = min(ev, max(0, math.log2(0.98 / max(bright, 0.001))))
        results.append({
            "detection_id": detection["id"], "crop": crop,
            "quality_score": round(score, 4), "exposure_ev": round(ev, 2),
            "features": {"subject_sharpness": sharpness, "subject_size": area,
                         "subject_clip_high": high, "subject_clip_low": low,
                         "subject_y_median": median},
        })
    return results


def analyze_photo(db, photo_id, image_path, *, min_conf=None, force=False, checkpoint=None, detection_id=None):
    """Backfill on cached detection runs too; failed/offline reads remain retryable."""
    from image_loader import load_image
    from pipeline_locks import acquire_photo_mask

    detections = retained(db, photo_id, min_conf)
    if detection_id is not None:
        detections = [d for d in detections if d["id"] == detection_id]
        if not detections:
            raise ValueError("Chosen subject is no longer available")
    if not detections:
        # Migrated catalogs have legacy subject-derived fields on ``photos``
        # without any ``photo_subject_state`` row. Triggering ``sync_primary``
        # only when that row exists made the intended cleanup a no-op there,
        # leaving stale mask/quality/DINO/eye state attached to a photo with
        # no detections. Check either signal (Codex r4056698678).
        stale = db.subjects.has_stale_primary_state(photo_id)
        if stale:
            with acquire_photo_mask(photo_id):
                if checkpoint:
                    checkpoint()
                sync_primary(db, photo_id, min_conf=min_conf)
                db.commit_with_retry()
        return 0
    stat = os.stat(image_path)
    source_key = hashlib.sha256(f"{os.path.realpath(image_path)}:{stat.st_size}:{stat.st_mtime_ns}:{ANALYSIS_VERSION}".encode()).hexdigest()
    cached = db.subjects.cached_detection_ids(photo_id, source_key)
    pending = [d for d in detections if force or d["id"] not in cached]
    if not pending:
        with acquire_photo_mask(photo_id):
            if checkpoint:
                checkpoint()
            sync_primary(db, photo_id, min_conf=min_conf)
            db.commit_with_retry()
        return 0
    image = load_image(image_path, max_size=1024)
    if image is None:
        raise ValueError("Could not load photo for subject analysis")
    try:
        results = analyze_image(image, pending)
    finally:
        image.close()
    with acquire_photo_mask(photo_id):
        if checkpoint:
            checkpoint()
        # The detector may have been replaced while the image was decoded.
        if detection_id is not None:
            current = next((d for d in retained(db, photo_id, min_conf) if d["id"] == detection_id), None)
            # Detection IDs quantize coordinates; an exact box edit may reuse an ID.
            if current is None or any(current["box_" + k] != detections[0]["box_" + k] for k in "xywh"):
                raise ValueError("Chosen subject changed during analysis; select it again")
        db.subjects.save_analyses(source_key, results)
        sync_primary(db, photo_id, min_conf=min_conf)
        db.commit_with_retry()
    return len(results)


def sync_primary(db, photo_id, *, min_conf=None, detector_model=None):
    """Project primary quality; clear stale subject-dependent outputs on a switch.

    Mask snapshots referenced by manual edits are immutable and untouched.
    Existing photo_masks remain cached, and prompt matching decides reuse.
    """
    detections = retained(db, photo_id, min_conf, detector_model=detector_model)
    if not detections:
        db.subjects.clear_primary_features(photo_id)
        db.subjects.clear_primary_state(photo_id)
        return
    primary = detections[0]
    analysis = db.subjects.analysis(primary["id"])
    previous = db.subjects.primary_state(photo_id)
    active_mask = db.subjects.active_mask(photo_id)
    mask_matches = active_mask and active_mask["detector_model"] == primary["detector_model"] and all(
        active_mask["prompt_" + k] == primary["box_" + k] for k in "xywh")
    changed = previous and previous["detection_id"] != primary["id"]
    if changed or (active_mask and not mask_matches):
        db.subjects.clear_primary_features(photo_id)
    if analysis:
        features = json.loads(analysis["features"])
        db.subjects.set_primary_quality(photo_id, analysis["quality_score"], features["subject_sharpness"])
        if changed or not mask_matches:
            db.subjects.set_primary_box_features(photo_id, features)
    db.subjects.set_primary_state(photo_id, primary["id"])


def select_primary(db, photo_id, detection_id):
    if detection_id is not None:
        if isinstance(detection_id, bool) or not isinstance(detection_id, int):
            raise ValueError("detection_id must be an integer or null for automatic selection")
        if not any(d["id"] == detection_id for d in retained(db, photo_id)):
            raise ValueError("Choose a retained animal detection from this photo")
    sync_primary(db, photo_id)
    if detection_id is None:
        db.subjects.clear_choice(photo_id)
    else:
        # No detection FK: content-addressed identities survive clear/recreate
        # during reprocessing. A temporarily missing choice remains remembered.
        db.subjects.set_choice(photo_id, detection_id)
    sync_primary(db, photo_id)
    db.commit_with_retry()


def payload(db, photo_id):
    detections = retained(db, photo_id)
    choice = db.subjects.choice(photo_id)
    analyses = db.subjects.analyses_for_photo(photo_id)
    import config as cfg
    floor = db.get_effective_config(cfg.load()).get("classifier_confidence", 0)
    predictions = db.subjects.visible_predictions(db.require_workspace_id(), photo_id, floor)
    grouped = {}
    for prediction in predictions:
        grouped.setdefault(prediction["detection_id"], []).append(dict(prediction))
    for detection in detections:
        analysis = analyses.get(detection["id"])
        detection["analysis"] = ({"crop": json.loads(analysis["crop"]),
            "quality_score": analysis["quality_score"], "exposure_ev": analysis["exposure_ev"],
            "basis": "detection_box", "source_key": analysis["source_key"]} if analysis else None)
        detection["evidence_basis"] = "reviewer_box" if detection["detector_model"] == REVIEWER_MODEL else "detector_box"
        detection["predictions"] = grouped.get(detection["id"], [])
        detection["is_primary"] = detection is detections[0]
    primary_id = detections[0]["id"] if detections else None
    full_image_ids = {d["id"] for d in detection_store.get_detections(db, photo_id, min_conf=0)
                      if d["detector_model"] == "full-image"}
    return {"photo_id": photo_id, "subjects": detections,
            "full_image_predictions": [p for did in full_image_ids for p in grouped.get(did, [])], "primary_detection_id": primary_id,
            "selection": "manual" if choice else "automatic",
            "choice_unavailable": bool(choice and choice["detection_id"] != primary_id)}


def select_reviewer_box(db, photo_id, box):
    """Save an explicit human region separately from model output, then select it.

    Coordinates use the upright, unedited source frame. The stored confidence
    of 1 keeps the explicit choice above detector thresholds; it is not a
    model probability. No detector run or species prediction is manufactured.
    Caller holds the photo lock.
    """
    if not isinstance(box, dict) or set(box) != set("xywh"):
        raise ValueError("box must contain x, y, w and h")
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in box.values()):
        raise ValueError("Box coordinates must be finite numbers")
    try:
        finite = all(math.isfinite(v) for v in box.values())
    except OverflowError:
        finite = False
    if not finite:
        raise ValueError("Box coordinates must be finite numbers")
    x, y, w, h = (box[k] for k in "xywh")
    if min(x, y) < 0 or min(w, h) <= 0 or x + w > 1 or y + h > 1:
        raise ValueError("Choose a nonempty box inside the source image")
    with db.transaction(immediate=True):
        for previous in detection_store.get_detections(db, photo_id, min_conf=0, detector_model=REVIEWER_MODEL):
            db.masks_features.clear_detection_embeddings(photo_id, previous["id"])
        # Clear previous measurements even if a legacy catalog lacks primary state.
        db.subjects.clear_primary_features(photo_id)
        ids = db.detections.upsert_rows(photo_id, REVIEWER_MODEL, [
            {"box": box, "confidence": 1.0, "category": "animal"}], "reviewer-v1")
        # Re-saving an identical or quantization-equivalent box reuses its ID.
        # Retire that ID's cached box analysis before sync_primary can project it.
        db.subjects.clear_analysis(ids[0])
        db.subjects.set_choice(photo_id, ids[0])
        sync_primary(db, photo_id)
    return ids[0]
