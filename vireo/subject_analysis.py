"""Background, detection-scoped comparison analysis at a fixed 1024px scale."""
import io
import os

import numpy as np
from config import config_value
from detection_id import detection_prompt_runtime, require_original_box
from PIL import Image
from subject_ranking import analysis_key, comparison_analysis, resolve_subject, source_path
from subject_readiness import usable_selected_analysis


def measure_eye(db, photo_id, detection, image, mask, effective_cfg):
    import keypoints as kp
    from pipeline import _best_eye_by_tenengrad, _eye_candidates_in_mask, _keypoints_in_mask, _resolve_keypoint_model
    from quality import eye_measurement_window

    settings = effective_cfg.get("pipeline", {})
    result = dict.fromkeys(("eye_x", "eye_y", "eye_conf", "eye_tenengrad"))
    result["eye_status"] = "disabled"
    if not settings.get("eye_detect_enabled", False):
        return result
    result["eye_status"] = "identity_unavailable"
    predictions = [dict(p) for p in db.subjects.visible_predictions(db.require_workspace_id(), photo_id, 0)
                   if p["detection_id"] == detection["id"]]
    prediction = next((p for p in predictions if p["status"] == "accepted"), predictions[0] if predictions else None)
    if prediction is None or prediction["confidence"] < config_value(settings, "eye_classifier_conf_gate", section="pipeline"):
        return result
    model = _resolve_keypoint_model(db, prediction)
    if model is None:
        return result
    result["eye_status"] = "model_unavailable"
    if not all(os.path.isfile(os.path.join(kp.MODELS_DIR, model, f)) for f in ("model.onnx", "config.json")):
        return result
    iw, ih = image.size
    x, y, w, h = (detection["box_" + k] for k in "xywh")
    bbox = tuple(int(round(v)) for v in (x * iw, y * ih, (x + w) * iw, (y + h) * ih))
    points = kp.detect_keypoints(image, bbox, model)
    threshold = config_value(settings, "eye_detection_conf_gate", section="pipeline")
    eyes = _eye_candidates_in_mask(points, mask, threshold)
    if model == "superanimal-bird":
        head = _keypoints_in_mask(points, mask, threshold, {"bill", "crown", "forehead", "nape"})
        result["head_landmarks"] = {p["name"]: [p["x"] / iw, p["y"] / ih] for p in head}

    result["eye_status"] = "not_located"
    if eyes:
        fraction = config_value(settings, "eye_window_k", section="pipeline")
        eye, score = _best_eye_by_tenengrad(image, eyes, bbox, fraction)
        left, top, right, bottom = eye_measurement_window(image.size, (eye["x"], eye["y"]), bbox, fraction)
        result["eye_measurement_box"] = [left / iw, top / ih, (right - left) / iw, (bottom - top) / ih]
        result.update(eye_x=eye["x"] / iw, eye_y=eye["y"] / ih, eye_conf=eye["conf"],
                      eye_tenengrad=score, eye_status="measured")
    return result


def analyze_detection(db, photo, detection, effective_cfg, checkpoint, *, force=False, existing_mask_path=None, defer_publication=False):
    from masking import (
        crop_completeness,
        generate_mask,
        render_proxy,
        require_aligned_detector_prompt,
        require_original_reviewer_frame,
        sam_generation,
        write_mask_png,
    )
    from primary_comparison import measurement_inputs
    from quality import compute_all_quality_features, saved_mask_matches_frame

    path = source_path(db, photo)
    require_original_box(dict(detection).get("runtime_fingerprint"))
    key = analysis_key(path, detection, effective_cfg)
    if not force and usable_selected_analysis(db.subjects.ranking_analysis(detection["id"], key)):
        require_original_reviewer_frame(path, detection_prompt_runtime(detection), detector_model=detection["detector_model"])
        return False
    checkpoint()
    image = render_proxy(path, longest_edge=1024)
    if image is None:
        raise ValueError("Could not decode original for subject analysis")
    try:
        require_aligned_detector_prompt(image, detection_prompt_runtime(detection), detector_model=detection["detector_model"])
        if existing_mask_path:
            with Image.open(existing_mask_path) as saved_mask:
                if saved_mask_matches_frame(saved_mask.size, saved_mask.info.get("vireo_mask_frame"), image,
                                            prompt_frame=saved_mask.info.get("vireo_prompt_frame"),
                                            mask_frame_id=saved_mask.info.get("vireo_subject_frame_id")):
                    mask = np.asarray(saved_mask.convert("L").resize(image.size, Image.Resampling.NEAREST)) > 0
                else:
                    existing_mask_path = None
        variant = effective_cfg.get("pipeline", {}).get("sam2_variant") or "sam2-small"
        if not existing_mask_path:
            mask = generate_mask(image, {k: detection["box_" + k] for k in "xywh"},
                                 variant=variant)
        inputs = None
        if defer_publication:
            generation = sam_generation(variant) if existing_mask_path else image.info.get("vireo_sam_generation")
            inputs = measurement_inputs(image, detection["id"], detection_prompt_runtime(detection), generation)
        if mask is None or not np.any(mask):
            raise ValueError("No subject mask was produced")
        checkpoint()
        features = compute_all_quality_features(image, mask)
        features.update(subject_size=float(mask.mean()), crop_complete=crop_completeness(mask))
        frame_id = image.info.get("vireo_subject_frame_id")
        features.update(_measure_detail(path, mask, photo, frame_id))
        features.update(measure_eye(db, photo["id"], detection, image, mask, effective_cfg))
        features["analysis"] = {"source": "original", "width": image.width, "height": image.height,
                                "frame_id": frame_id,
                                "source_generation": image.info.get("vireo_source_generation"),
                                "longest_edge_limit": 1024, "measurement_basis": "subject_mask"}
        buffer = io.BytesIO()
        write_mask_png(mask, buffer, frame_id=frame_id)
        checkpoint()
        analysis = {"source_key": key, "features": features, "mask_png": buffer.getvalue()}
        if not usable_selected_analysis(analysis):
            raise ValueError("Required subject measurements are still incomplete")
        if defer_publication:
            analysis["comparison_inputs"] = inputs
            analysis["existing_mask_path"] = existing_mask_path
            return analysis
        with db.transaction(immediate=True):
            publish_analysis(db, photo, detection, analysis, effective_cfg)
    finally:
        image.close()
    return True


def _measure_detail(path, mask, photo, frame_id=None):
    """Subject detail beside the 1024 px analysis (``quality.measure_subject_detail``).

    A source that cannot be re-read for it leaves the measurement out, so
    the cards show no cross-encounter detail rather than an interpolated one.
    """
    from quality import frame_size, measure_subject_detail

    proof = {"expected_frame_id": frame_id} if frame_id is not None else {}
    return measure_subject_detail(mask, source_path=path, frame_size=frame_size(photo), **proof)


def publish_analysis(db, photo, detection, analysis, effective_cfg):
    """Publish inside the caller's transaction; valid generations stay immutable."""
    from subject_ranking import SubjectComparison
    current_photo = db.photos.get(photo["id"], verify_workspace=True)
    if current_photo is None:
        raise ValueError("Photo is no longer visible in this workspace")
    current, _ = resolve_subject(db, photo["id"], SubjectComparison(subject_overrides={photo["id"]: detection["id"]}))
    key = analysis["source_key"]
    if current is None or analysis_key(source_path(db, current_photo), current, effective_cfg) != key:
        raise ValueError("Subject analysis inputs changed; retry this frame")
    if not usable_selected_analysis(analysis):
        raise ValueError("Required subject measurements are still incomplete")
    if analysis.get("existing_mask_path") and not analysis.get("comparison_inputs"):
        return  # Quality-only work can refresh primary scores without proving selected-cache compatibility.
    # Retire only unusable cache entries, after fresh work and input validation.
    # Valid bytes are immutable, and a failed outer publication rolls this back.
    if not usable_selected_analysis(db.subjects.ranking_analysis(detection["id"], key)):
        db.subjects.discard_ranking_analysis(detection["id"], key)
    db.subjects.save_ranking_analysis(detection["id"], key, analysis["features"], analysis["mask_png"])


def analyze_selected_subject(db, photo, comparison, effective_cfg, checkpoint):
    """Measure one selection and read its current immutable generation back."""
    detection, selection = resolve_subject(db, photo["id"], comparison)
    if detection is None:
        return {"status": "skipped", "reason": selection["selection"]}
    key = analysis_key(source_path(db, photo), detection, effective_cfg)
    with db.transaction():
        db.hold_read_snapshot()
        generation, analysis, _ = comparison_analysis(db, photo, detection, effective_cfg)
        if analysis and analysis["owner"]["kind"] == "primary":
            # Revalidate against snapshot state: the detection id alone cannot
            # tell whether prediction state or inputs changed between the first
            # resolution and the snapshot open, which would leave ``generation``
            # paired with a stale key downstream readers would not reproduce.
            current_photo = db.photos.get(photo["id"], verify_workspace=True)
            current, _ = resolve_subject(db, photo["id"], comparison)
            if (current_photo is None or not current or current["id"] != detection["id"]
                    or analysis_key(source_path(db, current_photo), current, effective_cfg) != key):
                raise ValueError("Selected subject changed during analysis")
            return {"status": "cached", "detection_id": detection["id"], "analysis_key": generation}
    changed = analyze_detection(db, photo, detection, effective_cfg, checkpoint)
    verify_selected_analysis(db, photo["id"], comparison, effective_cfg, detection["id"], key)
    return {"status": "analyzed" if changed else "cached", "detection_id": detection["id"], "analysis_key": key}


def verify_selected_analysis(db, photo_id, comparison, effective_cfg, detection_id, key):
    with db.transaction():
        db.hold_read_snapshot()
        current_photo = db.photos.get(photo_id, verify_workspace=True)
        current, _ = resolve_subject(db, photo_id, comparison)
        if (current_photo is None or current is None or current["id"] != detection_id
                or analysis_key(source_path(db, current_photo), current, effective_cfg) != key
                or not usable_selected_analysis(db.subjects.ranking_analysis(detection_id, key))):
            raise ValueError("Selected subject evidence changed during analysis; retry this frame")


def analyze_comparison(ctx, job, photo_ids, comparison, effective_cfg):
    """Each failure stays visible; unresolved identity never runs arbitrary inference."""
    import logging
    db = ctx.thread_db()
    result = {"analyzed": 0, "cached": 0, "unresolved": [], "errors": [], "photos": []}
    job["progress"]["total"] = len(photo_ids)
    try:
        for index, photo_id in enumerate(photo_ids):
            ctx.checkpoint(job)
            photo = db.photos.get(photo_id, verify_workspace=True)
            job["progress"]["current_file"] = photo["filename"] if photo else str(photo_id)
            try:
                outcome = (analyze_selected_subject(db, photo, comparison, effective_cfg, lambda: ctx.checkpoint(job))
                           if photo else {"status": "skipped", "reason": "photo_unavailable"})
                if outcome["status"] == "skipped":
                    result["unresolved"].append({"photo_id": photo_id, "reason": outcome["reason"]})
                else:
                    result[outcome["status"]] += 1
            except (OSError, ValueError, RuntimeError) as exc:
                logging.getLogger(__name__).exception("Subject analysis failed for photo %s", photo_id)
                result["errors"].append({"photo_id": photo_id, "error": str(exc)})
                outcome = {"status": "failed", "error": str(exc)}
            result["photos"].append({"photo_id": photo_id, **outcome})
            job["progress"]["current"] = index + 1
        return result
    finally:
        db.close()
