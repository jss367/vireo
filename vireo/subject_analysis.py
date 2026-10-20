"""Background, detection-scoped comparison analysis at a fixed 1024px scale."""
import io
import os

import numpy as np
from PIL import Image
from subject_ranking import analysis_key, cached_analysis, resolve_subject, source_path


def measure_eye(db, photo_id, detection, image, mask, effective_cfg):
    import keypoints as kp
    from pipeline import _best_eye_by_tenengrad, _eye_candidates_in_mask, _resolve_keypoint_model

    settings = effective_cfg.get("pipeline", {})
    result = dict.fromkeys(("eye_x", "eye_y", "eye_conf", "eye_tenengrad"))
    result["eye_status"] = "disabled"
    if not settings.get("eye_detect_enabled", False):
        return result
    result["eye_status"] = "identity_unavailable"
    predictions = [dict(p) for p in db.subjects.visible_predictions(db.require_workspace_id(), photo_id, 0)
                   if p["detection_id"] == detection["id"]]
    prediction = next((p for p in predictions if p["status"] == "accepted"), predictions[0] if predictions else None)
    if prediction is None or prediction["confidence"] < settings.get("eye_classifier_conf_gate", .5):
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
    eyes = _eye_candidates_in_mask(points, mask, settings.get("eye_detection_conf_gate", .5))
    result["eye_status"] = "not_located"
    if eyes:
        eye, score = _best_eye_by_tenengrad(image, eyes, bbox, settings.get("eye_window_k", .08))
        result.update(eye_x=eye["x"] / iw, eye_y=eye["y"] / ih, eye_conf=eye["conf"],
                      eye_tenengrad=score, eye_status="measured")
    return result


def analyze_detection(db, photo, detection, effective_cfg, checkpoint):
    from image_loader import load_image
    from masking import crop_completeness, generate_mask
    from quality import compute_all_quality_features

    key, cached = cached_analysis(db, photo, detection, effective_cfg)
    if cached:
        return False
    path = source_path(db, photo)
    key = analysis_key(path, detection, effective_cfg)
    checkpoint()
    image = load_image(path, max_size=1024)
    if image is None:
        raise ValueError("Could not decode original for subject analysis")
    try:
        mask = generate_mask(image, {k: detection["box_" + k] for k in "xywh"},
                             variant=effective_cfg.get("pipeline", {}).get("sam2_variant") or "sam2-small")
        if mask is None or not np.any(mask):
            raise ValueError("No subject mask was produced")
        checkpoint()
        features = compute_all_quality_features(image, mask)
        features.update(subject_size=float(mask.mean()), crop_complete=crop_completeness(mask))
        features.update(measure_eye(db, photo["id"], detection, image, mask, effective_cfg))
        features["analysis"] = {"source": "original", "width": image.width, "height": image.height,
                                "longest_edge_limit": 1024, "measurement_basis": "subject_mask"}
        buffer = io.BytesIO()
        Image.fromarray(np.asarray(mask, dtype=np.uint8) * 255).save(buffer, format="PNG")
        checkpoint()
        # Do not publish measurements of a source or prediction that changed mid-inference.
        from subject_ranking import SubjectComparison
        current, _ = resolve_subject(db, photo["id"], SubjectComparison(subject_overrides={photo["id"]: detection["id"]}))
        if current is None or analysis_key(path, current, effective_cfg) != key:
            raise ValueError("Subject analysis inputs changed; retry this frame")
        db.subjects.save_ranking_analysis(detection["id"], key, features, buffer.getvalue())
        db.commit_with_retry()
    finally:
        image.close()
    return True


def analyze_comparison(ctx, job, photo_ids, comparison, effective_cfg):
    """Each failure stays visible; unresolved identity never runs arbitrary inference."""
    import logging
    db = ctx.thread_db()
    result = {"analyzed": 0, "cached": 0, "unresolved": [], "errors": []}
    job["progress"]["total"] = len(photo_ids)
    try:
        for index, photo_id in enumerate(photo_ids):
            ctx.checkpoint(job)
            photo = db.photos.get(photo_id, verify_workspace=True)
            job["progress"]["current_file"] = photo["filename"] if photo else str(photo_id)
            detection, selection = resolve_subject(db, photo_id, comparison) if photo else (None, {})
            if not detection:
                result["unresolved"].append({"photo_id": photo_id, "reason": selection.get("selection", "photo_unavailable")})
            else:
                try:
                    changed = analyze_detection(db, photo, detection, effective_cfg, lambda: ctx.checkpoint(job))
                    result["analyzed" if changed else "cached"] += 1
                except (OSError, ValueError, RuntimeError) as exc:
                    logging.getLogger(__name__).exception("Subject analysis failed for photo %s", photo_id)
                    result["errors"].append({"photo_id": photo_id, "error": str(exc)})
            job["progress"]["current"] = index + 1
        return result
    finally:
        db.close()
