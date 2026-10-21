"""Background, detection-scoped comparison analysis at a fixed 1024px scale."""
import io
import json
import os

import numpy as np
from PIL import Image
from subject_ranking import analysis_key, resolve_subject, source_path


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


def analyze_detection(db, photo, detection, effective_cfg, checkpoint, *, force=False, existing_mask_path=None, defer_publication=False):
    from image_loader import load_image
    from masking import crop_completeness, generate_mask
    from quality import compute_all_quality_features

    path = source_path(db, photo)
    key = analysis_key(path, detection, effective_cfg)
    if not force and _usable_analysis(_existing_analysis(db, detection["id"], key)):
        return False
    checkpoint()
    image = load_image(path, max_size=1024)
    if image is None:
        raise ValueError("Could not decode original for subject analysis")
    try:
        if existing_mask_path:
            with Image.open(existing_mask_path) as saved_mask:
                mask = np.asarray(saved_mask.convert("L").resize(image.size, Image.Resampling.NEAREST)) > 0
        else:
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
        analysis = {"source_key": key, "features": features, "mask_png": buffer.getvalue()}
        if not _usable_analysis(analysis):
            raise ValueError("Required subject measurements are still incomplete")
        if defer_publication:
            return analysis
        with db.transaction():
            db.begin_immediate()
            publish_analysis(db, photo, detection, analysis, effective_cfg)
            db.commit_with_retry()
    finally:
        image.close()
    return True


def _existing_analysis(db, detection_id, key):
    try:
        return db.subjects.ranking_analysis(detection_id, key)
    except (json.JSONDecodeError, TypeError):
        return None  # malformed cached output may be repaired after fresh work succeeds


def _usable_analysis(analysis):
    from subject_readiness import subject_analysis_readiness
    if not isinstance(analysis, dict) or not isinstance(analysis.get("features"), dict):
        return False
    ready = subject_analysis_readiness({**analysis["features"], "subject_present": True,
                                       "subject_mask_available": bool(analysis.get("mask_png"))})
    if not ready["eligible"]:
        return False
    try:
        with Image.open(io.BytesIO(analysis["mask_png"])) as image:
            pixels = np.asarray(image)
            return pixels.ndim == 2 and bool(np.any(pixels))
    except (OSError, ValueError, TypeError):
        return False


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
    if not _usable_analysis(analysis):
        raise ValueError("Required subject measurements are still incomplete")
    # Retire only unusable cache entries, after fresh work and input validation.
    # Valid bytes are immutable, and a failed outer publication rolls this back.
    if not _usable_analysis(_existing_analysis(db, detection["id"], key)):
        db.subjects.discard_ranking_analysis(detection["id"], key)
    db.subjects.save_ranking_analysis(detection["id"], key, analysis["features"], analysis["mask_png"])


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
            try:
                detection, selection = resolve_subject(db, photo_id, comparison) if photo else (None, {})
                if not detection:
                    result["unresolved"].append({"photo_id": photo_id, "reason": selection.get("selection", "photo_unavailable")})
                else:
                    changed = analyze_detection(db, photo, detection, effective_cfg, lambda: ctx.checkpoint(job))
                    result["analyzed" if changed else "cached"] += 1
            except (OSError, ValueError, RuntimeError) as exc:
                logging.getLogger(__name__).exception("Subject analysis failed for photo %s", photo_id)
                result["errors"].append({"photo_id": photo_id, "error": str(exc)})
            job["progress"]["current"] = index + 1
        return result
    finally:
        db.close()
