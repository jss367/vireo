"""Cached, non-destructive analysis and primary selection for retained animals.

Subject scores compare boxes in the same image; they are not the encounter
percentile scores used by culling. Suggestions never modify an edit recipe.
"""
import hashlib
import json
import math
import os

import numpy as np

ANALYSIS_VERSION = 1


def primary_order_sql(alias="detections"):
    """Shared ordering for readers that choose one detection per photo."""
    return f"""CASE WHEN {alias}.category = 'animal'
                     AND {alias}.detector_model != 'full-image' THEN 1 ELSE 0 END DESC,
        COALESCE({alias}.id = (SELECT detection_id FROM photo_subject_choices
                              WHERE photo_id = {alias}.photo_id), 0) DESC,
        (SELECT quality_score FROM detection_subjects
         WHERE detection_id = {alias}.id) DESC,
        {alias}.detector_confidence DESC, {alias}.id ASC"""


def retained(db, photo_id, min_conf=None):
    detections = (dict(d) for d in db.get_detections(photo_id, min_conf=min_conf))
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


def analyze_photo(db, photo_id, image_path, *, min_conf=None, force=False, checkpoint=None):
    """Backfill on cached detection runs too; failed/offline reads remain retryable."""
    from db import commit_with_retry
    from image_loader import load_image
    from pipeline_locks import acquire_photo_mask

    detections = retained(db, photo_id, min_conf)
    if not detections:
        # Migrated catalogs have legacy subject-derived fields on ``photos``
        # without any ``photo_subject_state`` row. Triggering ``sync_primary``
        # only when that row exists made the intended cleanup a no-op there,
        # leaving stale mask/quality/DINO/eye state attached to a photo with
        # no detections. Check either signal (Codex r4056698678).
        stale = db.conn.execute(
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM photo_subject_state WHERE photo_id=?) "
            "OR EXISTS (SELECT 1 FROM photos WHERE id=? AND ("
            "mask_path IS NOT NULL OR quality_score IS NOT NULL "
            "OR subject_sharpness IS NOT NULL OR subject_size IS NOT NULL "
            "OR dino_subject_embedding IS NOT NULL OR eye_x IS NOT NULL "
            "OR eye_kp_fingerprint IS NOT NULL))",
            (photo_id, photo_id),
        ).fetchone()
        if stale:
            with acquire_photo_mask(photo_id):
                if checkpoint:
                    checkpoint()
                sync_primary(db, photo_id, min_conf=min_conf)
                commit_with_retry(db.conn)
        return 0
    stat = os.stat(image_path)
    source_key = hashlib.sha256(f"{os.path.realpath(image_path)}:{stat.st_size}:{stat.st_mtime_ns}:{ANALYSIS_VERSION}".encode()).hexdigest()
    cached = {r["detection_id"] for r in db.conn.execute(
        "SELECT s.detection_id FROM detection_subjects s JOIN detections d ON d.id=s.detection_id "
        "WHERE d.photo_id=? AND s.source_key=?", (photo_id, source_key))}
    pending = [d for d in detections if force or d["id"] not in cached]
    if not pending:
        with acquire_photo_mask(photo_id):
            if checkpoint:
                checkpoint()
            sync_primary(db, photo_id, min_conf=min_conf)
            commit_with_retry(db.conn)
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
        for result in results:
            # The detector may have been replaced while the image was decoded.
            db.conn.execute(
                """INSERT INTO detection_subjects
                   (detection_id, source_key, crop, quality_score, exposure_ev, features)
                   SELECT id, ?, ?, ?, ?, ? FROM detections WHERE id=?
                   ON CONFLICT(detection_id) DO UPDATE SET source_key=excluded.source_key,
                   crop=excluded.crop, quality_score=excluded.quality_score,
                   exposure_ev=excluded.exposure_ev, features=excluded.features""",
                (source_key, json.dumps(result["crop"]), result["quality_score"],
                 result["exposure_ev"], json.dumps(result["features"]), result["detection_id"]),
            )
        sync_primary(db, photo_id, min_conf=min_conf)
        commit_with_retry(db.conn)
    return len(results)


def _clear_primary_features(db, photo_id):
    db.conn.execute("""UPDATE photos SET mask_path=NULL, active_mask_variant=NULL,
        quality_score=NULL, subject_sharpness=NULL, subject_size=NULL,
        subject_clip_high=NULL, subject_clip_low=NULL, subject_y_median=NULL,
        subject_tenengrad=NULL, bg_tenengrad=NULL, crop_complete=NULL,
        bg_separation=NULL, phash_crop=NULL, noise_estimate=NULL,
        eye_x=NULL, eye_y=NULL, eye_conf=NULL, eye_tenengrad=NULL,
        eye_kp_fingerprint=NULL, dino_subject_embedding=NULL WHERE id=?""", (photo_id,))


def sync_primary(db, photo_id, *, min_conf=None):
    """Project primary quality; clear stale subject-dependent outputs on a switch.

    Mask snapshots referenced by manual edits are immutable and untouched.
    Existing photo_masks remain cached, and prompt matching decides reuse.
    """
    detections = retained(db, photo_id, min_conf)
    if not detections:
        _clear_primary_features(db, photo_id)
        db.conn.execute("DELETE FROM photo_subject_state WHERE photo_id=?", (photo_id,))
        return
    primary = detections[0]
    analysis = db.conn.execute("SELECT * FROM detection_subjects WHERE detection_id=?", (primary["id"],)).fetchone()
    previous = db.conn.execute("SELECT detection_id FROM photo_subject_state WHERE photo_id=?", (photo_id,)).fetchone()
    active_mask = db.conn.execute("""SELECT pm.* FROM photo_masks pm JOIN photos p
        ON p.id=pm.photo_id AND p.active_mask_variant=pm.variant WHERE p.id=?""", (photo_id,)).fetchone()
    mask_matches = active_mask and active_mask["detector_model"] == primary["detector_model"] and all(
        active_mask["prompt_" + k] == primary["box_" + k] for k in "xywh")
    changed = previous and previous["detection_id"] != primary["id"]
    if changed or (active_mask and not mask_matches):
        _clear_primary_features(db, photo_id)
    if analysis:
        features = json.loads(analysis["features"])
        db.conn.execute("UPDATE photos SET quality_score=?, subject_sharpness=? WHERE id=?",
                        (analysis["quality_score"], features["subject_sharpness"], photo_id))
        if changed or not mask_matches:
            db.conn.execute("""UPDATE photos SET subject_size=?,
                subject_clip_high=?, subject_clip_low=?, subject_y_median=? WHERE id=?""",
                (features["subject_size"], features["subject_clip_high"], features["subject_clip_low"],
                 features["subject_y_median"], photo_id))
    db.conn.execute("INSERT INTO photo_subject_state(photo_id,detection_id) VALUES (?,?) "
                    "ON CONFLICT(photo_id) DO UPDATE SET detection_id=excluded.detection_id", (photo_id, primary["id"]))


def select_primary(db, photo_id, detection_id):
    from db import commit_with_retry

    if detection_id is not None:
        if isinstance(detection_id, bool) or not isinstance(detection_id, int):
            raise ValueError("detection_id must be an integer or null for automatic selection")
        if not any(d["id"] == detection_id for d in retained(db, photo_id)):
            raise ValueError("Choose a retained animal detection from this photo")
    sync_primary(db, photo_id)
    if detection_id is None:
        db.conn.execute("DELETE FROM photo_subject_choices WHERE photo_id=?", (photo_id,))
    else:
        # No detection FK: content-addressed identities survive clear/recreate
        # during reprocessing. A temporarily missing choice remains remembered.
        db.conn.execute("INSERT INTO photo_subject_choices(photo_id,detection_id) VALUES (?,?) "
                        "ON CONFLICT(photo_id) DO UPDATE SET detection_id=excluded.detection_id", (photo_id, detection_id))
    sync_primary(db, photo_id)
    commit_with_retry(db.conn)


def payload(db, photo_id):
    detections = retained(db, photo_id)
    choice = db.conn.execute("SELECT detection_id FROM photo_subject_choices WHERE photo_id=?", (photo_id,)).fetchone()
    analyses = {r["detection_id"]: dict(r) for r in db.conn.execute(
        "SELECT s.* FROM detection_subjects s JOIN detections d ON d.id=s.detection_id WHERE d.photo_id=?", (photo_id,))}
    import config as cfg
    floor = db.get_effective_config(cfg.load()).get("classifier_confidence", 0)
    predictions = db.conn.execute("""SELECT pr.*, COALESCE(rv.status,'pending') AS status
        FROM predictions pr JOIN detections d ON d.id=pr.detection_id
        LEFT JOIN prediction_review rv ON rv.prediction_id=pr.id AND rv.workspace_id=?
        WHERE d.photo_id=? AND pr.confidence>=? AND COALESCE(rv.status,'pending')!='rejected'
        AND pr.labels_fingerprint=(SELECT pr2.labels_fingerprint FROM predictions pr2
            WHERE pr2.detection_id=pr.detection_id AND pr2.classifier_model=pr.classifier_model
            ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1)
        ORDER BY pr.confidence DESC, pr.id ASC""", (db._ws_id(), photo_id, floor)).fetchall()
    grouped = {}
    for prediction in predictions:
        grouped.setdefault(prediction["detection_id"], []).append(dict(prediction))
    for detection in detections:
        analysis = analyses.get(detection["id"])
        detection["analysis"] = ({"crop": json.loads(analysis["crop"]),
            "quality_score": analysis["quality_score"], "exposure_ev": analysis["exposure_ev"],
            "basis": "detection_box", "source_key": analysis["source_key"]} if analysis else None)
        detection["predictions"] = grouped.get(detection["id"], [])
        detection["is_primary"] = detection is detections[0]
    primary_id = detections[0]["id"] if detections else None
    return {"photo_id": photo_id, "subjects": detections, "primary_detection_id": primary_id,
            "selection": "manual" if choice else "automatic",
            "choice_unavailable": bool(choice and choice["detection_id"] != primary_id)}
