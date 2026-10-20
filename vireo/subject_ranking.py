"""Comparison-local subject resolution and scoring; never writes photographer decisions.

Photo keywords locate candidate photographs. Only detection-scoped confirmations
or an explicit comparison choice identify the animal whose pixels are measured.
"""
import hashlib
import json
import os
from dataclasses import dataclass, field

from species_identity import SpeciesResolver
from subjects import retained


@dataclass(frozen=True)
class SubjectComparison:
    requested_species: str | None = None
    subject_overrides: dict[int, int] = field(default_factory=dict)


def parse_comparison(values):
    enabled = values.get("compare_subjects", False)
    if isinstance(enabled, str) and enabled in {"1", "true", "0", "false"}:
        enabled = enabled in {"1", "true"}
    if not isinstance(enabled, bool):
        raise ValueError("compare_subjects must be a boolean")
    species = values.get("requested_species")
    overrides = values.get("subject_overrides", {})
    if isinstance(overrides, str):
        try:
            overrides = json.loads(overrides)
        except ValueError:
            raise ValueError("subject_overrides must be a photo-id to detection-id object") from None
    if species is not None and (not isinstance(species, str) or not species.strip() or len(species) > 300):
        raise ValueError("requested_species must be a non-empty species name or identity key")
    if isinstance(species, str) and species.strip().startswith("taxon:"):
        _requested_taxon_id(species.strip())
    if not isinstance(overrides, dict) or len(overrides) > 500:
        raise ValueError("subject_overrides must be a photo-id to detection-id object")
    parsed = {}
    for photo_id, detection_id in overrides.items():
        if not str(photo_id).isdigit() or int(photo_id) <= 0 or isinstance(detection_id, bool) or not isinstance(detection_id, int) or detection_id <= 0:
            raise ValueError("subject_overrides must contain positive integer ids")
        parsed[int(photo_id)] = detection_id
    return SubjectComparison(species.strip() if species else None, parsed) if species or parsed or enabled else None


def _requested_taxon_id(name):
    raw_id = name[6:]
    if not raw_id.isascii() or not raw_id.isdecimal() or not 0 < int(raw_id) < 2**63:
        raise ValueError("taxon id must be a positive signed-64-bit integer")
    return int(raw_id)


def requested_identity(resolver, name):
    if name.startswith("taxon:"):
        return resolver.resolve(name, source={"taxon_id": _requested_taxon_id(name)})
    if name.startswith("scientific:"):
        return resolver.resolve(name[11:], scientific_name=name[11:])
    return resolver.display(name[5:] if name.startswith("name:") else name)


def resolve_subject(db, photo_id, comparison):
    resolver = SpeciesResolver(db=db)
    target = requested_identity(resolver, comparison.requested_species) if comparison.requested_species else None
    detections = retained(db, photo_id)
    predictions = db.subjects.visible_predictions(db.require_workspace_id(), photo_id, 0)
    candidates = []
    for detection in detections:
        rows = [dict(p) for p in predictions if p["detection_id"] == detection["id"]]
        confirmed = {resolver.prediction(p).key for p in rows if p["status"] == "accepted"}
        candidates.append({"detection_id": detection["id"], "box": {k: detection["box_" + k] for k in "xywh"},
                           "predictions": [{"species": resolver.prediction(p).display_name, "status": p["status"],
                                            "prediction_id": p["id"], "species_key": resolver.prediction(p).key,
                                            "confidence": p["confidence"], "classifier_model": p["classifier_model"],
                                            "labels_fingerprint": p["labels_fingerprint"]} for p in rows],
                           "confirmed_keys": sorted(confirmed)})
    override = comparison.subject_overrides.get(photo_id)
    selected, state = None, "subject_identity_uncertain"
    if override is not None:
        selected = next((d for d in detections if d["id"] == override), None)
        if selected is None:
            raise ValueError(f"Detection {override} is not a retained animal in photo {photo_id}")
        candidate = next(c for c in candidates if c["detection_id"] == override)
        if target and candidate["confirmed_keys"] and set(candidate["confirmed_keys"]) != {target.key}:
            selected, state = None, "subject_identity_conflict"
        else:
            state = "selected"
    elif target:
        matches = [c for c in candidates if c["confirmed_keys"] == [target.key]]
        conflicts = any(target.key in c["confirmed_keys"] and len(c["confirmed_keys"]) > 1 for c in candidates)
        if conflicts:
            state = "subject_identity_conflict"
        elif len(matches) == 1:
            selected = next(d for d in detections if d["id"] == matches[0]["detection_id"])
            state = "confirmed"
        elif len(matches) > 1:
            state = "multiple_matching_subjects"
    elif detections:
        selected, state = detections[0], "primary"
    if selected:
        confirmations = next(c["confirmed_keys"] for c in candidates if c["detection_id"] == selected["id"])
        if len(confirmations) > 1:
            selected, state = None, "subject_identity_conflict"
    if selected:
        selected["prediction_fingerprint"] = hashlib.sha256(json.dumps(
            [dict(p) for p in predictions if p["detection_id"] == selected["id"]], sort_keys=True,
        ).encode()).hexdigest()
    return selected, {"detection_id": selected["id"] if selected else None,
                      "selection": state, "requested_species_key": target.key if target else None,
                      "candidates": candidates}


def source_path(db, photo):
    folder = db.folders.get(photo["folder_id"])
    if folder is None:
        raise FileNotFoundError("Photo folder is unavailable")
    return os.path.join(folder["path"], photo["filename"])


def analysis_key(path, detection, effective_cfg):
    """Bind a generation to exact source, box, model settings, and pixel scale."""
    import keypoints as kp
    from masking import _sam2_model_dir
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    pipeline = effective_cfg.get("pipeline", {})
    stat = os.stat(path)
    weights = []
    for model in ("superanimal-bird", "superanimal-quadruped"):
        for name in ("model.onnx", "config.json"):
            weight = os.path.join(kp.MODELS_DIR, model, name)
            try:
                info = os.stat(weight)
                weights.append((weight, info.st_size, info.st_mtime_ns))
            except FileNotFoundError:
                weights.append((weight, None))
    sam_dir = _sam2_model_dir(pipeline.get("sam2_variant") or "sam2-small")
    for name in ("image_encoder.onnx", "mask_decoder.onnx"):
        weight = os.path.join(sam_dir, name)
        try:
            info = os.stat(weight)
            weights.append((weight, info.st_size, info.st_mtime_ns))
        except FileNotFoundError:
            weights.append((weight, None))
    inputs = [1, EYE_KP_FINGERPRINT_VERSION, detection.get("prediction_fingerprint"), os.path.realpath(path), stat.st_size, stat.st_mtime_ns, detection["id"],
              detection["detector_model"], [detection["box_" + k] for k in "xywh"], 1024,
              {k: pipeline.get(k, default) for k, default in (
                  ("sam2_variant", "sam2-small"), ("eye_detect_enabled", False),
                  ("eye_classifier_conf_gate", .5), ("eye_detection_conf_gate", .5), ("eye_window_k", .08))}, weights]
    return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()


def cached_analysis(db, photo, detection, effective_cfg):
    try:
        key = analysis_key(source_path(db, photo), detection, effective_cfg)
    except OSError:
        return None, None
    return key, db.subjects.ranking_analysis(detection["id"], key)


def subject_urls(photo_id, selection, size=512):
    """Pin every subject image to the same detection and analysis generation."""
    detection_id, key = selection.get("detection_id"), selection.get("analysis_key")
    urls = {"preview_url": f"/photos/{photo_id}/preview?size=1920", "thumbnail_url": f"/thumbnails/{photo_id}.jpg",
            "crop_url": None, "focus_crop_url": None, "focus_crop_mask_url": None, "mask_url": None}
    if detection_id:
        urls["crop_url"] = f"/photos/{photo_id}/crop?detection_id={detection_id}"
    if key:
        query = f"detection_id={detection_id}&analysis_key={key}"
        focus = f"/photos/{photo_id}/focus-crop?size={size}&source=original&{query}"
        urls.update(focus_crop_url=focus, focus_crop_mask_url=focus + "&mask=1",
                    mask_url=f"/api/masks/{photo_id}/subject.png?{query}")
    return urls


def comparison_frames(db, photos, comparison, effective_cfg):
    unknown = set(comparison.subject_overrides) - {p["id"] for p in photos}
    if unknown:
        raise ValueError("subject_overrides must refer to photos in this comparison")
    frames, eligible = [], []
    for photo in photos:
        detection, selection = resolve_subject(db, photo["id"], comparison)
        analysis, key = None, None
        if detection:
            key, analysis = cached_analysis(db, photo, detection, effective_cfg)
        selection["analysis_key"] = key if analysis else None
        state = selection["selection"] if not detection else ("ready" if analysis else "subject_analysis_missing")
        card = {"id": photo["id"], "filename": photo["filename"], "rank": None, "role": "review", "label": "REVIEW",
                "quality": None, "quality_pct": None, "focus": None, "focus_basis": None,
                "sharpness": None, "eye_x": None, "eye_y": None, "eye_conf": None, "eye_tenengrad": None,
                "flag": photo.get("flag") or "none", "rating": photo.get("rating"), "reject_reasons": [],
                "subject": selection, "evidence_status": state, "detection_count": len(selection["candidates"]),
                "has_mask": bool(analysis), "has_quality": bool(analysis), "has_eye": False,
                "reasons": [] if state == "ready" else [state.replace("_", " ")],
                **subject_urls(photo["id"], selection)}
        if analysis:
            features = analysis["features"]
            # No photo-level subject measurement, embedding, or species vote may leak into this scorer.
            frame = {k: photo.get(k) for k in ("id", "filename", "width", "height", "flag", "rating")}
            frame.update(features)
            frame["subject_mask_available"] = bool(analysis["mask_png"])
            card.update(sharpness=features["subject_tenengrad"], has_eye=features.get("eye_tenengrad") is not None)
            for k in ("eye_x", "eye_y", "eye_conf", "eye_tenengrad", "crop_complete", "subject_size", "analysis", "eye_status"):
                card[k] = features.get(k)
            eligible.append(frame)
        frames.append(card)
    return frames, eligible


def build_subject_comparison(db, seed_photo_id, photos, comparison, effective_cfg):
    from best_batch import _best_batch_reasons
    from scoring import score_encounter
    cards, eligible = comparison_frames(db, photos, comparison, effective_cfg)
    if len(eligible) >= 2:
        score_encounter({"photos": eligible}, config=effective_cfg)
        eligible.sort(key=lambda p: (p.get("label") != "REJECT", p["quality_composite"], p["focus_score"]), reverse=True)
    else:
        eligible = []
    by_id = {c["id"]: c for c in cards}
    best = next((p for p in eligible if p.get("label") != "REJECT"), None)
    for rank, photo in enumerate(eligible, 1):
        card = by_id[photo["id"]]
        role = "best" if photo is best else ("reject" if photo.get("label") == "REJECT" else "alternate")
        card.update(rank=rank, role=role, label=photo.get("label") or "REVIEW", quality=photo["quality_composite"],
                    quality_pct=round(photo["quality_composite"] * 100), focus=photo["focus_score"],
                    focus_basis="eye" if photo.get("eye_focus_score") is not None else "subject",
                    reject_reasons=photo.get("reject_reasons", []),
                    reasons=_best_batch_reasons(photo, eligible, is_best=photo is best))
    cards.sort(key=lambda c: (c["rank"] is None, c["rank"] or 0))
    return {"seed_photo_id": seed_photo_id, "photo_ids": [p["id"] for p in photos], "count": len(photos),
            "cards": cards, "sequence_range": None, "best_photo_id": best["id"] if best else None,
            "best_filename": best["filename"] if best else None, "best_reasons": by_id[best["id"]]["reasons"] if best else [],
            "alternate_ids": [c["id"] for c in cards if c["role"] == "alternate"],
            "suggested_reject_ids": [c["id"] for c in cards if c["role"] == "reject" and c["flag"] == "none"],
            "comparison": {"requested_species": comparison.requested_species, "subject_overrides": comparison.subject_overrides},
            "scored": bool(eligible), "unscored_reason": None if eligible else "At least two selected subjects need current quality analysis"}
