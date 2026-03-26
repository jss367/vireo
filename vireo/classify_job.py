"""Classification job logic extracted from app.py.

This module contains the background work function for the /api/jobs/classify
endpoint. The route handler in app.py parses the request and delegates here.
"""

import json
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Optional

from labels import get_active_labels, get_saved_labels, load_merged_labels

try:
    from detector import detect_animals, get_primary_detection
except ImportError:
    detect_animals = None
    get_primary_detection = None

try:
    from sharpness import compute_sharpness
except ImportError:
    compute_sharpness = None

try:
    from image_loader import load_image
except ImportError:
    load_image = None

log = logging.getLogger(__name__)


@dataclass
class ClassifyParams:
    """Parameters for a classification job, parsed from the request body."""

    collection_id: str
    labels_file: Optional[str]
    labels_files: Optional[list]
    model_id: Optional[str]
    model_name: Optional[str]
    grouping_window: int
    similarity_threshold: float
    reclassify: bool


def _load_taxonomy(taxonomy_path):
    """Load taxonomy from JSON file. Returns Taxonomy instance or None."""
    if not os.path.exists(taxonomy_path):
        return None
    try:
        from taxonomy import Taxonomy

        return Taxonomy(taxonomy_path)
    except Exception as e:
        log.warning(
            "Could not load taxonomy: %s — continuing without taxonomy enrichment", e
        )
        return None


def _load_labels(model_type, model_str, labels_file, labels_files):
    """Resolve labels for classification.

    Returns:
        (labels, use_tol) where labels is a list of species strings or None,
        and use_tol is True if Tree of Life mode should be used.
    """
    if model_type == "timm":
        log.info("Classification config: model=%s (timm) — no labels needed", model_str)
        return None, False

    labels = None

    if labels_files and isinstance(labels_files, list):
        saved = get_saved_labels()
        saved_by_file = {s["labels_file"]: s for s in saved}
        active_sets = []
        for p in labels_files:
            meta = saved_by_file.get(p, {"labels_file": p})
            active_sets.append(meta)
        labels = load_merged_labels(active_sets)
        log.info("Using %d merged labels from %d sets", len(labels), len(active_sets))
    elif labels_file and os.path.exists(labels_file):
        with open(labels_file) as f:
            labels = [line.strip() for line in f if line.strip()]
        log.info("Using %d labels from file: %s", len(labels), labels_file)
    else:
        active_sets = get_active_labels()
        if active_sets:
            labels = load_merged_labels(active_sets)
            names = [s.get("name", "?") for s in active_sets]
            log.info(
                "Using %d merged labels from active sets: %s",
                len(labels),
                ", ".join(names),
            )

    if labels:
        log.info(
            "Classification config: model=%s, labels=%d from %s",
            model_str,
            len(labels),
            labels_file or "active labels",
        )
    else:
        log.info("Classification config: model=%s, no labels selected", model_str)

    tol_supported_models = {
        "hf-hub:imageomics/bioclip",
        "hf-hub:imageomics/bioclip-2",
    }
    use_tol = False
    if not labels:
        if model_str in tol_supported_models:
            log.info(
                "No regional labels available — using Tree of Life classifier (all species)"
            )
            use_tol = True
        else:
            raise RuntimeError(
                f"No labels available and Tree of Life mode is not supported "
                f"for {model_str}. Go to Settings > Labels and download "
                f"a species list for your region."
            )

    return labels, use_tol


def _detect_subjects(photos, folders, runner, job, reclassify, db):
    """Run MegaDetector on photos, storing quality metrics.

    Returns:
        (detection_map, detected_count) where detection_map is
        {photo_id: detection_dict} and detected_count is total detected.
    """
    detected = 0
    detection_map = {}
    total = len(photos)

    try:
        if detect_animals is None or get_primary_detection is None:
            raise ImportError(
                "PytorchWildlife is not installed — cannot run detection"
            )

        runner.push_event(
            job["id"],
            "progress",
            {
                "current": 0,
                "total": total,
                "current_file": "Loading MegaDetector...",
                "rate": 0,
                "phase": "Step 4/5: Detecting subjects",
            },
        )

        start_time = job.get("_start_time", time.time())
        skipped_det = 0
        for i, photo in enumerate(photos):
            folder_path = folders.get(photo["folder_id"], "")
            image_path = os.path.join(folder_path, photo["filename"])

            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": i + 1,
                    "total": total,
                    "current_file": photo["filename"],
                    "rate": round(
                        (i + 1) / max(time.time() - start_time, 0.01), 1
                    ),
                    "phase": "Step 4/5: Detecting subjects",
                },
            )

            # Skip if already detected (unless reclassifying)
            if not reclassify and photo["detection_box"]:
                det_box = photo["detection_box"]
                if isinstance(det_box, str):
                    det_box = json.loads(det_box)
                detection_map[photo["id"]] = {
                    "box": det_box,
                    "confidence": photo["detection_conf"] or 0,
                    "category": "animal",
                }
                detected += 1
                skipped_det += 1
                continue

            detections = detect_animals(image_path)
            primary = get_primary_detection(detections)

            if primary:
                detected += 1
                detection_map[photo["id"]] = primary

                det_box = primary["box"]
                det_conf = primary["confidence"]
                subject_size = det_box["w"] * det_box["h"]

                if compute_sharpness is not None:
                    overall_sharpness = compute_sharpness(image_path)
                    subject_sharpness = None
                    quality = 0

                    try:
                        from PIL import Image

                        img = Image.open(image_path)
                        iw, ih = img.size
                        px = int(det_box["x"] * iw)
                        py = int(det_box["y"] * ih)
                        pw = int(det_box["w"] * iw)
                        ph = int(det_box["h"] * ih)
                        subject_sharpness = compute_sharpness(
                            image_path, region=(px, py, pw, ph)
                        )
                    except Exception:
                        subject_sharpness = overall_sharpness

                    if subject_sharpness is not None and subject_size is not None:
                        norm_sharp = min(1.0, math.log1p(subject_sharpness) / 10.0)
                        norm_size = min(1.0, subject_size * 4)
                        quality = round(0.7 * norm_sharp + 0.3 * norm_size, 4)

                    db.update_photo_quality(
                        photo["id"],
                        detection_box=det_box,
                        detection_conf=det_conf,
                        subject_sharpness=subject_sharpness,
                        subject_size=subject_size,
                        quality_score=quality,
                        sharpness=overall_sharpness,
                    )
                else:
                    db.update_photo_quality(
                        photo["id"],
                        detection_box=det_box,
                        detection_conf=det_conf,
                    )

        log.info(
            "Detection done: %d animals detected out of %d photos (%d skipped, already detected)",
            detected,
            total,
            skipped_det,
        )
    except (ImportError, RuntimeError) as e:
        msg = str(e)
        if "PytorchWildlife" in msg:
            log.info(
                "PytorchWildlife not installed — skipping detection (classifying full images)"
            )
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": total,
                    "current_file": "",
                    "phase": "Step 4/5: Detection skipped (PytorchWildlife not installed)",
                },
            )
        else:
            log.warning("Detection unavailable: %s — classifying full images", e)
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": total,
                    "current_file": "",
                    "phase": f"Step 4/5: Detection failed — {msg[:120]}",
                },
            )
            job["errors"].append(f"Detection unavailable: {msg[:200]}")
    except Exception as e:
        log.warning(
            "Detection failed (non-fatal) — classifying full images", exc_info=True
        )
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": 0,
                "total": total,
                "current_file": "",
                "phase": f"Step 4/5: Detection failed — {str(e)[:120]}",
            },
        )
        job["errors"].append(f"Detection failed: {str(e)[:200]}")

    return detection_map, detected


def _classify_photos(
    photos, folders, detection_map, existing_preds, clf, model_type,
    model_name, runner, job, db,
):
    """Classify each photo, cropping to detected subject when available.

    Returns:
        (raw_results, failed_count, skipped_existing_count)
    """
    import tempfile
    from datetime import datetime as dt

    from PIL import Image

    raw_results = []
    failed = 0
    skipped_existing = 0
    total = len(photos)

    start_time = time.time()

    for i, photo in enumerate(photos):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])

        job["progress"]["current"] = i + 1
        job["progress"]["current_file"] = photo["filename"]
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": i + 1,
                "total": total,
                "current_file": photo["filename"],
                "rate": round((i + 1) / max(time.time() - start_time, 0.01), 1),
                "phase": "Step 5/5: Classifying species",
            },
        )

        if photo["id"] in existing_preds:
            skipped_existing += 1
            pred_row = db.get_prediction_for_photo(photo["id"], model_name)
            if pred_row:
                timestamp = None
                if photo["timestamp"]:
                    try:
                        timestamp = dt.fromisoformat(photo["timestamp"])
                    except Exception:
                        pass
                embedding = None
                if model_type != "timm":
                    emb_blob = db.get_photo_embedding(photo["id"])
                    if emb_blob:
                        import numpy as np

                        embedding = np.frombuffer(emb_blob, dtype=np.float32)
                raw_results.append(
                    {
                        "photo": photo,
                        "folder_path": folder_path,
                        "image_path": image_path,
                        "prediction": pred_row["species"],
                        "confidence": pred_row["confidence"],
                        "timestamp": timestamp,
                        "filename": photo["filename"],
                        "embedding": embedding,
                        "taxonomy": None,
                        "_existing": True,
                    }
                )
            continue

        img = load_image(image_path, max_size=None)
        if img is None:
            failed += 1
            continue

        # Crop to detected subject with padding
        primary = detection_map.get(photo["id"])
        if primary:
            iw, ih = img.size
            box = primary["box"]
            pad_w = box["w"] * 0.2
            pad_h = box["h"] * 0.2
            x1 = max(0, int((box["x"] - pad_w) * iw))
            y1 = max(0, int((box["y"] - pad_h) * ih))
            x2 = min(iw, int((box["x"] + box["w"] + pad_w) * iw))
            y2 = min(ih, int((box["y"] + box["h"] + pad_h) * ih))
            crop = img.crop((x1, y1, x2, y2))
            if crop.size[0] >= 50 and crop.size[1] >= 50:
                img = crop

        img.thumbnail((1024, 1024), Image.LANCZOS)

        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp_path = tmp.name
            img.save(tmp_path, quality=85)

        try:
            if model_type == "timm":
                all_preds = clf.classify(tmp_path, threshold=0)
                embedding = None
            else:
                all_preds, embedding = clf.classify_with_embedding(
                    tmp_path, threshold=0
                )
        except Exception:
            log.warning(
                "Classification failed for %s", photo["filename"], exc_info=True
            )
            failed += 1
            continue
        finally:
            os.unlink(tmp_path)

        if embedding is not None:
            db.store_photo_embedding(photo["id"], embedding.tobytes())

        if not all_preds:
            continue

        top = all_preds[0]
        log.info(
            '%s: "%s" at %.0f%%',
            photo["filename"],
            top["species"],
            top["score"] * 100,
        )

        timestamp = None
        if photo["timestamp"]:
            try:
                timestamp = dt.fromisoformat(photo["timestamp"])
            except Exception:
                pass

        raw_results.append(
            {
                "photo": photo,
                "folder_path": folder_path,
                "image_path": image_path,
                "prediction": top["species"],
                "confidence": top["score"],
                "timestamp": timestamp,
                "filename": photo["filename"],
                "embedding": embedding,
                "taxonomy": top.get("taxonomy"),
            }
        )

    return raw_results, failed, skipped_existing


def run_classify_job(job, runner, db_path, workspace_id, params):
    """Execute classification job. Called by JobRunner in a background thread.

    Args:
        job: job dict from JobRunner (has id, progress, errors, etc.)
        runner: JobRunner instance for push_event()
        db_path: path to SQLite database
        workspace_id: active workspace ID
        params: ClassifyParams with request parameters
    """
    raise NotImplementedError("TODO: move work() body here")
