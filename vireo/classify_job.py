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

from db import Database
from models import get_active_model, get_models

try:
    from classifier import Classifier
except ImportError:
    Classifier = None

try:
    from timm_classifier import TimmClassifier
except ImportError:
    TimmClassifier = None

log = logging.getLogger(__name__)


@dataclass
class ClassifyParams:
    """Parameters for a classification job, parsed from the request body."""

    collection_id: str
    labels_file: str | None
    labels_files: list | None
    model_id: str | None
    model_name: str | None
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


def _load_labels(model_type, model_str, labels_file, labels_files, db=None):
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
        # Try workspace-scoped active labels first
        ws_labels = db.get_workspace_active_labels() if db else None
        if ws_labels is not None:
            saved = get_saved_labels()
            saved_by_file = {s["labels_file"]: s for s in saved}
            active_sets = []
            for p in ws_labels:
                meta = saved_by_file.get(p, {"labels_file": p})
                active_sets.append(meta)
            labels = load_merged_labels(active_sets)
            names = [s.get("name", "?") for s in active_sets]
            log.info(
                "Using %d merged labels from workspace active sets: %s",
                len(labels),
                ", ".join(names),
            )
        else:
            active_sets = get_active_labels()
            if active_sets:
                labels = load_merged_labels(active_sets)
                names = [s.get("name", "?") for s in active_sets]
                log.info(
                    "Using %d merged labels from global active sets: %s",
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
                "MegaDetector ONNX model not available — cannot run detection"
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

        import config as cfg
        det_conf_threshold = cfg.load().get("detector_confidence", 0.2)

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

            detections = detect_animals(image_path, confidence_threshold=det_conf_threshold)
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
        if "ONNX model not available" in msg or "not found" in msg:
            log.info(
                "MegaDetector not available — skipping detection (classifying full images)"
            )
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": total,
                    "current_file": "",
                    "phase": "Step 4/5: Detection skipped (MegaDetector not available)",
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


_BATCH_SIZE = 16


def _prepare_image(photo, folders, detection_map):
    """Load, crop, and resize a photo for classification.

    Returns (PIL.Image, folder_path, image_path) or (None, folder_path, image_path) on failure.
    """
    from PIL import Image

    folder_path = folders.get(photo["folder_id"], "")
    image_path = os.path.join(folder_path, photo["filename"])

    img = load_image(image_path, max_size=None)
    if img is None:
        return None, folder_path, image_path

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
    return img, folder_path, image_path


def _flush_batch(batch, clf, model_type, model_name, db, raw_results):
    """Classify a batch of prepared images and append results.

    Returns the number of failures within this batch.
    """
    from datetime import datetime as dt

    images = [entry["img"] for entry in batch]
    failed = 0

    try:
        if model_type == "timm":
            batch_preds = clf.classify_batch(images, threshold=0)
            batch_results = [(preds, None) for preds in batch_preds]
        else:
            batch_results = clf.classify_batch_with_embedding(images, threshold=0)
    except Exception:
        log.warning("Batch classification failed, falling back to single-image", exc_info=True)
        batch_results = []
        for entry in batch:
            try:
                if model_type == "timm":
                    preds = clf.classify(entry["img"], threshold=0)
                    batch_results.append((preds, None))
                else:
                    preds, emb = clf.classify_with_embedding(entry["img"], threshold=0)
                    batch_results.append((preds, emb))
            except Exception:
                log.warning("Classification failed for %s", entry["photo"]["filename"], exc_info=True)
                batch_results.append(None)
                failed += 1

    for entry, result in zip(batch, batch_results, strict=True):
        if result is None:
            continue
        all_preds, embedding = result

        if embedding is not None:
            db.store_photo_embedding(
                entry["photo"]["id"], embedding.tobytes(), model=model_name
            )

        if not all_preds:
            continue

        top = all_preds[0]
        log.info(
            '%s: "%s" at %.0f%%',
            entry["photo"]["filename"],
            top["species"],
            top["score"] * 100,
        )

        timestamp = None
        if entry["photo"]["timestamp"]:
            try:
                timestamp = dt.fromisoformat(entry["photo"]["timestamp"])
            except Exception:
                pass

        raw_results.append(
            {
                "photo": entry["photo"],
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": top["species"],
                "confidence": top["score"],
                "timestamp": timestamp,
                "filename": entry["photo"]["filename"],
                "embedding": embedding,
                "taxonomy": top.get("taxonomy"),
            }
        )

    return failed


def _classify_photos(
    photos, folders, detection_map, existing_preds, clf, model_type,
    model_name, runner, job, db,
):
    """Classify photos in batches, cropping to detected subject when available.

    Images are passed directly to classifiers as PIL objects (no temp file I/O).
    Multiple images are batched into a single forward pass for throughput.

    Returns:
        (raw_results, failed_count, skipped_existing_count)
    """
    from datetime import datetime as dt

    if load_image is None:
        raise ImportError("image_loader module is required for classification")

    raw_results = []
    failed = 0
    skipped_existing = 0
    total = len(photos)
    batch = []

    start_time = time.time()

    for i, photo in enumerate(photos):
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

        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])

        if photo["id"] in existing_preds:
            # Flush pending batch to preserve photo ordering in raw_results
            if batch:
                failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results)
                batch = []
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

        img, folder_path, image_path = _prepare_image(photo, folders, detection_map)
        if img is None:
            failed += 1
            continue

        batch.append({
            "photo": photo,
            "folder_path": folder_path,
            "image_path": image_path,
            "img": img,
        })

        if len(batch) >= _BATCH_SIZE:
            failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results)
            batch = []

    # Flush remaining images
    if batch:
        failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results)

    return raw_results, failed, skipped_existing


def _store_grouped_predictions(
    raw_results, job_id, model_name, grouping_window, similarity_threshold, tax, db,
):
    """Group results by timestamp/similarity, compute consensus, store to DB.

    Returns:
        dict with predictions_stored, burst_groups, already_labeled counts.
    """
    from compare import categorize
    from grouping import (
        consensus_prediction,
        group_by_timestamp,
        refine_groups_by_similarity,
    )
    from xmp import read_keywords

    groups = group_by_timestamp(raw_results, window_seconds=grouping_window)
    groups = refine_groups_by_similarity(
        groups, similarity_threshold=similarity_threshold
    )
    predictions_stored = 0
    group_count = 0
    skipped_match = 0

    for group in groups:
        if len(group) == 1:
            item = group[0]
            photo = item["photo"]
            folder_path = item["folder_path"]

            category = "new"
            if tax:
                xmp_path = os.path.join(
                    folder_path,
                    os.path.splitext(photo["filename"])[0] + ".xmp",
                )
                existing = read_keywords(xmp_path)
                category = categorize(item["prediction"], existing, tax)

            if category == "match":
                skipped_match += 1
                continue

            tax_hierarchy = item.get("taxonomy") or (
                tax.get_hierarchy(item["prediction"]) if tax else {}
            )
            db.add_prediction(
                photo_id=photo["id"],
                species=item["prediction"],
                confidence=round(item["confidence"], 4),
                model=model_name,
                category=category,
                taxonomy=tax_hierarchy,
            )
            predictions_stored += 1
        else:
            group_count += 1
            gid = f"g{job_id[-6:]}-{group_count:04d}"
            cons_input = [
                {
                    "prediction": item["prediction"],
                    "confidence": item["confidence"],
                }
                for item in group
            ]
            cons = consensus_prediction(cons_input)
            if not cons:
                continue

            representative = group[0]
            category = "new"
            if tax:
                xmp_path = os.path.join(
                    representative["folder_path"],
                    os.path.splitext(representative["photo"]["filename"])[0] + ".xmp",
                )
                existing = read_keywords(xmp_path)
                category = categorize(cons["prediction"], existing, tax)

            if category == "match":
                skipped_match += len(group)
                continue

            individual_json = json.dumps(cons["individual_predictions"])
            rep_tax = group[0].get("taxonomy")
            cons_hierarchy = rep_tax or (
                tax.get_hierarchy(cons["prediction"]) if tax else {}
            )

            for item in group:
                if item.get("_existing"):
                    db.update_prediction_group_info(
                        photo_id=item["photo"]["id"],
                        model=model_name,
                        group_id=gid,
                        vote_count=cons["vote_count"],
                        total_votes=cons["total_votes"],
                        individual=individual_json,
                    )
                else:
                    db.add_prediction(
                        photo_id=item["photo"]["id"],
                        species=item["prediction"],
                        confidence=round(item["confidence"], 4),
                        model=model_name,
                        category=category,
                        group_id=gid,
                        vote_count=cons["vote_count"],
                        total_votes=cons["total_votes"],
                        individual=individual_json,
                        taxonomy=item.get("taxonomy") or cons_hierarchy,
                    )
            predictions_stored += len(group)

    singles = len([g for g in groups if len(g) == 1])
    grouped_photos = sum(len(g) for g in groups if len(g) > 1)
    log.info(
        "Grouping complete: %d predictions stored (%d singles, %d in %d burst groups), "
        "%d already labeled",
        predictions_stored,
        singles,
        grouped_photos,
        group_count,
        skipped_match,
    )

    return {
        "predictions_stored": predictions_stored,
        "burst_groups": group_count,
        "already_labeled": skipped_match,
    }


def run_classify_job(job, runner, db_path, workspace_id, params):
    """Execute classification job. Called by JobRunner in a background thread.

    Args:
        job: job dict from JobRunner (has id, progress, errors, etc.)
        runner: JobRunner instance for push_event()
        db_path: path to SQLite database
        workspace_id: active workspace ID
        params: ClassifyParams with request parameters
    """
    thread_db = Database(db_path)
    thread_db.set_active_workspace(workspace_id)
    job["_start_time"] = time.time()

    # Resolve model
    if params.model_id:
        all_models = get_models()
        active_model = next(
            (m for m in all_models if m["id"] == params.model_id and m["downloaded"]),
            None,
        )
        if not active_model:
            raise RuntimeError(
                f"Model '{params.model_id}' not found or not downloaded."
            )
    else:
        active_model = get_active_model()
    if not active_model:
        raise RuntimeError("No model available. Download one in Settings.")

    model_str = active_model["model_str"]
    weights_path = active_model["weights_path"]
    effective_name = active_model["name"]
    model_type = active_model.get("model_type", "bioclip")
    model_name = params.model_name or effective_name

    # Phase 1: Load taxonomy
    runner.push_event(
        job["id"],
        "progress",
        {
            "current": 0,
            "total": 0,
            "current_file": "Loading taxonomy...",
            "rate": 0,
            "phase": "Step 1/5: Loading taxonomy",
        },
    )
    taxonomy_path = os.path.join(os.path.dirname(__file__), "taxonomy.json")
    tax = _load_taxonomy(taxonomy_path)

    # Phase 2: Load labels
    labels, use_tol = _load_labels(
        model_type=model_type,
        model_str=model_str,
        labels_file=params.labels_file,
        labels_files=params.labels_files,
        db=thread_db,
    )

    # Phase 3: Get photos from collection
    runner.push_event(
        job["id"],
        "progress",
        {
            "current": 0,
            "total": 0,
            "current_file": "Loading collection photos...",
            "rate": 0,
            "phase": "Step 2/5: Loading photos",
        },
    )
    photos = thread_db.get_collection_photos(params.collection_id, per_page=999999)
    folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
    total = len(photos)
    job["progress"]["total"] = total

    log.info(
        "Classifying %d photos with '%s' (%s)", total, effective_name, model_str
    )

    if params.reclassify:
        photo_ids = [p["id"] for p in photos]
        thread_db.clear_predictions(model=effective_name, collection_photo_ids=photo_ids)
        log.info(
            "Cleared existing predictions for %d photos, model=%s (re-classify)",
            len(photo_ids),
            effective_name,
        )

    # Phase 4: Initialize classifier
    if model_type == "timm":
        phase_msg = f"Loading {effective_name} timm model..."
    elif use_tol:
        phase_msg = f"Loading {effective_name} Tree of Life classifier..."
    else:
        phase_msg = f"Loading {effective_name} model and computing label embeddings..."

    runner.push_event(
        job["id"],
        "progress",
        {
            "current": 0,
            "total": total,
            "current_file": phase_msg,
            "rate": 0,
            "phase": "Step 3/5: Loading model",
        },
    )

    if model_type == "timm":
        clf = TimmClassifier(model_str, taxonomy=tax)
    else:
        def _emb_progress(current, emb_total):
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": current,
                    "total": emb_total,
                    "current_file": f"Computing label embeddings ({current}/{emb_total})...",
                    "rate": 0,
                    "phase": "Step 3/5: Computing embeddings",
                },
            )

        clf = Classifier(
            labels=None if use_tol else labels,
            model_str=model_str,
            pretrained_str=weights_path,
            embedding_progress_callback=_emb_progress,
        )

    # Phase 5: Detect subjects
    detection_map, detected = _detect_subjects(
        photos=photos,
        folders=folders,
        runner=runner,
        job=job,
        reclassify=params.reclassify,
        db=thread_db,
    )

    # Phase 6: Classify each photo
    existing_preds = set()
    if not params.reclassify:
        existing_preds = thread_db.get_existing_prediction_photo_ids(model_name)
        if existing_preds:
            log.info(
                "Skipping %d photos with existing predictions (model=%s)",
                len(existing_preds),
                model_name,
            )

    job["_start_time"] = time.time()  # reset rate timer for classification phase

    raw_results, failed, skipped_existing = _classify_photos(
        photos=photos,
        folders=folders,
        detection_map=detection_map,
        existing_preds=existing_preds,
        clf=clf,
        model_type=model_type,
        model_name=model_name,
        runner=runner,
        job=job,
        db=thread_db,
    )

    # Phase 7: Group and store predictions
    runner.push_event(
        job["id"],
        "progress",
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
        model_name=model_name,
        grouping_window=params.grouping_window,
        similarity_threshold=params.similarity_threshold,
        tax=tax,
        db=thread_db,
    )

    log.info(
        "Classification complete: %d photos processed, %d predictions stored, "
        "%d already classified, %d already labeled, %d failed",
        total,
        group_result["predictions_stored"],
        skipped_existing,
        group_result["already_labeled"],
        failed,
    )

    return {
        "total": total,
        "predictions_stored": group_result["predictions_stored"],
        "burst_groups": group_result["burst_groups"],
        "already_classified": skipped_existing,
        "already_labeled": group_result["already_labeled"],
        "detected": detected,
        "failed": failed,
    }
