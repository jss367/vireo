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
    from image_loader import load_image, load_working_image
except ImportError:
    load_image = None
    load_working_image = None

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


def _detect_batch(photos, folders, runner, job, reclassify, db,
                   det_conf_threshold=None, already_detected_ids=None,
                   cached_detections=None):
    """Run MegaDetector on a batch of photos.

    Same interface as _detect_subjects but designed to be called with
    partial photo lists for interleaved detect+classify in the streaming
    pipeline.  Does NOT push progress events — that is the caller's
    responsibility.

    Args:
        det_conf_threshold: Detection confidence threshold. If None,
            loaded from config (fallback for callers that don't pre-load).
        already_detected_ids: Set of photo IDs that already have detections
            in the database. Used for skip-if-already-detected logic.
        cached_detections: Optional dict {photo_id: [detection_dicts]}
            produced by a prior model in the same pipeline run. When
            provided and a photo is in already_detected_ids, the cached
            entries are used instead of db.get_detections() so that
            model 2+ binds to the exact detection rows from this run,
            not stale rows from a previous pipeline pass.

    Returns:
        (detection_map, detected_count, processed_ids) where detection_map
        is {photo_id: [list_of_detection_dicts]}, detected_count is total
        photos with at least one detection, and processed_ids is the set
        of photo IDs whose per-photo iteration completed without raising
        (callers use this to distinguish "ran and found nothing" from
        "never reached because an earlier photo raised mid-loop").
    """
    detected = 0
    detection_map = {}
    processed_ids: set[int] = set()
    if already_detected_ids is None:
        already_detected_ids = set()
    if cached_detections is None:
        cached_detections = {}

    try:
        if detect_animals is None or get_primary_detection is None:
            return detection_map, detected, processed_ids

        for photo in photos:
            folder_path = folders.get(photo["folder_id"], "")
            image_path = os.path.join(folder_path, photo["filename"])

            # Skip if already detected (unless reclassifying)
            if not reclassify and photo["id"] in already_detected_ids:
                # Prefer cached detections from an earlier model in this
                # same pipeline run so that model 2+ is bound to the
                # detection rows just produced, not stale rows from a
                # prior pipeline pass that db.get_detections() would
                # return when old rows haven't been cleared.
                if cached_detections is not None and photo["id"] in cached_detections:
                    det_list = cached_detections[photo["id"]]
                    if det_list:
                        detection_map[photo["id"]] = det_list
                        detected += 1
                    processed_ids.add(photo["id"])
                    continue
                existing_dets = db.get_detections(photo["id"])
                if existing_dets:
                    det_list = []
                    for d in existing_dets:
                        det_list.append({
                            "id": d["id"],
                            "box_x": d["box_x"],
                            "box_y": d["box_y"],
                            "box_w": d["box_w"],
                            "box_h": d["box_h"],
                            "confidence": d["detector_confidence"],
                            "category": d["category"],
                        })
                    detection_map[photo["id"]] = det_list
                    detected += 1
                    processed_ids.add(photo["id"])
                    continue

            # Resolve threshold lazily on first actual detection call so a
            # batch where every photo hits the cached/already-detected
            # short-circuit doesn't need a working config/db at all (the
            # cached-detections short-circuit test relies on this).
            if det_conf_threshold is None:
                import config as cfg
                # Use workspace-effective config so per-workspace overrides
                # (e.g. bird-photography workspaces lowering the threshold)
                # are honored, not just the bare global default.
                effective_cfg = db.get_effective_config(cfg.load())
                det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)

            detections = detect_animals(image_path, confidence_threshold=det_conf_threshold)

            if detections:
                detected += 1

                # Store ALL detections in the database
                det_ids = db.save_detections(
                    photo["id"], detections, detector_model="MegaDetector"
                )

                # Build detection list with database IDs
                det_list = []
                for det, det_id in zip(detections, det_ids, strict=True):
                    det_list.append({
                        "id": det_id,
                        "box_x": det["box"]["x"],
                        "box_y": det["box"]["y"],
                        "box_w": det["box"]["w"],
                        "box_h": det["box"]["h"],
                        "confidence": det["confidence"],
                        "category": det.get("category", "animal"),
                    })
                detection_map[photo["id"]] = det_list

                # Mark as processed immediately after detection rows are committed
                # so that even if the quality-scoring calls below raise, the
                # reclassify purge in pipeline_job correctly removes the now-stale
                # pre-run detection rows for this photo rather than leaving them in
                # place and allowing future non-reclassify runs to reuse them.
                processed_ids.add(photo["id"])

                # Use highest-confidence detection as primary for quality scoring
                primary = get_primary_detection(detections)
                if primary:
                    det_box = primary["box"]
                    subject_size = det_box["w"] * det_box["h"]

                    if compute_sharpness is not None:
                        overall_sharpness = compute_sharpness(image_path)
                        subject_sharpness = None
                        quality = 0

                        try:
                            from PIL import Image

                            img = Image.open(image_path)
                            try:
                                iw, ih = img.size
                                px = int(det_box["x"] * iw)
                                py = int(det_box["y"] * ih)
                                pw = int(det_box["w"] * iw)
                                ph = int(det_box["h"] * ih)
                                subject_sharpness = compute_sharpness(
                                    image_path, region=(px, py, pw, ph)
                                )
                            finally:
                                img.close()
                        except Exception:
                            subject_sharpness = overall_sharpness

                        if subject_sharpness is not None and subject_size is not None:
                            norm_sharp = min(1.0, math.log1p(subject_sharpness) / 10.0)
                            norm_size = min(1.0, subject_size * 4)
                            quality = round(0.7 * norm_sharp + 0.3 * norm_size, 4)

                        db.update_photo_quality(
                            photo["id"],
                            subject_sharpness=subject_sharpness,
                            subject_size=subject_size,
                            quality_score=quality,
                            sharpness=overall_sharpness,
                        )
                    else:
                        db.update_photo_quality(
                            photo["id"],
                        )

            processed_ids.add(photo["id"])

    except (ImportError, RuntimeError):
        pass
    except Exception:
        log.warning("Detection failed for batch (non-fatal)", exc_info=True)

    return detection_map, detected, processed_ids


def _detect_subjects(photos, folders, runner, job, reclassify, db):
    """Run MegaDetector on photos, storing quality metrics.

    Wraps _detect_batch with progress reporting for the standalone classify job.

    Returns:
        (detection_map, detected_count) where detection_map is
        {photo_id: [list_of_detection_dicts]} and detected_count is total
        photos with at least one detection.
    """
    total = len(photos)

    # Resolve cached-detection state before running MegaDetector so we can skip
    # the weight download entirely when every photo already has a detection row.
    already_detected_ids = (
        db.get_existing_detection_photo_ids() if not reclassify else set()
    )

    if detect_animals is not None and get_primary_detection is not None:
        # Require at least one photo — a no-op reclassify over 0 photos should
        # not trigger a ~300 MB MegaDetector download.
        needs_fresh_detection = bool(photos) and (
            reclassify or any(
                p["id"] not in already_detected_ids for p in photos
            )
        )
        if needs_fresh_detection:
            from detector import ensure_megadetector_weights

            def _dl_progress(phase, current, total_steps):
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total_steps,
                        "current_file": "",
                        "phase": f"Step 4/5: {phase}",
                    },
                )

            ensure_megadetector_weights(progress_callback=_dl_progress)

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

        # Load config once for the entire detection loop. Use the
        # workspace-effective config so per-workspace overrides apply.
        import config as cfg
        effective_cfg = db.get_effective_config(cfg.load())
        det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)

        # Process one photo at a time so we can report per-photo progress
        detection_map = {}
        detected = 0
        skipped_det = 0
        start_time = job.get("_start_time", time.time())

        for i, photo in enumerate(photos):
            runner.update_step(
                job["id"], "detect",
                progress={"current": i + 1, "total": total},
            )
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

            was_cached = (
                not reclassify
                and photo["id"] in already_detected_ids
            )

            batch_map, batch_detected, _batch_processed = _detect_batch(
                [photo], folders, runner, job, reclassify, db,
                det_conf_threshold=det_conf_threshold,
                already_detected_ids=already_detected_ids,
            )
            detection_map.update(batch_map)
            detected += batch_detected

            if was_cached and batch_detected:
                skipped_det += 1

        log.info(
            "Detection done: %d animals detected out of %d photos (%d skipped, already detected)",
            detected,
            total,
            skipped_det,
        )
    except (ImportError, RuntimeError) as e:
        msg = str(e)
        if "ONNX model not available" in msg or "not found" in msg:
            log.warning(
                "MegaDetector weights not available — detection skipped; classifying full images. "
                "Download the MegaDetector V6 ONNX model from the pipeline models page to enable "
                "subject detection, cropped classification, and mask extraction."
            )
            job["errors"].append(
                "MegaDetector weights not downloaded — detection skipped. Classification ran on full "
                "images (less accurate) and no detections were stored, which also prevents the mask "
                "extraction stage from producing subject masks. Download MegaDetector V6 from the "
                "pipeline models page to fix."
            )
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": total,
                    "current_file": "",
                    "phase": "Step 4/5: Detection skipped — MegaDetector weights not downloaded",
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
        detection_map = {}
        detected = 0
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
        detection_map = {}
        detected = 0

    return detection_map, detected


_BATCH_SIZE = 16


def _prepare_image(photo, folders, detection, vireo_dir=None):
    """Load and crop a photo to a specific detection's bounding box.

    Args:
        photo: photo dict
        folders: {folder_id: path} mapping
        detection: detection dict with box_x, box_y, box_w, box_h keys
            (or None for full image classification)
        vireo_dir: optional path to ~/.vireo/; when set, tries to load the
            pre-extracted working copy JPEG before falling back to the
            original file via load_image().

    Returns:
        (PIL.Image, folder_path, image_path) or (None, folder_path, image_path) on failure.
    """
    from PIL import Image

    folder_path = folders.get(photo["folder_id"], "")
    image_path = os.path.join(folder_path, photo["filename"])

    img = None
    if vireo_dir and load_working_image is not None:
        img = load_working_image(photo, vireo_dir, max_size=None, folders=folders)
    if img is None:
        img = load_image(image_path, max_size=None)
    if img is None:
        return None, folder_path, image_path

    # Crop to detection bounding box with padding
    if detection:
        iw, ih = img.size
        pad_w = detection["box_w"] * 0.2
        pad_h = detection["box_h"] * 0.2
        x1 = max(0, int((detection["box_x"] - pad_w) * iw))
        y1 = max(0, int((detection["box_y"] - pad_h) * ih))
        x2 = min(iw, int((detection["box_x"] + detection["box_w"] + pad_w) * iw))
        y2 = min(ih, int((detection["box_y"] + detection["box_h"] + pad_h) * ih))
        crop = img.crop((x1, y1, x2, y2))
        if crop.size[0] >= 50 and crop.size[1] >= 50:
            img.close()
            img = crop
        else:
            crop.close()

    img.thumbnail((1024, 1024), Image.LANCZOS)
    return img, folder_path, image_path


def _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=1):
    """Classify a batch of prepared images and append results.

    Returns the number of failures within this batch.
    """
    from datetime import datetime as dt

    images = [entry["img"] for entry in batch]
    failed = 0

    try:
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

            # Build alternatives list (predictions 2..top_k)
            alternatives = []
            for alt_pred in all_preds[1:top_k]:
                alternatives.append({
                    "species": alt_pred["species"],
                    "confidence": alt_pred["score"],
                    "taxonomy": alt_pred.get("taxonomy"),
                })

            raw_results.append(
                {
                    "photo": entry["photo"],
                    "detection_id": entry.get("detection_id"),
                    "folder_path": entry["folder_path"],
                    "image_path": entry["image_path"],
                    "prediction": top["species"],
                    "confidence": top["score"],
                    "timestamp": timestamp,
                    "filename": entry["photo"]["filename"],
                    "embedding": embedding,
                    "taxonomy": top.get("taxonomy"),
                    "alternatives": alternatives,
                }
            )
    finally:
        # Close all PIL images to avoid resource leaks
        for entry in batch:
            entry["img"].close()

    return failed


def _classify_photos(
    photos, folders, detection_map, existing_preds, clf, model_type,
    model_name, runner, job, db, top_k=1, vireo_dir=None,
):
    """Classify detections in batches, cropping to each detection's bounding box.

    For each photo, iterates over all detections (from detection_map) and
    classifies each one independently. Photos without detections are
    classified as full images.

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
        runner.update_step(
            job["id"], "classify",
            progress={"current": i + 1, "total": total},
        )
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
                failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=top_k)
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
                        "detection_id": pred_row["detection_id"],
                        "folder_path": folder_path,
                        "image_path": image_path,
                        "prediction": pred_row["species"],
                        "confidence": pred_row["confidence"],
                        "timestamp": timestamp,
                        "filename": photo["filename"],
                        "embedding": embedding,
                        "taxonomy": None,
                        "alternatives": [],
                        "_existing": True,
                    }
                )
            continue

        # Get detections for this photo (list of detection dicts with IDs)
        photo_detections = detection_map.get(photo["id"], [])

        if photo_detections:
            # Classify each detection independently
            for detection in photo_detections:
                img, det_folder_path, det_image_path = _prepare_image(
                    photo, folders, detection, vireo_dir=vireo_dir
                )
                if img is None:
                    failed += 1
                    continue

                batch.append({
                    "photo": photo,
                    "detection_id": detection["id"],
                    "folder_path": det_folder_path,
                    "image_path": det_image_path,
                    "img": img,
                })

                if len(batch) >= _BATCH_SIZE:
                    failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=top_k)
                    batch = []
        else:
            # No detections — create a full-image detection and classify it
            full_image_det = [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
                               "confidence": 0, "category": "animal"}]
            full_det_ids = db.save_detections(photo["id"], full_image_det,
                                              detector_model="full-image")
            img, folder_path, image_path = _prepare_image(photo, folders, None, vireo_dir=vireo_dir)
            if img is None:
                failed += 1
                continue

            batch.append({
                "photo": photo,
                "detection_id": full_det_ids[0],
                "folder_path": folder_path,
                "image_path": image_path,
                "img": img,
            })

            if len(batch) >= _BATCH_SIZE:
                failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=top_k)
                batch = []

    # Flush remaining images
    if batch:
        failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=top_k)

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
                detection_id=item["detection_id"],
                species=item["prediction"],
                confidence=round(item["confidence"], 4),
                model=model_name,
                category=category,
                taxonomy=tax_hierarchy,
            )
            # Store alternative predictions
            for alt in item.get("alternatives", []):
                alt_tax = alt.get("taxonomy") or (
                    tax.get_hierarchy(alt["species"]) if tax else {}
                )
                db.add_prediction(
                    detection_id=item["detection_id"],
                    species=alt["species"],
                    confidence=round(alt["confidence"], 4),
                    model=model_name,
                    category=category,
                    status="alternative",
                    taxonomy=alt_tax,
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
                        detection_id=item["detection_id"],
                        model=model_name,
                        group_id=gid,
                        vote_count=cons["vote_count"],
                        total_votes=cons["total_votes"],
                        individual=individual_json,
                    )
                else:
                    db.add_prediction(
                        detection_id=item["detection_id"],
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
                    # Store alternative predictions for this group member
                    for alt in item.get("alternatives", []):
                        alt_tax = alt.get("taxonomy") or (
                            tax.get_hierarchy(alt["species"]) if tax else {}
                        )
                        db.add_prediction(
                            detection_id=item["detection_id"],
                            species=alt["species"],
                            confidence=round(alt["confidence"], 4),
                            model=model_name,
                            category=category,
                            status="alternative",
                            taxonomy=alt_tax,
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


def run_classify_job(job, runner, db_path, workspace_id, params, vireo_dir=None):
    """Execute classification job. Called by JobRunner in a background thread.

    Args:
        job: job dict from JobRunner (has id, progress, errors, etc.)
        runner: JobRunner instance for push_event()
        db_path: path to SQLite database
        workspace_id: active workspace ID
        params: ClassifyParams with request parameters
        vireo_dir: optional path to ~/.vireo/; when set, classification uses
            pre-extracted working copy JPEGs instead of decoding RAW files.
    """
    thread_db = Database(db_path)
    try:
        thread_db.set_active_workspace(workspace_id)
        job["_start_time"] = time.time()

        runner.set_steps(job["id"], [
            {"id": "load_taxonomy", "label": "Load taxonomy"},
            {"id": "load_photos", "label": "Load photos"},
            {"id": "load_model", "label": "Load model"},
            {"id": "detect", "label": "Detect subjects"},
            {"id": "classify", "label": "Classify species"},
            {"id": "finalize", "label": "Finalize results"},
        ])

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
        runner.update_step(job["id"], "load_taxonomy", status="running")
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
        from taxonomy import load_local_taxonomy
        tax = load_local_taxonomy()

        # Phase 2: Load labels
        labels, use_tol = _load_labels(
            model_type=model_type,
            model_str=model_str,
            labels_file=params.labels_file,
            labels_files=params.labels_files,
            db=thread_db,
        )
        tax_summary = "Taxonomy loaded" if tax else "No taxonomy"
        labels_summary = f"{len(labels)} labels" if labels else ("Tree of Life" if use_tol else "no labels")
        runner.update_step(
            job["id"], "load_taxonomy", status="completed",
            summary=f"{tax_summary}, {labels_summary}",
        )

        # Phase 3: Get photos from collection
        runner.update_step(job["id"], "load_photos", status="running")
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
        runner.update_step(
            job["id"], "load_photos", status="completed",
            summary=f"{total} photos",
        )

        log.info(
            "Classifying %d photos with '%s' (%s)", total, effective_name, model_str
        )

        if params.reclassify:
            photo_ids = [p["id"] for p in photos]
            thread_db.clear_predictions(model=effective_name, collection_photo_ids=photo_ids)
            # Also clear existing detections so they get re-detected
            for pid in photo_ids:
                thread_db.clear_detections(pid)
            log.info(
                "Cleared existing predictions and detections for %d photos, model=%s (re-classify)",
                len(photo_ids),
                effective_name,
            )

        # Phase 4: Initialize classifier
        runner.update_step(job["id"], "load_model", status="running")
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
                runner.update_step(
                    job["id"], "load_model",
                    progress={"current": current, "total": emb_total},
                )
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
        runner.update_step(
            job["id"], "load_model", status="completed",
            summary=effective_name,
        )

        # Phase 5: Detect subjects
        runner.update_step(job["id"], "detect", status="running")
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=params.reclassify,
            db=thread_db,
        )
        runner.update_step(
            job["id"], "detect", status="completed",
            summary=f"{detected} animals detected in {total} photos",
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

        import config as cfg
        effective_cfg = thread_db.get_effective_config(cfg.load())
        top_k = effective_cfg.get("top_k_predictions", 5)

        runner.update_step(job["id"], "classify", status="running")
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
            top_k=top_k,
            vireo_dir=vireo_dir,
        )
        classified_count = len(raw_results) - skipped_existing
        parts = [f"{classified_count} classified"]
        if skipped_existing:
            parts.append(f"{skipped_existing} cached")
        if failed:
            parts.append(f"{failed} failed")
        runner.update_step(
            job["id"], "classify", status="completed",
            summary=", ".join(parts),
        )

        # Phase 7: Group and store predictions
        runner.update_step(job["id"], "finalize", status="running")
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
        finalize_parts = [f"{group_result['predictions_stored']} predictions"]
        if group_result["burst_groups"]:
            finalize_parts.append(f"{group_result['burst_groups']} burst groups")
        if group_result["already_labeled"]:
            finalize_parts.append(f"{group_result['already_labeled']} already labeled")
        runner.update_step(
            job["id"], "finalize", status="completed",
            summary=", ".join(finalize_parts),
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
    finally:
        thread_db.conn.close()
