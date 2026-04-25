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


def _record_labels_fingerprint(db, fingerprint, labels, sources):
    """Populate the labels_fingerprints sidecar. Cosmetic — powers UX lookups."""
    display = ", ".join(os.path.basename(s) for s in (sources or [])) or None
    db.upsert_labels_fingerprint(
        fingerprint=fingerprint,
        display_name=display,
        sources=sources,
        label_count=len(labels or []),
    )


def _run_classifier_on_detection(db, detection_id, classifier_model, labels,
                                  labels_fingerprint, classify_fn=None):
    """Run the classifier for a single detection and persist results.

    This is a thin adapter that the gate wrapper calls. ``classify_fn`` is an
    injection seam for the higher-level classify/pipeline code that already
    has a loaded model bundle and prepared image — it should return a list of
    prediction dicts that get stored in the ``predictions`` table for this
    (detection, classifier_model, labels_fingerprint) triple.

    Returns the list of prediction dicts that were stored (may be empty).
    """
    if classify_fn is None:
        # No classifier plugged in — return [] without side effects. The
        # gate wrapper treats a zero-prediction return as a failed attempt
        # and does NOT record a classifier_run row, so the next call will
        # retry. Used in tests that just exercise the gating logic without
        # actually running a model.
        return []

    predictions = classify_fn() or []
    # Persist predictions with the new (classifier_model, labels_fingerprint)
    # identity. INSERT OR REPLACE on the UNIQUE
    # (detection_id, classifier_model, labels_fingerprint, species) so a
    # re-classify with reclassify=True refreshes the row in place.
    for pred in predictions:
        species = pred.get("species")
        if not species:
            continue
        confidence = pred.get("confidence") or pred.get("score")
        tax = pred.get("taxonomy") or {}
        db.conn.execute(
            """INSERT OR REPLACE INTO predictions
                (detection_id, classifier_model, labels_fingerprint, species,
                 confidence, category, scientific_name,
                 taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                 taxonomy_order, taxonomy_family, taxonomy_genus)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                detection_id,
                classifier_model,
                labels_fingerprint,
                species,
                confidence,
                pred.get("category", "new"),
                tax.get("scientific_name"),
                tax.get("kingdom"),
                tax.get("phylum"),
                tax.get("class"),
                tax.get("order"),
                tax.get("family"),
                tax.get("genus"),
            ),
        )
    db.conn.commit()
    return predictions


def _classify_detection_gated(db, detection_id, classifier_model,
                               labels_fingerprint, labels, reclassify,
                               classify_fn=None):
    """Run the classifier only if we haven't already for this triple.

    The gate is keyed on (detection_id, classifier_model, labels_fingerprint):
    if a row exists in classifier_runs and reclassify is False, the classifier
    is not invoked. After a successful invocation that produced at least one
    prediction, the classifier_runs row is written (or refreshed) so
    subsequent passes skip.

    Mirrors ``_record_batch_classifier_runs`` and the inline pipeline_job
    guard: a zero-count run is treated as a failed attempt, not a completed
    one. Recording it would permanently strand the detection on the next
    non-reclassify pass — the cache would claim "done" with no rows to show.
    """
    if not reclassify:
        existing = db.get_classifier_run_keys(detection_id)
        if (classifier_model, labels_fingerprint) in existing:
            return []
    predictions = _run_classifier_on_detection(
        db, detection_id, classifier_model, labels,
        labels_fingerprint=labels_fingerprint,
        classify_fn=classify_fn,
    )
    if predictions:
        db.record_classifier_run(
            detection_id, classifier_model, labels_fingerprint,
            prediction_count=len(predictions),
        )
    return predictions


def _resolve_label_sources(params, db):
    """Return list of source file paths used to build the active label set.

    Mirrors the lookup order in _load_labels — but only produces the source
    paths so the caller can stash them on the labels_fingerprints row.
    """
    if params.labels_files and isinstance(params.labels_files, list):
        return list(params.labels_files)
    if params.labels_file:
        return [params.labels_file]
    ws_labels = db.get_workspace_active_labels() if db else None
    if ws_labels is not None:
        return list(ws_labels)
    active_sets = get_active_labels()
    return [s.get("labels_file") for s in (active_sets or []) if s.get("labels_file")]


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

            # Skip if already detected (unless reclassifying). After the
            # detector_runs migration, `already_detected_ids` includes
            # empty-scene photos (box_count=0) — we must not re-invoke
            # MegaDetector for them either. Either the detector produced
            # rows (reuse them) or it ran and found nothing (skip entirely).
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
                # Pull cached rows if any; an empty result means this photo
                # was scanned and had no animals, which is still a skip.
                # (Task 20 will add a min_conf filter to get_detections.)
                try:
                    existing_dets = db.get_detections(photo["id"])
                except Exception:
                    existing_dets = []
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

            # Resolve workspace-effective threshold lazily on first actual
            # detection call so a batch where every photo hits the
            # cached/already-detected short-circuit doesn't need a working
            # config/db at all (the cached-detections short-circuit test
            # relies on this).
            #
            # The threshold is NOT passed to detect_animals — detector writes
            # everything above RAW_CONF_FLOOR so results can be globally
            # cached. The effective threshold is applied as a read-time
            # filter by get_detections / stats queries (Tasks 20-22).
            if det_conf_threshold is None:
                import config as cfg
                effective_cfg = db.get_effective_config(cfg.load())
                det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)

            detections = detect_animals(image_path)

            if detections is None:
                # Detector run itself failed (image decode error, ONNX
                # error, etc.). Do NOT clear prior detections and do NOT
                # record a run — otherwise future non-reclassify passes
                # would skip the photo permanently, leaving it without
                # detections unless the user forces --reclassify.
                # The photo stays out of processed_ids so the caller
                # treats it as "will be retried next pass".
                continue

            # Persist detection rows and record the detector run atomically —
            # `write_detection_batch` wraps both writes in one transaction so a
            # crash between them can't leave a torn state (detections without a
            # matching detector_runs row, or the reverse for empty scenes).
            # Failures from `detect_animals` were handled by the ``is None``
            # early-continue above and must not poison the skip set.
            det_ids = db.write_detection_batch(
                photo["id"], "megadetector-v6", detections,
            )

            if detections:
                detected += 1

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

            if detections:
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
    # the weight download entirely when every photo already has a detector_runs
    # row (including empty-scene rows with box_count=0).
    already_detected_ids = (
        db.get_detector_run_photo_ids("megadetector-v6") if not reclassify else set()
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
                db.upsert_photo_embedding(
                    entry["photo"]["id"], model_name, embedding.tobytes()
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
    labels_fingerprint=None, reclassify=False,
):
    """Classify detections in batches, cropping to each detection's bounding box.

    For each photo, iterates over all detections (from detection_map) and
    classifies each one independently. Photos without detections are
    classified as full images.

    Images are passed directly to classifiers as PIL objects (no temp file I/O).
    Multiple images are batched into a single forward pass for throughput.

    A per-detection classifier_runs gate keyed on (detection_id, model_name,
    labels_fingerprint) short-circuits re-work when the same triple already
    ran. reclassify=True bypasses the gate.

    Returns:
        (raw_results, failed_count, skipped_existing_count)
    """
    # Fall back to the legacy sentinel when the caller didn't compute a
    # fingerprint — matches the default used by classifier_runs.
    fp = labels_fingerprint or "legacy"
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

        # Get detections for this photo (list of detection dicts with IDs)
        photo_detections = detection_map.get(photo["id"], [])

        if photo_detections:
            # Classify each detection independently.
            #
            # No photo-level short-circuit here: a prior short-circuit that
            # skipped photos with any cached prediction under (model, fp)
            # silently dropped newly-surfaced detections after the user
            # lowered `detector_confidence`, leaving them unclassified
            # until --reclassify. The per-detection classifier_runs gate
            # below handles incremental work correctly.
            timestamp = None
            if photo["timestamp"]:
                try:
                    timestamp = dt.fromisoformat(photo["timestamp"])
                except Exception:
                    pass

            for detection in photo_detections:
                # Classifier-run gate: if (detection, model, fingerprint)
                # has a run key AND has cached prediction rows, surface the
                # cached top-1 and skip inference. If the run key exists
                # but no cached rows do (e.g. the prior pass stored
                # `category == 'match'` which is intentionally not written,
                # or transient ordering between record_classifier_run and
                # _store_grouped_predictions), DON'T short-circuit —
                # otherwise the photo is stranded until the user forces
                # --reclassify. Fall through to re-classify instead.
                if not reclassify:
                    run_keys = db.get_classifier_run_keys(detection["id"])
                    if (model_name, fp) in run_keys:
                        cached = db.get_predictions_for_detection(
                            detection["id"],
                            classifier_model=model_name,
                            labels_fingerprint=fp,
                            min_classifier_conf=0,
                        )
                        if cached:
                            skipped_existing += 1
                            top = cached[0]  # ordered by confidence DESC
                            embedding = None
                            if model_type != "timm":
                                emb_blob = db.get_photo_embedding(
                                    photo["id"], model_name,
                                )
                                if emb_blob:
                                    import numpy as np
                                    embedding = np.frombuffer(
                                        emb_blob, dtype=np.float32,
                                    )
                            raw_results.append({
                                "photo": photo,
                                "detection_id": detection["id"],
                                "folder_path": folder_path,
                                "image_path": image_path,
                                "prediction": top["species"],
                                "confidence": top["confidence"],
                                "timestamp": timestamp,
                                "filename": photo["filename"],
                                "embedding": embedding,
                                "taxonomy": None,
                                "alternatives": [],
                                "_existing": True,
                            })
                            continue
                        # Run key without cached rows → fall through to
                        # classify this detection.

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
                    _record_batch_classifier_runs(db, batch, model_name, fp, raw_results)
                    batch = []
        else:
            # No detections — use (or create) a full-image synthetic detection
            # to carry the classifier output.
            #
            # save_detections() does clear-and-reinsert per
            # (photo_id, detector_model), so calling it on every pass would
            # generate a new id each time and cascade-delete prior predictions
            # and classifier_runs tied to the old id. Reuse the existing
            # full-image detection when one is already cached, and only
            # create a fresh one if none exists (or if the caller asked for
            # a reclassify).
            # min_conf=0 because the synthetic full-image detection is
            # written with confidence=0 — the default threshold filter would
            # hide it.
            existing_full = db.get_detections(
                photo["id"], detector_model="full-image", min_conf=0,
            )
            if existing_full and not reclassify:
                full_det_id = existing_full[0]["id"]
            else:
                full_image_det = [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
                                   "confidence": 0, "category": "animal"}]
                full_det_ids = db.save_detections(
                    photo["id"], full_image_det,
                    detector_model="full-image",
                )
                full_det_id = full_det_ids[0]
            # Gate check for the synthetic full-image detection too.
            # Mirror the regular detection branch: when gated, surface the
            # cached top-1 prediction into raw_results so downstream
            # grouping/storage still sees it. Without this, non-reclassify
            # reruns silently drop cached full-image photos even though
            # those photos were intentionally kept in the cache.
            if not reclassify:
                run_keys = db.get_classifier_run_keys(full_det_id)
                if (model_name, fp) in run_keys:
                    cached = db.get_predictions_for_detection(
                        full_det_id,
                        classifier_model=model_name,
                        labels_fingerprint=fp,
                        min_classifier_conf=0,
                    )
                    if cached:
                        skipped_existing += 1
                        top = cached[0]
                        timestamp = None
                        if photo["timestamp"]:
                            try:
                                timestamp = dt.fromisoformat(photo["timestamp"])
                            except Exception:
                                pass
                        embedding = None
                        if model_type != "timm":
                            emb_blob = db.get_photo_embedding(
                                photo["id"], model_name,
                            )
                            if emb_blob:
                                import numpy as np
                                embedding = np.frombuffer(
                                    emb_blob, dtype=np.float32,
                                )
                        raw_results.append({
                            "photo": photo,
                            "detection_id": full_det_id,
                            "folder_path": folder_path,
                            "image_path": image_path,
                            "prediction": top["species"],
                            "confidence": top["confidence"],
                            "timestamp": timestamp,
                            "filename": photo["filename"],
                            "embedding": embedding,
                            "taxonomy": None,
                            "alternatives": [],
                            "_existing": True,
                        })
                        continue
                    # Run key without cached rows → fall through to
                    # re-classify this full-image detection.
            img, folder_path, image_path = _prepare_image(photo, folders, None, vireo_dir=vireo_dir)
            if img is None:
                failed += 1
                continue

            batch.append({
                "photo": photo,
                "detection_id": full_det_id,
                "folder_path": folder_path,
                "image_path": image_path,
                "img": img,
            })

            if len(batch) >= _BATCH_SIZE:
                failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=top_k)
                _record_batch_classifier_runs(db, batch, model_name, fp, raw_results)
                batch = []

    # Flush remaining images
    if batch:
        failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=top_k)
        _record_batch_classifier_runs(db, batch, model_name, fp, raw_results)

    return raw_results, failed, skipped_existing


def _record_batch_classifier_runs(db, batch, model_name, labels_fingerprint, raw_results):
    """Record classifier_runs rows for every detection in ``batch``.

    ``batch`` is the list of entries that were just passed to _flush_batch;
    ``raw_results`` may already contain entries from prior batches, so we scope
    the prediction_count lookup to entries that reference this batch's
    detection_ids.  Called after _flush_batch has committed predictions so the
    run row is only written for detections that actually produced output.
    """
    if not batch:
        return
    # Tally how many raw_results entries reference each detection_id. Entries
    # without a detection_id (unusual, but possible on synthesized rows) are
    # ignored. For a per-detection batch this is typically 0 or 1.
    counts: dict = {}
    for r in raw_results:
        did = r.get("detection_id")
        if did is not None:
            counts[did] = counts.get(did, 0) + 1
    seen: set = set()
    for entry in batch:
        did = entry.get("detection_id")
        if did is None or did in seen:
            continue
        seen.add(did)
        # Only record the run for detections that actually produced a
        # prediction. A count of 0 means the classifier failed (transient
        # load error, decode error, etc.) — caching it as "done" would
        # permanently strand the detection on the next non-reclassify run.
        n = counts.get(did, 0)
        if n <= 0:
            continue
        db.record_classifier_run(
            did, model_name, labels_fingerprint,
            prediction_count=n,
        )


def _store_grouped_predictions(
    raw_results, job_id, model_name, grouping_window, similarity_threshold, tax, db,
    labels_fingerprint="legacy",
):
    """Group results by timestamp/similarity, compute consensus, store to DB.

    ``labels_fingerprint`` is written verbatim onto each prediction row so
    the fingerprint-aware skip gate (``get_existing_prediction_photo_ids``)
    actually finds them. Defaulting to ``'legacy'`` would make cache
    lookups miss and force reclassification on every pass — callers must
    pass the active fingerprint.

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
                labels_fingerprint=labels_fingerprint,
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
                    labels_fingerprint=labels_fingerprint,
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
                        labels_fingerprint=labels_fingerprint,
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
                        labels_fingerprint=labels_fingerprint,
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
                            labels_fingerprint=labels_fingerprint,
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
        # Compute a content-addressable fingerprint for the active label set.
        # Kept in scope so downstream classifier_runs writes can record the
        # exact (classifier_model, labels_fingerprint) that produced a result.
        from labels_fingerprint import compute_fingerprint
        fp = compute_fingerprint(labels)
        label_sources = _resolve_label_sources(params, thread_db)
        _record_labels_fingerprint(thread_db, fp, labels, sources=label_sources)

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

        # Phase 4: Initialize classifier
        # The reclassify purge (destructive clears of detections + predictions +
        # cascaded review state) is deferred until AFTER the classifier
        # initializes. Running it before model load means any weight-load
        # failure leaves affected photos with no predictions AND no
        # detections AND no replacement results — shared-folder workspaces
        # lose their cached state too. Deferring preserves the cache on
        # setup failure; users see a clean error and their workspace is
        # unchanged.
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

        # Classifier init succeeded — now it's safe to purge existing
        # cache for reclassify. Any failure before this point leaves the
        # cache intact (see comment at the top of this function).
        if params.reclassify:
            photo_ids = [p["id"] for p in photos]
            thread_db.clear_predictions(
                model=effective_name, collection_photo_ids=photo_ids,
            )
            # Also clear existing detections so they get re-detected.
            for pid in photo_ids:
                thread_db.clear_detections(pid)
            log.info(
                "Cleared existing predictions and detections for %d photos, "
                "model=%s (re-classify, post-model-load)",
                len(photo_ids), effective_name,
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

        # Phase 6: Classify each photo. The per-detection classifier_runs
        # gate inside _classify_photos skips already-done detections and
        # still surfaces their cached predictions into raw_results, so a
        # photo-level short-circuit is both unnecessary and actively
        # harmful (it hides newly-surfaced detections after the user
        # lowers detector_confidence).
        existing_preds = set()

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
            labels_fingerprint=fp,
            reclassify=params.reclassify,
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
            labels_fingerprint=fp,
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
