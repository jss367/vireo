"""Streaming pipeline job -- overlaps I/O stages and interleaves detect+classify.

This module orchestrates the full pipeline (scan -> thumbnails -> classify ->
extract-masks -> regroup) as a single background job with concurrent stages
connected by queues.

Existing standalone jobs (/api/jobs/scan, /api/jobs/classify, etc.) are
untouched. This is an additive orchestration layer.
"""

import contextlib
import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass

from db import Database

log = logging.getLogger(__name__)

_SENTINEL = object()  # unique end-of-stream marker


@dataclass
class PipelineParams:
    """Parameters for a streaming pipeline job."""

    collection_id: int | None = None
    source: str | None = None
    sources: list | None = None
    destination: str | None = None
    file_types: str = "both"
    folder_template: str = "%Y/%m-%d"
    skip_duplicates: bool = True
    labels_file: str | None = None
    labels_files: list | None = None
    model_id: str | None = None
    reclassify: bool = False
    skip_extract_masks: bool = False
    skip_regroup: bool = False
    skip_classify: bool = False
    download_taxonomy: bool = True
    preview_max_size: int = 1920
    exclude_paths: set | None = None


def _should_abort(abort_event):
    """Check if the pipeline should abort."""
    return abort_event.is_set()


def _update_stages(runner, job_id, stages):
    """Push a stages progress update."""
    runner.push_event(job_id, "progress", {
        "phase": _current_phase(stages),
        "current": 0,
        "total": 0,
        "stages": {k: dict(v) for k, v in stages.items()},
    })


def _current_phase(stages):
    """Determine the primary phase label from stage statuses."""
    for name in ["regroup", "extract_masks", "classify", "model_loader",
                 "previews", "thumbnails", "scan"]:
        info = stages.get(name, {})
        if info.get("status") == "running":
            return info.get("label", name)
    return "Pipeline"


def run_pipeline_job(job, runner, db_path, workspace_id, params):
    """Execute streaming pipeline. Called by JobRunner in a background thread.

    Args:
        job: job dict from JobRunner (has id, progress, errors, etc.)
        runner: JobRunner instance for push_event()
        db_path: path to SQLite database
        workspace_id: active workspace ID
        params: PipelineParams with request parameters

    Returns:
        dict with stage results, duration, and errors
    """
    job["_start_time"] = time.time()
    abort = threading.Event()
    errors = job["errors"]  # shared list, append is thread-safe

    stages = {
        "scan": {"status": "pending", "count": 0, "label": "Scanning photos"},
        "thumbnails": {"status": "pending", "count": 0, "label": "Generating thumbnails"},
        "previews": {"status": "pending", "count": 0, "label": "Generating previews"},
        "model_loader": {"status": "pending", "label": "Loading models"},
        "classify": {"status": "pending", "count": 0, "label": "Classifying species"},
        "extract_masks": {"status": "pending", "count": 0, "label": "Extracting features"},
        "regroup": {"status": "pending", "label": "Grouping encounters"},
    }

    # Define step tracking for the jobs page
    step_defs = [
        {"id": "scan", "label": "Scan photos"},
        {"id": "thumbnails", "label": "Generate thumbnails"},
        {"id": "previews", "label": "Generate previews"},
    ]
    if not params.skip_classify:
        step_defs.append({"id": "model_loader", "label": "Load models"})
        step_defs.append({"id": "classify", "label": "Classify species"})
    if not params.skip_extract_masks:
        step_defs.append({"id": "extract_masks", "label": "Extract features"})
    if not params.skip_regroup:
        step_defs.append({"id": "regroup", "label": "Group encounters"})
    runner.set_steps(job["id"], step_defs)

    result = {"stages": {}}
    collection_id = params.collection_id
    scan_to_thumb = queue.Queue(maxsize=200)
    collected_photo_ids = []
    collection_ready = threading.Event()
    models_ready = threading.Event()
    loaded_models = {}  # populated by model_loader thread

    skip_scan = collection_id is not None

    # --- Stage functions ---

    def scanner_stage():
        nonlocal collection_id

        if skip_scan:
            runner.update_step(job["id"], "scan", status="completed",
                               summary="Skipped (using collection)")
            scan_to_thumb.put(_SENTINEL)
            return
        stages["scan"]["status"] = "running"
        runner.update_step(job["id"], "scan", status="running")
        _update_stages(runner, job["id"], stages)
        try:
            import config as cfg
            from scanner import scan as do_scan

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            effective_cfg = thread_db.get_effective_config(cfg.load())
            pipeline_cfg = effective_cfg.get("pipeline", {})

            def photo_cb(photo_id, path):
                collected_photo_ids.append(photo_id)
                scan_to_thumb.put((photo_id, path))
                stages["scan"]["count"] = len(collected_photo_ids)
                runner.update_step(job["id"], "scan",
                                   current_file=os.path.basename(path))

            def status_cb(message):
                runner.update_step(job["id"], "scan", current_file=message)

            def progress_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                elapsed = time.time() - job["_start_time"]
                rate = round(current / max(elapsed, 0.01) * 60, 1)  # files/min
                remaining = total - current
                rate_per_sec = current / max(elapsed, 0.01)
                eta = round(remaining / rate_per_sec) if rate_per_sec > 0 and current >= 10 else None
                runner.update_step(job["id"], "scan",
                                   progress={"current": current, "total": total})
                runner.push_event(job["id"], "progress", {
                    "phase": "Scanning photos",
                    "current": current,
                    "total": total,
                    "rate": rate,
                    "eta_seconds": eta,
                    "stages": {k: dict(v) for k, v in stages.items()},
                })

            # Determine source folder(s)
            sources = params.sources or ([params.source] if params.source else [])

            if params.destination:
                from ingest import ingest as do_ingest

                def ingest_cb(current, total, filename):
                    runner.update_step(job["id"], "scan",
                                       current_file=filename,
                                       progress={"current": current, "total": total})
                    runner.push_event(job["id"], "progress", {
                        "phase": "Importing photos",
                        "current": current,
                        "total": total,
                        "current_file": filename,
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })

            if params.destination:
                # Copy mode: ingest all sources first, then scan destination once.
                # Scanning inside the loop would rescan the entire destination on
                # each iteration, re-queuing unchanged files and inflating counts.
                #
                # Preserve cross-source duplicate detection: files copied from
                # earlier sources are not yet in the DB (the scan hasn't run),
                # so we accumulate their hashes in a shared set and pass it to
                # each subsequent ingest() call via extra_known_hashes.
                accumulated_hashes: set = set()
                for src_folder in sources:
                    result_info = do_ingest(
                        source_dir=src_folder,
                        destination_dir=params.destination,
                        db=thread_db,
                        file_types=params.file_types,
                        folder_template=params.folder_template,
                        skip_duplicates=params.skip_duplicates,
                        progress_callback=ingest_cb,
                        extra_known_hashes=accumulated_hashes,
                        skip_paths=params.exclude_paths,
                    )
                    # Collect hashes of files just copied so the next source
                    # iteration treats them as known even before the DB scan.
                    if params.skip_duplicates:
                        import contextlib

                        from scanner import compute_file_hash
                        for path in result_info.get("copied_paths", []):
                            with contextlib.suppress(Exception):
                                accumulated_hashes.add(compute_file_hash(path))
                do_scan(
                    params.destination, thread_db,
                    progress_callback=progress_cb,
                    incremental=True,
                    extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                    photo_callback=photo_cb,
                    status_callback=status_cb,
                )
            else:
                # Scan-in-place: scan each source folder independently.
                for src_folder in sources:
                    do_scan(
                        src_folder, thread_db,
                        progress_callback=progress_cb,
                        incremental=True,
                        extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                        photo_callback=photo_cb,
                        skip_paths=params.exclude_paths,
                        status_callback=status_cb,
                    )
            stages["scan"]["status"] = "completed"
            runner.update_step(job["id"], "scan", status="completed",
                               summary=f"{stages['scan']['count']} photos")
        except Exception as e:
            errors.append(f"[scan] Fatal: {e}")
            log.exception("Pipeline scan stage failed")
            abort.set()
            stages["scan"]["status"] = "failed"
            runner.update_step(job["id"], "scan", status="failed", error=str(e))
        finally:
            scan_to_thumb.put(_SENTINEL)
            _update_stages(runner, job["id"], stages)

    def collection_stage():
        """Wait for scan to finish, build collection, signal classifier."""
        nonlocal collection_id

        if skip_scan:
            collection_ready.set()
            return

        # Wait for scanner to complete (don't check abort -- we want the
        # collection regardless so the user can see scanned photos)
        while True:
            if stages["scan"]["status"] in ("completed", "failed"):
                break
            time.sleep(0.1)

        if not collected_photo_ids:
            collection_ready.set()
            return

        try:
            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)
            from datetime import datetime as dt

            name = "Pipeline " + dt.now().strftime("%Y-%m-%d %H:%M")
            collection_id = thread_db.add_collection(
                name,
                json.dumps([{"field": "photo_ids", "value": collected_photo_ids}]),
            )
            result["collection_id"] = collection_id
        except Exception as e:
            errors.append(f"[collection] Fatal: {e}")
            log.exception("Pipeline collection stage failed")
            abort.set()
        finally:
            collection_ready.set()

    def thumbnail_stage():
        stages["thumbnails"]["status"] = "running"
        runner.update_step(job["id"], "thumbnails", status="running")
        _update_stages(runner, job["id"], stages)
        try:
            from thumbnails import generate_thumbnail

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            import config as cfg
            effective_cfg = thread_db.get_effective_config(cfg.load())
            thumb_size = effective_cfg.get("display", {}).get("thumbnail_size", 300)

            # Resolve thumb cache dir from db_path
            cache_dir = os.path.join(os.path.dirname(db_path), "thumbnails")
            os.makedirs(cache_dir, exist_ok=True)

            generated = 0
            skipped = 0
            failed = 0

            while True:
                try:
                    item = scan_to_thumb.get(timeout=1.0)
                except queue.Empty:
                    # Keep draining even if abort is set -- we want thumbnails
                    # for any photos already scanned. Only stop on sentinel.
                    if _should_abort(abort) and scan_to_thumb.empty():
                        break
                    continue
                if item is _SENTINEL:
                    break
                photo_id, photo_path = item
                try:
                    thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
                    already_exists = os.path.exists(thumb_path)
                    result_path = generate_thumbnail(photo_id, photo_path, cache_dir, size=thumb_size)
                    if result_path is None:
                        failed += 1
                    elif already_exists:
                        skipped += 1
                    else:
                        generated += 1
                    stages["thumbnails"]["count"] = generated + skipped
                except Exception:
                    failed += 1
                    log.debug("Thumbnail failed for photo %s", photo_id)
                processed = generated + skipped + failed
                # Use scan count directly regardless of whether scan has
                # completed yet — this avoids the total staying at 0/? when
                # the thumbnail worker catches up with scan before scan's
                # status flips to "completed".
                scan_total = stages["scan"].get("count", 0)
                runner.update_step(job["id"], "thumbnails",
                                   current_file=os.path.basename(photo_path),
                                   progress={"current": processed, "total": scan_total})

            stages["thumbnails"]["status"] = "completed"
            from thumbnails import format_summary as thumb_summary
            thumb_result = {"generated": generated, "skipped": skipped, "failed": failed}
            processed = generated + skipped + failed
            runner.update_step(job["id"], "thumbnails", status="completed",
                               summary=thumb_summary(thumb_result),
                               progress={"current": processed, "total": processed})
            result["stages"]["thumbnails"] = thumb_result
        except Exception as e:
            errors.append(f"[thumbnails] Fatal: {e}")
            log.exception("Pipeline thumbnail stage failed")
            stages["thumbnails"]["status"] = "failed"
            runner.update_step(job["id"], "thumbnails", status="failed", error=str(e))
        _update_stages(runner, job["id"], stages)

    def previews_stage():
        """Generate preview images for browsed photos."""
        stages["previews"]["status"] = "running"
        runner.update_step(job["id"], "previews", status="running")
        _update_stages(runner, job["id"], stages)

        try:
            import config as cfg
            from image_loader import load_image

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            max_size = params.preview_max_size
            if max_size == 0:
                max_size = None  # Full resolution
            preview_quality = cfg.load().get("preview_quality", 90)
            base_dir = os.path.dirname(db_path)
            preview_dir = os.path.join(base_dir, "previews")
            os.makedirs(preview_dir, exist_ok=True)

            if collection_id:
                photos = thread_db.get_collection_photos(collection_id, per_page=999999)
            elif not skip_scan:
                # Scan ran but produced no photos — skip previews to avoid
                # unexpectedly processing the entire workspace.
                runner.update_step(job["id"], "previews", status="completed",
                                   summary="Skipped (no photos scanned)")
                stages["previews"]["status"] = "completed"
                return
            else:
                photos = thread_db.get_photos(per_page=999999)

            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            generated = 0
            skipped = 0

            for i, photo in enumerate(photos):
                if _should_abort(abort):
                    break
                cache_path = os.path.join(preview_dir, f'{photo["id"]}.jpg')
                if os.path.exists(cache_path):
                    skipped += 1
                else:
                    folder_path = folders.get(photo["folder_id"], "")
                    image_path = os.path.join(folder_path, photo["filename"])
                    img = load_image(image_path, max_size=max_size)
                    if img:
                        img.save(cache_path, format="JPEG", quality=preview_quality)
                        generated += 1

                stages["previews"]["count"] = i + 1
                runner.update_step(job["id"], "previews",
                                   current_file=photo["filename"],
                                   progress={"current": i + 1, "total": total})
                runner.push_event(job["id"], "progress", {
                    "phase": "Generating previews",
                    "current": i + 1,
                    "total": total,
                    "current_file": photo["filename"],
                    "rate": round(
                        (i + 1) / max(time.time() - job["_start_time"], 0.01) * 60, 1
                    ),
                    "stages": {k: dict(v) for k, v in stages.items()},
                })

            result["stages"]["previews"] = {
                "generated": generated, "skipped": skipped, "total": total
            }
            stages["previews"]["status"] = "completed"
            runner.update_step(job["id"], "previews", status="completed",
                               summary=f"{generated} generated")
        except Exception as e:
            errors.append(f"[previews] Fatal: {e}")
            log.exception("Pipeline previews stage failed")
            stages["previews"]["status"] = "failed"
            runner.update_step(job["id"], "previews", status="failed", error=str(e))

        _update_stages(runner, job["id"], stages)

    def model_loader_stage():
        if params.skip_classify:
            stages["model_loader"]["status"] = "skipped"
            runner.update_step(job["id"], "model_loader", status="completed",
                               summary="Skipped")
            _update_stages(runner, job["id"], stages)
            models_ready.set()
            return
        stages["model_loader"]["status"] = "running"
        runner.update_step(job["id"], "model_loader", status="running",
                           current_file="Resolving model...")
        _update_stages(runner, job["id"], stages)
        try:
            from classify_job import _load_labels, _load_taxonomy
            from models import get_active_model, get_models

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            # Resolve model
            if params.model_id:
                all_models = get_models()
                active_model = next(
                    (m for m in all_models if m["id"] == params.model_id and m["downloaded"]),
                    None,
                )
                if not active_model:
                    raise RuntimeError(f"Model '{params.model_id}' not found or not downloaded.")
            else:
                active_model = get_active_model()
            if not active_model:
                raise RuntimeError("No model available. Download one in Settings.")

            model_str = active_model["model_str"]
            weights_path = active_model["weights_path"]
            model_type = active_model.get("model_type", "bioclip")
            model_name = active_model["name"]
            runner.update_step(job["id"], "model_loader", current_file=model_name)

            # Download taxonomy if missing and requested
            taxonomy_path = os.path.join(os.path.dirname(__file__), "taxonomy.json")
            if params.download_taxonomy and not os.path.exists(taxonomy_path):
                try:
                    from taxonomy import download_taxonomy
                    runner.push_event(job["id"], "progress", {
                        "phase": "Downloading taxonomy...",
                        "current": 0, "total": 0,
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })
                    download_taxonomy(taxonomy_path, progress_callback=lambda msg:
                        runner.push_event(job["id"], "progress", {
                            "phase": msg,
                            "current": 0, "total": 0,
                            "stages": {k: dict(v) for k, v in stages.items()},
                        })
                    )
                except Exception as e:
                    log.warning("Taxonomy download failed, continuing without: %s", e)

            # Load taxonomy
            tax = _load_taxonomy(taxonomy_path)

            # Load labels
            labels, use_tol = _load_labels(
                model_type=model_type,
                model_str=model_str,
                labels_file=params.labels_file,
                labels_files=params.labels_files,
                db=thread_db,
            )

            # Load classifier
            runner.push_event(job["id"], "progress", {
                "phase": f"Loading {model_name}...",
                "current": 0, "total": 0,
                "stages": {k: dict(v) for k, v in stages.items()},
            })

            if model_type == "timm":
                from timm_classifier import TimmClassifier
                clf = TimmClassifier(model_str, taxonomy=tax)
            else:
                from classifier import Classifier
                clf = Classifier(
                    labels=None if use_tol else labels,
                    model_str=model_str,
                    pretrained_str=weights_path,
                )

            loaded_models["clf"] = clf
            loaded_models["model_type"] = model_type
            loaded_models["model_name"] = model_name
            loaded_models["model_str"] = model_str
            loaded_models["tax"] = tax
            loaded_models["labels"] = labels
            loaded_models["use_tol"] = use_tol
            loaded_models["active_model"] = active_model

            stages["model_loader"]["status"] = "completed"
            runner.update_step(job["id"], "model_loader", status="completed",
                               summary=model_name)
        except Exception as e:
            errors.append(f"[model_loader] Fatal: {e}")
            log.exception("Pipeline model loader stage failed")
            abort.set()
            stages["model_loader"]["status"] = "failed"
            runner.update_step(job["id"], "model_loader", status="failed", error=str(e))
        finally:
            models_ready.set()
            _update_stages(runner, job["id"], stages)

    def classify_stage():
        """Wait for collection + models, then run interleaved detect+classify."""
        collection_ready.wait()
        models_ready.wait()

        if params.skip_classify or abort.is_set() or not collection_id or "clf" not in loaded_models:
            stages["classify"]["status"] = "skipped"
            runner.update_step(job["id"], "classify", status="completed",
                               summary="Skipped")
            _update_stages(runner, job["id"], stages)
            return

        stages["classify"]["status"] = "running"
        runner.update_step(job["id"], "classify", status="running")
        _update_stages(runner, job["id"], stages)

        try:
            import config as cfg
            from classify_job import (
                _BATCH_SIZE,
                _detect_batch,
                _flush_batch,
                _prepare_image,
                _store_grouped_predictions,
            )

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            user_cfg = thread_db.get_effective_config(cfg.load())

            clf = loaded_models["clf"]
            model_type = loaded_models["model_type"]
            model_name = loaded_models["model_name"]
            tax = loaded_models["tax"]

            photos = thread_db.get_collection_photos(collection_id, per_page=999999)
            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)

            if params.reclassify:
                photo_ids = [p["id"] for p in photos]
                thread_db.clear_predictions(model=model_name, collection_photo_ids=photo_ids)

            existing_preds = set()
            if not params.reclassify:
                existing_preds = thread_db.get_existing_prediction_photo_ids(model_name)

            # Interleaved detect + classify in batches
            raw_results = []
            failed = 0
            detected = 0
            skipped_existing = 0
            start_time = time.time()

            from datetime import datetime as dt

            for batch_start in range(0, total, _BATCH_SIZE):
                if _should_abort(abort):
                    break

                batch = photos[batch_start:batch_start + _BATCH_SIZE]
                batch_idx = batch_start + len(batch)

                runner.push_event(job["id"], "progress", {
                    "phase": "Classifying species",
                    "current": batch_idx,
                    "total": total,
                    "rate": round(batch_idx / max(time.time() - start_time, 0.01) * 60, 1),
                    "stages": {k: dict(v) for k, v in stages.items()},
                })
                stages["classify"]["count"] = batch_idx
                runner.update_step(job["id"], "classify",
                                   progress={"current": batch_idx, "total": total})

                # Detect this batch
                det_map, det_count = _detect_batch(
                    batch, folders, runner, job, params.reclassify, thread_db,
                )
                detected += det_count

                # Classify this batch
                for photo in batch:
                    if photo["id"] in existing_preds:
                        skipped_existing += 1
                        pred_row = thread_db.get_prediction_for_photo(photo["id"], model_name)
                        if pred_row:
                            folder_path = folders.get(photo["folder_id"], "")
                            image_path = os.path.join(folder_path, photo["filename"])
                            timestamp = None
                            if photo["timestamp"]:
                                with contextlib.suppress(Exception):
                                    timestamp = dt.fromisoformat(photo["timestamp"])
                            embedding = None
                            if model_type != "timm":
                                emb_blob = thread_db.get_photo_embedding(photo["id"])
                                if emb_blob:
                                    import numpy as np
                                    embedding = np.frombuffer(emb_blob, dtype=np.float32)
                            raw_results.append({
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
                            })
                        continue

                    img, folder_path, image_path = _prepare_image(photo, folders, det_map)
                    if img is None:
                        failed += 1
                        continue

                    img_batch = [{"photo": photo, "folder_path": folder_path,
                                  "image_path": image_path, "img": img}]
                    failed += _flush_batch(img_batch, clf, model_type, model_name,
                                           thread_db, raw_results)

            # Group and store predictions
            grouping_window = user_cfg.get("grouping_window_seconds", 5)
            similarity_threshold = user_cfg.get("similarity_threshold", 0.85)

            group_result = _store_grouped_predictions(
                raw_results, job["id"], model_name,
                grouping_window, similarity_threshold, tax, thread_db,
            )

            stages["classify"]["status"] = "completed"
            runner.update_step(job["id"], "classify", status="completed",
                               summary=f"{group_result['predictions_stored']} predictions")
            result["stages"]["classify"] = {
                "total": total,
                "predictions_stored": group_result["predictions_stored"],
                "detected": detected,
                "failed": failed,
                "already_classified": skipped_existing,
            }

        except Exception as e:
            errors.append(f"[classify] Fatal: {e}")
            log.exception("Pipeline classify stage failed")
            abort.set()
            stages["classify"]["status"] = "failed"
            runner.update_step(job["id"], "classify", status="failed", error=str(e))

        _update_stages(runner, job["id"], stages)

    def extract_masks_stage():
        """Run SAM2 mask extraction + DINOv2 embeddings after classify."""
        if params.skip_extract_masks or abort.is_set() or not collection_id:
            stages["extract_masks"]["status"] = "skipped"
            runner.update_step(job["id"], "extract_masks", status="completed",
                               summary="Skipped")
            return

        stages["extract_masks"]["status"] = "running"
        runner.update_step(job["id"], "extract_masks", status="running")
        _update_stages(runner, job["id"], stages)

        try:
            import config as cfg
            from dino_embed import embed_global, embed_subject, embedding_to_blob
            from masking import (
                crop_completeness,
                crop_subject,
                generate_mask,
                render_proxy,
                save_mask,
            )
            from quality import compute_all_quality_features

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            effective_cfg = thread_db.get_effective_config(cfg.load())
            pipeline_cfg = effective_cfg.get("pipeline", {})
            sam2_variant = pipeline_cfg.get("sam2_variant", "sam2-small")
            dinov2_variant = pipeline_cfg.get("dinov2_variant", "vit-b14")
            proxy_longest_edge = pipeline_cfg.get("proxy_longest_edge", 1536)

            masks_dir = os.path.join(os.path.dirname(db_path), "masks")
            os.makedirs(masks_dir, exist_ok=True)

            photos = thread_db.get_collection_photos(collection_id, per_page=999999)

            # Build a map of photo_id -> primary detection (highest confidence)
            # from the detections table. Only photos with detections and without
            # masks need processing.
            photo_det_map = {}
            for p in photos:
                dets = thread_db.get_detections(p["id"])
                if dets:
                    primary = dets[0]  # already ordered by confidence DESC
                    has_mask = thread_db.conn.execute(
                        "SELECT mask_path FROM photos WHERE id=?", (p["id"],)
                    ).fetchone()[0]
                    if not has_mask:
                        photo_det_map[p["id"]] = {
                            "photo": p,
                            "det_box": {
                                "x": primary["box_x"],
                                "y": primary["box_y"],
                                "w": primary["box_w"],
                                "h": primary["box_h"],
                            },
                        }

            photos_to_process = [
                photo_det_map[pid] for pid in photo_det_map
            ]

            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos_to_process)
            masked = 0
            skipped = 0
            em_failed = 0
            start_time = time.time()

            for i, entry in enumerate(photos_to_process):
                if _should_abort(abort):
                    break

                photo = entry["photo"]
                det_box = entry["det_box"]
                photo_id = photo["id"]
                folder_path = folders.get(photo["folder_id"], "")
                image_path = os.path.join(folder_path, photo["filename"])

                try:
                    proxy = render_proxy(image_path, longest_edge=proxy_longest_edge)
                    if proxy is None:
                        skipped += 1
                        continue

                    mask = generate_mask(proxy, det_box, variant=sam2_variant)
                    if mask is None:
                        skipped += 1
                        continue

                    mask_path = save_mask(mask, masks_dir, photo_id)
                    completeness = crop_completeness(mask)
                    features = compute_all_quality_features(proxy, mask)

                    subject_crop = crop_subject(proxy, mask, margin=0.15)
                    subj_emb_blob = None
                    global_emb_blob = None
                    if subject_crop is not None:
                        subj_emb = embed_subject(subject_crop, variant=dinov2_variant)
                        subj_emb_blob = embedding_to_blob(subj_emb)
                    global_emb = embed_global(proxy, variant=dinov2_variant)
                    global_emb_blob = embedding_to_blob(global_emb)

                    thread_db.update_photo_pipeline_features(
                        photo_id, mask_path=mask_path, crop_complete=completeness,
                        **features,
                    )
                    thread_db.update_photo_embeddings(
                        photo_id,
                        dino_subject_embedding=subj_emb_blob,
                        dino_global_embedding=global_emb_blob,
                    )
                    masked += 1
                except Exception:
                    em_failed += 1
                    log.warning("Mask extraction failed for photo %s", photo_id, exc_info=True)
                    errors.append(f"Photo {photo_id}: mask extraction failed")

                stages["extract_masks"]["count"] = i + 1
                runner.update_step(job["id"], "extract_masks",
                                   progress={"current": i + 1, "total": total})
                runner.push_event(job["id"], "progress", {
                    "phase": "Extracting features (SAM2 + DINOv2)",
                    "current": i + 1,
                    "total": total,
                    "rate": round((i + 1) / max(time.time() - start_time, 0.01) * 60, 1),
                    "stages": {k: dict(v) for k, v in stages.items()},
                })

            stages["extract_masks"]["status"] = "completed"
            runner.update_step(job["id"], "extract_masks", status="completed",
                               summary=f"{masked} masked, {skipped} skipped")
            result["stages"]["extract_masks"] = {
                "masked": masked, "skipped": skipped, "failed": em_failed, "total": total,
            }
        except Exception as e:
            errors.append(f"[extract_masks] Fatal: {e}")
            log.exception("Pipeline extract-masks stage failed")
            stages["extract_masks"]["status"] = "failed"
            runner.update_step(job["id"], "extract_masks", status="failed", error=str(e))

        _update_stages(runner, job["id"], stages)

    def regroup_stage():
        """Run pipeline grouping + scoring + triage from cached features."""
        if params.skip_regroup or abort.is_set() or not collection_id:
            stages["regroup"]["status"] = "skipped"
            runner.update_step(job["id"], "regroup", status="completed",
                               summary="Skipped")
            return

        stages["regroup"]["status"] = "running"
        runner.update_step(job["id"], "regroup", status="running")
        _update_stages(runner, job["id"], stages)

        try:
            import config as cfg
            from pipeline import load_photo_features, run_full_pipeline, save_results

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            effective_cfg = thread_db.get_effective_config(cfg.load())
            pipeline_cfg = effective_cfg.get("pipeline", {})

            photos = load_photo_features(thread_db, collection_id=collection_id, config=effective_cfg)
            if not photos:
                result["stages"]["regroup"] = {"error": "No photos with pipeline features found."}
                stages["regroup"]["status"] = "completed"
                runner.update_step(job["id"], "regroup", status="completed",
                                   summary="No photos to group")
                return

            results = run_full_pipeline(photos, config=pipeline_cfg)
            cache_dir = os.path.dirname(db_path)
            save_results(results, cache_dir, workspace_id)

            stages["regroup"]["status"] = "completed"
            summary_info = results.get("summary", {})
            groups = summary_info.get("groups", "")
            runner.update_step(job["id"], "regroup", status="completed",
                               summary=f"{groups} groups" if groups else "Done")
            result["stages"]["regroup"] = summary_info
        except Exception as e:
            errors.append(f"[regroup] Fatal: {e}")
            log.exception("Pipeline regroup stage failed")
            stages["regroup"]["status"] = "failed"
            runner.update_step(job["id"], "regroup", status="failed", error=str(e))

        _update_stages(runner, job["id"], stages)

    # --- Launch threads ---

    threads = {}

    # Phase 1: scan + thumbnails + model loading (concurrent)
    threads["scanner"] = threading.Thread(target=scanner_stage, daemon=True)
    threads["collection"] = threading.Thread(target=collection_stage, daemon=True)
    threads["thumbnail"] = threading.Thread(target=thumbnail_stage, daemon=True)
    threads["model_loader"] = threading.Thread(target=model_loader_stage, daemon=True)

    for t in threads.values():
        t.start()

    # Wait for scan-related threads to finish
    threads["scanner"].join()
    threads["collection"].join()
    threads["thumbnail"].join()
    threads["model_loader"].join()

    # Phase 1.5: previews (needs scan complete, runs before classify)
    if not abort.is_set():
        previews_stage()

    # Phase 2: classify (needs collection + models)
    if not abort.is_set():
        classify_stage()

    # Phase 3: extract-masks (needs classify output)
    if not abort.is_set():
        extract_masks_stage()

    # Phase 4: regroup (needs extract-masks output)
    if not abort.is_set():
        regroup_stage()

    elapsed = time.time() - job["_start_time"]
    result["duration"] = round(elapsed, 1)
    result["errors"] = list(errors)

    return result
