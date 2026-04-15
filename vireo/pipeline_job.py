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
    folder_template: str = "%Y/%Y-%m-%d"
    skip_duplicates: bool = True
    labels_file: str | None = None
    labels_files: list | None = None
    model_id: str | None = None
    model_ids: list | None = None
    reclassify: bool = False
    skip_extract_masks: bool = False
    skip_regroup: bool = False
    skip_classify: bool = False
    download_taxonomy: bool = True
    preview_max_size: int = 1920
    exclude_paths: set | None = None
    exclude_photo_ids: set | None = None
    recursive: bool = True


def _should_abort(abort_event):
    """Check if the pipeline should abort."""
    return abort_event.is_set()


def _incomplete_model_message(model_name, is_custom=False):
    if is_custom:
        return (
            f"Model '{model_name}' appears to be missing required files. "
            f"Ensure all model files are present in the model directory."
        )
    return (
        f"Model '{model_name}' is incomplete. "
        f"Open Settings → Models and click Repair to finish the download."
    )


def _looks_like_missing_external_data(err):
    """Heuristic: does this exception look like ONNXRuntime failing to find
    an external-data sidecar? Matches the specific message the runtime
    raises when a graph references external weights that aren't on disk."""
    msg = str(err).lower()
    return (
        "model_path must not be empty" in msg
        or "external data" in msg
    )


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
    for name in ["regroup", "extract_masks", "classify", "detect",
                 "model_loader", "previews", "thumbnails", "scan", "ingest"]:
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

    # Bridge user-initiated cancellation (runner.cancel_job) to the local
    # abort Event so all stages that already honor `abort` stop promptly.
    cancel_watcher_stop = threading.Event()

    def _cancel_watcher():
        while not cancel_watcher_stop.is_set():
            if runner.is_cancelled(job["id"]):
                abort.set()
                return
            if cancel_watcher_stop.wait(0.25):
                return

    cancel_watcher = threading.Thread(target=_cancel_watcher, daemon=True)
    cancel_watcher.start()

    stages = {
        "ingest": {"status": "pending", "count": 0, "label": "Importing photos"},
        "scan": {"status": "pending", "count": 0, "label": "Scanning photos"},
        "thumbnails": {"status": "pending", "count": 0, "label": "Generating thumbnails"},
        "previews": {"status": "pending", "count": 0, "label": "Generating previews"},
        "model_loader": {"status": "pending", "label": "Loading models"},
        "detect": {"status": "pending", "count": 0, "label": "Detecting subjects"},
        "classify": {"status": "pending", "count": 0, "label": "Classifying species"},
        "extract_masks": {"status": "pending", "count": 0, "label": "Extracting features"},
        "regroup": {"status": "pending", "label": "Grouping encounters"},
    }

    # Normalize model_ids: prefer the explicit list, fall back to the legacy
    # single `model_id`, and finally to `[]` which means "use the active model
    # from config." This is the knob the multi-model fix hangs off of.
    if params.model_ids:
        effective_model_ids = list(params.model_ids)
    elif params.model_id:
        effective_model_ids = [params.model_id]
    else:
        effective_model_ids = []

    # Resolve model specs EARLY so per-model `classify:<id>` step_defs can
    # carry the model's display name as their label. Labels are immutable
    # after set_steps, so we cannot defer this to model_loader_stage.
    #
    # Resolution failures are captured (not raised) so the job still sets up
    # its step tree and the model_loader stage can surface a clean error.
    # For any id we fail to resolve we still emit a per-model step — labeled
    # with the id — so the user sees exactly which model broke.
    resolved_specs: list = []
    resolution_error: str | None = None
    if not params.skip_classify:
        try:
            from models import get_active_model, get_models
            if effective_model_ids:
                by_id = {m["id"]: m for m in get_models()}
                for mid in effective_model_ids:
                    spec = by_id.get(mid)
                    if not spec or not spec.get("downloaded"):
                        raise RuntimeError(
                            f"Model '{mid}' not found or not downloaded."
                        )
                    resolved_specs.append(spec)
            else:
                spec = get_active_model()
                if not spec:
                    raise RuntimeError(
                        "No model available. Download one in Settings."
                    )
                resolved_specs.append(spec)
        except Exception as e:
            resolution_error = str(e)

    # Define step tracking for the jobs page
    step_defs = []
    if params.destination:
        step_defs.append({"id": "ingest", "label": "Import photos"})
    step_defs.extend([
        {"id": "scan", "label": "Scan photos"},
        {"id": "thumbnails", "label": "Generate thumbnails"},
        {"id": "previews", "label": "Generate previews"},
    ])
    if not params.skip_classify:
        step_defs.append({"id": "model_loader", "label": "Load models"})
        step_defs.append({"id": "detect", "label": "Detect subjects"})
        # One row per model — label = model display name, id = classify:<mid>.
        # When resolution partially failed (e.g. 3 ids requested, 2nd not
        # downloaded), resolved_specs is a non-empty prefix of the requested
        # list. Emitting rows from resolved_specs alone would hide the later
        # failed ids — their "failed" update_step calls would then no-op
        # silently. Drive row creation off effective_model_ids whenever
        # resolution reported an error, so every requested model has a visible
        # step the model_loader stage can mark 'failed'.
        if resolved_specs and not resolution_error:
            for spec in resolved_specs:
                step_defs.append({
                    "id": f"classify:{spec['id']}",
                    "label": f"Classify with {spec['name']}",
                })
        elif effective_model_ids:
            # Partial or total resolution failure: use display names from any
            # resolved specs we did get, fall back to the raw id otherwise.
            by_id = {s["id"]: s for s in resolved_specs}
            for mid in effective_model_ids:
                spec = by_id.get(mid)
                label = (
                    f"Classify with {spec['name']}" if spec
                    else f"Classify with {mid}"
                )
                step_defs.append({
                    "id": f"classify:{mid}",
                    "label": label,
                })
        else:
            # No ids, no resolved spec (active-model resolution failed).
            # One placeholder row keeps the step tree consistent.
            step_defs.append({
                "id": "classify:__unresolved__",
                "label": "Classify species",
            })
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

    def _filter_excluded(photos):
        """Remove photos excluded by user selection in preview."""
        if not params.exclude_photo_ids:
            return photos
        return [p for p in photos if p["id"] not in params.exclude_photo_ids]

    # Mark ingest as skipped when not in copy mode so SSE events
    # don't show a perpetually-pending stage.
    if not params.destination:
        stages["ingest"]["status"] = "skipped"

    # --- Stage functions ---

    def scanner_stage():
        nonlocal collection_id

        if skip_scan:
            stages["scan"]["status"] = "skipped"
            runner.update_step(job["id"], "scan", status="completed",
                               summary="Skipped (using collection)")
            _update_stages(runner, job["id"], stages)
            scan_to_thumb.put(_SENTINEL)
            return
        # Note: stages["scan"]["status"] is NOT set to "running" here. It is
        # flipped to "running" just before each do_scan() call below, so
        # numScan doesn't pulse during the ingest sub-phase.
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
                runner.push_event(job["id"], "progress", {
                    "phase": message,
                    "stage_id": "scan",
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                    "stages": {k: dict(v) for k, v in stages.items()},
                })

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
                    "stage_id": "scan",
                    "current": current,
                    "total": total,
                    "rate": rate,
                    "eta_seconds": eta,
                    "stages": {k: dict(v) for k, v in stages.items()},
                })

            # Determine source folder(s)
            sources = params.sources or ([params.source] if params.source else [])

            if params.destination:
                from pathlib import Path

                from ingest import ingest as do_ingest

                def ingest_cb(current, total, filename):
                    stages["ingest"]["count"] = current
                    runner.update_step(job["id"], "ingest",
                                       current_file=filename,
                                       progress={"current": current, "total": total})
                    runner.push_event(job["id"], "progress", {
                        "phase": "Importing photos",
                        "stage_id": "ingest",
                        "current": current,
                        "total": total,
                        "current_file": filename,
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })

            if params.destination:
                # Copy mode: ingest all sources first, then scan destination
                # subfolders that received files.
                stages["ingest"]["status"] = "running"
                runner.update_step(job["id"], "ingest", status="running")
                _update_stages(runner, job["id"], stages)

                accumulated_hashes: set = set()
                all_copied_paths: list = []
                all_duplicate_folders: set = set()
                total_copied = 0
                total_skipped = 0
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
                        recursive=params.recursive,
                    )
                    all_copied_paths.extend(result_info.get("copied_paths", []))
                    all_duplicate_folders.update(result_info.get("duplicate_folders", []))
                    total_copied += result_info.get("copied", 0)
                    total_skipped += result_info.get("skipped_duplicate", 0)
                    # Collect hashes of files just copied so the next source
                    # iteration treats them as known even before the DB scan.
                    if params.skip_duplicates:
                        import contextlib

                        from scanner import compute_file_hash
                        for path in result_info.get("copied_paths", []):
                            with contextlib.suppress(OSError):
                                accumulated_hashes.add(compute_file_hash(path))

                # Mark ingest complete
                parts = []
                if total_copied:
                    parts.append(f"{total_copied} copied")
                if total_skipped:
                    parts.append(f"{total_skipped} skipped")
                stages["ingest"]["status"] = "completed"
                runner.update_step(job["id"], "ingest", status="completed",
                                   summary=", ".join(parts) or "0 files")
                _update_stages(runner, job["id"], stages)

                # Scan only the destination subfolders that actually contain
                # files we care about, not the entire destination tree. Use
                # restrict_dirs so the scanner still roots the folder hierarchy
                # at the destination, preserving parent folder links. Include
                # folders that received copies AND folders that already hold
                # duplicates of the source files — both need to be linked to
                # the active workspace. Guard every candidate at this seam:
                # scanner._ensure_folder recurses parents until it equals the
                # scan root; a non-descendant path would recurse all the way
                # to '/', so restrict_dirs must contain only descendants of
                # params.destination. ingest() already enforces this, but
                # we re-check here to keep the invariant local and obvious.
                # Both sides are lexically normalized via os.path.normpath so
                # a stored path containing ``..`` can't defeat the check.
                import os as _os
                dest_p = Path(_os.path.normpath(params.destination))

                def _under_destination(path: str) -> bool:
                    return Path(_os.path.normpath(path)).is_relative_to(dest_p)

                restrict_set: set[str] = set()
                if all_copied_paths:
                    restrict_set.update(
                        str(Path(p).parent) for p in all_copied_paths
                        if _under_destination(str(Path(p).parent))
                    )
                restrict_set.update(
                    f for f in all_duplicate_folders if _under_destination(f)
                )
                restrict = sorted(restrict_set) if restrict_set else None
                # Flip scan to running and reset job progress so status
                # events during enumeration don't carry ingest's numbers.
                stages["scan"]["status"] = "running"
                runner.update_step(job["id"], "scan", status="running")
                job["progress"]["current"] = 0
                job["progress"]["total"] = 0
                _update_stages(runner, job["id"], stages)
                do_scan(
                    params.destination, thread_db,
                    progress_callback=progress_cb,
                    incremental=True,
                    extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                    photo_callback=photo_cb,
                    status_callback=status_cb,
                    restrict_dirs=restrict,
                )
            else:
                # Scan-in-place: scan each source folder independently.
                stages["scan"]["status"] = "running"
                runner.update_step(job["id"], "scan", status="running")
                job["progress"]["current"] = 0
                job["progress"]["total"] = 0
                _update_stages(runner, job["id"], stages)
                for src_folder in sources:
                    do_scan(
                        src_folder, thread_db,
                        progress_callback=progress_cb,
                        incremental=True,
                        extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                        photo_callback=photo_cb,
                        skip_paths=params.exclude_paths,
                        status_callback=status_cb,
                        recursive=params.recursive,
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
                except Exception:
                    failed += 1
                    log.debug("Thumbnail failed for photo %s", photo_id)
                # Include failed in the progress counter so the dashboard
                # reflects all work attempted, not just successes. Mixed
                # success/failure must not hide behind a 0/N progress bar.
                stages["thumbnails"]["count"] = generated + skipped + failed
                processed = generated + skipped + failed
                # Use scan count directly regardless of whether scan has
                # completed yet — this avoids the total staying at 0/? when
                # the thumbnail worker catches up with scan before scan's
                # status flips to "completed".
                scan_total = stages["scan"].get("count", 0)
                runner.update_step(job["id"], "thumbnails",
                                   current_file=os.path.basename(photo_path),
                                   progress={"current": processed, "total": scan_total})
                elapsed = time.time() - job["_start_time"]
                rate = round(processed / max(elapsed, 0.01) * 60, 1)
                runner.push_event(job["id"], "progress", {
                    "phase": "Generating thumbnails",
                    "stage_id": "thumbnails",
                    "current": processed,
                    "total": scan_total,
                    "current_file": os.path.basename(photo_path),
                    "rate": rate,
                    "stages": {k: dict(v) for k, v in stages.items()},
                })

            from thumbnails import format_summary as thumb_summary
            thumb_result = {"generated": generated, "skipped": skipped, "failed": failed}
            processed = generated + skipped + failed
            # Mixed-outcome rollup: any failure flips status to 'failed'.
            # The summary still shows both counts so partial success is visible,
            # but status surfaces the problem on the job history list.
            final_status = "failed" if failed > 0 else "completed"
            stages["thumbnails"]["status"] = final_status
            thumb_rollup = (
                f"[thumbnails] {failed} of {processed} thumbnails failed to generate"
                if failed > 0 else None
            )
            if thumb_rollup:
                errors.append(thumb_rollup)
            runner.update_step(job["id"], "thumbnails", status=final_status,
                               summary=thumb_summary(thumb_result),
                               error_count=failed,
                               error=thumb_rollup,
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
                photos = _filter_excluded(thread_db.get_collection_photos(collection_id, per_page=999999))
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
            failed = 0

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
                    else:
                        # image_loader already logged the failure at WARNING;
                        # count it here so it surfaces in the rollup.
                        failed += 1

                stages["previews"]["count"] = i + 1
                runner.update_step(job["id"], "previews",
                                   current_file=photo["filename"],
                                   progress={"current": i + 1, "total": total})
                runner.push_event(job["id"], "progress", {
                    "phase": "Generating previews",
                    "stage_id": "previews",
                    "current": i + 1,
                    "total": total,
                    "current_file": photo["filename"],
                    "rate": round(
                        (i + 1) / max(time.time() - job["_start_time"], 0.01) * 60, 1
                    ),
                    "stages": {k: dict(v) for k, v in stages.items()},
                })

            result["stages"]["previews"] = {
                "generated": generated, "skipped": skipped, "failed": failed, "total": total
            }
            final_status = "failed" if failed > 0 else "completed"
            stages["previews"]["status"] = final_status
            previews_rollup = (
                f"[previews] {failed} of {total} previews failed to generate"
                if failed > 0 else None
            )
            if previews_rollup:
                errors.append(previews_rollup)
            summary_parts = [f"{generated} generated"]
            if skipped:
                summary_parts.append(f"{skipped} cached")
            if failed:
                summary_parts.append(f"{failed} failed")
            runner.update_step(job["id"], "previews", status=final_status,
                               summary=", ".join(summary_parts),
                               error_count=failed,
                               error=previews_rollup)
        except Exception as e:
            errors.append(f"[previews] Fatal: {e}")
            log.exception("Pipeline previews stage failed")
            stages["previews"]["status"] = "failed"
            runner.update_step(job["id"], "previews", status="failed", error=str(e))

        _update_stages(runner, job["id"], stages)

    def _load_model_bundle(active_model, tax, thread_db):
        """Turn a resolved model spec into a ready-to-use classifier bundle.

        Loads labels for the model and constructs the Classifier/TimmClassifier,
        translating ONNXRuntime's cryptic missing-weights errors into an
        actionable "Repair" hint. Called by both the model_loader stage (for
        the first model) and the classify stage (for each subsequent model in
        a multi-model run).
        """
        from classify_job import _load_labels
        from models import _classify_model_state

        model_str = active_model["model_str"]
        weights_path = active_model["weights_path"]
        model_type = active_model.get("model_type", "bioclip")
        model_name = active_model["name"]
        model_is_custom = active_model.get("source") == "custom"

        labels, use_tol = _load_labels(
            model_type=model_type,
            model_str=model_str,
            labels_file=params.labels_file,
            labels_files=params.labels_files,
            db=thread_db,
        )

        # Preflight: validate the on-disk model before handing it to
        # ONNXRuntime. A stale _check_onnx_downloaded result (e.g. after
        # the user deleted a .onnx.data file, or the download manifest
        # changed) would otherwise surface as an opaque ONNXRuntime crash.
        # "unverified" is accepted here: all files are present, only the
        # SHA256 cross-check with HuggingFace was skipped (transient network
        # issue). The lazy verify_if_needed call below will retry the hash
        # check, and get_models() already treats these as downloaded, so
        # rejecting them here turns a warning into a hard pipeline failure.
        files = active_model.get("files", [])
        if files and weights_path:
            state = _classify_model_state(weights_path, files)
            if state not in ("ok", "unverified"):
                raise RuntimeError(
                    _incomplete_model_message(model_name, model_is_custom)
                )

        # Lazy SHA256 verification: for known models (those with an
        # hf_subdir), hash every LFS file on first load in this process
        # and compare against HuggingFace's reported SHA256. Catches
        # silent corruption and truncated downloads that slipped past
        # hf_hub_download. Result is cached in-process so subsequent
        # pipeline runs pay zero cost.
        hf_subdir = active_model.get("hf_subdir")
        if hf_subdir and not model_is_custom and weights_path:
            import model_verify
            try:
                model_verify.verify_if_needed(
                    active_model["id"], weights_path, hf_subdir
                )
            except model_verify.ModelCorruptError as verify_err:
                log.warning(
                    "Lazy verification failed for %s: %s",
                    active_model["id"], verify_err,
                )
                raise RuntimeError(
                    _incomplete_model_message(model_name, model_is_custom)
                ) from verify_err
            except model_verify.VerifyError as verify_err:
                # Can't reach HF to fetch expected hashes — log and
                # proceed. This keeps offline pipeline runs working
                # when the model is already on disk.
                log.warning(
                    "Skipping verification for %s (could not fetch "
                    "expected hashes): %s",
                    active_model["id"], verify_err,
                )

        try:
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
        except Exception as load_err:
            # ONNXRuntime signals missing external-data with a
            # "model_path must not be empty" / "Initializer" error. Treat
            # any load failure as an incomplete-model hint for the user —
            # but only when we can confirm the on-disk files are actually
            # bad. A transient ONNX failure (memory pressure, mmap race,
            # test-suite monkeypatches from another process) should not
            # permanently mark a healthy install as "Incomplete".
            if _looks_like_missing_external_data(load_err):
                import model_verify

                files_ok = False
                hf_subdir = active_model.get("hf_subdir")
                if (
                    weights_path
                    and hf_subdir
                    and not model_is_custom
                ):
                    try:
                        result = model_verify.verify_model(
                            weights_path, hf_subdir
                        )
                        files_ok = result.ok
                    except model_verify.VerifyError:
                        # Network unavailable — can't confirm either way.
                        # Fall through to the conservative path that writes
                        # the sentinel so the user sees Repair.
                        files_ok = False

                if files_ok:
                    # Files match HF hashes exactly — the ONNX error is
                    # transient, not corruption. Do NOT write
                    # .verify_failed; do NOT tell the user to Repair. Just
                    # re-raise with a retry hint.
                    log.warning(
                        "ONNXRuntime load failed for %s but on-disk files "
                        "pass SHA256 verification — treating as transient.",
                        active_model.get("id", "<unknown>"),
                    )
                    raise RuntimeError(
                        f"Model '{model_name}' failed to load "
                        f"(transient ONNXRuntime error). Retry the "
                        f"pipeline. If this keeps happening, restart Vireo."
                    ) from load_err

                # Files are bad or unverifiable — write the sentinel so
                # Settings surfaces the Repair button.
                if weights_path:
                    sentinel_path = os.path.join(
                        weights_path,
                        model_verify.VERIFY_FAILED_SENTINEL,
                    )
                    try:
                        with open(sentinel_path, "w") as f:
                            f.write(f"onnx-load-failure: {load_err}\n")
                    except OSError:
                        pass
                raise RuntimeError(
                    _incomplete_model_message(model_name, model_is_custom)
                ) from load_err
            raise

        return {
            "clf": clf,
            "model_type": model_type,
            "model_name": model_name,
            "model_str": model_str,
            "labels": labels,
            "use_tol": use_tol,
            "active_model": active_model,
        }

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
            from classify_job import _load_taxonomy

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            # Specs were pre-resolved at job start so step_defs could carry
            # the model's display name on each `classify:<id>` row. If that
            # resolution raised, surface the same error here — model_loader
            # is the stage that owns "no model / bad id" failures.
            if resolution_error:
                raise RuntimeError(resolution_error)

            first_name = resolved_specs[0]["name"]
            runner.update_step(job["id"], "model_loader", current_file=first_name)

            # Download taxonomy if missing and requested
            taxonomy_path = os.path.join(os.path.dirname(__file__), "taxonomy.json")
            if params.download_taxonomy and not os.path.exists(taxonomy_path):
                try:
                    from taxonomy import download_taxonomy
                    runner.push_event(job["id"], "progress", {
                        "phase": "Downloading taxonomy...",
                        "stage_id": "model_loader",
                        "current": 0, "total": 0,
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })
                    download_taxonomy(taxonomy_path, progress_callback=lambda msg:
                        runner.push_event(job["id"], "progress", {
                            "phase": msg,
                            "stage_id": "model_loader",
                            "current": 0, "total": 0,
                            "stages": {k: dict(v) for k, v in stages.items()},
                        })
                    )
                except Exception as e:
                    log.warning("Taxonomy download failed, continuing without: %s", e)

            # Taxonomy is shared across every classifier in the run.
            tax = _load_taxonomy(taxonomy_path)
            loaded_models["tax"] = tax
            loaded_models["resolved_specs"] = resolved_specs

            # Load the first classifier so classify_stage can start as soon
            # as scan completes; any remaining specs are loaded inside
            # classify_stage so we never hold more than one model in memory.
            runner.push_event(job["id"], "progress", {
                "phase": f"Loading {first_name}...",
                "stage_id": "model_loader",
                "current": 0, "total": 0,
                "stages": {k: dict(v) for k, v in stages.items()},
            })

            try:
                bundle = _load_model_bundle(resolved_specs[0], tax, thread_db)
                loaded_models.update(bundle)
            except Exception as preload_err:
                if len(resolved_specs) > 1:
                    # Other models remain — don't abort the whole pipeline.
                    log.warning(
                        "First model %s failed to load, %d remaining: %s",
                        first_name, len(resolved_specs) - 1, preload_err,
                    )
                    loaded_models["preload_error"] = str(preload_err)
                else:
                    # Single model — fatal, let the outer handler abort.
                    raise

            loaded_models["pending_specs"] = resolved_specs[1:]

            stages["model_loader"]["status"] = "completed"
            summary = ", ".join(s["name"] for s in resolved_specs)
            if "preload_error" in loaded_models:
                summary += f" ({first_name} failed to preload)"
            runner.update_step(job["id"], "model_loader", status="completed",
                               summary=summary)
        except Exception as e:
            errors.append(f"[model_loader] Fatal: {e}")
            log.exception("Pipeline model loader stage failed")
            abort.set()
            stages["model_loader"]["status"] = "failed"
            runner.update_step(job["id"], "model_loader", status="failed", error=str(e))
        finally:
            models_ready.set()
            _update_stages(runner, job["id"], stages)

    # Shared state between detect_stage and classify_stage. Written by
    # detect_stage, consumed by classify_stage. Populated even on early
    # exit so classify_stage can reason about "detection ran but produced
    # nothing" vs. "detection never executed".
    detect_state = {
        "photos": [],        # list of photo dicts for the collection
        "folders": {},       # {folder_id: path}
        "detections": {},    # {photo_id: [detection_dict, ...]}
        "processed_ids": set(),  # photo_ids whose _detect_batch iteration completed
        "pre_run_det_ids": {},   # snapshot for reclassify purge
        "total_detected": 0,
        "ran": False,        # True once detect_stage's body executed (even if no-op)
    }

    def detect_stage():
        """Run MegaDetector across every collection photo once, ahead of any
        classification. Populates detect_state so each per-model classify
        step can pull cached detections rather than re-running MegaDetector.

        Splitting detect out (it used to run interleaved with model 1's
        classify loop) lets users see detection as its own row in the jobs
        view, and lets a multi-model run amortize one detection pass across
        every classifier.
        """
        collection_ready.wait()
        models_ready.wait()

        has_models_to_try = (
            "clf" in loaded_models
            or loaded_models.get("resolved_specs")
        )
        if (
            params.skip_classify
            or abort.is_set()
            or not collection_id
            or not has_models_to_try
        ):
            stages["detect"]["status"] = "skipped"
            runner.update_step(job["id"], "detect", status="completed",
                               summary="Skipped")
            _update_stages(runner, job["id"], stages)
            return

        stages["detect"]["status"] = "running"
        # Also mark the aggregate classify stage as running so the pipeline
        # wizard's "Classify" card (which predates the detect/classify split)
        # shows activity during the detect pre-pass instead of waiting for
        # the first per-model classify step to start.
        stages["classify"]["status"] = "running"
        runner.update_step(job["id"], "detect", status="running")
        _update_stages(runner, job["id"], stages)

        try:
            from classify_job import _BATCH_SIZE, _detect_batch

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            photos = _filter_excluded(
                thread_db.get_collection_photos(collection_id, per_page=999999)
            )
            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            detect_state["photos"] = photos
            detect_state["folders"] = folders
            detect_state["ran"] = True

            # Reclassify semantics (see prior interleaved implementation for
            # history): start with an empty already_detected so EVERY photo
            # is re-detected; snapshot pre-run detection IDs so we can purge
            # them after this detect pass completes. On a non-reclassify run,
            # pre-seed already_detected from the DB so _detect_batch reuses
            # rows instead of re-invoking MegaDetector.
            if params.reclassify:
                already_detected: set = set()
                photo_ids_list = [p["id"] for p in photos]
                pre_run_det_ids: dict = getattr(
                    thread_db, "get_detection_ids_for_photos", lambda _: {}
                )(photo_ids_list)
            else:
                already_detected = set(
                    getattr(
                        thread_db, "get_existing_detection_photo_ids", lambda: set()
                    )()
                )
                pre_run_det_ids = {}
            detect_state["pre_run_det_ids"] = pre_run_det_ids

            # Ensure MegaDetector weights only when we actually need fresh
            # detection work — an offline rerun over already-detected photos
            # should not trigger a ~300 MB download.
            needs_fresh_detection = bool(photos) and (
                params.reclassify
                or any(p["id"] not in already_detected for p in photos)
            )
            if needs_fresh_detection:
                from detector import ensure_megadetector_weights

                def _dl_progress(phase, current, total_steps):
                    runner.push_event(job["id"], "progress", {
                        "phase": phase,
                        "stage_id": "detect",
                        "current": current, "total": total_steps,
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })

                ensure_megadetector_weights(progress_callback=_dl_progress)

            this_run_detections: dict = detect_state["detections"]
            processed_ids: set = detect_state["processed_ids"]
            total_detected = 0
            start_time = time.time()

            for batch_start in range(0, total, _BATCH_SIZE):
                if _should_abort(abort):
                    break
                batch = photos[batch_start:batch_start + _BATCH_SIZE]
                batch_idx = batch_start + len(batch)

                runner.push_event(job["id"], "progress", {
                    "phase": "Detecting subjects",
                    "stage_id": "detect",
                    "current": batch_idx,
                    "total": total,
                    "rate": round(
                        batch_idx / max(time.time() - start_time, 0.01) * 60,
                        1,
                    ),
                    "stages": {k: dict(v) for k, v in stages.items()},
                })
                stages["detect"]["count"] = batch_idx
                runner.update_step(
                    job["id"], "detect",
                    progress={"current": batch_idx, "total": total},
                )

                det_map, det_count, det_processed = _detect_batch(
                    batch, folders, runner, job,
                    params.reclassify, thread_db,
                    already_detected_ids=already_detected,
                    cached_detections=None,
                )
                total_detected += det_count
                already_detected.update(det_processed)
                for pid, dets in det_map.items():
                    this_run_detections.setdefault(pid, dets)
                for pid in det_processed:
                    this_run_detections.setdefault(pid, [])
                processed_ids.update(det_processed)

            detect_state["total_detected"] = total_detected
            # The stale-detection purge is DEFERRED to classify_stage and
            # only fires after the first model successfully classifies.
            # Deleting the pre-run detection rows here would cascade through
            # the predictions FK and destroy prior results in the case where
            # every classifier ends up failing to load — leaving the user
            # with no detections AND no predictions. See classify_stage for
            # the actual delete.

            stages["detect"]["status"] = "completed"
            runner.update_step(
                job["id"], "detect", status="completed",
                summary=(
                    f"{total_detected} animals detected in {total} photos"
                    if total else "No photos to detect"
                ),
            )
            result["stages"]["detect"] = {
                "total": total,
                "detected": total_detected,
                "processed": len(processed_ids),
            }
        except Exception as e:
            errors.append(f"[detect] Fatal: {e}")
            log.exception("Pipeline detect stage failed")
            abort.set()
            stages["detect"]["status"] = "failed"
            runner.update_step(job["id"], "detect", status="failed",
                               error=str(e))

        _update_stages(runner, job["id"], stages)

    def classify_stage():
        """Run one classifier per model against the pre-computed detections.

        Each model drives its own `classify:<model_id>` step so users see
        per-model progress, duration, and summary instead of an aggregate.
        A model that fails to load is marked `failed` on its own row — the
        run continues with remaining models.
        """
        has_models_to_try = (
            "clf" in loaded_models
            or loaded_models.get("resolved_specs")
        )
        if (
            params.skip_classify
            or abort.is_set()
            or not collection_id
            or not has_models_to_try
        ):
            stages["classify"]["status"] = "skipped"
            # Mark every per-model step completed/skipped so the job tree
            # doesn't show pending rows forever.
            specs_for_step_ids = loaded_models.get("resolved_specs") or []
            if specs_for_step_ids:
                for spec in specs_for_step_ids:
                    runner.update_step(
                        job["id"], f"classify:{spec['id']}",
                        status="completed", summary="Skipped",
                    )
            else:
                for mid in (effective_model_ids or ["__unresolved__"]):
                    runner.update_step(
                        job["id"], f"classify:{mid}",
                        status="completed", summary="Skipped",
                    )
            _update_stages(runner, job["id"], stages)
            return

        stages["classify"]["status"] = "running"
        _update_stages(runner, job["id"], stages)

        # Track which per-model rows have reached a terminal state so a
        # fatal error raised by one model doesn't overwrite the status of
        # already-completed models (P2 from the Codex review). Defined
        # outside the try so the except handler can read them.
        completed_step_ids: set = set()
        failed_step_ids: set = set()

        try:
            import config as cfg
            from classify_job import (
                _flush_batch,
                _prepare_image,
                _store_grouped_predictions,
            )

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            user_cfg = thread_db.get_effective_config(cfg.load())
            grouping_window = user_cfg.get("grouping_window_seconds", 5)
            similarity_threshold = user_cfg.get("similarity_threshold", 0.85)

            tax = loaded_models["tax"]
            resolved_specs_local = loaded_models.get("resolved_specs") or [
                loaded_models["active_model"]
            ]

            photos = detect_state["photos"]
            folders = detect_state["folders"]
            cached_detections = detect_state["detections"]
            total = len(photos)

            total_predictions_stored = 0
            total_failed = 0
            total_skipped_existing = 0
            # Track unique photo IDs that failed in any model so the rollup
            # message always produces a valid X-of-N ratio. total_failed sums
            # per-model failures and can exceed total in multi-model runs.
            failed_photo_ids: set = set()

            skipped_model_names: list = []
            models_succeeded = 0
            # Track photo IDs actually processed by the first successful
            # model's classify loop so the stale-detection purge is scoped
            # to reclassified photos only. Using detect_state["processed_ids"]
            # (all detected photos) would incorrectly delete detections for
            # photos that weren't reached if the job was aborted mid-classify.
            first_model_photo_ids: set = set()

            from datetime import datetime as dt

            for spec_idx, active_spec in enumerate(resolved_specs_local):
                step_id = f"classify:{active_spec['id']}"
                if _should_abort(abort):
                    # Anything not yet touched stays pending; mark it skipped
                    # so the job tree finalizes cleanly.
                    runner.update_step(job["id"], step_id,
                                       status="completed",
                                       summary="Skipped (cancelled)")
                    continue

                runner.update_step(job["id"], step_id, status="running")

                if spec_idx == 0 and "clf" in loaded_models:
                    # First model preloaded by model_loader_stage.
                    clf = loaded_models["clf"]
                    model_type = loaded_models["model_type"]
                    model_name = loaded_models["model_name"]
                else:
                    runner.push_event(job["id"], "progress", {
                        "phase": f"Loading {active_spec['name']}...",
                        "stage_id": "classify",
                        "step_id": step_id,
                        "current": 0, "total": total,
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })
                    # Drop the prior model's per-photo payload BEFORE loading
                    # the next bundle so we don't hold old results + new model
                    # weights concurrently. Without this, multi-model runs on
                    # large collections can hit transient OOMs.
                    with contextlib.suppress(NameError, UnboundLocalError):
                        raw_results.clear()  # noqa: F821 — bound in prior iter
                    for k in ("clf", "model_type", "model_name", "model_str",
                              "labels", "use_tol", "active_model"):
                        loaded_models.pop(k, None)
                    clf = None
                    try:
                        bundle = _load_model_bundle(active_spec, tax, thread_db)
                    except Exception as model_err:
                        log.warning(
                            "Skipping model %s: %s",
                            active_spec["name"], model_err,
                        )
                        skipped_model_names.append(active_spec["name"])
                        runner.update_step(
                            job["id"], step_id,
                            status="failed",
                            error=str(model_err),
                            summary=f"Failed to load: {model_err}",
                        )
                        failed_step_ids.add(step_id)
                        continue
                    loaded_models.update(bundle)
                    clf = bundle["clf"]
                    model_type = bundle["model_type"]
                    model_name = bundle["model_name"]

                if params.reclassify:
                    photo_ids = [p["id"] for p in photos]
                    thread_db.clear_predictions(
                        model=model_name, collection_photo_ids=photo_ids
                    )

                existing_preds = set()
                if not params.reclassify:
                    existing_preds = thread_db.get_existing_prediction_photo_ids(
                        model_name
                    )

                raw_results: list = []
                failed = 0
                skipped_existing = 0
                start_time = time.time()
                batch_size = 32  # classification batch granularity

                for batch_start in range(0, total, batch_size):
                    if _should_abort(abort):
                        break
                    batch = photos[batch_start:batch_start + batch_size]
                    batch_idx = batch_start + len(batch)

                    phase_label = (
                        f"Classifying with {active_spec['name']}"
                        + (
                            f" ({spec_idx + 1}/{len(resolved_specs_local)})"
                            if len(resolved_specs_local) > 1 else ""
                        )
                    )
                    runner.push_event(job["id"], "progress", {
                        "phase": phase_label,
                        "stage_id": "classify",
                        "step_id": step_id,
                        "current": batch_idx,
                        "total": total,
                        "rate": round(
                            batch_idx / max(time.time() - start_time, 0.01) * 60,
                            1,
                        ),
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })
                    stages["classify"]["count"] = batch_idx
                    runner.update_step(
                        job["id"], step_id,
                        progress={"current": batch_idx, "total": total},
                    )

                    for photo in batch:
                        # Record this photo as classify-processed for the first
                        # successful model. Used by the stale-detection purge to
                        # restrict deletions to photos actually reclassified.
                        if models_succeeded == 0:
                            first_model_photo_ids.add(photo["id"])
                        if photo["id"] in existing_preds:
                            skipped_existing += 1
                            pred_row = thread_db.get_prediction_for_photo(
                                photo["id"], model_name,
                            )
                            if pred_row:
                                folder_path = folders.get(photo["folder_id"], "")
                                image_path = os.path.join(
                                    folder_path, photo["filename"],
                                )
                                timestamp = None
                                if photo["timestamp"]:
                                    with contextlib.suppress(ValueError, TypeError):
                                        timestamp = dt.fromisoformat(
                                            photo["timestamp"]
                                        )
                                embedding = None
                                if model_type != "timm":
                                    emb_blob = thread_db.get_photo_embedding(
                                        photo["id"]
                                    )
                                    if emb_blob:
                                        import numpy as np
                                        embedding = np.frombuffer(
                                            emb_blob, dtype=np.float32,
                                        )
                                raw_results.append({
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
                                    "_existing": True,
                                })
                            continue

                        # Pull the primary detection for this photo from the
                        # detect-stage cache. Fall back to db.get_detections()
                        # only for photos whose per-photo detect iteration
                        # never completed (e.g. mid-batch exception, or the
                        # detect stage was skipped for an already-detected
                        # non-reclassify run — in which case the DB holds the
                        # authoritative rows). Photos with no detections get
                        # skipped entirely; pipeline classify is detection-
                        # driven and won't synthesize full-image boxes.
                        if photo["id"] in cached_detections:
                            photo_dets = cached_detections[photo["id"]]
                        else:
                            photo_dets = [
                                {
                                    "id": d["id"],
                                    "box_x": d["box_x"],
                                    "box_y": d["box_y"],
                                    "box_w": d["box_w"],
                                    "box_h": d["box_h"],
                                    "confidence": d["detector_confidence"],
                                    "category": d["category"],
                                }
                                for d in thread_db.get_detections(photo["id"])
                                if d["detector_model"] != "full-image"
                            ]
                        primary_det = photo_dets[0] if photo_dets else None
                        if primary_det is None:
                            continue

                        img, folder_path, image_path = _prepare_image(
                            photo, folders, primary_det,
                        )
                        if img is None:
                            failed += 1
                            failed_photo_ids.add(photo["id"])
                            continue
                        img_batch = [{
                            "photo": photo,
                            "detection_id": primary_det["id"],
                            "folder_path": folder_path,
                            "image_path": image_path,
                            "img": img,
                        }]
                        n_batch_failed = _flush_batch(
                            img_batch, clf, model_type, model_name,
                            thread_db, raw_results,
                        )
                        if n_batch_failed:
                            failed_photo_ids.add(photo["id"])
                        failed += n_batch_failed

                group_result = _store_grouped_predictions(
                    raw_results, job["id"], model_name,
                    grouping_window, similarity_threshold, tax, thread_db,
                )
                preds = group_result["predictions_stored"]
                total_predictions_stored += preds
                total_failed += failed
                total_skipped_existing += skipped_existing
                models_succeeded += 1
                completed_step_ids.add(step_id)

                # Reclassify stale-row purge: only fires after the FIRST
                # successful model has written fresh predictions, so a run
                # where every model fails to load leaves prior detections
                # (and their cascaded predictions) intact. Photos whose
                # detect iteration never completed keep their old rows.
                if (
                    params.reclassify
                    and models_succeeded == 1
                    and detect_state["pre_run_det_ids"]
                ):
                    pre_ids = detect_state["pre_run_det_ids"]
                    # Scope the purge to photos whose detect AND classify
                    # iterations both completed in this run. Using only
                    # classify coverage would delete rows for photos that
                    # hit the db.get_detections() fallback (i.e. never got
                    # a fresh detect). Using only detect coverage would
                    # delete rows for photos the classifier never reached.
                    # The intersection guarantees there's a replacement
                    # detection AND that the classifier considered it.
                    purge_ids = (
                        first_model_photo_ids
                        & detect_state["processed_ids"]
                    )
                    stale_ids = [
                        det_id
                        for photo_id, id_set in pre_ids.items()
                        for det_id in id_set
                        if photo_id in purge_ids
                    ]
                    if stale_ids:
                        getattr(
                            thread_db,
                            "delete_detections_by_ids",
                            lambda _: None,
                        )(stale_ids)
                        log.debug(
                            "reclassify: purged %d stale detection rows for "
                            "%d photos (%d not in purge scope, rows preserved)",
                            len(stale_ids),
                            len(purge_ids & pre_ids.keys()),
                            len(pre_ids) - len(purge_ids & pre_ids.keys()),
                        )

                parts = [f"{preds} predictions"]
                if skipped_existing:
                    parts.append(f"{skipped_existing} cached")
                if failed:
                    parts.append(f"{failed} failed")
                runner.update_step(
                    job["id"], step_id, status="completed",
                    summary=", ".join(parts),
                )

            if models_succeeded == 0 and skipped_model_names:
                raise RuntimeError(
                    f"All {len(skipped_model_names)} model(s) failed to load: "
                    + ", ".join(skipped_model_names)
                )

            # Roll up per-photo failures into a single classify stage status
            # + errors[] entry, matching the pattern in #562. Per-model step
            # rows already carry their own summary; the stage status reflects
            # the whole classify pass.  error_count uses unique failed photo
            # IDs (not per-model attempt count) so the badge can never
            # exceed total photos.
            n_failed_photos = len(failed_photo_ids)
            stages["classify"]["status"] = (
                "failed" if total_failed > 0 else "completed"
            )
            if total_failed > 0:
                errors.append(
                    f"[classify] {n_failed_photos} of {total} photos "
                    "failed to classify"
                )
            result["stages"]["classify"] = {
                "total": total,
                "predictions_stored": total_predictions_stored,
                "detected": detect_state["total_detected"],
                "failed": total_failed,
                "already_classified": total_skipped_existing,
                "model_count": len(resolved_specs_local),
                "models_succeeded": models_succeeded,
                "models_skipped": len(skipped_model_names),
                "skipped_model_names": skipped_model_names,
            }
        except Exception as e:
            errors.append(f"[classify] Fatal: {e}")
            log.exception("Pipeline classify stage failed")
            abort.set()
            stages["classify"]["status"] = "failed"
            # Only surface the fatal error on rows that haven't already
            # reached a terminal state. Without this, a late-loop exception
            # would overwrite the 'completed' status of earlier models that
            # finished successfully, misreporting per-model outcomes.
            specs_for_step_ids = loaded_models.get("resolved_specs") or []
            for spec in specs_for_step_ids:
                sid = f"classify:{spec['id']}"
                if sid in completed_step_ids or sid in failed_step_ids:
                    continue
                runner.update_step(
                    job["id"], sid,
                    status="failed", error=str(e),
                )

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

            photos = _filter_excluded(thread_db.get_collection_photos(collection_id, per_page=999999))

            # Build a map of photo_id -> primary detection (highest confidence)
            # from the detections table. Only photos with detections and without
            # masks need processing.
            #
            # Skip synthetic full-image detections (detector_model='full-image').
            # Those rows exist only to give classify predictions a non-NULL FK
            # anchor for photos where MegaDetector found no animals — they are
            # not real subject boxes and should not drive mask extraction or
            # count toward the photos_with_detections safeguard below (which
            # surfaces the "weights missing / no detections" diagnostic).
            photo_det_map = {}
            photos_with_detections = 0
            for p in photos:
                dets = [
                    d for d in thread_db.get_detections(p["id"])
                    if d["detector_model"] != "full-image"
                ]
                if dets:
                    photos_with_detections += 1
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

            # If the input collection has photos but none carry detections,
            # surface a clear status instead of silently completing with
            # masked=0 — otherwise the pipeline rejects every photo with
            # no_subject_mask without explaining why masks were never made.
            # Distinguish two cases:
            #   (a) weights missing → actionable remediation
            #   (b) weights present → legitimate outcome (empty scenes,
            #       strict confidence threshold, non-wildlife photos)
            # Only fire this diagnostic when classify actually ran in this
            # invocation.  If classify was skipped (skip_classify=True, no
            # models available, abort, etc.) zero detections is expected and
            # appending an extract_masks error would be factually incorrect.
            classify_ran = stages["classify"]["status"] not in ("skipped", "pending")
            if photos_with_detections == 0 and len(photos) > 0 and classify_ran:
                weights_present = False
                try:
                    from detector import MEGADETECTOR_ONNX_PATH
                    weights_present = os.path.isfile(MEGADETECTOR_ONNX_PATH)
                except ImportError:
                    weights_present = False

                if weights_present:
                    reason = (
                        f"No detections produced for {len(photos)} photo(s). MegaDetector ran but "
                        "found no animals meeting the confidence threshold. The pipeline will "
                        "reject every photo with `no_subject_mask`. Lower `detector_confidence` "
                        "in settings or rerun classify with a different threshold if detections "
                        "were expected."
                    )
                    summary = "Skipped — MegaDetector produced no detections"
                else:
                    reason = (
                        f"No detections available for {len(photos)} photo(s). MegaDetector "
                        "weights are not downloaded, so the classify stage ran on full images "
                        "and stored no detections. Without detections the mask extraction stage "
                        "has nothing to process, and the pipeline will reject every photo with "
                        "`no_subject_mask`. Download MegaDetector V6 from the pipeline models "
                        "page and rerun the pipeline."
                    )
                    summary = "Skipped — MegaDetector weights not downloaded"

                log.warning("Pipeline extract-masks: %s", reason)
                errors.append(f"[extract_masks] {reason}")
                stages["extract_masks"]["status"] = "skipped"
                runner.update_step(
                    job["id"], "extract_masks", status="completed",
                    summary=summary,
                )
                result["stages"]["extract_masks"] = {
                    "masked": 0, "skipped": 0, "failed": 0, "total": 0,
                    "reason": "weights_missing" if not weights_present else "no_detections",
                }
                _update_stages(runner, job["id"], stages)
                return

            # Auto-download SAM2 + DINOv2 weights on first pipeline run.
            # Mirrors the MegaDetector auto-download pattern (commit 90cd0f9):
            # without this, first-time users hit 1 FileNotFoundError per
            # photo (e.g. 124 identical tracebacks) instead of either
            # getting the weights automatically or seeing one actionable
            # message.  Only fire when there is actually work to do — a
            # no-op rerun over 0 photos should not trigger a multi-hundred
            # MB download.
            if total > 0:
                from dino_embed import ensure_dinov2_weights
                from masking import ensure_sam2_weights

                def _dl_progress(phase, current, total_steps):
                    runner.push_event(job["id"], "progress", {
                        "phase": phase,
                        "stage_id": "extract_masks",
                        "current": current, "total": total_steps,
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })

                ensure_sam2_weights(
                    variant=sam2_variant, progress_callback=_dl_progress,
                )
                ensure_dinov2_weights(
                    variant=dinov2_variant, progress_callback=_dl_progress,
                )

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
                        variant=dinov2_variant,
                    )
                    masked += 1
                except Exception:
                    em_failed += 1
                    log.warning("Mask extraction failed for photo %s", photo_id, exc_info=True)

                stages["extract_masks"]["count"] = i + 1
                runner.update_step(job["id"], "extract_masks",
                                   progress={"current": i + 1, "total": total},
                                   error_count=em_failed)
                runner.push_event(job["id"], "progress", {
                    "phase": "Extracting features (SAM2 + DINOv2)",
                    "stage_id": "extract_masks",
                    "current": i + 1,
                    "total": total,
                    "rate": round((i + 1) / max(time.time() - start_time, 0.01) * 60, 1),
                    "stages": {k: dict(v) for k, v in stages.items()},
                })

            final_status = "failed" if em_failed > 0 else "completed"
            stages["extract_masks"]["status"] = final_status
            em_rollup = (
                f"[extract_masks] {em_failed} of {total} photos failed mask extraction"
                if em_failed > 0 else None
            )
            if em_rollup:
                errors.append(em_rollup)
            em_summary_parts = [f"{masked} masked", f"{skipped} skipped"]
            if em_failed:
                em_summary_parts.append(f"{em_failed} failed")
            runner.update_step(job["id"], "extract_masks", status=final_status,
                               summary=", ".join(em_summary_parts),
                               error_count=em_failed,
                               error=em_rollup)
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
            if params.exclude_photo_ids:
                photos = [p for p in photos if p["id"] not in params.exclude_photo_ids]
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

    # Phase 2: detect (needs collection; runs MegaDetector once across all
    # photos so each per-model classify step reuses cached detections
    # instead of re-running the detector).
    #
    # Always invoked — even when `abort` is set by an earlier stage — so
    # the `detect` step row reaches a terminal status. Skipping the call
    # would leave the row pending forever on a model-loader failure.
    # detect_stage handles abort internally and marks itself skipped.
    detect_stage()

    # Phase 3: classify per model (reads cached detections from detect_stage).
    # Always invoked for the same reason: every `classify:<model_id>` row
    # must land in a terminal state so the jobs tree finalizes cleanly on
    # a loader-triggered abort.
    classify_stage()

    # Phase 3: extract-masks (needs classify output)
    if not abort.is_set():
        extract_masks_stage()

    # Phase 4: regroup (needs extract-masks output)
    if not abort.is_set():
        regroup_stage()

    cancel_watcher_stop.set()

    elapsed = time.time() - job["_start_time"]
    result["duration"] = round(elapsed, 1)
    result["errors"] = list(errors)

    # If any stage ended in 'failed' and the job wasn't cancelled, propagate
    # the failure so JobRunner marks the whole job as failed rather than
    # silently recording it as completed. Cancellation takes precedence:
    # a cancelled job stays cancelled even if stages crashed on the way down.
    failed_stages = [
        name for name, s in stages.items() if s.get("status") == "failed"
    ]
    if failed_stages and not runner.is_cancelled(job["id"]):
        # Stash the structured result on the job BEFORE raising so the
        # completion event and job_history still carry per-stage details
        # (stages dict, errors list). Without this, the pipeline UI loses
        # the "Failed: [stage_name]" mapping on the card that owned the
        # failure because it reads result.result.stages / .errors.
        job["result"] = result
        # Prefer a "[stage] Fatal: …" error from one of the failed stages
        # rather than blindly using errors[0], which may be a non-fatal
        # per-photo warning (e.g. "Photo <id>: mask extraction failed")
        # logged before the stage-level failure. Falling back to errors[0]
        # when no stage-fatal entry exists keeps backward compatibility for
        # any edge case where a stage marks itself failed without appending a
        # Fatal error; the final fallback covers an empty errors list.
        first_error = next(
            (e for e in errors if any(e.startswith(f"[{s}] Fatal:") for s in failed_stages)),
            errors[0] if errors else f"stage '{failed_stages[0]}' failed",
        )
        # Record the fatal error for _persist_job so it can store the stage
        # failure message rather than job["errors"][0], which may be a
        # non-fatal per-photo warning that was logged before this failure.
        job["_fatal_error"] = first_error
        raise RuntimeError(first_error)

    return result
