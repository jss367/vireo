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
    for name in ["regroup", "extract_masks", "classify", "model_loader",
                 "previews", "thumbnails", "scan", "ingest"]:
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
        "classify": {"status": "pending", "count": 0, "label": "Classifying species"},
        "extract_masks": {"status": "pending", "count": 0, "label": "Extracting features"},
        "regroup": {"status": "pending", "label": "Grouping encounters"},
    }

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

    # Normalize model_ids: prefer the explicit list, fall back to the legacy
    # single `model_id`, and finally to `[]` which means "use the active model
    # from config." This is the knob the multi-model fix hangs off of.
    if params.model_ids:
        effective_model_ids = list(params.model_ids)
    elif params.model_id:
        effective_model_ids = [params.model_id]
    else:
        effective_model_ids = []

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

    def _resolve_model_spec(model_id_arg):
        """Look up a model by id and require it to be fully downloaded."""
        from models import get_active_model, get_models

        if model_id_arg:
            all_models = get_models()
            spec = next(
                (m for m in all_models if m["id"] == model_id_arg and m["downloaded"]),
                None,
            )
            if not spec:
                raise RuntimeError(
                    f"Model '{model_id_arg}' not found or not downloaded."
                )
            return spec
        spec = get_active_model()
        if not spec:
            raise RuntimeError("No model available. Download one in Settings.")
        return spec

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
            # any load failure as an incomplete-model hint for the user.
            if _looks_like_missing_external_data(load_err):
                # Write the .verify_failed sentinel so that
                # _classify_model_state (used by get_models() / Settings
                # UI) also reports 'incomplete' and shows the Repair
                # button.  Without this the pipeline tells the user to
                # "click Repair" but Settings sees all files present and
                # no sentinel, so no Repair button appears.
                if weights_path:
                    import model_verify
                    try:
                        with open(
                            os.path.join(
                                weights_path,
                                model_verify.VERIFY_FAILED_SENTINEL,
                            ),
                            "w",
                        ) as f:
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

            # Resolve every requested model upfront so a bad id fails fast
            # BEFORE taxonomy download, rather than mid-run after the user
            # has already watched one classifier finish.
            if effective_model_ids:
                resolved_specs = [
                    _resolve_model_spec(mid) for mid in effective_model_ids
                ]
            else:
                resolved_specs = [_resolve_model_spec(None)]

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

    def classify_stage():
        """Wait for collection + models, then run interleaved detect+classify."""
        collection_ready.wait()
        models_ready.wait()

        # Skip classify only when there is truly nothing to run.  When the
        # first model's preload failed but resolved_specs still contains
        # other models, we must NOT skip — classify_stage will try each
        # remaining spec in turn.
        has_models_to_try = (
            "clf" in loaded_models
            or loaded_models.get("resolved_specs")
        )
        if params.skip_classify or abort.is_set() or not collection_id or not has_models_to_try:
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
            grouping_window = user_cfg.get("grouping_window_seconds", 5)
            similarity_threshold = user_cfg.get("similarity_threshold", 0.85)

            tax = loaded_models["tax"]
            resolved_specs = loaded_models.get("resolved_specs") or [
                loaded_models["active_model"]
            ]

            photos = _filter_excluded(thread_db.get_collection_photos(collection_id, per_page=999999))
            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)

            # Aggregate counters across every model run. The UI reads a single
            # stages.classify dict, so we sum across models rather than emit
            # per-model rows.
            total_predictions_stored = 0
            total_detected = 0
            total_failed = 0
            total_skipped_existing = 0

            # For reclassify: start with an empty already_detected so model 1
            # re-runs MegaDetector on every photo. We intentionally do NOT
            # clear detection rows from the DB up-front: doing so would
            # cascade-delete prediction rows from OTHER models (not in this
            # run's model_ids) via the predictions.detection_id FK, causing
            # permanent data loss for any model not included in the subset
            # reclassify. Instead, model 2+ receives the this_run_detections
            # cache (filled by model 1's detect pass) via _detect_batch's
            # cached_detections parameter, so later models bind predictions
            # to the detection rows just produced — not to stale rows from a
            # prior pass that db.get_detections() would otherwise return.
            #
            # After model 1's full detection pass we DELETE the pre-run
            # detection rows (snapshotted below) for all collection photos.
            # This prevents stale prior-run boxes from being reused by
            # subsequent non-reclassify runs via get_existing_detection_photo_ids
            # + db.get_detections() (the false-positive reuse regression flagged
            # in the Codex review on #511).  The cascade to predictions is
            # intentional: any predictions that referenced the OLD detection rows
            # are now stale (MegaDetector re-ran and produced fresh rows).
            #
            # Non-reclassify runs keep existing detections so the cached path
            # in _detect_batch can reuse them (that's the whole point of the
            # pre-seed).
            if params.reclassify:
                already_detected = set()
                # Snapshot detection IDs that exist BEFORE this run so we can
                # delete them after model 1 inserts fresh rows.
                photo_ids_list = [p["id"] for p in photos]
                _pre_run_det_ids: dict = getattr(
                    thread_db, "get_detection_ids_for_photos", lambda _: {}
                )(photo_ids_list)
            else:
                already_detected = set(
                    getattr(thread_db, "get_existing_detection_photo_ids", lambda: set())()
                )
                _pre_run_det_ids = {}

            # Accumulates the detection rows produced by model 1 (spec_idx==0)
            # so model 2+ can reference exactly those rows rather than calling
            # db.get_detections() which would include stale rows from prior runs.
            # Only allocated for multi-model runs; single-model runs never read
            # the cache, so populating it would just waste memory.
            _multi_model = len(resolved_specs) > 1
            this_run_detections: dict = {}

            # Tracks photo IDs whose per-photo iteration in _detect_batch ran
            # to completion during model 1's pass.  Used to gate the stale
            # detection purge: only photos that were actually re-processed in
            # this run should lose their prior-run rows.  Photos whose batch
            # was never reached (abort) or whose iteration was cut short by a
            # mid-batch exception keep their old detection rows so a partial
            # reclassify does not cause data loss.
            _model1_processed_photo_ids: set = set()

            # Track models that failed to load so we can report them.
            skipped_model_names: list = []
            models_succeeded = 0

            from datetime import datetime as dt

            # Ensure MegaDetector weights are on disk before any fresh detection
            # runs. Skip when every photo already has cached detections and we're
            # not reclassifying — _detect_batch will reuse DB rows and never call
            # MegaDetector, so an offline rerun should not abort on missing weights.
            # Also skip for empty photo sets — a no-op reclassify over 0 photos
            # should not trigger a ~300 MB download.
            needs_fresh_detection = bool(photos) and (
                params.reclassify or any(
                    p["id"] not in already_detected for p in photos
                )
            )
            if needs_fresh_detection:
                from detector import ensure_megadetector_weights

                def _dl_progress(phase, current, total_steps):
                    runner.push_event(job["id"], "progress", {
                        "phase": phase,
                        "stage_id": "classify",
                        "current": current, "total": total_steps,
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })

                ensure_megadetector_weights(progress_callback=_dl_progress)

            for spec_idx, active_spec in enumerate(resolved_specs):
                if _should_abort(abort):
                    break

                if spec_idx == 0 and "clf" in loaded_models:
                    # First model was preloaded by model_loader_stage.
                    clf = loaded_models["clf"]
                    model_type = loaded_models["model_type"]
                    model_name = loaded_models["model_name"]
                else:
                    # Either a secondary model (spec_idx > 0), or the first
                    # model whose preload failed in model_loader_stage.
                    # Load it now with try/except so one bad model doesn't
                    # kill the entire multi-model run.
                    runner.push_event(job["id"], "progress", {
                        "phase": f"Loading {active_spec['name']}...",
                        "stage_id": "classify",
                        "current": 0, "total": total,
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })
                    # Release previous model from memory.
                    for k in ("clf", "model_type", "model_name", "model_str",
                              "labels", "use_tol", "active_model"):
                        loaded_models.pop(k, None)
                    # Drop the local clf reference so the previous ONNX graph
                    # is eligible for GC before the next model is loaded.
                    # Without this, two large model graphs can be resident
                    # simultaneously during the handoff.
                    clf = None
                    # Also release the previous iteration's output list so
                    # embeddings/image metadata for all photos don't stay
                    # alive during the model-load handoff.
                    raw_results = None
                    try:
                        bundle = _load_model_bundle(active_spec, tax, thread_db)
                    except Exception as model_err:
                        log.warning(
                            "Skipping model %s: %s",
                            active_spec["name"], model_err,
                        )
                        runner.push_event(job["id"], "progress", {
                            "phase": f"Skipping {active_spec['name']}: {model_err}",
                            "stage_id": "classify",
                            "current": 0, "total": total,
                            "stages": {k: dict(v) for k, v in stages.items()},
                        })
                        skipped_model_names.append(active_spec["name"])
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
                    existing_preds = thread_db.get_existing_prediction_photo_ids(model_name)

                # Interleaved detect + classify in batches
                raw_results = []
                failed = 0
                detected = 0
                skipped_existing = 0
                start_time = time.time()

                for batch_start in range(0, total, _BATCH_SIZE):
                    if _should_abort(abort):
                        break

                    batch = photos[batch_start:batch_start + _BATCH_SIZE]
                    batch_idx = batch_start + len(batch)

                    if len(resolved_specs) > 1:
                        phase_label = (
                            f"Classifying species ({model_name}, "
                            f"{spec_idx + 1}/{len(resolved_specs)})"
                        )
                    else:
                        phase_label = "Classifying species"

                    runner.push_event(job["id"], "progress", {
                        "phase": phase_label,
                        "stage_id": "classify",
                        "current": batch_idx,
                        "total": total,
                        "rate": round(batch_idx / max(time.time() - start_time, 0.01) * 60, 1),
                        "stages": {k: dict(v) for k, v in stages.items()},
                    })
                    stages["classify"]["count"] = batch_idx
                    runner.update_step(job["id"], "classify",
                                       progress={"current": batch_idx, "total": total})

                    # Detect this batch. Pass already_detected so subsequent
                    # models skip MegaDetector on photos that already have
                    # detections in the DB.  On reclassify runs, only the
                    # first *successfully processed* model re-runs detection
                    # (models_succeeded == 0 before this iteration completes);
                    # subsequent models share those detections rather than
                    # inserting duplicate rows for the same photos.
                    # cached_detections gives model 2+ the exact detection
                    # rows from this run rather than querying the DB (which
                    # could return stale rows from a prior pipeline pass).
                    det_map, det_count, det_processed_ids = _detect_batch(
                        batch, folders, runner, job,
                        params.reclassify and models_succeeded == 0, thread_db,
                        already_detected_ids=already_detected,
                        cached_detections=this_run_detections if _multi_model else None,
                    )
                    detected += det_count
                    # Track ALL processed photos — including those where
                    # MegaDetector found zero detections — so model 2+ skips
                    # MegaDetector for them instead of re-running detection on
                    # empty-frame photos each iteration.
                    already_detected.update(det_processed_ids)
                    if _multi_model:
                        # Cache detections from every model iteration so
                        # later models use this-run rows rather than stale
                        # rows from db.get_detections().  Only add photos
                        # not already cached — model 1's results take
                        # precedence for photos it successfully processed.
                        for pid, dets in det_map.items():
                            if pid not in this_run_detections:
                                this_run_detections[pid] = dets
                        for pid in det_processed_ids:
                            if pid not in this_run_detections:
                                this_run_detections[pid] = []
                    if models_succeeded == 0:
                        # Key purge eligibility on photos whose per-photo
                        # iteration in _detect_batch actually completed —
                        # not the whole submitted batch.  If _detect_batch
                        # caught an exception mid-loop and returned early,
                        # unprocessed photos will be absent from this set
                        # and their stale rows will be preserved.
                        # Use models_succeeded == 0 (not spec_idx == 0) so
                        # this still fires when the first spec failed to load
                        # and a later spec is the first to successfully run.
                        _model1_processed_photo_ids.update(det_processed_ids)

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
                                    with contextlib.suppress(ValueError, TypeError):
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

                # Group and store predictions for this model
                group_result = _store_grouped_predictions(
                    raw_results, job["id"], model_name,
                    grouping_window, similarity_threshold, tax, thread_db,
                )

                total_predictions_stored += group_result["predictions_stored"]
                total_detected += detected
                total_failed += failed
                total_skipped_existing += skipped_existing
                models_succeeded += 1

                # After the first successfully processed model has inserted all
                # fresh detection rows, delete the pre-run stale rows we
                # snapshotted before the loop.  This prevents stale prior-run
                # boxes from polluting future non-reclassify runs (the
                # false-positive reuse regression flagged by Codex on #511
                # line 848).  We do this AFTER the first model's full pass —
                # not batch-by-batch — so that all new rows are committed
                # before the old ones are removed.
                # models_succeeded == 1 (just incremented) identifies the
                # first successfully completed model regardless of spec index,
                # so the purge still fires even when spec_idx == 0 was skipped
                # due to a model load failure.
                if params.reclassify and models_succeeded == 1 and _pre_run_det_ids:
                    # Only purge stale rows for photos whose per-photo
                    # iteration in _detect_batch actually ran to completion.
                    # If the run was aborted before a batch was submitted,
                    # or _detect_batch caught an exception and returned
                    # mid-batch, unprocessed photos are absent from
                    # _model1_processed_photo_ids and their old rows stay.
                    stale_ids = [
                        det_id
                        for photo_id, id_set in _pre_run_det_ids.items()
                        for det_id in id_set
                        if photo_id in _model1_processed_photo_ids
                    ]
                    if stale_ids:
                        getattr(
                            thread_db, "delete_detections_by_ids", lambda _: None
                        )(stale_ids)
                        processed_with_priors = (
                            _model1_processed_photo_ids & _pre_run_det_ids.keys()
                        )
                        log.debug(
                            "reclassify: purged %d stale detection rows for %d "
                            "photos (%d photos not processed, rows preserved)",
                            len(stale_ids),
                            len(processed_with_priors),
                            len(_pre_run_det_ids) - len(processed_with_priors),
                        )

            # If every model failed to load, mark classify as failed.
            if models_succeeded == 0 and skipped_model_names:
                fail_msg = (
                    f"All {len(skipped_model_names)} model(s) failed to load: "
                    + ", ".join(skipped_model_names)
                )
                raise RuntimeError(fail_msg)

            summary_parts = [f"{total_predictions_stored} predictions"]
            if skipped_model_names:
                skipped_str = ", ".join(skipped_model_names)
                summary_parts.append(
                    f"{len(skipped_model_names)} model(s) skipped: {skipped_str}"
                )

            stages["classify"]["status"] = "completed"
            runner.update_step(job["id"], "classify", status="completed",
                               summary="; ".join(summary_parts))
            result["stages"]["classify"] = {
                "total": total,
                "predictions_stored": total_predictions_stored,
                "detected": total_detected,
                "failed": total_failed,
                "already_classified": total_skipped_existing,
                "model_count": len(resolved_specs),
                "models_succeeded": models_succeeded,
                "models_skipped": len(skipped_model_names),
                "skipped_model_names": skipped_model_names,
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

            photos = _filter_excluded(thread_db.get_collection_photos(collection_id, per_page=999999))

            # Build a map of photo_id -> primary detection (highest confidence)
            # from the detections table. Only photos with detections and without
            # masks need processing.
            photo_det_map = {}
            photos_with_detections = 0
            for p in photos:
                dets = thread_db.get_detections(p["id"])
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

    # Phase 2: classify (needs collection + models)
    if not abort.is_set():
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
