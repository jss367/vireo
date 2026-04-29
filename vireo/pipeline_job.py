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
import math
import os
import queue
import threading
import time
from dataclasses import dataclass

from db import Database, commit_with_retry

log = logging.getLogger(__name__)

_SENTINEL = object()  # unique end-of-stream marker


@dataclass
class PipelineParams:
    """Parameters for a streaming pipeline job."""

    collection_id: int | None = None
    source: str | None = None
    sources: list | None = None
    source_snapshot_id: int | None = None
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


_RAW_EXTENSIONS = (".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf")


def _find_broken_metadata_folders(db, photo_ids):
    """Find folders containing photos with broken metadata in the given scope.

    A photo is considered broken if EXIF extraction never produced usable
    output (``exif_data IS NULL``) and either ``timestamp IS NULL`` or,
    for RAW files, dimensions under 1000px — the latter indicates the
    embedded JPEG thumbnail leaked through instead of the true sensor
    size. The ``exif_data IS NULL`` clause mirrors the scanner's
    ``exif_extracted`` guard: once ExifTool has stored output for a
    photo, the scanner won't retry regardless of signal, so flagging
    such rows here would cause the repair path to fire on every pipeline
    run without accomplishing anything.

    Rows whose file no longer exists on disk are filtered out: the
    scanner only repairs files it can rediscover via ``Path.iterdir()``,
    so a missing-file row would stay broken forever and keep the
    collection stuck in repair mode on every run instead of returning to
    the fast-path "Skipped (using collection)" summary.

    IDs are queried in chunks of at most 900 to stay safely under
    SQLite's default bound-parameter limit (SQLITE_LIMIT_VARIABLE_NUMBER,
    typically 999 in production builds). Without chunking, large
    collections would hit ``OperationalError: too many SQL variables``
    and abort the scan stage.

    Returns a list of ``(folder_path, file_paths)`` tuples where
    ``file_paths`` is a list of absolute image paths in that folder.
    Empty list when nothing needs repair.
    """
    if not photo_ids:
        return []
    raw_list = ",".join(f"'{e}'" for e in _RAW_EXTENSIONS)
    ids = list(photo_ids)
    _CHUNK = 900
    by_folder: dict[str, list[str]] = {}
    for i in range(0, len(ids), _CHUNK):
        chunk = ids[i : i + _CHUNK]
        placeholders = ",".join("?" * len(chunk))
        rows = db.conn.execute(
            f"""SELECT f.path AS folder_path, p.filename
                FROM photos p
                JOIN folders f ON p.folder_id = f.id
                WHERE p.id IN ({placeholders})
                  AND p.exif_data IS NULL
                  AND (p.timestamp IS NULL
                       OR (p.extension IN ({raw_list})
                           AND p.width IS NOT NULL AND p.width < 1000))
                ORDER BY f.path, p.filename""",
            tuple(chunk),
        ).fetchall()
        for r in rows:
            full_path = os.path.join(r["folder_path"], r["filename"])
            if not os.path.isfile(full_path):
                continue
            by_folder.setdefault(r["folder_path"], []).append(full_path)
    return [(fp, paths) for fp, paths in by_folder.items()]


# Approximate relative runtime cost per stage, used to weight the overall
# progress bar so a fast stage finishing doesn't push the bar to 100%.
# Heuristic: classify dominates on big imports; detect and eye_keypoints are
# also GPU-heavy; ingest / model_loader / regroup are quick.
STAGE_WEIGHTS = {
    "ingest": 2,
    "scan": 8,
    "thumbnails": 6,
    "previews": 6,
    "model_loader": 2,
    "detect": 15,
    "classify": 30,
    "extract_masks": 10,
    "eye_keypoints": 15,
    "regroup": 6,
    "misses": 4,
}


def _stage_fraction(info):
    """Return a 0..1 completion fraction for one stage entry.

    'failed' stages still contribute their partial progress: heavy stages
    like classify and extract_masks often process most items before marking
    themselves failed due to per-item errors, and dropping their weight to
    0 would make the overall bar lurch backward when that failure surfaces.

    Prefer ``seen`` (every photo the stage iterated past, regardless of
    outcome) when present; fall back to ``count`` for stages that don't
    surface seen. Without this, classify's per-photo split into ``count``
    (inferred) + ``cached`` would leave the overall pipeline bar stuck on
    cached-heavy runs, since count alone stops growing once the cache
    starts hitting."""
    status = info.get("status", "pending")
    if status in ("completed", "skipped"):
        return 1.0
    if status not in ("running", "failed"):
        return 0.0
    total = info.get("total") or 0
    progressed = info.get("seen")
    if progressed is None:
        progressed = info.get("count") or 0
    if total <= 0:
        return 0.0
    if progressed >= total:
        return 1.0
    return progressed / total


def _weighted_progress(stages):
    """Overall pipeline progress as (current, total), weighted by stage cost.

    Scaled so total == sum(STAGE_WEIGHTS.values()), which keeps the UI's
    `Math.round(current/total * 100)` rendering whole percent steps.

    Uses floor rather than round: a done-but-not-quite value like 99.94
    must not render as 100 because the overall bar reaching total is what
    the UI treats as 'pipeline complete'. Only a genuinely completed
    pipeline (all stages completed/skipped) produces done == total."""
    total = sum(STAGE_WEIGHTS.values())
    if total == 0:
        return 0, 0
    done = sum(
        weight * _stage_fraction(stages.get(name, {}))
        for name, weight in STAGE_WEIGHTS.items()
    )
    return int(math.floor(done)), total


def _progress_event(stages, stage_id, phase, **extra):
    """Build a push_event 'progress' payload with weighted overall current/total.

    Call sites pass per-stage context (stage_id, phase, current_file, rate,
    eta_seconds, step_id). Per-stage counts still live in `stages[...]` and
    reach the UI via the `stages` snapshot, so step-level bars are unaffected."""
    current, total = _weighted_progress(stages)
    data = {
        "phase": phase,
        "stage_id": stage_id,
        "current": current,
        "total": total,
        "stages": {k: dict(v) for k, v in stages.items()},
    }
    data.update(extra)
    return data


def _update_stages(runner, job_id, stages):
    """Push a stages progress update with weighted overall current/total."""
    current, total = _weighted_progress(stages)
    runner.push_event(job_id, "progress", {
        "phase": _current_phase(stages),
        "current": current,
        "total": total,
        "stages": {k: dict(v) for k, v in stages.items()},
    })


def _current_phase(stages):
    """Determine the primary phase label from stage statuses."""
    for name in ["misses", "regroup", "eye_keypoints", "extract_masks", "classify", "detect",
                 "model_loader", "previews", "thumbnails", "scan", "ingest"]:
        info = stages.get(name, {})
        if info.get("status") == "running":
            return info.get("label", name)
    return "Pipeline"


def _collapse_scan_roots(paths):
    """Reduce ``paths`` to the minimal non-overlapping ancestor set.

    Descendants of a kept path are dropped (the scanner walks recursively).
    The filesystem root needs special handling because ``'/' + os.sep``
    is ``'//'`` and would not prefix-match a child like ``/sub``.
    """
    candidates = sorted(set(paths), key=len)
    kept: list[str] = []
    for cand in candidates:
        is_descendant = False
        for k in kept:
            prefix = k if k.endswith(os.sep) else k + os.sep
            if cand.startswith(prefix):
                is_descendant = True
                break
        if not is_descendant:
            kept.append(cand)
    kept.sort()
    return kept


def run_pipeline_job(job, runner, db_path, workspace_id, params,
                     thumb_cache_dir=None):
    """Execute streaming pipeline. Called by JobRunner in a background thread.

    Args:
        job: job dict from JobRunner (has id, progress, errors, etc.)
        runner: JobRunner instance for push_event()
        db_path: path to SQLite database
        workspace_id: active workspace ID
        params: PipelineParams with request parameters
        thumb_cache_dir: configured thumbnail cache directory. Forwarded
            to scanner.scan() and used by the thumbnail stage so the
            pipeline writes and invalidates the real cache even when
            ``--thumb-dir`` points outside ``dirname(db_path)/thumbnails``.
            Defaults to that convention for backward compatibility.

    Returns:
        dict with stage results, duration, and errors
    """
    job["_start_time"] = time.time()
    abort = threading.Event()
    errors = job["errors"]  # shared list, append is thread-safe

    # Effective thumbnail cache directory for every internal call below.
    # Falls back to the historical ``<db_dir>/thumbnails`` convention when
    # the caller didn't supply an explicit value — matches prior behavior
    # for the default ~/.vireo layout.
    effective_thumb_cache_dir = thumb_cache_dir or os.path.join(
        os.path.dirname(db_path), "thumbnails",
    )
    # vireo_dir must match the Flask serve convention — app.py computes
    # ``vireo_dir = os.path.dirname(THUMB_CACHE_DIR)`` for
    # previews/working. When the caller provided thumb_cache_dir
    # explicitly we derive from its parent; otherwise fall back to the
    # db_dir (same as the historical layout where everything sits
    # alongside vireo.db).
    effective_vireo_dir = (
        os.path.dirname(thumb_cache_dir)
        if thumb_cache_dir
        else os.path.dirname(db_path)
    )

    # Snapshot-scoped pipelines: load the snapshot up front so scan targets
    # are derived from the captured file paths (not a folder the user picked
    # later). Raises if the snapshot has been garbage-collected — the API
    # layer is expected to return 404 before this job ever runs, but we fail
    # loud here to avoid silently running an unbounded scan.
    snapshot_paths: list[str] | None = None
    if params.source_snapshot_id is not None:
        db_ro = Database(db_path)
        db_ro.set_active_workspace(workspace_id)
        snap = db_ro.get_new_images_snapshot(params.source_snapshot_id)
        if snap is None:
            raise ValueError(
                f"snapshot {params.source_snapshot_id} not found"
            )
        snapshot_paths = list(snap["file_paths"])
        # Collapse to the minimal non-overlapping ancestor set: if the
        # snapshot has files at both /root/a.jpg and /root/sub/b.jpg the
        # naive derived roots (/root, /root/sub) would make the scanner walk
        # /root/sub twice — once on its own, once as a descendant of /root.
        scan_roots = _collapse_scan_roots(
            [os.path.dirname(p) for p in snapshot_paths]
        )
        # Override any source/sources/collection_id the caller passed; the
        # snapshot is the single source of truth for what to scan.
        params.sources = scan_roots
        params.source = None
        params.collection_id = None

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
        "classify": {"status": "pending", "count": 0, "cached": 0, "seen": 0, "label": "Classifying species"},
        "extract_masks": {"status": "pending", "count": 0, "label": "Extracting features"},
        "eye_keypoints": {"status": "pending", "count": 0, "label": "Detecting eye keypoints"},
        "regroup": {"status": "pending", "label": "Grouping encounters"},
        "misses": {"status": "pending", "count": 0, "label": "Flagging missed shots"},
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
        step_defs.append({"id": "eye_keypoints", "label": "Detect eye keypoints"})
    if not params.skip_regroup:
        step_defs.append({"id": "regroup", "label": "Group encounters"})
        step_defs.append({"id": "misses", "label": "Flag missed shots"})
    runner.set_steps(job["id"], step_defs)

    result = {"stages": {}}
    collection_id = params.collection_id
    scan_to_thumb = queue.Queue(maxsize=200)
    collected_photo_ids = []
    collection_ready = threading.Event()
    models_ready = threading.Event()
    loaded_models = {}  # populated by model_loader thread
    # Resolved in collection_stage once the scanner has committed photo rows.
    # When set (i.e. snapshot-scoped runs), the collection is trimmed to this
    # set so every downstream stage (classify, extract_masks, eye_keypoints,
    # regroup) operates only on the files captured in the snapshot — files
    # that landed in the folder after the snapshot are scanned (we walk the
    # folder) but not further processed.
    snapshot_photo_ids: set[int] | None = None

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

        # Note: stages["scan"]["status"] is NOT set to "running" here. It is
        # flipped to "running" just before each do_scan() call below, so
        # numScan doesn't pulse during the ingest sub-phase.
        # Collect the scan roots actually fed to do_scan so the finally clause
        # can invalidate the new-images cache for each one, matching the
        # try/finally pattern used by api_job_scan / api_job_import_full in
        # vireo/app.py. scanner.scan commits photo rows incrementally, so even
        # a mid-scan failure needs invalidation.
        scanned_roots: list[str] = []
        thread_db = None
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
                runner.push_event(job["id"], "progress", _progress_event(
                    stages, "scan", message,
                ))

            # Accumulator so multi-folder scans (repair loop, scan-in-place
            # with sources=[...]) don't rewind the overall progress at each
            # folder boundary. scan() reports (current, total) local to the
            # invocation; we fold those into cumulative counters that the
            # weighted overall bar reads via stages["scan"].
            scan_acc = {"prior": 0, "last_total": 0}

            def progress_cb(current, total):
                scan_acc["last_total"] = total
                cum_current = scan_acc["prior"] + current
                cum_total = scan_acc["prior"] + total
                stages["scan"]["count"] = cum_current
                stages["scan"]["total"] = cum_total
                elapsed = time.time() - job["_start_time"]
                rate = round(cum_current / max(elapsed, 0.01) * 60, 1)  # files/min
                remaining = cum_total - cum_current
                rate_per_sec = cum_current / max(elapsed, 0.01)
                eta = round(remaining / rate_per_sec) if rate_per_sec > 0 and cum_current >= 10 else None
                runner.update_step(job["id"], "scan",
                                   progress={"current": cum_current, "total": cum_total})
                runner.push_event(job["id"], "progress", _progress_event(
                    stages, "scan", "Scanning photos",
                    rate=rate,
                    eta_seconds=eta,
                ))

            def advance_scan_acc():
                scan_acc["prior"] += scan_acc["last_total"]
                scan_acc["last_total"] = 0

            # Collection mode: no scan targets, but check whether any
            # photos in the collection have broken metadata (NULL timestamp
            # or RAW thumbnail-sized dimensions) that would poison
            # downstream stages. If so, run a targeted repair scan on just
            # the affected folders. This is the self-healing path — when
            # nothing's broken, we keep the historical "Skipped" summary.
            if skip_scan:
                coll_photos = thread_db.get_collection_photos(
                    collection_id, per_page=999999,
                )
                # Respect the user's preview-time exclusions: photos removed
                # from this run must not be rescanned or have their metadata
                # rewritten as a side effect of repair.
                in_scope_photos = _filter_excluded(coll_photos)
                broken = _find_broken_metadata_folders(
                    thread_db, [p["id"] for p in in_scope_photos],
                )
                if not broken:
                    stages["scan"]["status"] = "skipped"
                    runner.update_step(
                        job["id"], "scan", status="completed",
                        summary="Skipped (using collection)",
                    )
                    _update_stages(runner, job["id"], stages)
                    scan_to_thumb.put(_SENTINEL)
                    return

                total_broken = sum(len(paths) for _, paths in broken)
                stages["scan"]["label"] = (
                    f"Repair metadata ({total_broken} photos)"
                )
                stages["scan"]["status"] = "running"
                runner.update_step(
                    job["id"], "scan", status="running",
                    summary=(f"Repairing {total_broken} photos in "
                             f"{len(broken)} folder"
                             f"{'s' if len(broken) != 1 else ''}"),
                )
                _update_stages(runner, job["id"], stages)

                # Display-only callback for the repair path: updates the
                # scan step's current_file indicator but does NOT enqueue
                # into scan_to_thumb. In collection mode thumbnail_stage
                # already replays the full collection against the thumb
                # cache, so enqueueing here would double-process every
                # repaired photo and inflate the thumbnail totals.
                def repair_photo_cb(photo_id, path):
                    runner.update_step(
                        job["id"], "scan",
                        current_file=os.path.basename(path),
                    )

                unreachable = 0
                for folder_path, file_paths in broken:
                    if not os.path.isdir(folder_path):
                        log.warning(
                            "Repair scan skipped for missing folder: %s",
                            folder_path,
                        )
                        unreachable += 1
                        continue
                    try:
                        # restrict_files limits discovery to the known
                        # broken photos in this folder. Without it, new
                        # untracked files in the same folder would get
                        # ingested as a side effect of the repair.
                        do_scan(
                            folder_path, thread_db,
                            progress_callback=progress_cb,
                            incremental=True,
                            extract_full_metadata=pipeline_cfg.get(
                                "extract_full_metadata", True,
                            ),
                            photo_callback=repair_photo_cb,
                            status_callback=status_cb,
                            restrict_dirs=[folder_path],
                            restrict_files=set(file_paths),
                            vireo_dir=effective_vireo_dir,
                            thumb_cache_dir=effective_thumb_cache_dir,
                        )
                    except (OSError, RuntimeError) as e:
                        log.warning(
                            "Repair scan failed for %s: %s", folder_path, e,
                        )
                        unreachable += 1
                    finally:
                        advance_scan_acc()

                summary = f"{total_broken} photos repaired"
                if unreachable:
                    summary += (f", {unreachable} folder"
                                f"{'s' if unreachable != 1 else ''} unreachable")
                stages["scan"]["status"] = "completed"
                runner.update_step(
                    job["id"], "scan", status="completed", summary=summary,
                )
                scan_to_thumb.put(_SENTINEL)
                return

            # Determine source folder(s)
            sources = params.sources or ([params.source] if params.source else [])

            if params.destination:
                from pathlib import Path

                from ingest import ingest as do_ingest

                # Same accumulator pattern as scan_acc: do_ingest() is called
                # once per source folder, with (current, total) local to each
                # call. Without accumulation, overall progress rewinds at each
                # source boundary.
                ingest_acc = {"prior": 0, "last_total": 0}

                def ingest_cb(current, total, filename):
                    ingest_acc["last_total"] = total
                    cum_current = ingest_acc["prior"] + current
                    cum_total = ingest_acc["prior"] + total
                    stages["ingest"]["count"] = cum_current
                    stages["ingest"]["total"] = cum_total
                    runner.update_step(job["id"], "ingest",
                                       current_file=filename,
                                       progress={"current": cum_current, "total": cum_total})
                    runner.push_event(job["id"], "progress", _progress_event(
                        stages, "ingest", "Importing photos",
                        current_file=filename,
                    ))

                def advance_ingest_acc():
                    ingest_acc["prior"] += ingest_acc["last_total"]
                    ingest_acc["last_total"] = 0

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
                    try:
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
                    finally:
                        advance_ingest_acc()
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
                scanned_roots.append(params.destination)
                do_scan(
                    params.destination, thread_db,
                    progress_callback=progress_cb,
                    incremental=True,
                    extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                    photo_callback=photo_cb,
                    status_callback=status_cb,
                    restrict_dirs=restrict,
                    vireo_dir=effective_vireo_dir,
                    thumb_cache_dir=effective_thumb_cache_dir,
                )
            else:
                # Scan-in-place: scan each source folder independently.
                stages["scan"]["status"] = "running"
                runner.update_step(job["id"], "scan", status="running")
                job["progress"]["current"] = 0
                job["progress"]["total"] = 0
                _update_stages(runner, job["id"], stages)
                for src_folder in sources:
                    scanned_roots.append(src_folder)
                    try:
                        do_scan(
                            src_folder, thread_db,
                            progress_callback=progress_cb,
                            incremental=True,
                            extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                            photo_callback=photo_cb,
                            skip_paths=params.exclude_paths,
                            status_callback=status_cb,
                            recursive=params.recursive,
                            vireo_dir=effective_vireo_dir,
                            thumb_cache_dir=effective_thumb_cache_dir,
                        )
                    finally:
                        advance_scan_acc()
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
            # Invalidate the new-images cache for every root fed to do_scan,
            # on both success and exception paths. scanner.scan commits photo
            # rows incrementally, so even a mid-scan failure can leave DB
            # state that invalidates cached new-image counts. Mirrors the
            # try/finally in api_job_scan and api_job_import_full.
            if thread_db is not None and scanned_roots:
                from new_images import invalidate_new_images_after_scan
                for scanned_root in scanned_roots:
                    try:
                        invalidate_new_images_after_scan(thread_db, scanned_root)
                    except Exception:
                        log.exception(
                            "Failed to invalidate new-images cache for %s",
                            scanned_root,
                        )
            scan_to_thumb.put(_SENTINEL)
            _update_stages(runner, job["id"], stages)

    def collection_stage():
        """Wait for scan to finish, build collection, signal classifier."""
        nonlocal collection_id, snapshot_photo_ids

        if skip_scan:
            collection_ready.set()
            return

        # Wait for scanner to complete (don't check abort -- we want the
        # collection regardless so the user can see scanned photos)
        while True:
            if stages["scan"]["status"] in ("completed", "failed"):
                break
            time.sleep(0.1)

        # Snapshot-scoped runs: resolve the captured file paths to photo IDs
        # now that the scanner has committed rows, and trim the collection
        # to exactly that set. A late-arriving file (landed in the folder
        # after the snapshot) was still scanned — we walk the whole folder —
        # but must not be classified or scored. Any snapshot path that never
        # resolved (file was moved/deleted between snapshot and pipeline
        # run) is logged so an unexpectedly small collection is auditable.
        if snapshot_paths is not None:
            resolver_db = Database(db_path)
            resolver_db.set_active_workspace(workspace_id)
            # Split each snapshot path into (dirname, basename) and match on
            # the two columns directly. Concatenating with a hardcoded '/'
            # would mismatch Windows paths captured via os.path.join, where
            # both the snapshot and folders.path use backslash separators.
            pairs = [os.path.split(p) for p in snapshot_paths]
            resolved: set[int] = set()
            # 2 placeholders per pair; cap below SQLite's default 999-param
            # limit (pre-3.32) with headroom.
            _CHUNK = 400
            for i in range(0, len(pairs), _CHUNK):
                chunk = pairs[i : i + _CHUNK]
                values = ",".join("(?, ?)" for _ in chunk)
                flat_params = tuple(v for pair in chunk for v in pair)
                rows = resolver_db.conn.execute(
                    f"""SELECT p.id
                          FROM photos p
                          JOIN folders f ON f.id = p.folder_id
                         WHERE (f.path, p.filename) IN (VALUES {values})""",
                    flat_params,
                ).fetchall()
                resolved.update(r["id"] for r in rows)
            snapshot_photo_ids = resolved

            missing = len(snapshot_paths) - len(snapshot_photo_ids)
            log.info(
                "pipeline: snapshot %s had %d files, %d ingested, %d missing on disk",
                params.source_snapshot_id,
                len(snapshot_paths),
                len(snapshot_photo_ids),
                missing,
            )

            # Filter collected_photo_ids to the snapshot set. collected_photo_ids
            # is only read by this stage (to build the collection); the thumbnail
            # queue has already drained it independently.
            collected_photo_ids[:] = [
                pid for pid in collected_photo_ids if pid in snapshot_photo_ids
            ]

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

            # Write thumbnails to the configured cache dir so custom
            # --thumb-dir layouts receive the files the Flask serve
            # route (reading from app.config["THUMB_CACHE_DIR"]) will
            # look for. Falls back to <db_dir>/thumbnails only when the
            # caller passed no explicit override.
            cache_dir = effective_thumb_cache_dir
            os.makedirs(cache_dir, exist_ok=True)

            generated = 0
            skipped = 0
            failed = 0

            # Mark photos.thumb_path so the dashboard's coverage query
            # (`thumb_path IS NOT NULL`) reflects each freshly-generated or
            # already-cached thumbnail. Batched so the writer lock isn't held
            # per-row under sustained scan throughput.
            THUMB_PATH_BATCH = 25
            pending_thumb_paths = []

            def _flush_thumb_paths():
                if pending_thumb_paths:
                    thread_db.conn.executemany(
                        "UPDATE photos SET thumb_path=? WHERE id=?",
                        pending_thumb_paths,
                    )
                    commit_with_retry(thread_db.conn)
                    pending_thumb_paths.clear()

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
                        pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                    else:
                        generated += 1
                        pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                    if len(pending_thumb_paths) >= THUMB_PATH_BATCH:
                        _flush_thumb_paths()
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
                stages["thumbnails"]["total"] = scan_total
                runner.update_step(job["id"], "thumbnails",
                                   current_file=os.path.basename(photo_path),
                                   progress={"current": processed, "total": scan_total})
                elapsed = time.time() - job["_start_time"]
                rate = round(processed / max(elapsed, 0.01) * 60, 1)
                runner.push_event(job["id"], "progress", _progress_event(
                    stages, "thumbnails", "Generating thumbnails",
                    current_file=os.path.basename(photo_path),
                    rate=rate,
                ))

            # Collection mode: the scanner is skipped so the queue above was
            # empty. Iterate the collection's photos directly — mirrors the
            # pattern used by previews_stage — so replays against an existing
            # collection still regenerate any missing thumbs.
            if skip_scan and collection_id:
                coll_photos = _filter_excluded(
                    thread_db.get_collection_photos(collection_id, per_page=999999)
                )
                folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
                total = len(coll_photos)
                for photo in coll_photos:
                    if _should_abort(abort):
                        break
                    photo_id = photo["id"]
                    folder_path = folders.get(photo["folder_id"], "")
                    photo_path = os.path.join(folder_path, photo["filename"])
                    thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
                    already_exists = os.path.exists(thumb_path)
                    try:
                        result_path = generate_thumbnail(
                            photo_id, photo_path, cache_dir, size=thumb_size,
                        )
                        if result_path is None:
                            failed += 1
                        elif already_exists:
                            skipped += 1
                            pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                        else:
                            generated += 1
                            pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                        if len(pending_thumb_paths) >= THUMB_PATH_BATCH:
                            _flush_thumb_paths()
                    except Exception:
                        failed += 1
                        log.debug("Thumbnail failed for photo %s", photo_id)
                    stages["thumbnails"]["count"] = generated + skipped + failed
                    stages["thumbnails"]["total"] = total
                    processed = generated + skipped + failed
                    runner.update_step(
                        job["id"], "thumbnails",
                        current_file=os.path.basename(photo_path),
                        progress={"current": processed, "total": total},
                    )
                    elapsed = time.time() - job["_start_time"]
                    rate = round(processed / max(elapsed, 0.01) * 60, 1)
                    runner.push_event(job["id"], "progress", _progress_event(
                        stages, "thumbnails", "Generating thumbnails",
                        current_file=os.path.basename(photo_path),
                        rate=rate,
                    ))

            # Flush any thumb_path updates from the final partial batch.
            _flush_thumb_paths()

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
            from image_loader import get_canonical_image_path, load_image

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            raw_size = params.preview_max_size
            if raw_size == 0:
                # "Full resolution" — /full redirects to /original, so
                # there's no size-suffixed file to warm. Skip rather
                # than produce untracked {id}.jpg files.
                runner.update_step(
                    job["id"], "previews", status="completed",
                    summary="Skipped (preview_max_size=0 → serves originals)",
                )
                stages["previews"]["status"] = "completed"
                return
            max_size = int(raw_size or 1920)
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
                cache_path = os.path.join(preview_dir, f'{photo["id"]}_{max_size}.jpg')
                if os.path.exists(cache_path):
                    skipped += 1
                    try:
                        if not thread_db.preview_cache_get(photo["id"], max_size):
                            thread_db.preview_cache_insert(
                                photo["id"], max_size, os.path.getsize(cache_path),
                            )
                    except Exception:
                        pass  # photo may have been deleted mid-pipeline
                else:
                    canonical = get_canonical_image_path(photo, base_dir, folders)
                    img = load_image(canonical, max_size=max_size)
                    if img:
                        img.save(cache_path, format="JPEG", quality=preview_quality)
                        with contextlib.suppress(Exception):
                            thread_db.preview_cache_insert(
                                photo["id"], max_size, os.path.getsize(cache_path),
                            )
                        generated += 1
                    else:
                        # image_loader already logged the failure at WARNING;
                        # count it here so it surfaces in the rollup.
                        failed += 1

                stages["previews"]["count"] = i + 1
                stages["previews"]["total"] = total
                runner.update_step(job["id"], "previews",
                                   current_file=photo["filename"],
                                   progress={"current": i + 1, "total": total})
                runner.push_event(job["id"], "progress", _progress_event(
                    stages, "previews", "Generating previews",
                    current_file=photo["filename"],
                    rate=round(
                        (i + 1) / max(time.time() - job["_start_time"], 0.01) * 60, 1
                    ),
                ))

            # One eviction pass after the stage so preview_cache_max_mb is
            # enforced even when the pipeline is the only producer (e.g.
            # first-run ingest). Writes happen per-photo above to avoid
            # per-row fsyncs.
            from preview_cache import evict_if_over_quota
            evict_if_over_quota(thread_db, base_dir)

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
        from classify_job import (
            _load_labels,
            _record_labels_fingerprint,
            _resolve_label_sources,
        )
        from labels_fingerprint import compute_fingerprint
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
        # Compute a content-addressable fingerprint for the active label set
        # and record it in the labels_fingerprints sidecar. Kept on the bundle
        # so classify_stage can pass it to record_classifier_run for each
        # (detection, model, fingerprint) triple.
        fp = compute_fingerprint(labels)
        label_sources = _resolve_label_sources(params, thread_db)
        _record_labels_fingerprint(thread_db, fp, labels, sources=label_sources)

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
            "labels_fingerprint": fp,
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
            from taxonomy import TAXONOMY_JSON_PATH, find_taxonomy_json
            taxonomy_path = find_taxonomy_json()
            if params.download_taxonomy and not os.path.exists(taxonomy_path):
                try:
                    from taxonomy import download_taxonomy
                    runner.push_event(job["id"], "progress", _progress_event(
                        stages, "model_loader", "Downloading taxonomy...",
                    ))
                    # Always write new downloads to the persistent path.
                    taxonomy_path = TAXONOMY_JSON_PATH
                    download_taxonomy(taxonomy_path, progress_callback=lambda msg:
                        runner.push_event(job["id"], "progress", _progress_event(
                            stages, "model_loader", msg,
                        ))
                    )
                except Exception as e:
                    log.warning("Taxonomy download failed, continuing without: %s", e)

            # Taxonomy is shared across every classifier in the run.
            # Use load_local_taxonomy() so a corrupt persistent file
            # falls back to the legacy package-dir copy.
            from taxonomy import load_local_taxonomy
            tax = load_local_taxonomy()
            loaded_models["tax"] = tax
            loaded_models["resolved_specs"] = resolved_specs

            # Load the first classifier so classify_stage can start as soon
            # as scan completes; any remaining specs are loaded inside
            # classify_stage so we never hold more than one model in memory.
            runner.push_event(job["id"], "progress", _progress_event(
                stages, "model_loader", f"Loading {first_name}...",
            ))

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
            # pre-seed already_detected from detector_runs so _detect_batch
            # reuses rows instead of re-invoking MegaDetector — including
            # empty-scene photos (box_count=0) which would otherwise be
            # re-detected forever by a legacy detections-only seed.
            if params.reclassify:
                already_detected: set = set()
                photo_ids_list = [p["id"] for p in photos]
                pre_run_det_ids: dict = getattr(
                    thread_db, "get_detection_ids_for_photos", lambda _: {}
                )(photo_ids_list)
            else:
                already_detected = set(
                    thread_db.get_detector_run_photo_ids("megadetector-v6")
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
                    # Weight download is a sub-phase of detect; don't treat
                    # its bytes as detect's stage-level counter or the bar
                    # jumps ahead before any photo has been detected.
                    runner.push_event(job["id"], "progress", _progress_event(
                        stages, "detect", phase,
                    ))

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

                stages["detect"]["count"] = batch_idx
                stages["detect"]["total"] = total
                runner.push_event(job["id"], "progress", _progress_event(
                    stages, "detect", "Detecting subjects",
                    rate=round(
                        batch_idx / max(time.time() - start_time, 0.01) * 60,
                        1,
                    ),
                ))
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
            # Distinguish loader-driven abort (model resolution or preload
            # failure) from benign skips (skip_classify, user cancellation,
            # missing collection): rows for the former must surface as
            # 'failed' so the per-model failure is visible on the job tree
            # — the whole point of splitting classify into per-model rows.
            loader_failed = stages["model_loader"]["status"] == "failed"
            loader_err = next(
                (e for e in errors if e.startswith("[model_loader] Fatal:")),
                None,
            )
            row_status = "failed" if loader_failed else "completed"
            row_summary = (
                "Model load failed" if loader_failed else "Skipped"
            )
            stages["classify"]["status"] = (
                "failed" if loader_failed else "skipped"
            )

            specs_for_step_ids = loaded_models.get("resolved_specs") or []
            if specs_for_step_ids:
                for spec in specs_for_step_ids:
                    runner.update_step(
                        job["id"], f"classify:{spec['id']}",
                        status=row_status, summary=row_summary,
                        error=loader_err if loader_failed else None,
                    )
            else:
                for mid in (effective_model_ids or ["__unresolved__"]):
                    runner.update_step(
                        job["id"], f"classify:{mid}",
                        status=row_status, summary=row_summary,
                        error=loader_err if loader_failed else None,
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
            # Fingerprint for the FIRST model is preloaded by model_loader_stage.
            # Each subsequent iteration reloads its own bundle (with its own fp)
            # inside the loop, so we read loaded_models["labels_fingerprint"]
            # per-spec below rather than capturing a single value here.
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
                    # Don't reset stages["classify"]["count"] here — it now
                    # accumulates real inferences per-photo (Task 3); jumping
                    # it to spec_idx * total would silently double-count the
                    # cached hits from prior specs.  total is left at its
                    # multi-spec value (set by the batch-end push of the
                    # previous spec, or unchanged on first entry); explicitly
                    # restate it so the "Loading next model..." event always
                    # carries the multi-spec total.
                    stages["classify"]["total"] = total * len(resolved_specs_local)
                    runner.push_event(job["id"], "progress", _progress_event(
                        stages, "classify", f"Loading {active_spec['name']}...",
                        step_id=step_id,
                    ))
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

                # The fingerprint for THIS model's label set — pinned by
                # model_loader_stage for the first model and by _load_model_bundle
                # for subsequent ones. Used to key the classifier_runs gate so
                # a repeat pass over the same (detection, model, fingerprint)
                # skips work instead of re-running inference. Hoisted above
                # the reclassify clear so the clear can scope by fingerprint.
                spec_fp = loaded_models.get("labels_fingerprint", "legacy")

                # The reclassify clear (wipes prior predictions for this
                # model+fingerprint) is intentionally deferred to just before
                # _store_grouped_predictions below — clearing here and then
                # cancelling mid-classify would leave the predictions table
                # empty for this model with no replacement, erasing the
                # user's prior classifications instead of preserving them.

                # No photo-level short-circuit: it would hide detections
                # that newly cross the workspace's detector_confidence
                # threshold on photos that already had a cached prediction
                # for some other detection. The per-detection
                # classifier_runs gate below handles skipping correctly
                # and still surfaces cached results into raw_results so
                # grouping sees them.
                # Pre-flight cache estimate. One indexed query per spec
                # so the UI can display "~M cached, ~K to classify" before
                # the first inference runs and ETAs are honest from the
                # start. The estimate may overcount if a run-key exists
                # but no cached predictions do (see lines ~2000-2004); the
                # live `cached` counter reflects actual skips.
                cached_est = thread_db.count_classifier_runs(
                    [p["id"] for p in photos],
                    model_name,
                    spec_fp,
                )
                stages["classify"]["cached_estimate"] = (
                    stages["classify"].get("cached_estimate", 0) + cached_est
                )
                # Set total BEFORE the pre-flight event so the UI's
                # ``stageTotal - stageCachedEst`` subtraction renders the
                # real "to classify" count on the first event the user
                # sees, not 0.
                stages["classify"]["total"] = total * len(resolved_specs_local)
                runner.push_event(job["id"], "progress", _progress_event(
                    stages, "classify",
                    f"Classifying with {active_spec['name']}",
                    step_id=step_id,
                ))
                raw_results: list = []
                failed = 0
                skipped_existing = 0
                stages["classify"].setdefault("cached", 0)
                stages["classify"].setdefault("seen", 0)
                # Photos that iterated past the inner abort check IN THIS spec.
                # Used for the per-spec ``runner.update_step`` progress (which
                # is bounded by ``total``, not the multi-spec stage total) and
                # for the batch-end rate calc.  Captures every branch — cache
                # hit, successful inference, no-detection, image decode fail,
                # inference fail — so spec-level progress reaches ``total`` at
                # the end of the spec regardless of outcome mix.
                processed_in_spec = 0
                start_time = time.time()
                batch_size = 32  # classification batch granularity

                for batch_start in range(0, total, batch_size):
                    if _should_abort(abort):
                        break
                    batch = photos[batch_start:batch_start + batch_size]

                    for photo in batch:
                        # Per-photo abort check so cancel takes effect within
                        # one inference (~seconds) instead of waiting for the
                        # next batch boundary (~32 photos). The outer batch
                        # loop's check at the top of the next iteration will
                        # then break out of the batch loop entirely.
                        if _should_abort(abort):
                            break
                        processed_in_spec += 1
                        stages["classify"]["seen"] = (
                            stages["classify"].get("seen", 0) + 1
                        )
                        # Record this photo as classify-processed for the first
                        # successful model. Used by the stale-detection purge to
                        # restrict deletions to photos actually reclassified.
                        if models_succeeded == 0:
                            first_model_photo_ids.add(photo["id"])

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

                        # Classifier-run gate: skip work when this exact
                        # (detection, classifier_model, labels_fingerprint)
                        # triple was already classified. Reclassify bypasses
                        # the gate so users can force a fresh pass. When
                        # gated, surface the cached top-1 prediction into
                        # raw_results so downstream grouping/storage sees
                        # it — otherwise the cached detection would silently
                        # drop out of the grouping pipeline.
                        if not params.reclassify:
                            run_keys = thread_db.get_classifier_run_keys(
                                primary_det["id"]
                            )
                            if (model_name, spec_fp) in run_keys:
                                cached = thread_db.get_predictions_for_detection(
                                    primary_det["id"],
                                    classifier_model=model_name,
                                    labels_fingerprint=spec_fp,
                                    min_classifier_conf=0,
                                )
                                if cached:
                                    skipped_existing += 1
                                    stages["classify"]["cached"] += 1
                                    top = cached[0]
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
                                            photo["id"], model_name,
                                        )
                                        if emb_blob:
                                            import numpy as np
                                            embedding = np.frombuffer(
                                                emb_blob, dtype=np.float32,
                                            )
                                    raw_results.append({
                                        "photo": photo,
                                        "detection_id": primary_det["id"],
                                        "folder_path": folder_path,
                                        "image_path": image_path,
                                        "prediction": top["species"],
                                        "confidence": top["confidence"],
                                        "timestamp": timestamp,
                                        "filename": photo["filename"],
                                        "embedding": embedding,
                                        "taxonomy": None,
                                        "_existing": True,
                                    })
                                    continue
                                # Run key with no cached rows (e.g.
                                # prior pass stored `category == 'match'`
                                # so the prediction was intentionally not
                                # written). Fall through to re-classify
                                # instead of stranding the detection.

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
                        pre_len = len(raw_results)
                        n_batch_failed = _flush_batch(
                            img_batch, clf, model_type, model_name,
                            thread_db, raw_results,
                        )
                        if n_batch_failed:
                            failed_photo_ids.add(photo["id"])
                        failed += n_batch_failed

                        # Record the classifier_runs row so the next pass over
                        # the same (detection, model, fingerprint) short-
                        # circuits. prediction_count reflects how many rows
                        # _flush_batch added to raw_results for this detection.
                        #
                        # Only persist when the batch produced at least one
                        # prediction. A count of 0 means the classifier
                        # failed (transient load error, decode error, etc.);
                        # caching that would strand the detection without
                        # predictions until the user forces --reclassify.
                        new_count = len(raw_results) - pre_len
                        if new_count > 0:
                            thread_db.record_classifier_run(
                                primary_det["id"], model_name, spec_fp,
                                prediction_count=new_count,
                            )
                            stages["classify"]["count"] = (
                                stages["classify"].get("count", 0) + 1
                            )

                    # Batch boundary: surface the per-photo accumulated
                    # count + cached to the UI. Replaces the old per-batch
                    # pre-advance which lied about progress when batches
                    # contained cache hits.
                    stages["classify"]["total"] = total * len(resolved_specs_local)
                    elapsed = max(time.time() - start_time, 0.01)
                    runner.push_event(job["id"], "progress", _progress_event(
                        stages, "classify",
                        f"Classifying with {active_spec['name']}"
                        + (
                            f" ({spec_idx + 1}/{len(resolved_specs_local)})"
                            if len(resolved_specs_local) > 1 else ""
                        ),
                        step_id=step_id,
                        rate=round(processed_in_spec / elapsed * 60, 1),
                    ))
                    runner.update_step(
                        job["id"], step_id,
                        progress={
                            "current": processed_in_spec,
                            "total": total,
                        },
                    )

                # Skip the grouping/storage finalization on cancel — it can
                # take a minute on large collections and the user has already
                # asked us to stop. Per-photo counters are accurate, no
                # corrective fixup needed.
                if _should_abort(abort):
                    runner.push_event(job["id"], "progress", _progress_event(
                        stages, "classify",
                        f"Cancelled — {processed_in_spec} of "
                        f"{total} processed",
                        step_id=step_id,
                    ))
                    runner.update_step(
                        job["id"], step_id,
                        status="completed",
                        progress={
                            "current": processed_in_spec,
                            "total": total,
                        },
                        summary=(
                            f"Cancelled "
                            f"({stages['classify']['count']} classified, "
                            f"{stages['classify']['cached']} cached "
                            f"of {total})"
                        ),
                    )
                    continue

                # Reclassify clear, deferred from the top of the per-spec body
                # so a mid-batch cancel above leaves the user's prior
                # predictions intact (Codex P1 review on #710).  Scope by
                # labels_fingerprint so reclassifying one workspace's label
                # set doesn't wipe another workspace's cached predictions on
                # the same photos under its own fingerprint (shared-folder
                # setups).  ``clear_run_keys=False`` because the per-photo
                # ``record_classifier_run`` calls inside the loop above
                # already wrote fresh classifier_runs rows for processed
                # detections — wiping them here would strand the gate and
                # force the next non-reclassify pass to re-infer everything.
                if params.reclassify:
                    thread_db.clear_predictions(
                        model=model_name,
                        collection_photo_ids=[p["id"] for p in photos],
                        labels_fingerprint=spec_fp,
                        clear_run_keys=False,
                    )

                group_result = _store_grouped_predictions(
                    raw_results, job["id"], model_name,
                    grouping_window, similarity_threshold, tax, thread_db,
                    labels_fingerprint=spec_fp,
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

            # Cancellation takes precedence over the all-models-failed-to-load
            # signal: if the user cancelled mid-classify after a prior model
            # had already been added to skipped_model_names, raising here
            # would misclassify the cancel as a fatal load failure and
            # overwrite the per-model 'Cancelled' summary in the exception
            # handler.
            if (
                models_succeeded == 0
                and skipped_model_names
                and not _should_abort(abort)
            ):
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
                    runner.push_event(job["id"], "progress", _progress_event(
                        stages, "extract_masks", phase,
                    ))

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
                stages["extract_masks"]["total"] = total
                runner.update_step(job["id"], "extract_masks",
                                   progress={"current": i + 1, "total": total},
                                   error_count=em_failed)
                runner.push_event(job["id"], "progress", _progress_event(
                    stages, "extract_masks",
                    "Extracting features (SAM2 + DINOv2)",
                    rate=round((i + 1) / max(time.time() - start_time, 0.01) * 60, 1),
                ))

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

    def eye_keypoints_stage():
        """Run per-photo eye keypoint detection between mask extraction and scoring.

        No-op when the stage is disabled by config, when no SuperAnimal
        weights are on disk (users opt-in via the pipeline models card),
        or when no eligible photos remain. Per-photo failures are logged
        and do not abort the stage.
        """
        if params.skip_extract_masks or abort.is_set() or not collection_id:
            stages["eye_keypoints"]["status"] = "skipped"
            runner.update_step(
                job["id"], "eye_keypoints", status="completed", summary="Skipped",
            )
            return

        stages["eye_keypoints"]["status"] = "running"
        runner.update_step(job["id"], "eye_keypoints", status="running")
        _update_stages(runner, job["id"], stages)

        try:
            import config as cfg
            from pipeline import (
                _resolve_collection_photo_ids,
                detect_eye_keypoints_stage,
                eye_keypoint_stage_preflight,
            )

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)
            effective_cfg = thread_db.get_effective_config(cfg.load())
            pipeline_cfg = effective_cfg.get("pipeline", {})

            # Mirror the stage-level preflight so a no-op run doesn't pay the
            # O(N) eligibility join cost or report a misleading
            # "0 of N processed" summary on large libraries.
            skip_reason = eye_keypoint_stage_preflight(pipeline_cfg)
            if skip_reason is not None:
                stages["eye_keypoints"]["status"] = "skipped"
                runner.update_step(
                    job["id"], "eye_keypoints",
                    status="completed", summary=f"Skipped — {skip_reason}",
                )
                result["stages"]["eye_keypoints"] = {
                    "processed": 0, "total": 0, "skipped": skip_reason,
                }
                _update_stages(runner, job["id"], stages)
                return

            collection_photo_ids = (
                _resolve_collection_photo_ids(thread_db, collection_id)
                if collection_id is not None else None
            )
            # Honor preview-deselection so the eye stage matches the set of
            # photos extract/regroup will act on. Without this the stage
            # mutates eye_* for unchecked photos and those values are locked
            # in by the eye_tenengrad IS NULL idempotency guard on reruns.
            if params.exclude_photo_ids and collection_photo_ids is not None:
                collection_photo_ids = {
                    pid for pid in collection_photo_ids
                    if pid not in params.exclude_photo_ids
                }
            total = len(thread_db.list_photos_for_eye_keypoint_stage(
                photo_ids=collection_photo_ids,
            ))
            start_time = time.time()
            processed = {"count": 0}

            def _progress(phase, current, total_steps):
                processed["count"] = current
                stages["eye_keypoints"]["count"] = current
                stages["eye_keypoints"]["total"] = total_steps
                runner.update_step(
                    job["id"], "eye_keypoints",
                    progress={"current": current, "total": total_steps},
                )
                runner.push_event(job["id"], "progress", _progress_event(
                    stages, "eye_keypoints", phase,
                    rate=round(
                        current / max(time.time() - start_time, 0.01) * 60, 1
                    ),
                ))

            detect_eye_keypoints_stage(
                thread_db, config=pipeline_cfg, progress_callback=_progress,
                collection_id=collection_id,
                exclude_photo_ids=params.exclude_photo_ids,
            )

            stages["eye_keypoints"]["status"] = "completed"
            summary = (
                f"{processed['count']} of {total} photos processed"
                if total else "No eligible photos"
            )
            runner.update_step(
                job["id"], "eye_keypoints", status="completed", summary=summary,
            )
            result["stages"]["eye_keypoints"] = {
                "processed": processed["count"], "total": total,
            }
        except Exception as e:
            errors.append(f"[eye_keypoints] Fatal: {e}")
            log.exception("Pipeline eye-keypoints stage failed")
            stages["eye_keypoints"]["status"] = "failed"
            runner.update_step(
                job["id"], "eye_keypoints", status="failed", error=str(e),
            )

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

    def miss_stage():
        """Compute miss-detection flags for the workspace after regroup.

        Runs last so burst_id is available. Uses only per-photo features
        already computed by earlier stages — no model inference.
        """
        # Skip when classify was skipped: classify_miss depends on fresh
        # detections/classifications written by the classify stage, and
        # without them it would mass-flag "no_subject" on photos whose
        # subjects simply weren't re-evaluated this run.
        if (
            params.skip_regroup
            or params.skip_classify
            or abort.is_set()
            or not collection_id
        ):
            stages["misses"]["status"] = "skipped"
            runner.update_step(job["id"], "misses", status="completed",
                               summary="Skipped")
            return

        stages["misses"]["status"] = "running"
        runner.update_step(job["id"], "misses", status="running")
        _update_stages(runner, job["id"], stages)

        try:
            from datetime import UTC, datetime

            import config as cfg
            from misses import compute_misses_for_workspace
            from pipeline import load_results_raw, save_results_raw

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            effective_cfg = thread_db.get_effective_config(cfg.load())
            pipeline_cfg = effective_cfg.get("pipeline", {})

            # Share one timestamp between the DB write and the saved
            # pipeline-results cache so pipeline_review's "Review misses"
            # shortcut can gate on actual recomputation in this run and
            # scope /misses?since=... to exactly what was just written.
            now_ts = datetime.now(UTC).isoformat(timespec="microseconds")
            miss_enabled = pipeline_cfg.get("miss_enabled", True)

            n = compute_misses_for_workspace(
                thread_db,
                pipeline_cfg,
                collection_id=collection_id,
                exclude_photo_ids=params.exclude_photo_ids,
                now=now_ts,
            )

            stages["misses"]["status"] = "completed"
            stages["misses"]["count"] = n
            runner.update_step(job["id"], "misses", status="completed",
                               summary=f"{n} photos evaluated")
            result["stages"]["misses"] = {"evaluated": n}

            # Mark the cached results so the review UI knows misses
            # were actually recomputed this run. Without this, the
            # shortcut would surface stale miss flags from a prior
            # run as "current-run misses" whenever miss_enabled=False
            # or the stage was skipped.
            if miss_enabled:
                cache_dir = os.path.dirname(db_path)
                cached = load_results_raw(cache_dir, workspace_id)
                if cached is not None:
                    cached["miss_computed_at"] = now_ts
                    save_results_raw(cached, cache_dir, workspace_id)
        except Exception as e:
            errors.append(f"[misses] Fatal: {e}")
            log.exception("Pipeline miss-detection stage failed")
            stages["misses"]["status"] = "failed"
            runner.update_step(job["id"], "misses", status="failed", error=str(e))

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

    # Phase 3.5: eye keypoints (needs masks + classifier output). No-op when
    # SuperAnimal weights are absent — users opt in on the pipeline models
    # card. Per-photo failures log and continue rather than abort the stage.
    if not abort.is_set():
        eye_keypoints_stage()

    # Phase 4: regroup (needs extract-masks + eye-keypoints output)
    if not abort.is_set():
        regroup_stage()

    # Phase 5: miss detection (pure derivation from per-photo features +
    # burst_id written by regroup). Cheap; no model inference.
    # Skip when regroup failed: miss classification depends on regroup's
    # burst_id output, so running here after a regroup failure would
    # overwrite miss_* flags with stale context during an already-failing
    # job. regroup_stage marks itself "failed" without setting abort, so
    # check the stage status explicitly.
    if not abort.is_set() and stages["regroup"].get("status") != "failed":
        miss_stage()

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
