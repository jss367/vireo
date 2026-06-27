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
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np
from db import Database, commit_with_retry
from exif_orientation import orientation_swaps_axes as _orientation_swaps_axes
from model_cache import get_default_cache
from pipeline_locks import (
    acquire_photo_mask,
    acquire_workspace_regroup,
    release_archive_destination,
    try_reserve_archive_destination,
)

log = logging.getLogger(__name__)

_SENTINEL = object()  # unique end-of-stream marker
_EXIF_ORIENTATION_TAG = 274


@dataclass
class PipelineParams:
    """Parameters for a streaming pipeline job."""

    collection_id: int | None = None
    source: str | None = None
    sources: list | None = None
    source_snapshot_id: int | None = None
    destination: str | None = None
    local_processing: bool = False
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
    skip_eye_keypoints: bool = False
    download_taxonomy: bool = True
    # None means "use the workspace-effective preview_max_size setting".
    # Explicit values are kept for API/back-compat and tests that need to pin
    # a preview tier.
    preview_max_size: int | None = None
    exclude_paths: set | None = None
    exclude_photo_ids: set | None = None
    recursive: bool = True


def _should_abort(abort_event):
    """Check if the pipeline should abort."""
    return abort_event.is_set()


def _rendered_recipe_long_edge(width, height, recipe):
    rotation = (recipe or {}).get("rotation", 0)
    if rotation in (90, 270):
        width, height = height, width
    crop = (recipe or {}).get("crop") if recipe else None
    if crop:
        return max(float(crop["w"]) * width, float(crop["h"]) * height)
    return max(width, height)


def _photo_value(photo, key):
    try:
        return photo[key]
    except (KeyError, IndexError, TypeError):
        if hasattr(photo, "get"):
            return photo.get(key)
    return None


def _exif_orientation(exif_data):
    if not exif_data:
        return None
    if isinstance(exif_data, str):
        try:
            metadata = json.loads(exif_data)
        except (TypeError, ValueError):
            return None
    elif isinstance(exif_data, dict):
        metadata = exif_data
    else:
        return None
    if not isinstance(metadata, dict):
        return None
    for group in ("EXIF", "IFD0", "TIFF", "File"):
        values = metadata.get(group)
        if isinstance(values, dict) and "Orientation" in values:
            return values["Orientation"]
    return metadata.get("Orientation")


def _recipe_source_dimensions(photo):
    try:
        width = int(_photo_value(photo, "width") or 0)
        height = int(_photo_value(photo, "height") or 0)
    except (TypeError, ValueError):
        return 0, 0
    if (
        width > 0
        and height > 0
        and _orientation_swaps_axes(_exif_orientation(_photo_value(photo, "exif_data")))
    ):
        return height, width
    return width, height


def _scaled_recipe_source_dimensions(photo, max_size=None):
    width, height = _recipe_source_dimensions(photo)
    if width <= 0 or height <= 0:
        return 0, 0
    if max_size:
        long_edge = max(width, height)
        if long_edge > max_size:
            scale = max_size / long_edge
            width = round(width * scale)
            height = round(height * scale)
    return width, height


def _image_is_smaller_than_expected(img, expected_w, expected_h):
    return (
        expected_w > 0
        and expected_h > 0
        and (
            img.size[0] + 1 < expected_w
            or img.size[1] + 1 < expected_h
        )
    )


def _companion_image_can_replace_raw_result(
    companion_img, current_img, expected_w, expected_h,
):
    if companion_img is None:
        return False
    if expected_w > 0 and expected_h > 0:
        return not _image_is_smaller_than_expected(
            companion_img, expected_w, expected_h,
        )
    if current_img is None:
        return True
    return (
        companion_img.size[0] >= current_img.size[0]
        and companion_img.size[1] >= current_img.size[1]
    )


def _image_size_after_exif_orientation(img):
    width, height = img.size
    orientation = None
    with contextlib.suppress(Exception):
        orientation = img.getexif().get(_EXIF_ORIENTATION_TAG)
    if _orientation_swaps_axes(orientation):
        return height, width
    return width, height


def _working_copy_satisfies_recipe_render(photo, recipe, max_size, vireo_dir):
    if not recipe or not recipe.get("crop"):
        return True
    wc_rel = photo["working_copy_path"]
    if not wc_rel:
        return False
    wc_path = os.path.join(vireo_dir, wc_rel)
    if not os.path.exists(wc_path):
        return False
    try:
        from PIL import Image as _PILImage
        with _PILImage.open(wc_path) as wc_img:
            wc_w, wc_h = _image_size_after_exif_orientation(wc_img)
    except Exception:
        return False
    original_w, original_h = _recipe_source_dimensions(photo)
    if original_w <= 0 or original_h <= 0:
        return False
    original_render_long = _rendered_recipe_long_edge(original_w, original_h, recipe)
    required_long = min(max_size, original_render_long) if max_size else original_render_long
    wc_render_long = _rendered_recipe_long_edge(wc_w, wc_h, recipe)
    return wc_render_long >= required_long


def _path_satisfies_recipe_render(path, photo, recipe, max_size):
    original_w, original_h = _recipe_source_dimensions(photo)
    if original_w <= 0 or original_h <= 0:
        return False
    try:
        from PIL import Image as _PILImage
        with _PILImage.open(path) as img:
            width, height = _image_size_after_exif_orientation(img)
    except Exception:
        return False
    original_render_long = _rendered_recipe_long_edge(
        original_w, original_h, recipe,
    )
    required_long = min(max_size, original_render_long) if max_size else original_render_long
    return _rendered_recipe_long_edge(width, height, recipe) >= required_long


def _recipe_render_source(photo, recipe, max_size, vireo_dir, folders):
    from image_loader import get_canonical_image_path

    if not recipe:
        return get_canonical_image_path(photo, vireo_dir, folders)
    primary_is_raw = (
        os.path.splitext(photo["filename"])[1].lower() in _RAW_EXTENSIONS
    )
    canonical = get_canonical_image_path(photo, vireo_dir, folders)
    wc_rel = photo["working_copy_path"]
    # For RAW primaries with a recipe, never short-circuit to the working
    # copy: legacy working copies predate the highlight-preserving RAW
    # decode, and EDIT_MATH_VERSION's migration only purges preview/thumb
    # caches, not working copies. Reusing one would apply the recipe to
    # clipped bytes and bypass RAW_DECODE_PRESERVE_HIGHLIGHTS.
    if not primary_is_raw:
        if not recipe.get("crop") and canonical and wc_rel:
            wc_path = wc_rel if os.path.isabs(wc_rel) else os.path.join(vireo_dir, wc_rel)
            if os.path.abspath(canonical) == os.path.abspath(wc_path):
                return canonical
        if recipe.get("crop") and _working_copy_satisfies_recipe_render(
            photo, recipe, max_size, vireo_dir,
        ):
            return canonical

    folder_path = folders.get(photo["folder_id"])
    if not folder_path:
        if photo["working_copy_path"]:
            wc_path = os.path.join(vireo_dir, photo["working_copy_path"])
            if os.path.exists(wc_path):
                return wc_path
        return ""
    # When the primary is RAW, prefer the RAW source so the caller can decode
    # with highlight preservation rather than serving the camera's already-
    # clipped companion JPEG. The companion remains a valid fallback when the
    # RAW is known to fail extraction for this mtime.
    companion_path = photo["companion_path"]
    original = os.path.join(folder_path, photo["filename"])
    allow_companion = not primary_is_raw or _has_current_working_copy_failure(
        photo,
        vireo_dir,
        trust_existing_working_copy=False,
        live_source_path=original,
        folder_path=folder_path,
    )
    if companion_path and allow_companion:
        companion = os.path.join(folder_path, companion_path)
        if (
            os.path.exists(companion)
            and _path_satisfies_recipe_render(companion, photo, recipe, max_size)
        ):
            return companion
    if not os.path.exists(original) and photo["working_copy_path"]:
        wc_path = os.path.join(vireo_dir, photo["working_copy_path"])
        if os.path.exists(wc_path):
            return wc_path
    return original


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
_WORKING_COPY_FAILURE_RETRY_SECONDS = 24 * 60 * 60


def _thumb_raw_decode_kwargs(photo, recipe):
    """Return raw_decode kwargs for generate_thumbnail."""
    if not recipe or not photo:
        return {}
    filename = _photo_value(photo, "filename") or ""
    if os.path.splitext(filename)[1].lower() not in _RAW_EXTENSIONS:
        return {}
    from image_loader import RAW_DECODE_PRESERVE_HIGHLIGHTS
    return {"raw_decode": RAW_DECODE_PRESERVE_HIGHLIGHTS}


def _thumb_min_source_size_kwargs(photo, recipe, thumb_size, source_path):
    if not recipe or not photo:
        return {}
    filename = _photo_value(photo, "filename") or ""
    if os.path.splitext(filename)[1].lower() not in _RAW_EXTENSIONS:
        return {}
    if os.path.splitext(source_path or "")[1].lower() not in _RAW_EXTENSIONS:
        return {}
    load_max_size = None if recipe.get("crop") else thumb_size
    return {
        "min_source_size": _scaled_recipe_source_dimensions(photo, load_max_size),
    }


def _has_current_working_copy_failure(
    photo, vireo_dir=None, trust_existing_working_copy=True,
    live_source_path=None, folder_path=None,
):
    working_copy_path = _photo_value(photo, "working_copy_path")
    if working_copy_path and trust_existing_working_copy:
        if not vireo_dir:
            return False
        wc_abs = (
            working_copy_path if os.path.isabs(working_copy_path)
            else os.path.join(vireo_dir, working_copy_path)
        )
        if os.path.exists(wc_abs):
            return False

    filename = _photo_value(photo, "filename") or ""
    if os.path.splitext(filename)[1].lower() not in _RAW_EXTENSIONS:
        return False

    companion_path = _photo_value(photo, "companion_path")
    if companion_path and live_source_path and folder_path:
        companion_abs = os.path.join(folder_path, companion_path)
        failed_source = _photo_value(photo, "working_copy_failed_source")
        if (
            failed_source != "source"
            and os.path.exists(live_source_path)
            and os.path.exists(companion_abs)
        ):
            return False

    failed_at = _photo_value(photo, "working_copy_failed_at")
    failed_mtime = _photo_value(photo, "working_copy_failed_mtime")
    file_mtime = _photo_value(photo, "file_mtime")
    if not failed_at or failed_mtime is None or file_mtime is None:
        return False
    try:
        if float(failed_mtime) != float(file_mtime):
            return False
    except (TypeError, ValueError):
        return False
    try:
        failed_s = str(failed_at).strip()
        if failed_s.endswith("Z"):
            failed_s = failed_s[:-1] + "+00:00"
        failed_dt = datetime.fromisoformat(failed_s)
        if failed_dt.tzinfo is None:
            failed_dt = failed_dt.replace(tzinfo=UTC)
    except (TypeError, ValueError):
        return False
    age = (datetime.now(UTC) - failed_dt.astimezone(UTC)).total_seconds()
    return age < _WORKING_COPY_FAILURE_RETRY_SECONDS


def _retry_thumbnail_with_companion(
    thread_db, generate_thumbnail, photo, photo_id, raw_source_path,
    cache_dir, thumb_size, recipe, folder_path,
):
    """Mirror serve_thumb's RAW->companion fallback for pipeline jobs."""
    if not photo or not folder_path:
        return None
    companion_rel = _photo_value(photo, "companion_path")
    if not companion_rel:
        return None
    companion_abs = os.path.join(folder_path, companion_rel)
    if (
        not os.path.exists(companion_abs)
        or os.path.abspath(companion_abs) == os.path.abspath(raw_source_path)
    ):
        return None
    log.info(
        "Pipeline thumbnail RAW decode failed for photo %s; "
        "falling back to companion JPEG",
        photo_id,
    )
    file_mtime = _photo_value(photo, "file_mtime")
    if file_mtime is not None:
        with contextlib.suppress(Exception):
            thread_db.conn.execute(
                "UPDATE photos SET"
                " working_copy_failed_at=datetime('now'),"
                " working_copy_failed_mtime=?,"
                " working_copy_failed_source='source'"
                " WHERE id=?",
                (file_mtime, photo_id),
            )
            commit_with_retry(thread_db.conn)
    recipe_kwargs = {"recipe": recipe} if recipe else {}
    return generate_thumbnail(
        photo_id,
        companion_abs,
        cache_dir,
        size=thumb_size,
        **recipe_kwargs,
    )


_CLASSIFIER_BUNDLE_FIELDS = (
    "clf", "model_type", "model_name", "model_str",
    "labels", "use_tol", "active_model", "labels_fingerprint",
)


def _taxonomy_fingerprint(tax):
    """Stable key component representing the taxonomy backing a classifier.

    Used in the timm classifier cache key so a pipeline that loads the
    classifier with no taxonomy (or a stale one) does not get reused after
    a taxonomy download or refresh updates the on-disk file. ``None`` is
    a valid input — pipelines run without taxonomy still hit the cache
    consistently among themselves.
    """
    if tax is None:
        return ("no-tax",)
    path = getattr(tax, "_path", None)
    if not path:
        return ("inline", id(tax))
    try:
        st = os.stat(path)
        return (path, int(st.st_mtime_ns), st.st_size)
    except OSError:
        return (path, None, None)


def _weights_fingerprint(weights_path, files):
    """Stable key component reflecting the on-disk state of the weights.

    A cached ONNX session is keyed by ``weights_path`` plus this
    fingerprint. Without it, a Repair (download in place) or a custom-
    model re-registration overwrites the file at the same path but the
    classifier cache reuses the previously-loaded session built from the
    old bytes — silently classifying with stale or corrupt weights.
    Stat-only (size + mtime_ns) so the lookup stays cheap; in-place
    replacement reliably bumps mtime even for byte-identical files.
    Missing files map to a sentinel so a partial repair still misses.

    Custom models have no declared ``files`` list, so fall back to
    listing the directory (or stat'ing the single file if
    ``weights_path`` points at one). Without this fallback a user who
    re-registers a custom model at the same path would reuse the stale
    session until the idle window expires.
    """
    if not weights_path:
        return None
    if files:
        parts = []
        for rel in files:
            path = os.path.join(weights_path, rel)
            try:
                st = os.stat(path)
                parts.append((rel, st.st_size, int(st.st_mtime_ns)))
            except OSError:
                parts.append((rel, None, None))
        return tuple(parts)
    try:
        if os.path.isdir(weights_path):
            parts = []
            for name in sorted(os.listdir(weights_path)):
                path = os.path.join(weights_path, name)
                try:
                    st = os.stat(path)
                    parts.append((name, st.st_size, int(st.st_mtime_ns)))
                except OSError:
                    parts.append((name, None, None))
            return tuple(parts)
        if os.path.isfile(weights_path):
            st = os.stat(weights_path)
            return (("__file__", st.st_size, int(st.st_mtime_ns)),)
    except OSError:
        pass
    return None


def _release_classifier_cache_handle(loaded_models):
    """Release the cache handle AND drop the bundle's strong refs.

    Releasing the handle alone isn't enough: ``loaded_models`` still
    holds ``clf`` (the live Classifier/ONNX session) so the GC can't
    reclaim it. After idle eviction removes the cache entry, a second
    pipeline that loads the same model gets a fresh session — doubling
    VRAM — while this pipeline's stale ``clf`` is still pinned. Dropping
    the bundle fields here lets idle eviction actually free VRAM and
    lets same-model reloads hit the cache.

    Idempotent: no-op if no handle present (classify skipped before any
    model loaded).
    """
    handle = loaded_models.pop("_cache_handle", None)
    for k in _CLASSIFIER_BUNDLE_FIELDS:
        loaded_models.pop(k, None)
    if handle is None:
        return
    try:
        handle.release()
    except Exception:
        # Releasing a cache handle must never break pipeline cleanup.
        # The cache's idle timer will reclaim leaked entries eventually.
        log.exception("ModelCache: handle release raised; leak will be reclaimed by idle timer")


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


# Serializes snapshot-and-push of `stages` across the pipeline's daemon
# threads (scanner, thumbnail, model_loader, ...). All of them emit
# progress events whose payload includes a shallow copy of the shared
# `stages` dict; without this lock a thread can build the snapshot, get
# preempted, and land its stale event after another thread has already
# pushed events with newer counts — producing a non-monotonic SSE stream
# (the CI flake on test_pipeline_multi_source_ingest_progress_is_monotonic).
# Lock order: _progress_lock outside, JobRunner._lock inside.
_progress_lock = threading.Lock()


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


def _emit_progress(runner, job_id, stages, stage_id, phase, **extra):
    """Atomically snapshot `stages` and push a progress event.

    Replaces the unguarded ``runner.push_event(..., _progress_event(...))``
    pattern at every per-stage callback. Holding ``_progress_lock`` across
    the snapshot construction and the push_event call prevents a stale
    snapshot from another thread from landing out of order in the event
    log."""
    with _progress_lock:
        runner.push_event(
            job_id, "progress",
            _progress_event(stages, stage_id, phase, **extra),
        )


def _update_stages(runner, job_id, stages):
    """Push a stages progress update with weighted overall current/total.

    Snapshot+push are atomic under ``_progress_lock`` so concurrent emits
    from other pipeline threads can't produce stale events that land out
    of order."""
    with _progress_lock:
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
    final_destination = params.destination if params.local_processing else None
    staging_parent = None
    archive_destination_reserved = False
    if params.local_processing and params.destination:
        from local_processing import staging_root

        # Reserve the final destination across the whole process BEFORE any
        # staging or scanning starts. SLOT_CAP=2 in JobRunner means two
        # local-processing pipelines can race past the storage stage's
        # DB-only overlap check; without this reservation the second one
        # would only fail inside ``move_folder`` after staging and
        # processing everything. ``release_archive_destination`` runs in
        # the finally below so retries can re-claim once this run ends.
        if not try_reserve_archive_destination(final_destination):
            raise RuntimeError(
                f"Archive destination {final_destination} is already "
                "being used by another local-processing pipeline. Wait "
                "for that job to finish, or pick a different destination."
            )
        archive_destination_reserved = True

        staging_parent = os.path.join(effective_vireo_dir, "staging", job["id"])
        params.destination = staging_root(
            effective_vireo_dir, job["id"], params.destination,
        )

    try:
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
            "storage": {"status": "pending", "label": "Checking local storage"},
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
            "archive": {"status": "pending", "count": 0, "label": "Archiving photos"},
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
            if params.local_processing:
                step_defs.append({"id": "storage", "label": "Check local storage"})
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
        if params.local_processing:
            step_defs.append({"id": "archive", "label": "Archive to destination"})
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
        if not params.local_processing:
            stages["storage"]["status"] = "skipped"
            stages["archive"]["status"] = "skipped"

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
                    # Abort-aware put: a blocking no-timeout put would wedge the
                    # scanner forever if the thumbnail consumer died with a full
                    # queue (its setup can fail before its drain loop starts).
                    # On abort the item is dropped — the consumer is gone and the
                    # pipeline is tearing down anyway.
                    while not _should_abort(abort):
                        try:
                            scan_to_thumb.put((photo_id, path), timeout=0.5)
                            break
                        except queue.Full:
                            continue
                    stages["scan"]["count"] = len(collected_photo_ids)
                    runner.update_step(job["id"], "scan",
                                       current_file=os.path.basename(path))

                def status_cb(message):
                    runner.update_step(job["id"], "scan", current_file=message)
                    _emit_progress(
                        runner, job["id"], stages, "scan", message,
                    )

                def cancel_check():
                    return _should_abort(abort) or runner.is_cancelled(job["id"])

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
                    _emit_progress(
                        runner, job["id"], stages, "scan", "Scanning photos",
                        rate=rate,
                        eta_seconds=eta,
                    )

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
                                cancel_check=cancel_check,
                            )
                        except (OSError, RuntimeError) as e:
                            if str(e) == "scan cancelled" and (
                                _should_abort(abort) or runner.is_cancelled(job["id"])
                            ):
                                abort.set()
                                stages["scan"]["status"] = "skipped"
                                runner.update_step(
                                    job["id"], "scan",
                                    status="completed",
                                    summary="Cancelled",
                                )
                                break
                            log.warning(
                                "Repair scan failed for %s: %s", folder_path, e,
                            )
                            unreachable += 1
                        finally:
                            advance_scan_acc()

                    if _should_abort(abort) or runner.is_cancelled(job["id"]):
                        stages["scan"]["status"] = "skipped"
                        runner.update_step(
                            job["id"], "scan",
                            status="completed",
                            summary="Cancelled",
                        )
                        scan_to_thumb.put(_SENTINEL)
                        return

                    from metadata import scan_metadata_warning

                    summary = f"{total_broken} photos repaired"
                    if unreachable:
                        summary += (f", {unreachable} folder"
                                    f"{'s' if unreachable != 1 else ''} unreachable")
                    # Mirror the standalone scan/import paths in app.py: append
                    # the missing-exiftool warning so a repair scan that lost
                    # metadata reads as degraded, not as a clean success.
                    metadata_warning = scan_metadata_warning()
                    if metadata_warning:
                        summary += f" — {metadata_warning}"
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

                    if params.local_processing:
                        from local_processing import (
                            format_bytes,
                            non_duplicate_bytes,
                            selected_source_files,
                            storage_plan,
                            total_file_bytes,
                        )
                        from move import (
                            _tracked_destination_ancestor,
                            _tracked_destination_overlap,
                        )

                        stages["storage"]["status"] = "running"
                        runner.update_step(job["id"], "storage", status="running")
                        _update_stages(runner, job["id"], stages)

                        def _bail_storage(msg):
                            # collection_stage spins on stages["scan"]["status"]
                            # until it reaches a terminal value, so a storage
                            # failure that skips scan and ingest entirely must
                            # mark both as skipped here — otherwise its join()
                            # blocks the whole pipeline forever.
                            errors.append(f"[storage] Fatal: {msg}")
                            stages["storage"]["status"] = "failed"
                            runner.update_step(
                                job["id"], "storage",
                                status="failed", error=msg,
                            )
                            for skipped in ("ingest", "scan"):
                                stages[skipped]["status"] = "skipped"
                                runner.update_step(
                                    job["id"], skipped,
                                    status="completed", summary="Skipped",
                                )
                            abort.set()
                            scan_to_thumb.put(_SENTINEL)

                        try:
                            # Reject up front if the archive destination is already
                            # a Vireo-managed folder (e.g., a repeat import to the
                            # same archive root). Without this check the pipeline
                            # would stage everything, complete every processing
                            # step, and then fail in move_folder's tracked-overlap
                            # guard — leaving the staged copy stranded under
                            # ~/.vireo/staging with no way to resume.
                            tracked = _tracked_destination_overlap(
                                thread_db, -1, final_destination,
                            )
                            if tracked:
                                _bail_storage(
                                    f"Archive destination {final_destination} "
                                    f"overlaps a folder Vireo already manages "
                                    f"({tracked['path']}). Local processing "
                                    "imports must land at a new archive folder; "
                                    "pick a different destination or import "
                                    "without local processing."
                                )
                                return

                            # Also reject when the archive destination would land
                            # INSIDE an already-tracked folder. db.move_folder_path
                            # (called by move_folder during archive) only rewrites
                            # the moved row's path string — it does NOT reparent the
                            # row under the tracked ancestor. The catalog would end
                            # up with two unrelated workspace roots whose path
                            # strings overlap, e.g. /Photos and /Photos/NewShoot,
                            # silently confusing the folder tree and breaking
                            # future scans of the ancestor root.
                            ancestor = _tracked_destination_ancestor(
                                thread_db, -1, final_destination,
                            )
                            if ancestor:
                                _bail_storage(
                                    f"Archive destination {final_destination} "
                                    f"is inside a folder Vireo already manages "
                                    f"({ancestor['path']}). Pick an archive "
                                    f"folder outside {ancestor['path']}, or "
                                    "import without local processing so the "
                                    "scan can attach to the existing folder."
                                )
                                return

                            # Make sure the archive parent exists NOW. Otherwise
                            # the pipeline would stage and process everything,
                            # then fail at the final move_folder call when rsync
                            # tries to write to a missing parent — leaving the
                            # staged copy stranded under ~/.vireo/staging with no
                            # archive at the final destination. Nested archive
                            # targets like /mnt/nas/NewShoot/Photos are the
                            # common case: the parent /mnt/nas/NewShoot may not
                            # have been created yet by the user.
                            archive_parent = os.path.dirname(
                                os.path.normpath(final_destination),
                            )
                            try:
                                os.makedirs(archive_parent, exist_ok=True)
                            except OSError as exc:
                                _bail_storage(
                                    f"Archive parent {archive_parent} could "
                                    f"not be created: {exc}. Check that the "
                                    "destination drive is mounted and writable."
                                )
                                return

                            os.makedirs(params.destination, exist_ok=True)
                            selected_files = selected_source_files(
                                sources,
                                params.file_types,
                                recursive=params.recursive,
                                exclude_paths=params.exclude_paths,
                            )
                            source_bytes = total_file_bytes(selected_files)
                            plan = storage_plan(
                                params.destination, source_bytes,
                                archive_parent=archive_parent,
                            )
                            # When skip_duplicates is on, ingest() will hash and
                            # skip files whose hash is already in the catalog
                            # OR already seen earlier in this same run before
                            # writing to staging. The naive byte sum above would
                            # mark a mostly-duplicate card — or one where the
                            # same folder appears in `sources` twice — as
                            # batching-required even though the staging copy
                            # would fit. Re-check using the duplicate-filtered
                            # set, but only when the optimistic check failed —
                            # otherwise we'd hash the entire source set on every
                            # import. The filter runs even when the catalog is
                            # empty so intra-run duplicates still collapse.
                            if (
                                plan["batching_required"]
                                and params.skip_duplicates
                                and selected_files
                            ):
                                known_hashes = {
                                    row["file_hash"] for row in thread_db.conn.execute(
                                        "SELECT file_hash FROM photos "
                                        "WHERE file_hash IS NOT NULL"
                                    )
                                }
                                filtered_bytes = non_duplicate_bytes(
                                    selected_files, known_hashes,
                                )
                                if filtered_bytes < source_bytes:
                                    plan = storage_plan(
                                        params.destination, filtered_bytes,
                                        archive_parent=archive_parent,
                                    )
                            result["local_processing"] = {
                                **plan,
                                "staging_destination": params.destination,
                                "final_destination": final_destination,
                            }
                            if plan["batching_required"]:
                                # Tell the user which volume came up short — the
                                # destination running out of room reads as a
                                # different problem (pick a bigger archive
                                # drive) than the staging volume running out
                                # (free space on ~/.vireo or batch later).
                                if not plan.get("archive_enough", True):
                                    _bail_storage(
                                        "Archive destination needs about "
                                        f"{format_bytes(plan['archive_required_bytes'])}, "
                                        "but only "
                                        f"{format_bytes(plan['archive_usable_bytes'] or 0)} "
                                        f"is free under {archive_parent} after "
                                        "the free-space reserve. Free space at "
                                        "the destination or pick a different "
                                        "archive folder."
                                    )
                                else:
                                    _bail_storage(
                                        "Local processing needs about "
                                        f"{format_bytes(plan['required_bytes'])}, but "
                                        f"only {format_bytes(plan['usable_bytes'])} is "
                                        "available after keeping local free-space "
                                        "reserve. This import needs "
                                        f"{plan['batch_count']} local-processing "
                                        "batches; automatic batch execution is not "
                                        "available in this build yet."
                                    )
                                return
                            summary = (
                                f"{format_bytes(plan['required_bytes'])} needed, "
                                f"{format_bytes(plan['usable_bytes'])} available"
                            )
                            stages["storage"]["status"] = "completed"
                            runner.update_step(
                                job["id"], "storage",
                                status="completed",
                                summary=summary,
                            )
                        except Exception as e:
                            log.exception("Pipeline local-storage preflight failed")
                            _bail_storage(str(e))
                            return

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
                        _emit_progress(
                            runner, job["id"], stages, "ingest", "Importing photos",
                            current_file=filename,
                        )

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
                    total_failed = 0
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
                        total_failed += result_info.get("failed", 0)
                        # Collect hashes of files just copied so the next source
                        # iteration treats them as known even before the DB scan.
                        if params.skip_duplicates:
                            import contextlib

                            from scanner import compute_file_hash
                            for path in result_info.get("copied_paths", []):
                                with contextlib.suppress(OSError):
                                    accumulated_hashes.add(compute_file_hash(path))

                    # In local-processing mode, ingest failures must fail the
                    # ingest stage so archive_stage's "any earlier stage failed"
                    # gate skips publishing. ingest() catches per-file copy
                    # errors (unreadable source, disk full mid-card) and returns
                    # a non-zero ``failed`` count without raising. Without
                    # propagating that here the archive step would happily move
                    # the partial staging tree to the user's final destination
                    # — publishing a partial result that the rest of the
                    # pipeline would otherwise treat as a successful import.
                    parts = []
                    if total_copied:
                        parts.append(f"{total_copied} copied")
                    if total_skipped:
                        parts.append(f"{total_skipped} skipped")
                    if total_failed:
                        parts.append(f"{total_failed} failed")
                    summary = ", ".join(parts) or "0 files"
                    if params.local_processing and total_failed:
                        msg = (
                            f"{total_failed} file"
                            f"{'s' if total_failed != 1 else ''} failed to copy "
                            f"during ingest; archive skipped to avoid publishing "
                            f"a partial result"
                        )
                        errors.append(f"[ingest] Fatal: {msg}")
                        stages["ingest"]["status"] = "failed"
                        runner.update_step(
                            job["id"], "ingest",
                            status="failed",
                            error=msg,
                            summary=summary,
                        )
                    else:
                        stages["ingest"]["status"] = "completed"
                        runner.update_step(
                            job["id"], "ingest", status="completed",
                            summary=summary,
                        )
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
                    # Surface kernel-level enumeration denials (macOS TCC EPERM,
                    # POSIX EACCES) into job["errors"]. Without this, scanner.scan
                    # silently skipped the subtree and the scan stage finished as
                    # "completed" with 0 photos — a black-box outcome the user
                    # reads as "no photos found here" when the truth is "Vireo
                    # was denied access". Dedup per path because the walk may
                    # signal the same dir more than once.
                    denied_seen: set[str] = set()
                    def _on_denied(path: str) -> None:
                        if path in denied_seen:
                            return
                        denied_seen.add(path)
                        errors.append(
                            f"[scan] PERMISSION_DENIED: {path} — macOS or "
                            f"filesystem refused enumeration. On macOS open "
                            f"System Settings → Privacy & Security → Files "
                            f"and Folders (or Removable/Network Volumes) and "
                            f"grant Vireo access."
                        )
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
                        permission_error_callback=_on_denied,
                        cancel_check=cancel_check,
                    )
                else:
                    # Scan-in-place: scan each source folder independently.
                    stages["scan"]["status"] = "running"
                    runner.update_step(job["id"], "scan", status="running")
                    job["progress"]["current"] = 0
                    job["progress"]["total"] = 0
                    _update_stages(runner, job["id"], stages)
                    # See ingest branch above — same denial-surfacing rationale.
                    denied_seen: set[str] = set()
                    def _on_denied(path: str) -> None:
                        if path in denied_seen:
                            return
                        denied_seen.add(path)
                        errors.append(
                            f"[scan] PERMISSION_DENIED: {path} — macOS or "
                            f"filesystem refused enumeration. On macOS open "
                            f"System Settings → Privacy & Security → Files "
                            f"and Folders (or Removable/Network Volumes) and "
                            f"grant Vireo access."
                        )
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
                                permission_error_callback=_on_denied,
                                cancel_check=cancel_check,
                            )
                        finally:
                            advance_scan_acc()
                if _should_abort(abort) or runner.is_cancelled(job["id"]):
                    stages["scan"]["status"] = "skipped"
                    runner.update_step(
                        job["id"], "scan", status="completed", summary="Cancelled",
                    )
                else:
                    from metadata import scan_metadata_warning

                    stages["scan"]["status"] = "completed"
                    # Pipeline scans use scanner.scan exactly like the standalone
                    # /api/jobs/scan path, so a missing exiftool silently strips
                    # capture dates, GPS, and camera info here too. Append the
                    # same warning the standalone path appends.
                    scan_summary = f"{stages['scan']['count']} photos"
                    metadata_warning = scan_metadata_warning()
                    if metadata_warning:
                        scan_summary += f" — {metadata_warning}"
                    runner.update_step(job["id"], "scan", status="completed",
                                       summary=scan_summary)
            except Exception as e:
                if str(e) == "scan cancelled" and (
                    _should_abort(abort) or runner.is_cancelled(job["id"])
                ):
                    abort.set()
                    stages["scan"]["status"] = "skipped"
                    runner.update_step(
                        job["id"], "scan", status="completed", summary="Cancelled",
                    )
                else:
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
                if stages["scan"]["status"] in ("completed", "failed", "skipped"):
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
                        recipe = thread_db.get_photo_edit_recipe(photo_id)
                        detail_photo = None
                        if recipe:
                            detail_photo = thread_db.get_photo(photo_id)
                            if detail_photo:
                                folder_row = thread_db.get_folder(detail_photo["folder_id"])
                                folders = (
                                    {folder_row["id"]: folder_row["path"]}
                                    if folder_row else {}
                                )
                                photo_path = _recipe_render_source(
                                    detail_photo,
                                    recipe,
                                    thumb_size,
                                    effective_vireo_dir,
                                    folders,
                                )
                                if (
                                    os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                                    and _has_current_working_copy_failure(
                                        detail_photo,
                                        effective_vireo_dir,
                                        trust_existing_working_copy=False,
                                        live_source_path=photo_path,
                                        folder_path=folders.get(detail_photo["folder_id"]),
                                    )
                                ):
                                    skipped += 1
                                    continue
                        recipe_kwargs = {"recipe": recipe} if recipe else {}
                        raw_decode_kwargs = _thumb_raw_decode_kwargs(
                            detail_photo, recipe,
                        )
                        min_size_kwargs = _thumb_min_source_size_kwargs(
                            detail_photo, recipe, thumb_size, photo_path,
                        )
                        result_path = generate_thumbnail(
                            photo_id,
                            photo_path,
                            cache_dir,
                            size=thumb_size,
                            **recipe_kwargs,
                            **raw_decode_kwargs,
                            **min_size_kwargs,
                        )
                        if (
                            result_path is None
                            and detail_photo is not None
                            and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                        ):
                            result_path = _retry_thumbnail_with_companion(
                                thread_db, generate_thumbnail, detail_photo,
                                photo_id, photo_path, cache_dir, thumb_size,
                                recipe, folders.get(detail_photo["folder_id"]),
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
                    _emit_progress(
                        runner, job["id"], stages, "thumbnails", "Generating thumbnails",
                        current_file=os.path.basename(photo_path),
                        rate=rate,
                    )

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
                            recipe = thread_db.get_photo_edit_recipe(photo_id)
                            detail_photo = None
                            if recipe:
                                detail_photo = thread_db.get_photo(photo_id) or photo
                                photo_path = _recipe_render_source(
                                    detail_photo,
                                    recipe,
                                    thumb_size,
                                    effective_vireo_dir,
                                    folders,
                                )
                                if (
                                    os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                                    and _has_current_working_copy_failure(
                                        detail_photo,
                                        effective_vireo_dir,
                                        trust_existing_working_copy=False,
                                        live_source_path=photo_path,
                                        folder_path=folders.get(detail_photo["folder_id"]),
                                    )
                                ):
                                    skipped += 1
                                    continue
                            recipe_kwargs = {"recipe": recipe} if recipe else {}
                            raw_decode_kwargs = _thumb_raw_decode_kwargs(
                                detail_photo, recipe,
                            )
                            min_size_kwargs = _thumb_min_source_size_kwargs(
                                detail_photo, recipe, thumb_size, photo_path,
                            )
                            result_path = generate_thumbnail(
                                photo_id,
                                photo_path,
                                cache_dir,
                                size=thumb_size,
                                **recipe_kwargs,
                                **raw_decode_kwargs,
                                **min_size_kwargs,
                            )
                            if (
                                result_path is None
                                and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                            ):
                                fallback_photo = detail_photo or photo
                                result_path = _retry_thumbnail_with_companion(
                                    thread_db, generate_thumbnail, fallback_photo,
                                    photo_id, photo_path, cache_dir, thumb_size,
                                    recipe, folder_path,
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
                        _emit_progress(
                            runner, job["id"], stages, "thumbnails", "Generating thumbnails",
                            current_file=os.path.basename(photo_path),
                            rate=rate,
                        )

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
                # This stage is the sole consumer of scan_to_thumb. Anything
                # above can raise BEFORE the drain loop (import, Database(),
                # cfg.load(), os.makedirs) — if we just returned, the scanner
                # would eventually block forever in put() once the queue fills,
                # wedging threads["scanner"].join() and leaking a pipeline slot
                # until restart. Set abort so the scanner stops producing, then
                # drain whatever is already queued. Stop at the sentinel, or
                # when the queue stays empty (the sentinel may already have
                # been consumed by the main loop before a late failure; with
                # abort set, photo_cb no longer blocks, so breaking on Empty
                # is safe).
                abort.set()
                while True:
                    try:
                        item = scan_to_thumb.get(timeout=1.0)
                    except queue.Empty:
                        break
                    if item is _SENTINEL:
                        break
            _update_stages(runner, job["id"], stages)

        def previews_stage():
            """Generate preview images for browsed photos."""
            if abort.is_set():
                stages["previews"]["status"] = "skipped"
                runner.update_step(job["id"], "previews", status="completed",
                                   summary="Skipped")
                return

            stages["previews"]["status"] = "running"
            runner.update_step(job["id"], "previews", status="running")
            _update_stages(runner, job["id"], stages)

            try:
                import config as cfg
                from image_edits import apply_recipe_to_loaded_image
                from image_loader import (
                    RAW_DECODE_PRESERVE_HIGHLIGHTS,
                    load_image,
                )

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                effective = thread_db.get_effective_config(cfg.load())
                raw_size = (
                    params.preview_max_size
                    if params.preview_max_size is not None
                    else effective.get("preview_max_size", 1920)
                )
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
                preview_quality = effective.get("preview_quality", 90)
                # Must match the Flask serve convention — app.py reads, reaps,
                # and evicts previews under dirname(THUMB_CACHE_DIR)/previews.
                # Using dirname(db_path) here would, with a custom --thumb-dir,
                # warm previews (and preview_cache rows) under a root the app
                # never serves from.
                base_dir = effective_vireo_dir
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
                    detail_photo = thread_db.get_photo(photo["id"]) or photo
                    cache_path = os.path.join(preview_dir, f'{photo["id"]}_{max_size}.jpg')
                    recipe = thread_db.get_photo_edit_recipe(photo["id"])
                    if os.path.exists(cache_path):
                        cache_row = None
                        with contextlib.suppress(Exception):
                            cache_row = thread_db.preview_cache_get(photo["id"], max_size)
                        if recipe and cache_row is None:
                            with contextlib.suppress(OSError):
                                os.remove(cache_path)
                            if os.path.exists(cache_path):
                                skipped += 1
                                log.info(
                                    "Skipping untracked edited preview for photo %s; "
                                    "existing cache file could not be removed",
                                    photo["id"],
                                )
                                continue
                        else:
                            skipped += 1
                            try:
                                if cache_row is None:
                                    thread_db.preview_cache_insert(
                                        photo["id"],
                                        max_size,
                                        os.path.getsize(cache_path),
                                    )
                            except Exception:
                                pass  # photo may have been deleted mid-pipeline
                            continue
                    if not os.path.exists(cache_path):
                        canonical = _recipe_render_source(
                            detail_photo, recipe, max_size, base_dir, folders,
                        )
                        if (
                            os.path.splitext(canonical)[1].lower() in _RAW_EXTENSIONS
                            and _has_current_working_copy_failure(
                                detail_photo,
                                base_dir,
                                trust_existing_working_copy=False,
                                live_source_path=canonical,
                                folder_path=folders.get(detail_photo["folder_id"]),
                            )
                        ):
                            skipped += 1
                            log.info(
                                "Skipping pipeline preview for photo %s; RAW "
                                "working-copy extraction already failed for "
                                "current source mtime",
                                photo["id"],
                            )
                            continue
                        load_max_size = (
                            None if recipe and recipe.get("crop") else max_size
                        )
                        raw_decode = (
                            RAW_DECODE_PRESERVE_HIGHLIGHTS
                            if recipe
                            and os.path.splitext(canonical)[1].lower() in _RAW_EXTENSIONS
                            else None
                        )
                        load_kwargs = (
                            {"raw_decode": raw_decode} if raw_decode else {}
                        )
                        img = load_image(canonical, max_size=load_max_size, **load_kwargs)
                        if (
                            img is not None
                            and os.path.splitext(canonical)[1].lower() in _RAW_EXTENSIONS
                            and detail_photo["width"]
                            and detail_photo["height"]
                        ):
                            expected_w, expected_h = _scaled_recipe_source_dimensions(
                                detail_photo, load_max_size,
                            )
                            if _image_is_smaller_than_expected(
                                img, expected_w, expected_h,
                            ):
                                companion_rel = detail_photo["companion_path"]
                                folder_path = folders.get(
                                    detail_photo["folder_id"]
                                )
                                if companion_rel and folder_path:
                                    companion_abs = os.path.join(
                                        folder_path, companion_rel,
                                    )
                                    if (
                                        os.path.exists(companion_abs)
                                        and companion_abs != canonical
                                    ):
                                        companion_img = load_image(
                                            companion_abs,
                                            max_size=load_max_size,
                                        )
                                        if _companion_image_can_replace_raw_result(
                                            companion_img, img,
                                            expected_w, expected_h,
                                        ):
                                            log.info(
                                                "RAW decode for photo %s "
                                                "pipeline preview at size=%s "
                                                "returned undersized embedded "
                                                "preview (%dx%d, expected "
                                                "%dx%d); falling back to "
                                                "companion JPEG",
                                                detail_photo["id"], max_size,
                                                img.size[0], img.size[1],
                                                expected_w, expected_h,
                                            )
                                            img.close()
                                            img = companion_img
                                            canonical = companion_abs
                                        elif companion_img is not None:
                                            companion_img.close()
                        if (
                            img is None
                            and os.path.splitext(canonical)[1].lower() in _RAW_EXTENSIONS
                        ):
                            companion_rel = detail_photo["companion_path"]
                            folder_path = folders.get(detail_photo["folder_id"])
                            if companion_rel and folder_path:
                                companion_abs = os.path.join(folder_path, companion_rel)
                                if (
                                    os.path.exists(companion_abs)
                                    and companion_abs != canonical
                                ):
                                    log.info(
                                        "RAW decode failed for photo %s pipeline "
                                        "preview at size=%s; falling back to "
                                        "companion JPEG",
                                        detail_photo["id"], max_size,
                                    )
                                    file_mtime = detail_photo["file_mtime"]
                                    if file_mtime is not None:
                                        with contextlib.suppress(Exception):
                                            thread_db.conn.execute(
                                                "UPDATE photos SET"
                                                " working_copy_failed_at=datetime('now'),"
                                                " working_copy_failed_mtime=?,"
                                                " working_copy_failed_source='source'"
                                                " WHERE id=?",
                                                (file_mtime, detail_photo["id"]),
                                            )
                                            commit_with_retry(thread_db.conn)
                                    img = load_image(
                                        companion_abs, max_size=load_max_size,
                                    )
                                    if img is not None:
                                        canonical = companion_abs
                        if img:
                            if recipe:
                                img = apply_recipe_to_loaded_image(
                                    img, recipe, max_size=max_size,
                                )
                            # Atomic write: with SLOT_CAP > 1 two pipelines
                            # processing the same photo can both miss the
                            # os.path.exists() check above and race here on the
                            # deterministic {id}_{max_size}.jpg path. A direct
                            # img.save(cache_path) would interleave/truncate the
                            # JPEG bytes; tempfile + os.replace makes the visible
                            # file flip atomically (same pattern as
                            # thumbnails.generate_thumbnail).
                            fd, tmp_path = tempfile.mkstemp(
                                prefix=f'.{photo["id"]}.', suffix=".jpg.tmp",
                                dir=preview_dir,
                            )
                            os.close(fd)
                            try:
                                img.save(tmp_path, format="JPEG", quality=preview_quality)
                                os.replace(tmp_path, cache_path)
                            except Exception:
                                with contextlib.suppress(OSError):
                                    os.unlink(tmp_path)
                                raise
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
                    _emit_progress(
                        runner, job["id"], stages, "previews", "Generating previews",
                        current_file=photo["filename"],
                        rate=round(
                            (i + 1) / max(time.time() - job["_start_time"], 0.01) * 60, 1
                        ),
                    )

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

            def _construct_classifier():
                if model_type == "timm":
                    from timm_classifier import TimmClassifier
                    return TimmClassifier(model_str, taxonomy=tax)
                from classifier import Classifier
                return Classifier(
                    labels=None if use_tol else labels,
                    model_str=model_str,
                    pretrained_str=weights_path,
                )

            # Cache key includes the labels fingerprint when use_tol=False because
            # the Classifier pre-computes text embeddings for the provided labels
            # at construction time. Two pipelines with the same model but
            # different labels must NOT share a session. Tree-of-Life mode
            # (use_tol=True) reads precomputed embeddings from disk and is
            # label-independent, so the key collapses to a constant.
            #
            # The timm key also varies by taxonomy fingerprint: TimmClassifier
            # captures the taxonomy at construction and resolves common names /
            # hierarchy from it on every prediction. Reusing a classifier loaded
            # against a stale taxonomy (or no taxonomy) after a later run
            # downloads or refreshes one would silently emit predictions
            # missing the enrichment, so a change in taxonomy must miss the
            # cache and rebuild.
            #
            # The weights fingerprint catches in-place model replacement
            # (Repair, custom re-register). Without it, a pipeline started
            # before the old session's idle timer fires would reuse the
            # stale ONNX session on the new bytes.
            tax_fp = _taxonomy_fingerprint(tax) if model_type == "timm" else None
            files = active_model.get("files")

            def _build_cache_key(weights_fp):
                return (
                    "timm" if model_type == "timm" else "bioclip",
                    active_model["id"],
                    model_str,
                    weights_path,
                    "__tol__" if use_tol else fp,
                    tax_fp,
                    weights_fp,
                )

            cache_key = _build_cache_key(_weights_fingerprint(weights_path, files))

            # _construct_classifier may trigger the ONNX self-heal path
            # (create_session_with_self_heal) which deletes corrupt weights
            # and redownloads them inside the factory. That bumps the file
            # mtime/size, so the pre-load fingerprint baked into ``cache_key``
            # no longer matches what the *next* pipeline will compute. Rekey
            # to the post-load fingerprint so the second pipeline hits this
            # entry instead of loading a duplicate session against the freshly
            # written bytes.
            def _post_load_key(_value):
                return _build_cache_key(_weights_fingerprint(weights_path, files))

            cache_handle = None
            try:
                cache_handle = get_default_cache().acquire(
                    cache_key, _construct_classifier,
                    post_load_key=_post_load_key,
                )
                clf = cache_handle.__enter__()
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
                "_cache_handle": cache_handle,
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

                if collection_id:
                    candidate_photos = _filter_excluded(
                        thread_db.get_collection_photos(collection_id, per_page=999999)
                    )
                    photo_ids = [p["id"] for p in candidate_photos]
                    candidate_ids = thread_db.filter_out_wildlife_excluded(photo_ids)
                    if candidate_photos and not candidate_ids:
                        stages["model_loader"]["status"] = "skipped"
                        runner.update_step(
                            job["id"], "model_loader", status="completed",
                            summary="Skipped (all photos marked not wildlife)",
                        )
                        _update_stages(runner, job["id"], stages)
                        models_ready.set()
                        return

                # Specs were pre-resolved at job start so step_defs could carry
                # the model's display name on each `classify:<id>` row. If that
                # resolution raised, surface the same error here — model_loader
                # is the stage that owns "no model / bad id" failures.
                if resolution_error:
                    raise RuntimeError(resolution_error)

                first_name = resolved_specs[0]["name"]
                runner.update_step(job["id"], "model_loader", current_file=first_name)

                # Download taxonomy if missing/unusable and requested. Mirrors the
                # availability check used by /api/pipeline/page-init: a 0-byte stub
                # from an interrupted download "exists" but is not a usable
                # taxonomy, and the user opted into a download to recover from
                # exactly that state.
                from models import get_taxonomy_info
                from taxonomy import TAXONOMY_JSON_PATH, find_taxonomy_json
                taxonomy_path = find_taxonomy_json()
                if params.download_taxonomy and not get_taxonomy_info().get("available"):
                    try:
                        from taxonomy import download_taxonomy
                        _emit_progress(
                            runner, job["id"], stages, "model_loader", "Downloading taxonomy...",
                        )
                        # Always write new downloads to the persistent path.
                        taxonomy_path = TAXONOMY_JSON_PATH
                        download_taxonomy(taxonomy_path, progress_callback=lambda msg:
                            _emit_progress(
                                runner, job["id"], stages, "model_loader", msg,
                            )
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
                _emit_progress(
                    runner, job["id"], stages, "model_loader", f"Loading {first_name}...",
                )

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
                photo_ids = [p["id"] for p in photos]
                kept_ids = set(thread_db.filter_out_wildlife_excluded(photo_ids))
                skipped_wildlife = len(photos) - len(kept_ids)
                if skipped_wildlife:
                    log.info(
                        "Skipping %d photo(s) marked not wildlife",
                        skipped_wildlife,
                    )
                photos = [p for p in photos if p["id"] in kept_ids]
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
                        _emit_progress(
                            runner, job["id"], stages, "detect", phase,
                        )

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
                    _emit_progress(
                        runner, job["id"], stages, "detect", "Detecting subjects",
                        rate=round(
                            batch_idx / max(time.time() - start_time, 0.01) * 60,
                            1,
                        ),
                    )
                    runner.update_step(
                        job["id"], "detect",
                        progress={"current": batch_idx, "total": total},
                    )

                    # GPU serialisation lives inside detector.detect_animals()
                    # — wrapping the whole batch here would hold the semaphore
                    # across DB writes and CPU sharpness/quality work, blocking
                    # a concurrent pipeline's GPU stages on this pipeline's
                    # non-GPU work.
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
                # model_loader may have already loaded the first classifier
                # before this early-return path was hit (e.g. abort.is_set()).
                # Release its cache handle so a same-key reload can be a hit
                # and idle eviction can reclaim VRAM.
                _release_classifier_cache_handle(loaded_models)
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
                    _BATCH_SIZE,
                    _flush_batch,
                    _prepare_image,
                    _record_batch_classifier_runs,
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
                        _emit_progress(
                            runner, job["id"], stages, "classify", f"Loading {active_spec['name']}...",
                            step_id=step_id,
                        )
                        # Drop the prior model's per-photo payload BEFORE loading
                        # the next bundle so we don't hold old results + new model
                        # weights concurrently. Without this, multi-model runs on
                        # large collections can hit transient OOMs.
                        with contextlib.suppress(NameError, UnboundLocalError):
                            raw_results.clear()  # noqa: F821 — bound in prior iter
                        # Release the previous spec's cache handle so its
                        # refcount drops and a same-cache-key reload below (or
                        # in another pipeline) can be a hit. Must happen
                        # BEFORE popping ``clf`` so we don't lose the only
                        # reference to the bundle that owns the handle.
                        _release_classifier_cache_handle(loaded_models)
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
                    # Pre-flight cache estimate. One indexed query so the UI
                    # can display "~M cached, ~K to classify" before the first
                    # inference runs and ETAs are honest from the start. The
                    # estimate may overcount if a run-key exists but no cached
                    # predictions do (see lines ~2000-2004); the live `cached`
                    # counter reflects actual skips.
                    #
                    # Skipped on reclassify runs (the gate below is bypassed
                    # so every photo is re-inferred regardless of cache state)
                    # and on multi-spec runs (the estimate would only cover
                    # the current spec while ``total`` already spans every
                    # spec, producing a banner that undercounts cache and
                    # overstates remaining work — and since the UI hides the
                    # banner once any photo lands, there's no correction
                    # window). The single-spec case is the common one.
                    if (
                        not params.reclassify
                        and len(resolved_specs_local) == 1
                    ):
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
                    _emit_progress(
                        runner, job["id"], stages, "classify",
                        f"Classifying with {active_spec['name']}",
                        step_id=step_id,
                    )
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
                    inference_batch_size = _BATCH_SIZE
                    inference_batch: list = []
                    has_flushed_in_spec = False

                    def _close_pending_inference(inference_batch=inference_batch):
                        for entry in inference_batch:
                            with contextlib.suppress(Exception):
                                entry["img"].close()
                        inference_batch.clear()

                    def _flush_pending_inference(
                        inference_batch=inference_batch,
                        raw_results=raw_results,
                        clf=clf,
                        model_type=model_type,
                        model_name=model_name,
                        spec_fp=spec_fp,
                    ):
                        nonlocal failed, has_flushed_in_spec
                        if not inference_batch:
                            return

                        pending = list(inference_batch)
                        inference_batch.clear()
                        has_flushed_in_spec = True
                        pre_len = len(raw_results)
                        # GPU serialisation lives inside _flush_batch around the
                        # inference call so the DB upserts/result-building afterward
                        # don't hold the semaphore while the GPU is idle.
                        n_batch_failed = _flush_batch(
                            pending, clf, model_type, model_name,
                            thread_db, raw_results,
                        )
                        failed += n_batch_failed

                        successful_det_ids = {
                            r.get("detection_id") for r in raw_results[pre_len:]
                        }
                        if n_batch_failed:
                            for entry in pending:
                                if entry.get("detection_id") not in successful_det_ids:
                                    failed_photo_ids.add(entry["photo"]["id"])

                        _record_batch_classifier_runs(
                            thread_db, pending, model_name, spec_fp, raw_results,
                            pre_len,
                        )

                        new_count = len(raw_results) - pre_len
                        if new_count > 0:
                            stages["classify"]["count"] = (
                                stages["classify"].get("count", 0) + new_count
                            )

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
                                # cached_detections from _detect_batch can include
                                # full-image rows when an earlier pass synthesized
                                # them (legacy db state); filter to match the
                                # fallback-query branch below so primary_det never
                                # lands on a full-image box. Pipeline classify is
                                # detection-driven and won't classify full-image.
                                photo_dets = [
                                    d for d in cached_detections[photo["id"]]
                                    if d.get("detector_model") != "full-image"
                                ]
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
                            inference_batch.append({
                                "photo": photo,
                                "detection_id": primary_det["id"],
                                "folder_path": folder_path,
                                "image_path": image_path,
                                "img": img,
                            })
                            # Flush the first real inference immediately. That
                            # preserves the existing cancel checkpoint after model
                            # warm-up, then later images batch normally for GPU
                            # throughput.
                            if (
                                not has_flushed_in_spec
                                or len(inference_batch) >= inference_batch_size
                            ):
                                _flush_pending_inference()

                        # Batch boundary: surface the per-photo accumulated
                        # count + cached to the UI. Replaces the old per-batch
                        # pre-advance which lied about progress when batches
                        # contained cache hits.
                        if _should_abort(abort):
                            _close_pending_inference()
                        else:
                            _flush_pending_inference()
                        stages["classify"]["total"] = total * len(resolved_specs_local)
                        elapsed = max(time.time() - start_time, 0.01)
                        _emit_progress(
                            runner, job["id"], stages, "classify",
                            f"Classifying with {active_spec['name']}"
                            + (
                                f" ({spec_idx + 1}/{len(resolved_specs_local)})"
                                if len(resolved_specs_local) > 1 else ""
                            ),
                            step_id=step_id,
                            rate=round(processed_in_spec / elapsed * 60, 1),
                        )
                        runner.update_step(
                            job["id"], step_id,
                            progress={
                                "current": processed_in_spec,
                                "total": total,
                            },
                        )

                    if _should_abort(abort):
                        _close_pending_inference()
                    else:
                        _flush_pending_inference()

                    # Skip the grouping/storage finalization on cancel — it can
                    # take a minute on large collections and the user has already
                    # asked us to stop. Per-photo counters are accurate, no
                    # corrective fixup needed.
                    if _should_abort(abort):
                        _emit_progress(
                            runner, job["id"], stages, "classify",
                            f"Cancelled — {processed_in_spec} of "
                            f"{total} processed",
                            step_id=step_id,
                        )
                        runner.update_step(
                            job["id"], step_id,
                            status="completed",
                            progress={
                                "current": processed_in_spec,
                                "total": total,
                            },
                            summary=(
                                f"Cancelled "
                                f"({processed_in_spec} of {total} processed)"
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
                        # Delete only pre-run ids the current run did NOT
                        # re-produce. Detection ids are content-addressed
                        # (vireo/detection_id.py), so re-detecting the same boxes
                        # yields the SAME ids as the pre-run snapshot, and
                        # write_detection_batch UPSERTs them with the freshly
                        # written predictions now hanging off them. Deleting every
                        # pre-run id unconditionally would cascade-delete those
                        # predictions (and the live detection rows) for every photo
                        # whose boxes didn't change — the common reclassify case.
                        # Compare against the ids THIS run actually re-detected
                        # (detect_state["detections"] is the in-memory map
                        # _detect_batch built); a photo that came back empty has
                        # no entry, so its pre-run rows are all stale and get
                        # purged (write_detection_batch([]) already cleared them at
                        # the data layer — this is the belt-and-suspenders pass and
                        # cross-model cleanup). A photo re-detected with the same
                        # boxes has its ids in the fresh set, so they survive.
                        fresh_by_photo = detect_state["detections"]
                        stale_ids = [
                            det_id
                            for photo_id, id_set in pre_ids.items()
                            if photo_id in purge_ids
                            for det_id in id_set
                            if det_id not in {
                                d["id"]
                                for d in fresh_by_photo.get(photo_id, [])
                            }
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
            finally:
                # Release the held classifier so subsequent pipelines can reuse
                # the cached session (or the idle timer can reclaim VRAM). Runs
                # whether classify completed cleanly, errored mid-loop, or hit
                # the fatal-exception path above.
                _release_classifier_cache_handle(loaded_models)

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
                from dino_embed import embed, embed_batch, embedding_to_blob
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
                sam2_variant = pipeline_cfg.get("sam2_variant")
                dinov2_variant = pipeline_cfg.get("dinov2_variant")
                proxy_longest_edge = pipeline_cfg.get("proxy_longest_edge")

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
                # Note: we intentionally do NOT short-circuit when the photo
                # already has *some* mask in the photos table — that legacy
                # check ignored which SAM variant produced the mask, so a
                # config change to a different variant would never re-run.
                # The per-photo cache check happens inside the loop below
                # against photo_masks(photo_id, sam2_variant).
                #
                # Track sub-threshold-only photos separately so the silent-
                # completion guard can distinguish "no detection rows at all"
                # from "rows exist but every confidence is below
                # detector_confidence" — the user's remediation differs
                # (download weights vs lower the threshold).
                detector_confidence = effective_cfg.get("detector_confidence", 0.2)
                photo_det_map = {}
                photos_with_detections = 0
                photos_subthreshold_only = 0
                for p in photos:
                    # Pass the captured detector_confidence explicitly so the
                    # floor matches effective_cfg (and the standalone
                    # /api/jobs/extract-masks path), not whatever cfg.load()
                    # would re-read from disk inside get_detections. With the
                    # legacy `mask_path IS NULL` prefilter gone, sub-threshold-
                    # only photos would otherwise enter SAM extraction on
                    # variant cache misses.
                    dets = [
                        d for d in thread_db.get_detections(
                            p["id"], min_conf=detector_confidence,
                        )
                        if d["detector_model"] != "full-image"
                    ]
                    if dets:
                        photos_with_detections += 1
                        primary = dets[0]  # already ordered by confidence DESC
                        photo_det_map[p["id"]] = {
                            "photo": p,
                            "det_box": {
                                "x": primary["box_x"],
                                "y": primary["box_y"],
                                "w": primary["box_w"],
                                "h": primary["box_h"],
                            },
                            "detector_model": primary["detector_model"],
                            # Stored prompt provenance: full-precision bbox
                            # tuple. detections.box_* are normalized REAL
                            # values in [0, 1], so int()-truncating would
                            # collapse every prompt to (0, 0, 0, 0) and the
                            # cache/staleness check would never invalidate
                            # on bbox change. SQLite's column type affinity
                            # accepts REAL into the INTEGER-declared
                            # columns and stores them verbatim.
                            "prompt": (
                                primary["box_x"],
                                primary["box_y"],
                                primary["box_w"],
                                primary["box_h"],
                            ),
                        }
                    else:
                        # No qualifying detection — but check whether sub-
                        # threshold rows exist so the silent-completion guard
                        # can distinguish "weights never ran" from "threshold
                        # too high". This counter only matters for photos
                        # that haven't been masked yet (an already-masked
                        # photo isn't in the "what got skipped silently"
                        # diagnostic regardless of threshold).
                        raw_dets = [
                            d for d in thread_db.get_detections(p["id"], min_conf=0)
                            if d["detector_model"] != "full-image"
                        ]
                        if raw_dets:
                            has_mask = thread_db.conn.execute(
                                "SELECT mask_path FROM photos WHERE id=?", (p["id"],)
                            ).fetchone()[0]
                            if not has_mask:
                                photos_subthreshold_only += 1

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

                    if photos_subthreshold_only > 0:
                        reason = (
                            f"{photos_subthreshold_only} photo(s) have detections but every "
                            f"detection is below the current detector_confidence threshold "
                            f"({detector_confidence}). The pipeline will reject these photos "
                            "with `no_subject_mask`. Lower `detector_confidence` in workspace "
                            "settings to extract masks for them."
                        )
                        summary = (
                            f"Skipped — {photos_subthreshold_only} photo(s) below "
                            f"detector_confidence threshold ({detector_confidence})"
                        )
                    elif weights_present:
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
                    if photos_subthreshold_only > 0:
                        em_reason = "all_subthreshold"
                    elif weights_present:
                        em_reason = "no_detections"
                    else:
                        em_reason = "weights_missing"
                    result["stages"]["extract_masks"] = {
                        "masked": 0, "skipped": 0, "failed": 0, "total": 0,
                        "subthreshold": photos_subthreshold_only,
                        "reason": em_reason,
                    }
                    _update_stages(runner, job["id"], stages)
                    return

                # Mixed-state guard: photos_with_detections > 0 (some photos have
                # qualifying detections — already masked from a prior run) but
                # there are also unmasked photos whose only detections are below
                # the threshold. Without this branch the stage completes silently
                # with "0 masked, 0 skipped" and the user has no way to discover
                # why the unmasked photos were never processed. Production hit
                # this when 4166 of 5054 photos were already masked and the
                # remaining 727 had only sub-threshold detections.
                if total == 0 and photos_subthreshold_only > 0 and classify_ran:
                    reason = (
                        f"{photos_subthreshold_only} photo(s) have detections but every "
                        f"detection is below the current detector_confidence threshold "
                        f"({detector_confidence}). The pipeline will reject these photos "
                        "with `no_subject_mask`. Lower `detector_confidence` in workspace "
                        "settings to extract masks for them."
                    )
                    summary = (
                        f"Skipped — {photos_subthreshold_only} photo(s) below "
                        f"detector_confidence threshold ({detector_confidence})"
                    )
                    log.warning("Pipeline extract-masks: %s", reason)
                    errors.append(f"[extract_masks] {reason}")
                    stages["extract_masks"]["status"] = "skipped"
                    runner.update_step(
                        job["id"], "extract_masks", status="completed",
                        summary=summary,
                    )
                    result["stages"]["extract_masks"] = {
                        "masked": 0, "skipped": 0, "failed": 0, "total": 0,
                        "subthreshold": photos_subthreshold_only,
                        "reason": "all_subthreshold",
                    }
                    _update_stages(runner, job["id"], stages)
                    return

                # Auto-download SAM2 + DINOv2 weights on first pipeline run.
                # Mirrors the MegaDetector auto-download pattern (commit 90cd0f9):
                # without this, first-time users hit 1 FileNotFoundError per
                # photo instead of either getting the weights automatically
                # or seeing one actionable message.
                #
                # The download is deferred until the loop hits the first true
                # cache miss. With per-variant photo_masks, the worklist now
                # includes every photo with a detection (cache hits are
                # filtered inside the loop, not by a `mask_path IS NULL`
                # prefilter), so gating on ``total > 0`` would force a
                # multi-hundred-MB download even on a fully-cached rerun in
                # an offline / fresh-checkout environment. ``_ensure_weights``
                # is idempotent and a no-op on second invocation.
                from dino_embed import ensure_dinov2_weights
                from masking import ensure_sam2_weights

                def _dl_progress(phase, current, total_steps):
                    _emit_progress(
                        runner, job["id"], stages, "extract_masks", phase,
                    )

                _weights_ensured = [False]

                def _ensure_weights():
                    if _weights_ensured[0]:
                        return
                    ensure_sam2_weights(
                        variant=sam2_variant, progress_callback=_dl_progress,
                    )
                    ensure_dinov2_weights(
                        variant=dinov2_variant, progress_callback=_dl_progress,
                    )
                    _weights_ensured[0] = True

                processed = 0
                for i, entry in enumerate(photos_to_process):
                    if _should_abort(abort):
                        break

                    photo = entry["photo"]
                    det_box = entry["det_box"]
                    photo_id = photo["id"]
                    folder_path = folders.get(photo["folder_id"], "")
                    image_path = os.path.join(folder_path, photo["filename"])

                    try:
                        # Per-photo serialisation. Two pipelines whose
                        # collections overlap can both reach this photo.
                        # Without this lock:
                        #
                        #   - Same variant: both write the same
                        #     ``masks/{photo_id}.{variant}.png`` file and
                        #     can corrupt each other's bytes mid-write.
                        #   - Different variants (e.g. two workspaces
                        #     sharing folders but configured with sam2-small
                        #     vs sam2-large): the per-variant mask files
                        #     don't collide, BUT both runs denormalise into
                        #     the same ``photos`` row via
                        #     ``set_active_mask_variant`` and
                        #     ``update_photo_embeddings``. Their writes can
                        #     interleave, leaving photos.active_mask_variant
                        #     pointing at one variant while photos.dino_*
                        #     embeddings were cropped from the other's mask.
                        #     regroup reads these denormalised columns, so
                        #     the corruption would silently flow into
                        #     grouping.
                        #
                        # Keyed by photo_id alone — not (photo_id, variant) —
                        # so the cross-variant collision in (2) is covered.
                        # Workspace isn't part of the key because photos are
                        # global in Vireo.
                        with acquire_photo_mask(photo_id):
                            # Cache hit: a row already exists for (photo, variant)
                            # AND its stored prompt + detector still match the
                            # current primary detection AND the file is on disk.
                            # In that case the SAM result is unchanged, so we
                            # only re-activate the mask (cheap denormalize) and
                            # skip the heavy SAM + DINOv2 work.
                            existing = thread_db.get_photo_mask(
                                photo_id, sam2_variant,
                            )
                            if existing is not None:
                                cached_prompt = (
                                    existing["prompt_x"], existing["prompt_y"],
                                    existing["prompt_w"], existing["prompt_h"],
                                )
                                if (existing["detector_model"]
                                        == entry["detector_model"]
                                        and cached_prompt == entry["prompt"]
                                        and existing["path"]
                                        and os.path.isfile(existing["path"])):
                                    # The cheap skip (re-activate only) is correct
                                    # ONLY when the photos row is already fully
                                    # consistent for this variant. set_active_mask_
                                    # variant denormalises this variant's mask
                                    # features, but it does NOT touch the
                                    # dino_* embeddings — those still describe
                                    # whatever mask was active when they were last
                                    # computed. If the row is currently active on a
                                    # different SAM variant (e.g. two workspaces
                                    # share a folder but use sam2-small vs -large),
                                    # re-activating would leave the denormalised
                                    # mask features describing this variant while
                                    # the subject embedding was cropped from the
                                    # other variant's mask. regroup reads both off
                                    # the photos row, so it would mix them. Only
                                    # skip when active_mask_variant AND
                                    # dino_embedding_variant already match; else
                                    # fall through to the full recompute, which
                                    # writes set_active_mask_variant +
                                    # update_photo_embeddings together.
                                    state = thread_db.conn.execute(
                                        "SELECT active_mask_variant, "
                                        "dino_embedding_variant FROM photos "
                                        "WHERE id = ?",
                                        (photo_id,),
                                    ).fetchone()
                                    if (state is not None
                                            and state["active_mask_variant"]
                                            == sam2_variant
                                            and state["dino_embedding_variant"]
                                            == dinov2_variant):
                                        masked += 1
                                        processed = i + 1
                                        continue

                            # First true cache miss: ensure SAM2 + DINOv2 weights
                            # are present before render_proxy/generate_mask runs.
                            # No-op on subsequent iterations.
                            _ensure_weights()

                            proxy = render_proxy(image_path, longest_edge=proxy_longest_edge)
                            if proxy is None:
                                skipped += 1
                                processed = i + 1
                                continue
                            if _should_abort(abort):
                                break

                            # GPU serialisation lives inside masking.generate_mask
                            # (around the encoder/decoder session.run calls). The
                            # wider wrap previously here held the semaphore through
                            # SAM weight load + image preprocessing + prompt-coord
                            # math, blocking other pipelines' GPU work for CPU-only
                            # phases.
                            mask = generate_mask(proxy, det_box, variant=sam2_variant)
                            if mask is None:
                                skipped += 1
                                processed = i + 1
                                continue
                            if _should_abort(abort):
                                break

                            mask_path = save_mask(
                                mask, masks_dir, photo_id, sam2_variant,
                            )
                            completeness = crop_completeness(mask)
                            features = compute_all_quality_features(proxy, mask)
                            if _should_abort(abort):
                                break

                            # Per-mask features (move from photos row into
                            # photo_masks; set_active_mask_variant denormalizes
                            # them back into photos for downstream readers).
                            mask_subject_tenengrad = features.pop(
                                "subject_tenengrad", None,
                            )
                            mask_bg_tenengrad = features.pop("bg_tenengrad", None)
                            # Mask-derived subject_size: fraction of frame
                            # covered by the boolean mask. Replaces the
                            # detection-bbox approximation classify uses.
                            total_pixels = float(mask.size)
                            if total_pixels > 0:
                                mask_subject_size = float(
                                    np.count_nonzero(mask) / total_pixels
                                )
                            else:
                                mask_subject_size = None

                            # GPU serialisation lives inside dino_embed.embed /
                            # embed_batch (around the session.run call). The wider
                            # wrap previously here held the semaphore through
                            # per-image resize/normalize preprocessing.
                            subject_crop = crop_subject(proxy, mask, margin=0.15)
                            if subject_crop is not None:
                                embs = embed_batch(
                                    [subject_crop, proxy], variant=dinov2_variant,
                                )
                                subj_emb_blob = embedding_to_blob(embs[0])
                                global_emb_blob = embedding_to_blob(embs[1])
                            else:
                                subj_emb_blob = None
                                global_emb_blob = embedding_to_blob(
                                    embed(proxy, variant=dinov2_variant),
                                )

                            thread_db.upsert_photo_mask(
                                photo_id=photo_id,
                                variant=sam2_variant,
                                path=mask_path,
                                detector_model=entry["detector_model"],
                                prompt_x=entry["prompt"][0],
                                prompt_y=entry["prompt"][1],
                                prompt_w=entry["prompt"][2],
                                prompt_h=entry["prompt"][3],
                                subject_size=mask_subject_size,
                                subject_tenengrad=mask_subject_tenengrad,
                                bg_tenengrad=mask_bg_tenengrad,
                                crop_complete=completeness,
                            )
                            thread_db.set_active_mask_variant(
                                photo_id, sam2_variant,
                            )
                            # Remaining (non-mask) per-photo features still land
                            # on the photos row.  mask_path / crop_complete /
                            # subject_tenengrad / bg_tenengrad now flow via
                            # set_active_mask_variant above, so they are
                            # intentionally NOT passed here.
                            if features:
                                thread_db.update_photo_pipeline_features(
                                    photo_id, **features,
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

                    processed = i + 1
                    stages["extract_masks"]["count"] = processed
                    stages["extract_masks"]["total"] = total
                    runner.update_step(job["id"], "extract_masks",
                                       progress={"current": processed, "total": total},
                                       error_count=em_failed)
                    _emit_progress(
                        runner, job["id"], stages, "extract_masks",
                        "Extracting features (SAM2 + DINOv2)",
                        rate=round(processed / max(time.time() - start_time, 0.01) * 60, 1),
                    )

                if _should_abort(abort):
                    # Distinguish a user cancel from a clean completion: pin a
                    # "Cancelled" summary on the final step update so the job
                    # tree doesn't report a half-done stage as if it ran to
                    # term. Mirrors the classify-cancel path PR #710 added.
                    stages["extract_masks"]["status"] = "completed"
                    em_summary = (
                        f"Cancelled ({processed} of {total} processed)"
                        if total else "Cancelled"
                    )
                    runner.update_step(
                        job["id"], "extract_masks", status="completed",
                        progress={"current": processed, "total": total},
                        summary=em_summary,
                        error_count=em_failed,
                    )
                    result["stages"]["extract_masks"] = {
                        "masked": masked, "skipped": skipped, "failed": em_failed,
                        "total": total, "cancelled": True,
                    }
                else:
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
                    if photos_subthreshold_only > 0:
                        em_summary_parts.append(
                            f"{photos_subthreshold_only} below detector_confidence "
                            f"({detector_confidence})"
                        )
                    runner.update_step(job["id"], "extract_masks", status=final_status,
                                       summary=", ".join(em_summary_parts),
                                       error_count=em_failed,
                                       error=em_rollup)
                    result["stages"]["extract_masks"] = {
                        "masked": masked, "skipped": skipped, "failed": em_failed,
                        "total": total, "subthreshold": photos_subthreshold_only,
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
            if (
                params.skip_eye_keypoints
                or params.skip_extract_masks
                or abort.is_set()
                or not collection_id
            ):
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
                photos_for_stage = thread_db.list_photos_for_eye_keypoint_stage(
                    photo_ids=collection_photo_ids,
                )
                # Defensive second filter: when collection_photo_ids is None
                # (whole-workspace path) the DB query above returned every
                # eligible row, so excluded IDs would otherwise still influence
                # the download planner below and trigger weights for variants no
                # included photo routes to.
                if params.exclude_photo_ids:
                    photos_for_stage = [
                        p for p in photos_for_stage
                        if p["id"] not in params.exclude_photo_ids
                    ]
                total = len(photos_for_stage)
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
                    _emit_progress(
                        runner, job["id"], stages, "eye_keypoints", phase,
                        rate=round(
                            current / max(time.time() - start_time, 0.01) * 60, 1
                        ),
                    )

                # Auto-download SuperAnimal weights on first pipeline run.
                # Mirrors the SAM2/DINOv2 auto-download pattern in extract_masks
                # (commit 90cd0f9): without this, every photo silently skips on
                # a fresh install. Only fetch variants the per-photo router
                # would actually pick — a collection of out-of-scope classes
                # (fish/reptiles/invertebrates) shouldn't pay the bandwidth
                # cost for weights that will never be used.
                if total > 0:
                    import keypoints as kp
                    from pipeline import _resolve_keypoint_model

                    # Mirror Gate 1 in _process_photo_for_eye: rows whose
                    # classifier confidence is below eye_classifier_conf_gate
                    # get skipped at run time, so they shouldn't influence
                    # which variants get downloaded — otherwise an all-low-
                    # confidence collection still pays the bandwidth cost
                    # for weights no photo can reach.
                    conf_gate = pipeline_cfg.get(
                        "eye_classifier_conf_gate", 0.5,
                    )
                    needed_models = []
                    for row in photos_for_stage:
                        if (row.get("species_conf") or 0.0) < conf_gate:
                            continue
                        model_name = _resolve_keypoint_model(thread_db, row)
                        if model_name and model_name not in needed_models:
                            needed_models.append(model_name)

                    # Use a separate download-progress callback so a cancel
                    # during/just after weight download doesn't leak the
                    # download counter into `processed['count']` (which would
                    # surface as e.g. "Cancelled (1 of N processed)" before any
                    # photo has actually been touched).
                    def _dl_progress(phase, current, total_steps):
                        _emit_progress(
                            runner, job["id"], stages, "eye_keypoints", phase,
                        )

                    # Preserve a stable order (quadruped, bird) for tests and
                    # log readability when both variants are needed. Re-check
                    # abort between models so a cancel that arrives after the
                    # first weights download can short-circuit the second
                    # multi-hundred-MB fetch instead of forcing the user to
                    # wait through it.
                    #
                    # Eye Keypoints is an optional stage: a transient HF /
                    # network failure must degrade to a skipped stage, not a
                    # hard pipeline failure. Without this guard the RuntimeError
                    # raised by ensure_keypoint_weights bubbles to the outer
                    # except, marks the stage 'failed', and tanks the whole
                    # run for a first-run/offline user who never opted into
                    # eye keypoints in the first place.
                    try:
                        for kp_model in (
                            "superanimal-quadruped", "superanimal-bird",
                        ):
                            if _should_abort(abort):
                                break
                            if kp_model in needed_models:
                                kp.ensure_keypoint_weights(
                                    kp_model, progress_callback=_dl_progress,
                                )
                    except Exception as dl_err:
                        log.warning(
                            "Eye keypoints stage skipped — weight download "
                            "failed: %s", dl_err,
                        )
                        errors.append(f"[eye_keypoints] {dl_err}")
                        stages["eye_keypoints"]["status"] = "skipped"
                        runner.update_step(
                            job["id"], "eye_keypoints",
                            status="completed",
                            summary=(
                                f"Skipped — failed to download keypoint "
                                f"weights: {dl_err}"
                            ),
                        )
                        result["stages"]["eye_keypoints"] = {
                            "processed": 0, "total": total,
                            "skipped": "weight_download_failed",
                        }
                        _update_stages(runner, job["id"], stages)
                        return

                detect_eye_keypoints_stage(
                    thread_db, config=pipeline_cfg, progress_callback=_progress,
                    collection_id=collection_id,
                    exclude_photo_ids=params.exclude_photo_ids,
                    abort_check=lambda: _should_abort(abort),
                )

                stages["eye_keypoints"]["status"] = "completed"
                if _should_abort(abort):
                    # Match the classify- and extract_masks-cancel summaries so
                    # the job tree distinguishes a user cancel from a clean
                    # finish that happened to process the same count.
                    summary = (
                        f"Cancelled ({processed['count']} of {total} processed)"
                        if total else "Cancelled"
                    )
                    runner.update_step(
                        job["id"], "eye_keypoints",
                        status="completed", summary=summary,
                    )
                    result["stages"]["eye_keypoints"] = {
                        "processed": processed["count"], "total": total,
                        "cancelled": True,
                    }
                else:
                    summary = (
                        f"{processed['count']} of {total} photos processed"
                        if total else "No eligible photos"
                    )
                    runner.update_step(
                        job["id"], "eye_keypoints",
                        status="completed", summary=summary,
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

            # The per-workspace regroup lock is now acquired by the
            # orchestrator (see run_pipeline_job body) so it spans BOTH
            # regroup_stage and miss_stage atomically — the inner lock
            # that used to live here would deadlock against the outer one
            # (Python locks aren't reentrant). The deferred-update pattern
            # likewise goes away: runner.update_step is fine to call here
            # because nothing under JobRunner._lock acquires the workspace
            # regroup lock, so there's no cycle to invert.
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
                    runner.update_step(
                        job["id"], "regroup",
                        status="completed", summary="No photos to group",
                    )
                else:
                    results = run_full_pipeline(photos, config=pipeline_cfg, emit_trace=True)
                    cache_dir = os.path.dirname(db_path)
                    save_results(results, cache_dir, workspace_id)

                    # Stamp the grouping fingerprint + timestamp BEFORE marking
                    # the step completed, so a partial regroup that crashes
                    # between here and update_step doesn't end up labeled "fresh"
                    # with a stale fp.
                    #
                    # Only stamp when the regroup actually covered the whole
                    # workspace — if it ran on a filtered subset (a
                    # sub-collection, or with exclude_photo_ids set) some
                    # workspace photos were intentionally not regrouped, so
                    # claiming workspace-level freshness would let the pipeline
                    # page hide a real stale state.
                    from pipeline import (
                        _resolve_collection_photo_ids,
                        compute_group_fingerprint,
                    )
                    collection_photo_ids = _resolve_collection_photo_ids(
                        thread_db, collection_id,
                    )
                    ws_photo_ids = {
                        r["id"] for r in thread_db.conn.execute(
                            """SELECT p.id
                                 FROM photos p
                                 JOIN workspace_folders wf
                                   ON wf.folder_id = p.folder_id
                                WHERE wf.workspace_id = ?""",
                            (workspace_id,),
                        ).fetchall()
                    }
                    covered_full_workspace = (
                        not params.exclude_photo_ids
                        and ws_photo_ids.issubset(collection_photo_ids)
                    )
                    if covered_full_workspace:
                        thread_db.set_workspace_group_state(
                            workspace_id=workspace_id,
                            fingerprint=compute_group_fingerprint(effective_cfg),
                            when_ts=int(time.time()),
                        )
                    else:
                        # Partial run — save_results just clobbered
                        # pipeline_results_ws*.json with subset output, so any
                        # pre-existing fingerprint now points at a cache that
                        # no longer reflects the full workspace. Invalidate so
                        # the pipeline page surfaces the staleness as will-run
                        # instead of falsely reporting done-prior.
                        thread_db.set_workspace_group_state(
                            workspace_id=workspace_id,
                            fingerprint=None,
                            when_ts=None,
                        )

                    stages["regroup"]["status"] = "completed"
                    summary_info = results.get("summary", {})
                    groups = summary_info.get("groups", "")
                    runner.update_step(
                        job["id"], "regroup",
                        status="completed",
                        summary=f"{groups} groups" if groups else "Done",
                    )
                    result["stages"]["regroup"] = summary_info
            except Exception as e:
                errors.append(f"[regroup] Fatal: {e}")
                log.exception("Pipeline regroup stage failed")
                stages["regroup"]["status"] = "failed"
                runner.update_step(
                    job["id"], "regroup", status="failed", error=str(e),
                )
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
            #
            # Also skip when regroup failed: miss classification depends on
            # regroup's burst_id output, so running here after a regroup
            # failure would overwrite miss_* flags with stale context during
            # an already-failing job. regroup_stage marks itself "failed"
            # without setting abort, so check the stage status explicitly.
            if (
                params.skip_regroup
                or params.skip_classify
                or abort.is_set()
                or not collection_id
                or stages["regroup"].get("status") == "failed"
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

        def archive_stage():
            """Move a local-processing staging folder to the final destination."""
            if not params.local_processing:
                return

            def _deindex_staging():
                # scanner_stage may have already registered the staging folder
                # and its photos' hashes before we got here. Without removing
                # those rows a retry of the same source would hit ingest()'s
                # known-hash skip and copy nothing — the user would then
                # "successfully" archive an empty destination while the
                # original files only ever existed in the abandoned staging
                # tree. Leave the on-disk staging files in place so the user
                # can still recover them.
                #
                # Cached thumbnails/previews/working copies for the staged
                # photos also have to go: the FK cascade clears preview_cache
                # rows but leaves the on-disk ``{photo_id}.jpg`` files. SQLite
                # reuses freed rowids, so a retry that lands on one of the
                # abandoned staging photo IDs would treat the stale cache file
                # as a valid thumbnail and skip regenerating it.
                try:
                    from preview_cache import (
                        cleanup_cached_files_for_deleted_photos,
                    )

                    thread_db = Database(db_path)
                    thread_db.set_active_workspace(workspace_id)
                    folder = thread_db.conn.execute(
                        "SELECT id FROM folders WHERE path = ?",
                        (params.destination,),
                    ).fetchone()
                    if folder:
                        result = thread_db.delete_folder(folder["id"])
                        cleanup_cached_files_for_deleted_photos(
                            effective_thumb_cache_dir,
                            result.get("files", []),
                        )
                except Exception:
                    log.exception(
                        "Failed to deindex local staging folder on archive skip",
                    )

            if abort.is_set() or runner.is_cancelled(job["id"]):
                # Fatal upstream stages (partial scan, thumbnail setup, model
                # load, detect, classify) set abort and fall through to here
                # without populating stages[*]["status"] == "failed", so the
                # already_failed branch below would miss them. Deindex here too
                # — otherwise the next retry of the same source would treat
                # every file as a duplicate and publish an empty archive.
                _deindex_staging()
                stages["archive"]["status"] = "skipped"
                runner.update_step(
                    job["id"], "archive", status="completed", summary="Skipped",
                )
                _update_stages(runner, job["id"], stages)
                return
            if not final_destination:
                stages["archive"]["status"] = "skipped"
                runner.update_step(
                    job["id"], "archive", status="completed", summary="Skipped",
                )
                _update_stages(runner, job["id"], stages)
                return
            # Don't publish partial results: previews, extract_masks,
            # eye_keypoints, regroup, and miss can all fail without setting
            # abort, and run_pipeline_job marks the whole job failed at the
            # end whenever any stage status is "failed". If we ran the archive
            # in between, the staged folder would have already moved to
            # final_destination by the time that failure was raised — leaving
            # the user with a published archive whose pipeline never finished.
            # Skip instead so staging stays intact and the failure is visible.
            already_failed = [
                name for name, s in stages.items() if s.get("status") == "failed"
            ]
            if already_failed:
                _deindex_staging()
                stages["archive"]["status"] = "skipped"
                runner.update_step(
                    job["id"], "archive",
                    status="completed",
                    summary=f"Skipped ({already_failed[0]} failed)",
                )
                _update_stages(runner, job["id"], stages)
                return

            stages["archive"]["status"] = "running"
            runner.update_step(job["id"], "archive", status="running")
            _update_stages(runner, job["id"], stages)

            try:
                import config as cfg
                from move import move_folder

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)
                folder = thread_db.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (params.destination,)
                ).fetchone()
                if not folder:
                    raise RuntimeError(
                        f"local staging folder was not indexed: {params.destination}"
                    )

                effective_cfg = thread_db.get_effective_config(cfg.load())
                developed_dir = effective_cfg.get("darktable_output_dir", "") or ""
                archive_parent = os.path.dirname(os.path.normpath(final_destination))
                total_files = {"value": 0}

                def archive_cb(current, total, filename, phase=None):
                    total_files["value"] = max(total_files["value"], total or 0)
                    stages["archive"]["count"] = current
                    stages["archive"]["total"] = total
                    runner.update_step(
                        job["id"], "archive",
                        current_file=filename,
                        progress={"current": current, "total": total},
                    )
                    _emit_progress(
                        runner, job["id"], stages, "archive",
                        phase or "Archiving photos",
                        current_file=filename,
                    )

                # Consume any pending cancellation and lock the job uncancellable
                # BEFORE the move begins. move_folder is an uninterruptible commit
                # step (copy → verify → repoint catalog → delete originals); we
                # can't tear it down mid-flight without leaving partial state.
                # Clearing AFTER move_folder returns leaves a window where a Stop
                # press landing during the move would survive into the failure
                # path: if move_folder raises (ENOSPC, rsync error, verify
                # mismatch), runner.is_cancelled(job_id) would still be True at
                # the outer job-terminalization check, JobRunner would record
                # "cancelled", and the user would never see the archive failure.
                # Consume up front so a successful commit reports "completed" AND
                # a failed commit reports "failed" — both win against the racing
                # Stop press, which couldn't have been honored anyway.
                runner.clear_cancellation(job["id"])

                move_result = move_folder(
                    thread_db,
                    folder["id"],
                    archive_parent,
                    progress_cb=archive_cb,
                    developed_dir=developed_dir,
                    merge=True,
                )
                if move_result.get("errors"):
                    raise RuntimeError("; ".join(move_result["errors"]))

                if staging_parent:
                    with contextlib.suppress(OSError):
                        os.rmdir(staging_parent)

                # move_folder repointed the catalog at final_destination
                # before deleting the source originals, so a
                # ``cleanup_error`` means the archive IS committed — files
                # are at the destination, but some leftovers remain in
                # staging (locked file, permission issue, etc.). Report
                # success with a warning rather than failure: the
                # alternative ("failed" + "results remain in staging") tells
                # the user their data is lost when it's actually safe at
                # final_destination, and would also leave the freshly
                # created tracked folder row in the catalog while we tried
                # to deindex a staging row that move_folder_path already
                # renamed away.
                cleanup_error = move_result.get("cleanup_error")
                stages["archive"]["status"] = "completed"
                moved = move_result.get("moved", 0)
                if cleanup_error:
                    summary = (
                        f"{moved} photos archived "
                        f"(staging cleanup failed: {cleanup_error})"
                    )
                    errors.append(
                        f"[archive] Warning: staging cleanup failed after "
                        f"commit — leftover files in {params.destination}: "
                        f"{cleanup_error}"
                    )
                else:
                    summary = f"{moved} photos archived"
                runner.update_step(
                    job["id"], "archive", status="completed", summary=summary,
                )
                result["archive"] = {
                    "final_destination": final_destination,
                    "moved": moved,
                }
                if cleanup_error:
                    result["archive"]["cleanup_error"] = cleanup_error
            except Exception as e:
                msg = (
                    f"{e}. Processing results remain in local staging: "
                    f"{params.destination}"
                )
                errors.append(f"[archive] Fatal: {msg}")
                log.exception("Pipeline archive stage failed")
                # move_folder doesn't repoint the catalog until it has
                # verified every copied file, so a raise here means the
                # folders/photos rows still point at the staging tree. If we
                # leave them indexed, a retry of the same source sees the
                # staging hashes via ingest()'s global duplicate check, skips
                # the copy, and "successfully" publishes an empty archive
                # while the original files remain only under ~/.vireo/staging.
                # Deindex to match the archive-skip paths above; the on-disk
                # staging files are left in place per the error message so
                # the user can still recover them manually.
                _deindex_staging()
                stages["archive"]["status"] = "failed"
                runner.update_step(
                    job["id"], "archive", status="failed", error=msg,
                )

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

        # Phase 1.5: previews (needs scan complete, runs before classify).
        #
        # This and every later stage are always invoked — even when `abort`
        # is set — so their step rows reach a terminal status. Gating the
        # call on abort would leave the runner.set_steps-created rows
        # persisted as "pending" with no finished_at, forever. Each stage
        # checks abort internally and marks itself "Skipped".
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
        extract_masks_stage()

        # Phase 3.5: eye keypoints (needs masks + classifier output). No-op when
        # SuperAnimal weights are absent — users opt in on the pipeline models
        # card. Per-photo failures log and continue rather than abort the stage.
        eye_keypoints_stage()

        # Phases 4 + 5: regroup and miss detection. Held under the
        # per-workspace regroup lock TOGETHER so a concurrent same-workspace
        # pipeline can't slip a regroup_stage in between this run's
        # regroup_stage and its miss_stage — that would leave the persisted
        # miss flags + ``miss_computed_at`` paired with a grouping
        # (burst_id / pipeline_results_ws*.json) the miss computation never
        # saw. Pipelines targeting different workspaces share neither
        # stage's state and don't contend here.
        #
        # miss_stage's own gate covers the regroup-failed and abort cases, so
        # both stages can be invoked unconditionally and still reach a
        # terminal step status.
        if abort.is_set():
            # Both stages early-return as "Skipped" without touching grouping
            # state, so the lock isn't needed — and skipping the calls would
            # leave their step rows pending forever. Staying outside the lock
            # also keeps an aborted/cancelled run from blocking behind a
            # concurrent pipeline's regroup.
            regroup_stage()
            miss_stage()
        else:
            with acquire_workspace_regroup(workspace_id):
                regroup_stage()
                miss_stage()

        archive_stage()

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
    finally:
        if archive_destination_reserved:
            release_archive_destination(final_destination)
