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

import numpy as np
from db import Database, commit_with_retry
from job_contract import progress_event
from model_cache import get_default_cache
from pipeline_locks import (
    acquire_photo_mask,
    acquire_workspace_regroup,
    release_archive_destination,
    try_reserve_archive_destination,
)
from render_source import (
    companion_image_can_replace_raw_result as _companion_image_can_replace_raw_result,
)
from render_source import (
    has_current_working_copy_failure as _has_current_working_copy_failure,
)
from render_source import (
    image_is_smaller_than_expected as _image_is_smaller_than_expected,
)
from render_source import (
    photo_value as _photo_value,
)
from render_source import (
    recipe_render_source,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from render_source import (
    scaled_recipe_source_dimensions as _scaled_recipe_source_dimensions,
)
from render_source import (
    working_copy_path_if_satisfies as _working_copy_path_if_satisfies,
)

log = logging.getLogger(__name__)

_SENTINEL = object()  # unique end-of-stream marker


def _missing_archive_mount_root(path: str) -> str | None:
    """Return a likely missing mount root that must not be auto-created."""
    def _candidate(posix_path: str) -> str | None:
        parts = posix_path.split("/")
        if len(parts) >= 3 and parts[0] == "" and parts[1] in {"Volumes", "mnt"}:
            return f"/{parts[1]}/{parts[2]}"
        if len(parts) >= 4 and parts[0] == "" and parts[1] == "media":
            return f"/media/{parts[2]}/{parts[3]}"
        return None

    raw_posix = os.path.expanduser(path).replace("\\", "/")
    normalized = os.path.normpath(os.path.abspath(os.path.expanduser(path)))
    normalized_posix = normalized.replace("\\", "/")

    for mount_root in (_candidate(raw_posix), _candidate(normalized_posix)):
        if mount_root and not os.path.lexists(mount_root):
            return mount_root
    return None


@dataclass
class PipelineParams:
    """Parameters for a streaming pipeline job."""

    collection_id: int | None = None
    source: str | None = None
    sources: list | None = None
    source_snapshot_id: int | None = None
    destination: str | None = None
    local_processing: bool = False
    # Remote (SSH) archive destination for local-processing runs: the id of a
    # saved remote target (config remote_targets) plus a required relative
    # subpath naming the archive folder under the target's base paths.
    # Mutually exclusive with ``destination``; resolved via
    # ``resolve_remote_archive``. The staged tree is rsynced over SSH to
    # ``remote_path/subpath`` and the catalog is repointed at
    # ``mount_path/subpath``, mirroring the Move page's remote folder moves.
    remote_target_id: str | None = None
    remote_subpath: str = ""
    # Snapshot of the resolved remote target dict (from cfg.get_remote_target)
    # captured at ENQUEUE time so a queued run archives to the destination the
    # user saw when they clicked Start, not whatever the saved target got edited
    # to before the pipeline slot opened. The API always populates this
    # alongside ``remote_target_id``; when it is None, ``run_pipeline_job``
    # falls back to re-reading the mutable target (mostly for direct-call
    # tests). Mirrors how the move-folder endpoint builds its remote spec
    # before enqueueing.
    remote_target_snapshot: dict | None = None
    file_types: str = "both"
    folder_template: str = "%Y/%Y-%m-%d"
    skip_duplicates: bool = True
    # Identify duplicates by content hash alone (reads every byte of every
    # source file). Default False: metadata-first matching with a hash
    # fallback — see import_dedup.
    verify_by_hash: bool = False
    labels_file: str | None = None
    labels_files: list | None = None
    model_id: str | None = None
    model_ids: list | None = None
    reclassify: bool = False
    skip_extract_masks: bool = False
    skip_regroup: bool = False
    # Distinguishes the identify preset's species-only review from a
    # generic ``skip_regroup=True`` run. Only set to ``"species"`` by
    # ``process_strategies.identify`` — Advanced/Custom on the Process
    # page and API clients sending ``skip_regroup: true`` without a
    # strategy leave this ``None`` so regroup_stage skips cleanly instead
    # of overwriting the workspace cache with all-REVIEW output.
    review_mode: str | None = None
    skip_classify: bool = False
    skip_eye_keypoints: bool = False
    # Per-run override for the config-gated eye-detect setting. Semantics
    # match miss_enabled: None defers to the workspace-effective
    # ``pipeline.eye_detect_enabled``, a bool wins over workspace config in
    # both directions. Set to True by the Process page when the user
    # explicitly checks the Eye Keypoints stage box — that box is a
    # per-run opt-in that must override the (default-off) Settings value
    # so preflight and scoring see the enabled state. Left None by
    # strategy expansion (the saved-process flag expansion) so a
    # ``full`` strategy chain from after-import respects the user's
    # Settings default instead of silently forcing eye detection on.
    eye_detect_override: bool | None = None
    # Per-run override for the config-gated misses stage. None defers to the
    # workspace-effective ``pipeline.miss_enabled`` (today's behavior); a
    # bool wins over workspace config in BOTH directions, mirroring how the
    # skip_* flags override workspace defaults. Process strategies
    # (process_strategies.py) set this so e.g. cull_ready suppresses misses
    # on a workspace that has them enabled.
    miss_enabled: bool | None = None
    download_taxonomy: bool = True
    # None means "use the workspace-effective preview_max_size setting".
    # Explicit values are kept for API/back-compat and tests that need to pin
    # a preview tier.
    preview_max_size: int | None = None
    exclude_paths: set | None = None
    exclude_photo_ids: set | None = None
    recursive: bool = True


def resolve_remote_archive(target, subpath):
    """Resolve a saved remote target + subpath into the pipeline's
    remote-archive context.

    ``target`` is a validated dict from ``config.get_remote_target``.
    ``subpath`` names the archive folder under BOTH base paths — it is
    required (unlike the Move page, where the moved folder's own name
    provides the landing leaf) because ``move_folder`` lands the staged
    folder inside a parent keeping its name: the subpath's last segment is
    the staging root's name, so the archive lands at exactly
    ``remote_path/subpath`` over SSH while the catalog is repointed at
    exactly ``mount_path/subpath``.

    Raises ValueError with a user-facing message when the pieces can't form
    a safe archive destination. Returns a dict:

    * ``target`` — the target passed in.
    * ``subpath`` — the sanitized relative subpath.
    * ``parent_subpath`` — subpath minus its last segment ("" for a single
      segment); feed this to ``build_remote_move_spec`` so the staged leaf
      lands at the full subpath.
    * ``ssh_final`` — NAS-side path the archive lands at.
    * ``mount_final`` — local mount path the catalog points at afterward.
    * ``display`` — ``user@host:ssh_final`` for messages/UI.
    """
    import posixpath

    from move import rsync_dest_spec, sanitize_subpath

    sub = sanitize_subpath(subpath)  # raises ValueError on absolute / '..'
    if not sub:
        raise ValueError(
            "remote_subpath is required — it names the archive folder under "
            "the remote target's base path (e.g. \"2026/kenya-trip\")."
        )
    mount_path = (target.get("mount_path") or "").strip()
    if not mount_path:
        raise ValueError(
            "This remote target has no local mount path, so archived photos "
            "couldn't stay in your library. Add a mount path under "
            "Settings → Remote targets."
        )
    if not os.path.isabs(mount_path):
        raise ValueError(
            "This remote target's local mount path isn't absolute "
            f"(\"{mount_path}\"). Archived photos would be repointed to a "
            "path relative to the server's working directory and appear "
            "missing. Set an absolute mount path under Settings → Remote "
            "targets."
        )
    ssh_final = posixpath.join(target["remote_path"], sub)
    mount_final = os.path.join(mount_path, *sub.split("/"))
    return {
        "target": target,
        "subpath": sub,
        "parent_subpath": posixpath.dirname(sub),
        "ssh_final": ssh_final,
        "mount_final": mount_final,
        "display": rsync_dest_spec(target, ssh_final),
    }


def _should_abort(abort_event):
    """Check if the pipeline should abort."""
    return abort_event.is_set()


def _recipe_render_source(photo, recipe, max_size, vireo_dir, folders):
    """Thin wrapper around the shared resolver, returning just the path.

    Pipeline callers don't need the ``using_working_copy`` flag, so the second
    element of :func:`render_source.recipe_render_source` is dropped.
    """
    return recipe_render_source(photo, recipe, max_size, vireo_dir, folders)[0]


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
    if recipe:
        recipe_kwargs["native_size"] = (
            _recipe_source_dimensions(photo)
        )
    return generate_thumbnail(
        photo_id,
        companion_abs,
        cache_dir,
        size=thumb_size,
        **recipe_kwargs,
    )


def _retry_thumbnail_with_working_copy(
    thread_db, generate_thumbnail, photo, photo_id, raw_source_path,
    cache_dir, thumb_size, recipe, vireo_dir,
):
    """Retry an edited RAW thumbnail from a near-full local JPEG copy."""
    if not photo or not recipe or not vireo_dir:
        return None
    if os.path.splitext(raw_source_path or "")[1].lower() not in _RAW_EXTENSIONS:
        return None
    wc_path = _working_copy_path_if_satisfies(
        photo, recipe, thumb_size, vireo_dir, thumbnail_tolerance=True,
    )
    if not wc_path or os.path.abspath(wc_path) == os.path.abspath(raw_source_path):
        return None
    log.info(
        "Pipeline thumbnail RAW decode failed for photo %s; "
        "falling back to near-full JPEG working copy",
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
    return generate_thumbnail(
        photo_id,
        wc_path,
        cache_dir,
        size=thumb_size,
        recipe=recipe,
        native_size=_recipe_source_dimensions(photo),
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
    data = progress_event(
        phase,
        current,
        total,
        stage_id=stage_id,
        stages={k: dict(v) for k, v in stages.items()},
        # JobRunner.push_event merges progress payloads into job["progress"]
        # rather than replacing them, so a sub-phase (e.g. "Extracting metadata"
        # with phase_current/phase_total set) would otherwise linger through
        # every later stage that omits these keys and keep the /api/jobs
        # poll — and thus the jobs page + navbar sub-progress bar — rendering
        # a stale phase. Default the triple to None here so callers with no
        # active sub-phase actively clear it; the update() below lets callers
        # with a real sub-phase override.
        phase_current=None,
        phase_total=None,
        phase_label=None,
    )
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
                     thumb_cache_dir=None,
                     missing_originals_invalidator=None):
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
        missing_originals_invalidator: optional zero-arg callable that
            drops the Flask app's Missing Originals cache for this DB.
            Called after every scanned root in the finally block, mirroring
            api_job_scan / api_job_import_full so a pipeline scan that
            touches disk doesn't leave GET /api/photos/missing serving a
            pre-scan ghost list.

    Returns:
        dict with stage results, duration, and errors
    """
    job["_start_time"] = time.time()
    abort = threading.Event()
    errors = job["errors"]  # shared list, append is thread-safe
    if params.destination or params.local_processing or params.remote_target_id:
        raise RuntimeError(
            "Pipeline import/archive mode has been removed. Use the Import "
            "page or /api/jobs/import-photos to copy photos into the archive, "
            "then run Process on the imported workspace photos."
        )

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
    remote_archive = None
    if params.local_processing and params.remote_target_id:
        # Prefer the snapshot captured at enqueue time so a settings edit
        # between click-Start and slot-open cannot redirect the archive to a
        # different host/mount than the jobs panel is showing. The Settings
        # fallback is a last resort for callers (mainly tests) that build
        # PipelineParams by hand without pre-resolving the target.
        target = params.remote_target_snapshot
        if not target:
            import config as _cfg_mod

            target = _cfg_mod.get_remote_target(params.remote_target_id)
        if not target:
            raise RuntimeError(
                f"Remote target '{params.remote_target_id}' not found — it "
                "may have been removed from Settings after this job was "
                "queued. Pick a saved remote target and retry."
            )
        try:
            remote_archive = resolve_remote_archive(
                target, params.remote_subpath,
            )
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        # Everything below that keys off final_destination locally — the
        # in-flight destination reservation, the tracked-destination
        # preflight, the staging-root name — cares about where the CATALOG
        # will point after the archive, which for a remote destination is
        # the target's local mount path, not the NAS-side path.
        final_destination = remote_archive["mount_final"]
    archive_destination_reserved = False
    if params.local_processing and final_destination:
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

        # final_destination (not params.destination, which is None for a
        # remote archive): the staging root's basename is the leaf the
        # archive move lands at, and for remote that's the mount-path leaf —
        # the same last-subpath-segment as the NAS side.
        params.destination = staging_root(
            effective_vireo_dir, job["id"], final_destination,
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
        elif not params.skip_classify:
            step_defs.append({"id": "regroup", "label": "Prepare review"})
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
                from scanner import ScanCancelled
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

                def status_cb(message, phase_current=None, phase_total=None, phase_label=None):
                    runner.update_step(job["id"], "scan", current_file=message)
                    extra = {"current_file": message}
                    if phase_current is not None or phase_total is not None:
                        extra.update({
                            "phase_current": phase_current,
                            "phase_total": phase_total,
                            "phase_label": phase_label,
                        })
                    _emit_progress(
                        runner, job["id"], stages, "scan",
                        phase_label or message, **extra,
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
                        # Track the repair folder so the outer finally
                        # invalidates the Missing Originals cache for it —
                        # scanner.scan touches the folder on disk and can
                        # revalidate a restored original that a ready
                        # /api/photos/missing payload still lists as a
                        # ghost. Matches the append pattern used by the
                        # normal ingest/scan-in-place paths below.
                        scanned_roots.append(folder_path)
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
                            if isinstance(e, ScanCancelled) and (
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

                    from import_dedup import CatalogIndex, DuplicateChecker
                    from ingest import ingest as do_ingest

                    # Duplicate-oracle infrastructure shared by the
                    # local-processing preflight and the ingest loop. The
                    # catalog index is loaded once; every prediction pass
                    # gets a FRESH checker over it (seen-state must not
                    # leak between predictions or into the real ingest),
                    # while the shared times cache keeps each source
                    # file's EXIF header read to once per run.
                    dedup_times_cache: dict = {}
                    catalog_index = None
                    if params.skip_duplicates:
                        catalog_index = CatalogIndex.from_db(thread_db)

                    def _fresh_checker():
                        if catalog_index is None:
                            return None
                        return DuplicateChecker(
                            catalog_index,
                            verify_by_hash=params.verify_by_hash,
                            times_cache=dedup_times_cache,
                        )

                    if params.local_processing:
                        from local_processing import (
                            archive_conflict_report,
                            existing_archive_bytes,
                            format_bytes,
                            non_duplicate_files,
                            selected_source_files,
                            storage_plan,
                            total_file_bytes,
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
                            if remote_archive is not None:
                                import move as move_mod

                                # Refuse BEFORE staging/processing hours of
                                # work, in the same spirit as the local
                                # archive-parent checks below: a missing GNU
                                # rsync or an unreachable target would
                                # otherwise only surface at the final archive
                                # move, stranding processed results in
                                # staging.
                                rsync_bin = move_mod.resolve_rsync_bin(
                                    effective_cfg.get("rsync_bin", "") or "",
                                )
                                if rsync_bin and not move_mod.is_gnu_rsync(
                                    rsync_bin,
                                ):
                                    rsync_bin = ""
                                if not rsync_bin:
                                    _bail_storage(
                                        "No usable GNU rsync was found for the "
                                        "remote archive. Install GNU rsync for "
                                        "your platform or set its executable "
                                        "under Settings → Paths."
                                    )
                                    return
                                conn = move_mod.test_remote_connection(
                                    remote_archive["target"], rsync_bin,
                                )
                                if not conn.get("ok"):
                                    _bail_storage(
                                        "Remote archive target "
                                        f"'{remote_archive['target']['name']}'"
                                        f" ({remote_archive['display']}) "
                                        "isn't usable: "
                                        f"{conn.get('message') or 'connection test failed'}"
                                    )
                                    return

                            # A tracked archive destination (the import lands at
                            # or inside a folder Vireo already manages) is no
                            # longer a hard failure: the archive move opts into
                            # merging (allow_tracked_merge=True) and folds the
                            # staged tree into the existing archive. The precise
                            # per-file content-conflict guard below
                            # (conflicting_archive_paths) still refuses any
                            # same-path file whose bytes differ.
                            #
                            # BUT — the merge only supports the "exact overlap"
                            # (destination IS a tracked folder) and the "ancestor
                            # overlap" (destination is INSIDE a tracked folder)
                            # cases. A tracked row STRICTLY BELOW the destination
                            # (e.g. /Photos/USA already tracked and the user
                            # picks /Photos) is the "wrap a fresh parent around
                            # an existing tracked subtree" case which
                            # move_folder refuses even with allow_tracked_merge.
                            # Without an early refuse here the pipeline would
                            # stage and process everything, then fail only at
                            # the archive step and leave processed results
                            # stranded under staging. Mirror move_folder's
                            # alias-folded check so a symlink or case-only alias
                            # of the tracked path is treated as the exact-match
                            # case, not a descendant.
                            from move import (
                                _path_equal_or_descends,
                                _tracked_destination_overlap,
                            )
                            # For a remote archive, final_destination is the
                            # catalog-facing MOUNT path (see where
                            # remote_archive is resolved) — the tracked check
                            # applies there too, because a prior remote
                            # archive to the same target leaves tracked rows
                            # at the mount path and the archive move merges
                            # into (or refuses around) those exactly like a
                            # local destination.
                            preflight_tracked = _tracked_destination_overlap(
                                thread_db, -1, final_destination,
                            )
                            if preflight_tracked and not _path_equal_or_descends(
                                final_destination, preflight_tracked["path"],
                            ):
                                _bail_storage(
                                    f"Archive destination {final_destination} "
                                    "sits above a folder Vireo already manages "
                                    f"({preflight_tracked['path']}). Merging "
                                    "around a tracked subfolder isn't "
                                    "supported. Pick the tracked folder itself "
                                    "or a location outside it."
                                )
                                return

                            if remote_archive is None:
                                # Make sure the archive parent exists NOW. Otherwise
                                # the pipeline would stage and process everything,
                                # then fail at the final move_folder call when rsync
                                # tries to write to a missing parent — leaving the
                                # staged copy stranded under ~/.vireo/staging with no
                                # archive at the final destination. Nested archive
                                # targets like /mnt/nas/NewShoot/Photos are the
                                # common case: the parent /mnt/nas/NewShoot may not
                                # have been created yet by the user.
                                #
                                # All four checks in this branch are
                                # local-filesystem-only. For a remote archive
                                # the destination lives on the NAS: the SSH
                                # connection test above already proved the
                                # remote base is a writable directory, and
                                # move_folder's remote path mkdir-p's the
                                # subpath parents itself. The local mount
                                # path deliberately isn't probed — it may
                                # legitimately be unmounted while archiving
                                # over SSH (that's the point of this mode).
                                archive_parent = os.path.dirname(
                                    os.path.normpath(final_destination),
                                )
                                # Use lexists so a broken/dangling symlink at
                                # final_destination is caught here too. os.path
                                # .exists returns False for a broken symlink, so
                                # a stale link left by an unmounted or moved
                                # archive root would slip through, let the
                                # pipeline stage and process everything, and
                                # only fail when move_folder/rsync tried to
                                # create a directory at a path already occupied
                                # by that symlink entry.
                                if (
                                    os.path.lexists(final_destination)
                                    and not os.path.isdir(final_destination)
                                ):
                                    _bail_storage(
                                        f"Archive destination {final_destination} "
                                        "already exists and is not a directory."
                                    )
                                    return
                                # Existing archive roots can be mounted volumes; new
                                # archive leaves have to probe the existing parent.
                                archive_space_path = (
                                    final_destination
                                    if os.path.exists(final_destination)
                                    else archive_parent
                                )
                                missing_mount_root = (
                                    _missing_archive_mount_root(final_destination)
                                    or _missing_archive_mount_root(archive_parent)
                                )
                                if missing_mount_root:
                                    _bail_storage(
                                        f"Archive mount root {missing_mount_root} "
                                        "is not available. Check that the "
                                        "destination drive is mounted and writable."
                                    )
                                    return
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
                            # When skip_duplicates is on, ingest() will skip
                            # sources that duplicate cataloged photos before
                            # they ever reach staging. Give the conflict
                            # preflight a fresh instance of the same
                            # duplicate oracle so a duplicate-source that
                            # happens to share an archive path with an
                            # unrelated file does not falsely abort the run
                            # — ingest will not copy it, so it cannot
                            # conflict at archive time.

                            from move import _case_insensitive_root

                            def _indexed_archive_paths(root: str) -> set[str]:
                                # Fold symlink/case aliases before deciding a
                                # cataloged row belongs under this destination:
                                # the tracked-destination preflight above
                                # accepts alias-equal roots via
                                # _path_equal_or_descends, so anything less here
                                # would drop indexed rows whose stored path uses
                                # a different alias than the user-picked
                                # destination (symlink target vs. link, or a
                                # case-only twin on case-insensitive POSIX like
                                # default APFS — os.path.normcase is a no-op on
                                # POSIX and os.path.realpath preserves the
                                # supplied spelling, so a lexical
                                # commonpath/is_relative_to check misses the
                                # case-only alias). Dropping the row would then
                                # feed an empty index to
                                # archive_conflict_report and get a
                                # zero-byte/truncated indexed archive file
                                # labelled as unindexed failed-copy debris —
                                # telling the user to remove a cataloged file.
                                # Probe the case-insensitive fold root once and
                                # reuse it per row so
                                # _path_equal_or_descends' listdir/samefile
                                # probe doesn't re-run per catalog folder.
                                root_path = Path(os.path.normpath(root))
                                root_real = os.path.normcase(
                                    os.path.realpath(root),
                                )
                                dest_ci_root = _case_insensitive_root(root)
                                indexed: set[str] = set()
                                rows = thread_db.conn.execute(
                                    """SELECT f.path, p.filename
                                         FROM photos p
                                         JOIN folders f ON f.id = p.folder_id"""
                                ).fetchall()
                                for row in rows:
                                    folder = row["path"]
                                    if not _path_equal_or_descends(
                                        folder, root,
                                        case_insensitive_root=dest_ci_root,
                                    ):
                                        continue
                                    # Extract the below-root portion of the
                                    # folder path so we can rebase onto the
                                    # user-picked root spelling
                                    # (archive_conflict_report joins
                                    # `path`/rel_folder/filename to build the
                                    # dest_key we're matching against). A
                                    # realpath-based string prefix covers the
                                    # same-case and symlink-alias cases; the
                                    # case-fold branch mirrors
                                    # _path_equal_or_descends' probed-CI-root
                                    # logic so a POSIX case-only alias still
                                    # yields the right tail.
                                    folder_real = os.path.normcase(
                                        os.path.realpath(folder),
                                    )
                                    rel_suffix: str | None = None
                                    if folder_real == root_real:
                                        rel_suffix = ""
                                    elif folder_real.startswith(
                                        root_real + os.sep,
                                    ):
                                        rel_suffix = folder_real[
                                            len(root_real) + 1:
                                        ]
                                    elif dest_ci_root:
                                        root_low = root_real.lower()
                                        folder_low = folder_real.lower()
                                        if folder_low == root_low:
                                            rel_suffix = ""
                                        elif folder_low.startswith(
                                            root_low + os.sep,
                                        ):
                                            rel_suffix = folder_real[
                                                len(root_real) + 1:
                                            ]
                                    if rel_suffix is None:
                                        # _path_equal_or_descends accepted the
                                        # row via a samefile walk-up (missing
                                        # intermediate leaf whose parent aliases
                                        # to root), so the realpath spelling
                                        # doesn't line up as a string prefix and
                                        # we can't safely rebase onto the
                                        # user-picked spelling. Catalog folders
                                        # exist on disk by construction — this
                                        # branch is rare — so drop the row
                                        # rather than fabricate a spelling.
                                        continue
                                    indexed_folder = (
                                        root_path if rel_suffix == ""
                                        else root_path.joinpath(
                                            *rel_suffix.split(os.sep),
                                        )
                                    )
                                    indexed.add(
                                        str(indexed_folder / row["filename"]),
                                    )
                                return indexed

                            if remote_archive is None:
                                archive_report = archive_conflict_report(
                                    final_destination,
                                    selected_files,
                                    params.folder_template,
                                    duplicate_checker=_fresh_checker(),
                                    indexed_paths=_indexed_archive_paths(
                                        final_destination,
                                    ),
                                )
                                archive_conflicts = (
                                    archive_report["empty"]
                                    + archive_report["partial"]
                                    + archive_report["conflicts"]
                                )
                            else:
                                # The conflict report walks the destination
                                # tree, which for a remote archive lives on
                                # the NAS and isn't locally walkable. The
                                # archive move itself runs the equivalent
                                # guard over SSH before any file is copied —
                                # move_folder's remote merge path probes with
                                # ``rsync -an --existing --checksum`` and
                                # refuses on any same-path file whose bytes
                                # differ — so a conflict still cancels
                                # cleanly, just at archive time instead of
                                # here.
                                archive_conflicts = []
                            if archive_conflicts:
                                incomplete = (
                                    archive_report["empty"]
                                    + archive_report["partial"]
                                )
                                if incomplete:
                                    # Only surface incomplete-file paths in
                                    # this branch: the message tells the user
                                    # to remove empty/partial debris, so
                                    # mixing full-content conflict paths into
                                    # the example list would point them at
                                    # files that are neither empty nor
                                    # truncated.
                                    incomplete_examples = ", ".join(
                                        incomplete[:3],
                                    )
                                    incomplete_more = (
                                        f" and {len(incomplete) - 3} more"
                                        if len(incomplete) > 3 else ""
                                    )
                                    bits = []
                                    if archive_report["empty"]:
                                        bits.append(
                                            f"{len(archive_report['empty'])} empty"
                                        )
                                    if archive_report["partial"]:
                                        bits.append(
                                            f"{len(archive_report['partial'])} "
                                            "partial"
                                        )
                                    _bail_storage(
                                        "Archive destination contains "
                                        f"{' and '.join(bits)} unindexed file"
                                        f"{'s' if len(incomplete) != 1 else ''} "
                                        "at incoming import paths: "
                                        f"{incomplete_examples}"
                                        f"{incomplete_more}. This looks like "
                                        "an interrupted previous archive "
                                        "copy. Remove or replace those "
                                        "incomplete files, then retry; Vireo "
                                        "will not suffix around likely "
                                        "corrupt archive files."
                                    )
                                    return
                                examples = ", ".join(archive_conflicts[:3])
                                more = (
                                    f" and {len(archive_conflicts) - 3} more"
                                    if len(archive_conflicts) > 3 else ""
                                )
                                _bail_storage(
                                    "Archive destination already contains "
                                    "different files at the same import paths: "
                                    f"{examples}{more}. Pick an empty archive "
                                    "folder, remove the conflicting files, or "
                                    "import without local processing."
                                )
                                return
                            planning_files = selected_files
                            if (
                                params.skip_duplicates
                                and selected_files
                                and catalog_index is not None
                            ):
                                # Plan against the exact files ingest will
                                # stage, not the full selection. This keeps
                                # both source_bytes and resume credit aligned
                                # with skip_duplicates even when the unfiltered
                                # plan appears to have enough space.
                                planning_files = non_duplicate_files(
                                    selected_files, _fresh_checker(),
                                )
                            source_bytes = total_file_bytes(planning_files)
                            remote_summary_bits = []
                            if remote_archive is None:
                                # When a previous archive attempt left a partial
                                # untracked directory at final_destination, the
                                # retry uses move_folder(..., merge=True), which
                                # rsyncs only the missing files. Credit the bytes
                                # already published so the preflight doesn't
                                # reject a retry whose remaining delta would fit.
                                existing_bytes = existing_archive_bytes(
                                    final_destination,
                                    planning_files,
                                    params.folder_template,
                                )
                                plan = storage_plan(
                                    params.destination, source_bytes,
                                    archive_parent=archive_space_path,
                                    archive_existing_bytes=existing_bytes,
                                )
                            else:
                                # Staging-only local plan (the archive volume
                                # is the NAS, never the same device), then a
                                # remote df probe for the archive side.
                                # Probe failures degrade to "check skipped" —
                                # logged and surfaced in the step summary and
                                # result payload, never faked as numbers; the
                                # archive move's own rsync failure is the
                                # backstop if space actually runs out.
                                from local_processing import (
                                    RESERVED_FREE_BYTES,
                                )
                                from move import _remote_free_bytes
                                plan = storage_plan(
                                    params.destination, source_bytes,
                                )
                                target = remote_archive["target"]
                                # No merge/resume credit for a remote archive:
                                # the local resume-credit path (existing_archive_bytes)
                                # compares each destination file's size+content
                                # against the source, but a remote equivalent
                                # would need a per-file walk over SSH. A
                                # whole-tree `du` reports every byte at the
                                # path — including unrelated files or stale
                                # partials that rsync --ignore-existing will
                                # still copy past — which could cancel out
                                # source_bytes and let the preflight pass on
                                # a nearly-full NAS. Budget the full source
                                # here; a retry whose remaining delta would
                                # actually fit but the full source wouldn't
                                # is a rare batch-reject we take over the
                                # false-positive that lets processing burn
                                # hours before the transfer fails on space.
                                archive_delta = source_bytes
                                plan["archive_existing_bytes"] = 0
                                plan["archive_required_bytes"] = archive_delta
                                # df the configured base (just verified as an
                                # existing writable dir by the connection
                                # test) rather than the not-yet-created leaf.
                                remote_free = _remote_free_bytes(
                                    target, target["remote_path"],
                                )
                                plan["archive_free_bytes"] = remote_free
                                if remote_free is None:
                                    log.warning(
                                        "Couldn't probe free space at %s; "
                                        "skipping remote free-space check",
                                        remote_archive["display"],
                                    )
                                    remote_summary_bits.append(
                                        "remote free-space check skipped "
                                        "(probe failed)"
                                    )
                                    plan["archive_usable_bytes"] = None
                                else:
                                    archive_usable = max(
                                        0, remote_free - RESERVED_FREE_BYTES,
                                    )
                                    plan["archive_usable_bytes"] = archive_usable
                                    plan["archive_enough"] = (
                                        archive_delta <= archive_usable
                                    )
                                    plan["enough"] = (
                                        plan["staging_enough"]
                                        and plan["archive_enough"]
                                    )
                                    plan["batching_required"] = not plan["enough"]
                                    remote_summary_bits.append(
                                        f"{format_bytes(remote_free)} free at "
                                        f"{target['name']}"
                                    )
                            result["local_processing"] = {
                                **plan,
                                "staging_destination": params.destination,
                                "final_destination": final_destination,
                            }
                            if remote_archive is not None:
                                target = remote_archive["target"]
                                result["local_processing"]["remote"] = {
                                    "target_id": target["id"],
                                    "target_name": target["name"],
                                    "host": target["host"],
                                    "user": target["user"],
                                    "ssh_destination": remote_archive["ssh_final"],
                                    "free_space_checked": (
                                        plan["archive_free_bytes"] is not None
                                    ),
                                }
                            if plan["batching_required"]:
                                # Tell the user which volume came up short — the
                                # destination running out of room reads as a
                                # different problem (pick a bigger archive
                                # drive) than the staging volume running out
                                # (free space on ~/.vireo or batch later).
                                if not plan.get("archive_enough", True):
                                    if remote_archive is not None:
                                        _bail_storage(
                                            "Remote archive needs about "
                                            f"{format_bytes(plan['archive_required_bytes'])}, "
                                            "but only "
                                            f"{format_bytes(plan['archive_usable_bytes'] or 0)} "
                                            "is free at "
                                            f"{remote_archive['display']} after "
                                            "the free-space reserve. Free space "
                                            "on the remote volume or pick a "
                                            "different target or subpath."
                                        )
                                    else:
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
                            if remote_summary_bits:
                                summary += "; " + "; ".join(remote_summary_bits)
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

                    # One shared checker across the whole source loop: files
                    # copied by earlier iterations are recorded in it, so
                    # later sources treat them as duplicates even before the
                    # DB scan (this replaces the old accumulated-hashes
                    # re-read of every copied file between sources).
                    ingest_checker = _fresh_checker()
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
                                duplicate_checker=ingest_checker,
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
                        # Stop the run here. Without abort, scanner/previews/
                        # classify/regroup would all execute against the
                        # partial subset ingest did manage to copy — regroup
                        # in particular overwrites the workspace pipeline
                        # results with photo IDs that archive_stage's
                        # deindex_staging is then going to delete, leaving
                        # the workspace pointing at rows that no longer
                        # exist. archive_stage already gates on abort and
                        # any earlier-stage failure, so this also publishes
                        # nothing. Finalize the remaining step rows as
                        # skipped so the SSE clients don't see perpetually
                        # pending stages.
                        abort.set()
                        stages["scan"]["status"] = "skipped"
                        runner.update_step(
                            job["id"], "scan",
                            status="completed",
                            summary="Skipped (ingest failed)",
                        )
                        _update_stages(runner, job["id"], stages)
                        # The finally clause at the bottom of scanner_stage
                        # puts the sentinel on scan_to_thumb so the
                        # thumbnail consumer drains and exits.
                        return
                    else:
                        stages["ingest"]["status"] = "completed"
                        # Ingest is the only stage that ever reads the source
                        # (SD card/etc.) — everything after this point works
                        # from the copy. Record counts so the UI can tell the
                        # user the card is safe to eject instead of leaving
                        # them to guess. Only claim this when every discovered
                        # file actually made it off the card: local_processing
                        # aborts above on any failure, but plain copy mode
                        # (local_processing=False) reaches this branch even
                        # with total_failed > 0, and the card still holds
                        # files that never got copied.
                        if total_failed == 0:
                            stages["ingest"]["copied"] = total_copied
                            stages["ingest"]["skipped_duplicate"] = total_skipped
                            result["stages"]["ingest"] = {
                                "copied": total_copied,
                                "skipped_duplicate": total_skipped,
                            }
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
                    # Snapshot-scoped: hand the scanner the exact file set
                    # captured at snapshot time so a file that landed in the
                    # folder AFTER the snapshot doesn't get cataloged here.
                    # Without this, the scan walks the whole folder, commits
                    # a photos row for the late arrival, then the collection
                    # stage filters it out of downstream work AND the finally
                    # block invalidates the new-images cache — orphaning the
                    # file (cataloged in DB, never classified, never
                    # re-surfaced by a later banner probe). Skipping it at
                    # scan time keeps it uncataloged so the next probe
                    # rediscovers it.
                    snapshot_files_set = (
                        set(snapshot_paths) if snapshot_paths is not None else None
                    )
                    for src_folder in sources:
                        scanned_roots.append(src_folder)
                        snapshot_restrict_dirs = None
                        snapshot_restrict_files = None
                        if snapshot_files_set is not None:
                            src_norm = os.path.normpath(src_folder)
                            prefix = (
                                src_norm if src_norm.endswith(os.sep)
                                else src_norm + os.sep
                            )
                            files_under_src = [
                                p for p in snapshot_paths
                                if os.path.normpath(p).startswith(prefix)
                            ]
                            snapshot_restrict_files = set(files_under_src)
                            snapshot_restrict_dirs = sorted(
                                {os.path.dirname(p) for p in files_under_src}
                            )
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
                                restrict_dirs=snapshot_restrict_dirs,
                                restrict_files=snapshot_restrict_files,
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
                if isinstance(e, ScanCancelled) and (
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
                        # scanner.scan touches disk and may add or remove
                        # photo rows; a ready Missing Originals payload
                        # computed before the pipeline scan can now be
                        # stale (e.g. user restored an original before
                        # running Process). Standalone scan / import jobs
                        # already invalidate here — mirror that for
                        # pipeline scans so GET /api/photos/missing does
                        # not keep serving the pre-scan photo list.
                        if missing_originals_invalidator is not None:
                            try:
                                missing_originals_invalidator()
                            except Exception:
                                log.exception(
                                    "Failed to invalidate missing-originals "
                                    "cache for %s",
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
            # to exactly that set. The scan stage already restricted the walk
            # to the snapshot's file set via ``restrict_dirs`` + ``restrict_files``
            # so late arrivals aren't cataloged in the first place; this filter
            # is a belt-and-suspenders trim in case a pre-existing (already
            # cataloged) photo somehow ends up in ``collected_photo_ids``. Any
            # snapshot path that never resolved (file was moved/deleted between
            # snapshot and pipeline run) is logged so an unexpectedly small
            # collection is auditable.
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
                failed_photos = []
                failure_detail_limit = 100

                def _record_failure(photo_id, photo_path, reason):
                    if len(failed_photos) >= failure_detail_limit:
                        return
                    failed_photos.append({
                        "id": photo_id,
                        "filename": os.path.basename(photo_path or ""),
                        "reason": reason,
                    })

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
                                    failed += 1
                                    _record_failure(
                                        photo_id, photo_path,
                                        "RAW decode previously failed and no "
                                        "acceptable fallback is available",
                                    )
                                    stages["thumbnails"]["count"] = (
                                        generated + skipped + failed
                                    )
                                    continue
                        recipe_kwargs = {"recipe": recipe} if recipe else {}
                        if recipe:
                            recipe_kwargs["native_size"] = (
                                _recipe_source_dimensions(detail_photo)
                            )
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
                        if (
                            result_path is None
                            and detail_photo is not None
                            and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                        ):
                            result_path = _retry_thumbnail_with_working_copy(
                                thread_db, generate_thumbnail, detail_photo,
                                photo_id, photo_path, cache_dir, thumb_size,
                                recipe, effective_vireo_dir,
                            )
                        if result_path is None:
                            failed += 1
                            _record_failure(
                                photo_id, photo_path,
                                "No acceptable thumbnail render source",
                            )
                        elif already_exists:
                            skipped += 1
                            pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                        else:
                            generated += 1
                            pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                        if len(pending_thumb_paths) >= THUMB_PATH_BATCH:
                            _flush_thumb_paths()
                    except Exception as exc:
                        failed += 1
                        _record_failure(photo_id, photo_path, str(exc))
                        log.debug(
                            "Thumbnail failed for photo %s", photo_id,
                            exc_info=True,
                        )
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
                                    failed += 1
                                    _record_failure(
                                        photo_id, photo_path,
                                        "RAW decode previously failed and no "
                                        "acceptable fallback is available",
                                    )
                                    stages["thumbnails"]["count"] = (
                                        generated + skipped + failed
                                    )
                                    continue
                            recipe_kwargs = {"recipe": recipe} if recipe else {}
                            if recipe:
                                recipe_kwargs["native_size"] = (
                                    _recipe_source_dimensions(detail_photo)
                                )
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
                            if (
                                result_path is None
                                and detail_photo is not None
                                and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                            ):
                                result_path = _retry_thumbnail_with_working_copy(
                                    thread_db, generate_thumbnail, detail_photo,
                                    photo_id, photo_path, cache_dir, thumb_size,
                                    recipe, effective_vireo_dir,
                                )
                            if result_path is None:
                                failed += 1
                                _record_failure(
                                    photo_id, photo_path,
                                    "No acceptable thumbnail render source",
                                )
                            elif already_exists:
                                skipped += 1
                                pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                            else:
                                generated += 1
                                pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                            if len(pending_thumb_paths) >= THUMB_PATH_BATCH:
                                _flush_thumb_paths()
                        except Exception as exc:
                            failed += 1
                            _record_failure(photo_id, photo_path, str(exc))
                            log.debug(
                                "Thumbnail failed for photo %s", photo_id,
                                exc_info=True,
                            )
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
                thumb_result = {
                    "generated": generated,
                    "skipped": skipped,
                    "failed": failed,
                }
                if failed_photos:
                    thumb_result["failed_photos"] = failed_photos
                if failed > len(failed_photos):
                    thumb_result["failed_photos_truncated"] = (
                        failed - len(failed_photos)
                    )
                processed = generated + skipped + failed
                # Per-photo failures leave coverage gaps but do not invalidate
                # thumbnails that were generated successfully. Keep the stage
                # terminal and expose the affected photos as repair details;
                # fatal setup/runtime exceptions still take the except path.
                stages["thumbnails"]["status"] = "completed"
                stages["thumbnails"]["error_count"] = failed
                thumb_rollup = (
                    f"{failed} of {processed} thumbnails need attention"
                    if failed > 0 else None
                )
                if thumb_rollup:
                    result.setdefault("warnings", []).append(
                        f"[thumbnails] {thumb_rollup}"
                    )
                runner.update_step(job["id"], "thumbnails", status="completed",
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
                        folder_path = folders.get(detail_photo["folder_id"])
                        raw_source_path = None
                        if (
                            not recipe
                            and folder_path
                            and os.path.splitext(detail_photo["filename"])[1].lower()
                            in _RAW_EXTENSIONS
                        ):
                            # Mirror /photos/<id>/preview: an unedited RAW
                            # must warm from the camera-rendered source,
                            # not the highlight-preserving working copy.
                            # Otherwise the pipeline preview stage writes
                            # flatter/darker bytes into the tracked preview
                            # cache and _serve_preview returns those cache
                            # hits before its own RAW-source branch ever
                            # runs, so the migration's one-time purge is
                            # undone the first time this stage runs.
                            candidate = os.path.join(
                                folder_path, detail_photo["filename"],
                            )
                            if os.path.exists(candidate) and not _has_current_working_copy_failure(
                                detail_photo,
                                base_dir,
                                trust_existing_working_copy=False,
                                live_source_path=candidate,
                                folder_path=folder_path,
                            ):
                                raw_source_path = candidate
                        if raw_source_path:
                            canonical = raw_source_path
                        else:
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
                                import local_masks
                                img = apply_recipe_to_loaded_image(
                                    img, recipe, max_size=max_size,
                                    native_size=_recipe_source_dimensions(
                                        detail_photo
                                    ),
                                    local_mask=local_masks.load_snapshot(
                                        effective_vireo_dir,
                                        photo["id"], recipe,
                                    ),
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
                model_dir=weights_path,
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
                        active_model["id"], weights_path, hf_subdir,
                        optional_files=active_model.get("optional_files"),
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
                try:
                    from classifier import ClassificationCancelled
                except ImportError:
                    class ClassificationCancelled(RuntimeError):
                        pass

                def cancel_check():
                    return _should_abort(abort) or runner.is_cancelled(job["id"])

                if model_type == "timm":
                    if cancel_check():
                        raise ClassificationCancelled("classification cancelled")
                    from timm_classifier import TimmClassifier
                    return TimmClassifier(model_str, taxonomy=tax)
                if cancel_check():
                    raise ClassificationCancelled("classification cancelled")
                from classifier import Classifier
                return Classifier(
                    labels=None if use_tol else labels,
                    model_str=model_str,
                    pretrained_str=weights_path,
                    cancel_check=cancel_check,
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
                                weights_path, hf_subdir,
                                optional_files=active_model.get(
                                    "optional_files"
                                ),
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
                    is_classification_cancelled = (
                        preload_err.__class__.__name__ == "ClassificationCancelled"
                    )
                    if (
                        runner.is_cancelled(job["id"])
                        or is_classification_cancelled
                        or str(preload_err) == "classification cancelled"
                    ):
                        raise
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
                abort.set()
                is_classification_cancelled = (
                    e.__class__.__name__ == "ClassificationCancelled"
                )
                if (
                    runner.is_cancelled(job["id"])
                    or is_classification_cancelled
                    or str(e) == "classification cancelled"
                ):
                    stages["model_loader"]["status"] = "skipped"
                    runner.update_step(
                        job["id"], "model_loader",
                        status="completed", summary="Skipped (cancelled)",
                    )
                else:
                    errors.append(f"[model_loader] Fatal: {e}")
                    log.exception("Pipeline model loader stage failed")
                    stages["model_loader"]["status"] = "failed"
                    runner.update_step(
                        job["id"], "model_loader", status="failed", error=str(e),
                    )
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
                total_full_image_fallbacks = 0
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
                fresh_full_image_ids_by_photo: dict = {}

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
                            is_classification_cancelled = (
                                model_err.__class__.__name__ == "ClassificationCancelled"
                            )
                            if (
                                _should_abort(abort)
                                or runner.is_cancelled(job["id"])
                                or is_classification_cancelled
                                or str(model_err) == "classification cancelled"
                            ):
                                abort.set()
                                runner.update_step(
                                    job["id"], step_id,
                                    status="completed",
                                    summary="Skipped (cancelled)",
                                )
                                completed_step_ids.add(step_id)
                                continue
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
                    full_image_fallbacks = 0
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
                            # authoritative rows). If MegaDetector produced no
                            # real rows at all, synthesize a full-image anchor so
                            # classifiers still get one attempt and future reruns
                            # can hit classifier_runs for that attempt.
                            full_image_fallback = False
                            if photo["id"] in cached_detections:
                                # cached_detections from _detect_batch can include
                                # full-image rows when an earlier pass synthesized
                                # them (legacy db state); filter to match the
                                # fallback-query branch below so primary_det only
                                # lands on a real detector box.
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
                                # Distinguish "no eligible detection at the
                                # workspace threshold" from "MegaDetector found
                                # nothing at all." Weak raw detections keep the
                                # existing behavior for now; true no-detection
                                # photos get full-image classification.
                                raw_real_dets = [
                                    d for d in thread_db.get_detections(
                                        photo["id"], min_conf=0,
                                    )
                                    if d["detector_model"] != "full-image"
                                ]
                                if raw_real_dets:
                                    continue

                                existing_full = thread_db.get_detections(
                                    photo["id"],
                                    detector_model="full-image",
                                    min_conf=0,
                                )
                                if existing_full and not params.reclassify:
                                    full_det_id = existing_full[0]["id"]
                                else:
                                    full_det_ids = thread_db.save_detections(
                                        photo["id"],
                                        [{
                                            "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                                            "confidence": 0,
                                            "category": "animal",
                                        }],
                                        detector_model="full-image",
                                    )
                                    full_det_id = full_det_ids[0]
                                primary_det = {
                                    "id": full_det_id,
                                    "box_x": 0,
                                    "box_y": 0,
                                    "box_w": 1,
                                    "box_h": 1,
                                    "confidence": 0,
                                    "category": "animal",
                                    "detector_model": "full-image",
                                }
                                full_image_fallback = True
                                full_image_fallbacks += 1
                                fresh_full_image_ids_by_photo.setdefault(
                                    photo["id"], set(),
                                ).add(full_det_id)

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
                                photo, folders,
                                None if full_image_fallback else primary_det,
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
                    total_full_image_fallbacks += full_image_fallbacks
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
                        # _detect_batch built). A no-detection photo may also
                        # have a freshly used synthetic full-image anchor from
                        # the fallback path; preserve that id so the purge does
                        # not cascade-delete the new fallback prediction. Other
                        # pre-run rows on empty photos are stale and get purged
                        # (write_detection_batch([]) already cleared the
                        # MegaDetector rows at the data layer — this is the
                        # belt-and-suspenders pass and cross-model cleanup). A
                        # photo re-detected with the same boxes has its ids in
                        # the fresh set, so they survive.
                        fresh_by_photo = detect_state["detections"]
                        stale_ids = [
                            det_id
                            for photo_id, id_set in pre_ids.items()
                            if photo_id in purge_ids
                            for det_id in id_set
                            if det_id not in (
                                {
                                    d["id"]
                                    for d in fresh_by_photo.get(photo_id, [])
                                }
                                | fresh_full_image_ids_by_photo.get(photo_id, set())
                            )
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
                    if full_image_fallbacks:
                        parts.append(f"{full_image_fallbacks} full-image fallback")
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
                    "full_image_fallbacks": total_full_image_fallbacks,
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
                pipeline_cfg = dict(effective_cfg.get("pipeline", {}))

                # Apply the per-run eye-detect override only when the caller
                # sent an explicit signal. ``skip_eye_keypoints=False`` alone
                # is not proof of opt-in: ``the "Full" saved process`` sets it
                # to False as a base default, so an after-import ``full``
                # chain would otherwise force ``eye_detect_enabled=True``
                # against a workspace whose Settings default is False (the
                # new default) — triggering SuperAnimal downloads and eye-
                # based scoring by default. ``eye_detect_override`` is the
                # explicit signal the Process page sends alongside its
                # checkbox state; strategy expansion leaves it None, so a
                # chained ``full`` run respects the user's Settings value.
                if params.eye_detect_override is not None:
                    pipeline_cfg["eye_detect_enabled"] = params.eye_detect_override

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
            if params.skip_regroup:
                # Only take the species-review save path when the caller
                # explicitly asked for it (the identify preset sets
                # ``review_mode="species"`` via process_strategies). A
                # classify-only run without that opt-in — Advanced/Custom
                # on the Process page, or an API client sending
                # ``skip_regroup: true`` — must NOT overwrite
                # ``pipeline_results_ws*.json`` with all-REVIEW species
                # output, since that would silently reintroduce the
                # culling-pipeline downgrade the reviewer flagged (the
                # user just wanted to refresh classifications, not turn
                # the workspace cache into a species-review cache).
                do_species = (
                    params.review_mode == "species"
                    and not abort.is_set()
                    and collection_id
                    and not params.skip_classify
                )
                if not do_species:
                    stages["regroup"]["status"] = "skipped"
                    runner.update_step(job["id"], "regroup", status="completed",
                                       summary="Skipped")
                    # Emit a progress event so the SSE stream (and tests
                    # asserting on the last progress payload) can see the
                    # stage's terminal "skipped" state. Without this, a
                    # downstream miss_stage that also short-circuits
                    # leaves the last stages dict stuck at whatever the
                    # detect/classify stage emitted last.
                    _update_stages(runner, job["id"], stages)
                    return
                try:
                    import config as cfg
                    from pipeline import (
                        load_photo_features,
                        run_species_review_pipeline,
                        save_results,
                    )

                    thread_db = Database(db_path)
                    thread_db.set_active_workspace(workspace_id)

                    effective_cfg = thread_db.get_effective_config(cfg.load())
                    pipeline_cfg = effective_cfg.get("pipeline", {})

                    photos = load_photo_features(
                        thread_db,
                        collection_id=collection_id,
                        config=effective_cfg,
                    )
                    if params.exclude_photo_ids:
                        photos = [
                            p for p in photos
                            if p["id"] not in params.exclude_photo_ids
                        ]
                    if not photos:
                        result["stages"]["review"] = {
                            "error": "No photos with pipeline features found.",
                        }
                    else:
                        results = run_species_review_pipeline(
                            photos,
                            config=pipeline_cfg,
                            emit_trace=True,
                        )
                        cache_dir = os.path.dirname(db_path)
                        # Don't preserve miss_computed_at from any prior
                        # full run. The identify strategy skips the miss
                        # stage entirely, so the cache we're writing has
                        # no misses of its own; carrying the old marker
                        # forward would make Pipeline Review call
                        # /api/misses?since=<old marker> and render miss
                        # rows from the previous full run as if they were
                        # produced by this identify pass.
                        save_results(
                            results,
                            cache_dir,
                            workspace_id,
                            preserve_miss_marker=False,
                        )
                        # The species-only pipeline overwrites
                        # pipeline_results_ws*.json with review-only output
                        # (no burst/keep/reject scoring). If a prior full
                        # regroup left a valid last_group_fingerprint stamped
                        # on the workspace, pipeline_plan._group_plan would
                        # match it against current settings and report
                        # "done-prior" — silently letting an advanced Group
                        # & Score run be skipped even though the cache no
                        # longer contains any triage output. Invalidate the
                        # stamp so a subsequent full run is correctly shown
                        # as will-run.
                        thread_db.set_workspace_group_state(
                            workspace_id=workspace_id,
                            fingerprint=None,
                            when_ts=None,
                        )
                        result["stages"]["review"] = results.get("summary", {})

                    stages["regroup"]["status"] = "completed"
                    runner.update_step(
                        job["id"], "regroup",
                        status="completed",
                        summary="Review results ready" if photos else "No photos to group",
                    )
                except Exception as e:
                    errors.append(f"[review] Fatal: {e}")
                    log.exception("Pipeline species-review stage failed")
                    stages["regroup"]["status"] = "failed"
                    runner.update_step(
                        job["id"], "regroup", status="failed", error=str(e),
                    )
                _update_stages(runner, job["id"], stages)
                return

            if abort.is_set() or not collection_id:
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
                pipeline_cfg = dict(effective_cfg.get("pipeline", {}))

                # Mirror the eye_keypoints_stage per-run override so scoring
                # honors the same explicit intent. Without carrying the
                # override into pipeline_cfg here, ``run_full_pipeline``
                # reloads workspace config with ``eye_detect_enabled=False``
                # (the new default) and ``score_encounter`` ignores the
                # ``eye_tenengrad`` values the eye stage just wrote — so the
                # visible checkbox would affect only the expensive keypoint
                # pass, not the culling result the user actually sees.
                # Gated on ``eye_detect_override`` (an explicit per-run
                # signal), NOT on ``not skip_eye_keypoints``, because the
                # latter is False by default in ``the "Full" saved process``
                # too — using it would force eye scoring on for any chained
                # ``full`` run regardless of workspace Settings.
                if params.eye_detect_override is not None:
                    pipeline_cfg["eye_detect_enabled"] = params.eye_detect_override

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
                    # A per-run eye override that differs from the
                    # workspace's own effective ``eye_detect_enabled`` means
                    # this run's KEEP/REJECT decisions came from scoring
                    # settings the workspace's normal state wouldn't produce.
                    # ``compute_group_fingerprint`` reads only encounter/burst
                    # keys — not ``eye_detect_enabled`` — so stamping it here
                    # would mark eye-scored (or eye-disabled) results as
                    # settings-fresh for a later plan run against the
                    # workspace's real settings. Treat that as a partial run
                    # so ``pipeline_plan`` reports the cache as needing to
                    # re-run instead of hiding the mismatch.
                    workspace_eye_setting = bool(
                        effective_cfg.get("pipeline", {}).get(
                            "eye_detect_enabled", False,
                        )
                    )
                    per_run_eye_override_differs = (
                        params.eye_detect_override is not None
                        and bool(params.eye_detect_override) != workspace_eye_setting
                    )
                    covered_full_workspace = (
                        not params.exclude_photo_ids
                        and ws_photo_ids.issubset(collection_photo_ids)
                        and not per_run_eye_override_differs
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

            # Hoisted from the try: block below so the miss_enabled guard can
            # read effective config before the transient "running" status is
            # written.
            try:
                from datetime import UTC, datetime

                import config as cfg
                from misses import compute_misses_for_workspace
                from pipeline import load_results_raw, save_results_raw

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                effective_cfg = thread_db.get_effective_config(cfg.load())
                pipeline_cfg = effective_cfg.get("pipeline", {})
            except Exception as e:
                # Mark the stage failed BEFORE returning. The transient
                # "running" write is *below* this guard, so without stamping
                # "failed" here the stage would stay "pending"; the pipeline
                # finalizer treats absence-of-failed as success and would
                # wrongly mark the whole job completed despite a fatal setup
                # error (cfg.load, Database(...), or any import raising).
                stages["misses"]["status"] = "failed"
                runner.update_step(job["id"], "misses", status="failed",
                                   error=str(e))
                errors.append(f"[misses] Fatal: {e}")
                log.exception("Pipeline miss-detection setup failed")
                _update_stages(runner, job["id"], stages)
                return

            # Effective miss_enabled: per-run PipelineParams override wins
            # over workspace config, mirroring how other skip_* flags
            # override workspace defaults. Inject the effective value into
            # pipeline_cfg *before* the guard so both branches — the
            # short-circuit skip AND the fall-through to compute — see the
            # same value: compute_misses_for_workspace reads
            # pipeline_cfg["miss_enabled"] itself, so a strategy that
            # enables misses on a workspace where they're disabled would
            # otherwise get a silent 0 from compute.
            if params.miss_enabled is not None:
                pipeline_cfg = {**pipeline_cfg,
                                "miss_enabled": params.miss_enabled}
            miss_enabled = pipeline_cfg.get("miss_enabled", True)
            if not miss_enabled:
                # Do NOT fall through to compute_misses_for_workspace: it
                # returns 0 when disabled and the completion path would then
                # stamp "0 photos evaluated", which reads as "misses ran and
                # found none" rather than "misses were disabled". Skipping
                # here also leaves the miss_computed_at cache marker
                # unstamped, which pipeline_review's "current-run misses"
                # shortcut depends on.
                stages["misses"]["status"] = "skipped"
                runner.update_step(job["id"], "misses", status="completed",
                                   summary="Skipped")
                _update_stages(runner, job["id"], stages)
                return

            stages["misses"]["status"] = "running"
            runner.update_step(job["id"], "misses", status="running")
            _update_stages(runner, job["id"], stages)

            try:
                # Share one timestamp between the DB write and the saved
                # pipeline-results cache so pipeline_review's "Review misses"
                # shortcut can gate on actual recomputation in this run and
                # scope /misses?since=... to exactly what was just written.
                now_ts = datetime.now(UTC).isoformat(timespec="microseconds")

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
            """Retired import/archive path guard.

            Importing photos now runs through import_job.py and
            /api/jobs/import-photos. The old local-processing archive stage
            intentionally has no cleanup/deindex path left here: orphaned
            staging folders are reconciled by staging_recovery.py before any
            deletion is offered.
            """
            if params.local_processing or params.destination:
                raise RuntimeError(
                    "Pipeline import/archive mode has been removed. Use the "
                    "Import page or /api/jobs/import-photos to copy photos "
                    "into the archive."
                )
            return

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
