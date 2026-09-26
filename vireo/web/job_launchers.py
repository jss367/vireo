"""Background-job launchers that call per-app services.

``web.jobs`` holds job control and the launchers that need nothing beyond the
standard blueprint arguments. The launchers here start scans, previews,
ingest/move/offline-cache/full-resolution preparation, folder moves, XMP sync,
classification, darktable develop, mask extraction, label fetching, embedding
precompute and the background batch delete. Their request handling leans on
the app's scan, delete, move-folder and missing-originals services, whose
bound methods ``create_app`` passes in.
"""

from __future__ import annotations

import json
import logging
import os
import time

from artifact_flight import ArtifactProducerFailed
from config import read_raw_config_file, settings_write_lock
from db import Database, commit_with_retry
from flask import Blueprint, current_app, jsonify, request
from preview_cache import (
    evict_if_over_quota as evict_preview_cache_if_over_quota,
)
from preview_materializer import (
    PreviewMaterializationError,
    materialize_preview,
)
from runtime_warnings import (
    build_cpu_runtime_warning,
    runtime_warning_work_units,
)
from services.local_folder import (
    local_copy_scan_conflict,
    stage_pending_source_paths,
)
from services.local_workspace import (
    has_local_workspace,
    stage_boundary_lock,
)
from services.startup_tasks import metadata_repair_count
from sql_chunks import chunked
from web.background_jobs import make_background_job
from web.request_args import (
    coerce_collection_id,
    parse_selection_photo_ids,
    reject_visual_collection,
)

log = logging.getLogger(__name__)


def _count_lines(path):
    if not path:
        return None
    try:
        with open(path) as f:
            return sum(1 for line in f if line.strip())
    except (OSError, UnicodeError):
        return None


def create_job_launchers_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    config,
    *,
    invalidate_missing_originals,
    run_batch_delete,
    build_scan_work,
    pending_local_workspace_transition,
    guard_move_folder,
    start_move_folder_job,
    serve_original_photo,
    sync_job_lock,
):
    """Build the job-launchers blueprint.

    ``config`` is the Flask app's config mapping, read when a job starts
    (``THUMB_CACHE_DIR``, ``COMPUTATION_CACHE_DIR``). Everything keyword-only
    is per-app state from ``create_app``:

    - ``build_scan_work`` (``services.scan_work.build_scan_work`` bound to
      the app) is also used by ``/api/folders/<id>/rescan``.
    - ``pending_local_workspace_transition``, ``guard_move_folder`` and
      ``start_move_folder_job`` are the app's ``FolderMoves`` methods, shared
      with the import and move-cleanup blueprints and the post-pipeline NAS
      move.
    - ``run_batch_delete`` (the app's ``PhotoDeletion``) backs
      ``/api/batch/delete`` and ``/api/photos/missing/remove`` too;
      ``invalidate_missing_originals`` is the app's ``MissingOriginals``
      cache reset.
    - ``serve_original_photo`` is the ``/photos/<id>/original`` view, which
      full-resolution preparation drives so it renders exactly what the
      lightbox would.
    - ``sync_job_lock`` is ``app._sync_job_lock``, which serializes XMP sync
      with the import-side sync.

    The scan launcher remembers its roots in ``config.json`` through
    ``config.read_raw_config_file`` under ``config.settings_write_lock``.
    """
    blueprint = Blueprint("job_launchers", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)

    @blueprint.route("/api/jobs/batch-delete", methods=["POST"])
    @background_job
    def api_job_batch_delete(ctx):
        """Start a background delete job with phase progress."""
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        mode = body.get("mode", "vireo")
        include_companions = body.get("include_companions", False)

        if mode not in ("vireo", "disk", "disk_permanent"):
            return json_error("mode must be 'vireo', 'disk', or 'disk_permanent'")
        if not photo_ids:
            return json_error("photo_ids required")
        if not isinstance(photo_ids, list):
            return json_error("photo_ids must be a list")

        def work(job):
            thread_db = ctx.thread_db()
            started = time.time()

            def progress(payload):
                current = payload.get("current", 0)
                total = payload.get("total", 0)
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = payload.get("current_file", "")
                ctx.runner.push_event(job["id"], "progress", {
                    **payload,
                    "rate": round(current / max(time.time() - started, 0.01), 1)
                    if current else 0,
                })

            try:
                result = run_batch_delete(
                    thread_db,
                    photo_ids,
                    mode,
                    include_companions,
                    progress_callback=progress,
                )
                if result.get("deleted"):
                    invalidate_missing_originals()
                return result
            finally:
                thread_db.conn.close()

        return ctx.start(
            "batch-delete",
            work,
            config={
                "photo_count": len(photo_ids or []),
                "mode": mode,
                "include_companions": bool(include_companions),
            },
        )

    @blueprint.route("/api/jobs/precompute-embeddings", methods=["POST"])
    @background_job
    def api_job_precompute_embeddings(ctx):
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        labels_file = body.get("labels_file")
        if not model_id or not labels_file:
            return json_error("model_id and labels_file required")

        def work(job):
            from classifier import precompute_label_embeddings
            from labels import get_saved_labels, load_label_set
            from models import get_models

            # Find the model
            models = get_models()
            model = None
            for m in models:
                if m["id"] == model_id:
                    model = m
                    break
            if not model or not model["downloaded"]:
                raise RuntimeError(f"Model {model_id} not found or not downloaded")
            if model.get("model_type", "bioclip") == "timm":
                raise RuntimeError(
                    f"Model {model['name']} has a fixed class head and does "
                    "not use per-label embeddings — nothing to precompute."
                )

            ctx.runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": 0,
                    "current_file": f'Loading {model["name"]} and computing embeddings...',
                    "rate": 0,
                },
            )

            # The same list the classify job will load, so the warmed
            # cache is the one it looks for.
            saved = {ls.get("labels_file"): ls for ls in get_saved_labels()}
            labels = load_label_set(labels_file, saved.get(labels_file))
            if not labels:
                skipped = len(getattr(labels, "dropped_ambiguous", ()))
                raise RuntimeError(
                    f"{os.path.basename(labels_file)} has no usable species"
                    + (f" — all {skipped:,} of its names are shared by more "
                       "than one species" if skipped else "")
                    + ". Download the list again in Settings → Labels to "
                    "split them by scientific name."
                )

            log.info(
                "Pre-computing embeddings: %d labels with %s",
                len(labels),
                model["name"],
            )

            def _progress(current, total):
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": f"Computing embeddings ({current}/{total} labels)…",
                        "rate": 0,
                    },
                )

            from classifier import ClassifierLoadPaused

            def _pause_requested():
                return ctx.runner.pause_requested(job["id"])

            def _cancelled():
                # Pure cancel probe: the pause is handled by the retry
                # loop below, outside the embedding single-flight, so the
                # producer never parks while equal-key waiters are joined.
                return ctx.runner.cancellation_requested(job["id"])

            # The shared service loads only the text side and joins an
            # equal-key pipeline/precompute already doing this work.
            while True:
                try:
                    precompute_label_embeddings(
                        labels=labels,
                        model_str=model["model_str"],
                        pretrained_str=model["weights_path"],
                        progress_callback=_progress,
                        cancel_check=_cancelled,
                        pause_check=_pause_requested,
                    )
                    break
                except ClassifierLoadPaused:
                    # Progress is checkpointed. Park here with no shared
                    # lock held; the next attempt resumes from the
                    # checkpoint. A Cancel during the pause surfaces on
                    # the retry through the cancel probe.
                    log.info(
                        "Pre-computing embeddings paused; parking until "
                        "Resume",
                    )
                    ctx.runner.is_cancelled(job["id"])
                    continue

            return {"labels": len(labels), "model": model["name"]}

        return ctx.start(
            "precompute-embeddings",
            work,
            config={
                "model_id": model_id,
                "labels_file": labels_file,
            },
            pausable=True,
            runtime_warning=build_cpu_runtime_warning(
                "precompute-embeddings",
                work_units=runtime_warning_work_units(
                    "precompute labels file",
                    lambda: _count_lines(labels_file),
                ),
                reason="large_embedding_precompute_cpu_only",
            ),
        )

    @blueprint.route("/api/jobs/fetch-labels", methods=["POST"])
    @background_job
    def api_job_fetch_labels(ctx):
        body = request.get_json(silent=True) or {}
        place_id = body.get("place_id")
        place_name = body.get("place_name", "")
        taxon_groups = body.get("taxon_groups", ["birds"])
        observation_filter = body.get("observation_filter", "research")
        name = body.get("name", "")
        if not place_id:
            return json_error("place_id required")
        if observation_filter not in ("research", "wild", "all"):
            observation_filter = "research"
        if not name:
            from labels import OBSERVATION_FILTERS
            group_names = ", ".join(g.title() for g in taxon_groups)
            filter_label = OBSERVATION_FILTERS[observation_filter]["name"]
            name = f"{place_name} {group_names} ({filter_label})".strip()

        def work(job):
            from labels import fetch_species_list, load_label_set, save_labels

            def progress_cb(msg, current=None, total=None):
                ctx.checkpoint(job)
                job["progress"]["current_file"] = msg
                if current is not None:
                    job["progress"]["current"] = current
                if total is not None:
                    job["progress"]["total"] = total
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current or 0,
                        "total": total or 0,
                        "current_file": msg,
                        "rate": 0,
                    },
                )

            species = fetch_species_list(
                place_id, taxon_groups,
                observation_filter=observation_filter,
                progress_callback=progress_cb,
            )
            if not species:
                raise RuntimeError(
                    "No species found for this region and taxa selection"
                )
            labels_path = save_labels(
                name, place_id, place_name, taxon_groups, species,
                observation_filter=observation_filter,
            )
            thread_db = ctx.thread_db()
            thread_db.set_workspace_active_labels([labels_path])

            precompute = None
            from models import get_active_model

            active_model = get_active_model()
            # timm models have a fixed class head — no per-label embeddings.
            if (
                active_model
                and active_model["downloaded"]
                and active_model.get("model_type", "bioclip") != "timm"
            ):
                try:
                    from classifier import _embedding_is_cached, _resolve_model_dir

                    labels = load_label_set(labels_path)
                    model_dir = _resolve_model_dir(
                        active_model["model_str"], active_model.get("weights_path")
                    )
                    # Use the shared helper: it validates payload shape and
                    # dtype rather than accepting any file at the identity
                    # path, so a truncated or wrong-shape cache no longer
                    # reports as ready here.
                    if not _embedding_is_cached(
                        labels, active_model["model_str"], model_dir
                    ):
                        precompute = {
                            "model_id": active_model["id"],
                            "model_name": active_model["name"],
                            "labels_file": labels_path,
                        }
                except Exception:
                    log.warning(
                        "Could not inspect embedding cache after label download",
                        exc_info=True,
                    )

            return {
                "species_count": len(set(species)),
                "labels_file": labels_path,
                # Common names iNaturalist gives to more than one taxon,
                # saved as "Common Name (Scientific name)" so each species
                # keeps its own prompt. Reported so the list the user sees
                # matches the names they will get back.
                "disambiguated": len(getattr(species, "disambiguated", [])),
                "embedding_precompute": precompute,
            }

        return ctx.start(
            "fetch-labels",
            work,
            pausable=True,
            config={
                "place_id": place_id,
                "place_name": place_name,
                "taxon_groups": taxon_groups,
            },
        )

    @blueprint.route("/api/jobs/scan", methods=["POST"])
    @background_job
    def api_job_scan(ctx):
        """Queue a scan job.

        Body accepts either:
          * ``{"root": "/path"}`` -- single root (back-compat).
          * ``{"roots": ["/a", "/b", ...]}`` -- multiple roots, scanned
            serially inside a single job. Multi-root support avoids the
            SQLite writer-lock contention that used to happen when the
            UI enqueued one job per root (PR #634 added retry/backoff
            as defense-in-depth; this is the root-cause fix).
        """
        # A scan without an active workspace would create catalog entries
        # invisible to every workspace: the worker calls
        # ``set_active_workspace(None)`` and ``Database.add_folder`` then
        # skips the workspace link because ``_active_workspace_id is None``.
        # The mutation-reservation hook deliberately lets no-workspace
        # requests through so routes that answer that state can respond
        # cleanly; this route is not one of them, so refuse here.
        if ctx.workspace_id is None:
            return json_error("no active workspace", 400)
        body = request.get_json(silent=True) or {}
        incremental = body.get("incremental", False)

        # Normalize inputs. Prefer the explicit plural form when both are
        # provided; a caller who sends ``roots`` has opted into the new API.
        if "roots" in body:
            roots_in = body.get("roots")
            if not isinstance(roots_in, list) or not roots_in:
                return json_error("roots must be a non-empty list")
            roots_list = [str(r) for r in roots_in if r]
            if not roots_list:
                return json_error("roots must be a non-empty list")
        else:
            root = body.get("root", "")
            if not root:
                return json_error("root path required")
            roots_list = [root]

        from image_loader import is_excluded_scan_path

        for r in roots_list:
            # Reject other-app data bundles (Apple Photos / Aperture / Photo
            # Booth) before any stat: ``os.path.isdir`` on a ``.photoslibrary``
            # path — or a symlink to one — itself trips the macOS
            # "access data from other apps" TCC prompt, defeating the guards
            # inside scan().
            if is_excluded_scan_path(r):
                return json_error(
                    f"path is inside a macOS app-managed library and cannot "
                    f"be scanned: {r}"
                )
            if not os.path.isdir(r):
                return json_error(f"directory not found: {r}")

        # Remember scan roots (skip temp directories from tests)
        import tempfile

        import config as cfg

        tmp_prefix = os.path.realpath(tempfile.gettempdir())
        # Lock + raw read-modify-write so a concurrent settings PATCH isn't
        # reverted and we don't pin every DEFAULTS value into the user's
        # file (see api_pipeline_save_grouping_defaults).
        with settings_write_lock:
            raw = read_raw_config_file()
            saved_roots = raw.get("scan_roots")
            if not isinstance(saved_roots, list):
                saved_roots = []
            changed = False
            for r in roots_list:
                if os.path.realpath(r).startswith(tmp_prefix):
                    continue
                if r not in saved_roots:
                    saved_roots.insert(0, r)
                    changed = True
            if changed:
                raw["scan_roots"] = saved_roots
                cfg.save(raw)

        # A workspace with active local state has its folder paths rebased
        # into the managed copy; scanner.scan() calls
        # db.add_folder(link_to_workspace=True) for every folder it discovers,
        # so any new root scanned here would add folder/workspace_folders rows
        # that the manifest and local_workspace_folders don't cover — sync and
        # discard could not rebase or remove them, and the workspace would mix
        # unmanaged source paths with the managed local copy. Refuse until
        # the local copy is synced or discarded, mirroring the folder-add,
        # folder-remove, and move-folders guards. Also refuse when a
        # stage/sync/discard job is queued or running but hasn't yet touched
        # local_workspaces — otherwise the scan enqueued in that window
        # could add unmanaged rows before the transition worker claims the
        # workspace.
        if ctx.workspace_id is not None:
            with stage_boundary_lock():
                if has_local_workspace(get_db(), ctx.workspace_id):
                    return json_error(
                        "Cannot scan folders while working locally. Sync or discard the local copy first.",
                        409,
                    )
                pending = pending_local_workspace_transition(ctx.workspace_id)
                if pending:
                    return json_error(
                        f"Wait for the {pending['type']} job to finish before scanning; "
                        "otherwise the scan would add folder rows the workspace's local "
                        "manifest doesn't cover.",
                        409,
                    )
        # A folder-level local copy (shared across workspaces) rebases its
        # catalog rows onto the managed copy, so walking its original source
        # tree would catalog every one of those photos a second time. Include
        # queued folder-stage jobs so a stage another workspace has waiting
        # for its worker cannot slip a scan registration through the window
        # before its mapping row exists, and hold ``stage_boundary_lock``
        # across the check and ``ctx.start`` so a stage registering after us
        # sees this scan in ``_busy_job`` -- otherwise the reverse race
        # remains open between our conflict check and job registration.
        db_for_conflict = get_db()
        work = build_scan_work(roots_list, incremental, ctx.workspace_id)
        job_config = {"roots": roots_list, "incremental": incremental}
        # Back-compat: keep ``root`` in config when exactly one was given,
        # so existing consumers (history viewers, etc.) still find it.
        if len(roots_list) == 1:
            job_config["root"] = roots_list[0]
        with stage_boundary_lock():
            pending_sources = stage_pending_source_paths(
                ctx.runner.list_jobs if ctx.runner is not None else None,
                db_for_conflict,
            )
            conflict = local_copy_scan_conflict(
                db_for_conflict, roots_list,
                active_workspace_id=ctx.workspace_id,
                pending_stage_sources=pending_sources,
            )
            if conflict:
                return json_error(conflict, 409)
            return ctx.start(
                "scan", work, config=job_config,
                pausable=True,
            )

    @blueprint.route("/api/jobs/scan-workspace", methods=["POST"])
    @background_job
    def api_job_scan_workspace(ctx):
        """Queue an incremental scan across every root folder of the active
        workspace.

        Powers the "Rescan entire workspace" option. Roots that no longer
        resolve on disk are skipped (they're surfaced separately by the
        missing-folders flow) and reported back in ``skipped`` so the UI can
        be honest about what actually ran. Returns 400 with a message when no
        root is currently reachable, so the caller never shows "rescan
        started" for a no-op.
        """
        body = request.get_json(silent=True) or {}
        incremental = bool(body.get("incremental", True))
        db = get_db()
        roots = [r["path"] for r in db.get_workspace_folder_roots(ctx.workspace_id)]
        existing = [r for r in roots if os.path.isdir(r)]
        skipped = [r for r in roots if not os.path.isdir(r)]
        if not existing:
            if roots:
                return json_error("no workspace folders are currently on disk")
            return json_error("this workspace has no folders to rescan")
        # A staged root is scanned at its local path, which is safe; a root
        # that contains another workspace's staged folder would walk that
        # folder's original source and duplicate its catalog rows. Also
        # include queued folder-stage jobs so a stage waiting for its
        # worker cannot slip through the mapping-row window. Hold
        # ``stage_boundary_lock`` across ``ctx.start`` so a stage
        # registering after us cannot miss the scan in ``_busy_job``.
        work = build_scan_work(existing, incremental, ctx.workspace_id)
        job_config = {"roots": existing, "incremental": incremental}
        if len(existing) == 1:
            job_config["root"] = existing[0]
        with stage_boundary_lock():
            pending_sources = stage_pending_source_paths(
                ctx.runner.list_jobs if ctx.runner is not None else None, db,
            )
            conflict = local_copy_scan_conflict(
                db, existing,
                active_workspace_id=ctx.workspace_id,
                pending_stage_sources=pending_sources,
            )
            if conflict:
                return json_error(conflict, 409)
            return ctx.start(
                "scan", work, config=job_config,
                pausable=True,
                extra={"roots": existing, "skipped": skipped},
            )

    @blueprint.route("/api/jobs/repair-metadata", methods=["POST"])
    @background_job
    def api_job_repair_metadata(ctx):
        """Incrementally rescan reachable roots that contain missing EXIF."""
        from image_loader import is_excluded_scan_path
        from metadata import exiftool_status

        status = exiftool_status()
        if not status["available"]:
            return jsonify({
                "error": "Repair ExifTool before repairing photo metadata.",
                "code": "exiftool_required",
                "exiftool": status,
            }), 409

        db = get_db()
        roots = [r["path"] for r in db.get_workspace_folder_roots(ctx.workspace_id)]
        # See api_import_readiness for why excluded bundles must be filtered
        # before os.path.isdir here (macOS TCC prompt on Photos Library).
        existing = [
            root for root in roots
            if not is_excluded_scan_path(root) and os.path.isdir(root)
        ]
        if not existing:
            return json_error("no metadata-repair folders are currently on disk")
        # A reachable root can contain another workspace's staged descendant.
        # Its originals still sit at the source path with ``exif_data IS NULL``,
        # but staging rebased the catalog rows to the local copy: walking the
        # root here would let the scanner recreate the original-path rows the
        # local copy replaced. Match the ``/api/jobs/scan-workspace`` guard,
        # including queued folder-stage jobs whose mapping row does not exist
        # yet. The conflict check must precede the "no photos need metadata
        # repair" fast-path so a repair against a staged root is reported as
        # a local-copy conflict rather than as a no-op.
        with stage_boundary_lock():
            pending_sources = stage_pending_source_paths(
                ctx.runner.list_jobs if ctx.runner is not None else None, db,
            )
            conflict = local_copy_scan_conflict(
                db, existing,
                active_workspace_id=ctx.workspace_id,
                pending_stage_sources=pending_sources,
            )
        if conflict:
            return json_error(conflict, 409)
        # Count against the actually-reachable roots so the response's
        # ``photo_count`` matches what the job will process. An unscoped
        # count could report photos under an offline sibling root that
        # this job will never touch.
        repair_count = metadata_repair_count(db, ctx.workspace_id, existing)
        if not repair_count:
            return json_error("no photos need metadata repair", 409)

        work = build_scan_work(
            existing, True, ctx.workspace_id, repair_missing_metadata=True,
        )
        job_config = {
            "roots": existing,
            "incremental": True,
            "repair_metadata": True,
            "repair_photo_count": repair_count,
        }
        if len(existing) == 1:
            job_config["root"] = existing[0]
        # Re-check under ``stage_boundary_lock`` and register atomically so a
        # stage that lands between the checks and our ``ctx.start`` is seen
        # by ``_busy_job`` on the next stage admission.
        with stage_boundary_lock():
            pending_sources = stage_pending_source_paths(
                ctx.runner.list_jobs if ctx.runner is not None else None, db,
            )
            conflict = local_copy_scan_conflict(
                db, existing,
                active_workspace_id=ctx.workspace_id,
                pending_stage_sources=pending_sources,
            )
            if conflict:
                return json_error(conflict, 409)
            return ctx.start(
                "metadata-repair", work, config=job_config, pausable=True,
                extra={"photo_count": repair_count, "roots": existing},
            )

    @blueprint.route("/api/jobs/previews", methods=["POST"])
    @background_job
    def api_job_previews(ctx):
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        requested_photo_ids = body.get("photo_ids")
        if requested_photo_ids is not None:
            if (
                not isinstance(requested_photo_ids, list)
                or any(
                    isinstance(photo_id, bool)
                    or not isinstance(photo_id, int)
                    or photo_id <= 0
                    for photo_id in requested_photo_ids
                )
            ):
                return json_error("photo_ids must be a list of positive integers")
            requested_photo_ids = list(dict.fromkeys(requested_photo_ids))
        if collection_id is not None and requested_photo_ids is not None:
            return json_error("collection_id and photo_ids cannot be combined")
        db = get_db()
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err

        def work(job):
            import contextlib

            import config as cfg

            thread_db = ctx.thread_db()
            vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
            # Use workspace-effective config so per-workspace preview_max_size
            # overrides are honored — otherwise precompute warms the wrong
            # size and /photos/<id>/full (which uses workspace overrides) still
            # misses on first view.
            effective = thread_db.get_effective_config(cfg.load())
            raw_size = effective.get("preview_max_size")
            if raw_size == 0:
                # "Full resolution" — /full redirects to /original so
                # there's no size-suffixed file to warm. Skip precompute
                # rather than produce untracked {id}.jpg files.
                job["_start_time"] = time.time()
                return {"generated": 0, "skipped": 0, "total": 0,
                        "note": "skipped (preview_max_size=0)"}
            max_size = int(raw_size or 1920)

            preview_quality = effective.get("preview_quality", 90)
            preview_dir = os.path.join(vireo_dir, "previews")
            os.makedirs(preview_dir, exist_ok=True)

            if requested_photo_ids is not None:
                photos = []
                for photo_id in requested_photo_ids:
                    scoped_photo = thread_db.get_photo(
                        photo_id, verify_workspace=True,
                    )
                    if scoped_photo is not None:
                        photos.append(scoped_photo)
            elif collection_id:
                photos = thread_db.get_collection_photos(collection_id, per_page=999999)
            else:
                photos = thread_db.get_photos(per_page=999999)

            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            generated = 0
            skipped = 0
            failed = 0
            job["_start_time"] = time.time()

            for i, photo in enumerate(photos):
                if ctx.runner.is_cancelled(job["id"]):
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
                        # Adopt untracked unedited files so precompute output is
                        # visible to eviction and /api/preview-cache.
                        # Best-effort: photo may be deleted mid-job (FK error).
                        with contextlib.suppress(Exception):
                            if cache_row is None:
                                thread_db.preview_cache_insert(
                                    photo["id"],
                                    max_size,
                                    os.path.getsize(cache_path),
                                )
                        continue

                if not os.path.exists(cache_path):
                    folder_path = folders.get(detail_photo["folder_id"])
                    try:
                        materialized = materialize_preview(
                            thread_db,
                            detail_photo,
                            folder_path,
                            size=max_size,
                            vireo_dir=vireo_dir,
                            preview_quality=preview_quality,
                            recipe=recipe,
                            cache_path=cache_path,
                        )
                    except (ArtifactProducerFailed, PreviewMaterializationError) as exc:
                        # A source we could not read is a failure, not a
                        # cache hit; counting it as skipped made an
                        # all-unreadable run look fully cached.
                        failed += 1
                        log.info(
                            "Preview warmup failed for photo %s: %s",
                            photo["id"], exc,
                        )
                    else:
                        if materialized.generated:
                            generated += 1
                        else:
                            skipped += 1

                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": i + 1,
                        "total": total,
                        "current_file": photo["filename"],
                        "rate": round(
                            (i + 1) / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Generating previews",
                    },
                )

            # Park a pause requested during the final iteration before the
            # eviction pass, which can unlink many previews and rewrite their
            # database rows. Without this checkpoint the accepted request
            # would remain ``pausing`` while eviction ran, and a cancel from
            # that state could leave the cache mid-eviction.
            if ctx.runner.is_cancelled(job["id"]):
                return {"generated": generated, "skipped": skipped,
                        "failed": failed, "total": total}

            # Run a single eviction pass at the end so the batch doesn't
            # fsync after every photo.
            evict_preview_cache_if_over_quota(thread_db, vireo_dir)

            return {"generated": generated, "skipped": skipped,
                    "failed": failed, "total": total}

        return ctx.start(
            "previews",
            work,
            pausable=True,
            config={
                "collection_id": collection_id,
                "photo_ids": requested_photo_ids,
            },
        )

    @blueprint.route("/api/jobs/ingest", methods=["POST"])
    @background_job
    def api_job_ingest(ctx):
        body = request.get_json(silent=True) or {}
        source = body.get("source", "")
        destination = body.get("destination", "")
        file_types = body.get("file_types", "both")
        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        skip_duplicates = body.get("skip_duplicates", True)
        verify_by_hash = bool(body.get("verify_by_hash"))

        if not source or not destination:
            return json_error("source and destination are required")
        from image_loader import is_excluded_scan_path
        # See api_job_scan for why this must run before os.path.isdir.
        if is_excluded_scan_path(source):
            return json_error(
                f"source is inside a macOS app-managed library and cannot "
                f"be imported: {source}"
            )
        if not os.path.isdir(source):
            return json_error(f"source directory not found: {source}")
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path")
        from ingest import _is_unsafe_path
        if folder_template and _is_unsafe_path(folder_template):
            return json_error("folder_template must be a relative path without '..' or backslashes")

        def work(job):
            from ingest import ingest as do_ingest

            thread_db = ctx.thread_db()

            job["_start_time"] = time.time()

            def progress_cb(current, total, filename):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": filename,
                        "rate": round(
                            current / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Importing photos",
                    },
                )

            result = do_ingest(
                source_dir=source,
                destination_dir=destination,
                db=thread_db,
                file_types=file_types,
                folder_template=folder_template,
                skip_duplicates=skip_duplicates,
                verify_by_hash=verify_by_hash,
                progress_callback=progress_cb,
                pause_callback=lambda: ctx.checkpoint(job),
            )
            return result

        return ctx.start(
            "ingest",
            work,
            pausable=True,
            config={
                "source": source,
                "destination": destination,
                "file_types": file_types,
                "folder_template": folder_template,
            },
        )

    @blueprint.route("/api/jobs/move-photos", methods=["POST"])
    @background_job
    def api_job_move_photos(ctx):
        """Move selected photos to a destination directory."""
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        destination = body.get("destination", "")
        rule_id = body.get("rule_id")

        # Photos are global but visibility is per workspace: a move rewrites
        # the photo's folder and links the destination only to the active
        # workspace, so an id from another workspace must be refused here,
        # not silently moved out from under that workspace.
        photo_ids, err = parse_selection_photo_ids(
            get_db(), body, json_error=json_error, limit=None,
        )
        if err is not None:
            return err
        if not destination:
            return json_error("destination required")
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path")

        import config as cfg
        effective_cfg = get_db().get_effective_config(cfg.load())
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""

        def work(job):
            from move import move_photos

            thread_db = ctx.thread_db()

            job["_start_time"] = time.time()
            job["progress"]["total"] = len(photo_ids)

            def progress_cb(current, total, filename):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                ctx.runner.push_event(job["id"], "progress", {
                    "current": current,
                    "total": total,
                    "current_file": filename,
                    "rate": round(
                        current / max(time.time() - job["_start_time"], 0.01), 1
                    ),
                    "phase": "Moving photos",
                })

            result = move_photos(
                db=thread_db,
                photo_ids=photo_ids,
                destination=destination,
                progress_cb=progress_cb,
                developed_dir=developed_dir,
                cancel_check=lambda: ctx.runner.cancellation_requested(job["id"]),
                pause_requested=lambda: ctx.runner.pause_requested(job["id"]),
                pause_callback=lambda: ctx.runner.is_cancelled(job["id"]),
            )
            if int(result.get("moved") or 0) > 0:
                try:
                    invalidate_missing_originals()
                except Exception:
                    log.exception(
                        "Failed to invalidate missing-originals cache "
                        "after move-photos job",
                    )

            if rule_id:
                thread_db.touch_move_rule(rule_id)

            return result

        return ctx.start(
            "move-photos", work, pausable=True,
            config={"photo_ids": photo_ids, "destination": destination},
        )

    @blueprint.route("/api/jobs/offline-cache", methods=["POST"])
    @background_job
    def api_job_offline_cache(ctx):
        """Copy selected originals into Vireo's managed offline cache."""
        body = request.get_json(silent=True) or {}
        # Flask returns top-level JSON lists/numbers/strings as-is, so guard
        # against `body.get(...)` raising AttributeError on non-object payloads
        # (e.g. a client posting `[]` directly).
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        raw_ids = body.get("photo_ids", [])
        collection_id = coerce_collection_id(body.get("collection_id"))
        if collection_id is False:
            return json_error("collection_id must be an integer")
        if raw_ids and not isinstance(raw_ids, list):
            return json_error("photo_ids must be a list of integers")

        db = get_db()
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        if not raw_ids and collection_id is not None:
            raw_ids = [
                p["id"]
                for p in db.get_collection_photos(collection_id, per_page=999999)
            ]
        if not raw_ids:
            return json_error("photo_ids or collection_id required")
        photo_ids = []
        for pid in raw_ids:
            # `bool` is a subclass of `int`, and `int(1.9)` silently
            # truncates to `1` — reject both so the wrong photo can't be
            # cached without any client-visible error.
            if isinstance(pid, bool | float):
                return json_error("photo_ids must be integers")
            try:
                photo_ids.append(int(pid))
            except (ValueError, TypeError):
                return json_error("photo_ids must be integers")

        visible_set = set()
        for chunk in chunked(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                f"""SELECT p.id FROM photos p
                    JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    WHERE wf.workspace_id = ? AND p.id IN ({placeholders})""",
                [ctx.workspace_id, *chunk],
            ).fetchall()
            visible_set.update(r["id"] for r in rows)
        photo_ids = [pid for pid in photo_ids if pid in visible_set]
        if not photo_ids:
            return json_error("no cacheable photos in current workspace")

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])

        def work(job):
            from offline_cache import cache_photo_original

            thread_db = ctx.thread_db()
            photos_map = thread_db.get_photos_by_ids(photo_ids)
            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}

            cached = 0
            skipped = 0
            failed = 0
            copied_bytes = 0
            total = len(photo_ids)
            job["_start_time"] = time.time()
            job["progress"]["total"] = total

            for i, pid in enumerate(photo_ids):
                if ctx.runner.is_cancelled(job["id"]):
                    break
                photo = photos_map.get(pid)
                filename = photo["filename"] if photo else ""
                if not photo:
                    failed += 1
                    job["errors"].append(f"Photo {pid}: not found")
                else:
                    try:
                        result = cache_photo_original(
                            thread_db, photo, vireo_dir, folders
                        )
                        if result["status"] == "cached":
                            cached += 1
                            copied_bytes += int(result.get("bytes") or 0)
                        elif result["status"] == "skipped":
                            skipped += 1
                        else:
                            failed += 1
                            job["errors"].append(
                                f'{photo["filename"]}: {result["status"]}'
                            )
                    except Exception as exc:
                        thread_db.conn.rollback()
                        failed += 1
                        job["errors"].append(f'{photo["filename"]}: {exc}')
                        log.warning("Offline cache failed for %s: %s", filename, exc)

                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": i + 1,
                        "total": total,
                        "current_file": filename,
                        "rate": round(
                            (i + 1) / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Caching originals for offline use",
                    },
                )

            result = {
                "cached": cached,
                "skipped": skipped,
                "failed": failed,
                "total": total,
                "bytes": copied_bytes,
            }
            if failed:
                job["result"] = result
                first_err = job["errors"][0] if job["errors"] else "offline cache failed"
                job["_fatal_error"] = (
                    f"{failed}/{total} photos could not be cached: {first_err}"
                )
                raise RuntimeError(first_err)
            return result

        return ctx.start(
            "offline-cache",
            work,
            pausable=True,
            config={"photo_ids": photo_ids},
        )

    @blueprint.route("/api/jobs/prepare-full-resolution", methods=["POST"])
    @background_job
    def api_job_prepare_full_resolution(ctx):
        """Prepare selected photos for uninterrupted full-resolution review.

        The job first copies source assets into Vireo's managed local cache,
        then drives the exact /original render path used by the lightbox. RAW
        working copies and edited full-resolution renders are therefore ready
        before the user begins inspecting the selection.
        """
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        raw_ids = body.get("photo_ids")
        if not isinstance(raw_ids, list) or not raw_ids:
            return json_error("photo_ids must be a non-empty list of integers")

        photo_ids = []
        seen = set()
        for raw_id in raw_ids:
            if isinstance(raw_id, bool | float):
                return json_error("photo_ids must contain only integers")
            try:
                photo_id = int(raw_id)
            except (TypeError, ValueError):
                return json_error("photo_ids must contain only integers")
            if photo_id not in seen:
                seen.add(photo_id)
                photo_ids.append(photo_id)

        db = get_db()
        visible_set = set()
        for chunk in chunked(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                f"""SELECT p.id FROM photos p
                    JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    WHERE wf.workspace_id = ? AND p.id IN ({placeholders})""",
                [ctx.workspace_id, *chunk],
            ).fetchall()
            visible_set.update(row["id"] for row in rows)
        photo_ids = [photo_id for photo_id in photo_ids if photo_id in visible_set]
        if not photo_ids:
            return json_error("no photos in the current workspace can be prepared")

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        # Preserve the selected source identity even if SQLite recycles its
        # numeric ID before this job reaches it.
        selected_photos = db.get_photos_by_ids(photo_ids)
        # The worker renders through the app object from a job
        # thread, which has no application context of its own.
        flask_app = current_app._get_current_object()

        def work(job):
            from offline_cache import (
                cache_photo_original,
                cleanup_preparation_offline_files,
                original_preparation_guard,
                photo_source_matches,
            )

            thread_db = ctx.thread_db()
            try:
                folders = {
                    folder["id"]: folder["path"]
                    for folder in thread_db.get_folder_tree()
                }
                ready = 0
                copied = 0
                reused = 0
                failed = 0
                skipped_deleted = 0
                copied_bytes = 0
                total = len(photo_ids)
                job["_start_time"] = time.time()
                job["progress"]["total"] = total

                for index, photo_id in enumerate(photo_ids, start=1):
                    if ctx.runner.is_cancelled(job["id"]):
                        break
                    # The lock must precede database transactions: other
                    # writers publish files before inserting their cache row.
                    with original_preparation_guard(vireo_dir, photo_id):
                        # Re-read each photo: deletion may have run while this
                        # job was preparing earlier selections.
                        photo = selected_photos.get(photo_id)
                        current_photo = thread_db.get_photo(photo_id)
                        filename = photo["filename"] if photo else f"Photo {photo_id}"
                        error = None
                        cached = None
                        attempted = photo_source_matches(photo, current_photo)
                        if attempted:
                            try:
                                cached = cache_photo_original(
                                    thread_db, photo, vireo_dir, folders,
                                )
                                cache_status = cached.get("status")
                                if cache_status not in ("cached", "skipped"):
                                    error = cache_status or "source could not be cached"

                                if error is None and photo_source_matches(
                                    photo, thread_db.get_photo(photo_id),
                                ):
                                    # Execute the canonical renderer inside an
                                    # isolated request context. This avoids a
                                    # second implementation drifting from the
                                    # lightbox's RAW/companion/edit fallbacks.
                                    with flask_app.test_request_context(
                                        f"/photos/{photo_id}/original"
                                    ):
                                        request_db = get_db()
                                        request_db.set_active_workspace(ctx.workspace_id)
                                        response = flask_app.make_response(
                                            serve_original_photo(photo_id, _prepare_source=photo)
                                        )
                                        try:
                                            if not 200 <= response.status_code < 300:
                                                response.direct_passthrough = False
                                                detail = response.get_data(
                                                    as_text=True,
                                                ).strip()
                                                error = detail or (
                                                    "full-resolution render failed "
                                                    f"with status {response.status_code}"
                                                )
                                        finally:
                                            response.close()
                            except Exception as exc:
                                thread_db.conn.rollback()
                                error = str(exc) or exc.__class__.__name__
                                if photo_source_matches(photo, thread_db.get_photo(photo_id)):
                                    log.warning(
                                        "Full-resolution preparation failed for %s: %s",
                                        filename, exc, exc_info=True,
                                    )

                        # Serialize this final identity check and cache cleanup
                        # against catalog writers. An import can reuse a deleted
                        # ID; any cache written by this iteration is then suspect,
                        # including the offline row attached to that replacement.
                        # Purge disposable caches only if we actually attempted
                        # work, leaving replacements found before our turn alone.
                        with thread_db.conn:
                            thread_db.conn.execute("BEGIN IMMEDIATE")
                            current_photo = thread_db.get_photo(photo_id)
                            if not attempted or not photo_source_matches(photo, current_photo):
                                from preview_cache import cleanup_cached_files_for_deleted_photos

                                skipped_deleted += 1
                                if attempted:
                                    thread_db.conn.execute(
                                        "DELETE FROM offline_originals WHERE photo_id=?",
                                        (photo_id,),
                                    )
                                    if current_photo is None:
                                        cleanup_cached_files_for_deleted_photos(
                                            config["THUMB_CACHE_DIR"],
                                            [{"photo_id": photo_id}],
                                            vireo_dir=vireo_dir,
                                        )
                                    else:
                                        # Other cache families may already
                                        # belong to the replacement photo.
                                        cleanup_preparation_offline_files(
                                            vireo_dir, photo_id,
                                        )
                            elif error is None:
                                ready += 1
                                if cached["status"] == "cached":
                                    copied += 1
                                    copied_bytes += int(cached.get("bytes") or 0)
                                elif cached["status"] == "skipped":
                                    reused += 1
                            else:
                                failed += 1
                                job["errors"].append(f"{filename}: {error}")

                    progress = {
                        "current": index,
                        "total": total,
                        "current_file": filename,
                        "rate": round(
                            index
                            / max(time.time() - job["_start_time"], 0.01),
                            1,
                        ),
                        "phase": "Preparing full-resolution photos",
                    }
                    job["progress"].update(progress)
                    ctx.runner.push_event(job["id"], "progress", progress)

                return {
                    "ok": failed == 0,
                    "ready": ready,
                    "copied": copied,
                    "reused": reused,
                    "failed": failed,
                    "skipped_deleted": skipped_deleted,
                    "total": total,
                    "bytes": copied_bytes,
                    "errors": list(job["errors"]),
                }
            finally:
                thread_db.close()

        return ctx.start(
            "prepare-full-resolution",
            work,
            pausable=True,
            config={"photo_ids": photo_ids},
            extra={"total": len(photo_ids)},
        )

    @blueprint.route("/api/jobs/move-folder", methods=["POST"])
    @background_job
    def api_job_move_folder(ctx):
        """Move an entire folder to a destination."""
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("JSON body must be an object")
        folder_id = body.get("folder_id")
        destination = body.get("destination", "")
        destination_name_raw = body.get("destination_name", "")
        folder_template_raw = body.get("folder_template", "")
        remote_target_id = (body.get("remote_target_id") or "").strip()
        subpath = body.get("subpath", "")
        merge_raw = body.get("merge", False)
        if not isinstance(merge_raw, bool):
            return json_error("merge must be a boolean")
        merge = merge_raw

        if not folder_id:
            return json_error("folder_id required")
        if not isinstance(folder_template_raw, str):
            return json_error("folder_template must be a string")
        folder_template = folder_template_raw.strip()
        if folder_template and destination_name_raw:
            return json_error(
                "destination_name cannot be combined with folder_template"
            )

        request_db = get_db()
        folder = request_db.conn.execute(
            "SELECT path, name FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if not folder:
            return json_error("Folder not found", status=404)

        guard_err = guard_move_folder(request_db, folder_id)
        if guard_err is not None:
            return json_error(guard_err, 409)

        import config as cfg
        import move as move_mod
        try:
            destination_name = move_mod.normalize_destination_name(
                destination_name_raw)
        except ValueError as exc:
            return json_error(str(exc))
        effective_cfg = request_db.get_effective_config(cfg.load())
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""

        # Remote (SSH) destination vs local path. A remote target carries its
        # own destination (remote_path + optional subpath), so `destination`
        # is not required in that case.
        remote = None
        if remote_target_id:
            if folder_template:
                return json_error(
                    "Organizing one folder into capture-date folders is only "
                    "available for local or mounted-drive destinations."
                )
            target = cfg.get_remote_target(remote_target_id)
            if not target:
                return json_error("Remote target not found", status=404)
            mount_path = (target.get("mount_path") or "").strip()
            if not mount_path:
                return json_error(
                    "This remote target has no local mount path, so moved "
                    "photos couldn't stay in your library. Add a mount path "
                    "under Settings → Remote targets."
                )
            # Reject a relative mount path (e.g. saved as "Photos" instead of
            # "/Volumes/Photos") before any transfer runs. move_folder also
            # refuses as a defense-in-depth check, but failing here gives the
            # UI a clean error instead of starting a job that immediately
            # reports failure.
            if not os.path.isabs(mount_path):
                return json_error(
                    "This remote target's local mount path isn't absolute "
                    f"(\"{mount_path}\"). Moved photos would be repointed "
                    "to a path relative to the server's working directory "
                    "and appear missing. Set an absolute mount path under "
                    "Settings → Remote targets."
                )
            rsync_bin = move_mod.resolve_rsync_bin(
                effective_cfg.get("rsync_bin", "") or "")
            if not rsync_bin:
                return json_error(
                    "No usable GNU rsync was found for remote moves. Install "
                    "GNU rsync for your platform or set its executable under "
                    "Settings → Paths."
                )
            ssh_bin = move_mod.resolve_ssh_bin(
                effective_cfg.get("ssh_bin", "") or "")
            if not ssh_bin:
                return json_error(
                    "OpenSSH Client was not found. Install the Windows "
                    "OpenSSH Client optional feature or configure ssh.exe in Settings."
                )
            try:
                remote = move_mod.build_remote_move_spec(
                    target, subpath, rsync_bin, ssh_bin)
            except ValueError as exc:
                return json_error(str(exc))
            # Pass the mount path as `destination` for informational use; the
            # move uses the SSH base from `remote`.
            destination = remote["mount_dest_base"]
            display_dest = move_mod.rsync_dest_spec(
                target, remote["ssh_dest_base"])
        else:
            if not isinstance(destination, str):
                return json_error("destination must be a string")
            if not destination:
                return json_error("destination required")
            if not os.path.isabs(destination):
                return json_error("destination must be an absolute path")
            display_dest = destination

        date_plan = None
        date_destinations = None
        if folder_template:
            try:
                date_plan = move_mod.plan_folder_date_moves(
                    request_db, folder_id, destination, folder_template,
                )
            except ValueError as exc:
                return json_error(str(exc))
            if not date_plan:
                return json_error("No tracked photos found in the source folder")
            date_destinations = [
                {
                    "path": item["destination"],
                    "relative_path": item["relative_path"],
                    "photo_count": item["photo_count"],
                }
                for item in date_plan
            ]

        if folder_template:
            # A date-organized move fans out into one folder per capture date.
            # When the plan resolves to a single date folder — the common case
            # for a one-shoot source folder — that folder *is* where every
            # photo lands, so show it in full instead of the selected root the
            # user would otherwise read as the landing path. With several date
            # folders there is no single landing path; keep the root and let
            # the jobs panel list the folders underneath it.
            resolved_destination = (
                date_destinations[0]["path"] if len(date_destinations) == 1
                else display_dest
            )
        elif remote:
            import posixpath

            landing_name = destination_name or folder["name"] \
                or os.path.basename(folder["path"].rstrip("/\\"))
            resolved_destination = move_mod.rsync_dest_spec(
                target,
                posixpath.join(remote["ssh_dest_base"], landing_name),
            )
        else:
            resolved_destination = move_mod.resolve_folder_dest(
                folder["path"], folder["name"], destination,
                destination_name,
            )

        job_id = start_move_folder_job(
            ctx.runner, ctx.workspace_id,
            folder_id=folder_id,
            destination=destination,
            display_dest=display_dest,
            destination_name=destination_name,
            source_path=folder["path"],
            resolved_destination=resolved_destination,
            merge=merge,
            remote=remote,
            developed_dir=developed_dir,
            folder_template=folder_template,
            date_destinations=date_destinations,
        )
        return jsonify({"job_id": job_id})

    @blueprint.route("/api/jobs/sync", methods=["POST"])
    @background_job
    def api_job_sync(ctx):
        body = request.get_json(silent=True)
        if body is None:
            body = {}
        elif not isinstance(body, dict):
            return json_error("request body must be a JSON object", 400)

        raw_change_ids = body.get("change_ids")
        change_ids = None
        if raw_change_ids is not None:
            if not isinstance(raw_change_ids, list):
                return json_error("change_ids must be a list", 400)
            if not raw_change_ids:
                return json_error("change_ids required", 400)
            if len(raw_change_ids) > 50000:
                return json_error("too many change_ids", 400)
            change_ids = []
            seen = set()
            for raw in raw_change_ids:
                if isinstance(raw, bool):
                    return json_error("change_ids must be integers", 400)
                try:
                    cid = int(raw)
                except (TypeError, ValueError):
                    return json_error("change_ids must be integers", 400)
                if cid <= 0:
                    return json_error("change_ids must be positive integers", 400)
                if cid not in seen:
                    seen.add(cid)
                    change_ids.append(cid)

        def work(job):
            from sync import sync_to_xmp

            thread_db = ctx.thread_db()

            def status_cb(progress):
                ctx.runner.push_event(
                    job["id"], "progress",
                    {**progress, "current_file": "Writing XMP metadata...",
                     "phase": "Writing XMP metadata"},
                )

            lock_acquired = sync_job_lock.acquire(blocking=False)
            if not lock_acquired:
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": "Waiting for current XMP sync...",
                        "phase": "Waiting for current XMP sync",
                    },
                )
                while not lock_acquired:
                    if ctx.runner.is_cancelled(job["id"]):
                        return {"synced": 0, "failed": 0, "failures": [],
                                "ok": True, "errors": []}
                    lock_acquired = sync_job_lock.acquire(timeout=0.1)

            try:
                if ctx.runner.is_cancelled(job["id"]):
                    return {"synced": 0, "failed": 0, "failures": [],
                            "ok": True, "errors": []}
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": "Preparing XMP sync...",
                        "phase": "Preparing XMP sync",
                    },
                )
                return sync_to_xmp(
                    thread_db,
                    status_callback=status_cb,
                    change_ids=change_ids,
                )
            finally:
                sync_job_lock.release()

        config = {"change_ids": change_ids} if change_ids is not None else {}
        return ctx.start("sync", work, config=config)

    @blueprint.route("/api/jobs/classify", methods=["POST"])
    @background_job
    def api_job_classify(ctx):
        import config as cfg
        from classify_job import ClassifyParams, run_classify_job

        db = get_db()
        user_cfg = db.get_effective_config(cfg.load())
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")

        if not collection_id:
            return json_error("collection_id required")
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err

        params = ClassifyParams(
            collection_id=collection_id,
            labels_file=body.get("labels_file"),
            labels_files=body.get("labels_files"),
            model_id=body.get("model_id"),
            model_name=body.get("model_name"),
            grouping_window=body.get(
                "grouping_window", user_cfg["grouping_window_seconds"]
            ),
            similarity_threshold=body.get(
                "similarity_threshold", user_cfg.get("similarity_threshold", 0.85)
            ),
            reclassify=body.get("reclassify", False),
        )

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        work_units = runtime_warning_work_units(
            "classify collection",
            lambda: db.count_collection_photos(collection_id),
        )
        runtime_warning = build_cpu_runtime_warning(
            "classify",
            work_units=work_units,
            reason="large_classification_job_cpu_only",
        )

        def work(job):
            return run_classify_job(
                job, ctx.runner, db_path, ctx.workspace_id, params,
                vireo_dir=vireo_dir,
                computation_cache_dir=config["COMPUTATION_CACHE_DIR"],
            )

        return ctx.start(
            "classify",
            work,
            pausable=True,
            config={
                "collection_id": collection_id,
                "model_name": params.model_name,
            },
            runtime_warning=runtime_warning,
        )

    @blueprint.route("/api/jobs/develop", methods=["POST"])
    @background_job
    def api_job_develop(ctx):
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        if not photo_ids:
            return json_error("photo_ids required")

        import config as cfg
        from develop import find_darktable

        darktable_bin = cfg.get("darktable_bin")
        binary = find_darktable(darktable_bin)
        if not binary:
            return json_error("darktable-cli not found. Configure the path in Settings.")

        style = body.get("style") or cfg.get("darktable_style") or ""
        output_format = body.get("output_format") or cfg.get("darktable_output_format") or "jpg"
        output_dir = body.get("output_dir") or cfg.get("darktable_output_dir") or ""
        auto_convert_dng = body.get("auto_convert_dng")
        if auto_convert_dng is None:
            auto_convert_dng = cfg.get("darktable_auto_convert_dng")
        dng_converter_bin = body.get("dng_converter_bin") or cfg.get("dng_converter_bin") or ""
        width = body.get("width")

        def work(job):
            from develop import develop_photo, output_path_for_photo
            from export import developed_folder_key

            thread_db = ctx.thread_db()

            photos = []
            for pid in photo_ids:
                p = thread_db.get_photo(pid, verify_workspace=True)
                if p:
                    photos.append(p)

            if not photos:
                return {"developed": 0, "errors": 0, "total": 0}

            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            developed = 0
            errors = 0
            job["_start_time"] = time.time()

            for i, photo in enumerate(photos):
                if ctx.runner.is_cancelled(job["id"]):
                    break
                folder_path = folders.get(photo["folder_id"])
                if not folder_path:
                    errors += 1
                    continue
                input_path = os.path.join(folder_path, photo["filename"])

                # Determine output directory. The per-folder "developed/" default
                # is naturally disambiguated (one dir per source folder). The
                # globally configured dir is flat, so nest each photo under a
                # stable key derived from the source folder's path (not its row
                # id — SQLite reuses those after deletion, which would silently
                # cross-wire new folders onto stale developed files left on
                # disk by a previously-deleted folder).
                if output_dir:
                    out_dir = os.path.join(output_dir, developed_folder_key(folder_path))
                else:
                    out_dir = os.path.join(folder_path, "developed")
                out_path = output_path_for_photo(photo["filename"], out_dir, output_format)

                try:
                    photo_metadata = json.loads(photo["exif_data"]) if photo["exif_data"] else None
                except (TypeError, json.JSONDecodeError):
                    photo_metadata = None

                result = develop_photo(
                    darktable_bin=binary,
                    input_path=input_path,
                    output_path=out_path,
                    style=style if style else None,
                    width=width,
                    auto_convert_dng=bool(auto_convert_dng),
                    dng_converter_bin=dng_converter_bin,
                    metadata=photo_metadata,
                )

                if result["success"]:
                    developed += 1
                else:
                    errors += 1
                    job["errors"].append(f'{photo["filename"]}: {result["error"]}')
                    log.warning("Failed to develop %s: %s", photo["filename"], result["error"])

                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": i + 1,
                        "total": total,
                        "current_file": photo["filename"],
                        "rate": round(
                            (i + 1) / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Developing photos",
                    },
                )

            result = {"developed": developed, "errors": errors, "total": total}
            if errors > 0:
                # Rollup rule: if any per-photo develop failed, the overall
                # job is failed (not completed). Stash the counts on the job
                # so _persist_job (jobs.py ~L197-210) merges them with the
                # primary error message, then raise so _run_job records
                # status="failed".
                #
                # Re-raise with an existing entry in job["errors"] so
                # _run_job's dedup guard (err_str not in job["errors"])
                # skips the append — otherwise a novel exception string
                # gets tacked on as a synthetic extra error, inflating
                # error_count by 1. Keep the nicer "N/M failed: <err>"
                # summary on job["_fatal_error"], which _persist_job
                # prefers over errors[0] when building the result row.
                job["result"] = result
                first_err = job["errors"][0]
                job["_fatal_error"] = (
                    f"{errors}/{total} develop operations failed: {first_err}"
                )
                raise RuntimeError(first_err)
            return result

        return ctx.start(
            "develop",
            work,
            pausable=True,
            config={
                "photo_ids": photo_ids,
                "style": style,
                "output_format": output_format,
            },
        )

    @blueprint.route("/api/jobs/extract-masks", methods=["POST"])
    @background_job
    def api_job_extract_masks(ctx):
        """Run SAM2 mask extraction as a background job.

        Requires MegaDetector detections to already be computed (run classify first).
        For each photo with a detection but no mask, loads a working-resolution proxy,
        runs SAM2 to refine the bounding box into a pixel mask, and saves it.
        """
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")

        import config as cfg

        db = get_db()
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        effective_cfg = db.get_effective_config(cfg.load())
        pipeline_cfg = effective_cfg.get("pipeline", {})
        sam2_variant = pipeline_cfg.get("sam2_variant")
        dinov2_variant = pipeline_cfg.get("dinov2_variant")
        proxy_longest_edge = pipeline_cfg.get("proxy_longest_edge")
        # Workspace-effective detector_confidence floor. Both the
        # collection branch (via get_detections) and the workspace SQL
        # below filter on this so we don't run SAM/DINO on noisy
        # below-threshold boxes — matching get_photos_missing_masks.
        min_detector_conf = effective_cfg.get("detector_confidence", 0.2)

        work_units = runtime_warning_work_units(
            "extract-masks collection" if collection_id else "extract-masks workspace",
            lambda: db.count_collection_photos(collection_id)
            if collection_id
            else db.count_photos(),
        )
        runtime_warning = build_cpu_runtime_warning(
            "extract-masks",
            work_units=work_units,
            reason="large_segmentation_job_cpu_only",
        )

        def work(job):
            import numpy as np
            from dino_embed import embed, embed_batch, embedding_to_blob
            from masking import (
                crop_completeness,
                crop_subject,
                generate_mask,
                render_proxy,
                save_mask,
            )
            from pipeline_job import (
                _rollback_failed_mask_photo,
                _StagedMaskFile,
            )
            from pipeline_locks import acquire_photo_mask
            from quality import compute_all_quality_features
            from resource_ledger import ResourceWaitCancelled
            from subjects import primary_order_sql, sync_primary

            thread_db = ctx.thread_db()

            masks_dir = os.path.join(os.path.dirname(db_path), "masks")
            os.makedirs(masks_dir, exist_ok=True)

            # Get photos that have a real (non full-image) detection. The
            # legacy `mask_path IS NULL` gate would silently skip every
            # photo whose stored mask was made by a *different* SAM
            # variant — leaving photo_masks empty for the configured
            # variant. The per-photo cache check inside the loop below
            # against photo_masks(photo_id, sam2_variant) handles
            # "already cached for this variant" correctly.
            if collection_id:
                coll_photos = thread_db.get_collection_photos(
                    collection_id, per_page=999999
                )
                photos = []
                for p in coll_photos:
                    dets = [
                        d for d in thread_db.get_detections(
                            p["id"], min_conf=min_detector_conf,
                        )
                        if d["detector_model"] != "full-image"
                    ]
                    if dets:
                        det = dets[0]
                        photos.append({
                            "id": p["id"],
                            "folder_id": p["folder_id"],
                            "filename": p["filename"],
                            "detector_model": det["detector_model"],
                            "detection_box": json.dumps({
                                "x": det["box_x"], "y": det["box_y"],
                                "w": det["box_w"], "h": det["box_h"],
                            }),
                            "detection_conf": det["detector_confidence"],
                            # Full-precision REAL prompt — see pipeline_job
                            # for the rationale; int() would truncate the
                            # normalized [0,1] bbox to (0,0,0,0) and break
                            # cache invalidation on bbox change.
                            "prompt": (
                                det["box_x"], det["box_y"],
                                det["box_w"], det["box_h"],
                            ),
                        })
            else:
                # All workspace photos with at least one real
                # (non full-image) detection. Direct SQL keeps this
                # one round-trip to SQLite per workspace; the per-photo
                # cache check inside the loop handles "skip when
                # already masked for this variant".
                ws_id = thread_db._active_workspace_id
                rows = thread_db.conn.execute(
                    f"""SELECT p.id, p.folder_id, p.filename,
                              d.detector_model,
                              d.box_x, d.box_y, d.box_w, d.box_h,
                              d.detector_confidence
                         FROM photos p
                         JOIN workspace_folders wf
                              ON wf.folder_id = p.folder_id
                         JOIN detections d ON d.photo_id = p.id
                        WHERE wf.workspace_id = ?
                          AND d.detector_model != 'full-image'
                          AND d.detector_confidence >= ?
                        ORDER BY p.id, {primary_order_sql("d")}""",
                    (ws_id, min_detector_conf),
                ).fetchall()
                seen = set()
                photos = []
                for r in rows:
                    if r["id"] in seen:
                        continue
                    seen.add(r["id"])
                    photos.append({
                        "id": r["id"],
                        "folder_id": r["folder_id"],
                        "filename": r["filename"],
                        "detector_model": r["detector_model"],
                        "detection_box": json.dumps({
                            "x": r["box_x"], "y": r["box_y"],
                            "w": r["box_w"], "h": r["box_h"],
                        }),
                        "detection_conf": r["detector_confidence"],
                        "prompt": (
                            r["box_x"], r["box_y"],
                            r["box_w"], r["box_h"],
                        ),
                    })

            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            masked = 0
            skipped = 0
            failed = 0
            # A source file that never opened is not the same as "SAM found
            # no subject here", even though both leave the photo unmasked.
            # Only the second is an answer about the photo; the first means
            # we never looked. Folding them together let a dropped share
            # finish this job green while every unmasked photo went on to be
            # hard-rejected in Process Review as `no_subject_mask`
            # (Codex #1392 P1). Mirrors extract_masks_stage in pipeline_job.
            unreadable = 0
            job["_start_time"] = time.time()

            for i, photo in enumerate(photos):
                if ctx.runner.is_cancelled(job["id"]):
                    break
                photo_id = photo["id"]
                folder_path = folders.get(photo["folder_id"], "")
                image_path = os.path.join(folder_path, photo["filename"])

                # Share the same photo-wide critical section as Process jobs.
                # This standalone route writes the deterministic predecessor
                # filename, so serializing its full cache-check/write sequence
                # lets Process safely reclaim that predecessor after commit.
                photo_mask_lock = acquire_photo_mask(photo_id)
                while not ctx.runner.is_cancelled(job["id"]):
                    if not photo_mask_lock.acquire(timeout=0.1):
                        continue
                    # A pause can arrive during acquisition. Release before
                    # parking so another Process job can use this photo.
                    if (ctx.runner.pause_requested(job["id"])
                            or ctx.runner.cancellation_requested(job["id"])):
                        photo_mask_lock.release()
                        continue
                    break
                else:
                    break
                mask_file_stage = None
                try:
                    # Sync the primary before deciding whether a cached
                    # mask still applies. A workspace confidence-floor
                    # change (or another workspace sharing this photo
                    # writing photo_subject_state with a different floor)
                    # can promote a new primary since this job was queued;
                    # sync_primary clears mask_path, active_mask_variant,
                    # dino_subject_embedding, and eye_*/eye_kp_fingerprint
                    # when the primary detection actually changed, so the
                    # subsequent eye stage recomputes for the newly
                    # published subject instead of retaining the previous
                    # subject's eye focus (Codex r4056563009).
                    sync_primary(thread_db, photo_id, min_conf=min_detector_conf)
                    commit_with_retry(thread_db.conn)
                    # A user can change primary after this job was queued.
                    current = [d for d in thread_db.get_detections(photo_id, min_conf=min_detector_conf)
                               if d["detector_model"] != "full-image"]
                    if not current:
                        skipped += 1
                        continue
                    selected = current[0]
                    photo["detector_model"] = selected["detector_model"]
                    photo["prompt"] = tuple(selected["box_" + k] for k in "xywh")
                    photo["detection_box"] = {k: selected["box_" + k] for k in "xywh"}
                    # Cache hit: photo_masks already has a row for
                    # (photo, configured variant) AND its stored prompt
                    # + detector still match the current primary
                    # detection AND the file is on disk AND the photos row
                    # is already fully consistent for this (sam, dino)
                    # pair. A subject A→B→A round-trip clears
                    # dino_subject_embedding via sync_primary while the
                    # matching photo_masks row for A remains cached; a
                    # bare cache-hit shortcut would then re-activate the
                    # mask and skip DINO, leaving dino_subject_embedding
                    # null even though the job reported the photo
                    # processed. Mirror the Process pipeline's
                    # active_mask_variant/dino_embedding_variant guard
                    # (Codex r4056402007).
                    existing = thread_db.get_photo_mask(
                        photo_id, sam2_variant,
                    )
                    if existing is not None:
                        cached_prompt = (
                            existing["prompt_x"], existing["prompt_y"],
                            existing["prompt_w"], existing["prompt_h"],
                        )
                        if (existing["detector_model"]
                                == photo["detector_model"]
                                and cached_prompt == photo["prompt"]
                                and existing["path"]
                                and os.path.isfile(existing["path"])):
                            state = thread_db.conn.execute(
                                "SELECT active_mask_variant, "
                                "dino_embedding_variant, quality_input_recipe FROM photos "
                                "WHERE id = ?",
                                (photo_id,),
                            ).fetchone()
                            if (state is not None
                                    and state["active_mask_variant"]
                                    == sam2_variant
                                    and state["dino_embedding_variant"]
                                    == dinov2_variant
                                    and state["quality_input_recipe"] is None):
                                masked += 1
                                ctx.runner.push_event(
                                    job["id"],
                                    "progress",
                                    {
                                        "current": i + 1,
                                        "total": total,
                                        "current_file": photo["filename"],
                                        "rate": round(
                                            (i + 1) / max(
                                                time.time() - job["_start_time"], 0.01,
                                            ),
                                            1,
                                        ),
                                        "phase": "Extracting features (SAM2 + DINOv2)",
                                    },
                                )
                                continue
                            # Denormalised subject state is stale: fall
                            # through to the full recompute below, which
                            # writes set_active_mask_variant +
                            # update_photo_embeddings atomically.

                    # Load working-resolution proxy
                    proxy = render_proxy(image_path, longest_edge=proxy_longest_edge)
                    if proxy is None:
                        unreadable += 1
                        continue

                    # Parse detection box
                    det_box = photo["detection_box"]
                    if isinstance(det_box, str):
                        det_box = json.loads(det_box)

                    # Generate mask via SAM2
                    mask = generate_mask(proxy, det_box, variant=sam2_variant)
                    if mask is None:
                        skipped += 1
                        continue

                    # Compute crop completeness + all quality features
                    completeness = crop_completeness(mask)
                    features = compute_all_quality_features(proxy, mask)

                    # Compute DINOv2 embeddings — one batched call when both
                    # subject and global are needed, since DINOv2 is CPU-only
                    # for external-data ONNX exports and per-call overhead is
                    # the bottleneck.
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

                    mask_file_stage = _StagedMaskFile.create(
                        mask,
                        masks_dir,
                        photo_id,
                        sam2_variant,
                        save_mask,
                        previous_path=(
                            existing["path"] if existing else None
                        ),
                    )
                    mask_path = mask_file_stage.final_path

                    # Per-mask features: pop them out of `features` so
                    # they land on the photo_masks row and not on the
                    # photos row directly. set_active_mask_variant
                    # denormalizes them back into photos for downstream
                    # readers.
                    mask_subject_tenengrad = features.pop(
                        "subject_tenengrad", None,
                    )
                    mask_bg_tenengrad = features.pop("bg_tenengrad", None)
                    # Mask-derived subject_size: fraction of frame
                    # covered by the boolean mask.
                    total_pixels = float(mask.size)
                    if total_pixels > 0:
                        mask_subject_size = float(
                            np.count_nonzero(mask) / total_pixels
                        )
                    else:
                        mask_subject_size = None

                    thread_db.upsert_photo_mask(
                        photo_id=photo_id,
                        variant=sam2_variant,
                        path=mask_path,
                        detector_model=photo["detector_model"],
                        prompt_x=photo["prompt"][0],
                        prompt_y=photo["prompt"][1],
                        prompt_w=photo["prompt"][2],
                        prompt_h=photo["prompt"][3],
                        subject_size=mask_subject_size,
                        subject_tenengrad=mask_subject_tenengrad,
                        bg_tenengrad=mask_bg_tenengrad,
                        crop_complete=completeness,
                        quality_input_recipe=None,
                        subject_clip_high=features.pop("subject_clip_high", None),
                        subject_clip_low=features.pop("subject_clip_low", None),
                        subject_y_median=features.pop("subject_y_median", None),
                        bg_separation=features.pop("bg_separation", None),
                        phash_crop=features.pop("phash_crop", None),
                        noise_estimate=features.pop("noise_estimate", None),
                        _commit=False,
                    )
                    thread_db.set_active_mask_variant(
                        photo_id, sam2_variant, _commit=False,
                    )
                    # Remaining (non-mask) per-photo features still land
                    # on the photos row. mask_path / crop_complete /
                    # subject_tenengrad / bg_tenengrad / subject_size
                    # flow via set_active_mask_variant above and are
                    # intentionally NOT passed here.
                    if features:
                        thread_db.update_photo_pipeline_features(
                            photo_id, **features, _commit=False,
                        )
                    thread_db.update_photo_embeddings(
                        photo_id,
                        dino_subject_embedding=subj_emb_blob,
                        dino_global_embedding=global_emb_blob,
                        variant=dinov2_variant,
                        _commit=False,
                    )
                    mask_file_stage.install()
                    commit_with_retry(thread_db.conn)
                    mask_file_stage.finish()
                    mask_file_stage = None
                    masked += 1

                except ResourceWaitCancelled:
                    # Job cancelled while waiting for the CPU inference
                    # lease. Bail out of the loop immediately instead of
                    # marking every remaining photo as a mask failure
                    # (which would then reopen the source, preprocess it,
                    # and hit the same cancellation on the next iteration).
                    log.info(
                        "extract-masks cancelled while waiting for inference resources",
                    )
                    break
                except Exception:
                    failed += 1
                    log.warning(
                        "Mask extraction failed for photo %s", photo_id, exc_info=True
                    )
                    job["errors"].append(
                        f"Photo {photo_id}: mask extraction failed"
                    )
                    try:
                        _rollback_failed_mask_photo(thread_db, photo_id)
                    finally:
                        if mask_file_stage is not None:
                            mask_file_stage.restore()
                finally:
                    photo_mask_lock.release()

                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": i + 1,
                        "total": total,
                        "current_file": photo["filename"]
                        # ``photo`` may be a ``sqlite3.Row``, whose ``in``
                        # tests values, not column names: keep ``.keys()``.
                        if hasattr(photo, "__getitem__") and "filename" in photo.keys()  # noqa: SIM118
                        else str(photo_id),
                        "rate": round(
                            (i + 1)
                            / max(time.time() - job["_start_time"], 0.01),
                            1,
                        ),
                        "phase": "Extracting features (SAM2 + DINOv2)",
                    },
                )

            # Opt into JobRunner's ok/errors convention when photos went
            # unmasked. Styling the card honestly isn't enough: the Jobs
            # page, job history, API clients and the completion event all
            # read the job status, and "completed" there tells the user their
            # library was processed when some of it was never opened
            # (Codex #1392).
            job_errors = []
            if unreadable:
                job_errors.append(
                    f"{unreadable} of {total} photos could not be read, so "
                    f"they have no mask and Process Review rejects them as "
                    f"`no_subject_mask`. Reconnect the source and run Extract "
                    f"again."
                )
            if failed:
                job_errors.append(
                    f"{failed} of {total} photos failed mask extraction"
                )
            return {"masked": masked, "skipped": skipped, "failed": failed,
                    "unreadable": unreadable, "total": total,
                    "ok": not job_errors, "errors": job_errors}

        return ctx.start(
            "extract-masks",
            work,
            pausable=True,
            config={
                "collection_id": collection_id,
                "sam2_variant": sam2_variant,
                "dinov2_variant": dinov2_variant,
                "proxy_longest_edge": proxy_longest_edge,
            },
            runtime_warning=runtime_warning,
        )

    return blueprint
