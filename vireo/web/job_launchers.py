"""Background-job launchers that call per-app services.

``web.jobs`` holds job control and the launchers that need nothing beyond the
standard blueprint arguments. The launchers here start scans, previews,
ingest/move/offline-cache/full-resolution preparation, folder moves, XMP sync,
classification, darktable develop, mask extraction, label fetching, embedding
precompute and the background batch delete. Their request handling leans on
the app's scan, delete, move-folder and missing-originals services, whose
bound methods ``create_app`` passes in. Standalone mask and preview processing live in
``services.mask_extraction`` and ``services.preview_warmup``; this module
validates and launches those jobs.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from types import SimpleNamespace
from typing import TYPE_CHECKING, Protocol, cast

from app_paths import media_root
from config import config_value, read_raw_config_file, settings_write_lock
from db import Database
from flask import Blueprint, current_app, jsonify, request
from preview_cache import lightbox_fit_preview_sizes, paired_preview_ready
from runtime_warnings import (
    build_cpu_runtime_warning,
    runtime_warning_work_units,
)
from services import workspace_config, workspace_folders
from services.folder_moves import FolderMoveRequest
from services.local_folder import (
    local_copy_scan_conflict,
    stage_pending_source_paths,
)
from services.local_workspace import (
    has_local_workspace,
    stage_boundary_lock,
)
from services.mask_extraction import MaskExtractionOptions, extract_masks
from services.preview_warmup import warm_previews
from services.startup_tasks import metadata_repair_count
from web.background_jobs import make_background_job
from web.request_args import (
    coerce_collection_id,
    parse_selection_photo_ids,
    reject_visual_collection,
)

if TYPE_CHECKING:
    from sqlite3 import Row
    from threading import Lock
    from types import ModuleType
    from typing import Never

    from flask import Flask
    from flask.typing import ResponseReturnValue
    from labels import SpeciesLabels
    from services.folder_moves import DateDestination, RemoteMoveSpec
    from services.service_types import JobDict, JsonDict, JsonError
    from web.background_jobs import JobLaunch
    from web.web_types import (
        AppConfig,
        BuildScanWork,
        GetDb,
        GetRunner,
        GuardMoveFolder,
        InvalidateMissingOriginals,
        PendingLocalWorkspaceTransition,
        PreparedPhotoView,
        RunBatchDelete,
        StartMoveFolderJob,
    )
    from werkzeug.local import LocalProxy

log = logging.getLogger(__name__)



class LoadLabelSet(Protocol):
    """``labels.load_label_set``: one label file as the classify job sees it."""

    def __call__(self, path: str, meta: JsonDict | None = None, /) -> SpeciesLabels: ...

# Representative reasons kept on the release job result. The Jobs page shows
# ten items (``MAX_DETAIL_ITEMS``) and the ``kept`` count already reports the
# total, so the rest of the list only grows the payload serialized into each
# checkpoint, ``job_history`` row and SSE event. ``_MAX_ERROR_REASONS`` caps
# the per-photo failure samples the same way; the ``failed`` counter is
# published as ``errors_total`` when it exceeds the cap so job history still
# records the true count (the importer uses the same idiom).
_MAX_KEPT_REASONS = 50
_MAX_ERROR_REASONS = 50


def _count_lines(path: str) -> int | None:
    if not path:
        return None
    try:
        with open(path) as f:
            return sum(1 for line in f if line.strip())
    except (OSError, UnicodeError):
        return None


def _photo_is_raw_jpeg_pair(photo: Row | None) -> bool:
    """Mirror of ``static/lightbox/edits.js``'s ``vireoPhotoIsRawJpegPair``.

    The lightbox defaults a photo matching this predicate to its JPEG and
    appends ``?source=jpeg`` to every render URL. ``_serve_preview``
    bypasses the ordinary ``(photo_id, size)`` cache for source-specific
    requests and writes the paired shadow cache instead — so a
    prepare-full-resolution run that warmed only the ordinary tiers would
    still trigger a decode on the lightbox's first fit-size request for
    the pair.
    """
    if not photo or not photo["companion_path"]:
        return False
    from image_loader import RAW_EXTENSIONS

    primary_ext = os.path.splitext(photo["filename"])[1].lower()
    if primary_ext not in RAW_EXTENSIONS:
        return False
    companion_ext = os.path.splitext(photo["companion_path"])[1].lower()
    return companion_ext in {".jpg", ".jpeg"}


def _paired_jpeg_preview_exists(preview_dir: str, photo: Row, size: int, db: Database) -> bool:
    """Check the exact current JPEG artifact using the renderer's predicate."""
    from web.media_common import _paired_render_state_hash
    from web.media_previews import _paired_preview_path

    if not photo or not photo["companion_path"]:
        return False
    # A relocation keeps folder_id, but changes which live source the
    # renderer selects. Resolve its current path just as each request does.
    folder = db.folders.get(photo["folder_id"])
    if not folder:
        return False
    source_path = os.path.join(folder["path"], photo["companion_path"])
    if not os.path.isfile(source_path):
        # Match the renderer's live-source-first, offline-companion fallback.
        cached = db.caches.offline_original_get(photo["id"])
        source_path = cached["companion_path"] if cached else None
        if not source_path:
            return False
        if not os.path.isabs(source_path):
            source_path = os.path.join(os.path.dirname(preview_dir), source_path)
        if not os.path.isfile(source_path):
            return False
    state_hash = _paired_render_state_hash(
        photo, size, "jpeg", source_path, None,
    )
    return paired_preview_ready(db, _paired_preview_path(
        preview_dir, photo["id"], size, "jpeg", state_hash,
    ))


def _move_folder_destination(
    cfg: ModuleType, move_mod: ModuleType, effective_cfg: JsonDict, *, remote_target_id: str, subpath: str,
    destination: object, folder_template: str,
) -> tuple[JsonDict | None, RemoteMoveSpec | None, str, str, tuple[str, int] | None]:
    """Resolve a folder move's remote or local destination.

    Returns ``(target, remote, destination, display_dest, error)``;
    ``error`` holds the ``json_error`` arguments when the request must be
    refused (with empty destinations), and is None otherwise.
    """
    # Remote (SSH) destination vs local path. A remote target carries its
    # own destination (remote_path + optional subpath), so `destination`
    # is not required in that case.
    if remote_target_id:
        if folder_template:
            return None, None, "", "", (
                "Organizing one folder into capture-date folders is only "
                "available for local or mounted-drive destinations.", 400,
            )
        target, remote, err = _remote_move_spec(
            cfg, move_mod, effective_cfg, remote_target_id, subpath,
        )
        if err is not None or remote is None:
            return target, None, "", "", err
        # Pass the mount path as `destination` for informational use; the
        # move uses the SSH base from `remote`.
        display_dest = move_mod.rsync_dest_spec(
            target, remote["ssh_dest_base"])
        return target, remote, remote["mount_dest_base"], display_dest, None
    if not isinstance(destination, str):
        return None, None, "", "", ("destination must be a string", 400)
    if not destination:
        return None, None, "", "", ("destination required", 400)
    if not os.path.isabs(destination):
        return None, None, "", "", (
            "destination must be an absolute path", 400,
        )
    return None, None, destination, destination, None


def _remote_move_spec(
    cfg: ModuleType, move_mod: ModuleType, effective_cfg: JsonDict, remote_target_id: str, subpath: str,
) -> tuple[JsonDict | None, RemoteMoveSpec | None, tuple[str, int] | None]:
    """Validate a move-folder remote (SSH) target and build its move spec.

    Returns ``(target, remote, error)``; ``error`` holds the ``json_error``
    arguments when the request must be refused (``remote`` is then None),
    and is None otherwise.
    """
    target = cfg.get_remote_target(remote_target_id)
    if not target:
        return target, None, ("Remote target not found", 404)
    mount_path = (target.get("mount_path") or "").strip()
    if not mount_path:
        return target, None, (
            "This remote target has no local mount path, so moved "
            "photos couldn't stay in your library. Add a mount path "
            "under Settings → Remote targets.", 400,
        )
    # Reject a relative mount path (e.g. saved as "Photos" instead of
    # "/Volumes/Photos") before any transfer runs. move_folder also
    # refuses as a defense-in-depth check, but failing here gives the
    # UI a clean error instead of starting a job that immediately
    # reports failure.
    if not os.path.isabs(mount_path):
        return target, None, (
            "This remote target's local mount path isn't absolute "
            f"(\"{mount_path}\"). Moved photos would be repointed "
            "to a path relative to the server's working directory "
            "and appear missing. Set an absolute mount path under "
            "Settings → Remote targets.", 400,
        )
    rsync_bin = move_mod.resolve_rsync_bin(
        effective_cfg.get("rsync_bin", "") or "")
    if not rsync_bin:
        return target, None, (
            "No usable GNU rsync was found for remote moves. Install "
            "GNU rsync for your platform or set its executable under "
            "Settings → Paths.", 400,
        )
    ssh_bin = move_mod.resolve_ssh_bin(
        effective_cfg.get("ssh_bin", "") or "")
    if not ssh_bin:
        return target, None, (
            "OpenSSH Client was not found. Install the Windows "
            "OpenSSH Client optional feature or configure ssh.exe in Settings.", 400,
        )
    try:
        # cast: ``move`` is not annotated yet; RemoteMoveSpec mirrors what
        # build_remote_move_spec returns.
        remote = cast("RemoteMoveSpec", move_mod.build_remote_move_spec(
            target, subpath, rsync_bin, ssh_bin))
    except ValueError as exc:
        return target, None, (str(exc), 400)
    return target, remote, None


def _plan_date_destinations(
    move_mod: ModuleType, request_db: Database, folder_id: int, destination: str, folder_template: str,
) -> tuple[list[DateDestination], str | None]:
    """Plan a date-organized folder move into its per-date destinations.

    Returns ``(date_destinations, error)``; ``error`` is the message to
    refuse the request with (beside ``[]``), or None.
    """
    try:
        date_plan = move_mod.plan_folder_date_moves(
            request_db, folder_id, destination, folder_template,
        )
    except ValueError as exc:
        return [], str(exc)
    if not date_plan:
        return [], "No tracked photos found in the source folder"
    return [
        {
            "path": item["destination"],
            "relative_path": item["relative_path"],
            "photo_count": item["photo_count"],
        }
        for item in date_plan
    ], None


def _move_folder_landing(
    move_mod: ModuleType, folder: Row, *, folder_template: str, remote: RemoteMoveSpec | None, target: JsonDict | None,
    destination: str, display_dest: str, destination_name: str, date_destinations: list[DateDestination] | None,
) -> str:
    """The destination a folder move reports as where its photos land."""
    if folder_template:
        # A date-organized move fans out into one folder per capture date.
        # When the plan resolves to a single date folder — the common case
        # for a one-shoot source folder — that folder *is* where every
        # photo lands, so show it in full instead of the selected root the
        # user would otherwise read as the landing path. With several date
        # folders there is no single landing path; keep the root and let
        # the jobs panel list the folders underneath it.
        # A date-organized move always carries its planned folders.
        date_destinations = cast("list[DateDestination]", date_destinations)
        return (
            date_destinations[0]["path"] if len(date_destinations) == 1
            else display_dest
        )
    if remote:
        import posixpath

        landing_name = destination_name or folder["name"] \
            or os.path.basename(folder["path"].rstrip("/\\"))
        return move_mod.rsync_dest_spec(
            target,
            posixpath.join(remote["ssh_dest_base"], landing_name),
        )
    return move_mod.resolve_folder_dest(
        folder["path"], folder["name"], destination,
        destination_name,
    )


def _scan_roots_from_body(body: JsonDict) -> tuple[list[str], str | None]:
    """Read and check the scan roots a ``/api/jobs/scan`` body names.

    Returns ``(roots_list, error)``; ``error`` is the message to refuse
    the request with (beside ``[]``), or None.
    """
    # Normalize inputs. Prefer the explicit plural form when both are
    # provided; a caller who sends ``roots`` has opted into the new API.
    if "roots" in body:
        roots_in = body.get("roots")
        if not isinstance(roots_in, list) or not roots_in:
            return [], "roots must be a non-empty list"
        roots_list = [str(r) for r in roots_in if r]
        if not roots_list:
            return [], "roots must be a non-empty list"
    else:
        root = body.get("root", "")
        if not root:
            return [], "root path required"
        roots_list = [root]

    from image_loader import is_excluded_scan_path

    for r in roots_list:
        # Reject other-app data bundles (Apple Photos / Aperture / Photo
        # Booth) before any stat: ``os.path.isdir`` on a ``.photoslibrary``
        # path — or a symlink to one — itself trips the macOS
        # "access data from other apps" TCC prompt, defeating the guards
        # inside scan().
        if is_excluded_scan_path(r):
            return [], (
                f"path is inside a macOS app-managed library and cannot "
                f"be scanned: {r}"
            )
        if not os.path.isdir(r):
            return [], f"directory not found: {r}"
    return roots_list, None


def _remember_scan_roots(roots_list: list[str]) -> None:
    """Remember scan roots in ``config.json`` (skip temp directories from tests)."""
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


def create_job_launchers_blueprint(
    get_db: GetDb,
    json_error: JsonError,
    get_runner: GetRunner,
    db_path: str,
    config: AppConfig,
    *,
    invalidate_missing_originals: InvalidateMissingOriginals,
    run_batch_delete: RunBatchDelete,
    build_scan_work: BuildScanWork,
    pending_local_workspace_transition: PendingLocalWorkspaceTransition,
    guard_move_folder: GuardMoveFolder,
    start_move_folder_job: StartMoveFolderJob,
    serve_original_photo: PreparedPhotoView,
    serve_photo_preview: PreparedPhotoView,
    sync_job_lock: Lock,
) -> Blueprint:
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
    - ``serve_original_photo`` and ``serve_photo_preview`` are the
      ``/photos/<id>/original`` and ``/photos/<id>/preview`` views, which
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
    def api_job_batch_delete(ctx: JobLaunch) -> ResponseReturnValue:
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

        def work(job: JobDict) -> JsonDict:
            thread_db = ctx.thread_db()
            started = time.time()

            def progress(payload: JsonDict) -> None:
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
                # Photos retained after a Trash/filesystem error mean the
                # delete partly failed; the result still lists them so the
                # dialog can offer the permanent-delete fallback.
                result["ok"] = not result.get("failed_photo_ids")
                for failure in result.get("trash_failed") or []:
                    msg = (
                        f'{failure.get("path") or "photo " + str(failure.get("photo_id"))}: '
                        f'{failure.get("error") or "could not be removed"}'
                    )
                    if msg not in job["errors"]:
                        job["errors"].append(msg)
                # A TypedDict is a plain dict at runtime.
                return cast("JsonDict", result)
            finally:
                thread_db.close()

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
    def api_job_precompute_embeddings(ctx: JobLaunch) -> ResponseReturnValue:
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        labels_file = body.get("labels_file")
        if not model_id or not labels_file:
            return json_error("model_id and labels_file required")

        def work(job: JobDict) -> JsonDict:
            return _run_precompute_embeddings(ctx, job, model_id, labels_file)

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
    def api_job_fetch_labels(ctx: JobLaunch) -> ResponseReturnValue:
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

        def work(job: JobDict) -> JsonDict:
            from labels import fetch_species_list, load_label_set, save_labels

            def progress_cb(msg: str, current: int | None = None, total: int | None = None) -> None:
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
            workspace_config.set_workspace_active_labels(thread_db, [labels_path])

            precompute = _embedding_precompute_offer(load_label_set, labels_path)

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
    def api_job_scan(ctx: JobLaunch) -> ResponseReturnValue:
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
        # ``set_active_workspace(None)`` and ``workspace_folders.add_folder``
        # then skips the workspace link because no workspace is active.
        # The mutation-reservation hook deliberately lets no-workspace
        # requests through so routes that answer that state can respond
        # cleanly; this route is not one of them, so refuse here.
        if ctx.workspace_id is None:
            return json_error("no active workspace", 400)
        body = request.get_json(silent=True) or {}
        incremental = body.get("incremental", False)

        roots_list, err = _scan_roots_from_body(body)
        if err is not None:
            return json_error(err)

        _remember_scan_roots(roots_list)

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
    def api_job_scan_workspace(ctx: JobLaunch) -> ResponseReturnValue:
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
        roots = [r["path"] for r in workspace_folders.get_workspace_folder_roots(db, db.require_workspace_id())]
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
    def api_job_repair_metadata(ctx: JobLaunch) -> ResponseReturnValue:
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
        roots = [r["path"] for r in workspace_folders.get_workspace_folder_roots(db, db.require_workspace_id())]
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
        # A scan runs in the request's active workspace.
        repair_count = metadata_repair_count(db, cast(int, ctx.workspace_id), existing)
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
    def api_job_previews(ctx: JobLaunch) -> ResponseReturnValue:
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

        def work(job: JobDict) -> JsonDict:
            return warm_previews(
                job, media_root(config["THUMB_CACHE_DIR"]),
                runner=ctx.runner, open_db=ctx.thread_db,
                collection_id=collection_id, photo_ids=requested_photo_ids,
            )

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
    def api_job_ingest(ctx: JobLaunch) -> ResponseReturnValue:
        body = request.get_json(silent=True) or {}
        source = body.get("source", "")
        destination = body.get("destination", "")
        file_types = config_value(body, "file_types", section="ingest")
        folder_template = config_value(body, "folder_template", section="ingest")
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

        def work(job: JobDict) -> JsonDict:
            from ingest import ingest as do_ingest

            thread_db = ctx.thread_db()

            job["_start_time"] = time.time()

            def progress_cb(current: int, total: int, filename: str) -> None:
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
    def api_job_move_photos(ctx: JobLaunch) -> ResponseReturnValue:
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
        keep_visible = body.get("keep_visible", cfg.load().get("move_keep_visible_in_other_workspaces", True))
        if not isinstance(keep_visible, bool):
            return json_error("keep_visible must be a boolean")
        effective_cfg = get_db().get_effective_config(cfg.load())
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""

        def work(job: JobDict) -> JsonDict:
            from move import move_photos

            thread_db = ctx.thread_db()

            job["_start_time"] = time.time()
            job["progress"]["total"] = len(photo_ids)

            def progress_cb(current: int, total: int, filename: str) -> None:
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
                keep_visible=keep_visible,
                progress_cb=progress_cb,
                developed_dir=developed_dir,
                cancel_check=lambda: ctx.runner.cancellation_requested(job["id"]),
                pause_requested=lambda: ctx.runner.pause_requested(job["id"]),
                pause_callback=lambda: ctx.runner.is_cancelled(job["id"]),
            )
            if int(result.get("moved") or 0) > 0:
                try:
                    invalidate_missing_originals()
                except Exception:  # broad: cache invalidation is best-effort; the move already finished
                    log.exception(
                        "Failed to invalidate missing-originals cache "
                        "after move-photos job",
                    )

            if rule_id:
                with thread_db.transaction():
                    thread_db.moves_merge.touch_rule(rule_id)

            # Per-photo move failures fail the job (the runner folds them in).
            result["ok"] = not result.get("errors")
            return result

        return ctx.start(
            "move-photos", work, pausable=True,
            config={"photo_ids": photo_ids, "destination": destination, "keep_visible": keep_visible},
        )

    @blueprint.route("/api/jobs/offline-cache", methods=["POST"])
    @background_job
    def api_job_offline_cache(ctx: JobLaunch) -> ResponseReturnValue:
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
                for p in db.collections.get_photos(collection_id, per_page=999999)
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

        # Scoped to the request database's active workspace (``ctx.workspace_id``).
        # With none active nothing is visible, as the inline query binding a
        # NULL workspace id behaved, so the route answers its 400, not a 500.
        visible_set = (
            set(db.photo_visibility.visible_photo_ids(photo_ids))
            if ctx.workspace_id is not None else set()
        )
        photo_ids = [pid for pid in photo_ids if pid in visible_set]
        if not photo_ids:
            return json_error("no cacheable photos in current workspace")

        vireo_dir = media_root(config["THUMB_CACHE_DIR"])

        def work(job: JobDict) -> dict[str, int]:
            return _run_offline_cache(ctx, job, photo_ids, vireo_dir)

        return ctx.start(
            "offline-cache",
            work,
            pausable=True,
            config={"photo_ids": photo_ids},
        )

    @blueprint.route("/api/jobs/release-local-originals", methods=["POST"])
    @background_job
    def api_job_release_local_originals(ctx: JobLaunch) -> ResponseReturnValue:
        """Release selected, folder-scoped, or all managed offline originals."""
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        scopes = [key for key in ("photo_ids", "folder_id", "all_cached") if key in body]
        if len(scopes) != 1:
            return json_error("provide exactly one of photo_ids, folder_id, or all_cached")
        db = get_db()
        all_cached = scopes[0] == "all_cached"
        if all_cached:
            if body["all_cached"] is not True:
                return json_error("all_cached must be true")
            # Storage reports catalog-wide usage, across every workspace.
            photo_ids = db.caches.offline_original_ids()
        elif scopes[0] == "folder_id":
            folder_id = body["folder_id"]
            if isinstance(folder_id, bool) or not isinstance(folder_id, int):
                return json_error("folder_id must be an integer")
            visible_folders = {folder["id"] for folder in db.folders.tree()}
            if ctx.workspace_id is None or folder_id not in visible_folders:
                return json_error("folder does not belong to the active workspace", 403)
            photo_ids = db.caches.offline_original_ids(
                workspace_id=ctx.workspace_id,
                folder_ids=db.get_folder_subtree_ids(folder_id),
            )
        else:
            if ctx.workspace_id is None:
                return json_error("no active workspace", 403)
            photo_ids, error = parse_selection_photo_ids(
                db, body, json_error=json_error, limit=None,
            )
            if error is not None:
                return error
        vireo_dir = media_root(config["THUMB_CACHE_DIR"])
        selected_photos = db.photos.get_by_ids(photo_ids)

        def work(job: JobDict) -> JsonDict:
            from offline_cache import release_photo_original

            thread_db = ctx.thread_db()
            result = {
                "ok": True, "released": 0, "kept": 0, "not_cached": 0,
                "failed": 0, "bytes": 0, "total": len(photo_ids),
                "processed": 0, "kept_reasons": [], "errors": [],
            }
            job["result"] = result
            try:
                for photo_id in photo_ids:
                    ctx.checkpoint(job)
                    photo = selected_photos.get(photo_id)
                    filename = photo["filename"] if photo else f"Photo {photo_id}"
                    outcome = (
                        release_photo_original(
                            thread_db, photo_id, vireo_dir, verify_workspace=not all_cached,
                            selected_photo=photo,
                        ) if photo is not None else {"status": "not_cached", "bytes": 0}
                    )
                    _record_release_outcome(job, result, filename, outcome)
                    ctx.runner.push_event(job["id"], "progress", {
                        "current": result["processed"], "total": result["total"],
                        "current_file": filename, "phase": "Releasing local originals",
                    })
                return result
            finally:
                thread_db.close()

        return ctx.start(
            "release-local-originals", work, pausable=True,
            config=body, extra={"total": len(photo_ids)},
        )

    @blueprint.route("/api/jobs/prepare-full-resolution", methods=["POST"])
    @background_job
    def api_job_prepare_full_resolution(ctx: JobLaunch) -> ResponseReturnValue:
        """Prepare selected photos for uninterrupted full-resolution review.

        The job first copies source assets into Vireo's managed local cache,
        then drives the exact /original and /preview render paths used by the
        lightbox. RAW working copies, edited full-resolution renders, and every
        preview size the lightbox shows at fit (``/full`` and the larger tiers
        it steps through before /original) are therefore ready before the user
        begins flipping through the selection.
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
        # Scoped to the request database's active workspace (``ctx.workspace_id``).
        # With none active nothing is visible, as the inline query binding a
        # NULL workspace id behaved, so the route answers its 400, not a 500.
        visible_set = (
            set(db.photo_visibility.visible_photo_ids(photo_ids))
            if ctx.workspace_id is not None else set()
        )
        photo_ids = [photo_id for photo_id in photo_ids if photo_id in visible_set]
        if not photo_ids:
            return json_error("no photos in the current workspace can be prepared")

        vireo_dir = media_root(config["THUMB_CACHE_DIR"])
        # Preserve the selected source identity even if SQLite recycles its
        # numeric ID before this job reaches it.
        selected_photos = db.photos.get_by_ids(photo_ids)
        # The worker renders through the app object from a job
        # thread, which has no application context of its own.
        # ``current_app`` is a proxy; unwrap it for the worker thread.
        flask_app = cast("LocalProxy[Flask]", current_app)._get_current_object()

        def work(job: JobDict) -> JsonDict:
            return _FullResolutionPreparation(
                ctx,
                job,
                photo_ids=photo_ids,
                selected_photos=selected_photos,
                vireo_dir=vireo_dir,
                flask_app=flask_app,
                get_db=get_db,
                config=config,
                serve_original_photo=serve_original_photo,
                serve_photo_preview=serve_photo_preview,
            ).run()

        return ctx.start(
            "prepare-full-resolution",
            work,
            pausable=True,
            config={"photo_ids": photo_ids},
            extra={"total": len(photo_ids)},
        )

    @blueprint.route("/api/jobs/move-folder", methods=["POST"])
    @background_job
    def api_job_move_folder(ctx: JobLaunch) -> ResponseReturnValue:
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
        folder = request_db.folders.get(folder_id)
        if not folder or not workspace_folders.workspace_has_folder_link(request_db, folder_id):
            return json_error("Folder not found", status=404)

        guard_err = guard_move_folder(request_db, folder_id)
        if guard_err is not None:
            return json_error(guard_err, 409)

        import config as cfg
        import move as move_mod
        keep_visible = body.get("keep_visible", cfg.load().get("move_keep_visible_in_other_workspaces", True))
        if not isinstance(keep_visible, bool):
            return json_error("keep_visible must be a boolean")
        try:
            destination_name = move_mod.normalize_destination_name(
                destination_name_raw)
        except ValueError as exc:
            return json_error(str(exc))
        effective_cfg = request_db.get_effective_config(cfg.load())
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""

        target, remote, destination, display_dest, err = _move_folder_destination(
            cfg, move_mod, effective_cfg,
            remote_target_id=remote_target_id,
            subpath=subpath,
            destination=destination,
            folder_template=folder_template,
        )
        if err is not None:
            return json_error(*err)

        date_destinations = None
        if folder_template:
            date_destinations, err = _plan_date_destinations(
                move_mod, request_db, folder_id, destination, folder_template,
            )
            if err is not None:
                return json_error(err)

        resolved_destination = _move_folder_landing(
            move_mod, folder,
            folder_template=folder_template,
            remote=remote,
            target=target,
            destination=destination,
            display_dest=display_dest,
            destination_name=destination_name,
            date_destinations=date_destinations,
        )

        job_id = start_move_folder_job(
            ctx.runner, ctx.workspace_id,
            FolderMoveRequest(
                folder_id=folder_id,
                source_path=folder["path"],
                destination=destination,
                display_dest=display_dest,
                resolved_destination=resolved_destination,
                merge=merge,
                developed_dir=developed_dir,
                destination_name=destination_name,
                remote=remote,
                folder_template=folder_template,
                date_destinations=date_destinations,
                keep_visible=keep_visible,
            ),
        )
        return jsonify({"job_id": job_id})

    @blueprint.route("/api/jobs/sync", methods=["POST"])
    @background_job
    def api_job_sync(ctx: JobLaunch) -> ResponseReturnValue:
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

        def work(job: JobDict) -> JsonDict:
            from sync import sync_to_xmp

            thread_db = ctx.thread_db()

            def status_cb(progress: JsonDict) -> None:
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
    def api_job_classify(ctx: JobLaunch) -> ResponseReturnValue:
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
                "similarity_threshold", config_value(user_cfg, "similarity_threshold")
            ),
            reclassify=body.get("reclassify", False),
        )

        vireo_dir = media_root(config["THUMB_CACHE_DIR"])
        work_units = runtime_warning_work_units(
            "classify collection",
            lambda: db.collections.count_photos(collection_id),
        )
        runtime_warning = build_cpu_runtime_warning(
            "classify",
            work_units=work_units,
            reason="large_classification_job_cpu_only",
        )

        def work(job: JobDict) -> JsonDict:
            # Classification runs in the request's active workspace.
            return run_classify_job(
                job, ctx.runner, db_path, cast(int, ctx.workspace_id), params,
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
    def api_job_develop(ctx: JobLaunch) -> ResponseReturnValue:
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

        settings = SimpleNamespace(
            binary=binary,
            style=style,
            output_format=output_format,
            output_dir=output_dir,
            auto_convert_dng=auto_convert_dng,
            dng_converter_bin=dng_converter_bin,
            width=width,
        )

        def work(job: JobDict) -> dict[str, int]:
            return _run_develop(ctx, job, photo_ids, settings)

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
    def api_job_extract_masks(ctx: JobLaunch) -> ResponseReturnValue:
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
        min_detector_conf = config_value(effective_cfg, "detector_confidence")

        work_units = runtime_warning_work_units(
            "extract-masks collection" if collection_id else "extract-masks workspace",
            lambda: db.collections.count_photos(collection_id)
            if collection_id
            else db.photos.count(),
        )
        runtime_warning = build_cpu_runtime_warning(
            "extract-masks",
            work_units=work_units,
            reason="large_segmentation_job_cpu_only",
        )

        def work(job: JobDict) -> JsonDict:
            return extract_masks(
                job, db_path, runner=ctx.runner, open_db=ctx.thread_db,
                options=MaskExtractionOptions(
                    collection_id=collection_id,
                    sam2_variant=sam2_variant,
                    dinov2_variant=dinov2_variant,
                    proxy_longest_edge=proxy_longest_edge,
                    min_detector_conf=min_detector_conf,
                ),
            )

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


class _FullResolutionPreparation:
    """The ``/api/jobs/prepare-full-resolution`` work function and its run-wide state."""

    def __init__(
        self, ctx: JobLaunch, job: JobDict, *, photo_ids: list[int], selected_photos: dict[int, Row], vireo_dir: str,
        flask_app: Flask, get_db: GetDb, config: AppConfig, serve_original_photo: PreparedPhotoView,
        serve_photo_preview: PreparedPhotoView,
    ) -> None:
        self.ctx = ctx
        self.job = job
        self.photo_ids = photo_ids
        self.selected_photos = selected_photos
        self.vireo_dir = vireo_dir
        self.flask_app = flask_app
        self.get_db = get_db
        self.config = config
        self.serve_original_photo = serve_original_photo
        self.serve_photo_preview = serve_photo_preview
        self.preview_dir = os.path.join(vireo_dir, "previews")

    def run(self) -> JsonDict:
        import config as cfg
        from offline_cache import (
            cache_photo_original,
            cleanup_preparation_offline_files,
            original_preparation_guard,
            photo_source_matches,
        )

        self.cache_photo_original = cache_photo_original
        self.cleanup_preparation_offline_files = cleanup_preparation_offline_files
        self.photo_source_matches = photo_source_matches
        job = self.job

        thread_db = self.thread_db = self.ctx.thread_db()

        try:
            self.preview_sizes = lightbox_fit_preview_sizes(
                thread_db.get_effective_config(cfg.load()).get(
                    "preview_max_size",
                ),
            )
            self.folders = {
                folder["id"]: folder["path"]
                for folder in thread_db.folders.tree()
            }
            self.ready = 0
            self.copied = 0
            self.reused = 0
            self.failed = 0
            self.skipped_deleted = 0
            self.copied_bytes = 0
            # Photos counted ready by the in-loop pass: a final
            # cross-selection check verifies no later photo's warming
            # evicted their tiers before the job reports them ready.
            self.pending_ready_meta = {}
            self.total = len(self.photo_ids)
            job["_start_time"] = time.time()
            job["progress"]["total"] = self.total

            for index, photo_id in enumerate(self.photo_ids, start=1):
                if self.ctx.runner.is_cancelled(job["id"]):
                    break
                # The lock must precede database transactions: other
                # writers publish files before inserting their cache row.
                with original_preparation_guard(self.vireo_dir, photo_id):
                    filename = self._prepare_photo(photo_id)
                self._push_progress(index, filename)

            self._verify_ready_tiers()
            return self._result()
        finally:
            thread_db.close()

    def _render_through_view(self, path: str, view: PreparedPhotoView, photo_id: int, photo: Row, label: str) -> str | None:
        """Run a lightbox view in an isolated request context.

        Returns an error string, or None when the view succeeded.
        """
        flask_app = self.flask_app
        with flask_app.test_request_context(path):
            request_db = self.get_db()
            request_db.set_active_workspace(self.ctx.workspace_id)
            response = flask_app.make_response(
                view(photo_id, _prepare_source=photo)
            )
            try:
                if 200 <= response.status_code < 300:
                    return None
                response.direct_passthrough = False
                detail = response.get_data(as_text=True).strip()
                return detail or (
                    f"{label} render failed with status "
                    f"{response.status_code}"
                )
            finally:
                response.close()

    def _prepare_photo(self, photo_id: int) -> str:
        """Cache, warm and settle one photo under its preparation lock.

        Returns the filename the progress event reports.
        """
        thread_db = self.thread_db
        # Re-read each photo: deletion may have run while this
        # job was preparing earlier selections.
        photo = self.selected_photos.get(photo_id)
        current_photo = thread_db.photos.get(photo_id)
        filename = photo["filename"] if photo else f"Photo {photo_id}"
        is_pair = _photo_is_raw_jpeg_pair(photo)
        error = None
        cached = None
        attempted = self.photo_source_matches(photo, current_photo)
        if attempted:
            # A matching source means the selected row is there.
            cached, error = self._cache_and_warm(
                photo_id, cast("Row", photo), filename, is_pair,
            )
        self._settle_photo(
            photo_id, photo, filename, is_pair, attempted, cached, error,
        )
        return filename

    def _cache_and_warm(self, photo_id: int, photo: Row, filename: str, is_pair: bool) -> tuple[JsonDict | None, str | None]:
        """Copy the source into the offline cache and warm its renders.

        Returns ``(cached, error)``: the offline-cache result (None when
        caching raised) and an error string, or None when every tier is
        ready.
        """
        thread_db = self.thread_db
        cached = None
        error = None
        try:
            cached = self.cache_photo_original(
                thread_db, photo, self.vireo_dir, self.folders,
            )
            cache_status = cached.get("status")
            if cache_status not in ("cached", "skipped"):
                error = cache_status or "source could not be cached"

            if error is None and self.photo_source_matches(
                photo, thread_db.photos.get(photo_id),
            ):
                error = self._warm_renders(photo_id, photo, is_pair)
                # Eviction can delete a just-warmed
                # preview the moment it is published
                # (preview_cache_max_mb too small for the
                # combined tiers, or 0). _serve_preview
                # still returns 200 from the in-memory
                # bytes, so a successful render alone is
                # not proof that the next lightbox
                # request will be a cache hit — check
                # after warming instead of calling the
                # photo ready prematurely.
                if error is None and self.preview_sizes:
                    error = self._evicted_tier_error(photo_id, photo, is_pair)
        except Exception as exc:  # broad: per-photo boundary; the error is recorded for this photo
            thread_db.rollback()
            error = str(exc) or exc.__class__.__name__
            if self.photo_source_matches(photo, thread_db.photos.get(photo_id)):
                log.warning(
                    "Full-resolution preparation failed for %s: %s",
                    filename, exc, exc_info=True,
                )
        return cached, error

    def _warm_renders(self, photo_id: int, photo: Row, is_pair: bool) -> str | None:
        """Render /original and every fit-size preview; return the first error."""
        # Execute the canonical renderers inside
        # isolated request contexts. This avoids a
        # second implementation drifting from the
        # lightbox's RAW/companion/edit fallbacks.
        # /original backs 1:1 zoom; the previews
        # are what flipping at fit loads.
        error = self._render_through_view(
            f"/photos/{photo_id}/original",
            self.serve_original_photo, photo_id, photo,
            "full-resolution",
        )
        for size in self.preview_sizes:
            if error is not None:
                break
            error = self._render_through_view(
                f"/photos/{photo_id}/preview?size={size}",
                self.serve_photo_preview, photo_id, photo,
                f"{size}px preview",
            )
            # RAW+JPEG pairs: the lightbox
            # defaults to the JPEG and appends
            # ?source=jpeg to every render URL,
            # which bypasses the ordinary
            # (photo_id, size) cache. Warm the
            # paired shadow cache too, otherwise
            # the first fit-size request still
            # decodes the companion JPEG.
            if error is None and is_pair:
                error = self._render_through_view(
                    f"/photos/{photo_id}/preview"
                    f"?size={size}&source=jpeg",
                    self.serve_photo_preview, photo_id,
                    photo,
                    f"{size}px paired preview",
                )
        return error

    def _missing_tier(self, photo_id: int, photo: Row, is_pair: bool) -> tuple[int, str] | tuple[None, None]:
        """Return ``(size, kind)`` for the first tier missing from the cache.

        ``kind`` is ``"preview"`` or ``"paired preview"``; both are None
        when every fit-size tier (and its paired twin) is present.
        """
        thread_db = self.thread_db
        for size in self.preview_sizes:
            cache_file = os.path.join(
                self.preview_dir, f"{photo_id}_{size}.jpg",
            )
            if not (
                thread_db.caches.preview_get(photo_id, size)
                and os.path.exists(cache_file)
            ):
                return size, "preview"
            # Paired tiers share the preview
            # quota and can be evicted too.
            if (
                is_pair
                and not _paired_jpeg_preview_exists(
                    self.preview_dir, photo, size, thread_db,
                )
            ):
                return size, "paired preview"
        return None, None

    def _evicted_tier_error(self, photo_id: int, photo: Row, is_pair: bool) -> str | None:
        """The in-loop check that this photo's warmed tiers survived."""
        size, kind = self._missing_tier(photo_id, photo, is_pair)
        if size is None:
            return None
        if kind == "preview":
            return (
                f"{size}px preview was "
                "evicted during warming "
                "(preview_cache_max_mb "
                "too small for the "
                "lightbox tiers)"
            )
        return (
            f"{size}px paired "
            "preview was not "
            "written (RAW+JPEG "
            "companion unavailable "
            "or preview cache quota too small)"
        )

    def _settle_photo(
        self, photo_id: int, photo: Row | None, filename: str, is_pair: bool, attempted: bool, cached: JsonDict | None,
        error: str | None,
    ) -> None:
        """Record one photo's outcome, purging caches if its identity changed."""
        thread_db = self.thread_db
        # Serialize this final identity check and cache cleanup
        # against catalog writers. An import can reuse a deleted
        # ID; any cache written by this iteration is then suspect,
        # including the offline row attached to that replacement.
        # Purge disposable caches only if we actually attempted
        # work, leaving replacements found before our turn alone.
        with thread_db.transaction(immediate=True):
            current_photo = thread_db.photos.get(photo_id)
            if not attempted or not self.photo_source_matches(photo, current_photo):
                from preview_cache import cleanup_cached_files_for_deleted_photos

                self.skipped_deleted += 1
                if attempted:
                    thread_db.caches.offline_original_delete(
                        photo_id, _commit=False,
                    )
                    if current_photo is None:
                        cleanup_cached_files_for_deleted_photos(
                            self.config["THUMB_CACHE_DIR"],
                            [{"photo_id": photo_id}],
                            vireo_dir=self.vireo_dir,
                        )
                    else:
                        # Other cache families may already
                        # belong to the replacement photo.
                        self.cleanup_preparation_offline_files(
                            self.vireo_dir, photo_id,
                        )
            elif error is None:
                # The offline-cache result is set whenever there is no error.
                cached = cast("JsonDict", cached)
                self.ready += 1
                cache_bytes = int(cached.get("bytes") or 0)
                if cached["status"] == "cached":
                    self.copied += 1
                    self.copied_bytes += cache_bytes
                elif cached["status"] == "skipped":
                    self.reused += 1
                self.pending_ready_meta[photo_id] = {
                    "filename": filename,
                    "cached_status": cached["status"],
                    "cached_bytes": cache_bytes,
                    "is_pair": is_pair,
                }
            else:
                self.failed += 1
                self.job["errors"].append(f"{filename}: {error}")

    def _push_progress(self, index: int, filename: str) -> None:
        job = self.job
        progress = {
            "current": index,
            "total": self.total,
            "current_file": filename,
            "rate": round(
                index
                / max(time.time() - job["_start_time"], 0.01),
                1,
            ),
            "phase": "Preparing full-resolution photos",
        }
        job["progress"].update(progress)
        self.ctx.runner.push_event(job["id"], "progress", progress)

    def _verify_ready_tiers(self) -> None:
        """Reclassify ready photos whose tiers a later photo evicted.

        Final cross-selection verification: later photos' warming
        may have evicted earlier photos' tiers once the combined
        selection exceeds preview_cache_max_mb. The in-loop check
        cannot see this — it runs before subsequent photos touch
        the cache. Reclassify any photo whose tiers no longer
        exist on disk / in the preview_cache table as failed.
        """
        if not (self.preview_sizes and self.pending_ready_meta):
            return
        for photo_id, info in self.pending_ready_meta.items():
            current_photo = self.thread_db.photos.get(photo_id)
            missing_size, missing_kind = self._missing_tier(
                photo_id, current_photo, info["is_pair"],
            )
            if missing_size is None:
                continue
            self.ready -= 1
            self.failed += 1
            if info["cached_status"] == "cached":
                self.copied -= 1
                self.copied_bytes -= info["cached_bytes"]
            elif info["cached_status"] == "skipped":
                self.reused -= 1
            if missing_kind == "paired preview":
                self.job["errors"].append(
                    f"{info['filename']}: {missing_size}px "
                    "paired preview went missing after warming "
                    "(source changed or preview cache quota too small)"
                )
            else:
                self.job["errors"].append(
                    f"{info['filename']}: {missing_size}px "
                    "preview was evicted during warming "
                    "(preview_cache_max_mb too small for the "
                    "lightbox tiers)"
                )

    def _result(self) -> JsonDict:
        return {
            "ok": self.failed == 0,
            "ready": self.ready,
            "copied": self.copied,
            "reused": self.reused,
            "failed": self.failed,
            "skipped_deleted": self.skipped_deleted,
            "total": self.total,
            "bytes": self.copied_bytes,
            "errors": list(self.job["errors"]),
        }


def _run_develop(ctx: JobLaunch, job: JobDict, photo_ids: list[int], settings: SimpleNamespace) -> dict[str, int]:
    """The ``/api/jobs/develop`` work function.

    ``settings`` carries the darktable binary and the request's resolved
    style, output and conversion options.
    """
    from develop import develop_photo, output_path_for_photo
    from export import developed_folder_key

    thread_db = ctx.thread_db()

    photos = []
    for pid in photo_ids:
        p = thread_db.photos.get(pid, verify_workspace=True)
        if p:
            photos.append(p)

    if not photos:
        return {"developed": 0, "errors": 0, "total": 0}

    folders = {f["id"]: f["path"] for f in thread_db.folders.tree()}
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
        if settings.output_dir:
            out_dir = os.path.join(settings.output_dir, developed_folder_key(folder_path))
        else:
            out_dir = os.path.join(folder_path, "developed")
        out_path = output_path_for_photo(
            photo["filename"], out_dir, settings.output_format,
        )

        try:
            photo_metadata = json.loads(photo["exif_data"]) if photo["exif_data"] else None
        except (TypeError, json.JSONDecodeError):
            photo_metadata = None

        result = develop_photo(
            darktable_bin=settings.binary,
            input_path=input_path,
            output_path=out_path,
            style=settings.style if settings.style else None,
            width=settings.width,
            auto_convert_dng=bool(settings.auto_convert_dng),
            dng_converter_bin=settings.dng_converter_bin,
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
        _fail_develop_job(job, result, errors, total)
    return result


def _fail_develop_job(job: JobDict, result: JsonDict, errors: int, total: int) -> Never:
    """Raise so a develop run with any per-photo failure ends "failed".

    Rollup rule: if any per-photo develop failed, the overall
    job is failed (not completed). Stash the counts on the job
    so _persist_job (jobs.py ~L197-210) merges them with the
    primary error message, then raise so _run_job records
    status="failed".

    Re-raise with an existing entry in job["errors"] so
    _run_job's dedup guard (err_str not in job["errors"])
    skips the append — otherwise a novel exception string
    gets tacked on as a synthetic extra error, inflating
    error_count by 1. Keep the nicer "N/M failed: <err>"
    summary on job["_fatal_error"], which _persist_job
    prefers over errors[0] when building the result row.
    """
    job["result"] = result
    first_err = job["errors"][0]
    job["_fatal_error"] = (
        f"{errors}/{total} develop operations failed: {first_err}"
    )
    raise RuntimeError(first_err)


def _run_offline_cache(ctx: JobLaunch, job: JobDict, photo_ids: list[int], vireo_dir: str) -> dict[str, int]:
    """The ``/api/jobs/offline-cache`` work function."""
    from offline_cache import cache_photo_original

    thread_db = ctx.thread_db()
    photos_map = thread_db.photos.get_by_ids(photo_ids)
    folders = {f["id"]: f["path"] for f in thread_db.folders.tree()}

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
            except Exception as exc:  # broad: per-photo boundary; the failure is counted and listed in the job errors
                thread_db.rollback()
                failed += 1
                job["errors"].append(f'{photo["filename"]}: {exc}')
                log.warning("Offline cache failed for %s: %s", filename, exc, exc_info=True)

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


def _record_release_outcome(job: JobDict, result: JsonDict, filename: str, outcome: JsonDict) -> None:
    """Tally one photo's release outcome into the job result."""
    status = outcome["status"]
    result[status] += 1
    result["bytes"] += outcome["bytes"]
    result["processed"] += 1
    if status in {"kept", "failed"}:
        reason = f'{filename}: {outcome["reason"]}'
        if status == "kept":
            if len(result["kept_reasons"]) < _MAX_KEPT_REASONS:
                result["kept_reasons"].append(reason)
        else:
            if len(result["errors"]) < _MAX_ERROR_REASONS:
                # Append to ``job["errors"]`` first so a
                # snapshot caught between the two appends
                # sees ``snap["errors"]`` at least as long
                # as ``snap["result"]["errors"]``.
                # ``_error_total(snap)`` reads from
                # ``snap["errors"]`` while
                # ``_write_checkpoints`` serializes
                # ``snap["result"]``, so appending to the
                # job-level list first keeps the
                # ``error_count`` column from under-reporting
                # the serialized result's error details
                # before the ``errors_total`` cap branch
                # kicks in.
                job["errors"].append(reason)
                result["errors"].append(reason)
            elif result["failed"] > _MAX_ERROR_REASONS:
                # Live checkpoints snapshot ``result`` and
                # persist ``_error_total`` — publish the
                # count the moment it diverges from the
                # capped list, not just at the final return,
                # so a checkpoint mid-run (and the +1 the
                # startup sweep adds to an interrupted row)
                # reports the true total.
                result["errors_total"] = result["failed"]
            result["ok"] = False


def _run_precompute_embeddings(ctx: JobLaunch, job: JobDict, model_id: str, labels_file: str) -> JsonDict:
    """The ``/api/jobs/precompute-embeddings`` work function."""
    from classifier import precompute_label_embeddings
    from labels import get_saved_labels, load_label_set
    from models import get_models

    model = _precompute_model(get_models, model_id)

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

    labels = _precompute_labels(get_saved_labels, load_label_set, labels_file)

    log.info(
        "Pre-computing embeddings: %d labels with %s",
        len(labels),
        model["name"],
    )

    def _progress(current: int, total: int) -> None:
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

    def _pause_requested() -> bool:
        return ctx.runner.pause_requested(job["id"])

    def _cancelled() -> bool:
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


def _precompute_model(get_models: Callable[[], list[JsonDict]], model_id: str) -> JsonDict:
    """Find the downloaded embedding model a precompute job runs with."""
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
    return model


def _precompute_labels(
    get_saved_labels: Callable[[], list[JsonDict]], load_label_set: LoadLabelSet, labels_file: str,
) -> SpeciesLabels:
    """Load the label set a precompute job embeds, refusing an empty one."""
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
    return labels


def _embedding_precompute_offer(load_label_set: LoadLabelSet, labels_path: str) -> JsonDict | None:
    """The precompute the fetch-labels result offers, or None.

    Offered when the active model uses per-label embeddings and the
    freshly downloaded list has no valid embedding cache for it yet.
    """
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
        except Exception:  # broad: advisory check; the label download already succeeded
            log.warning(
                "Could not inspect embedding cache after label download",
                exc_info=True,
            )
    return precompute
