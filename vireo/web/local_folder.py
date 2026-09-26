"""HTTP endpoints for shared, folder-scoped Work Locally sessions."""

from __future__ import annotations

import os
import threading
import unicodedata

from db import Database
from flask import Blueprint, jsonify, request
from services.local_folder import (
    LOCAL_FOLDER_JOB_TYPES,
    LocalWorkspaceCancelled,
    LocalWorkspaceError,
    _path_overlaps_source,
    _resolve_physical,
    affected_workspace_ids,
    discard_folder,
    folder_status,
    local_copy_preflight,
    local_path_for_base,
    local_root_for_folder,
    local_root_under_folder,
    stage_folder,
    sync_folder,
    workspace_ids_for_folder_tree,
    workspace_local_root_ids,
    workspace_status,
)
from services.local_workspace import destination_case_insensitive, stage_boundary_lock
from services.local_workspace import local_state as legacy_local_state


def create_local_folder_blueprint(
    get_db, json_error, get_runner, db_path, vireo_dir, invalidate_missing_originals=None
):
    blueprint = Blueprint("local_folder", __name__)
    transition_lock = threading.RLock()
    preflight_lock = threading.Lock()
    active_preflights = {}
    cancelled_preflight_ids = {}
    max_cancelled_preflight_ids = 256
    # Highest ordering token seen per workspace and browser page. Scoping the
    # high-water mark by client lets a refreshed page start its sequence at one
    # without being rejected by the previous page's larger counter.
    last_preflight_seq: dict[tuple[int, str | None], int] = {}
    max_preflight_sequences = 64

    def _trim_preflight_sequences(protected_key=None):
        while len(last_preflight_seq) > max_preflight_sequences:
            inactive_key = next(
                (
                    key
                    for key in last_preflight_seq
                    if key not in active_preflights and key != protected_key
                ),
                None,
            )
            if inactive_key is None:
                # The number of active scans is already constrained by server
                # workers. Keep their ordering records until they finish, then
                # trim from ``_finish_preflight`` below.
                break
            last_preflight_seq.pop(inactive_key, None)

    def _begin_preflight(
        workspace_id, preflight_id, preflight_client_id=None, preflight_seq=None
    ):
        cancelled = threading.Event()
        with preflight_lock:
            active_key = (int(workspace_id), preflight_client_id)
            cancellation_key = (int(workspace_id), preflight_id)
            # Client-provided monotonic ordering token. Waitress can deliver
            # an older request after a newer one has already registered (the
            # older one waited longer for a free worker, or its cancel signal
            # never arrived). Without this check the older request would
            # unconditionally cancel the newer scan and take over as active;
            # a subsequent cancel for the older id then cancels it too, and
            # the client is stuck at 409 while the user's latest inputs never
            # get a size back. Reject the stale arrival instead.
            if preflight_seq is not None:
                ordering_key = (int(workspace_id), preflight_client_id)
                last_seen = last_preflight_seq.get(ordering_key)
                if last_seen is not None and preflight_seq <= last_seen:
                    cancelled.set()
                    return cancelled
                # Refresh insertion order so the bounded cache retains active
                # browser pages rather than whichever key was first seen.
                last_preflight_seq.pop(ordering_key, None)
                last_preflight_seq[ordering_key] = preflight_seq
                _trim_preflight_sequences(ordering_key)
            if preflight_id is not None and cancellation_key in cancelled_preflight_ids:
                # The cancel request can beat the original request to a free
                # Waitress worker. Advance ordering above before consuming its
                # tombstone so an even older delayed request is still rejected.
                # If that predecessor registered between the cancel and this
                # request, stop it too; the dismissed page no longer needs
                # either scan. Do not disrupt a scan from another browser page.
                cancelled_preflight_ids.pop(cancellation_key, None)
                previous = active_preflights.get(active_key)
                if previous is not None:
                    previous[1].set()
                cancelled.set()
                return cancelled
            previous = active_preflights.get(active_key)
            if previous is not None:
                previous[1].set()
            active_preflights[active_key] = (
                preflight_id,
                cancelled,
            )
        return cancelled

    def _finish_preflight(
        workspace_id, preflight_id, preflight_client_id, cancelled
    ):
        with preflight_lock:
            active_key = (int(workspace_id), preflight_client_id)
            current = active_preflights.get(active_key)
            if current == (preflight_id, cancelled):
                active_preflights.pop(active_key, None)
            _trim_preflight_sequences()

    def _cancel_preflight(workspace_id, preflight_id):
        with preflight_lock:
            cancellation_key = (int(workspace_id), preflight_id)
            cancelled_preflight_ids[cancellation_key] = None
            while len(cancelled_preflight_ids) > max_cancelled_preflight_ids:
                cancelled_preflight_ids.pop(next(iter(cancelled_preflight_ids)))
            for active_key, current in active_preflights.items():
                if active_key[0] == int(workspace_id) and current[0] == preflight_id:
                    current[1].set()
                    return True
            # Preflight IDs are generated uniquely per browser request. This
            # fallback handles workspace activation racing ahead of pagehide
            # in older clients that did not include their originating id.
            for current in active_preflights.values():
                if current[0] == preflight_id:
                    current[1].set()
                    return True
            return False

    def _active_context():
        db = get_db()
        workspace_id = db._active_workspace_id
        if workspace_id is None:
            return None, None, json_error("No active workspace", 400)
        return db, int(workspace_id), None

    def _preflight_context(body):
        """Bind a queued preflight to the workspace that submitted it."""
        raw_workspace_id = body.get("workspace_id")
        if raw_workspace_id is None:
            # Backward compatibility for clients loaded before workspace-bound
            # preflights were introduced.
            return _active_context()
        if isinstance(raw_workspace_id, bool):
            return None, None, json_error("workspace_id must be an integer", 400)
        try:
            workspace_id = int(raw_workspace_id)
        except (TypeError, ValueError):
            return None, None, json_error("workspace_id must be an integer", 400)
        db = get_db()
        if db.get_workspace(workspace_id) is None:
            return None, None, json_error("Workspace not found", 404)
        return db, workspace_id, None

    def _active_root_ids(db, workspace_id):
        # Ids only: the blocker endpoint is polled every 15s from Browse and
        # must not run the per-root photo-count subquery that
        # ``get_workspace_folder_roots`` pays for (~0.75s on a large catalog).
        return db.get_workspace_root_folder_ids(workspace_id)

    def _requested_roots(db, workspace_id, body, *, local_only=False):
        active_roots = set(_active_root_ids(db, workspace_id))
        # A workspace can reach a shared local copy through a recursive
        # ancestor (workspace A links /parent while workspace B stages
        # /parent/child). workspace_status surfaces those descendant sessions
        # in the folder-scoped local status; sync/discard has to accept the
        # descendant's root_folder_id as a valid target too or the UI would
        # show sync/discard controls that always 404.
        descendant_local_roots: set[int] = set()
        if local_only:
            covered = {
                local_root_for_folder(db, folder_id)
                for folder_id in active_roots
            } - {None}
            descendant_local_roots = set(
                workspace_local_root_ids(db, workspace_id)
            ) - covered
        requested = body.get("folder_ids")
        if requested is None:
            if local_only:
                active_local = {
                    local_root_for_folder(db, folder_id)
                    for folder_id in active_roots
                    if local_root_for_folder(db, folder_id) is not None
                }
                return sorted(active_local | descendant_local_roots), None
            return sorted(active_roots), None
        if not isinstance(requested, list) or not requested:
            return None, json_error("folder_ids must be a non-empty list", 400)
        result = []
        for raw in requested:
            try:
                folder_id = int(raw)
            except (TypeError, ValueError):
                return None, json_error("folder_ids must contain integers", 400)
            if folder_id in active_roots:
                covering = local_root_for_folder(db, folder_id)
                result.append(covering if local_only and covering is not None else folder_id)
                continue
            if local_only and folder_id in descendant_local_roots:
                # The descendant session's root_folder_id is already a local root.
                result.append(folder_id)
                continue
            return None, json_error(
                f"Folder {folder_id} is not a root of the active workspace", 404
            )
        return sorted(set(result)), None

    # Job types whose ``config`` records on-disk paths that a stage worker
    # must consider when deciding whether to commit a mapping. A scan or
    # import queued or running against a path this stage is about to rebase
    # would catalog the originals a second time once the mapping publishes,
    # even when that scan lives in a workspace whose folder set has no
    # overlap with the one we are staging. ``_busy_job`` refuses the
    # transition in that case so ``_busy_job_error`` names the racing job.
    # ``_job_config_paths`` handles the per-type key names (``roots``/``root``
    # for scan and metadata-repair; ``source``/``destination`` for
    # ``import-full``; ``sources``/``destination`` for ``import-in-place``
    # and the ``import`` job the ``/api/jobs/import-photos`` route
    # registers).
    _PATH_CONFIG_JOB_TYPES = frozenset(
        {
            "scan", "import-full", "import-in-place",
            "import", "metadata-repair",
        }
    )

    def _stage_source_paths(db, root_ids):
        """Pre-stage source paths for ``root_ids`` (before rebase)."""
        paths = []
        for root_id in root_ids:
            row = db.conn.execute(
                "SELECT path FROM folders WHERE id=?",
                (int(root_id),),
            ).fetchone()
            if row and row["path"]:
                paths.append(row["path"])
        return paths

    def _job_config_paths(config):
        # Different job types record their on-disk paths under different
        # keys. Collecting every shape here keeps ``_busy_job`` honest about
        # what a queued or running import actually walks:
        # * scan and metadata-repair use ``roots``/``root``.
        # * ``import-full`` copies ``source`` to ``destination``.
        # * ``import-in-place`` and the ``import`` job registered by
        #   ``/api/jobs/import-photos`` list source paths in ``sources`` and
        #   record the archive destination (when present) in ``destination``.
        # An overlap on either the source tree or the destination tree lets
        # the importer catalog originals a second time, so both belong here.
        if not isinstance(config, dict):
            return []
        paths = []
        raw_roots = config.get("roots")
        if isinstance(raw_roots, list):
            paths.extend(p for p in raw_roots if isinstance(p, str) and p)
        raw_sources = config.get("sources")
        if isinstance(raw_sources, list):
            paths.extend(p for p in raw_sources if isinstance(p, str) and p)
        for key in ("root", "source", "destination"):
            raw = config.get(key)
            if isinstance(raw, str) and raw:
                paths.append(raw)
        return paths

    def _busy_job(db, root_ids, initiating_workspace_id):
        workspace_ids = {int(initiating_workspace_id)}
        for root_id in root_ids:
            workspace_ids.update(affected_workspace_ids(db, root_id))
            workspace_ids.update(workspace_ids_for_folder_tree(db, root_id))
        stage_source_paths = _stage_source_paths(db, root_ids)
        stage_source_physicals = [
            (source, _resolve_physical(source)) for source in stage_source_paths
        ]
        for job in get_runner().list_jobs():
            # Jobs that carry no pre-rebase photo/folder paths opt out —
            # model/label downloads and embedding precomputes, which touch
            # only ~/.vireo, plus observational jobs such as the automatic
            # new-images walk, whose cache generation is bumped when the
            # transition rebases paths so a result from the old layout is
            # dropped instead of leaking back into the UI. See
            # jobs.CATALOG_INDEPENDENT_JOB_TYPES.
            if job.get("blocks_local_transitions") is False:
                continue
            # ``pausing``/``paused`` jobs still hold their original workspace
            # and root assumptions in the worker's memory. A stage/sync/discard
            # starting under them would race the paused work when it resumes,
            # leaving catalog rows written against paths the folder manifest
            # does not cover.
            if job.get("status") not in {"queued", "running", "pausing", "paused"}:
                continue
            config = job.get("config") or {}
            job_roots = set(config.get("root_folder_ids") or []) if isinstance(config, dict) else set()
            if job.get("workspace_id") in workspace_ids or job_roots.intersection(root_ids):
                return job
            # A scan or import in another workspace whose ``config`` names a
            # path that overlaps our stage source would race us: the mapping
            # this stage creates rebases the catalog under the local copy, but
            # that job walks the original tree and can re-catalog it. Local
            # folder mappings are keyed on source path, not workspace, so the
            # cross-workspace check is what actually protects the shared
            # catalog. ``_busy_job_error`` reports the job type, not any
            # workspace path we did not read from the caller.
            if (
                stage_source_paths
                and job.get("type") in _PATH_CONFIG_JOB_TYPES
            ):
                for job_path in _job_config_paths(config):
                    job_physical = _resolve_physical(job_path)
                    for source, source_physical in stage_source_physicals:
                        if _path_overlaps_source(
                            job_path, job_physical,
                            source, source_physical,
                            include_descendants=True,
                        ):
                            return job
        return None

    def _busy_job_error(job):
        return f"Wait for the {job['type']} job to finish before working locally"

    def _legacy_error(db, workspace_id):
        if legacy_local_state(db, workspace_id):
            return json_error(
                "This workspace has a local session created by an earlier Vireo version. "
                "Finish or discard that session before starting folder-level local work.",
                409,
            )
        return None

    def _folder_names(db, root_ids):
        names = {}
        paths = {}
        for root_id in root_ids:
            row = db.conn.execute(
                """SELECT COALESCE(lfm.source_path, f.path) AS source_path
                   FROM folders f
                   LEFT JOIN local_folder_mappings lfm
                     ON lfm.folder_id=f.id AND lfm.is_root=1
                   WHERE f.id=?""",
                (root_id,),
            ).fetchone()
            path = row["source_path"] if row else ""
            paths[root_id] = path
            names[root_id] = os.path.basename(path.rstrip("/\\")) or "Folder"
        return names, paths

    def _stage_destinations(body, root_ids, root_names, source_paths, *, cancel_check=None):
        raw_destinations = body.get("destination_bases") or {}
        if not isinstance(raw_destinations, dict):
            return None, json_error("destination_bases must be an object", 400)
        destination_bases = {}
        final_destinations = []
        for root_id in root_ids:
            raw = raw_destinations.get(str(root_id), raw_destinations.get(root_id))
            if raw is None:
                continue
            if not isinstance(raw, str) or not raw.strip():
                return None, json_error("Each local destination must be a non-empty path", 400)
            destination = os.path.normpath(os.path.expanduser(raw.strip()))
            if not os.path.isabs(destination):
                return None, json_error("Each local destination must be an absolute path", 400)
            destination_bases[root_id] = destination
            final_path = os.path.abspath(
                local_path_for_base(destination, root_id, source_paths[root_id])
            )
            # ``destination_case_insensitive`` probes the destination volume,
            # which can block for seconds on a slow or unreachable network
            # mount. Cooperate with cancellation between folders so a cancel
            # request only has to wait for the current probe, not every
            # remaining destination, before the request returns.
            if cancel_check and cancel_check():
                raise LocalWorkspaceCancelled(
                    "Preflight cancelled during destination validation"
                )
            try:
                case_insensitive = destination_case_insensitive(
                    final_path, cancel_check=cancel_check
                )
            except LocalWorkspaceCancelled:
                # ``LocalWorkspaceCancelled`` subclasses ``LocalWorkspaceError``.
                # Let it propagate so the route-level handler returns the
                # cancellation-specific error message instead of the generic
                # 409 the destination probe returns for real filesystem
                # failures.
                raise
            except LocalWorkspaceError as exc:
                return None, json_error(str(exc), 409)
            final_destinations.append(
                (
                    root_id,
                    os.path.normcase(final_path),
                    case_insensitive,
                )
            )
        for index, (root_id, path, case_insensitive) in enumerate(final_destinations):
            for other_id, other_path, other_case_insensitive in final_destinations[index + 1 :]:
                compared_path = path
                compared_other_path = other_path
                if case_insensitive or other_case_insensitive:
                    compared_path = unicodedata.normalize("NFC", path).casefold()
                    compared_other_path = unicodedata.normalize("NFC", other_path).casefold()
                try:
                    overlaps = os.path.commonpath(
                        [compared_path, compared_other_path]
                    ) in {compared_path, compared_other_path}
                except ValueError:
                    overlaps = False
                if overlaps:
                    return None, json_error(
                        f"The selected destinations for {root_names[root_id]} and "
                        f"{root_names[other_id]} overlap. Choose separate locations.",
                        400,
                    )
        return destination_bases, None

    def _job_payload(job):
        if job is None:
            return None
        return {
            "id": job["id"],
            "type": job["type"],
            "status": job["status"],
        }

    def _residency_fingerprint(db, workspace_id, selectable_root_ids):
        # A short residency signature so the blocker poller can spot changes
        # that leave no active job behind. Without it, a stage/sync/discard
        # started in another tab that starts *and* finishes between two of
        # this tab's blocker polls looks identical to "nothing happening" —
        # both polls return no job, the signature matches, and load() never
        # refires. Folder-tree badges then stay pinned at their pre-job
        # state (for example LOCAL after another tab already synced and
        # removed the copy) until the page is reloaded.
        visible: set[int] = set()
        for root_id in selectable_root_ids:
            visible.add(int(root_id))
            covering = local_root_for_folder(db, root_id)
            if covering is not None:
                visible.add(int(covering))
        if not visible:
            return ""
        placeholders = ",".join("?" for _ in visible)
        rows = db.conn.execute(
            f"""SELECT root_folder_id, state, activated_at, created_at
                FROM local_folders
                WHERE root_folder_id IN ({placeholders})
                ORDER BY root_folder_id""",
            tuple(sorted(visible)),
        ).fetchall()
        parts = [
            "{root}:{state}:{activated}:{created}".format(
                root=row["root_folder_id"],
                state=row["state"] or "",
                activated=row["activated_at"] or "",
                created=row["created_at"] or "",
            )
            for row in rows
        ]
        return ";".join(parts)

    def _blocking_status_payload(db, workspace_id):
        root_ids = _active_root_ids(db, workspace_id)
        selectable_root_ids = set(root_ids) | set(
            workspace_local_root_ids(db, workspace_id)
        )
        folder_blocking_jobs = {}
        for root_id in selectable_root_ids:
            blocking_job = _busy_job(db, [root_id], workspace_id)
            if blocking_job is not None:
                folder_blocking_jobs[str(root_id)] = _job_payload(blocking_job)
        return {
            "blocking_job": _job_payload(
                _busy_job(db, selectable_root_ids, workspace_id)
            ),
            "folder_blocking_jobs": folder_blocking_jobs,
            "residency_fingerprint": _residency_fingerprint(
                db, workspace_id, selectable_root_ids
            ),
        }

    @blueprint.get("/api/workspaces/active/local-folders/blocker")
    def local_folder_blocker():
        # The full /local-folders payload recursively walks every managed local
        # tree to compute a change summary, so polling it every few seconds to
        # detect newly started jobs keeps the disk busy for large libraries.
        # This endpoint returns only what the UI needs to enable or disable
        # Work Locally controls, so it can safely be polled instead.
        db, workspace_id, error = _active_context()
        if error:
            return error
        return jsonify(_blocking_status_payload(db, workspace_id))

    @blueprint.get("/api/workspaces/active/local-folders")
    def local_folder_status():
        db, workspace_id, error = _active_context()
        if error:
            return error
        try:
            payload = workspace_status(db, workspace_id, vireo_dir)
        except LocalWorkspaceError as exc:
            return json_error(str(exc), 409)
        payload["legacy_workspace_session"] = bool(legacy_local_state(db, workspace_id))

        payload.update(_blocking_status_payload(db, workspace_id))

        jobs = []
        active_roots = set(_active_root_ids(db, workspace_id))
        # Include descendant local sessions (workspace A links /parent while
        # workspace B stages /parent/child): workspace_status surfaces them,
        # so an in-flight sync/discard against those roots has to appear in
        # A's jobs list too — otherwise the UI shows a stale "active" state
        # after the owning workspace kicks off a sync.
        local_roots = {
            local_root_for_folder(db, folder_id)
            for folder_id in active_roots
            if local_root_for_folder(db, folder_id) is not None
        } | set(workspace_local_root_ids(db, workspace_id))
        for job in get_runner().list_jobs():
            if job.get("status") not in {"queued", "running"}:
                continue
            config = job.get("config") or {}
            job_roots = set(config.get("root_folder_ids") or []) if isinstance(config, dict) else set()
            if job.get("type") in LOCAL_FOLDER_JOB_TYPES and (
                job.get("workspace_id") == workspace_id
                or job_roots.intersection(active_roots | local_roots)
            ):
                jobs.append({"id": job["id"], "type": job["type"], "folder_ids": sorted(job_roots)})
        payload["jobs"] = jobs
        return jsonify(payload)

    @blueprint.post("/api/workspaces/active/local-folders/preflight")
    def preflight_local_folders():
        body = request.get_json(silent=True) or {}
        db, workspace_id, error = _preflight_context(body)
        if error:
            return error
        legacy_error = _legacy_error(db, workspace_id)
        if legacy_error is not None:
            return legacy_error
        root_ids, request_error = _requested_roots(db, workspace_id, body)
        if request_error is not None:
            return request_error
        root_ids = [
            root_id
            for root_id in root_ids
            if local_root_for_folder(db, root_id) is None
            and local_root_under_folder(db, root_id) is None
        ]
        if not root_ids:
            return json_error(
                "The selected folders are already local or contain a folder working locally",
                409,
            )
        busy = _busy_job(db, root_ids, workspace_id)
        if busy:
            return json_error(_busy_job_error(busy), 409)
        root_names, source_paths = _folder_names(db, root_ids)
        preflight_id = body.get("preflight_id")
        if not isinstance(preflight_id, str) or not preflight_id.strip():
            preflight_id = None
        preflight_client_id = body.get("preflight_client_id")
        if (
            not isinstance(preflight_client_id, str)
            or not preflight_client_id.strip()
            or len(preflight_client_id) > 128
        ):
            preflight_client_id = None
        raw_seq = body.get("preflight_seq")
        preflight_seq: int | None
        if isinstance(raw_seq, bool):
            preflight_seq = None
        else:
            try:
                preflight_seq = int(raw_seq) if raw_seq is not None else None
            except (TypeError, ValueError):
                preflight_seq = None
        # Register cancellation state before probing destinations so a concurrent
        # cancel request can flip the shared Event between filesystem calls,
        # rather than being reduced to a tombstone that only takes effect once
        # every destination volume has finished responding to
        # ``destination_case_insensitive`` — which can hang for seconds each on
        # an unavailable network mount.
        cancelled = _begin_preflight(
            workspace_id,
            preflight_id,
            preflight_client_id,
            preflight_seq,
        )
        try:
            try:
                destination_bases, destination_error = _stage_destinations(
                    body,
                    root_ids,
                    root_names,
                    source_paths,
                    cancel_check=cancelled.is_set,
                )
            except LocalWorkspaceCancelled:
                return json_error("Folder size calculation was cancelled", 409)
            if destination_error is not None:
                return destination_error
            try:
                result = local_copy_preflight(
                    db,
                    root_ids,
                    vireo_dir,
                    destination_bases=destination_bases,
                    cancel_check=cancelled.is_set,
                )
            except LocalWorkspaceCancelled:
                return json_error("Folder size calculation was cancelled", 409)
            except LocalWorkspaceError as exc:
                return json_error(str(exc), 409)
        finally:
            _finish_preflight(
                workspace_id,
                preflight_id,
                preflight_client_id,
                cancelled,
            )
        return jsonify(result)

    @blueprint.post("/api/workspaces/active/local-folders/preflight/cancel")
    def cancel_preflight_local_folders():
        _db, workspace_id, error = _active_context()
        if error:
            return error
        body = request.get_json(silent=True) or {}
        preflight_id = body.get("preflight_id")
        if not isinstance(preflight_id, str) or not preflight_id.strip():
            return json_error("preflight_id must be a non-empty string", 400)
        cancel_workspace_id = body.get("workspace_id")
        if isinstance(cancel_workspace_id, bool):
            cancel_workspace_id = workspace_id
        else:
            try:
                cancel_workspace_id = int(cancel_workspace_id)
            except (TypeError, ValueError):
                cancel_workspace_id = workspace_id
        return jsonify(
            {"cancelled": _cancel_preflight(cancel_workspace_id, preflight_id)}
        )

    @blueprint.post("/api/workspaces/active/local-folders/stage")
    def stage_local_folders():
        db, workspace_id, error = _active_context()
        if error:
            return error
        legacy_error = _legacy_error(db, workspace_id)
        if legacy_error is not None:
            return legacy_error
        body = request.get_json(silent=True) or {}
        root_ids, request_error = _requested_roots(db, workspace_id, body)
        if request_error is not None:
            return request_error
        # Filter out roots already covered by a local session — either exactly
        # (this root is a staged local root) or as an ancestor of one (a
        # descendant is staged, so the workspace has partial local coverage
        # here already). stage_folder() would otherwise reject the ancestor
        # case with an "overlaps existing local copy" error mid-job, failing
        # the whole bulk stage and leaving the sibling remote roots unstaged.
        remaining = []
        for root_id in root_ids:
            if local_root_for_folder(db, root_id) is not None:
                continue
            if local_root_under_folder(db, root_id) is not None:
                continue
            remaining.append(root_id)
        root_ids = remaining
        if not root_ids:
            return json_error(
                "The selected folders are already local or contain a folder working locally",
                409,
            )
        root_names, source_paths = _folder_names(db, root_ids)
        destination_bases, destination_error = _stage_destinations(
            body, root_ids, root_names, source_paths
        )
        if destination_error is not None:
            return destination_error
        runner = get_runner()

        def work(job):
            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)
            results = []
            try:
                runner.set_steps(
                    job["id"],
                    [
                        {"id": f"folder-{root_id}", "label": f"Copy {root_names[root_id]} locally"}
                        for root_id in root_ids
                    ],
                )
                for root_id in root_ids:
                    step_id = f"folder-{root_id}"
                    runner.update_step(job["id"], step_id, status="running")

                    def report(
                        current, total, current_bytes, total_bytes, path,
                        _root=root_id, _name=root_names[root_id],
                    ):
                        job["progress"].update(
                            {
                                "current": current,
                                "total": total,
                                "current_file": path,
                                "phase": f"Copying {_name} locally",
                                "bytes_current": current_bytes,
                                "bytes_total": total_bytes,
                                "root_folder_id": _root,
                            }
                        )
                        runner.update_step(
                            job["id"],
                            f"folder-{_root}",
                            progress={"current": current, "total": total},
                            current_file=path,
                        )
                        runner.push_event(job["id"], "progress", dict(job["progress"]))

                    result = stage_folder(
                        thread_db,
                        root_id,
                        vireo_dir,
                        local_base=destination_bases.get(root_id),
                        progress=report,
                        cancel_check=lambda: runner.is_cancelled(job["id"]),
                        begin_commit=lambda: runner.begin_uncancellable(job["id"]),
                    )
                    results.append(result)
                    runner.update_step(
                        job["id"], step_id, status="completed", summary=f"{result['files']} files copied"
                    )
                if invalidate_missing_originals:
                    invalidate_missing_originals()
                return {"folders": results}
            finally:
                thread_db.close()

        # Nested lock order is deliberate: ``transition_lock`` first (matches
        # every other stage/sync/discard entry point) and ``stage_boundary_lock``
        # second so ``_busy_job`` can observe scan/import jobs that hold
        # ``stage_boundary_lock`` across their own check-and-register. Scan
        # admissions never take ``transition_lock``, so no cycle is possible.
        with transition_lock, stage_boundary_lock():
            busy = _busy_job(db, root_ids, workspace_id)
            if busy:
                return json_error(_busy_job_error(busy), 409)
            # Recheck residency inside the same registration boundary so
            # two simultaneous requests cannot both report 202 for one
            # folder.
            if any(local_root_for_folder(db, root_id) is not None for root_id in root_ids):
                return json_error("A selected folder is already local", 409)
            job_id = runner.start(
                "work-locally-folder-stage",
                work,
                workspace_id=workspace_id,
                config={"root_folder_ids": root_ids},
            )
        return jsonify({"job_id": job_id, "folder_ids": root_ids}), 202

    @blueprint.post("/api/workspaces/active/local-folders/sync")
    def sync_local_folders():
        db, workspace_id, error = _active_context()
        if error:
            return error
        legacy_error = _legacy_error(db, workspace_id)
        if legacy_error is not None:
            return legacy_error
        body = request.get_json(silent=True) or {}
        root_ids, request_error = _requested_roots(db, workspace_id, body, local_only=True)
        if request_error is not None:
            return request_error
        if not root_ids:
            return json_error("No selected folders are working locally", 409)
        root_names, _source_paths = _folder_names(db, root_ids)
        counts = body.get("confirmed_deletion_counts") or {}
        if not isinstance(counts, dict):
            return json_error("confirmed_deletion_counts must be an object", 400)
        confirmed = {}
        for root_id in root_ids:
            try:
                current = folder_status(db, root_id, vireo_dir)
            except LocalWorkspaceError as exc:
                return json_error(str(exc), 409)
            if current.get("state") not in {"active", "recovery"}:
                return json_error(
                    f"Folder {root_id} has an incomplete local copy; discard it before continuing",
                    409,
                )
            if current.get("state") == "recovery" and current.get("recovery_kind") != "sync":
                return json_error(
                    f"Folder {root_id} cannot be synced because its local copy is incomplete",
                    409,
                )
            if current.get("changes_error"):
                return json_error(current["changes_error"], 409)
            deleted = int((current.get("changes") or {}).get("deleted", 0))
            raw = counts.get(str(root_id), counts.get(root_id))
            if deleted and raw is None:
                return json_error(
                    f"Folder {root_id} would delete {deleted} source file(s); confirm deletions first",
                    409,
                )
            if raw is not None:
                try:
                    raw = int(raw)
                except (TypeError, ValueError):
                    return json_error("Deletion confirmation counts must be integers", 400)
            confirmed[root_id] = raw
        runner = get_runner()

        def work(job):
            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)
            results = []
            try:
                runner.set_steps(
                    job["id"],
                    # Two steps per folder, because a sync has two phases with
                    # very different meanings: checking re-reads source files
                    # and writes nothing, publishing overwrites and deletes
                    # them. One shared counter would hide which is running and
                    # would feed the checking phase's elapsed time into the
                    # publishing phase's ETA.
                    [
                        step
                        for root_id in root_ids
                        for step in (
                            {
                                "id": f"check-{root_id}",
                                "label": f"Check {root_names[root_id]} against source",
                            },
                            {
                                "id": f"folder-{root_id}",
                                "label": f"Sync {root_names[root_id]} to source",
                            },
                        )
                    ],
                )
                scan_totals: dict[int, int] = {}
                active_step = None
                for root_id in root_ids:
                    check_id = f"check-{root_id}"
                    step_id = f"folder-{root_id}"
                    active_step = check_id
                    runner.update_step(job["id"], check_id, status="running")
                    job["progress"].update(
                        {
                            "current": 0,
                            "total": 0,
                            "current_file": "",
                            "phase": f"Checking {root_names[root_id]} against source",
                            "root_folder_id": root_id,
                        }
                    )
                    runner.push_event(job["id"], "progress", dict(job["progress"]))

                    def scan_report(
                        current, total, path,
                        _root=root_id, _name=root_names[root_id],
                    ):
                        scan_totals[_root] = total
                        job["progress"].update(
                            {
                                "current": current,
                                "total": total,
                                "current_file": path,
                                "phase": f"Checking {_name} against source",
                                "root_folder_id": _root,
                            }
                        )
                        runner.update_step(
                            job["id"],
                            f"check-{_root}",
                            progress={"current": current, "total": total},
                            current_file=path,
                        )
                        runner.push_event(job["id"], "progress", dict(job["progress"]))

                    def begin_publish(_root=root_id, _name=root_names[root_id]):
                        """Close the checking phase and open the publishing one."""
                        nonlocal active_step
                        if not runner.begin_uncancellable(job["id"]):
                            return False
                        checked = scan_totals.get(_root, 0)
                        runner.update_step(
                            job["id"],
                            f"check-{_root}",
                            status="completed",
                            summary=(
                                f"{checked:,} checked, no conflicts"
                                if checked
                                else "nothing to check against source"
                            ),
                        )
                        active_step = f"folder-{_root}"
                        runner.update_step(job["id"], active_step, status="running")
                        job["progress"].update(
                            {
                                "current": 0,
                                "total": 0,
                                "current_file": "",
                                "phase": f"Syncing {_name} to source",
                                "root_folder_id": _root,
                            }
                        )
                        runner.push_event(job["id"], "progress", dict(job["progress"]))
                        return True

                    def report(
                        current, total, path,
                        _root=root_id, _name=root_names[root_id],
                    ):
                        job["progress"].update(
                            {
                                "current": current,
                                "total": total,
                                "current_file": path,
                                "phase": f"Syncing {_name} to source",
                                "root_folder_id": _root,
                            }
                        )
                        runner.update_step(
                            job["id"],
                            f"folder-{_root}",
                            progress={"current": current, "total": total},
                            current_file=path,
                        )
                        runner.push_event(job["id"], "progress", dict(job["progress"]))

                    count = confirmed[root_id]
                    try:
                        result = sync_folder(
                            thread_db,
                            root_id,
                            vireo_dir,
                            allow_deletions=count is not None,
                            confirmed_deletions=count,
                            progress=report,
                            scan_progress=scan_report,
                            cancel_check=lambda: runner.is_cancelled(job["id"]),
                            begin_commit=begin_publish,
                        )
                    except LocalWorkspaceCancelled:
                        runner.update_step(job["id"], active_step, status="cancelled")
                        raise
                    except Exception:
                        # A conflict (the usual failure) ends the checking
                        # phase, not the publishing one. Mark whichever step
                        # was running so the card says where it stopped.
                        runner.update_step(job["id"], active_step, status="failed")
                        raise
                    results.append(result)
                    runner.update_step(
                        job["id"],
                        step_id,
                        status="completed",
                        summary=f"{result['created_or_modified']} published, {result['deleted']} deleted",
                    )
                if invalidate_missing_originals:
                    invalidate_missing_originals()
                return {"folders": results}
            finally:
                thread_db.close()

        with transition_lock:
            busy = _busy_job(db, root_ids, workspace_id)
            if busy:
                return json_error(f"Wait for the {busy['type']} job to finish before syncing", 409)
            job_id = runner.start(
                "work-locally-folder-sync",
                work,
                workspace_id=workspace_id,
                config={"root_folder_ids": root_ids},
            )
        return jsonify({"job_id": job_id, "folder_ids": root_ids}), 202

    @blueprint.post("/api/workspaces/active/local-folders/discard")
    def discard_local_folders():
        db, workspace_id, error = _active_context()
        if error:
            return error
        legacy_error = _legacy_error(db, workspace_id)
        if legacy_error is not None:
            return legacy_error
        body = request.get_json(silent=True) or {}
        if body.get("confirm") is not True:
            return json_error("Confirm that local changes may be discarded", 400)
        root_ids, request_error = _requested_roots(db, workspace_id, body, local_only=True)
        if request_error is not None:
            return request_error
        if not root_ids:
            return json_error("No selected folders are working locally", 409)
        root_names, _source_paths = _folder_names(db, root_ids)
        acknowledge = body.get("acknowledge_published") is True
        runner = get_runner()

        def work(job):
            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)
            results = []
            try:
                runner.set_steps(
                    job["id"],
                    [
                        {"id": f"folder-{root_id}", "label": f"Discard local copy of {root_names[root_id]}"}
                        for root_id in root_ids
                    ],
                )
                for root_id in root_ids:
                    step_id = f"folder-{root_id}"
                    runner.update_step(job["id"], step_id, status="running")
                    result = discard_folder(
                        thread_db, root_id, vireo_dir, acknowledge_published=acknowledge
                    )
                    results.append(result)
                    runner.update_step(job["id"], step_id, status="completed")
                if invalidate_missing_originals:
                    invalidate_missing_originals()
                return {"folders": results}
            finally:
                thread_db.close()

        with transition_lock:
            busy = _busy_job(db, root_ids, workspace_id)
            if busy:
                return json_error(
                    f"Wait for the {busy['type']} job to finish before discarding", 409
                )
            job_id = runner.start(
                "work-locally-folder-discard",
                work,
                workspace_id=workspace_id,
                config={"root_folder_ids": root_ids},
            )
        return jsonify({"job_id": job_id, "folder_ids": root_ids}), 202

    return blueprint
