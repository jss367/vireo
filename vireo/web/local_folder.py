"""HTTP endpoints for shared, folder-scoped Work Locally sessions."""

from __future__ import annotations

import os
import threading

from db import Database
from flask import Blueprint, jsonify, request
from services.local_folder import (
    LocalWorkspaceError,
    affected_workspace_ids,
    discard_folder,
    folder_status,
    local_path_for_base,
    local_root_for_folder,
    local_root_under_folder,
    stage_folder,
    sync_folder,
    workspace_ids_for_folder_tree,
    workspace_local_root_ids,
    workspace_status,
)
from services.local_workspace import local_state as legacy_local_state

LOCAL_FOLDER_JOB_TYPES = frozenset(
    {"work-locally-folder-stage", "work-locally-folder-sync", "work-locally-folder-discard"}
)


def create_local_folder_blueprint(
    get_db, json_error, get_runner, db_path, vireo_dir, invalidate_missing_originals=None
):
    blueprint = Blueprint("local_folder", __name__)
    transition_lock = threading.RLock()

    def _active_context():
        db = get_db()
        workspace_id = db._active_workspace_id
        if workspace_id is None:
            return None, None, json_error("No active workspace", 400)
        return db, int(workspace_id), None

    def _active_root_ids(db, workspace_id):
        return [int(row["id"]) for row in db.get_workspace_folder_roots(workspace_id)]

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

    def _busy_job(db, root_ids, initiating_workspace_id):
        workspace_ids = {int(initiating_workspace_id)}
        for root_id in root_ids:
            workspace_ids.update(affected_workspace_ids(db, root_id))
            workspace_ids.update(workspace_ids_for_folder_tree(db, root_id))
        for job in get_runner().list_jobs():
            # Observational jobs such as the automatic new-images walk do not
            # retain catalog writes or path mutations that a local transition
            # can invalidate. Their cache generation is bumped when the
            # transition rebases paths, so a result from the old layout is
            # dropped instead of leaking back into the UI.
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
        return None

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
        raw_destinations = body.get("destination_bases") or {}
        if not isinstance(raw_destinations, dict):
            return json_error("destination_bases must be an object", 400)
        root_names, source_paths = _folder_names(db, root_ids)
        destination_bases = {}
        final_destinations = []
        for root_id in root_ids:
            raw = raw_destinations.get(str(root_id), raw_destinations.get(root_id))
            if raw is None:
                continue
            if not isinstance(raw, str) or not raw.strip():
                return json_error("Each local destination must be a non-empty path", 400)
            destination = os.path.normpath(os.path.expanduser(raw.strip()))
            if not os.path.isabs(destination):
                return json_error("Each local destination must be an absolute path", 400)
            destination_bases[root_id] = destination
            final_destinations.append(
                (root_id, os.path.normcase(os.path.abspath(local_path_for_base(
                    destination, root_id, source_paths[root_id]
                ))))
            )
        for index, (root_id, path) in enumerate(final_destinations):
            for other_id, other_path in final_destinations[index + 1:]:
                try:
                    overlaps = os.path.commonpath([path, other_path]) in {path, other_path}
                except ValueError:
                    overlaps = False
                if overlaps:
                    return json_error(
                        f"The selected destinations for {root_names[root_id]} and "
                        f"{root_names[other_id]} overlap. Choose separate locations.",
                        400,
                    )
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

        with transition_lock:
            busy = _busy_job(db, root_ids, workspace_id)
            if busy:
                return json_error(
                    f"Wait for the {busy['type']} job to finish before working locally", 409
                )
            # Recheck residency inside the same registration boundary so two
            # simultaneous requests cannot both report 202 for one folder.
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
                    [
                        {"id": f"folder-{root_id}", "label": f"Sync {root_names[root_id]} to source"}
                        for root_id in root_ids
                    ],
                )
                for root_id in root_ids:
                    step_id = f"folder-{root_id}"
                    runner.update_step(job["id"], step_id, status="running")

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
                    result = sync_folder(
                        thread_db,
                        root_id,
                        vireo_dir,
                        allow_deletions=count is not None,
                        confirmed_deletions=count,
                        progress=report,
                        cancel_check=lambda: runner.is_cancelled(job["id"]),
                        begin_commit=lambda: runner.begin_uncancellable(job["id"]),
                    )
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
