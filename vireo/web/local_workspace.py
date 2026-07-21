"""HTTP endpoints for the managed Work Locally workflow."""

from __future__ import annotations

import threading

from db import Database
from flask import Blueprint, jsonify, request
from services.local_folder import workspace_local_root_ids
from services.local_workspace import (
    LocalWorkspaceError,
    discard_local,
    stage_workspace,
    status,
    sync_back,
)

LOCAL_WORKSPACE_JOB_TYPES = frozenset(
    {"work-locally-stage", "work-locally-sync", "work-locally-discard"}
)


def create_local_workspace_blueprint(
    get_db, json_error, get_runner, db_path, vireo_dir, invalidate_missing_originals=None
):
    blueprint = Blueprint("local_workspace", __name__)
    transition_lock = threading.RLock()

    def _invalidate_caches(workspace_id):
        # Stage/sync/discard rewrite folders.path for the whole workspace;
        # the Missing Originals payload caches absolute paths and must not
        # survive a rebase (the service invalidates the new-images cache).
        if invalidate_missing_originals:
            invalidate_missing_originals(workspace_id)

    def _active_context():
        db = get_db()
        workspace_id = db._active_workspace_id
        if workspace_id is None:
            return None, None, json_error("No active workspace", 400)
        return db, workspace_id, None

    def _busy_job(workspace_id):
        # ``pausing``/``paused`` jobs still own the workspace: their worker
        # threads keep their original workspace and root assumptions in
        # memory and will resume writing catalog rows against the pre-
        # transition layout. Letting a stage/sync/discard proceed while a
        # scan/import is paused would silently orphan rows the manifest
        # doesn't cover once the paused job resumes.
        for job in get_runner().list_jobs():
            # Observational jobs can opt out when their cache invalidation
            # already makes an in-flight result from the old path layout
            # harmless. The automatic new-images walk uses this path.
            if job.get("blocks_local_transitions") is False:
                continue
            if (
                job.get("workspace_id") == workspace_id
                and job.get("status") in {"queued", "running", "pausing", "paused"}
            ):
                return job
        return None

    def _busy_error(busy, action):
        return json_error(f"Wait for the {busy['type']} job to finish before {action}", 409)

    def _folder_sessions_error(db, workspace_id):
        # The legacy workspace-scoped stage/sync/discard uses ``status()``,
        # which only sees ``local_workspace_folders``. A folder-scoped session
        # (``local_folder_mappings``) affecting one of this workspace's folders
        # leaves ``status()`` reporting ``remote``, so a stale client or direct
        # API caller could otherwise start a legacy stage that copies the
        # already-rebased ``local-folders/`` path again — orphaning the folder
        # manifest and ``local_folder_mappings`` from the catalog.
        if workspace_local_root_ids(db, workspace_id):
            return json_error(
                "This workspace has folders working locally through the "
                "shared folder-scoped workflow. Sync or discard those folder "
                "sessions before using the legacy workspace-level action.",
                409,
            )
        return None

    def _start_job_exclusive(workspace_id, job_type, work, validate):
        """Atomically validate workspace state and register its job."""
        with transition_lock:
            busy = _busy_job(workspace_id)
            if busy:
                return None, busy, None
            validation_error = validate()
            if validation_error is not None:
                return None, None, validation_error
            job_id = get_runner().start(
                job_type,
                work,
                workspace_id=workspace_id,
            )
            return job_id, None, None

    @blueprint.get("/api/workspaces/active/local-workspace")
    def local_workspace_status():
        db, workspace_id, error = _active_context()
        if error:
            return error
        try:
            payload = status(db, workspace_id, vireo_dir)
        except LocalWorkspaceError as exc:
            return json_error(str(exc), 409)
        # Live-job awareness: a fresh page load (or second tab) must see a
        # running transfer as in-progress work, never as an interrupted
        # state needing recovery. Only local-workspace transfer jobs are
        # surfaced here; an unrelated pipeline/scan on the same workspace
        # would otherwise render as "Copying workspace locally..." because
        # the client's job watcher treats unknown types as staging.
        busy = _busy_job(workspace_id)
        if busy and busy.get("type") in LOCAL_WORKSPACE_JOB_TYPES:
            payload["job"] = {"id": busy["id"], "type": busy["type"]}
        return jsonify(payload)

    @blueprint.post("/api/workspaces/active/local-workspace/stage")
    def stage_local_workspace():
        db, workspace_id, error = _active_context()
        if error:
            return error
        runner = get_runner()

        def work(job):
            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)
            try:
                runner.set_steps(
                    job["id"],
                    [
                        {"id": "stage", "label": "Copy workspace locally"},
                        {"id": "activate", "label": "Switch catalog to local files"},
                    ],
                )
                runner.update_step(job["id"], "stage", status="running")

                def report(current, total, current_bytes, total_bytes, path):
                    job["progress"].update(
                        {
                            "current": current,
                            "total": total,
                            "current_file": path,
                            "phase": "Copying workspace locally",
                            "bytes_current": current_bytes,
                            "bytes_total": total_bytes,
                        }
                    )
                    runner.update_step(
                        job["id"],
                        "stage",
                        progress={"current": current, "total": total},
                        current_file=path,
                    )
                    runner.push_event(job["id"], "progress", dict(job["progress"]))

                def begin_commit():
                    # The catalog rebase is about to run; a cancel landing
                    # after this point must not mark the job cancelled while
                    # the workspace actually activated.
                    if not runner.begin_uncancellable(job["id"]):
                        return False
                    runner.update_step(
                        job["id"],
                        "stage",
                        status="completed",
                        summary="Workspace copied locally",
                    )
                    runner.update_step(job["id"], "activate", status="running")
                    return True

                result = stage_workspace(
                    thread_db,
                    workspace_id,
                    vireo_dir,
                    progress=report,
                    cancel_check=lambda: runner.is_cancelled(job["id"]),
                    begin_commit=begin_commit,
                )
                runner.update_step(
                    job["id"],
                    "stage",
                    status="completed",
                    summary=f"{result['files']} files copied",
                )
                runner.update_step(
                    job["id"],
                    "activate",
                    status="completed",
                    summary="Workspace is using local files",
                )
                _invalidate_caches(workspace_id)
                return result
            finally:
                thread_db.close()

        def validate_stage():
            try:
                current = status(db, workspace_id, vireo_dir)
            except LocalWorkspaceError as exc:
                return json_error(str(exc), 409)
            if current["state"] != "remote":
                return json_error("This workspace is already staged locally", 409)
            error = _folder_sessions_error(db, workspace_id)
            if error is not None:
                return error
            return None

        job_id, busy, validation_error = _start_job_exclusive(
            workspace_id,
            "work-locally-stage",
            work,
            validate_stage,
        )
        if busy:
            return _busy_error(busy, "working locally")
        if validation_error is not None:
            return validation_error
        return jsonify({"job_id": job_id}), 202

    @blueprint.post("/api/workspaces/active/local-workspace/sync")
    def sync_local_workspace():
        db, workspace_id, error = _active_context()
        if error:
            return error
        body = request.get_json(silent=True) or {}
        allow_deletions = body.get("confirm_deletions") is True
        confirmed_deletions = body.get("confirmed_deletion_count")
        if confirmed_deletions is not None:
            try:
                confirmed_deletions = int(confirmed_deletions)
            except (TypeError, ValueError):
                return json_error("confirmed_deletion_count must be a number", 400)
        # Confirmation without a count is meaningless: the workflow binds the
        # authorization to what the user actually saw. A stale client or direct
        # API caller that sends ``confirm_deletions: true`` alone must not be
        # able to authorize whatever number of deletions happens to be pending
        # at execution time.
        if allow_deletions and confirmed_deletions is None:
            return json_error(
                "confirmed_deletion_count is required when confirming deletions",
                400,
            )

        runner = get_runner()

        def work(job):
            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)
            try:
                runner.set_steps(
                    job["id"],
                    [
                        {"id": "verify", "label": "Check source for conflicts"},
                        {"id": "publish", "label": "Sync local changes to source"},
                        {"id": "restore", "label": "Restore source paths"},
                    ],
                )
                runner.update_step(job["id"], "verify", status="running")

                def begin_commit():
                    if not runner.begin_uncancellable(job["id"]):
                        return False
                    runner.update_step(
                        job["id"],
                        "verify",
                        status="completed",
                        summary="No source conflicts",
                    )
                    runner.update_step(job["id"], "publish", status="running")
                    return True

                def report(current, total, path):
                    job["progress"].update(
                        {
                            "current": current,
                            "total": total,
                            "current_file": path,
                            "phase": "Syncing local changes to source",
                        }
                    )
                    runner.update_step(
                        job["id"],
                        "publish",
                        progress={"current": current, "total": total},
                        current_file=path,
                    )
                    runner.push_event(job["id"], "progress", dict(job["progress"]))

                result = sync_back(
                    thread_db,
                    workspace_id,
                    vireo_dir,
                    allow_deletions=allow_deletions,
                    confirmed_deletions=confirmed_deletions,
                    progress=report,
                    cancel_check=lambda: runner.is_cancelled(job["id"]),
                    begin_commit=begin_commit,
                )
                runner.update_step(
                    job["id"],
                    "publish",
                    status="completed",
                    summary=f"{result['created_or_modified']} files published, {result['deleted']} deleted",
                )
                runner.update_step(
                    job["id"],
                    "restore",
                    status="completed",
                    summary="Workspace is using source storage",
                )
                _invalidate_caches(workspace_id)
                return result
            finally:
                thread_db.close()

        def validate_sync():
            try:
                current = status(db, workspace_id, vireo_dir)
            except LocalWorkspaceError as exc:
                return json_error(str(exc), 409)
            state = current["state"]
            is_sync_recovery = state == "recovery" and current.get("recovery_kind") == "sync"
            if state != "active" and not is_sync_recovery:
                return json_error("This workspace is not working locally", 409)
            error = _folder_sessions_error(db, workspace_id)
            if error is not None:
                return error
            if current.get("changes_error"):
                return json_error(current["changes_error"], 409)
            deletion_count = (current.get("changes") or {}).get("deleted", 0)
            if deletion_count and not allow_deletions and not is_sync_recovery:
                return json_error(
                    f"Sync would delete {deletion_count} source file(s); confirm deletions first",
                    409,
                )
            return None

        job_id, busy, validation_error = _start_job_exclusive(
            workspace_id,
            "work-locally-sync",
            work,
            validate_sync,
        )
        if busy:
            return _busy_error(busy, "syncing back")
        if validation_error is not None:
            return validation_error
        return jsonify({"job_id": job_id}), 202

    @blueprint.post("/api/workspaces/active/local-workspace/discard")
    def discard_local_workspace():
        db, workspace_id, error = _active_context()
        if error:
            return error
        body = request.get_json(silent=True) or {}
        if body.get("confirm") is not True:
            return json_error("Confirm that local changes may be discarded", 400)
        expected_state = body.get("expected_state")
        acknowledge_published = body.get("acknowledge_published") is True

        runner = get_runner()

        def work(job):
            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)
            try:
                runner.set_steps(
                    job["id"],
                    [{"id": "discard", "label": "Discard local copy and restore source paths"}],
                )
                runner.update_step(job["id"], "discard", status="running")
                result = discard_local(
                    thread_db,
                    workspace_id,
                    vireo_dir,
                    acknowledge_published=acknowledge_published,
                )
                runner.update_step(
                    job["id"],
                    "discard",
                    status="completed",
                    summary="Workspace is using source storage",
                )
                _invalidate_caches(workspace_id)
                return result
            finally:
                thread_db.close()

        def validate_discard():
            try:
                current = status(db, workspace_id, vireo_dir)
            except LocalWorkspaceError as exc:
                return json_error(str(exc), 409)
            state = current["state"]
            if state == "remote":
                return json_error("This workspace is not working locally", 409)
            error = _folder_sessions_error(db, workspace_id)
            if error is not None:
                return error
            # The client confirms against the state it rendered. A stale
            # page (e.g. a 'Clean Up Incomplete Copy' button left over from
            # before a stage finished) must never discard a healthy
            # workspace it never described to the user.
            if expected_state is not None and state != expected_state:
                return json_error(
                    "The local workspace changed since this page loaded; refresh to see its current state",
                    409,
                )
            if state == "recovery" and current.get("recovery_kind") == "sync" and not acknowledge_published:
                return json_error(
                    "A sync-back was interrupted after some files were already published. "
                    "Finish the sync-back, or acknowledge that unpublished local changes will be lost.",
                    409,
                )
            return None

        job_id, busy, validation_error = _start_job_exclusive(
            workspace_id,
            "work-locally-discard",
            work,
            validate_discard,
        )
        if busy:
            return _busy_error(busy, "discarding local work")
        if validation_error is not None:
            return validation_error
        return jsonify({"job_id": job_id}), 202

    return blueprint
