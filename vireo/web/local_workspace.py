"""HTTP endpoints for the managed Work Locally workflow."""

from __future__ import annotations

import threading

from db import Database
from flask import Blueprint, jsonify, request
from services.local_workspace import (
    LocalWorkspaceError,
    discard_local,
    stage_workspace,
    status,
    sync_back,
)


def create_local_workspace_blueprint(get_db, json_error, get_runner, db_path, vireo_dir):
    blueprint = Blueprint("local_workspace", __name__)
    transition_lock = threading.RLock()

    def _active_context():
        db = get_db()
        workspace_id = db._active_workspace_id
        if workspace_id is None:
            return None, None, json_error("No active workspace", 400)
        return db, workspace_id, None

    def _busy_job(workspace_id):
        for job in get_runner().list_jobs():
            if job.get("workspace_id") == workspace_id and job.get("status") in {"queued", "running"}:
                return job
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
            return jsonify(status(db, workspace_id, vireo_dir))
        except LocalWorkspaceError as exc:
            return json_error(str(exc), 409)

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

                result = stage_workspace(
                    thread_db,
                    workspace_id,
                    vireo_dir,
                    progress=report,
                    cancel_check=lambda: runner.is_cancelled(job["id"]),
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
            return None

        job_id, busy, validation_error = _start_job_exclusive(
            workspace_id,
            "work-locally-stage",
            work,
            validate_stage,
        )
        if busy:
            return json_error(
                f"Wait for the {busy['type']} job to finish before working locally",
                409,
            )
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
                return result
            finally:
                thread_db.close()

        def validate_sync():
            try:
                current = status(db, workspace_id, vireo_dir)
            except LocalWorkspaceError as exc:
                return json_error(str(exc), 409)
            if current["state"] != "active":
                return json_error("This workspace is not working locally", 409)
            deletion_count = (current.get("changes") or {}).get("deleted", 0)
            if deletion_count and not allow_deletions:
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
            return json_error(
                f"Wait for the {busy['type']} job to finish before syncing back",
                409,
            )
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
        with transition_lock:
            busy = _busy_job(workspace_id)
            if busy:
                return json_error(
                    f"Wait for the {busy['type']} job to finish before discarding local work",
                    409,
                )
            try:
                return jsonify(discard_local(db, workspace_id, vireo_dir))
            except LocalWorkspaceError as exc:
                return json_error(str(exc), 409)

    return blueprint
