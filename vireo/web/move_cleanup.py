"""Optional source cleanup for completed date-organized folder moves."""

import json
import os
import threading
from contextlib import ExitStack

from flask import Blueprint, jsonify, request
from jobs import WorkspaceBusyError
from move_cleanup import cleanup_source, review_source


def create_move_cleanup_blueprint(get_db, get_runner, json_error, trash_paths,
                                  guard_move_folder):
    blueprint = Blueprint("move_cleanup", __name__)
    cleanup_lock = threading.Lock()

    @blueprint.route("/api/jobs/<job_id>/source-cleanup", methods=["GET", "POST"])
    def source_cleanup(job_id):
        db, runner = get_db(), get_runner()
        # Completed history is authoritative, including cleanup receipts saved
        # after the worker finished. The runner's snapshot may predate cleanup.
        row = db.conn.execute("SELECT * FROM job_history WHERE id = ?", (job_id,)).fetchone()
        job = dict(row) if row else runner.get(job_id)
        if not job or job.get("workspace_id") != db._active_workspace_id:
            return json_error("Move job not found in this workspace", 404)
        config = job.get("config") or {}
        result = job.get("result") or {}
        try:
            if isinstance(config, str):
                config = json.loads(config)
            if isinstance(result, str):
                result = json.loads(result)
        except (ValueError, TypeError):
            return json_error("Cleanup job data is invalid", 409)
        if not isinstance(config, dict) or not isinstance(result, dict):
            return json_error("Cleanup job data is invalid", 409)
        if (job.get("type") != "move-folder" or job.get("status") != "completed"
                or not config.get("folder_template") or not config.get("source_path")
                or not result.get("moved") or result.get("errors") or result.get("ok") is False):
            return json_error("Cleanup requires a successfully completed date-organized move", 409)
        try:
            with cleanup_lock, ExitStack() as reservations:
                reservations.enter_context(runner.workspace_mutation(db._active_workspace_id, exclusive=True))
                if request.method == "POST":
                    # An import in any workspace can discover this source.
                    # Hold every workspace reservation through review and Trash,
                    # including workspaces created while reservations are acquired.
                    reserved = {db._active_workspace_id}
                    while pending := {item[0] for item in db.conn.execute("SELECT id FROM workspaces")} - reserved:
                        for workspace_id in sorted(pending):
                            reservations.enter_context(runner.workspace_mutation(workspace_id, exclusive=True))
                            reserved.add(workspace_id)
                source = config["source_path"]
                # A successful cleanup can retire the old ID. Resolve the
                # current row by path so a reused ID cannot guard another folder.
                folder = db.conn.execute("SELECT id FROM folders WHERE path = ?", (source,)).fetchone()
                if folder:
                    error = guard_move_folder(db, folder["id"])
                    if error:
                        return json_error(error, 409)
                if request.method == "GET":
                    receipt = result.get("source_cleanup")
                    device = receipt.get("source_device") if isinstance(receipt, dict) else None
                    inode = receipt.get("source_inode") if isinstance(receipt, dict) else None
                    # Existing files can be freshly reviewed after a legitimate
                    # remount. A missing source needs the saved device evidence.
                    if os.path.lexists(source):
                        device = None
                    review = review_source(db, source, device, inode)
                    if request.args.get("summary") == "1":
                        review = {key: review[key] for key in
                                  ("state", "source_path", "file_count", "xmp_count") if key in review}
                    return jsonify(review)
                body = request.get_json(silent=True)
                if not isinstance(body, dict) or body.get("confirm_trash") is not True:
                    return json_error("Confirm moving the reviewed files to Trash", 400)
                if row is None:
                    return json_error("The move result is still being saved; review again in a moment", 409)
                cleanup = cleanup_source(db, source, body.get("review_token"), trash_paths)
                result["source_cleanup"] = cleanup
                db.conn.execute("UPDATE job_history SET result = ? WHERE id = ?",
                                (json.dumps(result), job_id))
                db.conn.commit()
                return jsonify(cleanup)
        except (ValueError, OSError, WorkspaceBusyError) as exc:
            return json_error(str(exc), 409)

    return blueprint
