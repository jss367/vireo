"""Optional source cleanup for completed date-organized folder moves."""

import json
import threading

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
        job = runner.get(job_id)
        if job is None:
            row = db.conn.execute("SELECT * FROM job_history WHERE id = ?", (job_id,)).fetchone()
            job = dict(row) if row else None
        if not job or job.get("workspace_id") != db._active_workspace_id:
            return json_error("Move job not found in this workspace", 404)
        config = job.get("config") or {}
        result = job.get("result") or {}
        if isinstance(config, str):
            config = json.loads(config)
        if isinstance(result, str):
            result = json.loads(result)
        if (job.get("type") != "move-folder" or job.get("status") != "completed"
                or not config.get("folder_template") or not config.get("source_path")
                or not result.get("moved") or result.get("errors") or result.get("ok") is False):
            return json_error("Cleanup requires a successfully completed date-organized move", 409)
        try:
            with cleanup_lock, runner.workspace_mutation(db._active_workspace_id, exclusive=True):
                error = guard_move_folder(db, config.get("folder_id"))
                if error:
                    return json_error(error, 409)
                source = config["source_path"]
                if request.method == "GET":
                    return jsonify(review_source(db, source))
                body = request.get_json(silent=True)
                if not isinstance(body, dict) or body.get("confirm_trash") is not True:
                    return json_error("Confirm moving the reviewed files to Trash", 400)
                return jsonify(cleanup_source(db, source, body.get("review_token"), trash_paths))
        except (ValueError, OSError, WorkspaceBusyError) as exc:
            return json_error(str(exc), 409)

    return blueprint
