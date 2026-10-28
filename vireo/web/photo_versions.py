"""Virtual copies and named checkpoints, scoped by the original photo's visibility."""

from __future__ import annotations

from typing import TYPE_CHECKING

from app_paths import media_root
from flask import Blueprint, jsonify, request
from repositories.photo_versions import VersionConflict
from services.photo_versions import change_version
from web.photo_edit_recipes import _local_mask_stale

if TYPE_CHECKING:
    from flask.typing import ResponseReturnValue
    from services.service_types import JsonError
    from web.web_types import AppConfig, GetDb


def create_photo_versions_blueprint(get_db: GetDb, json_error: JsonError, config: AppConfig) -> Blueprint:
    blueprint = Blueprint("photo_versions", __name__)

    @blueprint.route("/api/photos/<int:photo_id>/versions", methods=["GET", "POST"])
    @blueprint.route("/api/photos/<int:photo_id>/versions/<version_id>", methods=["GET", "PUT", "DELETE"])
    def versions(photo_id: int, version_id: str | None = None) -> ResponseReturnValue:
        db = get_db()
        if not db.photos.get(photo_id, verify_workspace=True):
            return json_error("Photo not found", 404)
        if request.method == "GET":
            if version_id is None:
                return jsonify({"versions": db.photo_versions.list(photo_id)})
            version = db.photo_versions.get(photo_id, version_id)
            if version is None:
                return json_error("Version not found", 404)
            return jsonify({"version": version, "recipe": version["recipe"],
                            "local_mask_stale": _local_mask_stale(db, photo_id, version["recipe"])})
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        try:
            version = change_version(db, photo_id, version_id, body,
                                     media_root(config["THUMB_CACHE_DIR"]), delete=request.method == "DELETE")
        except VersionConflict as exc:
            return json_error(str(exc), 409)
        except LookupError as exc:
            return json_error(str(exc), 404)
        except ValueError as exc:
            return json_error(str(exc))
        recipe = version["recipe"] if version else None
        return jsonify({"ok": True, "version": version, "recipe": recipe,
                        "local_mask_stale": _local_mask_stale(db, photo_id, recipe)})

    @blueprint.route("/api/photos/<int:photo_id>/versions/<version_id>/history")
    def history(photo_id: int, version_id: str) -> ResponseReturnValue:
        db = get_db()
        if not db.photos.get(photo_id, verify_workspace=True) or not db.photo_versions.get(photo_id, version_id):
            return json_error("Version not found", 404)
        return jsonify({"history": db.photo_versions.history(photo_id, version_id, db.require_workspace_id())})

    return blueprint
