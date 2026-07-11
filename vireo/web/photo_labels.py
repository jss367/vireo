"""Workspace-scoped photo color-label endpoints."""

from flask import Blueprint, jsonify, request
from repositories.photo_labels import VALID_COLOR_LABELS
from services.photo_labels import PhotoLabelService


def create_photo_labels_blueprint(get_db, json_error):
    blueprint = Blueprint("photo_labels", __name__)

    @blueprint.get("/api/photos/color_labels")
    def get_labels():
        ids_str = request.args.get("ids", "")
        if not ids_str:
            return jsonify({})
        photo_ids = [
            int(value)
            for value in ids_str.split(",")
            if value.strip().isdigit()
        ]
        labels = PhotoLabelService(get_db()).labels_for_photos(photo_ids)
        return jsonify(labels)

    @blueprint.post("/api/photos/<int:photo_id>/color_label")
    def set_label(photo_id):
        body = request.get_json(silent=True) or {}
        color = body.get("color")
        if color is not None and color not in VALID_COLOR_LABELS:
            return json_error(f"color must be one of {VALID_COLOR_LABELS}")
        try:
            PhotoLabelService(get_db()).set_label(photo_id, color)
        except LookupError:
            return json_error("not found", 404)
        except ValueError as exc:
            return json_error(str(exc), 403)
        return jsonify({"ok": True})

    @blueprint.post("/api/batch/color_label")
    def set_labels():
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        color = body.get("color")
        if color is not None and color not in VALID_COLOR_LABELS:
            return json_error(f"color must be one of {VALID_COLOR_LABELS}")
        if not photo_ids:
            return json_error("photo_ids required")
        updated = PhotoLabelService(get_db()).set_labels(photo_ids, color)
        return jsonify({"ok": True, "updated": updated})

    return blueprint
