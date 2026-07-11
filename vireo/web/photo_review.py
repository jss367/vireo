"""Workspace-scoped photo rating and flag endpoints."""

from flask import Blueprint, jsonify, request
from services.photo_review import PhotoReviewService

VALID_FLAGS = ("none", "flagged", "rejected")


def create_photo_review_blueprint(get_db, json_error):
    blueprint = Blueprint("photo_review", __name__)

    @blueprint.post("/api/photos/<int:photo_id>/rating")
    def set_rating(photo_id):
        body = request.get_json(silent=True) or {}
        rating = body.get("rating", 0)
        if (
            isinstance(rating, bool)
            or not isinstance(rating, int)
            or rating < 0
            or rating > 5
        ):
            return json_error("rating must be an integer 0-5")
        try:
            PhotoReviewService(get_db()).set_rating(photo_id, rating)
        except ValueError as exc:
            return json_error(str(exc), 403)
        return jsonify({"ok": True})

    @blueprint.post("/api/photos/<int:photo_id>/flag")
    def set_flag(photo_id):
        body = request.get_json(silent=True) or {}
        flag = body.get("flag", "none")
        if flag not in VALID_FLAGS:
            return json_error("flag must be 'none', 'flagged', or 'rejected'")
        try:
            PhotoReviewService(get_db()).set_flag(photo_id, flag)
        except ValueError as exc:
            return json_error(str(exc), 403)
        return jsonify({"ok": True})

    @blueprint.post("/api/batch/rating")
    def set_ratings():
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        rating = body.get("rating", 0)
        if (
            isinstance(rating, bool)
            or not isinstance(rating, int)
            or rating < 0
            or rating > 5
        ):
            return json_error("rating must be an integer 0-5")
        if not photo_ids:
            return json_error("photo_ids required")
        try:
            updated = PhotoReviewService(get_db()).set_ratings(photo_ids, rating)
        except ValueError as exc:
            return json_error(str(exc), 403)
        return jsonify({"ok": True, "updated": updated})

    @blueprint.post("/api/batch/flag")
    def set_flags():
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        flag = body.get("flag", "none")
        if flag not in VALID_FLAGS:
            return json_error("flag must be 'none', 'flagged', or 'rejected'")
        if not photo_ids:
            return json_error("photo_ids required")
        try:
            updated = PhotoReviewService(get_db()).set_flags(photo_ids, flag)
        except ValueError as exc:
            return json_error(str(exc), 403)
        return jsonify({"ok": True, "updated": updated})

    return blueprint
