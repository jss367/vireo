"""Workspace-scoped photo rating and flag endpoints.

A flag write may carry provenance: ``source`` (who decided, e.g. the model
id of a headless reviewer) and ``reason`` (why). Both land in the edit's
history row, so the Undo panel, the toast and the photo detail's
``flag_note`` say who rejected a photo and why, and one Undo reverses it
like any hand flag. The user's own clicks send neither.
"""

from flask import Blueprint, jsonify, request
from services.photo_review import (
    FLAG_REASON_MAX_CHARS,
    FLAG_SOURCE_MAX_CHARS,
    PhotoReviewService,
)

VALID_FLAGS = ("none", "flagged", "rejected")


def flag_provenance(body):
    """``((source, reason), None)`` from a flag body, or ``(None, message)``.

    Each is optional; when present it must be a string, is stripped, and an
    empty string counts as absent. ``source`` is capped at
    ``FLAG_SOURCE_MAX_CHARS`` and ``reason`` at ``FLAG_REASON_MAX_CHARS``.
    """
    values = []
    for key, limit in (("source", FLAG_SOURCE_MAX_CHARS), ("reason", FLAG_REASON_MAX_CHARS)):
        value = body.get(key)
        if value is None:
            values.append(None)
            continue
        if not isinstance(value, str):
            return None, f"{key} must be a string"
        value = value.strip()
        if len(value) > limit:
            return None, f"{key} must be at most {limit} characters"
        values.append(value or None)
    return tuple(values), None


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
        provenance, message = flag_provenance(body)
        if message:
            return json_error(message)
        source, reason = provenance
        try:
            PhotoReviewService(get_db()).set_flag(photo_id, flag, source=source, reason=reason)
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
        provenance, message = flag_provenance(body)
        if message:
            return json_error(message)
        source, reason = provenance
        try:
            updated = PhotoReviewService(get_db()).set_flags(
                photo_ids, flag, source=source, reason=reason,
            )
        except ValueError as exc:
            return json_error(str(exc), 403)
        return jsonify({"ok": True, "updated": updated})

    return blueprint
