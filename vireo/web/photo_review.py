"""Workspace-scoped photo rating and flag endpoints.

A flag write may carry provenance: ``source`` (who decided, e.g. the model
id of a headless reviewer) and ``reason`` (why). Both land in the edit's
history row, so the Undo panel, the toast and the photo detail's
``flag_note`` say who rejected a photo and why, and one Undo reverses it
like any hand flag. The user's own clicks send neither.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from flask import Blueprint, jsonify, request
from services.photo_review import (
    FLAG_REASON_MAX_CHARS,
    FLAG_SOURCE_MAX_CHARS,
    FlagConflict,
    PhotoReviewService,
    WorkspaceConflict,
)

if TYPE_CHECKING:
    from flask.typing import ResponseReturnValue
    from services.service_types import JsonDict, JsonError
    from web.web_types import GetDb

VALID_FLAGS = ("none", "flagged", "rejected")


def flag_provenance(body: JsonDict) -> tuple[tuple[str | None, str | None], None] | tuple[None, str]:
    """``((source, reason), None)`` from a flag body, or ``(None, message)``.

    Each is optional; when present it must be a string, is stripped, and an
    empty string counts as absent. ``source`` is capped at
    ``FLAG_SOURCE_MAX_CHARS`` and ``reason`` at ``FLAG_REASON_MAX_CHARS``.
    """
    values: list[str | None] = []
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
    return (values[0], values[1]), None



def _valid_flag_photo_id(photo_id: object) -> bool:
    """SQLite photo keys are positive signed 64-bit integers, never booleans."""
    return isinstance(photo_id, int) and not isinstance(photo_id, bool) and 0 < photo_id < 2**63


def create_photo_review_blueprint(get_db: GetDb, json_error: JsonError) -> Blueprint:
    blueprint = Blueprint("photo_review", __name__)

    @blueprint.post("/api/photos/<int:photo_id>/rating")
    def set_rating(photo_id: int) -> ResponseReturnValue:
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
    def set_flag(photo_id: int) -> ResponseReturnValue:
        if not _valid_flag_photo_id(photo_id):
            return json_error("photo_id must be a positive signed 64-bit integer")
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        # An explicit flag is required: a missing key must not default to
        # "none" and silently clear the photo's flag (with an undo entry)
        # because a client sent a truncated or unparsable body.
        flag = body.get("flag")
        if flag not in VALID_FLAGS:
            return json_error("flag must be 'none', 'flagged', or 'rejected'")
        provenance = flag_provenance(body)
        if provenance[0] is None:
            return json_error(provenance[1])
        (source, reason), _ = provenance
        observed_flag = body.get("observed_flag")
        if "observed_flag" in body and observed_flag not in VALID_FLAGS:
            return json_error("observed_flag must be 'none', 'flagged', or 'rejected'")
        observed_workspace = body.get("observed_workspace_id")
        if "observed_workspace_id" in body and not _valid_flag_photo_id(observed_workspace):
            return json_error("observed_workspace_id must be a positive integer")
        db = get_db()
        try:
            PhotoReviewService(db).set_flag(
                photo_id, flag, source=source, reason=reason, observed_flag=observed_flag,
                observed_workspace_id=observed_workspace,
            )
        except WorkspaceConflict as exc:
            return json_error(str(exc), 409, code="workspace_changed")
        except FlagConflict as exc:
            return json_error(str(exc), 409, code="flag_conflict")
        except ValueError as exc:
            return json_error(str(exc), 403)
        return jsonify({"ok": True})

    @blueprint.post("/api/batch/rating")
    def set_ratings() -> ResponseReturnValue:
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
    def set_flags() -> ResponseReturnValue:
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        photo_ids = body.get("photo_ids", [])
        # Explicit flag required, as on the single-photo route.
        flag = body.get("flag")
        if flag not in VALID_FLAGS:
            return json_error("flag must be 'none', 'flagged', or 'rejected'")
        if not photo_ids:
            return json_error("photo_ids required")
        if not isinstance(photo_ids, list) or any(
            not _valid_flag_photo_id(pid) for pid in photo_ids
        ):
            return json_error("photo_ids must be a list of positive signed 64-bit integer IDs")
        provenance = flag_provenance(body)
        if provenance[0] is None:
            return json_error(provenance[1])
        (source, reason), _ = provenance
        try:
            updated = PhotoReviewService(get_db()).set_flags(
                photo_ids, flag, source=source, reason=reason,
            )
        except ValueError as exc:
            return json_error(str(exc), 403)
        return jsonify({"ok": True, "updated": updated})

    return blueprint
