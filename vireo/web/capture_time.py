"""Capture-time correction preview.

``/api/capture-time/preview`` shows how a capture-time correction would
rewrite the first few selected photos before the user starts the
``/api/jobs/capture-time`` job (which lives with the job routes in
``web.jobs``). The correction math is in the top-level ``capture_time``
module.
"""

from __future__ import annotations

from flask import Blueprint, jsonify, request


def create_capture_time_blueprint(get_db, json_error):
    """Build the capture-time preview blueprint. Nothing beyond ``get_db``
    and ``json_error`` is injected."""
    blueprint = Blueprint("capture_time", __name__)

    @blueprint.route("/api/capture-time/preview", methods=["POST"])
    def api_capture_time_preview():
        """Preview a capture-time correction for selected photos."""
        from capture_time import build_capture_time_preview

        db = get_db()
        body = request.get_json(silent=True)
        if body is None:
            body = {}
        elif not isinstance(body, dict):
            return json_error("request body must be a JSON object", 400)
        raw_ids = body.get("photo_ids", [])
        if not isinstance(raw_ids, list):
            return json_error("photo_ids must be a list", 400)
        if not raw_ids:
            return json_error("photo_ids required", 400)
        if len(raw_ids) > 50000:
            return json_error("too many photo_ids", 400)

        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must be integers", 400)
            if raw in seen:
                continue
            seen.add(raw)
            photo_ids.append(raw)

        photos = []
        for pid in photo_ids[:20]:
            photo = db.get_photo(pid, verify_workspace=True)
            if photo:
                photos.append(photo)
        if not photos:
            return json_error("no photos found", 404)

        try:
            preview = build_capture_time_preview(
                photos,
                mode=body.get("mode", "preserve_instant"),
                target_offset=body.get("target_offset"),
                shift_minutes=body.get("shift_minutes"),
                limit=5,
            )
        except ValueError as exc:
            return json_error(str(exc), 400)
        preview["count"] = len(photo_ids)
        return jsonify(preview)

    return blueprint
