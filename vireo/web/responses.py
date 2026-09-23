"""JSON error responses shared by every route.

``json_error`` is the one shape for a failed API request: a stable legacy
``error`` string, a machine-readable ``code`` (defaulted from the status),
the request id ``web.app_hooks`` stamped on ``g``, and an optional
user-facing ``message``. ``create_app`` injects it into every blueprint.
"""

from flask import g, jsonify

_DEFAULT_ERROR_CODES = {
    400: "invalid_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    409: "conflict",
}


def json_error(msg, status=400, *, code=None, message=None):
    """Return a JSON error response with an optional user-facing message."""
    if code is None:
        code = _DEFAULT_ERROR_CODES.get(status, "request_failed")
    payload = {
        "error": msg,
        "code": code,
        "request_id": getattr(g, "request_id", None),
    }
    if message:
        payload["message"] = message
    return jsonify(payload), status


def photo_not_found_error(*, legacy_error="photo_not_found"):
    """404 for a photo id that is gone or outside the active workspace."""
    return json_error(
        legacy_error,
        404,
        message=(
            "This photo is no longer available in the active workspace. "
            "Refresh the page and try again."
        ),
    )
