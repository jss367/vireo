"""JSON error responses shared by every route.

``json_error`` is the one shape for a failed API request: a stable legacy
``error`` string, a machine-readable ``code`` (defaulted from the status),
the request id ``web.app_hooks`` stamped on ``g``, and an optional
user-facing ``message``. ``create_app`` injects it into every blueprint.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from flask import g, jsonify

if TYPE_CHECKING:
    from services.service_types import ErrorResponse, JsonDict

_DEFAULT_ERROR_CODES = {
    400: "invalid_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    409: "conflict",
}


def json_error(
    msg: str, status: int = 400, *, code: str | None = None, message: str | None = None,
) -> ErrorResponse:
    """Return a JSON error response with an optional user-facing message."""
    if code is None:
        code = _DEFAULT_ERROR_CODES.get(status, "request_failed")
    payload: JsonDict = {
        "error": msg,
        "code": code,
        "request_id": getattr(g, "request_id", None),
    }
    if message:
        payload["message"] = message
    return jsonify(payload), status


def photo_not_found_error(*, legacy_error: str = "photo_not_found") -> ErrorResponse:
    """404 for a photo id that is gone or outside the active workspace."""
    return json_error(
        legacy_error,
        404,
        message=(
            "This photo is no longer available in the active workspace. "
            "Refresh the page and try again."
        ),
    )
