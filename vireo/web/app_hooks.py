"""App-wide request hooks and the per-request ``Database``.

``register_app_hooks`` installs, in one place, every hook ``create_app``
used to define inline:

* ``before_request``, in this order: request timing and id
  (``_start_timer``), the browser-surface guard
  (``_protect_browser_surface``), the ``/api/v1`` token check
  (``_enforce_api_v1_token``), the non-object JSON body check
  (``_reject_non_object_json_body``), and the workspace mutation
  reservation (``_reserve_workspace_mutation``). Flask runs them in
  registration order and stops at the first that returns a response, so the
  order is part of the security contract: a cross-site or malformed request
  is refused before it can take a reservation.
* ``after_request``: action/slow/API logging plus the security headers and
  browser session cookie (``_log_requests``).
* ``errorhandler(Exception)``: ``_handle_error``.
* ``teardown_appcontext``: ``close_request_db`` closes the per-request
  ``Database`` that ``get_request_db`` opened on ``g``.
* ``teardown_request``: ``_release_workspace_mutation``.
"""

import contextlib
import logging
import re
import secrets
import time
import uuid
from urllib.parse import urlsplit

from db import Database
from flask import g, jsonify, request
from jobs import WorkspaceBusyError
from web.responses import json_error
from werkzeug.exceptions import HTTPException

log = logging.getLogger(__name__)

# Sending and source cleanup establish their own exclusive reservations.
# Control requests remain available while a transfer holds the workspace.
# ``create_app`` copies this into the mutable set it hands to
# ``register_app_hooks``; its /api/v1 alias loop then adds ``v1_<view>`` for
# every aliased endpoint listed here, so headless clients get the same
# exemptions.
RESERVATION_EXEMPT_ENDPOINTS = frozenset({
    "imports.api_send_pending_archive", "workspaces.api_activate_workspace",
    "system.api_shutdown", "system.api_v1_shutdown",
    "jobs.api_job_cancel", "jobs.api_job_pause", "jobs.api_job_resume",
    "jobs.api_jobs_cancel_queued",
    "move_cleanup.source_cleanup",
})


def get_request_db(db_path):
    """Get a Database instance. One connection per request via Flask g."""
    if "db" not in g:
        g.db = Database(
            db_path,
            initialize_schema=(db_path == ":memory:"),
        )
    return g.db


def close_request_db(exc):
    """Close the request's Database, if one was opened."""
    db = g.pop("db", None)
    if db is not None:
        db.conn.close()


def register_app_hooks(app, *, get_db, reservation_exempt_endpoints):
    """Install the app-wide request hooks on ``app``.

    ``get_db`` returns the request's ``Database``.
    ``reservation_exempt_endpoints`` is the set of endpoint names that skip
    the workspace mutation reservation; it is read at request time, so
    endpoints ``create_app`` adds after this call are honored.
    """

    # Request timing middleware — logs slow requests and user actions
    @app.before_request
    def _start_timer():
        request._start_time = time.time()
        supplied_request_id = request.headers.get("X-Request-ID", "")
        if re.fullmatch(r"[A-Za-z0-9._-]{1,64}", supplied_request_id):
            g.request_id = supplied_request_id
        else:
            g.request_id = uuid.uuid4().hex

    @app.before_request
    def _protect_browser_surface():
        """Keep hostile web pages away from Vireo's localhost interface.

        `/api/v1` remains token-authenticated for automation.  The internal
        browser API and photo responses use an HttpOnly, SameSite-strict
        session established by a Vireo HTML page.  Unsafe browser requests
        additionally carry a header that cross-origin forms cannot emit.
        """
        if not app.config["BROWSER_AUTH_ENABLED"]:
            return None

        path = request.path
        if path.startswith("/api/v1/") or path in {
            "/api/health",
            "/api/shutdown",
        }:
            return None

        protected = (
            path.startswith("/api/")
            or path.startswith("/photos/")
            or path.startswith("/thumbnails/")
        )
        if not protected:
            return None

        # Native desktop clients cannot use the HttpOnly browser cookie.
        # They authenticate with the same per-runtime secret as /api/v1.
        expected_api_token = app.config.get("API_TOKEN")
        supplied_api_token = request.headers.get("X-Vireo-Token", "")
        if (
            expected_api_token
            and supplied_api_token
            and secrets.compare_digest(
                supplied_api_token.encode("utf-8"),
                expected_api_token.encode("utf-8"),
            )
        ):
            return None

        fetch_site = request.headers.get("Sec-Fetch-Site", "").lower()
        if fetch_site and fetch_site not in {"same-origin", "none"}:
            return json_error(
                "Non-origin requests are not allowed",
                403,
                code="cross_site_request",
            )

        origin = request.headers.get("Origin")
        if origin:
            parsed = urlsplit(origin)
            expected = urlsplit(request.host_url)
            if (parsed.scheme, parsed.netloc) != (expected.scheme, expected.netloc):
                return json_error(
                    "Cross-origin requests are not allowed",
                    403,
                    code="cross_origin_request",
                )

        cookie_name = app.config["BROWSER_SESSION_COOKIE"]
        expected_token = app.config["BROWSER_SESSION_TOKEN"]
        if not secrets.compare_digest(
            request.cookies.get(cookie_name, "").encode("utf-8"),
            expected_token.encode("utf-8"),
        ):
            return json_error(
                "Browser session required",
                401,
                code="browser_session_required",
            )

        if (
            request.method not in {"GET", "HEAD", "OPTIONS"}
            and request.headers.get("X-Vireo-Client") != "browser"
        ):
            return json_error(
                "Missing browser request header",
                403,
                code="browser_header_required",
            )
        return None

    @app.after_request
    def _log_requests(response):
        if hasattr(request, "_start_time"):
            elapsed = time.time() - request._start_time
            if request.method in ("POST", "DELETE"):
                # Log user actions with details about what changed
                detail = ""
                path = request.path
                if path in ("/api/capture-time/preview", "/api/jobs/capture-time"):
                    body = {}
                else:
                    body = request.get_json(silent=True) or {}
                    if not isinstance(body, dict):
                        # Valid non-object JSON (5, "x", [..]) — the
                        # .get() calls below would 500 the response of
                        # any endpoint after it already ran.
                        body = {}
                if "/rating" in path:
                    detail = f" rating={body.get('rating')}"
                elif "/flag" in path:
                    detail = f" flag={body.get('flag')}"
                elif "/keywords" in path and request.method == "POST":
                    detail = f" keyword={body.get('name')}"
                elif "/accept" in path:
                    detail = " (accept prediction)"
                elif "/reject" in path:
                    detail = " (reject prediction)"
                elif "batch" in path:
                    # The view already ran (and may have committed), so a
                    # malformed ``photo_ids`` must not turn its response
                    # into a 500 here.
                    ids = body.get("photo_ids")
                    if isinstance(ids, list):
                        detail = f" ({len(ids)} photos)"
                elif "/classify" in path:
                    detail = f" collection={body.get('collection_id')}"
                elif "/scan" in path:
                    detail = f" root={body.get('root', '')}"
                log.info(
                    "Action: %s %s → %s (%.1fs)%s request_id=%s",
                    request.method,
                    path,
                    response.status_code,
                    elapsed,
                    detail,
                    getattr(g, "request_id", "-"),
                )
            elif elapsed > 0.5:
                log.warning(
                    "Slow request: %s %s took %.1fs request_id=%s",
                    request.method,
                    request.path,
                    elapsed,
                    getattr(g, "request_id", "-"),
                )
            if request.path.startswith("/api/"):
                _quiet = request.method == "GET" and request.path == "/api/jobs"
                (log.debug if _quiet else log.info)(
                    "API: %s %s → %s (%.3fs) request_id=%s",
                    request.method,
                    request.path,
                    response.status_code,
                    elapsed,
                    getattr(g, "request_id", "-"),
                )
        request_id = getattr(g, "request_id", None)
        if request_id:
            response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Content-Security-Policy-Report-Only"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://unpkg.com; "
            "style-src 'self' 'unsafe-inline' https://unpkg.com; "
            "img-src 'self' data: blob: https:; "
            "connect-src 'self' https:; frame-ancestors 'none'; "
            "base-uri 'self'; form-action 'self'"
        )
        if (
            app.config["BROWSER_AUTH_ENABLED"]
            and response.mimetype == "text/html"
            and 200 <= response.status_code < 400
        ):
            response.set_cookie(
                app.config["BROWSER_SESSION_COOKIE"],
                app.config["BROWSER_SESSION_TOKEN"],
                httponly=True,
                samesite="Strict",
                secure=request.is_secure,
                path="/",
            )
        return response

    # Catch uncaught exceptions so they don't disappear silently
    @app.errorhandler(Exception)
    def _handle_error(e):
        if isinstance(e, WorkspaceBusyError):
            return json_error(str(e), 409)
        if isinstance(e, HTTPException):
            return e
        log.exception("Unhandled error: %s %s", request.method, request.path)
        return jsonify({
            "error": "Internal server error",
            "code": "internal_error",
            "message": "Something went wrong in Vireo. Try again.",
            "request_id": getattr(g, "request_id", None),
        }), 500

    app.teardown_appcontext(close_request_db)

    @app.before_request
    def _enforce_api_v1_token():
        if not request.path.startswith("/api/v1/"):
            return None
        expected = app.config.get("API_TOKEN")
        if not expected:
            # No token configured → deny all v1 traffic.
            return json_error("API token not configured", 401)
        supplied = request.headers.get("X-Vireo-Token", "")
        # ``secrets.compare_digest`` raises ``TypeError`` when either str
        # operand contains a non-ASCII code point, which would surface as a
        # 500 for an attacker-supplied token — encode to bytes so a bogus
        # header is a plain 401 like any other wrong value.
        if not secrets.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8")):
            return json_error("Invalid or missing X-Vireo-Token", 401)
        return None

    @app.before_request
    def _reject_non_object_json_body():
        """Refuse a JSON body that parses to anything but an object.

        Every API route reads its body as an object, usually through
        ``request.get_json(silent=True) or {}``. That guards only against a
        missing or unparsable body: a valid non-object document such as
        ``"x"``, ``[1]`` or ``5`` comes through as-is and the route's first
        ``body.get(...)`` raises, so the client gets a 500 instead of a 400.
        Answering here covers every route at once, before the request takes
        a workspace reservation. Unparsable JSON is left to the route, which
        already decides how to report it.
        """
        if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
            return None
        if not request.path.startswith("/api/") or not request.is_json:
            return None
        body = request.get_json(silent=True)
        if body is None or isinstance(body, dict):
            return None
        return json_error(
            "request body must be a JSON object", 400,
            code="json_body_not_object",
        )

    @app.before_request
    def _reserve_workspace_mutation():
        if request.method not in {"POST", "PUT", "PATCH", "DELETE"} or not request.path.startswith("/api/"):
            return None
        if request.endpoint in reservation_exempt_endpoints:
            return None
        target_ws = (request.view_args or {}).get("ws_id")
        # A request with no active workspace and no explicit target has no
        # workspace to reserve; endpoints that handle "no active workspace"
        # themselves (e.g. the offline-banner recheck no-op) must still reach
        # their view function instead of 500ing out of the before_request.
        # Background-job routes that capture ``ctx.workspace_id`` here and
        # hand it to a worker (scan, import-full, import-photos,
        # import-in-place) must reject the no-workspace case themselves so
        # they do not commit catalog rows invisible to every workspace.
        active_ws = get_db()._active_workspace_id
        workspaces = set()
        if active_ws is not None:
            workspaces.add(active_ws)
        if target_ws is not None:
            workspaces.add(target_ws)
        if not workspaces:
            return None
        with contextlib.ExitStack() as reservation:
            for workspace_id in sorted(workspaces):
                reservation.enter_context(app._job_runner.workspace_mutation(
                    workspace_id,
                    exclusive=(
                        request.endpoint == "workspaces.api_delete_workspace"
                        and workspace_id == target_ws
                    ),
                ))
            g.nas_workspace_mutation = reservation.pop_all()
        return None

    @app.teardown_request
    def _release_workspace_mutation(exc):
        reservation = g.pop("nas_workspace_mutation", None)
        if reservation is not None:
            reservation.__exit__(None, None, None)
