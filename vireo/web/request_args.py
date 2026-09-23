"""Request-argument parsers and scope guards shared by photo route groups.

Browse, collections, filters, predictions, selection, dashboard and the job
launchers read the same query parameters and body fields (boolean flags, the
universal-filter ``rules`` tree, the ``visual`` clause, collection ids,
selection ``photo_ids``). Parsing them in one place keeps every endpoint's
400s and scoping identical as those groups move into their own blueprints.

The ``request_*`` parsers (including the ``flag`` / ``location_status``
filters) read ``flask.request`` and raise ``ValueError`` for the caller to
turn into a 400. Helpers that must answer with a response take the app's
``json_error`` as a keyword argument, the same convention as
``services.pipeline_launch.resolve_remote_archive_target``.
"""

from __future__ import annotations

import json

from flask import request
from services.visual_scope import (
    VISUAL_COLLECTION_MSG,
    collection_row,
    inject_active_visual_model,
    validate_visual_arg,
)

# Largest photo selection the Browse selection panels will act on. Owned here
# rather than inline so the producer (``/api/selection/*``, which reads a
# selection) and the consumers (``/api/predictions/batch-*``, which write one)
# are bounded by the same number. A batch payload is validated in photos, not
# in prediction ids, precisely so it cannot be tighter than what the producer
# is allowed to emit for that selection.
MAX_SELECTION_PHOTOS = 1000

# A focused lookup may be asked about several photos at once (every frame
# of the stack Browse is holding onto). Bursts are runs of frames, not
# catalogs, so this is a sanity bound on the request rather than a policy
# limit — each candidate is one more ID in one ranking query.
MAX_FOCUS_PHOTO_IDS = 200


def request_bool_arg(name):
    """Return whether query param ``name`` holds a truthy flag value."""
    raw = request.args.get(name, "")
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def request_flag_filter():
    """Parse the optional ``flag`` query param (``none``/``flagged``/``rejected``)."""
    flag = request.args.get("flag", None)
    if flag in (None, ""):
        return None
    if flag not in ("none", "flagged", "rejected"):
        raise ValueError("flag must be 'none', 'flagged', or 'rejected'")
    return flag


def request_location_status_filter():
    """Parse the optional ``location_status`` query param (``exif``/``assigned``/``none``)."""
    value = (request.args.get("location_status") or "").strip().lower()
    if not value:
        return None
    if value not in {"exif", "assigned", "none"}:
        raise ValueError(
            "location_status must be 'exif', 'assigned', or 'none'"
        )
    return value


def request_rules_arg():
    """Parse the optional ``rules`` query param (a JSON universal-filter
    rule tree). Returns None when absent; raises ValueError on invalid
    JSON so callers surface a 400. The active visual model is injected
    exactly as /api/photos/query does, keeping every rules-accepting
    endpoint's notion of "has a visual index" consistent.
    """
    raw = request.args.get("rules")
    if not raw:
        return None
    try:
        rules = json.loads(raw)
    except ValueError as exc:
        raise ValueError("rules must be valid JSON") from exc
    return inject_active_visual_model(rules)


def request_visual_arg():
    """Parse the optional ``visual`` query param (JSON clause)."""
    raw = request.args.get("visual")
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise ValueError("visual must be valid JSON") from exc
    return validate_visual_arg(parsed)


def dashboard_scope_args():
    """Read the Dashboard's folder/collection/date scope query params."""
    return {
        "folder_id": request.args.get("folder_id", None, type=int),
        "collection_id": request.args.get("collection_id", None, type=int),
        "date_from": request.args.get("date_from", None),
        "date_to": request.args.get("date_to", None),
    }


def focus_candidate_ids(focus_photo_id, focus_photo_ids):
    """The photos a focused request may be placed by, in caller order.

    ``focus_photo_id`` stays first when both are given: it is the card
    the caller actually wants, and the list is its fallback.
    """
    candidates = []
    for pid in ([focus_photo_id] if focus_photo_id is not None else []) + list(
        focus_photo_ids or []
    ):
        if pid not in candidates:
            candidates.append(pid)
    return candidates


def coerce_collection_id(raw):
    """Parse an optional collection_id from a request body.

    Returns ``None`` if absent/blank, an ``int`` if valid, or the
    sentinel ``False`` if present but unparseable (so callers can
    distinguish "not provided" from "invalid"). ``bool`` is rejected
    because it's an ``int`` subclass.
    """
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return False
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        try:
            return int(raw)
        except ValueError:
            return False
    return False


def reject_visual_collection(db, collection_id, *, json_error):
    """Return a 400 response if the collection is visual, else None.

    The consumers that call ``db.get_collection_photos`` /
    ``collection_photo_ids`` (pipeline stages, sharpness/cull/classify/
    regroup/masks jobs, predictions-compare, the Misses legacy API, import
    preview) evaluate ``rules`` only. A visual-only collection would
    otherwise silently scope those runs to every metadata-matching
    photo instead of the visually-matched subset — the fix Codex
    flagged in review r3620423210. Pickers hide these collections too,
    but this is the source-of-truth boundary check.

    Coerce the raw JSON value before the ``visual_json`` lookup:
    SQLite's ``WHERE id = ?`` still matches integer id 1 when handed
    ``"1"`` or ``True``, so an early ``isinstance`` bail-out would let
    a string- or bool-typed id skip the guard and hit the rules-only
    path anyway (Codex review r3620636582). Return a 400 on an
    unparseable id so callers can't silently widen the scope.
    """
    coerced = coerce_collection_id(collection_id)
    if coerced is None:
        return None
    if coerced is False:
        return json_error("collection_id must be an integer", 400)
    row = collection_row(db, coerced)
    if row is not None and row["visual_json"] is not None:
        return json_error(VISUAL_COLLECTION_MSG, 400)
    return None


def parse_selection_photo_ids(db, body, *, json_error, limit=MAX_SELECTION_PHOTOS):
    """Validate a selection payload's ``photo_ids``.

    Returns ``(photo_ids, None)`` or ``(None, error_response)``. Shared by
    the keyword and prediction selection panels so both enforce the same
    cap and workspace scoping — a selection that is safe to read keywords
    for is exactly the selection that is safe to read predictions for.
    """
    raw_ids = body.get("photo_ids", [])
    if not isinstance(raw_ids, list) or not raw_ids:
        return None, json_error("photo_ids required")

    photo_ids = []
    seen = set()
    for raw in raw_ids:
        if isinstance(raw, bool) or not isinstance(raw, int):
            return None, json_error("photo_ids must be integers")
        if raw not in seen:
            photo_ids.append(raw)
            seen.add(raw)
    if not photo_ids:
        return None, json_error("photo_ids required")
    if limit is not None and len(photo_ids) > limit:
        return None, json_error("too many photo_ids", 400)

    visible_ids = set(db.filter_photo_ids_in_workspace(photo_ids))
    for pid in photo_ids:
        if pid not in visible_ids:
            return None, json_error(
                f"Photo {pid} does not belong to the active workspace", 403
            )
    return photo_ids, None
