"""Bulk location assignment from EXIF GPS, and the reverse-geocode cache codec.

``/api/batch/location/from-exif`` (preview and apply), the location-review
EXIF suggestions and the post-import GPS location step all turn photos'
EXIF coordinates into Google places the same way: skip photos that already
carry a location keyword, reverse-geocode each remaining photo through the
DB's ~110m grid cache (one lookup per grid cell per request), and group the
photos by resolved ``place_id``.

The cache rows are tagged with the Google result language they were fetched
under (``encode_cached_reverse_geocode`` / ``decode_cached_reverse_geocode``)
so a change to the "prefer English" setting refreshes stale entries; the
``/api/places/reverse-geocode`` proxy uses the same codec.

The flows that answer with an error response need the app's ``json_error``
and its ``LocationErrors``, so they live on :class:`BulkGpsLocations`, which
``create_app`` builds once. The rest are plain functions of the request
``Database``.
"""

from __future__ import annotations

import json
import logging
import math

import places
from db import Database
from services.visual_scope import VISUAL_COLLECTION_MSG, coerce_collection_id

log = logging.getLogger(__name__)

REVERSE_GEOCODE_CACHE_LANGUAGE_KEY = "_vireo_result_language"

_REVERSE_GEOCODE_CACHE_LEGACY = object()


def gps_location_chunks(values, size=800):
    values = list(values)
    for idx in range(0, len(values), size):
        yield values[idx:idx + size]


def location_keyword_photo_ids(db, photo_ids):
    """Return ids that already have any linked location keyword."""
    if not photo_ids:
        return set()
    found = set()
    for chunk in gps_location_chunks(photo_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = db.conn.execute(
            "SELECT DISTINCT pk.photo_id "
            "FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            f"WHERE k.type = 'location' AND pk.photo_id IN ({placeholders})",
            chunk,
        ).fetchall()
        found.update(row["photo_id"] for row in rows)
    return found


def decode_cached_reverse_geocode(cached, language):
    """Return ``(matches_language, details)`` for a cached response.

    Rows written before the language field existed have no
    ``_vireo_result_language`` key at all; treat those as compatible
    with the current preference so a rollout does not silently
    invalidate every previously cached lookup — and, when no API
    key is configured, force the caller into the ``no_api_key``
    branch instead of reusing the cached result. Post-PR writes
    always include the key (``null`` for the opt-out preference,
    ``"en"`` for the default), so subsequent preference changes
    still invalidate correctly.
    """
    try:
        details = json.loads(cached["response"] or "{}")
    except (ValueError, TypeError):
        details = {}
    if not isinstance(details, dict):
        details = {}
    cached_language = details.pop(
        REVERSE_GEOCODE_CACHE_LANGUAGE_KEY,
        _REVERSE_GEOCODE_CACHE_LEGACY,
    )
    if cached_language is _REVERSE_GEOCODE_CACHE_LEGACY:
        return True, details
    return cached_language == language, details


def encode_cached_reverse_geocode(details, language):
    """Serialize details with the requested language for cache matching.

    The language key is always written — including as ``null`` for
    the opt-out preference — so the decoder can distinguish a
    legacy untagged row from a row explicitly written under
    ``language=None``.
    """
    payload = dict(details) if isinstance(details, dict) else {}
    payload[REVERSE_GEOCODE_CACHE_LANGUAGE_KEY] = language
    return json.dumps(payload)


def resolve_exif_place_for_photo(
    db, photo, api_key, language, grid_cache,
):
    """Resolve one photo's EXIF coordinates into normalized place details.

    Returns ``(details, reason)`` where ``details`` is the normalized
    Google-place dict on success and ``reason`` is a short unresolved code
    on failure. Results are de-duped by the DB's reverse-geocode grid so a
    burst in the same cell only performs/cache-checks one lookup.
    """
    lat = photo["latitude"]
    lng = photo["longitude"]
    if lat is None or lng is None:
        return None, "missing_gps"
    try:
        lat = float(lat)
        lng = float(lng)
    except (TypeError, ValueError):
        return None, "invalid_gps"
    if not (math.isfinite(lat) and math.isfinite(lng)):
        return None, "invalid_gps"

    grid = Database._reverse_geocode_grid(lat, lng)
    if grid in grid_cache:
        return grid_cache[grid]

    cached = db.reverse_geocode_cache_get(lat, lng)
    if cached is not None:
        language_matches, details = decode_cached_reverse_geocode(
            cached, language,
        )
        if language_matches:
            if cached["place_id"] is None:
                result = (None, "no_match")
                grid_cache[grid] = result
                return result
            if not details.get("place_id"):
                details["place_id"] = cached["place_id"]
            if details.get("place_id"):
                result = (details, None)
            else:
                result = (None, "no_match")
            grid_cache[grid] = result
            return result

    if not api_key:
        result = (None, "no_api_key")
        grid_cache[grid] = result
        return result

    try:
        details = places.reverse_geocode_for_language(
            lat, lng, api_key, language,
        )
    except places.PlacesTransientError:
        log.warning(
            "bulk reverse_geocode transient failure for photo=%s lat=%s lng=%s",
            photo["id"],
            lat,
            lng,
        )
        result = (None, "transient_error")
        grid_cache[grid] = result
        return result

    cache_place_id = details.get("place_id") if details else None
    db.reverse_geocode_cache_put(
        lat,
        lng,
        place_id=cache_place_id,
        response_json=encode_cached_reverse_geocode(details, language),
    )
    if not details or not details.get("place_id"):
        result = (None, "no_match")
    else:
        result = (details, None)
    grid_cache[grid] = result
    return result


def summarize_details(details):
    """Build a short human-friendly summary string from a Place Details dict.

    Format: ``"<leaf name> · <broadest 1-2 parents>"``. Google's
    ``address_components`` are ordered narrowest-first, so the broadest
    parents (country, state) sit at the END of the list. We pick at most
    the last two, dedupe against the leaf name, and join with " · ".

    Examples::

        "Central Park · New York · United States"
        "Some Lighthouse · Iceland"
        "JustALeaf"  # if no usable parent components
    """
    leaf = (details or {}).get("name", "") or ""
    components = (details or {}).get("address_components") or []

    # Broadest 1-2 parents = last two components (Google orders broad-last).
    tail = components[-2:] if len(components) >= 2 else components[-1:]
    # Walk in reverse so we render broadest-first to broader-second
    # ("New York · United States" reads better than "United States · New York"
    # given the leaf comes first; iNaturalist uses leaf-then-narrowest-up).
    # Actually: leaf · narrowest-parent · ... · broadest-parent reads most
    # naturally for breadcrumbs. So reverse the tail so the closest parent
    # is first.
    parts = [leaf] if leaf else []
    for comp in reversed(tail):
        name = (comp or {}).get("name") or (comp or {}).get("long_name") or ""
        if not name:
            continue
        if name == leaf or name in parts:
            continue
        parts.append(name)

    if not parts:
        return ""
    return " · ".join(parts)


class BulkGpsLocations:
    """Bulk EXIF-GPS location flows that answer with error responses.

    ``json_error`` is the app's JSON error-response builder and
    ``location_errors`` its ``web.location_edits.LocationErrors``; methods
    return ``(value, error_response)`` pairs with exactly one set.
    """

    def __init__(self, *, json_error, location_errors):
        self._json_error = json_error
        self._location_errors = location_errors

    def normalize_photo_id_list(self, raw_ids):
        """Validate and de-dupe a JSON ``photo_ids`` list, preserving order."""
        json_error = self._json_error
        if not isinstance(raw_ids, list) or not raw_ids:
            return None, json_error("photo_ids required", 400)
        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return None, json_error("photo_ids must contain only integers", 400)
            if raw not in seen:
                photo_ids.append(raw)
                seen.add(raw)
        if not photo_ids:
            return None, json_error("photo_ids required", 400)
        return photo_ids, None

    def source_ids(self, db, body):
        """Return source photo ids from either ``photo_ids`` or ``collection_id``."""
        json_error = self._json_error
        raw_ids = body.get("photo_ids")
        if raw_ids:
            return self.normalize_photo_id_list(raw_ids)

        collection_id = coerce_collection_id(body.get("collection_id"))
        if collection_id is False:
            return None, json_error("collection_id must be an integer", 400)
        if collection_id is None:
            return None, json_error("photo_ids or collection_id required", 400)

        row = db.conn.execute(
            "SELECT id, visual_json FROM collections "
            "WHERE id = ? AND workspace_id = ?",
            (collection_id, db._ws_id()),
        ).fetchone()
        if row is None:
            return None, json_error("collection not found", 404)
        # get_collection_photo_ids evaluates ``rules`` only; a visual-only
        # collection would silently expand to every metadata match. The
        # picker filters these out, but reject here as the boundary.
        if row["visual_json"] is not None:
            return None, json_error(VISUAL_COLLECTION_MSG, 400)
        return db.get_collection_photo_ids(collection_id), None

    def payload(self, db, body, cancel_check=None):
        """Build preview/apply data for resolving locations from EXIF GPS."""
        photo_ids, error = self.source_ids(db, body)
        if error is not None:
            return None, error
        if not photo_ids:
            return {
                "total": 0,
                "resolvable": 0,
                "updated": 0,
                "groups": [],
                "unresolved": [],
                "skipped": [],
            }, None
        photos_map = db.get_photos_by_ids(photo_ids)
        if len(photos_map) != len(photo_ids):
            return None, self._json_error("One or more photos were not found", 404)
        for pid in photo_ids:
            edit_error = self._location_errors.photo_location_edit_error(db, pid)
            if edit_error is not None:
                return None, edit_error

        assigned_ids = location_keyword_photo_ids(db, photo_ids)
        import config as cfg
        maps_config = cfg.load()
        api_key = (maps_config.get("google_maps_api_key", "") or "").strip()
        language = places.result_language(maps_config)

        grid_cache = {}
        groups = {}
        unresolved = []
        skipped = []
        ordered_group_keys = []
        cancelled = False
        for pid in photo_ids:
            if cancel_check is not None and cancel_check():
                cancelled = True
                break
            photo = photos_map[pid]
            if pid in assigned_ids:
                skipped.append({
                    "photo_id": pid,
                    "filename": photo["filename"],
                    "reason": "already_has_location",
                })
                continue
            details, reason = resolve_exif_place_for_photo(
                db, photo, api_key, language, grid_cache,
            )
            if reason is not None:
                unresolved.append({
                    "photo_id": pid,
                    "filename": photo["filename"],
                    "reason": reason,
                })
                continue

            place_id = details.get("place_id")
            if place_id not in groups:
                groups[place_id] = {
                    "place_id": place_id,
                    "summary": summarize_details(details),
                    "name": details.get("name") or "",
                    "details": details,
                    "photo_ids": [],
                    "sample_filenames": [],
                }
                ordered_group_keys.append(place_id)
            group = groups[place_id]
            group["photo_ids"].append(pid)
            if len(group["sample_filenames"]) < 3:
                group["sample_filenames"].append(photo["filename"])

        group_list = []
        for place_id in ordered_group_keys:
            group = groups[place_id]
            group_list.append({
                "place_id": group["place_id"],
                "summary": group["summary"],
                "name": group["name"],
                "count": len(group["photo_ids"]),
                "photo_ids": group["photo_ids"],
                "sample_filenames": group["sample_filenames"],
            })

        result = {
            "total": len(photo_ids),
            "resolvable": sum(group["count"] for group in group_list),
            "updated": 0,
            "groups": group_list,
            "unresolved": unresolved,
            "skipped": skipped,
            "_details_by_place_id": {k: v["details"] for k, v in groups.items()},
        }
        if cancelled:
            result["cancelled"] = True
        return result, None
