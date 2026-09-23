"""Request parsing, payloads, and error responses for location edits.

Shared by the routes that assign, clear, and link locations: the photo
location endpoints, the batch location/keyword edits, location review,
and place linking. The parsing and serialization helpers are plain
functions of the request body or the request ``Database``. The error
responses need the app's ``json_error``, so they live on
:class:`LocationErrors`, which ``create_app`` builds once and blueprints
receive as a single dependency.
"""

import math
import re

from flask import g, jsonify

# Shared message for a location name that carries the XMP hierarchy
# delimiter. Rejecting at assignment time (rather than at sync time) is
# what keeps the pending change from being silently cleared for a name
# ``SidecarEditor.set_location_keywords`` cannot round-trip.
LOCATION_NAME_PIPE_ERROR = (
    "location name may not contain '|' -- XMP keyword hierarchies "
    "reserve it as the level delimiter"
)


def location_name_conflict_payload(err):
    """Describe a location-keyword collision for both people and clients.

    ``error`` remains the stable legacy code because existing clients use
    it for branching. ``message`` is the text browser surfaces should show
    to a person; ``error_detail`` retains the lower-level diagnostic for
    logs and troubleshooting.
    """
    detail = str(err)
    match = re.search(r"(?:child )?keyword '(.+?)' \((?:parent_id|id)=", detail)
    if match:
        message = (
            f"Couldn’t assign this location because “{match.group(1)}” is "
            "already used by another keyword. Rename that keyword in "
            "Keywords, then try again."
        )
    else:
        message = (
            "Couldn’t assign this location because one of its place names "
            "conflicts with an existing keyword. Rename the conflicting "
            "keyword in Keywords, then try again."
        )
    return {
        "error": "name_conflict",
        "code": "name_conflict",
        "message": message,
        "error_detail": detail,
    }


def location_name_conflict_response(err):
    payload = location_name_conflict_payload(err)
    payload["request_id"] = getattr(g, "request_id", None)
    return jsonify(payload), 409


def coerce_place_id(candidate):
    """Return a stripped string ``place_id`` for any JSON scalar value."""
    if candidate is None:
        return ""
    if isinstance(candidate, str):
        return candidate.strip()
    return str(candidate).strip()


def extract_place_id(body):
    """Extract ``place_id`` from top-level or nested client place payloads."""
    candidate = body.get("place_id")
    if candidate is None and isinstance(body.get("place"), dict):
        candidate = body["place"].get("place_id")
    if candidate is None and isinstance(body.get("details"), dict):
        candidate = body["details"].get("place_id")
    return coerce_place_id(candidate)


def extract_keyword_id(body):
    """Return an integer keyword_id from a JSON body, or None."""
    candidate = body.get("keyword_id")
    if candidate is None:
        return None
    if isinstance(candidate, bool):
        return None
    if isinstance(candidate, int):
        return candidate
    if isinstance(candidate, str):
        stripped = candidate.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def normalize_client_place_details(body):
    """Normalize a Google Maps JS Place payload from the request body.

    Browser autocomplete already receives geometry and address components
    when the Maps JS key is valid. Accepting that payload avoids a second
    server-side Place Details request, which can fail for correctly
    referrer-restricted browser keys.
    """
    raw = body.get("place") or body.get("details")
    if not isinstance(raw, dict):
        return None

    place_id = coerce_place_id(raw.get("place_id"))
    if not place_id:
        return None
    body_place_id = coerce_place_id(body.get("place_id"))
    if body_place_id and body_place_id != place_id:
        return None

    def first_present(*values):
        for value in values:
            if value is not None:
                return value
        return None

    geometry = raw.get("geometry")
    geometry_location = {}
    if isinstance(geometry, dict):
        location = geometry.get("location")
        if isinstance(location, dict):
            geometry_location = location
    lat_value = first_present(
        raw.get("lat"),
        raw.get("latitude"),
        geometry_location.get("lat") if isinstance(geometry_location, dict) else None,
    )
    lng_value = first_present(
        raw.get("lng"),
        raw.get("longitude"),
        geometry_location.get("lng") if isinstance(geometry_location, dict) else None,
    )
    try:
        lat = float(lat_value)
        lng = float(lng_value)
    except (TypeError, ValueError):
        return None
    if (
        not math.isfinite(lat)
        or not math.isfinite(lng)
        or lat < -90
        or lat > 90
        or lng < -180
        or lng > 180
    ):
        return None

    raw_components = raw.get("address_components")
    if not isinstance(raw_components, list):
        raw_components = []
    components = []
    for comp in raw_components:
        if not isinstance(comp, dict):
            continue
        name = comp.get("name") or comp.get("long_name") or ""
        if not name:
            continue
        types = comp.get("types")
        if not isinstance(types, list):
            types = []
        components.append({
            "name": name,
            "short_name": comp.get("short_name") or "",
            "types": types,
        })

    raw_types = raw.get("types")
    if not isinstance(raw_types, list):
        raw_types = []

    return {
        "place_id": place_id,
        "name": raw.get("name") or raw.get("formatted_address") or "",
        "types": raw_types,
        "lat": lat,
        "lng": lng,
        "address_components": components,
    }


def walk_parent_chain(db, leaf_parent_id):
    """Walk ``parent_id`` upward from ``leaf_parent_id`` to the root.

    Returns a list of ``{"id": int, "name": str}`` dicts in broadest →
    narrowest order, EXCLUDING the leaf itself. Pass the leaf's
    ``parent_id`` (i.e. the *first* parent), not the leaf's own id.

    Depth cap of 10 — chains are bounded ~5 in practice, but guard
    against pathological/malformed cycles (link_keyword_to_place
    already prevents creating cycles, but a corrupted DB could).
    """
    parents = []
    current_parent_id = leaf_parent_id
    for _ in range(10):
        if current_parent_id is None:
            break
        row = db.conn.execute(
            "SELECT id, name, parent_id FROM keywords WHERE id = ?",
            (current_parent_id,),
        ).fetchone()
        if row is None:
            break
        parents.append({"id": row["id"], "name": row["name"]})
        current_parent_id = row["parent_id"]
    # Reverse so broadest (e.g. country) comes first, narrowest last.
    parents.reverse()
    return parents


def serialize_photo_location(db, photo_id):
    """Return a summary dict for the photo's current location keyword.

    The shape matches the JSON the location section UI expects::

        {
            "keyword_id":   int,
            "name":         str,
            "place_id":     str | None,
            "latitude":     float | None,
            "longitude":    float | None,
            "parent_chain": [{"id": int, "name": str}, ...],  # broadest -> narrowest, EXCLUDES leaf
        }

    Returns ``None`` if the photo has no ``type='location'`` keyword link.
    """
    leaf = db.conn.execute(
        "SELECT k.id, k.name, k.place_id, k.latitude, k.longitude, k.parent_id "
        "FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location' "
        "LIMIT 1",
        (photo_id,),
    ).fetchone()
    if leaf is None:
        return None

    return {
        "keyword_id": leaf["id"],
        "name": leaf["name"],
        "place_id": leaf["place_id"],
        "latitude": leaf["latitude"],
        "longitude": leaf["longitude"],
        "parent_chain": walk_parent_chain(db, leaf["parent_id"]),
    }


class LocationErrors:
    """Location error responses bound to the app's ``json_error``.

    ``photo_not_found_error`` is the app's shared photo-not-found
    response, reused so a missing photo reads the same on every route.
    """

    def __init__(self, *, json_error, photo_not_found_error):
        self._json_error = json_error
        self._photo_not_found_error = photo_not_found_error

    def keyword_not_found_error(self):
        return self._json_error(
            "keyword_not_found",
            404,
            message=(
                "That saved location no longer exists. Refresh the page and "
                "select another location."
            ),
        )

    def google_maps_not_configured_error(self):
        return self._json_error(
            "no_api_key",
            400,
            message=(
                "Google Maps isn’t configured. Add an API key in Settings to "
                "use Google place search."
            ),
        )

    def google_place_not_found_error(self):
        return self._json_error(
            "place_not_found",
            404,
            message=(
                "Google Maps couldn’t find that place. Search again and choose "
                "another result."
            ),
        )

    def photo_location_edit_error(self, db, photo_id):
        """Return an error response when a photo cannot be edited in this workspace."""
        if db.conn.execute(
            "SELECT 1 FROM photos WHERE id = ?", (photo_id,)
        ).fetchone() is None:
            return self._photo_not_found_error()
        if not db._photo_in_workspace(photo_id):
            return self._json_error(
                f"Photo {photo_id} does not belong to the active workspace", 403,
            )
        return None

    def location_keyword_edit_error(self, db, keyword_id):
        """Return an error response unless ``keyword_id`` is a location keyword."""
        if keyword_id is None:
            return self._json_error("invalid keyword_id", 400)
        row = db.conn.execute(
            "SELECT id, type FROM keywords WHERE id = ?", (keyword_id,),
        ).fetchone()
        if row is None:
            return self.keyword_not_found_error()
        if row["type"] != "location":
            return self._json_error("keyword is not a location", 400)
        return None
