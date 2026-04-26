"""Thin HTTP wrappers around Google's Places + Geocoding APIs.

This module is intentionally minimal:

* No retries — callers can decide policy.
* No caching — that lives in the DB layer (see ``Database`` reverse-geocode
  cache).
* No new dependencies — only ``urllib`` + stdlib.

Both public functions normalize Google's response into a single shape::

    {
        "place_id":            str,
        "name":                str,
        "lat":                 float,
        "lng":                 float,
        "address_components":  [
            {
                "name":        str,   # Google's ``long_name``
                "short_name":  str,
                "types":       list[str],
                # ``place_id`` may be present if Google returned one for the
                # component — the standard Place Details / Geocoding responses
                # generally do *not* include per-component place_ids, so
                # absence is the common case.
            },
            ...
        ],
    }

Returns ``None`` whenever Google reports ``ZERO_RESULTS`` / ``NOT_FOUND`` / no
results, or any error status (logged at WARNING).
"""

from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

_PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# Google statuses that mean "no match" — we return ``None`` silently.
_EMPTY_STATUSES = {"ZERO_RESULTS", "NOT_FOUND"}

# Google statuses that mean "something went wrong" — we log a warning and
# still return ``None`` (callers should not raise on transient API failures).
_ERROR_STATUSES = {
    "OVER_QUERY_LIMIT",
    "REQUEST_DENIED",
    "INVALID_REQUEST",
    "UNKNOWN_ERROR",
}


def _get_json(url: str) -> dict:
    """Fetch ``url`` and parse the response as JSON.

    Kept tiny so tests can monkeypatch ``urllib.request.urlopen`` directly.
    """
    with urllib.request.urlopen(url, timeout=10) as f:
        body = f.read()
    return json.loads(body)


def _normalize_components(components: list[dict] | None) -> list[dict]:
    """Convert Google's ``address_components`` into our shape."""
    out: list[dict] = []
    for c in components or []:
        comp = {
            "name": c.get("long_name", ""),
            "short_name": c.get("short_name", ""),
            "types": list(c.get("types", []) or []),
        }
        # The standard Place Details / Geocoding endpoints don't return a
        # per-component place_id, but pass it through if Google ever does.
        if "place_id" in c:
            comp["place_id"] = c["place_id"]
        out.append(comp)
    return out


def _check_status(status: str, where: str) -> bool:
    """Return True if ``status == 'OK'``. Otherwise log + return False."""
    if status == "OK":
        return True
    if status in _EMPTY_STATUSES:
        return False
    if status in _ERROR_STATUSES:
        logger.warning("%s: Google API returned status=%s", where, status)
        return False
    # Unknown status — treat as error.
    logger.warning("%s: Google API returned unexpected status=%s", where, status)
    return False


def place_details(place_id: str, api_key: str) -> dict | None:
    """Look up a Google place by its ``place_id``.

    Returns the normalized dict described in the module docstring, or ``None``
    if Google returns no result / an error status.
    """
    params = {
        "place_id": place_id,
        "key": api_key,
        "fields": "place_id,name,geometry/location,address_components",
    }
    url = f"{_PLACE_DETAILS_URL}?{urllib.parse.urlencode(params)}"

    try:
        payload = _get_json(url)
    except Exception:  # noqa: BLE001 — log + degrade
        logger.warning("place_details: HTTP/JSON failure for place_id=%s", place_id, exc_info=True)
        return None

    status = payload.get("status", "")
    if not _check_status(status, "place_details"):
        return None

    result = payload.get("result") or {}
    if not result:
        return None

    location = (result.get("geometry") or {}).get("location") or {}
    try:
        lat = float(location["lat"])
        lng = float(location["lng"])
    except (KeyError, TypeError, ValueError):
        logger.warning(
            "place_details: missing/invalid geometry.location for place_id=%s",
            place_id,
        )
        return None

    return {
        "place_id": result.get("place_id", place_id),
        "name": result.get("name", ""),
        "lat": lat,
        "lng": lng,
        "address_components": _normalize_components(result.get("address_components")),
    }


def reverse_geocode(lat: float, lng: float, api_key: str) -> dict | None:
    """Reverse-geocode a (lat, lng) pair via the Geocoding API.

    Returns the normalized dict for the first result, or ``None`` if Google
    has no match / returned an error status.
    """
    params = {
        "latlng": f"{lat},{lng}",
        "key": api_key,
    }
    url = f"{_GEOCODE_URL}?{urllib.parse.urlencode(params)}"

    try:
        payload = _get_json(url)
    except Exception:  # noqa: BLE001 — log + degrade
        logger.warning(
            "reverse_geocode: HTTP/JSON failure for latlng=%s,%s", lat, lng, exc_info=True
        )
        return None

    status = payload.get("status", "")
    if not _check_status(status, "reverse_geocode"):
        return None

    results = payload.get("results") or []
    if not results:
        return None

    result = results[0]

    location = (result.get("geometry") or {}).get("location") or {}
    try:
        out_lat = float(location["lat"])
        out_lng = float(location["lng"])
    except (KeyError, TypeError, ValueError):
        logger.warning(
            "reverse_geocode: missing/invalid geometry.location for latlng=%s,%s",
            lat,
            lng,
        )
        return None

    # ``name`` for reverse-geocode results: prefer formatted_address, fall back
    # to the broadest address component's long_name.
    name = result.get("formatted_address", "")
    components = result.get("address_components") or []
    if not name and components:
        name = components[0].get("long_name", "")

    return {
        "place_id": result.get("place_id", ""),
        "name": name,
        "lat": out_lat,
        "lng": out_lng,
        "address_components": _normalize_components(components),
    }
