"""Thin HTTP wrappers around Google's Places + Geocoding APIs.

This module is intentionally minimal:

* No retries — callers can decide policy.
* No caching — that lives in the DB layer (see ``Database`` reverse-geocode
  cache).
* No new dependencies — only ``urllib`` + stdlib.

Both lookups normalize Google's response into a single shape::

    {
        "place_id":            str,
        "name":                str,
        "types":               list[str],
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
import math
import re
import ssl
import urllib.parse
import urllib.request

import certifi

logger = logging.getLogger(__name__)

_PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
_NEARBY_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

def _create_ssl_context():
    """Return a trust context containing configured and bundled CA roots."""
    context = ssl.create_default_context()
    # PyInstaller's embedded Python does not reliably discover a system CA
    # bundle on macOS. Add Vireo's bundled roots without discarding system,
    # SSL_CERT_FILE, or SSL_CERT_DIR roots needed by managed environments.
    context.load_verify_locations(cafile=certifi.where())
    return context


_SSL_CONTEXT = _create_ssl_context()

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
    with urllib.request.urlopen(url, timeout=10, context=_SSL_CONTEXT) as f:
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


def place_details(
    place_id: str,
    api_key: str,
    *,
    language: str | None = "en",
) -> dict | None:
    """Look up a Google place by its ``place_id``.

    Returns the normalized dict described in the module docstring, or ``None``
    if Google returns no result / an error status.
    """
    params = {
        "place_id": place_id,
        "key": api_key,
        "fields": "place_id,name,type,geometry/location,address_components",
    }
    if language:
        params["language"] = language
    url = f"{_PLACE_DETAILS_URL}?{urllib.parse.urlencode(params)}"

    try:
        payload = _get_json(url)
    except Exception:  # noqa: BLE001  # broad: Places HTTP/JSON errors degrade to no details
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
        "name": _location_safe_name(result.get("name", "")),
        "types": list(result.get("types", []) or []),
        "lat": lat,
        "lng": lng,
        "address_components": _normalize_components(result.get("address_components")),
    }


def _location_safe_name(name: str) -> str:
    """Google's name for a place, usable as a location keyword.

    Lightroom reserves ``|`` as its keyword hierarchy delimiter, so a name
    such as "Ocean Beach | Dog Beach" becomes "Ocean Beach - Dog Beach".
    """
    return re.sub(r"\s*\|\s*", " - ", name or "").strip()


def result_language(config) -> str | None:
    """Return Google's language preference for a Vireo configuration."""
    return "en" if config.get("google_maps_prefer_english", True) else None


def place_details_for_language(place_id: str, api_key: str, language: str | None) -> dict | None:
    """Fetch details for ``language`` from :func:`result_language`.

    ``"en"`` calls :func:`place_details` without the keyword so its
    default-on English preference applies; anything else opts out.
    """
    if language == "en":
        return place_details(place_id, api_key)
    return place_details(place_id, api_key, language=None)


def reverse_geocode_for_language(lat: float, lng: float, api_key: str, language: str | None) -> dict | None:
    """Reverse-geocode for ``language`` from :func:`result_language`.

    ``"en"`` calls :func:`reverse_geocode` without the keyword so its
    default-on English preference applies; anything else opts out.
    """
    if language == "en":
        return reverse_geocode(lat, lng, api_key)
    return reverse_geocode(lat, lng, api_key, language=None)


class PlacesTransientError(Exception):
    """Raised by :func:`reverse_geocode` on transient Google API failures
    (rate limits, request denied, malformed response, network/JSON errors).

    Distinguishes "Google said no match" (return ``None``) from "we couldn't
    ask Google right now" (raise this) so callers that cache negative
    results don't poison the cache with transient failures.
    """


def reverse_geocode(
    lat: float,
    lng: float,
    api_key: str,
    *,
    language: str | None = "en",
) -> dict | None:
    """Name the place at a (lat, lng) pair, never as a street address.

    Geocoding's first result on land is almost always the nearest street
    address ("2525 Bacon St, San Diego, CA 92107, USA"), which is useless as a
    photo location. So the answer is, in order:

    1. the nearest park within :data:`PARK_RADIUS_M` (Places Nearby Search,
       then Place Details for its address components), or
    2. the most specific named area Geocoding knows: a natural feature,
       park, neighborhood, then city and broader (:data:`AREA_TYPE_PRIORITY`).

    The normalized dict gains ``"source"``: ``"park"`` or ``"area"``.

    - Returns ``None`` only when Google reports a true no-match
      (``ZERO_RESULTS`` / ``NOT_FOUND`` / no named area at all).
    - Raises :class:`PlacesTransientError` on transient failures
      (``OVER_QUERY_LIMIT``, ``REQUEST_DENIED``, network/JSON errors).
      Callers that cache results MUST NOT cache when this is raised.
    """
    park = _nearest_park(lat, lng, api_key, language)
    if park is not None:
        details = place_details(park["place_id"], api_key, language=language)
        if details is None:
            # Nearby Search just returned this place, so a missing record
            # is a failed request, not a no-match; don't let a cache store
            # the area fallback in its place.
            raise PlacesTransientError("Place Details failed for nearby park")
        return {**details, "source": "park"}
    return _named_area(lat, lng, api_key, language)


# How close a park must be to name the spot. Large enough to cover a photo
# taken from a park's edge or trail, small enough not to rename a whole
# neighborhood after the nearest playground.
PARK_RADIUS_M = 400

# Anyone can add a "park" to Google Maps, so the type alone admits benches,
# volleyball courts and "welcome to zatchu punch". Parks people actually
# visit have reviews: Robb Field has over a thousand.
PARK_MIN_RATINGS = 100

# Geocoding result types that name a place, most specific first. Street
# addresses, premises, routes, intersections, plus codes, postal codes and
# bare establishments are deliberately absent: an establishment's result
# carries only its street address, not its name.
AREA_TYPE_PRIORITY = (
    "natural_feature",
    "park",
    "colloquial_area",
    "neighborhood",
    "sublocality_level_1",
    "sublocality",
    "locality",
    "postal_town",
    "administrative_area_level_3",
    "administrative_area_level_2",
    "administrative_area_level_1",
    "country",
)


def _google_results(url: str, where: str) -> list[dict] | None:
    """Fetch a Google search/geocode URL; ``None`` means a true no-match."""
    try:
        payload = _get_json(url)
    except Exception as e:  # noqa: BLE001  # broad: Places HTTP/JSON errors re-raise as transient
        logger.warning("%s: HTTP/JSON failure", where, exc_info=True)
        raise PlacesTransientError("HTTP/JSON failure") from e
    status = payload.get("status", "")
    if status in _EMPTY_STATUSES:
        return None
    if status != "OK":
        # _ERROR_STATUSES (OVER_QUERY_LIMIT, REQUEST_DENIED, INVALID_REQUEST,
        # UNKNOWN_ERROR) and any other unexpected status are transient.
        logger.warning("%s: Google API returned status=%s", where, status)
        raise PlacesTransientError(f"Google API status={status}")
    return payload.get("results") or None


def _result_point(result: dict) -> tuple[float, float] | None:
    location = (result.get("geometry") or {}).get("location") or {}
    try:
        return float(location["lat"]), float(location["lng"])
    except (KeyError, TypeError, ValueError):
        return None


def _distance_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance in meters."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371000 * math.asin(math.sqrt(min(1.0, a)))


def _nearest_park(lat: float, lng: float, api_key: str, language: str | None) -> dict | None:
    """The nearest well-known park within :data:`PARK_RADIUS_M`, as a raw result."""
    params = {
        "location": f"{lat},{lng}",
        "rankby": "distance",
        "type": "park",
        "key": api_key,
    }
    if language:
        params["language"] = language
    url = f"{_NEARBY_SEARCH_URL}?{urllib.parse.urlencode(params)}"
    # rankby=distance orders the results, so the first one past the radius
    # ends the search.
    for result in _google_results(url, "nearby park search") or []:
        point = _result_point(result)
        if point is None or not result.get("place_id"):
            continue
        if _distance_m(lat, lng, *point) > PARK_RADIUS_M:
            return None
        if (result.get("user_ratings_total") or 0) >= PARK_MIN_RATINGS:
            return result
    return None


def _area_rank(result: dict) -> int | None:
    ranks = [AREA_TYPE_PRIORITY.index(t) for t in result.get("types") or []
             if t in AREA_TYPE_PRIORITY]
    return min(ranks) if ranks else None


def _named_area(lat: float, lng: float, api_key: str, language: str | None) -> dict | None:
    """The most specific named area Geocoding returns for the point."""
    params = {
        "latlng": f"{lat},{lng}",
        "key": api_key,
    }
    if language:
        params["language"] = language
    url = f"{_GEOCODE_URL}?{urllib.parse.urlencode(params)}"
    ranked = []
    for result in _google_results(url, "reverse_geocode") or []:
        rank = _area_rank(result)
        point = _result_point(result)
        components = result.get("address_components") or []
        if rank is None or point is None or not components:
            continue
        ranked.append((rank, point, result))
    if not ranked:
        return None
    _, (out_lat, out_lng), result = min(ranked, key=lambda item: item[0])
    components = result.get("address_components") or []
    # An area result's first component is the area itself ("Ocean Beach");
    # formatted_address would be the whole breadcrumb.
    return {
        "place_id": result.get("place_id", ""),
        "name": _location_safe_name(components[0].get("long_name", "")),
        "types": list(result.get("types", []) or []),
        "lat": out_lat,
        "lng": out_lng,
        "address_components": _normalize_components(components),
        "source": "area",
    }
