"""Tests for vireo.places — Google Maps HTTP wrappers.

The tests do NOT hit the network. They monkeypatch
``vireo.places.urllib.request.urlopen`` with a fake that returns canned JSON
responses. This means the implementation must call ``urlopen`` through
``urllib.request.urlopen`` (e.g. via ``with urlopen(url) as f:``) so that the
patch resolves to the fake.
"""

import io
import json
import urllib.error

import pytest


class _FakeResponse:
    """Minimal stand-in for the object returned by ``urllib.request.urlopen``.

    Supports the context-manager protocol (``with urlopen(url) as f:``) and
    a ``read()`` method that returns bytes — same surface our wrapper uses.
    """

    def __init__(self, payload: dict):
        self._buf = io.BytesIO(json.dumps(payload).encode("utf-8"))

    def read(self):
        return self._buf.read()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._buf.close()
        return False


def _make_fake_urlopen(payload: dict, captured_urls: list | None = None):
    def fake_urlopen(url, *args, **kwargs):
        if captured_urls is not None:
            captured_urls.append(url)
        return _FakeResponse(payload)

    return fake_urlopen


def test_place_details_parses_response(monkeypatch):
    """Place Details OK response is normalized into the wrapper's dict shape."""
    from vireo import places

    canned = {
        "status": "OK",
        "result": {
            "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
            "name": "Central Park",
            "geometry": {"location": {"lat": 40.7828647, "lng": -73.9653551}},
            "address_components": [
                {
                    "long_name": "Manhattan",
                    "short_name": "Manhattan",
                    "types": ["political", "sublocality", "sublocality_level_1"],
                },
                {
                    "long_name": "New York",
                    "short_name": "New York",
                    "types": ["locality", "political"],
                },
                {
                    "long_name": "New York",
                    "short_name": "NY",
                    "types": ["administrative_area_level_1", "political"],
                },
                {
                    "long_name": "United States",
                    "short_name": "US",
                    "types": ["country", "political"],
                },
            ],
        },
    }

    captured = []
    monkeypatch.setattr(
        "vireo.places.urllib.request.urlopen",
        _make_fake_urlopen(canned, captured),
    )

    out = places.place_details("ChIJ4zGFAZpYwokRGUGph3Mf37k", "FAKE_KEY")

    assert out is not None
    assert out["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert out["name"] == "Central Park"
    assert isinstance(out["lat"], float)
    assert isinstance(out["lng"], float)
    assert out["lat"] == pytest.approx(40.7828647)
    assert out["lng"] == pytest.approx(-73.9653551)
    assert isinstance(out["address_components"], list)
    assert len(out["address_components"]) == 4

    # Components should expose normalized keys.
    leaf_country = out["address_components"][-1]
    assert leaf_country["name"] == "United States"
    assert leaf_country["short_name"] == "US"
    assert "country" in leaf_country["types"]

    # The URL must include the place_id and the key (passed through urlencode).
    assert len(captured) == 1
    url = captured[0]
    assert "place_id=ChIJ4zGFAZpYwokRGUGph3Mf37k" in url
    assert "key=FAKE_KEY" in url


def test_place_details_returns_none_on_zero_results(monkeypatch):
    """ZERO_RESULTS / NOT_FOUND / empty result → ``None``, no raise."""
    from vireo import places

    monkeypatch.setattr(
        "vireo.places.urllib.request.urlopen",
        _make_fake_urlopen({"status": "ZERO_RESULTS"}),
    )

    assert places.place_details("ChIJ_does_not_exist", "FAKE_KEY") is None


def test_reverse_geocode_parses_response(monkeypatch):
    """Geocoding OK response is normalized into the wrapper's dict shape."""
    from vireo import places

    canned = {
        "status": "OK",
        "results": [
            {
                "place_id": "ChIJOwg_06VPwokRYv534QaPC8g",
                "formatted_address": "New York, NY, USA",
                "geometry": {"location": {"lat": 40.7127753, "lng": -74.0059728}},
                "address_components": [
                    {
                        "long_name": "New York",
                        "short_name": "New York",
                        "types": ["locality", "political"],
                    },
                    {
                        "long_name": "New York",
                        "short_name": "NY",
                        "types": ["administrative_area_level_1", "political"],
                    },
                    {
                        "long_name": "United States",
                        "short_name": "US",
                        "types": ["country", "political"],
                    },
                ],
            }
        ],
    }

    captured = []
    monkeypatch.setattr(
        "vireo.places.urllib.request.urlopen",
        _make_fake_urlopen(canned, captured),
    )

    out = places.reverse_geocode(40.7127753, -74.0059728, "FAKE_KEY")

    assert out is not None
    assert out["place_id"] == "ChIJOwg_06VPwokRYv534QaPC8g"
    assert out["name"] == "New York, NY, USA"
    assert isinstance(out["lat"], float)
    assert isinstance(out["lng"], float)
    assert out["lat"] == pytest.approx(40.7127753)
    assert out["lng"] == pytest.approx(-74.0059728)
    assert isinstance(out["address_components"], list)
    assert len(out["address_components"]) == 3

    # latlng=lat,lng should be in the URL — comma may be percent-encoded.
    assert len(captured) == 1
    url = captured[0]
    assert ("latlng=40.7127753%2C-74.0059728" in url) or (
        "latlng=40.7127753,-74.0059728" in url
    )
    assert "key=FAKE_KEY" in url


def test_reverse_geocode_returns_none_on_zero_results(monkeypatch):
    """ZERO_RESULTS is a true no-match — return ``None`` so callers may
    cache the negative result."""
    from vireo import places

    monkeypatch.setattr(
        "vireo.places.urllib.request.urlopen",
        _make_fake_urlopen({"status": "ZERO_RESULTS"}),
    )

    assert places.reverse_geocode(0.0, 0.0, "FAKE_KEY") is None


def test_reverse_geocode_raises_transient_on_over_query_limit(monkeypatch):
    """OVER_QUERY_LIMIT is a transient API failure, not a real no-match.
    Wrapper raises PlacesTransientError so callers can avoid caching."""
    from vireo import places

    monkeypatch.setattr(
        "vireo.places.urllib.request.urlopen",
        _make_fake_urlopen({"status": "OVER_QUERY_LIMIT"}),
    )

    with pytest.raises(places.PlacesTransientError):
        places.reverse_geocode(40.0, -73.0, "FAKE_KEY")


def test_reverse_geocode_raises_transient_on_request_denied(monkeypatch):
    """REQUEST_DENIED (e.g. bad key, referrer mismatch) is also transient
    from the cache's perspective — the user may fix it and retry."""
    from vireo import places

    monkeypatch.setattr(
        "vireo.places.urllib.request.urlopen",
        _make_fake_urlopen({"status": "REQUEST_DENIED"}),
    )

    with pytest.raises(places.PlacesTransientError):
        places.reverse_geocode(40.0, -73.0, "FAKE_KEY")


def test_reverse_geocode_raises_transient_on_network_failure(monkeypatch):
    """Transport errors (URLError, socket timeout, malformed JSON) are
    transient — wrapper raises PlacesTransientError chained from the cause."""
    from vireo import places

    def boom(url, timeout=10):
        raise urllib.error.URLError("simulated network failure")

    monkeypatch.setattr("vireo.places.urllib.request.urlopen", boom)

    with pytest.raises(places.PlacesTransientError):
        places.reverse_geocode(40.0, -73.0, "FAKE_KEY")
