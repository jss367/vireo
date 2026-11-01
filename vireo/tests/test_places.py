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
import urllib.parse
from pathlib import Path

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


def test_http_requests_use_bundled_ca_bundle(monkeypatch):
    """Packaged builds must not rely on Python's build-time CA paths."""
    from vireo import places

    captured = {}

    def fake_urlopen(url, *args, **kwargs):
        captured.update(kwargs)
        return _FakeResponse({"status": "ZERO_RESULTS"})

    monkeypatch.setattr("vireo.places.urllib.request.urlopen", fake_urlopen)

    assert places.place_details("missing", "FAKE_KEY") is None
    assert captured["context"] is places._SSL_CONTEXT
    assert places._SSL_CONTEXT.cert_store_stats()["x509_ca"] > 0


def test_ssl_context_preserves_configured_roots_and_adds_bundled_roots(monkeypatch):
    """The bundled roots augment rather than replace environment defaults."""
    from vireo import places

    class FakeContext:
        loaded_cafiles = []

        def load_verify_locations(self, *, cafile):
            self.loaded_cafiles.append(cafile)

    context = FakeContext()
    monkeypatch.setattr(places.ssl, "create_default_context", lambda: context)
    monkeypatch.setattr(places.certifi, "where", lambda: "/bundled/cacert.pem")

    assert places._create_ssl_context() is context
    assert context.loaded_cafiles == ["/bundled/cacert.pem"]


def test_place_details_parses_response(monkeypatch):
    """Place Details OK response is normalized into the wrapper's dict shape."""
    from vireo import places

    canned = {
        "status": "OK",
        "result": {
            "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
            "name": "Central Park",
            "types": ["park", "point_of_interest", "establishment"],
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
    assert out["types"] == ["park", "point_of_interest", "establishment"]
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
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    assert query["language"] == ["en"]
    fields = query["fields"][0]
    assert "type" in fields.split(",")
    assert "types" not in fields.split(",")


def test_place_details_returns_none_on_zero_results(monkeypatch):
    """ZERO_RESULTS / NOT_FOUND / empty result → ``None``, no raise."""
    from vireo import places

    monkeypatch.setattr(
        "vireo.places.urllib.request.urlopen",
        _make_fake_urlopen({"status": "ZERO_RESULTS"}),
    )

    assert places.place_details("ChIJ_does_not_exist", "FAKE_KEY") is None


def _routing_urlopen(routes: dict, captured: list | None = None):
    """A fake urlopen answering each Google endpoint with its own payload."""
    def fake_urlopen(url, *args, **kwargs):
        if captured is not None:
            captured.append(url)
        for fragment, payload in routes.items():
            if fragment in url:
                return _FakeResponse(payload)
        raise AssertionError(f"unexpected Google request {url}")
    return fake_urlopen


def _component(name, *types):
    return {"long_name": name, "short_name": name, "types": list(types)}


_OCEAN_BEACH_GEOCODE = {
    "status": "OK",
    "results": [
        {
            # Geocoding's first answer on land: the nearest street address.
            "place_id": "street",
            "formatted_address": "2525 Bacon St, San Diego, CA 92107, USA",
            "types": ["establishment", "point_of_interest"],
            "geometry": {"location": {"lat": 32.7543, "lng": -117.2414}},
            "address_components": [_component("2525", "street_number"),
                                   _component("Bacon Street", "route")],
        },
        {
            "place_id": "plus",
            "formatted_address": "QQ35+XR San Diego, CA, USA",
            "types": ["plus_code"],
            "geometry": {"location": {"lat": 32.7549, "lng": -117.2404}},
            "address_components": [_component("QQ35+XR", "plus_code")],
        },
        {
            "place_id": "city",
            "formatted_address": "San Diego, CA, USA",
            "types": ["locality", "political"],
            "geometry": {"location": {"lat": 32.7157, "lng": -117.1611}},
            "address_components": [_component("San Diego", "locality", "political")],
        },
        {
            "place_id": "ocean-beach",
            "formatted_address": "Ocean Beach, San Diego, CA, USA",
            "types": ["neighborhood", "political"],
            "geometry": {"location": {"lat": 32.7495, "lng": -117.2470}},
            "address_components": [
                _component("Ocean Beach", "neighborhood", "political"),
                _component("San Diego", "locality", "political"),
                _component("California", "administrative_area_level_1", "political"),
                _component("United States", "country", "political"),
            ],
        },
    ],
}


def test_reverse_geocode_names_the_area_never_a_street_address(monkeypatch):
    """With no park nearby, the most specific named area wins, named by
    the area itself rather than its formatted address."""
    from vireo import places

    captured = []
    monkeypatch.setattr("vireo.places.urllib.request.urlopen", _routing_urlopen({
        "nearbysearch": {"status": "ZERO_RESULTS"},
        "geocode": _OCEAN_BEACH_GEOCODE,
    }, captured))

    out = places.reverse_geocode(32.7549808, -117.2404343, "FAKE_KEY")

    assert out["place_id"] == "ocean-beach"
    assert out["name"] == "Ocean Beach"
    assert out["source"] == "area"
    assert out["lat"] == pytest.approx(32.7495)
    assert [c["name"] for c in out["address_components"]][-1] == "United States"
    assert len(captured) == 2
    nearby = urllib.parse.parse_qs(urllib.parse.urlparse(captured[0]).query)
    assert nearby["type"] == ["park"]
    assert nearby["rankby"] == ["distance"]
    assert nearby["location"] == ["32.7549808,-117.2404343"]
    geocode = urllib.parse.parse_qs(urllib.parse.urlparse(captured[1]).query)
    assert geocode["latlng"] == ["32.7549808,-117.2404343"]
    assert geocode["key"] == ["FAKE_KEY"]
    assert nearby["language"] == geocode["language"] == ["en"]


def test_reverse_geocode_prefers_a_well_known_park_nearby(monkeypatch):
    """A reviewed park within reach names the spot; a "|" in its Google
    name, Lightroom's hierarchy delimiter, becomes a dash."""
    from vireo import places

    monkeypatch.setattr("vireo.places.urllib.request.urlopen", _routing_urlopen({
        "nearbysearch": {"status": "OK", "results": [{
            "place_id": "dog-beach",
            "name": "Ocean Beach | Dog Beach",
            "types": ["park"],
            "user_ratings_total": 1026,
            "geometry": {"location": {"lat": 32.7545, "lng": -117.2500}},
        }]},
        "details": {"status": "OK", "result": {
            "place_id": "dog-beach",
            "name": "Ocean Beach | Dog Beach",
            "types": ["park", "point_of_interest"],
            "geometry": {"location": {"lat": 32.7545, "lng": -117.2500}},
            "address_components": [_component("San Diego", "locality")],
        }},
    }))

    out = places.reverse_geocode(32.7541, -117.2504, "FAKE_KEY")

    assert out["place_id"] == "dog-beach"
    assert out["name"] == "Ocean Beach - Dog Beach"
    assert out["source"] == "park"


def test_reverse_geocode_ignores_unreviewed_and_distant_parks(monkeypatch):
    """Anyone can add a "park"; an unreviewed one nearby and a famous one
    out of reach both fall through to the named area."""
    from vireo import places

    monkeypatch.setattr("vireo.places.urllib.request.urlopen", _routing_urlopen({
        "nearbysearch": {"status": "OK", "results": [
            {"place_id": "bench", "name": "Bench", "user_ratings_total": 2,
             "geometry": {"location": {"lat": 32.75500, "lng": -117.24040}}},
            {"place_id": "far", "name": "Fiesta Island", "user_ratings_total": 1996,
             "geometry": {"location": {"lat": 32.7700, "lng": -117.2200}}},
        ]},
        "geocode": _OCEAN_BEACH_GEOCODE,
    }))

    out = places.reverse_geocode(32.7549808, -117.2404343, "FAKE_KEY")

    assert out["name"] == "Ocean Beach"


def test_reverse_geocode_treats_a_missing_park_record_as_transient(monkeypatch):
    """Nearby Search just returned the park, so a failed details lookup must
    not be cached as the area fallback."""
    from vireo import places

    monkeypatch.setattr("vireo.places.urllib.request.urlopen", _routing_urlopen({
        "nearbysearch": {"status": "OK", "results": [{
            "place_id": "robb-field", "name": "Robb Field", "user_ratings_total": 1121,
            "geometry": {"location": {"lat": 32.7549, "lng": -117.2404}},
        }]},
        "details": {"status": "UNKNOWN_ERROR"},
        "geocode": _OCEAN_BEACH_GEOCODE,
    }))

    with pytest.raises(places.PlacesTransientError):
        places.reverse_geocode(32.7549808, -117.2404343, "FAKE_KEY")


def test_reverse_geocode_returns_none_without_a_named_area(monkeypatch):
    """Only addresses and plus codes (open water, say) is a no-match."""
    from vireo import places

    geocode = {"status": "OK", "results": _OCEAN_BEACH_GEOCODE["results"][:2]}
    monkeypatch.setattr("vireo.places.urllib.request.urlopen", _routing_urlopen({
        "nearbysearch": {"status": "ZERO_RESULTS"},
        "geocode": geocode,
    }))

    assert places.reverse_geocode(32.7549808, -117.2404343, "FAKE_KEY") is None


@pytest.mark.parametrize(
    "page_sources",
    [
        # Browse reads the preference in config.js and loads Maps in location.js.
        ("static/browse/config.js", "static/browse/location.js"),
        # Keywords loads Maps (and reads the preference) in link-place.js.
        ("static/keywords/link-place.js",),
        # Location Review loads Maps (and reads the preference) in map.js.
        ("static/location-review/map.js",),
    ],
)
def test_google_maps_javascript_loaders_honor_english_preference(page_sources):
    """All browser Places flows should conditionally request English."""
    source = "\n".join(
        (Path(__file__).parent.parent / page_source).read_text(encoding="utf-8")
        for page_source in page_sources
    )

    assert "google_maps_prefer_english" in source
    assert "&language=en" in source


def test_place_details_omits_language_when_disabled(monkeypatch):
    """The local-name preference leaves Google's language parameter unset."""
    from vireo import places

    captured = []
    monkeypatch.setattr(
        "vireo.places.urllib.request.urlopen",
        _make_fake_urlopen({"status": "ZERO_RESULTS"}, captured),
    )

    assert places.place_details("somewhere", "FAKE_KEY", language=None) is None
    query = urllib.parse.parse_qs(
        urllib.parse.urlparse(captured[0]).query,
    )
    assert "language" not in query


def test_reverse_geocode_omits_language_when_disabled(monkeypatch):
    """Reverse geocoding also supports Google's default localization."""
    from vireo import places

    captured = []
    monkeypatch.setattr(
        "vireo.places.urllib.request.urlopen",
        _make_fake_urlopen({"status": "ZERO_RESULTS"}, captured),
    )

    assert places.reverse_geocode(1.0, 2.0, "FAKE_KEY", language=None) is None
    assert len(captured) == 2
    for url in captured:
        assert "language" not in urllib.parse.parse_qs(urllib.parse.urlparse(url).query)


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

    def boom(url, timeout=10, context=None):
        raise urllib.error.URLError("simulated network failure")

    monkeypatch.setattr("vireo.places.urllib.request.urlopen", boom)

    with pytest.raises(places.PlacesTransientError):
        places.reverse_geocode(40.0, -73.0, "FAKE_KEY")


def _nearby_result(place_id, name, lat, lng, ratings):
    return {"place_id": place_id, "name": name, "user_ratings_total": ratings,
            "geometry": {"location": {"lat": lat, "lng": lng}}}


def test_nearby_named_places_lists_visited_parks_and_features_nearest_first(monkeypatch):
    """Around Laguna Meadow (32.872, -116.459), Google names ravines nobody
    reviews; visited lakes, meadows and the recreation area are offered."""
    from vireo import places

    captured = []

    def fake_urlopen(url, *args, **kwargs):
        captured.append(url)
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        if query["type"] == ["park"]:
            return _FakeResponse({"status": "OK", "results": [
                _nearby_result("ravine-park", "Los Rasalies Ravine", 32.8810, -116.4560, 0),
                _nearby_result("lmra", "Laguna Mountain Recreation Area", 32.8601, -116.4339, 43),
            ]})
        return _FakeResponse({"status": "OK", "results": [
            _nearby_result("ravine", "Los Rasalies Ravine", 32.8770, -116.4580, 2),
            _nearby_result("lake", "Big Laguna Lake", 32.8810, -116.4560, 31),
            _nearby_result("meadow", "Laguna | Meadow", 32.8840, -116.4560, 4),
            _nearby_result("lmra", "Laguna Mountain Recreation Area", 32.8601, -116.4339, 43),
            {"name": "No id", "user_ratings_total": 50,
             "geometry": {"location": {"lat": 32.87, "lng": -116.46}}},
        ]})

    monkeypatch.setattr("vireo.places.urllib.request.urlopen", fake_urlopen)

    out = places.nearby_named_places(32.8722, -116.4593, "FAKE_KEY")

    assert [(p["place_id"], p["name"], p["type"]) for p in out] == [
        ("lake", "Big Laguna Lake", "natural_feature"),
        ("meadow", "Laguna - Meadow", "natural_feature"),
        ("lmra", "Laguna Mountain Recreation Area", "park"),
    ]
    assert out[0]["lat"] == pytest.approx(32.8810)
    queries = [urllib.parse.parse_qs(urllib.parse.urlparse(u).query) for u in captured]
    assert [q["type"] for q in queries] == [["park"], ["natural_feature"]]
    assert all(q["rankby"] == ["distance"] and q["language"] == ["en"] for q in queries)


def test_nearby_named_places_raises_transient_and_returns_empty_on_no_match(monkeypatch):
    from vireo import places

    monkeypatch.setattr("vireo.places.urllib.request.urlopen",
                        _make_fake_urlopen({"status": "OVER_QUERY_LIMIT"}))
    with pytest.raises(places.PlacesTransientError):
        places.nearby_named_places(32.87, -116.46, "FAKE_KEY")

    monkeypatch.setattr("vireo.places.urllib.request.urlopen",
                        _make_fake_urlopen({"status": "ZERO_RESULTS"}))
    assert places.nearby_named_places(32.87, -116.46, "FAKE_KEY", language=None) == []


@pytest.mark.parametrize(("details", "broad"), [
    ({"source": "area", "types": ["administrative_area_level_2", "political"]}, True),
    ({"source": "area", "types": ["administrative_area_level_1", "political"]}, True),
    ({"source": "area", "types": ["neighborhood", "political"]}, False),
    ({"source": "area", "types": ["locality", "political"]}, False),
    ({"source": "park", "types": ["park"]}, False),
    ({"source": "area", "types": []}, False),
    (None, False),
])
def test_is_broad_area_means_county_or_broader(details, broad):
    from vireo import places

    assert places.is_broad_area(details) is broad
