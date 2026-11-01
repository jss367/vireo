"""Import location suggestions and the choices an import applies.

``services.import_locations`` suggests one place per GPS cell before the
import runs, from the user's own tagged photos and places first and Google
second, and after cataloging applies only the choices the page sent.
"""

import json
from types import SimpleNamespace

import places
import pytest
from services import gps_locations, import_locations, photo_ingest, workspace_folders
from services.gps_locations import (
    REVERSE_GEOCODE_CACHE_RESOLVER,
    encode_cached_reverse_geocode,
)
from services.import_locations import (
    LocationChoiceError,
    apply_location_choices,
    cell_key,
    gps_point,
    location_options,
    parse_location_choices,
    suggest_import_locations,
)

# A walk along the San Diego River's south bank, about 100 m per step.
RIVER = [(32.7549, -117.2404), (32.7547, -117.2415), (32.7545, -117.2426)]
FAR_AWAY = (40.7851, -73.9683)


@pytest.fixture
def fid(db):
    return workspace_folders.add_folder(db, "/photos", name="photos")


def _photo(db, fid, name, point=None):
    pid = photo_ingest.add_photo(
        db, folder_id=fid, filename=name, extension=".jpg", file_size=10, file_mtime=1.0,
    )
    if point is not None:
        db.conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?", (*point, pid),
        )
        db.conn.commit()
    return pid


def _kw(db, name, parent_id=None, point=(None, None), place_id=None):
    cur = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type, latitude, longitude, place_id) "
        "VALUES (?, ?, 'location', ?, ?, ?)",
        (name, parent_id, *point, place_id),
    )
    db.conn.commit()
    return cur.lastrowid


def _tag(db, pid, kid):
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id, source) VALUES (?, ?, 'manual')",
        (pid, kid),
    )
    db.conn.commit()


def _location(db, pid):
    """The name of the photo's location keyword, or None."""
    row = db.conn.execute(
        "SELECT k.name FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'", (pid,),
    ).fetchone()
    return row["name"] if row else None


@pytest.fixture
def no_google(monkeypatch):
    """Fail the test if a suggestion reaches Google."""
    def refuse(*args, **kwargs):
        raise AssertionError("Google was asked for a place the catalog already names")
    monkeypatch.setattr(places, "reverse_geocode", refuse)
    monkeypatch.setattr(places, "nearby_named_places", refuse)


@pytest.fixture(autouse=True)
def fresh_nearby_cache(monkeypatch):
    """Nearby answers are cached per process; start each test without them."""
    monkeypatch.setattr(import_locations, "_nearby_cache", {})


def _files(points):
    return {f"/card/DSC_{i:04d}.NEF": point for i, point in enumerate(points)}


# -- reading GPS ----------------------------------------------------------------


def test_gps_point_reads_composite_first_like_the_scanner():
    assert gps_point({"Composite": {"GPSLatitude": 32.75, "GPSLongitude": -117.24},
                      "EXIF": {"GPSLatitude": 32.75, "GPSLongitude": 117.24}}) == (32.75, -117.24)
    assert gps_point({"EXIF": {"GPSLatitude": 1.5, "GPSLongitude": 2.5}}) == (1.5, 2.5)
    assert gps_point({"EXIF": {"GPSLatitude": 1.5}}) is None
    assert gps_point({"Composite": {"GPSLatitude": "nan", "GPSLongitude": 2}}) is None
    assert gps_point({"Composite": {"GPSLatitude": 91, "GPSLongitude": 2}}) is None


# -- suggestions -----------------------------------------------------------------


def test_photos_you_tagged_nearby_name_the_spot_without_asking_google(db, fid, no_google):
    river = _kw(db, "San Diego River")
    other = _kw(db, "Robb Field")
    # Three tagged photos along the bank, one stray elsewhere-tag among them.
    for i, point in enumerate(RIVER):
        _tag(db, _photo(db, fid, f"old{i}.jpg", point), river)
    _tag(db, _photo(db, fid, "odd.jpg", RIVER[0]), other)

    files = _files(RIVER * 2)
    result = suggest_import_locations(db, files, api_key="KEY", language="en")

    [group] = result["groups"]
    assert group["file_count"] == 6
    assert sorted(group["cells"]) == sorted({cell_key(*p) for p in RIVER})
    suggestion = group["suggestion"]
    assert suggestion["kind"] == "keyword"
    assert suggestion["keyword_id"] == river
    assert suggestion["source"] == "your_photos"
    assert suggestion["name"] == "San Diego River"
    # Distinct photos, though every cell sees the same three.
    assert suggestion["photo_count"] == 3
    assert result["file_cells"] == {path: cell_key(*p) for path, p in files.items()}


def test_tagged_photos_out_of_reach_do_not_name_the_spot(db, fid, monkeypatch):
    river = _kw(db, "San Diego River")
    _tag(db, _photo(db, fid, "old.jpg", (32.7600, -117.2115)), river)  # 3 km upstream
    monkeypatch.setattr(places, "reverse_geocode", lambda *a, **k: None)

    result = suggest_import_locations(db, _files(RIVER[:1]), api_key="KEY", language="en")

    [group] = result["groups"]
    assert group["suggestion"] is None
    assert group["unresolved_reasons"] == {"no_match": 1}


def test_your_saved_place_point_names_a_spot_with_no_tagged_photos(db, no_google):
    parent = _kw(db, "San Diego")
    slough = _kw(db, "Famosa Slough", parent_id=parent, point=(32.7530, -117.2400))

    result = suggest_import_locations(db, _files(RIVER[:1]), api_key="KEY", language="en")

    suggestion = result["groups"][0]["suggestion"]
    assert suggestion["keyword_id"] == slough
    assert suggestion["source"] == "your_place"
    assert suggestion["parents"] == ["San Diego"]
    assert 0 < suggestion["distance_m"] <= import_locations.PLACE_RADIUS_M


def test_google_names_new_spots_and_the_answer_is_cached(db, monkeypatch):
    calls = []

    def fake_reverse_geocode(lat, lng, api_key, *, language="en"):
        calls.append((lat, lng))
        return {"place_id": "robb-field", "name": "Robb Field", "types": ["park"],
                "lat": 32.7549, "lng": -117.2404, "source": "park",
                "address_components": [{"name": "San Diego", "types": ["locality"]}]}

    monkeypatch.setattr(places, "reverse_geocode", fake_reverse_geocode)
    files = _files(RIVER[:2])
    first = suggest_import_locations(db, files, api_key="KEY", language="en")
    second = suggest_import_locations(db, files, api_key="KEY", language="en")

    [group] = first["groups"]
    assert group["file_count"] == 2
    assert group["suggestion"] == {
        "kind": "google", "place_id": "robb-field", "name": "Robb Field",
        "summary": "Robb Field · San Diego", "source": "google_park",
    }
    assert second["groups"] == first["groups"]
    assert len(calls) == 2  # one per cell, and none the second time


def test_a_google_place_you_already_have_is_suggested_as_your_keyword(db, monkeypatch):
    robb = _kw(db, "Robb Field", place_id="robb-field")
    monkeypatch.setattr(places, "reverse_geocode", lambda *a, **k: {
        "place_id": "robb-field", "name": "Robb Field", "types": ["park"],
        "lat": 1.0, "lng": 2.0, "source": "park", "address_components": []})

    result = suggest_import_locations(db, _files(RIVER[:1]), api_key="KEY", language="en")

    suggestion = result["groups"][0]["suggestion"]
    assert (suggestion["kind"], suggestion["keyword_id"]) == ("keyword", robb)
    assert suggestion["source"] == "your_place"


def test_street_addresses_cached_by_the_old_resolver_are_looked_up_again(db, monkeypatch):
    lat, lng = RIVER[0]
    gps_locations.reverse_geocode_cache_put(db, lat, lng, place_id="street", response_json=json.dumps({
        "place_id": "street", "name": "2525 Bacon St, San Diego, CA 92107, USA",
        "_vireo_result_language": "en",
    }))
    monkeypatch.setattr(places, "reverse_geocode", lambda *a, **k: {
        "place_id": "ocean-beach", "name": "Ocean Beach", "types": ["neighborhood"],
        "lat": lat, "lng": lng, "source": "area", "address_components": []})

    result = suggest_import_locations(db, _files([RIVER[0]]), api_key="KEY", language="en")

    assert result["groups"][0]["suggestion"]["name"] == "Ocean Beach"
    cached = gps_locations.reverse_geocode_cache_get(db, lat, lng)
    assert cached["place_id"] == "ocean-beach"


def test_without_an_api_key_unknown_spots_are_offered_unnamed(db, fid, no_google):
    river = _kw(db, "San Diego River")
    _tag(db, _photo(db, fid, "old.jpg", RIVER[0]), river)

    files = _files([RIVER[0], FAR_AWAY, FAR_AWAY])
    result = suggest_import_locations(db, files, api_key="", language="en")

    named, unnamed = result["groups"]
    assert named["suggestion"]["keyword_id"] == river
    assert unnamed["suggestion"] is None
    assert unnamed["file_count"] == 2
    assert unnamed["unresolved_reasons"] == {"no_api_key": 1}


def test_vanished_keyword_groups_keep_unique_keys(db, fid, no_google):
    """When a suggested keyword is deleted mid-request, the fallback must
    keep each group's key unique: the frontend stores per-group edits by
    that key, so colliding keys would apply one row's typed name to every
    other collided group — tagging cells the user left blank."""
    near_a = _kw(db, "Place A", point=(RIVER[0][0] + 0.0001, RIVER[0][1]))
    near_c = _kw(db, "Place C", point=(RIVER[2][0] + 0.0001, RIVER[2][1]))
    _photo(db, fid, "unnamed.jpg", FAR_AWAY)  # one cell with no suggestion
    files = _files([RIVER[0], RIVER[2], FAR_AWAY])
    suggester = import_locations.ImportLocationSuggester(
        db, files, api_key="", language="en")
    for key in suggester.cell_keys:
        suggester.name_cell(key)
    # Both suggested keywords vanish before the groups are finished.
    db.conn.execute("DELETE FROM keywords WHERE id IN (?, ?)", (near_a, near_c))
    db.conn.commit()

    groups = suggester.result()["groups"]
    assert [g["suggestion"] for g in groups] == [None, None, None]
    keys = [g["key"] for g in groups]
    assert len(set(keys)) == 3


def test_location_options_tell_same_named_places_apart(db):
    slo = _kw(db, "San Luis Obispo")
    sd = _kw(db, "San Diego")
    _kw(db, "Laguna Lake", parent_id=slo)
    _kw(db, "Laguna Lake", parent_id=sd)
    _kw(db, "Famosa Slough", parent_id=sd)

    labels = [o["label"] for o in location_options(db)]

    assert "Famosa Slough" in labels
    assert "Laguna Lake · San Diego" in labels
    assert "Laguna Lake · San Luis Obispo" in labels
    assert len(labels) == len(set(labels))


def test_location_options_stay_unique_when_ancestry_reads_alike(db):
    """Same names three parents deep, a name that reads like another place's
    breadcrumb, and case-only differences all get distinct labels."""
    ids = []
    for _ in range(2):
        top = _kw(db, "USA")
        state = _kw(db, "California", parent_id=top)
        county = _kw(db, "San Diego County", parent_id=state)
        ids.append(_kw(db, "Laguna Lake", parent_id=county))
    beach = _kw(db, "Ocean Beach")
    dog = _kw(db, "Dog Beach", parent_id=beach)
    _kw(db, "Dog Beach")  # a second one forces the first to "Dog Beach · Ocean Beach"
    breadcrumb = _kw(db, "Dog Beach · Ocean Beach")
    _kw(db, "famosa slough")
    _kw(db, "Famosa Slough")

    options = location_options(db)
    labels = {o["id"]: o["label"] for o in options}

    assert len({label.casefold() for label in labels.values()}) == len(labels)
    assert labels[ids[0]] == f"Laguna Lake · San Diego County · California · USA [#{ids[0]}]"
    assert labels[dog] == f"Dog Beach · Ocean Beach [#{dog}]"
    assert labels[breadcrumb] == "Dog Beach · Ocean Beach"


# -- named places near broadly named spots ---------------------------------------

# A walk past Laguna Meadow in the Cleveland National Forest, where Geocoding's
# most specific name is the county.
LAGUNA = [(32.8750, -116.4640), (32.8780, -116.4610), (32.8750, -116.4490),
          (32.8620, -116.4620)]
_COUNTY = {
    "place_id": "sd-county", "name": "San Diego County",
    "types": ["administrative_area_level_2", "political"], "lat": 33.0, "lng": -116.7,
    "source": "area", "address_components": [
        {"name": "San Diego County", "types": ["administrative_area_level_2"]},
        {"name": "California", "types": ["administrative_area_level_1"]}],
}
_LAGUNA_PLACES = [
    {"place_id": "lake", "name": "Big Laguna Lake", "type": "natural_feature",
     "lat": 32.8810, "lng": -116.4560},
    {"place_id": "lmra", "name": "Laguna Mountain Recreation Area", "type": "park",
     "lat": 32.8601, "lng": -116.4339},
    {"place_id": "far", "name": "Cuyamaca Rancho State Park", "type": "park",
     "lat": 32.9200, "lng": -116.5600},
]


@pytest.fixture
def broad_google(monkeypatch):
    """Google names every spot only as the county; returns the nearby lookups."""
    lookups = []

    def fake_nearby(lat, lng, api_key, *, language="en"):
        lookups.append((lat, lng, language))
        return _LAGUNA_PLACES

    monkeypatch.setattr(places, "reverse_geocode", lambda *a, **k: dict(_COUNTY))
    monkeypatch.setattr(places, "nearby_named_places", fake_nearby)
    return lookups


def test_a_county_only_spot_offers_named_places_nearby_with_their_distance(db, broad_google):
    result = suggest_import_locations(db, _files(LAGUNA), api_key="KEY", language="en")

    [group] = result["groups"]
    assert group["suggestion"]["name"] == "San Diego County"
    assert group["suggestion"]["source"] == "google_area"
    nearby = group["nearby_places"]
    # Nearest first, measured from the closest photo spot to each pin; the
    # state park's pin is out of reach.
    assert [p["place_id"] for p in nearby] == ["lake", "lmra"]
    assert nearby[0] == {"place_id": "lake", "name": "Big Laguna Lake",
                         "type": "natural_feature", "distance_m": 574}
    assert nearby[1]["distance_m"] == 2176
    assert "nearby_unresolved" not in group
    # One lookup per ~2 km bucket, not per cell.
    buckets = {import_locations._nearby_bucket(*p) for p in LAGUNA}
    assert len(broad_google) == len(buckets) < len(LAGUNA)
    assert {language for *_, language in broad_google} == {"en"}


def test_nearby_places_are_cached_for_the_next_preview(db, broad_google):
    first = suggest_import_locations(db, _files(LAGUNA), api_key="KEY", language="en")
    asked = len(broad_google)
    second = suggest_import_locations(db, _files(LAGUNA), api_key="KEY", language="en")

    assert second["groups"] == first["groups"]
    assert len(broad_google) == asked


def test_specifically_named_spots_do_not_look_for_nearby_places(db, monkeypatch):
    monkeypatch.setattr(places, "reverse_geocode", lambda *a, **k: {
        "place_id": "ocean-beach", "name": "Ocean Beach", "types": ["neighborhood", "political"],
        "lat": 1.0, "lng": 2.0, "source": "area", "address_components": []})
    monkeypatch.setattr(places, "nearby_named_places",
                        lambda *a, **k: pytest.fail("looked for places near a named spot"))

    [group] = suggest_import_locations(db, _files(RIVER), api_key="KEY", language="en")["groups"]

    assert group["suggestion"]["name"] == "Ocean Beach"
    assert "nearby_places" not in group


def test_a_failed_nearby_lookup_is_reported_and_asked_again_next_time(db, monkeypatch):
    calls = []

    def failing(*args, **kwargs):
        calls.append(args)
        raise places.PlacesTransientError("OVER_QUERY_LIMIT")

    monkeypatch.setattr(places, "reverse_geocode", lambda *a, **k: dict(_COUNTY))
    monkeypatch.setattr(places, "nearby_named_places", failing)

    [group] = suggest_import_locations(db, _files(LAGUNA[:1]), api_key="KEY", language="en")["groups"]
    suggest_import_locations(db, _files(LAGUNA[:1]), api_key="KEY", language="en")

    assert group["nearby_places"] == []
    assert group["nearby_unresolved"] == "transient_error"
    assert len(calls) == 2


def test_a_cached_county_without_an_api_key_says_why_nothing_is_nearby(db, broad_google):
    suggest_import_locations(db, _files(LAGUNA[:1]), api_key="KEY", language="en")
    import_locations._nearby_cache.clear()
    asked = len(broad_google)

    [group] = suggest_import_locations(db, _files(LAGUNA[:1]), api_key="", language="en")["groups"]

    assert group["suggestion"]["name"] == "San Diego County"  # from the geocode cache
    assert group["nearby_places"] == []
    assert group["nearby_unresolved"] == "no_api_key"
    assert len(broad_google) == asked


def test_cached_nearby_results_are_suppressed_after_the_api_key_is_removed(db, broad_google):
    """Clicking a cached button would fail at import time without a key, so
    the preview must hide the cached buttons instead of promising them."""
    suggest_import_locations(db, _files(LAGUNA[:1]), api_key="KEY", language="en")
    asked = len(broad_google)

    [group] = suggest_import_locations(db, _files(LAGUNA[:1]), api_key="", language="en")["groups"]

    assert group["nearby_places"] == []
    assert group["nearby_unresolved"] == "no_api_key"
    assert len(broad_google) == asked  # no new provider calls either


def test_choosing_a_nearby_place_tags_it_and_keeps_the_photos_measured_gps(db, fid, monkeypatch):
    """The place's pin becomes the keyword's point; no photo is moved to it."""
    pids = [_photo(db, fid, f"new{i}.jpg", p) for i, p in enumerate(LAGUNA)]
    monkeypatch.setattr(places, "place_details_for_language", lambda *a: {
        "place_id": "lake", "name": "Big Laguna Lake", "types": ["natural_feature"],
        "lat": 32.8810, "lng": -116.4560, "address_components": [
            {"name": "Big Laguna Lake", "types": ["natural_feature"]},
            {"name": "San Diego County", "types": ["administrative_area_level_2"]}]})
    choices = parse_location_choices([{
        "cells": sorted({cell_key(*p) for p in LAGUNA}), "kind": "google",
        "place_id": "lake", "name": "Big Laguna Lake",
    }])
    summary = _summary()

    apply_location_choices(db, pids, choices, api_key="KEY", language="en",
                           summary=summary, cancel_check=_never)

    assert summary["locations_added"] == len(LAGUNA)
    for pid, point in zip(pids, LAGUNA, strict=True):
        assert _location(db, pid) == "Big Laguna Lake"
        row = db.conn.execute(
            "SELECT latitude, longitude FROM photos WHERE id = ?", (pid,)).fetchone()
        assert (row["latitude"], row["longitude"]) == point


def test_an_over_limit_preview_refuses_before_paying_for_any_nearby_lookups(db, monkeypatch):
    """100,000 broad cells in distinct 2 km buckets must not drive 200,000 paid calls."""
    monkeypatch.setattr(places, "reverse_geocode", lambda *a, **k: dict(_COUNTY))
    monkeypatch.setattr(places, "nearby_named_places",
                        lambda *a, **k: pytest.fail("looked up nearby for an over-limit preview"))
    # 0.03-degree steps so each cell lives in its own 0.02-degree bucket.
    points = [(10.0 + i * 0.03, 20.0) for i in range(import_locations.MAX_CHOICES + 1)]
    assert len({import_locations._nearby_bucket(*p) for p in points}) == len(points)

    result = suggest_import_locations(db, _files(points), api_key="KEY", language="en")

    assert result["groups"] == []
    assert "work_limit_error" in result


def test_nearby_distance_is_from_the_closest_photo_in_the_cell(db, monkeypatch):
    """For multiple photos in one reverse-geocode cell, pick the per-photo
    minimum: a place just inside 5 km of one photo must not be excluded when
    the cell's averaged centroid falls outside."""
    # Two photos ~55 m apart in one reverse-geocode cell: the centroid is
    # between them. The pin at (32.9205, -116.5440) is ~1 m north of photo A
    # but ~110 m north of the centroid.
    photo_a = (32.9205, -116.5440)
    photo_b = (32.9195, -116.5440)
    assert cell_key(*photo_a) == cell_key(*photo_b)
    pin = (32.9206, -116.5440)
    nearby_place = {"place_id": "spot", "name": "Spot", "type": "park",
                    "lat": pin[0], "lng": pin[1]}
    monkeypatch.setattr(places, "reverse_geocode", lambda *a, **k: dict(_COUNTY))
    monkeypatch.setattr(places, "nearby_named_places", lambda *a, **k: [nearby_place])

    [group] = suggest_import_locations(
        db, _files([photo_a, photo_b]), api_key="KEY", language="en")["groups"]

    assert len(group["nearby_places"]) == 1
    assert group["nearby_places"][0]["distance_m"] < 60  # not the ~110 m centroid distance


def test_stream_reports_nearby_lookups_after_naming(db, monkeypatch, broad_google, exiftool_present):
    monkeypatch.setattr(import_locations, "read_source_gps",
                        lambda paths: dict(zip(paths, LAGUNA, strict=True)))
    paths = [f"/card/{i}.NEF" for i in range(len(LAGUNA))]

    frames = [json.loads(f[len("data: "):]) for f in import_locations.stream_location_suggestions(
        db._db_path, paths, {"google_maps_api_key": "KEY"})]

    nearby = [f for f in frames if "nearby" in f]
    assert nearby and nearby[-1] == {"nearby": len(broad_google), "total": len(broad_google)}
    first_nearby = frames.index(nearby[0])
    assert all("naming" in f for f in frames[first_nearby - len(LAGUNA):first_nearby])
    assert frames[-1]["groups"][0]["nearby_places"][0]["name"] == "Big Laguna Lake"


# -- choices ------------------------------------------------------------------------


def test_parse_location_choices_keeps_the_request_shape():
    raw = [
        {"cells": ["32755,-117240", "32755,-117240"], "kind": "keyword", "keyword_id": 7},
        {"cells": ["1,1"], "kind": "google", "place_id": " robb ", "name": " Robb Field "},
        {"cells": ["2,2"], "kind": "new", "name": "Dog Beach", "latitude": 32.75, "longitude": -117.25},
        {"cells": ["3,3"], "kind": "new", "name": "Somewhere", "latitude": None, "longitude": None},
    ]
    parsed = parse_location_choices(raw)
    assert parsed == [
        {"kind": "keyword", "keyword_id": 7, "cells": ["32755,-117240"]},
        {"kind": "google", "place_id": "robb", "name": "Robb Field", "cells": ["1,1"]},
        {"kind": "new", "name": "Dog Beach", "latitude": 32.75, "longitude": -117.25, "cells": ["2,2"]},
        {"kind": "new", "name": "Somewhere", "latitude": None, "longitude": None, "cells": ["3,3"]},
    ]
    assert parse_location_choices(parsed) == parsed
    assert parse_location_choices(None) == []


def test_admission_bounds_group_work_separately_from_preview_cells():
    """File/cell capacity never authorizes extra provider calls or writes."""
    cap = import_locations.MAX_CHOICES
    raw = [{"cells": [f"{i // 1000},{i % 1000}"], "kind": "keyword", "keyword_id": 1}
           for i in range(cap)]
    assert len(parse_location_choices(raw)) == cap
    with pytest.raises(LocationChoiceError):
        parse_location_choices(raw + [{"cells": ["-1,-1"], "kind": "keyword", "keyword_id": 1}])
    cells = [f"{i // 1000},{i % 1000}" for i in range(import_locations.MAX_CHOICE_CELLS)]
    assert len(parse_location_choices([{"kind": "keyword", "keyword_id": 1, "cells": cells}])[0]["cells"]) == len(cells)
    with pytest.raises(LocationChoiceError):
        parse_location_choices([{"kind": "keyword", "keyword_id": 1, "cells": cells + ["-1,-1"]}])



@pytest.mark.parametrize("raw", [
    "x",
    [{"cells": ["1,1"], "kind": "new", "name": "A", "latitude": "x", "longitude": 2}],
    [{"cells": ["1,1"], "kind": "new", "name": "A", "latitude": 1.5}],
    [{"cells": ["1,1"], "kind": "new", "name": "A", "latitude": 95, "longitude": 2}],
    [{"cells": [], "kind": "new", "name": "A"}],
    [{"cells": ["1,1"], "kind": "keyword", "keyword_id": True}],
    [{"cells": ["1,1"], "kind": "new", "name": "x" * 201}],
])
def test_parse_location_choices_rejects_malformed_entries(raw):
    with pytest.raises(LocationChoiceError):
        parse_location_choices(raw)


def _summary():
    return {"errors": []}


def _never(**kwargs):
    return False


def test_apply_tags_only_the_chosen_cells_and_keeps_existing_locations(db, fid):
    river = _kw(db, "San Diego River")
    elsewhere = _kw(db, "Elsewhere")
    chosen = [_photo(db, fid, f"new{i}.jpg", p) for i, p in enumerate(RIVER)]
    already = _photo(db, fid, "placed.jpg", RIVER[0])
    _tag(db, already, elsewhere)
    skipped_spot = _photo(db, fid, "far.jpg", FAR_AWAY)
    no_gps = _photo(db, fid, "nogps.jpg")
    choices = parse_location_choices([{
        "cells": sorted({cell_key(*p) for p in RIVER}), "kind": "keyword", "keyword_id": river,
    }])
    summary = _summary()

    apply_location_choices(
        db, [*chosen, already, skipped_spot, no_gps], choices,
        api_key="", language="en", summary=summary, cancel_check=_never,
    )

    assert summary["locations_added"] == 3
    assert summary["locations_kept"] == 1
    assert summary["locations_unchosen"] == 1
    assert summary["locations_no_gps"] == 1
    assert summary["errors"] == []
    for pid in chosen:
        assert _location(db, pid) == "San Diego River"
    assert _location(db, already) == "Elsewhere"
    assert _location(db, skipped_spot) is None
    history = db.conn.execute(
        "SELECT description FROM edit_history ORDER BY id DESC LIMIT 1").fetchone()
    assert history["description"] == 'Added location "San Diego River" during import to 3 photos'


def test_apply_creates_a_typed_location_at_the_groups_point(db, fid):
    pid = _photo(db, fid, "new.jpg", RIVER[0])
    choices = parse_location_choices([{
        "cells": [cell_key(*RIVER[0])], "kind": "new", "name": "Dog Beach",
        "latitude": 32.7545, "longitude": -117.2500,
    }])

    apply_location_choices(db, [pid], choices, api_key="", language="en",
                           summary=_summary(), cancel_check=_never)

    row = db.conn.execute(
        "SELECT type, latitude, longitude FROM keywords WHERE name = 'Dog Beach'").fetchone()
    assert (row["type"], row["latitude"], row["longitude"]) == ("location", 32.7545, -117.25)
    assert _location(db, pid) == "Dog Beach"


def test_apply_reuses_a_google_place_already_in_the_catalog(db, fid, monkeypatch):
    robb = _kw(db, "Robb Field", place_id="robb-field")
    pid = _photo(db, fid, "new.jpg", RIVER[0])
    monkeypatch.setattr(places, "place_details_for_language",
                        lambda *a: pytest.fail("looked up a place the catalog has"))
    choices = parse_location_choices([{
        "cells": [cell_key(*RIVER[0])], "kind": "google", "place_id": "robb-field",
        "name": "Robb Field",
    }])

    apply_location_choices(db, [pid], choices, api_key="KEY", language="en",
                           summary=_summary(), cancel_check=_never)

    linked = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid,)).fetchone()
    assert linked["keyword_id"] == robb


def test_a_failed_choice_leaves_its_photos_untagged_and_says_so(db, fid, monkeypatch):
    lost = _photo(db, fid, "lost.jpg", RIVER[0])
    kept = _photo(db, fid, "kept.jpg", FAR_AWAY)
    monkeypatch.setattr(places, "place_details_for_language", lambda *a: None)
    choices = parse_location_choices([
        {"cells": [cell_key(*RIVER[0])], "kind": "google", "place_id": "gone", "name": "Gone Park"},
        {"cells": [cell_key(*FAR_AWAY)], "kind": "new", "name": "Central Park"},
    ])
    summary = _summary()

    apply_location_choices(db, [lost, kept], choices, api_key="KEY", language="en",
                           summary=summary, cancel_check=_never)

    assert summary["locations_unresolved"] == 1
    assert summary["locations_added"] == 1
    assert summary["errors"] == [
        'Could not look up "Gone Park" on Google Maps; 1 photos were left untagged']
    assert _location(db, lost) is None
    assert _location(db, kept) == "Central Park"


def test_cancelling_mid_choice_leaves_nothing_behind(db, fid, monkeypatch):
    """A new keyword, its assignments and their sync rows commit together or
    not at all, so a Stop partway through leaves no orphan location."""
    pids = [_photo(db, fid, f"new{i}.jpg", RIVER[0]) for i in range(3)]
    choices = parse_location_choices([{
        "cells": [cell_key(*RIVER[0])], "kind": "new", "name": "Dog Beach",
        "latitude": 32.7545, "longitude": -117.25,
    }])
    from services import edit_history

    assigned = []
    change_location = edit_history.change_photo_location

    def observed_assignment(*args, **kwargs):
        item = change_location(*args, **kwargs)
        if item is not None:
            assigned.append(item["photo_id"])
        return item

    monkeypatch.setattr(edit_history, "change_photo_location", observed_assignment)

    def cancel_after_first_photo(**kwargs):
        return bool(assigned)

    summary = _summary()
    apply_location_choices(db, pids, choices, api_key="", language="en",
                           summary=summary, cancel_check=cancel_after_first_photo)

    assert assigned == [pids[0]]  # A write happened before Stop; rollback must undo it.
    assert summary["cancelled"] is True
    assert summary["locations_added"] == 0
    assert [_location(db, pid) for pid in pids] == [None, None, None]
    assert db.conn.execute("SELECT 1 FROM keywords WHERE name = 'Dog Beach'").fetchone() is None
    assert db.conn.execute(
        "SELECT 1 FROM edit_history WHERE description LIKE '%Dog Beach%'").fetchone() is None
    assert db.conn.execute(
        "SELECT COUNT(*) FROM pending_changes WHERE change_type = 'location'").fetchone()[0] == 0


def test_cancelling_during_the_final_assignment_rolls_back_too(db, fid, monkeypatch):
    """Stop arriving during the final (or only) ``change_photo_location``
    must not leave a tagged photo and an undo step behind: without the
    final in-transaction cancel_check, the loop ends and record_edit plus
    the commit still fly even though cancellation has been requested."""
    pid = _photo(db, fid, "lone.jpg", RIVER[0])
    choices = parse_location_choices([{
        "cells": [cell_key(*RIVER[0])], "kind": "new", "name": "Dog Beach",
        "latitude": 32.7545, "longitude": -117.25,
    }])
    from services import edit_history
    change_location = edit_history.change_photo_location
    assignments = []

    def observed_change(*args, **kwargs):
        item = change_location(*args, **kwargs)
        assignments.append(True)
        return item

    monkeypatch.setattr(edit_history, "change_photo_location", observed_change)

    def cancel_after_the_change(**kwargs):
        return bool(assignments)

    summary = _summary()
    apply_location_choices(db, [pid], choices, api_key="", language="en",
                           summary=summary, cancel_check=cancel_after_the_change)

    assert summary["cancelled"] is True
    assert summary.get("locations_added", 0) == 0
    assert _location(db, pid) is None
    assert db.conn.execute(
        "SELECT 1 FROM keywords WHERE name = 'Dog Beach'").fetchone() is None
    assert db.conn.execute(
        "SELECT 1 FROM edit_history WHERE description LIKE '%Dog Beach%'").fetchone() is None
    assert db.conn.execute(
        "SELECT COUNT(*) FROM pending_changes WHERE change_type = 'location'").fetchone()[0] == 0


def test_a_location_set_elsewhere_during_the_import_is_kept(db, fid, monkeypatch):
    """The keep-existing check is repeated inside the write transaction."""
    river = _kw(db, "San Diego River")
    elsewhere = _kw(db, "Elsewhere")
    raced, plain = _photo(db, fid, "raced.jpg", RIVER[0]), _photo(db, fid, "plain.jpg", RIVER[0])
    real_details = import_locations._google_details

    def another_tab_places_a_photo(*args):
        _tag(db, raced, elsewhere)
        return real_details(*args)

    monkeypatch.setattr(import_locations, "_google_details", another_tab_places_a_photo)
    summary = _summary()
    apply_location_choices(
        db, [raced, plain],
        parse_location_choices([{"cells": [cell_key(*RIVER[0])], "kind": "keyword", "keyword_id": river}]),
        api_key="", language="en", summary=summary, cancel_check=_never,
    )

    assert _location(db, raced) == "Elsewhere"
    assert _location(db, plain) == "San Diego River"
    assert (summary["locations_added"], summary["locations_kept"]) == (1, 1)


def test_a_deleted_keyword_choice_is_reported_not_applied(db, fid):
    pid = _photo(db, fid, "new.jpg", RIVER[0])
    summary = _summary()
    apply_location_choices(
        db, [pid],
        parse_location_choices([{"cells": [cell_key(*RIVER[0])], "kind": "keyword", "keyword_id": 999}]),
        api_key="", language="en", summary=summary, cancel_check=_never,
    )
    assert summary["locations_unresolved"] == 1
    assert "no longer exists" in summary["errors"][0]


# -- the stream and route ----------------------------------------------------------------


@pytest.fixture
def exiftool_present(monkeypatch):
    import metadata
    monkeypatch.setattr(metadata, "exiftool_available", lambda: True)


def test_stream_reports_progress_then_the_suggestions(db, monkeypatch, exiftool_present):
    monkeypatch.setattr(import_locations, "_GPS_READ_CHUNK", 2)
    monkeypatch.setattr(import_locations, "read_source_gps",
                        lambda paths: {p: RIVER[0] for p in paths if "nogps" not in p})
    monkeypatch.setattr(places, "reverse_geocode", lambda *a, **k: None)
    paths = ["/card/a.NEF", "/card/b.NEF", "/card/nogps.NEF", "/card/a.NEF"]

    frames = [json.loads(f[len("data: "):]) for f in
              import_locations.stream_location_suggestions(db._db_path, paths, {})]

    assert frames[:4] == [{"reading": 0, "total": 3}, {"reading": 2, "total": 3},
                          {"reading": 3, "total": 3}, {"naming": 1, "total": 1}]
    done = frames[-1]
    assert done["done"] is True
    assert done["no_gps_paths"] == ["/card/nogps.NEF"]
    assert done["file_cells"] == {"/card/a.NEF": cell_key(*RIVER[0]),
                                  "/card/b.NEF": cell_key(*RIVER[0])}
    assert done["groups"][0]["unresolved_reasons"] == {"no_api_key": 1}
    assert "location_options" in done


def test_closing_the_stream_stops_google_lookups_between_cells(db, monkeypatch, exiftool_present):
    """A superseded preview must not keep spending Google quota."""
    points = [RIVER[0], FAR_AWAY, (51.5, -0.12)]
    monkeypatch.setattr(import_locations, "read_source_gps",
                        lambda paths: {p: points[i] for i, p in enumerate(paths)})
    lookups = []

    def fake_reverse_geocode(lat, lng, api_key, *, language="en"):
        lookups.append((lat, lng))

    monkeypatch.setattr(places, "reverse_geocode", fake_reverse_geocode)
    stream = import_locations.stream_location_suggestions(
        db._db_path, ["/card/a.NEF", "/card/b.NEF", "/card/c.NEF"],
        {"google_maps_api_key": "KEY"})
    for line in stream:
        if json.loads(line[len("data: "):]).get("naming") == 1:
            break
    stream.close()

    assert len(lookups) == 1


def test_stream_says_when_exiftool_is_missing(db, monkeypatch):
    """A missing ExifTool returns no metadata; that must not read as "no GPS"."""
    import metadata
    monkeypatch.setattr(metadata, "exiftool_available", lambda: False)

    frames = [json.loads(f[len("data: "):]) for f in
              import_locations.stream_location_suggestions(db._db_path, ["/card/a.NEF"], {})]

    assert frames == [{"done": True,
                       "error": "ExifTool isn't installed, so Vireo can't read the photos' GPS."}]


@pytest.mark.parametrize("body", [{}, {"paths": []}, {"paths": ["ok", 3]}, {"paths": "x"}])
def test_location_suggestions_route_rejects_bad_paths(app_and_db, body):
    app, _ = app_and_db
    resp = app.test_client().post("/api/import/location-suggestions", json=body)
    assert resp.status_code == 400


def test_location_suggestions_route_streams(app_and_db, monkeypatch, exiftool_present):
    app, _ = app_and_db
    monkeypatch.setattr(import_locations, "read_source_gps", lambda paths: {})
    resp = app.test_client().post("/api/import/location-suggestions",
                                  json={"paths": ["/card/a.NEF"]})
    assert resp.status_code == 200
    assert resp.mimetype == "text/event-stream"
    frames = [json.loads(line[len("data: "):]) for line in
              resp.get_data(as_text=True).split("\n\n") if line]
    assert frames[-1]["no_gps_paths"] == ["/card/a.NEF"]
    assert frames[-1]["groups"] == []


def test_cache_rows_record_the_resolver():
    payload = json.loads(encode_cached_reverse_geocode({"place_id": "p"}, "en"))
    assert payload["_vireo_place_resolver"] == REVERSE_GEOCODE_CACHE_RESOLVER
    assert payload["_vireo_result_language"] == "en"


def test_picker_labels_are_globally_unique_stable_and_reserve_literal_names():
    rows = []

    def add(kid, name, parent=None):
        rows.append({"id": kid, "name": name, "parent_id": parent})

    # The first distinction is deeper than the old three-parent limit.
    for offset, root_name in [(0, "North"), (10, "South")]:
        add(offset + 1, "Lake", offset + 2)
        add(offset + 2, "Region", offset + 3)
        add(offset + 3, "County", offset + 4)
        add(offset + 4, "State", offset + 5)
        add(offset + 5, root_name)
    # Identical complete ancestry needs stable IDs, even if a literal name
    # already occupies the first proposed ID-qualified label.
    add(20, "Exact", 21)
    add(21, "Root")
    add(22, "EXACT", 21)
    add(23, "Exact · Root [#20]")
    add(24, "Exact · Root (#20)")
    # Another option's raw name can collide with a breadcrumb.
    add(30, "Beach", 31)
    add(31, "City")
    add(32, "Beach", 33)
    add(33, "Other")
    add(34, "Beach · City")
    db = SimpleNamespace(locations=SimpleNamespace(location_keyword_rows=lambda: rows))
    first = {o["id"]: o["label"] for o in location_options(db)}
    assert len({label.casefold() for label in first.values()}) == len(rows)
    assert first[1].endswith("North") and first[11].endswith("South")
    assert "20" in first[20] and "22" in first[22]
    rows.reverse()
    assert {o["id"]: o["label"] for o in location_options(db)} == first
