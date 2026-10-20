"""Capture evidence and location precedence for rendered export metadata."""

import pytest
from export_metadata import export_metadata_args, resolve_capture, timestamp_parts


def _source(**overrides):
    return {"EXIF": {"DateTimeOriginal": "2026:10:17 09:27:50",
                     "SubSecTimeOriginal": "24", "OffsetTimeOriginal": "-07:00", **overrides}}


@pytest.mark.parametrize(("timestamp", "metadata", "offset", "status"), [
    ("2026-10-17T09:27:50.240000", _source(), "-07:00", "source_exif"),
    ("2026-10-17T09:27:50.240000+03:00", _source(), "+03:00", "catalog"),
    ("2026-10-17T10:27:50.240000", _source(), None, "unavailable:timestamp_changed"),
    ("2026-10-17T09:27:50.250000", _source(), None, "unavailable:timestamp_changed"),
    ("2026-10-17T09:27:50", _source(), None, "unavailable:timestamp_changed"),
    ("2026-10-17T09:27:50.240000", _source(OffsetTimeOriginal=None), None, "unavailable:missing_source_offset"),
    ("2026-10-17T09:27:50.240000", _source(OffsetTimeOriginal="-99:00"), None, "unavailable:invalid_source_offset"),
    ("2026-10-17T09:27:50.240000", {}, None, "unavailable:invalid_or_missing_source_time"),
    ("2026-10-17T09:27:50.240000", "broken JSON", None, "unavailable:invalid_or_missing_source_time"),
])
def test_capture_offset_requires_matching_capture(timestamp, metadata, offset, status):
    from export_metadata import metadata_object

    report = {}
    parts = resolve_capture(timestamp, metadata_object(metadata), report)
    assert parts[5] == offset
    assert report["capture_offset"] == status
    assert parts[4] == ("240000" if ".240000" in timestamp else "250000" if ".250000" in timestamp else None)


def test_capture_rejects_conflicting_matching_xmp_offset():
    metadata = _source()
    metadata["XMP"] = {"DateTimeOriginal": "2026:10:17 09:27:50.24+02:00"}
    report = {}
    assert resolve_capture("2026-10-17T09:27:50.240000", metadata, report)[5] is None
    assert report["capture_offset"] == "unavailable:conflicting_source_offsets"


def test_capture_digitized_fallback_uses_its_own_fraction_and_offset():
    metadata = {"EXIF": {"CreateDate": "2026:10:17 09:27:50", "SubSecTimeDigitized": "24",
                         "OffsetTimeDigitized": "-07:00", "OffsetTimeOriginal": "+03:00"}}
    report = {}
    assert resolve_capture("2026-10-17T09:27:50.240000", metadata, report)[5] == "-07:00"
    assert report["capture_offset"] == "source_exif_digitized"


def test_capture_original_offset_has_precedence_over_digitized_offset():
    report = {}
    assert resolve_capture("2026-10-17T09:27:50.24", _source(OffsetTimeDigitized="+03:00"), report)[5] == "-07:00"


def test_source_inline_fraction_cannot_disagree_with_separate_fraction():
    report = {}
    metadata = _source(DateTimeOriginal="2026:10:17 09:27:50.25")
    assert resolve_capture("2026-10-17T09:27:50.25", metadata, report)[5] is None
    assert report["capture_offset"] == "unavailable:conflicting_source_subseconds"


def test_inline_offset_and_separate_fraction_can_be_combined():
    report = {}
    metadata = _source(DateTimeOriginal="2026:10:17 09:27:50-07:00")
    assert resolve_capture("2026-10-17T09:27:50.240000", metadata, report)[5] == "-07:00"


def test_matching_xmp_can_supply_missing_offset_with_accurate_provenance():
    metadata = _source(OffsetTimeOriginal=None)
    metadata["XMP"] = {"DateTimeOriginal": "2026:10:17 09:27:50.24-07:00"}
    report = {}
    assert resolve_capture("2026-10-17T09:27:50.240000", metadata, report)[5] == "-07:00"
    assert report["capture_offset"] == "source_xmp"


def test_source_subsecond_beyond_microseconds_matches_truncated_catalog():
    """The scanner truncates fractions beyond six digits, so a source
    `.1234567` must not be read as a changed time against the catalog's
    `.123456` written from the same capture."""
    report = {}
    metadata = _source(SubSecTimeOriginal="1234567")
    parts = resolve_capture("2026-10-17T09:27:50.123456", metadata, report)
    assert parts[5] == "-07:00"
    assert report["capture_offset"] == "source_exif"


@pytest.mark.parametrize("timestamp", ["2026-02-30T09:27:50", "2026-10-17T29:27:50", "2026-10-17T09:27:50+03:99"])
def test_invalid_catalog_time_is_not_exported(timestamp):
    assert timestamp_parts(timestamp) == (None,) * 6


@pytest.mark.parametrize("selected", [[], ["rating"], ["capture_date"], ["capture_time"], ["camera"], ["species"]])
def test_metadata_exclusions_are_respected(selected):
    args = export_metadata_args(selected, {"timestamp": "2026-10-17T09:27:50.24", "rating": 5}, ["Bird"], {},
                                {"latitude": 1, "longitude": 2, "hierarchy": ["Country", "Park"]},
                                source_metadata=_source())
    assert not any("GPS" in arg or "Location" in arg or "HierarchicalSubject" in arg for arg in args)
    if "capture_time" not in selected:
        assert not any("09:27:50" in arg or "Offset" in arg or "SubSec" in arg for arg in args)
    if "capture_date" not in selected:
        assert not any("2026" in arg for arg in args)


def test_unknown_offset_does_not_allow_exiftool_to_invent_host_timezone():
    report = {}
    args = export_metadata_args(["capture_date", "capture_time"], {"timestamp": "2026-10-17T09:27:50.24"}, [], {},
                                source_metadata=_source(OffsetTimeOriginal=None), report=report)
    assert "-EXIF:DateTimeOriginal=2026:10:17 09:27:50" in args
    assert "-XMP-exif:DateTimeOriginal=2026-10-17T09:27:50.24" in args
    assert not any("IPTC:TimeCreated" in arg or "OffsetTime" in arg for arg in args)
    assert report["iptc_capture_time"] == "unavailable:requires_capture_offset"


def test_location_source_fields_must_match_current_hierarchy():
    report = {}
    args = export_metadata_args(["location"], {}, [], {},
                                {"hierarchy": ["United States", "California", "San Diego", "New Park"]},
                                source_metadata={"XMP": {"City": "San Diego", "State": "California", "Country": "United States",
                                                         "Location": "Old Park"}}, report=report)
    assert "-XMP-iptcCore:Location=New Park" in args
    assert "-XMP-photoshop:City=San Diego" in args
    assert "-XMP-photoshop:State=California" in args
    assert "-XMP-photoshop:Country=United States" in args
    assert all("Old Park" not in arg for arg in args)
    assert report["location_coordinates"] == "unavailable:missing_coordinates"


def test_cleared_location_does_not_restore_source_place_or_gps():
    report = {}
    args = export_metadata_args(["location"], {}, [], {}, {},
                                source_metadata={"EXIF": {"GPSLatitude": 10, "GPSLongitude": 20},
                                                 "XMP": {"Location": "Old Park", "City": "Old City"}}, report=report)
    assert args == []
    assert report["location_hierarchy"] == "unavailable:missing_hierarchy"


def test_untyped_hierarchy_does_not_guess_geographic_levels():
    report = {}
    args = export_metadata_args(["location"], {}, [], {}, {"hierarchy": ["Region", "County", "Park"]}, report=report)
    assert "-XMP-lr:HierarchicalSubject=Region|County|Park" in args
    assert "-XMP-iptcCore:Location=Park" in args
    assert not any(":City=" in arg or ":State=" in arg or ":Country=" in arg for arg in args)
    assert report["location_city"] == "unavailable:untyped_or_missing"


@pytest.mark.parametrize("details", [
    {"place_id": "different", "name": "Park"},
    {"place_id": "park", "name": "Former Park Name"},
])
def test_stale_place_cache_cannot_supply_geographic_labels(details):
    details["address_components"] = [{"name": "City", "types": ["locality"]}]
    args = export_metadata_args(["location"], {}, [], {},
                                {"hierarchy": ["City", "Park"], "place_id": "park", "place_details": details})
    assert not any(":City=" in arg for arg in args)


def test_conflicting_source_place_labels_are_unavailable():
    report = {}
    args = export_metadata_args(["location"], {}, [], {}, {"hierarchy": ["City", "Park"]},
                                source_metadata={"XMP": {"City": "City"}, "IPTC": {"City": "Other City"}}, report=report)
    assert not any(":City=" in arg for arg in args)
    assert report["location_city"] == "unavailable:untyped_or_missing"


@pytest.mark.parametrize(("selected", "camera", "species", "rating", "expected"), [
    (["species"], {}, ["Bird"], None, {"species_keywords": "catalog"}),
    (["species"], {}, [], None, {"species_keywords": "unavailable:no_species"}),
    (["rating"], {}, [], 5, {"rating": "catalog"}),
    (["rating"], {}, [], None, {"rating": "unavailable:no_rating"}),
    (["camera"], {"camera_make": "Nikon"}, [], None, {"camera": "catalog"}),
    (["camera"], {}, [], None, {"camera": "unavailable:no_camera_data"}),
])
def test_catalog_only_categories_report_presence_or_unavailability(selected, camera, species, rating, expected):
    """Selected species/rating/camera report provenance even without multi-source precedence."""
    report = {}
    export_metadata_args(selected, {"rating": rating}, species, camera, source_metadata=None, report=report)
    for key, value in expected.items():
        assert report.get(key) == value
