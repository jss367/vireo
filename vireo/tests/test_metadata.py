"""Tests for vireo/metadata.py — ExifTool wrapper."""

import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image
from PIL.ExifTags import IFD, Base

requires_exiftool = pytest.mark.skipif(
    shutil.which("exiftool") is None,
    reason="exiftool not installed",
)


def _create_jpg_with_exif(path):
    """Create a JPEG with EXIF data via PIL's native Exif support."""
    img = Image.new('RGB', (200, 100), color='green')
    exif = img.getexif()
    exif[Base.Make] = 'TestCam'
    exif[Base.Model] = 'Model X'

    ifd = exif.get_ifd(IFD.Exif)
    ifd[33437] = 5.6        # FNumber
    ifd[33434] = 0.0005     # ExposureTime
    ifd[34855] = 3200       # ISOSpeedRatings
    ifd[37386] = 450.0      # FocalLength
    ifd[36867] = '2026:03:15 08:30:00'  # DateTimeOriginal

    img.save(path, exif=exif.tobytes())


def _create_plain_jpg(path):
    """Create a minimal JPEG with no EXIF data."""
    img = Image.new('RGB', (200, 100), color='blue')
    img.save(path)


@requires_exiftool
def test_extract_metadata_single_file(tmp_path):
    """extract_metadata returns grouped tag dict for a single file."""
    from metadata import extract_metadata

    img_path = str(tmp_path / "test.jpg")
    _create_jpg_with_exif(img_path)

    results = extract_metadata([img_path])
    assert len(results) == 1

    meta = results[img_path]
    assert isinstance(meta, dict)
    # Should have group keys like EXIF, File, etc.
    assert "EXIF" in meta
    assert meta["EXIF"]["Make"] == "TestCam"
    assert meta["EXIF"]["Model"] == "Model X"
    assert meta["EXIF"]["FocalLength"] == 450
    assert meta["EXIF"]["FNumber"] == 5.6
    assert meta["EXIF"]["ISO"] == 3200
    assert meta["EXIF"]["DateTimeOriginal"] == "2026:03:15 08:30:00"

    # File group should always be present
    assert "File" in meta
    assert meta["File"]["FileType"] == "JPEG"
    assert meta["File"]["ImageWidth"] == 200
    assert meta["File"]["ImageHeight"] == 100


@requires_exiftool
def test_extract_metadata_returns_empty_for_missing_file(tmp_path):
    """extract_metadata returns empty dict for nonexistent file."""
    from metadata import extract_metadata

    results = extract_metadata([str(tmp_path / "nope.jpg")])
    # ExifTool may skip missing files or return an error entry —
    # either way, the missing file should not appear with useful data
    missing = str(tmp_path / "nope.jpg")
    assert results.get(missing) is None or results == {}


@requires_exiftool
def test_extract_metadata_batch(tmp_path):
    """extract_metadata handles multiple files in one call."""
    from metadata import extract_metadata

    paths = []
    for i in range(3):
        p = str(tmp_path / f"img{i}.jpg")
        _create_jpg_with_exif(p)
        paths.append(p)

    results = extract_metadata(paths)
    assert len(results) == 3
    for p in paths:
        assert "EXIF" in results[p]
        assert results[p]["EXIF"]["Make"] == "TestCam"


def test_extract_metadata_empty_list():
    """extract_metadata with empty list returns empty dict."""
    from metadata import extract_metadata
    assert extract_metadata([]) == {}


@requires_exiftool
def test_extract_metadata_with_restricted_tags(tmp_path):
    """extract_metadata can restrict which tags are returned."""
    from metadata import extract_metadata

    img_path = str(tmp_path / "test.jpg")
    _create_jpg_with_exif(img_path)

    results = extract_metadata([img_path], restricted_tags=["-Make", "-Model"])
    meta = results[img_path]
    assert "EXIF" in meta
    assert meta["EXIF"]["Make"] == "TestCam"
    assert meta["EXIF"]["Model"] == "Model X"
    # Should not have other EXIF fields like FocalLength
    assert "FocalLength" not in meta.get("EXIF", {})


def test_extract_summary_fields_full():
    """extract_summary_fields pulls the key fields from grouped metadata."""
    from metadata import extract_summary_fields

    meta = {
        "EXIF": {
            "Make": "Canon",
            "Model": "EOS R5",
            "FocalLength": 450.0,
            "FNumber": 5.6,
            "ExposureTime": 0.0005,
            "ISO": 3200,
            "DateTimeOriginal": "2026:03:15 08:30:00",
        },
        "Composite": {
            "LensID": "RF 100-500mm F4.5-7.1 L IS USM",
        },
    }

    summary = extract_summary_fields(meta)
    assert summary["camera_make"] == "Canon"
    assert summary["camera_model"] == "EOS R5"
    assert summary["lens"] == "RF 100-500mm F4.5-7.1 L IS USM"
    assert summary["focal_length"] == 450.0
    assert summary["f_number"] == 5.6
    assert summary["exposure_time"] == 0.0005
    assert summary["iso"] == 3200
    assert summary["datetime_original"] == "2026:03:15 08:30:00"


def test_extract_summary_fields_missing_data():
    """extract_summary_fields handles missing groups/tags gracefully."""
    from metadata import extract_summary_fields

    summary = extract_summary_fields({})
    assert summary["camera_make"] is None
    assert summary["camera_model"] is None
    assert summary["lens"] is None
    assert summary["focal_length"] is None
    assert summary["f_number"] is None
    assert summary["exposure_time"] is None
    assert summary["iso"] is None
    assert summary["datetime_original"] is None


def test_extract_summary_fields_lens_fallback():
    """extract_summary_fields falls back through LensID -> LensModel -> Lens."""
    from metadata import extract_summary_fields

    # Fallback to EXIF LensModel
    meta1 = {"EXIF": {"LensModel": "EF 100-400mm"}}
    assert extract_summary_fields(meta1)["lens"] == "EF 100-400mm"

    # Fallback to Composite Lens
    meta2 = {"Composite": {"Lens": "24-70mm f/2.8"}}
    assert extract_summary_fields(meta2)["lens"] == "24-70mm f/2.8"

    # LensID takes priority
    meta3 = {
        "EXIF": {"LensModel": "EF 100-400mm"},
        "Composite": {"LensID": "RF 100-500mm"},
    }
    assert extract_summary_fields(meta3)["lens"] == "RF 100-500mm"


def test_group_tags_separates_by_group():
    """_group_tags converts flat Group:Tag keys to nested dicts."""
    from metadata import _group_tags

    flat = {
        "SourceFile": "/tmp/test.jpg",
        "EXIF:Make": "Canon",
        "EXIF:Model": "EOS R5",
        "File:FileType": "JPEG",
        "File:ImageWidth": 8192,
        "Composite:ImageSize": "8192 5464",
    }

    grouped = _group_tags(flat)
    assert "EXIF" in grouped
    assert grouped["EXIF"]["Make"] == "Canon"
    assert grouped["EXIF"]["Model"] == "EOS R5"
    assert grouped["File"]["FileType"] == "JPEG"
    assert grouped["File"]["ImageWidth"] == 8192
    assert grouped["Composite"]["ImageSize"] == "8192 5464"
    # SourceFile should be excluded
    assert "SourceFile" not in grouped.get("_meta", {})


def test_group_tags_ungrouped_keys_go_to_meta():
    """_group_tags puts keys without a colon into the _meta group."""
    from metadata import _group_tags

    flat = {
        "SourceFile": "/tmp/test.jpg",
        "Warning": "some warning",
        "EXIF:Make": "Canon",
    }

    grouped = _group_tags(flat)
    assert grouped["_meta"]["Warning"] == "some warning"
    assert grouped["EXIF"]["Make"] == "Canon"


@requires_exiftool
def test_extract_metadata_integration_with_summary(tmp_path):
    """End-to-end: extract_metadata then extract_summary_fields."""
    from metadata import extract_metadata, extract_summary_fields

    img_path = str(tmp_path / "test.jpg")
    _create_jpg_with_exif(img_path)

    results = extract_metadata([img_path])
    summary = extract_summary_fields(results[img_path])

    assert summary["camera_make"] == "TestCam"
    assert summary["camera_model"] == "Model X"
    assert summary["focal_length"] == 450
    assert summary["f_number"] == 5.6
    assert summary["exposure_time"] == 0.0005
    assert summary["iso"] == 3200
    assert summary["datetime_original"] == "2026:03:15 08:30:00"
