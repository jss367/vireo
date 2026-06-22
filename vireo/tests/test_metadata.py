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


def test_extract_metadata_retries_failed_batches(monkeypatch):
    """A failed ExifTool batch should be split so good files still get metadata."""
    import metadata

    paths = [f"/photos/img{i}.jpg" for i in range(4)]
    calls = []

    def fake_run(file_paths, extra_args=None):
        calls.append(list(file_paths))
        if len(file_paths) > 1:
            return metadata._TIMEOUT
        path = file_paths[0]
        return [{"SourceFile": path, "EXIF:Make": "TestCam"}]

    monkeypatch.setattr(metadata, "_run_exiftool", fake_run)

    results = metadata.extract_metadata(paths)

    assert set(results) == set(paths)
    assert all(results[p]["EXIF"]["Make"] == "TestCam" for p in paths)
    assert calls == [
        paths,
        paths[:2],
        paths[:1],
        paths[1:2],
        paths[2:],
        paths[2:3],
        paths[3:],
    ]


def test_extract_metadata_does_not_retry_permanent_failures(monkeypatch):
    """Permanent ExifTool failures should not split into many retries."""
    import metadata

    paths = [f"/photos/img{i}.jpg" for i in range(4)]
    calls = []

    def fake_run(file_paths, extra_args=None):
        calls.append(list(file_paths))
        return []

    monkeypatch.setattr(metadata, "_run_exiftool", fake_run)

    assert metadata.extract_metadata(paths) == {}
    assert calls == [paths]


def test_extract_metadata_timeout_retries_isolate_slow_file(monkeypatch):
    """Timed-out ExifTool batches split until a slow file is isolated."""
    import metadata

    bad_path = "/photos/bad.jpg"
    paths = [f"/photos/img{i}.jpg" for i in range(7)] + [bad_path]
    calls = []

    def fake_run(file_paths, extra_args=None):
        calls.append(list(file_paths))
        if bad_path in file_paths:
            return metadata._TIMEOUT
        return [
            {"SourceFile": path, "EXIF:Make": "TestCam"}
            for path in file_paths
        ]

    monkeypatch.setattr(metadata, "_run_exiftool", fake_run)

    results = metadata.extract_metadata(paths)

    assert set(results) == set(paths) - {bad_path}
    assert bad_path not in results
    assert [bad_path] in calls
    assert all(results[p]["EXIF"]["Make"] == "TestCam" for p in results)


def test_extract_metadata_timeout_retries_are_bounded(monkeypatch):
    """Repeated ExifTool timeouts stop after the configured split budget."""
    import metadata

    paths = [f"/photos/img{i}.jpg" for i in range(8)]
    calls = []

    def fake_run(file_paths, extra_args=None):
        calls.append(list(file_paths))
        return metadata._TIMEOUT

    monkeypatch.setattr(metadata, "_MAX_TIMEOUT_SPLIT_ATTEMPTS", 3)
    monkeypatch.setattr(metadata, "_run_exiftool", fake_run)

    assert metadata.extract_metadata(paths) == {}
    assert calls == [
        paths,
        paths[:4],
        paths[:2],
        paths[:1],
        paths[1:2],
        paths[2:4],
        paths[4:],
    ]


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


# ---- exiftool presence detection (exiftool_available / exiftool_status) ----


def test_exiftool_available_requires_runnable_binary(monkeypatch):
    """exiftool_available is True only when PATH resolves AND the probe runs."""
    import metadata

    class _Ok:
        returncode = 0
        stdout = "12.76\n"

    monkeypatch.setattr(metadata.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(metadata.subprocess, "run", lambda *a, **k: _Ok())
    assert metadata.exiftool_available() is True

    monkeypatch.setattr(metadata.shutil, "which", lambda name: None)
    assert metadata.exiftool_available() is False


def test_exiftool_available_false_for_broken_install(monkeypatch):
    """A PATH hit with a failing -ver probe is treated as unavailable.

    Without this, scans would silently lose metadata while the UI rendered
    a green "installed" state — the exact black box ``CORE_PHILOSOPHY``
    forbids and Codex flagged on PR #1017.
    """
    import metadata

    monkeypatch.setattr(metadata.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(metadata.subprocess, "run",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("broken perl")))
    assert metadata.exiftool_available() is False


def test_exiftool_status_missing(monkeypatch):
    """When exiftool is absent, status reports unavailable with an install hint."""
    import metadata

    monkeypatch.setattr(metadata.shutil, "which", lambda name: None)
    status = metadata.exiftool_status()

    assert status["available"] is False
    assert status["path"] == ""
    assert status["version"] is None
    assert status["hint"]  # platform-appropriate, non-empty


def test_exiftool_status_present(monkeypatch):
    """When exiftool resolves, status reports the path and parsed version."""
    import metadata

    monkeypatch.setattr(metadata.shutil, "which", lambda name: "/usr/bin/exiftool")

    class _Result:
        returncode = 0
        stdout = "12.76\n"

    monkeypatch.setattr(
        metadata.subprocess, "run", lambda *a, **k: _Result(),
    )
    status = metadata.exiftool_status()

    assert status["available"] is True
    assert status["path"] == "/usr/bin/exiftool"
    assert status["version"] == "12.76"


def test_exiftool_status_present_but_unrunnable(monkeypatch):
    """A resolvable-but-broken binary is reported unavailable with the path kept.

    Keeping the path lets the UI distinguish "not installed" from "broken
    install" so the user can act on it.
    """
    import metadata

    monkeypatch.setattr(metadata.shutil, "which", lambda name: "/usr/bin/exiftool")

    def _boom(*a, **k):
        raise OSError("broken perl")

    monkeypatch.setattr(metadata.subprocess, "run", _boom)
    status = metadata.exiftool_status()

    assert status["available"] is False
    assert status["path"] == "/usr/bin/exiftool"
    assert status["version"] is None
    assert status["error"] and "broken perl" in status["error"]
    assert "/usr/bin/exiftool" in status["hint"]


def test_scan_metadata_warning_silent_when_available(monkeypatch):
    """scan_metadata_warning returns None when exiftool runs cleanly."""
    import metadata

    class _Ok:
        returncode = 0
        stdout = "12.76\n"

    monkeypatch.setattr(metadata.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(metadata.subprocess, "run", lambda *a, **k: _Ok())
    assert metadata.scan_metadata_warning() is None


def test_scan_metadata_warning_warns_when_missing(monkeypatch):
    """scan_metadata_warning returns a user-facing string when exiftool is gone."""
    import metadata

    monkeypatch.setattr(metadata.shutil, "which", lambda name: None)
    warning = metadata.scan_metadata_warning()
    assert warning is not None
    assert "ExifTool" in warning
    assert "capture date" in warning.lower()


def test_scan_metadata_warning_warns_for_broken_install(monkeypatch):
    """A broken (PATH-resolvable but unrunnable) install still triggers the warning."""
    import metadata

    monkeypatch.setattr(metadata.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(
        metadata.subprocess, "run",
        lambda *a, **k: (_ for _ in ()).throw(OSError("broken perl")),
    )
    warning = metadata.scan_metadata_warning()
    assert warning is not None
    assert "ExifTool" in warning


def test_exiftool_status_present_but_nonzero_returncode(monkeypatch):
    """A returncode != 0 from -ver also marks the install unavailable."""
    import metadata

    monkeypatch.setattr(metadata.shutil, "which", lambda name: "/usr/bin/exiftool")

    class _Bad:
        returncode = 2
        stdout = ""
        stderr = "Can't locate Image/ExifTool.pm"

    monkeypatch.setattr(metadata.subprocess, "run", lambda *a, **k: _Bad())
    status = metadata.exiftool_status()

    assert status["available"] is False
    assert status["path"] == "/usr/bin/exiftool"
    assert status["version"] is None
    assert "Can't locate Image/ExifTool.pm" in status["error"]
