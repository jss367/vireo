import json
import os
import shutil
import stat
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from db import Database
from inat_export import (
    _destination_name_limit,
    _exif_datetime,
    _metadata_args,
    _reserve_destination,
    export_inat_photo,
    write_inat_metadata,
)
from metadata import _exiftool_command, find_exiftool
from PIL import Image
from services import photo_ingest, workspace_folders


def test_exif_datetime_preserves_time_and_offset():
    assert _exif_datetime("2026-08-18T14:03:22-0700") == (
        "2026:08:18 14:03:22",
        "-07:00",
    )


def test_exif_datetime_accepts_date_only():
    assert _exif_datetime("2026-08-18") == (
        "2026:08:18 00:00:00",
        None,
    )


def test_metadata_args_include_selected_inaturalist_fields():
    args = _metadata_args({
        "taxon_name": "Cardinalis cardinalis",
        "timestamp": "2024-06-01T10:11:12",
        "latitude": 38.9,
        "longitude": -77.0,
        "description": "At the feeder",
    })

    assert args[0] == "-all="
    assert "-XMP-dc:Title=Cardinalis cardinalis" in args
    assert "-XMP-dc:Subject=Cardinalis cardinalis" in args
    assert "-EXIF:DateTimeOriginal=2024:06:01 10:11:12" in args
    assert "-EXIF:GPSLatitude=38.90000000" in args
    assert "-EXIF:GPSLatitudeRef=N" in args
    assert "-EXIF:GPSLongitude=77.00000000" in args
    assert "-EXIF:GPSLongitudeRef=W" in args
    assert "-XMP-dc:Description=At the feeder" in args


def test_metadata_args_strip_unselected_location_and_date():
    args = _metadata_args({"taxon_name": "Corvus corax"})

    assert args[0] == "-all="
    assert not any("GPS" in arg for arg in args)
    assert not any("DateTimeOriginal" in arg for arg in args)


def test_metadata_args_date_only_does_not_invent_capture_time():
    args = _metadata_args({"timestamp": "2024-06-01"})

    assert "-IPTC:DateCreated=2024:06:01" in args
    assert not any("DateTimeOriginal" in arg for arg in args)
    assert not any("CreateDate=2024:06:01 00:00:00" in arg for arg in args)


def test_reserve_destination_is_atomic_across_concurrent_exports(tmp_path):
    requested = str(tmp_path / "bird-iNaturalist.jpg")

    with ThreadPoolExecutor(max_workers=8) as pool:
        reserved = list(pool.map(lambda _index: _reserve_destination(requested), range(8)))

    assert len(set(reserved)) == 8
    assert {os.path.basename(path) for path in reserved} == {
        "bird-iNaturalist.jpg",
        *(f"bird-iNaturalist_{index}.jpg" for index in range(2, 9)),
    }
    assert all(os.path.isfile(path) for path in reserved)


def test_reserve_destination_bounds_utf8_name_and_preserves_suffix(tmp_path):
    requested = str(tmp_path / ("é" * 20 + "-iNaturalist.jpg"))

    with patch("inat_export._destination_name_limit", return_value=24):
        first = _reserve_destination(
            requested, preserve_suffix="-iNaturalist",
        )
        second = _reserve_destination(
            requested, preserve_suffix="-iNaturalist",
        )

    assert len(os.path.basename(first).encode("utf-8")) <= 24
    assert len(os.path.basename(second).encode("utf-8")) <= 24
    assert first.endswith("-iNaturalist.jpg")
    assert second.endswith("-iNaturalist_2.jpg")


def test_write_inat_metadata_replaces_existing_metadata(tmp_path):
    path = tmp_path / "export.jpg"
    path.write_bytes(b"jpeg")
    completed = MagicMock(returncode=0, stdout="1 image files updated", stderr="")

    with (
        patch("inat_export.find_exiftool", return_value="/tools/exiftool"),
        patch("inat_export._exiftool_command", return_value=["/tools/exiftool"]),
        patch("inat_export.subprocess.run", return_value=completed) as run,
    ):
        write_inat_metadata(str(path), {"taxon_name": "Corvus corax"})

    command = run.call_args.args[0]
    assert command[:3] == ["/tools/exiftool", "-overwrite_original", "-all="]
    assert command[-2:] == ["--", str(path)]


@pytest.mark.skipif(find_exiftool() is None, reason="ExifTool not installed")
def test_write_inat_metadata_round_trips_selected_fields(tmp_path):
    path = tmp_path / "export.jpg"
    Image.new("RGB", (16, 16), (20, 80, 160)).save(path)

    write_inat_metadata(str(path), {
        "taxon_name": "Corvus corax",
        "timestamp": "2026-08-18T14:03:22-07:00",
        "latitude": 47.61,
        "longitude": -122.33,
    })
    result = subprocess.run(
        [
            *_exiftool_command(find_exiftool()),
            "-j",
            "-n",
            "-XMP-dc:Title",
            "-EXIF:DateTimeOriginal",
            "-EXIF:GPSLatitude",
            "-EXIF:GPSLatitudeRef",
            "-EXIF:GPSLongitude",
            "-EXIF:GPSLongitudeRef",
            "--",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(result.stdout)[0]

    assert metadata["Title"] == "Corvus corax"
    assert metadata["DateTimeOriginal"] == "2026:08:18 14:03:22"
    assert metadata["GPSLatitude"] == pytest.approx(47.61)
    assert metadata["GPSLatitudeRef"] == "N"
    assert metadata["GPSLongitude"] == pytest.approx(122.33)
    assert metadata["GPSLongitudeRef"] == "W"


def test_export_inat_photo_renders_separate_edited_jpeg(tmp_path):
    db = Database(str(tmp_path / "vireo.db"))
    workspace_id = db.ensure_default_workspace()
    db.set_active_workspace(workspace_id)
    source_dir = tmp_path / "photos"
    source_dir.mkdir()
    source = source_dir / "cardinal.jpg"
    Image.new("RGB", (32, 24), (190, 30, 20)).save(source)
    source_before = source.read_bytes()
    folder_id = workspace_folders.add_folder(db, str(source_dir), name="photos")
    photo_id = photo_ingest.add_photo(
        db, folder_id=folder_id,
        filename=source.name,
        extension=".jpg",
        file_size=source.stat().st_size,
        file_mtime=source.stat().st_mtime,
        timestamp="2024-06-01T10:00:00",
    )
    destination = tmp_path / "exports"

    with patch("inat_export.write_inat_metadata") as write_metadata:
        output = export_inat_photo(
            db,
            str(tmp_path / "cache"),
            photo_id,
            str(destination),
            {"taxon_name": "Cardinalis cardinalis"},
        )

    assert output == str(destination / "cardinal-iNaturalist.jpg")
    assert os.path.isfile(output)
    assert source.read_bytes() == source_before
    write_metadata.assert_called_once()
    metadata_path, metadata = write_metadata.call_args.args
    assert metadata_path != str(source)
    assert metadata == {"taxon_name": "Cardinalis cardinalis"}


def test_export_inat_photo_uses_bounded_staging_filename(tmp_path):
    db = Database(str(tmp_path / "vireo.db"))
    workspace_id = db.ensure_default_workspace()
    db.set_active_workspace(workspace_id)
    source_dir = tmp_path / "photos"
    source_dir.mkdir()
    long_stem = "b" * 240
    source = source_dir / f"{long_stem}.jpg"
    Image.new("RGB", (32, 24), (190, 30, 20)).save(source)
    folder_id = workspace_folders.add_folder(db, str(source_dir), name="photos")
    photo_id = photo_ingest.add_photo(
        db, folder_id=folder_id,
        filename=source.name,
        extension=".jpg",
        file_size=source.stat().st_size,
        file_mtime=source.stat().st_mtime,
        timestamp="2024-06-01T10:00:00",
    )
    destination = tmp_path / "exports"

    with patch("inat_export.write_inat_metadata"):
        first_output = export_inat_photo(
            db,
            str(tmp_path / "cache"),
            photo_id,
            str(destination),
            {},
        )
        second_output = export_inat_photo(
            db,
            str(tmp_path / "cache"),
            photo_id,
            str(destination),
            {},
        )

    first_name = os.path.basename(first_output)
    second_name = os.path.basename(second_output)
    assert first_name.endswith("-iNaturalist.jpg")
    assert second_name.endswith("-iNaturalist_2.jpg")
    name_limit = _destination_name_limit(str(destination))
    assert len(first_name.encode("utf-8")) <= name_limit
    assert len(second_name.encode("utf-8")) <= name_limit
    assert os.path.isfile(first_output)
    assert os.path.isfile(second_output)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX mode bits")
def test_export_inat_photo_restores_rendered_file_mode(tmp_path):
    db = Database(str(tmp_path / "vireo.db"))
    workspace_id = db.ensure_default_workspace()
    db.set_active_workspace(workspace_id)
    source_dir = tmp_path / "photos"
    source_dir.mkdir()
    source = source_dir / "cardinal.jpg"
    Image.new("RGB", (32, 24), (190, 30, 20)).save(source)
    folder_id = workspace_folders.add_folder(db, str(source_dir), name="photos")
    photo_id = photo_ingest.add_photo(
        db, folder_id=folder_id,
        filename=source.name,
        extension=".jpg",
        file_size=source.stat().st_size,
        file_mtime=source.stat().st_mtime,
        timestamp="2024-06-01T10:00:00",
    )
    rendered_modes = []

    def copy_without_mode(source_path, destination_path):
        rendered_modes.append(stat.S_IMODE(os.stat(source_path).st_mode))
        return shutil.copyfile(source_path, destination_path)

    with (
        patch("inat_export.write_inat_metadata"),
        patch("inat_export.shutil.copy2", side_effect=copy_without_mode),
    ):
        output = export_inat_photo(
            db,
            str(tmp_path / "cache"),
            photo_id,
            str(tmp_path / "exports"),
            {},
        )

    assert stat.S_IMODE(os.stat(output).st_mode) == rendered_modes[0]
