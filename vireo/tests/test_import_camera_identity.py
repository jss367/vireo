"""Synthetic camera identities stay separate through import metadata readers."""

from datetime import datetime, timedelta

import grouping
import import_dedup
import pytest
from camera_clock import ClockFrame, find_clock_problems
from PIL import Image
from PIL.ExifTags import IFD
from PIL.ExifTags import Base as ExifBase

SERIAL_TAGS = {"SerialNumber": 0xFDE9, "BodySerialNumber": 0xA431, "CameraSerialNumber": 0xC62F}


def jpeg(path, serial_tag, serial, captured="2026:05:30 12:00:00", in_sub_ifd=True, maker_note=False):
    image = Image.new("RGB", (16, 16))
    exif = image.getexif()
    exif[ExifBase.Model] = "Synthetic Camera"
    sub = exif.get_ifd(IFD.Exif)
    sub[ExifBase.DateTimeOriginal] = captured
    sub[ExifBase.OffsetTimeOriginal] = "-07:00"
    if serial_tag:
        (sub if in_sub_ifd else exif)[SERIAL_TAGS[serial_tag]] = serial
    if maker_note:
        sub[ExifBase.MakerNote] = b"synthetic maker note"
    image.save(path, exif=exif)


@pytest.mark.parametrize("tag", SERIAL_TAGS)
@pytest.mark.parametrize("in_sub_ifd", [False, True])
def test_pillow_retains_supported_camera_serials(tmp_path, monkeypatch, tag, in_sub_ifd):
    import exifread

    calls = []
    monkeypatch.setattr(exifread, "process_file", lambda *args, **kwargs: calls.append(args) or {})
    path = tmp_path / "DSC_0001.jpg"
    jpeg(path, tag, "body-A", in_sub_ifd=in_sub_ifd)
    clock = grouping.read_exif_clock(path)
    assert clock.captured == datetime(2026, 5, 30, 12)
    assert clock.offset == "-07:00"
    assert clock.camera == "Synthetic Camera #body-A"
    assert calls == []


@pytest.mark.parametrize("tag", SERIAL_TAGS)
@pytest.mark.parametrize("group", ["EXIF", "MakerNote", "Image"])
def test_exifread_retains_supported_camera_serials(tmp_path, monkeypatch, tag, group):
    import exifread

    path = tmp_path / "DSC_0001.NEF"
    path.write_bytes(b"synthetic raw header")

    def parsed(_file, **options):
        assert options["details"] is True
        assert options["extract_thumbnail"] is False
        return {"EXIF DateTimeOriginal": "2026:05:30 12:00:00", "EXIF OffsetTimeOriginal": "-07:00",
                "Image Model": "Synthetic Camera", f"{group} {tag}": "body-A"}

    monkeypatch.setattr(exifread, "process_file", parsed)
    clock = grouping.read_exif_clock(path)
    assert clock.captured == datetime(2026, 5, 30, 12)
    assert clock.offset == "-07:00"
    assert clock.camera == "Synthetic Camera #body-A"


@pytest.mark.parametrize("tag", SERIAL_TAGS)
def test_pillow_maker_note_uses_exifread_without_losing_the_capture_time(tmp_path, monkeypatch, tag):
    import exifread

    path = tmp_path / "DSC_0001.jpg"
    jpeg(path, None, None, maker_note=True)
    calls = []

    def parsed(_file, **options):
        calls.append(options)
        return {f"MakerNote {tag}": "body-A"}

    monkeypatch.setattr(exifread, "process_file", parsed)
    clock = grouping.read_exif_clock(path)
    assert len(calls) == 1
    assert clock.captured == datetime(2026, 5, 30, 12)
    assert clock.offset == "-07:00"
    assert clock.camera == "Synthetic Camera #body-A"


@pytest.mark.parametrize("tag", SERIAL_TAGS)
@pytest.mark.parametrize("group", ["EXIF", "MakerNotes"])
def test_exiftool_fallback_requests_and_keeps_camera_identity(tmp_path, monkeypatch, tag, group):
    import metadata

    path = tmp_path / "DSC_0001.heic"
    path.write_bytes(b"synthetic heic header")
    monkeypatch.setattr(import_dedup, "read_exif_clock", lambda _: grouping.ExifClock(None, None, None))

    def parsed(paths, restricted_tags):
        assert paths == [str(path)]
        assert {"-Model", "-SerialNumber", "-BodySerialNumber", "-CameraSerialNumber"} <= set(restricted_tags)
        result = {"EXIF": {"Model": "Synthetic Camera", "DateTimeOriginal": "2026:05:30 12:00:00",
                           "OffsetTimeOriginal": "-07:00"}}
        result.setdefault(group, {})[tag] = "body-A"
        return {str(path): result}

    monkeypatch.setattr(metadata, "extract_metadata", parsed)
    times = import_dedup.source_capture_timestamps([path])
    assert times[path] == datetime(2026, 5, 30, 12)
    assert times.offsets[path] == "-07:00"
    assert times.cameras[path] == "Synthetic Camera #body-A"


@pytest.mark.parametrize("tag", SERIAL_TAGS)
@pytest.mark.parametrize("distinct_bodies", [False, True])
def test_same_model_bodies_do_not_manufacture_clock_jumps(tmp_path, tag, distinct_bodies):
    paths = []
    for i in range(12):
        path = tmp_path / f"DSC_{i + 1:04d}.jpg"
        body_b = i % 2 == 1
        captured = datetime(2026, 5, 30, 10 if body_b else 12) + timedelta(seconds=i)
        jpeg(path, tag, "body-B" if body_b and distinct_bodies else "body-A", captured.strftime("%Y:%m:%d %H:%M:%S"))
        paths.append(path)
    times = import_dedup.source_capture_timestamps(paths)
    frames = [ClockFrame(ref=str(p), filename=p.name, captured=times[p], camera=times.cameras[p]) for p in paths]
    assert bool(find_clock_problems(frames, use_gps=False)) is not distinct_bodies


def test_maker_note_failure_keeps_pillow_capture_time(tmp_path, monkeypatch):
    import exifread

    path = tmp_path / "DSC_0001.jpg"
    jpeg(path, None, None, maker_note=True)

    def unreadable(_file, **_options):
        raise ValueError("synthetic malformed maker note")

    monkeypatch.setattr(exifread, "process_file", unreadable)
    clock = grouping.read_exif_clock(path)
    assert clock == grouping.ExifClock(datetime(2026, 5, 30, 12), "-07:00", "Synthetic Camera")


def test_lens_serial_does_not_identify_a_camera_body(tmp_path, monkeypatch):
    import exifread

    path = tmp_path / "DSC_0001.NEF"
    path.write_bytes(b"synthetic raw header")
    monkeypatch.setattr(exifread, "process_file", lambda *_args, **_options: {
        "EXIF DateTimeOriginal": "2026:05:30 12:00:00", "Image Model": "Synthetic Camera",
        "EXIF LensSerialNumber": "lens-A", "MakerNote LensSerialNumber": "lens-A",
    })
    assert grouping.read_exif_clock(path).camera == "Synthetic Camera"
