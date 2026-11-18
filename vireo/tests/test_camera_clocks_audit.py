"""The Audit's Camera Clocks check, its route, and the capture-time mode it uses."""

import json
import os
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from wait import wait_for_job_via_client


def _exif(captured, offset, shutter, gps_error=None, serial=3066996):
    data = {
        "EXIF": {
            "Make": "NIKON CORPORATION",
            "Model": "NIKON D850",
            "DateTimeOriginal": captured.strftime("%Y:%m:%d %H:%M:%S"),
            "SubSecTimeOriginal": 50,
            "OffsetTimeOriginal": offset,
        },
        "MakerNotes": {"SerialNumber": serial, "ShutterCount": shutter},
    }
    if gps_error is not None:
        sign = -1 if offset.startswith("-") else 1
        hours, minutes = offset[1:].split(":")
        offset_minutes = sign * (int(hours) * 60 + int(minutes))
        gps = captured - timedelta(minutes=offset_minutes + gps_error)
        data["Composite"] = {
            "GPSDateTime": gps.strftime("%Y:%m:%d %H:%M:%S.00Z"),
            "GPSLatitude": 32.85,
            "GPSLongitude": -117.0,
        }
    return data


def _add_shoot(db, folder_id):
    """A -05:00 run, then a -07:00 run whose clock is an hour behind GPS."""
    from services import photo_ingest

    ids = {}
    for i in range(12):
        captured = datetime(2026, 1, 25, 12, 53) + timedelta(seconds=20 * i)
        ids[f"_D85{9685 + i:04d}.NEF"] = (captured, "-05:00", 108497 + i, None)
    for i in range(20):
        captured = datetime(2026, 1, 25, 11, 21) + timedelta(seconds=30 * i)
        ids[f"_D85{16 + i:04d}.NEF"] = (captured, "-07:00", 108520 + i, -60)
    photo_ids = {}
    for name, (captured, offset, shutter, gps_error) in ids.items():
        pid = photo_ingest.add_photo(
            db, folder_id=folder_id, filename=name, extension=".nef", file_size=1,
            file_mtime=0.0, timestamp=captured.isoformat(),
        )
        db.conn.execute(
            "UPDATE photos SET exif_data = ? WHERE id = ?",
            (json.dumps(_exif(captured, offset, shutter, gps_error)), pid),
        )
        photo_ids[name] = pid
    db.conn.commit()
    return photo_ids


@pytest.fixture
def clocks_db(tmp_path):
    from db import Database
    from services import workspace_folders

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.commit()
    db.set_active_workspace(ws)
    folder_id = workspace_folders.add_folder(db, str(tmp_path / "2026-01-25"), name="2026-01-25")
    photo_ids = _add_shoot(db, folder_id)
    yield db, folder_id, photo_ids
    db.close()


def test_check_finds_the_zone_change_and_fixes_each_group(clocks_db, monkeypatch):
    import time_zones
    from services import camera_clocks

    db, folder_id, photo_ids = clocks_db
    monkeypatch.setattr(time_zones, "available", lambda: True)
    monkeypatch.setattr(camera_clocks.time_zones, "local_zone",
                        lambda lat, lon, utc: ("America/Los_Angeles", -480))

    result = camera_clocks.check_camera_clocks(db)

    assert result["location_available"] is True
    [finding] = result["findings"]
    assert finding["folder_id"] == folder_id
    assert finding["camera"] == "NIKON D850 #3066996"
    assert "time-zone setting changed" in finding["summary"]
    fixes = {f["recorded_offset"]: f for f in finding["fixes"]}
    assert fixes["-05:00"]["target_offset"] == "-08:00"
    assert fixes["-05:00"]["shift_minutes"] == -120
    assert sorted(fixes["-05:00"]["refs"]) == sorted(
        pid for name, pid in photo_ids.items() if name.startswith("_D859"))
    assert fixes["-07:00"]["target_offset"] == "-08:00"
    assert fixes["-07:00"]["shift_minutes"] == 0


def test_check_without_location_still_reports_shot_order(clocks_db, monkeypatch):
    import time_zones
    from services import camera_clocks

    db, _, _ = clocks_db
    monkeypatch.setattr(time_zones, "available", lambda: False)

    result = camera_clocks.check_camera_clocks(db)
    assert result["location_available"] is False
    [finding] = result["findings"]
    # Without a local zone, GPS still measures the clock (an hour behind).
    # The -05:00 frames, shot minutes before the -07:00 ones, are restated
    # in -07:00 as well, so both groups end up in shot order.
    fixes = {f["recorded_offset"]: f for f in finding["fixes"]}
    assert fixes["-07:00"]["target_offset"] is None
    assert fixes["-07:00"]["shift_minutes"] == 60
    assert fixes["-05:00"]["target_offset"] == "-07:00"
    assert fixes["-05:00"]["shift_minutes"] == -60


def test_route_records_the_run_for_the_summary(app_and_db, monkeypatch):
    import time_zones
    from services import workspace_folders

    app, db = app_and_db
    monkeypatch.setattr(time_zones, "available", lambda: False)
    folder_id = workspace_folders.add_folder(db, "/clocks/2026-01-25", name="2026-01-25")
    _add_shoot(db, folder_id)

    client = app.test_client()
    resp = client.get("/api/audit/camera-clocks")
    assert resp.status_code == 200
    body = resp.get_json()
    assert len(body["findings"]) == 1

    summary = client.get("/api/audit/summary").get_json()
    assert summary["checks"]["clocks"]["problem_count"] == 1


def test_set_offset_mode_keeps_clock_time_and_writes_offset(client_with_photo, monkeypatch):
    import capture_time

    app, db, photo_id = client_with_photo
    db.conn.execute(
        "UPDATE photos SET exif_data = ? WHERE id = ?",
        (json.dumps({"EXIF": {"DateTimeOriginal": "2026:01:25 11:21:57",
                              "OffsetTimeOriginal": "-07:00"}}), photo_id),
    )
    db.conn.commit()
    commands = []

    def fake_run(cmd, **_kwargs):
        commands.append(list(cmd))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(capture_time.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(capture_time.subprocess, "run", fake_run)
    monkeypatch.setattr(capture_time, "extract_metadata",
                        lambda paths: {paths[0]: {"EXIF": {"OffsetTimeOriginal": "-08:00"}}})

    client = app.test_client()
    preview = client.post("/api/capture-time/preview", json={
        "photo_ids": [photo_id], "mode": "set_offset", "target_offset": "-08:00", "shift_minutes": 0,
    }).get_json()
    assert preview["samples"][0]["after_time"] == preview["samples"][0]["before_time"]
    assert preview["samples"][0]["after_offset"] == "-08:00"

    resp = client.post("/api/jobs/capture-time", json={
        "photo_ids": [photo_id], "mode": "set_offset", "target_offset": "-08:00",
        "shift_minutes": 0, "keep_backups": False,
    })
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["updated"] == 1
    cmd = next(c for c in commands if any(os.path.basename(a) == "exiftool" for a in c[:2]))
    assert "-OffsetTimeOriginal=-08:00" in cmd
    assert not any(arg.startswith("-AllDates") for arg in cmd)


def test_set_offset_mode_requires_a_target():
    import capture_time

    with pytest.raises(ValueError, match="target_offset"):
        capture_time._normalize_inputs("set_offset", None, 0)
    with pytest.raises(ValueError, match="shift_minutes"):
        capture_time._normalize_inputs("set_offset", "-08:00", True)
    assert capture_time._normalize_inputs("set_offset", "-08:00", -120) == (None, -120, "-08:00")
