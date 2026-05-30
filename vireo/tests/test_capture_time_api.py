import json
import os
from types import SimpleNamespace

from wait import wait_for_job_via_client


def test_capture_time_preview_preserves_instant_from_current_offset(app_and_db):
    app, db = app_and_db
    photo = db.get_photos()[0]
    db.conn.execute(
        "UPDATE photos SET exif_data = ? WHERE id = ?",
        (
            json.dumps(
                {
                    "EXIF": {
                        "DateTimeOriginal": "2026:05:22 20:07:23",
                        "SubSecTimeOriginal": "56",
                        "OffsetTimeOriginal": "-07:00",
                    }
                }
            ),
            photo["id"],
        ),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.post(
        "/api/capture-time/preview",
        json={
            "photo_ids": [photo["id"]],
            "mode": "preserve_instant",
            "target_offset": "-10:00",
        },
    )

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["shift_minutes"] == -180
    assert data["samples"][0]["before_time"] == "2026-05-22 20:07:23.56"
    assert data["samples"][0]["before_offset"] == "-07:00"
    assert data["samples"][0]["after_time"] == "2026-05-22 17:07:23.56"
    assert data["samples"][0]["after_offset"] == "-10:00"


def test_capture_time_job_writes_exiftool_and_refreshes_cache(client_with_photo, monkeypatch):
    import capture_time

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()
    path = os.path.join(folder["path"], photo["filename"])
    db.conn.execute(
        "UPDATE photos SET timestamp = ?, exif_data = ? WHERE id = ?",
        (
            "2026-05-22T20:07:23.560000",
            json.dumps(
                {
                    "EXIF": {
                        "DateTimeOriginal": "2026:05:22 20:07:23",
                        "SubSecTimeOriginal": "56",
                        "OffsetTimeOriginal": "-07:00",
                    }
                }
            ),
            photo_id,
        ),
    )
    db.conn.commit()

    commands = []

    def fake_run(cmd, **_kwargs):
        commands.append(cmd)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_extract(paths):
        assert paths == [path]
        return {
            path: {
                "EXIF": {
                    "DateTimeOriginal": "2026:05:22 17:07:23",
                    "SubSecTimeOriginal": "56",
                    "OffsetTimeOriginal": "-10:00",
                    "OffsetTime": "-10:00",
                    "OffsetTimeDigitized": "-10:00",
                }
            }
        }

    monkeypatch.setattr(capture_time.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(capture_time.subprocess, "run", fake_run)
    monkeypatch.setattr(capture_time, "extract_metadata", fake_extract)

    client = app.test_client()
    resp = client.post(
        "/api/jobs/capture-time",
        json={
            "photo_ids": [photo_id],
            "mode": "preserve_instant",
            "target_offset": "-10:00",
            "keep_backups": False,
        },
    )
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["updated"] == 1
    assert job["result"]["shift_minutes"] == -180

    assert commands
    cmd = commands[0]
    assert "-overwrite_original" in cmd
    assert "-AllDates-=0:0:0 3:0:0" in cmd
    assert "-OffsetTimeOriginal=-10:00" in cmd
    assert path in cmd

    refreshed = db.get_photo(photo_id)
    assert refreshed["timestamp"] == "2026-05-22T17:07:23.560000"
    metadata = json.loads(refreshed["exif_data"])
    assert metadata["EXIF"]["OffsetTimeOriginal"] == "-10:00"


def test_capture_time_preview_preserves_instant_uses_per_photo_shifts(app_and_db):
    """preserve_instant must shift each photo by *its own* current offset.

    Regression test for the bug where a single shift was computed from the
    first sampled photo and reused for every photo, silently breaking the
    capture instant for any selection whose photos had differing offsets.
    """
    app, db = app_and_db
    photos = db.get_photos()
    assert len(photos) >= 2
    p1, p2 = photos[0], photos[1]
    db.conn.execute(
        "UPDATE photos SET exif_data = ? WHERE id = ?",
        (
            json.dumps(
                {
                    "EXIF": {
                        "DateTimeOriginal": "2026:05:22 20:00:00",
                        "OffsetTimeOriginal": "-07:00",
                    }
                }
            ),
            p1["id"],
        ),
    )
    db.conn.execute(
        "UPDATE photos SET exif_data = ? WHERE id = ?",
        (
            json.dumps(
                {
                    "EXIF": {
                        "DateTimeOriginal": "2026:05:22 20:00:00",
                        "OffsetTimeOriginal": "-08:00",
                    }
                }
            ),
            p2["id"],
        ),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.post(
        "/api/capture-time/preview",
        json={
            "photo_ids": [p1["id"], p2["id"]],
            "mode": "preserve_instant",
            "target_offset": "-10:00",
        },
    )

    assert resp.status_code == 200
    data = resp.get_json()
    samples_by_id = {row["photo_id"]: row for row in data["samples"]}
    assert samples_by_id[p1["id"]]["shift_minutes"] == -180
    assert samples_by_id[p1["id"]]["after_time"] == "2026-05-22 17:00:00"
    assert samples_by_id[p2["id"]]["shift_minutes"] == -120
    assert samples_by_id[p2["id"]]["after_time"] == "2026-05-22 18:00:00"
    assert data["shifts_vary"] is True
    assert data["shift_minutes"] is None


def test_capture_time_job_applies_per_photo_shifts(client_with_photo, monkeypatch):
    """Each ExifTool invocation must use that photo's own derived shift."""
    import capture_time

    app, db, p1 = client_with_photo
    photo = db.get_photo(p1)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()
    photos_dir = folder["path"]

    from PIL import Image as _Image

    second_path = os.path.join(photos_dir, "second.jpg")
    _Image.new("RGB", (800, 600), (40, 90, 180)).save(second_path, "JPEG", quality=85)
    p2 = db.add_photo(
        folder_id=photo["folder_id"],
        filename="second.jpg",
        extension=".jpg",
        file_size=os.path.getsize(second_path),
        file_mtime=os.path.getmtime(second_path),
        width=800, height=600,
    )

    db.conn.execute(
        "UPDATE photos SET exif_data = ? WHERE id = ?",
        (
            json.dumps(
                {
                    "EXIF": {
                        "DateTimeOriginal": "2026:05:22 20:00:00",
                        "OffsetTimeOriginal": "-07:00",
                    }
                }
            ),
            p1,
        ),
    )
    db.conn.execute(
        "UPDATE photos SET exif_data = ? WHERE id = ?",
        (
            json.dumps(
                {
                    "EXIF": {
                        "DateTimeOriginal": "2026:05:22 20:00:00",
                        "OffsetTimeOriginal": "-08:00",
                    }
                }
            ),
            p2,
        ),
    )
    db.conn.commit()

    commands = []

    def fake_run(cmd, **_kwargs):
        commands.append(list(cmd))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_extract(paths):
        return {paths[0]: {"EXIF": {"OffsetTimeOriginal": "-10:00"}}}

    monkeypatch.setattr(capture_time.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(capture_time.subprocess, "run", fake_run)
    monkeypatch.setattr(capture_time, "extract_metadata", fake_extract)

    client = app.test_client()
    resp = client.post(
        "/api/jobs/capture-time",
        json={
            "photo_ids": [p1, p2],
            "mode": "preserve_instant",
            "target_offset": "-10:00",
            "keep_backups": False,
        },
    )
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["updated"] == 2
    assert job["result"]["shifts_vary"] is True
    assert job["result"]["shift_minutes"] is None

    by_file = {}
    for cmd in commands:
        target = cmd[-1]
        shift_args = [a for a in cmd if a.startswith("-AllDates")]
        by_file[os.path.basename(target)] = shift_args[0] if shift_args else None
    assert by_file["test.jpg"] == "-AllDates-=0:0:0 3:0:0"
    assert by_file["second.jpg"] == "-AllDates-=0:0:0 2:0:0"
