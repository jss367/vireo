import json
import os
from types import SimpleNamespace

import pytest
from services import photo_ingest
from wait import wait_for_job_via_client


def _exiftool_command(commands):
    """Select the command under test, ignoring unrelated platform probes."""
    return next(
        command
        for command in commands
        if command
        and any(
            os.path.basename(argument).lower() in {"exiftool", "exiftool.exe"}
            for argument in command[:2]
        )
    )


def test_exiftool_command_accepts_supported_launchers():
    perl_command = ["/usr/bin/perl", "/opt/vireo/exiftool", "-json"]
    windows_command = ["exiftool.exe", "-json"]

    assert _exiftool_command([["/usr/sbin/sysctl"], perl_command]) == perl_command
    assert _exiftool_command([["wmic"], windows_command]) == windows_command


def test_capture_time_offset_validation_rejects_invalid_extremes():
    from capture_time import parse_offset_minutes

    assert parse_offset_minutes("+14:00") == 14 * 60
    assert parse_offset_minutes("-12:00") == -12 * 60
    assert parse_offset_minutes("+14:30") is None
    assert parse_offset_minutes("-12:30") is None
    assert parse_offset_minutes("-13:00") is None


def test_capture_time_preview_preserves_instant_from_current_offset(app_and_db):
    app, db = app_and_db
    photo = db.photos.list_page()[0]
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


def test_capture_time_routes_reject_non_object_json(app_and_db):
    app, _ = app_and_db
    client = app.test_client()

    preview = client.post(
        "/api/capture-time/preview",
        data='["not", "an", "object"]',
        content_type="application/json",
    )
    job = client.post(
        "/api/jobs/capture-time",
        data='"not an object"',
        content_type="application/json",
    )

    assert preview.status_code == 400
    assert job.status_code == 400


def test_capture_time_job_writes_exiftool_and_refreshes_cache(client_with_photo, monkeypatch):
    import capture_time

    app, db, photo_id = client_with_photo
    photo = db.photos.get(photo_id)
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
    cmd = _exiftool_command(commands)
    assert "-overwrite_original" in cmd
    assert "-AllDates-=0:0:0 3:0:0" in cmd
    assert "-OffsetTimeOriginal=-10:00" in cmd
    assert path in cmd

    refreshed = db.photos.get(photo_id)
    assert refreshed["timestamp"] == "2026-05-22T17:07:23.560000"
    metadata = json.loads(refreshed["exif_data"])
    assert metadata["EXIF"]["OffsetTimeOriginal"] == "-10:00"


def test_capture_time_job_skips_already_correct_offset(client_with_photo, monkeypatch):
    """Already-correct preserve-instant photos should not be rewritten."""
    import capture_time

    app, db, photo_id = client_with_photo
    db.conn.execute(
        "UPDATE photos SET timestamp = ?, exif_data = ? WHERE id = ?",
        (
            "2026-05-22T17:07:23.560000",
            json.dumps(
                {
                    "EXIF": {
                        "DateTimeOriginal": "2026:05:22 17:07:23",
                        "SubSecTimeOriginal": "56",
                        "OffsetTimeOriginal": "-10:00",
                        "OffsetTime": "-10:00",
                        "OffsetTimeDigitized": "-10:00",
                    }
                }
            ),
            photo_id,
        ),
    )
    db.conn.commit()

    def unexpected_write(*_args):
        raise AssertionError("ExifTool should not run for a no-op capture-time adjustment")

    monkeypatch.setattr(capture_time.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(capture_time, "_write_capture_time", unexpected_write)

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
    assert job["result"]["updated"] == 0
    assert job["result"]["skipped"] == 1
    assert job["result"]["failed"] == 0


def test_capture_time_job_writes_missing_offset_tags(client_with_photo, monkeypatch):
    """A matching OffsetTimeOriginal alone is not enough to skip the file."""
    import capture_time

    app, db, photo_id = client_with_photo
    photo = db.photos.get(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()
    path = os.path.join(folder["path"], photo["filename"])
    db.conn.execute(
        "UPDATE photos SET timestamp = ?, exif_data = ? WHERE id = ?",
        (
            "2026-05-22T17:07:23.560000",
            json.dumps(
                {
                    "EXIF": {
                        "DateTimeOriginal": "2026:05:22 17:07:23",
                        "SubSecTimeOriginal": "56",
                        "OffsetTimeOriginal": "-10:00",
                    }
                }
            ),
            photo_id,
        ),
    )
    db.conn.commit()

    commands = []

    def fake_run(cmd, **_kwargs):
        commands.append(list(cmd))
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
    assert job["result"]["skipped"] == 0
    assert commands
    cmd = _exiftool_command(commands)
    assert "-OffsetTime=-10:00" in cmd
    assert "-OffsetTimeOriginal=-10:00" in cmd
    assert "-OffsetTimeDigitized=-10:00" in cmd
    assert not any(arg.startswith("-AllDates") for arg in cmd)


def test_capture_time_job_does_not_skip_companion_pair_from_primary_cache(
    client_with_photo, monkeypatch
):
    """Companion files must still be rewritten when only primary cached EXIF is known."""
    import capture_time

    app, db, photo_id = client_with_photo
    photo = db.photos.get(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()
    primary_path = os.path.join(folder["path"], photo["filename"])
    companion_name = "test.xmp"
    companion_path = os.path.join(folder["path"], companion_name)
    with open(companion_path, "w", encoding="utf-8") as handle:
        handle.write("<xmpmeta></xmpmeta>")

    db.conn.execute(
        "UPDATE photos SET companion_path = ?, timestamp = ?, exif_data = ? WHERE id = ?",
        (
            companion_name,
            "2026-05-22T17:07:23.560000",
            json.dumps(
                {
                    "EXIF": {
                        "DateTimeOriginal": "2026:05:22 17:07:23",
                        "SubSecTimeOriginal": "56",
                        "OffsetTimeOriginal": "-10:00",
                        "OffsetTime": "-10:00",
                        "OffsetTimeDigitized": "-10:00",
                    }
                }
            ),
            photo_id,
        ),
    )
    db.conn.commit()

    commands = []

    def fake_run(cmd, **_kwargs):
        commands.append(list(cmd))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_extract(paths):
        assert paths == [primary_path]
        return {
            primary_path: {
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
    assert job["result"]["skipped"] == 0
    assert commands
    cmd = _exiftool_command(commands)
    assert primary_path in cmd
    assert companion_path in cmd
    assert not any(arg.startswith("-AllDates") for arg in cmd)


def test_capture_time_preview_preserves_instant_uses_per_photo_shifts(app_and_db):
    """preserve_instant must shift each photo by *its own* current offset.

    Regression test for the bug where a single shift was computed from the
    first sampled photo and reused for every photo, silently breaking the
    capture instant for any selection whose photos had differing offsets.
    """
    app, db = app_and_db
    photos = db.photos.list_page()
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


def test_capture_time_manual_mode_ignores_target_offset(client_with_photo, monkeypatch):
    """Manual shifts must not rewrite OffsetTime* tags, even if target_offset is sent."""
    import capture_time

    app, db, photo_id = client_with_photo
    photo = db.photos.get(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()
    path = os.path.join(folder["path"], photo["filename"])
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
            photo_id,
        ),
    )
    db.conn.commit()

    commands = []

    def fake_run(cmd, **_kwargs):
        commands.append(list(cmd))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_extract(paths):
        return {paths[0]: {"EXIF": {"OffsetTimeOriginal": "-07:00"}}}

    monkeypatch.setattr(capture_time.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(capture_time.subprocess, "run", fake_run)
    monkeypatch.setattr(capture_time, "extract_metadata", fake_extract)

    client = app.test_client()
    resp = client.post(
        "/api/jobs/capture-time",
        json={
            "photo_ids": [photo_id],
            "mode": "manual",
            "target_offset": "-10:00",
            "shift_minutes": 30,
            "keep_backups": False,
        },
    )
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["updated"] == 1
    assert job["result"]["target_offset"] is None

    assert commands
    cmd = _exiftool_command(commands)
    assert "-AllDates+=0:0:0 0:30:0" in cmd
    assert not any(arg.startswith("-OffsetTime") for arg in cmd), cmd


def test_capture_time_preview_manual_mode_ignores_target_offset(app_and_db):
    """Manual preview must not present target_offset as the after-offset."""
    app, db = app_and_db
    photo = db.photos.list_page()[0]
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
            photo["id"],
        ),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.post(
        "/api/capture-time/preview",
        json={
            "photo_ids": [photo["id"]],
            "mode": "manual",
            "target_offset": "-10:00",
            "shift_minutes": 30,
        },
    )

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["target_offset"] is None
    assert data["samples"][0]["after_offset"] == "-07:00"


def test_capture_time_preview_accepts_large_select_all_payload(app_and_db):
    """Preview accepts full-result select-all payloads while sampling rows."""
    app, db = app_and_db
    photo = db.photos.list_page()[0]

    client = app.test_client()
    resp = client.post(
        "/api/capture-time/preview",
        json={
            "photo_ids": [photo["id"]] * 501,
            "mode": "manual",
            "shift_minutes": 0,
        },
    )

    assert resp.status_code == 200
    assert resp.get_json()["samples"][0]["photo_id"] == photo["id"]


def test_capture_time_job_accepts_large_select_all_payload(client_with_photo, monkeypatch):
    """Capture-time jobs can run selections above the old 5000-photo UI limit."""
    import capture_time

    app, db, photo_id = client_with_photo
    def unexpected_write(*args):
        raise AssertionError("A zero-minute shift must skip the ExifTool write")

    monkeypatch.setattr(capture_time.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(capture_time, "_write_capture_time", unexpected_write)

    client = app.test_client()
    resp = client.post(
        "/api/jobs/capture-time",
        json={
            "photo_ids": [photo_id] * 5001,
            "mode": "manual",
            "shift_minutes": 0,
            "keep_backups": False,
        },
    )

    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["updated"] == 0
    assert job["result"]["skipped"] == 1
    assert job["config"]["photo_count"] == 1
    assert job["config"]["photo_ids_sample"] == [photo_id]
    assert "photo_ids" not in job["config"]
    assert job["result"]["failed"] == 0


def test_capture_time_job_applies_per_photo_shifts(client_with_photo, monkeypatch):
    """Each ExifTool invocation must use that photo's own derived shift."""
    import capture_time

    app, db, p1 = client_with_photo
    photo = db.photos.get(p1)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()
    photos_dir = folder["path"]

    from PIL import Image as _Image

    second_path = os.path.join(photos_dir, "second.jpg")
    _Image.new("RGB", (800, 600), (40, 90, 180)).save(second_path, "JPEG", quality=85)
    p2 = photo_ingest.add_photo(
        db, folder_id=photo["folder_id"],
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


def test_capture_time_job_counts_exiftool_write_error_as_failed(client_with_photo, monkeypatch):
    """ExifTool exits 1 when a write fails — the photo must be counted as
    failed, not updated, and its metadata must not be refreshed."""
    import capture_time

    app, db, photo_id = client_with_photo
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

    def fake_run(cmd, **_kwargs):
        return SimpleNamespace(
            returncode=1,
            stdout="    0 image files updated\n    1 files weren't updated due to errors\n",
            stderr="Error: File is read-only",
        )

    def fail_extract(paths):
        raise AssertionError("metadata must not be refreshed after a failed write")

    monkeypatch.setattr(capture_time.shutil, "which", lambda name: "/usr/bin/exiftool")
    monkeypatch.setattr(capture_time.subprocess, "run", fake_run)
    monkeypatch.setattr(capture_time, "extract_metadata", fail_extract)

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
    assert job["result"]["updated"] == 0
    assert job["result"]["failed"] == 1
    assert "read-only" in job["result"]["failures"][0]["error"]

    # Timestamp untouched — no false "updated" report
    refreshed = db.photos.get(photo_id)
    assert refreshed["timestamp"] == "2026-05-22T20:07:23.560000"


def test_capture_time_companion_only_preserves_primary_identity(client_with_photo, monkeypatch):
    import capture_time
    import metadata

    _, db, pid = client_with_photo
    photo = db.photos.get(pid)
    folder = db.conn.execute("SELECT path FROM folders WHERE id=?", (photo["folder_id"],)).fetchone()[0]
    companion = os.path.join(folder, photo["filename"])
    db.conn.execute(
        "UPDATE photos SET filename='missing.nef', extension='.nef', companion_path=?, "
        "file_hash='raw-hash', file_size=123456, file_mtime=42, hash_status='ok' WHERE id=?",
        (photo["filename"], pid),
    )
    db.conn.commit()
    commands = []

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(metadata, "find_exiftool", lambda: "exiftool")
    monkeypatch.setattr(capture_time.subprocess, "run", fake_run)
    monkeypatch.setattr(capture_time, "extract_metadata", lambda paths: {
        paths[0]: {"EXIF": {"DateTimeOriginal": "2026:05:22 18:00:00"}},
    })
    result = capture_time.adjust_capture_time(db, [pid], mode="manual", shift_minutes=60)
    assert result["updated"] == 1, result
    assert commands[0][-1] == companion
    row = db.conn.execute("SELECT * FROM photos WHERE id=?", (pid,)).fetchone()
    assert (row["file_hash"], row["file_size"], row["file_mtime"], row["hash_status"]) == (
        "raw-hash", 123456, 42, "ok",
    )
    assert row["timestamp"] == "2026-05-22T18:00:00"


def test_capture_time_primary_only_preserves_companion_identity(client_with_photo, monkeypatch, tmp_path):
    import capture_time
    import metadata
    from import_dedup import CatalogIndex, DuplicateChecker
    from scanner import compute_file_hash

    _, db, pid = client_with_photo
    source = tmp_path / "offline-companion.jpg"
    source.write_bytes(b"original companion bytes from card")
    companion_hash = compute_file_hash(str(source))
    db.conn.execute("UPDATE photos SET companion_path=? WHERE id=?", (source.name, pid))
    db.conn.execute(
        "INSERT INTO companion_identities (photo_id, filename, file_size, file_hash) VALUES (?, ?, ?, ?)",
        (pid, source.name, source.stat().st_size, companion_hash),
    )
    db.conn.commit()
    monkeypatch.setattr(metadata, "find_exiftool", lambda: "exiftool")
    commands = []

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(capture_time.subprocess, "run", fake_run)
    monkeypatch.setattr(capture_time, "extract_metadata", lambda paths: {
        paths[0]: {"EXIF": {"DateTimeOriginal": "2026:05:22 18:00:00"}},
    })
    result = capture_time.adjust_capture_time(db, [pid], mode="manual", shift_minutes=60)
    assert result["updated"] == 1, result
    command = _exiftool_command(commands)
    assert len(command[command.index("--") + 1:]) == 1
    identity = db.conn.execute("SELECT file_hash FROM companion_identities WHERE photo_id=?", (pid,)).fetchone()
    assert identity is not None
    assert identity["file_hash"] == companion_hash
    assert DuplicateChecker(CatalogIndex.from_db(db), verify_by_hash=True).match(source)


def test_capture_time_rejects_a_stale_preview(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    client = app.test_client()
    body = {"photo_ids": [pid], "mode": "manual", "shift_minutes": 15}
    token = client.post("/api/capture-time/preview", json=body).get_json()["expected_state"]
    db.conn.execute("UPDATE photos SET timestamp='2026-03-18T01:23:45' WHERE id=?", (pid,))
    db.commit()
    result = client.post("/api/jobs/capture-time", json={**body, "expected_state": token})
    assert result.status_code == 409


def test_capture_time_overlap_is_rejected_before_writing(app_and_db):
    from capture_time import current_capture_time_token, guard_capture_time

    _, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    token = current_capture_time_token(db, [pid])
    with guard_capture_time(db, [pid], token), pytest.raises(ValueError, match="already running"):
        with guard_capture_time(db, [pid], token):
            pytest.fail("overlapping correction acquired the same photo")
    with guard_capture_time(db, [pid], token):
        pass


def test_capture_time_fallback_changes_invalidate_preview(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = db.photos.list_page()[0]["id"]
    body = {"photo_ids": [pid], "mode": "preserve_instant", "target_offset": "-08:00"}
    exif = {"CreateDate": "2026:05:22 20:07:23", "SubSecTime": "56", "OffsetTimeDigitized": "-07:00"}
    for tag, value in [("CreateDate", "2026:05:22 20:08:00"), ("SubSecTime", "12"), ("OffsetTimeDigitized", "-06:00")]:
        db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (json.dumps({"EXIF": exif}), pid))
        db.commit()
        preview = client.post("/api/capture-time/preview", json=body).get_json()
        exif[tag] = value
        db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (json.dumps({"EXIF": exif}), pid))
        db.commit()
        result = client.post("/api/jobs/capture-time", json={**body, "expected_state": preview["expected_state"]})
        assert result.status_code == 409, tag


@pytest.mark.parametrize("shift,offset", [(60, None), (60, "-06:00"), (0, "-06:00")])
def test_dependent_reset_write_matches_reviewed_shift_and_offset(client_with_photo, monkeypatch, tmp_path, shift, offset):
    import capture_time
    import metadata

    app, db, first = client_with_photo
    second = photo_ingest.add_photo(db, folder_id=db.photos.get(first)["folder_id"], filename="later.jpg",
                                    extension=".jpg", file_size=1, file_mtime=0, timestamp="2026-01-25T13:06:00")
    (tmp_path / "photos" / "later.jpg").touch()
    db.commit()
    client = app.test_client()
    body = {"photo_ids": [first], "mode": "set_offset", "target_offset": "-08:00", "shift_minutes": 0}
    body["expected_state"] = client.post("/api/capture-time/preview", json=body).get_json()["expected_state"]
    step = {"photo_ids": [second], "mode": "set_offset" if offset else "manual",
            "target_offset": offset, "shift_minutes": shift}
    step["expected_state"] = client.post("/api/capture-time/preview", json=step).get_json()["expected_state"]
    body["dependent_shifts"] = [step]
    db.conn.execute("UPDATE photos SET timestamp='2026-01-25T13:07:00' WHERE id=?", (second,))
    db.commit()
    assert client.post("/api/jobs/capture-time", json=body).status_code == 409
    step["expected_state"] = client.post("/api/capture-time/preview", json=step).get_json()["expected_state"]
    commands = {}
    monkeypatch.setattr(metadata, "find_exiftool", lambda: "exiftool")
    monkeypatch.setattr(capture_time, "_write_capture_time",
                        lambda db, cmd, photo, paths, catalog_paths: commands.update({photo["id"]: cmd}))
    result = client.post("/api/jobs/capture-time", json=body)
    job = wait_for_job_via_client(client, result.get_json()["job_id"])
    assert job["result"]["updated"] == 2
    assert "-OffsetTimeOriginal=-08:00" in commands[first]
    assert not any(arg.startswith("-AllDates") for arg in commands[first])
    shifts = [arg for arg in commands[second] if arg.startswith("-AllDates")]
    assert shifts == (["-AllDates+=0:0:0 1:0:0"] if shift else [])
    offsets = [arg for arg in commands[second] if arg.startswith("-OffsetTime")]
    assert offsets == ([f"-OffsetTime={offset}", f"-OffsetTimeOriginal={offset}", f"-OffsetTimeDigitized={offset}"]
                       if offset else [])
