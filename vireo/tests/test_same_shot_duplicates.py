"""Metadata edits must remain discoverable without weakening byte-safe cleanup."""
import json
import shutil
import struct

import pytest
from db import Database
from duplicate_scan import revalidate_scan_result, run_duplicate_scan
from services import photo_ingest
from services.same_shot_duplicates import is_duplicate_review_member, scan_same_shot_candidates


@pytest.fixture
def catalog(tmp_path):
    db = Database(str(tmp_path / "catalog.db"))
    yield db
    db.close()


def metadata(serial="SN-12345", count=386):
    return {"EXIF": {"Make": "NIKON CORPORATION", "Model": "NIKON Z 6", "SerialNumber": serial},
            "MakerNotes": {"ShutterCount": count}}


def seed(db, folder, name, file_hash, *, exif=None, phash="123456789abcdef0", timestamp=None):
    fid = db.add_folder(str(folder))
    pid = photo_ingest.add_photo(
        db, folder_id=fid, filename=name, extension=".jpg", file_size=1000,
        file_mtime=100.0, file_hash=file_hash, timestamp=timestamp,
    )
    db.conn.execute("UPDATE photos SET exif_data=?, phash=? WHERE id=?",
                    (json.dumps(metadata() if exif is None else exif), phash, pid))
    db.commit()
    return pid


def save_scan(db, result):
    db.conn.execute(
        "INSERT INTO job_history (id,type,status,started_at,finished_at,result) "
        "VALUES ('same-shot-test','duplicate-scan','completed','2026-10-24T10:00:00',"
        "'2026-10-24T10:00:01',?)", (json.dumps(result),),
    )
    db.commit()


def test_changed_dates_sizes_names_and_hashes_are_review_only(catalog, tmp_path, monkeypatch):
    a = seed(catalog, tmp_path / "2022", "original.jpg", "old", timestamp="2023-01-01T01:02:03")
    b = seed(catalog, tmp_path / "2026", "renamed.jpg", "new", timestamp="2026-04-03T01:02:03")
    catalog.conn.execute("UPDATE photos SET file_size=2000 WHERE id=?", (b,))
    catalog.commit()
    # Same-shot discovery must work with a sleeping/offline NAS, with no stat.
    import services.same_shot_duplicates as same_shot
    monkeypatch.setattr(same_shot.os.path, "exists", lambda *_: pytest.fail("unexpected original access"))
    result = run_duplicate_scan({"progress": {}}, catalog)
    assert result["proposals"] == []
    assert result["buckets"] == []
    assert result["loser_count"] == result["resolved_loser_count"] == 0
    group, = result["same_shot_groups"]
    assert {p["id"] for p in group["photos"]} == {a, b}
    assert group["visual_evidence"] == "matching"
    assert {p["timestamp"] for p in group["photos"]} == {"2023-01-01T01:02:03", "2026-04-03T01:02:03"}
    assert all(row[0] != "rejected" for row in catalog.conn.execute("SELECT flag FROM photos"))


@pytest.mark.parametrize("change", [
    {"EXIF": {"Make": "Nikon", "Model": "Z6", "LensSerialNumber": "SN-12345"}, "MakerNotes": {"ShutterCount": 386}},
    metadata(serial="000000"), metadata(serial="unknown"), metadata(serial=True),
    metadata(count=0), metadata(count=-1), metadata(count=True), metadata(count=386.5),
    metadata(count="386.0"), metadata(count=""), {}, [], "bad", None,
    {"EXIF": {"SerialNumber": "SN-12345"}, "MakerNotes": {"ShutterCount": 386}},
    {**metadata(), "EXIF": {**metadata()["EXIF"], "BodySerialNumber": "other-body"}},
    {"EXIF": {**metadata()["EXIF"], "ShutterCount": 387}, "MakerNotes": {"ShutterCount": 386}},
])
def test_ambiguous_or_missing_camera_fields_do_not_match(catalog, tmp_path, change):
    seed(catalog, tmp_path, "a.jpg", "a")
    b = seed(catalog, tmp_path, "b.jpg", "b")
    catalog.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (json.dumps(change), b))
    catalog.commit()
    assert scan_same_shot_candidates(catalog, {})["same_shot_groups"] == []


@pytest.mark.parametrize("field,value", [("SerialNumber", "different-body"), ("Model", "NIKON Z 8"), ("Make", "different-make")])
def test_same_counter_on_a_different_camera_does_not_match(catalog, tmp_path, field, value):
    seed(catalog, tmp_path, "a.jpg", "a")
    other = metadata()
    other["EXIF"][field] = value
    seed(catalog, tmp_path, "b.jpg", "b", exif=other)
    assert scan_same_shot_candidates(catalog, {})["same_shot_groups"] == []


@pytest.mark.parametrize("phash,evidence", [(None, "unavailable"), ("bad", "unavailable"),
                                             ("ffffffffffffffff", "different"), ("123456789ABCDEF0", "matching")])
def test_visual_evidence_is_reported_without_claiming_identical_bytes(catalog, tmp_path, phash, evidence):
    seed(catalog, tmp_path, "a.jpg", "a")
    seed(catalog, tmp_path, "b.jpg", "b", phash=phash)
    group, = scan_same_shot_candidates(catalog, {})["same_shot_groups"]
    assert group["visual_evidence"] == evidence


def test_identical_hashes_and_rejected_or_empty_photos_are_excluded(catalog, tmp_path):
    seed(catalog, tmp_path, "a.jpg", "same")
    b = seed(catalog, tmp_path, "b.jpg", "same")
    c = seed(catalog, tmp_path, "c.jpg", "empty")
    catalog.conn.execute("UPDATE photos SET flag='none' WHERE id=?", (b,))
    catalog.conn.execute("UPDATE photos SET file_size=0 WHERE id=?", (c,))
    catalog.commit()
    assert scan_same_shot_candidates(catalog, {})["same_shot_groups"] == []
    catalog.conn.execute("UPDATE photos SET file_size=1000, flag='rejected' WHERE id=?", (c,))
    catalog.commit()
    assert scan_same_shot_candidates(catalog, {})["same_shot_groups"] == []


@pytest.mark.parametrize("change", ["hash", "path", "identity", "rejected", "deleted"])
def test_restore_and_library_wide_access_revalidate_saved_members(catalog, tmp_path, change):
    a = seed(catalog, tmp_path, "a.jpg", "a")
    b = seed(catalog, tmp_path, "b.jpg", "b")
    result = run_duplicate_scan({}, catalog)
    save_scan(catalog, result)
    assert is_duplicate_review_member(catalog, a)
    assert is_duplicate_review_member(catalog, b)
    sql, args = {
        "hash": ("UPDATE photos SET file_hash='new' WHERE id=?", (b,)),
        "path": ("UPDATE photos SET filename='reused.jpg' WHERE id=?", (b,)),
        "identity": ("UPDATE photos SET exif_data=? WHERE id=?", (json.dumps(metadata(count=387)), b)),
        "rejected": ("UPDATE photos SET flag='rejected' WHERE id=?", (b,)),
        "deleted": ("DELETE FROM photos WHERE id=?", (b,)),
    }[change]
    catalog.conn.execute(sql, args)
    catalog.commit()
    assert revalidate_scan_result(catalog, result)["same_shot_groups"] == []
    assert not is_duplicate_review_member(catalog, a)
    assert not is_duplicate_review_member(catalog, b)


def test_cancel_stops_the_catalog_walk(catalog, tmp_path):
    seed(catalog, tmp_path, "a.jpg", "a")
    seed(catalog, tmp_path, "b.jpg", "b")
    result = scan_same_shot_candidates(catalog, {}, lambda: True)
    assert result["same_shot_examined_photos"] == 0
    assert result["same_shot_groups"] == []


@pytest.mark.parametrize("status,persisted,allowed", [
    ("running", False, False), ("failed", False, False), ("cancelled", False, False),
    ("completed", True, False), ("completed", False, True),
])
def test_only_a_completed_unpersisted_review_can_supply_live_membership(
    catalog, tmp_path, status, persisted, allowed,
):
    a = seed(catalog, tmp_path, "a.jpg", "a")
    seed(catalog, tmp_path, "b.jpg", "b")
    result = scan_same_shot_candidates(catalog, {})
    def jobs():
        return [{"type": "duplicate-scan", "status": status, "_persisted": persisted, "result": result}]

    assert is_duplicate_review_member(catalog, a, jobs) is allowed


def test_real_exiftool_date_correction_remains_discoverable(catalog, tmp_path):
    """Exercise real metadata extraction and rewriting, including Nikon MakerNotes."""
    import imagehash
    from capture_time import adjust_capture_time
    from metadata import extract_metadata, find_exiftool
    from PIL import Image
    from scanner import compute_file_hash

    if not find_exiftool():
        pytest.skip("ExifTool is not installed")
    # A small, valid Nikon type-3 MakerNote containing the actual ShutterCount
    # tag. This avoids depending on a large or copyrighted RAW test fixture.
    note = (b"Nikon\x00\x02\x10\x00\x00II*\x00\x08\x00\x00\x00" +
            struct.pack("<H", 1) + struct.pack("<HHII", 0xa7, 4, 1, 386) + struct.pack("<I", 0))
    exif = Image.Exif()
    exif[0x010f], exif[0x0110] = "NIKON CORPORATION", "NIKON Z 6"
    exif[0x8769] = {0x9003: "2023:01:01 01:02:03", 0xa431: "SN-12345", 0x927c: note}
    original, corrected = tmp_path / "original.jpg", tmp_path / "corrected.jpg"
    Image.effect_noise((128, 96), 50).convert("RGB").save(original, exif=exif)
    shutil.copyfile(original, corrected)
    # Add both before assigning hashes to avoid the unrelated exact auto-resolver.
    ids = []
    for path in (original, corrected):
        meta = extract_metadata([str(path)])[str(path)]
        with Image.open(path) as image:
            phash = str(imagehash.phash(image))
        ids.append(seed(catalog, tmp_path, path.name, str(path), exif=meta, phash=phash,
                        timestamp="2023-01-01T01:02:03"))
    for pid, path in zip(ids, (original, corrected), strict=True):
        catalog.conn.execute("UPDATE photos SET file_hash=?, file_size=? WHERE id=?",
                             (compute_file_hash(str(path)), path.stat().st_size, pid))
    catalog.commit()
    summary = adjust_capture_time(catalog, [ids[1]], mode="manual", shift_minutes=60, keep_backups=False)
    assert summary["updated"] == 1
    assert compute_file_hash(str(original)) != compute_file_hash(str(corrected))
    result = run_duplicate_scan({}, catalog)
    group, = result["same_shot_groups"]
    assert group["shutter_count"] == 386
    assert group["visual_evidence"] == "matching"
    assert {p["timestamp"] for p in group["photos"]} == {"2023-01-01T01:02:03", "2023-01-01T02:02:03"}
    assert result["proposals"] == []


def test_same_shot_api_restore_media_and_cleanup_guards(app_and_db, tmp_path, monkeypatch):
    from unittest.mock import MagicMock

    import web.media as media
    import web.system as system
    from wait import wait_for_job_via_client

    app, db = app_and_db
    active = db.active_workspace_id
    a = seed(db, tmp_path / "a", "a.jpg", "a")
    db.set_active_workspace(db.create_workspace("Other workspace"))
    b = seed(db, tmp_path / "b", "b.jpg", "b")
    db.set_active_workspace(active)
    client = app.test_client()
    job = client.post("/api/duplicates/scan").get_json()["job_id"]
    data = wait_for_job_via_client(client, job)
    assert data["status"] == "completed"
    group, = data["result"]["same_shot_groups"]
    assert {p["id"] for p in group["photos"]} == {a, b}
    restored = client.get("/api/duplicates/last-scan").get_json()["result"]
    assert restored["same_shot_groups"][0]["photos"][1]["workspaces"] == ["Other workspace"]
    monkeypatch.setattr(media, "_serve_photo_thumbnail", lambda *args: ("thumbnail", 200))
    assert client.get(f"/thumbnails/{b}.jpg").status_code == 404
    assert client.get(f"/thumbnails/duplicate/{b}.jpg").status_code == 200
    run = MagicMock(return_value=MagicMock(returncode=0))
    monkeypatch.setattr(system.subprocess, "run", run)
    monkeypatch.setattr(system.sys, "platform", "darwin")
    assert client.post("/api/files/reveal", json={"photo_id": b, "scope": "duplicates"}).status_code == 200
    assert run.call_args.args[0][-1] == str(tmp_path / "b" / "b.jpg")
    # Even a manually rejected candidate has no exact-byte keeper anchor.
    assert client.post("/api/duplicates/keep", json={"photo_id": a}).status_code == 409
    db.conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (b,))
    db.commit()
    response = client.post("/api/duplicates/delete-loser-files", json={"photo_ids": [b]}).get_json()
    assert response["trashed"] == 0
    assert response["skipped"][0]["reason"] == "no duplicate winner exists"
    assert client.get(f"/thumbnails/duplicate/{b}.jpg").status_code == 404


def test_just_completed_review_works_before_history_is_saved(app_and_db, tmp_path, monkeypatch):
    import threading

    import web.media as media
    from testing.waits import synchronization_timeout
    from wait import wait_for_job_via_client

    app, db = app_and_db
    a = seed(db, tmp_path, "a.jpg", "a")
    seed(db, tmp_path, "b.jpg", "b")
    entered, release = threading.Event(), threading.Event()
    runner = app._job_runner
    persist = runner._history.persist_job

    def delayed_persist(job, duration):
        if job["type"] == "duplicate-scan" and job["status"] == "completed":
            entered.set()
            assert release.wait(synchronization_timeout(10))
        return persist(job, duration)

    monkeypatch.setattr(runner._history, "persist_job", delayed_persist)
    monkeypatch.setattr(media, "_serve_photo_thumbnail", lambda *args: ("thumbnail", 200))
    client = app.test_client()
    job = client.post("/api/duplicates/scan").get_json()["job_id"]
    try:
        assert entered.wait(synchronization_timeout(10))
        assert db.job_history.last_completed_with_result("duplicate-scan") is None
        assert client.get(f"/thumbnails/duplicate/{a}.jpg").status_code == 200
    finally:
        release.set()
    assert wait_for_job_via_client(client, job)["status"] == "completed"
