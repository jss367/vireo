"""API tests for the card-cleanup scan/manifest/delete endpoints.

Spec: docs/superpowers/specs/2026-08-07-card-cleanup-design.md

The `app_and_db` fixture comes from vireo/tests/conftest.py — a Flask app
plus its Database, pre-seeded with a couple of folders/photos unrelated to
these tests. Do not redefine it here.
"""
import json
import os
import sys
import threading

import card_cleanup
import pytest
from scanner import compute_file_hash as _sha
from wait import wait_for_job_via_client


def _wait_for_job(client, job_id, timeout=30):
    """Poll GET /api/jobs/<id> until it reaches a terminal state and
    assert it completed."""
    job = wait_for_job_via_client(client, job_id, timeout=timeout)
    assert job["status"] == "completed", job
    return job


def _archive_photo(db, tmp_path, name="IMG_0001.NEF", content=b"raw-one",
                   folder="archive/2026/2026-08-01"):
    """Create an archive file on disk + its cataloged, verified row."""
    folder_path = tmp_path / folder
    folder_path.mkdir(parents=True, exist_ok=True)
    f = folder_path / name
    f.write_bytes(content)
    st = os.stat(f)
    fid = db.add_folder(str(folder_path))
    pid = db.add_photo(
        folder_id=fid, filename=name, extension=os.path.splitext(name)[1],
        file_size=st.st_size, file_mtime=st.st_mtime,
        file_hash=_sha(str(f)),
    )
    db.update_photo_hash_check(pid, "ok")
    return f, pid


def _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one"):
    card = tmp_path / "card" / "DCIM"
    card.mkdir(parents=True, exist_ok=True)
    f = card / name
    f.write_bytes(content)
    return f


def _make_verified_pair(db, tmp_path):
    """Archive file + verified catalog row, plus a matching card file."""
    archive_file, _pid = _archive_photo(db, tmp_path)
    card_file = _card_file(tmp_path)
    return archive_file, card_file


def test_scan_rejects_missing_source(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/card-cleanup/scan", json={"source": "/nope/missing"})
    assert resp.status_code == 400


def test_scan_rejects_archive_overlap(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    db.add_folder(str(archive_root))

    # Exact overlap.
    resp = client.post(
        "/api/card-cleanup/scan", json={"source": str(archive_root)})
    assert resp.status_code == 400
    assert "removable media" in resp.get_json()["error"]

    # Source CONTAINS the archive root.
    resp2 = client.post(
        "/api/card-cleanup/scan", json={"source": str(tmp_path)})
    assert resp2.status_code == 400

    # Source is INSIDE the archive root.
    sub = archive_root / "2026"
    sub.mkdir()
    resp3 = client.post(
        "/api/card-cleanup/scan", json={"source": str(sub)})
    assert resp3.status_code == 400


@pytest.mark.skipif(
    sys.platform not in ("darwin", "win32"),
    reason="case-insensitive overlap only applies on darwin/win32 by default",
)
def test_scan_rejects_case_swapped_overlap(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    archive_root = tmp_path / "Archive"
    archive_root.mkdir()
    db.add_folder(str(archive_root))

    resp = client.post(
        "/api/card-cleanup/scan",
        json={"source": str(tmp_path / "archive")})
    assert resp.status_code == 400


def test_scan_then_manifest_then_delete_end_to_end(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    archive_file, card_file = _make_verified_pair(db, tmp_path)

    resp = client.post(
        "/api/card-cleanup/scan",
        json={"source": str(tmp_path / "card")})
    assert resp.status_code == 200
    scan_job_id = resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)

    manifest_resp = client.get(f"/api/card-cleanup/{scan_job_id}/manifest")
    assert manifest_resp.status_code == 200
    manifest = manifest_resp.get_json()
    assert manifest["totals"]["deletable"]["count"] == 1

    delete_resp = client.post(
        "/api/card-cleanup/delete", json={"scan_job_id": scan_job_id})
    assert delete_resp.status_code == 200
    delete_job_id = delete_resp.get_json()["job_id"]
    _wait_for_job(client, delete_job_id)

    assert not card_file.exists()
    assert archive_file.exists()


def test_scan_job_result_carries_totals_not_entries(app_and_db, tmp_path):
    """Spec: the scan job's RESULT carries only bucket totals, resolved
    source root, and the manifest path — never the (potentially
    multi-MB) per-file entries list. The UI fetches entries from the
    manifest endpoint instead."""
    app, db = app_and_db
    client = app.test_client()
    _make_verified_pair(db, tmp_path)

    resp = client.post(
        "/api/card-cleanup/scan",
        json={"source": str(tmp_path / "card")})
    scan_job_id = resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)

    job_resp = client.get(f"/api/jobs/{scan_job_id}")
    assert job_resp.status_code == 200
    job = job_resp.get_json()
    result = job["result"]
    assert "entries" not in result
    assert result["cancelled"] is False
    assert result["totals"]["deletable"]["count"] == 1
    assert result["source_root"] == os.path.realpath(str(tmp_path / "card"))
    assert result["manifest_path"] == card_cleanup.manifest_path(
        app.config["CARD_CLEANUP_DIR"], scan_job_id)
    assert isinstance(result["walk_errors"], int)


def test_delete_unknown_scan_job_404(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/card-cleanup/delete", json={"scan_job_id": "nope"})
    assert resp.status_code == 404


def test_delete_refuses_cancelled_scan(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at) "
        "VALUES (?, ?, ?, ?)",
        ("scan-c", "card-cleanup-scan", "cancelled", "2026-08-08T00:00:00"),
    )
    db.conn.commit()
    resp = client.post(
        "/api/card-cleanup/delete", json={"scan_job_id": "scan-c"})
    assert resp.status_code == 400


def test_delete_refuses_running_scan_without_telling_user_to_rescan(
        app_and_db):
    """A delete requested while the scan is still going is early, not
    doomed — the error must say to wait, not to re-scan."""
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at) "
        "VALUES (?, ?, ?, ?)",
        ("scan-run", "card-cleanup-scan", "running", "2026-08-08T00:00:00"),
    )
    db.conn.commit()
    resp = client.post(
        "/api/card-cleanup/delete", json={"scan_job_id": "scan-run"})
    assert resp.status_code == 400
    error = resp.get_json()["error"]
    assert "still running" in error
    assert "re-scan" not in error


def test_manifest_unknown_scan_job_404(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    resp = client.get("/api/card-cleanup/nope/manifest")
    assert resp.status_code == 404


def test_delete_after_restart_uses_history_and_disk_manifest(
        app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    archive_file, card_file = _make_verified_pair(db, tmp_path)

    manifest_dir = app.config["CARD_CLEANUP_DIR"]
    result = card_cleanup.scan_card(
        db, str(tmp_path / "card"), True, manifest_dir, "scan-r")
    assert result["totals"]["deletable"]["count"] == 1

    db.conn.execute(
        "INSERT INTO job_history "
        "(id, type, status, started_at, finished_at) "
        "VALUES (?, ?, ?, ?, ?)",
        ("scan-r", "card-cleanup-scan", "completed",
         "2026-08-08T00:00:00", "2026-08-08T00:00:01"),
    )
    db.conn.commit()

    delete_resp = client.post(
        "/api/card-cleanup/delete", json={"scan_job_id": "scan-r"})
    assert delete_resp.status_code == 200
    delete_job_id = delete_resp.get_json()["job_id"]
    _wait_for_job(client, delete_job_id)

    assert not card_file.exists()
    assert archive_file.exists()


def test_delete_expired_manifest_404(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    _make_verified_pair(db, tmp_path)

    resp = client.post(
        "/api/card-cleanup/scan",
        json={"source": str(tmp_path / "card")})
    scan_job_id = resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)

    manifest_dir = app.config["CARD_CLEANUP_DIR"]
    mpath = card_cleanup.manifest_path(manifest_dir, scan_job_id)
    with open(mpath, encoding="utf-8") as f:
        manifest = json.load(f)
    from datetime import UTC, datetime, timedelta
    manifest["created_at"] = (
        datetime.now(UTC) - timedelta(days=8)).isoformat()
    with open(mpath, "w", encoding="utf-8") as f:
        json.dump(manifest, f)

    delete_resp = client.post(
        "/api/card-cleanup/delete", json={"scan_job_id": scan_job_id})
    assert delete_resp.status_code == 404
    assert "re-scan" in delete_resp.get_json()["error"]


def test_delete_concurrent_delete_409(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    client = app.test_client()
    _make_verified_pair(db, tmp_path)

    resp = client.post(
        "/api/card-cleanup/scan",
        json={"source": str(tmp_path / "card")})
    scan_job_id = resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)

    started = threading.Event()
    release = threading.Event()
    real_delete_verified = card_cleanup.delete_verified

    def blocking_delete_verified(db_, manifest, progress_cb=None,
                                 should_cancel=None):
        started.set()
        release.wait(timeout=15)
        return {
            "deleted": 0, "deleted_bytes": 0, "skipped": [], "failed": [],
            "cancelled": False, "remaining": 0,
        }

    monkeypatch.setattr(
        card_cleanup, "delete_verified", blocking_delete_verified)
    # Bound before the try so a failure on the first POST surfaces as
    # itself, not as a NameError raised from the finally drain.
    job1_id = None
    try:
        resp1 = client.post(
            "/api/card-cleanup/delete", json={"scan_job_id": scan_job_id})
        assert resp1.status_code == 200
        job1_id = resp1.get_json()["job_id"]
        assert started.wait(timeout=15)

        runner = app._job_runner

        def _running():
            job = runner.get(job1_id)
            return job is not None and job.get("status") == "running"
        import time
        deadline = time.monotonic() + 15
        while not _running() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert _running()

        resp2 = client.post(
            "/api/card-cleanup/delete", json={"scan_job_id": scan_job_id})
        assert resp2.status_code == 409
    finally:
        release.set()
        monkeypatch.setattr(
            card_cleanup, "delete_verified", real_delete_verified)
        if job1_id:
            _wait_for_job(client, job1_id)


def test_scan_rejects_relative_or_nondir_source(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()

    resp = client.post(
        "/api/card-cleanup/scan", json={"source": "relative/path"})
    assert resp.status_code == 400

    a_file = tmp_path / "not_a_dir.txt"
    a_file.write_text("x")
    resp2 = client.post(
        "/api/card-cleanup/scan", json={"source": str(a_file)})
    assert resp2.status_code == 400

    resp3 = client.post("/api/card-cleanup/scan", json={})
    assert resp3.status_code == 400


def test_scan_rejects_non_boolean_recursive(app_and_db, tmp_path):
    # bool("false") is True — stringly-typed clients must get a 400, not
    # the opposite of what they asked for.
    app, _ = app_and_db
    card = tmp_path / "card"
    card.mkdir()
    resp = app.test_client().post(
        "/api/card-cleanup/scan",
        json={"source": str(card), "recursive": "false"})
    assert resp.status_code == 400
    assert "boolean" in resp.get_json()["error"]


def test_delete_result_carries_exact_totals(app_and_db, tmp_path):
    # The job result bounds skipped/failed to a sample; *_total fields
    # carry the exact counts the UI renders.
    app, db = app_and_db
    client = app.test_client()
    _make_verified_pair(db, tmp_path)
    resp = client.post("/api/card-cleanup/scan",
                       json={"source": str(tmp_path / "card")})
    scan_job_id = resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)
    resp = client.post("/api/card-cleanup/delete",
                       json={"scan_job_id": scan_job_id})
    job_id = resp.get_json()["job_id"]
    _wait_for_job(client, job_id)
    result = client.get(f"/api/jobs/{job_id}").get_json()["result"]
    assert result["skipped_total"] == len(result["skipped"]) == 0
    assert result["failed_total"] == len(result["failed"]) == 0
    assert result["deleted"] == 1


def test_endpoints_reject_non_object_json_body(app_and_db):
    # get_json returns 5 for a valid non-object JSON document; the
    # endpoints must 400, not 500 on body.get.
    app, _ = app_and_db
    client = app.test_client()
    for url in ("/api/card-cleanup/scan", "/api/card-cleanup/delete"):
        resp = client.post(url, json=5)
        assert resp.status_code == 400, url
        assert "JSON object" in resp.get_json()["error"]
