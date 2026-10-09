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
from page_scripts import page_with_scripts
from scanner import compute_file_hash as _sha
from testing.waits import synchronization_timeout
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
    db.audit.update_photo_hash_check(pid, "ok")
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
        "/api/card-cleanup/delete",
        json={"scan_job_id": scan_job_id,
              "manifest_revision": manifest["revision"]})
    assert delete_resp.status_code == 200
    delete_job_id = delete_resp.get_json()["job_id"]
    _wait_for_job(client, delete_job_id)

    assert not card_file.exists()
    assert archive_file.exists()


def test_scoped_verify_endpoint_promotes_preview_without_global_audit(
        app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    archive_file, pid = _archive_photo(db, tmp_path)
    unrelated, unrelated_pid = _archive_photo(
        db, tmp_path, name="OTHER.NEF", content=b"other",
        folder="archive/other")
    db.conn.execute(
        "UPDATE photos SET hash_status = NULL, hash_checked_at = NULL "
        "WHERE id IN (?, ?)", (pid, unrelated_pid))
    db.conn.commit()
    _card_file(tmp_path)

    scan_resp = client.post(
        "/api/card-cleanup/scan", json={"source": str(tmp_path / "card")})
    scan_job_id = scan_resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)
    before = client.get(
        f"/api/card-cleanup/{scan_job_id}/manifest").get_json()
    assert before["totals"]["deletable"]["count"] == 0

    verify_resp = client.post(
        "/api/card-cleanup/verify", json={"scan_job_id": scan_job_id})
    assert verify_resp.status_code == 200
    verify_job_id = verify_resp.get_json()["job_id"]
    job = _wait_for_job(client, verify_job_id)
    assert job["type"] == "card-cleanup-verify"
    assert job["result"]["archive_files_read"] == 1
    assert job["result"]["unblocked_files"] == 1

    after = client.get(
        f"/api/card-cleanup/{scan_job_id}/manifest").get_json()
    assert after["totals"]["deletable"]["count"] == 1
    deletable = [e for e in after["entries"] if e["bucket"] == "deletable"]
    assert [e["archive_path"] for e in deletable] == [str(archive_file)]
    assert unrelated.exists()
    unrelated_status = db.conn.execute(
        "SELECT hash_status FROM photos WHERE id = ?", (unrelated_pid,)
    ).fetchone()["hash_status"]
    assert unrelated_status is None


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


@pytest.mark.parametrize("status", ["running", "pausing", "paused"])
def test_delete_refuses_running_scan_without_telling_user_to_rescan(
        app_and_db, status):
    """A delete requested while the scan is still going is early, not
    doomed — the error must say to wait, not to re-scan."""
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at) "
        "VALUES (?, ?, ?, ?)",
        ("scan-run", "card-cleanup-scan", status, "2026-08-08T00:00:00"),
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

    manifest = client.get(
        "/api/card-cleanup/scan-r/manifest").get_json()
    delete_resp = client.post(
        "/api/card-cleanup/delete",
        json={"scan_job_id": "scan-r",
              "manifest_revision": manifest["revision"]})
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
        "/api/card-cleanup/delete",
        json={"scan_job_id": scan_job_id,
              "manifest_revision": manifest["revision"]})
    assert delete_resp.status_code == 404
    assert "re-scan" in delete_resp.get_json()["error"]


@pytest.mark.parametrize("status", ["running", "pausing", "paused"])
def test_delete_concurrent_delete_409(app_and_db, tmp_path, monkeypatch, status):
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
        release.wait(timeout=synchronization_timeout(15))
        if should_cancel:
            should_cancel()
        return {
            "deleted": 0, "deleted_bytes": 0, "skipped": [], "failed": [],
            "cancelled": False, "remaining": 0,
        }

    monkeypatch.setattr(
        card_cleanup, "delete_verified", blocking_delete_verified)
    manifest = client.get(
        f"/api/card-cleanup/{scan_job_id}/manifest").get_json()
    # Bound before the try so a failure on the first POST surfaces as
    # itself, not as a NameError raised from the finally drain.
    job1_id = None
    try:
        resp1 = client.post(
            "/api/card-cleanup/delete",
            json={"scan_job_id": scan_job_id,
                  "manifest_revision": manifest["revision"]})
        assert resp1.status_code == 200
        job1_id = resp1.get_json()["job_id"]
        assert started.wait(timeout=synchronization_timeout(15))

        runner = app._job_runner

        def _running():
            job = runner.get(job1_id)
            return job is not None and job.get("status") == "running"
        import time
        deadline = time.monotonic() + 15
        while not _running() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert _running()
        if status != "running":
            assert runner.pause_job(job1_id)
            if status == "paused":
                release.set()
                deadline = time.monotonic() + 15
                while runner.get(job1_id)["status"] != "paused" and time.monotonic() < deadline:
                    time.sleep(0.01)
            assert runner.get(job1_id)["status"] == status

        resp2 = client.post(
            "/api/card-cleanup/delete",
            json={"scan_job_id": scan_job_id,
                  "manifest_revision": manifest["revision"]})
        assert resp2.status_code == 409
    finally:
        release.set()
        monkeypatch.setattr(
            card_cleanup, "delete_verified", real_delete_verified)
        if job1_id:
            app._job_runner.resume_job(job1_id)
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
    manifest = client.get(
        f"/api/card-cleanup/{scan_job_id}/manifest").get_json()
    resp = client.post("/api/card-cleanup/delete",
                       json={"scan_job_id": scan_job_id,
                             "manifest_revision": manifest["revision"]})
    job_id = resp.get_json()["job_id"]
    _wait_for_job(client, job_id)
    result = client.get(f"/api/jobs/{job_id}").get_json()["result"]
    assert result["skipped_total"] == len(result["skipped"]) == 0
    assert result["failed_total"] == len(result["failed"]) == 0
    assert result["deleted"] == 1


def test_scan_manifest_carries_initial_revision(app_and_db, tmp_path):
    """Codex P1: the first manifest a scan writes must carry
    INITIAL_MANIFEST_REVISION so the delete endpoint's revision check
    has something concrete to compare against on the very first pass."""
    app, db = app_and_db
    client = app.test_client()
    _make_verified_pair(db, tmp_path)
    resp = client.post("/api/card-cleanup/scan",
                       json={"source": str(tmp_path / "card")})
    scan_job_id = resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)
    manifest = client.get(
        f"/api/card-cleanup/{scan_job_id}/manifest").get_json()
    assert manifest["revision"] == card_cleanup.INITIAL_MANIFEST_REVISION


def test_verify_bumps_manifest_revision(app_and_db, tmp_path):
    """Codex P1: a completed verify must land a fresh revision so any
    still-open confirmation from before the verify fails the delete-side
    revision check instead of quietly deleting newly-promoted files."""
    app, db = app_and_db
    client = app.test_client()
    _archive_photo(db, tmp_path)
    db.conn.execute(
        "UPDATE photos SET hash_status = NULL, hash_checked_at = NULL")
    db.conn.commit()
    _card_file(tmp_path)
    scan_resp = client.post(
        "/api/card-cleanup/scan", json={"source": str(tmp_path / "card")})
    scan_job_id = scan_resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)

    before = client.get(
        f"/api/card-cleanup/{scan_job_id}/manifest").get_json()
    verify_resp = client.post(
        "/api/card-cleanup/verify", json={"scan_job_id": scan_job_id})
    _wait_for_job(client, verify_resp.get_json()["job_id"])
    after = client.get(
        f"/api/card-cleanup/{scan_job_id}/manifest").get_json()
    assert after["revision"] == before["revision"] + 1


@pytest.mark.parametrize("count", [1, 2])
@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_verify_pauses_while_rechecking_manifest(
    app_and_db, tmp_path, monkeypatch, count, action,
):
    from db import Database
    from wait import wait_for_job

    app, db = app_and_db
    card_paths = set()
    for i in range(count):
        name = f"IMG_{i}.NEF"
        _archive_photo(db, tmp_path, name=name)
        card_paths.add(str(_card_file(tmp_path, name=name)))
    manifest_dir = str(tmp_path / "manifests")
    manifest = card_cleanup.scan_card(
        db, str(tmp_path / "card"), True, manifest_dir, "scan-pause",
    )
    manifest_path = card_cleanup.manifest_path(manifest_dir, "scan-pause")
    with open(manifest_path, "rb") as f:
        before = f.read()
    entered, release = threading.Event(), threading.Event()
    checked = []
    original = card_cleanup.os.lstat

    def lstat(path, *args, **kwargs):
        st = original(path, *args, **kwargs)
        if str(path) in card_paths:
            checked.append(str(path))
            if len(checked) == 1:
                entered.set()
                assert release.wait(synchronization_timeout(5))
        return st

    monkeypatch.setattr(card_cleanup.os, "lstat", lstat)
    runner = app._job_runner

    def work(job):
        thread_db = Database(db._db_path)
        thread_db.set_active_workspace(db._ws_id())
        try:
            return card_cleanup.verify_manifest_archives(
                thread_db, manifest, manifest_dir,
                should_cancel=lambda: runner.is_cancelled(job["id"]),
            )
        finally:
            thread_db.close()

    job_id = runner.start("card-cleanup-verify", work, pausable=True)
    client = app.test_client()
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        wait_for_job(lambda: runner.get(job_id), terminal=("paused",), timeout=5)
        assert len(checked) == 1
        with open(manifest_path, "rb") as f:
            assert f.read() == before
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        job = wait_for_job_via_client(client, job_id)
        assert job["status"] == ("completed" if action == "resume" else "cancelled")
        with open(manifest_path, "rb") as f:
            after = f.read()
        if action == "resume":
            assert len(checked) == count
            assert json.loads(after)["revision"] == json.loads(before)["revision"] + 1
        else:
            assert len(checked) == 1
            assert after == before
    finally:
        release.set()
        runner.cancel_job(job_id)


def test_verify_endpoint_loads_manifest_inside_cleanup_job_lock():
    """Codex P1 (commit ad9375db): the verify endpoint must load the
    manifest INSIDE the ``_CARD_CLEANUP_JOB_LOCK`` critical section, not
    before it.

    Race: request A loads revision N, releases before taking the lock,
    then request B runs to completion and writes revision N+1. If A
    then enters the lock (B is no longer registered as running), passes
    the conflict check, and hands its stale revision-N manifest to
    verify_manifest_archives, the worker bumps to N+1 and writes it —
    silently clobbering B's promotions with entries the user never saw
    promoted. The conflict check alone does NOT close this window
    because the check catches only queued/running jobs, and A can enter
    the lock after B has finished.

    Load-inside-lock closes the window: every verify starts from the
    manifest that the previous verify's write landed. Pinning this via
    source inspection because reproducing the race in a functional test
    would need scheduler hooks we do not have; a future refactor that
    quietly hoists load_manifest above the ``with`` block would
    reintroduce the exact bug this commit fixed.
    """
    import inspect

    from web.card_cleanup import create_card_cleanup_blueprint

    src = inspect.getsource(create_card_cleanup_blueprint)
    start = src.index("def api_card_cleanup_verify(")
    end = src.index("def api_card_cleanup_delete(", start)
    verify_src = src[start:end]

    lock_idx = verify_src.index("with _CARD_CLEANUP_JOB_LOCK")
    load_idx = verify_src.index("card_cleanup.load_manifest(")
    conflict_idx = verify_src.index(
        "_card_cleanup_job_conflict_response(")
    start_idx = verify_src.index("ctx.start_job(")

    assert lock_idx < conflict_idx < load_idx < start_idx, (
        "api_card_cleanup_verify must run these steps IN ORDER inside "
        "the _CARD_CLEANUP_JOB_LOCK critical section: (1) take the "
        "lock, (2) call _card_cleanup_job_conflict_response, (3) load "
        "the manifest, (4) ctx.start_job. Hoisting load_manifest above "
        "the `with` block reopens the load-before-lock window Codex "
        "P1 called out at commit ad9375db: a verify can enter after "
        "another verify has completed, see no conflict, and hand a "
        "stale snapshot to verify_manifest_archives that clobbers the "
        "just-landed revision."
    )


def test_delete_rejects_missing_manifest_revision(app_and_db, tmp_path):
    """The revision check is required on any completed scan.  A body
    that omits it must 400 rather than defaulting silently."""
    app, db = app_and_db
    client = app.test_client()
    _make_verified_pair(db, tmp_path)
    scan_resp = client.post(
        "/api/card-cleanup/scan", json={"source": str(tmp_path / "card")})
    scan_job_id = scan_resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)
    resp = client.post(
        "/api/card-cleanup/delete", json={"scan_job_id": scan_job_id})
    assert resp.status_code == 400
    assert "manifest_revision" in resp.get_json()["error"]


def test_delete_rejects_stale_manifest_revision_after_verify(
        app_and_db, tmp_path):
    """Codex P1: a delete that confirms an older revision after verify
    has rewritten the manifest must be rejected with a distinguishable
    code so the UI can hand the user back to the refreshed preview."""
    app, db = app_and_db
    client = app.test_client()
    _archive_photo(db, tmp_path)
    db.conn.execute(
        "UPDATE photos SET hash_status = NULL, hash_checked_at = NULL")
    db.conn.commit()
    _card_file(tmp_path)
    scan_resp = client.post(
        "/api/card-cleanup/scan", json={"source": str(tmp_path / "card")})
    scan_job_id = scan_resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)
    stale = client.get(
        f"/api/card-cleanup/{scan_job_id}/manifest").get_json()
    stale_revision = stale["revision"]

    # Verify promotes the unchecked archive row and bumps the revision.
    verify_resp = client.post(
        "/api/card-cleanup/verify", json={"scan_job_id": scan_job_id})
    _wait_for_job(client, verify_resp.get_json()["job_id"])

    resp = client.post(
        "/api/card-cleanup/delete",
        json={"scan_job_id": scan_job_id,
              "manifest_revision": stale_revision})
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["code"] == "manifest_revision_mismatch"


def test_endpoints_reject_non_object_json_body(app_and_db):
    # get_json returns 5 for a valid non-object JSON document; the
    # endpoints must 400, not 500 on body.get.
    app, _ = app_and_db
    client = app.test_client()
    for url in ("/api/card-cleanup/scan", "/api/card-cleanup/verify",
                "/api/card-cleanup/delete"):
        resp = client.post(url, json=5)
        assert resp.status_code == 400, url
        assert "JSON object" in resp.get_json()["error"]


def test_card_cleanup_page_renders(app_and_db):
    # The flow lives on its own page now (spec:
    # docs/superpowers/specs/2026-08-08-card-cleanup-page-design.md).
    app, _ = app_and_db
    resp = app.test_client().get("/card-cleanup")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "card-cleanup-section" in body
    assert "card-cleanup-scan-btn" in body
    assert "card-cleanup-verify-btn" in body


def test_import_and_card_cleanup_share_folder_browser(app_and_db):
    """Both pages use one browser implementation, partial, and stylesheet.

    This pins the architectural reason for the refactor: count rendering and
    navigation fixes must land once rather than drift between template copies.
    """
    app, _ = app_and_db
    client = app.test_client()
    import_body = client.get("/import").get_data(as_text=True)
    cleanup_body = client.get("/card-cleanup").get_data(as_text=True)
    for body in (import_body, cleanup_body):
        assert '/static/vireo-folder-browser.js' in body
        assert '/static/vireo-folder-browser.css' in body
        assert 'data-folder-browser-action="select"' in body
    assert client.get("/static/vireo-folder-browser.js").status_code == 200
    assert client.get("/static/vireo-folder-browser.css").status_code == 200


def test_import_page_links_to_card_cleanup_instead_of_hosting_it(app_and_db):
    app, _ = app_and_db
    resp = app.test_client().get("/import")
    assert resp.status_code == 200
    body = page_with_scripts(app.test_client(), "/import")
    assert "card-cleanup-section" not in body
    # The entry point stays: it now navigates to the dedicated page.
    assert "btnFreeUpCardSpace" in body
    assert "/card-cleanup" in body


def test_verification_callout_reason_stays_in_sync(app_and_db):
    """The scoped verification callout and backend reason stay coupled."""
    app, _ = app_and_db
    matched = card_cleanup.KEEP_NOT_VERIFIED
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    assert f"CARD_CLEANUP_VERIFY_REASON = '{matched}'" in body
    assert "/api/card-cleanup/verify" in body
    assert "/api/jobs/verify-hashes" not in body


def test_hash_failed_callout_reason_stays_in_sync(app_and_db):
    """The hash-failed callout has the same shape of coupling as the
    audit callout (Codex P2 review): the page filters kept entries by a
    tail of KEEP_ARCHIVE_HASH_FAILED. Pin both ends so a reword on either
    side stops silently. Also assert the two literals do NOT overlap —
    if they did, a KEEP_ARCHIVE_HASH_FAILED entry would count toward the
    audit callout and get double-remediated."""
    app, _ = app_and_db
    matched = "see the Audit page"
    assert matched in card_cleanup.KEEP_ARCHIVE_HASH_FAILED
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    assert f"CARD_CLEANUP_HASH_FAILED_REASON = '{matched}'" in body
    assert matched not in card_cleanup.KEEP_NOT_VERIFIED
    assert "run the integrity audit" not in card_cleanup.KEEP_ARCHIVE_HASH_FAILED


def test_hash_failed_callout_states_audit_workspace_scope(app_and_db):
    """Second-order Codex P2 review: the audit remedy suggested by the
    hash-failed callout is only reachable when the failed row is in the
    active workspace, because db.audit.get_integrity_flagged() filters
    to the active workspace but _load_catalog_by_hash() matches globally.
    The callout must say so up front so the user does not click through
    to an empty Audit page — mirroring the workspace-scope note added
    to the verify-hashes callout for the same asymmetry."""
    app, _ = app_and_db
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    # A dedicated hint element for the scope note, rendered inside the
    # hash-failed callout container.
    assert 'id="card-cleanup-hash-failed-scope"' in body
    # The copy itself: names the current-workspace scope of the Audit
    # page and tells the user how to reach a failed row in a different
    # workspace's folders. Pinned literally so a reword does not quietly
    # drop the guidance the Codex P2 asked for.
    assert "Audit page shows flagged rows for the current workspace only" in body
    assert "switch to that workspace to find it there" in body


def test_finish_scoped_verification_refreshes_preview_without_rescan(
        app_and_db):
    """Scoped verification only promotes pending rows, so it can atomically
    refresh the existing manifest instead of forcing another card scan."""
    app, _ = app_and_db
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    finish = body.index("async function cardCleanupFinishVerification(")
    end = body.index("function cardCleanupBindBucket(", finish)
    section = body[finish:end]
    assert "await cardCleanupLoadManifest()" in section
    assert section.index("await cardCleanupLoadManifest()") < section.index(
        "cardCleanupSetBusy(false")
    # After the Codex P1 flicker fix, keepDeleteState is truthy for any
    # completed/cancelled verification so cardCleanupSetBusy never
    # restores the pre-verification Delete snapshot — success re-renders
    # via cardCleanupLoadManifest, failure explicitly disables below.
    assert "keepDeleteState: attemptedReload || refreshed" in section
    assert "re-scan the card before deleting" not in section


def test_finish_scoped_verification_locks_delete_when_manifest_reload_fails(
        app_and_db):
    """Codex P1 review: if verification finishes but the refreshed manifest
    cannot be loaded, keep Delete disabled and require a reload or re-scan.

    Verification can promote kept rows to deletable. When it does, the
    pre-verification Delete state understates what is now on disk: a
    click would confirm the old totals while /api/card-cleanup/delete
    operates on the freshly-promoted set too. The prior implementation
    called cardCleanupSetBusy(false, {keepDeleteState: false}) on reload
    failure, which restored the pre-verification busyRestore snapshot —
    including Delete enabled with the old count. It also unconditionally
    told the user "Preview updated" on a completed status even when the
    updated preview never rendered.

    Pin the recovered behaviour so a future refactor cannot silently
    re-open the gap: on the failure branch the Delete button must be
    explicitly disabled with a hint that directs the user to reload or
    scan again, and the "Preview updated" copy must be gated on the
    reload succeeding.
    """
    app, _ = app_and_db
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    finish = body.index("async function cardCleanupFinishVerification(")
    end = body.index("function cardCleanupBindBucket(", finish)
    section = body[finish:end]
    # The failure branch must explicitly force Delete off — a plain
    # setBusy(false) restore is not enough because busyRestore holds the
    # pre-verification Delete state (possibly enabled with stale counts).
    assert "deleteBtn.disabled = true" in section, (
        "cardCleanupFinishVerification must explicitly disable Delete "
        "when the post-verification manifest reload fails; otherwise the "
        "pre-verification Delete state (possibly enabled with stale "
        "counts) is what the user sees"
    )
    # The delete-hint must tell the user why Delete is off and how to
    # recover (reload / re-scan). Match the actionable phrase.
    assert "reload the page or scan again before deleting" in section, (
        "the delete hint must direct the user to reload or scan again "
        "before deletion is possible after a failed manifest reload"
    )
    # The "Preview updated" copy must only appear on the refreshed
    # branch — pin the conditional so a rewrite cannot drop back to the
    # unconditional message that was misleading before this fix.
    completed_start = section.index("if (status === 'completed'")
    completed_end = section.index("else if (status === 'cancelled')", completed_start)
    completed_block = section[completed_start:completed_end]
    assert "refreshed" in completed_block and "Preview updated" in completed_block, (
        "the completed-status branch must gate 'Preview updated' on the "
        "refreshed flag so the message is only shown after the reload "
        "actually rendered the new manifest"
    )
    # The cancelled branch must also gate its "preview was updated" copy
    # on the same flag.
    cancelled_start = section.index("else if (status === 'cancelled')")
    cancelled_end = section.index("else if (status)", cancelled_start)
    cancelled_block = section[cancelled_start:cancelled_end]
    assert "refreshed" in cancelled_block, (
        "the cancelled-status branch must gate its 'preview was updated' "
        "copy on the refreshed flag so a failed reload after cancel does "
        "not still claim the preview reflects the completed checks"
    )


def test_confirm_delete_locks_page_before_post(app_and_db):
    """Codex P2 reviews (commits 96137e3 and 012aec2c): cardCleanupConfirmDelete
    must set the page-wide busy state *before* the POST /api/card-cleanup/delete
    request is issued, and its failure paths must treat HTTP failures and
    network errors differently.

    Ordering: the confirm dialog can be dismissed (backdrop click or Escape)
    while the request is pending. If Scan and Verify remained enabled during
    that window, the user could kick off a second job whose watch would
    overwrite this delete's jobId — the exact orphan the single-busy-state
    owner exists to prevent, and one that could strand a destructive
    deletion. Busy must be set before the fetch so a dismissal never opens
    the gap.

    Failure handling: an HTTP response proves whether the server queued the
    delete, so !resp.ok can hand the buttons back. A `fetch` rejection is
    ambiguous — the POST may have reached the server and started the
    destructive job before the connection dropped — so the catch must NOT
    unlock, and must direct the user to the Jobs page. Pinning both here so
    a refactor can't quietly re-open the gap."""
    app, _ = app_and_db
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    start = body.index("async function cardCleanupConfirmDelete(")
    end = body.index("function cardCleanupRenderDeleteResult(", start)
    section = body[start:end]
    # Busy must be set BEFORE the fetch — locate both and require the
    # setBusy call to come first.
    fetch_idx = section.index("/api/card-cleanup/delete")
    busy_true_idx = section.index("cardCleanupSetBusy(true")
    assert busy_true_idx < fetch_idx, (
        "cardCleanupSetBusy(true) must run before the delete POST is issued "
        "so a dialog dismissal during the pending request can't leave "
        "Scan/Verify live and race a second job into cardCleanupWatchJob"
    )
    # An HTTP response that proves nothing was queued (!resp.ok) must
    # release the lock so the user isn't stranded with a dead page.
    ok_branch_start = section.index("if (!resp.ok)")
    ok_branch_end = section.index("cardCleanupWatchJob", ok_branch_start)
    ok_branch = section[ok_branch_start:ok_branch_end]
    assert "cardCleanupSetBusy(false)" in ok_branch, (
        "the !resp.ok branch must call cardCleanupSetBusy(false) — the "
        "server said the delete was not queued, so the page can safely hand "
        "the buttons back"
    )
    # The catch branch is different: `fetch` rejects for network errors
    # where the request may or may not have reached the server. If the
    # server already queued the delete, the destructive job is running
    # unseen — unlocking would let a second job overwrite this page's
    # jobId (the exact orphan the single-busy-state owner exists to
    # prevent). Codex P2 (commit 012aec2c) called this out: only unlock
    # after an HTTP response proves no job was queued; treat network
    # errors as unknown-running and direct the user to Jobs.
    catch_start = section.index("} catch (e) {")
    catch_end = section.index("} finally {", catch_start)
    catch_body = section[catch_start:catch_end]
    assert "cardCleanupSetBusy(false)" not in catch_body, (
        "the network-error catch must NOT release the page lock — the "
        "server may have queued the delete before the connection dropped, "
        "and unlocking would let a second job race the destructive job"
    )
    assert "Jobs page" in catch_body, (
        "the network-error catch must direct the user to the Jobs page so "
        "they can find out whether the server queued the delete before the "
        "connection dropped"
    )


def test_confirm_delete_defaults_manifest_revision_for_pre_upgrade_manifests(
        app_and_db):
    """Codex P2 (commit ad9375db): cardCleanupConfirmDelete must send a
    concrete manifest_revision even when the loaded manifest has no
    revision key.

    A manifest saved before the revision schema landed has no ``revision``
    property. If the client reads it as ``undefined``, JSON.stringify
    drops the key from the request body, and the server rejects a missing
    manifest_revision with 400 required — making every carried-over
    preview undeletable. The server treats a missing on-disk revision as
    ``INITIAL_MANIFEST_REVISION`` (1), so the client must default to the
    same value to keep the two sides in sync.
    """
    app, _ = app_and_db
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    start = body.index("async function cardCleanupConfirmDelete(")
    end = body.index("function cardCleanupRenderDeleteResult(", start)
    section = body[start:end]
    # Locate the confirmedRevision assignment and require a truthy
    # fallback to 1 so an undefined/absent manifest.revision does not
    # silently drop the key from the request body.
    rev_idx = section.index("const confirmedRevision")
    body_idx = section.index("manifest_revision:", rev_idx)
    rev_expr = section[rev_idx:body_idx]
    assert "|| 1" in rev_expr, (
        "cardCleanupConfirmDelete must default confirmedRevision to 1 "
        "(INITIAL_MANIFEST_REVISION) when cardCleanupState.manifest.revision "
        "is undefined — sending undefined here would omit manifest_revision "
        "from JSON.stringify and the delete endpoint would reject a "
        "pre-upgrade manifest that has no revision key with 400 required, "
        "even though the server treats a missing on-disk revision as the "
        "initial revision on load"
    )


def test_confirm_delete_awaits_manifest_reload_on_revision_mismatch(
        app_and_db):
    """Codex P1 review: cardCleanupConfirmDelete must keep the page locked
    until the post-mismatch manifest reload finishes.

    When /api/card-cleanup/delete returns 409 manifest_revision_mismatch
    the on-disk manifest has already moved past the revision the user
    just confirmed against — a concurrent verify (from another tab or
    another client) promoted new files. The page must reload the fresh
    manifest so the preview and Delete button describe what would
    actually be deleted next.

    The prior flow ran cardCleanupSetBusy(false) first (re-enabling
    Delete against the pre-mismatch cardCleanupState.manifest), then
    kicked cardCleanupLoadManifest() off as fire-and-forget. In the
    gap before the reload landed a user could reopen the confirm
    dialog with the old totals; once the reload swapped the manifest,
    confirming would send the new revision and the server would accept
    deletion of the newly promoted files that never appeared in the
    dialog.

    Pin the closed gap: the mismatch branch must await
    cardCleanupLoadManifest, keep the busy state during the await,
    unlock with keepDeleteState so the freshly rendered Delete state
    survives, and explicitly disable Delete on reload failure so a
    stale preview cannot authorize a deletion.
    """
    app, _ = app_and_db
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    start = body.index("async function cardCleanupConfirmDelete(")
    end = body.index("function cardCleanupRenderDeleteResult(", start)
    section = body[start:end]
    # Locate the mismatch branch.
    branch_start = section.index("manifest_revision_mismatch")
    branch_end = section.index("} else {", branch_start)
    branch = section[branch_start:branch_end]
    # The reload must be awaited, not fire-and-forget.
    assert "await cardCleanupLoadManifest()" in branch, (
        "the manifest_revision_mismatch branch must await "
        "cardCleanupLoadManifest — a fire-and-forget reload lets the "
        "user reopen the confirm dialog against the pre-mismatch "
        "manifest in the gap before the response lands, and the "
        "next confirmation sends the new revision the server will "
        "accept for the newly promoted set"
    )
    # The unlock must run AFTER the await and must use keepDeleteState
    # so the freshly rendered Delete state is not overwritten by
    # busyRestore.
    reload_idx = branch.index("await cardCleanupLoadManifest()")
    unlock_idx = branch.index("cardCleanupSetBusy(false", reload_idx)
    assert unlock_idx > reload_idx, (
        "cardCleanupSetBusy(false, …) must run after awaiting the "
        "reload — otherwise the page unlocks before the fresh manifest "
        "has replaced the pre-mismatch state and the race the P1 fix "
        "closes reopens"
    )
    assert "keepDeleteState: true" in branch[unlock_idx:], (
        "the post-reload cardCleanupSetBusy(false, …) must pass "
        "keepDeleteState so busyRestore does not overwrite the "
        "Delete-button state that cardCleanupRenderManifest just set "
        "from the fresh totals"
    )
    # The failure path must explicitly disable Delete — busyRestore
    # was skipped and the render never ran, so Delete would still
    # carry setBusy(true)'s disabled=true; pinning the explicit disable
    # here so a future refactor cannot leave it dangling.
    assert "deleteBtn.disabled = true" in branch, (
        "the reload-failure branch must explicitly disable the Delete "
        "button — a stale preview must not be able to authorize a "
        "deletion when the fresh manifest could not be loaded"
    )
    # And it must tell the user why Delete is off and how to recover.
    assert "Reload the page or scan again before deleting" in branch, (
        "the reload-failure hint must direct the user to reload or "
        "scan again before deletion is possible"
    )


def test_start_verification_keeps_page_locked_on_ambiguous_start(app_and_db):
    """An ambiguous POST outcome must not unlock deletion while the scoped
    verification job may be refreshing the manifest."""
    app, _ = app_and_db
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    start = body.index("async function cardCleanupStartVerification(")
    end = body.index("async function cardCleanupFinishVerification(", start)
    section = body[start:end]
    # An HTTP response that proves nothing was queued (!resp.ok) must
    # release the lock so the user isn't stranded with a dead page.
    ok_branch_start = section.index("if (!resp.ok)")
    # The branch closes before the JSON re-parse below; scope to just that
    # arm by ending at the trailing "return;" and its closing brace.
    ok_branch_end = section.index("let data;", ok_branch_start)
    ok_branch = section[ok_branch_start:ok_branch_end]
    assert "cardCleanupSetBusy(false)" in ok_branch, (
        "the !resp.ok branch must call cardCleanupSetBusy(false) — the "
        "server said the verification job was not queued, so the page can "
        "safely hand the buttons back"
    )
    fetch_catch_start = section.index("resp = await fetch(")
    fetch_catch_start = section.index("} catch (e) {", fetch_catch_start)
    fetch_catch_end = section.index("if (!resp.ok)", fetch_catch_start)
    fetch_catch_body = section[fetch_catch_start:fetch_catch_end]
    assert "cardCleanupSetBusy(false)" not in fetch_catch_body, (
        "the fetch-rejection catch must not unlock a possibly running "
        "verification job"
    )
    assert "Jobs page" in fetch_catch_body, (
        "the fetch-rejection catch must direct the user to the Jobs page so "
        "they can find out whether verification was queued"
    )
    # The JSON-parse catch on the OK response is the second ambiguous
    # outcome: a 2xx status means the endpoint returned, but if
    # the body couldn't be decoded we can't be sure the job wasn't queued.
    # Same reasoning — keep the lock, route to Jobs.
    json_catch_start = section.index("data = await resp.json()")
    json_catch_start = section.index("} catch (e) {", json_catch_start)
    json_catch_end = section.index("cardCleanupWatchJob(", json_catch_start)
    json_catch_body = section[json_catch_start:json_catch_end]
    assert "cardCleanupSetBusy(false)" not in json_catch_body, (
        "the JSON-parse catch on a 2xx response must NOT release the page "
        "lock — the server returned OK, so verification may be running"
    )
    assert "Jobs page" in json_catch_body, (
        "the JSON-parse catch on a 2xx response must direct the user to the "
        "Jobs page for the actual outcome"
    )


def test_load_manifest_discards_stale_scan_response(app_and_db):
    """Codex P2 review (commit 16a3f8e4): cardCleanupLoadManifest must
    discard responses whose scan job id is no longer current.

    Scenario: cardCleanupFinishJob unlocks the buttons and then awaits
    cardCleanupLoadManifest() for scan #1. During that await a user can
    change the source and start scan #2, which sets scanJobId to jobId2.
    If scan #1's response then arrives it would (a) render scan #1's
    source and totals over scan #2's state via cardCleanupRenderManifest
    — including re-enabling the Delete button based on scan #1's
    deletable count — while the delete POST uses cardCleanupState.
    scanJobId, which is now jobId2, so the confirmation dialog would
    advertise a set that does not match the manifest the destructive job
    would operate on; or (b) on a 404, null out scanJobId and clobber
    scan #2's identity; or (c) surface scan #1's fetch/parse error as if
    scan #2 had failed.

    Fix: capture scanJobId at request time and drop the response on both
    the success and error paths when it has moved on. Pinning both paths
    here so a refactor cannot quietly re-open either one.
    """
    app, _ = app_and_db
    body = app.test_client().get("/card-cleanup").get_data(as_text=True)
    start = body.index("async function cardCleanupLoadManifest(")
    end = body.index("function cardCleanupRenderManifest(", start)
    section = body[start:end]
    # The success path must run its stale-response check before ANY
    # state mutation or render — both the 404 handler (which nulls
    # scanJobId) and cardCleanupRenderManifest (which shows scan #1's
    # totals and re-enables Delete based on them) would corrupt scan #2's
    # state if reached with a stale response.
    first_mutation_idx = min(
        section.index("cardCleanupState.scanJobId = null"),
        section.index("cardCleanupRenderManifest("),
    )
    pre_mutation = section[:first_mutation_idx]
    assert "cardCleanupState.scanJobId !==" in pre_mutation, (
        "cardCleanupLoadManifest must compare state.scanJobId against the "
        "job id captured before the fetch, and return early on mismatch, "
        "BEFORE touching state or rendering — otherwise a scan started "
        "during the fetch will be overwritten by the stale response and "
        "the Delete confirmation will advertise numbers that do not match "
        "the manifest the delete POST would operate on"
    )
    # The error path (network failure, unreadable body, !resp.ok throw)
    # must also discard on mismatch — a scan #2 in flight should not
    # inherit scan #1's error banner as if it had failed itself.
    catch_idx = section.index("} catch (e) {")
    catch_body = section[catch_idx:]
    assert "cardCleanupState.scanJobId !==" in catch_body, (
        "the catch branch of cardCleanupLoadManifest must also check "
        "state.scanJobId — a fetch or parse error on the stale scan #1 "
        "response must not surface as an error message while scan #2 is "
        "running, because it would misattribute the failure to the scan "
        "the user is currently watching"
    )
