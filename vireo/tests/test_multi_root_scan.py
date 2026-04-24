"""Multi-root scan: one job, multiple roots, scanned serially.

Root cause fix complementary to PR #634 (DB-lock resilience): instead of
the UI enqueueing one scan job per folder root (which made the jobs race
for the SQLite writer lock), a single scan job now iterates roots
serially so there is no contention in the first place.
"""
import os
import time

import pytest
from PIL import Image


def _make_photo(folder, name):
    os.makedirs(folder, exist_ok=True)
    Image.new("RGB", (100, 100), color="red").save(os.path.join(folder, name))


def _wait_for_terminal(client, job_id, timeout=15.0):
    """Poll /api/jobs/<id> until status is completed/failed/cancelled."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        resp = client.get(f"/api/jobs/{job_id}")
        data = resp.get_json()
        last = data
        if data["status"] in ("completed", "failed", "cancelled"):
            return data
        time.sleep(0.1)
    pytest.fail(f"job {job_id} did not terminate within {timeout}s: last={last}")


def test_scan_handles_multiple_roots_serially(app_and_db, tmp_path):
    """POST /api/jobs/scan with a list of roots scans them all in one job."""
    app, db = app_and_db
    client = app.test_client()

    root_a = str(tmp_path / "a")
    root_b = str(tmp_path / "b")
    _make_photo(root_a, "a1.jpg")
    _make_photo(root_a, "a2.jpg")
    _make_photo(root_b, "b1.jpg")

    resp = client.post("/api/jobs/scan", json={"roots": [root_a, root_b]})
    assert resp.status_code == 200, resp.get_json()
    job_id = resp.get_json()["job_id"]

    data = _wait_for_terminal(client, job_id)
    assert data["status"] == "completed", data

    # Both roots ended up in the DB.
    filenames = {
        r["filename"]
        for r in db.conn.execute("SELECT filename FROM photos").fetchall()
    }
    assert {"a1.jpg", "a2.jpg", "b1.jpg"}.issubset(filenames), filenames

    # Only ONE scan job was enqueued — not one-per-root.
    scan_jobs = [j for j in app._job_runner.list_jobs() if j.get("type") == "scan"]
    assert len(scan_jobs) == 1, scan_jobs


def test_single_scan_job_for_all_roots(app_and_db, tmp_path):
    """Verify only one JobRunner job is created for a multi-root request."""
    app, _ = app_and_db
    client = app.test_client()

    roots = []
    for name in ("r1", "r2", "r3"):
        root = str(tmp_path / name)
        _make_photo(root, f"{name}.jpg")
        roots.append(root)

    baseline = len(app._job_runner.list_jobs())
    resp = client.post("/api/jobs/scan", json={"roots": roots})
    assert resp.status_code == 200, resp.get_json()
    _wait_for_terminal(client, resp.get_json()["job_id"])

    # One new job, not three.
    after = len(app._job_runner.list_jobs())
    assert after - baseline == 1, (
        f"expected exactly 1 new job for 3 roots, got {after - baseline}"
    )


def test_scan_continues_after_one_root_fails(app_and_db, tmp_path, monkeypatch):
    """If root A raises mid-scan, root B still completes and job is 'failed'.

    Mixed-outcome rollup convention: any failed sub-task makes the
    aggregate status 'failed', not 'completed'.
    """
    app, db = app_and_db
    client = app.test_client()

    root_bad = str(tmp_path / "bad")
    root_good = str(tmp_path / "good")
    _make_photo(root_bad, "x.jpg")
    _make_photo(root_good, "y.jpg")

    # Patch scanner.scan so the first root raises, second succeeds.
    import scanner as real_scanner
    real_scan = real_scanner.scan

    def flaky_scan(root, db, *args, **kwargs):
        if root == root_bad:
            raise RuntimeError("simulated failure on bad root")
        return real_scan(root, db, *args, **kwargs)

    monkeypatch.setattr("scanner.scan", flaky_scan)

    resp = client.post("/api/jobs/scan", json={"roots": [root_bad, root_good]})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    data = _wait_for_terminal(client, job_id)

    # Mixed outcome -> "failed" per project convention.
    assert data["status"] == "failed", data
    # But the good root was still processed.
    filenames = {
        r["filename"]
        for r in db.conn.execute("SELECT filename FROM photos").fetchall()
    }
    assert "y.jpg" in filenames, (
        f"good root should still have been scanned after bad root failed, "
        f"got {filenames}"
    )
    # And errors carry the failure context.
    assert any("bad" in e or "simulated failure" in e for e in data["errors"]), data


def test_single_root_string_still_works(app_and_db, tmp_path):
    """Back-compat: posting {"root": "..."} (singular) still works."""
    app, db = app_and_db
    client = app.test_client()

    root = str(tmp_path / "only")
    _make_photo(root, "only.jpg")

    resp = client.post("/api/jobs/scan", json={"root": root})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    data = _wait_for_terminal(client, job_id)
    assert data["status"] == "completed", data

    filenames = {
        r["filename"]
        for r in db.conn.execute("SELECT filename FROM photos").fetchall()
    }
    assert "only.jpg" in filenames


def test_scan_job_config_preserves_roots_list(app_and_db, tmp_path):
    """Job config carries the full list of roots so history shows them."""
    app, _ = app_and_db
    client = app.test_client()

    roots = []
    for name in ("one", "two"):
        root = str(tmp_path / name)
        _make_photo(root, f"{name}.jpg")
        roots.append(root)

    resp = client.post("/api/jobs/scan", json={"roots": roots})
    job_id = resp.get_json()["job_id"]
    _wait_for_terminal(client, job_id)

    job = next(j for j in app._job_runner.list_jobs() if j.get("id") == job_id)
    cfg = job.get("config") or {}
    assert cfg.get("roots") == roots, cfg


def test_scan_roots_empty_list_returns_error(app_and_db):
    """POST with an empty roots list is a 400, not a no-op success."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.post("/api/jobs/scan", json={"roots": []})
    assert resp.status_code == 400


def test_scan_roots_invalid_path_returns_error(app_and_db, tmp_path):
    """If any root in the list is bogus, we reject the whole request."""
    app, _ = app_and_db
    client = app.test_client()

    good = str(tmp_path / "good")
    _make_photo(good, "g.jpg")

    resp = client.post(
        "/api/jobs/scan",
        json={"roots": [good, "/definitely/not/a/real/path"]},
    )
    assert resp.status_code == 400


def test_mixed_outcome_does_not_inflate_error_count(app_and_db, tmp_path, monkeypatch):
    """Two failing roots + one good root => error_count is 2, not 3.

    Regression: when the scan loop pre-appends each per-root error to
    job["errors"] and then raises an aggregated RuntimeError, JobRunner's
    dedup (exact string match) treats the aggregate as a new distinct
    entry, inflating error_count by 1 for every mixed-outcome run.
    """
    app, _ = app_and_db
    client = app.test_client()

    bad_a = str(tmp_path / "bad_a")
    bad_b = str(tmp_path / "bad_b")
    good = str(tmp_path / "good")
    _make_photo(bad_a, "a.jpg")
    _make_photo(bad_b, "b.jpg")
    _make_photo(good, "c.jpg")

    import scanner as real_scanner
    real_scan = real_scanner.scan

    def flaky_scan(root, db, *args, **kwargs):
        if root == bad_a:
            raise RuntimeError("boom A")
        if root == bad_b:
            raise RuntimeError("boom B")
        return real_scan(root, db, *args, **kwargs)

    monkeypatch.setattr("scanner.scan", flaky_scan)

    resp = client.post("/api/jobs/scan", json={"roots": [bad_a, bad_b, good]})
    job_id = resp.get_json()["job_id"]
    data = _wait_for_terminal(client, job_id)

    assert data["status"] == "failed"
    assert len(data["errors"]) == 2, (
        f"expected exactly 2 error entries (one per failed root), "
        f"got {len(data['errors'])}: {data['errors']}"
    )
    # Both per-root messages preserved.
    joined = " | ".join(data["errors"])
    assert "boom A" in joined and "boom B" in joined, data["errors"]


def test_failed_root_does_not_inflate_cumulative_progress(app_and_db, tmp_path, monkeypatch):
    """Root A fails after partial progress; root B's cumulative counters
    must start from root A's processed count, not its planned total.

    Regression: advance_scan_acc() previously added last_total (planned
    ceiling) to the accumulator. If root A had 10 files planned and
    failed after processing 3, root B started from a baseline of 10,
    inflating both cumulative progress and the final "photos indexed"
    summary with 7 phantom files.
    """
    app, db = app_and_db
    client = app.test_client()

    root_bad = str(tmp_path / "bad")
    root_good = str(tmp_path / "good")
    # 10 files in the bad root, one processes then scan raises.
    for i in range(10):
        _make_photo(root_bad, f"bad_{i}.jpg")
    for i in range(2):
        _make_photo(root_good, f"good_{i}.jpg")

    import scanner as real_scanner
    real_scan = real_scanner.scan

    def flaky_scan(root, db, *args, progress_callback=None, **kwargs):
        if root == root_bad:
            # Report partial progress (3 of 10) then fail, simulating
            # a mid-scan error after some photos were processed.
            if progress_callback is not None:
                progress_callback(3, 10)
            raise RuntimeError("simulated failure after partial progress")
        return real_scan(root, db, *args, progress_callback=progress_callback, **kwargs)

    monkeypatch.setattr("scanner.scan", flaky_scan)

    resp = client.post("/api/jobs/scan", json={"roots": [root_bad, root_good]})
    job_id = resp.get_json()["job_id"]
    data = _wait_for_terminal(client, job_id)

    assert data["status"] == "failed"
    # The scan summary should reflect photos ACTUALLY indexed, not the
    # inflated planned total. Good root contributes 2, bad root
    # contributes its processed count (3), so the summary should cite
    # a number consistent with that — and critically, must NOT include
    # the 7 phantom planned-but-unprocessed files from the bad root.
    scan_step = next(s for s in data["steps"] if s["id"] == "scan")
    summary = scan_step.get("summary", "")
    # Extract the leading "<N> photos" number.
    leading_n = int(summary.split()[0])
    # Planned total across both roots was 10 + 2 = 12. With the bug,
    # photo_count would be 12 (inflated). With the fix it's <= 5 (3
    # processed from bad + 2 from good).
    assert leading_n < 12, (
        f"photo_count inflated by phantom planned-but-unprocessed files: "
        f"summary={summary!r}"
    )


def test_thumbnails_skipped_when_all_roots_fail(app_and_db, tmp_path, monkeypatch):
    """If every scan root fails, the thumbnail phase must be skipped.
    generate_all() walks the whole library looking for missing thumbs;
    running it after a total scan failure does a long unrelated pass
    that delays failure feedback and does work the user didn't ask for.
    When at least one root succeeds, thumbs still run normally."""
    app, _ = app_and_db
    client = app.test_client()

    bad_a = str(tmp_path / "bad_a")
    bad_b = str(tmp_path / "bad_b")
    _make_photo(bad_a, "a.jpg")
    _make_photo(bad_b, "b.jpg")


    def always_fails(root, db, *args, **kwargs):
        raise RuntimeError(f"simulated immediate failure on {root}")

    monkeypatch.setattr("scanner.scan", always_fails)

    # Sentinel to detect if generate_all was called.
    generate_calls = {"n": 0}
    import thumbnails as real_thumb
    real_generate_all = real_thumb.generate_all

    def tracking_generate_all(*args, **kwargs):
        generate_calls["n"] += 1
        return real_generate_all(*args, **kwargs)

    monkeypatch.setattr("thumbnails.generate_all", tracking_generate_all)

    resp = client.post("/api/jobs/scan", json={"roots": [bad_a, bad_b]})
    job_id = resp.get_json()["job_id"]
    data = _wait_for_terminal(client, job_id)

    assert data["status"] == "failed"
    assert generate_calls["n"] == 0, (
        "generate_all must NOT run when every scan root failed "
        f"(called {generate_calls['n']} times)"
    )
    # Thumbnail step should be marked skipped, not running/failed.
    thumb_step = next(s for s in data["steps"] if s["id"] == "thumbnails")
    assert thumb_step["status"] == "skipped", thumb_step


def test_thumbnails_still_run_when_some_roots_succeed(app_and_db, tmp_path, monkeypatch):
    """Mixed outcome (some roots fail, some succeed) still runs thumbs
    so the successfully-indexed photos get covered."""
    app, _ = app_and_db
    client = app.test_client()

    bad = str(tmp_path / "bad")
    good = str(tmp_path / "good")
    _make_photo(bad, "b.jpg")
    _make_photo(good, "g.jpg")

    import scanner as real_scanner
    real_scan = real_scanner.scan

    def flaky_scan(root, db, *args, **kwargs):
        if root == bad:
            raise RuntimeError("simulated fail on bad root")
        return real_scan(root, db, *args, **kwargs)

    monkeypatch.setattr("scanner.scan", flaky_scan)

    generate_calls = {"n": 0}
    import thumbnails as real_thumb
    real_generate_all = real_thumb.generate_all

    def tracking_generate_all(*args, **kwargs):
        generate_calls["n"] += 1
        return real_generate_all(*args, **kwargs)

    monkeypatch.setattr("thumbnails.generate_all", tracking_generate_all)

    resp = client.post("/api/jobs/scan", json={"roots": [bad, good]})
    job_id = resp.get_json()["job_id"]
    data = _wait_for_terminal(client, job_id)

    assert data["status"] == "failed"  # mixed-outcome rollup
    assert generate_calls["n"] == 1, (
        "generate_all must run when at least one root succeeded"
    )


def test_summary_counts_unique_failed_roots_not_error_entries(
    app_and_db, tmp_path, monkeypatch
):
    """A root that raises in both scan AND cache invalidation counts
    as ONE failed root in the summary, not two.

    Regression: summary used to derive "N of M failed" from
    len(root_errors), so a single root hitting both scan failure and
    cache-invalidation failure would report "2 of 2 failed" even when
    one of the two roots succeeded.
    """
    app, _ = app_and_db
    client = app.test_client()

    bad = str(tmp_path / "bad")
    good = str(tmp_path / "good")
    _make_photo(bad, "b.jpg")
    _make_photo(good, "g.jpg")

    import scanner as real_scanner
    real_scan = real_scanner.scan

    def flaky_scan(root, db, *args, **kwargs):
        if root == bad:
            raise RuntimeError("scan boom")
        return real_scan(root, db, *args, **kwargs)

    monkeypatch.setattr("scanner.scan", flaky_scan)

    # Also make cache invalidation fail on the SAME bad root so it
    # contributes two error entries but is still only one failed root.
    import app as app_module
    real_invalidate = app_module._invalidate_new_images_after_scan

    def flaky_invalidate(db, root, *args, **kwargs):
        if root == bad:
            raise RuntimeError("cache boom")
        return real_invalidate(db, root, *args, **kwargs)

    monkeypatch.setattr(
        app_module, "_invalidate_new_images_after_scan", flaky_invalidate,
    )

    resp = client.post("/api/jobs/scan", json={"roots": [bad, good]})
    job_id = resp.get_json()["job_id"]
    data = _wait_for_terminal(client, job_id)

    assert data["status"] == "failed"
    scan_step = next(s for s in data["steps"] if s["id"] == "scan")
    summary = scan_step.get("summary", "")
    # Exactly one root failed, out of two. NOT "2 of 2".
    assert "1 of 2" in summary, (
        f"expected '1 of 2 roots failed' in summary, got {summary!r}"
    )


def test_cache_only_failure_still_runs_thumbnails(
    app_and_db, tmp_path, monkeypatch
):
    """A root whose scan succeeds but cache invalidation fails still
    produced indexed photos, so thumbnails must still run.

    Regression: all_roots_failed used to be len(root_errors) ==
    len(roots_list). A two-root run where root A's scan raised and
    root B's cache invalidation raised produced 2 errors across 2
    roots — incorrectly triggering the thumbnail skip even though
    root B had indexed photos that needed thumbs.
    """
    app, _ = app_and_db
    client = app.test_client()

    bad = str(tmp_path / "bad")
    good = str(tmp_path / "good")
    _make_photo(bad, "b.jpg")
    _make_photo(good, "g.jpg")

    import scanner as real_scanner
    real_scan = real_scanner.scan

    def flaky_scan(root, db, *args, **kwargs):
        if root == bad:
            raise RuntimeError("scan boom")
        return real_scan(root, db, *args, **kwargs)

    monkeypatch.setattr("scanner.scan", flaky_scan)

    # Cache invalidation fails only on the good root — its scan
    # succeeded (photos indexed), but its cache invalidation raised.
    import app as app_module
    real_invalidate = app_module._invalidate_new_images_after_scan

    def flaky_invalidate(db, root, *args, **kwargs):
        if root == good:
            raise RuntimeError("cache boom on good")
        return real_invalidate(db, root, *args, **kwargs)

    monkeypatch.setattr(
        app_module, "_invalidate_new_images_after_scan", flaky_invalidate,
    )

    generate_calls = {"n": 0}
    import thumbnails as real_thumb
    real_generate_all = real_thumb.generate_all

    def tracking_generate_all(*args, **kwargs):
        generate_calls["n"] += 1
        return real_generate_all(*args, **kwargs)

    monkeypatch.setattr("thumbnails.generate_all", tracking_generate_all)

    resp = client.post("/api/jobs/scan", json={"roots": [bad, good]})
    job_id = resp.get_json()["job_id"]
    data = _wait_for_terminal(client, job_id)

    assert data["status"] == "failed"  # mixed outcome
    assert generate_calls["n"] == 1, (
        "generate_all must run when any root's scan succeeded, even if "
        f"that root had a cache-invalidation failure (called "
        f"{generate_calls['n']} times)"
    )
    thumb_step = next(s for s in data["steps"] if s["id"] == "thumbnails")
    assert thumb_step["status"] != "skipped", thumb_step


def test_cache_invalidation_failure_flips_job_to_failed(app_and_db, tmp_path, monkeypatch):
    """If _invalidate_new_images_after_scan raises after a scan, the
    job must NOT report success. Previously the error was logged and
    swallowed, so a scan that completed successfully would appear as
    "completed" even though the shared new-images cache (5-min TTL)
    was left stale — users would see wrong 'new images' counts with
    no job-level failure signal.
    """
    app, _ = app_and_db
    client = app.test_client()

    root = str(tmp_path / "r")
    _make_photo(root, "a.jpg")

    import app as app_module

    def boom(*args, **kwargs):
        raise RuntimeError("cache invalidation exploded")

    monkeypatch.setattr(app_module, "_invalidate_new_images_after_scan", boom)

    resp = client.post("/api/jobs/scan", json={"roots": [root]})
    job_id = resp.get_json()["job_id"]
    data = _wait_for_terminal(client, job_id)

    # Scan itself succeeded but cache invalidation failed → job failed.
    assert data["status"] == "failed", data
    # The cache failure must be visible in the recorded errors.
    assert any(
        "cache invalidation" in e and "exploded" in e
        for e in data["errors"]
    ), data["errors"]
