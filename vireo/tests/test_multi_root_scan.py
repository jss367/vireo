"""Multi-root scan: one job, multiple roots, scanned serially.

Root cause fix complementary to PR #634 (DB-lock resilience): instead of
the UI enqueueing one scan job per folder root (which made the jobs race
for the SQLite writer lock), a single scan job now iterates roots
serially so there is no contention in the first place.
"""
import os
import threading
import time

from PIL import Image
from wait import wait_for_job_via_client


def _make_photo(folder, name):
    os.makedirs(folder, exist_ok=True)
    Image.new("RGB", (100, 100), color="red").save(os.path.join(folder, name))


def _wait_for_terminal(client, job_id):
    """Use the shared timeout budget for loaded Windows CI runners."""
    return wait_for_job_via_client(client, job_id)


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


def test_scan_job_cancel_is_forwarded_to_scanner(app_and_db, tmp_path, monkeypatch):
    """Cancelling a standalone scan job must interrupt scanner.scan itself."""
    app, _ = app_and_db
    client = app.test_client()

    root_a = str(tmp_path / "a")
    root_b = str(tmp_path / "b")
    _make_photo(root_a, "a.jpg")
    _make_photo(root_b, "b.jpg")

    scan_started = threading.Event()
    scanned_roots = []
    from scanner import ScanCancelled

    def cancellable_scan(root, db, *args, cancel_check=None, **kwargs):
        scanned_roots.append(root)
        assert callable(cancel_check), "scan job did not pass cancel_check"
        scan_started.set()
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if cancel_check():
                raise ScanCancelled("scan cancelled")
            time.sleep(0.02)
        raise AssertionError("scanner.scan never observed cancellation")

    monkeypatch.setattr("scanner.scan", cancellable_scan)

    generate_calls = {"n": 0}

    def tracking_generate_all(*args, **kwargs):
        generate_calls["n"] += 1
        return {"generated": 0, "skipped": 0, "errors": []}

    monkeypatch.setattr("thumbnails.generate_all", tracking_generate_all)

    resp = client.post("/api/jobs/scan", json={"roots": [root_a, root_b]})
    assert resp.status_code == 200, resp.get_json()
    job_id = resp.get_json()["job_id"]
    assert scan_started.wait(timeout=2.0), "scan job did not start"

    cancel_resp = client.post(f"/api/jobs/{job_id}/cancel")
    assert cancel_resp.status_code == 200, cancel_resp.get_json()

    data = _wait_for_terminal(client, job_id)
    assert data["status"] == "cancelled", data
    assert scanned_roots == [root_a]
    assert generate_calls["n"] == 0

    scan_step = next(s for s in data["steps"] if s["id"] == "scan")
    thumb_step = next(s for s in data["steps"] if s["id"] == "thumbnails")
    assert scan_step["status"] == "cancelled", scan_step
    assert thumb_step["status"] == "skipped", thumb_step


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
    # The scan summary reflects photos ACTUALLY indexed. The good root
    # indexes 2. The bad root reports progress for 3 files but never
    # reaches the real scanner, so it indexes nothing and contributes 0 —
    # reported progress is not evidence a photo was cataloged. Either way
    # the 7 planned-but-unprocessed files must never appear.
    scan_step = next(s for s in data["steps"] if s["id"] == "scan")
    summary = scan_step.get("summary", "")
    # Extract the leading "<N> photos" number.
    leading_n = int(summary.split()[0])
    assert leading_n == 2, (
        f"summary should credit only the 2 photos the good root actually "
        f"indexed: summary={summary!r}"
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
    from services import scan_work
    real_invalidate = scan_work._invalidate_new_images_after_scan

    def flaky_invalidate(db, root, *args, **kwargs):
        if root == bad:
            raise RuntimeError("cache boom")
        return real_invalidate(db, root, *args, **kwargs)

    monkeypatch.setattr(
        scan_work, "_invalidate_new_images_after_scan", flaky_invalidate,
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
    from services import scan_work
    real_invalidate = scan_work._invalidate_new_images_after_scan

    def flaky_invalidate(db, root, *args, **kwargs):
        if root == good:
            raise RuntimeError("cache boom on good")
        return real_invalidate(db, root, *args, **kwargs)

    monkeypatch.setattr(
        scan_work, "_invalidate_new_images_after_scan", flaky_invalidate,
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

    from services import scan_work

    def boom(*args, **kwargs):
        raise RuntimeError("cache invalidation exploded")

    monkeypatch.setattr(scan_work, "_invalidate_new_images_after_scan", boom)

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


def test_scan_summary_excludes_files_that_vanished_mid_scan(
        app_and_db, tmp_path, monkeypatch):
    """The "N photos" scan summary must count photos indexed, not files
    walked.

    ``job["progress"]["current"]`` is a progress counter: it advances for
    every file the scan disposes of, including ones it skipped because
    they vanished. Using it as the summary meant an archive share that
    unmounted mid-scan reported a full "984 photos" success line having
    indexed nothing (2026-07-30). The summary must answer "how many
    photos are in the catalog because of this run".
    """
    app, db = app_and_db
    client = app.test_client()

    root = str(tmp_path / "archive")
    for i in range(4):
        _make_photo(root, f"p_{i}.jpg")

    import scanner as real_scanner
    real_scan = real_scanner.scan

    def vanishing_scan(scan_root, sdb, *args, progress_callback=None, **kwargs):
        # Wrap the app's progress callback so two files disappear at the
        # discovery/processing seam — the real scanner then hits real
        # stat() failures, exactly as it did when the SMB mount dropped.
        def wrapped(current, total):
            if current == 0:
                os.remove(os.path.join(scan_root, "p_2.jpg"))
                os.remove(os.path.join(scan_root, "p_3.jpg"))
            if progress_callback is not None:
                progress_callback(current, total)

        return real_scan(
            scan_root, sdb, *args, progress_callback=wrapped, **kwargs
        )

    monkeypatch.setattr("scanner.scan", vanishing_scan)

    resp = client.post("/api/jobs/scan", json={"roots": [root]})
    job_id = resp.get_json()["job_id"]
    data = _wait_for_terminal(client, job_id)

    scan_step = next(s for s in data["steps"] if s["id"] == "scan")
    summary = scan_step.get("summary", "")
    leading_n = int(summary.split()[0])
    assert leading_n == 2, (
        f"summary counts vanished files as indexed: {summary!r} (only 2 of "
        "4 files survived to be cataloged)"
    )

    # And the claim is checkable against the catalog it describes. Scope
    # to this root — the app_and_db fixture's catalog outlives one test.
    indexed = db.conn.execute(
        "SELECT COUNT(*) FROM photos p JOIN folders f ON f.id = p.folder_id "
        "WHERE f.path = ?",
        (root,),
    ).fetchone()[0]
    assert indexed == 2, indexed


def test_scan_summary_credits_photos_indexed_before_a_root_raised(
        app_and_db, tmp_path, monkeypatch):
    """A root that raises after indexing must still be credited.

    scan() commits rows incrementally and can raise after the per-file
    loop finished (the pairing / working-copy / preview passes all run
    inside its try block). Reporting 0 photos for a run that cataloged
    everything is the same kind of false status as reporting 984 for a
    run that cataloged nothing — just in the other direction.
    """
    app, db = app_and_db
    client = app.test_client()

    root = str(tmp_path / "late_failure")
    for i in range(3):
        _make_photo(root, f"q_{i}.jpg")

    import scanner as real_scanner
    real_scan = real_scanner.scan

    def scan_then_raise(scan_root, sdb, *args, **kwargs):
        real_scan(scan_root, sdb, *args, **kwargs)
        raise RuntimeError("post-loop pass blew up after indexing")

    monkeypatch.setattr("scanner.scan", scan_then_raise)

    resp = client.post("/api/jobs/scan", json={"roots": [root]})
    job_id = resp.get_json()["job_id"]
    data = _wait_for_terminal(client, job_id)

    assert data["status"] == "failed", data
    scan_step = next(s for s in data["steps"] if s["id"] == "scan")
    summary = scan_step.get("summary", "")
    assert int(summary.split()[0]) == 3, (
        f"photos indexed before the failure were dropped from the "
        f"summary: {summary!r}"
    )
