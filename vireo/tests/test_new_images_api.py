import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from PIL import Image


def _touch_image(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(path, "JPEG")


@pytest.fixture(autouse=True)
def _clear_shared_new_images_cache():
    from new_images import get_shared_cache
    get_shared_cache().clear()
    yield
    get_shared_cache().clear()


@pytest.fixture
def app_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db, ws_id, tmp_path


def test_api_new_images_reports_unscanned_files(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["new_count"] == 1
    assert len(data["per_root"]) == 1
    assert data["workspace_id"] == ws_id


def test_api_new_images_zero_when_fully_ingested(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    fid = db.add_folder(str(root), name="shoot")
    db.add_photo(folder_id=fid, filename="IMG.JPG", extension=".JPG",
                 file_size=1, file_mtime=0.0)

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    data = resp.get_json()
    assert data["new_count"] == 0
    assert data["workspace_id"] == ws_id


def test_api_new_images_returns_pending_when_walk_is_slow(app_and_db, monkeypatch):
    """Cold-cache calls return pending instead of blocking the request thread
    on a long os.walk. Without this, the navbar's poll on app start ties up a
    Flask worker for the entire walk (observed at 12.9s on a real library)."""
    import threading

    import new_images as new_images_module

    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    release = threading.Event()
    started = threading.Event()
    real_count = new_images_module.count_new_images_for_workspace

    def slow_count(*args, **kwargs):
        started.set()
        # Block until the test releases us, so the kickoff thread is still
        # in flight when the request returns.
        release.wait(timeout=5)
        return real_count(*args, **kwargs)

    monkeypatch.setattr(new_images_module, "count_new_images_for_workspace", slow_count)

    client = app.test_client()
    try:
        resp = client.get("/api/workspaces/active/new-images")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data.get("pending") is True, (
            f"expected pending=True while walk is in flight, got {data!r}"
        )
        assert data["new_count"] is None
        assert data["workspace_id"] == ws_id
        # The endpoint must have actually kicked off the background compute.
        assert started.is_set()
    finally:
        # Let the background thread finish before pytest tears down tmp_path,
        # otherwise it walks a deleted directory tree.
        release.set()


def test_api_new_images_returns_cached_after_background_compute_finishes(app_and_db):
    """Once the background walk finishes, the cache is populated and the next
    request returns the real count instantly — no second walk."""
    import time

    from new_images import get_shared_cache

    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    client = app.test_client()
    # First request: walk is fast on tmp_path so the 500ms wait will catch it
    # and we'll see the count synchronously. To exercise the *cache hit after
    # async compute* path we wait for the cache key to appear, then re-probe.
    resp1 = client.get("/api/workspaces/active/new-images")
    assert resp1.status_code == 200

    cache = get_shared_cache()
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if cache.get(db._db_path, ws_id) is not None:
            break
        time.sleep(0.01)
    assert cache.get(db._db_path, ws_id) is not None, (
        "background compute never populated the cache"
    )

    resp2 = client.get("/api/workspaces/active/new-images")
    data = resp2.get_json()
    assert data.get("pending") is None or data.get("pending") is False
    assert data["new_count"] == 1


def test_api_new_images_response_trims_full_cached_sample(app_and_db):
    """The navbar only needs a tiny sample, but the server cache keeps the full
    path list so "Create a pipeline" can snapshot without a second full walk."""
    import time

    from new_images import get_shared_cache

    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    for i in range(8):
        _touch_image(str(root / f"IMG_{i:03d}.JPG"))
    db.add_folder(str(root), name="shoot")

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["new_count"] == 8
    assert len(data["sample"]) == 5
    assert "sample_complete" not in data

    cache = get_shared_cache()
    deadline = time.monotonic() + 2.0
    cached = None
    while time.monotonic() < deadline:
        cached = cache.get(db._db_path, ws_id)
        if cached is not None:
            break
        time.sleep(0.01)
    assert cached is not None
    assert cached["sample_complete"] is True
    assert len(cached["sample"]) == 8


def test_api_new_images_cached_response_slices_without_full_copy(app_and_db):
    """Cached full snapshots may contain tens of thousands of paths. The
    navbar response should copy only the five paths it returns, not materialize
    the entire cached sequence before slicing."""
    from new_images import get_shared_cache

    class SampleOnlySlice:
        def __bool__(self):
            return True

        def __iter__(self):
            raise AssertionError("sample should be sliced directly")

        def __getitem__(self, key):
            assert isinstance(key, slice), f"unexpected key: {key!r}"
            assert key.start is None
            assert key.stop == 5
            return [f"/tmp/IMG_{i:03d}.JPG" for i in range(5)]

    app, db, ws_id, tmp_path = app_and_db
    get_shared_cache().set(db._db_path, ws_id, {
        "new_count": 100000,
        "per_root": [],
        "sample": SampleOnlySlice(),
        "sample_complete": True,
    })

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["sample"]) == 5
    assert "sample_complete" not in data


def test_api_new_images_returns_error_when_compute_fails(app_and_db, monkeypatch):
    """If the background walk raises (e.g. unavailable volume, DB error), the
    endpoint must surface an error instead of looping forever on pending.

    Without this, the navbar's 3s pending re-poll keeps firing because the
    cache stays empty after every failed compute — leaving the UI in a
    permanent retry state and hammering the failing resource.
    """
    import time

    import new_images as new_images_module

    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    def boom(*args, **kwargs):
        raise RuntimeError("disk unreachable")

    monkeypatch.setattr(new_images_module, "count_new_images_for_workspace", boom)

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    # Wait briefly for the worker thread to finish raising and storing the
    # error — the 500ms grace inside the endpoint may have already caught it,
    # but in case it didn't, repoll once.
    if data.get("pending"):
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            time.sleep(0.05)
            data = client.get("/api/workspaces/active/new-images").get_json()
            if not data.get("pending"):
                break
    assert data.get("error"), f"expected an error in payload, got {data!r}"
    assert "disk unreachable" in data["error"]
    assert data.get("pending") is None or data.get("pending") is False
    assert data["new_count"] is None or data["new_count"] == 0
    assert data["workspace_id"] == ws_id


def test_api_new_images_registers_ephemeral_job_on_cache_cold(app_and_db, monkeypatch):
    """When the new-images walk has to actually run (cache cold, no error
    backoff), an ephemeral ``new_images_walk`` job appears in the active jobs
    list while it runs, and is *not* persisted to job_history when it
    finishes. This is the transparency hook: the user can see the walk
    happening in the bottom panel rather than the silent background thread.
    """
    import threading
    import time

    import new_images as new_images_module

    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    release = threading.Event()
    started = threading.Event()
    real_count = new_images_module.count_new_images_for_workspace

    def slow_count(*args, **kwargs):
        started.set()
        release.wait(timeout=5)
        return real_count(*args, **kwargs)

    monkeypatch.setattr(
        new_images_module, "count_new_images_for_workspace", slow_count,
    )

    client = app.test_client()
    try:
        resp = client.get("/api/workspaces/active/new-images")
        assert resp.status_code == 200
        # While the walk is blocked, /api/jobs must show our ephemeral job.
        deadline = time.monotonic() + 2.0
        walk_jobs = []
        while time.monotonic() < deadline:
            jobs_resp = client.get("/api/jobs")
            jobs_data = jobs_resp.get_json()
            walk_jobs = [
                j for j in jobs_data["active"]
                if j["type"] == "new_images_walk"
            ]
            if walk_jobs:
                break
            time.sleep(0.02)
        assert walk_jobs, (
            "expected a new_images_walk job in /api/jobs while walk is in flight"
        )
        job = walk_jobs[0]
        assert job["status"] == "running"
        assert job["workspace_id"] == ws_id
        assert job["counts_for_badge"] is False
        # Job is tied to the workspace via config.workspace_name for the UI label.
        assert "workspace_name" in job["config"]
    finally:
        release.set()

    # After the walk finishes, no row was written to job_history — ephemeral.
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        jobs_data = client.get("/api/jobs").get_json()
        if not any(j["type"] == "new_images_walk" and j["status"] == "running"
                   for j in jobs_data["active"]):
            break
        time.sleep(0.02)
    history_rows = db.conn.execute(
        "SELECT id FROM job_history WHERE type = 'new_images_walk'"
    ).fetchall()
    assert history_rows == [], (
        f"new_images_walk must not persist to job_history, got {history_rows}"
    )


def test_api_new_images_job_marked_failed_when_walk_raises(app_and_db, monkeypatch):
    """If the cache worker raises (unreadable volume, DB error, etc.) the
    ephemeral job must be reported as ``failed`` rather than ``completed``.

    Without this, /api/jobs and /api/workspaces/active/new-images disagree:
    the endpoint surfaces an error via get_recent_error but the job entry
    still says it succeeded — misleading the user about what actually
    happened.
    """
    import time

    import new_images as new_images_module

    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    def boom(*args, **kwargs):
        raise RuntimeError("disk unreachable")

    monkeypatch.setattr(
        new_images_module, "count_new_images_for_workspace", boom,
    )

    client = app.test_client()
    client.get("/api/workspaces/active/new-images")

    deadline = time.monotonic() + 2.0
    walk_job = None
    while time.monotonic() < deadline:
        active = client.get("/api/jobs").get_json()["active"]
        candidates = [j for j in active if j["type"] == "new_images_walk"]
        if candidates and candidates[0]["status"] in ("failed", "completed"):
            walk_job = candidates[0]
            break
        time.sleep(0.02)

    assert walk_job is not None, "ephemeral new_images_walk job never finished"
    assert walk_job["status"] == "failed", (
        f"job should be 'failed' when walk raised, got {walk_job['status']}; "
        f"errors={walk_job.get('errors')}"
    )
    # The walker's exception message is preserved on the job so the user can
    # see WHY it failed in the bottom panel, not just that something failed.
    assert any("disk unreachable" in e for e in walk_job["errors"]), (
        f"job errors should include the walker's failure message; "
        f"got {walk_job['errors']}"
    )


def test_api_new_images_no_extra_job_on_cache_hit(app_and_db):
    """A second request that hits the cache must NOT spawn another job entry —
    only the cache-cold path surfaces a job, otherwise every navbar poll
    would clutter the jobs list.
    """
    import time

    from new_images import get_shared_cache

    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    client = app.test_client()
    # Prime the cache.
    client.get("/api/workspaces/active/new-images")
    cache = get_shared_cache()
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if cache.get(db._db_path, ws_id) is not None:
            break
        time.sleep(0.01)
    assert cache.get(db._db_path, ws_id) is not None

    # Wait for any prior job to leave the running state, then count.
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        active = client.get("/api/jobs").get_json()["active"]
        if not any(j["type"] == "new_images_walk" and j["status"] == "running"
                   for j in active):
            break
        time.sleep(0.02)

    before = sum(
        1 for j in client.get("/api/jobs").get_json()["active"]
        if j["type"] == "new_images_walk"
    )
    # Cache hit — should not spawn anything new.
    client.get("/api/workspaces/active/new-images")
    after = sum(
        1 for j in client.get("/api/jobs").get_json()["active"]
        if j["type"] == "new_images_walk"
    )
    assert after == before, (
        f"cache hits must not spawn new jobs (before={before}, after={after})"
    )


def test_api_new_images_does_not_hot_loop_on_persistent_failure(app_and_db, monkeypatch):
    """Repeated requests within the error backoff window must not spawn a
    fresh compute every time — otherwise a broken volume gets hammered by
    every navbar poll across every open page."""
    import threading
    import time

    import new_images as new_images_module

    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    call_count = {"n": 0}
    lock = threading.Lock()

    def boom(*args, **kwargs):
        with lock:
            call_count["n"] += 1
        raise RuntimeError("disk unreachable")

    monkeypatch.setattr(new_images_module, "count_new_images_for_workspace", boom)

    client = app.test_client()
    # Fire one request; wait for compute to finish and the error to land.
    client.get("/api/workspaces/active/new-images")
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if call_count["n"] >= 1:
            break
        time.sleep(0.01)
    initial = call_count["n"]
    assert initial >= 1, "compute_fn was never called for the first request"

    # Hammer the endpoint several more times immediately. Each call is
    # within the error backoff window so no new compute should fire.
    for _ in range(5):
        client.get("/api/workspaces/active/new-images")
    # Give any spurious thread a moment to actually hit boom().
    time.sleep(0.1)
    assert call_count["n"] == initial, (
        f"compute_fn called {call_count['n']} times; expected backoff after "
        f"{initial} call(s)"
    )


def test_api_new_images_returns_null_workspace_when_none_active(app_and_db, monkeypatch):
    app, db, ws_id, tmp_path = app_and_db
    # Each request creates its own Database via _get_db(), which auto-restores
    # the last-used workspace. To simulate "no active workspace", patch
    # set_active_workspace to a no-op so the per-request db starts with
    # _active_workspace_id = None.
    from db import Database
    monkeypatch.setattr(Database, "set_active_workspace",
                        lambda self, ws_id: None)

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["workspace_id"] is None
    assert data["new_count"] == 0
    assert data["per_root"] == []


def test_post_snapshot_creates_row_with_current_new_images(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    _touch_image(str(folder / "IMG_001.JPG"))
    _touch_image(str(folder / "IMG_002.JPG"))

    with app.test_client() as client:
        resp = client.post("/api/workspaces/active/new-images/snapshot")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["file_count"] == 2
        assert isinstance(data["snapshot_id"], int)
        assert str(folder) in data["folders"]

    snap = db.get_new_images_snapshot(data["snapshot_id"])
    assert snap["file_count"] == 2


def test_post_snapshot_recomputes_over_navbar_populated_cache(app_and_db):
    """The navbar probe populates the shared new-images cache and holds it
    for the ``NewImagesCache`` TTL. Ordinary file copies into a mapped folder
    do not invalidate it, so a user who gets a banner count, drops more
    images into the folder, and then clicks "Create a pipeline" must not
    receive a snapshot built from the stale cached sample — the endpoint
    must re-walk the disk at click time. Verifies the fix for the Codex
    review comment on the async-snapshot PR."""
    from new_images import get_shared_cache

    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))

    # Simulate the navbar's cache after an earlier probe: two images were
    # on disk and captured in the sample.
    old_paths = [str(folder / "IMG_001.JPG"), str(folder / "IMG_002.JPG")]
    for p in old_paths:
        _touch_image(p)
    get_shared_cache().set(db._db_path, ws_id, {
        "new_count": len(old_paths),
        "per_root": [{"folder_id": 1, "path": str(folder), "new_count": len(old_paths)}],
        "sample": list(old_paths),
        "sample_complete": True,
    })

    # A third image lands on disk between the navbar probe and the click.
    fresh_path = str(folder / "IMG_003.JPG")
    _touch_image(fresh_path)

    with app.test_client() as client:
        resp = client.post("/api/workspaces/active/new-images/snapshot")
        assert resp.status_code == 200
        data = resp.get_json()
        # The endpoint must have re-walked and picked up the third file,
        # not returned the two-file navbar cache.
        assert data["file_count"] == 3, (
            f"expected fresh walk to include new file, got {data!r}"
        )

    snap = db.get_new_images_snapshot(data["snapshot_id"])
    assert snap["file_count"] == 3
    assert fresh_path in snap["file_paths"]


def test_post_snapshot_invalidates_before_trusting_newer_navbar_cache(
        app_and_db, monkeypatch):
    """A navbar walk can begin before the click but write its cache after the
    snapshot session timestamp. The first snapshot POST must invalidate before
    checking cache freshness, so that pre-click walk cannot masquerade as the
    snapshot-owned result."""
    import app as app_module
    from new_images import get_shared_cache

    fake_now = {"value": 200.0}
    monkeypatch.setattr(
        app_module.time, "monotonic", lambda: fake_now["value"],
    )

    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    old_path = str(folder / "IMG_001.JPG")
    fresh_path = str(folder / "IMG_002.JPG")
    _touch_image(old_path)
    _touch_image(fresh_path)

    # Simulate a navbar worker that started before the click but wrote cache
    # just after the snapshot session's kickoff timestamp.
    get_shared_cache().set(db._db_path, ws_id, {
        "new_count": 1,
        "per_root": [{"folder_id": 1, "path": str(folder), "new_count": 1}],
        "sample": [old_path],
        "sample_complete": True,
    })
    fake_now["value"] = 100.0

    with app.test_client() as client:
        resp = client.post("/api/workspaces/active/new-images/snapshot")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["file_count"] == 2

    snap = db.get_new_images_snapshot(data["snapshot_id"])
    assert fresh_path in snap["file_paths"]


def test_post_snapshot_reuses_walk_across_polls(app_and_db, monkeypatch):
    """The client polls the snapshot endpoint on 202 responses. Once the
    fresh walk this session kicked off populates the cache, subsequent
    polls must reuse that result rather than tearing it down and starting
    yet another walk — otherwise polling would livelock, never converging
    on a snapshot."""
    import threading

    import new_images as new_images_module

    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    _touch_image(str(folder / "IMG_001.JPG"))
    db.add_folder(str(folder))

    call_count = {"n": 0}
    started = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    real_count = new_images_module.count_new_images_for_workspace

    def counting_slow(*args, **kwargs):
        with lock:
            call_count["n"] += 1
        started.set()
        release.wait(timeout=5)
        return real_count(*args, **kwargs)

    monkeypatch.setattr(
        new_images_module, "count_new_images_for_workspace", counting_slow,
    )

    try:
        with app.test_client() as client:
            # First POST kicks off the walk; walk is blocked in-flight.
            first = client.post("/api/workspaces/active/new-images/snapshot")
            assert first.status_code == 202
            assert started.is_set()
            calls_after_first = call_count["n"]

            # A poll arriving before the walk finishes must coalesce, not
            # spawn a second walk.
            second = client.post("/api/workspaces/active/new-images/snapshot")
            assert second.status_code == 202
            assert call_count["n"] == calls_after_first, (
                "poll during in-flight walk must not spawn another compute"
            )

            # Let the walk finish, then poll again — this time the endpoint
            # should reuse the fresh cache and return the snapshot without
            # walking a third time.
            release.set()

            import time as _time
            deadline = _time.monotonic() + 2.0
            resp = None
            while _time.monotonic() < deadline:
                resp = client.post("/api/workspaces/active/new-images/snapshot")
                if resp.status_code == 200:
                    break
                _time.sleep(0.05)
            assert resp is not None and resp.status_code == 200, (
                f"snapshot never converged after walk; last status {resp and resp.status_code}"
            )
            assert resp.get_json()["file_count"] == 1
            assert call_count["n"] == calls_after_first, (
                f"post-walk poll must not spawn another compute; "
                f"call_count={call_count['n']} vs baseline {calls_after_first}"
            )
    finally:
        release.set()


def test_post_snapshot_queues_fresh_walk_when_navbar_walk_inflight(
        app_and_db, monkeypatch):
    """A snapshot click must not accept an already-running navbar probe as
    the snapshot source. That probe began before the click, so files copied
    while it was walking could be missing. The POST should invalidate the
    generation, wait for the stale walk only as a queueing point, then return
    a snapshot from the fresh rerun it owns."""
    import threading
    import time

    import new_images as new_images_module
    from new_images import get_shared_cache

    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    old_path = str(folder / "IMG_001.JPG")
    fresh_path = str(folder / "IMG_002.JPG")
    _touch_image(old_path)
    _touch_image(fresh_path)

    old_started = threading.Event()
    old_release = threading.Event()
    fresh_calls = {"n": 0}

    def old_navbar_compute():
        old_started.set()
        old_release.wait(timeout=5)
        return {
            "new_count": 1,
            "per_root": [{"folder_id": 1, "path": str(folder), "new_count": 1}],
            "sample": [old_path],
            "sample_complete": True,
        }

    def fresh_snapshot_count(*args, **kwargs):
        fresh_calls["n"] += 1
        return {
            "new_count": 2,
            "per_root": [{"folder_id": 1, "path": str(folder), "new_count": 2}],
            "sample": [old_path, fresh_path],
            "sample_complete": True,
        }

    cache = get_shared_cache()
    cache.kickoff_compute(db._db_path, ws_id, old_navbar_compute)
    assert old_started.wait(timeout=2), "navbar walk never started"
    monkeypatch.setattr(
        new_images_module, "count_new_images_for_workspace",
        fresh_snapshot_count,
    )

    try:
        with app.test_client() as client:
            first = client.post("/api/workspaces/active/new-images/snapshot")
            assert first.status_code == 202
            assert first.get_json() == {"pending": True}
            assert fresh_calls["n"] == 0, (
                "fresh rerun should be queued behind the existing walk, not "
                "started in parallel"
            )

            old_release.set()
            deadline = time.monotonic() + 2.0
            resp = None
            while time.monotonic() < deadline:
                resp = client.post("/api/workspaces/active/new-images/snapshot")
                if resp.status_code == 200:
                    break
                time.sleep(0.05)

            assert resp is not None and resp.status_code == 200, (
                f"snapshot never converged after queued rerun; "
                f"last={resp and resp.status_code}"
            )
            data = resp.get_json()
            assert data["file_count"] == 2
            assert fresh_calls["n"] == 1

        snap = db.get_new_images_snapshot(data["snapshot_id"])
        assert snap["file_paths"] == [old_path, fresh_path]
    finally:
        old_release.set()


def test_post_snapshot_expires_abandoned_session_before_reuse(app_and_db):
    """If a client abandons a 202 snapshot session, its kickoff marker must
    not let a later click reuse that old session's cached sample. Once the
    marker is older than the client retry window, a new POST must invalidate
    and recompute from current disk state."""
    import time

    from new_images import get_shared_cache

    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    old_path = str(folder / "IMG_001.JPG")
    fresh_path = str(folder / "IMG_002.JPG")
    _touch_image(old_path)

    cache = get_shared_cache()
    with app._new_images_snapshot_kickoff_lock:
        app._new_images_snapshot_kickoff_at[(db._db_path, ws_id)] = (
            time.monotonic() - 121
        )
    cache.set(db._db_path, ws_id, {
        "new_count": 1,
        "per_root": [{"folder_id": 1, "path": str(folder), "new_count": 1}],
        "sample": [old_path],
        "sample_complete": True,
    })

    _touch_image(fresh_path)

    with app.test_client() as client:
        resp = client.post("/api/workspaces/active/new-images/snapshot")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["file_count"] == 2

    snap = db.get_new_images_snapshot(data["snapshot_id"])
    assert fresh_path in snap["file_paths"]


def test_post_snapshot_surfaces_async_error_without_retry_loop(
        app_and_db, monkeypatch):
    """If a snapshot-owned async walk fails after returning 202, the next poll
    should surface that error and stop the snapshot session. It must not clear
    the failure by invalidating again and start a new full walk every poll."""
    import threading
    import time

    import new_images as new_images_module

    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    _touch_image(str(folder / "IMG_001.JPG"))
    db.add_folder(str(folder))

    call_count = {"n": 0}
    started = threading.Event()
    release = threading.Event()

    def slow_boom(*args, **kwargs):
        call_count["n"] += 1
        started.set()
        release.wait(timeout=5)
        raise RuntimeError("disk unreachable")

    monkeypatch.setattr(
        new_images_module, "count_new_images_for_workspace", slow_boom,
    )

    try:
        with app.test_client() as client:
            first = client.post("/api/workspaces/active/new-images/snapshot")
            assert first.status_code == 202
            assert started.is_set()
            assert call_count["n"] == 1

            release.set()
            deadline = time.monotonic() + 2.0
            error_resp = None
            while time.monotonic() < deadline:
                error_resp = client.post(
                    "/api/workspaces/active/new-images/snapshot",
                )
                if error_resp.status_code == 500:
                    break
                time.sleep(0.05)

            assert error_resp is not None and error_resp.status_code == 500
            assert "disk unreachable" in error_resp.get_json()["error"]

            for _ in range(3):
                retry = client.post("/api/workspaces/active/new-images/snapshot")
                assert retry.status_code == 500
            time.sleep(0.1)
            assert call_count["n"] == 1, (
                f"snapshot polls should not restart failed walks inside "
                f"backoff; got {call_count['n']} calls"
            )
    finally:
        release.set()


def test_post_snapshot_next_click_after_success_forces_fresh_walk(app_and_db, monkeypatch):
    """After a snapshot returns successfully, a fresh click (e.g. the user
    dismissed the pipeline and clicked "Create a pipeline" again after
    dropping more files) must trigger a new walk — the previous snapshot
    session's cache reuse must not linger and mask new arrivals."""
    import new_images as new_images_module

    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    _touch_image(str(folder / "IMG_001.JPG"))
    db.add_folder(str(folder))

    call_count = {"n": 0}
    real_count = new_images_module.count_new_images_for_workspace

    def counting(*args, **kwargs):
        call_count["n"] += 1
        return real_count(*args, **kwargs)

    monkeypatch.setattr(
        new_images_module, "count_new_images_for_workspace", counting,
    )

    with app.test_client() as client:
        first = client.post("/api/workspaces/active/new-images/snapshot")
        assert first.status_code == 200
        assert first.get_json()["file_count"] == 1
        calls_after_first = call_count["n"]
        assert calls_after_first >= 1

        # User copies another image, then clicks again.
        _touch_image(str(folder / "IMG_002.JPG"))
        second = client.post("/api/workspaces/active/new-images/snapshot")
        assert second.status_code == 200
        assert second.get_json()["file_count"] == 2, (
            "second click must recompute and pick up newly-copied file"
        )
        assert call_count["n"] > calls_after_first, (
            f"second click must trigger a fresh walk; "
            f"call_count went {calls_after_first} -> {call_count['n']}"
        )


def test_post_snapshot_returns_pending_when_cache_is_incomplete(app_and_db, monkeypatch):
    import threading

    import new_images as new_images_module

    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    _touch_image(str(folder / "IMG_001.JPG"))
    db.add_folder(str(folder))

    release = threading.Event()
    started = threading.Event()
    real_count = new_images_module.count_new_images_for_workspace

    def slow_count(*args, **kwargs):
        started.set()
        release.wait(timeout=5)
        return real_count(*args, **kwargs)

    monkeypatch.setattr(
        new_images_module, "count_new_images_for_workspace", slow_count,
    )

    try:
        with app.test_client() as client:
            resp = client.post("/api/workspaces/active/new-images/snapshot")
            assert resp.status_code == 202
            assert resp.get_json() == {"pending": True}
            assert started.is_set()
    finally:
        release.set()


def test_post_snapshot_zero_new_images_returns_200(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/workspaces/active/new-images/snapshot")
        assert resp.status_code == 200
        assert resp.get_json()["file_count"] == 0


def test_get_snapshot_returns_summary(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    _touch_image(str(folder / "IMG_001.JPG"))

    with app.test_client() as client:
        post = client.post("/api/workspaces/active/new-images/snapshot")
        snap_id = post.get_json()["snapshot_id"]

        resp = client.get(f"/api/workspaces/active/new-images/snapshot/{snap_id}")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["file_count"] == 1
        assert data["folder_paths"] == [str(folder)]
        assert data["files_sample"][0].endswith("IMG_001.JPG")


def test_get_snapshot_unknown_id_returns_404(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    with app.test_client() as client:
        resp = client.get("/api/workspaces/active/new-images/snapshot/99999")
        assert resp.status_code == 404


def test_get_snapshot_oversized_id_returns_404_not_500(app_and_db):
    """Werkzeug's <int:> converter accepts arbitrary digit strings, producing
    Python ints larger than SQLite's signed 64-bit range. Passing those
    straight to the DB would raise OverflowError (→ 500). Treat them as
    "not found" rather than leaking a server error."""
    app, db, ws_id, tmp_path = app_and_db
    huge = 10 ** 100
    with app.test_client() as client:
        resp = client.get(f"/api/workspaces/active/new-images/snapshot/{huge}")
        assert resp.status_code == 404, (
            f"oversized snapshot id must yield 404, got {resp.status_code}"
        )


def test_get_snapshot_cross_workspace_returns_404(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    snap_id = db.create_new_images_snapshot(["/tmp/a.jpg"])
    other = db.create_workspace("Other")
    # Persist the switch so per-request Database instances restore "Other" as
    # the active workspace (Database.__init__ picks the workspace with the most
    # recent last_opened_at).
    from datetime import datetime
    db.update_workspace(other, last_opened_at=datetime.now().isoformat())
    db.set_active_workspace(other)
    with app.test_client() as client:
        resp = client.get(f"/api/workspaces/active/new-images/snapshot/{snap_id}")
        assert resp.status_code == 404


def test_new_images_preview_returns_folder_preview_shape(app_and_db):
    """POST /api/import/new-images-preview returns the same shape as
    folder-preview so the pipeline renderer can group and display files."""
    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "shoot"
    folder.mkdir()
    db.add_folder(str(folder), name="shoot")
    _touch_image(str(folder / "IMG_001.JPG"))
    _touch_image(str(folder / "sub" / "IMG_002.JPG"))

    with app.test_client() as client:
        post = client.post("/api/workspaces/active/new-images/snapshot")
        snap_id = post.get_json()["snapshot_id"]

        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        data = resp.get_json()

    assert data["total_count"] == 2
    assert data["total_size"] > 0
    assert data["duplicate_count"] == 0
    assert ".jpg" in data["type_breakdown"]
    assert data["type_breakdown"][".jpg"] == 2
    assert len(data["files"]) == 2

    files_by_name = {f["filename"]: f for f in data["files"]}
    assert set(files_by_name) == {"IMG_001.JPG", "IMG_002.JPG"}
    for f in data["files"]:
        assert f["path"]
        assert f["extension"] == ".jpg"
        assert f["size"] > 0
        assert "thumb_url" in f
        assert f["subfolder"]

    subfolders = {f["subfolder"] for f in data["files"]}
    assert len(subfolders) == 2


def test_new_images_preview_missing_snapshot_id(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/import/new-images-preview", json={})
        assert resp.status_code == 400


def test_new_images_preview_unknown_snapshot_returns_404(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    with app.test_client() as client:
        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": 99999},
        )
        assert resp.status_code == 404


def test_new_images_preview_scopes_roots_to_active_workspace(app_and_db):
    """Subfolder grouping must only consider folders in the active workspace.
    A folder in a different workspace whose path is a longer prefix of a
    snapshot file path must not win the prefix match and leak its name."""
    app, db, ws_a, tmp_path = app_and_db

    # Workspace A owns /photos/shoot_a (active when we add it, auto-linked)
    shoot_a = tmp_path / "photos" / "shoot_a"
    shoot_a.mkdir(parents=True)
    _touch_image(str(shoot_a / "pic.jpg"))
    db.add_folder(str(shoot_a), name="shoot_a-in-ws-A")

    # Workspace B owns /photos/shoot_a/inner — a longer prefix that, if
    # not filtered by workspace, would steal the subfolder label. Switch
    # active workspace before creating so add_folder auto-links to B only.
    ws_b = db.create_workspace("Other")
    db.set_active_workspace(ws_b)
    inner = shoot_a / "inner"
    inner.mkdir()
    _touch_image(str(inner / "deep.jpg"))
    db.add_folder(str(inner), name="inner-in-ws-B")
    db.set_active_workspace(ws_a)

    snap_id = db.create_new_images_snapshot([
        str(shoot_a / "pic.jpg"),
        str(inner / "deep.jpg"),
    ])

    with app.test_client() as client:
        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        data = resp.get_json()

    subfolders = {f["subfolder"] for f in data["files"]}
    for sf in subfolders:
        assert "inner-in-ws-B" not in sf, (
            f"Leaked folder label from workspace B: {sf}"
        )


def test_new_images_preview_groups_by_top_level_root_not_scanned_descendants(app_and_db):
    """The scanner auto-registers every descendant folder and auto-links
    it to the active workspace (db.py:1108, scanner.py _ensure_folder).
    A preview that uses all linked folders as candidate roots would pick
    the deepest nested descendant as the subfolder label, hiding the
    actual top-level source root. Verify grouping resolves to the
    user-mapped root, not an auto-registered subfolder."""
    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    (root / "trip1").mkdir(parents=True)
    _touch_image(str(root / "edge.jpg"))
    _touch_image(str(root / "trip1" / "bird.jpg"))

    # Simulate the post-scan state: top-level root + auto-registered
    # descendant both linked to the active workspace.
    root_id = db.add_folder(str(root), name="shoot")
    db.add_folder(str(root / "trip1"), name="trip1", parent_id=root_id)

    snap_id = db.create_new_images_snapshot([
        str(root / "edge.jpg"),
        str(root / "trip1" / "bird.jpg"),
    ])

    with app.test_client() as client:
        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        data = resp.get_json()

    files_by_name = {f["filename"]: f for f in data["files"]}
    # The top-level root's basename must appear in every subfolder so the
    # user sees which source the files came from — not just "trip1".
    for fname, f in files_by_name.items():
        assert "shoot" in f["subfolder"], (
            f"{fname} grouped under {f['subfolder']!r} — lost its top-level root"
        )


def test_new_images_preview_disambiguates_duplicate_basenames(app_and_db):
    """Two mapped roots with the same basename (e.g. /mnt/cardA/DCIM and
    /mnt/cardB/DCIM) must produce distinct subfolder labels — otherwise
    the preview grid groups their files together and a single group-level
    checkbox toggles photos from unrelated sources."""
    app, db, ws_id, tmp_path = app_and_db

    # Two sources with identical leaf names.
    card_a = tmp_path / "mnt" / "cardA" / "DCIM"
    card_b = tmp_path / "mnt" / "cardB" / "DCIM"
    card_a.mkdir(parents=True)
    card_b.mkdir(parents=True)
    _touch_image(str(card_a / "a.jpg"))
    _touch_image(str(card_b / "b.jpg"))
    db.add_folder(str(card_a), name="DCIM")
    db.add_folder(str(card_b), name="DCIM")

    snap_id = db.create_new_images_snapshot([
        str(card_a / "a.jpg"),
        str(card_b / "b.jpg"),
    ])

    with app.test_client() as client:
        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        data = resp.get_json()

    subfolders = {f["subfolder"] for f in data["files"]}
    assert len(subfolders) == 2, (
        f"Expected two distinct group labels, got {subfolders!r}"
    )
    for sf in subfolders:
        assert "DCIM" in sf


def test_new_images_preview_skips_missing_files(app_and_db):
    """If a path in the snapshot no longer exists on disk, skip it rather
    than 500ing — the file may have been moved or deleted since snapshot."""
    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "shoot"
    folder.mkdir()
    db.add_folder(str(folder), name="shoot")
    _touch_image(str(folder / "here.jpg"))

    # Snapshot includes a path that doesn't exist on disk.
    snap_id = db.create_new_images_snapshot([
        str(folder / "here.jpg"),
        str(folder / "gone.jpg"),
    ])

    with app.test_client() as client:
        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        data = resp.get_json()
    assert data["total_count"] == 1
    assert data["files"][0]["filename"] == "here.jpg"
