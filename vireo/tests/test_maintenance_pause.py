"""Pause integration at real per-photo and database transaction boundaries."""
import json
import os
import threading
import time

import pytest
from PIL import Image
from wait import wait_for_job_via_client


def _wait_status(runner, job_id, status):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        job = runner.get(job_id)
        if job["status"] == status:
            return job
        time.sleep(0.01)
    pytest.fail(f"Expected {status}: {runner.get(job_id)}")


def _second_photo(db, photo_id):
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],),
    ).fetchone()["path"]
    path = os.path.join(folder, "second.jpg")
    Image.new("RGB", (80, 60), "blue").save(path)
    return db.add_photo(
        folder_id=photo["folder_id"], filename="second.jpg", extension=".jpg",
        file_size=os.path.getsize(path), file_mtime=os.path.getmtime(path),
        width=80, height=60,
    )


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_move_pause_reconciles_folder_counts(client_with_photo, monkeypatch, tmp_path, action):
    import move

    app, db, first = client_with_photo
    second = _second_photo(db, first)
    source_id = db.get_photo(first)["folder_id"]
    db.update_folder_counts()
    destination = tmp_path / "moved"
    entered, release = threading.Event(), threading.Event()
    copies = []
    original = move._copy_and_verify

    def copy(src, dst):
        copies.append(src)
        if len(copies) == 1:
            entered.set()
            assert release.wait(5)
        return original(src, dst)

    monkeypatch.setattr(move, "_copy_and_verify", copy)
    client = app.test_client()
    runner = app._job_runner
    job_id = client.post("/api/jobs/move-photos", json={
        "photo_ids": [first, second], "destination": str(destination),
    }).get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert len(copies) == 1
        moved_folder = db.get_photo(first)["folder_id"]
        tree = {f["id"]: f for f in db.get_folder_tree()}
        assert tree[source_id]["photo_count"] == 1
        assert tree[moved_folder]["photo_count"] == 1
        db.conn.execute("UPDATE photos SET rating=3 WHERE id=?", (first,))
        db.conn.commit()
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        job = wait_for_job_via_client(client, job_id)
        assert job["status"] == ("completed" if action == "resume" else "cancelled")
        tree = {f["id"]: f for f in db.get_folder_tree()}
        assert tree[source_id]["photo_count"] == (0 if action == "resume" else 1)
        assert tree[moved_folder]["photo_count"] == (2 if action == "resume" else 1)
        assert job["result"]["moved"] == (2 if action == "resume" else 1)
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("phase", ["predictions", "metadata"])
@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_culling_pauses_during_metadata_loading(client_with_photo, monkeypatch, phase, action):
    from web.background_jobs import JobLaunch

    app, db, first = client_with_photo
    _second_photo(db, first)
    db.conn.execute("UPDATE photos SET phash='0000000000000000'")
    db.conn.commit()
    entered, release = threading.Event(), threading.Event()
    queries = []
    match = "FROM predictions pr" if phase == "predictions" else "SELECT quality_score, sharpness"
    original = JobLaunch.thread_db

    class Connection:
        def __init__(self, conn):
            self.conn = conn

        def __getattr__(self, name):
            return getattr(self.conn, name)

        def execute(self, query, *args):
            if match in query:
                queries.append(query)
                if len(queries) == 1:
                    entered.set()
                    assert release.wait(5)
            return self.conn.execute(query, *args)

    def thread_db(ctx):
        database = original(ctx)
        database.conn = Connection(database.conn)
        return database

    monkeypatch.setattr(JobLaunch, "thread_db", thread_db)
    client = app.test_client()
    runner = app._job_runner
    job_id = client.post("/api/jobs/cull", json={}).get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert len(queries) == 1
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        job = wait_for_job_via_client(client, job_id)
        assert job["status"] == ("completed" if action == "resume" else "cancelled")
        assert len(queries) == (2 if action == "resume" else 1)
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_lightroom_import_pauses_during_catalog_read(client_with_photo, monkeypatch, tmp_path, action):
    import sqlite3

    import catalog
    from test_importer import _create_test_catalog

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    source = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],),
    ).fetchone()["path"]
    catalog_path = tmp_path / "keywords.lrcat"
    entries = [(photo["filename"], "", [("Bird", None)])]
    entries.extend((f"extra{i}.jpg", "", [("Bird", None)]) for i in range(256))
    _create_test_catalog(str(catalog_path), source + os.sep, entries)
    entered, release = threading.Event(), threading.Event()
    processed = []
    original = catalog._build_hierarchy_path

    def hierarchy(*args):
        processed.append(True)
        if len(processed) == 1:
            entered.set()
            assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(catalog, "_build_hierarchy_path", hierarchy)
    client = app.test_client()
    runner = app._job_runner
    job_id = client.post("/api/jobs/import", json={
        "catalogs": [str(catalog_path)],
    }).get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert len(processed) == 1
        # A paused import must not retain the external catalog's read lock.
        with sqlite3.connect(str(catalog_path), timeout=0.5) as writer:
            writer.execute("UPDATE AgLibraryKeyword SET name=name")
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        job = wait_for_job_via_client(client, job_id)
        assert job["status"] == ("completed" if action == "resume" else "cancelled")
        if action == "resume":
            assert len(processed) == 257
            assert job["result"]["imported"] == 1
            assert job["result"]["skipped"] == 256
        else:
            assert len(processed) == 1
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_previews_pause_before_eviction(client_with_photo, monkeypatch, action):
    import app as app_module

    app, db, photo_id = client_with_photo
    entered = threading.Event()
    release = threading.Event()
    evictions = []
    original = app_module.materialize_preview

    def materialize(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(app_module, "materialize_preview", materialize)
    monkeypatch.setattr(
        app_module, "evict_preview_cache_if_over_quota",
        lambda *args: evictions.append(True),
    )
    client = app.test_client()
    runner = app._job_runner
    job_id = client.post("/api/jobs/previews", json={"photo_ids": [photo_id]}).get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert not evictions
        db.conn.execute("UPDATE photos SET rating=3 WHERE id=?", (photo_id,))
        db.conn.commit()
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        job = wait_for_job_via_client(client, job_id)
        assert job["status"] == ("completed" if action == "resume" else "cancelled")
        assert bool(evictions) == (action == "resume")
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("phase", ["scan", "thumbnails"])
@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_full_import_pauses_between_phases(client_with_photo, monkeypatch, phase, action):
    import scanner
    import thumbnails

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    source = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],),
    ).fetchone()["path"]
    previous_count = db.conn.execute("SELECT COUNT(*) FROM collections").fetchone()[0]
    entered = threading.Event()
    release = threading.Event()
    thumbs_started = threading.Event()

    def finish_phase():
        entered.set()
        assert release.wait(5)

    def scan(*args, **kwargs):
        if phase == "scan":
            finish_phase()

    def generate(*args, **kwargs):
        thumbs_started.set()
        if phase == "thumbnails":
            finish_phase()
        return {"generated": 1, "skipped": 0, "failed": 0}

    monkeypatch.setattr(scanner, "scan", scan)
    monkeypatch.setattr(thumbnails, "generate_all", generate)
    client = app.test_client()
    runner = app._job_runner
    response = client.post("/api/jobs/import-full", json={"source": source, "copy": False})
    assert response.status_code == 200, response.get_json()
    job_id = response.get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert thumbs_started.is_set() == (phase == "thumbnails")
        assert db.conn.execute("SELECT COUNT(*) FROM collections").fetchone()[0] == previous_count
        db.conn.execute("UPDATE photos SET rating=3 WHERE id=?", (photo_id,))
        db.conn.commit()
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        job = wait_for_job_via_client(client, job_id)
        assert job["status"] == ("completed" if action == "resume" else "cancelled")
        expected_count = previous_count + (1 if action == "resume" else 0)
        assert db.conn.execute("SELECT COUNT(*) FROM collections").fetchone()[0] == expected_count
        if action == "resume":
            assert job["result"]["collection_id"] is not None
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_card_scan_pauses_during_discovery(app_and_db, monkeypatch, tmp_path, action):
    import card_cleanup

    app, _db = app_and_db
    source = tmp_path / "card"
    source.mkdir()
    (source / "bird.jpg").write_bytes(b"photo")
    entered = threading.Event()
    release = threading.Event()
    continued = threading.Event()

    def walk(path, *, cancel_check=None, **kwargs):
        entered.set()
        assert release.wait(5)
        assert cancel_check is not None
        if cancel_check():
            raise card_cleanup.ScanCancelled("cancelled")
        continued.set()
        yield str(source), [], ["bird.jpg"]

    monkeypatch.setattr(card_cleanup, "safe_scan_walk", walk)
    client = app.test_client()
    runner = app._job_runner
    job_id = client.post("/api/card-cleanup/scan", json={
        "source": str(source), "recursive": True,
    }).get_json()["job_id"]
    manifest = card_cleanup.manifest_path(app.config["CARD_CLEANUP_DIR"], job_id)
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert not continued.is_set()
        assert not os.path.exists(manifest)
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        assert finished["status"] == ("completed" if action == "resume" else "cancelled")
        assert os.path.exists(manifest) == (action == "resume")
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
@pytest.mark.parametrize("phase", ["source_root", "file_discovery"])
def test_staging_verification_pauses_during_enumeration(
    client_with_photo, monkeypatch, action, phase,
):
    from contextlib import contextmanager

    import staging_recovery

    app, _db, _photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    root = os.path.join(vireo_dir, "staging", "pipeline-pause")
    source = os.path.join(root, "photos")
    os.makedirs(source)
    for name in ("bird.jpg", ".hidden.txt"):
        with open(os.path.join(source, name), "w") as f:
            f.write("staged data")
    entered = threading.Event()
    release = threading.Event()
    enumerated = []
    original = staging_recovery.os.scandir
    target = root if phase == "source_root" else source

    @contextmanager
    def scandir(path):
        with original(path) as entries:
            if str(path) != target:
                yield entries
                return

            def delayed_entries():
                for entry in entries:
                    enumerated.append(entry.name)
                    if len(enumerated) == 1:
                        entered.set()
                        assert release.wait(5)
                    yield entry

            yield delayed_entries()

    monkeypatch.setattr(staging_recovery.os, "scandir", scandir)
    client = app.test_client()
    runner = app._job_runner
    job_id = client.post("/api/import/orphaned-staging/verify", json={
        "path": root,
    }).get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert len(enumerated) == 1
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        assert finished["status"] == ("completed" if action == "resume" else "cancelled")
        if action == "resume":
            assert finished["result"]["file_count"] == 2
            assert finished["result"]["unaccounted"] == 2
        else:
            assert len(enumerated) == 1
        assert os.path.exists(os.path.join(source, "bird.jpg"))
        assert os.path.exists(os.path.join(source, ".hidden.txt"))
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_culling_pauses_before_publishing_final_result(client_with_photo, monkeypatch, action):
    import culling

    app, db, _photo_id = client_with_photo
    entered = threading.Event()
    release = threading.Event()
    result = {
        "total_photos": 1, "suggested_keepers": 1, "suggested_rejects": 0,
        "species_groups": [],
    }

    def analyze(*args, pause_callback, **kwargs):
        pause_callback()
        entered.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(culling, "analyze_for_culling", analyze)
    cache = os.path.join(
        os.path.dirname(app.config["THUMB_CACHE_DIR"]),
        f"culling_results_ws{db._ws_id()}.json",
    )
    with open(cache, "w") as f:
        json.dump({"previous": True}, f)
    client = app.test_client()
    runner = app._job_runner
    job_id = client.post("/api/jobs/cull", json={}).get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        with open(cache) as f:
            assert json.load(f) == {"previous": True}
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        assert finished["status"] == ("completed" if action == "resume" else "cancelled")
        with open(cache) as f:
            assert json.load(f) == (result if action == "resume" else {"previous": True})
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
@pytest.mark.parametrize("endpoint", ["prepare-full-resolution", "offline-cache"])
def test_cache_pause_finishes_current_photo_and_preserves_progress(
    client_with_photo, monkeypatch, action, endpoint,
):
    import offline_cache

    app, db, first = client_with_photo
    second = _second_photo(db, first)
    runner = app._job_runner
    client = app.test_client()
    entered = threading.Event()
    release = threading.Event()
    calls = []
    original = offline_cache.cache_photo_original

    def cache(*args, **kwargs):
        calls.append(args[1]["id"])
        if len(calls) == 1:
            entered.set()
            assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(offline_cache, "cache_photo_original", cache)
    job_id = client.post(
        f"/api/jobs/{endpoint}", json={"photo_ids": [first, second]},
    ).get_json()["job_id"]
    try:
        assert entered.wait(5)
        response = client.post(f"/api/jobs/{job_id}/pause")
        assert response.status_code == 200
        assert runner.get(job_id)["status"] == "pausing"
        release.set()
        paused = _wait_status(runner, job_id, "paused")
        assert paused["progress"]["current"] == 1
        assert calls == [first]
        assert db.offline_original_get(first) is not None
        assert db.offline_original_get(second) is None
        # Pausing must release the writer, so unrelated catalog work can run.
        db.conn.execute("UPDATE photos SET rating=3 WHERE id=?", (first,))
        db.conn.commit()
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        if action == "resume":
            assert finished["status"] == "completed", finished
            assert calls == [first, second]
            assert db.offline_original_get(second) is not None
        else:
            assert finished["status"] == "cancelled", finished
            assert calls == [first]
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("photo_count", [1, 2])
@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_hash_verification_commits_before_pause(
    client_with_photo, monkeypatch, photo_count, action,
):
    import scanner

    app, db, first = client_with_photo
    if photo_count == 2:
        _second_photo(db, first)
    client = app.test_client()
    runner = app._job_runner
    original = scanner.compute_file_hash
    entered = threading.Event()
    release = threading.Event()
    calls = []

    def hash_file(path, *args, **kwargs):
        calls.append(path)
        if len(calls) == 1:
            entered.set()
            assert release.wait(5)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(scanner, "compute_file_hash", hash_file)
    job_id = client.post("/api/jobs/verify-hashes").get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert client.post(f"/api/jobs/{job_id}/pause").status_code == 200
        release.set()
        _wait_status(runner, job_id, "paused")
        assert len(calls) == 1
        # The first hash was in an uncommitted batch when Pause arrived.
        assert db.conn.execute(
            "SELECT COUNT(*) FROM photos WHERE hash_status='ok'",
        ).fetchone()[0] == 1
        assert not db.get_audit_runs()
        db.conn.execute("UPDATE photos SET rating=4 WHERE id=?", (first,))
        db.conn.commit()
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        assert finished["status"] == ("completed" if action == "resume" else "cancelled")
        assert len(calls) == (photo_count if action == "resume" else 1)
        assert bool(db.get_audit_runs()) == (action == "resume")
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("endpoint", ["capture-time", "inat-export"])
@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_final_file_write_pauses_after_completion(
    client_with_photo, monkeypatch, tmp_path, endpoint, action,
):
    from types import SimpleNamespace

    app, db, photo_id = client_with_photo
    entered = threading.Event()
    release = threading.Event()
    writing = threading.Event()

    def finish_write():
        writing.set()
        entered.set()
        assert release.wait(5)
        writing.clear()

    if endpoint == "capture-time":
        import capture_time

        def run(*args, **kwargs):
            finish_write()
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        def refresh(database, ident, path):
            database.conn.execute("UPDATE photos SET rating=2 WHERE id=?", (ident,))

        monkeypatch.setattr(capture_time.shutil, "which", lambda _: "/usr/bin/exiftool")
        monkeypatch.setattr(capture_time.subprocess, "run", run)
        monkeypatch.setattr(capture_time, "_refresh_photo_metadata", refresh)
        url = "/api/jobs/capture-time"
        body = {"photo_ids": [photo_id], "mode": "manual", "shift_minutes": 60}
    else:
        import inat_export

        output = tmp_path / "exported.jpg"

        def export(*args, **kwargs):
            finish_write()
            output.write_bytes(b"exported photo")
            return str(output)

        monkeypatch.setattr(inat_export, "export_inat_photo", export)
        url = "/api/inat/export"
        body = {"submissions": [{"photo_id": photo_id}], "destination": str(tmp_path)}

    client = app.test_client()
    runner = app._job_runner
    response = client.post(url, json=body)
    assert response.status_code == 200, response.get_json()
    job_id = response.get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        assert runner.get(job_id)["status"] == "pausing"
        release.set()
        _wait_status(runner, job_id, "paused")
        assert not writing.is_set()
        if endpoint == "capture-time":
            assert db.get_photo(photo_id)["rating"] == 2
        else:
            assert output.exists()
        db.conn.execute("UPDATE photos SET rating=3 WHERE id=?", (photo_id,))
        db.conn.commit()
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        assert finished["status"] == ("completed" if action == "resume" else "cancelled")
        if endpoint == "capture-time":
            assert finished["result"]["updated"] == 1
        else:
            assert len(finished["result"]["exported"]) == 1
            assert finished["result"]["revealed"] is False
            assert output.exists()
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_previews_pause_after_final_photo_defers_eviction(
    client_with_photo, monkeypatch, action,
):
    """A pause requested during the final preview waits until eviction.

    The precompute loop must park after the last photo and before
    ``evict_preview_cache_if_over_quota`` runs, so a pause accepted during
    the final iteration is not silently overtaken by completion after
    eviction has already unlinked files and rewritten catalog rows.
    """
    import app as app_module
    from preview_materializer import materialize_preview as real_materialize

    app, db, first = client_with_photo
    second = _second_photo(db, first)
    runner = app._job_runner
    client = app.test_client()
    entered = threading.Event()
    release = threading.Event()
    materialized_ids = []
    eviction_started = threading.Event()

    def materialize(*args, **kwargs):
        photo = args[1] if len(args) > 1 else kwargs.get("photo")
        materialized_ids.append(photo["id"])
        if len(materialized_ids) == 2:
            entered.set()
            assert release.wait(5)
        return real_materialize(*args, **kwargs)

    def evict(*_args, **_kwargs):
        eviction_started.set()

    monkeypatch.setattr(app_module, "materialize_preview", materialize)
    monkeypatch.setattr(
        app_module, "evict_preview_cache_if_over_quota", evict,
    )

    response = client.post("/api/jobs/previews", json={})
    assert response.status_code == 200, response.get_json()
    job_id = response.get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        assert runner.get(job_id)["status"] == "pausing"
        release.set()
        _wait_status(runner, job_id, "paused")
        # The pause was requested during the last preview; the post-loop
        # checkpoint must park before eviction runs.
        assert not eviction_started.is_set()
        # Pausing releases the writer, so unrelated catalog work can run.
        db.conn.execute("UPDATE photos SET rating=4 WHERE id=?", (first,))
        db.conn.commit()
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        if action == "resume":
            assert finished["status"] == "completed", finished
            assert eviction_started.is_set()
        else:
            assert finished["status"] == "cancelled", finished
            assert not eviction_started.is_set()
        assert sorted(materialized_ids) == sorted([first, second])
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_callback_checkpoint_retains_state_and_unwinds_on_cancel(app_and_db, action):
    from web.background_jobs import JobLaunch

    app, _db = app_and_db
    runner = app._job_runner
    ctx = JobLaunch(runner, None, None, None)
    entered = threading.Event()
    release = threading.Event()
    steps = []
    cleaned_up = threading.Event()

    def work(job):
        try:
            steps.append("first")
            entered.set()
            assert release.wait(5)
            ctx.checkpoint(job)
            steps.append("second")
            return {"steps": steps}
        finally:
            cleaned_up.set()

    job_id = ctx.start_job("test-checkpoint", work, pausable=True)
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert steps == ["first"]
        assert not cleaned_up.is_set()
        getattr(runner, f"{action}_job")(job_id)
        expected = "completed" if action == "resume" else "cancelled"
        finished = _wait_status(runner, job_id, expected)
        assert steps == (["first", "second"] if action == "resume" else ["first"])
        assert cleaned_up.wait(5)
        assert finished["errors"] == []
    finally:
        release.set()
        runner.cancel_job(job_id)


def test_import_result_poll_waits_through_long_pause():
    """A paused staging verification must not unlock its destructive next step."""
    import json
    import shutil
    import subprocess
    from pathlib import Path

    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for the polling behavior test")
    template = (Path(__file__).parents[1] / "templates" / "import.html").read_text(encoding="utf-8")
    start = template.index("async function pollJobResult(jobId)")
    end = template.index("\n}", start) + 2
    script = r'''
const assert = require('node:assert/strict');
let polls = 0;
const fetch = async () => ({ok: true, json: async () => {
  polls++;
  return polls <= 200
    ? {status: polls === 1 ? 'pausing' : 'paused', result: null}
    : {status: 'completed', result: {verified: 2}};
}});
const setTimeout = (callback) => callback();
FUNCTION
(async () => {
  assert.deepEqual(await pollJobResult('verify-1'), {verified: 2});
  assert.equal(polls, 201);
})().catch(error => { console.error(error); process.exitCode = 1; });
'''.replace("FUNCTION", template[start:end])
    result = subprocess.run([node, "-e", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, json.dumps({"stdout": result.stdout, "stderr": result.stderr})


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_sharpness_auto_flags_observe_pause_and_cancel(
    client_with_photo, monkeypatch, action,
):
    import sharpness
    from db import Database

    app, db, first = client_with_photo
    second = _second_photo(db, first)
    runner = app._job_runner
    client = app.test_client()
    entered = threading.Event()
    release = threading.Event()
    flagged = []
    original = Database.update_photo_flag

    monkeypatch.setattr(sharpness, "score_collection_photos", lambda *args, **kwargs: {
        "results": [
            {"photo_id": pid, "sharpness": 100, "group_size": 2, "is_best": True}
            for pid in (first, second)
        ],
    })

    def flag_photo(worker_db, photo_id, flag, **kwargs):
        if not flagged:
            entered.set()
            assert release.wait(5)
        original(worker_db, photo_id, flag, **kwargs)
        flagged.append(photo_id)

    monkeypatch.setattr(Database, "update_photo_flag", flag_photo)
    response = client.post("/api/jobs/sharpness", json={})
    assert response.status_code == 200
    job_id = response.get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert client.post(f"/api/jobs/{job_id}/pause").status_code == 200
        assert runner.get(job_id)["status"] == "pausing"
        release.set()
        _wait_status(runner, job_id, "paused")
        assert flagged == [first]
        assert db.get_photo(first)["flag"] == "flagged"
        assert db.get_photo(second)["flag"] == "none"
        # The first flag's transaction must finish before the next checkpoint.
        db.conn.execute("UPDATE photos SET rating=3 WHERE id=?", (second,))
        db.conn.commit()
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        if action == "resume":
            assert finished["status"] == "completed", finished
            assert finished["result"]["auto_flagged"] == 2
            assert flagged == [first, second]
        else:
            assert finished["status"] == "cancelled", finished
            assert flagged == [first]
            assert db.get_photo(second)["flag"] == "none"
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_export_observes_pause_after_metadata_finishes(
    client_with_photo, monkeypatch, tmp_path, action,
):
    import export

    app, db, photo_id = client_with_photo
    db.conn.execute("UPDATE photos SET timestamp=? WHERE id=?", ("2026-08-01T12:30:00", photo_id))
    db.conn.commit()
    client = app.test_client()
    runner = app._job_runner
    entered = threading.Event()
    release = threading.Event()
    reaped = threading.Event()

    def metadata_batch(jobs, cancel_check=None):
        entered.set()
        assert release.wait(5)
        # The live subprocess's probe must not park its parent.
        assert cancel_check() is False
        reaped.set()
        return len(jobs), []

    monkeypatch.setattr(export, "_write_export_metadata_batch", metadata_batch)
    response = client.post("/api/jobs/export", json={
        "photo_ids": [photo_id], "destination": str(tmp_path / "export"),
        "metadata_fields": ["capture_date"], "reveal_after_export": False,
    })
    assert response.status_code == 200
    job_id = response.get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert client.post(f"/api/jobs/{job_id}/pause").status_code == 200
        assert runner.get(job_id)["status"] == "pausing"
        release.set()
        _wait_status(runner, job_id, "paused")
        assert reaped.is_set()
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        assert finished["status"] == ("completed" if action == "resume" else "cancelled"), finished
        assert finished["result"]["exported"] == 1
    finally:
        release.set()
        runner.cancel_job(job_id)


def test_ingest_can_pause_during_discovery(app_and_db, monkeypatch, tmp_path):
    import ingest

    app, _db = app_and_db
    source = tmp_path / "card"
    source.mkdir()
    Image.new("RGB", (8, 8)).save(source / "bird.jpg")
    entered = threading.Event()
    release = threading.Event()
    continued = threading.Event()

    def walk(path, *, cancel_check=None, **kwargs):
        entered.set()
        assert release.wait(5)
        assert cancel_check is not None
        assert cancel_check() is False
        continued.set()
        yield str(source), [], ["bird.jpg"]

    monkeypatch.setattr(ingest, "safe_scan_walk", walk)
    client = app.test_client()
    runner = app._job_runner
    job_id = client.post("/api/jobs/ingest", json={
        "source": str(source), "destination": str(tmp_path / "archive"),
    }).get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert not continued.is_set()
        assert runner.cancel_job(job_id)
        finished = wait_for_job_via_client(client, job_id)
        assert finished["status"] == "cancelled", finished
        assert not continued.is_set()
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
@pytest.mark.parametrize("skip_duplicates", [True, False])
def test_ingest_pauses_between_metadata_batches(
    app_and_db, monkeypatch, tmp_path, action, skip_duplicates,
):
    from datetime import datetime

    import import_dedup
    import ingest

    app, _db = app_and_db
    source = tmp_path / "card"
    source.mkdir()
    destination = tmp_path / "archive"
    for i in range(101):
        Image.new("RGB", (8, 8)).save(source / f"bird-{i:03}.jpg")
    entered = threading.Event()
    release = threading.Event()
    batches = []

    def timestamps(files):
        batches.append(list(files))
        if len(batches) == 1:
            entered.set()
            assert release.wait(5)
        return {f: datetime(2026, 8, 1, 12, 30) for f in files}

    # Duplicate preparation and folder planning use the same reader through
    # different imports; cover both the dedup and no-dedup preparation paths.
    monkeypatch.setattr(import_dedup, "source_capture_timestamps", timestamps)
    monkeypatch.setattr(ingest, "source_capture_timestamps", timestamps)
    client = app.test_client()
    runner = app._job_runner
    job_id = client.post("/api/jobs/ingest", json={
        "source": str(source), "destination": str(destination),
        "skip_duplicates": skip_duplicates,
    }).get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert [len(batch) for batch in batches] == [100]
        assert not list(destination.rglob("*.jpg"))
        assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        finished = wait_for_job_via_client(client, job_id)
        if action == "resume":
            assert finished["status"] == "completed", finished
            assert [len(batch) for batch in batches] == [100, 1]
            assert finished["result"]["copied"] == 101
        else:
            assert finished["status"] == "cancelled", finished
            assert [len(batch) for batch in batches] == [100]
            assert not list(destination.rglob("*.jpg"))
    finally:
        release.set()
        runner.cancel_job(job_id)


def test_snapshot_backed_import_in_place_is_not_pausable(app_and_db, tmp_path):
    """Snapshot-backed in-place imports serialize on a shared per-snapshot
    lock for the entire worker call. Pausing inside that critical section
    would sleep while holding the lock, so a queued second import for the
    same snapshot would block on the bare lock acquisition and could not
    reach any runner checkpoint — cancelling it would be a no-op until the
    first import resumed. Confirm this mode advertises pausable=False; the
    plain in-place import (no snapshot) stays pausable.
    """
    app, db = app_and_db
    client = app.test_client()
    runner = app._job_runner

    plain_source = tmp_path / "plain"
    plain_source.mkdir()
    Image.new("RGB", (8, 8), "green").save(plain_source / "plain.jpg")
    plain_resp = client.post("/api/jobs/import-in-place", json={
        "sources": [str(plain_source)],
        "after_import": None,
    })
    assert plain_resp.status_code == 200, plain_resp.get_json()
    plain_job_id = plain_resp.get_json()["job_id"]
    wait_for_job_via_client(client, plain_job_id)
    assert runner.get(plain_job_id).get("pausable") is True

    snap_root = tmp_path / "snap"
    snap_root.mkdir()
    frozen = snap_root / "frozen.jpg"
    Image.new("RGB", (8, 8), "red").save(frozen)
    db.add_folder(str(snap_root), name="snap")
    snap_id = db.create_new_images_snapshot([str(frozen)])
    snap_resp = client.post("/api/jobs/import-in-place", json={
        "source_snapshot_id": snap_id,
        "after_import": None,
    })
    assert snap_resp.status_code == 200, snap_resp.get_json()
    snap_job_id = snap_resp.get_json()["job_id"]
    wait_for_job_via_client(client, snap_job_id)
    assert runner.get(snap_job_id).get("pausable") is False


def test_import_photos_checkpoints_before_after_import_chain(
    app_and_db, tmp_path, monkeypatch,
):
    """A Pause requested between run_import_job's last checkpoint and the
    after-import chain must be observed. Without tags/GPS,
    ``_apply_import_tags`` returns without probing the runner, so a cancel
    that lands during that pause would otherwise still let
    ``_chain_after_import`` create the import collection and enqueue the
    child Process job. The pre-chain checkpoint sleeps through the pause and
    sees the cancellation, so no collection is created.
    """
    import import_job

    app, db = app_and_db
    client = app.test_client()
    runner = app._job_runner

    folder = tmp_path / "archive"
    folder.mkdir()
    photo_path = folder / "landed.jpg"
    Image.new("RGB", (8, 8), "red").save(photo_path)
    folder_id = db.add_folder(str(folder), name="archive")
    photo_id = db.add_photo(
        folder_id=folder_id, filename="landed.jpg", extension=".jpg",
        file_size=os.path.getsize(photo_path),
        file_mtime=os.path.getmtime(photo_path),
        width=8, height=8,
    )
    previous_collections = db.conn.execute(
        "SELECT COUNT(*) FROM collections",
    ).fetchone()[0]
    cull_ready_id = next(
        pr["id"] for pr in db.get_saved_processes()
        if pr["name"] == "Cull-ready"
    )

    entered = threading.Event()
    release = threading.Event()

    def fake_run_import_job(job, runner_arg, db_path_arg, workspace_id, params):
        # Park at run_import_job's own pause-safe boundary. The main thread
        # requests pause here so that when this fake returns, ``_apply_import_tags``
        # (with no tags configured) does not probe the runner and the new
        # pre-chain checkpoint is the first place the pause is observed.
        entered.set()
        assert release.wait(5)
        return {
            "ok": True,
            "photo_ids": [photo_id],
            "discovered": 1,
            "copied": 0,
            "skipped_duplicate": 0,
            "failed": 0,
            "total": 1,
            "safe_to_format": True,
        }

    monkeypatch.setattr(import_job, "run_import_job", fake_run_import_job)

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (8, 8), "blue").save(card / "src.jpg")
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [str(card)],
        "destination": str(folder),
        "after_import": cull_ready_id,
        "trust_likely_duplicates": True,
    })
    assert resp.status_code == 200, resp.get_json()
    job_id = resp.get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert runner.pause_job(job_id)
        release.set()
        _wait_status(runner, job_id, "paused")
        assert db.conn.execute(
            "SELECT COUNT(*) FROM collections",
        ).fetchone()[0] == previous_collections
        assert runner.cancel_job(job_id)
        finished = wait_for_job_via_client(client, job_id)
        assert finished["status"] == "cancelled", finished
        assert db.conn.execute(
            "SELECT COUNT(*) FROM collections",
        ).fetchone()[0] == previous_collections
        assert finished["result"]["after_import_skipped"] == "import cancelled"
        assert "process_job_id" not in finished["result"]
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("endpoint", ["import-photos", "import-in-place"])
def test_import_handoff_rejects_late_parent_pause(request, monkeypatch, tmp_path, endpoint):
    import import_job
    from db import Database
    from services.pipeline_launch import PipelineChain

    children = []

    def enqueue(*args, **kwargs):
        children.append(kwargs["chained_from"])
        return "test-child-process", None, None

    # Patch before app construction captures the bound enqueue method.
    monkeypatch.setattr(PipelineChain, "enqueue_process_job", enqueue)
    app, db, photo_id = request.getfixturevalue("client_with_photo")
    entered, release = threading.Event(), threading.Event()
    original_add = Database.add_collection

    def add_collection(database, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original_add(database, *args, **kwargs)

    monkeypatch.setattr(Database, "add_collection", add_collection)
    monkeypatch.setattr(import_job, "run_import_job", lambda *args, **kwargs: {
        "ok": True, "photo_ids": [photo_id], "discovered": 1,
        "copied": 1, "failed": 0, "total": 1,
    })
    process_id = next(p["id"] for p in db.get_saved_processes() if p["name"] == "Cull-ready")
    source = tmp_path / "card-handoff"
    source.mkdir()
    Image.new("RGB", (8, 8), "blue").save(source / "new.jpg")
    body = {"sources": [str(source)], "after_import": process_id}
    if endpoint == "import-photos":
        body["destination"] = str(tmp_path / "archive-handoff")
    client = app.test_client()
    runner = app._job_runner
    response = client.post(f"/api/jobs/{endpoint}", json=body)
    assert response.status_code == 200, response.get_json()
    job_id = response.get_json()["job_id"]
    try:
        assert entered.wait(5)
        assert not children
        assert client.post(f"/api/jobs/{job_id}/pause").status_code == 409
        assert client.post(f"/api/jobs/{job_id}/cancel").status_code == 404
        release.set()
        job = wait_for_job_via_client(client, job_id)
        assert job["status"] == "completed", job
        assert job["result"]["process_job_id"] == "test-child-process"
        assert children == [job_id]
    finally:
        release.set()
        runner.shutdown(timeout=5)
