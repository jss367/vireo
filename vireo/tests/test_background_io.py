"""Transfers take priority over independent background filesystem work."""

import threading

import pytest
from jobs import JobRunner
from testing.waits import synchronization_timeout
from wait import wait_for_job_via_runner


@pytest.mark.parametrize("transfer_type", ["send-to-nas", "move-folder", "move-photos", "future-transfer"])
@pytest.mark.parametrize("finish", ["complete", "cancel", "shutdown"])
def test_background_io_waits_visibly_and_can_be_stopped(transfer_type, finish):
    runner = JobRunner()
    release = threading.Event()
    waiting = threading.Event()
    resumed = threading.Event()
    original_push = runner.push_event

    def push(job_id, kind, payload):
        original_push(job_id, kind, payload)
        if payload.get("phase") == "Waiting for photo transfer":
            waiting.set()

    runner.push_event = push
    def transfer_work(job):
        while not release.wait(0.01):
            if runner.is_cancelled(job["id"]):
                return {}
        return {}

    transfer, _, _ = runner.start_singleton(
        transfer_type, transfer_work, singleton_key="transfer",
        workspace_id=2, exclusive_workspace=transfer_type == "future-transfer", pausable=False,
    )

    def work(job):
        runner.push_event(job["id"], "progress", {"phase": "Storage maintenance", "current": 7})
        runner.wait_for_background_io(job["id"])
        resumed.set()
        return {}

    background = runner.start("retained-cache-maintenance", work, workspace_id=None)
    try:
        assert waiting.wait(synchronization_timeout(5))
        progress = runner.get(background)["progress"]
        assert progress["phase"] == "Waiting for photo transfer"
        assert progress["current"] == 7
        assert not resumed.is_set()
        if finish == "complete":
            release.set()
            wait_for_job_via_runner(runner, transfer)
        elif finish == "cancel":
            runner.cancel_job(background)
        else:
            assert runner.shutdown()
        job = wait_for_job_via_runner(runner, background)
        assert job["status"] == ("completed" if finish == "complete" else "cancelled"), job
        assert resumed.is_set() == (finish == "complete")
        assert job["progress"]["phase"] == "Storage maintenance"
    finally:
        release.set()
        runner.shutdown()


@pytest.mark.parametrize("status, expected", [
    ("queued", True), ("running", True), ("pausing", True),
    ("paused", True), ("completed", False), ("failed", False), ("cancelled", False),
])
def test_transfer_priority_covers_only_live_io(status, expected):
    runner = JobRunner()
    with runner._lock:
        runner._jobs["transfer"] = {"type": "send-to-nas", "status": status}
    assert runner.storage_transfer_active() is expected
    runner._jobs.clear()


def test_unrelated_work_does_not_block_background_io():
    runner = JobRunner()
    with runner._lock:
        runner._jobs["processing"] = {"type": "pipeline", "status": "running"}
    assert not runner.storage_transfer_active()
    runner.wait_for_background_io()
    runner._jobs.clear()


def test_automatic_missing_originals_check_defers_to_nas_transfer(app_and_db):
    app, db = app_and_db
    runner = app._job_runner
    release = threading.Event()
    transfer = runner.start(
        "send-to-nas", lambda _: release.wait(synchronization_timeout(5)),
        workspace_id=db.active_workspace_id,
    )
    try:
        data = app.test_client().post(
            "/api/photos/missing/check", json={"automatic": True},
        ).get_json()
        assert data["suppressed"] is True
        assert data["reason"] == "heavy_job_active"
        assert not any(job["type"] == "missing_originals_scan" for job in runner.list_jobs())
    finally:
        release.set()
        wait_for_job_via_runner(runner, transfer)


def test_folder_health_abandons_observations_when_transfer_starts(app_and_db, tmp_path, monkeypatch):
    import os

    from services import missing_originals, photo_ingest, workspace_folders

    app, db = app_and_db
    runner = app._job_runner
    first_path, second_path = str(tmp_path / "first"), str(tmp_path / "second")
    first = workspace_folders.add_folder(db, first_path)
    second = workspace_folders.add_folder(db, second_path)
    photo = photo_ingest.add_photo(db, second, "bird.jpg", ".jpg", 12, 1)
    db.conn.execute("UPDATE photos SET last_move_source_folder_path=? WHERE id=?", (first_path, photo))
    db.commit()
    release = threading.Event()
    transfer_ids = []
    real_exists = os.path.exists

    def exists(path):
        assert not db.conn.in_transaction, "network probes must not retain a catalog writer"
        if path == second_path:
            transfer_ids.append(runner.start(
                "send-to-nas", lambda _: release.wait(synchronization_timeout(5)),
                workspace_id=db.active_workspace_id,
            ))
        return real_exists(path)

    service = missing_originals.MissingOriginals(
        db_path=db._db_path, config=app.config, get_runner=lambda: runner,
    )
    monkeypatch.setattr(missing_originals.os.path, "exists", exists)
    try:
        with pytest.raises(missing_originals._FolderHealthDeferred):
            db.folders.check_health(checkpoint=service._folder_health_checkpoint)
        assert db.folders.get(first)["status"] == "ok"
        assert db.folders.get(second)["status"] == "ok"
        assert db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id=?", (photo,),
        ).fetchone()[0] == first_path
        assert not db.conn.in_transaction
    finally:
        release.set()
        for job_id in transfer_ids:
            wait_for_job_via_runner(runner, job_id)


@pytest.mark.parametrize("automatic", [True, False])
@pytest.mark.parametrize("transfer_type", ["send-to-nas", "move-folder", "move-photos"])
def test_running_automatic_missing_scan_yields_and_can_retry(app_and_db, monkeypatch, automatic, transfer_type):
    from services import missing_originals

    app, db = app_and_db
    runner = app._job_runner
    entered, next_probe, release = threading.Event(), threading.Event(), threading.Event()
    probes = []
    service = missing_originals.MissingOriginals(
        db_path=db._db_path, config=app.config, get_runner=lambda: runner,
    )

    def build_rows(connection, *, cancel_callback, **kwargs):
        assert not cancel_callback()
        probes.append("first")
        entered.set()
        assert next_probe.wait(synchronization_timeout(5))
        assert not cancel_callback()
        probes.append("second")
        return []

    monkeypatch.setattr(service, "build_rows", build_rows)
    scan = service.start_scan(db, automatic=automatic)
    try:
        assert entered.wait(synchronization_timeout(5))
        transfer = runner.start(
            transfer_type, lambda _: release.wait(synchronization_timeout(10)),
            workspace_id=db.active_workspace_id,
        )
        next_probe.set()
        # The scan must finish BEFORE the transfer is released: parking here
        # would deadlock a transfer waiting for this workspace's scan to drain.
        job = wait_for_job_via_runner(runner, scan["job_id"])
        assert job["status"] == "completed", job
        assert not job["errors"]
        assert not service.inflight and not service.errors
        if automatic:
            assert job["result"]["deferred"] is True
            assert job["summary"] == "Check deferred until photo transfer finishes"
            assert probes == ["first"]
            assert not service.cache
        else:
            assert not job["result"].get("deferred")
            assert probes == ["first", "second"]
            assert service.cache
        release.set()
        wait_for_job_via_runner(runner, transfer)
        if automatic:
            previous = {job["id"] for job in runner.list_jobs()}
            retry = service.start_scan(db, automatic=True)
            retry_id = retry["job_id"] or next(
                job["id"] for job in runner.list_jobs() if job["id"] not in previous
            )
            finished = wait_for_job_via_runner(runner, retry_id)
            assert finished["status"] == "completed", finished
            assert not finished["result"].get("deferred")
            assert service.cache
    finally:
        next_probe.set()
        release.set()


def test_automatic_missing_scan_stops_between_live_symlink_target_probes(app_and_db, tmp_path, monkeypatch):
    import os

    import dir_listing_cache
    from services import missing_originals, photo_ingest, workspace_folders

    app, db = app_and_db
    runner = app._job_runner
    root = tmp_path / "photos"
    root.mkdir()
    folder = workspace_folders.add_folder(db, str(root))
    names = ("first.jpg", "second.jpg")
    for name in names:
        photo_ingest.add_photo(db, folder, name, ".jpg", 12, 1)
    listing = dir_listing_cache.DirListing(names, frozenset(), frozenset(names))
    monkeypatch.setattr(dir_listing_cache, "read_directory", lambda *a, **kw: listing)
    paths = {str(root / name) for name in names}
    probes, transfers = [], []
    release = threading.Event()
    real_exists = os.path.exists
    ws_id = db.active_workspace_id

    def exists(path):
        if path in paths:
            probes.append(path)
            if not transfers:
                transfers.append(runner.start(
                    "send-to-nas", lambda _: release.wait(synchronization_timeout(10)),
                    workspace_id=ws_id,
                ))
            return True
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", exists)
    service = missing_originals.MissingOriginals(
        db_path=db._db_path, config=app.config, get_runner=lambda: runner,
    )
    scan = service.start_scan(db, automatic=True)
    try:
        job = wait_for_job_via_runner(runner, scan["job_id"])
        assert job["result"]["deferred"] is True, job
        assert probes == [str(root / names[0])]
        assert not service.cache and not service.errors and not service.inflight
        release.set()
        wait_for_job_via_runner(runner, transfers[0])
        previous = {job["id"] for job in runner.list_jobs()}
        retry = service.start_scan(db, automatic=True)
        retry_id = retry["job_id"] or next(job["id"] for job in runner.list_jobs() if job["id"] not in previous)
        finished = wait_for_job_via_runner(runner, retry_id)
        assert finished["status"] == "completed", finished
        assert not finished["result"].get("deferred")
        assert probes[-2:] == [str(root / name) for name in names]
        assert service.cache and not service.errors
    finally:
        release.set()
