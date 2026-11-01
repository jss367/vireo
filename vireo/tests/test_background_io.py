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
