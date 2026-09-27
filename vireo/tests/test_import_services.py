"""Import jobs can be admitted and completed without an HTTP context."""

from unittest.mock import Mock

import pytest
from db import Database
from jobs import JobRunner
from PIL import Image
from services.imports import ImportFailure, ImportService
from wait import wait_for_job_via_runner


@pytest.fixture
def import_service(tmp_path, monkeypatch):
    import config as cfg

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    db_path = str(tmp_path / "catalog.db")
    db = Database(db_path)
    db.set_active_workspace(db.ensure_default_workspace())
    runner = JobRunner(db)
    settings = {
        "THUMB_CACHE_DIR": str(tmp_path / "thumbs"),
        "REQUIRE_EXIFTOOL_FOR_IMPORT": False,
    }
    service = ImportService(
        lambda: runner, db_path, settings,
        invalidate_missing_originals=Mock(),
        enqueue_process_job=Mock(),
        chain_after_move=Mock(),
        bulk_gps_location_payload=Mock(),
    )
    yield service, db, runner
    assert runner.shutdown(timeout=30)
    db.close()


@pytest.mark.parametrize("method", ["import_in_place", "import_photos"])
def test_import_runs_without_flask_context(import_service, tmp_path, method):
    service, db, runner = import_service
    source = tmp_path / "card"
    source.mkdir()
    Image.new("RGB", (32, 32), "red").save(source / "bird.jpg")
    archive = tmp_path / "archive"
    body = {
        "sources": [str(source)],
        "destination": str(archive),
        "folder_template": "trip",
        "after_import": None,
        "new_workspace_name": "Birding trip",
        "tags": ["Birding"],
    }

    response = getattr(service, method)(db, body)
    assert isinstance(response, dict)
    assert response["workspace"]["name"] == "Birding trip"
    job = wait_for_job_via_runner(runner, response["job_id"], wait_for_history=True)
    assert job["status"] == "completed", job.get("errors")
    assert job["workspace_id"] == response["workspace"]["id"]
    result = job["result"]
    assert result["ok"]
    assert len(result["photo_ids"]) == 1
    assert result["collection_id"]
    assert result["tagging"]["tagged_photos"] == 1
    assert result["after_import_skipped"] == "import-only"
    assert (source / "bird.jpg").exists()
    if method == "import_photos":
        assert (archive / "trip" / "bird.jpg").exists()
    else:
        assert not archive.exists()
    service.enqueue_process_job.assert_not_called()
    service.invalidate_missing_originals.assert_called()


def test_snapshot_replay_without_flask_context(import_service, tmp_path):
    service, db, runner = import_service
    source = tmp_path / "registered"
    source.mkdir()
    photo = source / "bird.jpg"
    Image.new("RGB", (32, 32), "blue").save(photo)
    db.add_folder(str(source), name="registered")
    snapshot_id = db.create_new_images_snapshot([str(photo)])
    body = {"source_snapshot_id": snapshot_id, "after_import": None}

    def launch():
        response = service.import_in_place(db, body)
        assert isinstance(response, dict)
        job = wait_for_job_via_runner(runner, response["job_id"], wait_for_history=True)
        assert job["status"] == "completed", job.get("errors")
        return job["result"]

    first = launch()
    replay = launch()
    assert first["imported"] == 1
    assert first["collection_id"]
    assert replay["imported"] == 0
    assert "collection_id" not in replay
    service.enqueue_process_job.assert_not_called()


@pytest.mark.parametrize("method", ["import_in_place", "import_photos"])
def test_admission_failures_and_live_settings_without_flask_context(
    import_service, monkeypatch, method,
):
    import metadata

    service, db, runner = import_service
    launch = getattr(service, method)
    original_workspace = db._active_workspace_id
    assert launch(db, {}) == ImportFailure("sources must be a non-empty list of paths")

    # The app can change this setting after constructing the service.
    service.config["REQUIRE_EXIFTOOL_FOR_IMPORT"] = True
    status = {"available": False, "error": "missing"}
    monkeypatch.setattr(metadata, "exiftool_status", lambda: status)
    failure = launch(db, {"new_workspace_name": "Should not be created"})
    assert isinstance(failure, ImportFailure)
    assert failure.status == 409
    assert failure.details == {"code": "exiftool_required", "exiftool": status}
    assert db._active_workspace_id == original_workspace
    assert runner.list_jobs() == []
