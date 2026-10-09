"""Import jobs can be admitted and completed without an HTTP context."""

from unittest.mock import Mock

import pytest
from db import Database
from jobs import JobRunner
from PIL import Image
from repositories.processes import ProcessesRepository
from services.imports import ImportFailure, ImportService
from wait import wait_for_job_via_runner


@pytest.mark.parametrize("path_style", ["windows", "posix"])
@pytest.mark.parametrize("same_bytes", [True, False])
def test_relocated_descendant_landings_use_native_paths(monkeypatch, path_style, same_bytes):
    """A moved descendant remains recoverable across native path separators."""
    import ntpath
    import posixpath
    from types import SimpleNamespace

    import import_job
    import services.imports as imports

    path = ntpath if path_style == "windows" else posixpath
    folder = r"C:\Photos\NAS" if path_style == "windows" else "/photos/nas"
    old_folder = r"C:\Photos\archive" if path_style == "windows" else "/photos/archive"
    old_fingerprint = f"{old_folder}/bird.jpg|s=123|h=original"
    current_hash = "original" if same_bytes else "replacement"
    fingerprint = f"{folder}/bird.jpg|s=123|h={current_hash}"
    service = Mock()
    service._capture_photo_fingerprints_for_ids.return_value = {5: fingerprint}
    takeover = {
        "descendant_photo_fingerprints": {"5": old_fingerprint},
        "descendant_landed_files": {},
    }
    parent_fingerprints = {5: old_fingerprint}
    # Replace only each module's os reference, leaving pytest and the host
    # filesystem on their real platform while exercising Windows semantics.
    monkeypatch.setattr(imports, "os", SimpleNamespace(path=path))
    monkeypatch.setattr(import_job, "os", SimpleNamespace(path=path))
    ImportService._recover_relocated_descendant_landings(
        service, Mock(), takeover, parent_fingerprints,
    )
    catalog = Mock()
    catalog.conn.execute.return_value.fetchall.return_value = [{
        "id": 5, "filename": "bird.jpg", "companion_path": None,
        "file_hash": current_hash,
    }]
    state = SimpleNamespace(recovered_photo_ids=set())
    import_job._recover_parent_landings(
        state, SimpleNamespace(recover_landed_files=takeover["descendant_landed_files"]),
        catalog,
    )
    assert state.recovered_photo_ids == ({5} if same_bytes else set())
    if same_bytes:
        catalog.conn.execute.assert_called_once()
        assert catalog.conn.execute.call_args.args[1] == (folder,)
        # Persisted fingerprint strings retain their existing format.
        assert parent_fingerprints[5] == fingerprint
    else:
        assert parent_fingerprints[5] == old_fingerprint


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
    snapshot_id = db.workspaces.create_new_images_snapshot([str(photo)])
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


def _source_body(tmp_path, method, name):
    source = tmp_path / "card"
    source.mkdir(exist_ok=True)
    Image.new("RGB", (32, 32), "green").save(source / "bird.jpg")
    body = {"sources": [str(source)], "new_workspace_name": name}
    if method == "import_photos":
        body["destination"] = str(tmp_path / "archive")
        body["folder_template"] = "trip"
    return body


def _assert_workspace_rolled_back(db, runner, name, original_workspace):
    """A rejected new-workspace import leaves no workspace and no switch."""
    assert not any(ws["name"] == name for ws in db.workspaces.list_all())
    assert db._active_workspace_id == original_workspace
    assert runner.list_jobs() == []


def test_in_place_default_after_import_failure_rolls_back_new_workspace(
    import_service, tmp_path,
):
    """The omitted after_import resolves to the default process only after
    the workspace switch; a default naming a deleted process must still
    leave no orphan workspace and keep the previous workspace active."""
    import config as cfg

    service, db, runner = import_service
    original_workspace = db._active_workspace_id
    cfg.save({"pipeline": {"default_process_id": 987654}})
    body = _source_body(tmp_path, "import_in_place", "Default gone")

    failure = service.import_in_place(db, body)

    assert failure == ImportFailure("unknown process id: 987654")
    _assert_workspace_rolled_back(db, runner, "Default gone", original_workspace)


def test_in_place_process_snapshot_failure_rolls_back_new_workspace(
    import_service, tmp_path, monkeypatch,
):
    """A process deleted between validation and the enqueue-time snapshot
    makes ``resolve_process`` raise after the switch; roll the workspace back."""
    service, db, runner = import_service
    original_workspace = db._active_workspace_id
    monkeypatch.setattr(ProcessesRepository, "get", lambda self, pid: {"id": pid})

    def gone(pid):
        raise ValueError(f"process {pid} was deleted")

    monkeypatch.setattr(db, "resolve_process", gone)
    body = _source_body(tmp_path, "import_in_place", "Snapshot gone")
    body["after_import"] = 4242

    failure = service.import_in_place(db, body)

    assert failure == ImportFailure("process 4242 was deleted", 404)
    _assert_workspace_rolled_back(db, runner, "Snapshot gone", original_workspace)


def test_import_photos_competing_retry_rolls_back_new_workspace(
    import_service, tmp_path, monkeypatch,
):
    """The competing-retry 409 runs after the workspace switch."""
    import services.import_photos as import_photos

    service, db, runner = import_service
    original_workspace = db._active_workspace_id
    monkeypatch.setattr(
        import_photos, "_competing_retry_failure",
        lambda runner, job_config: ImportFailure("retry already running", 409),
    )
    body = _source_body(tmp_path, "import_photos", "Competing retry")
    body["after_import"] = None

    failure = service.import_photos(db, body)

    assert failure == ImportFailure("retry already running", 409)
    _assert_workspace_rolled_back(db, runner, "Competing retry", original_workspace)


@pytest.mark.parametrize("method", ["import_in_place", "import_photos"])
def test_exception_after_workspace_switch_rolls_back_new_workspace(
    import_service, tmp_path, monkeypatch, method,
):
    """An exception between the switch and a started job (here the runner
    refusing to start) must not leak the workspace either."""
    service, db, runner = import_service
    original_workspace = db._active_workspace_id

    def refuse(*args, **kwargs):
        raise RuntimeError("JobRunner is shut down")

    monkeypatch.setattr(runner, "start", refuse)
    body = _source_body(tmp_path, method, "Runner down")
    body["after_import"] = None

    with pytest.raises(RuntimeError, match="shut down"):
        getattr(service, method)(db, body)

    _assert_workspace_rolled_back(db, runner, "Runner down", original_workspace)


@pytest.mark.parametrize("method", ["import_in_place", "import_photos"])
def test_workspace_setup_failure_rolls_back_created_workspace(
    import_service, tmp_path, monkeypatch, method,
):
    """``create_workspace`` commits, so a failure in a later setup step of
    ``_prepare_import_workspace`` must delete the half-made workspace."""
    service, db, runner = import_service
    original_workspace = db._active_workspace_id

    def broken(*args, **kwargs):
        raise RuntimeError("collections unavailable")

    monkeypatch.setattr(db, "create_default_collections", broken)
    body = _source_body(tmp_path, method, "Half made")
    body["after_import"] = None

    failure = getattr(service, method)(db, body)

    assert failure == ImportFailure("collections unavailable")
    _assert_workspace_rolled_back(db, runner, "Half made", original_workspace)


@pytest.mark.parametrize("method", ["import_in_place", "import_photos"])
def test_started_import_keeps_new_workspace(import_service, tmp_path, method):
    """The rollback wrapper never undoes a workspace whose job started."""
    service, db, runner = import_service
    body = _source_body(tmp_path, method, "Kept")
    body["after_import"] = None

    response = getattr(service, method)(db, body)

    assert isinstance(response, dict)
    wait_for_job_via_runner(runner, response["job_id"], wait_for_history=True)
    assert any(ws["name"] == "Kept" for ws in db.workspaces.list_all())
    assert db._active_workspace_id == response["workspace"]["id"]
