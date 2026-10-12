"""Import jobs can be admitted and completed without an HTTP context."""

import json
from unittest.mock import Mock

import pytest
from db import Database
from jobs import JobRunner
from PIL import Image
from repositories.processes import ProcessesRepository
from services.imports import ImportFailure, ImportService
from wait import wait_for_job_via_runner


def _interrupted_in_place(db, config, *, job_id="interrupted", status="failed"):
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, config, result, workspace_id, started_at) "
        "VALUES (?, 'import-in-place', ?, ?, ?, ?, '2026-10-09T12:00:00')",
        (job_id, status, json.dumps(config), json.dumps({"interrupted": True}), db.active_workspace_id),
    )
    db.commit()
    return job_id


def test_restart_in_place_reuses_catalog_and_restores_settings(import_service, tmp_path):
    import scanner

    service, db, runner = import_service
    source = tmp_path / "photos"
    source.mkdir()
    Image.new("RGB", (32, 32), "red").save(source / "saved.jpg")
    scanner.scan(str(source), db, incremental=True)
    saved_id = db.conn.execute("SELECT id FROM photos WHERE filename='saved.jpg'").fetchone()[0]
    Image.new("RGB", (32, 32), "blue").save(source / "remaining.jpg")
    nested = source / "excluded"
    nested.mkdir()
    Image.new("RGB", (32, 32)).save(nested / "excluded.jpg")
    workspace_count = len(db.workspaces.list_all())
    parent_id = _interrupted_in_place(db, {
        "sources": [str(source)], "recursive": False, "tags": ["Birding"],
        "location_from_gps": False, "after_import": None,
        "created_workspace": {"id": db.active_workspace_id, "name": "Original"},
    })

    response = service.restart_in_place(db, parent_id)
    assert isinstance(response, dict), response
    job = wait_for_job_via_runner(runner, response["job_id"], wait_for_history=True)
    assert job["status"] == "completed", job
    assert job["workspace_id"] == db.active_workspace_id
    assert job["config"]["restarted_from"] == parent_id
    assert job["config"]["recursive"] is False
    assert len(db.workspaces.list_all()) == workspace_count
    rows = db.conn.execute("SELECT id, filename FROM photos").fetchall()
    assert {row["filename"] for row in rows} == {"saved.jpg", "remaining.jpg"}
    assert next(row["id"] for row in rows if row["filename"] == "saved.jpg") == saved_id
    assert set(job["result"]["photo_ids"]) == {row["id"] for row in rows}
    assert job["result"]["tagging"]["tagged_photos"] == 2
    assert {photo["id"] for photo in db.get_collection_photos(
        job["result"]["collection_id"], per_page=100,
    )} == {row["id"] for row in rows}

    # The parent's button must never launch the same work twice, even
    # after its child finished. A failed child would offer its own restart.
    failure = service.restart_in_place(db, parent_id)
    assert failure.status == 409
    assert failure.details == {"restart_job_id": job["id"]}


def test_restart_snapshot_finishes_processing_for_already_cataloged_files(import_service, tmp_path):
    import scanner

    service, db, runner = import_service
    source = tmp_path / "photos"
    source.mkdir()
    selected = source / "selected.jpg"
    Image.new("RGB", (32, 32), "red").save(selected)
    db.add_folder(str(source), name="photos")
    snapshot_id = db.workspaces.create_new_images_snapshot([str(selected)])
    scanner.scan(str(source), db, incremental=True)
    Image.new("RGB", (32, 32), "blue").save(source / "unselected.jpg")
    process_id = db.processes.create("After import")
    frozen_process = db.resolve_process(process_id)
    parent_id = _interrupted_in_place(db, {
        "sources": [str(source)], "source_snapshot_id": snapshot_id,
        "after_import": process_id, "after_import_snapshot": frozen_process,
        "tags": ["Birding"],
    })
    db.update_saved_process(process_id, skip_classify=True)
    service.enqueue_process_job.return_value = ("processing", None, None)

    response = service.restart_in_place(db, parent_id)
    assert isinstance(response, dict), response
    job = wait_for_job_via_runner(runner, response["job_id"], wait_for_history=True)
    assert job["status"] == "completed", job
    assert job["result"]["imported"] == 0
    assert job["result"]["already_cataloged"] == 1
    assert job["result"]["process_job_id"] == "processing"
    assert job["result"]["tagging"]["tagged_photos"] == 1
    assert {row[0] for row in db.conn.execute("SELECT filename FROM photos")} == {"selected.jpg"}
    service.enqueue_process_job.assert_called_once()
    assert service.enqueue_process_job.call_args.kwargs["expanded"] == frozen_process


@pytest.mark.parametrize("never_started", [False, True])
def test_restart_in_place_does_not_duplicate_processing(import_service, tmp_path, never_started):
    service, db, runner = import_service
    source = tmp_path / "photos"
    source.mkdir()
    Image.new("RGB", (32, 32)).save(source / "bird.jpg")
    process_id = db.processes.create("After import")
    parent_id = _interrupted_in_place(db, {
        "sources": [str(source)], "after_import": process_id,
        "after_import_owner": "earlier-import", "skip_after_import": True,
    })
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, config, result, workspace_id) "
        "VALUES ('prior-processing', 'pipeline', 'failed', ?, ?, ?)",
        (json.dumps({"chained_from": "earlier-import"}), json.dumps({"never_started": never_started}),
         db.active_workspace_id),
    )
    db.commit()
    service.enqueue_process_job.return_value = ("new-processing", None, None)
    response = service.restart_in_place(db, parent_id)
    assert isinstance(response, dict), response
    job = wait_for_job_via_runner(runner, response["job_id"], wait_for_history=True)
    assert job["status"] == "completed", job
    if never_started:
        service.enqueue_process_job.assert_called_once()
        assert job["result"]["process_job_id"] == "new-processing"
    else:
        service.enqueue_process_job.assert_not_called()
        assert "already started" in job["result"]["after_import_skipped"]
        assert "collection_id" not in job["result"]


def test_restart_in_place_serializes_two_tabs_before_child_is_persisted(
    import_service, tmp_path, monkeypatch,
):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from services.import_in_place import _InPlaceImportJob
    from testing.waits import synchronization_timeout

    service, db, runner = import_service
    source = tmp_path / "photos"
    source.mkdir()
    parent_id = _interrupted_in_place(db, {"sources": [str(source)], "after_import": None})
    entered = threading.Event()
    release = threading.Event()
    barrier = threading.Barrier(2)

    def blocked_work(_self, _job):
        entered.set()
        assert release.wait(synchronization_timeout(5))
        return {"ok": True}

    monkeypatch.setattr(_InPlaceImportJob, "work", blocked_work)
    monkeypatch.setattr(runner, "_record_job_started", lambda _job: None)
    monkeypatch.setattr(runner, "_ensure_checkpoint_thread", lambda: None)
    workspace_id = db.active_workspace_id

    def restart():
        with Database(service.db_path) as request_db:
            request_db.set_active_workspace(workspace_id)
            barrier.wait(timeout=synchronization_timeout(5))
            return service.restart_in_place(request_db, parent_id)

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(restart) for _ in range(2)]
            responses = [future.result(timeout=synchronization_timeout(10)) for future in futures]
        assert entered.wait(synchronization_timeout(5))
        launched = [response for response in responses if isinstance(response, dict)]
        refused = [response for response in responses if isinstance(response, ImportFailure)]
        assert len(launched) == len(refused) == 1
        assert refused[0].details == {"restart_job_id": launched[0]["job_id"]}
    finally:
        release.set()
    wait_for_job_via_runner(runner, launched[0]["job_id"], wait_for_history=True)


@pytest.mark.parametrize("status", ["running", "queued", "completed"])
def test_restart_in_place_rejects_nonrestartable_jobs(import_service, tmp_path, status):
    service, db, runner = import_service
    parent_id = _interrupted_in_place(db, {"sources": [str(tmp_path)]}, status=status)
    assert service.restart_in_place(db, parent_id).status == 409
    assert runner.list_jobs() == []


def test_restart_in_place_revalidates_sources_and_workspace(import_service, tmp_path):
    service, db, runner = import_service
    parent_id = _interrupted_in_place(db, {
        "sources": [str(tmp_path / "missing")], "after_import": None,
    }, status="cancelled")
    assert "source directory not found" in service.restart_in_place(db, parent_id).message
    original_ws = db.active_workspace_id
    db.set_active_workspace(db.create_workspace("Other"))
    failure = service.restart_in_place(db, parent_id)
    assert failure.status == 409
    assert "original workspace" in failure.message
    db.set_active_workspace(original_ws)
    assert service.restart_in_place(db, "missing").status == 404
    assert runner.list_jobs() == []


@pytest.mark.parametrize("config", [None, "not json", "[]"])
def test_restart_in_place_missing_saved_settings(import_service, config):
    service, db, runner = import_service
    parent_id = _interrupted_in_place(db, {})
    db.conn.execute("UPDATE job_history SET config=? WHERE id=?", (config, parent_id))
    db.commit()
    failure = service.restart_in_place(db, parent_id)
    assert failure.status == 409
    assert "settings are no longer available" in failure.message
    assert runner.list_jobs() == []


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
