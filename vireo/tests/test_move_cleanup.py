"""Source cleanup preserves unreviewed files and cataloged originals."""

import json
import os

import pytest
from move_cleanup import cleanup_source, finish_source, review_source


@pytest.fixture
def cleanup_case(app_and_db, tmp_path):
    app, db = app_and_db
    source = tmp_path / "original"
    source.mkdir()
    (source / "orphan.xmp").write_text("editing settings")
    folder_id = db.add_folder(str(source), name="original")
    # Persisted history exercises jobs created before the cleanup feature,
    # including a restart (no matching in-memory job).
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, workspace_id, config, result) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("old-move", "move-folder", "completed", db._active_workspace_id,
         json.dumps({"source_path": str(source), "folder_id": folder_id,
                     "folder_template": "%Y-%m-%d"}),
         json.dumps({"moved": 1, "errors": []})),
    )
    db.conn.commit()
    return app, db, source, folder_id


URL = "/api/jobs/old-move/source-cleanup"


def test_review_and_optional_trash_from_old_history(cleanup_case, monkeypatch):
    app, db, source, _ = cleanup_case
    child = source / "subfolder"
    child.mkdir()
    (child / "notes.txt").write_text("notes")
    calls = []

    def trash(paths):
        calls.extend(paths)
        for path in paths:
            os.unlink(path)
        return len(paths), set(paths), []

    monkeypatch.setattr("app._trash_paths", trash)
    client = app.test_client()
    response = client.get(URL)
    assert response.status_code == 200, response.json
    review = response.json
    assert review["file_count"] == 2
    assert review["xmp_count"] == 1
    assert calls == []
    assert client.post(URL, json={"review_token": review["review_token"]}).status_code == 400
    result = client.post(URL, json={"review_token": review["review_token"], "confirm_trash": True})
    assert result.status_code == 200, result.json
    assert result.json["trashed"] == 2
    assert result.json["state"] == "removed"
    assert not source.exists()
    assert source.parent.exists()
    assert client.get(URL).json["state"] == "removed"


@pytest.mark.parametrize("change", ["added", "edited", "replaced", "symlink"])
def test_cleanup_refuses_changed_review(cleanup_case, monkeypatch, tmp_path, change):
    app, db, source, _ = cleanup_case
    client = app.test_client()
    review = client.get(URL).json
    if change == "added":
        (source / "new.NEF").write_text("new original")
    elif change == "edited":
        (source / "orphan.xmp").write_text("new editing settings")
    elif change == "replaced":
        source.rename(tmp_path / "saved")
        source.mkdir()
        (source / "orphan.xmp").write_text("editing settings")
    else:
        (source / "orphan.xmp").unlink()
        (source / "orphan.xmp").symlink_to(tmp_path / "outside")
    calls = []
    monkeypatch.setattr("app._trash_paths", lambda paths: calls.append(paths))
    response = client.post(URL, json={"confirm_trash": True, "review_token": review["review_token"]})
    assert response.status_code == 409
    assert calls == []
    assert source.exists()


def test_cleanup_refuses_cataloged_photos_in_another_workspace(cleanup_case):
    app, db, source, _ = cleanup_case
    review = review_source(db, str(source))
    ws = db.create_workspace("Another workspace")
    db.set_active_workspace(ws)
    child = source / "child"
    child.mkdir()
    fid = db.add_folder(str(child), name="child")
    db.add_photo(folder_id=fid, filename="keep.NEF", extension=".NEF", file_size=1, file_mtime=1)
    with pytest.raises(ValueError, match="cataloged photos"):
        cleanup_source(db, str(source), review["review_token"], lambda paths: pytest.fail("Trash called"))


def test_partial_trash_failure_keeps_folder_and_supports_retry(cleanup_case):
    _, db, source, _ = cleanup_case
    (source / "other.xmp").write_text("other settings")
    review = review_source(db, str(source))

    def partial(paths):
        os.unlink(paths[0])
        return 1, {paths[0]}, [{"path": paths[1], "error": "Trash unavailable"}]

    result = cleanup_source(db, str(source), review["review_token"], partial)
    assert result["state"] == "remaining"
    assert result["trashed"] == 1
    assert result["file_count"] == 1
    assert result["failures"][0]["error"] == "Trash unavailable"
    assert review_source(db, str(source))["review_token"] != review["review_token"]


def test_finish_removes_only_empty_selected_folder(cleanup_case):
    _, db, source, _ = cleanup_case
    assert finish_source(db, str(source))["state"] == "remaining"
    (source / "orphan.xmp").unlink()
    assert finish_source(db, str(source))["state"] == "removed"
    assert not source.exists()
    assert source.parent.exists()


def test_files_added_during_trash_are_preserved(cleanup_case):
    _, db, source, _ = cleanup_case
    review = review_source(db, str(source))

    def trash(paths):
        for path in paths:
            os.unlink(path)
        (source / "new.NEF").write_text("new original")
        return len(paths), set(paths), []

    result = cleanup_source(db, str(source), review["review_token"], trash)
    assert result["state"] == "remaining"
    assert (source / "new.NEF").read_text() == "new original"


def test_missing_parent_is_not_reported_as_successful_cleanup(cleanup_case, tmp_path):
    _, db, _, _ = cleanup_case
    result = finish_source(db, str(tmp_path / "offline-volume" / "source"))
    assert result["state"] == "unavailable"


def test_cleanup_is_blocked_while_workspace_job_runs(cleanup_case, monkeypatch):
    import threading

    app, _, _, _ = cleanup_case
    runner = app._job_runner
    release = threading.Event()
    runner.start("scan", lambda job: release.wait(5), workspace_id=1)
    try:
        response = app.test_client().get(URL)
        assert response.status_code == 409
        assert "Wait" in response.json["error"]
    finally:
        release.set()


@pytest.mark.parametrize("field,value", [
    ("status", "running"), ("status", "failed"), ("type", "import"),
    ("workspace_id", 999), ("result", '{"moved":1,"errors":["failed"]}'),
])
def test_cleanup_rejects_ineligible_job(cleanup_case, field, value):
    app, db, source, _ = cleanup_case
    db.conn.execute(f"UPDATE job_history SET {field} = ? WHERE id = ?", (value, "old-move"))
    db.conn.commit()
    assert app.test_client().get(URL).status_code in (404, 409)
    assert (source / "orphan.xmp").exists()
