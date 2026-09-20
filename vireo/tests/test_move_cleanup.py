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
    stored = json.loads(db.conn.execute("SELECT result FROM job_history WHERE id = 'old-move'").fetchone()[0])
    assert stored["source_cleanup"]["source_device"] == source.parent.stat().st_dev
    assert client.get(URL + "?summary=1").json["state"] == "removed"


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
        if paths[0].endswith("orphan.xmp"):
            os.unlink(paths[0])
            return 1, {paths[0]}, []
        return 0, set(), [{"path": paths[0], "error": "Trash unavailable"}]

    result = cleanup_source(db, str(source), review["review_token"], partial)
    assert result["state"] == "remaining"
    assert result["trashed"] == 1
    assert result["file_count"] == 1
    assert result["failures"][0]["error"] == "Trash unavailable"
    assert review_source(db, str(source))["review_token"] != review["review_token"]


def test_finish_removes_only_empty_selected_folder(cleanup_case):
    _, db, source, folder_id = cleanup_case
    assert finish_source(db, str(source))["state"] == "remaining"
    (source / "orphan.xmp").unlink()
    assert finish_source(db, str(source))["state"] == "removed"
    assert not source.exists()
    assert source.parent.exists()
    assert db.conn.execute("SELECT 1 FROM folders WHERE id = ?", (folder_id,)).fetchone() is None
    assert db.conn.execute("SELECT 1 FROM workspace_folders WHERE folder_id = ?", (folder_id,)).fetchone() is None
    db.check_folder_health()
    assert str(source) not in [row["path"] for row in db.get_missing_folders()]


def test_cleanup_retires_empty_child_catalog_rows(cleanup_case):
    _, db, source, folder_id = cleanup_case
    child = source / "nested"
    child.mkdir()
    child_id = db.add_folder(str(child), name="nested", parent_id=folder_id)
    review = review_source(db, str(source))

    def trash(paths):
        for path in paths:
            os.unlink(path)
        return len(paths), set(paths), []

    assert cleanup_source(db, str(source), review["review_token"], trash)["state"] == "removed"
    assert db.conn.execute("SELECT 1 FROM folders WHERE id IN (?, ?)", (folder_id, child_id)).fetchone() is None


def test_shared_empty_folder_and_membership_are_retained(cleanup_case):
    _, db, source, folder_id = cleanup_case
    (source / "orphan.xmp").unlink()
    other_workspace = db.create_workspace("Shared folder workspace")
    db.add_workspace_folder(other_workspace, folder_id)
    result = finish_source(db, str(source))
    assert result["state"] == "unavailable"
    assert "another workspace" in result["error"]
    assert source.is_dir()
    assert db.conn.execute("SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
                           (other_workspace, folder_id)).fetchone()


def test_failed_directory_removal_preserves_catalog(cleanup_case, monkeypatch):
    _, db, source, folder_id = cleanup_case
    (source / "orphan.xmp").unlink()
    monkeypatch.setattr("move_cleanup.os.rmdir", lambda path: (_ for _ in ()).throw(PermissionError("denied")))
    assert finish_source(db, str(source))["state"] == "unavailable"
    assert db.conn.execute("SELECT 1 FROM folders WHERE id = ?", (folder_id,)).fetchone()
    assert db.conn.execute("SELECT 1 FROM workspace_folders WHERE folder_id = ?", (folder_id,)).fetchone()


def test_inaccessible_source_is_not_reported_removed(cleanup_case, monkeypatch):
    _, db, source, _ = cleanup_case
    original = os.lstat

    def inaccessible(path, *args, **kwargs):
        if os.fspath(path) == str(source):
            raise PermissionError("source is inaccessible")
        return original(path, *args, **kwargs)

    monkeypatch.setattr("move_cleanup.os.lstat", inaccessible)
    result = finish_source(db, str(source))
    assert result["state"] == "unavailable"
    assert "inaccessible" in result["error"]


@pytest.mark.parametrize("field", ["config", "result"])
@pytest.mark.parametrize("value", ['{invalid', 'null', '[]', '"text"', '1'])
def test_invalid_history_json_returns_conflict(cleanup_case, field, value):
    app, db, source, _ = cleanup_case
    db.conn.execute(f"UPDATE job_history SET {field} = ? WHERE id = ?", (value, "old-move"))
    db.conn.commit()
    response = app.test_client().get(URL)
    assert response.status_code == 409
    assert response.json["error"] == "Cleanup job data is invalid"
    assert (source / "orphan.xmp").exists()


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


def test_missing_source_without_volume_baseline_keeps_catalog(cleanup_case):
    _, db, source, folder_id = cleanup_case
    (source / "orphan.xmp").unlink()
    source.rmdir()
    assert finish_source(db, str(source))["state"] == "unavailable"
    assert db.conn.execute("SELECT 1 FROM folders WHERE id = ?", (folder_id,)).fetchone()


def test_disconnect_during_trash_preserves_catalog_and_reports_failure(cleanup_case, monkeypatch):
    from types import SimpleNamespace

    _, db, source, folder_id = cleanup_case
    review = review_source(db, str(source))
    original_stat, original_lstat = os.stat, os.lstat
    disconnected = False

    def lstat(path, *args, **kwargs):
        if disconnected and os.fspath(path) == str(source):
            raise FileNotFoundError(str(source))
        return original_lstat(path, *args, **kwargs)

    def stat(path, *args, **kwargs):
        if disconnected and os.fspath(path) == str(source.parent):
            return SimpleNamespace(st_dev=review["source_device"] + 1)
        return original_stat(path, *args, **kwargs)

    def trash(paths):
        nonlocal disconnected
        disconnected = True
        return 0, set(), [{"path": paths[0], "error": "volume disconnected"}]

    monkeypatch.setattr("move_cleanup.os.lstat", lstat)
    monkeypatch.setattr("move_cleanup.os.stat", stat)
    result = cleanup_source(db, str(source), review["review_token"], trash)
    assert result["state"] == "unavailable"
    assert result["failures"][0]["error"] == "volume disconnected"
    assert db.conn.execute("SELECT 1 FROM folders WHERE id = ?", (folder_id,)).fetchone()
    assert db.conn.execute("SELECT 1 FROM workspace_folders WHERE folder_id = ?", (folder_id,)).fetchone()
    disconnected = False
    assert (source / "orphan.xmp").read_text() == "editing settings"


def test_device_change_between_inventory_and_removal_keeps_catalog(cleanup_case, monkeypatch):
    import move_cleanup

    _, db, source, folder_id = cleanup_case
    (source / "orphan.xmp").unlink()
    device = source.stat().st_dev
    original_review = move_cleanup.review_source

    def changed_review(db, path, expected_device=None, expected_inode=None):
        if expected_device is not None:
            raise ValueError("The original volume changed")
        return original_review(db, path)

    monkeypatch.setattr(move_cleanup, "review_source", changed_review)
    assert finish_source(db, str(source))["state"] == "unavailable"
    assert source.is_dir()
    assert source.stat().st_dev == device
    assert db.conn.execute("SELECT 1 FROM folders WHERE id = ?", (folder_id,)).fetchone()


def test_cleanup_refuses_job_running_in_another_workspace(cleanup_case, monkeypatch):
    import threading

    app, db, source, _ = cleanup_case
    client = app.test_client()
    review = client.get(URL).json
    other = db.create_workspace("Import workspace")
    release = threading.Event()
    app._job_runner.start("scan", lambda job: release.wait(5), workspace_id=other)
    monkeypatch.setattr("app._trash_paths", lambda paths: pytest.fail("Trash called during another workspace's scan"))
    try:
        response = client.post(URL, json={"confirm_trash": True, "review_token": review["review_token"]})
        assert response.status_code == 409
        assert (source / "orphan.xmp").exists()
    finally:
        release.set()


def test_cleanup_blocks_new_cross_workspace_import_until_trash_finishes(cleanup_case, monkeypatch):
    from jobs import WorkspaceBusyError

    app, db, source, _ = cleanup_case
    client = app.test_client()
    review = client.get(URL).json
    other = db.create_workspace("Import workspace")

    def trash(paths):
        with pytest.raises(WorkspaceBusyError):
            app._job_runner.start("import", lambda job: {}, workspace_id=other)
        for path in paths:
            os.unlink(path)
        return len(paths), set(paths), []

    monkeypatch.setattr("app._trash_paths", trash)
    response = client.post(URL, json={"confirm_trash": True, "review_token": review["review_token"]})
    assert response.status_code == 200, response.json
    assert response.json["state"] == "removed"
    assert not source.exists()


@pytest.mark.parametrize("tracked", [True, False])
def test_case_alias_catalog_rows_are_protected(cleanup_case, monkeypatch, tracked):
    _, db, source, folder_id = cleanup_case
    alias = str(source.parent / source.name.upper())
    db.conn.execute("UPDATE folders SET path = ? WHERE id = ?", (alias, folder_id))
    db.conn.commit()
    if tracked:
        db.add_photo(folder_id=folder_id, filename="orphan.NEF", extension=".NEF", file_size=1, file_mtime=1)
    else:
        other = db.create_workspace("Alias workspace")
        db.add_workspace_folder(other, folder_id)
    original_exists, original_samefile = os.path.exists, os.path.samefile

    def exists(path):
        return True if os.fspath(path) == alias else original_exists(path)

    def samefile(a, b):
        if {os.fspath(a), os.fspath(b)} == {alias, str(source)}:
            return True
        return original_samefile(a, b)

    monkeypatch.setattr(os.path, "exists", exists)
    monkeypatch.setattr(os.path, "samefile", samefile)
    with pytest.raises(ValueError, match="cataloged photos" if tracked else "another workspace"):
        review_source(db, str(source))


def test_auto_cleanup_keeps_replacement_directory(cleanup_case, monkeypatch):
    import move

    _, db, source, folder_id = cleanup_case
    (source / "orphan.xmp").unlink()
    (source / "photo.NEF").write_text("original")
    db.add_photo(folder_id=folder_id, filename="photo.NEF", extension=".NEF",
                 file_size=8, file_mtime=1, timestamp="2026-07-11T10:00:00")
    original_move = move.move_photos

    def move_then_replace(*args, **kwargs):
        result = original_move(*args, **kwargs)
        source.rename(source.parent / "renamed-original")
        source.mkdir()
        return result

    monkeypatch.setattr(move, "move_photos", move_then_replace)
    result = move.move_folder_by_date(db, folder_id, str(source.parent / "archive"), "%Y-%m-%d")
    assert result["moved"] == 1
    assert result["source_cleanup"]["state"] == "unavailable"
    assert "replaced" in result["source_cleanup"]["error"]
    assert source.is_dir()
    assert db.conn.execute("SELECT 1 FROM folders WHERE id = ?", (folder_id,)).fetchone()


@pytest.mark.parametrize("kind", ["workspace", "folder"])
@pytest.mark.parametrize("location", ["same", "ancestor", "descendant"])
def test_staged_source_mappings_block_cleanup_after_catalog_rebase(cleanup_case, monkeypatch, kind, location):
    app, db, source, folder_id = cleanup_case
    client = app.test_client()
    token = client.get(URL).json["review_token"]
    managed = str(source.parent / "managed-copy")
    mapped_source = {"same": str(source), "ancestor": str(source.parent),
                     "descendant": str(source / "child")}[location]
    db.conn.execute("UPDATE folders SET path = ? WHERE id = ?", (managed, folder_id))
    if kind == "workspace":
        db.conn.execute("INSERT INTO local_workspaces (workspace_id, state) VALUES (?, 'active')",
                        (db._active_workspace_id,))
        db.conn.execute("INSERT INTO local_workspace_folders (workspace_id, folder_id, source_path, local_path) "
                        "VALUES (?, ?, ?, ?)", (db._active_workspace_id, folder_id, mapped_source, managed))
    else:
        db.conn.execute("INSERT INTO local_folders (root_folder_id, state) VALUES (?, 'active')", (folder_id,))
        db.conn.execute("INSERT INTO local_folder_mappings (root_folder_id, folder_id, source_path, local_path) "
                        "VALUES (?, ?, ?, ?)", (folder_id, folder_id, mapped_source, managed))
    db.conn.commit()
    monkeypatch.setattr("app._trash_paths", lambda paths: pytest.fail("Trash called for staged source"))
    for response in [client.get(URL), client.post(URL, json={"confirm_trash": True, "review_token": token})]:
        assert response.status_code == 409
        assert "Work Locally" in response.json["error"]
    assert (source / "orphan.xmp").exists()
    assert finish_source(db, str(source))["state"] == "unavailable"


@pytest.mark.parametrize("removed", [False, True])
def test_recursive_workspace_ownership_respects_explicit_removal(cleanup_case, removed):
    app, db, source, folder_id = cleanup_case
    parent_id = db.add_folder(str(source.parent), name="Library root")
    other = db.create_workspace("Recursive library")
    db.add_workspace_folder(other, parent_id)
    # Model a descendant whose inherited membership has not materialized yet.
    db.conn.execute("DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?", (other, folder_id))
    db.conn.commit()
    if removed:
        db.remove_workspace_folder_tree(other, folder_id)
    response = app.test_client().get(URL)
    if removed:
        assert response.status_code == 200, response.json
        (source / "orphan.xmp").unlink()
        assert finish_source(db, str(source))["state"] == "removed"
        assert db.conn.execute("SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
                               (other, parent_id)).fetchone()
    else:
        assert response.status_code == 409
        assert "another workspace" in response.json["error"]
        assert (source / "orphan.xmp").exists()


def test_review_rejects_replacement_using_saved_source_inode(cleanup_case):
    app, db, source, _ = cleanup_case
    identity = source.stat()
    result = {"moved": 1, "errors": [], "source_cleanup": {
        "state": "remaining", "source_device": identity.st_dev, "source_inode": identity.st_ino,
    }}
    db.conn.execute("UPDATE job_history SET result = ? WHERE id = 'old-move'", (json.dumps(result),))
    db.conn.commit()
    source.rename(source.parent / "previous-source")
    source.mkdir()
    (source / "unrelated.xmp").write_text("unrelated settings")
    response = app.test_client().get(URL)
    assert response.status_code == 409
    assert "replaced" in response.json["error"]
    assert (source / "unrelated.xmp").read_text() == "unrelated settings"


@pytest.mark.parametrize("replace", [False, True])
def test_later_file_changed_during_trash_is_retained(cleanup_case, replace):
    _, db, source, _ = cleanup_case
    later = source / "second.xmp"
    later.write_text("reviewed settings")
    review = review_source(db, str(source))
    sent = []

    def trash(paths):
        sent.extend(paths)
        os.unlink(paths[0])
        if replace:
            later.rename(source / "saved-original.xmp")
        later.write_text("new settings from editor")
        return 1, set(paths), []

    result = cleanup_source(db, str(source), review["review_token"], trash)
    assert result["trashed"] == 1
    assert sent == [str(source / "orphan.xmp")]
    assert result["state"] == "remaining"
    assert "changed since review" in result["failures"][0]["error"]
    assert later.read_text() == "new settings from editor"


def test_review_rejects_different_device_with_same_saved_inode(cleanup_case):
    app, db, source, _ = cleanup_case
    identity = source.stat()
    result = {"moved": 1, "errors": [], "source_cleanup": {
        "state": "remaining", "source_device": identity.st_dev + 1, "source_inode": identity.st_ino,
    }}
    db.conn.execute("UPDATE job_history SET result = ? WHERE id = 'old-move'", (json.dumps(result),))
    db.conn.commit()
    response = app.test_client().get(URL)
    assert response.status_code == 409
    assert "volume changed" in response.json["error"]
    assert (source / "orphan.xmp").read_text() == "editing settings"


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
