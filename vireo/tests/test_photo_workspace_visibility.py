"""Photo-only workspace membership across moves, catalog reads and restarts."""

import pytest
from db import Database
from move import move_photos


def _photo(db, folder, name, content=b"photo"):
    folder.mkdir(exist_ok=True)
    (folder / name).write_bytes(content)
    fid = db.add_folder(str(folder))
    pid = db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                       file_size=len(content), file_mtime=1)
    return fid, pid


@pytest.mark.parametrize("keep_visible", [True, False])
def test_move_preserves_only_selected_photo_when_checked(tmp_path, keep_visible):
    path = str(tmp_path / "catalog.db")
    with Database(path) as db:
        a = db._active_workspace_id
        b = db.create_workspace("Other")
        source, moved = _photo(db, tmp_path / "source", "moved.jpg")
        _, unselected = _photo(db, tmp_path / "source", "unselected.jpg", b"other")
        destination, unrelated = _photo(db, tmp_path / "destination", "unrelated.jpg", b"unrelated")
        db.add_workspace_folder(b, source)
        assert db.photo_move_affected_workspaces([moved, unselected]) == [
            {"id": b, "name": "Other", "photo_count": 2}]
        result = move_photos(db, [moved], str(tmp_path / "destination"), keep_visible=keep_visible)
        assert result["moved"] == 1 and not result["errors"]
        assert not db.conn.execute("SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?", (b, destination)).fetchone()
        db.set_active_workspace(b)
        expected = [moved, unselected] if keep_visible else [unselected]
        assert db.filter_photo_ids_in_workspace([moved, unselected, unrelated]) == expected
        assert bool(db.get_photo(moved, verify_workspace=True)) == keep_visible
        visible_folders = {row["id"] for row in db.get_folder_tree()}
        assert (destination in visible_folders) == keep_visible
        if keep_visible:
            assert next(row for row in db.get_folder_tree() if row["id"] == destination)["photo_count"] == 1
            coverage = db.get_folder_coverage_stats()
            assert next(row for row in coverage if row["folder_id"] == destination)["total"] == 1
        db.set_active_workspace(a)
    with Database(path) as reopened:
        reopened.set_active_workspace(b)
        assert reopened.filter_photo_ids_in_workspace([moved, unselected, unrelated]) == expected


def test_unchecked_subsequent_move_revokes_prior_photo_grant(tmp_path):
    with Database(str(tmp_path / "db")) as db:
        a = db._active_workspace_id
        b = db.create_workspace("Other")
        source, photo = _photo(db, tmp_path / "source", "photo.jpg")
        db.add_workspace_folder(b, source)
        assert move_photos(db, [photo], str(tmp_path / "first"))["moved"] == 1
        assert move_photos(db, [photo], str(tmp_path / "second"), keep_visible=False)["moved"] == 1
        db.set_active_workspace(b)
        assert db.filter_photo_ids_in_workspace([photo]) == []
        db.set_active_workspace(a)


def test_unchecked_move_does_not_remove_existing_destination_sharing(tmp_path):
    with Database(str(tmp_path / "db")) as db:
        b = db.create_workspace("Other")
        source, photo = _photo(db, tmp_path / "source", "photo.jpg")
        destination, sibling = _photo(db, tmp_path / "destination", "sibling.jpg", b"sibling")
        db.add_workspace_folder(b, source)
        db.add_workspace_folder(b, destination)
        assert move_photos(db, [photo], str(tmp_path / "destination"), keep_visible=False)["moved"] == 1
        db.set_active_workspace(b)
        assert db.filter_photo_ids_in_workspace([photo, sibling]) == [photo, sibling]


def test_visibility_write_failure_keeps_original_catalog_and_bytes(tmp_path, monkeypatch):
    with Database(str(tmp_path / "db")) as db:
        source, photo = _photo(db, tmp_path / "source", "photo.jpg")
        def fail(*args):
            raise RuntimeError("visibility write failed")
        monkeypatch.setattr(db, "preserve_photo_visibility_for_move", fail)
        with pytest.raises(RuntimeError, match="visibility write failed"):
            move_photos(db, [photo], str(tmp_path / "destination"))
        assert db.get_photo(photo)["folder_id"] == source
        assert (tmp_path / "source" / "photo.jpg").read_bytes() == b"photo"
        assert not db.conn.execute("SELECT 1 FROM workspace_photos").fetchone()


def test_removing_folder_membership_removes_photo_grants(tmp_path):
    with Database(str(tmp_path / "db")) as db:
        b = db.create_workspace("Other")
        folder, photo = _photo(db, tmp_path / "folder", "photo.jpg")
        db.grant_workspace_photos(b, [photo])
        db.conn.commit()
        db.remove_workspace_folder(b, folder)
        db.set_active_workspace(b)
        assert db.filter_photo_ids_in_workspace([photo]) == []


def test_move_preference_persists_and_default_is_checked(tmp_path, monkeypatch):
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    assert cfg.load()["move_keep_visible_in_other_workspaces"] is True
    raw = cfg.read_raw_config_file()
    raw["move_keep_visible_in_other_workspaces"] = False
    cfg.save(raw)
    assert cfg.load()["move_keep_visible_in_other_workspaces"] is False


def test_visibility_preview_is_scoped_and_names_affected_workspaces(app_and_db, tmp_path):
    app, db = app_and_db
    b = db.create_workspace("Birds")
    source, photo = _photo(db, tmp_path / "source", "photo.jpg")
    db.add_workspace_folder(b, source)
    client = app.test_client()
    response = client.post("/api/move-photos/visibility", json={"photo_ids": [photo]})
    assert response.status_code == 200
    assert response.json == {"workspaces": [{"id": b, "name": "Birds", "photo_count": 1}]}
    c = db.create_workspace("Hidden")
    db.set_active_workspace(c)
    _, hidden = _photo(db, tmp_path / "hidden", "hidden.jpg")
    db.set_active_workspace(1)
    assert client.post("/api/move-photos/visibility", json={"photo_ids": [hidden]}).status_code == 403


def test_move_choice_config_api_roundtrip(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    assert client.get("/api/config").json["move_keep_visible_in_other_workspaces"] is True
    assert client.post("/api/config", json={"move_keep_visible_in_other_workspaces": False}).status_code == 200
    assert client.get("/api/config").json["move_keep_visible_in_other_workspaces"] is False


@pytest.mark.parametrize("trust_likely", [False, True])
def test_reimport_moved_duplicate_grants_only_existing_photo(tmp_path, monkeypatch, trust_likely):
    from datetime import datetime

    import metadata
    import scanner
    from import_job import ImportParams, run_import_job
    from move import move_folder
    from test_import_job import FakeRunner, _make_card, _make_job

    monkeypatch.setattr(metadata, "extract_metadata", lambda *args, **kwargs: {})
    monkeypatch.setattr(scanner, "extract_metadata", lambda *args, **kwargs: {})
    card = _make_card(tmp_path, [("bird.jpg", datetime(2026, 5, 1))])
    params = ImportParams(sources=[str(card)], destination=str(tmp_path / "library"),
                          trust_likely_duplicates=trust_likely)
    db_path = str(tmp_path / "catalog.db")
    with Database(db_path) as db:
        a = db._active_workspace_id
        b = db.create_workspace("Receiving")
        first = run_import_job(_make_job(), FakeRunner(), db_path, a, params)
        assert first["ok"] and first["copied"] == 1
        row = db.conn.execute("SELECT id, folder_id FROM photos").fetchone()
        (tmp_path / "moved").mkdir()
        moved_result = move_folder(db, row["folder_id"], str(tmp_path / "moved"))
        assert moved_result["moved"] == 1, moved_result
        folder = db.get_folder(row["folder_id"])
        from pathlib import Path
        _, sibling = _photo(db, Path(folder["path"]), "unrelated.jpg", b"unrelated")
        db.set_active_workspace(b)
        second = run_import_job(_make_job("reimport"), FakeRunner(), db_path, b, params)
        assert second["ok"] and second["copied"] == 0 and second["skipped_duplicate"] == 1
        assert not second["failed"]
        assert db.filter_photo_ids_in_workspace([row["id"], sibling]) == [row["id"]]
        assert not db.conn.execute("SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?", (b, row["folder_id"])).fetchone()


def test_reimport_visibility_failure_reports_failure(tmp_path, monkeypatch):
    from datetime import datetime

    import metadata
    import scanner
    from import_job import ImportParams, run_import_job
    from test_import_job import FakeRunner, _make_card, _make_job

    monkeypatch.setattr(metadata, "extract_metadata", lambda *args, **kwargs: {})
    monkeypatch.setattr(scanner, "extract_metadata", lambda *args, **kwargs: {})
    card = _make_card(tmp_path, [("bird.jpg", datetime(2026, 5, 1))])
    params = ImportParams(sources=[str(card)], destination=str(tmp_path / "library"))
    db_path = str(tmp_path / "catalog.db")
    with Database(db_path) as db:
        a = db._active_workspace_id
        b = db.create_workspace("Receiving")
        assert run_import_job(_make_job(), FakeRunner(), db_path, a, params)["ok"]
        def fail(self, workspace_id, rows):
            raise RuntimeError("cannot persist visibility")
        monkeypatch.setattr(Database, "grant_verified_twin_photos", fail)
        second = run_import_job(_make_job("reimport"), FakeRunner(), db_path, b, params)
        assert not second["ok"] and second["failed"] == 1
        assert second["skipped_duplicate"] == 0


def test_grant_only_folder_is_not_a_storage_import_root(tmp_path):
    with Database(str(tmp_path / "db")) as db:
        b = db.create_workspace("Other")
        folder, photo = _photo(db, tmp_path / "folder", "photo.jpg")
        db.grant_workspace_photos(b, [photo])
        db.conn.commit()
        assert db.get_workspace_folders(b) == []
        db.set_active_workspace(b)
        assert folder in {row["id"] for row in db.get_folder_tree()}


def test_deleting_folder_does_not_delete_another_workspaces_granted_photo(tmp_path):
    with Database(str(tmp_path / "db")) as db:
        a = db._active_workspace_id
        b = db.create_workspace("Other")
        folder, photo = _photo(db, tmp_path / "folder", "photo.jpg")
        db.grant_workspace_photos(b, [photo])
        db.grant_workspace_photos(a, [photo])
        db.conn.commit()
        db.delete_folder(folder)
        assert db.get_photo(photo) is not None
        assert not db.filter_photo_ids_in_workspace([photo])
        db.set_active_workspace(b)
        assert db.filter_photo_ids_in_workspace([photo]) == [photo]
        db.set_active_workspace(a)


def test_identity_fold_transfers_grants_without_sharing_destination(tmp_path):
    from repositories.photo_visibility import remap_photo_visibility
    with Database(str(tmp_path / "db")) as db:
        b = db.create_workspace("Other")
        _, losing = _photo(db, tmp_path / "old", "photo.jpg")
        destination, survivor = _photo(db, tmp_path / "new", "photo.jpg")
        _, unrelated = _photo(db, tmp_path / "new", "unrelated.jpg", b"unrelated")
        db.grant_workspace_photos(b, [losing])
        remap_photo_visibility(db.conn, {losing: survivor})
        db.delete_photos([losing])
        db.set_active_workspace(b)
        assert db.filter_photo_ids_in_workspace([survivor, unrelated]) == [survivor]
        assert not db.conn.execute("SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?", (b, destination)).fetchone()


def test_large_affected_selection_is_deduplicated_and_chunked(tmp_path):
    with Database(str(tmp_path / "db")) as db:
        b = db.create_workspace("Other")
        source, photo = _photo(db, tmp_path / "source", "photo.jpg")
        db.add_workspace_folder(b, source)
        assert db.photo_move_affected_workspaces([photo] * 900 + list(range(10000, 10900))) == [
            {"id": b, "name": "Other", "photo_count": 1}]


@pytest.mark.parametrize("saved,explicit,expected", [(True, None, True), (False, None, False),
                                                     (False, True, True), (True, False, False)])
def test_move_job_uses_remembered_choice_or_explicit_override(app_and_db, tmp_path, saved, explicit, expected):
    import config as cfg
    from wait import wait_for_job_via_client

    app, db = app_and_db
    b = db.create_workspace("Other")
    source, photo = _photo(db, tmp_path / "job-source", "photo.jpg")
    db.add_workspace_folder(b, source)
    raw = cfg.read_raw_config_file()
    raw["move_keep_visible_in_other_workspaces"] = saved
    cfg.save(raw)
    body = {"photo_ids": [photo], "destination": str(tmp_path / "job-destination")}
    if explicit is not None:
        body["keep_visible"] = explicit
    client = app.test_client()
    response = client.post("/api/jobs/move-photos", json=body)
    assert response.status_code == 200
    job = wait_for_job_via_client(client, response.json["job_id"])
    assert job["status"] == "completed", job
    db.set_active_workspace(b)
    assert bool(db.filter_photo_ids_in_workspace([photo])) == expected
    assert cfg.load()["move_keep_visible_in_other_workspaces"] == saved


def test_grant_only_folder_path_is_readable_but_cannot_be_rescanned(app_and_db, tmp_path):
    app, db = app_and_db
    a = db._active_workspace_id
    b = db.create_workspace("Other")
    folder, photo = _photo(db, tmp_path / "grant-folder", "photo.jpg")
    db.grant_workspace_photos(b, [photo])
    db.conn.commit()
    db.set_active_workspace(b)
    client = app.test_client()
    assert client.post(f"/api/workspaces/{b}/activate").status_code == 200
    response = client.get(f"/api/folders/{folder}")
    assert response.status_code == 200
    assert response.json["path"] == str(tmp_path / "grant-folder")
    response = client.post(f"/api/folders/{folder}/rescan", json={})
    assert response.status_code == 404
    db.set_active_workspace(a)


def test_grants_reach_browse_collections_but_not_missing_siblings(tmp_path):
    import json

    with Database(str(tmp_path / 'db')) as db:
        b = db.create_workspace('Other')
        folder, visible = _photo(db, tmp_path / 'folder', 'visible.jpg')
        _, hidden = _photo(db, tmp_path / 'folder', 'hidden.jpg', b'hidden')
        db.grant_workspace_photos(b, [visible])
        db.conn.commit()
        db.set_active_workspace(b)
        assert [r['id'] for r in db.query_photos([])] == [visible]
        assert db.query_photo_ids([]) == [visible]
        assert db.count_photos_for_rules([]) == 1
        cid = db.add_collection('Granted', json.dumps([]))
        assert db.get_collection_photo_ids(cid) == [visible]
        assert [r['id'] for r in db.get_collection_photos(cid)] == [visible]
        (tmp_path / 'folder' / 'visible.jpg').unlink()
        (tmp_path / 'folder' / 'hidden.jpg').unlink()
        progress = []
        assert [r['id'] for r in db.get_missing_photos(progress_callback=progress.append)] == [visible]
        assert progress[-1]['total_photos'] == 1
        db.conn.execute("UPDATE folders SET status='missing' WHERE id=?", (folder,))
        db.conn.commit()
        assert db.get_missing_folders()[0]['photo_count'] == 1
        assert db.filter_photo_ids_in_workspace([visible, hidden]) == [visible]


@pytest.mark.parametrize('keep_visible', [True, False])
def test_date_move_preview_includes_detached_physical_descendants(app_and_db, tmp_path, keep_visible):
    from move import move_folder_by_date, plan_folder_date_moves

    app, db = app_and_db
    a = db._active_workspace_id
    b = db.create_workspace('Hidden descendant owner')
    root, visible = _photo(db, tmp_path / 'root', 'visible.jpg')
    child, hidden = _photo(db, tmp_path / 'root' / 'detached', 'hidden.jpg', b'hidden')
    db.add_workspace_folder(b, child)
    db.remove_workspace_folder(a, child)
    assert db.filter_photo_ids_in_workspace([visible, hidden]) == [visible]
    destination = str(tmp_path / 'destination')
    plans = plan_folder_date_moves(db, root, destination, '%Y/%m/%d')
    assert {pid for plan in plans for pid in plan['photo_ids']} == {visible, hidden}
    client = app.test_client()
    preview = client.post('/api/move-photos/visibility', json={'folder_id': root})
    assert preview.status_code == 200
    assert preview.json == {'workspaces': [{'id': b, 'name': 'Hidden descendant owner', 'photo_count': 1}]}
    assert client.post('/api/move-photos/visibility', json={'folder_id': child}).status_code == 404
    result = move_folder_by_date(db, root, destination, '%Y/%m/%d', keep_visible=keep_visible)
    assert result['moved'] == 2 and not result['errors'], result
    db.set_active_workspace(b)
    assert bool(db.filter_photo_ids_in_workspace([hidden])) == keep_visible
