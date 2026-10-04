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
        monkeypatch.setattr(Database, "grant_verified_twin_photos_tracked", fail)
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


@pytest.mark.parametrize('abort_transfer', [False, True])
def test_workspace_folder_transfer_removes_source_grants_atomically(tmp_path, abort_transfer):
    import sqlite3

    with Database(str(tmp_path / 'db')) as db:
        source = db._active_workspace_id
        target = db.create_workspace('Target')
        other = db.create_workspace('Other')
        folder, photo = _photo(db, tmp_path / 'folder', 'photo.jpg')
        db.grant_workspace_photos(source, [photo])
        db.grant_workspace_photos(other, [photo])
        db.conn.commit()
        if abort_transfer:
            db.conn.execute(f"CREATE TRIGGER reject_folder_transfer BEFORE DELETE ON workspace_folders WHEN OLD.workspace_id={source} BEGIN SELECT RAISE(ABORT, 'transfer rejected'); END")
            db.conn.commit()
            with pytest.raises(sqlite3.IntegrityError, match='transfer rejected'):
                db.move_folders_to_workspace(source, target, [folder])
        else:
            db.move_folders_to_workspace(source, target, [folder])
        db.set_active_workspace(source)
        assert bool(db.filter_photo_ids_in_workspace([photo])) == abort_transfer
        assert bool(db.conn.execute('SELECT 1 FROM workspace_photos WHERE workspace_id=? AND photo_id=?', (source, photo)).fetchone()) == abort_transfer
        db.set_active_workspace(target)
        assert bool(db.filter_photo_ids_in_workspace([photo])) != abort_transfer
        db.set_active_workspace(other)
        assert db.filter_photo_ids_in_workspace([photo]) == [photo]


def test_raw_jpeg_fold_preserves_photo_grants_without_sibling_access(tmp_path):
    from scanner import _pair_raw_jpeg_companions

    with Database(str(tmp_path / 'db')) as db:
        b = db.create_workspace('Other')
        folder, jpeg = _photo(db, tmp_path / 'folder', 'IMG_001.jpg')
        raw = db.add_photo(folder_id=folder, filename='IMG_001.cr3', extension='.cr3', file_size=2000, file_mtime=1)
        _, sibling = _photo(db, tmp_path / 'folder', 'unrelated.jpg', b'unrelated')
        db.grant_workspace_photos(b, [jpeg])
        db.conn.commit()
        _pair_raw_jpeg_companions(db)
        db.conn.commit()
        assert db.get_photo(jpeg) is None
        db.set_active_workspace(b)
        assert db.filter_photo_ids_in_workspace([raw, sibling]) == [raw]
        assert not db.conn.execute('SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?', (b, folder)).fetchone()


def test_associated_workspaces_include_only_exact_photo_grant_folder(app_and_db, tmp_path):
    app, db = app_and_db
    b = db.create_workspace('Other')
    folder, photo = _photo(db, tmp_path / 'folder', 'photo.jpg')
    child, _ = _photo(db, tmp_path / 'folder' / 'child', 'hidden.jpg')
    db.grant_workspace_photos(b, [photo])
    db.conn.commit()
    associated = {r['id']: r for r in db.get_folder_workspaces(folder)}
    assert b in associated and not associated[b]['is_root']
    assert b not in {r['id'] for r in db.get_folder_workspaces(child)}
    client = app.test_client()
    assert client.post(f'/api/workspaces/{b}/activate').status_code == 200
    response = client.get(f'/api/folders/{folder}/workspaces')
    assert response.status_code == 200
    assert any(r['id'] == b and r['is_active'] for r in response.json['workspaces'])
    assert client.get(f'/api/folders/{child}/workspaces').status_code == 404


def test_grant_only_folder_read_scopes_keep_siblings_hidden(app_and_db, tmp_path):
    from services.missing_originals import resolve_folder_id

    app, db = app_and_db
    b = db.create_workspace('Other')
    folder, photo = _photo(db, tmp_path / 'folder', 'photo.jpg')
    _, sibling = _photo(db, tmp_path / 'folder', 'hidden.jpg', b'hidden')
    db.grant_workspace_photos(b, [photo])
    db.conn.commit()
    db.set_active_workspace(b)
    assert resolve_folder_id(db, folder) == folder
    assert db.get_folder_coverage_stats(folder_id=folder)[0]['total'] == 1
    assert db.query_photo_ids([], folder_id=folder) == [photo]
    assert b in db._workspace_repository(scoped=False).ids_for_folders([folder])
    from web.pipeline import _PipelineLaunch

    launch = _PipelineLaunch({}, lambda: db, lambda *args: pytest.fail(str(args)))
    assert launch._folder_scope_photo_ids({folder}) == [photo]
    client = app.test_client()
    assert client.post(f'/api/workspaces/{b}/activate').status_code == 200
    response = client.post('/api/pipeline/plan', json={'folder_ids': [folder]})
    assert response.status_code == 200, response.json
    assert response.json['scope']['photo_count'] == 1
    assert db.filter_photo_ids_in_workspace([photo, sibling]) == [photo]


def test_grant_only_folder_is_excluded_from_audit_roots(app_and_db, tmp_path):
    """A grant-only folder must not appear as a storage root to the audit
    scans: ``/api/audit/untracked`` would otherwise enumerate hidden sibling
    files, and ``/api/audit/import-untracked`` would create a real
    workspace_folders link that expands the grant-only visibility to every
    sibling photo. Regression for Codex P1 (latest-head review of
    ``fix/photo-workspace-visibility``).
    """
    app, db = app_and_db
    b = db.create_workspace('Other')
    linked_dir = tmp_path / 'linked'
    linked_dir.mkdir()
    linked_folder = db.add_folder(str(linked_dir))
    db.add_workspace_folder(b, linked_folder)
    grant_dir = tmp_path / 'grant-only'
    grant_folder, granted = _photo(db, grant_dir, 'granted.jpg')
    _, hidden_sibling = _photo(db, grant_dir, 'hidden.jpg', b'hidden')
    db.grant_workspace_photos(b, [granted])
    (grant_dir / 'untracked.jpg').write_bytes(b'untracked')
    db.conn.commit()
    db.set_active_workspace(b)
    # Direct repository check: the audit root paths exclude the grant-only
    # folder even though ``get_folder_tree`` surfaces it as a parentless
    # entry (its own synthetic root).
    audit_roots = db.get_audit_root_paths()
    assert str(linked_dir) in audit_roots
    assert str(grant_dir) not in audit_roots
    assert any(not row['parent_id'] and row['path'] == str(grant_dir)
               for row in db.get_folder_tree()), \
        'tree still carries the synthetic grant-only entry for folder reads'
    client = app.test_client()
    assert client.post(f'/api/workspaces/{b}/activate').status_code == 200
    response = client.get('/api/audit/untracked')
    assert response.status_code == 200
    untracked_paths = {entry['path'] for entry in response.json}
    # The untracked scan now walks only the linked directory, so the
    # hidden sibling and the stray file in the grant-only folder are both
    # out of scope -- and import-untracked rejects them.
    assert str(grant_dir / 'untracked.jpg') not in untracked_paths
    assert str(grant_dir / 'hidden.jpg') not in untracked_paths
    import_response = client.post(
        '/api/audit/import-untracked',
        json={'paths': [str(grant_dir / 'untracked.jpg')]},
    )
    assert import_response.status_code == 400
    # Grant-only folder gained no real workspace_folders link.
    assert not db.conn.execute(
        'SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?',
        (b, grant_folder),
    ).fetchone()
    # Hidden sibling is still invisible to b.
    assert db.filter_photo_ids_in_workspace([granted, hidden_sibling]) == [granted]


def test_folder_relocate_requires_real_folder_link_not_photo_grant(app_and_db, tmp_path):
    """``/api/folders/<id>/relocate`` rewrites the folder's global path for
    every workspace, so a workspace holding only a ``workspace_photos``
    grant must not be able to invoke it: otherwise the grant-only workspace
    could silently rewrite paths for the hidden sibling photos owned by
    other workspaces. Regression for Codex P1 (latest-head review).
    """
    app, db = app_and_db
    owner = db._active_workspace_id
    guest = db.create_workspace('Guest')
    folder_dir = tmp_path / 'owned-folder'
    folder, owned_photo = _photo(db, folder_dir, 'owned.jpg')
    _, sibling = _photo(db, folder_dir, 'sibling.jpg', b'sibling')
    db.add_workspace_folder(owner, folder)
    db.grant_workspace_photos(guest, [owned_photo])
    db.conn.commit()
    new_location = tmp_path / 'moved-folder'
    new_location.mkdir()
    client = app.test_client()
    assert client.post(f'/api/workspaces/{guest}/activate').status_code == 200
    # ``get_folder_workspaces`` still reports the guest workspace -- the
    # listing endpoint keeps the read-only association -- but the mutation
    # route rejects the request.
    assert guest in {row['id'] for row in db.get_folder_workspaces(folder)}
    assert not db.workspace_has_folder_link(folder, guest)
    response = client.post(
        f'/api/folders/{folder}/relocate', json={'path': str(new_location)},
    )
    assert response.status_code == 404
    # The folder row still points at its original location.
    assert db.get_folder(folder)['path'] == str(folder_dir)
    # The owner still has mutation rights.
    assert client.post(f'/api/workspaces/{owner}/activate').status_code == 200
    assert db.workspace_has_folder_link(folder, owner)
    # Sibling and owned_photo both remain owned by owner.
    db.set_active_workspace(owner)
    assert db.filter_photo_ids_in_workspace([owned_photo, sibling]) == [owned_photo, sibling]


def test_mount_loss_rollback_revokes_duplicate_grants_and_demotes_promotion(tmp_path):
    """A mount-loss detection must undo exactly the ``workspace_photos``
    grants and missing->ok folder promotions this batch inserted via
    ``grant_verified_twin_photos_tracked``. Pre-existing grants and
    unrelated folders are left alone. Regression for Codex P2
    (latest-head review).
    """
    from import_job import _ImportBatchState, _rollback_on_mount_loss

    with Database(str(tmp_path / 'db')) as db:
        workspace = db._active_workspace_id
        _, pre_existing_photo = _photo(db, tmp_path / 'pre', 'pre.jpg')
        folder, batch_photo = _photo(db, tmp_path / 'batch', 'batch.jpg')
        _, other_photo = _photo(db, tmp_path / 'other', 'other.jpg')
        unrelated_dir = tmp_path / 'unrelated'
        unrelated = db.add_folder(str(unrelated_dir))
        # Pre-existing grant that must survive the rollback.
        db.grant_workspace_photos(workspace, [pre_existing_photo])
        # Mark the batch's folder missing and the unrelated folder missing,
        # so we can prove the rollback demotes the one promoted this batch
        # and leaves the pre-existing missing folder alone.
        db.conn.execute("UPDATE folders SET status='missing' WHERE id IN (?, ?)",
                        (folder, unrelated))
        db.conn.commit()
        new_grants, promoted = db.grant_verified_twin_photos_tracked(
            workspace,
            [{'id': batch_photo, 'folder_status': 'missing',
              'folder_path': str(tmp_path / 'batch'), 'filename': 'batch.jpg'}],
        )
        # Re-granting the pre-existing photo in the same tracked call must
        # not report it as new (so the rollback does not revoke it).
        pre_grants, _ = db.grant_verified_twin_photos_tracked(
            workspace,
            [{'id': pre_existing_photo, 'folder_status': 'ok',
              'folder_path': str(tmp_path / 'pre'), 'filename': 'pre.jpg'}],
        )
        assert pre_grants == []
        assert new_grants == [batch_photo]
        assert promoted == [folder]
        db.conn.commit()
        # Simulate the batch state after the tracked grants.
        from types import SimpleNamespace

        batch_st = _ImportBatchState(rel='batch', dest_folder='')
        batch_st.dup_granted_photo_ids = list(new_grants)
        batch_st.dup_promoted_folder_ids = list(promoted)
        batch_st.mount_lost = '/mnt/archive'
        state = SimpleNamespace(
            skipped_duplicate=0, unverified_duplicate=0,
            failed=0, unsafe_files=[], log_label='test',
            copied=0, verified=0, landed_files={},
            folder_counts={}, mount_ever_lost=None,
        )
        _rollback_on_mount_loss(
            state, batch_st, attests_bytes=False,
            db=db, workspace_id=workspace,
        )
        # Grant inserted this batch is gone.
        assert not db.conn.execute(
            'SELECT 1 FROM workspace_photos WHERE workspace_id=? AND photo_id=?',
            (workspace, batch_photo),
        ).fetchone()
        # Pre-existing grant and unrelated grants survive.
        assert db.conn.execute(
            'SELECT 1 FROM workspace_photos WHERE workspace_id=? AND photo_id=?',
            (workspace, pre_existing_photo),
        ).fetchone()
        # Promoted folder is back to 'missing'.
        assert db.get_folder(folder)['status'] == 'missing'
        # Unrelated missing folder also stays 'missing' -- the demotion
        # list only touches ids the tracked grant promoted.
        assert db.get_folder(unrelated)['status'] == 'missing'
        # Unrelated photo in another folder is untouched.
        assert not db.conn.execute(
            'SELECT 1 FROM workspace_photos WHERE workspace_id=? AND photo_id=?',
            (workspace, other_photo),
        ).fetchone()


def test_grant_only_workspace_cannot_preflight_or_launch_whole_folder_move(app_and_db, tmp_path):
    app, db = app_and_db
    owner = db._active_workspace_id
    guest = db.create_workspace("Guest")
    source = tmp_path / "owned-source"
    folder, shared = _photo(db, source, "shared.jpg")
    _, sibling = _photo(db, source, "private.jpg", b"private")
    untracked = source / "untracked.txt"
    untracked.write_bytes(b"untracked")
    db.add_workspace_folder(owner, folder)
    db.grant_workspace_photos(guest, [shared])
    db.conn.commit()
    client = app.test_client()
    assert client.post(f"/api/workspaces/{guest}/activate").status_code == 200
    destination = tmp_path / "destination"
    destination.mkdir()
    body = {"folder_id": folder, "destination": str(destination)}
    for route in ["/api/move-folder/preflight", "/api/jobs/move-folder"]:
        assert client.post(route, json=body).status_code == 404
    assert (source / "shared.jpg").exists() and (source / "private.jpg").read_bytes() == b"private"
    assert untracked.read_bytes() == b"untracked"
    assert not list(destination.iterdir())
    assert db.get_folder(folder)["path"] == str(source)
    assert db.get_photo(sibling) is not None
    assert client.post(f"/api/workspaces/{owner}/activate").status_code == 200
    assert client.post("/api/move-folder/preflight", json=body).status_code == 200


def test_grant_only_folder_cannot_become_a_workspace_scan_or_local_copy_root(app_and_db, tmp_path):
    from services.local_workspace import LocalWorkspaceError, _root_records

    app, db = app_and_db
    owner = db._active_workspace_id
    guest = db.create_workspace("Photo-only guest")
    folder, shared = _photo(db, tmp_path / "root", "shared.jpg")
    _, sibling = _photo(db, tmp_path / "root", "private.jpg", b"private")
    db.grant_workspace_photos(guest, [shared])
    db.conn.commit()
    assert folder in {row["id"] for row in db.get_workspace_folder_roots(owner)}
    assert db.get_workspace_folder_roots(guest) == []
    db.set_active_workspace(guest)
    client = app.test_client()
    assert client.post(f"/api/workspaces/{guest}/activate").status_code == 200
    response = client.post("/api/jobs/scan-workspace", json={})
    assert response.status_code == 400
    assert "no folders to rescan" in response.json["error"]
    with pytest.raises(LocalWorkspaceError, match="Add at least one folder"):
        _root_records(db, guest, tmp_path / "local")
    assert db.filter_photo_ids_in_workspace([shared, sibling]) == [shared]
    assert not db.workspace_has_folder_link(folder)
    assert not (tmp_path / "local").exists()


def test_grant_only_missing_folder_cannot_delete_hidden_siblings(app_and_db, tmp_path):
    app, db = app_and_db
    owner = db._active_workspace_id
    guest = db.create_workspace("Photo-only guest")
    folder, shared = _photo(db, tmp_path / "missing-root", "shared.jpg")
    _, sibling = _photo(db, tmp_path / "missing-root", "private.jpg", b"private")
    db.grant_workspace_photos(guest, [shared])
    db.conn.commit()
    db.remove_workspace_folder(owner, folder)
    db.conn.execute("UPDATE folders SET status='missing' WHERE id=?", (folder,))
    db.conn.commit()
    db.set_active_workspace(guest)
    client = app.test_client()
    assert client.post(f"/api/workspaces/{guest}/activate").status_code == 200
    missing = client.get("/api/folders/missing").json
    assert next(row for row in missing if row["id"] == folder)["photo_count"] == 1
    response = client.delete(f"/api/folders/{folder}")
    assert response.status_code == 404
    with pytest.raises(ValueError, match="not linked"):
        db.delete_folder(folder)
    assert db.get_photo(shared) is not None
    assert db.get_photo(sibling) is not None
    assert db.filter_photo_ids_in_workspace([shared, sibling]) == [shared]
    assert (tmp_path / "missing-root" / "private.jpg").read_bytes() == b"private"
