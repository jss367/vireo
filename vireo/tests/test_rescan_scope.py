"""Tests for folder-scoped deleted-photo detection and workspace rescan.

Backs the "Rescan folders" feature: the user can rescan a single folder or
the whole workspace, and the deleted-original review must be scoped to match
so a folder rescan never surfaces ghosts from unrelated folders.
"""

import os

import pytest
from db import Database


def _db_with_active_ws(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    return db


def _add_photo_file(db, folder_dir, folder_id, name):
    (folder_dir / name).write_bytes(b"jpegbytes")
    return db.add_photo(
        folder_id=folder_id, filename=name, extension=".jpg",
        file_size=9, file_mtime=1.0,
    )


# ---- DB layer: get_missing_photos(folder_id=...) -------------------------

def test_get_missing_photos_scoped_to_folder(tmp_path):
    db = _db_with_active_ws(tmp_path)
    a = tmp_path / "A"
    a.mkdir()
    b = tmp_path / "B"
    b.mkdir()
    fa = db.add_folder(str(a), name="A")
    fb = db.add_folder(str(b), name="B")
    pa = _add_photo_file(db, a, fa, "a1.jpg")
    _add_photo_file(db, b, fb, "b1.jpg")

    # Delete A's original outside Vireo -> it becomes a ghost.
    (a / "a1.jpg").unlink()

    # Whole-workspace check finds A's ghost.
    assert [r["id"] for r in db.get_missing_photos()] == [pa]
    # Scoped to B (whose file still exists) finds nothing.
    assert db.get_missing_photos(folder_id=fb) == []
    # Scoped to A finds only A's ghost.
    assert [r["id"] for r in db.get_missing_photos(folder_id=fa)] == [pa]
    db.close()


def test_get_missing_photos_scope_includes_subtree(tmp_path):
    db = _db_with_active_ws(tmp_path)
    root = tmp_path / "root"
    root.mkdir()
    sub = root / "sub"
    sub.mkdir()
    froot = db.add_folder(str(root), name="root")
    fsub = db.add_folder(str(sub), name="sub", parent_id=froot)
    ps = _add_photo_file(db, sub, fsub, "s1.jpg")

    (sub / "s1.jpg").unlink()

    # Scoping by the root catches ghosts in descendant folders too.
    assert [r["id"] for r in db.get_missing_photos(folder_id=froot)] == [ps]
    db.close()


def test_get_missing_photos_scope_uses_temp_table_over_variable_cap(tmp_path, monkeypatch):
    """Large workspace roots must not overflow SQLITE_MAX_VARIABLE_NUMBER.

    Legacy SQLite builds cap bound-variable count at 999. A workspace root
    with thousands of descendant folders would blow past that in a single
    IN(...) clause; the scoped Missing Originals query stages the ids in a
    connection-local temp table once the subtree exceeds the chunk size.
    Force the chunk size down to a handful and verify the query still
    returns the right ghosts via the temp-table branch.
    """
    import db as db_module

    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 3)

    db = _db_with_active_ws(tmp_path)
    root = tmp_path / "root"
    root.mkdir()
    froot = db.add_folder(str(root), name="root")
    ghost_ids = []
    # Enough subfolders to exceed the patched chunk size several times over.
    for i in range(10):
        sub = root / f"sub{i}"
        sub.mkdir()
        fsub = db.add_folder(str(sub), name=f"sub{i}", parent_id=froot)
        pid = _add_photo_file(db, sub, fsub, f"g{i}.jpg")
        (sub / f"g{i}.jpg").unlink()
        ghost_ids.append(pid)

    got = sorted(r["id"] for r in db.get_missing_photos(folder_id=froot))
    assert got == sorted(ghost_ids)
    db.close()


def test_get_missing_photos_scope_includes_legacy_null_parent_descendants(tmp_path):
    """Legacy DBs can have descendant folders with parent_id=NULL.

    A recursive walk over parent_id would silently skip them; the path-prefix
    fallback (shared with _folder_subtree_ids_by_path) keeps the scoped
    Missing Originals review honest on those older workspaces.
    """
    db = _db_with_active_ws(tmp_path)
    root = tmp_path / "root"
    root.mkdir()
    sub = root / "sub"
    sub.mkdir()
    froot = db.add_folder(str(root), name="root")
    # Simulate a legacy row: descendant path under root, but no parent_id link.
    fsub = db.add_folder(str(sub), name="sub", parent_id=None)
    ps = _add_photo_file(db, sub, fsub, "s1.jpg")

    (sub / "s1.jpg").unlink()

    assert [r["id"] for r in db.get_missing_photos(folder_id=froot)] == [ps]
    db.close()


# ---- App layer: /api/photos/missing?folder_id= and /api/jobs/scan-workspace

@pytest.fixture
def app_with_real_folders(tmp_path, monkeypatch):
    """create_app backed by two on-disk workspace root folders."""
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    a = tmp_path / "A"
    a.mkdir()
    b = tmp_path / "B"
    b.mkdir()
    fa = db.add_folder(str(a), name="A")
    fb = db.add_folder(str(b), name="B")
    pa = _add_photo_file(db, a, fa, "a1.jpg")
    _add_photo_file(db, b, fb, "b1.jpg")
    (a / "a1.jpg").unlink()  # A's original is gone

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")
    yield app, db, {"fa": fa, "fb": fb, "pa": pa}
    db.close()


def test_missing_endpoint_scoped_by_folder(app_with_real_folders):
    app, _db, ids = app_with_real_folders
    client = app.test_client()

    # Whole-workspace: the one ghost.
    allm = client.get("/api/photos/missing").get_json()
    assert [p["id"] for p in allm] == [ids["pa"]]

    # Scoped to A: the ghost.
    scoped_a = client.get(f"/api/photos/missing?folder_id={ids['fa']}").get_json()
    assert [p["id"] for p in scoped_a] == [ids["pa"]]

    # Scoped to B: empty.
    scoped_b = client.get(f"/api/photos/missing?folder_id={ids['fb']}").get_json()
    assert scoped_b == []


def test_missing_endpoint_rejects_unknown_folder(app_with_real_folders):
    app, _db, _ids = app_with_real_folders
    client = app.test_client()
    assert client.get("/api/photos/missing?folder_id=999999").status_code == 404
    assert client.get("/api/photos/missing?folder_id=abc").status_code == 400


def test_scan_workspace_queues_job(app_with_real_folders):
    app, _db, _ids = app_with_real_folders
    client = app.test_client()
    resp = client.post("/api/jobs/scan-workspace", json={})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["job_id"]
    # Both real roots scheduled; none skipped.
    assert len(data["roots"]) == 2
    assert data["skipped"] == []


def test_scan_workspace_empty_workspace(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")

    resp = app.test_client().post("/api/jobs/scan-workspace", json={})
    assert resp.status_code == 400
    db.close()
