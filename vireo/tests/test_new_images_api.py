import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from PIL import Image


def _touch_image(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(path, "JPEG")


@pytest.fixture(autouse=True)
def _clear_shared_new_images_cache():
    from new_images import get_shared_cache
    get_shared_cache().clear()
    yield
    get_shared_cache().clear()


@pytest.fixture
def app_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db, ws_id, tmp_path


def test_api_new_images_reports_unscanned_files(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["new_count"] == 1
    assert len(data["per_root"]) == 1
    assert data["workspace_id"] == ws_id


def test_api_new_images_zero_when_fully_ingested(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    fid = db.add_folder(str(root), name="shoot")
    db.add_photo(folder_id=fid, filename="IMG.JPG", extension=".JPG",
                 file_size=1, file_mtime=0.0)

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    data = resp.get_json()
    assert data["new_count"] == 0
    assert data["workspace_id"] == ws_id


def test_api_new_images_returns_null_workspace_when_none_active(app_and_db, monkeypatch):
    app, db, ws_id, tmp_path = app_and_db
    # Each request creates its own Database via _get_db(), which auto-restores
    # the last-used workspace. To simulate "no active workspace", patch
    # set_active_workspace to a no-op so the per-request db starts with
    # _active_workspace_id = None.
    from db import Database
    monkeypatch.setattr(Database, "set_active_workspace",
                        lambda self, ws_id: None)

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["workspace_id"] is None
    assert data["new_count"] == 0
    assert data["per_root"] == []


def test_post_snapshot_creates_row_with_current_new_images(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    _touch_image(str(folder / "IMG_001.JPG"))
    _touch_image(str(folder / "IMG_002.JPG"))

    with app.test_client() as client:
        resp = client.post("/api/workspaces/active/new-images/snapshot")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["file_count"] == 2
        assert isinstance(data["snapshot_id"], int)
        assert str(folder) in data["folders"]

    snap = db.get_new_images_snapshot(data["snapshot_id"])
    assert snap["file_count"] == 2


def test_post_snapshot_zero_new_images_returns_200(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/workspaces/active/new-images/snapshot")
        assert resp.status_code == 200
        assert resp.get_json()["file_count"] == 0


def test_get_snapshot_returns_summary(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    _touch_image(str(folder / "IMG_001.JPG"))

    with app.test_client() as client:
        post = client.post("/api/workspaces/active/new-images/snapshot")
        snap_id = post.get_json()["snapshot_id"]

        resp = client.get(f"/api/workspaces/active/new-images/snapshot/{snap_id}")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["file_count"] == 1
        assert data["folder_paths"] == [str(folder)]
        assert data["files_sample"][0].endswith("IMG_001.JPG")


def test_get_snapshot_unknown_id_returns_404(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    with app.test_client() as client:
        resp = client.get("/api/workspaces/active/new-images/snapshot/99999")
        assert resp.status_code == 404


def test_get_snapshot_oversized_id_returns_404_not_500(app_and_db):
    """Werkzeug's <int:> converter accepts arbitrary digit strings, producing
    Python ints larger than SQLite's signed 64-bit range. Passing those
    straight to the DB would raise OverflowError (→ 500). Treat them as
    "not found" rather than leaking a server error."""
    app, db, ws_id, tmp_path = app_and_db
    huge = 10 ** 100
    with app.test_client() as client:
        resp = client.get(f"/api/workspaces/active/new-images/snapshot/{huge}")
        assert resp.status_code == 404, (
            f"oversized snapshot id must yield 404, got {resp.status_code}"
        )


def test_get_snapshot_cross_workspace_returns_404(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    snap_id = db.create_new_images_snapshot(["/tmp/a.jpg"])
    other = db.create_workspace("Other")
    # Persist the switch so per-request Database instances restore "Other" as
    # the active workspace (Database.__init__ picks the workspace with the most
    # recent last_opened_at).
    from datetime import datetime
    db.update_workspace(other, last_opened_at=datetime.now().isoformat())
    db.set_active_workspace(other)
    with app.test_client() as client:
        resp = client.get(f"/api/workspaces/active/new-images/snapshot/{snap_id}")
        assert resp.status_code == 404


def test_new_images_preview_returns_folder_preview_shape(app_and_db):
    """POST /api/import/new-images-preview returns the same shape as
    folder-preview so the pipeline renderer can group and display files."""
    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "shoot"
    folder.mkdir()
    db.add_folder(str(folder), name="shoot")
    _touch_image(str(folder / "IMG_001.JPG"))
    _touch_image(str(folder / "sub" / "IMG_002.JPG"))

    with app.test_client() as client:
        post = client.post("/api/workspaces/active/new-images/snapshot")
        snap_id = post.get_json()["snapshot_id"]

        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        data = resp.get_json()

    assert data["total_count"] == 2
    assert data["total_size"] > 0
    assert data["duplicate_count"] == 0
    assert ".jpg" in data["type_breakdown"]
    assert data["type_breakdown"][".jpg"] == 2
    assert len(data["files"]) == 2

    files_by_name = {f["filename"]: f for f in data["files"]}
    assert set(files_by_name) == {"IMG_001.JPG", "IMG_002.JPG"}
    for f in data["files"]:
        assert f["path"]
        assert f["extension"] == ".jpg"
        assert f["size"] > 0
        assert "thumb_url" in f
        assert f["subfolder"]

    subfolders = {f["subfolder"] for f in data["files"]}
    assert len(subfolders) == 2


def test_new_images_preview_missing_snapshot_id(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/import/new-images-preview", json={})
        assert resp.status_code == 400


def test_new_images_preview_unknown_snapshot_returns_404(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    with app.test_client() as client:
        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": 99999},
        )
        assert resp.status_code == 404


def test_new_images_preview_scopes_roots_to_active_workspace(app_and_db):
    """Subfolder grouping must only consider folders in the active workspace.
    A folder in a different workspace whose path is a longer prefix of a
    snapshot file path must not win the prefix match and leak its name."""
    app, db, ws_a, tmp_path = app_and_db

    # Workspace A owns /photos/shoot_a (active when we add it, auto-linked)
    shoot_a = tmp_path / "photos" / "shoot_a"
    shoot_a.mkdir(parents=True)
    _touch_image(str(shoot_a / "pic.jpg"))
    db.add_folder(str(shoot_a), name="shoot_a-in-ws-A")

    # Workspace B owns /photos/shoot_a/inner — a longer prefix that, if
    # not filtered by workspace, would steal the subfolder label. Switch
    # active workspace before creating so add_folder auto-links to B only.
    ws_b = db.create_workspace("Other")
    db.set_active_workspace(ws_b)
    inner = shoot_a / "inner"
    inner.mkdir()
    _touch_image(str(inner / "deep.jpg"))
    db.add_folder(str(inner), name="inner-in-ws-B")
    db.set_active_workspace(ws_a)

    snap_id = db.create_new_images_snapshot([
        str(shoot_a / "pic.jpg"),
        str(inner / "deep.jpg"),
    ])

    with app.test_client() as client:
        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        data = resp.get_json()

    subfolders = {f["subfolder"] for f in data["files"]}
    for sf in subfolders:
        assert "inner-in-ws-B" not in sf, (
            f"Leaked folder label from workspace B: {sf}"
        )


def test_new_images_preview_skips_missing_files(app_and_db):
    """If a path in the snapshot no longer exists on disk, skip it rather
    than 500ing — the file may have been moved or deleted since snapshot."""
    app, db, ws_id, tmp_path = app_and_db
    folder = tmp_path / "shoot"
    folder.mkdir()
    db.add_folder(str(folder), name="shoot")
    _touch_image(str(folder / "here.jpg"))

    # Snapshot includes a path that doesn't exist on disk.
    snap_id = db.create_new_images_snapshot([
        str(folder / "here.jpg"),
        str(folder / "gone.jpg"),
    ])

    with app.test_client() as client:
        resp = client.post(
            "/api/import/new-images-preview",
            json={"snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        data = resp.get_json()
    assert data["total_count"] == 1
    assert data["files"][0]["filename"] == "here.jpg"
