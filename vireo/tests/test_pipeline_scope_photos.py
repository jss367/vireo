"""Tests for /api/pipeline/scope-photos, which Pipeline Review uses to
filter the saved review to a folder or collection."""
import json
import os

import pytest
from app import create_app
from db import Database


@pytest.fixture
def scoped_app(tmp_path):
    import config as cfg
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    root = tmp_path / "photos"
    child = root / "day2"
    os.makedirs(child)
    root_id = db.add_folder(str(root), name="photos")
    child_id = db.add_folder(str(child), name="day2", parent_id=root_id)
    p_root = db.add_photo(root_id, "a.jpg", ".jpg", 1000, 1.0,
                          timestamp="2026-01-01T10:00:00")
    p_child = db.add_photo(child_id, "b.jpg", ".jpg", 1000, 1.0,
                           timestamp="2026-01-01T11:00:00")
    coll_id = db.add_collection(
        "picked", json.dumps([{"field": "photo_ids", "value": [p_child]}]),
    )
    visual_id = db.add_collection(
        "owls", json.dumps([]),
        visual_json=json.dumps({"prompt": "owl", "strength": "medium"}),
    )
    db.close()

    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    app = create_app(db_path, thumb_dir)
    app.config["TESTING"] = True
    return app, {
        "root_id": root_id, "child_id": child_id,
        "p_root": p_root, "p_child": p_child,
        "coll_id": coll_id, "visual_id": visual_id,
    }


def test_folder_scope_includes_subfolders(scoped_app):
    app, ids = scoped_app
    with app.test_client() as c:
        resp = c.get(f"/api/pipeline/scope-photos?folder_id={ids['root_id']}")
        assert resp.status_code == 200
        data = resp.get_json()
        assert sorted(data["photo_ids"]) == sorted([ids["p_root"], ids["p_child"]])
        assert data["count"] == 2

        resp = c.get(f"/api/pipeline/scope-photos?folder_id={ids['child_id']}")
        assert resp.get_json()["photo_ids"] == [ids["p_child"]]


def test_collection_scope_returns_collection_photos(scoped_app):
    app, ids = scoped_app
    with app.test_client() as c:
        resp = c.get(f"/api/pipeline/scope-photos?collection_id={ids['coll_id']}")
        assert resp.status_code == 200
        assert resp.get_json() == {"photo_ids": [ids["p_child"]], "count": 1}


def test_visual_collection_is_rejected(scoped_app):
    """The endpoint resolves rules only, so a visual collection would widen
    the scope to every metadata match."""
    app, ids = scoped_app
    with app.test_client() as c:
        resp = c.get(f"/api/pipeline/scope-photos?collection_id={ids['visual_id']}")
        assert resp.status_code == 400


@pytest.mark.parametrize("query, status", [
    ("", 400),
    ("folder_id=1&collection_id=1", 400),
    ("folder_id=abc", 400),
    ("collection_id=abc", 400),
    ("folder_id=99999", 404),
    ("collection_id=99999", 404),
])
def test_scope_photos_rejects_bad_requests(scoped_app, query, status):
    app, _ = scoped_app
    with app.test_client() as c:
        resp = c.get(f"/api/pipeline/scope-photos?{query}")
        assert resp.status_code == status


def test_folder_outside_active_workspace_is_not_found(scoped_app):
    app, ids = scoped_app
    with app.test_client() as c:
        ws = c.post("/api/workspaces", json={"name": "Other"}).get_json()
        c.post(f"/api/workspaces/{ws['id']}/activate")
        resp = c.get(f"/api/pipeline/scope-photos?folder_id={ids['root_id']}")
        assert resp.status_code == 404
