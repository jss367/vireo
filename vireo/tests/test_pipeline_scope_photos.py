"""Tests for /api/pipeline/scope-photos, which Pipeline Review uses to
filter the saved review to a folder or collection."""
import json
import os
import shutil
import subprocess
from pathlib import Path

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


def test_folder_scope_includes_legacy_descendants_by_path(tmp_path):
    """A legacy folder with parent_id=NULL whose path sits under the
    selected root must be included — mirroring the folder resolution
    /api/pipeline/plan and /api/jobs/pipeline use, so the filter catches
    the same photos a scoped Process run would see. Without the
    path-based fallback the "N not in saved review" banner would
    understate the scope."""
    import config as cfg
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    root = tmp_path / "photos"
    legacy = root / "orphan"
    os.makedirs(legacy)
    root_id = db.add_folder(str(root), name="photos")
    # Legacy descendant: path sits under the root but parent_id stays NULL.
    legacy_id = db.add_folder(
        str(legacy), name="orphan", parent_id=None, workspace_root=False,
    )
    p_root = db.add_photo(root_id, "a.jpg", ".jpg", 1000, 1.0)
    p_legacy = db.add_photo(legacy_id, "b.jpg", ".jpg", 1000, 1.0)
    db.close()

    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    app = create_app(db_path, thumb_dir)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.get(f"/api/pipeline/scope-photos?folder_id={root_id}")
        assert resp.status_code == 200
        data = resp.get_json()
        assert sorted(data["photo_ids"]) == sorted([p_root, p_legacy])


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
    # Values just outside SQLite's signed 64-bit range: ``int()`` accepts
    # them but binding to sqlite3 raises ``OverflowError``, which would
    # escape as a 500 without the range guard.
    ("folder_id=9223372036854775808", 400),
    ("folder_id=-9223372036854775809", 400),
    ("collection_id=9223372036854775808", 400),
    ("collection_id=-9223372036854775809", 400),
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


def _render_burst_conflict(scope_ids):
    """Run the review page's burst conflict renderer over a two-burst
    encounter whose second burst has two conflicting frames and one
    agreeing frame (photo 13). ``scope_ids`` of None means unscoped."""
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required to execute the review renderer")
    source = (Path(__file__).parents[1]
              / "static/pipeline-review/species-conflicts.js").read_text(encoding="utf-8")
    script = (
        "function escapeHtml(s) { return String(s); }\n"
        "function escapeAttr(s) { return String(s); }\n"
        + source
        + "\nvar enc = {bursts: [{photo_ids: [10]}, {photo_ids: [11, 12, 13]}]};\n"
        "var grebe = {severity: 'strong', alternativeSpecies: 'Little Grebe'};\n"
        "var evidence = {11: grebe, 12: grebe, 13: {severity: null}};\n"
        "var scope = " + json.dumps(scope_ids) + ";\n"
        "var inScope = scope ? function(pid) { return scope.indexOf(pid) >= 0; } : undefined;\n"
        "process.stdout.write(renderBurstSpeciesConflict(enc, 0, enc.bursts[1], 1, evidence, inScope));"
    )
    result = subprocess.run([node, "-e", script], capture_output=True, text=True,
                            check=True, timeout=15)
    return result.stdout


def test_burst_split_is_offered_only_when_the_whole_burst_is_in_scope():
    """detachBurst refuses a burst that spans the Folder/Collection scope, so
    the conflict badge must not offer a Split it cannot perform."""
    for scope_ids in (None, [10, 11, 12, 13]):
        html = _render_burst_conflict(scope_ids)
        assert "2 suggest Little Grebe" in html
        assert "burst-conflict-split" in html

    partial = _render_burst_conflict([11, 12])
    assert "2 suggest Little Grebe" in partial
    assert "burst-conflict-split" not in partial
