"""Tests for the /api/misses Flask endpoints.

Covers the three routes: list (grouped and category-filtered),
bulk reject, and per-photo unflag.
"""

import json
import os

import pytest
from PIL import Image


@pytest.fixture
def db_with_misses(tmp_path, monkeypatch):
    """Flask app + DB seeded with one photo in each miss category.

    Returns (app, db, {"no_subject": id, "clipped": id, "oof": id}).
    """
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
    fid = db.add_folder("/photos/fake", name="fake")

    p_ns = db.add_photo(
        folder_id=fid, filename="ns.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0, timestamp="2026-04-22T10:00:00",
    )
    p_clip = db.add_photo(
        folder_id=fid, filename="clip.jpg", extension=".jpg",
        file_size=100, file_mtime=2.0, timestamp="2026-04-22T10:00:01",
    )
    p_oof = db.add_photo(
        folder_id=fid, filename="oof.jpg", extension=".jpg",
        file_size=100, file_mtime=3.0, timestamp="2026-04-22T10:00:02",
    )

    db.conn.execute(
        "UPDATE photos SET miss_no_subject=1, miss_computed_at='2026-04-22' "
        "WHERE id=?", (p_ns,),
    )
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1, miss_computed_at='2026-04-22' "
        "WHERE id=?", (p_clip,),
    )
    db.conn.execute(
        "UPDATE photos SET miss_oof=1, miss_computed_at='2026-04-22' "
        "WHERE id=?", (p_oof,),
    )
    db.conn.commit()

    for pid in (p_ns, p_clip, p_oof):
        Image.new("RGB", (100, 100)).save(os.path.join(thumb_dir, f"{pid}.jpg"))

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db, {"no_subject": p_ns, "clipped": p_clip, "oof": p_oof}


@pytest.fixture
def client(db_with_misses):
    app, _, _ = db_with_misses
    return app.test_client()


@pytest.fixture
def clipped_photo_id(db_with_misses):
    _, _, ids = db_with_misses
    return ids["clipped"]


def test_api_misses_returns_grouped_counts(client, db_with_misses):
    r = client.get("/api/misses")
    assert r.status_code == 200
    data = r.get_json()
    assert set(data.keys()) == {"no_subject", "clipped", "oof"}
    assert isinstance(data["clipped"], list)
    assert len(data["no_subject"]) == 1
    assert len(data["clipped"]) == 1
    assert len(data["oof"]) == 1


def test_api_misses_filter_by_category(client, db_with_misses):
    r = client.get("/api/misses?category=clipped")
    assert r.status_code == 200
    data = r.get_json()
    assert data["category"] == "clipped"
    assert all(p["miss_clipped"] == 1 for p in data["photos"])
    assert len(data["photos"]) == 1


def test_api_bulk_reject_sets_flag(client, db_with_misses):
    r = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "clipped"}),
        content_type="application/json",
    )
    assert r.status_code == 200
    body = r.get_json()
    assert body["rejected"] >= 1
    assert body["category"] == "clipped"


def test_api_unflag_miss_clears_boolean(client, db_with_misses, clipped_photo_id):
    r = client.post(
        f"/api/misses/{clipped_photo_id}/unflag",
        data=json.dumps({"category": "clipped"}),
        content_type="application/json",
    )
    assert r.status_code == 200
    assert r.get_json() == {"ok": True}


def test_api_misses_rejects_invalid_category_on_list(client, db_with_misses):
    r = client.get("/api/misses?category=bogus")
    assert r.status_code == 400


def test_api_misses_rejects_invalid_category_on_reject(client, db_with_misses):
    r = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "bogus"}),
        content_type="application/json",
    )
    assert r.status_code == 400


def test_api_misses_rejects_invalid_category_on_unflag(client, db_with_misses, clipped_photo_id):
    r = client.post(
        f"/api/misses/{clipped_photo_id}/unflag",
        data=json.dumps({"category": "bogus"}),
        content_type="application/json",
    )
    assert r.status_code == 400


def test_api_bulk_reject_records_edit_history(client, db_with_misses):
    """Bulk reject must write a batch `flag` edit_history entry so the change
    is undoable and shows up in the audit log, matching /api/batch/flag."""
    _, db, ids = db_with_misses
    r = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "clipped"}),
        content_type="application/json",
    )
    assert r.status_code == 200
    assert r.get_json()["rejected"] == 1

    history = db.get_edit_history(limit=5, offset=0)
    assert history, "bulk reject did not record an edit_history entry"
    entry = history[0]
    assert entry["action_type"] == "flag"
    assert entry["new_value"] == "rejected"
    assert entry["is_batch"] == 1
    assert entry["item_count"] == 1
    assert "category=clipped" in (entry["description"] or "")


def test_api_bulk_reject_no_matches_skips_edit_history(client, db_with_misses):
    """If nothing matches (empty category), no edit_history entry is written —
    avoids cluttering the undo log with no-op rows."""
    _, db, _ = db_with_misses
    before = len(db.get_edit_history(limit=50, offset=0))
    r = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "clipped",
                         "since": "2099-01-01T00:00:00+00:00"}),
        content_type="application/json",
    )
    assert r.status_code == 200
    assert r.get_json()["rejected"] == 0
    after = len(db.get_edit_history(limit=50, offset=0))
    assert before == after


def test_api_misses_since_restricts_to_recent_run(client, db_with_misses):
    """`?since=<ts>` filters the grouped response to photos computed at-or-after
    the timestamp. Used by the pipeline-review step to scope the grid to the
    current run."""
    _, db, ids = db_with_misses
    # Age the clipped and oof rows; keep no_subject on the "new" timestamp.
    db.conn.execute(
        "UPDATE photos SET miss_computed_at='2026-04-20T00:00:00+00:00' "
        "WHERE id IN (?, ?)", (ids["clipped"], ids["oof"]),
    )
    db.conn.commit()

    r = client.get("/api/misses?since=2026-04-21T00:00:00%2B00:00")
    assert r.status_code == 200
    data = r.get_json()
    assert len(data["no_subject"]) == 1
    assert data["clipped"] == []
    assert data["oof"] == []

    r2 = client.get("/api/misses?category=clipped&since=2026-04-21T00:00:00%2B00:00")
    assert r2.status_code == 200
    assert r2.get_json()["photos"] == []
