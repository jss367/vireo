"""Tests for POST /api/collections/<id>/duplicate and db.duplicate_collection."""

import json


def _clear_default_collections(db):
    for c in db.get_collections():
        db.delete_collection(c["id"])


def test_duplicate_collection_copies_name_and_rules(app_and_db):
    """Duplicating a collection creates a new collection with a '(copy)' name
    and identical rules."""
    app, db = app_and_db
    _clear_default_collections(db)

    rules = [{"field": "rating", "op": ">=", "value": 3}]
    cid = db.add_collection("My Picks", json.dumps(rules))

    with app.test_client() as c:
        resp = c.post(f"/api/collections/{cid}/duplicate", json={})
        assert resp.status_code == 200
        body = resp.get_json()
        assert "id" in body
        new_id = body["id"]
        assert new_id != cid

    collections = {c["id"]: c for c in db.get_collections()}
    assert new_id in collections
    new = collections[new_id]
    assert new["name"].startswith("My Picks")
    assert new["name"] != "My Picks"
    # Rules copied verbatim
    assert json.loads(new["rules"]) == rules


def test_duplicate_collection_copies_static_photo_memberships(app_and_db):
    """A static collection (photo_ids rule) duplicates with its membership
    intact, since rules are copied verbatim."""
    app, db = app_and_db
    _clear_default_collections(db)

    photos = db.get_photos()
    pids = [p["id"] for p in photos][:3]

    cid = db.add_collection("Static", json.dumps([]))
    # Use the existing add-photos endpoint to seed membership.
    with app.test_client() as c:
        resp = c.post(
            f"/api/collections/{cid}/add-photos",
            json={"photo_ids": pids},
        )
        assert resp.status_code == 200

        resp = c.post(f"/api/collections/{cid}/duplicate", json={})
        assert resp.status_code == 200
        new_id = resp.get_json()["id"]

    # New collection returns the same photos as the source.
    orig_photos = {p["id"] for p in db.get_collection_photos(cid, per_page=100)}
    new_photos = {p["id"] for p in db.get_collection_photos(new_id, per_page=100)}
    assert orig_photos == new_photos
    assert orig_photos == set(pids)


def test_duplicate_unknown_collection_returns_404(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/collections/999999/duplicate", json={})
        assert resp.status_code == 404
        body = resp.get_json()
        assert "error" in body


def test_duplicate_collection_is_workspace_scoped(app_and_db):
    """db.duplicate_collection is scoped to the active workspace: duplicating
    a collection that belongs to a different workspace raises, and the new
    collection lands in the active workspace."""
    app, db = app_and_db
    _clear_default_collections(db)

    source_ws = db._active_workspace_id
    cid = db.add_collection("Wsp", json.dumps([{"field": "all"}]))

    # Create a second workspace with no collections, and make it active.
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)

    # Collection belongs to source_ws — not findable from other_ws.
    import pytest

    with pytest.raises(ValueError):
        db.duplicate_collection(cid)

    # Duplicating from the source workspace lands the copy there.
    db.set_active_workspace(source_ws)
    new_id = db.duplicate_collection(cid)

    row = db.conn.execute(
        "SELECT workspace_id FROM collections WHERE id = ?", (new_id,)
    ).fetchone()
    assert row["workspace_id"] == source_ws
