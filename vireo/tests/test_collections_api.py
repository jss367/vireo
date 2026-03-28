import json


def _clear_default_collections(app, db):
    """Remove default collections so tests start from a clean slate."""
    for c in db.get_collections():
        db.delete_collection(c["id"])


def test_list_collections_empty(app_and_db):
    """GET /api/collections returns [] when no collections exist."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    resp = client.get("/api/collections")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data == []


def test_create_collection(app_and_db):
    """POST /api/collections with name creates collection, returns ok + id."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    resp = client.post(
        "/api/collections",
        json={"name": "My Collection"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert "id" in data
    assert isinstance(data["id"], int)


def test_create_collection_empty_name(app_and_db):
    """POST /api/collections with empty name returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/collections", json={"name": ""})
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_create_and_list_collection(app_and_db):
    """Created collection appears in GET /api/collections listing."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post("/api/collections", json={"name": "Test Smart"})
    assert resp.status_code == 200
    created_id = resp.get_json()["id"]

    resp = client.get("/api/collections")
    assert resp.status_code == 200
    collections = resp.get_json()
    names = [c["name"] for c in collections]
    assert "Test Smart" in names
    ids = [c["id"] for c in collections]
    assert created_id in ids


def test_delete_collection(app_and_db):
    """DELETE /api/collections/<id> removes it."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post("/api/collections", json={"name": "To Delete"})
    cid = resp.get_json()["id"]

    resp = client.delete(f"/api/collections/{cid}")
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True

    # Verify it's gone
    resp = client.get("/api/collections")
    ids = [c["id"] for c in resp.get_json()]
    assert cid not in ids


def test_create_collection_with_rules(app_and_db):
    """POST /api/collections with rules stores them correctly."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    rules = [{"field": "rating", "op": ">=", "value": 4}]
    resp = client.post(
        "/api/collections",
        json={"name": "High Rated", "rules": rules},
    )
    assert resp.status_code == 200

    # Verify via GET listing
    resp = client.get("/api/collections")
    collections = resp.get_json()
    match = [c for c in collections if c["name"] == "High Rated"]
    assert len(match) == 1
    stored_rules = json.loads(match[0]["rules"])
    assert stored_rules == rules


def test_collection_photos_with_rating_rule(app_and_db):
    """Collection with rating >= 3 rule returns photos with rating >= 3."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    rules = [{"field": "rating", "op": ">=", "value": 3}]
    resp = client.post(
        "/api/collections",
        json={"name": "Rated 3+", "rules": rules},
    )
    cid = resp.get_json()["id"]

    resp = client.get(f"/api/collections/{cid}/photos")
    assert resp.status_code == 200
    data = resp.get_json()
    # bird1 has rating 3, bird3 has rating 5; bird2 has no rating
    assert len(data["photos"]) == 2
    filenames = sorted(p["filename"] for p in data["photos"])
    assert filenames == ["bird1.jpg", "bird3.jpg"]
    assert data["total"] == 2


def test_collection_photos_pagination(app_and_db):
    """GET collection photos with per_page=1 returns at most 1 photo."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    rules = [{"field": "rating", "op": ">=", "value": 3}]
    resp = client.post(
        "/api/collections",
        json={"name": "Paginated", "rules": rules},
    )
    cid = resp.get_json()["id"]

    resp = client.get(f"/api/collections/{cid}/photos?per_page=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["photos"]) <= 1
    # Total should still reflect all matching photos
    assert data["total"] == 2


def test_collection_add_photos(app_and_db):
    """POST /api/collections/<id>/add-photos adds photo_ids and returns total."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    # Create an empty static collection
    resp = client.post("/api/collections", json={"name": "Static"})
    cid = resp.get_json()["id"]

    photos = db.get_photos()
    pid1 = photos[0]["id"]
    pid2 = photos[1]["id"]

    resp = client.post(
        f"/api/collections/{cid}/add-photos",
        json={"photo_ids": [pid1, pid2]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["total"] == 2


def test_collection_add_photos_empty_list(app_and_db):
    """POST /api/collections/<id>/add-photos with empty photo_ids returns 400."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post("/api/collections", json={"name": "Empty Add"})
    cid = resp.get_json()["id"]

    resp = client.post(
        f"/api/collections/{cid}/add-photos",
        json={"photo_ids": []},
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_collection_add_photos_nonexistent_collection(app_and_db):
    """POST /api/collections/99999/add-photos returns 404."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/collections/99999/add-photos",
        json={"photo_ids": [1]},
    )
    assert resp.status_code == 404
    data = resp.get_json()
    assert "error" in data


def test_collection_add_photos_deduplicates(app_and_db):
    """Adding the same photo_id twice keeps total at 1."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post("/api/collections", json={"name": "Dedup"})
    cid = resp.get_json()["id"]

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Add the same photo twice in one call
    resp = client.post(
        f"/api/collections/{cid}/add-photos",
        json={"photo_ids": [pid, pid]},
    )
    assert resp.status_code == 200
    assert resp.get_json()["total"] == 1

    # Add it again in a second call
    resp = client.post(
        f"/api/collections/{cid}/add-photos",
        json={"photo_ids": [pid]},
    )
    assert resp.status_code == 200
    assert resp.get_json()["total"] == 1
