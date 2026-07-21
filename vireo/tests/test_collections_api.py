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


def test_list_collections_marks_manual_photo_targets(app_and_db):
    """GET /api/collections reports which collections can accept manual adds."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    client.post(
        "/api/collections",
        json={"name": "Manual", "rules": [{"field": "photo_ids", "value": []}]},
    )
    client.post(
        "/api/collections",
        json={
            "name": "Smart",
            "rules": [{"field": "rating", "op": ">=", "value": 4}],
        },
    )
    client.post(
        "/api/collections",
        json={"name": "All Photos", "rules": [{"field": "all"}]},
    )

    resp = client.get("/api/collections")
    assert resp.status_code == 200
    by_name = {c["name"]: c for c in resp.get_json()}
    assert by_name["Manual"]["can_add_photos"] is True
    assert by_name["Smart"]["can_add_photos"] is False
    assert by_name["All Photos"]["can_add_photos"] is False


def test_browse_init_marks_manual_photo_targets(app_and_db):
    """Initial Browse payload includes collection type metadata for the sidebar."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    client.post(
        "/api/collections",
        json={"name": "Manual", "rules": [{"field": "photo_ids", "value": []}]},
    )
    client.post(
        "/api/collections",
        json={
            "name": "Smart",
            "rules": [{"field": "rating", "op": ">=", "value": 4}],
        },
    )

    resp = client.get("/api/browse/init")
    assert resp.status_code == 200
    by_name = {c["name"]: c for c in resp.get_json()["collections"]}
    assert by_name["Manual"]["can_add_photos"] is True
    assert by_name["Smart"]["can_add_photos"] is False


def test_browse_init_honors_folder_filter(app_and_db):
    """Deep-linked Browse loads the target folder's first page, not global page 1."""
    app, db = app_and_db
    client = app.test_client()

    january = db.conn.execute(
        "SELECT id FROM folders WHERE name = 'January'"
    ).fetchone()["id"]

    resp = client.get(f"/api/browse/init?folder_id={january}&per_page=10")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["total"] == 1
    assert [p["filename"] for p in data["photos"]] == ["bird2.jpg"]


def test_collection_photo_ids_returns_all_matching_ids(app_and_db):
    """Collection select-all support returns all matching IDs without pagination."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post(
        "/api/collections",
        json={
            "name": "Four Stars",
            "rules": [{"field": "rating", "op": ">=", "value": 4}],
        },
    )
    assert resp.status_code == 200
    collection_id = resp.get_json()["id"]
    expected = [p["id"] for p in db.get_collection_photos(collection_id, per_page=999999)]

    resp = client.get(f"/api/collections/{collection_id}/photo-ids")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["photo_ids"] == expected
    assert data["total"] == len(expected)


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


def test_update_collection_can_replace_rules(app_and_db):
    """PUT /api/collections/<id> updates rules as well as the name."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post(
        "/api/collections",
        json={"name": "Draft", "rules": [{"field": "rating", "op": ">=", "value": 5}]},
    )
    assert resp.status_code == 200
    cid = resp.get_json()["id"]

    grouped = {
        "mode": "any",
        "rules": [
            {"field": "rating", "op": ">=", "value": 3},
            {"field": "flag", "op": "equals", "value": "flagged"},
        ],
    }
    resp = client.put(
        f"/api/collections/{cid}",
        json={"name": "Useful", "rules": grouped},
    )
    assert resp.status_code == 200

    row = db.conn.execute(
        "SELECT name, rules FROM collections WHERE id = ?", (cid,)
    ).fetchone()
    assert row["name"] == "Useful"
    assert json.loads(row["rules"]) == grouped


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


def test_collection_photos_includes_detections_and_species(app_and_db):
    """GET /api/collections/<id>/photos attaches detections and species so the
    browse grid's detection-box toggle works in collection view."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    target = [p for p in db.get_photos() if p["filename"] == "bird1.jpg"][0]
    db.save_detections(target["id"], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.7, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2}, "confidence": 0.95, "category": "animal"},
    ], detector_model="MDV6")

    rules = [{"field": "rating", "op": ">=", "value": 3}]
    resp = client.post("/api/collections", json={"name": "Rated", "rules": rules})
    cid = resp.get_json()["id"]

    resp = client.get(f"/api/collections/{cid}/photos")
    assert resp.status_code == 200
    data = resp.get_json()

    bird1 = [p for p in data["photos"] if p["filename"] == "bird1.jpg"][0]
    assert "detections" in bird1
    assert len(bird1["detections"]) == 2
    assert bird1["detections"][0]["confidence"] == 0.95
    assert "species" in bird1

    bird3 = [p for p in data["photos"] if p["filename"] == "bird3.jpg"][0]
    assert bird3["detections"] == []


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


def test_cannot_add_photos_to_all_photos_default(app_and_db):
    """Add-photos on the 'All Photos' default is rejected so the sentinel rule
    can't be AND-combined with a photo_ids rule and silently narrowed."""
    app, db = app_and_db
    # Ensure defaults exist (including All Photos)
    db.create_default_collections()
    client = app.test_client()

    all_photos = next(c for c in db.get_collections() if c["name"] == "All Photos")
    photos = db.get_photos()
    pid = photos[0]["id"]

    resp = client.post(
        f"/api/collections/{all_photos['id']}/add-photos",
        json={"photo_ids": [pid]},
    )
    assert resp.status_code == 400
    # Rules must be unchanged
    row = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (all_photos["id"],)
    ).fetchone()
    assert json.loads(row["rules"]) == [{"field": "all"}]


def test_cannot_add_photos_to_smart_collection(app_and_db):
    """Manual adds to a smart collection should not convert it into a subset."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    rules = [{"field": "rating", "op": ">=", "value": 4}]
    resp = client.post(
        "/api/collections",
        json={"name": "High Rated", "rules": rules},
    )
    cid = resp.get_json()["id"]
    pid = db.get_photos()[0]["id"]

    resp = client.post(
        f"/api/collections/{cid}/add-photos",
        json={"photo_ids": [pid]},
    )
    assert resp.status_code == 400

    row = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,)
    ).fetchone()
    assert json.loads(row["rules"]) == rules


def test_cannot_add_photos_to_none_grouped_collection(app_and_db):
    """Adding photos to a "none" group would exclude the selected IDs."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    rules = {
        "mode": "none",
        "rules": [{"field": "flag", "op": "is", "value": "rejected"}],
    }
    resp = client.post(
        "/api/collections",
        json={"name": "Not rejected", "rules": rules},
    )
    cid = resp.get_json()["id"]

    pid = db.get_photos()[0]["id"]
    resp = client.post(
        f"/api/collections/{cid}/add-photos",
        json={"photo_ids": [pid]},
    )
    assert resp.status_code == 400

    row = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,)
    ).fetchone()
    assert json.loads(row["rules"]) == rules


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


# ---------------------------------------------------------------------------
# Visual collections must not be silently expanded by rules-only consumers.
# See Codex review r3620423210 on PR #1343 — a visual-only saved expression
# saved into ``collections.visual_json`` was still selectable by legacy
# consumers (``/api/collections/<id>/photos``, pipeline stages), whose
# ``get_collection_photos`` evaluates ``rules`` only. The consumer would
# silently scope its run to every metadata match instead of the
# visually-matched subset. These tests pin the boundary contract: pickers
# see ``has_visual``, and every rules-only endpoint 400s a visual collection.
# ---------------------------------------------------------------------------


def _make_visual_collection(db, name="Visual", rules=None, prompt="a bird"):
    """Insert a collection whose visual_json is set, mirroring what the
    Save-as-Collection flow persists for expressions with a visual clause."""
    if rules is None:
        rules = [{"field": "rating", "op": ">=", "value": 3}]
    return db.add_collection(
        name,
        json.dumps(rules),
        visual_json=json.dumps({"prompt": prompt, "strength": "balanced"}),
    )


def test_list_collections_flags_visual(app_and_db):
    """``/api/collections`` exposes ``has_visual`` so pickers can hide or
    disable visual collections wherever the downstream path is rules-only."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    _make_visual_collection(db, name="Visual birds")
    client.post(
        "/api/collections",
        json={"name": "Plain", "rules": [{"field": "rating", "op": ">=", "value": 3}]},
    )

    by_name = {c["name"]: c for c in client.get("/api/collections").get_json()}
    assert by_name["Visual birds"]["has_visual"] is True
    assert by_name["Plain"]["has_visual"] is False


def test_create_collection_rejects_non_string_visual_strength(app_and_db):
    """A JSON array for ``visual.strength`` is unhashable, so
    ``strength in _VISUAL_STRENGTH_THRESHOLDS`` raises TypeError; without
    an explicit type check that TypeError escapes the 400 handler as a 500
    (CodeRabbit review r3620473547 outside-diff note)."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post(
        "/api/collections",
        json={
            "name": "Bad strength",
            "rules": [],
            "visual": {"prompt": "bird", "strength": ["broad"]},
        },
    )
    assert resp.status_code == 400, resp.get_json()
    assert "strength" in resp.get_json()["error"]


def test_collection_photos_rejects_visual_collection(app_and_db):
    """``/api/collections/<id>/photos`` refuses visual collections. The
    endpoint only evaluates ``rules``; without this reject the Review and
    Pipeline pickers (which funnel through it) would silently scope their
    workload to every metadata match instead of the visually-matched subset.
    """
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    cid = _make_visual_collection(db)

    resp = client.get(f"/api/collections/{cid}/photos")
    assert resp.status_code == 400
    assert "visual-search clause" in resp.get_json()["error"]


def test_collection_photo_ids_rejects_visual_collection(app_and_db):
    """``/api/collections/<id>/photo-ids`` refuses visual collections for
    the same reason as ``/photos`` — rules-only expansion would widen the
    caller's scope past the visual clause."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    cid = _make_visual_collection(db)

    resp = client.get(f"/api/collections/{cid}/photo-ids")
    assert resp.status_code == 400
    assert "visual-search clause" in resp.get_json()["error"]


def test_collection_photos_still_serves_plain_collection(app_and_db):
    """The visual-rejection guard must not touch plain (rules-only)
    collections — the /photos endpoint remains the normal path for them."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post(
        "/api/collections",
        json={"name": "Plain", "rules": [{"field": "rating", "op": ">=", "value": 3}]},
    )
    cid = resp.get_json()["id"]

    resp = client.get(f"/api/collections/{cid}/photos")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "photos" in data and "total" in data
