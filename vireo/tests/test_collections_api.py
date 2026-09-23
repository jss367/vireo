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


def test_collection_list_keeps_offline_members_in_total(app_and_db):
    """Collection membership stays stable while storage is unavailable."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    available_before = db.count_photos()

    ok_folder = db.add_folder("/available", name="available")
    offline_folder = db.add_folder("/offline", name="offline")
    db.add_photo(folder_id=ok_folder, filename="online.jpg", extension=".jpg",
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=offline_folder, filename="offline.jpg", extension=".jpg",
                 file_size=100, file_mtime=1.0)
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?",
        (offline_folder,),
    )
    db.conn.commit()
    client.post(
        "/api/collections",
        json={"name": "Everything", "rules": [{"field": "all"}]},
    )

    collection = client.get("/api/collections").get_json()[0]
    assert collection["photo_count"] == available_before + 2
    assert collection["available_photo_count"] == available_before + 1
    assert collection["offline_photo_count"] == 1


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


def test_collection_photo_ids_honors_sort_query(app_and_db):
    """``/photo-ids`` mirrors the grid sort so Select-all-matching's first ID
    matches the first visible card — Best Batch seed, burst-review order, and
    export preview all read that first entry (Codex P2 on PR #1561).
    """
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post(
        "/api/collections",
        json={
            "name": "All",
            "rules": [{"field": "rating", "op": ">=", "value": 0}],
        },
    )
    assert resp.status_code == 200
    collection_id = resp.get_json()["id"]

    for sort in ("date", "rating", "name", "name_desc"):
        grid = [
            p["id"]
            for p in db.get_collection_photos(collection_id, per_page=999999, sort=sort)
        ]
        resp = client.get(
            f"/api/collections/{collection_id}/photo-ids?sort={sort}"
        )
        assert resp.status_code == 200
        assert resp.get_json()["photo_ids"] == grid, (
            f"sort={sort}: /photo-ids drifted from the grid"
        )

    resp_default = client.get(f"/api/collections/{collection_id}/photo-ids")
    resp_date = client.get(f"/api/collections/{collection_id}/photo-ids?sort=date")
    assert resp_default.get_json()["photo_ids"] == resp_date.get_json()["photo_ids"]

    resp_bogus = client.get(
        f"/api/collections/{collection_id}/photo-ids?sort=not-a-sort"
    )
    assert resp_bogus.status_code == 200
    assert resp_bogus.get_json()["photo_ids"] == resp_date.get_json()["photo_ids"]


def test_collection_photo_ids_stacks_query_puts_cover_first(app_and_db):
    """``/photo-ids?stacks=true`` returns each cover before its hidden members
    so Select-all-matching with Stacks enabled seeds Best Batch, burst-review,
    and the export preview from the visible top-level card (Codex P2 on
    PR #1561).

    Without ``stacks=true``, the endpoint returns raw member order — a hidden
    burst frame with an earlier filename can insert ahead of the visible
    higher-quality cover, so ``bestBatchSeedId()`` and Burst Review would
    start from the wrong photo.
    """
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    # bird1 and bird3 share a burst; bird3 has the higher rating so the
    # stack projection promotes it to cover even though bird1 sorts first
    # by filename.
    p1_id = db.conn.execute(
        "SELECT id FROM photos WHERE filename = 'bird1.jpg'"
    ).fetchone()["id"]
    p3_id = db.conn.execute(
        "SELECT id FROM photos WHERE filename = 'bird3.jpg'"
    ).fetchone()["id"]
    p2_id = db.conn.execute(
        "SELECT id FROM photos WHERE filename = 'bird2.jpg'"
    ).fetchone()["id"]
    # Browse groups bursts by capture-time proximity within a folder, so
    # put the pair a second apart in one folder. ``bird2`` keeps its own
    # folder and capture time and stays a single.
    db.conn.execute(
        "UPDATE photos SET folder_id = (SELECT folder_id FROM photos WHERE id = ?), "
        "timestamp = '2024-01-15T10:00:01' WHERE id = ?",
        (p1_id, p3_id),
    )
    db.conn.commit()

    resp = client.post(
        "/api/collections",
        json={
            "name": "All",
            "rules": [{"field": "rating", "op": ">=", "value": 0}],
        },
    )
    assert resp.status_code == 200
    collection_id = resp.get_json()["id"]

    # Baseline (no stacks): raw member order under sort=name puts the
    # hidden burst frame first — the shape the Codex finding calls out.
    unstacked = client.get(
        f"/api/collections/{collection_id}/photo-ids?sort=name"
    ).get_json()
    assert unstacked["photo_ids"] == [p1_id, p2_id, p3_id]

    # With Stacks enabled: cover first, then its hidden member, then the
    # single — the same shape ``/photos?stacks=true`` renders.
    stacked = client.get(
        f"/api/collections/{collection_id}/photo-ids?sort=name&stacks=true"
    ).get_json()
    assert stacked["photo_ids"] == [p3_id, p1_id, p2_id]
    assert stacked["total"] == 3

    # And the first stacked id agrees with the paginated /photos response,
    # which is the invariant Best Batch seed / Burst Review / export
    # preview all rely on.
    photos_resp = client.get(
        f"/api/collections/{collection_id}/photos?sort=name&stacks=true"
    ).get_json()
    assert stacked["photo_ids"][0] == photos_resp["photos"][0]["id"]


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


def test_collection_photos_respects_requested_sort(app_and_db):
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    cid = client.post(
        "/api/collections", json={"name": "All sorted", "rules": []},
    ).get_json()["id"]

    response = client.get(f"/api/collections/{cid}/photos?sort=name_desc")

    assert response.status_code == 200
    assert [photo["filename"] for photo in response.get_json()["photos"]] == [
        "bird3.jpg", "bird2.jpg", "bird1.jpg",
    ]


def _all_photos_collection(client):
    return client.post(
        "/api/collections", json={"name": "All", "rules": []},
    ).get_json()["id"]


def _photo_ids_by_name(db):
    return {
        row["filename"]: row["id"]
        for row in db.conn.execute("SELECT id, filename FROM photos")
    }


def test_collection_photos_focus_serves_that_photos_page(app_and_db):
    """A collection-scoped grid can be told where its card landed.

    Browse reloads around the card the user selected — a re-sort, an undo, a
    folder-health refresh. Without a focused lookup here it could only page
    towards that card, one request per page of a result set that may not
    even contain it any more.
    """
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    cid = _all_photos_collection(client)
    target = _photo_ids_by_name(db)["bird3.jpg"]

    payload = client.get(
        f"/api/collections/{cid}/photos?sort=name&per_page=1"
        f"&focus_photo_id={target}"
    ).get_json()

    assert payload["focus_index"] == 2
    assert payload["focus_page"] == 3
    assert payload["page"] == 3
    assert payload["focus_photo_id"] == target
    assert [photo["id"] for photo in payload["photos"]] == [target]


def test_collection_photos_focus_ids_place_by_a_surviving_frame(app_and_db):
    """The frame Browse asks about first may not be in the collection."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    cid = _all_photos_collection(client)
    target = _photo_ids_by_name(db)["bird2.jpg"]

    payload = client.get(
        f"/api/collections/{cid}/photos?sort=name&per_page=1"
        f"&focus_photo_id={10 ** 6}&focus_photo_ids={10 ** 6 + 1},{target}"
    ).get_json()

    assert payload["focus_photo_id"] == target
    assert payload["focus_index"] == 1
    assert payload["page"] == 2


def test_collection_photos_focus_reports_a_photo_it_does_not_hold(app_and_db):
    """Absent is an answer — never a silent page 1 the caller cannot spot."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    cid = _all_photos_collection(client)

    payload = client.get(
        f"/api/collections/{cid}/photos?sort=name&per_page=1"
        f"&focus_photo_id={10 ** 6}"
    ).get_json()

    assert payload["focus_index"] is None
    assert payload["focus_photo_id"] is None
    assert payload["page"] == 1


def test_collection_photos_focus_places_a_stack_by_any_frame(app_and_db):
    """Stacked pages count cards, so a hidden frame reports its cover's
    page — the same rule ``/api/photos/query`` applies."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    ids = _photo_ids_by_name(db)
    p1, p2, p3 = ids["bird1.jpg"], ids["bird2.jpg"], ids["bird3.jpg"]
    # bird1 and bird3 become one burst: same folder, a second apart.
    db.conn.execute(
        "UPDATE photos SET folder_id = (SELECT folder_id FROM photos WHERE id = ?), "
        "timestamp = '2024-01-15T10:00:01' WHERE id = ?",
        (p1, p3),
    )
    db.conn.execute(
        "UPDATE photos SET timestamp = '2024-01-15T10:00:00' WHERE id = ?", (p1,),
    )
    db.conn.commit()
    cid = _all_photos_collection(client)

    stacked = client.get(
        f"/api/collections/{cid}/photo-ids?sort=name&stacks=true"
    ).get_json()["photo_ids"]
    hidden = stacked[1]

    payload = client.get(
        f"/api/collections/{cid}/photos?sort=name&stacks=true&per_page=1"
        f"&focus_photo_id={hidden}"
    ).get_json()

    assert payload["focus_index"] == 0, (
        "the burst is the first card under sort=name"
    )
    assert payload["page"] == 1
    assert payload["photos"][0]["id"] == stacked[0]
    assert p2 not in [photo["id"] for photo in payload["photos"]]


def test_collection_photos_focus_ids_must_be_integers(app_and_db):
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    cid = _all_photos_collection(client)

    for bad in ("x", "1,x", ",".join(str(n) for n in range(201))):
        resp = client.get(
            f"/api/collections/{cid}/photos?focus_photo_ids={bad}"
        )
        assert resp.status_code == 400, bad
        assert "focus_photo_ids" in resp.get_json()["error"]


def test_collection_photos_omit_focus_keys_when_not_asked(app_and_db):
    """Ordinary paging must not start paying for a position it never
    requested."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()
    cid = _all_photos_collection(client)

    payload = client.get(f"/api/collections/{cid}/photos").get_json()

    assert "focus_index" not in payload
    assert "focus_photo_id" not in payload


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
    ``strength in VISUAL_STRENGTH_THRESHOLDS`` raises TypeError; without
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


def test_visual_collection_not_advertised_as_manual_add_target(app_and_db):
    """A visual collection stores ``rules: []`` which
    ``_collection_accepts_manual_photos`` treats as addable, but the
    ``/add-photos`` endpoint only appends to ``photo_ids`` and leaves
    ``visual_json`` alone — the added photos only surface on reopen if
    they also match the hidden visual prompt, so the add is silently
    ineffective. Both listing endpoints must therefore drop
    ``can_add_photos`` for visual collections so the Browse
    add-to-collection modal never offers one (Codex r3620791304)."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    visual = db.add_collection(
        "Visual",
        json.dumps([]),
        visual_json=json.dumps({"prompt": "bird", "strength": "balanced"}),
    )
    client.post(
        "/api/collections",
        json={"name": "Manual", "rules": [{"field": "photo_ids", "value": []}]},
    )

    listed = {c["name"]: c for c in client.get("/api/collections").get_json()}
    assert listed["Visual"]["can_add_photos"] is False
    assert listed["Visual"]["has_visual"] is True
    # Sanity check: a plain manual collection is still addable so this
    # test doesn't regress the picker gate for the normal case.
    assert listed["Manual"]["can_add_photos"] is True

    init = client.get("/api/browse/init").get_json()
    by_name = {c["name"]: c for c in init["collections"]}
    assert by_name["Visual"]["can_add_photos"] is False
    assert by_name["Manual"]["can_add_photos"] is True

    # Defense-in-depth: even if a caller reaches /add-photos directly
    # (bookmark, hand-crafted POST), the endpoint refuses so a stale UI
    # can't smuggle a silent no-op past the picker.
    pid = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    resp = client.post(f"/api/collections/{visual}/add-photos",
                       json={"photo_ids": [pid]})
    assert resp.status_code == 400
    assert "visual collection" in resp.get_json()["error"]


def test_list_collections_flags_degraded_visual_collection(app_and_db):
    """Visual collections skip ``count_collection_photos`` (too expensive to
    resolve embeddings on every list), but that call was also the only path
    that surfaced malformed JSON or unresolvable rule fields. Without a
    separate validation the sidebar reports a broken visual collection as
    healthy and Browse only 400s later when ``filterByCollection()`` routes
    the bad rules through ``/api/photos/query`` (Codex review r3621304875)."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    # Healthy visual collection: valid rules JSON + resolvable field.
    healthy = db.add_collection(
        "Healthy visual",
        json.dumps([{"field": "rating", "op": ">=", "value": 3}]),
        visual_json=json.dumps({"prompt": "bird", "strength": "balanced"}),
    )
    # Degraded visual collection: rules JSON references an unresolvable
    # field so ``rules_resolvable`` returns False.
    degraded = db.add_collection(
        "Degraded visual",
        json.dumps([{"field": "not_a_real_field", "op": "=", "value": "x"}]),
        visual_json=json.dumps({"prompt": "bird", "strength": "balanced"}),
    )

    by_name = {c["name"]: c for c in client.get("/api/collections").get_json()}

    # Both are visual, so photo_count is omitted regardless.
    assert by_name["Healthy visual"]["photo_count"] is None
    assert by_name["Degraded visual"]["photo_count"] is None
    # Only the degraded one carries the flag the sidebar picks up.
    assert by_name["Healthy visual"].get("count_error") is not True
    assert by_name["Degraded visual"].get("count_error") is True
    # Degraded rules must also lock out manual add — same rationale as the
    # plain-collection degraded path in browse_init.
    assert by_name["Degraded visual"]["can_add_photos"] is False

    # Sanity: the IDs round-trip cleanly.
    assert {c["id"] for c in [by_name["Healthy visual"], by_name["Degraded visual"]]} == {
        healthy,
        degraded,
    }


def test_browse_init_visual_collection_scope_returns_empty_first_paint(app_and_db):
    """``/api/browse/init?collection_id=<visual>`` cannot resolve the
    visual clause — ``db.get_photos(collection_id=...)`` /
    ``count_filtered_photos`` expand only ``collections.rules``, the same
    rules-only path guarded elsewhere by ``reject_visual_collection``.
    For a visual-only collection (rules ``[]``) that would silently widen
    the first paint to the entire workspace instead of the saved visual
    result set; the client's ``filterByCollection()`` reloads through
    ``/api/photos/query`` right after, but the wrong grid still flashes
    between the two — and any failure before ``filterByCollection()``
    runs leaves the user staring at the widened scope. Return
    ``photos: []`` and ``total: 0`` for the collection scope's first
    paint so the wrong data can never appear (Codex review on PR #1343)."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    # Baseline: unfiltered init returns every workspace photo. This is
    # exactly the scope that would leak through if the guard regressed.
    baseline = client.get("/api/browse/init").get_json()
    assert baseline["total"] >= 1
    assert len(baseline["photos"]) >= 1

    visual = db.add_collection(
        "Visual only",
        json.dumps([]),
        visual_json=json.dumps({"prompt": "bird", "strength": "balanced"}),
    )

    resp = client.get(f"/api/browse/init?collection_id={visual}")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["photos"] == []
    assert data["total"] == 0
    # Sidebar bootstrap payload is still there so Browse can render
    # folders/keywords/collections while filterByCollection() takes over.
    by_name = {c["name"]: c for c in data["collections"]}
    assert "Visual only" in by_name
    assert "folders" in data and "keywords" in data


def test_browse_init_visual_collection_ignores_rules_scope(app_and_db):
    """A visual collection whose ``rules`` would match every photo (e.g.
    ``[]`` or an ``all`` sentinel) must not have those rules applied at
    first paint either — the browse init endpoint has no way to combine
    the visual clause here, so it deliberately drops the scope. Guards
    against a future edit that "helpfully" falls back to the rules when
    the visual clause is present (Codex review on PR #1343)."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    # Rules that would count every workspace photo — proving the fix
    # never widens/reveals them for a visual collection.
    visual = db.add_collection(
        "Visual widest",
        json.dumps([{"field": "all"}]),
        visual_json=json.dumps({"prompt": "bird", "strength": "balanced"}),
    )

    resp = client.get(f"/api/browse/init?collection_id={visual}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["photos"] == []
    assert data["total"] == 0


def test_browse_init_plain_collection_still_scopes_normally(app_and_db):
    """The visual-scope guard must not touch plain (rules-only)
    collections — the first paint remains the normal rules-only path so
    Browse deep links to smart collections still open showing the
    matching photos before ``filterByCollection()`` runs."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    photo_ids = [
        row["id"] for row in db.conn.execute("SELECT id FROM photos").fetchall()
    ]
    assert len(photo_ids) >= 1

    plain = db.add_collection(
        "Plain",
        json.dumps([{"field": "photo_ids", "value": photo_ids[:1]}]),
    )

    resp = client.get(f"/api/browse/init?collection_id={plain}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 1
    assert [p["id"] for p in data["photos"]] == photo_ids[:1]


def test_create_collection_pins_active_model_on_has_visual_index(
    app_and_db, monkeypatch,
):
    """Saving a filter with ``has_visual_index`` must persist the active
    visual model — otherwise the rules-only sidebar count (which treats a
    model-less rule as "any embedding exists") silently disagrees with
    the save-time preview counter, which /api/photos/query pins to the
    active model. Codex review r3621749904 on PR #1343."""
    import models as models_mod
    monkeypatch.setattr(
        models_mod, "get_active_model",
        lambda: {"name": "current-model", "id": "current-model",
                 "downloaded": True},
    )

    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post(
        "/api/collections",
        json={
            "name": "Indexed",
            "rules": [{"field": "has_visual_index", "op": "is", "value": 1}],
        },
    )
    assert resp.status_code == 200, resp.get_json()
    cid = resp.get_json()["id"]

    row = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,),
    ).fetchone()
    stored = json.loads(row["rules"])
    # The rule leaf now carries ``model`` matching what /api/photos/query
    # would have counted at preview time.
    assert stored[0]["model"] == "current-model"


def test_update_collection_clears_visual_json_when_visual_null(app_and_db):
    """PUT /api/collections/<id> with ``visual: null`` clears the stored
    visual_json. Browse's rules-only Edit Rules modal sends this so an
    existing visual collection can't silently reopen with a hidden
    visual prompt the user just previewed without (Codex review
    r3623087112)."""
    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    cid = _make_visual_collection(db, name="Visual", prompt="a bird")
    row = db.conn.execute(
        "SELECT visual_json FROM collections WHERE id = ?", (cid,),
    ).fetchone()
    assert row["visual_json"] is not None

    resp = client.put(
        f"/api/collections/{cid}",
        json={"name": "Visual",
              "rules": [{"field": "rating", "op": ">=", "value": 3}],
              "visual": None},
    )
    assert resp.status_code == 200, resp.get_json()

    row = db.conn.execute(
        "SELECT visual_json FROM collections WHERE id = ?", (cid,),
    ).fetchone()
    assert row["visual_json"] is None


def test_update_collection_pins_active_model_on_has_visual_index(
    app_and_db, monkeypatch,
):
    """PUT /api/collections/<id> mirrors POST's normalization so editing
    a saved collection's rules can't reintroduce a model-less
    ``has_visual_index`` leaf. Codex review r3621749904."""
    import models as models_mod
    monkeypatch.setattr(
        models_mod, "get_active_model",
        lambda: {"name": "current-model", "id": "current-model",
                 "downloaded": True},
    )

    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post(
        "/api/collections",
        json={"name": "Indexed",
              "rules": [{"field": "rating", "op": ">=", "value": 3}]},
    )
    cid = resp.get_json()["id"]

    resp = client.put(
        f"/api/collections/{cid}",
        json={"rules": [{"field": "has_visual_index",
                          "op": "is", "value": 1}]},
    )
    assert resp.status_code == 200, resp.get_json()

    row = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,),
    ).fetchone()
    stored = json.loads(row["rules"])
    assert stored[0]["model"] == "current-model"


def test_create_collection_preserves_explicit_has_visual_index_model(
    app_and_db, monkeypatch,
):
    """When a saved rule already names a model (an existing collection
    imported/edited manually), POST must leave it alone — otherwise
    normalization would overwrite the user's explicit choice with
    whatever model happens to be active."""
    import models as models_mod
    monkeypatch.setattr(
        models_mod, "get_active_model",
        lambda: {"name": "current-model", "id": "current-model",
                 "downloaded": True},
    )

    app, db = app_and_db
    _clear_default_collections(app, db)
    client = app.test_client()

    resp = client.post(
        "/api/collections",
        json={"name": "Indexed",
              "rules": [{"field": "has_visual_index", "op": "is",
                          "value": 1, "model": "legacy-model"}]},
    )
    assert resp.status_code == 200, resp.get_json()
    cid = resp.get_json()["id"]

    row = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,),
    ).fetchone()
    stored = json.loads(row["rules"])
    assert stored[0]["model"] == "legacy-model"
