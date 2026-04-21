import os


def test_index_redirects_to_browse(app_and_db, monkeypatch):
    """GET / redirects to /browse when a model is available."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "test", "name": "Test", "downloaded": True
    })
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/')
    assert resp.status_code == 302
    assert '/browse' in resp.headers['Location']


def test_browse_page(app_and_db):
    """GET /browse returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    assert resp.status_code == 200


def test_help_static_assets_served(app_and_db):
    """The help modal's JS, JSON, and vendored Fuse library must be served.

    The shared navbar includes <script src="/static/help.js"> and
    <script src="/static/vendor/fuse.min.js">, and help.js fetches
    /static/help.json at runtime. If any of these 404, F1 and the
    navbar ? icon silently do nothing.
    """
    app, _ = app_and_db
    client = app.test_client()
    for path in ('/static/help.js', '/static/help.json', '/static/vendor/fuse.min.js'):
        resp = client.get(path)
        assert resp.status_code == 200, f"{path} returned {resp.status_code}"


def test_api_folders(app_and_db):
    """GET /api/folders returns folder tree."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/folders')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 2
    paths = {f['path'] for f in data}
    assert '/photos/2024' in paths


def test_api_keywords(app_and_db):
    """GET /api/keywords returns keyword tree."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/keywords')
    assert resp.status_code == 200
    data = resp.get_json()
    names = {k['name'] for k in data}
    assert 'Cardinal' in names
    assert 'Sparrow' in names


def test_logs_recent(app_and_db):
    """GET /api/logs/recent returns recent log entries."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/api/logs/recent?count=10')
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)


def test_logs_page(app_and_db):
    """GET /logs returns 200."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/logs')
    assert resp.status_code == 200


def test_settings_page_has_preview_cache_field(app_and_db):
    """Settings page renders the preview_cache_max_mb input and cache controls."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/settings')
    assert resp.status_code == 200
    assert b'preview_cache_max_mb' in resp.data
    assert b'cfgPreviewCacheMaxMb' in resp.data
    assert b'clearPreviewCache' in resp.data


def test_encounter_species_confirm(app_and_db):
    """POST /api/encounters/species tags photos with species keyword."""
    app, db = app_and_db
    client = app.test_client()

    # Get photo IDs
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    photo_ids = [p["id"] for p in photos]

    resp = client.post('/api/encounters/species',
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["species"] == "Blue Jay"
    assert data["photo_count"] == len(photo_ids)

    # Verify keyword was created with is_species=True
    kw = db.conn.execute(
        "SELECT * FROM keywords WHERE name = 'Blue Jay'").fetchone()
    assert kw is not None
    assert kw["is_species"] == 1

    # Verify all photos are tagged
    for pid in photo_ids:
        tags = db.get_photo_keywords(pid)
        species_tags = [t for t in tags if t["name"] == "Blue Jay"]
        assert len(species_tags) == 1

    # Verify pending changes queued
    pending = db.get_pending_changes()
    kw_adds = [c for c in pending if c["change_type"] == "keyword_add"
               and c["value"] == "Blue Jay"]
    assert len(kw_adds) == len(photo_ids)


def test_encounter_species_validation(app_and_db):
    """POST /api/encounters/species validates required fields."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.post('/api/encounters/species', json={"species": ""})
    assert resp.status_code == 400

    resp = client.post('/api/encounters/species',
                       json={"species": "Robin", "photo_ids": []})
    assert resp.status_code == 400


def test_encounter_species_rejects_invalid_photo_ids(app_and_db):
    """POST /api/encounters/species rejects stale/invalid photo_ids without partial writes."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    valid_id = photos[0]["id"]
    bogus_id = 99999

    resp = client.post('/api/encounters/species',
                       json={"species": "Robin", "photo_ids": [valid_id, bogus_id]})
    assert resp.status_code == 400
    assert "99999" in resp.get_json()["error"]

    # Verify nothing was written for the valid ID either
    tags = db.get_photo_keywords(valid_id)
    assert not any(t["name"] == "Robin" for t in tags)
    pending = db.get_pending_changes()
    assert not any(c["value"] == "Robin" for c in pending)


def test_encounter_species_updates_pipeline_cache(app_and_db):
    """POST /api/encounters/species updates species_confirmed in pipeline cache."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    photo_ids = [p["id"] for p in photos]

    # Create pipeline cache with unconfirmed encounter
    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Sparrow", 0.8],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": len(photo_ids),
                "burst_count": 0,
                "time_range": [None, None],
                "photo_ids": photo_ids,
            }
        ],
        "photos": [{"id": pid, "label": "KEEP", "filename": f"{pid}.jpg"} for pid in photo_ids],
        "summary": {"total_photos": len(photo_ids), "encounter_count": 1, "burst_count": 0,
                     "keep_count": len(photo_ids), "review_count": 0, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    # Confirm species
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200

    # Read cache back and check
    with open(path) as f:
        updated = _json.load(f)
    enc = updated["encounters"][0]
    assert enc["species_confirmed"] is True
    assert enc["confirmed_species"] == "Blue Jay"


def _seed_encounter_cache(app, db, photo_ids, *, confirmed_species=None, bursts=None):
    """Write a pipeline cache with one encounter for the given photos."""
    import json as _json
    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    enc = {
        "species": ["Sparrow", 0.8],
        "confirmed_species": confirmed_species,
        "species_predictions": [],
        "species_confirmed": confirmed_species is not None,
        "photo_count": len(photo_ids),
        "burst_count": len(bursts) if bursts else 0,
        "time_range": [None, None],
        "photo_ids": photo_ids,
    }
    if bursts is not None:
        enc["bursts"] = bursts
    results = {
        "encounters": [enc],
        "photos": [{"id": pid, "label": "KEEP", "filename": f"{pid}.jpg"} for pid in photo_ids],
        "summary": {"total_photos": len(photo_ids), "encounter_count": 1,
                    "burst_count": enc["burst_count"],
                    "keep_count": len(photo_ids), "review_count": 0,
                    "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)
    return path


def test_encounter_species_change_cancels_pending_add(app_and_db):
    """Changing the confirmed species before sync cancels the stale add."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)

    # First confirm as Sparrow
    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": photo_ids})
    assert resp.status_code == 200

    # Still pending (not synced yet) — change to Blue Jay
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200
    assert resp.get_json()["previous_species"] == "Sparrow"

    # Photos should now have Blue Jay but not Sparrow
    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" in names
        assert "Sparrow" not in names

    # Pending changes should contain keyword_add:Blue Jay only.
    # The Sparrow add had not synced, so it should be cancelled, not followed
    # by a keyword_remove (otherwise the sidecar would see a remove for a
    # keyword that was never written).
    changes = [dict(c) for c in db.get_pending_changes()]
    values_by_type = {(c["change_type"], c["value"]) for c in changes}
    assert ("keyword_add", "Blue Jay") in values_by_type
    assert ("keyword_add", "Sparrow") not in values_by_type
    assert ("keyword_remove", "Sparrow") not in values_by_type


def test_encounter_species_change_queues_remove_after_sync(app_and_db):
    """If the previous species was already synced, changing it queues a remove."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)

    # Confirm as Sparrow, then simulate a completed sync by clearing pending.
    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": photo_ids})
    assert resp.status_code == 200
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    # Change to Blue Jay
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200

    # Photos have Blue Jay, not Sparrow
    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" in names
        assert "Sparrow" not in names

    # Now a keyword_remove:Sparrow must be queued so the XMP drops the
    # already-written Sparrow tag.
    values_by_type = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_remove", "Sparrow") in values_by_type
    assert ("keyword_add", "Blue Jay") in values_by_type


def test_burst_override_change_untags_previous(app_and_db):
    """Changing a burst override removes the previously overridden species."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]
    burst_ids = photo_ids[:1]

    bursts = [{
        "photo_ids": burst_ids,
        "species_predictions": [],
        "species_override": None,
    }]
    _seed_encounter_cache(app, db, photo_ids, bursts=bursts)

    # Override burst 0 to Sparrow
    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": burst_ids,
                             "burst_index": 0})
    assert resp.status_code == 200
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    # Now change the burst override to Junco
    resp = client.post("/api/encounters/species",
                       json={"species": "Junco", "photo_ids": burst_ids,
                             "burst_index": 0})
    assert resp.status_code == 200
    assert resp.get_json()["previous_species"] == "Sparrow"

    for pid in burst_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Junco" in names
        assert "Sparrow" not in names

    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_remove", "Sparrow") in values
    assert ("keyword_add", "Junco") in values


def test_encounter_species_confirm_same_species_noop_on_keywords(app_and_db):
    """Re-confirming the same species doesn't queue a remove."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": photo_ids})
    assert resp.status_code == 200
    assert resp.get_json()["previous_species"] is None

    values = {c["change_type"] for c in db.get_pending_changes()}
    assert "keyword_remove" not in values


def test_encounter_species_replacement_is_atomic_in_history(app_and_db):
    """Replacing the encounter species records one history entry, not two."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    # Clear history from the initial confirm so we're only looking at the
    # replacement.
    db.conn.execute("DELETE FROM edit_history")
    db.conn.commit()

    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'species_replace'
    assert 'Sparrow' in history[0]['description']
    assert 'Blue Jay' in history[0]['description']


def test_encounter_species_replacement_undo_restores_previous(app_and_db):
    """One undo after a replacement restores the previous species, not neither."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    client.post("/api/encounters/species",
                json={"species": "Blue Jay", "photo_ids": photo_ids})

    # One undo should swap the photos back to Sparrow.
    undone = db.undo_last_edit()
    assert undone is not None
    assert undone['action_type'] == 'species_replace'

    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Sparrow" in names
        assert "Blue Jay" not in names

    # Neither species was synced, so undo should cancel the pending swap
    # outright rather than queue a keyword_remove for a never-written tag.
    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_remove", "Blue Jay") not in values
    assert ("keyword_remove", "Sparrow") not in values
    # Original keyword_add:Sparrow is back in the queue because the replace
    # had cancelled it — and undoing the replace's own add (Blue Jay) means
    # the sidecar state matches what was there before Blue Jay was confirmed.
    assert ("keyword_add", "Sparrow") in values
    assert ("keyword_add", "Blue Jay") not in values


def test_encounter_species_replacement_undo_after_sync_queues_swap(app_and_db):
    """If the replacement already synced, undo queues the reverse XMP ops."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    client.post("/api/encounters/species",
                json={"species": "Blue Jay", "photo_ids": photo_ids})
    # Pretend the replacement has synced: drop all pending changes.
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    undone = db.undo_last_edit()
    assert undone is not None
    assert undone['action_type'] == 'species_replace'

    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Sparrow" in names
        assert "Blue Jay" not in names

    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_remove", "Blue Jay") in values
    assert ("keyword_add", "Sparrow") in values


def test_encounter_species_replacement_redo_reapplies(app_and_db):
    """Redo after undo re-applies the replacement."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    client.post("/api/encounters/species",
                json={"species": "Blue Jay", "photo_ids": photo_ids})

    db.undo_last_edit()
    redone = db.redo_last_undo()
    assert redone is not None
    assert redone['action_type'] == 'species_replace'

    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" in names
        assert "Sparrow" not in names


def test_encounter_species_rejects_photo_ids_not_in_burst(app_and_db):
    """A valid burst_index plus photo_ids from a different burst must be rejected."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]
    assert len(photo_ids) >= 2

    # Two bursts: burst 0 holds photo_ids[:1], burst 1 holds photo_ids[1:].
    bursts = [
        {"photo_ids": photo_ids[:1], "species_predictions": [], "species_override": None},
        {"photo_ids": photo_ids[1:], "species_predictions": [], "species_override": None},
    ]
    _seed_encounter_cache(app, db, photo_ids, bursts=bursts)

    # burst_index 0 is in range, but we're submitting photos from burst 1.
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay",
                             "photo_ids": photo_ids[1:],
                             "burst_index": 0})
    assert resp.status_code == 400
    assert "bursts[0]" in resp.get_json()["error"]

    # Nothing should have been written.
    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" not in names
    assert not db.get_pending_changes()


def test_encounter_species_rejects_out_of_range_burst_index(app_and_db):
    """A stale burst_index must not silently fall through to an encounter update."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    # Encounter has exactly one burst.
    bursts = [{
        "photo_ids": photo_ids[:1],
        "species_predictions": [],
        "species_override": None,
    }]
    _seed_encounter_cache(app, db, photo_ids, bursts=bursts)

    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay",
                             "photo_ids": photo_ids[:1],
                             "burst_index": 99})
    assert resp.status_code == 400
    assert "burst_index" in resp.get_json()["error"]

    # Nothing should have been written.
    for pid in photo_ids[:1]:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" not in names
    assert not db.get_pending_changes()


def test_encounter_species_replacement_ignores_nested_homonym(app_and_db):
    """Old-species lookup must be scoped to root species keywords only."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)

    # Confirm as Sparrow (creates root species keyword).
    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": photo_ids})
    assert resp.status_code == 200
    root_sparrow_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Sparrow' AND parent_id IS NULL"
    ).fetchone()["id"]

    # Create a non-species homonym "Sparrow" nested under another keyword. If
    # the replacement lookup were scoped by name only, it could resolve here
    # and leave the real species tag intact.
    parent = db.add_keyword("Birds")
    db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species) VALUES ('Sparrow', ?, 0)",
        (parent,),
    )
    db.conn.commit()

    # Change species — the root Sparrow tag must still be removed.
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200

    for pid in photo_ids:
        kw_ids = {k["id"] for k in db.get_photo_keywords(pid)}
        assert root_sparrow_id not in kw_ids


def test_species_search(app_and_db):
    """GET /api/species/search returns matching species from keywords."""
    app, db = app_and_db
    client = app.test_client()

    # Add a species keyword
    db.add_keyword("American Robin", is_species=True)
    db.add_keyword("Robin Redbreast", is_species=True)

    resp = client.get('/api/species/search?q=robin')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) >= 2
    names_lower = [n.lower() for n in data]
    assert any("robin" in n for n in names_lower)

    # Too short query returns empty
    resp = client.get('/api/species/search?q=r')
    assert resp.status_code == 200
    assert resp.get_json() == []


def test_pipeline_review_page(app_and_db):
    """GET /pipeline/review returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/pipeline/review')
    assert resp.status_code == 200


def test_classify_route_removed(app_and_db):
    """GET /classify should return 404 after removal."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/classify')
    assert resp.status_code == 404


def test_pipeline_regroup_accepts_collection_id(app_and_db):
    """POST /api/jobs/regroup accepts collection_id parameter."""
    app, db = app_and_db
    client = app.test_client()

    # Create a smart collection containing all photos
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    photo_ids = [p["id"] for p in photos]
    cid = db.add_collection("test-pipeline", '[{"field":"photo_ids","value":' + str(photo_ids) + '}]')

    # The job will fail because no pipeline features exist, but the route
    # should accept collection_id without error
    resp = client.post('/api/jobs/regroup', json={"collection_id": cid})
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data


def test_static_css_served(app_and_db):
    """vireo-base.css is served from /static/."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/static/vireo-base.css')
    assert resp.status_code == 200
    assert 'text/css' in resp.content_type
    body = resp.data.decode()
    assert 'box-sizing: border-box' in body


def test_pages_link_base_css(app_and_db):
    """Every page includes a <link> to vireo-base.css."""
    app, _ = app_and_db
    client = app.test_client()
    pages = ['/browse', '/lightroom', '/audit', '/logs',
             '/settings', '/workspace', '/pipeline', '/dashboard',
             '/review', '/cull', '/pipeline/review', '/map', '/shortcuts']
    for page in pages:
        resp = client.get(page)
        assert resp.status_code == 200, f"{page} returned {resp.status_code}"
        html = resp.data.decode()
        assert 'vireo-base.css' in html, f"{page} missing vireo-base.css link"


def test_compare_page(app_and_db):
    """GET /compare returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/compare')
    assert resp.status_code == 200


def test_compare_link_in_navbar(app_and_db):
    """The navbar includes a link to /compare."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/compare')
    assert b'/compare' in resp.data
    assert b'Compare' in resp.data


def test_compare_predictions_api(app_and_db):
    """GET /api/predictions/compare returns per-photo, per-model data."""
    app, db = app_and_db

    # Get photo IDs
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    photo_ids = [p["id"] for p in photos]

    # Create a collection containing all photos
    import json
    rules = json.dumps([{"field": "photo_ids", "value": photo_ids}])
    cid = db.add_collection("Test Collection", rules)

    # Create detections, then add predictions from two models
    det_ids_0 = db.save_detections(photo_ids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    det_ids_1 = db.save_detections(photo_ids[1], [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.85, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids_0[0], "Cardinal", 0.95, "model-a")
    db.add_prediction(det_ids_0[0], "Blue Jay", 0.80, "model-b")
    db.add_prediction(det_ids_1[0], "Sparrow", 0.90, "model-a")
    db.add_prediction(det_ids_1[0], "Sparrow", 0.88, "model-b")

    client = app.test_client()
    resp = client.get(f"/api/predictions/compare?collection_id={cid}")
    assert resp.status_code == 200
    data = resp.get_json()

    assert "models" in data
    assert set(data["models"]) == {"model-a", "model-b"}
    assert "photos" in data
    assert len(data["photos"]) >= 2

    # Check structure of a photo entry
    photo = data["photos"][0]
    assert "photo_id" in photo
    assert "filename" in photo
    assert "predictions" in photo
    assert isinstance(photo["predictions"], dict)  # keyed by model name
    # Each model maps to a list of predictions (multi-detection support)
    for model_preds in photo["predictions"].values():
        assert isinstance(model_preds, list)
        assert len(model_preds) >= 1
        assert "species" in model_preds[0]
        assert "confidence" in model_preds[0]


def test_api_predictions_include_bounding_box(app_and_db):
    """GET /api/predictions should return bounding box data from detections."""
    app, db = app_and_db
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    pid = photos[0]["id"]
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Elk", confidence=0.9, model="bioclip")

    client = app.test_client()
    resp = client.get("/api/predictions")
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]["box_x"] == 0.1
    assert data[0]["box_y"] == 0.2
    assert data[0]["box_w"] == 0.3
    assert data[0]["box_h"] == 0.4
    assert data[0]["photo_id"] == pid


def test_api_predictions_multiple_detections(app_and_db):
    """GET /api/predictions should return one prediction per detection."""
    app, db = app_and_db
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    pid = photos[0]["id"]
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.3}, "confidence": 0.95, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.3}, "confidence": 0.80, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Elk", confidence=0.92, model="bioclip")
    db.add_prediction(det_ids[1], species="Magpie", confidence=0.85, model="bioclip")

    client = app.test_client()
    resp = client.get("/api/predictions")
    data = resp.get_json()
    assert len(data) == 2
    species = {d["species"] for d in data}
    assert species == {"Elk", "Magpie"}


def test_api_detections_endpoint(app_and_db):
    """GET /api/detections/<photo_id> returns all detections for a photo."""
    app, db = app_and_db
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    pid = photos[0]["id"]
    db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.6, "w": 0.2, "h": 0.1}, "confidence": 0.7, "category": "animal"},
    ], detector_model="MDV6")

    client = app.test_client()
    resp = client.get(f"/api/detections/{pid}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 2
    # Sorted by confidence descending
    assert data[0]["detector_confidence"] >= data[1]["detector_confidence"]
    assert data[0]["box_x"] == 0.1


def test_api_photo_pipeline_detections(app_and_db):
    """GET /api/photos/<id>/pipeline returns detections and predictions with box data."""
    app, db = app_and_db
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    pid = photos[0]["id"]
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Robin", confidence=0.88, model="bioclip")

    client = app.test_client()
    resp = client.get(f"/api/photos/{pid}/pipeline")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "detections" in data
    assert len(data["detections"]) == 1
    assert data["detections"][0]["box_x"] == 0.1
    assert "predictions" in data
    assert len(data["predictions"]) == 1
    assert data["predictions"][0]["species"] == "Robin"
    assert data["predictions"][0]["box_x"] == 0.1
    # crop_box should be computed from primary detection
    assert "crop_box" in data


def test_compare_predictions_api_requires_collection(app_and_db):
    """GET /api/predictions/compare without collection_id returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/predictions/compare")
    assert resp.status_code == 400


def test_pipeline_has_model_checkboxes(app_and_db):
    """Pipeline page uses checkboxes for model selection, not a single select."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/pipeline')
    assert resp.status_code == 200
    assert b'model-checkbox' in resp.data
    assert b'id="cfgModel"' not in resp.data  # old single select removed


def test_static_vireo_utils_served(app_and_db):
    """vireo-utils.js is served from /static/."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/static/vireo-utils.js')
    assert resp.status_code == 200
    assert 'javascript' in resp.content_type
    body = resp.data.decode()
    assert 'function escapeHtml' in body
    assert 'function escapeAttr' in body


def test_pages_include_vireo_utils(app_and_db):
    """Every page includes vireo-utils.js via _navbar.html."""
    app, _ = app_and_db
    client = app.test_client()
    pages = ['/browse', '/lightroom', '/audit', '/logs',
             '/settings', '/workspace', '/pipeline', '/dashboard',
             '/review', '/cull', '/variants', '/compare', '/map']
    for page in pages:
        resp = client.get(page)
        assert resp.status_code == 200, f"{page} returned {resp.status_code}"
        html = resp.data.decode()
        assert 'vireo-utils.js' in html, f"{page} missing vireo-utils.js script tag"


def test_map_page(app_and_db):
    """GET /map returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/map')
    assert resp.status_code == 200


def test_pages_no_inline_escapeHtml(app_and_db):
    """No page template should still define escapeHtml inline."""
    app, _ = app_and_db
    client = app.test_client()
    pages = ['/browse', '/lightroom', '/audit', '/logs',
             '/settings', '/workspace', '/pipeline', '/dashboard',
             '/review', '/cull', '/variants', '/compare', '/map']
    for page in pages:
        resp = client.get(page)
        html = resp.data.decode()
        # The function should exist (via vireo-utils.js) but not be
        # defined inline in a <script> block on the page itself.
        # We check that "function escapeHtml" does NOT appear in the
        # page body outside of the vireo-utils.js src tag.
        # Simple heuristic: count occurrences — should be 0 in inline script.
        # The <script src="...vireo-utils.js"> tag won't contain the function text.
        assert html.count('function escapeHtml') == 0, \
            f"{page} still has inline escapeHtml definition"


def test_health_endpoint(app_and_db):
    """GET /api/health returns 200 with status ok."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"


def test_shutdown_endpoint(app_and_db):
    """POST /api/shutdown returns 200 and signals shutdown."""
    from unittest.mock import MagicMock, patch

    app, _ = app_and_db
    client = app.test_client()
    # GET should not be allowed
    resp = client.get("/api/shutdown")
    assert resp.status_code == 405
    # POST without X-Vireo-Shutdown header is rejected (CSRF protection)
    resp = client.post("/api/shutdown")
    assert resp.status_code == 403
    # POST with header triggers shutdown (mock Timer so SIGTERM is never sent)
    mock_timer = MagicMock()
    with patch("threading.Timer", return_value=mock_timer) as mock_timer_cls:
        resp = client.post(
            "/api/shutdown", headers={"X-Vireo-Shutdown": "1"}
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "shutting_down"
        mock_timer_cls.assert_called_once()
        mock_timer.start.assert_called_once()


def test_pipeline_page_init_api(app_and_db):
    """GET /api/pipeline/page-init returns pipeline initialization data."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/pipeline/page-init')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'total_photos' in data
    assert 'has_detections' in data
    assert 'has_masks' in data
    assert 'has_sharpness' in data
    assert 'pipeline_config' in data
    assert 'results' in data
    # Verify pipeline_config has expected keys
    pc = data['pipeline_config']
    assert 'sam2_variant' in pc
    assert 'dinov2_variant' in pc
    assert 'proxy_longest_edge' in pc
    # total_photos should match our fixture data (3 photos)
    assert data['total_photos'] == 3


def test_pipeline_page_init_includes_recent_destinations(app_and_db):
    """page-init response includes recent_destinations from ingest config."""
    import config as cfg
    app, _ = app_and_db
    # Write config with recent_destinations
    config = cfg.load()
    config.setdefault("ingest", {})["recent_destinations"] = ["/photos/out1", "/photos/out2"]
    cfg.save(config)
    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "recent_destinations" in data
        assert data["recent_destinations"] == ["/photos/out1", "/photos/out2"]


def test_templates_jinja_free_except_includes():
    """All .html templates must be free of Jinja2 syntax except {% include '...' %}."""
    import os
    import re

    templates_dir = os.path.join(os.path.dirname(__file__), '..', 'templates')
    templates_dir = os.path.normpath(templates_dir)

    # Patterns that match Jinja2 block tags and expression tags
    jinja_block_re = re.compile(r'\{%.*?%\}', re.DOTALL)
    jinja_expr_re = re.compile(r'\{\{.*?\}\}', re.DOTALL)
    # Allowed: {% include '...' %} or {% include "..." %}
    include_re = re.compile(r"\{%\s*include\s+['\"].*?['\"]\s*%\}")

    violations = []

    for fname in sorted(os.listdir(templates_dir)):
        if not fname.endswith('.html'):
            continue
        fpath = os.path.join(templates_dir, fname)
        with open(fpath, encoding='utf-8') as f:
            lines = f.readlines()
        for lineno, line in enumerate(lines, start=1):
            # Check for {{ ... }} expressions — never allowed
            for m in jinja_expr_re.finditer(line):
                violations.append(f"{fname}:{lineno}: {m.group().strip()}")
            # Check for {% ... %} blocks — only includes are allowed
            for m in jinja_block_re.finditer(line):
                if not include_re.fullmatch(m.group()):
                    violations.append(f"{fname}:{lineno}: {m.group().strip()}")

    assert violations == [], (
        "Jinja2 syntax found in templates (only {% include '...' %} is allowed):\n"
        + "\n".join(violations)
    )


def test_bottom_panel_has_history_tab(app_and_db):
    """The bottom panel includes a History tab."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    html = resp.data.decode()
    assert "switchBpTab('history')" in html
    assert 'id="bpHistory"' in html


def test_text_search_requires_query(app_and_db):
    """Text search returns 400 when no query provided."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/search")
    assert resp.status_code == 400


def test_text_search_no_active_model(app_and_db):
    """Text search returns empty results when no model is downloaded."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/search?q=bird+in+flight")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["results"] == []
    assert data["total_matches"] == 0


def test_text_search_timm_model_returns_unsupported(app_and_db, monkeypatch):
    """Text search returns error when active model is timm (no CLIP embeddings)."""
    app, _ = app_and_db
    client = app.test_client()
    monkeypatch.setattr(
        "models.get_active_model",
        lambda: {
            "name": "iNat21 (EVA-02 Large)",
            "model_type": "timm",
            "model_str": "hf-hub:timm/eva02",
            "downloaded": True,
        },
    )
    resp = client.get("/api/photos/search?q=bird+on+water")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["results"] == []
    assert data["total_matches"] == 0
    # Should indicate text search is not supported for this model type
    assert data.get("reason") == "model_no_text_search"


def test_text_search_no_embeddings_returns_reason(app_and_db, monkeypatch):
    """Text search explains when no embeddings exist for the active model."""
    app, _ = app_and_db
    client = app.test_client()
    monkeypatch.setattr(
        "models.get_active_model",
        lambda: {
            "name": "BioCLIP-2",
            "model_type": "bioclip",
            "model_str": "hf-hub:imageomics/bioclip-2",
            "weights_path": "/fake/path",
            "downloaded": True,
        },
    )
    resp = client.get("/api/photos/search?q=bird+on+water")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["results"] == []
    assert data["total_matches"] == 0
    # Should indicate no embeddings exist for this model
    assert data.get("reason") == "no_embeddings"


def test_settings_has_edit_history_config(app_and_db):
    """Settings page includes the max_edit_history config field."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/settings')
    html = resp.data.decode()
    assert 'max_edit_history' in html


def test_pipeline_detach_burst(app_and_db):
    """POST /api/pipeline/detach-burst moves a burst to a new encounter."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    # Create fake pipeline results in cache
    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Robin", 0.9],
                "confirmed_species": None,
                "species_predictions": [{"species": "Robin", "count": 3, "models": [{"model": "m1", "confidence": 0.9, "photo_count": 3}]}],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 2,
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "species_top5": [["Robin", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "species_top5": [["Robin", 0.85, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "species_top5": [["Eagle", 0.8, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 2,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-burst",
                       json={"encounter_index": 0, "burst_index": 1})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    # Original encounter should now have 1 burst, new encounter created
    assert len(data["encounters"]) == 2
    assert len(data["encounters"][0]["bursts"]) == 1
    assert data["encounters"][1]["photo_ids"] == [3]
    # Remaining encounter predictions should only reflect photos 1,2
    remaining_species = [sp["species"] for sp in data["encounters"][0]["species_predictions"]]
    assert "Robin" in remaining_species
    assert "Eagle" not in remaining_species
    # New encounter predictions should reflect photo 3
    new_species = [sp["species"] for sp in data["encounters"][1]["species_predictions"]]
    assert "Eagle" in new_species


def test_pipeline_detach_photo(app_and_db):
    """POST /api/pipeline/detach-photo moves a photo to a new burst."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Robin", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 1,
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2, 3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "species_top5": [["Robin", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "species_top5": [["Robin", 0.85, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "species_top5": [["Eagle", 0.8, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 1,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-photo",
                       json={"encounter_index": 0, "burst_index": 0, "photo_id": 3})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    # Original burst should have 2 photos, new burst with 1 photo
    enc = data["encounters"][0]
    assert len(enc["bursts"]) == 2
    assert enc["bursts"][0]["photo_ids"] == [1, 2]
    assert enc["bursts"][1]["photo_ids"] == [3]
    # Source burst predictions should only reflect photos 1,2
    src_species = [sp["species"] for sp in enc["bursts"][0]["species_predictions"]]
    assert "Robin" in src_species
    assert "Eagle" not in src_species
    # New burst predictions should reflect photo 3
    new_species = [sp["species"] for sp in enc["bursts"][1]["species_predictions"]]
    assert "Eagle" in new_species


def test_encounter_species_auto_detaches_mixed_burst(app_and_db):
    """Confirming a burst to a species different from its encounter auto-detaches it."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Bald Eagle", 0.9],
                "confirmed_species": None,
                "species_predictions": [{"species": "Bald Eagle", "count": 3, "models": []}],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 2,
                "time_range": ["2024-06-10T09:00:00", "2024-06-10T09:05:00"],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "timestamp": "2024-06-10T09:00:00", "species_top5": [["Bald Eagle", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "timestamp": "2024-06-10T09:00:02", "species_top5": [["Bald Eagle", 0.9, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "timestamp": "2024-06-10T09:05:00", "species_top5": [["Golden Eagle", 0.6, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 2,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    # Confirm burst 1 (photo 3) as Golden Eagle — differs from encounter's Bald Eagle
    resp = client.post("/api/encounters/species",
                       json={"species": "Golden Eagle", "photo_ids": [3], "burst_index": 1})
    assert resp.status_code == 200

    # Response must include updated encounters so the client can refresh its
    # local state and avoid overwriting the detach via a later save-cache POST.
    body = resp.get_json()
    assert "encounters" in body
    assert "summary" in body
    assert len(body["encounters"]) == 2

    with open(path) as f:
        updated = _json.load(f)
    encounters = updated["encounters"]
    # Original encounter should no longer contain burst with photo 3
    assert len(encounters) == 2
    bald_enc = next(e for e in encounters if 1 in e["photo_ids"])
    eagle_enc = next(e for e in encounters if 3 in e["photo_ids"])
    assert bald_enc is not eagle_enc
    assert bald_enc["photo_ids"] == [1, 2]
    assert eagle_enc["photo_ids"] == [3]
    assert eagle_enc["species_confirmed"] is True
    assert eagle_enc["confirmed_species"] == "Golden Eagle"


def test_encounter_species_confirm_single_burst_does_not_detach(app_and_db):
    """Confirming the only burst in an encounter does not detach (nothing to split from)."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Bald Eagle", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 2,
                "burst_count": 1,
                "time_range": ["2024-06-10T09:00:00", "2024-06-10T09:00:02"],
                "photo_ids": [1, 2],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "timestamp": "2024-06-10T09:00:00"},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "timestamp": "2024-06-10T09:00:02"},
        ],
        "summary": {"total_photos": 2, "encounter_count": 1, "burst_count": 1,
                     "keep_count": 2, "review_count": 0, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/encounters/species",
                       json={"species": "Golden Eagle", "photo_ids": [1, 2], "burst_index": 0})
    assert resp.status_code == 200

    with open(path) as f:
        updated = _json.load(f)
    # Still one encounter, burst stays put, override recorded
    assert len(updated["encounters"]) == 1
    enc = updated["encounters"][0]
    assert len(enc["bursts"]) == 1
    assert enc["bursts"][0]["species_override"] == {"species": "Golden Eagle", "confirmed": True}


def test_encounter_species_detach_merges_into_adjacent_encounter(app_and_db):
    """Detaching a second burst merges it into an adjacent encounter with matching confirmed species."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    # Original encounter has 3 bursts, all "Bald Eagle" predictions.
    # After first burst confirmed Golden Eagle and auto-detached, confirming another
    # burst to Golden Eagle should merge into the detached encounter (adjacent in time).
    results = {
        "encounters": [
            {
                "species": ["Bald Eagle", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 3,
                "time_range": ["2024-06-10T09:00:00", "2024-06-10T09:10:00"],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1], "species_predictions": [], "species_override": None},
                    {"photo_ids": [2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "timestamp": "2024-06-10T09:00:00"},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "timestamp": "2024-06-10T09:05:00"},
            {"id": 3, "label": "KEEP", "filename": "c.jpg", "timestamp": "2024-06-10T09:10:00"},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 3,
                     "keep_count": 3, "review_count": 0, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    # Confirm burst 2 (photo 3) as Golden Eagle -> detaches to new Golden Eagle encounter
    resp = client.post("/api/encounters/species",
                       json={"species": "Golden Eagle", "photo_ids": [3], "burst_index": 2})
    assert resp.status_code == 200

    # Now confirm burst (photo 2, still in original encounter) as Golden Eagle.
    # Its burst_index in the original encounter is now 1 (after photo 3 detached).
    with open(path) as f:
        mid = _json.load(f)
    bald_idx = next(i for i, e in enumerate(mid["encounters"]) if 1 in e["photo_ids"])
    burst_idx_in_bald = next(
        i for i, b in enumerate(mid["encounters"][bald_idx]["bursts"]) if 2 in b["photo_ids"]
    )
    resp = client.post("/api/encounters/species",
                       json={"species": "Golden Eagle", "photo_ids": [2],
                             "burst_index": burst_idx_in_bald,
                             "encounter_index": bald_idx})
    assert resp.status_code == 200

    with open(path) as f:
        final = _json.load(f)
    # Expect 2 encounters: original Bald Eagle (photo 1), one Golden Eagle with photos 2 & 3
    assert len(final["encounters"]) == 2
    golden = next(e for e in final["encounters"] if e.get("confirmed_species") == "Golden Eagle")
    assert set(golden["photo_ids"]) == {2, 3}
    assert len(golden["bursts"]) == 2
    bald = next(e for e in final["encounters"] if e is not golden)
    assert bald["photo_ids"] == [1]


def test_keyword_duplicates_scoped_by_workspace(app_and_db):
    """Keyword duplicates endpoint only reports duplicates within the active workspace."""
    app, db = app_and_db
    ws = db._active_workspace_id

    # Default workspace already has photos from conftest — add a case-variant keyword
    # Must insert directly to bypass add_keyword's case-insensitive dedup
    cur = db.conn.execute(
        "INSERT INTO keywords (name, is_species) VALUES (?, 0)", ("cardinal",)
    )
    db.conn.commit()
    k = cur.lastrowid
    # Tag a photo in the current workspace with the variant
    photos = db.get_photos()
    db.tag_photo(photos[0]["id"], k)

    # Create workspace B with its own folder and photo
    ws_b = db.create_workspace("B")
    db.set_active_workspace(ws_b)
    fid_b = db.add_folder("/photos/b", name="b")
    pid_b = db.add_photo(folder_id=fid_b, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    # Insert a case-variant of "Sparrow" directly to bypass dedup
    cur_b = db.conn.execute(
        "INSERT INTO keywords (name, is_species) VALUES (?, 0)", ("sparrow",)
    )
    db.conn.commit()
    k_b = cur_b.lastrowid
    db.tag_photo(pid_b, k_b)

    # Switch back to default workspace for the API call
    db.set_active_workspace(ws)

    with app.test_client() as c:
        # In workspace A, should see Cardinal/cardinal dupe but not Sparrow/sparrow
        resp = c.get("/api/keywords/duplicates")
        data = resp.get_json()
        dupe_names = []
        for d in data:
            for v in d["variants"]:
                dupe_names.append(v["name"])
        assert "Cardinal" in dupe_names or "cardinal" in dupe_names
        # sparrow dupe is only in ws_b, should not appear
        assert "sparrow" not in dupe_names


def test_all_keywords_scoped_by_workspace(app_and_db):
    """GET /api/keywords/all only returns keywords used in the active workspace, plus ancestors."""
    app, db = app_and_db
    ws_a = db._active_workspace_id

    # Create parent keyword "Birds" and child "Hawk" under it
    k_birds = db.add_keyword("Birds")
    k_hawk = db.add_keyword("Hawk", parent_id=k_birds)
    # Tag a photo in workspace A with the child only
    photos_a = db.get_photos()
    db.tag_photo(photos_a[0]["id"], k_hawk)

    # Create workspace B with its own folder, photo, and keyword "Penguin"
    ws_b = db.create_workspace("B")
    db.set_active_workspace(ws_b)
    fid_b = db.add_folder("/photos/b", name="b")
    pid_b = db.add_photo(folder_id=fid_b, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    k_penguin = db.add_keyword("Penguin")
    db.tag_photo(pid_b, k_penguin)

    # Switch back to workspace A
    db.set_active_workspace(ws_a)

    with app.test_client() as c:
        resp = c.get("/api/keywords/all")
        data = resp.get_json()
        names = [k["name"] for k in data]
        # Child keyword tagged in workspace A — present
        assert "Hawk" in names
        # Parent keyword not tagged but is ancestor of Hawk — present with photo_count=0
        assert "Birds" in names
        birds = next(k for k in data if k["name"] == "Birds")
        assert birds["photo_count"] == 0
        # Keyword only in workspace B — absent
        assert "Penguin" not in names


def test_set_active_labels_scoped_to_workspace(app_and_db, tmp_path):
    """Setting active labels stores them in workspace config_overrides, not global file."""
    app, db = app_and_db

    # Create a fake label file
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(exist_ok=True)
    label_path = str(labels_dir / "test-birds.txt")
    with open(label_path, "w") as f:
        f.write("Robin\nJay\n")

    with app.test_client() as c:
        resp = c.post("/api/labels/active",
                       json={"labels_files": [label_path]},
                       content_type="application/json")
        assert resp.status_code == 200

    # Verify it's stored in workspace config_overrides
    result = db.get_workspace_active_labels()
    assert result == [label_path]


def test_labels_list_returns_workspace_active(app_and_db, tmp_path):
    """GET /api/labels returns active labels from the workspace, not global."""
    app, db = app_and_db

    # Set workspace-specific active labels
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(exist_ok=True)
    label_path = str(labels_dir / "test-birds.txt")
    with open(label_path, "w") as f:
        f.write("Robin\nJay\n")
    meta_path = str(labels_dir / "test-birds.json")
    import json as _json
    with open(meta_path, "w") as f:
        _json.dump({"name": "Test Birds", "labels_file": label_path, "species_count": 2}, f)

    db.set_workspace_active_labels([label_path])

    import labels as labels_mod
    orig_labels_dir = labels_mod.LABELS_DIR
    labels_mod.LABELS_DIR = str(labels_dir)
    try:
        with app.test_client() as c:
            resp = c.get("/api/labels")
            data = resp.get_json()
            active_files = [a.get("labels_file") for a in data["active"]]
            assert label_path in active_files
    finally:
        labels_mod.LABELS_DIR = orig_labels_dir


def test_pipeline_page_init_includes_workspace_overrides(app_and_db):
    """page-init response includes workspace config overrides."""
    app, db = app_and_db
    # Set a workspace override first
    db.update_workspace(db._active_workspace_id, config_overrides={"review_min_confidence": 25})
    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "workspace_overrides" in data
        assert data["workspace_overrides"]["review_min_confidence"] == 25


def test_review_min_confidence_persists_in_workspace(app_and_db):
    """review_min_confidence can be saved and read from workspace config."""
    app, db = app_and_db
    with app.test_client() as c:
        # Save threshold
        resp = c.post("/api/workspaces/active/config",
                       json={"review_min_confidence": 40},
                       content_type="application/json")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["overrides"]["review_min_confidence"] == 40

        # Read it back
        resp = c.get("/api/workspaces/active/config")
        assert resp.status_code == 200
        assert resp.get_json()["review_min_confidence"] == 40


def test_workspace_config_post_preserves_non_whitelisted_keys(app_and_db):
    """POST /api/workspaces/active/config merges into existing overrides,
    preserving keys not in the whitelist (e.g. active_labels)."""
    app, db = app_and_db
    # Pre-set overrides with a non-whitelisted key
    db.update_workspace(db._active_workspace_id,
                        config_overrides={"active_labels": ["/path/to/birds.txt"],
                                          "classification_threshold": 0.5})
    with app.test_client() as c:
        # POST only review_min_confidence
        resp = c.post("/api/workspaces/active/config",
                       json={"review_min_confidence": 30},
                       content_type="application/json")
        assert resp.status_code == 200
        overrides = resp.get_json()["overrides"]
        # New key saved
        assert overrides["review_min_confidence"] == 30
        # Whitelisted key preserved
        assert overrides["classification_threshold"] == 0.5
        # Non-whitelisted key preserved
        assert overrides["active_labels"] == ["/path/to/birds.txt"]


def test_get_all_keywords(app_and_db):
    """GET /api/keywords/all returns only keywords used in the active workspace."""
    app, db = app_and_db
    client = app.test_client()
    # conftest already created 'Cardinal' (tagged to p1) and 'Sparrow' (tagged to p2)
    # Add an untagged keyword — should NOT appear since it has no photos in workspace
    db.add_keyword("favorite")

    resp = client.get("/api/keywords/all")
    assert resp.status_code == 200
    data = resp.get_json()
    names = [k["name"] for k in data]
    assert "Cardinal" in names
    assert "Sparrow" in names
    assert "favorite" not in names
    cardinal = next(k for k in data if k["name"] == "Cardinal")
    assert cardinal["photo_count"] >= 1
    assert "type" in cardinal


def test_update_keyword_type(app_and_db):
    """PUT /api/keywords/<id> updates keyword type."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("Tim")
    resp = client.put(f"/api/keywords/{kid}", json={"type": "people"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    row = db.conn.execute("SELECT type FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["type"] == "people"


def test_update_keyword_type_invalid(app_and_db):
    """PUT /api/keywords/<id> rejects invalid types."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("test")
    resp = client.put(f"/api/keywords/{kid}", json={"type": "invalid_type"})
    assert resp.status_code == 400


def test_update_keyword_name(app_and_db):
    """PUT /api/keywords/<id> can rename a keyword."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("old_name")
    resp = client.put(f"/api/keywords/{kid}", json={"name": "new_name"})
    assert resp.status_code == 200
    row = db.conn.execute("SELECT name FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["name"] == "new_name"


def test_rename_keyword_queues_sidecar_changes(app_and_db):
    """Renaming a keyword queues remove+add pending changes for affected photos."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("OldBird")
    # conftest photos: p1 is in folder '/photos/2024'
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p1, kid)
    # Clear any prior pending changes
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.put(f"/api/keywords/{kid}", json={"name": "NewBird"})
    assert resp.status_code == 200

    changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? ORDER BY id",
        (p1,),
    ).fetchall()
    actions = [(c["change_type"], c["value"]) for c in changes]
    assert ("keyword_remove", "OldBird") in actions
    assert ("keyword_add", "NewBird") in actions


def test_delete_keyword_queues_sidecar_removals(app_and_db):
    """Deleting a keyword queues removal pending changes for affected photos."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("ToDelete")
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p1, kid)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.delete(f"/api/keywords/{kid}")
    assert resp.status_code == 200

    changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ?",
        (p1,),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "ToDelete" for c in changes)
    # Keyword should be gone
    assert db.conn.execute("SELECT id FROM keywords WHERE id = ?", (kid,)).fetchone() is None


def test_rename_with_invalid_type_queues_nothing(app_and_db):
    """PUT with invalid type + name returns 400 and queues no sidecar changes."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("StableKeyword")
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p1, kid)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.put(f"/api/keywords/{kid}", json={"name": "Renamed", "type": "invalid"})
    assert resp.status_code == 400

    # No sidecar changes should have been queued
    count = db.conn.execute(
        "SELECT COUNT(*) as cnt FROM pending_changes WHERE photo_id = ?", (p1,)
    ).fetchone()["cnt"]
    assert count == 0
    # Keyword name should be unchanged
    row = db.conn.execute("SELECT name FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["name"] == "StableKeyword"


def test_rename_keyword_queues_for_all_workspaces(app_and_db):
    """Renaming a keyword queues sidecar changes for photos in all workspaces."""
    app, db = app_and_db
    client = app.test_client()

    # Create a second workspace with its own folder and photo
    ws2 = db.create_workspace("Second")
    fid2 = db.add_folder("/photos/ws2", name="ws2")
    db.add_workspace_folder(ws2, fid2)
    p_ws2 = db.add_photo(folder_id=fid2, filename="ws2bird.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0, timestamp="2024-01-01T00:00:00")

    # Tag photos in both workspaces with the same keyword
    kid = db.add_keyword("SharedBird")
    p_ws1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p_ws1, kid)
    db.tag_photo(p_ws2, kid)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    # Rename keyword (active workspace is ws1)
    resp = client.put(f"/api/keywords/{kid}", json={"name": "RenamedBird"})
    assert resp.status_code == 200

    # Check pending changes for ws1 photo
    ws1_id = db._ws_id()
    ws1_changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? AND workspace_id = ?",
        (p_ws1, ws1_id),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "SharedBird" for c in ws1_changes)
    assert any(c["change_type"] == "keyword_add" and c["value"] == "RenamedBird" for c in ws1_changes)

    # Check pending changes for ws2 photo — should also be queued under ws2
    ws2_changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? AND workspace_id = ?",
        (p_ws2, ws2),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "SharedBird" for c in ws2_changes)
    assert any(c["change_type"] == "keyword_add" and c["value"] == "RenamedBird" for c in ws2_changes)


def test_delete_keyword_queues_for_all_workspaces(app_and_db):
    """Deleting a keyword queues sidecar removals for photos in all workspaces."""
    app, db = app_and_db
    client = app.test_client()

    ws2 = db.create_workspace("Second")
    fid2 = db.add_folder("/photos/ws2del", name="ws2del")
    db.add_workspace_folder(ws2, fid2)
    p_ws2 = db.add_photo(folder_id=fid2, filename="ws2del.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0, timestamp="2024-01-01T00:00:00")

    kid = db.add_keyword("SharedDelete")
    p_ws1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p_ws1, kid)
    db.tag_photo(p_ws2, kid)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.delete(f"/api/keywords/{kid}")
    assert resp.status_code == 200

    ws1_id = db._ws_id()
    ws1_changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? AND workspace_id = ?",
        (p_ws1, ws1_id),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "SharedDelete" for c in ws1_changes)

    ws2_changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? AND workspace_id = ?",
        (p_ws2, ws2),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "SharedDelete" for c in ws2_changes)


def test_shortcuts_page(app_and_db):
    """GET /shortcuts returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/shortcuts')
    assert resp.status_code == 200


def test_shortcuts_link_in_navbar(app_and_db):
    """The navbar includes a link to /shortcuts."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/shortcuts')
    assert b'/shortcuts' in resp.data
    assert b'Shortcuts' in resp.data


def test_settings_no_shortcuts_editor(app_and_db):
    """Settings page no longer contains the shortcuts editor."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/settings')
    html = resp.data.decode()
    assert 'shortcutsEditor' not in html


def test_shortcuts_cheat_sheet_in_navbar(app_and_db):
    """Every page includes the shortcuts cheat sheet overlay."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    html = resp.data.decode()
    assert 'shortcutsCheatSheet' in html


def test_api_browse_home(app_and_db, tmp_path, monkeypatch):
    """GET /api/browse without path returns home directory listing."""
    monkeypatch.setenv("HOME", str(tmp_path))
    sub = tmp_path / "Documents"
    sub.mkdir()
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/browse')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['path'] == str(tmp_path)
    names = [d['name'] for d in data['dirs']]
    assert 'Documents' in names


def test_api_browse_with_path(app_and_db, tmp_path):
    """GET /api/browse?path=... returns subdirectories."""
    parent = tmp_path / "photos"
    parent.mkdir()
    (parent / "2024").mkdir()
    (parent / "2025").mkdir()
    (parent / "file.txt").write_text("hi")  # should not appear
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get(f'/api/browse?path={parent}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['path'] == str(parent)
    names = [d['name'] for d in data['dirs']]
    assert '2024' in names
    assert '2025' in names
    assert 'file.txt' not in names


def test_api_browse_hides_dotfiles(app_and_db, tmp_path):
    """GET /api/browse hides dot-prefixed directories."""
    (tmp_path / ".hidden").mkdir()
    (tmp_path / "visible").mkdir()
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get(f'/api/browse?path={tmp_path}')
    data = resp.get_json()
    names = [d['name'] for d in data['dirs']]
    assert 'visible' in names
    assert '.hidden' not in names


def test_api_browse_invalid_path(app_and_db):
    """GET /api/browse with invalid path returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/browse?path=/nonexistent/path/xyz')
    assert resp.status_code == 400


def test_api_browse_mkdir(app_and_db, tmp_path):
    """POST /api/browse/mkdir creates a new directory."""
    new_dir = str(tmp_path / "new_folder")
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/mkdir',
                       json={"path": new_dir},
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['name'] == 'new_folder'
    assert data['path'] == new_dir
    assert os.path.isdir(new_dir)


def test_api_browse_mkdir_nested(app_and_db, tmp_path):
    """POST /api/browse/mkdir creates nested directories."""
    new_dir = str(tmp_path / "a" / "b" / "c")
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/mkdir',
                       json={"path": new_dir},
                       content_type='application/json')
    assert resp.status_code == 200
    assert os.path.isdir(new_dir)


def test_api_browse_mkdir_relative_path(app_and_db):
    """POST /api/browse/mkdir rejects relative paths."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/mkdir',
                       json={"path": "relative/path"},
                       content_type='application/json')
    assert resp.status_code == 400


def test_api_browse_mkdir_missing_path(app_and_db):
    """POST /api/browse/mkdir rejects missing path."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/mkdir',
                       json={},
                       content_type='application/json')
    assert resp.status_code == 400


def test_api_browse_photo_counts_recursive(app_and_db, tmp_path):
    """POST /api/browse/photo-counts returns recursive photo counts per path."""
    # Folder with photos at root
    a = tmp_path / "a"
    a.mkdir()
    (a / "one.jpg").write_bytes(b"x")
    (a / "two.jpg").write_bytes(b"x")
    # Folder with photos only in subfolder (recursive must find them)
    b = tmp_path / "b"
    b.mkdir()
    (b / "nested").mkdir()
    (b / "nested" / "deep.jpg").write_bytes(b"x")
    # Folder with no photos
    c = tmp_path / "c"
    c.mkdir()
    (c / "readme.txt").write_text("hi")

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [str(a), str(b), str(c)],
                             "file_types": [".jpg"]},
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["counts"][str(a)] == 2
    assert data["counts"][str(b)] == 1
    assert data["counts"][str(c)] == 0


def test_api_browse_photo_counts_empty_paths(app_and_db):
    """POST /api/browse/photo-counts with empty paths returns empty counts."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [], "file_types": [".jpg"]},
                       content_type='application/json')
    assert resp.status_code == 200
    assert resp.get_json()["counts"] == {}


def test_api_browse_photo_counts_skips_missing(app_and_db, tmp_path):
    """POST /api/browse/photo-counts tolerates paths that don't exist."""
    real = tmp_path / "real"
    real.mkdir()
    (real / "img.jpg").write_bytes(b"x")
    missing = str(tmp_path / "does_not_exist")

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [str(real), missing],
                             "file_types": [".jpg"]},
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["counts"][str(real)] == 1
    assert data["counts"][missing] == 0


def test_api_browse_photo_counts_skips_non_string_entries(app_and_db, tmp_path):
    """POST /api/browse/photo-counts skips non-string path entries (no 500)."""
    real = tmp_path / "real"
    real.mkdir()
    (real / "img.jpg").write_bytes(b"x")

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [str(real), {}, [], 42, None],
                             "file_types": [".jpg"]},
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["counts"] == {str(real): 1}


def test_api_browse_photo_counts_respects_file_types(app_and_db, tmp_path):
    """POST /api/browse/photo-counts only counts files matching requested types."""
    d = tmp_path / "mixed"
    d.mkdir()
    (d / "a.jpg").write_bytes(b"x")
    (d / "b.nef").write_bytes(b"x")
    (d / "c.txt").write_text("hi")

    app, _ = app_and_db
    client = app.test_client()
    # Only request .nef
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [str(d)], "file_types": [".nef"]},
                       content_type='application/json')
    assert resp.status_code == 200
    assert resp.get_json()["counts"][str(d)] == 1


def test_nav_order_save_and_load(app_and_db):
    """PUT /api/workspaces/active/nav-order saves and returns nav order."""
    app, db = app_and_db
    client = app.test_client()
    order = ["browse", "pipeline", "cull", "review"]
    resp = client.put('/api/workspaces/active/nav-order',
                      json={"nav_order": order},
                      content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["nav_order"] == order

    # Verify it persists in config overrides
    resp2 = client.get('/api/workspaces/active/config')
    assert resp2.status_code == 200
    assert resp2.get_json()["nav_order"] == order


def test_nav_order_rejects_non_list(app_and_db):
    """PUT /api/workspaces/active/nav-order rejects non-list input."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.put('/api/workspaces/active/nav-order',
                      json={"nav_order": "not-a-list"},
                      content_type='application/json')
    assert resp.status_code == 400


def test_workspace_page_no_scan_button(app_and_db):
    """Workspace page should not have a Scan & Add button — folders are added via Pipeline."""
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.get('/workspace')
        assert resp.status_code == 200
        html = resp.data.decode()
        assert 'Scan &amp; Add' not in html
        assert 'scanAndAddFolder' not in html


def test_workspace_page_has_add_folder_link(app_and_db):
    """Workspace page should have an Add Folder button linking to Pipeline."""
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.get('/workspace')
        assert resp.status_code == 200
        html = resp.data.decode()
        assert 'href="/pipeline"' in html
        assert 'Add Folder' in html


# -- Missing folder API tests --


def test_api_folders_missing(app_and_db):
    """GET /api/folders/missing returns missing folders with counts."""
    app, db = app_and_db
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE path = '/photos/2024'")
    db.conn.commit()

    client = app.test_client()
    resp = client.get("/api/folders/missing")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]["path"] == "/photos/2024"
    assert data[0]["photo_count"] >= 1


def test_api_folders_check_health(app_and_db):
    """POST /api/folders/check-health triggers health check and returns missing folders."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.post("/api/folders/check-health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "changed" in data
    assert "missing" in data
    assert isinstance(data["missing"], list)


def test_api_folder_relocate(app_and_db, tmp_path):
    """POST /api/folders/<id>/relocate updates path and status."""
    app, db = app_and_db
    fid = db.conn.execute("SELECT id FROM folders WHERE path = '/photos/2024'").fetchone()["id"]
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid,))
    db.conn.commit()

    new_path = str(tmp_path / "relocated")
    os.makedirs(new_path)

    client = app.test_client()
    resp = client.post(f"/api/folders/{fid}/relocate", json={"path": new_path})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"

    row = db.conn.execute("SELECT status, path FROM folders WHERE id = ?", (fid,)).fetchone()
    assert row["status"] == "ok"
    assert row["path"] == new_path


def test_api_folder_relocate_merge(app_and_db, tmp_path):
    """POST /api/folders/<id>/relocate merges into existing folder when paths conflict."""
    app, db = app_and_db

    dir_a = str(tmp_path / "folder_a")
    dir_b = str(tmp_path / "folder_b")
    os.makedirs(dir_a)
    os.makedirs(dir_b)

    fid_a = db.add_folder(dir_a, name="a")
    fid_b = db.add_folder(dir_b, name="b")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_a,))
    db.conn.commit()

    # Remove dir_a from disk so the source is truly missing
    os.rmdir(dir_a)

    # Create photo1.jpg on disk in the target folder
    (tmp_path / "folder_b" / "photo1.jpg").write_bytes(b"\xff\xd8")

    # Add a photo to each folder
    db.add_photo(fid_a, "photo1.jpg", ".jpg", 1000, 1.0)
    db.add_photo(fid_b, "photo2.jpg", ".jpg", 1000, 1.0)

    client = app.test_client()
    resp = client.post(f"/api/folders/{fid_a}/relocate", json={"path": dir_b})
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "ok"

    # Folder A should be gone
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid_a,)).fetchone() is None
    # Both photos should now be in folder B
    photos = db.conn.execute("SELECT filename FROM photos WHERE folder_id = ?", (fid_b,)).fetchall()
    filenames = {p["filename"] for p in photos}
    assert filenames == {"photo1.jpg", "photo2.jpg"}


def test_api_folder_delete(app_and_db):
    """DELETE /api/folders/<id> removes folder and its photos."""
    app, db = app_and_db
    fid = db.conn.execute("SELECT id FROM folders WHERE path = '/photos/2024/January'").fetchone()["id"]
    photo_count_before = db.conn.execute(
        "SELECT COUNT(*) FROM photos WHERE folder_id = ?", (fid,)
    ).fetchone()[0]
    assert photo_count_before > 0

    client = app.test_client()
    resp = client.delete(f"/api/folders/{fid}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted_photos"] == photo_count_before

    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid,)).fetchone() is None


def test_api_folder_delete_removes_preview_files(app_and_db, tmp_path):
    """Folder delete must unlink on-disk preview files, not just drop DB rows.

    The preview_cache FK cascades on photo delete, so rows vanish — but
    unless we explicitly unlink the files, they become untracked bytes
    that eviction can't reclaim.
    """
    app, db = app_and_db
    fid = db.conn.execute(
        "SELECT id FROM folders WHERE path = '/photos/2024/January'"
    ).fetchone()["id"]
    photo_ids = [r["id"] for r in db.conn.execute(
        "SELECT id FROM photos WHERE folder_id = ?", (fid,)
    ).fetchall()]
    assert photo_ids

    thumb_dir = app.config["THUMB_CACHE_DIR"]
    vireo_dir = os.path.dirname(thumb_dir)
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    created = []
    for pid in photo_ids:
        sized = os.path.join(preview_dir, f"{pid}_1920.jpg")
        legacy = os.path.join(preview_dir, f"{pid}.jpg")
        with open(sized, "wb") as f:
            f.write(b"\xff\xd8\xff\xd9")  # minimal JPEG SOI/EOI
        with open(legacy, "wb") as f:
            f.write(b"\xff\xd8\xff\xd9")
        db.preview_cache_insert(pid, 1920, 4)
        created.extend([sized, legacy])

    client = app.test_client()
    resp = client.delete(f"/api/folders/{fid}")
    assert resp.status_code == 200

    for path in created:
        assert not os.path.exists(path), f"Preview file leaked after folder delete: {path}"


def test_folder_health_check_runs_at_startup(app_and_db):
    """The app marks non-existent folders as missing after startup."""
    app, db = app_and_db
    # Folders in test fixture use fake paths that don't exist on disk.
    # The health check should mark them missing.
    changed = db.check_folder_health()
    assert changed >= 1  # /photos/2024 and /photos/2024/January don't exist

    missing = db.get_missing_folders()
    assert len(missing) >= 1


def test_highlights_page(app_and_db):
    """GET /highlights returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/highlights")
    assert resp.status_code == 200


def test_highlights_get_empty(app_and_db):
    """GET /api/highlights returns empty when no quality data exists."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/highlights")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["photos"] == []
    assert "folders" in data
    assert "meta" in data


def test_highlights_get_with_data(app_and_db):
    """GET /api/highlights returns highlight photos for a folder with quality data."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/highlights_test', 'highlights_test', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    for i in range(20):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, ?, ?, 'none')",
            (fid, f"img{i}.jpg", 0.9 - i * 0.03),
        )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}&count=5")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["photos"]) == 5
    assert data["meta"]["total_in_folder"] == 20


def test_highlights_save(app_and_db):
    """POST /api/highlights/save creates a static collection."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/save_test', 'save_test', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score) VALUES (?, 'a.jpg', 0.8)",
        (fid,),
    ).lastrowid
    db.conn.commit()

    resp = client.post("/api/highlights/save", json={
        "photo_ids": [pid],
        "name": "Highlights - save_test",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert "id" in data

    # Verify collection was created
    collections = db.get_collections()
    names = [c["name"] for c in collections]
    assert "Highlights - save_test" in names


def test_highlights_scope_workspace_blends_folders(app_and_db):
    """GET /api/highlights?scope=workspace draws candidates from every
    folder in the active workspace, so a shoot split across multiple dated
    folders produces one combined highlight pool."""
    app, db = app_and_db
    client = app.test_client()
    f1 = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/shoot/2024-01-15', '2024-01-15', 'ok')"
    ).lastrowid
    f2 = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/shoot/2024-01-16', '2024-01-16', 'ok')"
    ).lastrowid
    for fid in (f1, f2):
        db.conn.execute(
            "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
            (db._ws_id(), fid),
        )
    # 3 photos in each folder with strong quality scores.
    for fid, prefix in [(f1, "a"), (f2, "b")]:
        for i in range(3):
            db.conn.execute(
                "INSERT INTO photos (folder_id, filename, quality_score, flag) "
                "VALUES (?, ?, ?, 'none')",
                (fid, f"{prefix}{i}.jpg", 0.9 - i * 0.01),
            )
    db.conn.commit()

    resp = client.get("/api/highlights?scope=workspace&count=6&max_per_species=10")
    assert resp.status_code == 200
    data = resp.get_json()
    filenames = {p["filename"] for p in data["photos"]}
    # All six photos across both folders are eligible and selected. The
    # fixture photos lack quality scores so they do not show up here.
    assert filenames == {"a0.jpg", "a1.jpg", "a2.jpg", "b0.jpg", "b1.jpg", "b2.jpg"}
    # scope=workspace blends candidates from every folder visible in the
    # active workspace, which the API advertises via meta.scope.
    assert data["scope"] == "workspace"
    assert data["meta"]["eligible"] == 6


def test_highlights_scope_workspace_isolates_other_workspaces(app_and_db):
    """Folders in a non-active workspace must not leak into the scope=workspace pool."""
    app, db = app_and_db
    client = app.test_client()
    active_ws = db._ws_id()
    other_ws = db.create_workspace("Other")

    # Folder in the active workspace.
    f_active = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/active', 'active', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (active_ws, f_active),
    )
    # Folder only in the other workspace.
    f_other = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/other', 'other', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (other_ws, f_other),
    )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, 'keep.jpg', 0.8, 'none')",
        (f_active,),
    )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, 'leaked.jpg', 0.95, 'none')",
        (f_other,),
    )
    db.conn.commit()

    resp = client.get("/api/highlights?scope=workspace&count=10&max_per_species=10")
    assert resp.status_code == 200
    data = resp.get_json()
    filenames = {p["filename"] for p in data["photos"]}
    assert "leaked.jpg" not in filenames
    assert filenames == {"keep.jpg"}


def test_highlights_folder_scope_still_works(app_and_db):
    """Regression: omitting scope (or passing a folder_id) still filters by folder."""
    app, db = app_and_db
    client = app.test_client()
    f1 = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/a', 'a', 'ok')"
    ).lastrowid
    f2 = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/b', 'b', 'ok')"
    ).lastrowid
    for fid in (f1, f2):
        db.conn.execute(
            "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
            (db._ws_id(), fid),
        )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, 'only_a.jpg', 0.8, 'none')",
        (f1,),
    )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, 'only_b.jpg', 0.9, 'none')",
        (f2,),
    )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={f1}&count=10&max_per_species=10")
    assert resp.status_code == 200
    data = resp.get_json()
    filenames = {p["filename"] for p in data["photos"]}
    assert filenames == {"only_a.jpg"}


def test_api_import_folder_preview(app_and_db, tmp_path):
    """POST /api/import/folder-preview returns file discovery results."""
    app, db = app_and_db

    # Create test images in a temp folder
    source = tmp_path / "source_photos"
    source.mkdir()
    from PIL import Image
    for name in ["a.jpg", "b.jpg", "c.png"]:
        Image.new("RGB", (200, 150)).save(str(source / name))
    # Non-image file should be excluded
    (source / "readme.txt").write_text("ignore me")

    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": [str(source)],
        "file_types": [".jpg", ".jpeg", ".png"],
    })
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["total_count"] == 3
    assert data["total_size"] > 0
    assert ".jpg" in data["type_breakdown"]
    assert data["type_breakdown"][".jpg"] == 2
    assert data["type_breakdown"][".png"] == 1
    assert len(data["files"]) == 3
    assert data["duplicate_count"] == 0


def test_api_import_folder_preview_duplicate_count_deferred(app_and_db, tmp_path):
    """Folder preview returns duplicate_count=0 (duplicate detection deferred)."""
    app, db = app_and_db

    source = tmp_path / "source_dupes"
    source.mkdir()
    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(source / "bird1.jpg"))
    Image.new("RGB", (100, 100)).save(str(source / "newbird.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": [str(source)],
        "file_types": [".jpg", ".jpeg"],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total_count"] == 2
    assert data["duplicate_count"] == 0


def test_api_import_folder_preview_no_folders(app_and_db):
    """Folder preview returns error when no folders provided."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={})
    assert resp.status_code == 400


def test_api_import_folder_preview_nonexistent(app_and_db):
    """Folder preview returns error for non-existent folder."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": ["/nonexistent/path/xyz"],
        "file_types": [".jpg"],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total_count"] == 0


def test_api_import_folder_preview_subfolders(app_and_db, tmp_path):
    """Folder preview groups files by subfolder."""
    app, _ = app_and_db

    source = tmp_path / "nested"
    (source / "sub1").mkdir(parents=True)
    (source / "sub2").mkdir(parents=True)
    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(source / "root.jpg"))
    Image.new("RGB", (100, 100)).save(str(source / "sub1" / "a.jpg"))
    Image.new("RGB", (100, 100)).save(str(source / "sub2" / "b.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": [str(source)],
        "file_types": [".jpg", ".jpeg"],
    })
    data = resp.get_json()
    assert data["total_count"] == 3

    # Files should have subfolder info
    subfolders = set()
    for f in data["files"]:
        subfolders.add(f["subfolder"])
    assert len(subfolders) == 3  # root, sub1, sub2


def test_api_import_folder_preview_multi_source_same_basename(app_and_db, tmp_path):
    """Multi-source preview disambiguates folders with same basename."""
    app, _ = app_and_db

    # Two sources with identical leaf names and overlapping subfolders
    card_a = tmp_path / "mnt" / "cardA" / "DCIM"
    card_b = tmp_path / "mnt" / "cardB" / "DCIM"
    (card_a / "100CANON").mkdir(parents=True)
    (card_b / "100CANON").mkdir(parents=True)
    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(card_a / "100CANON" / "a.jpg"))
    Image.new("RGB", (100, 100)).save(str(card_b / "100CANON" / "b.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": [str(card_a), str(card_b)],
        "file_types": [".jpg", ".jpeg"],
    })
    data = resp.get_json()
    assert data["total_count"] == 2

    # Subfolders must be distinct even though both have 100CANON
    subfolders = {f["subfolder"] for f in data["files"]}
    assert len(subfolders) == 2
    # Should use parent to disambiguate: cardA/DCIM/100CANON vs cardB/DCIM/100CANON
    for sf in subfolders:
        assert "DCIM" in sf
        assert "100CANON" in sf


def test_api_import_folder_preview_thumbnail(app_and_db, tmp_path):
    """GET /api/import/folder-preview/thumbnail returns a JPEG thumbnail."""
    app, _ = app_and_db

    # Create a test image
    source = tmp_path / "thumb_test"
    source.mkdir()
    from PIL import Image
    img = Image.new("RGB", (800, 600), color=(255, 0, 0))
    img_path = source / "photo.jpg"
    img.save(str(img_path))

    client = app.test_client()
    resp = client.get(f"/api/import/folder-preview/thumbnail?path={img_path}")
    assert resp.status_code == 200
    assert resp.content_type == "image/jpeg"
    assert len(resp.data) > 0

    # Verify the returned image is resized (200px long edge)
    import io
    thumb = Image.open(io.BytesIO(resp.data))
    assert max(thumb.size) == 200


def test_api_import_folder_preview_thumbnail_missing(app_and_db):
    """Thumbnail endpoint returns 404 for non-existent file."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/import/folder-preview/thumbnail?path=/no/such/file.jpg")
    assert resp.status_code == 404


def test_api_import_folder_preview_thumbnail_no_path(app_and_db):
    """Thumbnail endpoint returns 400 when path param is missing."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/import/folder-preview/thumbnail")
    assert resp.status_code == 400


def test_api_import_full_accepts_exclude_paths(app_and_db, tmp_path):
    """POST /api/jobs/import-full accepts exclude_paths parameter."""
    app, _ = app_and_db

    source = tmp_path / "import_src"
    source.mkdir()
    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(source / "keep.jpg"))
    Image.new("RGB", (100, 100)).save(str(source / "skip.jpg"))

    client = app.test_client()
    resp = client.post("/api/jobs/import-full", json={
        "source": str(source),
        "copy": False,
        "file_types": [".jpg", ".jpeg"],
        "exclude_paths": [str(source / "skip.jpg")],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data


def test_system_info_megadetector_weights_missing(app_and_db, monkeypatch, tmp_path):
    """/api/system/info reports weights_missing (not installed) when only the
    detector module imports but the ONNX weights file is absent.
    """
    import detector
    missing_path = str(tmp_path / "does_not_exist.onnx")
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_PATH", missing_path)

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/system/info")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["megadetector"] == "weights_missing"
    assert data["megadetector_weights"] == "not downloaded"
    assert "weights not downloaded" in data["megadetector_detail"].lower()


def test_system_info_megadetector_installed_when_weights_present(app_and_db, monkeypatch, tmp_path):
    """/api/system/info reports installed only when weights are on disk."""
    import detector
    weights = tmp_path / "model.onnx"
    weights.write_bytes(b"\x00" * 1024)
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_PATH", str(weights))

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/system/info")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["megadetector"] == "installed"
    assert data["megadetector_weights"] == "downloaded"


def test_pipeline_models_dinov2_incomplete_without_data_sidecar(
    app_and_db, monkeypatch, tmp_path,
):
    """DINOv2 reports 'incomplete' when only model.onnx is on disk.

    DINOv2 uses external-data ONNX: the ~1 MB model.onnx graph is useless
    without the companion model.onnx.data weights file. Without this check
    the status endpoint used to report "downloaded 1.0 MB" for a broken
    install that couldn't actually run.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    variant_dir = tmp_path / ".vireo" / "models" / "dinov2-vit-b14"
    variant_dir.mkdir(parents=True)
    (variant_dir / "model.onnx").write_bytes(b"\x00" * 1024)  # graph only

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/models/pipeline")
    assert resp.status_code == 200
    entry = next(m for m in resp.get_json()["models"] if m["id"] == "vit-b14")
    assert entry["status"] == "incomplete"
    assert entry["size"] is None


def test_pipeline_models_dinov2_downloaded_sums_graph_and_data(
    app_and_db, monkeypatch, tmp_path,
):
    """DINOv2 reports 'downloaded' with total size once both files exist."""
    monkeypatch.setenv("HOME", str(tmp_path))
    variant_dir = tmp_path / ".vireo" / "models" / "dinov2-vit-b14"
    variant_dir.mkdir(parents=True)
    (variant_dir / "model.onnx").write_bytes(b"\x00" * (1 * 1024 * 1024))
    (variant_dir / "model.onnx.data").write_bytes(b"\x00" * (10 * 1024 * 1024))

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/models/pipeline")
    assert resp.status_code == 200
    entry = next(m for m in resp.get_json()["models"] if m["id"] == "vit-b14")
    assert entry["status"] == "downloaded"
    assert entry["size"] == "11.0 MB"


def test_embedding_matrix_excludes_timm_models(app_and_db, monkeypatch, tmp_path):
    """Timm models don't use per-label text embeddings, so the matrix should
    not list them — otherwise Settings renders a 'Compute' button that fails
    because timm model dirs lack image_encoder.onnx."""
    labels_file = tmp_path / "birds.txt"
    labels_file.write_text("robin\nsparrow\n")

    monkeypatch.setattr(
        "models.get_models",
        lambda: [
            {
                "id": "bioclip-vit-b-16",
                "name": "BioCLIP",
                "model_type": "bioclip",
                "model_str": "ViT-B-16",
                "weights_path": str(tmp_path),
                "downloaded": True,
            },
            {
                "id": "timm-inat21-eva02-l",
                "name": "iNat21 (EVA-02 Large)",
                "model_type": "timm",
                "model_str": "hf-hub:timm/eva02",
                "weights_path": str(tmp_path),
                "downloaded": True,
            },
        ],
    )
    monkeypatch.setattr(
        "labels.get_saved_labels",
        lambda: [{"name": "Birds", "labels_file": str(labels_file)}],
    )

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/embedding-matrix")
    assert resp.status_code == 200
    data = resp.get_json()
    model_ids = [m["id"] for m in data["models"]]
    assert "bioclip-vit-b-16" in model_ids
    assert "timm-inat21-eva02-l" not in model_ids


def test_precompute_embeddings_rejects_timm_models(app_and_db, monkeypatch):
    """Hitting precompute-embeddings for a timm model must fail fast instead
    of trying to load a non-existent image_encoder.onnx from the timm dir."""
    monkeypatch.setattr(
        "models.get_models",
        lambda: [
            {
                "id": "timm-inat21-eva02-l",
                "name": "iNat21 (EVA-02 Large)",
                "model_type": "timm",
                "model_str": "hf-hub:timm/eva02",
                "weights_path": "/fake",
                "downloaded": True,
            },
        ],
    )

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/jobs/precompute-embeddings",
        json={"model_id": "timm-inat21-eva02-l", "labels_file": "/tmp/x.txt"},
    )
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    runner = app._job_runner
    import time
    deadline = time.time() + 5
    while time.time() < deadline:
        job = runner.get(job_id)
        if job and job.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)
    job = runner.get(job_id)
    assert job["status"] == "failed"
    assert any("fixed class head" in e for e in (job.get("errors") or []))
