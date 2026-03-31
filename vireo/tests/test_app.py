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
