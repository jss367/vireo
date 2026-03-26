def test_index_redirects_to_browse(app_and_db):
    """GET / redirects to /browse."""
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
    pages = ['/browse', '/import', '/audit', '/logs',
             '/settings', '/workspace', '/pipeline', '/dashboard',
             '/review', '/cull', '/pipeline/review', '/map']
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

    # Add predictions from two models
    db.add_prediction(photo_ids[0], "Cardinal", 0.95, "model-a")
    db.add_prediction(photo_ids[0], "Blue Jay", 0.80, "model-b")
    db.add_prediction(photo_ids[1], "Sparrow", 0.90, "model-a")
    db.add_prediction(photo_ids[1], "Sparrow", 0.88, "model-b")

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
    pages = ['/browse', '/import', '/audit', '/logs',
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
    pages = ['/browse', '/import', '/audit', '/logs',
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
    from unittest.mock import patch, MagicMock

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
        with open(fpath, 'r', encoding='utf-8') as f:
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
