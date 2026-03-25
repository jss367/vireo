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
             '/settings', '/workspace', '/pipeline', '/stats']
    for page in pages:
        resp = client.get(page)
        assert resp.status_code == 200, f"{page} returned {resp.status_code}"
        html = resp.data.decode()
        assert 'vireo-base.css' in html, f"{page} missing vireo-base.css link"
