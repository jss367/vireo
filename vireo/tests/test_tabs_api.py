def test_open_tab_endpoint_appends(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/open", json={"nav_id": "keywords"})
    assert r.status_code == 200
    body = r.get_json()
    assert "keywords" in body["open_tabs"]


def test_open_tab_endpoint_rejects_unknown_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/open", json={"nav_id": "browse"})
    assert r.status_code == 400


def test_open_tab_endpoint_idempotent(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/open", json={"nav_id": "logs"})
    r = client.post("/api/workspace/tabs/open", json={"nav_id": "logs"})
    assert r.status_code == 200
    body = r.get_json()
    assert body["open_tabs"].count("logs") == 1
