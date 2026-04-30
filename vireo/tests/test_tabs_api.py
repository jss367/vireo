def test_pin_tab_endpoint_appends(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/pin", json={"nav_id": "logs"})
    assert r.status_code == 200
    body = r.get_json()
    assert "logs" in body["tabs"]


def test_pin_tab_endpoint_rejects_unknown_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/pin", json={"nav_id": "not_a_real_page"})
    assert r.status_code == 400


def test_pin_tab_endpoint_idempotent(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/pin", json={"nav_id": "logs"})
    r = client.post("/api/workspace/tabs/pin", json={"nav_id": "logs"})
    assert r.status_code == 200
    assert r.get_json()["tabs"].count("logs") == 1


def test_unpin_tab_endpoint_removes(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/unpin", json={"nav_id": "settings"})
    assert r.status_code == 200
    assert "settings" not in r.get_json()["tabs"]


def test_unpin_tab_endpoint_idempotent_when_not_pinned(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/unpin", json={"nav_id": "settings"})
    r = client.post("/api/workspace/tabs/unpin", json={"nav_id": "settings"})
    assert r.status_code == 200


def test_unpin_tab_endpoint_rejects_unknown_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/unpin", json={"nav_id": "not_a_real_page"})
    assert r.status_code == 400


def test_reorder_tabs_endpoint_replaces_order(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    new_order = ["cull", "review", "browse"]
    r = client.post("/api/workspace/tabs/reorder", json={"tabs": new_order})
    assert r.status_code == 200
    assert r.get_json()["tabs"] == new_order


def test_reorder_tabs_rejects_unknown_id(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/reorder",
                    json={"tabs": ["browse", "not_a_page"]})
    assert r.status_code == 400


def test_reorder_tabs_rejects_duplicates(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/reorder",
                    json={"tabs": ["browse", "browse"]})
    assert r.status_code == 400


def test_reorder_tabs_rejects_non_list(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/reorder", json={"tabs": "not-a-list"})
    assert r.status_code == 400


def test_get_tabs_endpoint_new_shape(app_and_db):
    from db import DEFAULT_TABS
    app, db = app_and_db
    client = app.test_client()
    r = client.get("/api/workspace/tabs")
    assert r.status_code == 200
    body = r.get_json()
    assert body["tabs"] == DEFAULT_TABS
    assert "all_pages" in body
    # all_pages must include every nav id, in a stable order, with label and href
    ids = [p["id"] for p in body["all_pages"]]
    assert "duplicates" in ids
    assert "browse" in ids
    assert len(ids) == 20
    sample = next(p for p in body["all_pages"] if p["id"] == "duplicates")
    assert sample == {"id": "duplicates", "label": "Duplicates", "href": "/duplicates"}
