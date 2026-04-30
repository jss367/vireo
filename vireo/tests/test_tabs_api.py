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


def test_close_tab_endpoint_removes(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/close", json={"nav_id": "settings"})
    assert r.status_code == 200
    assert "settings" not in r.get_json()["open_tabs"]


def test_close_tab_endpoint_idempotent_when_not_open(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/close", json={"nav_id": "settings"})
    r = client.post("/api/workspace/tabs/close", json={"nav_id": "settings"})
    assert r.status_code == 200


def test_close_tab_endpoint_rejects_unknown_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/close", json={"nav_id": "browse"})
    assert r.status_code == 400


def test_visiting_lightroom_url_auto_opens_tab(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    # Close lightroom first
    client.post("/api/workspace/tabs/close", json={"nav_id": "lightroom"})
    assert "lightroom" not in db.get_open_tabs()
    # Visit the page
    r = client.get("/lightroom")
    assert r.status_code == 200
    assert "lightroom" in db.get_open_tabs()


def test_visiting_logs_url_auto_opens_tab(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/close", json={"nav_id": "logs"})
    r = client.get("/logs")
    assert r.status_code == 200
    assert "logs" in db.get_open_tabs()


import pytest


@pytest.mark.parametrize("nav_id,url", [
    ("settings", "/settings"),
    ("workspace", "/workspace"),
    ("lightroom", "/lightroom"),
    ("shortcuts", "/shortcuts"),
    ("keywords", "/keywords"),
    ("duplicates", "/duplicates"),
    ("logs", "/logs"),
])
def test_visiting_openable_url_auto_opens_tab(app_and_db, nav_id, url):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/close", json={"nav_id": nav_id})
    assert nav_id not in db.get_open_tabs()
    r = client.get(url)
    assert r.status_code == 200
    assert nav_id in db.get_open_tabs()


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
