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


def test_pin_tab_endpoint_rejects_non_string_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    # Each of these would have raised TypeError on `nav_id not in ALL_NAV_IDS`
    # (unhashable type for list/dict) and surfaced as a 500. They must come
    # back as 400 instead.
    for bad in [["browse"], {"id": "browse"}, None, 42]:
        r = client.post("/api/workspace/tabs/pin", json={"nav_id": bad})
        assert r.status_code == 400, f"expected 400 for {bad!r}, got {r.status_code}"


def test_unpin_tab_endpoint_rejects_non_string_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    for bad in [["settings"], {"id": "settings"}, None, 42]:
        r = client.post("/api/workspace/tabs/unpin", json={"nav_id": bad})
        assert r.status_code == 400, f"expected 400 for {bad!r}, got {r.status_code}"


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


def test_reorder_tabs_rejects_non_string_entries(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    # Each of these would have raised TypeError on `nav_id not in ALL_NAV_IDS`
    # (unhashable / unsupported type) and surfaced as a 500. They must come
    # back as 400 instead.
    for bad in [[["browse"]], [{"id": "browse"}], [None], [42]]:
        r = client.post("/api/workspace/tabs/reorder", json={"tabs": bad})
        assert r.status_code == 400, f"expected 400 for {bad!r}, got {r.status_code}"


def test_get_tabs_drops_retired_nav_ids(app_and_db):
    """Workspaces upgraded from a version that had a since-retired page
    (e.g. ``zoom_test``) can still carry that id in ``workspaces.tabs``.
    ``get_tabs()`` must filter it out so the navbar never gets a nav id
    that isn't in ``all_pages`` — otherwise cmd+number reserves a dead
    slot and ``adjacentTabId()`` returns an id ``pageById`` doesn't know,
    which throws on close-adjacent.
    """
    import json
    app, db = app_and_db
    ws_id = db._active_workspace_id
    db.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json.dumps(["browse", "zoom_test", "cull"]), ws_id),
    )
    db.conn.commit()

    assert db.get_tabs() == ["browse", "cull"]

    client = app.test_client()
    body = client.get("/api/workspace/tabs").get_json()
    assert body["tabs"] == ["browse", "cull"]
    assert "zoom_test" not in [p["id"] for p in body["all_pages"]]


def test_get_tabs_endpoint_new_shape(app_and_db):
    from app import ALL_PAGES
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
    expected_ids = [p["id"] for p in ALL_PAGES]
    assert "duplicates" in ids
    assert "browse" in ids
    assert ids == expected_ids
    sample = next(p for p in body["all_pages"] if p["id"] == "duplicates")
    assert sample == {"id": "duplicates", "label": "Duplicates", "href": "/duplicates"}


def test_tabs_migration_adds_import_for_version_2_database(tmp_path):
    import json

    from db import Database

    db_path = str(tmp_path / "tabs-v2.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    db.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json.dumps(["browse", "pipeline", "review"]), ws_id),
    )
    db.conn.execute("PRAGMA user_version = 2")
    db.conn.commit()
    db.close()

    migrated = Database(db_path)
    try:
        assert migrated.get_tabs() == ["import", "browse", "pipeline", "review"]
        version = migrated.conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == 4
    finally:
        migrated.close()


def test_tabs_migration_moves_existing_import_to_front(tmp_path):
    import json

    from db import Database

    db_path = str(tmp_path / "tabs-v3.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    db.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json.dumps(["browse", "pipeline", "import", "review"]), ws_id),
    )
    db.conn.execute("PRAGMA user_version = 3")
    db.conn.commit()
    db.close()

    migrated = Database(db_path)
    try:
        assert migrated.get_tabs() == ["import", "browse", "pipeline", "review"]
        version = migrated.conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == 4
    finally:
        migrated.close()
