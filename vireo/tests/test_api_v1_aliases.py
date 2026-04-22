def _auth(app):
    return {"X-Vireo-Token": app.config["API_TOKEN"]}


def test_api_v1_photos_returns_list(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/photos", headers=_auth(app))
    assert resp.status_code == 200
    assert isinstance(resp.get_json(), (list, dict))


def test_api_v1_photo_by_id(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    # pick any existing photo
    photos = db.get_photos()
    pid = photos[0]["id"]
    resp = client.get(f"/api/v1/photos/{pid}", headers=_auth(app))
    assert resp.status_code == 200
    assert resp.get_json()["id"] == pid


def test_api_v1_collections(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/collections", headers=_auth(app))
    assert resp.status_code == 200


def test_api_v1_workspaces(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/workspaces", headers=_auth(app))
    assert resp.status_code == 200


def test_api_v1_keywords(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/keywords", headers=_auth(app))
    assert resp.status_code == 200
    names = {k["name"] for k in resp.get_json()}
    assert "Cardinal" in names


def test_api_v1_workspace_activate(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ws_id = db.get_workspaces()[0]["id"]
    resp = client.post(
        f"/api/v1/workspaces/{ws_id}/activate", headers=_auth(app)
    )
    assert resp.status_code == 200
