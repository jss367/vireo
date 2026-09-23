def _auth(app):
    return {"X-Vireo-Token": app.config["API_TOKEN"]}


def test_api_v1_photos_returns_list(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/photos", headers=_auth(app))
    assert resp.status_code == 200
    assert isinstance(resp.get_json(), list | dict)


def test_api_v1_photo_by_id(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    # pick any existing photo
    photos = db.get_photos()
    pid = photos[0]["id"]
    resp = client.get(f"/api/v1/photos/{pid}", headers=_auth(app))
    assert resp.status_code == 200
    assert resp.get_json()["id"] == pid


def test_api_v1_photo_aliases_share_the_photos_blueprint_views(app_and_db):
    """The v1 photo endpoints keep their names and alias the blueprint views."""
    app, _ = app_and_db
    views = app.view_functions
    assert views["v1_api_photos"] is views["photos.api_photos"]
    assert views["v1_api_photo_detail"] is views["photos.api_photo_detail"]


def test_api_v1_collections(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/collections", headers=_auth(app))
    assert resp.status_code == 200


def test_api_v1_collection_photos(app_and_db):
    import json

    app, db = app_and_db
    client = app.test_client()
    photo_id = db.get_photos()[0]["id"]
    cid = db.add_collection(
        "Picked", json.dumps([{"field": "photo_ids", "value": [photo_id]}])
    )
    resp = client.get(f"/api/v1/collections/{cid}/photos", headers=_auth(app))
    assert resp.status_code == 200
    body = resp.get_json()
    assert [p["id"] for p in body["photos"]] == [photo_id]
    # The v1 surface answers exactly as the internal route does.
    assert body == client.get(f"/api/collections/{cid}/photos").get_json()


def test_api_v1_collection_aliases_keep_their_endpoint_names(app_and_db):
    """The collections routes live in a blueprint; the v1 aliases keep their
    ``v1_<view>`` endpoint names and point at the blueprint's views."""
    app, _ = app_and_db
    views = app.view_functions
    assert views["v1_api_collections"] is views["collections.api_collections"]
    assert (
        views["v1_api_collection_photos"]
        is views["collections.api_collection_photos"]
    )


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
