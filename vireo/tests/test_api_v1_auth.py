def test_api_v1_requires_token(app_and_db):
    """GET /api/v1/health without a token → 401."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/health")
    assert resp.status_code == 401


def test_api_v1_wrong_token_rejected(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/health", headers={"X-Vireo-Token": "wrong"})
    assert resp.status_code == 401


def test_api_v1_correct_token_accepted(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    token = app.config["API_TOKEN"]
    resp = client.get("/api/v1/health", headers={"X-Vireo-Token": token})
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}


def test_internal_api_does_not_require_token(app_and_db):
    """Existing /api/* routes are unaffected by the v1 middleware."""
    app, _ = app_and_db
    client = app.test_client()
    # /api/health is an internal route and must keep working without a token
    resp = client.get("/api/health")
    assert resp.status_code == 200
