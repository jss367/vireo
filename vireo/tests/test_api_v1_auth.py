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


def test_api_v1_rejects_when_no_token_configured(tmp_path, monkeypatch):
    """Default api_token=None must deny all /api/v1 traffic."""
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    from app import create_app
    app = create_app(db_path=str(tmp_path / "x.db"))  # api_token defaults to None
    resp = app.test_client().get("/api/v1/health", headers={"X-Vireo-Token": ""})
    assert resp.status_code == 401


def test_api_v1_version(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    token = app.config["API_TOKEN"]
    resp = client.get("/api/v1/version", headers={"X-Vireo-Token": token})
    assert resp.status_code == 200
    assert "version" in resp.get_json()


def test_api_v1_shutdown_token_only(app_and_db, monkeypatch):
    """Unlike /api/shutdown, /api/v1/shutdown uses the token only (no
    X-Vireo-Shutdown header). The token itself blocks cross-origin attacks
    because browsers cannot set custom headers without CORS preflight."""
    app, _ = app_and_db
    client = app.test_client()
    token = app.config["API_TOKEN"]

    # Replace threading.Timer with a no-op so the scheduled SIGTERM never
    # fires and kills the pytest process after monkeypatch teardown. The
    # endpoint's public contract is the HTTP response; we don't need to
    # verify the actual kill side-effect here.
    import threading as _threading

    class _NoopTimer:
        def __init__(self, *_a, **_kw):
            pass
        def start(self):
            pass

    monkeypatch.setattr(_threading, "Timer", _NoopTimer)

    resp = client.post("/api/v1/shutdown", headers={"X-Vireo-Token": token})
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "shutting_down"
