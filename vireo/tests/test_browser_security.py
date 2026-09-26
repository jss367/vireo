"""Security contract for Vireo's localhost browser surface."""


def _enable_browser_auth(app):
    app.config["BROWSER_AUTH_ENABLED"] = True


def test_internal_api_requires_browser_session(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)

    response = app.test_client().get("/api/folders")

    assert response.status_code == 401
    assert response.get_json()["code"] == "browser_session_required"
    assert response.headers["X-Request-ID"]


def test_html_navigation_establishes_strict_browser_session(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)
    client = app.test_client()

    page = client.get("/browse")
    api = client.get("/api/folders")

    assert page.status_code == 200
    cookie = page.headers["Set-Cookie"]
    assert "HttpOnly" in cookie
    assert "SameSite=Strict" in cookie
    assert api.status_code == 200


def test_unsafe_internal_api_requires_browser_header(app_and_db):
    app, db = app_and_db
    _enable_browser_auth(app)
    client = app.test_client()
    client.get("/browse")
    photo_id = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 1").fetchone()[0]

    blocked = client.post(f"/api/photos/{photo_id}/rating", json={"rating": 4})
    allowed = client.post(
        f"/api/photos/{photo_id}/rating",
        json={"rating": 4},
        headers={"X-Vireo-Client": "browser"},
    )

    assert blocked.status_code == 403
    assert blocked.get_json()["code"] == "browser_header_required"
    assert allowed.status_code == 200


def test_cross_origin_request_is_rejected_even_with_session(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)
    client = app.test_client()
    client.get("/browse")

    response = client.get(
        "/api/folders",
        headers={"Origin": "https://attacker.example"},
    )

    assert response.status_code == 403
    assert response.get_json()["code"] == "cross_origin_request"


def test_same_site_request_from_another_localhost_port_is_rejected(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)
    client = app.test_client()
    client.get("/browse")

    response = client.get(
        "/api/folders",
        headers={"Sec-Fetch-Site": "same-site"},
    )

    assert response.status_code == 403
    assert response.get_json()["code"] == "cross_site_request"


def test_same_origin_request_with_session_is_allowed(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)
    client = app.test_client()
    client.get("/browse")

    response = client.get(
        "/api/folders",
        headers={"Sec-Fetch-Site": "same-origin"},
    )

    assert response.status_code == 200


def test_v1_token_api_does_not_require_browser_session(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)

    response = app.test_client().get(
        "/api/v1/photos",
        headers={"X-Vireo-Token": "test-token-123"},
    )

    assert response.status_code == 200


def test_native_token_can_access_internal_api_without_browser_session(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)

    response = app.test_client().get(
        "/api/jobs",
        headers={"X-Vireo-Token": "test-token-123"},
    )

    assert response.status_code == 200


def test_invalid_native_token_does_not_bypass_browser_session(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)

    response = app.test_client().get(
        "/api/jobs",
        headers={"X-Vireo-Token": "wrong-token"},
    )

    assert response.status_code == 401
    assert response.get_json()["code"] == "browser_session_required"


def test_photo_media_requires_browser_session(app_and_db):
    app, db = app_and_db
    _enable_browser_auth(app)
    photo_id = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 1").fetchone()[0]

    media_path = f"/thumbnails/{photo_id}.jpg"
    blocked = app.test_client().get(media_path)
    client = app.test_client()
    client.get("/browse")
    allowed = client.get(media_path)

    assert blocked.status_code == 401
    assert allowed.status_code == 200


def test_security_headers_are_present_on_html(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)

    response = app.test_client().get("/browse")

    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert "frame-ancestors 'none'" in response.headers[
        "Content-Security-Policy-Report-Only"
    ]


def test_request_ids_are_validated_and_error_codes_are_stable(app_and_db):
    app, _ = app_and_db
    _enable_browser_auth(app)

    response = app.test_client().get(
        "/api/folders",
        headers={"X-Request-ID": "invalid request id"},
    )

    body = response.get_json()
    assert response.status_code == 401
    assert body["code"] == "browser_session_required"
    assert body["request_id"] != "invalid request id"
    assert response.headers["X-Request-ID"] == body["request_id"]


def test_app_hooks_run_in_security_order(app_and_db):
    """``web.app_hooks`` registers the app-wide hooks in one call. Flask runs
    ``before_request`` hooks in registration order and stops at the first
    response, so the browser guard and the v1 token check must precede the
    workspace mutation reservation (a refused request takes no reservation),
    and the request id must be stamped before any of them can answer."""
    app, _ = app_and_db

    def names(funcs):
        return [f.__name__ for f in funcs]

    assert names(app.before_request_funcs[None]) == [
        "_start_timer",
        "_protect_browser_surface",
        "_enforce_api_v1_token",
        "_reject_non_object_json_body",
        "_reserve_workspace_mutation",
    ]
    assert names(app.after_request_funcs[None]) == ["_log_requests"]
    assert names(app.teardown_request_funcs[None]) == ["_release_workspace_mutation"]
    assert names(app.teardown_appcontext_funcs) == ["close_request_db"]
