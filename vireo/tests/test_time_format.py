"""The display preference persists through both settings APIs and page boot."""

import pytest


@pytest.mark.parametrize("value", ["system", "12h", "24h"])
@pytest.mark.parametrize("endpoint", ["/api/config", "/api/settings/global"])
def test_time_format_persists_and_bootstraps(app_and_db, endpoint, value):
    app, _ = app_and_db
    client = app.test_client()
    assert client.get("/api/config").json["time_format"] == "system"
    if endpoint == "/api/config":
        response = client.post(endpoint, json={"time_format": value})
    else:
        response = client.patch(endpoint, json={"key": "time_format", "value": value})
    assert response.status_code == 200
    assert client.get("/api/config").json["time_format"] == value
    boot = client.get("/config-defaults.js")
    assert f'window.VIREO_TIME_FORMAT = "{value}";' in boot.text
    assert boot.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("endpoint", ["/api/config", "/api/settings/global"])
def test_invalid_time_format_does_not_replace_saved_preference(app_and_db, endpoint):
    app, _ = app_and_db
    client = app.test_client()
    assert client.post("/api/config", json={"time_format": "24h"}).status_code == 200
    if endpoint == "/api/config":
        response = client.post(endpoint, json={"time_format": "invalid"})
    else:
        response = client.patch(endpoint, json={"key": "time_format", "value": "invalid"})
    assert response.status_code == 400
    assert client.get("/api/config").json["time_format"] == "24h"


def test_reset_time_format_restores_system_default(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    client.post("/api/config", json={"time_format": "12h"})
    assert client.delete("/api/settings/global/time_format").status_code == 200
    assert client.get("/api/config").json["time_format"] == "system"
