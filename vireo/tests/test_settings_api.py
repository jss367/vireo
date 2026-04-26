"""Tests for /api/settings/* endpoints (schema-driven settings UI)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# GET /api/settings/schema
# ---------------------------------------------------------------------------


def test_get_schema_returns_schema_and_categories(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/settings/schema")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "schema" in data
    assert "categories" in data
    assert isinstance(data["schema"], dict)
    assert isinstance(data["categories"], list)
    # Sample-check a couple of well-known keys.
    assert "classification_threshold" in data["schema"]
    assert "pipeline.w_focus" in data["schema"]


def test_get_schema_entries_have_required_fields(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/settings/schema")
    schema = resp.get_json()["schema"]
    for key, spec in schema.items():
        for required in ("type", "category", "scope", "label", "desc"):
            assert required in spec, f"{key} missing {required!r} in API response"


# ---------------------------------------------------------------------------
# GET /api/settings/values
# ---------------------------------------------------------------------------


def test_get_values_returns_four_layers(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/settings/values")
    assert resp.status_code == 200
    data = resp.get_json()
    for layer in ("default", "global", "workspace", "effective"):
        assert layer in data, f"missing layer {layer!r}"


def test_get_values_default_layer_covers_schema(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    schema = client.get("/api/settings/schema").get_json()["schema"]
    values = client.get("/api/settings/values").get_json()
    # Every schema key has a default value.
    assert set(schema.keys()) <= set(values["default"].keys())


def test_get_values_global_layer_empty_when_no_overrides(app_and_db):
    """With no config.json written, the global layer is empty (only DEFAULTS apply)."""
    app, _ = app_and_db
    client = app.test_client()
    values = client.get("/api/settings/values").get_json()
    assert values["global"] == {}


def test_get_values_global_reflects_written_file(app_and_db, tmp_path):
    """After writing a value to the config file, it appears in the global layer."""
    app, _ = app_and_db
    import config as cfg

    cfg.set("classification_threshold", 0.7)
    client = app.test_client()
    values = client.get("/api/settings/values").get_json()
    assert values["global"].get("classification_threshold") == 0.7
    # Effective reflects the override too.
    assert values["effective"].get("classification_threshold") == 0.7


def test_get_values_global_only_includes_schema_known_keys(app_and_db):
    """Hand-edited unknown keys in the file are not surfaced in the global layer."""
    app, _ = app_and_db
    import config as cfg

    raw = cfg.load()
    raw["bogus_legacy_key"] = "should-not-leak-into-global-layer"
    cfg.save(raw)

    client = app.test_client()
    values = client.get("/api/settings/values").get_json()
    assert "bogus_legacy_key" not in values["global"]


def test_get_values_workspace_empty_when_no_overrides(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    values = client.get("/api/settings/values").get_json()
    assert values["workspace"] == {}


def test_get_values_workspace_reflects_overrides(app_and_db):
    app, db = app_and_db
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"classification_threshold": 0.9},
    )
    client = app.test_client()
    values = client.get("/api/settings/values").get_json()
    assert values["workspace"].get("classification_threshold") == 0.9
    assert values["effective"].get("classification_threshold") == 0.9


def test_get_values_effective_falls_through_layers(app_and_db):
    """Effective = workspace > global > default."""
    app, db = app_and_db
    import config as cfg

    # default for similarity_threshold is 0.85
    values = client_get(app, "/api/settings/values")
    assert values["effective"]["similarity_threshold"] == 0.85

    # Override globally.
    cfg.set("similarity_threshold", 0.5)
    values = client_get(app, "/api/settings/values")
    assert values["effective"]["similarity_threshold"] == 0.5
    assert values["global"]["similarity_threshold"] == 0.5

    # Override per-workspace — wins over global.
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"similarity_threshold": 0.95},
    )
    values = client_get(app, "/api/settings/values")
    assert values["effective"]["similarity_threshold"] == 0.95


def test_get_values_workspace_excludes_non_schema_keys(app_and_db):
    """Internal keys stored in config_overrides (e.g. active_labels) are not exposed."""
    app, db = app_and_db
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"active_labels": ["birds.txt"], "classification_threshold": 0.6},
    )
    client = app.test_client()
    values = client.get("/api/settings/values").get_json()
    assert "active_labels" not in values["workspace"]
    assert values["workspace"].get("classification_threshold") == 0.6


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def client_get(app, path):
    return app.test_client().get(path).get_json()
