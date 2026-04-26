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


def test_get_values_global_excludes_keys_equal_to_default(app_and_db):
    """Legacy save paths write the entire DEFAULTS dict to disk; a value matching
    the default must not appear as a "globally set" override."""
    app, _ = app_and_db
    import config as cfg

    # cfg.save(cfg.load()) is what the legacy POST /api/config does — it
    # produces a file containing every default key.
    cfg.save(cfg.load())
    client = app.test_client()
    values = client.get("/api/settings/values").get_json()
    assert values["global"] == {}


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


# ---------------------------------------------------------------------------
# PATCH /api/settings/global
# ---------------------------------------------------------------------------


def test_patch_global_persists_value(app_and_db):
    app, _ = app_and_db
    import config as cfg

    client = app.test_client()
    resp = client.patch(
        "/api/settings/global",
        json={"key": "classification_threshold", "value": 0.65},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["value"] == 0.65
    # Persisted to disk.
    assert cfg.load()["classification_threshold"] == 0.65


def test_patch_global_coerces_string_to_float(app_and_db):
    app, _ = app_and_db
    import config as cfg

    client = app.test_client()
    resp = client.patch(
        "/api/settings/global",
        json={"key": "classification_threshold", "value": "0.42"},
    )
    assert resp.status_code == 200
    assert cfg.load()["classification_threshold"] == 0.42


def test_patch_global_writes_nested_key(app_and_db):
    app, _ = app_and_db
    import config as cfg

    client = app.test_client()
    resp = client.patch(
        "/api/settings/global",
        json={"key": "pipeline.w_focus", "value": 0.5},
    )
    assert resp.status_code == 200
    assert cfg.load()["pipeline"]["w_focus"] == 0.5


def test_patch_global_rejects_unknown_key(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.patch(
        "/api/settings/global",
        json={"key": "no_such_key", "value": 1},
    )
    assert resp.status_code == 400


def test_patch_global_rejects_out_of_range(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.patch(
        "/api/settings/global",
        json={"key": "classification_threshold", "value": 5.0},
    )
    assert resp.status_code == 400


def test_patch_global_rejects_unknown_enum(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.patch(
        "/api/settings/global",
        json={"key": "keyword_case", "value": "screaming-snake"},
    )
    assert resp.status_code == 400


def test_patch_global_rejects_type_mismatch(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.patch(
        "/api/settings/global",
        json={"key": "photos_per_page", "value": "not-a-number"},
    )
    assert resp.status_code == 400


def test_patch_global_updates_hf_token_env(app_and_db, monkeypatch):
    """Setting hf_token also pushes it into HF_TOKEN env var (legacy POST behavior)."""
    app, _ = app_and_db
    monkeypatch.delenv("HF_TOKEN", raising=False)
    client = app.test_client()
    client.patch(
        "/api/settings/global",
        json={"key": "hf_token", "value": "hf_test_xyz"},
    )
    assert os.environ.get("HF_TOKEN") == "hf_test_xyz"


def test_patch_global_clears_hf_token_env(app_and_db, monkeypatch):
    app, _ = app_and_db
    monkeypatch.setenv("HF_TOKEN", "previous")
    import config as cfg

    cfg.set("hf_token", "previous")
    client = app.test_client()
    client.patch(
        "/api/settings/global",
        json={"key": "hf_token", "value": ""},
    )
    assert "HF_TOKEN" not in os.environ


# ---------------------------------------------------------------------------
# DELETE /api/settings/global/<dotted-key>
# ---------------------------------------------------------------------------


def test_delete_global_reverts_to_default(app_and_db):
    app, _ = app_and_db
    import config as cfg

    cfg.set("classification_threshold", 0.65)
    assert cfg.load()["classification_threshold"] == 0.65

    client = app.test_client()
    resp = client.delete("/api/settings/global/classification_threshold")
    assert resp.status_code == 200
    # Reverted to the DEFAULTS value (deep-merge fills it back in).
    assert cfg.load()["classification_threshold"] == cfg.DEFAULTS["classification_threshold"]


def test_delete_global_nested_key(app_and_db):
    app, _ = app_and_db
    import config as cfg

    client = app.test_client()
    client.patch("/api/settings/global", json={"key": "pipeline.w_focus", "value": 0.99})
    assert cfg.load()["pipeline"]["w_focus"] == 0.99

    resp = client.delete("/api/settings/global/pipeline.w_focus")
    assert resp.status_code == 200
    assert cfg.load()["pipeline"]["w_focus"] == cfg.DEFAULTS["pipeline"]["w_focus"]


def test_delete_global_idempotent(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    # Delete a key that has no override — should not error.
    resp = client.delete("/api/settings/global/classification_threshold")
    assert resp.status_code == 200


def test_delete_global_rejects_unknown_key(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.delete("/api/settings/global/no_such_key")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# PATCH /api/settings/workspace
# ---------------------------------------------------------------------------


def _ws_overrides(db):
    """Return the active workspace's config_overrides as a dict."""
    import json as _json

    ws = db.get_workspace(db._active_workspace_id)
    if not ws or not ws["config_overrides"]:
        return {}
    raw = ws["config_overrides"]
    return _json.loads(raw) if isinstance(raw, str) else raw


def test_patch_workspace_persists_value(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    resp = client.patch(
        "/api/settings/workspace",
        json={"key": "classification_threshold", "value": 0.55},
    )
    assert resp.status_code == 200
    overrides = _ws_overrides(db)
    assert overrides.get("classification_threshold") == 0.55


def test_patch_workspace_writes_nested_key(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    resp = client.patch(
        "/api/settings/workspace",
        json={"key": "pipeline.w_focus", "value": 0.7},
    )
    assert resp.status_code == 200
    overrides = _ws_overrides(db)
    assert overrides.get("pipeline", {}).get("w_focus") == 0.7


def test_patch_workspace_rejects_global_scope_key(app_and_db):
    """hf_token has scope='global' — cannot be overridden per-workspace."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.patch(
        "/api/settings/workspace",
        json={"key": "hf_token", "value": "hf_xxx"},
    )
    assert resp.status_code == 400
    assert "hf_token" not in _ws_overrides(db)


def test_patch_workspace_rejects_unknown_key(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.patch(
        "/api/settings/workspace",
        json={"key": "no_such", "value": 1},
    )
    assert resp.status_code == 400


def test_patch_workspace_rejects_out_of_range(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.patch(
        "/api/settings/workspace",
        json={"key": "classification_threshold", "value": 99},
    )
    assert resp.status_code == 400


def test_patch_workspace_preserves_active_labels(app_and_db):
    """Existing non-schema keys (e.g. active_labels) survive a schema-driven write."""
    app, db = app_and_db
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"active_labels": ["birds.txt"]},
    )
    client = app.test_client()
    client.patch(
        "/api/settings/workspace",
        json={"key": "classification_threshold", "value": 0.55},
    )
    overrides = _ws_overrides(db)
    assert overrides.get("active_labels") == ["birds.txt"]
    assert overrides.get("classification_threshold") == 0.55


def test_patch_workspace_does_not_affect_other_workspaces(app_and_db):
    app, db = app_and_db
    other_ws = db.create_workspace("other")
    client = app.test_client()
    client.patch(
        "/api/settings/workspace",
        json={"key": "classification_threshold", "value": 0.55},
    )
    other = db.get_workspace(other_ws)
    assert not other["config_overrides"]


def test_patch_workspace_value_beats_global_in_effective(app_and_db):
    app, db = app_and_db
    import config as cfg

    cfg.set("classification_threshold", 0.5)  # global
    client = app.test_client()
    client.patch(
        "/api/settings/workspace",
        json={"key": "classification_threshold", "value": 0.95},
    )
    values = client.get("/api/settings/values").get_json()
    assert values["effective"]["classification_threshold"] == 0.95
    assert values["workspace"]["classification_threshold"] == 0.95
    assert values["global"]["classification_threshold"] == 0.5


# ---------------------------------------------------------------------------
# DELETE /api/settings/workspace/<dotted-key>
# ---------------------------------------------------------------------------


def test_delete_workspace_removes_override(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.patch(
        "/api/settings/workspace",
        json={"key": "classification_threshold", "value": 0.55},
    )
    assert "classification_threshold" in _ws_overrides(db)
    resp = client.delete("/api/settings/workspace/classification_threshold")
    assert resp.status_code == 200
    assert "classification_threshold" not in _ws_overrides(db)


def test_delete_workspace_nested_key(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.patch(
        "/api/settings/workspace",
        json={"key": "pipeline.w_focus", "value": 0.99},
    )
    assert _ws_overrides(db).get("pipeline", {}).get("w_focus") == 0.99
    resp = client.delete("/api/settings/workspace/pipeline.w_focus")
    assert resp.status_code == 200
    assert "w_focus" not in _ws_overrides(db).get("pipeline", {})


def test_delete_workspace_idempotent(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.delete("/api/settings/workspace/classification_threshold")
    assert resp.status_code == 200


def test_delete_workspace_rejects_unknown_key(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.delete("/api/settings/workspace/no_such_key")
    assert resp.status_code == 400


def test_delete_workspace_preserves_active_labels(app_and_db):
    app, db = app_and_db
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={
            "active_labels": ["birds.txt"],
            "classification_threshold": 0.55,
        },
    )
    client = app.test_client()
    resp = client.delete("/api/settings/workspace/classification_threshold")
    assert resp.status_code == 200
    overrides = _ws_overrides(db)
    assert overrides.get("active_labels") == ["birds.txt"]
    assert "classification_threshold" not in overrides
