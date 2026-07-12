# vireo/tests/test_processes_api.py
"""API tests for the saved-processes CRUD endpoints and the process picker
integration on the settings/workspace surfaces."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Reuse the shared app_and_db fixture from conftest.


def _names(client):
    return [p["name"] for p in client.get("/api/processes").get_json()]


def test_list_returns_seeded_processes(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/processes")
    assert resp.status_code == 200
    names = [p["name"] for p in resp.get_json()]
    assert names == ["Identify birds", "Full", "Cull-ready", "Quick look"]


def test_create_process(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/processes", json={
        "name": "My combo",
        "skip_extract_masks": True,
        "miss_enabled": False,
        "review_mode": "species",
    })
    assert resp.status_code == 200, resp.get_json()
    created = resp.get_json()
    assert created["name"] == "My combo"
    assert created["skip_extract_masks"] is True
    assert created["miss_enabled"] is False
    assert created["review_mode"] == "species"
    assert created["is_seed"] is False
    assert "My combo" in _names(client)


def test_create_requires_name(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/processes", json={"skip_classify": True})
    assert resp.status_code == 400
    assert "name" in resp.get_json()["error"]


def test_create_rejects_duplicate_name(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/processes", json={"name": "Full"})
    assert resp.status_code == 400
    assert "already exists" in resp.get_json()["error"]


def test_create_rejects_non_bool_flag(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/processes", json={
        "name": "Bad", "skip_classify": "yes",
    })
    assert resp.status_code == 400
    assert "boolean" in resp.get_json()["error"]


def test_create_rejects_bad_review_mode(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/processes", json={
        "name": "Bad", "review_mode": "whatever",
    })
    assert resp.status_code == 400
    assert "review_mode" in resp.get_json()["error"]


def test_update_renames_and_edits(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    pid = client.post("/api/processes", json={"name": "Temp"}).get_json()["id"]
    resp = client.put(f"/api/processes/{pid}", json={
        "name": "Renamed", "skip_classify": True,
    })
    assert resp.status_code == 200, resp.get_json()
    updated = resp.get_json()
    assert updated["name"] == "Renamed"
    assert updated["skip_classify"] is True


def test_update_partial_leaves_other_fields(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    pid = client.post("/api/processes", json={
        "name": "Base", "skip_regroup": True, "miss_enabled": False,
    }).get_json()["id"]
    client.put(f"/api/processes/{pid}", json={"name": "Base2"})
    proc = client.get("/api/processes").get_json()
    row = next(p for p in proc if p["id"] == pid)
    assert row["name"] == "Base2"
    assert row["skip_regroup"] is True
    assert row["miss_enabled"] is False


def test_update_missing_404(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.put("/api/processes/999999", json={"name": "x"})
    assert resp.status_code == 404


def test_update_duplicate_name_400(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    pid = client.post("/api/processes", json={"name": "Unique"}).get_json()["id"]
    resp = client.put(f"/api/processes/{pid}", json={"name": "Full"})
    assert resp.status_code == 400


def test_delete_process(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    pid = client.post("/api/processes", json={"name": "Doomed"}).get_json()["id"]
    resp = client.delete(f"/api/processes/{pid}")
    assert resp.status_code == 200
    assert "Doomed" not in _names(client)
    # Second delete 404s.
    assert client.delete(f"/api/processes/{pid}").status_code == 404


def test_delete_nulls_workspace_default(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = client.post("/api/processes", json={"name": "WsDefault"}).get_json()["id"]
    ws_id = db._active_workspace_id
    r = client.patch("/api/settings/workspace", json={
        "key": "pipeline.default_process_id", "value": pid,
    })
    assert r.status_code == 200, r.get_json()
    client.delete(f"/api/processes/{pid}")
    values = client.get("/api/settings/values").get_json()
    assert values["workspace"].get("pipeline.default_process_id") is None


def test_workspace_default_rejects_unknown_id(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.patch("/api/settings/workspace", json={
        "key": "pipeline.default_process_id", "value": 999999,
    })
    assert resp.status_code == 400
    assert "unknown process id" in resp.get_json()["error"]


def test_workspace_default_accepts_valid_id(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_saved_processes()[0]["id"]
    resp = client.patch("/api/settings/workspace", json={
        "key": "pipeline.default_process_id", "value": pid,
    })
    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json()["value"] == pid


def test_global_default_rejects_unknown_id(app_and_db):
    """The global settings PATCH must also validate default_process_id against
    the DB — a stale global default lets workspaces that inherit it hit the
    import endpoints' unknown-process wall and fail to auto-process."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.patch("/api/settings/global", json={
        "key": "pipeline.default_process_id", "value": 999999,
    })
    assert resp.status_code == 400
    assert "unknown process id" in resp.get_json()["error"]


def test_global_default_accepts_valid_id(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_saved_processes()[0]["id"]
    resp = client.patch("/api/settings/global", json={
        "key": "pipeline.default_process_id", "value": pid,
    })
    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json()["value"] == pid


def test_settings_import_rejects_unknown_default_process_id(app_and_db):
    """The settings-import endpoint must apply the same DB check as the
    global PATCH. An imported config carrying an id from another DB (or a
    since-deleted process) would otherwise write through and 400 every
    subsequent import that inherits it — the exact "silent stale default"
    Codex flagged for the PATCH path."""
    import json as _json
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/settings/import", json={
        "json": _json.dumps({"pipeline": {"default_process_id": 999999}}),
    })
    assert resp.status_code == 400
    body = resp.get_json()
    assert "pipeline.default_process_id" in body["errors"]
    assert "unknown process id" in body["errors"]["pipeline.default_process_id"]


def test_settings_import_accepts_valid_default_process_id(app_and_db):
    """Round trip for a valid id through the import endpoint."""
    import json as _json

    import config as cfg
    app, db = app_and_db
    pid = db.get_saved_processes()[0]["id"]
    client = app.test_client()
    resp = client.post("/api/settings/import", json={
        "json": _json.dumps({"pipeline": {"default_process_id": pid}}),
    })
    assert resp.status_code == 200
    assert cfg.load()["pipeline"]["default_process_id"] == pid


def test_settings_import_accepts_null_default_process_id(app_and_db):
    """Null (import only) short-circuits the DB check — the schema still
    accepts it as a nullable int and the runtime never dereferences None."""
    import json as _json

    import config as cfg
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/settings/import", json={
        "json": _json.dumps({"pipeline": {"default_process_id": None}}),
    })
    assert resp.status_code == 200
    assert cfg.load()["pipeline"]["default_process_id"] is None


def test_api_config_rejects_unknown_default_process_id(app_and_db):
    """The legacy /api/config POST deep-merges the pipeline block, so it must
    apply the same saved-process existence check as the settings PATCH/import
    paths — a stale global default here breaks every inheriting import."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/config", json={
        "pipeline": {"default_process_id": 999999},
    })
    assert resp.status_code == 400
    assert "unknown process id" in resp.get_json()["error"]


def test_api_config_accepts_valid_default_process_id(app_and_db):
    import config as cfg
    app, db = app_and_db
    pid = db.get_saved_processes()[0]["id"]
    client = app.test_client()
    resp = client.post("/api/config", json={
        "pipeline": {"default_process_id": pid},
    })
    assert resp.status_code == 200, resp.get_json()
    assert cfg.load()["pipeline"]["default_process_id"] == pid


def test_delete_process_does_not_fossilize_defaults(app_and_db):
    """Deleting the globally-defaulted process must NOT rewrite the raw
    config with the fully deep-merged DEFAULTS.

    Using ``cfg.load()`` + ``cfg.save()`` in the delete handler would turn
    every current DEFAULTS value into an explicit user override, so a
    future change to DEFAULTS would be invisible on this install. Only the
    ``default_process_id`` entry should change on disk; every other key
    the user never set stays absent from the raw file.
    """
    import json as _json

    import config as cfg

    app, _ = app_and_db
    client = app.test_client()
    pid = client.post(
        "/api/processes", json={"name": "GlobalDefault"},
    ).get_json()["id"]
    r = client.patch("/api/settings/global", json={
        "key": "pipeline.default_process_id", "value": pid,
    })
    assert r.status_code == 200, r.get_json()

    with open(cfg.CONFIG_PATH) as f:
        before = _json.load(f)
    # Sanity: only the one leaf under ``pipeline``, not every DEFAULT.
    assert before["pipeline"]["default_process_id"] == pid
    assert {k for k in before["pipeline"] if k != "default_process_id"} == set()

    # Delete the process; the app-wide default must clear.
    client.delete(f"/api/processes/{pid}")

    with open(cfg.CONFIG_PATH) as f:
        after = _json.load(f)
    assert after["pipeline"]["default_process_id"] is None
    # Nothing else the user never set should have leaked in from DEFAULTS.
    assert {k for k in after["pipeline"] if k != "default_process_id"} == set()


def test_settings_schema_injects_process_picker(app_and_db):
    """The settings widget renders as a picker: api_settings_schema swaps the
    stored int spec for an enum populated from the live process list.

    Enum values are numeric ids (not stringified) so the settings renderer,
    which selects the active option via strict equality against the effective
    value, matches when the stored ``default_process_id`` is an ``int``.
    Label keys survive JSON serialization as strings regardless, so lookups
    on the wire use ``str(id)``.
    """
    app, db = app_and_db
    client = app.test_client()
    schema = client.get("/api/settings/schema").get_json()["schema"]
    spec = schema["pipeline.default_process_id"]
    assert spec["type"] == "enum"
    ids = {p["id"] for p in db.get_saved_processes()}
    assert set(spec["enum"]) == ids
    assert all(isinstance(x, int) for x in spec["enum"])
    identify = next(
        p for p in db.get_saved_processes() if p["name"] == "Identify birds")
    assert spec["enum_labels"][str(identify["id"])] == "Identify birds"


def test_settings_schema_enum_matches_effective_default_by_strict_equality(
    app_and_db
):
    """Once a workspace default is set, the effective value and the picker's
    enum entry must be the *same* type: the settings-page widget selects the
    active option via strict equality, so a mismatch would silently drop
    through to ``(unset)`` and misrepresent an active default.
    """
    app, db = app_and_db
    client = app.test_client()

    pid = next(
        p["id"] for p in db.get_saved_processes()
        if p["name"] == "Identify birds"
    )
    resp = client.patch("/api/settings/workspace", json={
        "key": "pipeline.default_process_id", "value": pid,
    })
    assert resp.status_code == 200, resp.get_json()

    schema = client.get("/api/settings/schema").get_json()["schema"]
    values = client.get("/api/settings/values").get_json()
    effective = values["effective"]["pipeline.default_process_id"]
    enum = schema["pipeline.default_process_id"]["enum"]

    # The mirror of what settings.html:renderSettingWidget does: opt === effective.
    matching = [opt for opt in enum if opt == effective and type(opt) is type(effective)]
    assert matching == [effective]


def test_create_workspace_rejects_unknown_default_process(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/workspaces", json={
        "name": "Bad WS",
        "config_overrides": {"pipeline": {"default_process_id": 999999}},
    })
    assert resp.status_code == 400
    assert "unknown process id" in resp.get_json()["error"]
