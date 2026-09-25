"""Regression tests for route input validation found by the bug scan.

Each test pins one route that used to store an unusable value, answer a
malformed request with a 500, or leak a private config value.
"""
import json

import pytest


def _write_raw_config(data):
    import config as cfg

    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump(data, f)


# --- /api/config and stored config values -----------------------------------


@pytest.mark.parametrize("body", [
    {"detector_confidence": None},
    {"classification_threshold": "abc"},
    {"photos_per_page": {"n": 1}},
    {"pipeline": 5},
])
def test_api_config_rejects_unusable_values(app_and_db, body):
    import config as cfg

    app, _db = app_and_db
    before = cfg.load()
    resp = app.test_client().post("/api/config", json=body)
    assert resp.status_code == 400
    assert cfg.load() == before


def test_api_config_coerces_valid_values(app_and_db):
    import config as cfg

    app, _db = app_and_db
    resp = app.test_client().post(
        "/api/config", json={"classification_threshold": 0.55},
    )
    assert resp.status_code == 200
    assert cfg.load()["classification_threshold"] == 0.55


def test_stored_bad_numbers_fall_back_to_defaults(app_and_db):
    """A value an older build stored unvalidated no longer breaks Browse."""
    import config as cfg

    app, _db = app_and_db
    _write_raw_config({
        "detector_confidence": None,
        "classification_threshold": "abc",
        "similarity_threshold": "0.7",
    })
    loaded = cfg.load()
    assert loaded["detector_confidence"] == cfg.DEFAULTS["detector_confidence"]
    assert loaded["classification_threshold"] == cfg.DEFAULTS["classification_threshold"]
    assert loaded["similarity_threshold"] == 0.7
    assert app.test_client().get("/api/browse/init").status_code == 200


def test_stored_bad_workspace_override_inherits_global(app_and_db):
    import config as cfg

    app, db = app_and_db
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"detector_confidence": "abc"},
    )
    effective = db.get_effective_config(cfg.load())
    assert effective["detector_confidence"] == cfg.load()["detector_confidence"]
    assert app.test_client().get("/api/browse/init").status_code == 200


# --- /api/workspaces/active/config -------------------------------------------


@pytest.mark.parametrize("body", [
    {"classification_threshold": "abc"},
    {"detector_confidence": {}},
    {"review_min_confidence": 150},
    {"review_min_confidence": "50"},
])
def test_workspace_config_rejects_unusable_values(app_and_db, body):
    app, db = app_and_db
    resp = app.test_client().post("/api/workspaces/active/config", json=body)
    assert resp.status_code == 400
    ws = db.get_workspace(db._active_workspace_id)
    assert not ws["config_overrides"]


def test_workspace_config_accepts_valid_values_and_null_clears(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/workspaces/active/config",
        json={"classification_threshold": 0.6, "review_min_confidence": 40},
    )
    assert resp.status_code == 200
    assert resp.get_json()["overrides"] == {
        "classification_threshold": 0.6, "review_min_confidence": 40,
    }
    resp = client.post(
        "/api/workspaces/active/config", json={"classification_threshold": None},
    )
    assert resp.get_json()["overrides"] == {"review_min_confidence": 40}


def test_workspace_config_round_trips_a_previously_stored_value(app_and_db):
    """Pipeline Review posts the whole override object back with one change.

    A bad value stored before validation existed must not block that save.
    """
    app, db = app_and_db
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"classification_threshold": "abc"},
    )
    resp = app.test_client().post(
        "/api/workspaces/active/config",
        json={"classification_threshold": "abc", "review_min_confidence": 30},
    )
    assert resp.status_code == 200
    assert resp.get_json()["overrides"]["review_min_confidence"] == 30


# --- /api/pipeline/config ------------------------------------------------------


@pytest.mark.parametrize("body", [
    {"proxy_longest_edge": "abc"},
    {"sam2_variant": "sam2-bogus"},
])
def test_pipeline_config_rejects_unusable_values(app_and_db, body):
    app, db = app_and_db
    resp = app.test_client().post("/api/pipeline/config", json=body)
    assert resp.status_code == 400
    ws = db.get_workspace(db._active_workspace_id)
    assert not ws["config_overrides"]


# --- workspace create / rename ----------------------------------------------


def _workspace_count(db):
    return db.conn.execute("SELECT COUNT(*) FROM workspaces").fetchone()[0]


def test_rename_workspace_to_taken_name_is_409(app_and_db):
    app, db = app_and_db
    other = db.create_workspace("Other")
    client = app.test_client()
    resp = client.put(f"/api/workspaces/{other}", json={"name": "Default"})
    assert resp.status_code == 409
    # Keeping its own name is not a conflict.
    resp = client.put(f"/api/workspaces/{other}", json={"name": " Other "})
    assert resp.status_code == 200
    assert db.get_workspace(other)["name"] == "Other"


@pytest.mark.parametrize("name", ["", "   ", 5, None])
def test_rename_workspace_rejects_blank_or_non_string(app_and_db, name):
    app, db = app_and_db
    ws_id = db._active_workspace_id
    resp = app.test_client().put(f"/api/workspaces/{ws_id}", json={"name": name})
    assert resp.status_code == 400
    assert db.get_workspace(ws_id)["name"] == "Default"


def test_create_workspace_with_taken_name_is_409(app_and_db):
    app, db = app_and_db
    before = _workspace_count(db)
    resp = app.test_client().post("/api/workspaces", json={"name": "Default"})
    assert resp.status_code == 409
    assert _workspace_count(db) == before


@pytest.mark.parametrize("folder_ids", [[999999], ["1"], 7])
def test_create_workspace_bad_folder_ids_creates_nothing(app_and_db, folder_ids):
    app, db = app_and_db
    before = _workspace_count(db)
    client = app.test_client()
    for _ in range(2):
        resp = client.post(
            "/api/workspaces", json={"name": "Trip", "folder_ids": folder_ids},
        )
        assert resp.status_code in (400, 404)
    assert _workspace_count(db) == before


# --- non-object JSON bodies --------------------------------------------------


@pytest.mark.parametrize("raw", ['"x"', "[1, 2]", "5", "true"])
def test_non_object_json_body_is_400(app_and_db, raw):
    app, _db = app_and_db
    resp = app.test_client().post(
        "/api/batch/keyword", data=raw, content_type="application/json",
    )
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "json_body_not_object"


def test_object_and_empty_bodies_still_reach_the_route(app_and_db):
    app, _db = app_and_db
    client = app.test_client()
    # The route answers these itself; the hook only refuses non-objects.
    resp = client.post("/api/batch/keyword", json={})
    assert resp.status_code != 500
    assert resp.get_json().get("code") != "json_body_not_object"
    resp = client.post(
        "/api/batch/keyword", data="", content_type="application/json",
    )
    assert resp.status_code != 500


# --- PUT /api/keywords/<id> --------------------------------------------------


def _location_keyword(db, name="Backyard"):
    return db.add_keyword(name, kw_type="location")


def test_update_unknown_keyword_is_404(app_and_db):
    app, _db = app_and_db
    resp = app.test_client().put("/api/keywords/999999", json={"name": "X"})
    assert resp.status_code == 404


@pytest.mark.parametrize("body", [
    {"latitude": "abc"},
    {"longitude": {"x": 1}},
    {"latitude": 91},
    {"longitude": -181},
    {"latitude": True},
    {"name": 5},
    {"taxon_id": "12"},
])
def test_update_keyword_rejects_invalid_fields(app_and_db, body):
    app, db = app_and_db
    kid = _location_keyword(db)
    resp = app.test_client().put(f"/api/keywords/{kid}", json=body)
    assert resp.status_code == 400
    row = db.conn.execute(
        "SELECT name, latitude, longitude FROM keywords WHERE id = ?", (kid,),
    ).fetchone()
    assert tuple(row) == ("Backyard", None, None)


def test_update_keyword_ignores_unknown_fields_and_stores_coordinates(app_and_db):
    app, db = app_and_db
    kid = _location_keyword(db)
    client = app.test_client()
    resp = client.put(
        f"/api/keywords/{kid}",
        json={"keyword_id": 1, "parent_id": 5, "latitude": 38, "longitude": -77.5},
    )
    assert resp.status_code == 200
    row = db.conn.execute(
        "SELECT latitude, longitude, parent_id FROM keywords WHERE id = ?", (kid,),
    ).fetchone()
    assert tuple(row) == (38.0, -77.5, None)
    resp = client.put(
        f"/api/keywords/{kid}", json={"latitude": None, "longitude": None},
    )
    assert resp.status_code == 200
    row = db.conn.execute(
        "SELECT latitude, longitude FROM keywords WHERE id = ?", (kid,),
    ).fetchone()
    assert tuple(row) == (None, None)


# --- /api/report-issue -------------------------------------------------------


def test_report_issue_config_keeps_only_identifier_strings(app_and_db):
    import config as cfg

    app, _db = app_and_db
    current = cfg.load()
    current["report_url"] = ""
    current["remote_targets"] = [{
        "id": "t1", "name": "Home NAS", "host": "nas.home.example",
        "user": "jsmith", "remote_path": "/volume1/photos", "port": 22,
    }]
    current["ingest"] = dict(current.get("ingest") or {})
    current["ingest"]["recent_destinations"] = ["/Volumes/Private/Birds"]
    current["darktable_output_dir"] = "/Users/jsmith/Exports"
    current["darktable_style"] = "My Secret Style"
    current["keyword_case"] = "title"
    cfg.save(current)

    resp = app.test_client().post(
        "/api/report-issue", json={"description": "redaction"},
    )
    diagnostics = resp.get_json()["diagnostics"]
    serialized = json.dumps(diagnostics["config"])
    for private in (
        "nas.home.example", "jsmith", "Home NAS", "/volume1/photos",
        "/Volumes/Private/Birds", "/Users/jsmith/Exports", "My Secret Style",
    ):
        assert private not in serialized, private

    config_in_report = diagnostics["config"]
    # Identifiers and numbers stay so the report is still useful.
    assert config_in_report["keyword_case"] == "title"
    assert config_in_report["remote_targets"][0]["port"] == 22
    assert config_in_report["keyboard_shortcuts"]["browse"]["flag"] == "p"
    assert config_in_report["pipeline"]["sam2_variant"] == (
        current["pipeline"]["sam2_variant"]
    )
    assert config_in_report["classification_threshold"] == (
        current["classification_threshold"]
    )
