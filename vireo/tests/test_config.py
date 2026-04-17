import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_darktable_defaults_exist():
    """Config DEFAULTS includes darktable keys."""
    from config import DEFAULTS

    assert "darktable_bin" in DEFAULTS
    assert "darktable_style" in DEFAULTS
    assert "darktable_output_format" in DEFAULTS
    assert "darktable_output_dir" in DEFAULTS


def test_darktable_default_values():
    """Darktable defaults have sensible values."""
    from config import DEFAULTS

    assert DEFAULTS["darktable_bin"] == ""
    assert DEFAULTS["darktable_style"] == ""
    assert DEFAULTS["darktable_output_format"] == "jpg"
    assert DEFAULTS["darktable_output_dir"] == ""


def test_max_edit_history_default():
    """max_edit_history has a default value of 1000."""
    from vireo import config as cfg
    assert cfg.DEFAULTS['max_edit_history'] == 1000


def test_display_defaults_exist():
    """Config DEFAULTS includes display settings."""
    from config import DEFAULTS

    assert DEFAULTS["photos_per_page"] == 50
    assert DEFAULTS["thumbnail_size"] == 400
    assert DEFAULTS["thumbnail_quality"] == 85
    assert DEFAULTS["preview_quality"] == 90
    assert DEFAULTS["browse_thumb_default"] == 220


def test_detection_defaults_exist():
    """Config DEFAULTS includes detection settings."""
    from config import DEFAULTS

    assert DEFAULTS["detector_confidence"] == 0.2
    assert DEFAULTS["detection_padding"] == 0.2
    assert DEFAULTS["top_k_predictions"] == 5
    assert DEFAULTS["redundancy_threshold"] == 0.88


def test_culling_defaults_exist():
    """Config DEFAULTS includes culling settings."""
    from config import DEFAULTS

    assert DEFAULTS["cull_time_window"] == 60
    assert DEFAULTS["cull_phash_threshold"] == 19


def test_pipeline_extract_full_metadata_default():
    """Pipeline config has extract_full_metadata default set to True."""
    from config import DEFAULTS

    assert DEFAULTS["pipeline"]["extract_full_metadata"] is True


def test_pipeline_defaults_exist():
    """Config DEFAULTS includes nested pipeline settings."""
    from config import DEFAULTS

    p = DEFAULTS["pipeline"]
    assert p["w_focus"] == 0.45
    assert p["burst_time_gap"] == 3.0
    assert p["burst_lambda"] == 0.85
    assert p["encounter_lambda"] == 0.70
    assert p["reject_composite"] == 0.40
    assert p["hard_cut_score"] == 0.42
    assert p["merge_score"] == 0.62


def test_ingest_defaults_present():
    """Ingest config section has all required keys with correct defaults."""
    from config import DEFAULTS

    ingest = DEFAULTS["ingest"]
    assert ingest["folder_template"] == "%Y/%Y-%m-%d"
    assert ingest["skip_duplicates"] is True
    assert ingest["file_types"] == "both"


def test_ingest_recent_destinations_default():
    """Ingest config includes empty recent_destinations list by default."""
    from config import DEFAULTS

    assert DEFAULTS["ingest"]["recent_destinations"] == []


def test_working_copy_defaults(tmp_path, monkeypatch):
    """Config includes working copy defaults."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    assert config["working_copy_max_size"] == 4096
    assert config["working_copy_quality"] == 92


def test_preview_cache_max_mb_default():
    """Default config includes preview_cache_max_mb = 2048."""
    import config as cfg
    assert cfg.DEFAULTS["preview_cache_max_mb"] == 2048


def test_load_returns_defaults_when_no_file(tmp_path):
    """load() returns full defaults when config file does not exist."""
    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "nonexistent.json")
    loaded = cfg.load()
    assert loaded == cfg.DEFAULTS


def test_load_falls_back_on_corrupt_file(tmp_path):
    """load() returns defaults when config file contains invalid JSON."""
    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        f.write("not valid json {{{")
    loaded = cfg.load()
    assert loaded == cfg.DEFAULTS


def test_get_and_set_round_trip(tmp_path):
    """get() returns value previously written by set()."""
    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.set("classification_threshold", 0.75)
    assert cfg.get("classification_threshold") == 0.75
    # Other defaults still intact
    assert cfg.get("photos_per_page") == 50


def test_deep_merge_preserves_pipeline(tmp_path):
    """Deep merge correctly handles nested pipeline config."""
    import json

    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    # Save a partial pipeline override
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"pipeline": {"w_focus": 0.60}}, f)
    loaded = cfg.load()
    # Overridden key
    assert loaded["pipeline"]["w_focus"] == 0.60
    # Non-overridden key preserved from defaults
    assert loaded["pipeline"]["burst_time_gap"] == 3.0
    # Top-level defaults preserved
    assert loaded["photos_per_page"] == 50


def test_eye_focus_defaults_exist():
    """Config DEFAULTS includes eye-focus detection tunables."""
    from config import DEFAULTS

    p = DEFAULTS["pipeline"]
    assert p["eye_detect_enabled"] is True
    assert p["eye_classifier_conf_gate"] == 0.50
    assert p["eye_detection_conf_gate"] == 0.50
    assert p["eye_window_k"] == 0.08
    assert p["reject_eye_focus"] == 0.35


def test_eye_focus_config_round_trips_through_settings_api(tmp_path, monkeypatch):
    """Posting eye-focus settings via /api/config persists and reloads.

    Verifies the full wiring from settings.html → /api/config → config.json →
    cfg.load() → get_effective_config → score_encounter sees the new keys.
    """
    import json as _json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    from app import create_app
    db_path = str(tmp_path / "vireo.db")
    thumb_dir = tmp_path / "thumbs"
    thumb_dir.mkdir()
    app = create_app(db_path, str(thumb_dir))
    client = app.test_client()

    # Post a pipeline block that sets a non-default reject_eye_focus.
    r = client.post(
        "/api/config",
        data=_json.dumps({
            "pipeline": {
                "eye_detect_enabled": False,
                "eye_classifier_conf_gate": 0.72,
                "eye_detection_conf_gate": 0.61,
                "eye_window_k": 0.12,
                "reject_eye_focus": 0.55,
            },
        }),
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 200

    loaded = cfg.load()
    p = loaded["pipeline"]
    assert p["eye_detect_enabled"] is False
    assert p["eye_classifier_conf_gate"] == 0.72
    assert p["eye_detection_conf_gate"] == 0.61
    assert p["eye_window_k"] == 0.12
    assert p["reject_eye_focus"] == 0.55


def test_reject_eye_focus_flows_from_config_to_scoring(tmp_path, monkeypatch):
    """A reject_eye_focus value from effective config reaches score_encounter.

    End-to-end: config.json → cfg.load() → db.get_effective_config() → dict
    passed as score_encounter(config=...). Sets the threshold high enough
    that a sharp eye still gets rejected, proving the value took effect.
    """
    import json as _json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    with open(cfg.CONFIG_PATH, "w") as f:
        _json.dump({"pipeline": {"reject_eye_focus": 0.99}}, f)

    from db import Database
    from scoring import score_encounter

    db = Database(str(tmp_path / "vireo.db"))
    effective = db.get_effective_config(cfg.load())
    pipeline_cfg = effective.get("pipeline", {})

    photo = {
        "subject_tenengrad": 50000,
        "eye_tenengrad": 50000,  # "sharp" eye — yet rule still fires at 0.99
        "bg_tenengrad": 50,
        "subject_clip_high": 0.0,
        "subject_clip_low": 0.0,
        "subject_y_median": 115,
        "crop_complete": 0.95,
        "bg_separation": 10.0,
        "subject_size": 0.15,
        "mask_path": "/masks/1.png",
    }
    enc = {"photos": [photo]}
    score_encounter(enc, config=pipeline_cfg)

    assert any(
        "eye_soft" in r for r in photo.get("reject_reasons", [])
    ), (
        "reject_eye_focus from config.json was not applied by score_encounter; "
        f"reasons={photo.get('reject_reasons')}"
    )
