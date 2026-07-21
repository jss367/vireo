import json
import os
import sys

import pytest

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
    assert DEFAULTS["darktable_auto_convert_dng"] is True


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
    assert p["burst_embedding_threshold"] == 0.40
    assert p["burst_lambda"] == 0.85
    assert p["encounter_lambda"] == 0.70
    assert p["reject_composite"] == 0.40
    assert p["hard_cut_score"] == 0.42
    assert p["merge_score"] == 0.62
    assert p["sam2_variant"] == "sam2-small"
    assert p["dinov2_variant"] == "vit-b14"
    assert p["proxy_longest_edge"] == 1536


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


def test_keyboard_shortcut_defaults_include_rapid_review():
    """Rapid Review owns P/X/Z locally instead of relying on global nav."""
    from config import DEFAULTS

    shortcuts = DEFAULTS["keyboard_shortcuts"]
    assert shortcuts["navigation"]["pipeline"] == ""
    assert shortcuts["pipeline_rapid_review"]["pick"] == "p"
    assert shortcuts["pipeline_rapid_review"]["reject"] == "x"
    assert shortcuts["pipeline_rapid_review"]["zoom"] == "z"
    assert shortcuts["browse"]["compare"] == "c"
    assert shortcuts["browse"]["toggle_ui"] == "h"


def test_working_copy_defaults(tmp_path, monkeypatch):
    """Config includes working copy defaults."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    assert config["working_copy_max_size"] == 4096
    assert config["working_copy_quality"] == 92


def test_preview_cache_max_mb_default():
    """Default config includes preview_cache_max_mb = 20480 (20 GB).

    A 2 GB cap (the prior default) only fits ~5,000 previews at 1920px,
    so a single 22k-photo workspace would re-decode every preview from
    RAW on every pipeline run. 20 GB comfortably covers a typical
    library."""
    import config as cfg
    assert cfg.DEFAULTS["preview_cache_max_mb"] == 20480


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


def test_load_preserves_user_pipeline_p_shortcut(tmp_path):
    """A saved Pipeline=P binding is treated as user config, not rewritten."""
    import json

    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"keyboard_shortcuts": {"navigation": {"pipeline": "p"}}}, f)

    loaded = cfg.load()

    assert loaded["keyboard_shortcuts"]["navigation"]["pipeline"] == "p"
    assert loaded["keyboard_shortcuts"]["pipeline_rapid_review"]["pick"] == "p"


def test_conftest_autouse_fixture_restores_cfg_path(tmp_path):
    """Lock in the conftest autouse fixture that restores ``cfg.CONFIG_PATH``
    between tests. Runs a sub-pytest in a subprocess so the two halves of
    the regression (a leak test, then a verify test) execute serially
    regardless of how the parent ``-n auto`` xdist run distributes them.

    Without this subprocess wrapper, with xdist's default ``--dist=load``
    scheduling the leak/verify pair could land on different workers and the
    verify half would pass trivially even if restoration was broken —
    silently weakening the guard.

    If the conftest autouse fixture is removed, the inner ``test_z_verify``
    fails and so does this test. If you're tempted to "fix" this by
    skipping it, fix the conftest fixture instead — the underlying flake
    shipped as PR #722's CI failure.
    """
    import subprocess
    import sys
    import textwrap

    repo_tests_dir = os.path.dirname(__file__)
    repo_root = os.path.abspath(os.path.join(repo_tests_dir, "..", ".."))

    # Standalone sub-pytest test dir: a conftest that mirrors the production
    # autouse fixture (snapshotting cfg.CONFIG_PATH and restoring it on
    # teardown), and a single test file with two ordered tests.
    sub_dir = tmp_path / "subpytest"
    sub_dir.mkdir()
    (sub_dir / "conftest.py").write_text(textwrap.dedent(f"""
        import os, sys
        sys.path.insert(0, {os.path.join(repo_root, "vireo")!r})
        import pytest

        @pytest.fixture(autouse=True)
        def _restore_cfg_path():
            import config as cfg
            original = cfg.CONFIG_PATH
            yield
            cfg.CONFIG_PATH = original
    """))
    (sub_dir / "test_leak_then_verify.py").write_text(textwrap.dedent("""
        # Names chosen so default collection order runs leak before verify.
        def test_a_leak(tmp_path):
            import config as cfg
            cfg.CONFIG_PATH = str(tmp_path / "leaked.json")
            cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1234})
            assert cfg.load()["working_copy_max_size"] == 1234

        def test_z_verify():
            import config as cfg
            assert (
                cfg.load()["working_copy_max_size"]
                == cfg.DEFAULTS["working_copy_max_size"]
            ), (
                f"cfg.CONFIG_PATH leaked from prior test: "
                f"working_copy_max_size={cfg.load()['working_copy_max_size']!r}, "
                f"CONFIG_PATH={cfg.CONFIG_PATH!r}"
            )
    """))

    # Isolate the sub-pytest from the parent's env: drop any inherited
    # PYTEST_ADDOPTS/PYTEST_CURRENT_TEST and route TMP/TEMP into a fresh
    # subdir so the nested pytest's own tmp_path_factory doesn't see the
    # parent's shared Temp tree.
    child_tmp = tmp_path / "subpytest-tmp"
    child_tmp.mkdir()
    child_env = os.environ.copy()
    child_env.pop("PYTEST_ADDOPTS", None)
    child_env.pop("PYTEST_CURRENT_TEST", None)
    child_env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    child_env["TMP"] = str(child_tmp)
    child_env["TEMP"] = str(child_tmp)
    # ``--rootdir`` + ``--confcutdir`` pin the sub-pytest's collection scope
    # to ``sub_dir``. Without them, pytest walks ``argpath.parents`` up to the
    # drive root, and on Windows runners the parent walk through the shared
    # ``Temp\`` directory triggers ``Session._collect_path`` to iterate sibling
    # entries — when a sibling like ``Temp\firefox`` (left by an unrelated
    # process) is removed between scandir and pytest's Windows
    # ``samefile_nofollow`` lstat, collection dies with a spurious
    # ``FileNotFoundError`` and this test fails for reasons unrelated to the
    # autouse fixture under test. The TMP/TEMP override above doesn't help
    # here because ``sub_dir`` still lives under the parent's Temp.
    result = subprocess.run(
        [sys.executable, "-m", "pytest", str(sub_dir), "-q",
         "-p", "no:cacheprovider", "-p", "no:xdist", "--no-header",
         "--rootdir", str(sub_dir), "--confcutdir", str(sub_dir)],
        capture_output=True, text=True, timeout=60, env=child_env,
    )
    assert result.returncode == 0, (
        f"sub-pytest failed (exit {result.returncode}):\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )


def test_eye_focus_defaults_exist():
    """Config DEFAULTS includes eye-focus detection tunables."""
    from config import DEFAULTS

    p = DEFAULTS["pipeline"]
    assert p["eye_detect_enabled"] is False
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


def test_compare_shortcut_round_trips_through_config_api(tmp_path, monkeypatch):
    """The Browse compare shortcut must be in backend defaults or POST validation drops it."""
    import json as _json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    from app import create_app
    db_path = str(tmp_path / "vireo.db")
    thumb_dir = tmp_path / "thumbs"
    thumb_dir.mkdir()
    app = create_app(db_path, str(thumb_dir))
    client = app.test_client()

    r = client.post(
        "/api/config",
        data=_json.dumps({
            "keyboard_shortcuts": {
                "browse": {
                    "compare": "shift+c",
                },
            },
        }),
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 200

    loaded = cfg.load()
    assert loaded["keyboard_shortcuts"]["browse"]["compare"] == "shift+c"


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
        _json.dump(
            {"pipeline": {"eye_detect_enabled": True, "reject_eye_focus": 0.99}},
            f,
        )

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


def test_miss_defaults_present():
    import config as cfg
    d = cfg.DEFAULTS["pipeline"]
    assert d["miss_enabled"] is True
    assert d["miss_det_confidence"] == 0.20
    assert d["miss_det_confidence_burst"] == 0.12
    assert d["miss_bbox_area_min"] == 0.005
    assert d["miss_bbox_area_min_singleton"] == 0.002
    assert d["miss_oof_ratio"] == 0.5


def _write_raw(path, data):
    import json as _json
    with open(path, "w") as f:
        _json.dump(data, f)


def _read_raw(path):
    import json as _json
    with open(path) as f:
        return _json.load(f)


def test_migrate_legacy_miss_thresholds_rewrites_exact_pair(tmp_path, monkeypatch):
    """An install with the previous default pair persisted gets rewritten."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "pipeline": {
            "miss_det_confidence": 0.25,
            "miss_det_confidence_burst": 0.15,
        },
    })

    assert cfg.migrate_legacy_miss_thresholds() is True
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["miss_det_confidence"] == 0.20
    assert raw["pipeline"]["miss_det_confidence_burst"] == 0.12
    assert cfg.MIGRATION_MISS_THRESHOLDS in raw["_migrations_applied"]


def test_migrate_legacy_miss_thresholds_preserves_customized(tmp_path, monkeypatch):
    """A user who set a non-default pair (e.g. 0.30/0.18) is left alone."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "pipeline": {
            "miss_det_confidence": 0.30,
            "miss_det_confidence_burst": 0.18,
        },
    })

    cfg.migrate_legacy_miss_thresholds()
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["miss_det_confidence"] == 0.30
    assert raw["pipeline"]["miss_det_confidence_burst"] == 0.18
    assert cfg.MIGRATION_MISS_THRESHOLDS in raw["_migrations_applied"]


def test_migrate_legacy_miss_thresholds_skips_partial_pair(tmp_path, monkeypatch):
    """If only the singleton matches the legacy default but the burst is
    custom (or missing), neither is rewritten — the exact pair gate is what
    distinguishes 'never touched defaults' from 'partial customization'."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "pipeline": {
            "miss_det_confidence": 0.25,
            "miss_det_confidence_burst": 0.10,
        },
    })

    cfg.migrate_legacy_miss_thresholds()
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["miss_det_confidence"] == 0.25
    assert raw["pipeline"]["miss_det_confidence_burst"] == 0.10


def test_migrate_legacy_miss_thresholds_is_one_time(tmp_path, monkeypatch):
    """Once the marker is set, a user who explicitly re-saves the legacy
    pair (e.g. 25% via the slider) is NOT silently rewritten on next load.
    """
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "pipeline": {
            "miss_det_confidence": 0.25,
            "miss_det_confidence_burst": 0.15,
        },
    })
    cfg.migrate_legacy_miss_thresholds()

    # User explicitly re-saves 25%/15% later — paired by the settings UI.
    raw = _read_raw(cfg.CONFIG_PATH)
    raw["pipeline"]["miss_det_confidence"] = 0.25
    raw["pipeline"]["miss_det_confidence_burst"] = 0.15
    _write_raw(cfg.CONFIG_PATH, raw)

    assert cfg.migrate_legacy_miss_thresholds() is False
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["miss_det_confidence"] == 0.25
    assert raw["pipeline"]["miss_det_confidence_burst"] == 0.15


def test_migrate_legacy_miss_thresholds_no_config_file(tmp_path, monkeypatch):
    """No config.json yet — migration creates the file with just the marker
    and returns False (nothing was rewritten)."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    assert cfg.migrate_legacy_miss_thresholds() is False
    raw = _read_raw(cfg.CONFIG_PATH)
    assert cfg.MIGRATION_MISS_THRESHOLDS in raw["_migrations_applied"]


def test_migrate_legacy_miss_thresholds_rewrites_workspace_overrides(
    tmp_path, monkeypatch
):
    """Workspace overrides carrying the exact legacy pair are rewritten."""
    import json as _json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database

    db = Database(str(tmp_path / "vireo.db"))
    ws_id = db.create_workspace(
        "Legacy",
        config_overrides={
            "pipeline": {
                "miss_det_confidence": 0.25,
                "miss_det_confidence_burst": 0.15,
            },
        },
    )
    db.create_workspace(
        "Customized",
        config_overrides={
            "pipeline": {
                "miss_det_confidence": 0.30,
                "miss_det_confidence_burst": 0.18,
            },
        },
    )

    cfg.migrate_legacy_miss_thresholds(db)

    legacy_ws = db.get_workspace(ws_id)
    legacy_overrides = _json.loads(legacy_ws["config_overrides"])
    assert legacy_overrides["pipeline"]["miss_det_confidence"] == 0.20
    assert legacy_overrides["pipeline"]["miss_det_confidence_burst"] == 0.12

    custom_ws_id = next(
        w["id"] for w in db.get_workspaces() if w["name"] == "Customized"
    )
    custom_overrides = _json.loads(
        db.get_workspace(custom_ws_id)["config_overrides"]
    )
    assert custom_overrides["pipeline"]["miss_det_confidence"] == 0.30
    assert custom_overrides["pipeline"]["miss_det_confidence_burst"] == 0.18


def test_migrate_legacy_w_species_default_rewrites_exact_value(
    tmp_path, monkeypatch
):
    """An install with the previous default (0.10) persisted gets rewritten
    to the new default (0.40)."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {"pipeline": {"w_species": 0.10}})

    assert cfg.migrate_legacy_w_species_default() is True
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["w_species"] == 0.40
    assert cfg.MIGRATION_W_SPECIES_DEFAULT in raw["_migrations_applied"]


def test_migrate_legacy_w_species_default_preserves_customized(
    tmp_path, monkeypatch
):
    """A user who tuned w_species to something other than the legacy default
    is left alone."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {"pipeline": {"w_species": 0.25}})

    assert cfg.migrate_legacy_w_species_default() is False
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["w_species"] == 0.25
    assert cfg.MIGRATION_W_SPECIES_DEFAULT in raw["_migrations_applied"]


def test_migrate_legacy_w_species_default_is_one_time(tmp_path, monkeypatch):
    """Once the marker is set, a user who explicitly re-saves 0.10 via the
    slider is NOT silently rewritten on next load."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {"pipeline": {"w_species": 0.10}})
    cfg.migrate_legacy_w_species_default()

    raw = _read_raw(cfg.CONFIG_PATH)
    raw["pipeline"]["w_species"] = 0.10
    _write_raw(cfg.CONFIG_PATH, raw)

    assert cfg.migrate_legacy_w_species_default() is False
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["w_species"] == 0.10


def test_migrate_legacy_w_species_default_no_config_file(tmp_path, monkeypatch):
    """No config.json yet — migration stamps the marker and returns False."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    assert cfg.migrate_legacy_w_species_default() is False
    raw = _read_raw(cfg.CONFIG_PATH)
    assert cfg.MIGRATION_W_SPECIES_DEFAULT in raw["_migrations_applied"]


def test_migrate_legacy_w_species_default_rewrites_workspace_overrides(
    tmp_path, monkeypatch
):
    """Workspace overrides carrying the exact legacy value are rewritten;
    customized workspace overrides are left alone."""
    import json as _json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database

    db = Database(str(tmp_path / "vireo.db"))
    ws_id = db.create_workspace(
        "Legacy",
        config_overrides={"pipeline": {"w_species": 0.10}},
    )
    db.create_workspace(
        "Customized",
        config_overrides={"pipeline": {"w_species": 0.25}},
    )

    cfg.migrate_legacy_w_species_default(db)

    legacy_overrides = _json.loads(
        db.get_workspace(ws_id)["config_overrides"]
    )
    assert legacy_overrides["pipeline"]["w_species"] == 0.40

    custom_ws_id = next(
        w["id"] for w in db.get_workspaces() if w["name"] == "Customized"
    )
    custom_overrides = _json.loads(
        db.get_workspace(custom_ws_id)["config_overrides"]
    )
    assert custom_overrides["pipeline"]["w_species"] == 0.25


def test_migrate_toggle_ui_h_conflict_blanks_when_h_taken(tmp_path, monkeypatch):
    """A user upgrading with `browse.flag` already bound to `h` gets
    `browse.toggle_ui` blanked to `""` so the newly added default doesn't
    silently steal their existing binding."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "keyboard_shortcuts": {
            "browse": {
                "flag": "h",
                "reject": "x",
            },
        },
    })

    assert cfg.migrate_toggle_ui_h_conflict() is True
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["keyboard_shortcuts"]["browse"]["toggle_ui"] == ""
    assert raw["keyboard_shortcuts"]["browse"]["flag"] == "h"
    assert cfg.MIGRATION_TOGGLE_UI_H_CONFLICT in raw["_migrations_applied"]


def test_migrate_toggle_ui_h_conflict_case_insensitive(tmp_path, monkeypatch):
    """A saved config that spells the key `H` still triggers the conflict —
    `matchesShortcut` lower-cases the event key, so the collision is real."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "keyboard_shortcuts": {
            "browse": {
                "unflag": "H",
            },
        },
    })

    assert cfg.migrate_toggle_ui_h_conflict() is True
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["keyboard_shortcuts"]["browse"]["toggle_ui"] == ""


def test_migrate_toggle_ui_h_conflict_no_conflict_leaves_defaults(
    tmp_path, monkeypatch
):
    """No user binding uses `h` — nothing to rewrite. The default merged in
    from DEFAULTS stays as `h`."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "keyboard_shortcuts": {
            "browse": {
                "flag": "p",
                "reject": "x",
            },
        },
    })

    assert cfg.migrate_toggle_ui_h_conflict() is False
    raw = _read_raw(cfg.CONFIG_PATH)
    # Only the migration marker was added — no toggle_ui override written.
    assert "toggle_ui" not in raw["keyboard_shortcuts"]["browse"]
    assert cfg.MIGRATION_TOGGLE_UI_H_CONFLICT in raw["_migrations_applied"]
    # And the effective config still fills in the DEFAULTS binding.
    assert cfg.load()["keyboard_shortcuts"]["browse"]["toggle_ui"] == "h"


def test_migrate_toggle_ui_h_conflict_preserves_explicit_user_setting(
    tmp_path, monkeypatch
):
    """A user who already set `browse.toggle_ui` explicitly (even to `h`) is
    left alone — the migration only fills the gap when no explicit choice
    exists, so it never overrides a deliberate binding."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "keyboard_shortcuts": {
            "browse": {
                "flag": "h",
                "toggle_ui": "h",
            },
        },
    })

    assert cfg.migrate_toggle_ui_h_conflict() is False
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["keyboard_shortcuts"]["browse"]["toggle_ui"] == "h"


def test_migrate_toggle_ui_h_conflict_is_one_time(tmp_path, monkeypatch):
    """Once the marker is set, a later re-conflict (e.g. user rebinds another
    action to `h` and clears their `toggle_ui`) is NOT touched — respecting
    the user's explicit choice on subsequent boots."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "keyboard_shortcuts": {
            "browse": {
                "flag": "h",
            },
        },
    })
    assert cfg.migrate_toggle_ui_h_conflict() is True

    # User later reverts their toggle_ui override (by removing it from disk)
    # and picks another binding for `h` — a second migration run must not
    # silently rewrite anything.
    raw = _read_raw(cfg.CONFIG_PATH)
    raw["keyboard_shortcuts"]["browse"].pop("toggle_ui", None)
    raw["keyboard_shortcuts"]["browse"]["unflag"] = "h"
    _write_raw(cfg.CONFIG_PATH, raw)

    assert cfg.migrate_toggle_ui_h_conflict() is False
    raw = _read_raw(cfg.CONFIG_PATH)
    assert "toggle_ui" not in raw["keyboard_shortcuts"]["browse"]
    assert raw["keyboard_shortcuts"]["browse"]["unflag"] == "h"


def test_migrate_toggle_ui_h_conflict_no_config_file(tmp_path, monkeypatch):
    """Fresh install with no config.json — nothing to rewrite; only the
    marker gets stamped so future boots skip the check."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    assert cfg.migrate_toggle_ui_h_conflict() is False
    raw = _read_raw(cfg.CONFIG_PATH)
    assert cfg.MIGRATION_TOGGLE_UI_H_CONFLICT in raw["_migrations_applied"]


def test_migrate_browse_location_status_rewrites_exact_legacy_default(
    tmp_path, monkeypatch
):
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "browse_card_fields": ["filename", "rating", "flag", "sharpness"],
    })

    assert cfg.migrate_browse_location_status_field() is True
    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["browse_card_fields"] == [
        "filename", "location_status", "rating", "flag", "sharpness"
    ]
    assert cfg.MIGRATION_BROWSE_LOCATION_STATUS in raw["_migrations_applied"]


def test_migrate_browse_location_status_preserves_custom_layout(
    tmp_path, monkeypatch
):
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    custom = ["filename", "species"]
    _write_raw(cfg.CONFIG_PATH, {"browse_card_fields": custom})

    assert cfg.migrate_browse_location_status_field() is False
    assert _read_raw(cfg.CONFIG_PATH)["browse_card_fields"] == custom


def test_migrate_eye_detect_default_off_rewrites_legacy_true(
    tmp_path, monkeypatch
):
    import config as cfg

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    config_path.write_text(
        json.dumps({"pipeline": {"eye_detect_enabled": True}})
    )

    assert cfg.migrate_eye_detect_default_off() is True
    raw = json.loads(config_path.read_text())
    assert raw["pipeline"]["eye_detect_enabled"] is False
    assert cfg.MIGRATION_EYE_DETECT_DEFAULT_OFF in raw["_migrations_applied"]


def test_migrate_eye_detect_default_off_preserves_false(tmp_path, monkeypatch):
    import config as cfg

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    config_path.write_text(
        json.dumps({"pipeline": {"eye_detect_enabled": False}})
    )

    assert cfg.migrate_eye_detect_default_off() is False
    raw = json.loads(config_path.read_text())
    assert raw["pipeline"]["eye_detect_enabled"] is False
    assert cfg.MIGRATION_EYE_DETECT_DEFAULT_OFF in raw["_migrations_applied"]


def test_migrate_eye_detect_default_off_is_one_time(tmp_path, monkeypatch):
    import config as cfg

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    config_path.write_text(
        json.dumps({"pipeline": {"eye_detect_enabled": True}})
    )

    assert cfg.migrate_eye_detect_default_off() is True
    raw = json.loads(config_path.read_text())
    raw["pipeline"]["eye_detect_enabled"] = True
    config_path.write_text(json.dumps(raw))

    assert cfg.migrate_eye_detect_default_off() is False
    raw = json.loads(config_path.read_text())
    assert raw["pipeline"]["eye_detect_enabled"] is True


def test_migrate_eye_detect_default_off_rewrites_workspace_overrides(
    tmp_path, monkeypatch
):
    import config as cfg
    from db import Database

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    config_path.write_text("{}")
    db = Database(str(tmp_path / "test.db"), initialize_schema=True)
    db.create_workspace(
        "legacy",
        config_overrides={"pipeline": {"eye_detect_enabled": True}},
    )

    assert cfg.migrate_eye_detect_default_off(db) is True
    rows = db.conn.execute(
        "SELECT config_overrides FROM workspaces WHERE name = 'legacy'"
    ).fetchall()
    overrides = json.loads(rows[0]["config_overrides"])
    assert overrides["pipeline"]["eye_detect_enabled"] is False


def test_migrate_eye_detect_default_off_invalidates_workspace_group_fingerprint(
    tmp_path, monkeypatch,
):
    """A workspace override rewritten from True to False was previously
    scoring with eye detection on. Its cached
    ``pipeline_results_ws*.json`` reflects those KEEP/REJECT decisions,
    but ``compute_group_fingerprint`` only reads encounter/burst settings.
    The migration must null ``last_group_fingerprint`` on the rewritten
    workspace so the Process page reports the cache as outdated rather
    than serving stale eye-enabled scoring after the default flips.
    """
    import config as cfg
    from db import Database

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    config_path.write_text("{}")
    db = Database(str(tmp_path / "test.db"), initialize_schema=True)
    ws_id = db.create_workspace(
        "legacy",
        config_overrides={"pipeline": {"eye_detect_enabled": True}},
    )
    db.set_workspace_group_state(ws_id, "stale-fp", "2025-01-01T00:00:00Z")

    assert cfg.migrate_eye_detect_default_off(db) is True

    row = db.conn.execute(
        "SELECT last_group_fingerprint FROM workspaces WHERE id = ?", (ws_id,),
    ).fetchone()
    assert row["last_group_fingerprint"] is None, (
        "workspace whose eye_detect_enabled override was rewritten from True "
        "to False must have its group fingerprint cleared so the plan page "
        "treats prior eye-enabled scoring as outdated"
    )


def test_migrate_eye_detect_default_off_invalidates_default_relying_workspaces(
    tmp_path, monkeypatch,
):
    """When the global default flips from True to False, workspaces that
    were relying on the global default (no explicit False override) also
    had their effective ``eye_detect_enabled`` change. Their cached
    triage was scored with eye detection on and must be invalidated.
    Workspaces with an explicit False override were already scoring
    without eye detection and must keep their fingerprint.
    """
    import config as cfg
    from db import Database

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    config_path.write_text(
        json.dumps({"pipeline": {"eye_detect_enabled": True}})
    )
    db = Database(str(tmp_path / "test.db"), initialize_schema=True)
    ws_relying_id = db.create_workspace("relying")
    ws_explicit_false_id = db.create_workspace(
        "explicit-false",
        config_overrides={"pipeline": {"eye_detect_enabled": False}},
    )
    db.set_workspace_group_state(ws_relying_id, "relying-fp", "2025-01-01T00:00:00Z")
    db.set_workspace_group_state(
        ws_explicit_false_id, "explicit-false-fp", "2025-01-01T00:00:00Z",
    )

    assert cfg.migrate_eye_detect_default_off(db) is True

    rows = {
        r["id"]: r["last_group_fingerprint"]
        for r in db.conn.execute(
            "SELECT id, last_group_fingerprint FROM workspaces",
        ).fetchall()
    }
    assert rows[ws_relying_id] is None, (
        "workspace relying on the global True default must have its "
        "fingerprint cleared when the global flips to False"
    )
    assert rows[ws_explicit_false_id] == "explicit-false-fp", (
        "workspace with an explicit False override was already scoring "
        "eye-disabled; its fingerprint must not be cleared"
    )


def test_migrate_eye_detect_default_off_invalidates_when_global_key_absent(
    tmp_path, monkeypatch,
):
    """When the raw config lacks ``pipeline.eye_detect_enabled`` entirely,
    it was relying on the previous DEFAULTS value (``True``) — the same
    effective pre-migration state as an explicit True. Default-relying
    workspaces were therefore scoring eye-enabled and their fingerprints
    must be invalidated even though ``pipeline`` is absent from disk.
    Regression for Codex thread PRRT_kwDORn8c-s6QN0m2 (invalidation
    gated only on a raw global True).
    """
    import config as cfg
    from db import Database

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    # Two shapes of the same "no explicit key" case: (a) pipeline block
    # entirely absent, (b) pipeline present but eye_detect_enabled absent.
    # Both mean the workspace was relying on the old True DEFAULTS.
    config_path.write_text(json.dumps({"some_other_setting": 1}))

    db = Database(str(tmp_path / "test.db"), initialize_schema=True)
    ws_relying_id = db.create_workspace("relying")
    db.set_workspace_group_state(
        ws_relying_id, "relying-fp", "2025-01-01T00:00:00Z",
    )

    cfg.migrate_eye_detect_default_off(db)

    row = db.conn.execute(
        "SELECT last_group_fingerprint FROM workspaces WHERE id = ?",
        (ws_relying_id,),
    ).fetchone()
    assert row["last_group_fingerprint"] is None, (
        "workspace relying on the previous True DEFAULTS (raw config has "
        "no pipeline.eye_detect_enabled key) must have its fingerprint "
        "cleared when the default flips to False; otherwise the Process "
        "page keeps reporting eye-scored triage as fresh"
    )


def test_migrate_eye_detect_default_off_keeps_fingerprint_when_global_already_false(
    tmp_path, monkeypatch,
):
    """When the global default is already False, workspaces without any
    eye override were already scoring eye-disabled and their cached
    triage remains accurate. Only workspaces whose override rewrites
    from True to False should have their fingerprints cleared.
    """
    import config as cfg
    from db import Database

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    config_path.write_text(
        json.dumps({"pipeline": {"eye_detect_enabled": False}})
    )
    db = Database(str(tmp_path / "test.db"), initialize_schema=True)
    ws_no_override_id = db.create_workspace("no-override")
    db.set_workspace_group_state(
        ws_no_override_id, "keep-me", "2025-01-01T00:00:00Z",
    )

    assert cfg.migrate_eye_detect_default_off(db) is False

    row = db.conn.execute(
        "SELECT last_group_fingerprint FROM workspaces WHERE id = ?",
        (ws_no_override_id,),
    ).fetchone()
    assert row["last_group_fingerprint"] == "keep-me", (
        "when the global default was already False, workspaces without an "
        "eye override were already scoring eye-disabled and must keep their "
        "cached fingerprint"
    )


def test_migrate_eye_detect_default_off_preserves_explicit_workspace_opt_ins(
    tmp_path, monkeypatch,
):
    """When the global config already had ``eye_detect_enabled=False`` (an
    installation that intentionally scored eye-disabled globally before
    this migration ran), a workspace override of ``True`` is an explicit
    per-workspace opt-in — the user chose to run eye detection in that
    workspace despite the global default. The migration must leave those
    intentional overrides alone; otherwise upgrading silently disables
    eye detection/scoring for workspaces whose settings intentionally
    differed from the global default.
    """
    import config as cfg
    from db import Database

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    config_path.write_text(
        json.dumps({"pipeline": {"eye_detect_enabled": False}})
    )
    db = Database(str(tmp_path / "test.db"), initialize_schema=True)
    db.create_workspace(
        "opt-in",
        config_overrides={"pipeline": {"eye_detect_enabled": True}},
    )

    cfg.migrate_eye_detect_default_off(db)

    row = db.conn.execute(
        "SELECT config_overrides FROM workspaces WHERE name = 'opt-in'"
    ).fetchone()
    overrides = json.loads(row["config_overrides"])
    assert overrides["pipeline"]["eye_detect_enabled"] is True, (
        "when the global default was already False, a workspace "
        "eye_detect_enabled=True override is an explicit per-workspace "
        "opt-in and must not be flipped by the legacy-default migration"
    )


def test_migrate_eye_detect_default_off_chunks_workspace_invalidation(
    tmp_path, monkeypatch,
):
    """When more workspaces need their fingerprint cleared than SQLite's
    bound-parameter limit, the invalidation UPDATE must be chunked. A
    single ``UPDATE ... WHERE id IN (?, ?, ...)`` with a placeholder per
    workspace raises ``OperationalError: too many SQL variables`` on
    legacy 999-variable SQLite builds and would prevent the app from
    starting (the migration runs during ``create_app``). Regression for
    Codex thread PRRT_kwDORn8c-s6QODfD.
    """
    import config as cfg
    from db import _SQLITE_PARAM_CHUNK_SIZE, Database

    config_path = tmp_path / "config.json"
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(config_path))
    config_path.write_text(
        json.dumps({"pipeline": {"eye_detect_enabled": True}})
    )
    db = Database(str(tmp_path / "test.db"), initialize_schema=True)
    # Create more workspaces than the chunk size so a naive single-IN
    # UPDATE would need >_SQLITE_PARAM_CHUNK_SIZE placeholders. Each must
    # be marked as having a stamped fingerprint so they enter the
    # invalidation set.
    count = _SQLITE_PARAM_CHUNK_SIZE + 25
    ws_ids = []
    for i in range(count):
        ws_id = db.create_workspace(f"ws-{i}")
        db.set_workspace_group_state(ws_id, f"fp-{i}", "2025-01-01T00:00:00Z")
        ws_ids.append(ws_id)

    assert cfg.migrate_eye_detect_default_off(db) is True

    rows = db.conn.execute(
        "SELECT id, last_group_fingerprint FROM workspaces"
    ).fetchall()
    fingerprints = {r["id"]: r["last_group_fingerprint"] for r in rows}
    for ws_id in ws_ids:
        assert fingerprints[ws_id] is None, (
            f"workspace {ws_id} was relying on the global True default "
            f"and must have its fingerprint cleared even at scale"
        )


def test_migrate_default_strategy_to_process_id_rewrites_global(
    tmp_path, monkeypatch
):
    """The global config file's legacy ``pipeline.default_strategy`` is
    rewritten to ``pipeline.default_process_id`` at the matching seed id.

    Without this rewrite, an upgraded install with a global after-import
    default set the old way silently falls back to import-only because the
    import endpoints and ``get_effective_config`` now only read
    ``pipeline.default_process_id``.
    """
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "pipeline": {"default_strategy": "identify"},
    })
    from db import Database

    db = Database(str(tmp_path / "vireo.db"))
    try:
        identify_id = next(
            p["id"] for p in db.get_saved_processes()
            if p["name"] == "Identify birds"
        )
        assert cfg.migrate_default_strategy_to_process_id(db) is True
    finally:
        db.close()

    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["default_process_id"] == identify_id
    assert "default_strategy" not in raw["pipeline"]
    assert (
        cfg.MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID in raw["_migrations_applied"]
    )


def test_migrate_default_strategy_to_process_id_unknown_name_becomes_null(
    tmp_path, monkeypatch
):
    """An unknown/removed legacy strategy name maps to null (import only).
    The old key is still dropped and the marker is stamped so we don't try
    again on the next boot."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "pipeline": {"default_strategy": "some_deleted_preset"},
    })
    from db import Database

    db = Database(str(tmp_path / "vireo.db"))
    try:
        assert cfg.migrate_default_strategy_to_process_id(db) is True
    finally:
        db.close()

    raw = _read_raw(cfg.CONFIG_PATH)
    assert "default_strategy" not in raw["pipeline"]
    assert "default_process_id" not in raw["pipeline"]
    assert (
        cfg.MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID in raw["_migrations_applied"]
    )


def test_migrate_default_strategy_to_process_id_is_one_time(
    tmp_path, monkeypatch
):
    """After the marker is stamped, a user who later hand-adds
    ``default_strategy`` back is not silently rewritten — respecting a
    deliberate manual edit."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {
        "pipeline": {"default_strategy": "full"},
    })
    from db import Database

    db = Database(str(tmp_path / "vireo.db"))
    try:
        assert cfg.migrate_default_strategy_to_process_id(db) is True

        # User hand-edits the (now-legacy) key back in after upgrade.
        raw = _read_raw(cfg.CONFIG_PATH)
        raw["pipeline"]["default_strategy"] = "cull_ready"
        _write_raw(cfg.CONFIG_PATH, raw)

        assert cfg.migrate_default_strategy_to_process_id(db) is False
    finally:
        db.close()

    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["default_strategy"] == "cull_ready"


def test_migrate_default_strategy_to_process_id_no_config_file(
    tmp_path, monkeypatch
):
    """Fresh install (no config.json yet) — nothing to rewrite; just the
    marker gets stamped."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database

    db = Database(str(tmp_path / "vireo.db"))
    try:
        assert cfg.migrate_default_strategy_to_process_id(db) is False
    finally:
        db.close()

    raw = _read_raw(cfg.CONFIG_PATH)
    assert (
        cfg.MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID in raw["_migrations_applied"]
    )


def test_migrate_default_strategy_defers_when_saved_processes_absent(
    tmp_path, monkeypatch
):
    """The low-level migration function must DEFER when passed a
    schema-less connection (return False, keep the legacy key, not stamp
    the marker) instead of crashing with 'no such table: saved_processes'.

    ``create_app`` itself now opens a schema-initializing handle for this
    migration so it completes on the first boot after upgrade (see
    ``test_create_app_completes_default_strategy_migration_on_first_boot``);
    the defer path here is the safety net when a caller passes a
    ``initialize_schema=False`` handle directly, ensuring the migration is
    still recoverable on a later boot rather than blowing up startup.
    """
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {"pipeline": {"default_strategy": "identify"}})
    from db import Database

    db_path = str(tmp_path / "vireo.db")
    # Simulate a pre-saved_processes upgrade: a DB with the older tables but no
    # saved_processes. Build the full schema, then drop the new table.
    seed_db = Database(db_path)
    seed_db.conn.execute("DROP TABLE saved_processes")
    seed_db.conn.commit()
    seed_db.close()

    # init_db-style handle: no schema initialization on this connection.
    db = Database(db_path, initialize_schema=False)
    try:
        assert cfg.migrate_default_strategy_to_process_id(db) is False
    finally:
        db.close()

    raw = _read_raw(cfg.CONFIG_PATH)
    assert raw["pipeline"]["default_strategy"] == "identify"  # legacy key kept
    assert "default_process_id" not in raw["pipeline"]
    assert (
        cfg.MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID
        not in raw.get("_migrations_applied", [])
    )


def test_create_app_completes_default_strategy_migration_on_first_boot(
    tmp_path, monkeypatch
):
    """create_app's startup init_db uses initialize_schema=False, but the
    global default_strategy migration still needs the seeded
    saved_processes table to resolve legacy strategy names to ids. Without
    a schema-initializing pass gated on the migration marker, the
    migration silently defers on the first boot after upgrade — any
    import in that session inheriting the legacy default falls back to
    import-only. create_app must open a targeted schema-initializing
    handle so the migration completes on the very first boot.
    """
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    _write_raw(cfg.CONFIG_PATH, {"pipeline": {"default_strategy": "identify"}})

    from app import create_app

    db_path = str(tmp_path / "vireo.db")
    thumb_dir = tmp_path / "thumbs"
    thumb_dir.mkdir()
    # First boot after upgrade: DB file didn't exist before this call.
    app = create_app(db_path, str(thumb_dir))
    assert app is not None

    raw = _read_raw(cfg.CONFIG_PATH)
    # Legacy key must be gone and the migration marker stamped so the
    # next boot doesn't re-run.
    assert "default_strategy" not in raw.get("pipeline", {}), raw
    assert (
        cfg.MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID
        in raw.get("_migrations_applied", [])
    )
    # The seeded "Identify birds" process should exist and be pointed at.
    pid = raw["pipeline"].get("default_process_id")
    assert isinstance(pid, int) and pid > 0, raw
    from db import Database
    seeded = Database(db_path).get_saved_process(pid)
    assert seeded is not None
    assert seeded["name"] == "Identify birds"


def test_default_subject_types_includes_taxonomy_individual_genre(tmp_path, monkeypatch):
    """Default subject_types is the set of keyword types that count as
    'identifying' a photo — taxonomy + individual + genre by default."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    loaded = cfg.load()
    assert set(loaded.get("subject_types", [])) == {"taxonomy", "individual", "genre"}


def test_google_maps_api_key_default_is_empty(tmp_path, monkeypatch):
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    assert cfg.load().get("google_maps_api_key") == ""


def test_open_in_browser_default_is_false(tmp_path, monkeypatch):
    """open_in_browser defaults to False so the Tauri wrapper keeps its
    classic in-window behavior unless the user opts in."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    assert cfg.DEFAULTS["open_in_browser"] is False
    assert cfg.load().get("open_in_browser") is False


def test_open_in_browser_round_trip(tmp_path, monkeypatch):
    """Setting open_in_browser persists to disk and reloads as a real bool.

    The Rust side reads ~/.vireo/config.json directly with serde_json, so the
    value must round-trip as a JSON boolean (not a truthy int or string)."""
    import json as _json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.set("open_in_browser", True)
    assert cfg.get("open_in_browser") is True

    # Confirm it serialises as a bare JSON boolean — what Rust's serde_json
    # will deserialise into bool.
    with open(cfg.CONFIG_PATH) as f:
        on_disk = _json.load(f)
    assert on_disk["open_in_browser"] is True

    cfg.set("open_in_browser", False)
    assert cfg.get("open_in_browser") is False


def test_save_retries_on_windows_permission_error(tmp_path, monkeypatch):
    """On Windows, Defender / Search indexer transiently locks the destination
    after a write; ``cfg.save`` must retry ``os.replace`` instead of bubbling
    the ``PermissionError`` up as a 500. Regression for CI failure on PR #977
    where two consecutive saves in ``test_import_replaces_global_file`` hit
    ``[WinError 5] Access is denied`` from the second ``os.replace``."""
    import config as cfg

    monkeypatch.setattr(cfg, "sys", _FakeWin32Sys())
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    real_replace = cfg.os.replace
    calls = {"n": 0}

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] < 8:
            raise PermissionError(5, "Access is denied", dst)
        return real_replace(src, dst)

    monkeypatch.setattr(cfg.os, "replace", flaky_replace)
    monkeypatch.setattr(cfg.time, "sleep", lambda _s: None)

    cfg.save({"classification_threshold": 0.42})

    assert calls["n"] == 8
    assert cfg.load()["classification_threshold"] == 0.42


def test_save_raises_when_windows_retry_budget_exhausted(tmp_path, monkeypatch):
    """If every ``os.replace`` attempt raises ``PermissionError``, the last
    exception is re-raised so callers see the underlying failure rather than
    silently dropping the write."""
    import config as cfg

    monkeypatch.setattr(cfg, "sys", _FakeWin32Sys())
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    def always_fail(src, dst):
        raise PermissionError(5, "Access is denied", dst)

    monkeypatch.setattr(cfg.os, "replace", always_fail)
    monkeypatch.setattr(cfg.time, "sleep", lambda _s: None)

    import pytest

    with pytest.raises(PermissionError):
        cfg.save({"classification_threshold": 0.42})

    # Temp file should be cleaned up after the failure.
    assert not any(p.name.endswith(".tmp") for p in tmp_path.iterdir())


class _FakeWin32Sys:
    """Stand-in for ``sys`` that reports ``platform == 'win32'`` so the retry
    branch in ``config._replace_with_windows_retry`` exercises on POSIX CI."""

    platform = "win32"


# --------------------------------------------------------------------------
# remote_targets: local_archive_root
# --------------------------------------------------------------------------

def _base_target(**over):
    t = {"host": "nas", "user": "julius", "remote_path": "/volume1/Photos",
         "mount_path": "/Volumes/Photos"}
    t.update(over)
    return t


def test_remote_target_local_archive_root_passthrough(tmp_path, monkeypatch):
    """A valid absolute local_archive_root outside mount_path survives the
    save -> get_remote_targets() round trip verbatim."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    archive_root = str(tmp_path / "archive")
    cfg.save({"remote_targets": [_base_target(
        local_archive_root=archive_root,
    )]})

    targets = cfg.get_remote_targets()
    assert len(targets) == 1
    assert targets[0]["local_archive_root"] == archive_root


def test_remote_target_local_archive_root_defaults_empty(tmp_path, monkeypatch):
    """A saved target with no local_archive_root key coerces to ""."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    cfg.save({"remote_targets": [_base_target()]})

    targets = cfg.get_remote_targets()
    assert len(targets) == 1
    assert targets[0]["local_archive_root"] == ""


def test_remote_target_local_archive_root_rejects_relative(tmp_path, monkeypatch):
    """A relative local_archive_root is blanked to "" but the rest of the
    target (host/user/remote_path/mount_path) is still valid — invalid
    values are blanked rather than dropping the whole target."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    cfg.save({"remote_targets": [_base_target(local_archive_root="Photos")]})

    targets = cfg.get_remote_targets()
    assert len(targets) == 1
    assert targets[0]["local_archive_root"] == ""
    assert targets[0]["host"] == "nas"
    assert targets[0]["mount_path"] == "/Volumes/Photos"


def test_remote_target_local_archive_root_rejects_inside_mount(tmp_path, monkeypatch):
    """local_archive_root pointed inside mount_path is blanked — mount_path
    is the destination view of the NAS, so archiving into it would "move"
    files onto themselves."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    mount = str(tmp_path / "mount")
    sub = str(tmp_path / "mount" / "sub")
    coerced = cfg._coerce_remote_target(_base_target(
        mount_path=mount, local_archive_root=sub,
    ))
    assert coerced is not None
    assert coerced["local_archive_root"] == ""
    assert coerced["mount_path"] == mount


def test_remote_target_relative_mount_path_keeps_archive_root(tmp_path, monkeypatch):
    """A relative mount_path must not blank a valid local_archive_root: the
    inside-mount containment check would otherwise realpath the mount against
    the server's CWD, making the outcome depend on where the server was
    launched."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    archive_root = str(tmp_path / "archive")
    # Relative mount spelled so a CWD of tmp_path would make archive_root
    # look nested inside it.
    monkeypatch.chdir(tmp_path)
    coerced = cfg._coerce_remote_target(_base_target(
        mount_path=".", local_archive_root=archive_root,
    ))
    assert coerced is not None
    assert coerced["local_archive_root"] == archive_root


def test_remote_target_archive_root_case_alias_of_mount_is_blanked(
    tmp_path, monkeypatch,
):
    """On a case-insensitive volume, a local_archive_root that differs from
    mount_path only by case is the same directory: the archive root must be
    blanked so the target does not later fail chained moves as a
    source/destination overlap. A byte-wise commonpath compare would miss
    this alias."""
    import config as cfg

    probe = tmp_path / "CaseProbe"
    probe.mkdir()
    if not (tmp_path / "caseprobe").exists():
        pytest.skip("requires a case-insensitive filesystem")

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    mount = tmp_path / "Photos"
    mount.mkdir()
    alias_archive = str(tmp_path / "photos")  # same directory, different case
    coerced = cfg._coerce_remote_target(_base_target(
        mount_path=str(mount), local_archive_root=alias_archive,
    ))
    assert coerced is not None
    assert coerced["local_archive_root"] == ""
    assert coerced["mount_path"] == str(mount)


def test_remote_target_archive_root_inside_mount_via_case_alias_is_blanked(
    tmp_path, monkeypatch,
):
    """A local_archive_root strictly *inside* the mount via a case-alias
    ancestor (mount `/Volumes/Photos`, archive `/volumes/photos/staging`)
    must be blanked too — same directory-tree overlap as the equal case."""
    import config as cfg

    probe = tmp_path / "CaseProbe"
    probe.mkdir()
    if not (tmp_path / "caseprobe").exists():
        pytest.skip("requires a case-insensitive filesystem")

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    mount = tmp_path / "Photos"
    (mount / "staging").mkdir(parents=True)
    alias_sub = str(tmp_path / "photos" / "staging")
    coerced = cfg._coerce_remote_target(_base_target(
        mount_path=str(mount), local_archive_root=alias_sub,
    ))
    assert coerced is not None
    assert coerced["local_archive_root"] == ""
    assert coerced["mount_path"] == str(mount)
