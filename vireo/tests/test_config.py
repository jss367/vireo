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
    assert config["working_copy_cache_max_mb"] == 20480


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


def test_load_strict_raises_on_corrupt_file(tmp_path):
    """load_strict() propagates a parse failure so callers can tell
    "config unreadable" from "config says False". The ordinary load()
    catches the parse error and returns DEFAULTS, which is unsafe for
    destructive cleanup gated on a setting: an off-by-default key would
    read False from a corrupt config and a caller treating that as an
    explicit off would strip previously-written metadata and clear the
    pending row. load_strict() surfaces the failure instead.
    """
    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        f.write("not valid json {{{")

    with pytest.raises(Exception):  # noqa: B017 -- json.JSONDecodeError
        cfg.load_strict()


def test_load_strict_returns_defaults_when_no_file(tmp_path):
    """load_strict() returns defaults when the file is simply absent --
    only a real read/parse failure should raise. A fresh install with
    no config file is not a failure state, and cleanup-gating callers
    should see the default False just like the ordinary loader."""
    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "nonexistent.json")
    loaded = cfg.load_strict()
    assert loaded == cfg.DEFAULTS


def test_load_strict_returns_merged_config_when_valid(tmp_path):
    """load_strict() reads and merges just like load() when the file is
    parseable -- the strict variant only diverges on parse failure."""

    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"write_location_keywords_to_xmp": True}, f)

    loaded = cfg.load_strict()
    assert loaded["write_location_keywords_to_xmp"] is True
    # Non-overridden defaults still land.
    assert loaded["classification_threshold"] == cfg.DEFAULTS["classification_threshold"]


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


def _read_raw(path):
    import json as _json
    with open(path) as f:
        return _json.load(f)


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


def test_google_maps_english_names_default_is_enabled(tmp_path, monkeypatch):
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    assert cfg.load().get("google_maps_prefer_english") is True


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
    import file_replace

    monkeypatch.setattr(file_replace, "sys", _FakeWin32Sys())
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    real_replace = cfg.os.replace
    calls = {"n": 0}

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] < 8:
            raise PermissionError(5, "Access is denied", dst)
        return real_replace(src, dst)

    monkeypatch.setattr(cfg.os, "replace", flaky_replace)
    monkeypatch.setattr(file_replace.time, "sleep", lambda _s: None)

    cfg.save({"classification_threshold": 0.42})

    assert calls["n"] == 8
    assert cfg.load()["classification_threshold"] == 0.42


def test_save_raises_when_windows_retry_budget_exhausted(tmp_path, monkeypatch):
    """If every ``os.replace`` attempt raises ``PermissionError``, the last
    exception is re-raised so callers see the underlying failure rather than
    silently dropping the write."""
    import config as cfg
    import file_replace

    monkeypatch.setattr(file_replace, "sys", _FakeWin32Sys())
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    def always_fail(src, dst):
        raise PermissionError(5, "Access is denied", dst)

    monkeypatch.setattr(cfg.os, "replace", always_fail)
    monkeypatch.setattr(file_replace.time, "sleep", lambda _s: None)

    import pytest

    with pytest.raises(PermissionError):
        cfg.save({"classification_threshold": 0.42})

    # Temp file should be cleaned up after the failure.
    assert not any(p.name.endswith(".tmp") for p in tmp_path.iterdir())


class _FakeWin32Sys:
    """Stand-in for ``sys`` that reports ``platform == 'win32'`` so the retry
    branch in ``file_replace.replace_file`` exercises on POSIX CI."""

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


def test_load_preserves_corrupt_file_copy(tmp_path):
    """A corrupt config file is preserved next to the original before the app
    falls back to defaults, so a later save() can't silently destroy whatever
    the user had."""
    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    corrupt_body = "not valid json {{{"
    with open(cfg.CONFIG_PATH, "w") as f:
        f.write(corrupt_body)

    loaded = cfg.load()

    assert loaded == cfg.DEFAULTS
    backup_path = cfg.CONFIG_PATH + ".corrupt"
    assert os.path.exists(backup_path)
    with open(backup_path) as f:
        assert f.read() == corrupt_body


def test_set_after_corrupt_load_keeps_backup(tmp_path):
    """set() on a corrupt config overwrites the main file with defaults+key,
    but the preserved .corrupt copy keeps the user's original bytes."""

    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    corrupt_body = '{"classification_threshold": 0.9, TRUNCATED'
    with open(cfg.CONFIG_PATH, "w") as f:
        f.write(corrupt_body)

    cfg.set("photos_per_page", 25)

    with open(cfg.CONFIG_PATH) as f:
        assert json.load(f)["photos_per_page"] == 25
    with open(cfg.CONFIG_PATH + ".corrupt") as f:
        assert f.read() == corrupt_body


def test_read_raw_preserves_corrupt_file_copy(tmp_path):
    """_read_raw (the migration read path) also preserves a corrupt file —
    migrations call save() right after, which would otherwise clobber it."""
    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    corrupt_body = "[1, 2,"
    with open(cfg.CONFIG_PATH, "w") as f:
        f.write(corrupt_body)

    assert cfg._read_raw() == {}
    with open(cfg.CONFIG_PATH + ".corrupt") as f:
        assert f.read() == corrupt_body


def test_load_preserves_current_corrupt_over_older_backup_with_newer_mtime(tmp_path):
    """A pre-existing `.corrupt` file with an equal-or-newer mtime must not
    trick the preserve step into discarding the *current* corrupt bytes.
    Real triggers: restoring the backup from history, coarse-mtime
    filesystems (FAT/some NFS), or an editor that resets timestamps."""
    import os

    import config as cfg

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    backup_path = cfg.CONFIG_PATH + ".corrupt"
    old_backup = "{ old-corruption"
    with open(backup_path, "w") as f:
        f.write(old_backup)
    current_body = "{ different-current-corruption"
    with open(cfg.CONFIG_PATH, "w") as f:
        f.write(current_body)
    # Force the backup's mtime to look newer than the current file so a
    # timestamp-based check would incorrectly skip preservation.
    now = os.path.getmtime(cfg.CONFIG_PATH)
    os.utime(backup_path, (now + 10, now + 10))

    cfg.load()

    with open(backup_path) as f:
        assert f.read() == current_body


def test_remote_target_reads_do_not_touch_the_mounted_filesystem(tmp_path, monkeypatch):
    import config as cfg
    import move

    monkeypatch.setattr(cfg, 'CONFIG_PATH', str(tmp_path / 'config.json'))
    target = _base_target(mount_path=str(tmp_path / 'offline-share'),
                          local_archive_root=str(tmp_path / 'local-archive'))
    cfg.save({'remote_targets': [target]})

    def forbidden_probe(*args, **kwargs):
        raise AssertionError('A settings read probed a potentially unresponsive mount')

    monkeypatch.setattr(move, '_path_equal_or_descends', forbidden_probe)
    monkeypatch.setattr(os.path, 'realpath', forbidden_probe)
    monkeypatch.setattr(os.path, 'samefile', forbidden_probe)
    assert cfg.get_remote_targets()[0]['local_archive_root'] == target['local_archive_root']
    assert cfg.get_remote_target(cfg.get_remote_targets()[0]['id']) is not None
    # Save-time validation still uses the stronger filesystem-aware check.
    with pytest.raises(AssertionError, match='settings read probed'):
        cfg._coerce_remote_target(target)
