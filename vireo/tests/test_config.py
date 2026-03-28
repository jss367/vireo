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
