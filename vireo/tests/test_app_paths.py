"""Compatibility contract for storage defaults shared by Python and Tauri."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from app_paths import default_path

# Deliberately independent of the manifest: changing an existing location must
# be an explicit migration change, not an unnoticed path refactor.
LEGACY_PATHS = {
    "data": ".vireo",
    "log": ".vireo/vireo.log",
    "database": ".vireo/vireo.db",
    "config": ".vireo/config.json",
    "models": ".vireo/models",
    "model_config": ".vireo/models.json",
    "thumbnails": ".vireo/thumbnails",
    "embeddings": ".vireo/embedding_cache",
    "computation_cache": ".vireo/computation-cache",
    "labels": ".vireo/labels",
    "active_labels": ".vireo/labels_active.json",
    "taxonomy_json": ".vireo/taxonomy.json",
    "taxonomy": ".vireo/taxonomy",
    "tools": ".vireo/tools",
    "ssh": ".vireo/ssh",
    "runtime": ".vireo/runtime.json",
    "runtime_lock": ".vireo/runtime.lock",
    "gui_clients": ".vireo/gui-clients",
    "gui_clients_lock": ".vireo/gui-clients.lock",
    "hf_cache_lock": ".vireo/hf-cache-access.lock",
    "desktop_logs_macos": "Library/Logs/com.vireo.app",
}


@pytest.mark.parametrize("name,relative", LEGACY_PATHS.items())
def test_existing_locations_and_home_resolution(name, relative, tmp_path, monkeypatch):
    for home in (tmp_path / "first home", tmp_path / "second home"):
        monkeypatch.setenv("HOME", str(home))
        assert Path(default_path(name)) == home / relative
        assert default_path(name, expand_user=False) == "~/" + relative
        assert not home.exists(), "resolving defaults must not create directories"


def test_unknown_name_fails_loudly():
    with pytest.raises(KeyError):
        default_path("missing_name")


def test_cli_and_app_keep_explicit_catalog_and_thumbnail_paths(tmp_path, monkeypatch):
    from app import _new_configured_app, _parse_main_args

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(sys, "argv", ["vireo"])
    args = _parse_main_args()
    assert Path(args.db) == tmp_path / "home/.vireo/vireo.db"
    assert Path(args.thumb_dir) == tmp_path / "home/.vireo/thumbnails"

    database = str(tmp_path / "catalog/library.db")
    thumbnails = str(tmp_path / "cache/thumbs")
    monkeypatch.setattr(sys, "argv", ["vireo", "--db", database, "--thumb-dir", thumbnails])
    args = _parse_main_args()
    app = _new_configured_app(args.db, args.thumb_dir, "test-token")
    assert app.config["DB_PATH"] == database
    assert app.config["THUMB_CACHE_DIR"] == thumbnails
    assert app.config["CARD_CLEANUP_DIR"] == str(tmp_path / "catalog/card_cleanup")
    assert Path(app.config["COMPUTATION_CACHE_DIR"]) == tmp_path / "home/.vireo/computation-cache"
    assert not (tmp_path / "home").exists()


def test_import_time_defaults_in_fresh_process(tmp_path):
    """Do not reload modules in pytest: other tests patch these public constants."""
    home = tmp_path / "home with spaces"
    script = '''
import json
import classifier, config, detector, keypoints, labels, models, taxonomy
import text_encoder, thumbnails, timm_classifier
print(json.dumps([
    config.CONFIG_PATH, models.CONFIG_PATH, models.DEFAULT_MODELS_DIR,
    classifier.CACHE_DIR, classifier._MODELS_ROOT, text_encoder._MODELS_ROOT,
    timm_classifier._MODELS_ROOT, keypoints.MODELS_DIR, labels.LABELS_DIR,
    taxonomy.TAXONOMY_JSON_PATH, thumbnails.DEFAULT_CACHE_DIR,
    detector.MEGADETECTOR_ONNX_DIR,
]))
'''
    result = subprocess.run(
        [sys.executable, "-c", script], check=True, capture_output=True, text=True,
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, "HOME": str(home), "USERPROFILE": str(home)},
    )
    expected = [
        "config.json", "models.json", "models", "embedding_cache", "models",
        "models", "models", "models", "labels", "taxonomy.json", "thumbnails",
        "models/megadetector-v6",
    ]
    assert [Path(path) for path in json.loads(result.stdout)] == [home / ".vireo" / p for p in expected]


def test_computation_cache_retains_deferred_home_and_explicit_root(tmp_path, monkeypatch):
    import computation_cache

    assert computation_cache.DEFAULT_CACHE_DIR == "~/.vireo/computation-cache"
    monkeypatch.setenv("HOME", str(tmp_path / "new home"))
    assert computation_cache.ArtifactStore().root == tmp_path / "new home/.vireo/computation-cache"
    custom = tmp_path / "custom results"
    assert computation_cache.ArtifactStore(custom).root == custom
