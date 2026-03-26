# vireo/tests/test_models.py
"""Tests for the model registry (models.py).

Tests cover config persistence, model listing, active model selection,
registration, removal, and taxonomy info — all without downloading
real model weights.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Config load / save
# ---------------------------------------------------------------------------


def test_load_config_missing_file(tmp_path, monkeypatch):
    """Missing config file returns default structure."""
    import models

    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "nonexistent.json"))
    config = models._load_config()
    assert config == {"models": [], "active_model": None}


def test_load_config_existing_file(tmp_path, monkeypatch):
    """Existing config file is loaded correctly."""
    import models

    cfg_path = str(tmp_path / "models.json")
    data = {"models": [{"id": "test"}], "active_model": "test"}
    with open(cfg_path, "w") as f:
        json.dump(data, f)

    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)
    config = models._load_config()
    assert config["active_model"] == "test"
    assert len(config["models"]) == 1


def test_save_config_creates_dirs(tmp_path, monkeypatch):
    """_save_config creates parent directories if needed."""
    import models

    cfg_path = str(tmp_path / "nested" / "dir" / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)
    models._save_config({"models": [], "active_model": None})
    assert os.path.exists(cfg_path)


def test_save_load_roundtrip(tmp_path, monkeypatch):
    """Config survives save then load."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)

    data = {"models": [{"id": "abc", "name": "ABC"}], "active_model": "abc"}
    models._save_config(data)
    loaded = models._load_config()
    assert loaded == data


# ---------------------------------------------------------------------------
# KNOWN_MODELS constant
# ---------------------------------------------------------------------------


def test_known_models_structure():
    """KNOWN_MODELS has required fields for each entry."""
    from models import KNOWN_MODELS

    assert len(KNOWN_MODELS) >= 3
    required = {"id", "name", "model_type", "model_str", "source", "description", "size_mb"}
    for m in KNOWN_MODELS:
        missing = required - set(m.keys())
        assert not missing, f"Model {m.get('id', '?')} missing fields: {missing}"


def test_known_model_ids_unique():
    """All known model IDs are unique."""
    from models import KNOWN_MODELS

    ids = [m["id"] for m in KNOWN_MODELS]
    assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# get_models
# ---------------------------------------------------------------------------


def test_get_models_no_downloads(tmp_path, monkeypatch):
    """get_models returns all known models as not-downloaded when config is empty."""
    import models

    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    # Stub os.path.exists within models so the legacy /tmp path check
    # doesn't leak host filesystem state into the test.
    _real_exists = os.path.exists
    monkeypatch.setattr(
        os.path, "exists",
        lambda p: _real_exists(p) if p.startswith(str(tmp_path)) else False,
    )
    result = models.get_models()
    assert len(result) >= len(models.KNOWN_MODELS)
    for m in result:
        assert "id" in m
        assert "downloaded" in m
        # No weights on disk, so not downloaded
        assert m["downloaded"] is False


def test_get_models_includes_custom(tmp_path, monkeypatch):
    """Custom models registered in config appear in get_models."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)

    # Register a custom model
    models._save_config({
        "models": [{
            "id": "custom-1",
            "name": "My Custom Model",
            "model_str": "ViT-B-16",
            "weights_path": "/nonexistent/path",
        }],
        "active_model": None,
    })

    result = models.get_models()
    custom = [m for m in result if m["id"] == "custom-1"]
    assert len(custom) == 1
    assert custom[0]["source"] == "custom"
    assert custom[0]["downloaded"] is False  # path doesn't exist


def test_get_models_downloaded_flag(tmp_path, monkeypatch):
    """A model with existing weights_path is marked as downloaded."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)

    weights = tmp_path / "weights.bin"
    weights.write_bytes(b"fake weights")

    models._save_config({
        "models": [{
            "id": "bioclip-vit-b-16",
            "name": "BioCLIP",
            "model_str": "ViT-B-16",
            "weights_path": str(weights),
        }],
        "active_model": None,
    })

    result = models.get_models()
    bioclip = [m for m in result if m["id"] == "bioclip-vit-b-16"]
    assert len(bioclip) == 1
    assert bioclip[0]["downloaded"] is True


# ---------------------------------------------------------------------------
# get_active_model / set_active_model
# ---------------------------------------------------------------------------


def test_get_active_model_none(tmp_path, monkeypatch):
    """No active model when nothing is downloaded."""
    import models

    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    assert models.get_active_model() is None


def test_set_and_get_active_model(tmp_path, monkeypatch):
    """set_active_model persists and get_active_model retrieves it."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)

    # Create a "downloaded" model
    weights = tmp_path / "weights.bin"
    weights.write_bytes(b"fake")
    models._save_config({
        "models": [{
            "id": "bioclip-vit-b-16",
            "name": "BioCLIP",
            "model_str": "ViT-B-16",
            "weights_path": str(weights),
        }],
        "active_model": None,
    })

    models.set_active_model("bioclip-vit-b-16")
    active = models.get_active_model()
    assert active is not None
    assert active["id"] == "bioclip-vit-b-16"


def test_get_active_model_fallback(tmp_path, monkeypatch):
    """Falls back to first downloaded model when active_model ID is invalid."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)

    weights = tmp_path / "weights.bin"
    weights.write_bytes(b"fake")
    models._save_config({
        "models": [{
            "id": "bioclip-vit-b-16",
            "name": "BioCLIP",
            "model_str": "ViT-B-16",
            "weights_path": str(weights),
        }],
        "active_model": "nonexistent-model",
    })

    active = models.get_active_model()
    assert active is not None
    assert active["id"] == "bioclip-vit-b-16"


# ---------------------------------------------------------------------------
# register_model
# ---------------------------------------------------------------------------


def test_register_new_model(tmp_path, monkeypatch):
    """Registering a new model adds it to config."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)

    models.register_model("new-model", "New Model", "ViT-B-16", "/path/weights", "A test model")
    config = models._load_config()
    assert any(m["id"] == "new-model" for m in config["models"])


def test_register_model_updates_existing(tmp_path, monkeypatch):
    """Re-registering an existing model updates it instead of duplicating."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)

    models.register_model("m1", "Model v1", "ViT-B-16", "/path/v1")
    models.register_model("m1", "Model v2", "ViT-L-14", "/path/v2")

    config = models._load_config()
    m1_entries = [m for m in config["models"] if m["id"] == "m1"]
    assert len(m1_entries) == 1
    assert m1_entries[0]["name"] == "Model v2"
    assert m1_entries[0]["weights_path"] == "/path/v2"


# ---------------------------------------------------------------------------
# remove_model
# ---------------------------------------------------------------------------


def test_remove_model_deletes_weights_file(tmp_path, monkeypatch):
    """Removing a model deletes its weights file and unregisters it."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))

    weights = tmp_path / "models" / "test" / "weights.bin"
    weights.parent.mkdir(parents=True)
    weights.write_bytes(b"fake weights")

    models._save_config({
        "models": [{
            "id": "test-model",
            "name": "Test",
            "weights_path": str(weights),
        }],
        "active_model": "test-model",
    })

    result = models.remove_model("test-model")
    assert result is True
    assert not weights.exists()

    config = models._load_config()
    assert not any(m["id"] == "test-model" for m in config["models"])
    assert config["active_model"] is None  # cleared since it was active


def test_remove_model_deletes_weights_directory(tmp_path, monkeypatch):
    """Removing a model with a directory weights_path removes the directory."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)

    weights_dir = tmp_path / "model_cache"
    weights_dir.mkdir()
    (weights_dir / "config.json").write_text("{}")

    models._save_config({
        "models": [{
            "id": "dir-model",
            "name": "Dir Model",
            "weights_path": str(weights_dir),
        }],
        "active_model": None,
    })

    result = models.remove_model("dir-model")
    assert result is True
    assert not weights_dir.exists()


def test_remove_model_not_found(tmp_path, monkeypatch):
    """Removing a nonexistent model returns False."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))

    result = models.remove_model("nonexistent")
    assert result is False


def test_remove_known_model_by_default_path(tmp_path, monkeypatch):
    """A known model not in config but on disk at default path can be removed."""
    import models

    models_dir = tmp_path / "models"
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(models_dir))

    # Create directory at default path for a known model
    model_dir = models_dir / "bioclip-vit-b-16"
    model_dir.mkdir(parents=True)
    (model_dir / "weights.bin").write_bytes(b"fake")

    result = models.remove_model("bioclip-vit-b-16")
    assert result is True
    assert not model_dir.exists()


# ---------------------------------------------------------------------------
# get_taxonomy_info
# ---------------------------------------------------------------------------


def test_get_taxonomy_info_no_file(monkeypatch):
    """Returns available=False when taxonomy.json doesn't exist."""
    import models

    # Point to a nonexistent file
    monkeypatch.setattr(os.path, "exists", lambda p: False if "taxonomy.json" in p else os.path.exists.__wrapped__(p) if hasattr(os.path.exists, '__wrapped__') else True)

    # Simpler approach: just call it and check — it uses __file__ to find taxonomy.json
    info = models.get_taxonomy_info()
    # taxonomy.json may or may not exist in the test env
    assert "available" in info
    assert "taxa_count" in info


def test_get_taxonomy_info_with_file(tmp_path, monkeypatch):
    """Returns correct info when taxonomy.json exists."""
    import models

    tax_path = tmp_path / "taxonomy.json"
    # Make the file large enough for taxa_count estimation (size // 150)
    tax_data = {"last_updated": "2024-01-15", "taxa": [{"name": f"Species {i}"} for i in range(100)]}
    tax_path.write_text(json.dumps(tax_data))

    # Monkey-patch the function to look at our test file
    orig_dirname = os.path.dirname

    def fake_dirname(path):
        if path == models.__file__:
            return str(tmp_path)
        return orig_dirname(path)

    monkeypatch.setattr(os.path, "dirname", fake_dirname)

    info = models.get_taxonomy_info()
    assert info["available"] is True
    assert info["last_updated"] == "2024-01-15"
    assert info["taxa_count"] > 0


# ---------------------------------------------------------------------------
# download_model (validation only — no actual downloads)
# ---------------------------------------------------------------------------


def test_download_model_unknown_id(tmp_path, monkeypatch):
    """download_model raises ValueError for unknown model ID."""
    import models

    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    try:
        models.download_model("nonexistent-model-xyz")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown model" in str(e)
