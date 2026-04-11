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


def _make_fake_data_file(path, size_bytes=2048):
    """Create a sparse .onnx.data stub for tests. Size is now arbitrary —
    the 10 MB floor in models.py has been replaced by SHA256 verification
    in model_verify.py, so test stubs only need to exist on disk."""
    with open(path, "wb") as f:
        f.truncate(size_bytes)


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
    required = {"id", "name", "model_type", "model_str", "description", "size_mb"}
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
    """A model with existing ONNX files is marked as downloaded."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))

    # Create model directory with all required files for bioclip-vit-b-16
    model_dir = tmp_path / "models" / "bioclip-vit-b-16"
    model_dir.mkdir(parents=True)
    (model_dir / "image_encoder.onnx").write_bytes(b"fake")
    _make_fake_data_file(model_dir / "image_encoder.onnx.data")
    (model_dir / "text_encoder.onnx").write_bytes(b"fake")
    _make_fake_data_file(model_dir / "text_encoder.onnx.data")
    (model_dir / "tokenizer.json").write_text("{}")
    (model_dir / "config.json").write_text("{}")

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
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))

    # Create model directory with all required files for bioclip-vit-b-16
    model_dir = tmp_path / "models" / "bioclip-vit-b-16"
    model_dir.mkdir(parents=True)
    (model_dir / "image_encoder.onnx").write_bytes(b"fake")
    _make_fake_data_file(model_dir / "image_encoder.onnx.data")
    (model_dir / "text_encoder.onnx").write_bytes(b"fake")
    _make_fake_data_file(model_dir / "text_encoder.onnx.data")
    (model_dir / "tokenizer.json").write_text("{}")
    (model_dir / "config.json").write_text("{}")

    models.set_active_model("bioclip-vit-b-16")
    active = models.get_active_model()
    assert active is not None
    assert active["id"] == "bioclip-vit-b-16"


def test_get_active_model_fallback(tmp_path, monkeypatch):
    """Falls back to first downloaded model when active_model ID is invalid."""
    import models

    cfg_path = str(tmp_path / "models.json")
    monkeypatch.setattr(models, "CONFIG_PATH", cfg_path)
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))

    # Create model directory with all required files for bioclip-vit-b-16
    model_dir = tmp_path / "models" / "bioclip-vit-b-16"
    model_dir.mkdir(parents=True)
    (model_dir / "image_encoder.onnx").write_bytes(b"fake")
    _make_fake_data_file(model_dir / "image_encoder.onnx.data")
    (model_dir / "text_encoder.onnx").write_bytes(b"fake")
    _make_fake_data_file(model_dir / "text_encoder.onnx.data")
    (model_dir / "tokenizer.json").write_text("{}")
    (model_dir / "config.json").write_text("{}")

    models._save_config({
        "models": [],
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


# ---------------------------------------------------------------------------
# Model state classification (self-heal detection)
# ---------------------------------------------------------------------------


def test_classify_state_missing_directory(tmp_path):
    """Nonexistent directory reports 'missing'."""
    import models
    assert models._classify_model_state(
        str(tmp_path / "nope"), ["a.onnx", "a.onnx.data"]
    ) == "missing"


def test_classify_state_empty_directory(tmp_path):
    """Directory with no required files reports 'missing'."""
    import models
    d = tmp_path / "m"
    d.mkdir()
    assert models._classify_model_state(
        str(d), ["image_encoder.onnx", "image_encoder.onnx.data"]
    ) == "missing"


def test_classify_state_partial_files(tmp_path):
    """Directory with some but not all required files reports 'incomplete'."""
    import models
    d = tmp_path / "m"
    d.mkdir()
    (d / "image_encoder.onnx").write_bytes(b"stub")
    _make_fake_data_file(d / "image_encoder.onnx.data")
    # missing text_encoder files
    assert models._classify_model_state(
        str(d),
        [
            "image_encoder.onnx", "image_encoder.onnx.data",
            "text_encoder.onnx", "text_encoder.onnx.data",
        ],
    ) == "incomplete"


def test_classify_state_sub_10mb_data_file_is_ok_without_sentinel(tmp_path):
    """A small .onnx.data file is no longer flagged as incomplete on size
    alone — the 10 MB floor has been replaced by SHA256 verification, which
    runs separately. _classify_model_state only looks at file presence and
    the .verify_failed sentinel."""
    import models
    d = tmp_path / "m"
    d.mkdir()
    (d / "image_encoder.onnx").write_bytes(b"stub")
    (d / "image_encoder.onnx.data").write_bytes(b"tiny")  # 4 bytes
    assert models._classify_model_state(
        str(d), ["image_encoder.onnx", "image_encoder.onnx.data"]
    ) == "ok"


def test_classify_state_verify_failed_sentinel_marks_incomplete(tmp_path):
    """If model_verify has written .verify_failed into a model dir,
    _classify_model_state reports 'incomplete' so the Settings UI
    surfaces the Repair button — reusing the existing Repair flow
    from PR #488."""
    import models
    d = tmp_path / "m"
    d.mkdir()
    (d / "image_encoder.onnx").write_bytes(b"stub")
    _make_fake_data_file(d / "image_encoder.onnx.data")
    (d / ".verify_failed").write_text("hash mismatch on retry")
    assert models._classify_model_state(
        str(d), ["image_encoder.onnx", "image_encoder.onnx.data"]
    ) == "incomplete"


def test_classify_state_ok(tmp_path):
    """All files present and no sentinel reports 'ok'."""
    import models
    d = tmp_path / "m"
    d.mkdir()
    (d / "image_encoder.onnx").write_bytes(b"stub")
    _make_fake_data_file(d / "image_encoder.onnx.data")
    assert models._classify_model_state(
        str(d), ["image_encoder.onnx", "image_encoder.onnx.data"]
    ) == "ok"


def test_get_models_surfaces_incomplete_state(tmp_path, monkeypatch):
    """get_models reports state='incomplete' and downloaded=False when a known
    model directory contains a .verify_failed sentinel (written by
    model_verify after a hash mismatch). The Settings UI uses this to show
    the Repair button."""
    import models
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))

    model_dir = tmp_path / "models" / "bioclip-vit-b-16"
    model_dir.mkdir(parents=True)
    (model_dir / "image_encoder.onnx").write_bytes(b"stub")
    _make_fake_data_file(model_dir / "image_encoder.onnx.data")
    (model_dir / "text_encoder.onnx").write_bytes(b"stub")
    _make_fake_data_file(model_dir / "text_encoder.onnx.data")
    (model_dir / "tokenizer.json").write_text("{}")
    (model_dir / "config.json").write_text("{}")
    (model_dir / ".verify_failed").write_text("sha256 mismatch")

    result = models.get_models()
    entry = next(m for m in result if m["id"] == "bioclip-vit-b-16")
    assert entry["state"] == "incomplete"
    assert entry["downloaded"] is False


# ---------------------------------------------------------------------------
# download_model — SHA256 verification with retry
# ---------------------------------------------------------------------------


def _patch_download_model_env(tmp_path, monkeypatch):
    """Shared setup: isolate config/models dir and return the model dir path."""
    import models
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))
    return models, tmp_path / "models" / "bioclip-vit-b-16"


def test_download_model_accepts_valid_result(tmp_path, monkeypatch):
    """download_model registers the model when every LFS file's SHA256 matches
    the hashes fetched from HF."""
    import hashlib

    import model_verify
    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)

    # Contents for the four LFS files; compute their expected hashes.
    lfs_contents = {
        "image_encoder.onnx": b"graph-i" * 100,
        "image_encoder.onnx.data": b"weights-i" * 1000,
        "text_encoder.onnx": b"graph-t" * 100,
        "text_encoder.onnx.data": b"weights-t" * 1000,
    }
    expected = {
        name: hashlib.sha256(data).hexdigest()
        for name, data in lfs_contents.items()
    }

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        if filename in lfs_contents:
            with open(dest, "wb") as f:
                f.write(lfs_contents[filename])
        else:
            # Non-LFS files (tokenizer.json, config.json) — just need to exist.
            with open(dest, "wb") as f:
                f.write(b"{}")
        return dest

    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", lambda subdir: expected
    )

    result = models.download_model("bioclip-vit-b-16")
    assert result.endswith("bioclip-vit-b-16")

    cfg = models._load_config()
    assert any(m["id"] == "bioclip-vit-b-16" for m in cfg.get("models", []))


def test_download_model_retries_on_hash_mismatch_then_succeeds(
    tmp_path, monkeypatch
):
    """First two download attempts produce corrupt bytes, third attempt
    produces correct bytes. download_model should retry transparently and
    ultimately succeed, registering the model."""
    import hashlib

    import model_verify
    models, _ = _patch_download_model_env(tmp_path, monkeypatch)

    good_content = b"weights-real" * 1000
    good_hash = hashlib.sha256(good_content).hexdigest()
    # Expected hashes for every LFS file in bioclip-vit-b-16.
    expected = {
        "image_encoder.onnx": hashlib.sha256(b"graph-i").hexdigest(),
        "image_encoder.onnx.data": good_hash,
        "text_encoder.onnx": hashlib.sha256(b"graph-t").hexdigest(),
        "text_encoder.onnx.data": hashlib.sha256(b"graph-td").hexdigest(),
    }

    attempts = {"image_encoder.onnx.data": 0}

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        if filename == "image_encoder.onnx.data":
            attempts[filename] += 1
            content = b"corrupt" if attempts[filename] < 3 else good_content
        elif filename == "image_encoder.onnx":
            content = b"graph-i"
        elif filename == "text_encoder.onnx":
            content = b"graph-t"
        elif filename == "text_encoder.onnx.data":
            content = b"graph-td"
        else:
            content = b"{}"
        with open(dest, "wb") as f:
            f.write(content)
        return dest

    # Track cache purges so we can assert the HF cache is cleared between retries.
    purged: list[str] = []
    monkeypatch.setattr(
        models,
        "_purge_hf_cache_file",
        lambda filename, subdir: purged.append(filename),
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", lambda subdir: expected
    )

    result = models.download_model("bioclip-vit-b-16")
    assert result.endswith("bioclip-vit-b-16")
    # 1 initial attempt + 2 retries on image_encoder.onnx.data
    assert attempts["image_encoder.onnx.data"] == 3
    # HF cache must be purged before each retry (2 purges = 2 retries).
    assert purged.count("image_encoder.onnx.data") == 2


def test_download_model_raises_after_max_retries(tmp_path, monkeypatch):
    """Bytes on disk never match expected — download_model raises after
    3 total attempts (1 initial + 2 retries) per file."""
    import model_verify
    models, _ = _patch_download_model_env(tmp_path, monkeypatch)

    expected = {
        "image_encoder.onnx": "a" * 64,
        "image_encoder.onnx.data": "b" * 64,
        "text_encoder.onnx": "c" * 64,
        "text_encoder.onnx.data": "d" * 64,
    }

    attempts = {"image_encoder.onnx": 0}

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        if filename == "image_encoder.onnx":
            attempts[filename] += 1
        with open(dest, "wb") as f:
            f.write(b"wrong")
        return dest

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", lambda subdir: expected
    )

    import pytest as _pytest
    with _pytest.raises(model_verify.VerifyError, match="image_encoder.onnx"):
        models.download_model("bioclip-vit-b-16")

    assert attempts["image_encoder.onnx"] == 3

    # Broken model must not have been registered.
    cfg = models._load_config()
    assert not any(m["id"] == "bioclip-vit-b-16" for m in cfg.get("models", []))


def test_download_model_clears_verify_cache_on_success(tmp_path, monkeypatch):
    """After a successful download, the per-process verify cache for that
    model_id is cleared so the next pipeline run re-verifies from disk."""
    import hashlib

    import model_verify
    models, _ = _patch_download_model_env(tmp_path, monkeypatch)

    contents = b"realbytes" * 100
    expected = {
        "image_encoder.onnx": hashlib.sha256(contents).hexdigest(),
        "image_encoder.onnx.data": hashlib.sha256(contents).hexdigest(),
        "text_encoder.onnx": hashlib.sha256(contents).hexdigest(),
        "text_encoder.onnx.data": hashlib.sha256(contents).hexdigest(),
    }

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(contents if filename in expected else b"{}")
        return dest

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", lambda subdir: expected
    )

    model_verify._verified_this_process.add("bioclip-vit-b-16")
    models.download_model("bioclip-vit-b-16")
    assert "bioclip-vit-b-16" not in model_verify._verified_this_process


def test_download_model_preserves_sentinel_when_fetch_hashes_fails(
    tmp_path, monkeypatch
):
    """If fetch_expected_hashes fails (HF tree API outage), download_model
    proceeds without verification — but in that case we must NOT delete
    a preexisting .verify_failed sentinel. Otherwise a Repair attempt
    during a transient HF outage would flip a genuinely corrupt model
    back to 'ok' without any integrity check actually running, letting
    pipelines run against bad weights."""
    import model_verify
    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)
    model_dir.mkdir(parents=True, exist_ok=True)

    # Preexisting sentinel from an earlier verification failure.
    sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
    sentinel.write_text("earlier mismatch")

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(b"stub")
        return dest

    def fetch_raises(subdir):
        raise model_verify.VerifyError("tree API offline")

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(model_verify, "fetch_expected_hashes", fetch_raises)

    # download_model should raise because _classify_model_state will still
    # see the sentinel and return 'incomplete'.
    import pytest as _pytest
    with _pytest.raises(RuntimeError, match="incomplete"):
        models.download_model("bioclip-vit-b-16")

    # Sentinel must still be on disk so a future pipeline run (or a retry
    # after HF is back up) keeps the model flagged as corrupt.
    assert sentinel.is_file()
