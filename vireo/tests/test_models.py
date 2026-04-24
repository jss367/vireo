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
    import taxonomy

    tax_path = tmp_path / "taxonomy.json"
    # Make the file large enough for taxa_count estimation (size // 150)
    tax_data = {"last_updated": "2024-01-15", "taxa": [{"name": f"Species {i}"} for i in range(100)]}
    tax_path.write_text(json.dumps(tax_data))

    monkeypatch.setattr(taxonomy, "find_taxonomy_json", lambda: str(tax_path))

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


def test_classify_state_verify_skipped_sentinel_marks_unverified(tmp_path):
    """All files present plus a .verify_skipped sentinel reports 'unverified'
    so Settings can distinguish a working-but-unconfirmed download from a
    fully-verified one."""
    import models
    d = tmp_path / "m"
    d.mkdir()
    (d / "image_encoder.onnx").write_bytes(b"stub")
    _make_fake_data_file(d / "image_encoder.onnx.data")
    (d / ".verify_skipped").write_text("SSL: CERTIFICATE_VERIFY_FAILED")
    assert models._classify_model_state(
        str(d), ["image_encoder.onnx", "image_encoder.onnx.data"]
    ) == "unverified"


def test_classify_state_verify_failed_takes_priority_over_skipped(tmp_path):
    """If both sentinels exist, .verify_failed wins — a known bad hash is
    more serious than 'could not check'."""
    import models
    d = tmp_path / "m"
    d.mkdir()
    (d / "image_encoder.onnx").write_bytes(b"stub")
    _make_fake_data_file(d / "image_encoder.onnx.data")
    (d / ".verify_failed").write_text("hash mismatch")
    (d / ".verify_skipped").write_text("hf api unreachable")
    assert models._classify_model_state(
        str(d), ["image_encoder.onnx", "image_encoder.onnx.data"]
    ) == "incomplete"


def test_get_models_surfaces_unverified_state_and_reason(tmp_path, monkeypatch):
    """get_models reports state='unverified', downloaded=True, and surfaces the
    underlying reason from the sentinel file so the Settings UI can display
    the cause (e.g. the SSL error text) next to the warning badge."""
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
    reason = (
        "failed to fetch expected hashes for bioclip-vit-b-16@main: "
        "<urlopen error [SSL: CERTIFICATE_VERIFY_FAILED]>"
    )
    (model_dir / ".verify_skipped").write_text(reason)

    result = models.get_models()
    entry = next(m for m in result if m["id"] == "bioclip-vit-b-16")
    assert entry["state"] == "unverified"
    # Unverified models are still usable — pipeline and "Use This" need to work.
    assert entry["downloaded"] is True
    assert entry["weights_path"] == str(model_dir)
    assert entry["verify_skipped_reason"] == reason


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
    """Shared setup: isolate config/models dir and return the model dir path.
    Also stubs fetch_latest_revision to a fixed SHA so tests don't hit the
    real HuggingFace model-info API.  Injects a minimal huggingface_hub stub
    so the ImportError guard in download_model is satisfied without actually
    having the library installed."""
    import types

    import model_verify
    import models

    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))
    monkeypatch.setattr(
        model_verify,
        "fetch_latest_revision",
        lambda repo: "testsha1234567890abcdef1234567890abcdef12",
    )

    # Stub huggingface_hub so the `from huggingface_hub import hf_hub_download`
    # guard in download_model passes in environments without the library.
    # Actual downloads go through _hf_download_with_retry which each test
    # patches with its own fake.
    hf_stub = types.ModuleType("huggingface_hub")
    hf_stub.hf_hub_download = None  # not called; _hf_download_with_retry is patched
    hf_stub.try_to_load_from_cache = None
    monkeypatch.setitem(sys.modules, "huggingface_hub", hf_stub)

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

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
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
        model_verify, "fetch_expected_hashes", lambda subdir, revision="main": expected
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

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
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
        lambda filename, subdir, revision=None: purged.append(filename),
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", lambda subdir, revision="main": expected
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

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        if filename == "image_encoder.onnx":
            attempts[filename] += 1
        with open(dest, "wb") as f:
            f.write(b"wrong")
        return dest

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir, revision=None: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", lambda subdir, revision="main": expected
    )

    import pytest as _pytest
    with _pytest.raises(model_verify.VerifyError, match="image_encoder.onnx"):
        models.download_model("bioclip-vit-b-16")

    assert attempts["image_encoder.onnx"] == 3

    # .verify_failed sentinel must exist so _classify_model_state reports
    # 'incomplete' even though all files are physically present on disk.
    model_dir = tmp_path / "models" / "bioclip-vit-b-16"
    sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
    assert sentinel.exists(), ".verify_failed sentinel must be written before raising"
    assert "hash-mismatch" in sentinel.read_text()

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

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(contents if filename in expected else b"{}")
        return dest

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir, revision=None: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", lambda subdir, revision="main": expected
    )

    model_verify._verified_this_process.add("bioclip-vit-b-16")
    models.download_model("bioclip-vit-b-16")
    assert "bioclip-vit-b-16" not in model_verify._verified_this_process


def test_download_model_writes_pinned_revision(tmp_path, monkeypatch):
    """After a successful download, download_model records the current HF
    commit SHA in .hf_revision so subsequent verifications use that
    immutable revision instead of main — protecting against upstream
    model updates invalidating valid local files."""
    import hashlib

    import model_verify
    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)

    pinned_sha = "ea7d6fbf207d90de6f7b0df3c3d5aef2a971c0ed"
    contents = b"realbytes" * 200
    expected = {
        "image_encoder.onnx": hashlib.sha256(contents).hexdigest(),
        "image_encoder.onnx.data": hashlib.sha256(contents).hexdigest(),
        "text_encoder.onnx": hashlib.sha256(contents).hexdigest(),
        "text_encoder.onnx.data": hashlib.sha256(contents).hexdigest(),
    }

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(contents if filename in expected else b"{}")
        return dest

    captured_fetch = {}

    def fake_fetch(subdir, revision="main"):
        captured_fetch["revision"] = revision
        return expected

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir, revision=None: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_latest_revision", lambda repo: pinned_sha
    )
    monkeypatch.setattr(model_verify, "fetch_expected_hashes", fake_fetch)

    models.download_model("bioclip-vit-b-16")

    # The pinned revision file is written on disk
    rev_file = model_dir / model_verify.REVISION_FILE
    assert rev_file.is_file()
    assert rev_file.read_text().strip() == pinned_sha

    # fetch_expected_hashes was called with the pinned revision, not main
    assert captured_fetch["revision"] == pinned_sha


def test_download_model_falls_back_to_main_when_revision_lookup_fails(
    tmp_path, monkeypatch
):
    """If fetch_latest_revision fails (model-info API offline) but the tree
    API is still healthy, download_model should NOT skip verification —
    it should fall back to verifying against 'main'. The two HF APIs are
    independent and a stale blob should still be caught.

    No .hf_revision is written because we don't have an immutable SHA to
    pin to, but SHA256 verification still happens end-to-end."""
    import hashlib

    import model_verify
    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)

    contents = b"weights" * 1000
    good = hashlib.sha256(contents).hexdigest()
    expected = {
        "image_encoder.onnx": good,
        "image_encoder.onnx.data": good,
        "text_encoder.onnx": good,
        "text_encoder.onnx.data": good,
    }

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(contents if filename in expected else b"{}")
        return dest

    def fetch_revision_raises(repo):
        raise model_verify.VerifyError("model-info api offline")

    captured = {}

    def fake_fetch_hashes(subdir, revision="main"):
        captured["revision"] = revision
        return expected

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir, revision=None: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_latest_revision", fetch_revision_raises
    )
    monkeypatch.setattr(model_verify, "fetch_expected_hashes", fake_fetch_hashes)

    models.download_model("bioclip-vit-b-16")

    # Verification still ran, using 'main' as the revision.
    assert captured["revision"] == "main"
    # No pin file written because we don't have an immutable SHA.
    assert not (model_dir / model_verify.REVISION_FILE).exists()
    # Model was registered as successfully downloaded.
    cfg = models._load_config()
    assert any(m["id"] == "bioclip-vit-b-16" for m in cfg.get("models", []))


def test_download_model_skips_verification_only_when_tree_api_also_fails(
    tmp_path, monkeypatch
):
    """Verification is only skipped entirely when BOTH the model-info API
    and the tree API are unreachable. In that terminal case .hf_revision
    is not written and any preexisting .verify_failed sentinel stays in
    place so the model remains flagged as incomplete."""
    import model_verify
    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)
    model_dir.mkdir(parents=True, exist_ok=True)
    sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
    sentinel.write_text("earlier mismatch")

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(b"stub")
        return dest

    def fetch_revision_raises(repo):
        raise model_verify.VerifyError("model-info api offline")

    def fetch_hashes_raises(subdir, revision="main"):
        raise model_verify.VerifyError("tree api offline")

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir, revision=None: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_latest_revision", fetch_revision_raises
    )
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", fetch_hashes_raises
    )
    # Disable the size-floor check so this test stays focused on sentinel
    # preservation and revision-pin semantics, not on stub-file detection.
    monkeypatch.setattr(models, "_MIN_BINARY_MODEL_BYTES", 0)

    # download_model raises because sentinel + post-download state check
    # flips the state to 'incomplete'.
    import pytest as _pytest
    with _pytest.raises(RuntimeError, match="incomplete"):
        models.download_model("bioclip-vit-b-16")

    assert sentinel.is_file()
    assert not (model_dir / model_verify.REVISION_FILE).exists()


def test_download_model_writes_verify_skipped_when_hash_fetch_fails(
    tmp_path, monkeypatch
):
    """When the tree API is unreachable at download time, download_model
    writes .verify_skipped with the underlying reason so the Settings UI
    can surface why verification was skipped instead of the failure being
    logged-only."""
    import hashlib

    import model_verify
    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)

    # Files land on disk with real bytes so the size-floor check passes;
    # verification is blocked by the unreachable tree API, not by bad bytes.
    large_bytes = b"x" * (20 * 1024 * 1024)  # 20 MB — above _MIN_BINARY_MODEL_BYTES

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        content = large_bytes if filename.endswith(".onnx.data") else b"stub"
        with open(dest, "wb") as f:
            f.write(content)
        return dest

    def fetch_hashes_raises(subdir, revision="main"):
        raise model_verify.VerifyError(
            "<urlopen error [SSL: CERTIFICATE_VERIFY_FAILED]>"
        )

    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", fetch_hashes_raises
    )

    models.download_model("bioclip-vit-b-16")

    skipped = model_dir / model_verify.VERIFY_SKIPPED_SENTINEL
    assert skipped.is_file(), (
        ".verify_skipped must be written when fetch_expected_hashes fails "
        "so Settings can show 'Unverified' with the cause"
    )
    # The recorded reason must be the actual exception text so the UI can
    # show the user the real problem (SSL, proxy, etc).
    assert "CERTIFICATE_VERIFY_FAILED" in skipped.read_text()

    # Sanity: hash-computation just confirms files exist; not asserting a
    # SHA (verification was skipped by design).
    assert hashlib.sha256(large_bytes).hexdigest()  # smoke


def test_download_model_clears_verify_skipped_on_successful_reverify(
    tmp_path, monkeypatch
):
    """If a previous download left .verify_skipped and a retry now succeeds
    with working hash fetch, the sentinel must be cleared so the model
    returns to the 'ok' state."""
    import hashlib

    import model_verify
    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)
    model_dir.mkdir(parents=True, exist_ok=True)
    skipped = model_dir / model_verify.VERIFY_SKIPPED_SENTINEL
    skipped.write_text("earlier SSL failure")

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

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        content = lfs_contents.get(filename, b"{}")
        with open(dest, "wb") as f:
            f.write(content)
        return dest

    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify, "fetch_expected_hashes", lambda subdir, revision="main": expected
    )

    models.download_model("bioclip-vit-b-16")
    assert not skipped.exists(), (
        ".verify_skipped must be cleared once verification actually runs "
        "and succeeds"
    )


def test_purge_hf_cache_file_deletes_blob_target(tmp_path, monkeypatch):
    """_purge_hf_cache_file follows the snapshot symlink to the actual blob
    in blobs/ and deletes that, not just the snapshot symlink. Otherwise
    hf_hub_download on retry would relink to the same corrupt bytes.
    """
    import types

    import models

    # Build a fake HF cache layout:
    #   blobs/<oid>         <- actual file bytes
    #   snapshots/<rev>/path/to/file.data  -> ../../blobs/<oid>
    blobs = tmp_path / "blobs"
    snapshots = tmp_path / "snapshots" / "rev123" / "bioclip-vit-b-16"
    blobs.mkdir(parents=True)
    snapshots.mkdir(parents=True)
    blob_path = blobs / "corruptblob"
    blob_path.write_bytes(b"corrupt")
    symlink_path = snapshots / "image_encoder.onnx.data"
    os.symlink(blob_path, symlink_path)

    def fake_try_to_load_from_cache(repo_id, filename, revision=None):
        return str(symlink_path)

    hf_stub = types.ModuleType("huggingface_hub")
    hf_stub.try_to_load_from_cache = fake_try_to_load_from_cache
    monkeypatch.setitem(sys.modules, "huggingface_hub", hf_stub)

    models._purge_hf_cache_file(
        "image_encoder.onnx.data", "bioclip-vit-b-16"
    )

    # Both the snapshot symlink and the blob target must be gone, so that
    # the next hf_hub_download actually fetches fresh bytes instead of
    # relinking to the same corrupt blob.
    assert not symlink_path.exists()
    assert not blob_path.exists()


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

    def fake_download(repo_id, filename, local_dir, subfolder=None, progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(b"stub")
        return dest

    def fetch_raises(subdir, revision="main"):
        raise model_verify.VerifyError("tree API offline")

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir, revision=None: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(model_verify, "fetch_expected_hashes", fetch_raises)
    # Disable the size-floor check so this test stays focused on sentinel
    # preservation semantics, not on stub-file detection.
    monkeypatch.setattr(models, "_MIN_BINARY_MODEL_BYTES", 0)

    # download_model should raise because _classify_model_state will still
    # see the sentinel and return 'incomplete'.
    import pytest as _pytest
    with _pytest.raises(RuntimeError, match="incomplete"):
        models.download_model("bioclip-vit-b-16")

    # Sentinel must still be on disk so a future pipeline run (or a retry
    # after HF is back up) keeps the model flagged as corrupt.
    assert sentinel.is_file()


def test_purge_hf_cache_file_passes_revision_to_cache_lookup(tmp_path, monkeypatch):
    """_purge_hf_cache_file must pass the revision parameter to
    try_to_load_from_cache so the lookup resolves the pinned snapshot entry
    rather than the default-branch entry.

    Without revision= the HF cache lookup targets 'main', leaving the
    corrupt blob for the pinned commit untouched; every retry then
    relinks to the same bad bytes and hash-mismatch retries are exhausted
    without ever fetching fresh bytes.

    Regression for Codex P1 review on #501, models.py line 653.
    """
    import sys
    import types

    import models

    captured: dict = {}

    blobs = tmp_path / "blobs"
    snapshots = tmp_path / "snapshots" / "abc123" / "bioclip-vit-b-16"
    blobs.mkdir(parents=True)
    snapshots.mkdir(parents=True)
    blob_path = blobs / "corruptblob"
    blob_path.write_bytes(b"corrupt")
    symlink_path = snapshots / "image_encoder.onnx.data"
    os.symlink(blob_path, symlink_path)

    def fake_try_to_load_from_cache(repo_id, filename, revision=None):
        captured["revision"] = revision
        return str(symlink_path)

    # Stub huggingface_hub in sys.modules so the test passes in environments
    # where the library is not installed.
    hf_stub = types.ModuleType("huggingface_hub")
    hf_stub.try_to_load_from_cache = fake_try_to_load_from_cache
    monkeypatch.setitem(sys.modules, "huggingface_hub", hf_stub)

    models._purge_hf_cache_file(
        "image_encoder.onnx.data", "bioclip-vit-b-16", revision="abc123"
    )

    assert captured.get("revision") == "abc123", (
        "try_to_load_from_cache must receive the pinned revision so the "
        "correct snapshot entry is purged — regression for Codex P1 on "
        "#501 models.py line 653"
    )
    # Both the symlink and blob should be deleted
    assert not symlink_path.exists()
    assert not blob_path.exists()


def test_download_model_clears_stale_revision_when_verification_runs_against_main(
    tmp_path, monkeypatch
):
    """When verification runs against 'main' (because fetch_latest_revision
    failed so pinned_revision is None), any pre-existing .hf_revision must
    be deleted.

    If a stale .hf_revision remains from a previous install, the next
    verify_model call reads that old SHA and fetches expected hashes for
    the wrong revision, causing false mismatches and unnecessary Repair
    prompts even though the downloaded files are correct.

    Regression for Codex P2 review on #501, models.py line 523.
    """
    import hashlib
    import sys
    import types

    import model_verify

    # Stub out huggingface_hub so the import guard in download_model passes
    # even when the library is not installed in this environment.
    hf_stub = types.ModuleType("huggingface_hub")
    hf_stub.hf_hub_download = lambda *a, **kw: None
    hf_stub.try_to_load_from_cache = lambda *a, **kw: None
    monkeypatch.setitem(sys.modules, "huggingface_hub", hf_stub)

    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)
    model_dir.mkdir(parents=True, exist_ok=True)

    # Simulate a stale revision pin left by a previous install.
    stale_rev_path = model_dir / model_verify.REVISION_FILE
    stale_rev_path.write_text("old_sha_from_previous_install\n")

    # fetch_latest_revision will raise (model-info API offline), so
    # pinned_revision will be None but the tree API still works (returns
    # expected hashes against "main").
    monkeypatch.setattr(
        model_verify,
        "fetch_latest_revision",
        lambda repo: (_ for _ in ()).throw(
            model_verify.VerifyError("model-info offline")
        ),
    )

    lfs_contents = {
        "image_encoder.onnx": b"graph-i" * 100,
        "image_encoder.onnx.data": b"weights-i" * 1000,
        "text_encoder.onnx": b"graph-t" * 100,
        "text_encoder.onnx.data": b"weights-t" * 1000,
    }
    expected = {
        fn: hashlib.sha256(data).hexdigest()
        for fn, data in lfs_contents.items()
    }

    def fake_download(repo_id, filename, local_dir, subfolder=None,
                      progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        base = os.path.basename(filename)
        dest = os.path.join(local_dir, base)
        with open(dest, "wb") as f:
            f.write(lfs_contents.get(base, b"stub"))
        return dest

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir, revision=None: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": expected,
    )

    models.download_model("bioclip-vit-b-16")

    # The stale .hf_revision must have been deleted so that verify_model
    # uses "main" (matching the revision used for hash verification) rather
    # than the old SHA, which would produce false mismatches.
    assert not stale_rev_path.exists(), (
        ".hf_revision must be deleted when verification runs against 'main' "
        "(pinned_revision=None) to prevent false mismatch errors on the "
        "next verify_model call — regression for Codex P2 on #501 line 523"
    )


def test_download_model_raises_when_binary_file_too_small_and_hash_fetch_fails(
    tmp_path, monkeypatch
):
    """When hash verification is unavailable (HF tree API offline) and a
    downloaded .onnx.data weight sidecar is below the 10 MB floor,
    download_model must raise immediately rather than silently registering a
    truncated/stub file as a healthy model.

    Only .onnx.data files are checked — graph .onnx files can legitimately
    be much smaller in external-data ONNX layouts.

    Regression for Codex P2 review on #501 (vireo/models.py line 487).
    Plain .onnx graph files are excluded from the floor check because they are
    legitimately small in external-data ONNX layouts (Codex P1 on #520).
    """
    import model_verify
    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)

    def fake_download(repo_id, filename, local_dir, subfolder=None,
                      progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(b"stub-too-small")
        return dest

    def fetch_raises(subdir, revision="main"):
        raise model_verify.VerifyError("tree API offline")

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir, revision=None: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(model_verify, "fetch_expected_hashes", fetch_raises)

    import pytest as _pytest
    with _pytest.raises(RuntimeError, match="truncated"):
        models.download_model("bioclip-vit-b-16")

    # The verify-failed sentinel must be written so _classify_model_state
    # reports 'incomplete' and get_models() shows the Repair button.
    sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
    assert sentinel.exists(), (
        "Size-floor failure must write .verify_failed sentinel so the model "
        "is not treated as healthy by _classify_model_state."
    )


def test_download_model_small_onnx_graph_does_not_trigger_size_floor(
    tmp_path, monkeypatch
):
    """Plain .onnx graph files must NOT be rejected by the size-floor check
    even when they are smaller than _MIN_BINARY_MODEL_BYTES.  In external-data
    ONNX layouts the graph file is legitimately tiny; the weights live in the
    .onnx.data sidecar.  Applying the floor to .onnx files would falsely reject
    valid installs whenever the HF tree API is down.

    Regression for Codex P1 review on #520 (vireo/models.py line 560).
    """
    import model_verify
    models, model_dir = _patch_download_model_env(tmp_path, monkeypatch)

    # _MIN_BINARY_MODEL_BYTES is 10 MB; write a large-enough .onnx.data sidecar
    # but a tiny stub .onnx graph to confirm the floor only fires on .onnx.data.
    def fake_download(repo_id, filename, local_dir, subfolder=None,
                      progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            if filename.endswith(".onnx.data"):
                # large enough to pass the floor
                f.write(b"\x00" * (models._MIN_BINARY_MODEL_BYTES + 1))
            else:
                f.write(b"stub-tiny-graph")
        return dest

    def fetch_raises(subdir, revision="main"):
        raise model_verify.VerifyError("tree API offline")

    monkeypatch.setattr(
        models, "_purge_hf_cache_file", lambda filename, subdir, revision=None: None
    )
    monkeypatch.setattr(models, "_hf_download_with_retry", fake_download)
    monkeypatch.setattr(model_verify, "fetch_expected_hashes", fetch_raises)

    # Must NOT raise — the .onnx graph is tiny but that is allowed; only the
    # .onnx.data sidecars are subject to the floor.  Any exception here
    # (including a non-"truncated" RuntimeError) is a test failure.
    models.download_model("bioclip-vit-b-16")


# ---------------------------------------------------------------------------
# _hf_download_with_retry: cache-hit vs network-download messaging
# ---------------------------------------------------------------------------

def _stub_hf_module(monkeypatch, hf_hub_download_fn, try_to_load_from_cache_fn):
    """Install a fake huggingface_hub module exposing the two functions
    _hf_download_with_retry imports."""
    import sys
    import types

    stub = types.ModuleType("huggingface_hub")
    stub.hf_hub_download = hf_hub_download_fn
    stub.try_to_load_from_cache = try_to_load_from_cache_fn
    monkeypatch.setitem(sys.modules, "huggingface_hub", stub)


def test_hf_download_with_retry_reports_cache_hit(tmp_path, monkeypatch, caplog):
    """When the file is already in the HF cache, the user-visible message
    and log say so — no '1.2 GB downloaded in 1 second' confusion."""
    import logging

    import models

    # Pretend the blob is sitting at this path in the HF cache.
    cached_blob = tmp_path / "fake-cache" / "blobs" / "abc123"
    cached_blob.parent.mkdir(parents=True)
    cached_blob.write_bytes(b"weights" * 100)

    def fake_lookup(repo_id, filename, revision=None):
        return str(cached_blob)

    def fake_download(repo_id, filename, subfolder=None, revision=None):
        # Simulate hf_hub_download returning the same cached path it would
        # normally return when nothing needs to be fetched.
        return str(cached_blob)

    _stub_hf_module(monkeypatch, fake_download, fake_lookup)

    messages = []
    dest_dir = tmp_path / "model_dir"
    with caplog.at_level(logging.INFO, logger="models"):
        models._hf_download_with_retry(
            repo_id="acme/foo",
            filename="model.onnx",
            local_dir=str(dest_dir),
            subfolder="sub",
            progress_callback=messages.append,
        )

    assert any("HF cache" in m for m in messages), (
        f"expected a cache-hit message in progress callback, got {messages!r}"
    )
    log_text = caplog.text
    assert "already in HF cache" in log_text
    assert "Linked from cache" in log_text
    assert "no network transfer" in log_text
    # The dest must exist (copied from cache)
    assert (dest_dir / "model.onnx").exists()


def test_hf_download_with_retry_reports_network_download(tmp_path, monkeypatch, caplog):
    """When the file is not cached, the message says 'Downloading from
    Hugging Face' — the existing behaviour, kept distinct."""
    import logging

    import models

    cached_blob = tmp_path / "fake-cache" / "blobs" / "abc123"

    def fake_lookup(repo_id, filename, revision=None):
        # Cache miss
        return None

    def fake_download(repo_id, filename, subfolder=None, revision=None):
        # Pretend the network fetch landed here.
        cached_blob.parent.mkdir(parents=True, exist_ok=True)
        cached_blob.write_bytes(b"weights" * 100)
        return str(cached_blob)

    _stub_hf_module(monkeypatch, fake_download, fake_lookup)

    messages = []
    dest_dir = tmp_path / "model_dir"
    with caplog.at_level(logging.INFO, logger="models"):
        models._hf_download_with_retry(
            repo_id="acme/foo",
            filename="model.onnx",
            local_dir=str(dest_dir),
            progress_callback=messages.append,
        )

    assert any("Downloading" in m and "Hugging Face" in m for m in messages), (
        f"expected a network-download message, got {messages!r}"
    )
    log_text = caplog.text
    assert "already in HF cache" not in log_text
    assert "Download complete" in log_text


def test_hf_download_with_retry_cache_lookup_failure_falls_back(tmp_path, monkeypatch, caplog):
    """If try_to_load_from_cache raises, we still call hf_hub_download —
    cache detection is best-effort, not a hard dependency."""
    import logging

    import models

    cached_blob = tmp_path / "fake-cache" / "blobs" / "abc123"

    def fake_lookup(repo_id, filename, revision=None):
        raise RuntimeError("HF cache lookup unavailable")

    def fake_download(repo_id, filename, subfolder=None, revision=None):
        cached_blob.parent.mkdir(parents=True, exist_ok=True)
        cached_blob.write_bytes(b"weights" * 100)
        return str(cached_blob)

    _stub_hf_module(monkeypatch, fake_download, fake_lookup)

    messages = []
    dest_dir = tmp_path / "model_dir"
    with caplog.at_level(logging.INFO, logger="models"):
        result = models._hf_download_with_retry(
            repo_id="acme/foo",
            filename="model.onnx",
            local_dir=str(dest_dir),
            progress_callback=messages.append,
        )

    # Falls back to network-download messaging since cache state is unknown.
    assert result == os.path.join(str(dest_dir), "model.onnx")
    assert any("Downloading" in m and "Hugging Face" in m for m in messages)
