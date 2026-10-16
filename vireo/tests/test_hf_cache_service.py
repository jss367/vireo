"""services.hf_cache: per-model inventory and cleanup of the Hugging Face cache.

Builds the hub cache layout by hand (``blobs/<sha>`` plus
``snapshots/<rev>/<path>`` symlinks) so the tests never touch the network or
the developer's real cache.
"""

import hashlib
import os
import time

import pytest

ONNX_DIR = "models--jss367--vireo-onnx-models"


def _symlink(target, link):
    try:
        os.symlink(target, link)
    except (OSError, NotImplementedError):  # pragma: no cover - Windows without privilege
        pytest.skip("symlinks unavailable on this platform")


class FakeHub:
    def __init__(self, root):
        self.root = root

    def add(self, relpath, content, rev="rev1", repo=ONNX_DIR, link=True):
        """Store ``content`` as a blob and point ``snapshots/<rev>/<relpath>`` at it."""
        repo_dir = self.root / repo
        blob = repo_dir / "blobs" / hashlib.sha256(content).hexdigest()
        blob.parent.mkdir(parents=True, exist_ok=True)
        if not blob.exists():
            blob.write_bytes(content)
        snap = repo_dir / "snapshots" / rev / relpath
        snap.parent.mkdir(parents=True, exist_ok=True)
        if link:
            _symlink(os.path.relpath(blob, snap.parent), snap)
        else:
            snap.write_bytes(content)
            blob.unlink()
        return blob

    def blob(self, name, content, repo=ONNX_DIR):
        """A blob no snapshot refers to (interrupted or superseded download)."""
        path = self.root / repo / "blobs" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path


@pytest.fixture
def hub(tmp_path, monkeypatch):
    import models

    cache = tmp_path / "hub"
    cache.mkdir()
    monkeypatch.setenv("HF_HUB_CACHE", str(cache))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    return FakeHub(cache)


def _install(tmp_path, dirname):
    path = tmp_path / "models" / dirname
    path.mkdir(parents=True)
    (path / "model.onnx").write_bytes(b"installed")
    return path


def _entries(inv):
    return {e["key"]: e for e in inv["entries"]}


def test_inventory_splits_installed_from_unused(hub, tmp_path):
    from services import hf_cache

    hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 5000)
    hub.add("dinov2-vit-b14/model.onnx", b"d" * 300)
    _install(tmp_path, "dinov2-vit-b14")

    inv = hf_cache.inventory()
    entries = _entries(inv)
    bioclip = entries["jss367/vireo-onnx-models/bioclip-2.5-vith14"]
    dino = entries["jss367/vireo-onnx-models/dinov2-vit-b14"]
    assert bioclip["installed"] is False and bioclip["size"] == 5000
    assert "bioclip-2.5-vith14" in bioclip["name"]
    assert dino["installed"] is True and dino["size"] == 300
    assert dino["install_path"] == str(tmp_path / "models" / "dinov2-vit-b14")
    assert inv["unused_size"] == 5000
    assert inv["installed_size"] == 300
    assert inv["entries"][0] is inv["entries"][0] and inv["entries"][0]["size"] == 5000
    assert inv["path"] == str(hub.root)


def test_inventory_maps_known_model_id_to_its_hf_subfolder(hub, tmp_path):
    """EVA02 installs under its model id, which differs from its HF subfolder."""
    from services import hf_cache

    hub.add("timm-eva02-large-inat21/model.onnx.data", b"e" * 100)
    _install(tmp_path, "timm-inat21-eva02-l")

    entry = _entries(hf_cache.inventory())["jss367/vireo-onnx-models/timm-eva02-large-inat21"]
    assert entry["installed"] is True
    assert entry["install_path"].endswith("timm-inat21-eva02-l")


def test_inventory_counts_a_blob_once_across_snapshots(hub):
    from services import hf_cache

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 400, rev="old")
    hub.add("sam2-small/mask_decoder.onnx", b"m" * 400, rev="new")

    entry = _entries(hf_cache.inventory())["jss367/vireo-onnx-models/sam2-small"]
    assert entry["size"] == 400
    assert entry["file_count"] == 2


def test_inventory_reports_unreferenced_blobs_but_not_fresh_partials(hub):
    from services import hf_cache

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)
    hub.blob("abandoned", b"a" * 700)
    hub.blob("fresh.incomplete", b"f" * 50)
    old = hub.blob("old.incomplete", b"o" * 60)
    stale = time.time() - hf_cache.INCOMPLETE_GRACE_SECS - 60
    os.utime(old, (stale, stale))

    entry = _entries(hf_cache.inventory())["jss367/vireo-onnx-models#unreferenced"]
    assert entry["installed"] is False
    assert entry["size"] == 760
    assert entry["file_count"] == 2


def test_inventory_ignores_unrelated_repos(hub):
    from services import hf_cache
    from services.storage_paths import directory_bytes

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)
    hub.add("model.safetensors", b"x" * 9000, repo="models--somebody--unrelated")

    inv = hf_cache.inventory()
    assert list(_entries(inv)) == ["jss367/vireo-onnx-models/sam2-small"]
    assert inv["size"] == directory_bytes(str(hub.root / ONNX_DIR))


def test_remove_unused_keeps_installed_entries_and_shared_blobs(hub, tmp_path):
    from services import hf_cache

    shared = hub.add("bioclip-2.5-vith14/config.json", b"{}")
    hub.add("dinov2-vit-b14/config.json", b"{}")  # same bytes: same blob
    big = hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 5000)
    dino_blob = hub.add("dinov2-vit-b14/model.onnx", b"d" * 300)
    _install(tmp_path, "dinov2-vit-b14")

    result = hf_cache.remove_unused()

    assert result["removed"] == ["jss367/vireo-onnx-models/bioclip-2.5-vith14"]
    assert result["freed"] == 5000
    assert not big.exists()
    assert shared.exists(), "a blob the installed model still links must survive"
    assert dino_blob.exists()
    snapshot = hub.root / ONNX_DIR / "snapshots" / "rev1"
    assert not (snapshot / "bioclip-2.5-vith14").exists()
    assert (snapshot / "dinov2-vit-b14" / "model.onnx").exists()
    assert hf_cache.inventory()["unused_size"] == 0


def test_remove_drops_the_repo_dir_when_its_last_snapshot_goes(hub):
    from services import hf_cache

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 400)
    (hub.root / ONNX_DIR / "refs" / "main").parent.mkdir(parents=True)
    (hub.root / ONNX_DIR / "refs" / "main").write_text("rev1")

    result = hf_cache.remove(["jss367/vireo-onnx-models/sam2-small"])

    assert result == {"removed": ["jss367/vireo-onnx-models/sam2-small"], "freed": 400}
    assert not (hub.root / ONNX_DIR).exists()
    assert hf_cache.inventory()["entries"] == []


def test_remove_rejects_a_key_the_inventory_did_not_hand_out(hub):
    from services import hf_cache

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 400)
    with pytest.raises(KeyError):
        hf_cache.remove(["jss367/vireo-onnx-models/../../etc"])
    assert (hub.root / ONNX_DIR / "snapshots" / "rev1" / "sam2-small").exists()


def test_remove_handles_a_plain_file_layout(hub):
    """Where symlinks are unavailable HF copies files into the snapshot."""
    from services import hf_cache

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 400, link=False)

    inv = hf_cache.inventory()
    assert _entries(inv)["jss367/vireo-onnx-models/sam2-small"]["size"] == 400
    result = hf_cache.remove(["jss367/vireo-onnx-models/sam2-small"])
    assert result["freed"] == 400
    assert not (hub.root / ONNX_DIR).exists()


def test_remove_unreferenced_entry_deletes_only_dangling_blobs(hub):
    from services import hf_cache

    live = hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)
    dangling = hub.blob("abandoned", b"a" * 700)

    result = hf_cache.remove(["jss367/vireo-onnx-models#unreferenced"])

    assert result["freed"] == 700
    assert not dangling.exists()
    assert live.exists()


def test_purge_subfolder_removes_an_installed_entry_too(hub, tmp_path):
    from services import hf_cache

    blob = hub.add("dinov2-vit-b14/model.onnx", b"d" * 300)
    _install(tmp_path, "dinov2-vit-b14")

    assert hf_cache.purge_subfolder("dinov2-vit-b14") == 300
    assert not blob.exists()
    assert hf_cache.purge_subfolder("dinov2-vit-b14") == 0


def test_purge_model_uses_the_known_models_subfolder(hub):
    from services import hf_cache

    blob = hub.add("timm-eva02-large-inat21/model.onnx.data", b"e" * 100)
    other = hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)

    assert hf_cache.purge_model("timm-inat21-eva02-l") == 100
    assert not blob.exists()
    assert other.exists()
    assert hf_cache.purge_model("custom-abc") == 0


def test_remove_model_in_settings_takes_the_cache_copy_along(hub, tmp_path):
    """Settings → Models → Remove must not leave the download in the cache."""
    import models
    from services import hf_cache

    blob = hub.add("bioclip-vit-b-16/image_encoder.onnx.data", b"b" * 900)
    install = _install(tmp_path, "bioclip-vit-b-16")
    assert _entries(hf_cache.inventory())["jss367/vireo-onnx-models/bioclip-vit-b-16"]["installed"]

    result = models.remove_model("bioclip-vit-b-16")

    assert result == {"files_deleted": True, "kept_path": None}
    assert not install.exists()
    assert not blob.exists()
    assert hf_cache.inventory()["entries"] == []


def test_remove_registered_model_purges_cache_after_unregistering(hub, tmp_path):
    import models
    from services import hf_cache

    blob = hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 900)
    install = _install(tmp_path, "bioclip-2.5-vith14")
    models._save_config({
        "models": [{
            "id": "bioclip-2.5-vith14", "name": "BioCLIP 2.5",
            "weights_path": str(install), "managed": True,
        }],
        "active_model": "bioclip-2.5-vith14",
    })

    result = models.remove_model("bioclip-2.5-vith14")

    assert result["files_deleted"] is True
    assert not blob.exists()
    assert hf_cache.inventory()["entries"] == []
