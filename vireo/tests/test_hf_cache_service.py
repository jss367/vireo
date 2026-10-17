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
    from services import hf_cache, hf_cache_lock

    lock = hf_cache_lock.SharedDownloadLock(str(tmp_path / "cache-access.lock"))
    monkeypatch.setattr(hf_cache_lock, "DOWNLOAD_LOCK", lock)
    monkeypatch.setattr(hf_cache, "DOWNLOAD_LOCK", lock)
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


def test_inventory_ignores_unrelated_imageomics_and_bioclip_repos(hub):
    """A shared cache may hold repositories Vireo never downloaded.

    Earlier revisions matched ``models--imageomics--*`` and
    ``models--bioclip--*`` as a fallback, which would list another Hub
    client's ``imageomics/another-project`` or ``bioclip-foo/bar`` as an
    unused Vireo entry and let ``Remove unused`` evict their blobs.
    """
    from services import hf_cache

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)
    hub.add("some.onnx", b"x" * 500, repo="models--imageomics--another-project")
    hub.add("some.onnx", b"y" * 500, repo="models--bioclip-foo--bar")

    inv = hf_cache.inventory()
    assert list(_entries(inv)) == ["jss367/vireo-onnx-models/sam2-small"]


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_remove_unused_keeps_installed_entries_and_shared_blobs(hub, tmp_path):
    from services import hf_cache

    shared = hub.add("bioclip-2.5-vith14/config.json", b"{}")
    hub.add("dinov2-vit-b14/config.json", b"{}")  # same bytes: same blob
    big = hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 5000)
    dino_blob = hub.add("dinov2-vit-b14/model.onnx", b"d" * 300)
    _install(tmp_path, "dinov2-vit-b14")

    before = _entries(hf_cache.inventory())
    bioclip = before["jss367/vireo-onnx-models/bioclip-2.5-vith14"]
    assert bioclip["size"] == 5000, "a shared blob is not advertised as reclaimable"
    assert bioclip["shared_size"] == 2
    assert hf_cache.inventory()["unused_size"] == 5000

    result = hf_cache.remove_unused()

    assert result == {
        "removed": ["jss367/vireo-onnx-models/bioclip-2.5-vith14"], "failed": [], "freed": 5000,
    }
    assert not big.exists()
    assert shared.exists(), "a blob the installed model still links must survive"
    assert dino_blob.exists()
    snapshot = hub.root / ONNX_DIR / "snapshots" / "rev1"
    assert not (snapshot / "bioclip-2.5-vith14" / "image_encoder.onnx.data").exists()
    assert (snapshot / "dinov2-vit-b14" / "model.onnx").exists()
    assert hf_cache.inventory()["unused_size"] == 0


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_last_snapshot_removal_retains_directory_anchors_and_metadata(hub):
    from services import hf_cache

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 400)
    (hub.root / ONNX_DIR / "refs" / "main").parent.mkdir(parents=True)
    (hub.root / ONNX_DIR / "refs" / "main").write_text("rev1")

    result = hf_cache.remove(["jss367/vireo-onnx-models/sam2-small"])

    assert result == {"removed": ["jss367/vireo-onnx-models/sam2-small"], "failed": [], "freed": 400}
    assert (hub.root / ONNX_DIR / "refs" / "main").read_text() == "rev1"
    assert hf_cache.inventory()["entries"] == []


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_remove_rejects_a_key_the_inventory_did_not_hand_out(hub):
    from services import hf_cache

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 400)
    with pytest.raises(KeyError):
        hf_cache.remove(["jss367/vireo-onnx-models/../../etc"])
    assert (hub.root / ONNX_DIR / "snapshots" / "rev1" / "sam2-small").exists()


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_remove_retains_plain_snapshot_copies_without_a_known_blob_lock(hub):
    """Do not guess a copied snapshot's hub etag or race its writer."""
    from services import hf_cache

    hub.add("sam2-small/mask_decoder.onnx", b"m" * 400, link=False)
    snapshot = hub.root / ONNX_DIR / "snapshots" / "rev1" / "sam2-small" / "mask_decoder.onnx"
    entry = _entries(hf_cache.inventory())["jss367/vireo-onnx-models/sam2-small"]
    assert entry["size"] == 0 and entry["retained_size"] == 400
    assert entry["removable"] is False and hf_cache.inventory()["unused_size"] == 0
    result = hf_cache.remove(["jss367/vireo-onnx-models/sam2-small"])
    assert result["freed"] == 0 and result["removed"] == []
    assert len(result["failed"]) == 1
    assert snapshot.exists()


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_remove_unreferenced_entry_deletes_only_dangling_blobs(hub):
    from services import hf_cache

    live = hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)
    dangling = hub.blob("abandoned", b"a" * 700)

    result = hf_cache.remove(["jss367/vireo-onnx-models#unreferenced"])

    assert result["freed"] == 700
    assert not dangling.exists()
    assert live.exists()


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_purge_subfolder_skips_an_entry_whose_model_directory_is_back(hub, tmp_path):
    """Settings deletes the directory first; one that exists again is a reinstall."""
    import shutil

    from services import hf_cache

    blob = hub.add("dinov2-vit-b14/model.onnx", b"d" * 300)
    install = _install(tmp_path, "dinov2-vit-b14")

    assert hf_cache.purge_subfolder("dinov2-vit-b14") == 0
    assert blob.exists()
    shutil.rmtree(install)
    assert hf_cache.purge_subfolder("dinov2-vit-b14") == 300
    assert not blob.exists()
    assert hf_cache.purge_subfolder("dinov2-vit-b14") == 0


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_purge_model_uses_the_known_models_subfolder(hub):
    from services import hf_cache

    blob = hub.add("timm-eva02-large-inat21/model.onnx.data", b"e" * 100)
    other = hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)

    assert hf_cache.purge_model("timm-inat21-eva02-l") == 100
    assert not blob.exists()
    assert other.exists()
    assert hf_cache.purge_model("custom-abc") == 0


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_purge_model_for_dynamically_downloaded_model_uses_hf_source(hub):
    """``download_hf_model`` registers ids absent from ``KNOWN_MODELS``.

    ``purge_model`` is called after the entry has been unregistered, so its
    repo has also left ``huggingface_repositories()``. The ``hf_source``
    captured before unregistration is what names the cache payload to drop.
    """
    from services import hf_cache

    blob = hub.add("model.onnx", b"d" * 400, repo="models--mystery-owner--pretty-model")
    other = hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)

    freed = hf_cache.purge_model(
        "hf-mystery-owner-pretty-model",
        hf_source="hf-hub:mystery-owner/pretty-model",
    )

    assert freed == 400
    assert not blob.exists()
    assert other.exists()


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_purge_model_without_hf_source_leaves_an_unknown_dynamic_model_alone(hub):
    """A known-id lookup is a sufficient signal of what to purge.

    A caller that supplies neither a ``KNOWN_MODELS`` id nor an
    ``hf_source`` cannot name a specific repository, so the purge is a
    no-op rather than reaching for an organization-wide prefix.
    """
    from services import hf_cache

    blob = hub.add("model.onnx", b"d" * 400, repo="models--mystery-owner--pretty-model")

    assert hf_cache.purge_model("hf-mystery-owner-pretty-model") == 0
    assert blob.exists()


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_remove_model_purges_cache_for_dynamically_downloaded_model(hub, tmp_path):
    """``download_hf_model``-registered models leave an ``hf-hub:`` source.

    ``remove_model`` captures that string before unregistering so the
    cache copy follows the install out instead of lingering invisibly.
    """
    import models
    from services import hf_cache

    install = tmp_path / "models" / "pretty-model"
    install.mkdir(parents=True)
    (install / "model.onnx").write_bytes(b"installed")
    blob = hub.add("model.onnx", b"d" * 400, repo="models--mystery-owner--pretty-model")
    models._save_config({
        "models": [{
            "id": "hf-mystery-owner-pretty-model",
            "name": "Pretty Model",
            "model_str": "hf-hub:mystery-owner/pretty-model",
            "weights_path": str(install),
            "description": "Downloaded from HuggingFace: mystery-owner/pretty-model",
            "managed": True,
        }],
        "active_model": "hf-mystery-owner-pretty-model",
    })

    result = models.remove_model("hf-mystery-owner-pretty-model")

    assert result["files_deleted"] is True
    assert not install.exists()
    assert not blob.exists()
    assert hf_cache.inventory()["entries"] == []


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_purge_model_does_not_double_delete_when_source_equals_model_str(hub, monkeypatch):
    """bioclip-2 declares the same hf-hub repo for ``model_str`` and ``source``.

    Without deduplication the comprehension that picks groups to delete yields
    the same group twice; the second pass hits ENOENT and ``_delete_groups_locked``
    records spurious failures and warning logs. Deduplicate like ``remove()`` does.
    """
    from services import hf_cache

    hub.add("image_encoder.onnx", b"b" * 400, repo="models--imageomics--bioclip-2")

    seen_groups = []
    real = hf_cache._delete_groups_locked

    def spy(groups):
        seen_groups.append(list(groups))
        return real(groups)

    monkeypatch.setattr(hf_cache, "_delete_groups_locked", spy)

    assert hf_cache.purge_model("bioclip-2") == 400
    assert len(seen_groups) == 1
    keys = [g["key"] for g in seen_groups[0]]
    assert keys == ["imageomics/bioclip-2"], keys


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
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


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
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


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_shared_blob_between_two_unused_entries_counts_once_in_unused_size(hub):
    from services import hf_cache

    hub.add("bioclip-2.5-vith14/config.json", b"{}")
    hub.add("timm-eva02-large-inat21/config.json", b"{}")
    hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 500)

    inv = hf_cache.inventory()
    assert inv["unused_size"] == 502
    result = hf_cache.remove_unused()
    assert result["freed"] == 502
    assert hf_cache.inventory()["entries"] == []


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_remove_refuses_a_key_that_now_backs_an_installed_model(hub, tmp_path):
    """The page may have inventoried before the install finished."""
    from services import hf_cache

    blob = hub.add("dinov2-vit-b14/model.onnx", b"d" * 300)
    _install(tmp_path, "dinov2-vit-b14")

    with pytest.raises(ValueError):
        hf_cache.remove(["jss367/vireo-onnx-models/dinov2-vit-b14"])
    assert blob.exists()


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_symlinked_snapshot_directories_are_ignored_and_never_deleted_through(hub, tmp_path):
    from services import hf_cache

    outside = tmp_path / "outside" / "bioclip-2.5-vith14"
    outside.mkdir(parents=True)
    victim = outside / "image_encoder.onnx.data"
    victim.write_bytes(b"v" * 900)
    repo = hub.root / ONNX_DIR
    hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)
    _symlink(outside.parent, repo / "snapshots" / "linked-rev")
    other = hub.root / "models--imageomics--bioclip"
    (other / "blobs").mkdir(parents=True)
    _symlink(tmp_path / "outside", other / "snapshots")

    inv = hf_cache.inventory()
    assert set(_entries(inv)) == {"jss367/vireo-onnx-models/sam2-small"}
    hf_cache.remove_unused()
    assert victim.exists()
    assert (repo / "snapshots" / "linked-rev").is_symlink()


def test_deleting_uncollected_groups_is_refused(hub, tmp_path):
    from services import hf_cache

    outside = tmp_path / "outside"
    outside.write_bytes(b"keep")
    group = {"key": "invented", "repo_dir": str(hub.root / ONNX_DIR),
             "files": [str(outside)], "blobs": {str(outside): 4}}
    with pytest.raises(hf_cache.UnsafeCacheCleanup):
        hf_cache._delete_groups([group])
    assert outside.read_bytes() == b"keep"


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_last_snapshot_removal_keeps_the_repo_while_a_download_is_in_flight(hub):
    from services import hf_cache

    blob = hub.add("sam2-small/mask_decoder.onnx", b"m" * 400)
    partial = hub.blob("abc.incomplete", b"p" * 50)

    result = hf_cache.remove(["jss367/vireo-onnx-models/sam2-small"])

    assert result["freed"] == 400 and result["failed"] == []
    assert not blob.exists()
    assert partial.exists(), "a resumable partial download must survive"
    assert (hub.root / ONNX_DIR / "blobs").is_dir()


def test_emptied_repo_and_metadata_are_never_pruned(hub):
    from services import hf_cache

    repo = hub.root / ONNX_DIR
    (repo / "refs").mkdir(parents=True)
    (repo / "refs" / "main").write_text("rev1")
    assert hf_cache._remove_repo_if_empty(str(repo)) is False
    assert (repo / "refs" / "main").read_text() == "rev1"


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_deletion_failures_are_reported_not_claimed_as_removed(hub, monkeypatch):
    from services import hf_cache

    big = hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 5000)
    small = hub.add("sam2-small/mask_decoder.onnx", b"m" * 10)
    real = hf_cache._unlink_within

    def unlink_within(root, path):
        if os.path.realpath(path) == os.path.realpath(big):
            raise PermissionError(13, "Operation not permitted", str(path))
        return real(root, path)

    monkeypatch.setattr(hf_cache, "_unlink_within", unlink_within)

    result = hf_cache.remove_unused()

    assert result["removed"] == ["jss367/vireo-onnx-models/sam2-small"]
    assert result["freed"] == 10
    assert [f["key"] for f in result["failed"]] == ["jss367/vireo-onnx-models/bioclip-2.5-vith14"]
    assert "Operation not permitted" in result["failed"][0]["error"]
    assert big.exists() and not small.exists()


def test_an_empty_model_directory_marks_its_cache_entry_installed(hub, tmp_path):
    """Every download creates the directory before fetching, so an empty one
    is a download in flight (or one to repair), not an orphan."""
    from services import hf_cache

    hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 500)
    (tmp_path / "models" / "bioclip-2.5-vith14").mkdir(parents=True)

    entry = _entries(hf_cache.inventory())["jss367/vireo-onnx-models/bioclip-2.5-vith14"]
    assert entry["installed"] is True


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_cleanup_is_refused_while_a_download_holds_the_lock(hub):
    import threading

    from services import hf_cache

    blob = hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 500)
    held = threading.Event()
    release = threading.Event()

    def downloader():
        with hf_cache.DOWNLOAD_LOCK:
            held.set()
            release.wait(10)

    t = threading.Thread(target=downloader)
    t.start()
    held.wait(10)
    try:
        with pytest.raises(hf_cache.DownloadInProgress):
            hf_cache.remove_unused()
        assert blob.exists()
    finally:
        release.set()
        t.join(10)
    assert hf_cache.remove_unused()["freed"] == 500


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_cleanup_is_refused_while_another_downloader_holds_a_hub_lock(hub):
    """The hub holds a filelock on .locks/<repo>/<etag>.lock during a download."""
    import threading

    from filelock import FileLock
    from services import hf_cache

    blob = hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 500)
    locks = hub.root / ".locks" / ONNX_DIR
    locks.mkdir(parents=True)
    held, release = threading.Event(), threading.Event()

    def downloader():
        with FileLock(str(locks / "deadbeef.lock"), is_singleton=False):
            held.set()
            release.wait(10)

    t = threading.Thread(target=downloader)
    t.start()
    held.wait(10)
    try:
        with pytest.raises(hf_cache.DownloadInProgress):
            hf_cache.remove(["jss367/vireo-onnx-models/bioclip-2.5-vith14"])
        assert blob.exists()
    finally:
        release.set()
        t.join(10)
    assert hf_cache.remove(["jss367/vireo-onnx-models/bioclip-2.5-vith14"])["freed"] == 500


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_stale_hub_lock_files_do_not_block_cleanup(hub):
    """filelock leaves the lock file behind; presence alone must mean nothing."""
    from services import hf_cache

    blob = hub.add("bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 500)
    locks = hub.root / ".locks" / ONNX_DIR
    locks.mkdir(parents=True)
    (locks / "deadbeef.lock").write_bytes(b"")
    (locks / "cafebabe.lock").write_bytes(b"")

    assert hf_cache.remove_unused()["freed"] == 500
    assert not blob.exists()


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_failed_shared_blob_is_attributed_to_every_selected_owner(hub, monkeypatch):
    from services import hf_cache

    shared = hub.add("bioclip-2.5-vith14/config.json", b"{}")
    hub.add("timm-eva02-large-inat21/config.json", b"{}")
    real = hf_cache._unlink_within

    def unlink_within(root, path):
        if os.path.realpath(path) == os.path.realpath(shared):
            raise PermissionError(13, "Operation not permitted", path)
        return real(root, path)

    monkeypatch.setattr(hf_cache, "_unlink_within", unlink_within)

    result = hf_cache.remove_unused()

    assert result["removed"] == []
    assert sorted(f["key"] for f in result["failed"]) == [
        "jss367/vireo-onnx-models/bioclip-2.5-vith14",
        "jss367/vireo-onnx-models/timm-eva02-large-inat21",
    ]


def test_unlink_without_the_collected_tree_is_refused(tmp_path):
    from services import hf_cache

    inner = tmp_path / "file.bin"
    inner.write_bytes(b"x")
    with pytest.raises(hf_cache.UnsafeCacheCleanup):
        hf_cache._unlink_within(str(tmp_path), str(inner))
    assert inner.read_bytes() == b"x"


@pytest.mark.skipif(os.name != "posix", reason="dir_fd unlink is POSIX-only")
def test_unlink_within_does_not_follow_a_directory_swapped_for_a_symlink(tmp_path):
    """The containment decision survives a parent turning into a symlink."""
    from services import hf_cache

    root = tmp_path / "snapshots"
    (root / "rev").mkdir(parents=True)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    victim = elsewhere / "model.onnx"
    victim.write_bytes(b"v")
    _symlink(elsewhere, root / "rev" / "sam2-small")

    with pytest.raises(OSError):
        hf_cache._unlink_within(str(root), str(root / "rev" / "sam2-small" / "model.onnx"))
    assert victim.exists()


def test_cache_directory_pruning_is_refused(tmp_path):
    from services import hf_cache

    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(hf_cache.UnsafeCacheCleanup):
        hf_cache._rmdir_within(str(tmp_path), str(directory))
    assert directory.is_dir()


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup requires POSIX no-follow descriptors")
def test_remove_decides_on_state_collected_under_the_locks(hub, tmp_path, monkeypatch):
    """An install that lands between the page's inventory and the click is seen."""
    from services import hf_cache

    blob = hub.add("dinov2-vit-b14/model.onnx", b"d" * 300)
    assert not _entries(hf_cache.inventory())["jss367/vireo-onnx-models/dinov2-vit-b14"]["installed"]
    real_collect = hf_cache._collect

    def collect_after_install(cache_dir, models_dir, extra_repos=()):
        # Simulate the model directory appearing just before the locked collection.
        (tmp_path / "models" / "dinov2-vit-b14").mkdir(parents=True, exist_ok=True)
        return real_collect(cache_dir, models_dir, extra_repos)

    monkeypatch.setattr(hf_cache, "_collect", collect_after_install)
    with pytest.raises(ValueError):
        hf_cache.remove(["jss367/vireo-onnx-models/dinov2-vit-b14"])
    assert hf_cache.remove_unused()["removed"] == []
    assert blob.exists()
