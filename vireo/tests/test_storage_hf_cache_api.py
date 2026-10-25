"""The Storage page's Hugging Face cache card: inventory and removal routes.

The card used to send users to Settings, which lists installs and never the
cache, so the bulk of the cache (models removed or superseded) had no owner
anywhere in the UI. These routes give the card its own breakdown and cleanup.
"""

import hashlib
import os

import pytest
from page_scripts import page_with_scripts

ONNX_DIR = "models--jss367--vireo-onnx-models"


def _add(hub, relpath, content, rev="rev1"):
    repo_dir = hub / ONNX_DIR
    blob = repo_dir / "blobs" / hashlib.sha256(content).hexdigest()
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(content)
    snap = repo_dir / "snapshots" / rev / relpath
    snap.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.symlink(os.path.relpath(blob, snap.parent), snap)
    except (OSError, NotImplementedError):  # pragma: no cover - Windows without privilege
        pytest.skip("symlinks unavailable on this platform")
    return blob


@pytest.fixture
def seeded(app_and_db, tmp_path, monkeypatch):
    """One installed model (DINOv2) and one orphaned one (BioCLIP 2.5) in the cache."""
    import models
    from services import hf_cache, hf_cache_lock

    lock = hf_cache_lock.SharedDownloadLock(str(tmp_path / "cache-access.lock"))
    monkeypatch.setattr(hf_cache_lock, "DOWNLOAD_LOCK", lock)
    monkeypatch.setattr(hf_cache, "DOWNLOAD_LOCK", lock)
    app, _db = app_and_db
    hub = tmp_path / "hub"
    hub.mkdir()
    monkeypatch.setenv("HF_HUB_CACHE", str(hub))
    installed = tmp_path / "vireo-models" / "dinov2-vit-b14"
    installed.mkdir(parents=True)
    (installed / "model.onnx").write_bytes(b"installed")
    assert str(tmp_path / "vireo-models") == models.DEFAULT_MODELS_DIR
    return app, {
        "orphan": _add(hub, "bioclip-2.5-vith14/image_encoder.onnx.data", b"b" * 5000),
        "live": _add(hub, "dinov2-vit-b14/model.onnx", b"d" * 300),
    }


def test_storage_report_marks_each_cache_entry_installed_or_not(seeded):
    app, _blobs = seeded
    data = app.test_client().get("/api/storage").get_json()
    hf = data["hf_cache"]
    assert hf["unused_size"] == 5000
    assert hf["installed_size"] == 300
    by_key = {m["key"]: m for m in hf["models"]}
    assert by_key["jss367/vireo-onnx-models/bioclip-2.5-vith14"]["installed"] is False
    assert by_key["jss367/vireo-onnx-models/dinov2-vit-b14"]["installed"] is True
    assert hf["size"] >= 5300


def test_hf_cache_inventory_route(seeded):
    app, _blobs = seeded
    resp = app.test_client().get("/api/storage/hf-cache")
    assert resp.status_code == 200
    inv = resp.get_json()
    assert [e["key"] for e in inv["entries"]] == [
        "jss367/vireo-onnx-models/bioclip-2.5-vith14",
        "jss367/vireo-onnx-models/dinov2-vit-b14",
    ]
    assert inv["unused_size"] == 5000


def test_bulk_existing_model_cache_removal_keeps_installed_weights(seeded, tmp_path):
    app, blobs = seeded
    response = app.test_client().post("/api/storage/hf-cache/remove", json={"installed": True})
    assert response.status_code == 200
    assert response.get_json()["freed"] == 300
    assert not blobs["live"].exists()
    assert blobs["orphan"].exists()
    assert (tmp_path / "vireo-models/dinov2-vit-b14/model.onnx").read_bytes() == b"installed"


@pytest.mark.parametrize("body", [{"installed": "yes"}, {"installed": False},
                                  {"installed": True, "unused": True},
                                  {"installed": True, "keys": ["anything"]}])
def test_bulk_cache_removal_rejects_ambiguous_or_non_boolean_selection(seeded, body):
    app, blobs = seeded
    assert app.test_client().post("/api/storage/hf-cache/remove", json=body).status_code == 400
    assert all(blob.exists() for blob in blobs.values())


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup fails closed without dir_fd")
def test_remove_unused_frees_orphans_and_returns_fresh_inventory(seeded):
    app, blobs = seeded
    resp = app.test_client().post("/api/storage/hf-cache/remove", json={"unused": True})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["freed"] == 5000
    assert body["removed"] == ["jss367/vireo-onnx-models/bioclip-2.5-vith14"]
    assert body["unused_size"] == 0
    assert [e["key"] for e in body["entries"]] == ["jss367/vireo-onnx-models/dinov2-vit-b14"]
    assert not blobs["orphan"].exists()
    assert blobs["live"].exists()


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup fails closed without dir_fd")
def test_remove_by_key_only_accepts_inventory_keys(seeded):
    app, blobs = seeded
    client = app.test_client()
    resp = client.post("/api/storage/hf-cache/remove", json={"keys": ["../../etc"]})
    assert resp.status_code == 404
    assert "Unknown cache entry" in resp.get_json()["error"]
    resp = client.post("/api/storage/hf-cache/remove", json={"keys": []})
    assert resp.status_code == 400
    resp = client.post("/api/storage/hf-cache/remove", json={"keys": [7]})
    assert resp.status_code == 400
    assert blobs["orphan"].exists()

    resp = client.post(
        "/api/storage/hf-cache/remove",
        json={"keys": ["jss367/vireo-onnx-models/bioclip-2.5-vith14"]},
    )
    assert resp.status_code == 200
    assert resp.get_json()["freed"] == 5000
    assert not blobs["orphan"].exists()


def test_storage_page_manages_the_cache_itself(app_and_db):
    """The card opens its own modal; it no longer points at Settings."""
    app, _db = app_and_db
    html = page_with_scripts(app.test_client(), "/storage")
    assert "hfCache: true" in html
    assert "openHfCacheModal" in html
    assert 'id="hfCacheModal"' in html
    assert "/api/storage/hf-cache/remove" in html
    assert "hfCacheCardNote" in html


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup fails closed without dir_fd")
def test_remove_by_key_frees_a_duplicate_of_an_installed_model(seeded, tmp_path):
    """The install is its own full copy, so its cache twin is removable."""
    app, blobs = seeded
    resp = app.test_client().post(
        "/api/storage/hf-cache/remove",
        json={"keys": ["jss367/vireo-onnx-models/dinov2-vit-b14"]},
    )
    assert resp.status_code == 200
    assert resp.get_json()["freed"] == 300
    assert not blobs["live"].exists()
    assert (tmp_path / "vireo-models" / "dinov2-vit-b14" / "model.onnx").read_bytes() == b"installed"


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup fails closed without dir_fd")
def test_remove_reports_a_failed_deletion_instead_of_success(seeded, monkeypatch):
    from services import hf_cache

    app, blobs = seeded
    real = hf_cache._unlink_within

    def unlink_within(root, path):
        if os.path.realpath(path) == os.path.realpath(blobs["orphan"]):
            raise PermissionError(13, "Operation not permitted", str(path))
        return real(root, path)

    monkeypatch.setattr(hf_cache, "_unlink_within", unlink_within)
    resp = app.test_client().post("/api/storage/hf-cache/remove", json={"unused": True})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is False
    assert body["removed"] == []
    assert body["failed"][0]["key"] == "jss367/vireo-onnx-models/bioclip-2.5-vith14"
    assert blobs["orphan"].exists()
    assert [e["key"] for e in body["entries"]] == [
        "jss367/vireo-onnx-models/bioclip-2.5-vith14",
        "jss367/vireo-onnx-models/dinov2-vit-b14",
    ]


@pytest.mark.skipif(os.name != "posix", reason="cache cleanup fails closed without dir_fd")
def test_remove_answers_409_while_a_download_is_in_progress(seeded):
    from services import hf_cache

    app, blobs = seeded
    import threading

    held, release = threading.Event(), threading.Event()

    def downloader():
        with hf_cache.DOWNLOAD_LOCK:
            held.set()
            release.wait(10)

    t = threading.Thread(target=downloader)
    t.start()
    held.wait(10)
    try:
        resp = app.test_client().post("/api/storage/hf-cache/remove", json={"unused": True})
    finally:
        release.set()
        t.join(10)
    assert resp.status_code == 409
    assert "model or cache operation is in progress" in resp.get_json()["error"]
    assert blobs["orphan"].exists()
