"""POST /api/models/pipeline/delete must only ever remove a registered model dir."""

import os

import pytest


@pytest.fixture
def models_root(tmp_path, monkeypatch):
    """A fake ~/.vireo layout: models/ with two model dirs and a sibling DB."""
    import web.models as models_mod

    vireo_dir = tmp_path / "vireo"
    models_dir = vireo_dir / "models"
    (models_dir / "sam2-large").mkdir(parents=True)
    (models_dir / "sam2-large" / "image_encoder.onnx").write_bytes(b"x")
    (models_dir / "dinov2-vit-s14").mkdir()
    (models_dir / "dinov2-vit-s14" / "model.onnx").write_bytes(b"x")
    (vireo_dir / "vireo.db").write_bytes(b"db")
    monkeypatch.setattr(models_mod, "pipeline_models_dir", lambda: str(models_dir))
    return vireo_dir


def _post(app, payload):
    return app.test_client().post("/api/models/pipeline/delete", json=payload)


def test_delete_rejects_path_traversal_id(app_and_db, models_root):
    """A prefix-matching id with ``..`` used to rmtree the parent of models/."""
    app, _ = app_and_db
    resp = _post(app, {"model_id": "sam2-large/../.."})
    assert resp.status_code == 404
    assert (models_root / "vireo.db").exists()
    assert (models_root / "models" / "sam2-large" / "image_encoder.onnx").exists()
    assert (models_root / "models" / "dinov2-vit-s14" / "model.onnx").exists()


def test_delete_rejects_unknown_id(app_and_db, models_root):
    app, _ = app_and_db
    resp = _post(app, {"model_id": "sam2-huge"})
    assert resp.status_code == 404
    assert "Unknown pipeline model" in resp.get_json()["error"]
    assert (models_root / "models" / "sam2-large").is_dir()


def test_delete_requires_model_id(app_and_db, models_root):
    app, _ = app_and_db
    assert _post(app, {}).status_code == 400
    assert _post(app, {"model_id": 7}).status_code == 400


def test_delete_removes_only_the_registered_dir(app_and_db, models_root):
    app, _ = app_and_db
    resp = _post(app, {"model_id": "vit-s14"})
    assert resp.status_code == 200
    data = resp.get_json()
    expected = os.path.join(str(models_root / "models"), "dinov2-vit-s14")
    assert data == {"deleted": [expected], "count": 1, "model_id": "vit-s14"}
    assert not (models_root / "models" / "dinov2-vit-s14").exists()
    assert (models_root / "models" / "sam2-large").is_dir()
    assert (models_root / "vireo.db").exists()


def test_delete_of_absent_registered_model_is_a_noop(app_and_db, models_root):
    app, _ = app_and_db
    resp = _post(app, {"model_id": "sam2-tiny"})
    assert resp.status_code == 200
    assert resp.get_json() == {"deleted": [], "count": 0, "model_id": "sam2-tiny"}


def test_delete_refuses_while_a_download_holds_the_guard(
    app_and_db, models_root, monkeypatch,
):
    """A concurrent download holds ``DOWNLOAD_LOCK`` across each file copy; a
    delete that proceeded through its non-blocking acquire miss would erase an
    in-progress install, or recreate files the response just claimed to have
    removed. The route must answer 409 and leave the install dir alone."""
    from services import hf_cache_lock
    # A fresh lock keyed off the test tmp path so holding it in this thread
    # doesn't leak into other tests, and so the file lives inside the tmp HOME.
    lock = hf_cache_lock.SharedDownloadLock(
        str(models_root.parent / "hf-cache-access.lock"))
    monkeypatch.setattr(hf_cache_lock, "DOWNLOAD_LOCK", lock)

    import threading

    held = threading.Event()
    release = threading.Event()

    def holder():
        lock.acquire()
        try:
            held.set()
            release.wait(timeout=10)
        finally:
            lock.release()

    thread = threading.Thread(target=holder)
    thread.start()
    try:
        held.wait(timeout=10)
        app, _ = app_and_db
        resp = _post(app, {"model_id": "sam2-large"})
        assert resp.status_code == 409
        assert "download" in resp.get_json()["error"].lower()
        # The install dir must survive: nothing was removed.
        assert (models_root / "models" / "sam2-large" / "image_encoder.onnx").exists()
    finally:
        release.set()
        thread.join(timeout=10)


def test_delete_purges_the_models_hugging_face_cache_entry(
    app_and_db, models_root, tmp_path, monkeypatch,
):
    """Deleting a pipeline model must not leave its download in the HF cache,
    where Settings never shows it and Storage reported it as unmanaged."""
    import hashlib

    app, _ = app_and_db
    hub = tmp_path / "hub"
    monkeypatch.setenv("HF_HUB_CACHE", str(hub))
    repo = hub / "models--jss367--vireo-onnx-models"
    content = b"s" * 500
    blob = repo / "blobs" / hashlib.sha256(content).hexdigest()
    blob.parent.mkdir(parents=True)
    blob.write_bytes(content)
    snap = repo / "snapshots" / "rev1" / "sam2-large" / "image_encoder.onnx"
    snap.parent.mkdir(parents=True)
    try:
        os.symlink(os.path.relpath(blob, snap.parent), snap)
    except (OSError, NotImplementedError):  # pragma: no cover - Windows without privilege
        pytest.skip("symlinks unavailable on this platform")
    other = repo / "snapshots" / "rev1" / "dinov2-vit-s14" / "model.onnx"
    other.parent.mkdir(parents=True)
    other.write_bytes(b"keep")

    resp = _post(app, {"model_id": "sam2-large"})

    assert resp.status_code == 200
    assert not (models_root / "models" / "sam2-large").exists()
    if os.name != "posix":
        assert blob.exists() and snap.exists()
        assert resp.get_json()["ok"] is False
        assert resp.get_json()["cache_cleanup"]["failed"]
    else:
        assert not blob.exists() and not snap.exists()
        assert snap.parent.is_dir(), "safe cleanup retains directory anchors"
    assert other.exists()
