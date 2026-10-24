"""fetch_hub_file: one copy of each model on disk, published by rename.

Downloads used to land in the Hugging Face hub cache and get byte-copied into
~/.vireo/models (shutil.copy2 does not clone on APFS either), so every model
took twice its size.
"""

import hashlib
import os
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture
def fake_hub(monkeypatch):
    """A huggingface_hub whose hf_hub_download writes where local_dir says."""
    calls = []
    state = {"fail": False, "resumed": []}

    def hf_hub_download(repo_id, filename, subfolder=None, revision=None, local_dir=None):
        calls.append({"repo_id": repo_id, "filename": filename, "subfolder": subfolder,
                      "revision": revision, "local_dir": local_dir})
        assert local_dir is not None, "a download without local_dir fills the hub cache"
        rel = f"{subfolder}/{filename}" if subfolder else filename
        partial = os.path.join(local_dir, ".cache", "huggingface", "download", rel + ".incomplete")
        os.makedirs(os.path.dirname(partial), exist_ok=True)
        state["resumed"].append(Path(partial).read_bytes() if os.path.exists(partial) else None)
        with open(partial, "wb") as f:
            f.write(b"partial")
        if state["fail"]:
            raise ConnectionError("network dropped")
        path = os.path.join(local_dir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        os.replace(partial, path)
        with open(path, "wb") as f:
            f.write(f"bytes of {rel}".encode())
        return path

    module = types.ModuleType("huggingface_hub")
    module.hf_hub_download = hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", module)
    return calls, state


def test_downloads_beside_the_destination_and_leaves_one_copy(fake_hub, tmp_path):
    from hub_download import STAGING_DIRNAME, fetch_hub_file

    calls, _state = fake_hub
    dest = tmp_path / "models" / "megadetector-v6" / "model.onnx"

    result = fetch_hub_file("owner/repo", "model.onnx", str(dest),
                            subfolder="megadetector-v6", revision="abc")

    assert result == str(dest)
    assert dest.read_bytes() == b"bytes of megadetector-v6/model.onnx"
    assert calls == [{"repo_id": "owner/repo", "filename": "model.onnx",
                      "subfolder": "megadetector-v6", "revision": "abc",
                      "local_dir": calls[0]["local_dir"]}]
    assert Path(calls[0]["local_dir"]).parent == dest.parent / STAGING_DIRNAME
    assert sorted(os.listdir(dest.parent)) == ["model.onnx"], "staging is removed after publish"


def test_repo_relative_filename_lands_at_its_destination(fake_hub, tmp_path):
    from hub_download import fetch_hub_file

    dest = tmp_path / "models" / "slug" / "onnx" / "model.onnx"
    fetch_hub_file("owner/repo", "onnx/model.onnx", str(dest))
    assert dest.read_bytes() == b"bytes of onnx/model.onnx"
    assert os.listdir(dest.parent) == ["model.onnx"]


def test_failed_download_publishes_nothing_and_keeps_resume_data(fake_hub, tmp_path):
    from hub_download import fetch_hub_file

    calls, state = fake_hub
    state["fail"] = True
    dest = tmp_path / "models" / "sam2-small" / "image_encoder.onnx"

    with pytest.raises(ConnectionError):
        fetch_hub_file("owner/repo", "image_encoder.onnx", str(dest), subfolder="sam2-small")

    assert not dest.exists()
    partial = (Path(calls[-1]["local_dir"]) / ".cache" / "huggingface" / "download"
               / "sam2-small" / "image_encoder.onnx.incomplete")
    assert partial.read_bytes() == b"partial", "the retry resumes from here"

    state["fail"] = False
    fetch_hub_file("owner/repo", "image_encoder.onnx", str(dest), subfolder="sam2-small")
    assert dest.is_file()
    assert os.listdir(dest.parent) == ["image_encoder.onnx"]


def test_model_downloads_write_straight_into_the_models_dir(fake_hub, tmp_path, monkeypatch):
    """The manifest download path goes through fetch_hub_file, not the hub cache."""
    import models

    calls, _state = fake_hub
    hub_module = sys.modules["huggingface_hub"]
    hub_module.try_to_load_from_cache = lambda **_kw: None
    model_dir = tmp_path / "models" / "bioclip-2.5-vith14"

    dest = models._hf_download_with_retry(
        "owner/repo", "image_encoder.onnx", str(model_dir), subfolder="bioclip-2.5-vith14",
    )

    assert dest == str(model_dir / "image_encoder.onnx")
    assert calls[0]["local_dir"].startswith(str(model_dir))
    assert os.listdir(model_dir) == ["image_encoder.onnx"]


@pytest.mark.parametrize("other", [
    ("owner/repo", "config.json", "weights", "rev-a"),
    ("owner/other-repo", "model.onnx", "weights", "rev-a"),
    ("owner/repo", "model.onnx", "other-weights", "rev-a"),
    ("owner/repo", "model.onnx", "weights", "rev-b"),
])
def test_another_successful_download_preserves_interrupted_transfer(fake_hub, tmp_path, other):
    """A later file/repository/revision must not delete another transfer's resume data."""
    from hub_download import fetch_hub_file

    calls, state = fake_hub
    destination = tmp_path / "model.onnx"
    state["fail"] = True
    with pytest.raises(ConnectionError):
        fetch_hub_file("owner/repo", "model.onnx", str(destination),
                       subfolder="weights", revision="rev-a")
    interrupted_staging = Path(calls[-1]["local_dir"])
    partial = interrupted_staging / ".cache/huggingface/download/weights/model.onnx.incomplete"
    assert partial.read_bytes() == b"partial"

    state["fail"] = False
    repo, filename, subfolder, revision = other
    fetch_hub_file(repo, filename, str(tmp_path / filename), subfolder=subfolder, revision=revision)
    assert partial.read_bytes() == b"partial"

    fetch_hub_file("owner/repo", "model.onnx", str(destination),
                   subfolder="weights", revision="rev-a")
    assert state["resumed"][-1] == b"partial"
    assert not interrupted_staging.exists()
    assert destination.read_bytes() == b"bytes of weights/model.onnx"


@pytest.mark.parametrize("filename", ["model.onnx", "model.onnx.data", "embeddings.npy"])
def test_repair_releases_known_bad_weights_before_staging_legacy_cache(tmp_path, monkeypatch, filename):
    """A repair needs space for the legacy cache plus one replacement, not three copies."""
    import huggingface_hub
    import models

    cache_blob = tmp_path / "legacy-cache" / "blob"
    cache_blob.parent.mkdir()
    cache_blob.write_bytes(b"correct model bytes")
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    destination = model_dir / filename
    destination.write_bytes(b"corrupt model bytes")

    def download(*, local_dir, **kwargs):
        assert not destination.exists(), "known-bad weights still consume space during staging"
        assert cache_blob.is_file(), "repair must not discard a usable legacy cache blob"
        staged = Path(local_dir) / filename
        staged.write_bytes(cache_blob.read_bytes())
        return str(staged)

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", download)
    monkeypatch.setattr(huggingface_hub, "try_to_load_from_cache", lambda **kw: str(cache_blob))
    monkeypatch.setattr("time.sleep", lambda *_: None)
    models._download_and_verify_file(
        filename, str(model_dir), "weights",
        {filename: hashlib.sha256(cache_blob.read_bytes()).hexdigest()}, None,
    )
    assert destination.read_bytes() == cache_blob.read_bytes()


@pytest.mark.parametrize("filename,expected", [
    ("model.onnx", None),
    ("config.json", "mismatching-checksum"),
])
def test_failed_repair_preserves_unverified_weights_and_readable_metadata(
    tmp_path, monkeypatch, filename, expected,
):
    import models

    destination = tmp_path / filename
    destination.write_bytes(b"previous contents")

    def fail(*args, **kwargs):
        assert destination.read_bytes() == b"previous contents"
        raise ConnectionError("offline")

    monkeypatch.setattr(models, "_hf_download_with_retry", fail)
    hashes = {filename: expected} if expected else {}
    with pytest.raises(ConnectionError):
        models._download_and_verify_file(filename, str(tmp_path), "weights", hashes, None)
    assert destination.read_bytes() == b"previous contents"


@pytest.mark.parametrize("kind", ["custom", "pipeline"])
def test_model_batch_retry_keeps_completed_files(app_and_db, tmp_path, monkeypatch, kind):
    import huggingface_hub
    import models
    from web import models as routes

    app, _db = app_and_db
    files = (["model.onnx", "config.json"] if kind == "custom"
             else ["image_encoder.onnx", "mask_decoder.onnx"])
    downloads = []
    revisions = []
    listings = []
    server_revision = "a" * 40
    failing = True

    def download(*, filename, local_dir, **kwargs):
        downloads.append(filename)
        revisions.append(kwargs.get("revision"))
        if failing and filename == files[1]:
            raise ConnectionError("companion unavailable")
        destination = Path(local_dir) / filename
        destination.write_bytes(filename.encode())
        return str(destination)

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", download)
    monkeypatch.setattr(huggingface_hub, "try_to_load_from_cache", lambda **kw: None)
    def list_files(*args, revision=None):
        listings.append(revision)
        return files

    monkeypatch.setattr(huggingface_hub, "list_repo_files", list_files)
    monkeypatch.setattr(huggingface_hub, "HfApi", lambda: types.SimpleNamespace(
        model_info=lambda _: types.SimpleNamespace(sha=server_revision)))
    monkeypatch.setattr("time.sleep", lambda *_: None)

    if kind == "custom":
        def run():
            return models.download_hf_model("owner/test-model")
    else:
        pending = []
        events = []

        def start(_kind, work, **kwargs):
            pending.append(work)
            return "test-download"

        monkeypatch.setattr(routes, "pipeline_models_dir", lambda: str(tmp_path / "pipeline"))
        monkeypatch.setattr(app._job_runner, "start", start)
        monkeypatch.setattr(app._job_runner, "push_event", lambda *args: events.append(args))

        def run():
            response = app.test_client().post("/api/models/pipeline/download", json={"model_id": "sam2-small"})
            assert response.status_code == 200
            return pending.pop()({"id": "test-download"})

    with pytest.raises((RuntimeError, ConnectionError)):
        run()
    assert downloads == [files[0], files[1], files[1], files[1]]
    failing = False
    server_revision = "b" * 40
    run()
    assert downloads == [files[0], files[1], files[1], files[1], files[1]]
    assert revisions == ["a" * 40] * 5, "a retry must finish the original snapshot"
    if kind == "pipeline":
        assert events[-1][-1]["total"] == 1, "progress counts only files still to download"
    # A later explicit download is a fresh batch, allowing deliberate refresh.
    run()
    assert downloads[-2:] == files
    assert revisions[-2:] == ["b" * 40] * 2
    if kind == "custom":
        assert listings == ["a" * 40, "a" * 40, "b" * 40]


@pytest.mark.parametrize("change", ["other-repository", "modified", "missing", "unrecorded"])
def test_download_batch_does_not_reuse_unrelated_or_changed_files(tmp_path, monkeypatch, change):
    import huggingface_hub
    from hub_download import DownloadBatch

    monkeypatch.setattr(huggingface_hub, "HfApi", lambda: types.SimpleNamespace(
        model_info=lambda _: types.SimpleNamespace(sha="a" * 40)))
    file = tmp_path / "model.onnx"
    file.write_bytes(b"completed")
    batch = DownloadBatch("owner/model", str(tmp_path))
    if change != "unrecorded":
        batch.record(file.name)
    if change == "modified":
        file.write_bytes(b"different data")
    elif change == "missing":
        file.unlink()
    repo = "other-owner/model" if change == "other-repository" else "owner/model"
    assert DownloadBatch(repo, str(tmp_path)).pending([file.name]) == [file.name]


@pytest.mark.parametrize("saved_revision", [None, "main", 123])
def test_download_batch_does_not_reuse_unpinned_receipt(tmp_path, monkeypatch, saved_revision):
    import json

    import huggingface_hub
    from hub_download import DownloadBatch

    monkeypatch.setattr(huggingface_hub, "HfApi", lambda: types.SimpleNamespace(
        model_info=lambda _: types.SimpleNamespace(sha="b" * 40)))
    model = tmp_path / "model.onnx"
    model.write_bytes(b"old weights")
    batch = DownloadBatch("owner/model", str(tmp_path))
    batch.record(model.name)
    receipt = Path(batch.path)
    saved = json.loads(receipt.read_text())
    saved["revision"] = saved_revision
    receipt.write_text(json.dumps(saved))
    retry = DownloadBatch("owner/model", str(tmp_path))
    assert retry.revision == "b" * 40
    assert retry.pending([model.name]) == [model.name]


def test_download_batch_refuses_mutable_revision_before_any_transfer(tmp_path, monkeypatch):
    import huggingface_hub
    from hub_download import DownloadBatch

    monkeypatch.setattr(huggingface_hub, "HfApi", lambda: types.SimpleNamespace(
        model_info=lambda _: types.SimpleNamespace(sha="main")))
    with pytest.raises(RuntimeError, match="immutable revision"):
        DownloadBatch("owner/model", str(tmp_path))
    assert not (tmp_path / ".vireo-download-progress.json").exists()
