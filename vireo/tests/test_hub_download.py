"""fetch_hub_file: one copy of each model on disk, published by rename.

Downloads used to land in the Hugging Face hub cache and get byte-copied into
~/.vireo/models (shutil.copy2 does not clone on APFS either), so every model
took twice its size.
"""

import os
import sys
import types

import pytest


@pytest.fixture
def fake_hub(monkeypatch):
    """A huggingface_hub whose hf_hub_download writes where local_dir says."""
    calls = []
    state = {"fail": False}

    def hf_hub_download(repo_id, filename, subfolder=None, revision=None, local_dir=None):
        calls.append({"repo_id": repo_id, "filename": filename, "subfolder": subfolder,
                      "revision": revision, "local_dir": local_dir})
        assert local_dir is not None, "a download without local_dir fills the hub cache"
        rel = f"{subfolder}/{filename}" if subfolder else filename
        partial = os.path.join(local_dir, ".cache", "huggingface", "download", rel + ".incomplete")
        os.makedirs(os.path.dirname(partial), exist_ok=True)
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
                      "local_dir": str(dest.parent / STAGING_DIRNAME)}]
    assert sorted(os.listdir(dest.parent)) == ["model.onnx"], "staging is removed after publish"


def test_repo_relative_filename_lands_at_its_destination(fake_hub, tmp_path):
    from hub_download import fetch_hub_file

    dest = tmp_path / "models" / "slug" / "onnx" / "model.onnx"
    fetch_hub_file("owner/repo", "onnx/model.onnx", str(dest))
    assert dest.read_bytes() == b"bytes of onnx/model.onnx"
    assert os.listdir(dest.parent) == ["model.onnx"]


def test_failed_download_publishes_nothing_and_keeps_resume_data(fake_hub, tmp_path):
    from hub_download import STAGING_DIRNAME, fetch_hub_file

    _calls, state = fake_hub
    state["fail"] = True
    dest = tmp_path / "models" / "sam2-small" / "image_encoder.onnx"

    with pytest.raises(ConnectionError):
        fetch_hub_file("owner/repo", "image_encoder.onnx", str(dest), subfolder="sam2-small")

    assert not dest.exists()
    partial = (dest.parent / STAGING_DIRNAME / ".cache" / "huggingface" / "download"
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
