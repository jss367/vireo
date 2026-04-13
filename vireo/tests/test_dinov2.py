# vireo/tests/test_dinov2.py
"""Tests for DINOv2 embedding module (ONNX Runtime version).

Tests cover configuration, variant validation, embedding serialization,
preprocessing, singleton caching, and DB storage. Actual ONNX model
inference is not tested (requires downloading model weights) -- these
tests verify the surrounding logic.
"""
import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# -- Variant configuration --


def test_variants_dict():
    """DINOV2_VARIANTS contains expected model variants."""
    from dino_embed import DINOV2_VARIANTS

    assert "vit-s14" in DINOV2_VARIANTS
    assert "vit-b14" in DINOV2_VARIANTS
    assert "vit-l14" in DINOV2_VARIANTS


def test_variants_are_ints():
    """DINOV2_VARIANTS maps variant names to integer dimensions."""
    from dino_embed import DINOV2_VARIANTS

    for variant, dim in DINOV2_VARIANTS.items():
        assert isinstance(dim, int), f"{variant} should map to int, got {type(dim)}"


def test_get_embedding_dim():
    """get_embedding_dim returns correct dimensions per variant."""
    from dino_embed import get_embedding_dim

    assert get_embedding_dim("vit-s14") == 384
    assert get_embedding_dim("vit-b14") == 768
    assert get_embedding_dim("vit-l14") == 1024


def test_get_embedding_dim_invalid():
    """get_embedding_dim raises ValueError for unknown variant."""
    from dino_embed import get_embedding_dim

    try:
        get_embedding_dim("vit-xl99")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown DINOv2 variant" in str(e)


def test_session_loader_invalid_variant():
    """_get_dinov2_session raises ValueError for unknown variant."""
    from dino_embed import _get_dinov2_session

    with pytest.raises(ValueError, match="Unknown DINOv2 variant"):
        _get_dinov2_session("vit-nonexistent")


def test_session_loader_missing_model(tmp_path):
    """_get_dinov2_session raises FileNotFoundError if model file missing."""
    import dino_embed

    # Reset singleton
    dino_embed._session = None
    dino_embed._variant_loaded = None

    with patch.dict(os.environ, {"HOME": str(tmp_path)}):
        with patch("os.path.expanduser", return_value=str(tmp_path)):
            with pytest.raises(FileNotFoundError, match="DINOv2 ONNX model not found"):
                dino_embed._get_dinov2_session("vit-b14")


# -- Singleton caching --


def test_singleton_caching():
    """Session is cached and reused for same variant."""
    import dino_embed

    mock_session = MagicMock()
    dino_embed._session = mock_session
    dino_embed._variant_loaded = "vit-b14"

    result = dino_embed._get_dinov2_session("vit-b14")
    assert result is mock_session

    # Cleanup
    dino_embed._session = None
    dino_embed._variant_loaded = None


def test_singleton_reloads_for_different_variant(tmp_path):
    """Session is reloaded when variant changes."""
    import dino_embed

    mock_session_old = MagicMock()
    dino_embed._session = mock_session_old
    dino_embed._variant_loaded = "vit-s14"

    # Requesting different variant should not return cached session
    # It will try to load a new one; we verify it doesn't return the old one
    model_dir = tmp_path / ".vireo" / "models" / "dinov2-vit-b14"
    model_dir.mkdir(parents=True)
    model_path = model_dir / "model.onnx"
    model_path.write_bytes(b"fake")

    mock_new_session = MagicMock()
    with patch("os.path.expanduser", return_value=str(tmp_path)):
        with patch("dino_embed.onnx_runtime.create_session", return_value=mock_new_session):
            result = dino_embed._get_dinov2_session("vit-b14")

    assert result is mock_new_session
    assert dino_embed._variant_loaded == "vit-b14"

    # Cleanup
    dino_embed._session = None
    dino_embed._variant_loaded = None


# -- Preprocessing --


def test_preprocessing_produces_correct_shape():
    """preprocess_image with DINOv2 params gives (1, 3, 518, 518)."""
    from onnx_runtime import preprocess_image
    from PIL import Image

    img = Image.new("RGB", (800, 600))
    arr = preprocess_image(
        img,
        size=(518, 518),
        mean=[0.485, 0.456, 0.406],
        std=[0.229, 0.224, 0.225],
        center_crop=True,
    )
    assert arr.shape == (1, 3, 518, 518)
    assert arr.dtype == np.float32


def test_preprocessing_center_crop_nonsquare():
    """Center crop handles non-square images correctly."""
    from onnx_runtime import preprocess_image
    from PIL import Image

    img = Image.new("RGB", (1000, 200))
    arr = preprocess_image(
        img,
        size=(518, 518),
        mean=[0.485, 0.456, 0.406],
        std=[0.229, 0.224, 0.225],
        center_crop=True,
    )
    assert arr.shape == (1, 3, 518, 518)


# -- Embedding mock inference --


def test_embed_output_shape():
    """embed() returns correct shape for given variant."""
    import dino_embed

    # Create a mock session that returns correct-shaped output
    mock_session = MagicMock()
    mock_session.get_inputs.return_value = [MagicMock(name="input")]
    mock_session.run.return_value = [np.random.randn(1, 768).astype(np.float32)]

    dino_embed._session = mock_session
    dino_embed._variant_loaded = "vit-b14"

    from PIL import Image

    img = Image.new("RGB", (100, 100))
    result = dino_embed.embed(img, variant="vit-b14")

    assert result.shape == (768,)
    assert result.dtype == np.float32

    # Cleanup
    dino_embed._session = None
    dino_embed._variant_loaded = None


def test_embed_subject_delegates_to_embed():
    """embed_subject calls embed with the same arguments."""
    import dino_embed

    mock_session = MagicMock()
    mock_session.get_inputs.return_value = [MagicMock(name="input")]
    mock_session.run.return_value = [np.random.randn(1, 768).astype(np.float32)]

    dino_embed._session = mock_session
    dino_embed._variant_loaded = "vit-b14"

    from PIL import Image

    img = Image.new("RGB", (100, 100))
    result = dino_embed.embed_subject(img, variant="vit-b14")

    assert result.shape == (768,)

    # Cleanup
    dino_embed._session = None
    dino_embed._variant_loaded = None


def test_embed_global_delegates_to_embed():
    """embed_global calls embed with the same arguments."""
    import dino_embed

    mock_session = MagicMock()
    mock_session.get_inputs.return_value = [MagicMock(name="input")]
    mock_session.run.return_value = [np.random.randn(1, 768).astype(np.float32)]

    dino_embed._session = mock_session
    dino_embed._variant_loaded = "vit-b14"

    from PIL import Image

    img = Image.new("RGB", (100, 100))
    result = dino_embed.embed_global(img, variant="vit-b14")

    assert result.shape == (768,)

    # Cleanup
    dino_embed._session = None
    dino_embed._variant_loaded = None


# -- Embedding serialization --


def test_embedding_to_blob_roundtrip():
    """embedding_to_blob -> blob_to_embedding preserves data exactly."""
    from dino_embed import blob_to_embedding, embedding_to_blob

    emb = np.random.randn(768).astype(np.float32)
    blob = embedding_to_blob(emb)

    assert isinstance(blob, bytes)
    assert len(blob) == 768 * 4  # float32 = 4 bytes

    recovered = blob_to_embedding(blob)
    np.testing.assert_array_equal(emb, recovered)


def test_embedding_to_blob_different_dims():
    """Serialization works for all DINOv2 embedding dimensions."""
    from dino_embed import blob_to_embedding, embedding_to_blob

    for dim in [384, 768, 1024]:
        emb = np.random.randn(dim).astype(np.float32)
        blob = embedding_to_blob(emb)
        recovered = blob_to_embedding(blob)
        assert recovered.shape == (dim,)
        np.testing.assert_array_equal(emb, recovered)


def test_embedding_to_blob_preserves_float32():
    """Output dtype is always float32."""
    from dino_embed import blob_to_embedding, embedding_to_blob

    # Even if input is float64, output should be float32
    emb = np.random.randn(768).astype(np.float64)
    blob = embedding_to_blob(emb)
    recovered = blob_to_embedding(blob)
    assert recovered.dtype == np.float32


# -- DB storage --


def test_db_embedding_storage_roundtrip(tmp_path):
    """DINOv2 embeddings survive DB write -> read as BLOBs."""
    from db import Database
    from dino_embed import blob_to_embedding, embedding_to_blob

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="root")
    pid = db.add_photo(fid, "bird.jpg", ".jpg", 100, 1.0)

    # Create fake embeddings
    subj_emb = np.random.randn(768).astype(np.float32)
    global_emb = np.random.randn(768).astype(np.float32)

    db.update_photo_embeddings(
        pid,
        dino_subject_embedding=embedding_to_blob(subj_emb),
        dino_global_embedding=embedding_to_blob(global_emb),
    )

    row = db.conn.execute(
        "SELECT dino_subject_embedding, dino_global_embedding FROM photos WHERE id=?",
        (pid,),
    ).fetchone()

    recovered_subj = blob_to_embedding(row["dino_subject_embedding"])
    recovered_global = blob_to_embedding(row["dino_global_embedding"])

    np.testing.assert_array_equal(subj_emb, recovered_subj)
    np.testing.assert_array_equal(global_emb, recovered_global)


def test_db_embedding_null_by_default(tmp_path):
    """Embedding columns are NULL when no embedding has been stored."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="root")
    pid = db.add_photo(fid, "bird.jpg", ".jpg", 100, 1.0)

    row = db.conn.execute(
        "SELECT dino_subject_embedding, dino_global_embedding FROM photos WHERE id=?",
        (pid,),
    ).fetchone()

    assert row["dino_subject_embedding"] is None
    assert row["dino_global_embedding"] is None


def test_db_embedding_partial_update(tmp_path):
    """Updating only subject embedding doesn't clobber a previously stored global."""
    from db import Database
    from dino_embed import blob_to_embedding, embedding_to_blob

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="root")
    pid = db.add_photo(fid, "bird.jpg", ".jpg", 100, 1.0)

    global_emb = np.ones(768, dtype=np.float32)
    subj_emb = np.zeros(768, dtype=np.float32)

    # Store both
    db.update_photo_embeddings(
        pid,
        dino_subject_embedding=embedding_to_blob(subj_emb),
        dino_global_embedding=embedding_to_blob(global_emb),
    )

    # Update only subject
    new_subj = np.full(768, 0.5, dtype=np.float32)
    db.update_photo_embeddings(
        pid,
        dino_subject_embedding=embedding_to_blob(new_subj),
        dino_global_embedding=embedding_to_blob(global_emb),
    )

    row = db.conn.execute(
        "SELECT dino_subject_embedding, dino_global_embedding FROM photos WHERE id=?",
        (pid,),
    ).fetchone()

    recovered_subj = blob_to_embedding(row["dino_subject_embedding"])
    recovered_global = blob_to_embedding(row["dino_global_embedding"])

    np.testing.assert_array_equal(new_subj, recovered_subj)
    np.testing.assert_array_equal(global_emb, recovered_global)


# -- Input size constant --


def test_input_size_constant():
    """DINOv2 input size should be 518 (native resolution)."""
    from dino_embed import DINOV2_INPUT_SIZE

    assert DINOV2_INPUT_SIZE == 518


# -- ensure_dinov2_weights (auto-download on first pipeline run) --


def test_ensure_dinov2_weights_noop_when_present(tmp_path, monkeypatch):
    """ensure_dinov2_weights() returns path without downloading when file
    is already on disk."""
    import sys
    import types

    import dino_embed

    model_dir = tmp_path / "dinov2-vit-b14"
    model_dir.mkdir()
    model_path = model_dir / "model.onnx"
    model_path.write_bytes(b"x" * 1024)

    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    def fake_hf_hub_download(**kwargs):
        raise AssertionError("must not download when file already exists")

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    progress = []
    result = dino_embed.ensure_dinov2_weights(
        "vit-b14", progress_callback=lambda p, c, t: progress.append((p, c, t))
    )

    assert result == str(model_path)
    assert progress == []


def test_ensure_dinov2_weights_downloads_when_missing(tmp_path, monkeypatch):
    """ensure_dinov2_weights() fetches model.onnx and surfaces progress."""
    import sys
    import types

    import dino_embed

    model_dir = tmp_path / "dinov2-vit-b14"
    model_path = model_dir / "model.onnx"

    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    cache_path = tmp_path / "hf-cache" / "model.onnx"
    cache_path.parent.mkdir()
    cache_path.write_bytes(b"m" * 4096)

    seen_requests = []

    def fake_hf_hub_download(**kwargs):
        seen_requests.append((kwargs["filename"], kwargs["subfolder"]))
        return str(cache_path)

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    progress = []
    result = dino_embed.ensure_dinov2_weights(
        "vit-b14",
        progress_callback=lambda p, c, t: progress.append((p, c, t)),
    )

    assert result == str(model_path)
    assert model_path.read_bytes() == b"m" * 4096
    assert seen_requests == [("model.onnx", "dinov2-vit-b14")]
    assert progress[0] == (progress[0][0], 0, 1)
    assert progress[-1][1] == 1 and progress[-1][2] == 1


def test_ensure_dinov2_weights_raises_on_download_failure(tmp_path, monkeypatch):
    """A failed download must raise RuntimeError and leave no partial file
    at the final path."""
    import sys
    import types

    import dino_embed
    import pytest

    model_dir = tmp_path / "dinov2-vit-b14"
    model_path = model_dir / "model.onnx"
    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    def fake_hf_hub_download(**kwargs):
        raise ConnectionError("network unreachable")

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    with pytest.raises(RuntimeError, match="Failed to download DINOv2"):
        dino_embed.ensure_dinov2_weights("vit-b14")

    assert not model_path.exists()


def test_ensure_dinov2_weights_rejects_unknown_variant():
    """Guard against typos that would otherwise fetch from a wrong repo path."""
    import dino_embed
    import pytest

    with pytest.raises(ValueError, match="Unknown DINOv2 variant"):
        dino_embed.ensure_dinov2_weights("vit-xxl")
