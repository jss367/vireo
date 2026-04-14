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


def _install_fake_hf(monkeypatch, hf_hub_download, repo_sha="a" * 40):
    """Install a stub ``huggingface_hub`` module with ``hf_hub_download``
    and an ``HfApi`` whose ``model_info`` returns a fixed SHA (so the
    upfront revision-pin call in ``ensure_dinov2_weights`` resolves
    deterministically).  Returns the SHA that tests can assert against.
    """
    import sys
    import types

    class _FakeModelInfo:
        def __init__(self, sha):
            self.sha = sha

    class _FakeHfApi:
        def model_info(self, repo_id):
            return _FakeModelInfo(repo_sha)

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = hf_hub_download
    fake_hf.HfApi = _FakeHfApi
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)
    return repo_sha


def test_ensure_dinov2_weights_noop_only_when_stub_and_sidecar_present(
    tmp_path, monkeypatch,
):
    """ensure_dinov2_weights() short-circuits only when BOTH ``model.onnx``
    and ``model.onnx.data`` are on disk.  A stub-only install must NOT
    count as complete."""
    import dino_embed

    model_dir = tmp_path / "dinov2-vit-b14"
    model_dir.mkdir()
    model_path = model_dir / "model.onnx"
    data_path = model_dir / "model.onnx.data"
    model_path.write_bytes(b"x" * 1024)
    data_path.write_bytes(b"X" * 4096)

    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    def fake_hf_hub_download(**kwargs):
        raise AssertionError("must not download when both files exist")

    _install_fake_hf(monkeypatch, fake_hf_hub_download)

    progress = []
    result = dino_embed.ensure_dinov2_weights(
        "vit-b14", progress_callback=lambda p, c, t: progress.append((p, c, t))
    )

    assert result == str(model_path)
    assert progress == []


def test_ensure_dinov2_weights_refetches_when_only_stub_present(
    tmp_path, monkeypatch,
):
    """Regression: a previous install that left only the ~1 MB ``model.onnx``
    graph stub (without the ``model.onnx.data`` sidecar) must trigger a
    refetch of both files, not persist silently.

    This is the bug users hit after a pre-#550 UI download: the stub
    passed the noop check, the sidecar never arrived, and every pipeline
    run died with ``model_path must not be empty`` from ONNX Runtime.
    """
    import dino_embed

    model_dir = tmp_path / "dinov2-vit-b14"
    model_dir.mkdir()
    model_path = model_dir / "model.onnx"
    data_path = model_dir / "model.onnx.data"
    # Stub-only partial install from a prior broken download.
    model_path.write_bytes(b"stub" * 256)

    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    cache_dir = tmp_path / "hf-cache"
    cache_dir.mkdir()
    (cache_dir / "model.onnx").write_bytes(b"M" * 1024)
    (cache_dir / "model.onnx.data").write_bytes(b"D" * 8192)

    requested = []

    def fake_hf_hub_download(**kwargs):
        requested.append(kwargs["filename"])
        return str(cache_dir / kwargs["filename"])

    _install_fake_hf(monkeypatch, fake_hf_hub_download)

    dino_embed.ensure_dinov2_weights("vit-b14")

    assert requested == ["model.onnx", "model.onnx.data"]
    assert model_path.read_bytes() == b"M" * 1024
    assert data_path.read_bytes() == b"D" * 8192


def test_ensure_dinov2_weights_refetches_when_only_sidecar_present(
    tmp_path, monkeypatch,
):
    """The mirror case: sidecar on disk from a half-finished install but
    no graph stub.  Both files must be fetched."""
    import dino_embed

    model_dir = tmp_path / "dinov2-vit-b14"
    model_dir.mkdir()
    model_path = model_dir / "model.onnx"
    data_path = model_dir / "model.onnx.data"
    data_path.write_bytes(b"sidecar only" * 100)

    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    cache_dir = tmp_path / "hf-cache"
    cache_dir.mkdir()
    (cache_dir / "model.onnx").write_bytes(b"M" * 1024)
    (cache_dir / "model.onnx.data").write_bytes(b"D" * 8192)

    requested = []

    def fake_hf_hub_download(**kwargs):
        requested.append(kwargs["filename"])
        return str(cache_dir / kwargs["filename"])

    _install_fake_hf(monkeypatch, fake_hf_hub_download)

    dino_embed.ensure_dinov2_weights("vit-b14")

    assert requested == ["model.onnx", "model.onnx.data"]
    assert model_path.read_bytes() == b"M" * 1024
    assert data_path.read_bytes() == b"D" * 8192


def test_ensure_dinov2_weights_downloads_both_when_missing(tmp_path, monkeypatch):
    """Fresh install fetches graph + external-data sidecar in order and
    surfaces progress for each step."""
    import dino_embed

    model_dir = tmp_path / "dinov2-vit-b14"
    model_path = model_dir / "model.onnx"
    data_path = model_dir / "model.onnx.data"

    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    cache_dir = tmp_path / "hf-cache"
    cache_dir.mkdir()
    (cache_dir / "model.onnx").write_bytes(b"m" * 1024)
    (cache_dir / "model.onnx.data").write_bytes(b"d" * 8192)

    seen_requests = []

    def fake_hf_hub_download(**kwargs):
        seen_requests.append((kwargs["filename"], kwargs["subfolder"]))
        return str(cache_dir / kwargs["filename"])

    _install_fake_hf(monkeypatch, fake_hf_hub_download)

    progress = []
    dino_embed.ensure_dinov2_weights(
        "vit-b14",
        progress_callback=lambda p, c, t: progress.append((p, c, t)),
    )

    assert seen_requests == [
        ("model.onnx", "dinov2-vit-b14"),
        ("model.onnx.data", "dinov2-vit-b14"),
    ]
    assert model_path.read_bytes() == b"m" * 1024
    assert data_path.read_bytes() == b"d" * 8192
    # (initial announce, mid-download update, final ready) — total=2.
    assert progress[0][1] == 0 and progress[0][2] == 2
    assert progress[-1][1] == 2 and progress[-1][2] == 2


def test_ensure_dinov2_weights_raises_on_graph_download_failure(
    tmp_path, monkeypatch,
):
    """A failed graph download must raise RuntimeError and leave no
    partial files at the final paths."""
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

    _install_fake_hf(monkeypatch, fake_hf_hub_download)

    with pytest.raises(RuntimeError, match="Failed to download DINOv2"):
        dino_embed.ensure_dinov2_weights("vit-b14")

    assert not model_path.exists()
    assert not (model_dir / "model.onnx.data").exists()


def test_ensure_dinov2_weights_raises_on_sidecar_download_failure(
    tmp_path, monkeypatch,
):
    """If the graph fetch succeeds but the sidecar fetch fails, NEITHER
    file should be left at its final path.  Otherwise a future run would
    see `model.onnx` on disk with no sidecar and reproduce the broken
    state we're trying to prevent."""
    import dino_embed
    import pytest

    model_dir = tmp_path / "dinov2-vit-b14"
    model_path = model_dir / "model.onnx"
    data_path = model_dir / "model.onnx.data"

    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    cache_dir = tmp_path / "hf-cache"
    cache_dir.mkdir()
    (cache_dir / "model.onnx").write_bytes(b"graph" * 200)

    def fake_hf_hub_download(**kwargs):
        if kwargs["filename"] == "model.onnx.data":
            raise ConnectionError("sidecar network failed")
        return str(cache_dir / kwargs["filename"])

    _install_fake_hf(monkeypatch, fake_hf_hub_download)

    with pytest.raises(RuntimeError, match="Failed to download DINOv2"):
        dino_embed.ensure_dinov2_weights("vit-b14")

    # Neither final file should exist — the graph tmp is cleaned up and
    # never promoted, so a retry will re-download both.
    assert not model_path.exists()
    assert not data_path.exists()


def test_ensure_dinov2_weights_rejects_unknown_variant():
    """Guard against typos that would otherwise fetch from a wrong repo path."""
    import dino_embed
    import pytest

    with pytest.raises(ValueError, match="Unknown DINOv2 variant"):
        dino_embed.ensure_dinov2_weights("vit-xxl")


def test_ensure_dinov2_weights_pins_both_fetches_to_same_revision(
    tmp_path, monkeypatch,
):
    """Both ``hf_hub_download`` calls must share the same ``revision`` so
    a push to the ONNX repo between the graph and sidecar fetches can't
    produce a mismatched pair that ONNX Runtime refuses to load.

    The revision is resolved upfront via ``HfApi.model_info`` before
    either fetch runs.
    """
    import dino_embed

    model_dir = tmp_path / "dinov2-vit-b14"
    model_path = model_dir / "model.onnx"

    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    cache_dir = tmp_path / "hf-cache"
    cache_dir.mkdir()
    (cache_dir / "model.onnx").write_bytes(b"m" * 1024)
    (cache_dir / "model.onnx.data").write_bytes(b"d" * 8192)

    seen_revisions = []

    def fake_hf_hub_download(**kwargs):
        seen_revisions.append(kwargs.get("revision"))
        return str(cache_dir / kwargs["filename"])

    pinned = _install_fake_hf(monkeypatch, fake_hf_hub_download, repo_sha="b" * 40)

    dino_embed.ensure_dinov2_weights("vit-b14")

    # Both fetches must use the same non-None revision — the SHA
    # resolved from HfApi.model_info before either download started.
    assert len(seen_revisions) == 2
    assert seen_revisions[0] == pinned
    assert seen_revisions[1] == pinned


def test_ensure_dinov2_weights_falls_back_when_model_info_fails(
    tmp_path, monkeypatch,
):
    """If ``HfApi.model_info`` raises (e.g. transient network blip), we
    must still attempt both fetches with ``revision=None`` rather than
    fail the whole install.  This is best-effort graceful degradation."""
    import sys
    import types

    import dino_embed

    model_dir = tmp_path / "dinov2-vit-b14"
    model_path = model_dir / "model.onnx"
    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    cache_dir = tmp_path / "hf-cache"
    cache_dir.mkdir()
    (cache_dir / "model.onnx").write_bytes(b"m" * 1024)
    (cache_dir / "model.onnx.data").write_bytes(b"d" * 8192)

    seen_revisions = []

    def fake_hf_hub_download(**kwargs):
        seen_revisions.append(kwargs.get("revision"))
        return str(cache_dir / kwargs["filename"])

    class _BrokenHfApi:
        def model_info(self, repo_id):
            raise ConnectionError("HfApi network blip")

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    fake_hf.HfApi = _BrokenHfApi
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    dino_embed.ensure_dinov2_weights("vit-b14")

    # Both fetches fall back to revision=None (HF default branch).
    assert seen_revisions == [None, None]
    assert model_path.read_bytes() == b"m" * 1024


def test_ensure_dinov2_weights_rolls_back_sidecar_if_graph_replace_fails(
    tmp_path, monkeypatch,
):
    """If the graph ``os.replace`` fails after the sidecar has already
    been promoted (e.g. Windows file lock on the existing ``model.onnx``),
    the new sidecar must be rolled back to the prior install's sidecar.
    Otherwise the on-disk pair is mismatched and the noop check on the
    next run would short-circuit, sticking the user on a broken install
    forever.
    """
    import dino_embed
    import pytest

    model_dir = tmp_path / "dinov2-vit-b14"
    model_dir.mkdir()
    model_path = model_dir / "model.onnx"
    data_path = model_dir / "model.onnx.data"
    # Prior partial install: sidecar present, graph missing.  That
    # combination triggers a refetch (both-files check fails), exercising
    # the rollback path: new sidecar replaces old sidecar, new graph
    # replace fails, old sidecar must be restored.
    data_path.write_bytes(b"OLD_SIDECAR" * 400)
    prior_sidecar = data_path.read_bytes()

    monkeypatch.setattr(
        dino_embed, "_dinov2_model_path",
        lambda variant: (str(model_dir), str(model_path)),
    )

    cache_dir = tmp_path / "hf-cache"
    cache_dir.mkdir()
    (cache_dir / "model.onnx").write_bytes(b"NEW_GRAPH" * 100)
    (cache_dir / "model.onnx.data").write_bytes(b"NEW_SIDECAR" * 500)

    def fake_hf_hub_download(**kwargs):
        return str(cache_dir / kwargs["filename"])

    _install_fake_hf(monkeypatch, fake_hf_hub_download)

    # Make the graph replace (second os.replace call) blow up while the
    # sidecar replace (first call) succeeds.
    real_replace = os.replace
    calls = {"n": 0}

    def flaky_replace(src, dst):
        calls["n"] += 1
        # Call sequence inside the try block:
        #   1. backup old sidecar   → allow
        #   2. promote new sidecar  → allow
        #   3. promote new graph    → BOOM (simulate Windows lock)
        if calls["n"] == 3:
            raise PermissionError("simulated file-lock on model.onnx")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)

    with pytest.raises(RuntimeError, match="Failed to download DINOv2"):
        dino_embed.ensure_dinov2_weights("vit-b14")

    # Sidecar must be back to its prior contents — no mismatched
    # "new sidecar + missing graph" pair left on disk.
    assert data_path.read_bytes() == prior_sidecar
    # Graph replace never promoted, so model.onnx stays absent — the
    # next run will see an incomplete install and refetch cleanly.
    assert not model_path.exists()
    # No leftover .prev backup or .download tmps.
    assert not (model_dir / "model.onnx.data.prev").exists()
    assert not (model_dir / "model.onnx.download").exists()
    assert not (model_dir / "model.onnx.data.download").exists()
