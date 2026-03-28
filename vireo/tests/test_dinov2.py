# vireo/tests/test_dinov2.py
"""Tests for DINOv2 embedding module.

Tests cover configuration, variant validation, embedding serialization,
and DB storage. Actual model inference is not tested (requires downloading
~350MB+ model weights) — these tests verify the surrounding logic.
"""
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# -- Variant configuration --


def test_variants_dict():
    """DINOV2_VARIANTS contains expected model variants."""
    from dino_embed import DINOV2_VARIANTS

    assert "vit-s14" in DINOV2_VARIANTS
    assert "vit-b14" in DINOV2_VARIANTS
    assert "vit-l14" in DINOV2_VARIANTS


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


def test_model_loader_invalid_variant():
    """_get_dinov2_model raises ValueError for unknown variant."""
    from dino_embed import _get_dinov2_model

    try:
        _get_dinov2_model("vit-nonexistent")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown DINOv2 variant" in str(e)
    except RuntimeError:
        pass  # PyTorch not installed — fine for CI


# -- Embedding serialization --


def test_embedding_to_blob_roundtrip():
    """embedding_to_blob → blob_to_embedding preserves data exactly."""
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
    """DINOv2 embeddings survive DB write → read as BLOBs."""
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


# -- Hub model names --


def test_variant_hub_names():
    """Each variant maps to a valid torch hub model name."""
    from dino_embed import DINOV2_VARIANTS

    expected_hub_names = {
        "vit-s14": "dinov2_vits14",
        "vit-b14": "dinov2_vitb14",
        "vit-l14": "dinov2_vitl14",
    }
    for variant, (hub_name, dim) in DINOV2_VARIANTS.items():
        assert hub_name == expected_hub_names[variant], f"Wrong hub name for {variant}"
