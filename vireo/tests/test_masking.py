# vireo/tests/test_masking.py
"""Tests for the SAM2 masking pipeline utilities.

Tests cover image processing utilities (crop, blur, completeness, mask I/O)
using synthetic images. SAM2 model inference is not tested here — that requires
the model weights. These tests verify the surrounding pipeline logic.
"""
import os
import sys

import numpy as np
from PIL import Image

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# -- Schema migration --


def test_pipeline_columns_exist(tmp_path):
    """New pipeline columns are created on fresh databases."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    # All new columns should be queryable
    row = db.conn.execute(
        """SELECT mask_path, dino_subject_embedding, dino_global_embedding,
                  subject_tenengrad, bg_tenengrad, crop_complete, bg_separation,
                  subject_clip_high, subject_clip_low, subject_y_median, phash_crop
           FROM photos LIMIT 0"""
    ).fetchone()
    # No exception means columns exist
    assert row is None  # empty table


# -- update_photo_pipeline_features --


def test_update_photo_pipeline_features(tmp_path):
    """update_photo_pipeline_features updates only provided columns."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="root")
    pid = db.add_photo(fid, "bird.jpg", ".jpg", 100, 1.0)

    db.update_photo_pipeline_features(
        pid,
        mask_path="/masks/1.png",
        crop_complete=0.95,
        subject_clip_high=0.02,
    )

    row = db.conn.execute(
        "SELECT mask_path, crop_complete, subject_clip_high, subject_tenengrad "
        "FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row[0] == "/masks/1.png"
    assert abs(row[1] - 0.95) < 0.001
    assert abs(row[2] - 0.02) < 0.001
    assert row[3] is None  # not provided, so unchanged


# -- get_photos_missing_masks --


def test_get_photos_missing_masks(tmp_path):
    """get_photos_missing_masks returns photos with detection but no mask."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="root")

    # Photo with detection, no mask — should appear
    pid1 = db.add_photo(fid, "a.jpg", ".jpg", 100, 1.0)
    db.save_detections(pid1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9},
    ], detector_model="megadetector")

    # Photo with no detection — should NOT appear
    pid2 = db.add_photo(fid, "b.jpg", ".jpg", 100, 1.0)

    # Photo with detection AND mask — should NOT appear
    pid3 = db.add_photo(fid, "c.jpg", ".jpg", 100, 1.0)
    db.save_detections(pid3, [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.3}, "confidence": 0.8},
    ], detector_model="megadetector")
    db.update_photo_pipeline_features(pid3, mask_path="/masks/3.png")

    photos = db.get_photos_missing_masks()
    assert len(photos) == 1
    assert photos[0]["id"] == pid1


# -- Mask save / load round-trip --


def test_mask_save_load_roundtrip(tmp_path):
    """Masks survive a save→load cycle as boolean arrays."""
    from masking import load_mask, save_mask

    masks_dir = str(tmp_path / "masks")
    mask = np.zeros((100, 150), dtype=bool)
    mask[20:60, 30:90] = True  # rectangular subject

    path = save_mask(mask, masks_dir, photo_id=42, variant="sam2-small")
    assert os.path.exists(path)
    assert path.endswith("42.sam2-small.png")

    loaded = load_mask(masks_dir, photo_id=42, variant="sam2-small")
    assert loaded is not None
    assert loaded.shape == (100, 150)
    assert loaded.dtype == bool
    np.testing.assert_array_equal(mask, loaded)


def test_load_mask_missing(tmp_path):
    """load_mask returns None when mask file doesn't exist."""
    from masking import load_mask

    result = load_mask(str(tmp_path), photo_id=999, variant="sam2-small")
    assert result is None


def test_save_mask_uses_variant_in_filename(tmp_path):
    """save_mask writes ``{photo_id}.{variant}.png`` so multiple SAM
    variants for the same photo coexist on disk."""
    from masking import save_mask

    mask = np.array([[True, False], [False, True]], dtype=bool)
    out = save_mask(
        mask, str(tmp_path), photo_id=42, variant="sam2-large",
    )
    assert out == str(tmp_path / "42.sam2-large.png")
    assert (tmp_path / "42.sam2-large.png").exists()


def test_save_mask_per_variant_files_coexist(tmp_path):
    """Saving the same photo under two different variants leaves both
    files on disk side-by-side."""
    from masking import save_mask

    mask = np.zeros((4, 4), dtype=bool)
    mask[1:3, 1:3] = True

    p_small = save_mask(mask, str(tmp_path), photo_id=7, variant="sam2-small")
    p_large = save_mask(mask, str(tmp_path), photo_id=7, variant="sam2-large")
    assert p_small != p_large
    assert os.path.isfile(p_small) and os.path.isfile(p_large)


# -- crop_subject --


def test_crop_subject_basic(tmp_path):
    """crop_subject returns a padded crop around the mask bounding box."""
    from masking import crop_subject

    # 200x300 image with a 40x60 subject region
    img = Image.new("RGB", (300, 200), color=(0, 128, 0))
    mask = np.zeros((200, 300), dtype=bool)
    mask[50:90, 80:140] = True  # subject at y=[50,90], x=[80,140]

    crop = crop_subject(img, mask, margin=0.10)
    assert crop is not None
    cw, ch = crop.size
    # Should be larger than the mask bbox (40x60) due to margin
    assert cw > 60
    assert ch > 40
    # Should be smaller than the full image
    assert cw < 300
    assert ch < 200


def test_crop_subject_empty_mask():
    """crop_subject returns None for an empty mask."""
    from masking import crop_subject

    img = Image.new("RGB", (100, 100))
    mask = np.zeros((100, 100), dtype=bool)
    assert crop_subject(img, mask) is None


# -- blur_background --


def test_blur_background_preserves_subject():
    """blur_background keeps subject pixels intact, blurs background."""
    from masking import blur_background

    # Solid red image with a green subject patch
    img = Image.new("RGB", (200, 200), color=(255, 0, 0))
    arr = np.array(img)
    arr[50:100, 50:100] = [0, 255, 0]
    img = Image.fromarray(arr)

    mask = np.zeros((200, 200), dtype=bool)
    mask[50:100, 50:100] = True

    result = blur_background(img, mask, radius=21)
    result_arr = np.array(result)

    # Subject center should still be pure green
    center = result_arr[75, 75]
    assert center[0] == 0 and center[1] == 255 and center[2] == 0

    # Background corner should be blurred (still reddish, but not pure red
    # if near the subject boundary; far corner should be pure red still)
    corner = result_arr[5, 5]
    # The corner is far from subject, so after blur it should still be reddish
    assert corner[0] > 200  # still mostly red


def test_blur_background_different_from_original():
    """Background pixels near the subject should differ after blur."""
    from masking import blur_background

    # Create an image with sharp edges near the subject
    arr = np.zeros((100, 100, 3), dtype=np.uint8)
    arr[:50, :] = [255, 0, 0]  # top half red
    arr[50:, :] = [0, 0, 255]  # bottom half blue
    img = Image.fromarray(arr)

    mask = np.zeros((100, 100), dtype=bool)
    mask[45:55, 40:60] = True  # small subject at the boundary

    result = blur_background(img, mask, radius=15)
    result_arr = np.array(result)

    # A background pixel near the subject should be blurred (not the sharp edge)
    bg_pixel = result_arr[40, 50]  # just above subject, was pure red
    # After blur near the red/blue boundary, blue should bleed in
    # (this pixel is close to the border so some blue should mix in)
    assert bg_pixel[2] > 0 or bg_pixel[0] < 255  # some blurring happened


# -- crop_completeness --


def test_crop_completeness_full_interior():
    """A mask fully inside the frame should score ~1.0."""
    from masking import crop_completeness

    mask = np.zeros((200, 300), dtype=bool)
    mask[50:150, 80:220] = True  # well inside the frame

    score = crop_completeness(mask)
    assert score > 0.95


def test_crop_completeness_touching_edge():
    """A mask touching the frame edge should score < 1.0."""
    from masking import crop_completeness

    mask = np.zeros((200, 300), dtype=bool)
    mask[0:100, 80:220] = True  # touching top edge

    score = crop_completeness(mask)
    assert score < 0.90


def test_crop_completeness_half_clipped():
    """A mask with roughly half its perimeter clipped should score ~0.5."""
    from masking import crop_completeness

    # Fill the entire left half — touches top, bottom, and left edges
    mask = np.zeros((100, 100), dtype=bool)
    mask[:, :50] = True

    score = crop_completeness(mask)
    # Should be roughly 0.25-0.5 (only right edge of mask is interior)
    assert 0.1 < score < 0.6


def test_crop_completeness_empty_mask():
    """An empty mask returns 0.0."""
    from masking import crop_completeness

    mask = np.zeros((100, 100), dtype=bool)
    assert crop_completeness(mask) == 0.0


# -- render_proxy --


def test_render_proxy(tmp_path):
    """render_proxy loads an image at working resolution."""
    from masking import render_proxy

    src = str(tmp_path / "big.jpg")
    Image.new("RGB", (4000, 3000)).save(src)

    proxy = render_proxy(src, longest_edge=1536)
    assert proxy is not None
    assert max(proxy.size) <= 1536


def test_render_proxy_missing_file(tmp_path):
    """render_proxy returns None for missing files."""
    from masking import render_proxy

    result = render_proxy(str(tmp_path / "nope.jpg"))
    assert result is None


# -- SAM2 variant validation --


def test_sam2_variant_validation():
    """_get_sam2_sessions rejects unknown variants."""
    from masking import _get_sam2_sessions

    try:
        _get_sam2_sessions("sam2-nonexistent")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown SAM2 variant" in str(e)


def test_sam2_variants_dict():
    """SAM2_VARIANTS contains the expected model variants."""
    from masking import SAM2_VARIANTS

    assert "sam2-tiny" in SAM2_VARIANTS
    assert "sam2-small" in SAM2_VARIANTS
    assert "sam2-base-plus" in SAM2_VARIANTS
    assert "sam2-large" in SAM2_VARIANTS


def test_sam2_sessions_missing_model(tmp_path):
    """_get_sam2_sessions raises FileNotFoundError if model files missing."""
    import masking

    # Reset singletons
    masking._encoder_session = None
    masking._decoder_session = None
    masking._sam2_variant_loaded = None

    from unittest.mock import patch

    with patch("os.path.expanduser", return_value=str(tmp_path)):
        try:
            masking._get_sam2_sessions("sam2-small")
            assert False, "Should have raised FileNotFoundError"
        except FileNotFoundError as e:
            assert "SAM2" in str(e)

    # Cleanup
    masking._encoder_session = None
    masking._decoder_session = None
    masking._sam2_variant_loaded = None


def test_sam2_sessions_singleton_caching():
    """Sessions are cached and reused for same variant."""
    from unittest.mock import MagicMock

    import masking

    mock_enc = MagicMock()
    mock_dec = MagicMock()
    masking._encoder_session = mock_enc
    masking._decoder_session = mock_dec
    masking._sam2_variant_loaded = "sam2-small"

    enc, dec = masking._get_sam2_sessions("sam2-small")
    assert enc is mock_enc
    assert dec is mock_dec

    # Cleanup
    masking._encoder_session = None
    masking._decoder_session = None
    masking._sam2_variant_loaded = None


def test_sam2_sessions_reloads_for_different_variant(tmp_path):
    """Sessions are reloaded when variant changes."""
    from unittest.mock import MagicMock, patch

    import masking

    mock_old_enc = MagicMock()
    mock_old_dec = MagicMock()
    masking._encoder_session = mock_old_enc
    masking._decoder_session = mock_old_dec
    masking._sam2_variant_loaded = "sam2-tiny"

    # Create fake model files for sam2-small
    model_dir = tmp_path / ".vireo" / "models" / "sam2-small"
    model_dir.mkdir(parents=True)
    (model_dir / "image_encoder.onnx").write_bytes(b"fake")
    (model_dir / "mask_decoder.onnx").write_bytes(b"fake")

    mock_new_enc = MagicMock()
    mock_new_dec = MagicMock()
    call_count = [0]

    def mock_create_session(path):
        result = mock_new_enc if call_count[0] == 0 else mock_new_dec
        call_count[0] += 1
        return result

    with patch("os.path.expanduser", return_value=str(tmp_path)):
        with patch("masking.onnx_runtime.create_session", side_effect=mock_create_session):
            enc, dec = masking._get_sam2_sessions("sam2-small")

    assert enc is mock_new_enc
    assert dec is mock_new_dec
    assert masking._sam2_variant_loaded == "sam2-small"

    # Cleanup
    masking._encoder_session = None
    masking._decoder_session = None
    masking._sam2_variant_loaded = None


def test_sam2_input_size_constant():
    """SAM2 input size should be 1024 (native resolution)."""
    from masking import SAM2_INPUT_SIZE

    assert SAM2_INPUT_SIZE == 1024


def test_generate_mask_with_mock():
    """generate_mask returns a boolean mask using mocked ONNX sessions."""
    from unittest.mock import MagicMock

    import masking

    # Create mock sessions
    mock_enc = MagicMock()
    mock_dec = MagicMock()

    # Mock encoder output: image embeddings (1, 256, 64, 64)
    mock_enc.get_inputs.return_value = [MagicMock(name="image")]
    mock_enc.run.return_value = [np.zeros((1, 256, 64, 64), dtype=np.float32)]

    # Mock decoder output: masks (1, 3, 100, 150) and scores (1, 3)
    mock_dec_input_1 = MagicMock()
    mock_dec_input_1.name = "image_embeddings"
    mock_dec_input_2 = MagicMock()
    mock_dec_input_2.name = "point_coords"
    mock_dec_input_3 = MagicMock()
    mock_dec_input_3.name = "point_labels"
    mock_dec_input_4 = MagicMock()
    mock_dec_input_4.name = "mask_input"
    mock_dec_input_5 = MagicMock()
    mock_dec_input_5.name = "has_mask_input"
    mock_dec_input_6 = MagicMock()
    mock_dec_input_6.name = "orig_im_size"
    mock_dec.get_inputs.return_value = [
        mock_dec_input_1, mock_dec_input_2, mock_dec_input_3,
        mock_dec_input_4, mock_dec_input_5, mock_dec_input_6,
    ]

    # Create mask output: 3 masks, second one is best
    masks = np.zeros((1, 3, 100, 150), dtype=np.float32)
    masks[0, 1, 20:60, 30:90] = 1.0  # best mask has a rectangular region
    scores = np.array([[0.5, 0.9, 0.3]], dtype=np.float32)
    mock_dec.run.return_value = [masks, scores]

    masking._encoder_session = mock_enc
    masking._decoder_session = mock_dec
    masking._sam2_variant_loaded = "sam2-small"

    img = Image.new("RGB", (150, 100))
    detection_box = {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}

    result = masking.generate_mask(img, detection_box, variant="sam2-small")

    assert result is not None
    assert result.shape == (100, 150)
    assert result.dtype == bool
    # The best mask (index 1) should have True values in the subject region
    assert result[40, 60]  # center of the mask region
    assert not result[5, 5]  # outside the mask region

    # Cleanup
    masking._encoder_session = None
    masking._decoder_session = None
    masking._sam2_variant_loaded = None


# -- ensure_sam2_weights (auto-download on first pipeline run) --


def test_ensure_sam2_weights_noop_when_present(tmp_path, monkeypatch):
    """ensure_sam2_weights() returns immediately when both files exist and
    must never touch Hugging Face."""
    import sys
    import types

    import masking

    model_dir = tmp_path / "sam2-small"
    model_dir.mkdir()
    (model_dir / "image_encoder.onnx").write_bytes(b"e" * 1024)
    (model_dir / "mask_decoder.onnx").write_bytes(b"d" * 1024)
    monkeypatch.setattr(
        masking, "_sam2_model_dir", lambda variant: str(model_dir)
    )

    download_calls = []

    def fake_hf_hub_download(**kwargs):
        download_calls.append(kwargs)
        raise AssertionError("hf_hub_download must not be called when files exist")

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    progress = []
    result = masking.ensure_sam2_weights(
        "sam2-small", progress_callback=lambda p, c, t: progress.append((p, c, t))
    )

    assert result == str(model_dir)
    assert download_calls == []
    assert progress == []


def test_ensure_sam2_weights_downloads_both_files(tmp_path, monkeypatch):
    """ensure_sam2_weights() fetches encoder + decoder and surfaces progress
    for each file."""
    import sys
    import types

    import masking

    model_dir = tmp_path / "sam2-small"
    monkeypatch.setattr(
        masking, "_sam2_model_dir", lambda variant: str(model_dir)
    )

    cache_dir = tmp_path / "hf-cache"
    cache_dir.mkdir()
    (cache_dir / "image_encoder.onnx").write_bytes(b"E" * 2048)
    (cache_dir / "mask_decoder.onnx").write_bytes(b"D" * 512)

    seen_requests = []

    def fake_hf_hub_download(**kwargs):
        seen_requests.append((kwargs["filename"], kwargs["subfolder"]))
        return str(cache_dir / kwargs["filename"])

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    progress = []
    result = masking.ensure_sam2_weights(
        "sam2-small",
        progress_callback=lambda p, c, t: progress.append((p, c, t)),
    )

    assert result == str(model_dir)
    assert (model_dir / "image_encoder.onnx").read_bytes() == b"E" * 2048
    assert (model_dir / "mask_decoder.onnx").read_bytes() == b"D" * 512
    assert seen_requests == [
        ("image_encoder.onnx", "sam2-small"),
        ("mask_decoder.onnx", "sam2-small"),
    ]
    # Initial announce + once per downloaded file.
    assert progress[0][1] == 0 and progress[0][2] == 2
    assert progress[-1][1] == 2 and progress[-1][2] == 2


def test_ensure_sam2_weights_downloads_only_missing_file(tmp_path, monkeypatch):
    """If the encoder is already on disk but the decoder isn't, only fetch
    the decoder.  Lets a user recover from a partial download without paying
    for the big encoder again."""
    import sys
    import types

    import masking

    model_dir = tmp_path / "sam2-small"
    model_dir.mkdir()
    (model_dir / "image_encoder.onnx").write_bytes(b"E" * 2048)
    monkeypatch.setattr(
        masking, "_sam2_model_dir", lambda variant: str(model_dir)
    )

    cache_dir = tmp_path / "hf-cache"
    cache_dir.mkdir()
    (cache_dir / "mask_decoder.onnx").write_bytes(b"D" * 512)

    requested = []

    def fake_hf_hub_download(**kwargs):
        requested.append(kwargs["filename"])
        return str(cache_dir / kwargs["filename"])

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    masking.ensure_sam2_weights("sam2-small")

    assert requested == ["mask_decoder.onnx"]
    assert (model_dir / "image_encoder.onnx").read_bytes() == b"E" * 2048


def test_ensure_sam2_weights_raises_on_download_failure(tmp_path, monkeypatch):
    """A failed download must raise RuntimeError with a remediation hint
    and leave no partial file at the final path."""
    import sys
    import types

    import masking
    import pytest

    model_dir = tmp_path / "sam2-small"
    monkeypatch.setattr(
        masking, "_sam2_model_dir", lambda variant: str(model_dir)
    )

    def fake_hf_hub_download(**kwargs):
        raise ConnectionError("network unreachable")

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    with pytest.raises(RuntimeError, match="Failed to download SAM2"):
        masking.ensure_sam2_weights("sam2-small")

    assert not (model_dir / "image_encoder.onnx").exists()
    assert not (model_dir / "mask_decoder.onnx").exists()


def test_ensure_sam2_weights_rejects_unknown_variant():
    """Guard against typos that would otherwise fetch from a wrong repo
    path."""
    import masking
    import pytest

    with pytest.raises(ValueError, match="Unknown SAM2 variant"):
        masking.ensure_sam2_weights("sam2-jumbo")
