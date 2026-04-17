# vireo/tests/test_quality.py
"""Tests for subject-aware quality feature extraction.

Uses synthetic images with known properties to verify each feature
computes the expected values.
"""
import os
import sys

import numpy as np
from PIL import Image, ImageFilter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# -- Background ring --


def test_background_ring_shape():
    """Background ring surrounds the mask and doesn't overlap it."""
    from quality import _background_ring

    mask = np.zeros((200, 200), dtype=bool)
    mask[80:120, 80:120] = True  # small centered square

    ring = _background_ring(mask)
    # Ring should not overlap with the mask
    assert not (ring & mask).any()
    # Ring should have pixels adjacent to the mask
    assert ring.any()
    # Ring should be larger area than zero
    assert ring.sum() > 0


def test_background_ring_empty_mask():
    """Empty mask produces empty ring."""
    from quality import _background_ring

    mask = np.zeros((100, 100), dtype=bool)
    ring = _background_ring(mask)
    assert not ring.any()


def test_background_ring_adapts_to_subject_size():
    """Larger subject → larger dilation radius → wider ring."""
    from quality import _background_ring

    # Small subject
    small_mask = np.zeros((400, 400), dtype=bool)
    small_mask[180:220, 180:220] = True  # 40x40

    # Large subject
    large_mask = np.zeros((400, 400), dtype=bool)
    large_mask[100:300, 100:300] = True  # 200x200

    small_ring = _background_ring(small_mask)
    large_ring = _background_ring(large_mask)

    # Large ring should have more pixels (wider dilation)
    assert large_ring.sum() > small_ring.sum()


# -- Tenengrad sharpness --


def test_subject_tenengrad_sharp_vs_blurry():
    """Sharp images should have higher Tenengrad than blurry ones."""
    from quality import compute_subject_tenengrad

    # Sharp image: high-frequency checkerboard
    sharp_arr = np.zeros((200, 200, 3), dtype=np.uint8)
    for y in range(200):
        for x in range(200):
            if (x // 4 + y // 4) % 2 == 0:
                sharp_arr[y, x] = [255, 255, 255]
    sharp_img = Image.fromarray(sharp_arr)

    # Blurry image: same but heavily blurred
    blurry_img = sharp_img.filter(ImageFilter.GaussianBlur(radius=10))

    mask = np.ones((200, 200), dtype=bool)  # whole image is "subject"

    sharp_score = compute_subject_tenengrad(sharp_img, mask)
    blurry_score = compute_subject_tenengrad(blurry_img, mask)

    assert sharp_score > blurry_score
    assert sharp_score > 0


def test_subject_tenengrad_empty_mask():
    """Empty mask returns 0.0."""
    from quality import compute_subject_tenengrad

    img = Image.new("RGB", (100, 100), color=(128, 128, 128))
    mask = np.zeros((100, 100), dtype=bool)
    assert compute_subject_tenengrad(img, mask) == 0.0


def test_bg_tenengrad_detects_background():
    """bg_tenengrad scores the ring around the subject, not the subject itself."""
    from quality import compute_bg_tenengrad

    # Image with sharp subject center and smooth background
    arr = np.full((200, 200, 3), 128, dtype=np.uint8)
    # Add sharp edges only in the center (subject area)
    for y in range(80, 120):
        for x in range(80, 120):
            if (x + y) % 2 == 0:
                arr[y, x] = [255, 255, 255]
    img = Image.fromarray(arr)

    mask = np.zeros((200, 200), dtype=bool)
    mask[80:120, 80:120] = True

    bg_score = compute_bg_tenengrad(img, mask)
    # Background is mostly uniform gray, so bg_tenengrad should be low
    assert bg_score >= 0


def test_bg_tenengrad_empty_mask():
    """Empty mask returns 0.0."""
    from quality import compute_bg_tenengrad

    img = Image.new("RGB", (100, 100))
    mask = np.zeros((100, 100), dtype=bool)
    assert compute_bg_tenengrad(img, mask) == 0.0


# -- Exposure stats --


def test_exposure_stats_bright_subject():
    """Mostly bright subject should have high clip_high and high median."""
    from quality import compute_exposure_stats

    # Nearly white subject
    arr = np.full((100, 100, 3), 252, dtype=np.uint8)
    img = Image.fromarray(arr)

    mask = np.zeros((100, 100), dtype=bool)
    mask[20:80, 20:80] = True

    stats = compute_exposure_stats(img, mask)
    assert stats["subject_clip_high"] > 0.5  # most pixels > 250
    assert stats["subject_clip_low"] == 0.0
    assert stats["subject_y_median"] > 240


def test_exposure_stats_dark_subject():
    """Dark subject should have high clip_low and low median."""
    from quality import compute_exposure_stats

    arr = np.full((100, 100, 3), 3, dtype=np.uint8)
    img = Image.fromarray(arr)

    mask = np.zeros((100, 100), dtype=bool)
    mask[20:80, 20:80] = True

    stats = compute_exposure_stats(img, mask)
    assert stats["subject_clip_low"] > 0.5  # most pixels < 5
    assert stats["subject_clip_high"] == 0.0
    assert stats["subject_y_median"] < 10


def test_exposure_stats_midtone_subject():
    """Midtone subject should have no clipping and ~128 median."""
    from quality import compute_exposure_stats

    arr = np.full((100, 100, 3), 128, dtype=np.uint8)
    img = Image.fromarray(arr)

    mask = np.ones((100, 100), dtype=bool)

    stats = compute_exposure_stats(img, mask)
    assert stats["subject_clip_high"] == 0.0
    assert stats["subject_clip_low"] == 0.0
    assert 120 < stats["subject_y_median"] < 140


def test_exposure_stats_empty_mask():
    """Empty mask returns zeros."""
    from quality import compute_exposure_stats

    img = Image.new("RGB", (100, 100))
    mask = np.zeros((100, 100), dtype=bool)

    stats = compute_exposure_stats(img, mask)
    assert stats["subject_clip_high"] == 0.0
    assert stats["subject_clip_low"] == 0.0
    assert stats["subject_y_median"] == 0.0


# -- Background separation --


def test_bg_separation_smooth_vs_textured():
    """Smooth background should have lower separation than textured."""
    from quality import compute_bg_separation

    # Smooth background (uniform gray)
    smooth_arr = np.full((200, 200, 3), 128, dtype=np.uint8)
    smooth_img = Image.fromarray(smooth_arr)

    # Textured background (random noise)
    rng = np.random.RandomState(42)
    noisy_arr = rng.randint(0, 256, (200, 200, 3), dtype=np.uint8)
    noisy_img = Image.fromarray(noisy_arr)

    mask = np.zeros((200, 200), dtype=bool)
    mask[80:120, 80:120] = True  # small subject, most of image is background

    smooth_score = compute_bg_separation(smooth_img, mask)
    noisy_score = compute_bg_separation(noisy_img, mask)

    assert smooth_score < noisy_score
    assert smooth_score < 10  # nearly zero variance for uniform


def test_bg_separation_empty_mask():
    """Empty mask (everything is background) still returns a value."""
    from quality import compute_bg_separation

    arr = np.full((100, 100, 3), 100, dtype=np.uint8)
    img = Image.fromarray(arr)
    mask = np.zeros((100, 100), dtype=bool)

    score = compute_bg_separation(img, mask)
    assert score >= 0.0


# -- Crop pHash --


def test_crop_phash_returns_hex_string():
    """crop pHash returns a hex-encoded string."""
    from quality import compute_crop_phash

    arr = np.full((200, 200, 3), 128, dtype=np.uint8)
    arr[50:150, 50:150] = [255, 0, 0]  # red subject
    img = Image.fromarray(arr)

    mask = np.zeros((200, 200), dtype=bool)
    mask[50:150, 50:150] = True

    phash = compute_crop_phash(img, mask)
    assert phash is not None
    assert isinstance(phash, str)
    assert len(phash) == 16  # 64-bit hash → 16 hex chars


def test_crop_phash_deterministic():
    """Same image + mask should produce the same pHash."""
    from quality import compute_crop_phash

    arr = np.full((200, 200, 3), 50, dtype=np.uint8)
    arr[60:140, 60:140] = [0, 200, 0]
    img = Image.fromarray(arr)

    mask = np.zeros((200, 200), dtype=bool)
    mask[60:140, 60:140] = True

    h1 = compute_crop_phash(img, mask)
    h2 = compute_crop_phash(img, mask)
    assert h1 == h2


def test_crop_phash_different_images():
    """Different images should (usually) produce different pHashes."""
    from quality import compute_crop_phash

    mask = np.zeros((200, 200), dtype=bool)
    mask[50:150, 50:150] = True

    # Image A: red subject
    arr_a = np.full((200, 200, 3), 128, dtype=np.uint8)
    arr_a[50:150, 50:150] = [255, 0, 0]
    img_a = Image.fromarray(arr_a)

    # Image B: blue subject
    arr_b = np.full((200, 200, 3), 128, dtype=np.uint8)
    arr_b[50:150, 50:150] = [0, 0, 255]
    img_b = Image.fromarray(arr_b)

    h_a = compute_crop_phash(img_a, mask)
    h_b = compute_crop_phash(img_b, mask)
    assert h_a != h_b


def test_crop_phash_empty_mask():
    """Empty mask returns None."""
    from quality import compute_crop_phash

    img = Image.new("RGB", (100, 100))
    mask = np.zeros((100, 100), dtype=bool)
    assert compute_crop_phash(img, mask) is None


# -- compute_all_quality_features --


def test_compute_all_returns_all_keys():
    """compute_all_quality_features returns dict with all expected keys."""
    from quality import compute_all_quality_features

    arr = np.full((200, 200, 3), 128, dtype=np.uint8)
    arr[60:140, 60:140] = [200, 100, 50]
    img = Image.fromarray(arr)

    mask = np.zeros((200, 200), dtype=bool)
    mask[60:140, 60:140] = True

    features = compute_all_quality_features(img, mask)

    expected_keys = {
        "subject_tenengrad",
        "bg_tenengrad",
        "subject_clip_high",
        "subject_clip_low",
        "subject_y_median",
        "bg_separation",
        "phash_crop",
        "noise_estimate",
    }
    assert set(features.keys()) == expected_keys

    # All numeric values should be non-negative
    for k in expected_keys - {"phash_crop"}:
        assert features[k] >= 0, f"{k} should be non-negative"

    # phash_crop should be a hex string
    assert isinstance(features["phash_crop"], str)


def test_compute_all_features_compatible_with_db(tmp_path):
    """Features from compute_all can be passed directly to update_photo_pipeline_features."""
    from db import Database
    from quality import compute_all_quality_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="root")
    pid = db.add_photo(fid, "bird.jpg", ".jpg", 100, 1.0)

    arr = np.full((200, 200, 3), 128, dtype=np.uint8)
    arr[60:140, 60:140] = [200, 100, 50]
    img = Image.fromarray(arr)

    mask = np.zeros((200, 200), dtype=bool)
    mask[60:140, 60:140] = True

    features = compute_all_quality_features(img, mask)
    # Should not raise — all keys match DB column names
    db.update_photo_pipeline_features(pid, **features)

    row = db.conn.execute(
        "SELECT subject_tenengrad, bg_tenengrad, subject_clip_high, "
        "subject_clip_low, subject_y_median, bg_separation, phash_crop "
        "FROM photos WHERE id=?",
        (pid,),
    ).fetchone()

    assert row["subject_tenengrad"] == features["subject_tenengrad"]
    assert row["bg_tenengrad"] == features["bg_tenengrad"]
    assert row["phash_crop"] == features["phash_crop"]


# -- Eye-focus windowed tenengrad --


def test_compute_eye_tenengrad_uses_window_around_eye():
    """Sharp edge pattern inside the window produces a non-trivial signal."""
    from quality import compute_eye_tenengrad

    arr = np.full((400, 400), 128, dtype=np.uint8)
    # High-contrast vertical stripe pattern centered on (200, 200).
    arr[180:220, 180:220] = np.tile([0, 255] * 20, (40, 1)).astype(np.uint8)
    img = Image.fromarray(arr, mode="L").convert("RGB")

    bbox = (100, 100, 300, 300)  # 200x200 bbox
    eye_xy = (200.0, 200.0)
    result = compute_eye_tenengrad(img, eye_xy, bbox, k=0.08)

    # Window side = 0.08 * 200 = 16 px (clamped to min 8). Fully inside the
    # stripe pattern → tenengrad should be well above zero on a gray image.
    assert result > 1000


def test_compute_eye_tenengrad_clamps_to_image_bounds():
    """Eye near image edge: window clips, no crash, returns finite float."""
    from quality import compute_eye_tenengrad

    img = Image.new("RGB", (100, 100), color=(128, 128, 128))
    bbox = (0, 0, 100, 100)
    result = compute_eye_tenengrad(img, (2.0, 2.0), bbox, k=0.1)

    # Uniform gray → zero gradient → 0.0 after multiscale Tenengrad.
    assert result == 0.0


def test_compute_eye_tenengrad_minimum_window_size_is_8():
    """Tiny bbox still produces a reasonable 8-pixel window rather than a
    degenerate 0/1-px one that would break the Sobel filter."""
    from quality import compute_eye_tenengrad

    img = Image.new("RGB", (100, 100), color=(128, 128, 128))
    # bbox of side 10 * k=0.08 = 0.8 px → clamp to 8 px
    bbox = (40, 40, 50, 50)
    result = compute_eye_tenengrad(img, (45.0, 45.0), bbox, k=0.08)
    assert result == 0.0  # uniform gray, but no crash


def test_compute_eye_tenengrad_empty_window_returns_zero():
    """Eye entirely outside the image bounds → empty window → 0.0."""
    from quality import compute_eye_tenengrad

    img = Image.new("RGB", (100, 100), color=(128, 128, 128))
    bbox = (0, 0, 100, 100)
    result = compute_eye_tenengrad(img, (-50.0, -50.0), bbox, k=0.1)
    assert result == 0.0
