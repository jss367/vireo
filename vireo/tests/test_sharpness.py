# vireo/tests/test_sharpness.py
"""Tests for sharpness scoring (sharpness.py).

Tests use synthetic PIL images to exercise compute_sharpness and
score_burst_group without needing real photo files. The image_loader
dependency is patched to return PIL images directly.
"""
import os
import sys

from PIL import Image, ImageDraw, ImageFilter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _save_sharp_image(path, size=(200, 200)):
    """Save a high-frequency (sharp) synthetic image."""
    img = Image.new("RGB", size)
    draw = ImageDraw.Draw(img)
    # Draw a checkerboard pattern — lots of edges
    for x in range(0, size[0], 4):
        for y in range(0, size[1], 4):
            if (x // 4 + y // 4) % 2 == 0:
                draw.rectangle([x, y, x + 3, y + 3], fill="white")
            else:
                draw.rectangle([x, y, x + 3, y + 3], fill="black")
    img.save(path)
    return img


def _save_blurry_image(path, size=(200, 200)):
    """Save a low-frequency (blurry) synthetic image."""
    img = Image.new("RGB", size, color=(128, 128, 128))
    # Apply heavy blur
    img = img.filter(ImageFilter.GaussianBlur(radius=10))
    img.save(path)
    return img


def _save_flat_image(path, size=(200, 200)):
    """Save a completely flat (uniform) image — zero variance."""
    img = Image.new("RGB", size, color=(100, 100, 100))
    img.save(path)
    return img


# ---------------------------------------------------------------------------
# compute_sharpness
# ---------------------------------------------------------------------------


def test_compute_sharpness_sharp_image(tmp_path):
    """Sharp (high-edge) image gets a higher score than blurry."""
    from sharpness import compute_sharpness

    sharp_path = str(tmp_path / "sharp.jpg")
    blurry_path = str(tmp_path / "blurry.jpg")
    _save_sharp_image(sharp_path)
    _save_blurry_image(blurry_path)

    sharp_score = compute_sharpness(sharp_path)
    blurry_score = compute_sharpness(blurry_path)

    assert sharp_score is not None
    assert blurry_score is not None
    assert sharp_score > blurry_score


def test_compute_sharpness_flat_image(tmp_path):
    """A flat image should have much lower sharpness than a sharp one."""
    from sharpness import compute_sharpness

    flat_path = str(tmp_path / "flat.png")  # PNG to avoid JPEG compression artifacts
    img = Image.new("RGB", (200, 200), color=(100, 100, 100))
    img.save(flat_path)

    sharp_path = str(tmp_path / "sharp.png")
    _save_sharp_image(sharp_path)

    flat_score = compute_sharpness(flat_path)
    sharp_score = compute_sharpness(sharp_path)
    assert flat_score is not None
    assert sharp_score is not None
    assert flat_score < sharp_score * 0.1  # flat should be far lower


def test_compute_sharpness_returns_float(tmp_path):
    """Score is a rounded float."""
    from sharpness import compute_sharpness

    path = str(tmp_path / "test.jpg")
    _save_sharp_image(path)
    score = compute_sharpness(path)
    assert isinstance(score, float)


def test_compute_sharpness_nonexistent_file():
    """Nonexistent file returns None."""
    from sharpness import compute_sharpness

    score = compute_sharpness("/nonexistent/path/image.jpg")
    assert score is None


def test_compute_sharpness_region(tmp_path):
    """Region crop works and returns a score."""
    from sharpness import compute_sharpness

    path = str(tmp_path / "test.jpg")
    _save_sharp_image(path, size=(400, 400))
    score = compute_sharpness(path, region=(50, 50, 100, 100))
    assert score is not None
    assert score > 0


def test_compute_sharpness_region_vs_full(tmp_path):
    """Region score differs from full-image score (different content area)."""
    from sharpness import compute_sharpness

    path = str(tmp_path / "mixed.jpg")
    # Create an image that's sharp in the center, blurry at edges
    img = Image.new("RGB", (400, 400), color=(128, 128, 128))
    draw = ImageDraw.Draw(img)
    # Sharp checkerboard in center
    for x in range(150, 250, 4):
        for y in range(150, 250, 4):
            if (x // 4 + y // 4) % 2 == 0:
                draw.rectangle([x, y, x + 3, y + 3], fill="white")
            else:
                draw.rectangle([x, y, x + 3, y + 3], fill="black")
    img.save(path)

    full_score = compute_sharpness(path)
    center_score = compute_sharpness(path, region=(150, 150, 100, 100))

    assert full_score is not None
    assert center_score is not None
    # Center region has the sharp pattern, so should score higher
    assert center_score > full_score


# ---------------------------------------------------------------------------
# score_burst_group
# ---------------------------------------------------------------------------


def test_score_burst_group_ranking(tmp_path):
    """Photos are ranked by sharpness, best first."""
    from sharpness import score_burst_group

    sharp_path = str(tmp_path / "sharp.jpg")
    blurry_path = str(tmp_path / "blurry.jpg")
    flat_path = str(tmp_path / "flat.jpg")
    _save_sharp_image(sharp_path)
    _save_blurry_image(blurry_path)
    _save_flat_image(flat_path)

    results = score_burst_group([
        (1, sharp_path),
        (2, blurry_path),
        (3, flat_path),
    ])

    assert len(results) == 3
    # Should be sorted descending by sharpness
    assert results[0]["sharpness"] >= results[1]["sharpness"] >= results[2]["sharpness"]
    # Sharp image should be ranked first
    assert results[0]["photo_id"] == 1


def test_score_burst_group_rank_numbers(tmp_path):
    """Rank numbers are 1-indexed and sequential."""
    from sharpness import score_burst_group

    paths = []
    for i in range(3):
        path = str(tmp_path / f"img{i}.jpg")
        _save_sharp_image(path, size=(200 + i * 10, 200))
        paths.append((i + 1, path))

    results = score_burst_group(paths)
    ranks = [r["rank"] for r in results]
    assert ranks == [1, 2, 3]


def test_score_burst_group_best_worst_flags(tmp_path):
    """is_best=True for rank 1, is_worst=True for last rank."""
    from sharpness import score_burst_group

    sharp_path = str(tmp_path / "sharp.jpg")
    blurry_path = str(tmp_path / "blurry.jpg")
    flat_path = str(tmp_path / "flat.jpg")
    _save_sharp_image(sharp_path)
    _save_blurry_image(blurry_path)
    _save_flat_image(flat_path)

    results = score_burst_group([
        (1, sharp_path),
        (2, blurry_path),
        (3, flat_path),
    ])

    assert results[0]["is_best"] is True
    assert results[0]["is_worst"] is False
    assert results[1]["is_best"] is False
    assert results[1]["is_worst"] is False
    assert results[2]["is_best"] is False
    assert results[2]["is_worst"] is True


def test_score_burst_group_single_photo(tmp_path):
    """Single photo: is_best=True, is_worst=False."""
    from sharpness import score_burst_group

    path = str(tmp_path / "only.jpg")
    _save_sharp_image(path)

    results = score_burst_group([(1, path)])
    assert len(results) == 1
    assert results[0]["is_best"] is True
    assert results[0]["is_worst"] is False
    assert results[0]["rank"] == 1


def test_score_burst_group_two_photos(tmp_path):
    """Two photos: one is best, the other is worst."""
    from sharpness import score_burst_group

    sharp_path = str(tmp_path / "sharp.jpg")
    blurry_path = str(tmp_path / "blurry.jpg")
    _save_sharp_image(sharp_path)
    _save_blurry_image(blurry_path)

    results = score_burst_group([
        (1, sharp_path),
        (2, blurry_path),
    ])

    assert results[0]["is_best"] is True
    assert results[0]["is_worst"] is False
    assert results[1]["is_best"] is False
    assert results[1]["is_worst"] is True


def test_score_burst_group_nonexistent_file(tmp_path):
    """Nonexistent files get sharpness=0 and rank last."""
    from sharpness import score_burst_group

    sharp_path = str(tmp_path / "sharp.jpg")
    _save_sharp_image(sharp_path)

    results = score_burst_group([
        (1, sharp_path),
        (2, "/nonexistent/photo.jpg"),
    ])

    assert len(results) == 2
    # Real image should rank first
    assert results[0]["photo_id"] == 1
    assert results[1]["sharpness"] == 0


def test_score_burst_group_output_structure(tmp_path):
    """Each result has the expected keys."""
    from sharpness import score_burst_group

    path = str(tmp_path / "test.jpg")
    _save_sharp_image(path)

    results = score_burst_group([(42, path)])
    r = results[0]
    assert set(r.keys()) == {"photo_id", "path", "sharpness", "rank", "is_best", "is_worst"}
    assert r["photo_id"] == 42
    assert r["path"] == path


# ---------------------------------------------------------------------------
# compute_sharpness_for_photo
# ---------------------------------------------------------------------------


def test_compute_sharpness_for_photo_without_vireo_dir(tmp_path):
    """Without vireo_dir, loads image from folder_id + filename (original path)."""
    from sharpness import compute_sharpness_for_photo

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path)
    _save_sharp_image(os.path.join(folder_path, "sharp.jpg"))

    photo = {"folder_id": 1, "filename": "sharp.jpg", "working_copy_path": None}
    folders = {1: folder_path}

    score = compute_sharpness_for_photo(photo, folders)
    assert score is not None
    assert isinstance(score, float)
    assert score > 0


def test_compute_sharpness_for_photo_with_working_copy(tmp_path):
    """With vireo_dir and a working copy, uses the working copy JPEG."""
    from sharpness import compute_sharpness_for_photo

    vireo_dir = str(tmp_path / "vireo")
    wc_dir = os.path.join(vireo_dir, "working_copies")
    os.makedirs(wc_dir)
    wc_path = os.path.join(wc_dir, "abc123.jpg")
    _save_sharp_image(wc_path)

    photo = {
        "folder_id": 1,
        "filename": "original.nef",
        "working_copy_path": "working_copies/abc123.jpg",
    }
    folders = {1: "/nonexistent/folder"}

    score = compute_sharpness_for_photo(photo, folders, vireo_dir=vireo_dir)
    assert score is not None
    assert isinstance(score, float)
    assert score > 0


def test_compute_sharpness_for_photo_fallback_no_working_copy(tmp_path):
    """With vireo_dir but no working_copy_path, falls back to original."""
    from sharpness import compute_sharpness_for_photo

    vireo_dir = str(tmp_path / "vireo")
    os.makedirs(vireo_dir)

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path)
    _save_sharp_image(os.path.join(folder_path, "test.jpg"))

    photo = {"folder_id": 1, "filename": "test.jpg", "working_copy_path": None}
    folders = {1: folder_path}

    score = compute_sharpness_for_photo(photo, folders, vireo_dir=vireo_dir)
    assert score is not None
    assert score > 0


def test_compute_sharpness_for_photo_missing_file(tmp_path):
    """Returns None when file doesn't exist."""
    from sharpness import compute_sharpness_for_photo

    photo = {"folder_id": 1, "filename": "missing.jpg", "working_copy_path": None}
    folders = {1: "/nonexistent/path"}

    score = compute_sharpness_for_photo(photo, folders)
    assert score is None


def test_compute_sharpness_for_photo_with_region(tmp_path):
    """Region parameter works with compute_sharpness_for_photo."""
    from sharpness import compute_sharpness_for_photo

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path)
    _save_sharp_image(os.path.join(folder_path, "test.jpg"), size=(400, 400))

    photo = {"folder_id": 1, "filename": "test.jpg", "working_copy_path": None}
    folders = {1: folder_path}

    score = compute_sharpness_for_photo(photo, folders, region=(50, 50, 100, 100))
    assert score is not None
    assert score > 0


def test_compute_sharpness_for_photo_working_copy_preferred(tmp_path):
    """Working copy is used in preference to original when both exist."""
    from sharpness import compute_sharpness_for_photo

    vireo_dir = str(tmp_path / "vireo")
    wc_dir = os.path.join(vireo_dir, "working_copies")
    os.makedirs(wc_dir)

    # Working copy: sharp image
    wc_path = os.path.join(wc_dir, "wc.jpg")
    _save_sharp_image(wc_path)

    # Original: blurry image
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path)
    _save_blurry_image(os.path.join(folder_path, "original.jpg"))

    photo = {
        "folder_id": 1,
        "filename": "original.jpg",
        "working_copy_path": "working_copies/wc.jpg",
    }
    folders = {1: folder_path}

    # Score with working copy (sharp)
    score_wc = compute_sharpness_for_photo(photo, folders, vireo_dir=vireo_dir)
    # Score without vireo_dir — uses original (blurry)
    score_orig = compute_sharpness_for_photo(photo, folders)

    assert score_wc is not None
    assert score_orig is not None
    # Working copy (sharp) should score higher than original (blurry)
    assert score_wc > score_orig
