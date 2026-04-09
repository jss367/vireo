"""Tests for photo export operations."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from db import Database
from export import export_photos, resolve_template, sanitize_filename
from PIL import Image


def test_resolve_template_original():
    photo = {"filename": "DSC_4521.jpg", "timestamp": "2024-06-15T14:30:22",
             "rating": 3, "folder_name": "June_Trip"}
    result = resolve_template("{original}", photo, species="Red-tailed Hawk", seq=1)
    assert result == "DSC_4521"


def test_resolve_template_species_date_seq():
    photo = {"filename": "DSC_4521.jpg", "timestamp": "2024-06-15T14:30:22",
             "rating": 3, "folder_name": "June_Trip"}
    result = resolve_template("{species}/{date}_{seq}", photo,
                              species="Red-tailed Hawk", seq=5)
    assert result == "Red-tailed Hawk/2024-06-15_0005"


def test_resolve_template_datetime():
    photo = {"filename": "DSC_4521.jpg", "timestamp": "2024-06-15T14:30:22",
             "rating": 3, "folder_name": "June_Trip"}
    result = resolve_template("{datetime}_{original}", photo,
                              species="unknown", seq=1)
    assert result == "2024-06-15_143022_DSC_4521"


def test_resolve_template_all_variables():
    photo = {"filename": "bird.jpg", "timestamp": "2024-01-10T08:00:00",
             "rating": 5, "folder_name": "Safari"}
    result = resolve_template("{folder}/{species}/{rating}_{seq}", photo,
                              species="Elephant", seq=12)
    assert result == "Safari/Elephant/5_0012"


def test_resolve_template_missing_timestamp():
    photo = {"filename": "bird.jpg", "timestamp": None,
             "rating": 0, "folder_name": "Photos"}
    result = resolve_template("{date}_{original}", photo, species="unknown", seq=1)
    assert result == "unknown-date_bird"


def test_resolve_template_no_species():
    photo = {"filename": "bird.jpg", "timestamp": "2024-01-10T08:00:00",
             "rating": 0, "folder_name": "Photos"}
    result = resolve_template("{species}_{seq}", photo, species=None, seq=1)
    assert result == "unknown_0001"


def test_sanitize_filename_slashes():
    assert sanitize_filename("Black/White Warbler") == "Black_White Warbler"


def test_sanitize_filename_special_chars():
    assert sanitize_filename('bird: "best"') == "bird_ _best_"


@pytest.fixture
def export_env(tmp_path):
    """Set up a DB with photos and real image files for export testing."""
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src = tmp_path / "src"
    src.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    dest = tmp_path / "export_out"

    fid = db.add_folder(str(src), name="Safari")

    # Create real JPEG files
    img1 = Image.new("RGB", (800, 600), color="red")
    img1.save(str(src / "bird1.jpg"), "JPEG", quality=95)
    img2 = Image.new("RGB", (1200, 900), color="blue")
    img2.save(str(src / "bird2.jpg"), "JPEG", quality=95)

    p1 = db.add_photo(folder_id=fid, filename="bird1.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0,
                       timestamp="2024-06-15T14:30:22")
    p2 = db.add_photo(folder_id=fid, filename="bird2.jpg", extension=".jpg",
                       file_size=2000, file_mtime=2.0,
                       timestamp="2024-06-16T09:00:00")

    db.update_photo_rating(p1, 5)

    # Add a species keyword to p1
    k = db.add_keyword("Red-tailed Hawk", is_species=True)
    db.tag_photo(p1, k)

    return {
        "db": db, "tmp_path": tmp_path, "vireo_dir": str(vireo_dir),
        "src": src, "dest": str(dest),
        "fid": fid, "p1": p1, "p2": p2,
    }


def test_export_photos_basic(export_env):
    """export_photos copies photos to destination with original names."""
    env = export_env
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"], env["p2"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )
    assert result["exported"] == 2
    assert result["errors"] == []
    assert os.path.isfile(os.path.join(env["dest"], "bird1.jpg"))
    assert os.path.isfile(os.path.join(env["dest"], "bird2.jpg"))


def test_export_photos_resize(export_env):
    """export_photos resizes photos to max_size."""
    env = export_env
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p2"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "max_size": 400},
    )
    assert result["exported"] == 1
    out_path = os.path.join(env["dest"], "bird2.jpg")
    with Image.open(out_path) as img:
        assert max(img.size) <= 400


def test_export_photos_subdirectories(export_env):
    """export_photos creates subdirectories from template."""
    env = export_env
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"], env["p2"]],
        destination=env["dest"],
        options={"naming_template": "{species}/{date}_{seq}"},
    )
    assert result["exported"] == 2
    # p1 has species keyword
    assert os.path.isfile(os.path.join(env["dest"], "Red-tailed Hawk", "2024-06-15_0001.jpg"))
    # p2 has no species
    assert os.path.isfile(os.path.join(env["dest"], "unknown", "2024-06-16_0001.jpg"))


def test_export_photos_collision_renames(export_env):
    """export_photos appends _2, _3 on filename collisions."""
    env = export_env
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"], env["p2"]],
        destination=env["dest"],
        options={"naming_template": "photo"},
    )
    assert result["exported"] == 2
    assert os.path.isfile(os.path.join(env["dest"], "photo.jpg"))
    assert os.path.isfile(os.path.join(env["dest"], "photo_2.jpg"))


def test_export_photos_quality(export_env):
    """export_photos respects quality setting."""
    env = export_env
    # Export at low quality
    export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "quality": 20},
    )
    low_q = os.path.getsize(os.path.join(env["dest"], "bird1.jpg"))

    # Export at high quality to a different dest
    dest2 = env["dest"] + "_hq"
    export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=dest2,
        options={"naming_template": "{original}", "quality": 95},
    )
    high_q = os.path.getsize(os.path.join(dest2, "bird1.jpg"))
    assert high_q > low_q


def test_export_photos_progress_callback(export_env):
    """export_photos calls progress callback with current, total, filename."""
    env = export_env
    calls = []
    export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"], env["p2"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
        progress_cb=lambda c, t, f: calls.append((c, t, f)),
    )
    assert len(calls) == 2
    assert calls[0] == (1, 2, "bird1.jpg")
    assert calls[1] == (2, 2, "bird2.jpg")


def test_export_photos_missing_source(export_env):
    """export_photos reports error for missing source files."""
    env = export_env
    os.remove(str(env["src"] / "bird1.jpg"))
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )
    assert result["exported"] == 0
    assert len(result["errors"]) == 1


def test_export_traversal_template_is_sanitized(export_env, tmp_path):
    """Templates with '..' segments are sanitized so output stays inside destination."""
    env = export_env
    # A template like "../escaped/{original}" must not write outside destination.
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "../escaped/{original}"},
    )
    assert result["exported"] == 1
    # The file must exist inside the destination, not outside it.
    for root, _dirs, files in os.walk(env["dest"]):
        for f in files:
            full = os.path.join(root, f)
            assert os.path.realpath(full).startswith(os.path.realpath(env["dest"]))
    # Confirm the sibling directory was NOT created
    sibling = os.path.join(os.path.dirname(env["dest"]), "escaped")
    assert not os.path.exists(sibling), "path traversal escaped the destination directory"


def test_export_no_resize_uses_original_not_working_copy(export_env, tmp_path):
    """When no resize is requested, export uses the original file, not the working copy."""
    env = export_env
    # Create a fake (smaller) working copy
    wc_dir = os.path.join(env["vireo_dir"], "thumbnails")
    os.makedirs(wc_dir, exist_ok=True)
    small_wc = os.path.join(wc_dir, "bird1_wc.jpg")
    small_img = Image.new("RGB", (100, 75), color="green")
    small_img.save(small_wc, "JPEG")

    # Patch the DB to return a photo row that has working_copy_path set
    # We update the photo record directly via the DB connection
    env["db"].conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?",
        ("thumbnails/bird1_wc.jpg", env["p1"]),
    )
    env["db"].conn.commit()

    # Export without resize — must use original (800x600), not working copy (100x75)
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "max_size": None},
    )
    assert result["exported"] == 1
    out_path = os.path.join(env["dest"], "bird1.jpg")
    with Image.open(out_path) as img:
        assert img.size == (800, 600)  # original dimensions, not the 100x75 working copy


def test_export_with_resize_may_use_working_copy(export_env):
    """When resize is requested and a working copy exists, it is used as the source."""
    env = export_env
    # Create a fake working copy larger than the requested max_size
    wc_dir = os.path.join(env["vireo_dir"], "thumbnails")
    os.makedirs(wc_dir, exist_ok=True)
    small_wc = os.path.join(wc_dir, "bird1_wc.jpg")
    wc_img = Image.new("RGB", (600, 400), color="blue")
    wc_img.save(small_wc, "JPEG")

    env["db"].conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?",
        ("thumbnails/bird1_wc.jpg", env["p1"]),
    )
    env["db"].conn.commit()

    # Export with resize — working copy should be preferred
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "max_size": 300},
    )
    assert result["exported"] == 1
    out_path = os.path.join(env["dest"], "bird1.jpg")
    with Image.open(out_path) as img:
        assert max(img.size) <= 300


def test_export_large_max_size_uses_original(export_env):
    """When max_size exceeds working copy cap, export uses the original file."""
    env = export_env
    wc_dir = os.path.join(env["vireo_dir"], "thumbnails")
    os.makedirs(wc_dir, exist_ok=True)
    small_wc = os.path.join(wc_dir, "bird1_wc.jpg")
    # Simulate a capped working copy (100x75)
    Image.new("RGB", (100, 75), color="green").save(small_wc, "JPEG")

    env["db"].conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?",
        ("thumbnails/bird1_wc.jpg", env["p1"]),
    )
    env["db"].conn.commit()

    # Request max_size=8000, well above the default working_copy_max_size (4096)
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "max_size": 8000},
    )
    assert result["exported"] == 1
    out_path = os.path.join(env["dest"], "bird1.jpg")
    with Image.open(out_path) as img:
        # Should use original (800x600), not working copy (100x75)
        assert img.size == (800, 600)
