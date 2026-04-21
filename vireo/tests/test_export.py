"""Tests for photo export operations."""

import contextlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from db import Database
from export import (
    developed_folder_key,
    export_photos,
    relocate_developed_dir,
    resolve_template,
    sanitize_filename,
)
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


def _avg_rgb(path):
    """Return the average RGB tuple of an image across a small sample grid."""
    with Image.open(path) as img:
        rgb = img.convert("RGB")
        w, h = rgb.size
        xs = (w // 10, w // 4, w // 2, 3 * w // 4, 9 * w // 10)
        ys = (h // 10, h // 4, h // 2, 3 * h // 4, 9 * h // 10)
        rs, gs, bs, n = 0, 0, 0, 0
        for x in xs:
            for y in ys:
                r, g, b = rgb.getpixel((x, y))
                rs += r
                gs += g
                bs += b
                n += 1
        return (rs // n, gs // n, bs // n)


def test_export_prefers_developed_jpg_when_present(export_env):
    """When <folder>/developed/<stem>.jpg exists, export uses it, not the original.

    Motivation: darktable produces the user's intended rendering; export has
    historically re-decoded the RAW via libraw, discarding that rendering.
    """
    env = export_env
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    # Visually distinct from the red original.
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed_dir / "bird1.jpg"), "JPEG", quality=95,
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )

    assert result["exported"] == 1
    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.jpg"))
    assert g > r and g > b, f"expected green-dominant from developed JPG, got rgb=({r},{g},{b})"


def test_export_prefers_developed_tiff_when_jpg_absent(export_env):
    """Developed output may be .tiff — export prefers it over the RAW."""
    env = export_env
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    Image.new("RGB", (800, 600), color=(20, 30, 220)).save(
        str(developed_dir / "bird1.tiff"), "TIFF",
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )

    assert result["exported"] == 1
    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.jpg"))
    assert b > r and b > g, f"expected blue-dominant from developed TIFF, got rgb=({r},{g},{b})"


def test_export_falls_back_to_original_when_no_developed(export_env):
    """No developed output → export uses the original file (existing behavior)."""
    env = export_env
    export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )

    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.jpg"))
    assert r > g and r > b, f"expected red-dominant from original, got rgb=({r},{g},{b})"


def test_export_honors_configured_developed_dir(export_env):
    """When developed_dir option is passed, export looks there, not in <folder>/developed/.

    Mirrors the darktable_output_dir config: users who configure a custom
    output location expect export to find the developed outputs at that
    location. Files are looked up under a per-folder subdir keyed by a
    hash of the source folder's path (see `developed_folder_key`); that
    matches the develop job's write convention and keeps lookups
    one-to-one when two source folders share a basename.
    """
    env = export_env
    # Decoy in the default location — export must NOT pick this.
    decoy_dir = env["src"] / "developed"
    decoy_dir.mkdir()
    Image.new("RGB", (800, 600), color=(200, 200, 0)).save(
        str(decoy_dir / "bird1.jpg"), "JPEG",
    )
    # Real output in the configured dir, under the per-folder subdir.
    configured = env["tmp_path"] / "darktable_out"
    configured.mkdir()
    folder_subdir = configured / developed_folder_key(str(env["src"]))
    folder_subdir.mkdir()
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(folder_subdir / "bird1.jpg"), "JPEG",
    )

    export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "developed_dir": str(configured),
        },
    )

    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.jpg"))
    assert g > r and g > b, f"expected green-dominant from configured dir, got rgb=({r},{g},{b})"


def test_export_developed_matches_uppercase_extension(export_env):
    """Developed files with uppercase extensions are still matched.

    Regression: `_find_developed_output` previously probed only lowercase
    extensions, so on case-sensitive filesystems a developed file written
    as IMG_0001.JPG (or .TIFF) silently fell through to the RAW fallback.
    That can happen when darktable_output_format is configured uppercase
    or for files placed manually.
    """
    env = export_env
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed_dir / "bird1.JPG"), "JPEG", quality=95,
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )

    assert result["exported"] == 1
    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.jpg"))
    assert g > r and g > b, (
        f"expected green-dominant from uppercase-ext developed JPG, got rgb=({r},{g},{b})"
    )


def test_export_developed_matches_uppercase_tiff(export_env):
    """Uppercase TIFF extensions also match when no JPG is present."""
    env = export_env
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    Image.new("RGB", (800, 600), color=(20, 30, 220)).save(
        str(developed_dir / "bird1.TIFF"), "TIFF",
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )

    assert result["exported"] == 1
    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.jpg"))
    assert b > r and b > g, (
        f"expected blue-dominant from uppercase-ext developed TIFF, got rgb=({r},{g},{b})"
    )


def test_export_configured_developed_dir_disambiguates_same_basename(tmp_path):
    """Two folders with the same basename resolve to distinct developed outputs.

    Regression: previously the configured developed_dir lookup used only the
    filename stem, so two photos named IMG_0001.CR3 in different source
    folders both resolved to <developed_dir>/IMG_0001.jpg — silently mixing
    developed outputs. Each photo's developed file now lives under a
    per-source-folder subdir keyed by a hash of the folder's path
    (see `developed_folder_key`), so matches are one-to-one.
    """
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    # Two source folders, each with a photo that shares the same basename.
    src_a = tmp_path / "folderA"
    src_a.mkdir()
    src_b = tmp_path / "folderB"
    src_b.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    dest = tmp_path / "export_out"

    # Originals are visually distinct so we can tell them apart in the output.
    Image.new("RGB", (800, 600), color=(200, 0, 0)).save(
        str(src_a / "IMG_0001.jpg"), "JPEG", quality=95,
    )
    Image.new("RGB", (800, 600), color=(0, 0, 200)).save(
        str(src_b / "IMG_0001.jpg"), "JPEG", quality=95,
    )

    fid_a = db.add_folder(str(src_a), name="A")
    fid_b = db.add_folder(str(src_b), name="B")
    pid_a = db.add_photo(folder_id=fid_a, filename="IMG_0001.jpg", extension=".jpg",
                         file_size=1, file_mtime=1.0)
    pid_b = db.add_photo(folder_id=fid_b, filename="IMG_0001.jpg", extension=".jpg",
                         file_size=1, file_mtime=1.0)

    # Developed outputs under <developed_dir>/<path_key>/<stem>.jpg — two
    # distinct files, each a different solid color. Keys derive from each
    # folder's path, so the two folders get distinct subdirs.
    developed = tmp_path / "darktable_out"
    developed.mkdir()
    key_a = developed_folder_key(str(src_a))
    key_b = developed_folder_key(str(src_b))
    assert key_a != key_b
    (developed / key_a).mkdir()
    (developed / key_b).mkdir()
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed / key_a / "IMG_0001.jpg"), "JPEG", quality=95,
    )
    Image.new("RGB", (800, 600), color=(200, 200, 10)).save(
        str(developed / key_b / "IMG_0001.jpg"), "JPEG", quality=95,
    )

    export_photos(
        db=db,
        vireo_dir=str(vireo_dir),
        photo_ids=[pid_a, pid_b],
        destination=str(dest),
        options={
            "naming_template": "{folder}/{original}",
            "developed_dir": str(developed),
        },
    )

    # {folder} in the naming template resolves to the source folder's
    # basename (folderA / folderB), so outputs land under those dirs.
    r_a, g_a, b_a = _avg_rgb(os.path.join(str(dest), "folderA", "IMG_0001.jpg"))
    r_b, g_b, b_b = _avg_rgb(os.path.join(str(dest), "folderB", "IMG_0001.jpg"))
    # Folder A got the green-dominant developed output.
    assert g_a > r_a and g_a > b_a, f"A: expected green-dominant, got rgb=({r_a},{g_a},{b_a})"
    # Folder B got the yellow-dominant (red+green) developed output.
    assert r_b > b_b and g_b > b_b, f"B: expected yellow-dominant, got rgb=({r_b},{g_b},{b_b})"


def _fs_is_case_sensitive(path):
    """Return True if creating 'x' and 'X' in path yields two distinct files."""
    probe_lo = os.path.join(str(path), "_case_probe_a")
    probe_up = os.path.join(str(path), "_CASE_PROBE_A")
    try:
        open(probe_lo, "w").close()
        result = not os.path.exists(probe_up)
        return result
    finally:
        with contextlib.suppress(OSError):
            os.remove(probe_lo)


def test_export_developed_stem_match_is_case_sensitive(tmp_path):
    """Two photos whose filenames differ only by case resolve to distinct developed files.

    Regression: `_find_developed_output` used to lowercase the search stem
    as well as the directory entries, so on case-sensitive filesystems
    Bird1.CR3 and bird1.CR3 in the same source folder silently collided
    on the same developed file. Stems must match case-sensitively.
    """
    if not _fs_is_case_sensitive(tmp_path):
        pytest.skip("filesystem is case-insensitive; scenario cannot occur here")

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src = tmp_path / "src"
    src.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    dest = tmp_path / "export_out"

    # Two originals in the same folder with stems that differ only by case.
    Image.new("RGB", (800, 600), color=(200, 0, 0)).save(
        str(src / "Bird1.jpg"), "JPEG", quality=95,
    )
    Image.new("RGB", (800, 600), color=(0, 0, 200)).save(
        str(src / "bird1.jpg"), "JPEG", quality=95,
    )

    fid = db.add_folder(str(src), name="Mixed")
    pid_upper = db.add_photo(folder_id=fid, filename="Bird1.jpg", extension=".jpg",
                             file_size=1, file_mtime=1.0)
    pid_lower = db.add_photo(folder_id=fid, filename="bird1.jpg", extension=".jpg",
                             file_size=1, file_mtime=2.0)

    # Developed outputs with matching cases. If the lookup lowercases the
    # stem, both photos resolve to whichever entry os.listdir happens to
    # return first — a silent data-correctness bug.
    developed = src / "developed"
    developed.mkdir()
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed / "Bird1.jpg"), "JPEG", quality=95,
    )
    Image.new("RGB", (800, 600), color=(200, 200, 10)).save(
        str(developed / "bird1.jpg"), "JPEG", quality=95,
    )

    # Export one at a time so the two output filenames don't collide on
    # a case-insensitive destination view; they land with their own cases.
    export_photos(
        db=db,
        vireo_dir=str(vireo_dir),
        photo_ids=[pid_upper],
        destination=str(dest),
        options={"naming_template": "{original}"},
    )
    export_photos(
        db=db,
        vireo_dir=str(vireo_dir),
        photo_ids=[pid_lower],
        destination=str(dest),
        options={"naming_template": "{original}"},
    )

    r_u, g_u, b_u = _avg_rgb(os.path.join(str(dest), "Bird1.jpg"))
    r_l, g_l, b_l = _avg_rgb(os.path.join(str(dest), "bird1.jpg"))
    # Bird1.jpg (upper) → green-dominant developed output.
    assert g_u > r_u and g_u > b_u, (
        f"Bird1: expected green-dominant developed, got rgb=({r_u},{g_u},{b_u})"
    )
    # bird1.jpg (lower) → yellow-dominant developed output.
    assert r_l > b_l and g_l > b_l, (
        f"bird1: expected yellow-dominant developed, got rgb=({r_l},{g_l},{b_l})"
    )


def test_export_legacy_flat_developed_dir_fallback(export_env):
    """Developed outputs written flat to <developed_dir>/<stem>.<ext> still light up.

    Before the per-folder nesting convention existed, the develop job wrote
    directly into `darktable_output_dir` with no per-folder subdir. Users
    who upgraded would otherwise silently regress to RAW export; the flat
    path is still probed as a last-resort fallback.
    """
    env = export_env
    configured = env["tmp_path"] / "darktable_out"
    configured.mkdir()
    # Legacy flat layout — no <folder_id>/ subdir.
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(configured / "bird1.jpg"), "JPEG", quality=95,
    )

    export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "developed_dir": str(configured),
        },
    )

    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.jpg"))
    assert g > r and g > b, (
        f"expected green-dominant from legacy flat developed dir, got rgb=({r},{g},{b})"
    )


def test_export_folder_scoped_developed_wins_over_legacy_flat(export_env):
    """When both folder-scoped and legacy flat outputs exist, folder-scoped wins.

    The legacy flat fallback exists only to avoid regressing libraries that
    predate the per-folder nesting. Any newly-developed file (written under
    <path_key>/) must still take precedence so same-basename collisions
    across folders stay resolved one-to-one.
    """
    env = export_env
    configured = env["tmp_path"] / "darktable_out"
    configured.mkdir()
    # Legacy flat (yellow) — should NOT be picked when a folder-scoped file exists.
    Image.new("RGB", (800, 600), color=(200, 200, 10)).save(
        str(configured / "bird1.jpg"), "JPEG", quality=95,
    )
    # Folder-scoped (green) — wins.
    folder_subdir = configured / developed_folder_key(str(env["src"]))
    folder_subdir.mkdir()
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(folder_subdir / "bird1.jpg"), "JPEG", quality=95,
    )

    export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "developed_dir": str(configured),
        },
    )

    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.jpg"))
    assert g > r and g > b, (
        f"expected green-dominant folder-scoped output to win, got rgb=({r},{g},{b})"
    )


def test_export_caches_developed_dir_scans(export_env, monkeypatch):
    """os.listdir is called at most once per developed directory across the export.

    Regression guard: without caching, `_find_developed_output` rescans the
    same folder for every photo, turning a single-directory export of N
    photos into N×cost(listdir). The per-export index collapses that back
    to one listdir per distinct base directory.
    """
    env = export_env
    # Develop both photos into a shared directory so every photo probes
    # the same developed folder(s).
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed_dir / "bird1.jpg"), "JPEG", quality=95,
    )
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed_dir / "bird2.jpg"), "JPEG", quality=95,
    )

    import export as export_module

    counts = {}
    real_listdir = os.listdir

    def counting_listdir(path):
        counts[path] = counts.get(path, 0) + 1
        return real_listdir(path)

    monkeypatch.setattr(export_module.os, "listdir", counting_listdir)

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"], env["p2"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )

    assert result["exported"] == 2
    assert counts.get(str(developed_dir), 0) == 1, (
        f"expected 1 listdir of {developed_dir}, got {counts.get(str(developed_dir), 0)}"
    )
    # Sanity: any directory we scanned, we scanned at most once.
    for path, n in counts.items():
        assert n <= 1, f"{path} was scanned {n} times; expected ≤1 after caching"


def test_export_reused_folder_id_does_not_inherit_stale_developed(tmp_path):
    """A freshly-added folder that happens to reuse a deleted folder's row id
    must not inherit the deleted folder's developed files.

    Regression: when the configured developed_dir was nested by folder_id,
    a SQLite row-id reuse (folders.id is INTEGER PRIMARY KEY, not
    AUTOINCREMENT) silently cross-wired the new folder's export onto
    stale pixels left on disk by the deleted folder. Nesting by a hash
    of the folder path instead makes distinct paths always resolve to
    distinct on-disk keys, so the stale files never match.
    """
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    dest = tmp_path / "export_out"
    developed = tmp_path / "darktable_out"
    developed.mkdir()

    # Step 1: add a folder, add a photo, lay down a developed file for it,
    # then delete the folder. The external developed file is intentionally
    # left on disk (delete_folder does not reach into darktable_output_dir),
    # mirroring the real-world scenario.
    src_old = tmp_path / "old_folder"
    src_old.mkdir()
    Image.new("RGB", (800, 600), color=(200, 0, 0)).save(
        str(src_old / "IMG_0001.jpg"), "JPEG", quality=95,
    )
    fid_old = db.add_folder(str(src_old), name="Old")
    pid_old = db.add_photo(folder_id=fid_old, filename="IMG_0001.jpg", extension=".jpg",
                           file_size=1, file_mtime=1.0)
    # Yellow stale file under the OLD folder's path key — must not bleed
    # into the new folder's export.
    stale_key = developed_folder_key(str(src_old))
    (developed / stale_key).mkdir()
    Image.new("RGB", (800, 600), color=(200, 200, 10)).save(
        str(developed / stale_key / "IMG_0001.jpg"), "JPEG", quality=95,
    )
    db.delete_folder(fid_old)
    _ = pid_old  # photo row is cascaded away with its folder

    # Step 2: add a NEW folder at a different path, with a photo that has
    # the same basename as the deleted one. The new folder should receive
    # a distinct on-disk key and never see the stale yellow file.
    src_new = tmp_path / "new_folder"
    src_new.mkdir()
    # Blue original so we can tell in the output whether we got the stale
    # yellow developed file (regression) or fell back to the fresh blue
    # original (correct behavior when no developed file exists yet).
    Image.new("RGB", (800, 600), color=(0, 0, 200)).save(
        str(src_new / "IMG_0001.jpg"), "JPEG", quality=95,
    )
    fid_new = db.add_folder(str(src_new), name="New")
    pid_new = db.add_photo(folder_id=fid_new, filename="IMG_0001.jpg", extension=".jpg",
                           file_size=1, file_mtime=2.0)

    # The path-based key for the new folder must differ from the stale key.
    new_key = developed_folder_key(str(src_new))
    assert new_key != stale_key

    export_photos(
        db=db,
        vireo_dir=str(vireo_dir),
        photo_ids=[pid_new],
        destination=str(dest),
        options={
            "naming_template": "{original}",
            "developed_dir": str(developed),
        },
    )

    r, g, b = _avg_rgb(os.path.join(str(dest), "IMG_0001.jpg"))
    # Blue-dominant = fell back to the fresh original, not the stale
    # yellow file left behind by the deleted folder.
    assert b > r and b > g, (
        f"expected blue-dominant from fresh original (not stale yellow "
        f"developed file), got rgb=({r},{g},{b})"
    )


def test_export_case_variant_developed_prefers_lowercase_extension(tmp_path):
    """When both bird1.jpg and bird1.JPG exist, export picks the lowercase one deterministically.

    Regression: the developed directory index previously used
    `setdefault` keyed on the lowercased extension, so whichever entry
    `os.listdir` returned first won. That made the exported pixels
    nondeterministic across filesystems and between runs. The index now
    breaks ties in favour of the canonical lowercase extension — which
    is what `darktable-cli` writes when `darktable_output_format` is
    left at its default — so the winner is stable.
    """
    if not _fs_is_case_sensitive(tmp_path):
        pytest.skip("filesystem is case-insensitive; scenario cannot occur here")

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src = tmp_path / "src"
    src.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    dest = tmp_path / "export_out"

    Image.new("RGB", (800, 600), color=(200, 0, 0)).save(
        str(src / "bird1.jpg"), "JPEG", quality=95,
    )
    fid = db.add_folder(str(src), name="Src")
    pid = db.add_photo(folder_id=fid, filename="bird1.jpg", extension=".jpg",
                        file_size=1, file_mtime=1.0)

    developed = src / "developed"
    developed.mkdir()
    # Two developed files for the same stem, differing only by extension
    # case. The lowercase .jpg file encodes green; the uppercase .JPG
    # file encodes yellow. A deterministic winner is the lowercase one.
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed / "bird1.jpg"), "JPEG", quality=95,
    )
    Image.new("RGB", (800, 600), color=(200, 200, 10)).save(
        str(developed / "bird1.JPG"), "JPEG", quality=95,
    )

    export_photos(
        db=db,
        vireo_dir=str(vireo_dir),
        photo_ids=[pid],
        destination=str(dest),
        options={"naming_template": "{original}"},
    )

    r, g, b = _avg_rgb(os.path.join(str(dest), "bird1.jpg"))
    assert g > r and g > b, (
        f"expected green-dominant (lowercase .jpg) developed output to win "
        f"the tie over yellow (.JPG), got rgb=({r},{g},{b})"
    )


def test_relocate_developed_dir_renames_subdir_on_path_change(tmp_path):
    """After a folder move, its developed subdir is rebased to the new path key."""
    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_path = "/srv/photos/birds"
    new_path = "/srv/photos/archive/birds"

    old_key = developed_folder_key(old_path)
    new_key = developed_folder_key(new_path)
    assert old_key != new_key

    old_subdir = developed / old_key
    old_subdir.mkdir()
    (old_subdir / "IMG_0001.jpg").write_bytes(b"developed-bytes")

    assert relocate_developed_dir(str(developed), old_path, new_path) is True

    assert not old_subdir.exists()
    new_subdir = developed / new_key
    assert new_subdir.is_dir()
    assert (new_subdir / "IMG_0001.jpg").read_bytes() == b"developed-bytes"


def test_relocate_developed_dir_noop_when_nothing_to_move(tmp_path):
    """No-ops when developed_dir is unset, old subdir is absent, or paths match."""
    developed = tmp_path / "darktable_out"
    developed.mkdir()

    # No developed_dir configured.
    assert relocate_developed_dir("", "/a", "/b") is False
    # Old subdir missing.
    assert relocate_developed_dir(str(developed), "/missing", "/other") is False
    # Same path on both sides.
    assert relocate_developed_dir(str(developed), "/same", "/same") is False


def test_relocate_developed_dir_skips_when_target_exists(tmp_path):
    """If the target key already exists, leave the source in place and log."""
    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_path = "/srv/photos/a"
    new_path = "/srv/photos/b"

    (developed / developed_folder_key(old_path)).mkdir()
    (developed / developed_folder_key(new_path)).mkdir()

    assert relocate_developed_dir(str(developed), old_path, new_path) is False
    # Both still present — nothing was clobbered.
    assert (developed / developed_folder_key(old_path)).is_dir()
    assert (developed / developed_folder_key(new_path)).is_dir()


def test_move_folder_rebases_configured_developed_dir(tmp_path):
    """End-to-end: `move_folder` relocates the darktable output subdir to match the new path.

    Before this fix, a folder move updated `folders.path` in the DB
    without touching the external `darktable_output_dir` layout, so
    every previously-developed photo in the moved folder would silently
    fall back to RAW on export until re-developed.
    """
    from move import move_folder

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src_parent = tmp_path / "old_parent"
    src_parent.mkdir()
    src = src_parent / "birds"
    src.mkdir()
    # One photo in the source folder (contents don't matter for this test).
    (src / "IMG_0001.jpg").write_bytes(b"raw-bytes")

    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_key = developed_folder_key(str(src))
    (developed / old_key).mkdir()
    (developed / old_key / "IMG_0001.jpg").write_bytes(b"developed-bytes")

    fid = db.add_folder(str(src), name="birds")

    dest_parent = tmp_path / "new_parent"
    dest_parent.mkdir()

    result = move_folder(
        db=db,
        folder_id=fid,
        destination=str(dest_parent),
        developed_dir=str(developed),
    )

    assert not result["errors"], result["errors"]
    new_src = dest_parent / "birds"
    assert new_src.is_dir()

    new_key = developed_folder_key(str(new_src))
    assert old_key != new_key
    # Old key subdir gone; new key subdir carries the developed file.
    assert not (developed / old_key).exists()
    assert (developed / new_key / "IMG_0001.jpg").read_bytes() == b"developed-bytes"
