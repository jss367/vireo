"""Tests for photo export operations."""

import contextlib
import errno
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from db import Database
from export import (
    developed_folder_key,
    export_photos,
    relocate_developed_dir,
    relocate_developed_file,
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


def test_export_photos_applies_edit_recipe(export_env):
    """export_photos applies the stored non-destructive edit recipe."""
    env = export_env
    env["db"].set_photo_edit_recipe(
        env["p1"],
        {
            "rotation": 90,
            "crop": {"x": 0, "y": 0, "w": 0.5, "h": 1},
        },
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    with Image.open(os.path.join(env["dest"], "bird1.jpg")) as img:
        assert img.size == (300, 800)


def test_export_photos_applies_adjustment_recipe(export_env):
    """export_photos applies stored adjustment recipes."""
    env = export_env
    Image.new("RGB", (80, 60), color=(100, 100, 100)).save(
        str(env["src"] / "bird1.jpg"), "JPEG", quality=95,
    )
    env["db"].set_photo_edit_recipe(
        env["p1"],
        {
            "adjustments": {
                "exposure": 0.5,
                "contrast": 15,
                "white_balance": {"temperature": 80, "tint": -20},
                "saturation": 10,
            },
        },
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    with Image.open(os.path.join(env["dest"], "bird1.jpg")) as img:
        r, g, b = img.getpixel((0, 0))
    assert r > b
    assert max(r, g, b) > 100


def test_export_non_crop_recipe_loads_with_requested_size(export_env, monkeypatch):
    import export as export_module

    env = export_env
    env["db"].set_photo_edit_recipe(env["p2"], {"rotation": 90})
    original_load_image = export_module.load_image
    seen_max_sizes = []

    def tracking_load_image(file_path, max_size=1024):
        seen_max_sizes.append(max_size)
        return original_load_image(file_path, max_size=max_size)

    monkeypatch.setattr(export_module, "load_image", tracking_load_image)

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p2"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "max_size": 400},
    )

    assert result["exported"] == 1
    assert seen_max_sizes == [400]


def test_export_cropped_recipe_avoids_undersized_working_copy(export_env):
    """Cropped resized exports use the original when a capped WC is too small."""
    env = export_env
    db = env["db"]
    working_dir = os.path.join(env["vireo_dir"], "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{env['p1']}.jpg")
    Image.new("RGB", (400, 300), color="red").save(wc_path, "JPEG", quality=95)
    db.conn.execute(
        "UPDATE photos SET width=800, height=600, working_copy_path=? WHERE id=?",
        (f"working/{env['p1']}.jpg", env["p1"]),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        env["p1"],
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 300,
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    with Image.open(os.path.join(env["dest"], "bird1.jpg")) as img:
        assert img.size == (300, 225)


def test_export_edited_raw_skips_companion_jpeg_substitution(
    export_env, monkeypatch,
):
    """Edited RAW+JPEG exports must decode the RAW, not the clipped companion JPEG.

    Companion JPEGs are camera-baked: their highlights are already clipped.
    Substituting the companion would silently bypass the RAW_DECODE_PRESERVE_HIGHLIGHTS
    decode mode and apply edits to clipped data.
    """
    import export as export_module
    from image_loader import RAW_DECODE_PRESERVE_HIGHLIGHTS

    env = export_env
    db = env["db"]
    raw_path = env["src"] / "source.NEF"
    raw_path.write_bytes(b"\x00")
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='bird1.jpg',
               working_copy_path=NULL,
               width=800, height=600
           WHERE id=?""",
        (env["p1"],),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        env["p1"],
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    load_calls = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        load_calls.append((str(file_path), kwargs))
        return Image.new("RGB", (800, 600), color="red")

    monkeypatch.setattr(export_module, "load_image", tracking_load_image)

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 300,
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    assert len(load_calls) == 1
    loaded_path, loaded_kwargs = load_calls[0]
    assert loaded_path.lower().endswith(".nef"), (
        f"export should load the RAW primary, got {loaded_path!r}"
    )
    assert loaded_kwargs.get("raw_decode") == RAW_DECODE_PRESERVE_HIGHLIGHTS


def test_export_edited_raw_uses_working_copy_when_source_missing(export_env):
    """Resized edited RAW exports may use a sufficient working copy offline."""
    env = export_env
    db = env["db"]
    working_dir = os.path.join(env["vireo_dir"], "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_rel = f"working/{env['p1']}.jpg"
    Image.new("RGB", (800, 600), color="red").save(
        os.path.join(env["vireo_dir"], wc_rel), "JPEG", quality=95,
    )
    db.conn.execute(
        """UPDATE photos
           SET filename='offline.NEF', extension='.nef',
               working_copy_path=?,
               companion_path=NULL,
               width=800, height=600
           WHERE id=?""",
        (wc_rel, env["p1"]),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(env["p1"], {"rotation": 90})

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 400,
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    with Image.open(os.path.join(env["dest"], "offline.jpg")) as img:
        assert img.size == (300, 400)


def test_export_edited_raw_uses_working_copy_when_folder_missing(export_env):
    """Missing-folder RAW exports may still use a sufficient local working copy."""
    env = export_env
    db = env["db"]
    working_dir = os.path.join(env["vireo_dir"], "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_rel = f"working/{env['p1']}.jpg"
    Image.new("RGB", (800, 600), color="red").save(
        os.path.join(env["vireo_dir"], wc_rel), "JPEG", quality=95,
    )
    db.conn.execute(
        """UPDATE photos
           SET filename='offline.NEF', extension='.nef',
               working_copy_path=?,
               companion_path=NULL,
               width=800, height=600
           WHERE id=?""",
        (wc_rel, env["p1"]),
    )
    db.conn.execute("UPDATE folders SET status='missing' WHERE id=?", (env["fid"],))
    db.conn.commit()
    db.set_photo_edit_recipe(env["p1"], {"rotation": 90})

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 400,
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    with Image.open(os.path.join(env["dest"], "offline.jpg")) as img:
        assert img.size == (300, 400)


def test_export_edited_raw_uses_companion_when_source_missing(export_env):
    """Offline edited RAW+JPEG exports may use a full-size companion."""
    env = export_env
    db = env["db"]
    db.conn.execute(
        """UPDATE photos
           SET filename='offline.NEF', extension='.nef',
               working_copy_path=NULL,
               companion_path='bird1.jpg',
               width=800, height=600
           WHERE id=?""",
        (env["p1"],),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(env["p1"], {"rotation": 90})

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 400,
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    with Image.open(os.path.join(env["dest"], "offline.jpg")) as img:
        assert img.size == (300, 400)


def test_export_falls_back_to_companion_when_raw_decode_fails(
    export_env, monkeypatch,
):
    """When the RAW primary fails to decode, fall back to the companion JPEG.

    A camera JPEG with clipped highlights still beats a failed export. The
    unconditional RAW skip in `_companion_can_satisfy_export` would otherwise
    leave RAW+JPEG photos with no usable source whenever libraw can't
    demosaic the RAW.
    """
    import export as export_module
    from image_loader import RAW_DECODE_PRESERVE_HIGHLIGHTS

    env = export_env
    db = env["db"]
    raw_path = env["src"] / "source.NEF"
    raw_path.write_bytes(b"\x00")
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='bird1.jpg',
               working_copy_path=NULL,
               width=800, height=600
           WHERE id=?""",
        (env["p1"],),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        env["p1"],
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    load_calls = []

    def flaky_load_image(file_path, max_size=1024, **kwargs):
        load_calls.append((str(file_path), kwargs))
        if str(file_path).lower().endswith(".nef"):
            return None
        return Image.new("RGB", (800, 600), color="green")

    monkeypatch.setattr(export_module, "load_image", flaky_load_image)

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 300,
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    # RAW first (preserve-highlights), then companion as fallback.
    assert len(load_calls) == 2
    assert load_calls[0][0].lower().endswith(".nef")
    # The failed RAW attempt must still request preserve-highlights so the
    # fallback path can't quietly downgrade the RAW-first contract by
    # passing the default JPEG-first decode mode.
    assert (
        load_calls[0][1].get("raw_decode") == RAW_DECODE_PRESERVE_HIGHLIGHTS
    )
    assert load_calls[1][0].lower().endswith(".jpg")
    # The fallback companion load must NOT pass raw_decode (it's a JPEG).
    assert "raw_decode" not in load_calls[1][1]


def test_export_falls_back_to_companion_when_raw_returns_undersized_image(
    export_env, monkeypatch,
):
    """Undersized RAW results (embedded preview) trigger the companion fallback.

    ``image_loader._load_raw`` returns ``raw.extract_thumb()`` when libraw
    cannot demosaic the RAW. That preview can be much smaller than the
    full-size companion JPEG, so a non-None ``img`` here would silently
    produce a downscaled export. Compare against the source's expected
    long edge and prefer the companion when the RAW result falls short.
    """
    import export as export_module

    env = export_env
    db = env["db"]
    raw_path = env["src"] / "source.NEF"
    raw_path.write_bytes(b"\x00")
    # Overwrite the fixture's 800x600 JPEG with a full-size companion so
    # _companion_can_satisfy_export accepts it for a crop against the
    # 6000x4000 source.
    Image.new("RGB", (6000, 4000), color="green").save(
        str(env["src"] / "bird1.jpg"), "JPEG", quality=85,
    )
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='bird1.jpg',
               working_copy_path=NULL,
               width=6000, height=4000
           WHERE id=?""",
        (env["p1"],),
    )
    db.conn.commit()
    # Crop recipe so load_max_size is None and we ask for the full source.
    db.set_photo_edit_recipe(
        env["p1"],
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    load_calls = []

    def fake_load_image(file_path, max_size=1024, **kwargs):
        load_calls.append((str(file_path), kwargs))
        if str(file_path).lower().endswith(".nef"):
            # Stand in for _load_raw returning an undersized embedded JPEG
            # when libraw fails to demosaic the RAW.
            return Image.new("RGB", (1600, 1067), color="red")
        # Companion JPEG is the actual full-size source.
        return Image.new("RGB", (6000, 4000), color="green")

    monkeypatch.setattr(export_module, "load_image", fake_load_image)

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            # No max_size so the recipe's crop runs against the full source.
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    # RAW first (preserve-highlights), companion as fallback since the RAW
    # load returned an undersized result.
    assert len(load_calls) == 2
    assert load_calls[0][0].lower().endswith(".nef")
    assert load_calls[1][0].lower().endswith(".jpg")
    # Final export must be the full-resolution cropped output (3000x2000),
    # not the downscaled embedded preview's crop (800x533).
    with Image.open(os.path.join(env["dest"], "source.jpg")) as img:
        assert img.size == (3000, 2000)


def test_export_falls_back_to_companion_when_raw_short_edge_is_smaller(
    export_env, monkeypatch,
):
    """Companion replaces RAW even when long edges tie.

    Codex's regression case: a 6000x3376 embedded preview for a 6000x4000
    RAW has the same long edge as the companion JPEG. A long-edge-only
    swap gate (``max(companion.size) > max(img.size)``) would keep the
    embedded preview and crop from pixels missing short-edge content. The
    swap must compare both axes.
    """
    import export as export_module

    env = export_env
    db = env["db"]
    raw_path = env["src"] / "source.NEF"
    raw_path.write_bytes(b"\x00")
    Image.new("RGB", (6000, 4000), color="green").save(
        str(env["src"] / "bird1.jpg"), "JPEG", quality=85,
    )
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='bird1.jpg',
               working_copy_path=NULL,
               width=6000, height=4000
           WHERE id=?""",
        (env["p1"],),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        env["p1"],
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    load_calls = []

    def fake_load_image(file_path, max_size=1024, **kwargs):
        load_calls.append((str(file_path), kwargs))
        if str(file_path).lower().endswith(".nef"):
            # Long edge ties the companion (6000), but the short edge
            # falls short of the 4000 sensor height.
            return Image.new("RGB", (6000, 3376), color="red")
        return Image.new("RGB", (6000, 4000), color="green")

    monkeypatch.setattr(export_module, "load_image", fake_load_image)

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}"},
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    # Both loads happen and the companion wins despite the long-edge tie.
    assert len(load_calls) == 2
    assert load_calls[0][0].lower().endswith(".nef")
    assert load_calls[1][0].lower().endswith(".jpg")
    with Image.open(os.path.join(env["dest"], "source.jpg")) as img:
        # 50% crop of full-size companion (6000x4000 -> 3000x2000), not
        # the undersized embedded preview's crop (6000x3376 -> 3000x1688)
        # that a long-edge-only gate would have kept.
        assert img.size == (3000, 2000)


def test_export_rejects_reduced_companion_when_raw_decode_fails(
    export_env, monkeypatch,
):
    """A reduced/aspect-cropped sidecar must not satisfy RAW fallback export."""
    import export as export_module

    env = export_env
    db = env["db"]
    raw_path = env["src"] / "source.NEF"
    raw_path.write_bytes(b"\x00")
    Image.new("RGB", (6000, 3376), color="green").save(
        env["src"] / "source.jpg", "JPEG", quality=95,
    )
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='source.jpg',
               working_copy_path=NULL,
               width=6000, height=4000
           WHERE id=?""",
        (env["p1"],),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(env["p1"], {"rotation": 0})

    load_calls = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        load_calls.append(str(file_path))
        if str(file_path).lower().endswith(".nef"):
            return None
        return Image.new("RGB", (3000, 1688), color="green")

    monkeypatch.setattr(export_module, "load_image", tracking_load_image)

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 3000,
        },
    )

    assert result["exported"] == 0
    assert result["errors"] == ["source.NEF: failed to load image"]
    assert len(load_calls) == 1
    assert load_calls[0].lower().endswith(".nef")


def test_export_keeps_raw_when_decode_succeeds_at_full_size(
    export_env, monkeypatch,
):
    """A successful preserve-highlights RAW decode is not replaced by companion."""
    import export as export_module

    env = export_env
    db = env["db"]
    raw_path = env["src"] / "source.NEF"
    raw_path.write_bytes(b"\x00")
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='bird1.jpg',
               working_copy_path=NULL,
               width=6000, height=4000
           WHERE id=?""",
        (env["p1"],),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        env["p1"],
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    load_calls = []

    def fake_load_image(file_path, max_size=1024, **kwargs):
        load_calls.append((str(file_path), kwargs))
        # RAW decode succeeds at full sensor dimensions.
        return Image.new("RGB", (6000, 4000), color="red")

    monkeypatch.setattr(export_module, "load_image", fake_load_image)

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    # Only the RAW load; no companion fallback when the RAW result is
    # already full-size.
    assert len(load_calls) == 1
    assert load_calls[0][0].lower().endswith(".nef")


def test_export_cropped_recipe_avoids_undersized_developed_output(export_env):
    """Cropped resized exports skip developed files that lack source pixels."""
    env = export_env
    db = env["db"]
    db.conn.execute(
        "UPDATE photos SET width=800, height=600 WHERE id=?",
        (env["p1"],),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        env["p1"],
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    developed = env["tmp_path"] / "developed"
    developed_subdir = developed / developed_folder_key(str(env["src"]))
    developed_subdir.mkdir(parents=True)
    Image.new("RGB", (400, 300), color="red").save(
        str(developed_subdir / "bird1.jpg"), "JPEG", quality=95,
    )

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 300,
            "developed_dir": str(developed),
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    with Image.open(os.path.join(env["dest"], "bird1.jpg")) as img:
        assert img.size == (300, 225)


def test_export_cropped_recipe_uses_sufficient_developed_output(export_env):
    """Cropped exports keep developed files that satisfy the cropped output."""
    env = export_env
    db = env["db"]
    db.conn.execute(
        "UPDATE photos SET width=8000, height=6000 WHERE id=?",
        (env["p1"],),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        env["p1"],
        {"crop": {"x": 0, "y": 0, "w": 0.1, "h": 0.1}},
    )

    developed = env["tmp_path"] / "developed"
    developed_subdir = developed / developed_folder_key(str(env["src"]))
    developed_subdir.mkdir(parents=True)
    Image.new("RGB", (8000, 6000), color="green").save(
        str(developed_subdir / "bird1.jpg"), "JPEG", quality=95,
    )
    os.remove(env["src"] / "bird1.jpg")

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 4000,
            "developed_dir": str(developed),
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    with Image.open(os.path.join(env["dest"], "bird1.jpg")) as img:
        assert img.size == (800, 600)
        r, g, b = img.resize((1, 1)).getpixel((0, 0))
    assert g > r and g > b


def test_export_cropped_recipe_uses_exif_oriented_original_dimensions(export_env):
    """Crop-aware export sizing must match load_image's EXIF-transposed source."""
    env = export_env
    db = env["db"]
    src_path = env["src"] / "bird1.jpg"
    original = Image.new("RGB", (600, 400), color="red")
    exif = original.getexif()
    exif[0x0112] = 6
    original.save(str(src_path), "JPEG", quality=95, exif=exif.tobytes())
    db.conn.execute(
        "UPDATE photos SET width=?, height=?, exif_data=? WHERE id=?",
        (
            600,
            400,
            json.dumps({"EXIF": {"Orientation": 6}}),
            env["p1"],
        ),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        env["p1"],
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )

    developed = env["tmp_path"] / "developed"
    developed_subdir = developed / developed_folder_key(str(env["src"]))
    developed_subdir.mkdir(parents=True)
    Image.new("RGB", (400, 400), color="green").save(
        str(developed_subdir / "bird1.jpg"), "JPEG", quality=95,
    )

    result = export_photos(
        db=db,
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "max_size": 500,
            "developed_dir": str(developed),
        },
    )

    assert result["exported"] == 1
    assert result["errors"] == []
    with Image.open(os.path.join(env["dest"], "bird1.jpg")) as img:
        assert max(img.size) == 500
        r, g, b = img.resize((1, 1)).getpixel((0, 0))
    assert r > g and r > b


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


def test_export_photos_convert_png_batch_deduplicates_extension(export_env):
    """Batch conversion uses the selected extension for every output/collision."""
    env = export_env
    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"], env["p2"]],
        destination=env["dest"],
        options={"naming_template": "converted", "format": "png"},
    )

    assert result["exported"] == 2
    first = os.path.join(env["dest"], "converted.png")
    second = os.path.join(env["dest"], "converted_2.png")
    assert os.path.isfile(first)
    assert os.path.isfile(second)
    assert not os.path.exists(os.path.join(env["dest"], "converted.jpg"))
    with Image.open(first) as img:
        assert img.format == "PNG"
    with Image.open(second) as img:
        assert img.format == "PNG"


def test_export_photos_convert_tiff_applies_recipe_and_resize(export_env):
    """Converted exports still apply edit recipes before final resizing."""
    env = export_env
    env["db"].set_photo_edit_recipe(
        env["p1"],
        {
            "rotation": 90,
            "crop": {"x": 0, "y": 0, "w": 0.5, "h": 1},
        },
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={
            "naming_template": "{original}",
            "format": "tiff",
            "max_size": 400,
        },
    )

    assert result["exported"] == 1
    out_path = os.path.join(env["dest"], "bird1.tiff")
    with Image.open(out_path) as img:
        assert img.format == "TIFF"
        assert img.size == (150, 400)


def test_export_rejects_unknown_output_format(export_env):
    """Unknown export formats fail before writing partial outputs."""
    env = export_env
    with pytest.raises(ValueError, match="format must be one of"):
        export_photos(
            db=env["db"],
            vireo_dir=env["vireo_dir"],
            photo_ids=[env["p1"]],
            destination=env["dest"],
            options={"naming_template": "{original}", "format": "bmp"},
        )


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


def test_export_tiff_prefers_developed_tiff_over_developed_jpg(export_env):
    """TIFF exports use the TIFF developed source when both formats exist."""
    env = export_env
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed_dir / "bird1.jpg"), "JPEG", quality=95,
    )
    Image.new("RGB", (800, 600), color=(20, 30, 220)).save(
        str(developed_dir / "bird1.tiff"), "TIFF",
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "format": "tiff"},
    )

    assert result["exported"] == 1
    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.tiff"))
    assert b > r and b > g, (
        f"expected blue-dominant from developed TIFF, got rgb=({r},{g},{b})"
    )


def test_export_png_prefers_developed_tiff_over_developed_jpg(export_env):
    """PNG exports use the best developed source instead of stale JPEG."""
    env = export_env
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed_dir / "bird1.jpg"), "JPEG", quality=95,
    )
    Image.new("RGB", (800, 600), color=(20, 30, 220)).save(
        str(developed_dir / "bird1.tiff"), "TIFF",
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "format": "png"},
    )

    assert result["exported"] == 1
    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.png"))
    assert b > r and b > g, (
        f"expected blue-dominant from developed TIFF, got rgb=({r},{g},{b})"
    )


def test_export_tries_next_developed_candidate_when_preferred_is_too_small(export_env):
    """A too-small preferred developed file should not hide a usable fallback."""
    env = export_env
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    Image.new("RGB", (800, 600), color=(10, 200, 40)).save(
        str(developed_dir / "bird1.jpg"), "JPEG", quality=95,
    )
    Image.new("RGB", (200, 150), color=(20, 30, 220)).save(
        str(developed_dir / "bird1.tiff"), "TIFF",
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "format": "png", "max_size": 400},
    )

    assert result["exported"] == 1
    out_path = os.path.join(env["dest"], "bird1.png")
    with Image.open(out_path) as img:
        assert max(img.size) == 400
    r, g, b = _avg_rgb(out_path)
    assert g > r and g > b, (
        f"expected green-dominant from sufficient developed JPG, got rgb=({r},{g},{b})"
    )


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


def test_relocate_developed_dir_merges_empty_source_into_existing_target(tmp_path):
    """Empty source + existing target: merge succeeds, empty source is removed.

    With the merge semantics in place (relocation into an existing target
    is a merge, not a skip), an empty source directory is a no-op merge:
    there's nothing to move in, and the empty source is cleaned up so
    callers don't leak orphaned directories.
    """
    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_path = "/srv/photos/a"
    new_path = "/srv/photos/b"

    (developed / developed_folder_key(old_path)).mkdir()
    (developed / developed_folder_key(new_path)).mkdir()

    assert relocate_developed_dir(str(developed), old_path, new_path) is True
    assert not (developed / developed_folder_key(old_path)).exists()
    assert (developed / developed_folder_key(new_path)).is_dir()


def test_relocate_developed_file_moves_matching_stem(tmp_path):
    """Per-photo relocation moves every extension whose stem matches the photo.

    Used by ``move_photos`` for date-organized moves where a single source
    folder's photos fan out to many destinations, so the whole-subdir
    rename that ``relocate_developed_dir`` does can't apply.
    """
    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_path = "/srv/photos/card"
    new_path = "/srv/photos/archive/2026-07-12"
    old_key = developed_folder_key(old_path)
    new_key = developed_folder_key(new_path)
    (developed / old_key).mkdir()
    (developed / old_key / "IMG_0001.jpg").write_bytes(b"jpg")
    (developed / old_key / "IMG_0001.tiff").write_bytes(b"tiff")
    # A sibling photo's render stays behind — it isn't part of this move.
    (developed / old_key / "IMG_0002.jpg").write_bytes(b"sibling")

    moved = relocate_developed_file(
        str(developed), old_path, new_path, "IMG_0001",
    )
    assert moved == 2
    assert (developed / new_key / "IMG_0001.jpg").read_bytes() == b"jpg"
    assert (developed / new_key / "IMG_0001.tiff").read_bytes() == b"tiff"
    # Sibling stays under the old key; old subdir remains because it isn't
    # empty yet.
    assert (developed / old_key / "IMG_0002.jpg").read_bytes() == b"sibling"


def test_relocate_developed_file_removes_old_subdir_when_empty(tmp_path):
    """When the last matching file is moved, the old key's dir is cleaned up
    — matches ``relocate_developed_dir``'s post-rename semantics.
    """
    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_path = "/srv/photos/card"
    new_path = "/srv/photos/archive/2026-07-12"
    old_key = developed_folder_key(old_path)
    (developed / old_key).mkdir()
    (developed / old_key / "only.jpg").write_bytes(b"only")

    assert relocate_developed_file(
        str(developed), old_path, new_path, "only",
    ) == 1
    assert not (developed / old_key).exists()


def test_relocate_developed_file_preserves_existing_target(tmp_path):
    """A collision at the destination is preserved — matches the photo-file
    collision policy in ``move_photos``.
    """
    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_path = "/srv/photos/card"
    new_path = "/srv/photos/archive/2026-07-12"
    old_key = developed_folder_key(old_path)
    new_key = developed_folder_key(new_path)
    (developed / old_key).mkdir()
    (developed / new_key).mkdir()
    (developed / old_key / "IMG_0001.jpg").write_bytes(b"src")
    (developed / new_key / "IMG_0001.jpg").write_bytes(b"dst")

    moved = relocate_developed_file(
        str(developed), old_path, new_path, "IMG_0001",
    )
    assert moved == 0
    assert (developed / old_key / "IMG_0001.jpg").read_bytes() == b"src"
    assert (developed / new_key / "IMG_0001.jpg").read_bytes() == b"dst"


def test_relocate_developed_file_falls_back_across_filesystems(
    tmp_path, monkeypatch,
):
    """EXDEV falls back to copy+remove for mounted archive destinations."""
    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_path = "/srv/photos/card"
    new_path = "/mnt/archive/2026-07-12"
    old_key = developed_folder_key(old_path)
    new_key = developed_folder_key(new_path)
    old_subdir = developed / old_key
    old_subdir.mkdir()
    source = old_subdir / "IMG_0001.jpg"
    source.write_bytes(b"developed")

    real_rename = os.rename

    def cross_device_rename(src, dst):
        if os.fspath(src) == str(source):
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return real_rename(src, dst)

    monkeypatch.setattr(os, "rename", cross_device_rename)

    moved = relocate_developed_file(
        str(developed), old_path, new_path, "IMG_0001",
    )

    assert moved == 1
    assert not source.exists()
    assert (developed / new_key / "IMG_0001.jpg").read_bytes() == b"developed"


def test_relocate_developed_file_noop_without_configuration(tmp_path):
    """Missing developed_dir, missing stem, or identical paths is a no-op."""
    assert relocate_developed_file("", "/a", "/b", "stem") == 0
    assert relocate_developed_file(str(tmp_path), "", "/b", "stem") == 0
    assert relocate_developed_file(str(tmp_path), "/a", "/b", "") == 0
    assert relocate_developed_file(str(tmp_path), "/same", "/same", "stem") == 0


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


def test_export_skips_smaller_developed_for_full_res(export_env):
    """Full-res export must not silently use a down-scaled developed file.

    Regression: export_photos preferred the developed file whenever one
    existed, but `/api/jobs/develop` lets users pass `--width`, which
    writes a smaller JPEG/TIFF. A subsequent full-resolution export
    (max_size unset) would silently ship that smaller file instead of
    decoding the full-resolution original, dropping resolution without
    warning.
    """
    env = export_env
    # Record the original's true dimensions so the size-aware guard can
    # detect that the developed file is smaller than the original. In
    # production this is populated by the scan job.
    env["db"].conn.execute(
        "UPDATE photos SET width = ?, height = ? WHERE id = ?",
        (800, 600, env["p1"]),
    )
    env["db"].conn.commit()

    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    # Smaller than the 800x600 original the fixture writes to disk.
    Image.new("RGB", (200, 150), color=(10, 200, 40)).save(
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
    with Image.open(os.path.join(env["dest"], "bird1.jpg")) as out:
        assert out.size == (800, 600), (
            f"full-res export should ship the 800x600 original, not the "
            f"200x150 developed file; got {out.size}"
        )


def test_export_skips_developed_when_max_size_exceeds_developed_dims(export_env):
    """Export with max_size larger than the developed file's long edge skips it.

    If the developed output can't satisfy the requested max_size, using
    it silently downscales the output below the user's requested cap.
    """
    env = export_env
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    # Developed long-edge is 200; user asks for max_size=400.
    Image.new("RGB", (200, 150), color=(10, 200, 40)).save(
        str(developed_dir / "bird1.jpg"), "JPEG", quality=95,
    )

    result = export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "max_size": 400},
    )

    assert result["exported"] == 1
    with Image.open(os.path.join(env["dest"], "bird1.jpg")) as out:
        # Original (800x600) resized to max=400 → 400x300.
        # Developed would have silently capped at 200x150.
        assert max(out.size) == 400, (
            f"expected 400 long edge (from 800x600 original), got {out.size}"
        )


def test_export_uses_developed_when_max_size_fits(export_env):
    """Resize exports still use developed when it can satisfy max_size.

    Guardrail for the primary feature: resize workflows with max_size
    <= developed's long edge must ship the perfected rendering.
    """
    env = export_env
    developed_dir = env["src"] / "developed"
    developed_dir.mkdir()
    Image.new("RGB", (600, 450), color=(10, 200, 40)).save(
        str(developed_dir / "bird1.jpg"), "JPEG", quality=95,
    )

    export_photos(
        db=env["db"],
        vireo_dir=env["vireo_dir"],
        photo_ids=[env["p1"]],
        destination=env["dest"],
        options={"naming_template": "{original}", "max_size": 300},
    )

    r, g, b = _avg_rgb(os.path.join(env["dest"], "bird1.jpg"))
    assert g > r and g > b, (
        f"expected green-dominant (developed used for fitting resize), "
        f"got rgb=({r},{g},{b})"
    )


def test_relocate_developed_dir_merges_into_existing_target(tmp_path):
    """When target exists, merge source files in; don't strand them at old key.

    Regression: relocate_developed_dir used to bail out when the target
    subdir existed, but `db._merge_into_existing` (hit by
    /api/folders/<id>/relocate when the new path is already tracked)
    reassigns source photos to the target folder — their developed files
    belong under the target's key after the merge. The previous behaviour
    left them stranded under the old key, so export silently fell back
    to RAW for every merged photo.
    """
    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_path = "/srv/photos/source"
    new_path = "/srv/photos/target"

    old_key = developed_folder_key(old_path)
    new_key = developed_folder_key(new_path)

    old_subdir = developed / old_key
    new_subdir = developed / new_key
    old_subdir.mkdir()
    new_subdir.mkdir()

    # Source-only file (must move into target).
    (old_subdir / "unique_src.jpg").write_bytes(b"src-only")
    # Collision: target-side wins (matches merge semantics where target
    # folder's photos are authoritative on filename collisions).
    (old_subdir / "collision.jpg").write_bytes(b"src-version")
    (new_subdir / "collision.jpg").write_bytes(b"target-wins")
    # Target-only file (must stay untouched).
    (new_subdir / "unique_tgt.jpg").write_bytes(b"tgt-only")

    assert relocate_developed_dir(str(developed), old_path, new_path) is True

    assert not old_subdir.exists(), "source subdir must be removed after merge"
    assert (new_subdir / "unique_src.jpg").read_bytes() == b"src-only"
    assert (new_subdir / "unique_tgt.jpg").read_bytes() == b"tgt-only"
    # Collision: target's version must survive.
    assert (new_subdir / "collision.jpg").read_bytes() == b"target-wins"


def test_move_folder_descendant_query_ignores_like_wildcard_matches(tmp_path, monkeypatch):
    """LIKE wildcards in dest_path must not drag unrelated folders into the rebase.

    Regression: `move_folder`'s descendant query used `WHERE path LIKE ?`
    with `dest_path + "/%"`. SQL LIKE treats `_` and `%` inside `dest_path`
    as wildcards (valid characters on POSIX paths), so unrelated folders
    whose paths happen to match the wildcard pattern were returned and
    fed into `relocate_developed_dir` with a bogus computed old_path.
    """
    import export
    from db import Database
    from move import move_folder

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src_parent = tmp_path / "src"
    src_parent.mkdir()
    src = src_parent / "birds"
    src.mkdir()
    (src / "IMG_0001.jpg").write_bytes(b"raw")
    fid = db.add_folder(str(src), name="birds")

    # Destination dir whose name contains `_`: SQL LIKE treats `_` as any
    # single char. Final moved path becomes `<tmp>/d_st/birds`.
    dest_parent = tmp_path / "d_st"
    dest_parent.mkdir()

    # Unrelated folder whose path matches the buggy LIKE pattern
    # `<tmp>/d_st/birds/%` because `_` matches the literal `X`. The folder
    # is NOT a descendant of the moved folder and must be left alone.
    unrelated_parent = tmp_path / "dXst" / "birds"
    unrelated_parent.mkdir(parents=True)
    unrelated = unrelated_parent / "fake"
    unrelated.mkdir()
    db.add_folder(str(unrelated), name="fake")

    developed = tmp_path / "darktable_out"
    developed.mkdir()

    calls = []
    real_fn = export.relocate_developed_dir

    def spy(devdir, old, new):
        calls.append((old, new))
        return real_fn(devdir, old, new)

    monkeypatch.setattr(export, "relocate_developed_dir", spy)

    result = move_folder(
        db=db,
        folder_id=fid,
        destination=str(dest_parent),
        developed_dir=str(developed),
    )
    assert not result["errors"], result["errors"]

    # Only the parent rebase should have fired; no call should reference
    # the unrelated path on either side.
    unrelated_str = str(unrelated)
    touched = {old for old, _ in calls} | {new for _, new in calls}
    assert unrelated_str not in touched, (
        f"relocate_developed_dir was called against the unrelated folder; "
        f"calls={calls}"
    )
