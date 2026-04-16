import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from db import Database
from PIL import Image


def _touch_image(path):
    """Create a real 1x1 JPEG at path."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(path, "JPEG")


@pytest.fixture
def db_with_workspace(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    return db, ws_id, tmp_path


def test_count_new_images_detects_unscanned_files(db_with_workspace):
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "USA2026"
    _touch_image(str(root / "IMG_0001.JPG"))
    _touch_image(str(root / "IMG_0002.JPG"))
    db.add_folder(str(root), name="USA2026")

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 2
    assert len(result["per_root"]) == 1
    assert result["per_root"][0]["new_count"] == 2
    assert len(result["sample"]) == 2


def test_count_new_images_no_double_counting_with_nested_linked_folders(db_with_workspace):
    """Nested subfolders auto-linked to workspace_folders must not cause double-counting."""
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "USA2026"
    nested = root / "day1"
    deep = nested / "raw"
    _touch_image(str(deep / "IMG_0001.JPG"))  # one unscanned file, three levels deep

    # Register root AND the intermediate dirs as workspace_folders (mirrors what
    # the scanner's Database.add_folder does for every discovered subdirectory).
    root_id = db.add_folder(str(root), name="USA2026")
    nested_id = db.add_folder(str(nested), name="day1", parent_id=root_id)
    db.add_folder(str(deep), name="raw", parent_id=nested_id)

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 1, (
        f"Expected 1 new image, got {result['new_count']}. "
        f"per_root={result['per_root']}"
    )
    # Only the top-level root should appear in per_root.
    assert len(result["per_root"]) == 1
    assert result["per_root"][0]["path"] == str(root)
