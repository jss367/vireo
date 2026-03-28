import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime

from ingest import build_destination_path, discover_source_files
from PIL import Image


def test_build_destination_path_default_template():
    dt = datetime(2026, 3, 28, 14, 30, 0)
    assert build_destination_path(dt) == "2026/03/28"


def test_build_destination_path_custom_template():
    dt = datetime(2026, 3, 28, 14, 30, 0)
    assert build_destination_path(dt, "%Y/%m") == "2026/03"


def test_build_destination_path_none_returns_unsorted():
    assert build_destination_path(None) == "unsorted"


def _create_test_files(root, filenames):
    """Create test image/raw files in a directory."""
    os.makedirs(root, exist_ok=True)
    for fname in filenames:
        path = os.path.join(root, fname)
        if fname.lower().endswith((".jpg", ".jpeg", ".png")):
            Image.new("RGB", (100, 100), color="green").save(path)
        else:
            # For raw files, just create a dummy file
            with open(path, "wb") as f:
                f.write(b"\x00" * 100)


def test_discover_source_files_both(tmp_path):
    src = str(tmp_path / "sd_card")
    _create_test_files(src, ["IMG_001.jpg", "IMG_001.cr3", "IMG_002.jpg"])
    files = discover_source_files(src, file_types="both")
    names = [f.name for f in files]
    assert "IMG_001.jpg" in names
    assert "IMG_001.cr3" in names
    assert "IMG_002.jpg" in names
    assert len(files) == 3


def test_discover_source_files_jpeg_only(tmp_path):
    src = str(tmp_path / "sd_card")
    _create_test_files(src, ["IMG_001.jpg", "IMG_001.cr3", "IMG_002.jpg"])
    files = discover_source_files(src, file_types="jpeg")
    names = [f.name for f in files]
    assert "IMG_001.jpg" in names
    assert "IMG_002.jpg" in names
    assert "IMG_001.cr3" not in names


def test_discover_source_files_raw_only(tmp_path):
    src = str(tmp_path / "sd_card")
    _create_test_files(src, ["IMG_001.jpg", "IMG_001.cr3"])
    files = discover_source_files(src, file_types="raw")
    names = [f.name for f in files]
    assert "IMG_001.cr3" in names
    assert "IMG_001.jpg" not in names


def test_discover_source_files_skips_hidden(tmp_path):
    src = str(tmp_path / "sd_card")
    _create_test_files(src, [".hidden.jpg", "visible.jpg"])
    files = discover_source_files(src, file_types="both")
    names = [f.name for f in files]
    assert "visible.jpg" in names
    assert ".hidden.jpg" not in names


def test_discover_source_files_recursive(tmp_path):
    src = tmp_path / "sd_card"
    sub = src / "DCIM" / "100CANON"
    _create_test_files(str(sub), ["IMG_001.jpg"])
    files = discover_source_files(str(src), file_types="both")
    assert len(files) == 1
    assert files[0].name == "IMG_001.jpg"


def test_discover_source_files_nonexistent_dir():
    files = discover_source_files("/nonexistent/path", file_types="both")
    assert files == []
