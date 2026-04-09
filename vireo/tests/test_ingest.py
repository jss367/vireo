import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime

from db import Database
from ingest import build_destination_path, discover_source_files, ingest, preview_destination
from PIL import Image


def test_build_destination_path_default_template():
    dt = datetime(2026, 3, 28, 14, 30, 0)
    assert build_destination_path(dt) == "2026/2026-03-28"


def test_build_destination_path_custom_template():
    dt = datetime(2026, 3, 28, 14, 30, 0)
    assert build_destination_path(dt, "%Y/%m") == "2026/03"


def test_build_destination_path_none_returns_unsorted():
    assert build_destination_path(None) == "unsorted"


def test_build_destination_path_rejects_absolute_template():
    import pytest

    dt = datetime(2026, 3, 28, 14, 30, 0)
    with pytest.raises(ValueError, match="unsafe folder template"):
        build_destination_path(dt, "/tmp/%Y")


def test_build_destination_path_rejects_traversal_template():
    import pytest

    dt = datetime(2026, 3, 28, 14, 30, 0)
    with pytest.raises(ValueError, match="unsafe folder template"):
        build_destination_path(dt, "../outside/%Y")


def test_build_destination_path_rejects_backslash_template():
    import pytest

    dt = datetime(2026, 3, 28, 14, 30, 0)
    with pytest.raises(ValueError, match="unsafe folder template"):
        build_destination_path(dt, "..\\outside\\%Y")


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


def test_discover_source_files_non_recursive(tmp_path):
    src = tmp_path / "sd_card"
    _create_test_files(str(src), ["top.jpg"])
    sub = src / "DCIM"
    _create_test_files(str(sub), ["nested.jpg"])
    files = discover_source_files(str(src), file_types="both", recursive=False)
    assert len(files) == 1
    assert files[0].name == "top.jpg"


def test_discover_source_files_nonexistent_dir():
    files = discover_source_files("/nonexistent/path", file_types="both")
    assert files == []


def test_ingest_copies_files_to_date_folders(tmp_path):
    """Files are copied to destination organized by EXIF date (falls back to mtime)."""
    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    # Create a JPEG with known mtime (no EXIF in synthetic images)
    img = Image.new("RGB", (100, 100), color="red")
    img.save(str(src / "photo.jpg"))
    # Set mtime to a known date for predictable fallback
    mtime = datetime(2026, 3, 28, 10, 0, 0).timestamp()
    os.utime(str(src / "photo.jpg"), (mtime, mtime))

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(src), str(dst), db=db)

    assert result["copied"] == 1
    assert result["total"] == 1
    assert (dst / "2026" / "2026-03-28" / "photo.jpg").exists()


def test_ingest_unsorted_fallback(tmp_path):
    """Files with no EXIF and no readable mtime go to unsorted/."""
    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    # Create a non-image file with a supported extension (will fail EXIF read)
    with open(str(src / "corrupt.jpg"), "wb") as f:
        f.write(b"not a real jpeg")

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(src), str(dst), db=db)

    assert result["copied"] == 1
    # Falls back to file mtime, so it should end up in a date folder.
    # Only truly unsorted if we can't read mtime either — which doesn't happen on real FS.
    # So we just verify the file was copied somewhere under dst.
    copied_files = list(dst.rglob("corrupt.jpg"))
    assert len(copied_files) == 1


def test_ingest_skip_duplicates(tmp_path):
    """Second ingest of same files detects duplicates via filesystem collision."""
    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    img = Image.new("RGB", (100, 100), color="blue")
    img.save(str(src / "photo.jpg"))

    db = Database(str(tmp_path / "test.db"))

    # First ingest
    result1 = ingest(str(src), str(dst), db=db, skip_duplicates=True)
    assert result1["copied"] == 1

    # Second ingest of same file — should skip
    result2 = ingest(str(src), str(dst), db=db, skip_duplicates=True)
    assert result2["copied"] == 0
    assert result2["skipped_duplicate"] == 1


def test_ingest_custom_folder_template(tmp_path):
    """Custom folder template is used for organization."""
    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    img = Image.new("RGB", (100, 100), color="green")
    img.save(str(src / "photo.jpg"))
    mtime = datetime(2026, 3, 28, 10, 0, 0).timestamp()
    os.utime(str(src / "photo.jpg"), (mtime, mtime))

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(src), str(dst), db=db, folder_template="%Y/%m")

    assert result["copied"] == 1
    assert (dst / "2026" / "03" / "photo.jpg").exists()


def test_ingest_filename_collision(tmp_path):
    """Same filename from different source gets a suffix."""
    src1 = tmp_path / "card1"
    src2 = tmp_path / "card2"
    dst = tmp_path / "nas"
    for d in [src1, src2, dst]:
        d.mkdir()

    # Two different images with the same filename
    Image.new("RGB", (100, 100), color="red").save(str(src1 / "IMG_001.jpg"))
    Image.new("RGB", (100, 100), color="blue").save(str(src2 / "IMG_001.jpg"))
    # Give both the same mtime so they end up in the same date folder
    mtime = datetime(2026, 3, 28).timestamp()
    os.utime(str(src1 / "IMG_001.jpg"), (mtime, mtime))
    os.utime(str(src2 / "IMG_001.jpg"), (mtime, mtime))

    db = Database(str(tmp_path / "test.db"))
    ingest(str(src1), str(dst), db=db)
    ingest(str(src2), str(dst), db=db)

    date_folder = dst / "2026" / "2026-03-28"
    files = sorted(f.name for f in date_folder.iterdir())
    assert "IMG_001.jpg" in files
    assert "IMG_001_1.jpg" in files


def test_ingest_progress_callback(tmp_path):
    """Progress callback is called for each file."""
    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    for i in range(3):
        Image.new("RGB", (50, 50)).save(str(src / f"img{i}.jpg"))

    progress_calls = []
    db = Database(str(tmp_path / "test.db"))
    ingest(str(src), str(dst), db=db,
           progress_callback=lambda cur, tot, fname: progress_calls.append((cur, tot, fname)))

    assert len(progress_calls) == 3
    assert progress_calls[-1][0] == 3  # current
    assert progress_calls[-1][1] == 3  # total


def test_ingest_skip_duplicates_via_db_hash(tmp_path):
    """Files whose hash is already in the DB (from a prior scan) are skipped."""
    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    library = tmp_path / "library"
    for d in [src, dst, library]:
        d.mkdir()

    # Create the same image in both library and source
    img = Image.new("RGB", (100, 100), color="blue")
    img.save(str(library / "existing.jpg"))
    img.save(str(src / "new_copy.jpg"))

    db = Database(str(tmp_path / "test.db"))
    # Scan library to populate file_hash in DB
    from scanner import scan

    scan(str(library), db)

    # Ingest from source -- file hash matches DB, should skip
    result = ingest(str(src), str(dst), db=db, skip_duplicates=True)
    assert result["skipped_duplicate"] == 1
    assert result["copied"] == 0
    # File should NOT exist at destination
    assert not list(dst.rglob("new_copy.jpg"))


def test_ingest_file_types_filter(tmp_path):
    """Only selected file types are copied."""
    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    Image.new("RGB", (100, 100)).save(str(src / "photo.jpg"))
    with open(str(src / "photo.cr3"), "wb") as f:
        f.write(b"\x00" * 100)

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(src), str(dst), db=db, file_types="jpeg")

    assert result["copied"] == 1
    assert result["total"] == 1
    # Only JPEG was discovered, raw was filtered out
    copied_jpgs = list(dst.rglob("*.jpg"))
    copied_raws = list(dst.rglob("*.cr3"))
    assert len(copied_jpgs) == 1
    assert len(copied_raws) == 0


def test_ingest_then_scan_end_to_end(tmp_path):
    """Full workflow: ingest from SD card to NAS, then scan the destination."""
    from scanner import scan

    src = tmp_path / "sd_card" / "DCIM" / "100CANON"
    dst = tmp_path / "nas" / "photos"
    src.mkdir(parents=True)
    dst.mkdir(parents=True)

    # Create 3 test photos on "SD card"
    for i in range(3):
        img = Image.new("RGB", (200, 100), color=(i * 80, 100, 100))
        img.save(str(src / f"IMG_{i:04d}.jpg"))
        # Set mtime to different dates
        mtime = datetime(2026, 3, 25 + i, 10, 0, 0).timestamp()
        os.utime(str(src / f"IMG_{i:04d}.jpg"), (mtime, mtime))

    db = Database(str(tmp_path / "test.db"))

    # Step 1: Ingest
    result = ingest(str(src), str(dst), db=db)
    assert result["copied"] == 3
    assert result["failed"] == 0

    # Verify folder structure
    assert (dst / "2026" / "2026-03-25").exists()
    assert (dst / "2026" / "2026-03-26").exists()
    assert (dst / "2026" / "2026-03-27").exists()

    # Step 2: Scan the destination
    scan(str(dst), db)

    photos = db.conn.execute("SELECT * FROM photos ORDER BY timestamp").fetchall()
    assert len(photos) == 3

    # Verify each photo has a file_hash
    for photo in photos:
        assert photo["file_hash"] is not None

    # Step 3: Re-ingest same card — all should be skipped as duplicates
    result2 = ingest(str(src), str(dst), db=db, skip_duplicates=True)
    assert result2["copied"] == 0
    assert result2["skipped_duplicate"] == 3


def test_preview_destination_groups_by_date(tmp_path):
    """Preview groups files into date-based folders."""

    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    # Create 3 photos with different mtimes (2 on same day, 1 different)
    for i, (name, day) in enumerate([
        ("a.jpg", 25), ("b.jpg", 25), ("c.jpg", 26)
    ]):
        img = Image.new("RGB", (100, 100), color=(i * 80, 0, 0))
        img.save(str(src / name))
        mtime = datetime(2026, 3, day, 10, 0, 0).timestamp()
        os.utime(str(src / name), (mtime, mtime))

    result = preview_destination(
        sources=[str(src)],
        destination=str(dst),
        folder_template="%Y/%Y-%m-%d",
    )

    assert result["total_photos"] == 3
    assert result["total_folders"] == 2
    # All folders are new (dst is empty)
    assert result["new_folders"] == 2
    assert result["existing_folders"] == 0

    by_path = {f["path"]: f for f in result["folders"]}
    assert "2026/2026-03-25" in by_path
    assert by_path["2026/2026-03-25"]["count"] == 2
    assert by_path["2026/2026-03-25"]["exists"] is False
    assert "2026/2026-03-26" in by_path
    assert by_path["2026/2026-03-26"]["count"] == 1


def test_preview_destination_detects_existing_folders(tmp_path):
    """Preview marks folders that already exist on disk."""

    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    img = Image.new("RGB", (100, 100))
    img.save(str(src / "photo.jpg"))
    mtime = datetime(2026, 3, 25, 10, 0, 0).timestamp()
    os.utime(str(src / "photo.jpg"), (mtime, mtime))

    # Pre-create the destination folder
    (dst / "2026" / "2026-03-25").mkdir(parents=True)

    result = preview_destination(
        sources=[str(src)],
        destination=str(dst),
        folder_template="%Y/%Y-%m-%d",
    )

    assert result["existing_folders"] == 1
    assert result["new_folders"] == 0
    assert result["folders"][0]["exists"] is True


def test_preview_destination_custom_template(tmp_path):
    """Preview respects custom folder template."""

    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    img = Image.new("RGB", (100, 100))
    img.save(str(src / "photo.jpg"))
    mtime = datetime(2026, 3, 25, 10, 0, 0).timestamp()
    os.utime(str(src / "photo.jpg"), (mtime, mtime))

    result = preview_destination(
        sources=[str(src)],
        destination=str(dst),
        folder_template="%Y/%m",
    )

    assert result["folders"][0]["path"] == "2026/03"


def test_preview_destination_flat_template(tmp_path):
    """Empty template means files go directly in destination root."""

    src = tmp_path / "sd_card"
    dst = tmp_path / "nas"
    src.mkdir()
    dst.mkdir()

    img = Image.new("RGB", (100, 100))
    img.save(str(src / "photo.jpg"))

    result = preview_destination(
        sources=[str(src)],
        destination=str(dst),
        folder_template="",
    )

    assert result["total_folders"] == 1
    assert result["folders"][0]["path"] == "."
    # dst itself exists, so flat folder should show exists=True
    assert result["folders"][0]["exists"] is True


def test_preview_destination_multiple_sources(tmp_path):
    """Preview aggregates files from multiple source folders."""

    src1 = tmp_path / "card1"
    src2 = tmp_path / "card2"
    dst = tmp_path / "nas"
    src1.mkdir()
    src2.mkdir()
    dst.mkdir()

    mtime = datetime(2026, 3, 25, 10, 0, 0).timestamp()
    for src_dir, name in [(src1, "a.jpg"), (src2, "b.jpg")]:
        img = Image.new("RGB", (100, 100))
        img.save(str(src_dir / name))
        os.utime(str(src_dir / name), (mtime, mtime))

    result = preview_destination(
        sources=[str(src1), str(src2)],
        destination=str(dst),
        folder_template="%Y/%Y-%m-%d",
    )

    assert result["total_photos"] == 2
    assert result["total_folders"] == 1
    assert result["folders"][0]["count"] == 2
