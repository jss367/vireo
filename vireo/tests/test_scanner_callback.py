"""Tests for scanner photo_callback and incremental metadata support."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database
from PIL import Image
from scanner import scan


def test_scan_calls_photo_callback_per_photo(tmp_path):
    """photo_callback should fire once per photo with (photo_id, path)."""
    for name in ["a.jpg", "b.jpg", "c.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(tmp_path / name))

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db._active_workspace_id)

    callbacks = []
    scan(tmp_path, db, photo_callback=lambda pid, path: callbacks.append((pid, path)))

    assert len(callbacks) == 3
    # Each callback should have a valid photo_id (int) and a path string
    for pid, path in callbacks:
        assert isinstance(pid, int)
        assert path.endswith(".jpg")


def test_scan_works_without_photo_callback(tmp_path):
    """scan() still works when photo_callback is not provided (backward compat)."""
    img = Image.new("RGB", (100, 100), "red")
    img.save(str(tmp_path / "test.jpg"))

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db._active_workspace_id)

    # Should not raise
    scan(tmp_path, db)


def test_scan_photo_callback_fires_for_incremental_skip(tmp_path):
    """photo_callback fires even for photos skipped during incremental scan."""
    for name in ["a.jpg", "b.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(tmp_path / name))

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db._active_workspace_id)

    # First scan: no callback, just populate the DB
    scan(tmp_path, db, incremental=False)

    # Set timestamps so incremental scan considers metadata complete
    # (PIL-generated images lack EXIF timestamps)
    db.conn.execute("UPDATE photos SET timestamp = '2026-01-01T00:00:00'")
    db.conn.commit()

    # Second scan: incremental with callback — photos already exist, should still fire
    callbacks = []
    scan(tmp_path, db, incremental=True, photo_callback=lambda pid, path: callbacks.append((pid, path)))

    assert len(callbacks) == 2
    for pid, path in callbacks:
        assert isinstance(pid, int)
        assert path.endswith(".jpg")


def test_incremental_scan_reprocesses_photos_missing_metadata(tmp_path):
    """Photos with NULL timestamp AND NULL exif_data are re-processed.

    Covers the case where ExifTool was unavailable during the original scan,
    leaving critical metadata NULL. The scanner should retry extraction rather
    than skipping these photos forever.
    """
    img = Image.new("RGB", (200, 150), "blue")
    img.save(str(tmp_path / "bird.jpg"))

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db._active_workspace_id)

    # First scan populates the photo
    scan(tmp_path, db, incremental=False)
    photos = db.get_photos(per_page=999)
    assert len(photos) == 1
    photo_id = photos[0]["id"]

    # Simulate a failed initial scan: NULL out metadata AND exif_data
    db.conn.execute(
        "UPDATE photos SET timestamp = NULL, width = NULL, height = NULL, exif_data = NULL WHERE id = ?",
        (photo_id,),
    )
    db.conn.commit()

    # Verify it's really NULL
    row = db.conn.execute("SELECT width, exif_data FROM photos WHERE id = ?", (photo_id,)).fetchone()
    assert row["width"] is None
    assert row["exif_data"] is None

    # Incremental re-scan should re-process because timestamp AND exif_data are NULL
    scan(tmp_path, db, incremental=True)

    # Width should be restored by the re-scan
    row = db.conn.execute("SELECT width, height FROM photos WHERE id = ?", (photo_id,)).fetchone()
    assert row["width"] is not None, "Width should be restored after re-scan of metadata-missing photo"
    assert row["height"] is not None, "Height should be restored after re-scan of metadata-missing photo"


def test_incremental_scan_skips_photos_with_exif_data_but_no_timestamp(tmp_path):
    """Photos with NULL timestamp but non-NULL exif_data should be skipped.

    If ExifTool ran but the file genuinely has no timestamp (e.g. screenshots),
    the photo should not be perpetually re-processed.
    """
    img = Image.new("RGB", (200, 150), "blue")
    img.save(str(tmp_path / "screenshot.jpg"))

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db._active_workspace_id)

    scan(tmp_path, db, incremental=False)
    photos = db.get_photos(per_page=999)
    photo_id = photos[0]["id"]

    # Simulate: ExifTool ran (exif_data present) but no timestamp in EXIF
    db.conn.execute(
        "UPDATE photos SET timestamp = NULL, width = 999, exif_data = '{}' WHERE id = ?",
        (photo_id,),
    )
    db.conn.commit()

    # Incremental scan should SKIP this file (exif_data proves ExifTool ran)
    scan(tmp_path, db, incremental=True)

    # Width should still be 999 (not overwritten by re-processing)
    row = db.conn.execute("SELECT width FROM photos WHERE id = ?", (photo_id,)).fetchone()
    assert row["width"] == 999, "Photo with exif_data should not be re-processed"
