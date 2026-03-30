"""Tests for scanner photo_callback support."""

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

    # Second scan: incremental with callback — photos already exist, should still fire
    callbacks = []
    scan(tmp_path, db, incremental=True, photo_callback=lambda pid, path: callbacks.append((pid, path)))

    assert len(callbacks) == 2
    for pid, path in callbacks:
        assert isinstance(pid, int)
        assert path.endswith(".jpg")
