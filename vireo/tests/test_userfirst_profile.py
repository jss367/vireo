"""Safety-invariant tests for the user-first testing profile guard.

The guard must refuse to start the harness against real data:
  - VIREO_PROFILE env var must be set
  - profile cannot be under ~/.vireo/
  - profile cannot be $HOME
  - if VIREO_TEST_PHOTOS is set, no DB folder may live outside it
"""
import sqlite3
from pathlib import Path

import pytest
from testing.userfirst.profile import (
    UnsafeProfileError,
    profile_paths,
    resolve_photos_root,
    resolve_profile,
    validate_db_folders,
)


def test_resolve_profile_requires_env(monkeypatch):
    monkeypatch.delenv("VIREO_PROFILE", raising=False)
    with pytest.raises(UnsafeProfileError, match="VIREO_PROFILE"):
        resolve_profile()


def test_resolve_profile_rejects_real_vireo_dir(monkeypatch):
    real_vireo = Path.home() / ".vireo" / "fake-test-subdir"
    monkeypatch.setenv("VIREO_PROFILE", str(real_vireo))
    with pytest.raises(UnsafeProfileError, match=r"\.vireo"):
        resolve_profile()


def test_resolve_profile_rejects_home(monkeypatch):
    monkeypatch.setenv("VIREO_PROFILE", str(Path.home()))
    with pytest.raises(UnsafeProfileError, match="HOME"):
        resolve_profile()


def test_resolve_profile_accepts_tmp(monkeypatch, tmp_path):
    profile = tmp_path / "test-profile"
    monkeypatch.setenv("VIREO_PROFILE", str(profile))
    result = resolve_profile()
    assert result == profile.resolve()


def test_resolve_photos_root_returns_none_when_unset(monkeypatch):
    monkeypatch.delenv("VIREO_TEST_PHOTOS", raising=False)
    assert resolve_photos_root() is None


def test_resolve_photos_root_rejects_real_vireo_dir(monkeypatch):
    monkeypatch.setenv("VIREO_TEST_PHOTOS", str(Path.home() / ".vireo" / "photos"))
    with pytest.raises(UnsafeProfileError, match=r"\.vireo"):
        resolve_photos_root()


def test_validate_db_folders_rejects_outside_root(tmp_path):
    photos_root = tmp_path / "test-photos"
    photos_root.mkdir()

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE folders (path TEXT)")
    conn.execute("INSERT INTO folders VALUES (?)", ("/Users/julius/real-photos",))
    conn.commit()
    conn.close()

    with pytest.raises(UnsafeProfileError, match="outside photos root"):
        validate_db_folders(db_path, photos_root)


def test_validate_db_folders_accepts_inside_root(tmp_path):
    photos_root = tmp_path / "test-photos"
    photos_root.mkdir()
    (photos_root / "2024").mkdir()

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE folders (path TEXT)")
    conn.execute("INSERT INTO folders VALUES (?)", (str(photos_root / "2024"),))
    conn.commit()
    conn.close()

    validate_db_folders(db_path, photos_root)


def test_validate_db_folders_noop_when_root_is_none(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE folders (path TEXT)")
    conn.execute("INSERT INTO folders VALUES (?)", ("/Users/julius/real-photos",))
    conn.commit()
    conn.close()

    validate_db_folders(db_path, None)


def test_profile_paths_structure(tmp_path):
    paths = profile_paths(tmp_path)
    assert paths["db"] == tmp_path / "vireo.db"
    assert paths["thumbnails"] == tmp_path / "thumbnails"
    assert paths["labels"] == tmp_path / "labels"
    assert paths["config"] == tmp_path / "config.json"
    assert paths["runs"] == tmp_path / "runs"
