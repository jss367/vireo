"""Tests for photo move operations."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from db import Database


@pytest.fixture
def move_env(tmp_path):
    """Set up a DB with two folders and photos on disk."""
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src = tmp_path / "src"
    src.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()

    fid_src = db.add_folder(str(src), name="src")
    fid_dst = db.add_folder(str(dst), name="dst")

    # Create real files
    (src / "bird1.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 100)
    (src / "bird1.xmp").write_text("<xmp/>")
    (src / "bird2.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 200)

    p1 = db.add_photo(folder_id=fid_src, filename="bird1.jpg", extension=".jpg",
                       file_size=102, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid_src, filename="bird2.jpg", extension=".jpg",
                       file_size=202, file_mtime=2.0)

    return {
        "db": db, "tmp_path": tmp_path,
        "src": src, "dst": dst,
        "fid_src": fid_src, "fid_dst": fid_dst,
        "p1": p1, "p2": p2,
    }


def test_move_photos_copies_and_deletes(move_env):
    """move_photos copies files to destination and removes originals."""
    from move import move_photos

    env = move_env
    result = move_photos(
        db=env["db"],
        photo_ids=[env["p1"], env["p2"]],
        destination=str(env["dst"]),
    )
    assert result["moved"] == 2
    assert result["errors"] == []
    # Files exist at destination
    assert (env["dst"] / "bird1.jpg").exists()
    assert (env["dst"] / "bird2.jpg").exists()
    # XMP sidecar also moved
    assert (env["dst"] / "bird1.xmp").exists()
    # Originals removed
    assert not (env["src"] / "bird1.jpg").exists()
    assert not (env["src"] / "bird2.jpg").exists()
    assert not (env["src"] / "bird1.xmp").exists()


def test_move_photos_updates_db(move_env):
    """move_photos updates folder_id in the database."""
    from move import move_photos

    env = move_env
    move_photos(db=env["db"], photo_ids=[env["p1"]], destination=str(env["dst"]))
    photo = env["db"].get_photo(env["p1"])
    assert photo["folder_id"] == env["fid_dst"]


def test_move_photos_collision_skips(move_env):
    """move_photos reports collision and skips conflicting files."""
    from move import move_photos

    env = move_env
    # Pre-place a file at destination
    (env["dst"] / "bird1.jpg").write_bytes(b"existing")

    result = move_photos(db=env["db"], photo_ids=[env["p1"]], destination=str(env["dst"]))
    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "bird1.jpg" in result["errors"][0]
    # Original still exists
    assert (env["src"] / "bird1.jpg").exists()


def test_move_photos_creates_dest_folder_record(move_env):
    """move_photos creates a new folder DB record if destination folder is new."""
    from move import move_photos

    env = move_env
    new_dst = env["tmp_path"] / "new_dest"
    new_dst.mkdir()

    move_photos(db=env["db"], photo_ids=[env["p1"]], destination=str(new_dst))
    row = env["db"].conn.execute("SELECT id FROM folders WHERE path = ?", (str(new_dst),)).fetchone()
    assert row is not None


def test_move_photos_companion_files(move_env):
    """move_photos moves companion (RAW) files alongside the photo."""
    from move import move_photos

    env = move_env
    # Set companion_path on p1
    env["db"].conn.execute("UPDATE photos SET companion_path = 'bird1.nef' WHERE id = ?", (env["p1"],))
    env["db"].conn.commit()
    (env["src"] / "bird1.nef").write_bytes(b"\x00" * 50)

    move_photos(db=env["db"], photo_ids=[env["p1"]], destination=str(env["dst"]))
    assert (env["dst"] / "bird1.nef").exists()
    assert not (env["src"] / "bird1.nef").exists()


def test_move_folder_copies_tree(move_env):
    """move_folder moves entire folder tree preserving structure."""
    from move import move_folder

    env = move_env
    # Create subfolder with file
    sub = env["src"] / "sub"
    sub.mkdir()
    (sub / "nest.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 50)
    fid_sub = env["db"].add_folder(str(sub), name="sub", parent_id=env["fid_src"])
    env["db"].add_photo(folder_id=fid_sub, filename="nest.jpg", extension=".jpg",
                        file_size=52, file_mtime=3.0)

    result = move_folder(db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]))
    assert result["moved"] >= 2  # at least bird1 + bird2 from src
    # Subfolder structure preserved
    assert (env["dst"] / "src" / "sub" / "nest.jpg").exists()
    # DB paths updated
    folder = env["db"].conn.execute("SELECT path FROM folders WHERE id = ?", (env["fid_src"],)).fetchone()
    assert folder["path"] == str(env["dst"] / "src")


def test_move_folder_refuses_existing_dest_without_merge(move_env):
    """move_folder refuses an existing destination and preserves originals."""
    from move import move_folder

    env = move_env
    # Pre-create the resolved landing path (dst/src)
    landing = env["dst"] / "src"
    landing.mkdir()

    result = move_folder(db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]))
    assert result["moved"] == 0
    assert result.get("needs_merge") is True
    assert any("already exists" in e for e in result["errors"])
    # Originals untouched
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()


def test_move_folder_merge_resumes(move_env):
    """merge=True copies only missing files into an existing destination,
    verifies, then removes originals and updates the DB path."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    # Simulate a partially-completed prior move: bird1 already copied.
    (landing / "bird1.jpg").write_bytes((env["src"] / "bird1.jpg").read_bytes())
    # And a leftover rsync temp file from the interrupted run.
    (landing / ".bird2.jpg.AbCdEf").write_bytes(b"partial")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["errors"] == []
    # Missing file got copied; both now present at destination
    assert (landing / "bird1.jpg").exists()
    assert (landing / "bird2.jpg").exists()
    # Originals removed
    assert not env["src"].exists()
    # DB path updated to the merged location
    folder = env["db"].conn.execute(
        "SELECT path FROM folders WHERE id = ?", (env["fid_src"],)
    ).fetchone()
    assert folder["path"] == str(landing)


def test_move_folder_merge_verify_fail_preserves_originals_and_dest(move_env, monkeypatch):
    """If a source file is missing at the destination after copy, the merge
    aborts without deleting originals or the pre-existing destination."""
    import move as move_mod

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    sentinel = landing / "user_file.txt"
    sentinel.write_text("pre-existing")

    # Force the copy step to be a no-op so a source file is "missing" at dest.
    monkeypatch.setattr(move_mod.subprocess, "run",
                        lambda *a, **k: type("R", (), {"returncode": 0, "stderr": ""})())

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Verification failed" in e for e in result["errors"])
    # Originals preserved, pre-existing destination file untouched
    assert (env["src"] / "bird1.jpg").exists()
    assert sentinel.exists()


def test_resolve_folder_dest():
    """resolve_folder_dest places the folder inside the destination."""
    from move import resolve_folder_dest

    assert resolve_folder_dest("/a/birds", "birds", "/nas/photos") == "/nas/photos/birds"
    # Falls back to basename when name is empty
    assert resolve_folder_dest("/a/birds/", "", "/nas/photos") == "/nas/photos/birds"


def test_move_folder_updates_counts(move_env):
    """move_folder updates photo_count on folders."""
    from move import move_folder

    env = move_env
    move_folder(db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]))
    env["db"].update_folder_counts()
    row = env["db"].conn.execute(
        "SELECT photo_count FROM folders WHERE path = ?",
        (str(env["dst"] / "src"),),
    ).fetchone()
    assert row["photo_count"] == 2


def test_move_photos_progress_callback(move_env):
    """move_photos calls progress callback with current/total."""
    from move import move_photos

    env = move_env
    calls = []
    move_photos(
        db=env["db"],
        photo_ids=[env["p1"], env["p2"]],
        destination=str(env["dst"]),
        progress_cb=lambda cur, tot, fn: calls.append((cur, tot, fn)),
    )
    assert len(calls) == 2
    assert calls[0][0] == 1  # current=1
    assert calls[1][0] == 2  # current=2
    assert calls[0][1] == 2  # total=2
