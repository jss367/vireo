"""Tests for photo move operations."""

import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from db import Database


def _tmp_folds_case():
    """True if the filesystem holding pytest's temp dir folds case (macOS
    APFS default, Windows NTFS). Some tests below build scenarios where two
    case variants of an ancestor path must resolve to DISTINCT on-disk
    directories — only possible on a case-sensitive parent FS. On a folding
    host the two variants are the same directory and the assertion under
    test can't hold; skip rather than miscompare.
    """
    with tempfile.TemporaryDirectory(suffix="A") as d:
        flipped = d[:-1] + "a"
        if flipped == d:
            return False
        try:
            return os.path.samefile(d, flipped)
        except OSError:
            return False


_TMP_FOLDS_CASE = _tmp_folds_case()


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


def test_move_folder_by_date_splits_photos_and_moves_sidecars(tmp_path):
    """A date template can fan one source folder into multiple destinations."""
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    src.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    fid = db.add_folder(str(src), name="card")

    (src / "first.jpg").write_bytes(b"first")
    (src / "first.xmp").write_text("<xmp/>")
    (src / "second.jpg").write_bytes(b"second")
    p1 = db.add_photo(
        folder_id=fid, filename="first.jpg", extension=".jpg",
        file_size=5, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    p2 = db.add_photo(
        folder_id=fid, filename="second.jpg", extension=".jpg",
        file_size=6, file_mtime=2.0, timestamp="2026-07-13T10:15:00",
    )

    result = move_folder_by_date(
        db, fid, str(archive), "%Y-%m-%d",
    )

    assert result["moved"] == 2
    assert result["errors"] == []
    assert result["destination_count"] == 2
    assert (archive / "2026-07-12" / "first.jpg").read_bytes() == b"first"
    assert (archive / "2026-07-12" / "first.xmp").exists()
    assert (archive / "2026-07-13" / "second.jpg").read_bytes() == b"second"
    assert not (src / "first.jpg").exists()
    assert not (src / "second.jpg").exists()
    rows = db.conn.execute(
        """SELECT p.id, f.path FROM photos p
           JOIN folders f ON f.id = p.folder_id
           WHERE p.id IN (?, ?)""",
        (p1, p2),
    ).fetchall()
    assert {row["path"] for row in rows} == {
        str(archive / "2026-07-12"),
        str(archive / "2026-07-13"),
    }


def test_move_folder_by_date_copies_shared_xmp_to_each_date(tmp_path):
    """Same-stem photos split by date each retain their shared XMP."""
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    src.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    fid = db.add_folder(str(src), name="card")

    (src / "IMG.CR3").write_bytes(b"raw")
    (src / "IMG.JPG").write_bytes(b"jpeg")
    (src / "IMG.xmp").write_bytes(b"shared-xmp")
    db.add_photo(
        folder_id=fid, filename="IMG.CR3", extension=".cr3",
        file_size=3, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    db.add_photo(
        folder_id=fid, filename="IMG.JPG", extension=".jpg",
        file_size=4, file_mtime=2.0, timestamp="2026-07-13T10:15:00",
    )

    result = move_folder_by_date(db, fid, str(archive), "%Y-%m-%d")

    assert result["moved"] == 2
    assert result["errors"] == []
    assert (
        archive / "2026-07-12" / "IMG.xmp"
    ).read_bytes() == b"shared-xmp"
    assert (
        archive / "2026-07-13" / "IMG.xmp"
    ).read_bytes() == b"shared-xmp"
    assert not (src / "IMG.xmp").exists()


def test_move_photos_reuses_shared_xmp_within_one_destination(tmp_path):
    """Same-batch siblings reuse the first verified XMP copy."""
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    src.mkdir()
    destination = tmp_path / "archive"
    fid = db.add_folder(str(src), name="card")
    (src / "IMG.CR3").write_bytes(b"raw")
    (src / "IMG.JPG").write_bytes(b"jpeg")
    (src / "IMG.xmp").write_bytes(b"shared-xmp")
    raw_pid = db.add_photo(
        folder_id=fid, filename="IMG.CR3", extension=".cr3",
        file_size=3, file_mtime=1.0,
    )
    jpeg_pid = db.add_photo(
        folder_id=fid, filename="IMG.JPG", extension=".jpg",
        file_size=4, file_mtime=2.0,
    )

    result = move_photos(db, [raw_pid, jpeg_pid], str(destination))

    assert result["moved"] == 2
    assert result["errors"] == []
    assert (destination / "IMG.xmp").read_bytes() == b"shared-xmp"
    assert not (src / "IMG.xmp").exists()


def test_move_folder_by_date_rebases_developed_outputs(tmp_path):
    """Regression: when photos in one folder fan out to per-date destinations,
    each photo's developed-output file must move from the OLD folder-key
    subdir to the NEW folder-key subdir. Without this rebase, previously
    developed renders stay under the old key while the catalog points each
    photo at its date folder — export/full-resolution lookups then miss the
    render and fall back to RAW.
    """
    from export import developed_folder_key
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    src.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    fid = db.add_folder(str(src), name="card")

    (src / "first.jpg").write_bytes(b"first")
    (src / "second.jpg").write_bytes(b"second")
    db.add_photo(
        folder_id=fid, filename="first.jpg", extension=".jpg",
        file_size=5, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    db.add_photo(
        folder_id=fid, filename="second.jpg", extension=".jpg",
        file_size=6, file_mtime=2.0, timestamp="2026-07-13T10:15:00",
    )

    developed = tmp_path / "developed"
    developed.mkdir()
    old_key = developed_folder_key(str(src))
    (developed / old_key).mkdir()
    (developed / old_key / "first.jpg").write_bytes(b"first-dev")
    (developed / old_key / "second.tiff").write_bytes(b"second-dev")

    result = move_folder_by_date(
        db, fid, str(archive), "%Y-%m-%d",
        developed_dir=str(developed),
    )
    assert result["errors"] == []
    assert result["moved"] == 2

    # Each photo's developed render moved to its new date-folder key.
    new_first_key = developed_folder_key(str(archive / "2026-07-12"))
    new_second_key = developed_folder_key(str(archive / "2026-07-13"))
    assert (developed / new_first_key / "first.jpg").read_bytes() == b"first-dev"
    assert (developed / new_second_key / "second.tiff").read_bytes() == b"second-dev"
    # Old key's subdir is emptied and cleaned up.
    assert not (developed / old_key).exists()


def test_move_folder_by_date_without_developed_dir_leaves_disk_alone(tmp_path):
    """When ``darktable_output_dir`` is unset (or the caller doesn't pass
    ``developed_dir``), the date move must not touch any external dir —
    same as before the rebase was added.
    """
    from export import developed_folder_key
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    src.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    fid = db.add_folder(str(src), name="card")
    (src / "first.jpg").write_bytes(b"first")
    db.add_photo(
        folder_id=fid, filename="first.jpg", extension=".jpg",
        file_size=5, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )

    developed = tmp_path / "developed"
    developed.mkdir()
    old_key = developed_folder_key(str(src))
    (developed / old_key).mkdir()
    (developed / old_key / "first.jpg").write_bytes(b"first-dev")

    move_folder_by_date(db, fid, str(archive), "%Y-%m-%d")

    # No developed_dir passed → the old key's subdir stays untouched.
    assert (developed / old_key / "first.jpg").read_bytes() == b"first-dev"


def test_move_photos_relocates_developed_output_per_photo(move_env):
    """``move_photos`` relocates each moved photo's developed render to the
    destination folder's key when ``developed_dir`` is supplied — the same
    helper that ``move_folder_by_date`` relies on for date-fanned moves.
    """
    from export import developed_folder_key
    from move import move_photos

    env = move_env
    developed = env["tmp_path"] / "developed"
    developed.mkdir()
    old_key = developed_folder_key(str(env["src"]))
    (developed / old_key).mkdir()
    (developed / old_key / "bird1.jpg").write_bytes(b"bird1-dev")
    # A sibling render stays behind because its photo isn't in this move.
    (developed / old_key / "bird2.jpg").write_bytes(b"bird2-dev")

    result = move_photos(
        db=env["db"],
        photo_ids=[env["p1"]],
        destination=str(env["dst"]),
        developed_dir=str(developed),
    )
    assert result["errors"] == []

    new_key = developed_folder_key(str(env["dst"]))
    assert (developed / new_key / "bird1.jpg").read_bytes() == b"bird1-dev"
    # bird2's render is left alone under the old key — its photo didn't move.
    assert (developed / old_key / "bird2.jpg").read_bytes() == b"bird2-dev"


def test_move_photos_rejects_stale_developed_render_at_destination(move_env):
    """A pre-existing render at the destination key blocks the move.

    ``_iter_developed_outputs`` resolves developed renders by destination
    folder + stem alone. An untracked ``<new_key>/bird1.jpg`` left behind
    from a previously deleted photo, a manually placed file, or a partial
    copy from an aborted earlier move would silently be served as the
    moved photo's render if we repointed the row against it. Treat that
    as a move collision instead so the source photo and its render stay
    intact for manual reconciliation.
    """
    from export import developed_folder_key
    from move import move_photos

    env = move_env
    developed = env["tmp_path"] / "developed"
    developed.mkdir()
    old_key = developed_folder_key(str(env["src"]))
    new_key = developed_folder_key(str(env["dst"]))
    (developed / old_key).mkdir()
    (developed / new_key).mkdir()
    (developed / old_key / "bird1.jpg").write_bytes(b"src-dev")
    (developed / new_key / "bird1.jpg").write_bytes(b"dst-dev")

    result = move_photos(
        db=env["db"],
        photo_ids=[env["p1"]],
        destination=str(env["dst"]),
        developed_dir=str(developed),
    )

    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render" in result["errors"][0]
    # Both developed files remain exactly where they were.
    assert (developed / new_key / "bird1.jpg").read_bytes() == b"dst-dev"
    assert (developed / old_key / "bird1.jpg").read_bytes() == b"src-dev"
    # Source photo file and catalog row untouched.
    assert (env["src"] / "bird1.jpg").exists()
    assert not (env["dst"] / "bird1.jpg").exists()
    assert env["db"].get_photo(env["p1"])["folder_id"] == env["fid_src"]


def test_move_photos_rejects_stale_default_developed_render_at_destination(
    tmp_path,
):
    """When ``darktable_output_dir`` is unset the develop job writes to
    ``<folder>/developed/``. A leftover file there for the same stem
    would be silently picked up as the moved photo's render after the
    row is repointed, so the pre-move guard must cover this layout too.
    """
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()
    fid_src = db.add_folder(str(src), name="src")
    db.add_folder(str(dst), name="dst")

    (src / "bird.cr3").write_bytes(b"raw")
    pid = db.add_photo(
        folder_id=fid_src, filename="bird.cr3", extension=".cr3",
        file_size=3, file_mtime=1.0,
    )

    stale_dev_dir = dst / "developed"
    stale_dev_dir.mkdir()
    (stale_dev_dir / "bird.jpg").write_bytes(b"stale-dev")

    result = move_photos(
        db=db,
        photo_ids=[pid],
        destination=str(dst),
    )

    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render" in result["errors"][0]
    assert (src / "bird.cr3").exists()
    assert not (dst / "bird.cr3").exists()
    assert (stale_dev_dir / "bird.jpg").read_bytes() == b"stale-dev"
    assert db.get_photo(pid)["folder_id"] == fid_src


def test_move_photos_rejects_case_folded_stale_developed_render(
    tmp_path, monkeypatch,
):
    """A case-only stale render blocks moves on case-folding volumes."""
    import move as move_module
    from move import move_photos

    monkeypatch.setattr(
        move_module, "_is_case_insensitive_path", lambda _: True,
    )

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()
    fid_src = db.add_folder(str(src), name="src")
    db.add_folder(str(dst), name="dst")

    incoming = src / "img.cr3"
    incoming.write_bytes(b"raw")
    pid = db.add_photo(
        folder_id=fid_src, filename=incoming.name, extension=".cr3",
        file_size=3, file_mtime=1.0,
    )
    stale_dev_dir = dst / "developed"
    stale_dev_dir.mkdir()
    (stale_dev_dir / "IMG.jpg").write_bytes(b"stale-dev")

    result = move_photos(db, [pid], str(dst))

    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render" in result["errors"][0]
    assert incoming.exists()
    assert not (dst / incoming.name).exists()
    assert db.get_photo(pid)["folder_id"] == fid_src


def test_move_photos_out_of_developed_folder_ignores_source_original(
    tmp_path,
):
    """A source folder named developed is not a stale-render directory."""
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    destination = tmp_path / "archive"
    source = destination / "developed"
    source.mkdir(parents=True)
    destination_id = db.add_folder(str(destination), name="archive")
    source_id = db.add_folder(
        str(source), name="developed", parent_id=destination_id,
    )
    source_file = source / "bird.jpg"
    source_file.write_bytes(b"original")
    pid = db.add_photo(
        folder_id=source_id, filename=source_file.name, extension=".jpg",
        file_size=source_file.stat().st_size, file_mtime=1.0,
    )

    result = move_photos(db, [pid], str(destination))

    assert result["moved"] == 1
    assert result["errors"] == []
    assert (destination / "bird.jpg").read_bytes() == b"original"
    assert not source_file.exists()
    assert db.get_photo(pid)["folder_id"] == destination_id


def test_move_photos_stale_check_allows_same_source_shared_stem(tmp_path):
    """A same-source RAW+JPEG pair legitimately shares its render. When
    the earlier sibling has already relocated the render, the later
    sibling must not be tripped up by the stale-render guard -- the
    catalog origin proves the file belongs to their shared stem, not a
    stray leftover.
    """
    from export import developed_folder_key
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()
    fid_src = db.add_folder(str(src), name="src")
    db.add_folder(str(dst), name="dst")

    (src / "IMG.raw").write_bytes(b"raw")
    (src / "IMG.jpg").write_bytes(b"jpeg")
    pid_raw = db.add_photo(
        folder_id=fid_src, filename="IMG.raw", extension=".raw",
        file_size=3, file_mtime=1.0,
    )
    pid_jpg = db.add_photo(
        folder_id=fid_src, filename="IMG.jpg", extension=".jpg",
        file_size=4, file_mtime=2.0,
    )

    developed = tmp_path / "developed"
    developed.mkdir()
    old_key = developed_folder_key(str(src))
    (developed / old_key).mkdir()
    (developed / old_key / "IMG.jpg").write_bytes(b"shared-dev")

    result = move_photos(
        db=db,
        photo_ids=[pid_raw, pid_jpg],
        destination=str(dst),
        developed_dir=str(developed),
    )

    assert result["errors"] == []
    assert result["moved"] == 2
    new_key = developed_folder_key(str(dst))
    assert (developed / new_key / "IMG.jpg").read_bytes() == b"shared-dev"


def test_move_photos_keeps_render_for_unselected_same_stem_photo(tmp_path):
    """A same-stem photo left in the source must retain its shared render."""
    from export import developed_folder_key
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_folder(str(dst), name="dst")

    (src / "IMG.raw").write_bytes(b"raw")
    (src / "IMG.jpg").write_bytes(b"jpeg")
    moved_pid = db.add_photo(
        folder_id=fid, filename="IMG.raw", extension=".raw",
        file_size=3, file_mtime=1.0,
    )
    db.add_photo(
        folder_id=fid, filename="IMG.jpg", extension=".jpg",
        file_size=4, file_mtime=2.0,
    )
    source_developed = src / "developed"
    source_developed.mkdir()
    (source_developed / "IMG.tiff").write_bytes(b"shared-render")
    configured = tmp_path / "configured-developed"
    configured.mkdir()
    old_key = developed_folder_key(str(src))
    (configured / old_key).mkdir()
    (configured / old_key / "IMG.jpg").write_bytes(b"configured-render")

    result = move_photos(
        db, [moved_pid], str(dst), developed_dir=str(configured),
    )

    assert result["errors"] == []
    assert (source_developed / "IMG.tiff").read_bytes() == b"shared-render"
    assert (dst / "developed" / "IMG.tiff").read_bytes() == b"shared-render"
    new_key = developed_folder_key(str(dst))
    assert (configured / old_key / "IMG.jpg").read_bytes() \
        == b"configured-render"
    assert (configured / new_key / "IMG.jpg").read_bytes() \
        == b"configured-render"


def test_move_folder_by_date_rebases_default_developed_renders(tmp_path):
    """Regression: when ``darktable_output_dir`` is unset, the develop job
    writes to ``<folder>/developed/<stem>.<ext>`` — the export/full-
    resolution lookup's default probe location. Per-photo date-organized
    moves must rebase those renders too, or the catalog silently points
    at a destination folder whose ``developed/`` subdir is empty and the
    app falls back to the RAW/original.
    """
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    src.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    fid = db.add_folder(str(src), name="card")

    (src / "first.jpg").write_bytes(b"first")
    (src / "second.jpg").write_bytes(b"second")
    db.add_photo(
        folder_id=fid, filename="first.jpg", extension=".jpg",
        file_size=5, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    db.add_photo(
        folder_id=fid, filename="second.jpg", extension=".jpg",
        file_size=6, file_mtime=2.0, timestamp="2026-07-13T10:15:00",
    )

    default_developed = src / "developed"
    default_developed.mkdir()
    (default_developed / "first.jpg").write_bytes(b"first-dev")
    (default_developed / "second.tiff").write_bytes(b"second-dev")

    result = move_folder_by_date(db, fid, str(archive), "%Y-%m-%d")
    assert result["errors"] == []
    assert result["moved"] == 2

    # Each photo's default-location developed render moved alongside its
    # photo to the destination date folder's ``developed/`` subdir.
    assert (archive / "2026-07-12" / "developed" / "first.jpg").read_bytes() \
        == b"first-dev"
    assert (archive / "2026-07-13" / "developed" / "second.tiff").read_bytes() \
        == b"second-dev"
    # Source ``developed/`` is cleaned up once emptied.
    assert not default_developed.exists()


def test_move_folder_by_date_preserves_tracked_developed_child(tmp_path):
    """A tracked child named ``developed`` contains originals, not renders."""
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    tracked_child = src / "developed"
    tracked_child.mkdir(parents=True)
    archive = tmp_path / "archive"
    archive.mkdir()
    fid = db.add_folder(str(src), name="card")
    child_fid = db.add_folder(
        str(tracked_child), name="developed", parent_id=fid,
    )

    # Matching stems reproduce the bug: moving the parent's bird first used
    # to mistake the child's tracked original for a generated render.
    (src / "bird.jpg").write_bytes(b"parent")
    (tracked_child / "bird.jpg").write_bytes(b"child")
    db.add_photo(
        folder_id=fid, filename="bird.jpg", extension=".jpg",
        file_size=6, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    child_pid = db.add_photo(
        folder_id=child_fid, filename="bird.jpg", extension=".jpg",
        file_size=5, file_mtime=2.0, timestamp="2026-07-13T10:15:00",
    )

    result = move_folder_by_date(db, fid, str(archive), "%Y-%m-%d")

    assert result["errors"] == []
    assert result["moved"] == 2
    assert (archive / "2026-07-12" / "bird.jpg").read_bytes() == b"parent"
    assert (archive / "2026-07-13" / "bird.jpg").read_bytes() == b"child"
    child_row = db.conn.execute(
        """SELECT f.path FROM photos p
           JOIN folders f ON f.id = p.folder_id
           WHERE p.id = ?""",
        (child_pid,),
    ).fetchone()
    assert child_row["path"] == str(archive / "2026-07-13")


def test_move_folder_by_date_copies_shared_stem_render_to_each_date(tmp_path):
    """Same-stem photo rows fanned to different dates share one render."""
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    src.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    fid = db.add_folder(str(src), name="card")

    (src / "IMG.raw").write_bytes(b"raw")
    (src / "IMG.jpg").write_bytes(b"jpeg")
    db.add_photo(
        folder_id=fid, filename="IMG.raw", extension=".raw",
        file_size=3, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    db.add_photo(
        folder_id=fid, filename="IMG.jpg", extension=".jpg",
        file_size=4, file_mtime=2.0, timestamp="2026-07-13T10:15:00",
    )
    default_developed = src / "developed"
    default_developed.mkdir()
    (default_developed / "IMG.tiff").write_bytes(b"shared-render")

    result = move_folder_by_date(db, fid, str(archive), "%Y-%m-%d")

    assert result["errors"] == []
    assert result["moved"] == 2
    for date in ("2026-07-12", "2026-07-13"):
        assert (
            archive / date / "developed" / "IMG.tiff"
        ).read_bytes() == b"shared-render"
    assert not default_developed.exists()


def test_move_folder_by_date_copies_shared_stem_render_configured_dir(
    tmp_path,
):
    """Same shared-stem fan-out regression, configured-``darktable_output_dir``
    variant. Sibling of
    ``test_move_folder_by_date_copies_shared_stem_render_to_each_date`` —
    that one only exercises the default ``<folder>/developed/`` layout via
    ``relocate_default_developed_file``. This one exercises the flat-key
    layout via ``relocate_developed_file``, which routes through the same
    ``_relocate_stem_files`` helper but from a different call site with
    different old/new subdirs. Without the shared-move record in the
    listing cache, the second destination's folder key would be empty
    even though the render existed under the source key.
    """
    from export import developed_folder_key
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    src.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    fid = db.add_folder(str(src), name="card")

    (src / "IMG.CR3").write_bytes(b"raw")
    (src / "IMG.JPG").write_bytes(b"jpg")
    db.add_photo(
        folder_id=fid, filename="IMG.CR3", extension=".cr3",
        file_size=3, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    db.add_photo(
        folder_id=fid, filename="IMG.JPG", extension=".jpg",
        file_size=3, file_mtime=2.0, timestamp="2026-07-13T10:15:00",
    )

    developed = tmp_path / "developed"
    developed.mkdir()
    old_key = developed_folder_key(str(src))
    (developed / old_key).mkdir()
    (developed / old_key / "IMG.jpg").write_bytes(b"shared-dev")

    result = move_folder_by_date(
        db, fid, str(archive), "%Y-%m-%d",
        developed_dir=str(developed),
    )
    assert result["errors"] == []
    assert result["moved"] == 2

    first_key = developed_folder_key(str(archive / "2026-07-12"))
    second_key = developed_folder_key(str(archive / "2026-07-13"))
    # Both new folder keys carry the render — one arrived via
    # ``shutil.move`` and the other via ``shutil.copy2`` from the first
    # destination — so neither photo silently loses its edit under the
    # flat-key layout.
    assert (developed / first_key / "IMG.jpg").read_bytes() == b"shared-dev"
    assert (developed / second_key / "IMG.jpg").read_bytes() == b"shared-dev"


def test_move_folder_by_date_rejects_distinct_same_stem_renders(tmp_path):
    """Distinct source-folder renders cannot share one destination stem."""
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    parent = tmp_path / "card"
    first_src = parent / "a"
    second_src = parent / "b"
    first_src.mkdir(parents=True)
    second_src.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    parent_fid = db.add_folder(str(parent), name="card")
    first_fid = db.add_folder(str(first_src), name="a", parent_id=parent_fid)
    second_fid = db.add_folder(str(second_src), name="b", parent_id=parent_fid)

    (first_src / "IMG.CR3").write_bytes(b"first-raw")
    (second_src / "IMG.NEF").write_bytes(b"second-raw")
    first_pid = db.add_photo(
        folder_id=first_fid, filename="IMG.CR3", extension=".cr3",
        file_size=9, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    second_pid = db.add_photo(
        folder_id=second_fid, filename="IMG.NEF", extension=".nef",
        file_size=10, file_mtime=2.0, timestamp="2026-07-12T10:15:00",
    )
    (first_src / "developed").mkdir()
    (second_src / "developed").mkdir()
    (first_src / "developed" / "IMG.jpg").write_bytes(b"first-render")
    (second_src / "developed" / "IMG.jpg").write_bytes(b"second-render")

    result = move_folder_by_date(
        db, parent_fid, str(archive), "%Y-%m-%d",
    )

    assert result["moved"] == 1
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    destination = archive / "2026-07-12"
    assert (destination / "IMG.CR3").read_bytes() == b"first-raw"
    assert (destination / "developed" / "IMG.jpg").read_bytes() \
        == b"first-render"
    assert (second_src / "IMG.NEF").read_bytes() == b"second-raw"
    assert (second_src / "developed" / "IMG.jpg").read_bytes() \
        == b"second-render"
    assert db.get_photo(first_pid)["folder_id"] != first_fid
    assert db.get_photo(second_pid)["folder_id"] == second_fid


def test_move_folder_by_date_lists_developed_dir_once_per_source(
    tmp_path, monkeypatch,
):
    """Regression: fanning N developed photos through per-date destinations
    must not rescan the source developed subdir once per photo. Prior to
    the shared listing cache, ``move_folder_by_date`` would call the
    per-photo relocate helper N times and each call would re-list the
    same directory — quadratic on large libraries.
    """
    import export as export_mod
    from export import developed_folder_key
    from move import move_folder_by_date

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "card"
    src.mkdir()
    archive = tmp_path / "archive"
    archive.mkdir()
    fid = db.add_folder(str(src), name="card")

    # Three distinct capture-date destinations so the group loop runs
    # three times, each of which used to list the source developed subdir
    # independently.
    photos = [
        ("first.jpg", "2026-07-12T09:30:00"),
        ("second.jpg", "2026-07-13T10:15:00"),
        ("third.jpg", "2026-07-14T08:00:00"),
    ]
    for name, ts in photos:
        (src / name).write_bytes(b"x")
        db.add_photo(
            folder_id=fid, filename=name, extension=".jpg",
            file_size=1, file_mtime=1.0, timestamp=ts,
        )

    developed = tmp_path / "developed"
    developed.mkdir()
    old_key = developed_folder_key(str(src))
    (developed / old_key).mkdir()
    for name, _ in photos:
        (developed / old_key / name).write_bytes(b"dev")

    target = str(developed / old_key)
    listdir_calls = []
    real_listdir = os.listdir

    def counting_listdir(path):
        if str(path) == target:
            listdir_calls.append(str(path))
        return real_listdir(path)

    monkeypatch.setattr(export_mod.os, "listdir", counting_listdir)

    result = move_folder_by_date(
        db, fid, str(archive), "%Y-%m-%d",
        developed_dir=str(developed),
    )
    assert result["errors"] == []
    assert result["moved"] == 3
    # With the shared listing cache the initial scan of the old-key subdir
    # runs once for the whole run instead of once per photo — so we allow
    # at most 1 initial listdir + one post-rename cleanup listdir per
    # relocated photo (3 here). Without the cache we would see 3 initial
    # scans + 3 cleanup scans = 6.
    assert len(listdir_calls) <= 4, (
        f"expected at most 4 listdirs of the source developed subdir, "
        f"got {len(listdir_calls)}"
    )


def test_plan_folder_date_moves_uses_unsorted_without_a_usable_time(tmp_path):
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    (src / "unknown.jpg").write_bytes(b"x")
    db.add_photo(
        folder_id=fid, filename="unknown.jpg", extension=".jpg",
        file_size=1, file_mtime=None,
    )

    plan = plan_folder_date_moves(db, fid, str(tmp_path / "archive"), "%Y-%m-%d")

    assert len(plan) == 1
    assert plan[0]["relative_path"] == "unsorted"


def test_plan_folder_date_moves_normalizes_dot_components(tmp_path):
    """Rendered dot components never leak into catalog destination paths."""
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    archive = tmp_path / "archive"

    plan = plan_folder_date_moves(
        db, fid, str(archive), "./%Y-%m-%d",
    )

    assert plan[0]["relative_path"] == "2026-07-12"
    assert plan[0]["destination"] == str(archive / "2026-07-12")


def test_plan_folder_date_moves_rejects_dot_only_template(tmp_path):
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )

    with pytest.raises(ValueError, match="empty path"):
        plan_folder_date_moves(db, fid, str(tmp_path / "archive"), ".")


def test_plan_folder_date_moves_rejects_template_that_escapes_destination(tmp_path):
    """Defense-in-depth: even when a caller reaches the underlying planner
    without the API-layer template validator, a rendered path that
    resolves outside ``destination`` must be rejected rather than
    silently placing a group above the chosen root.

    ``build_destination_path`` already rejects a template with a literal
    ``..`` segment (``ingest._is_unsafe_path``); this test locks in the
    belt-and-suspenders resolved-path check that catches anything that
    slips past the string-level guard.
    """
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    (src / "photo.jpg").write_bytes(b"x")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )

    with pytest.raises(ValueError):
        plan_folder_date_moves(
            db, fid, str(tmp_path / "archive"), "%Y/../%m",
        )


def test_plan_folder_date_moves_rejects_destination_occupied_by_file(tmp_path):
    """A rendered date path must be a directory or available to create."""
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    archive = tmp_path / "archive"
    archive.mkdir()
    (archive / "2026-07-12").write_bytes(b"occupied")

    with pytest.raises(ValueError, match="not a directory"):
        plan_folder_date_moves(db, fid, str(archive), "%Y-%m-%d")


def test_plan_folder_date_moves_rejects_destination_root_regular_file(tmp_path):
    """The destination root itself must be validated as a directory.

    The ancestor walk only checks paths *inside* ``destination`` (``depth``
    starts at 1), so a destination path that is itself a regular file (or
    dangling symlink) would slip past preflight — none of ``destination/2026``,
    ``destination/2026/07`` lexist yet — and ``os.makedirs`` in the worker
    would then raise ``NotADirectoryError``/``FileExistsError`` inside the
    background job. Preflight must reject the destination root itself.
    """
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    # ``archive`` is a regular file, not a directory. Any nested template
    # rendered under it would end up trying to ``makedirs`` a child of a
    # regular file.
    archive = tmp_path / "archive"
    archive.write_bytes(b"blocked")

    with pytest.raises(ValueError, match="not a directory"):
        plan_folder_date_moves(db, fid, str(archive), "%Y/%m")


def test_plan_folder_date_moves_rejects_parent_of_missing_destination_root(tmp_path):
    """When the requested destination root does not exist yet and one of its
    parents is already a regular file, preflight must reject the plan.

    The destination check at the top of ``plan_folder_date_moves`` only fires
    when ``destination`` itself lexists, and the intermediate-ancestor walk
    only visits paths *inside* ``destination``. Without walking
    ``destination``'s own parents, a request like
    ``destination='/archive/root'`` where ``/archive`` is a plain file would
    pass preflight, and ``move_photos`` would then call
    ``os.makedirs('/archive/root/2026/07', exist_ok=True)`` and raise
    ``NotADirectoryError`` inside the background job — losing the structured
    date-destination error this guard is meant to provide.
    """
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    # ``archive`` is a regular file — a nested destination root under it
    # ('/archive/root') doesn't lexist, but the parent is blocking.
    archive = tmp_path / "archive"
    archive.write_bytes(b"blocked")
    blocked_destination = archive / "root"

    with pytest.raises(ValueError, match="not a directory"):
        plan_folder_date_moves(db, fid, str(blocked_destination), "%Y/%m")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Broken symlinks are unusual on Windows and require admin.",
)
def test_plan_folder_date_moves_rejects_dangling_symlink_parent_of_root(tmp_path):
    """A dangling-symlink parent of a not-yet-existing destination root must
    fail preflight for the same reason a plain-file parent does.
    """
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    archive_parent = tmp_path / "archive"
    os.symlink(str(tmp_path / "disconnected-volume"), str(archive_parent))
    blocked_destination = archive_parent / "root"

    with pytest.raises(ValueError, match="not a directory"):
        plan_folder_date_moves(db, fid, str(blocked_destination), "%Y/%m")


def test_plan_folder_date_moves_rejects_ancestor_occupied_by_file(tmp_path):
    """A nested template must reject a plan whose intermediate ancestor is a
    regular file.

    For template ``%Y/%m`` rendering ``2026/07``, the leaf-only check misses
    a case where ``<destination>/2026`` is already a regular file — the leaf
    ``<destination>/2026/07`` does not lexist yet. ``move_photos`` would then
    call ``os.makedirs(<destination>/2026/07, exist_ok=True)`` and raise
    ``NotADirectoryError`` inside the background job. Preflight must walk
    each ancestor and surface a structured error instead.
    """
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    archive = tmp_path / "archive"
    archive.mkdir()
    # ``<archive>/2026`` is a regular file, blocking any ``2026/*`` child.
    (archive / "2026").write_bytes(b"blocked")

    with pytest.raises(ValueError, match="not a directory"):
        plan_folder_date_moves(db, fid, str(archive), "%Y/%m")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Broken symlinks are unusual on Windows and require admin.",
)
def test_plan_folder_date_moves_rejects_dangling_symlink_ancestor(tmp_path):
    """A dangling-symlink intermediate ancestor must fail preflight.

    For template ``%Y/%m`` rendering ``2026/07`` where ``<destination>/2026``
    is a dangling symlink, the leaf check accepts the plan because
    ``<destination>/2026/07`` does not lexist. ``os.makedirs(..., exist_ok=True)``
    would then raise ``FileExistsError``/``NotADirectoryError`` inside the
    worker — this is especially plausible for mounted-drive symlinks whose
    target is disconnected.
    """
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    archive = tmp_path / "archive"
    archive.mkdir()
    # Point the symlink at a sibling *inside* the destination root so the
    # traversal safety check doesn't fire first — the intermediate ancestor
    # is the scenario we're guarding against.
    os.symlink(str(archive / "target"), str(archive / "2026"))

    with pytest.raises(ValueError, match="not a directory"):
        plan_folder_date_moves(db, fid, str(archive), "%Y/%m")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Broken symlinks are unusual on Windows and require admin.",
)
def test_plan_folder_date_moves_rejects_dangling_symlink_destination(tmp_path):
    """A rendered date destination that is a dangling symlink must be
    rejected in preflight.

    ``os.path.exists`` follows the link and reports False for a dangling
    symlink, so the older check accepted it and the background job later
    crashed with ``FileExistsError`` inside ``move_photos``' ``makedirs``.
    Use ``os.path.lexists`` so preflight fails cleanly instead.
    """
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    archive = tmp_path / "archive"
    archive.mkdir()
    # Point the symlink at a would-be sibling *inside* the destination
    # root so the traversal safety check doesn't fire first — the
    # scenario we're guarding against is a preflight-accepted dangling
    # link that then trips makedirs's FileExistsError in the worker.
    os.symlink(str(archive / "target"), str(archive / "2026-07-12"))

    with pytest.raises(ValueError, match="not a directory"):
        plan_folder_date_moves(db, fid, str(archive), "%Y-%m-%d")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows doesn't allow '\\' in folder names, so folding '\\' to '/' "
    "in the descendants query is the correct Windows behavior; the leak only "
    "affects POSIX where a literal backslash IS a legal filename character.",
)
def test_plan_folder_date_moves_does_not_leak_slash_siblings_on_posix(tmp_path):
    """Regression: a POSIX folder whose name contains '\\' must not swallow
    a sibling '/'-separated subtree.

    ``/photos/a\\b`` and ``/photos/a/b/nested`` are distinct directories on
    POSIX. If the descendants query folds '\\' to '/' before comparing
    prefixes, the ``/photos/a\\b`` prefix becomes ``/photos/a/b/`` and matches
    every row under ``/photos/a/b/…`` — pulling photos that don't belong to
    the selected folder into the plan (and, worse, into the move that
    executes it).
    """
    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    photos_root = tmp_path / "photos"
    photos_root.mkdir()

    backslash_dir = photos_root / "a\\b"
    backslash_dir.mkdir()
    (backslash_dir / "target.jpg").write_bytes(b"target")

    slash_parent = photos_root / "a"
    slash_parent.mkdir()
    (slash_parent / "b").mkdir()
    slash_nested = slash_parent / "b" / "nested"
    slash_nested.mkdir()
    (slash_nested / "sibling.jpg").write_bytes(b"sibling")

    fid_target = db.add_folder(str(backslash_dir), name="a\\b")
    fid_nested = db.add_folder(str(slash_nested), name="nested")
    db.add_photo(
        folder_id=fid_target, filename="target.jpg", extension=".jpg",
        file_size=6, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    db.add_photo(
        folder_id=fid_nested, filename="sibling.jpg", extension=".jpg",
        file_size=7, file_mtime=2.0, timestamp="2026-07-13T10:15:00",
    )

    plan = plan_folder_date_moves(
        db, fid_target, str(tmp_path / "archive"), "%Y-%m-%d",
    )

    # Only the photo actually under /photos/a\b appears in the plan.
    planned_ids = {pid for group in plan for pid in group["photo_ids"]}
    assert len(planned_ids) == 1
    only_id = next(iter(planned_ids))
    only_photo = db.get_photo(only_id)
    assert only_photo["filename"] == "target.jpg"


def test_plan_folder_date_moves_matches_descendant_by_case_on_windows(
    tmp_path, monkeypatch,
):
    """Regression: on Windows, tracked descendants whose stored path differs
    from the selected folder only by case must still be picked up by the
    descendants prefix query. Before the fix the query only normalized
    separators (``REPLACE(f.path, '\\', '/')``), so a folder stored as
    ``C:\\Photos`` with a descendant row ``c:\\photos\\2026`` would drop
    the descendant and leave those photos behind.

    Executes on non-Windows hosts by swapping ``move``'s ``sys`` for a
    shim that reports ``platform == "win32"`` — patching the real
    ``sys.platform`` would leak into stdlib modules that gate
    Windows-only imports (multiprocessing, etc.).
    """
    import types

    import move as move_mod
    from move import plan_folder_date_moves

    shim = types.SimpleNamespace(platform="win32")
    monkeypatch.setattr(move_mod, "sys", shim)

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    # Rows are stored with mixed case; the actual on-disk paths are used
    # only to satisfy add_folder's isdir check.
    parent_disk = tmp_path / "photos"
    parent_disk.mkdir()
    child_disk = parent_disk / "2026"
    child_disk.mkdir()
    alias_disk = tmp_path / "alias"
    alias_disk.mkdir()
    (parent_disk / "root.jpg").write_bytes(b"root")
    (child_disk / "child.jpg").write_bytes(b"child")
    (alias_disk / "alias.jpg").write_bytes(b"alias")

    # Insert the folder rows directly so we can control the exact case that
    # gets stored — add_folder normalizes through the filesystem.
    parent_fid = db.add_folder(str(parent_disk), name="photos")
    child_fid = db.add_folder(str(child_disk), name="2026")
    alias_fid = db.add_folder(str(alias_disk), name="alias")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        ("C:\\Photos", parent_fid),
    )
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        ("c:\\photos\\2026", child_fid),
    )
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        ("c:\\PHOTOS", alias_fid),
    )
    db.conn.commit()

    db.add_photo(
        folder_id=parent_fid, filename="root.jpg", extension=".jpg",
        file_size=4, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    db.add_photo(
        folder_id=child_fid, filename="child.jpg", extension=".jpg",
        file_size=5, file_mtime=2.0, timestamp="2026-07-13T09:30:00",
    )
    db.add_photo(
        folder_id=alias_fid, filename="alias.jpg", extension=".jpg",
        file_size=5, file_mtime=3.0, timestamp="2026-07-14T09:30:00",
    )

    # The destination itself is host-OS absolute so os.path.isabs and the
    # traversal check pass under the test runner; only the descendants
    # query needs the Windows-style stored rows to trigger the case-fold
    # branch.
    plan = plan_folder_date_moves(
        db, parent_fid, str(tmp_path / "archive"), "%Y-%m-%d",
    )

    planned_ids = {pid for group in plan for pid in group["photo_ids"]}
    assert len(planned_ids) == 3, (
        "differently-cased descendant and exact alias should be included"
    )


def test_plan_folder_date_moves_matches_case_alias_on_casefolding_posix(
    tmp_path, monkeypatch,
):
    """Case-only aliases on a case-insensitive POSIX volume are descendants."""
    import move as move_mod

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    parent_disk = tmp_path / "Photos"
    child_disk = parent_disk / "2026"
    child_disk.mkdir(parents=True)
    parent_fid = db.add_folder(str(parent_disk), name="Photos")
    child_fid = db.add_folder(str(child_disk), name="2026")
    parent_path = str(parent_disk)
    child_alias = str(tmp_path / "photos" / "2026")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (child_alias, child_fid),
    )
    db.conn.commit()
    child_pid = db.add_photo(
        folder_id=child_fid, filename="child.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )
    # Simulate a case-insensitive POSIX filesystem while retaining the
    # host's POSIX path implementation. Root-scoped fallback in
    # _path_equal_or_descends then folds the case-only alias safely.
    monkeypatch.setattr(move_mod, "_case_insensitive_root", lambda path: "/")

    plan = move_mod.plan_folder_date_moves(
        db, parent_fid, str(tmp_path / "archive"), "%Y-%m-%d",
    )

    assert parent_path != child_alias
    assert [pid for item in plan for pid in item["photo_ids"]] == [child_pid]


def test_plan_folder_date_moves_matches_symlink_alias_descendants_on_posix(
    tmp_path,
):
    """A tracked child stored through a symlink target must still be planned
    as a descendant of the selected folder.

    Regression: on case-sensitive POSIX the descendants query used a raw
    ``substr(f.path, ...)`` prefix, which is false when the selected folder is
    stored as ``/photos/card`` (a symlink to ``/mnt/card``) but the child row
    is stored as ``/mnt/card/day``. Those photos were silently dropped from
    the date-move plan and left behind on disk.
    """
    if sys.platform == "win32":
        pytest.skip("POSIX-only symlink semantics")

    from move import plan_folder_date_moves

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    target_root = tmp_path / "mnt" / "card"
    target_root.mkdir(parents=True)
    (target_root / "day").mkdir()

    alias_root = tmp_path / "photos" / "card"
    alias_root.parent.mkdir(parents=True)
    try:
        os.symlink(target_root, alias_root, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("filesystem does not support symlinks")

    parent_fid = db.add_folder(str(alias_root), name="card")
    child_fid = db.add_folder(str(target_root / "day"), name="day")
    child_pid = db.add_photo(
        folder_id=child_fid, filename="child.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2026-07-12T09:30:00",
    )

    plan = plan_folder_date_moves(
        db, parent_fid, str(tmp_path / "archive"), "%Y-%m-%d",
    )

    assert [pid for item in plan for pid in item["photo_ids"]] == [child_pid]


def test_move_photos_allows_incremental_same_stem_siblings(tmp_path):
    """A RAW/JPEG pair may be moved to one destination in separate calls."""
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    destination = tmp_path / "destination"
    raw = src / "IMG.CR3"
    jpeg = src / "IMG.JPG"
    raw.write_bytes(b"raw")
    jpeg.write_bytes(b"jpeg")
    raw_pid = db.add_photo(
        folder_id=db.add_folder(str(src), name="src"),
        filename=raw.name, extension=".cr3", file_size=3, file_mtime=1.0,
    )
    source_fid = db.get_photo(raw_pid)["folder_id"]
    jpeg_pid = db.add_photo(
        folder_id=source_fid, filename=jpeg.name, extension=".jpg",
        file_size=4, file_mtime=2.0,
    )
    developed = src / "developed"
    developed.mkdir()
    (developed / "IMG.jpg").write_bytes(b"render")

    first = move_photos(db, [raw_pid], str(destination))
    second = move_photos(db, [jpeg_pid], str(destination))

    assert first["moved"] == 1
    assert first["errors"] == []
    assert second["moved"] == 1
    assert second["errors"] == []
    assert (destination / raw.name).read_bytes() == b"raw"
    assert (destination / jpeg.name).read_bytes() == b"jpeg"
    assert not raw.exists()
    assert not jpeg.exists()


def test_move_photos_rejects_unknown_destination_same_stem_origin(tmp_path):
    """A pre-existing same-stem photo is not assumed to be a moved sibling."""
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    source_fid = db.add_folder(str(source), name="source")
    destination_fid = db.add_folder(str(destination), name="destination")
    incoming = source / "IMG.CR3"
    existing = destination / "IMG.JPG"
    incoming.write_bytes(b"unrelated-raw")
    existing.write_bytes(b"existing-jpeg")
    incoming_pid = db.add_photo(
        folder_id=source_fid, filename=incoming.name, extension=".cr3",
        file_size=incoming.stat().st_size, file_mtime=1.0,
    )
    db.add_photo(
        folder_id=destination_fid, filename=existing.name, extension=".jpg",
        file_size=existing.stat().st_size, file_mtime=2.0,
    )

    result = move_photos(db, [incoming_pid], str(destination))

    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert incoming.read_bytes() == b"unrelated-raw"
    assert existing.read_bytes() == b"existing-jpeg"
    assert db.get_photo(incoming_pid)["folder_id"] == source_fid


def test_move_photos_provenance_survives_folder_id_reuse(tmp_path):
    """SQLite ``INTEGER PRIMARY KEY`` without AUTOINCREMENT reuses freed
    rowids after ``delete_folder``. Storing the source folder's *id* on
    ``last_move_source_folder_*`` would let a brand-new unrelated folder
    that lands on the same rowid compare equal to a stale reference and
    bypass the same-stem developed-render collision guard. The check must
    use a non-reusable identifier (the source folder's path).
    """
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    original_source = tmp_path / "original"
    destination = tmp_path / "destination"
    original_source.mkdir()
    original_fid = db.add_folder(str(original_source), name="original")
    original_file = original_source / "IMG.JPG"
    original_file.write_bytes(b"original-jpeg")
    original_pid = db.add_photo(
        folder_id=original_fid, filename=original_file.name,
        extension=".jpg", file_size=original_file.stat().st_size,
        file_mtime=1.0,
    )
    db.add_workspace_folder(ws_id, original_fid)

    first = move_photos(db, [original_pid], str(destination))
    assert first["moved"] == 1
    assert first["errors"] == []
    dest_fid = db.get_photo(original_pid)["folder_id"]
    assert dest_fid != original_fid

    # Drop the original source folder. SQLite is now free to reuse its
    # rowid on the next folders INSERT.
    db.delete_folder(original_fid)

    # Force a rowid collision by inserting a synthetic folder row at the
    # freed id. This mirrors what SQLite would do on the next real
    # add_folder if that id happens to be at max(id)+1.
    replacement_source = tmp_path / "replacement"
    replacement_source.mkdir()
    db.conn.execute(
        "INSERT INTO folders (id, path, name) VALUES (?, ?, ?)",
        (original_fid, str(replacement_source), "replacement"),
    )
    db.conn.commit()
    db.add_workspace_folder(ws_id, original_fid)

    incoming_file = replacement_source / "IMG.CR3"
    incoming_file.write_bytes(b"replacement-raw")
    incoming_pid = db.add_photo(
        folder_id=original_fid, filename=incoming_file.name,
        extension=".cr3", file_size=incoming_file.stat().st_size,
        file_mtime=2.0,
    )

    result = move_photos(db, [incoming_pid], str(destination))

    # The replacement folder shares its rowid with the deleted original,
    # but its path is different, so the collision guard must reject the
    # move rather than treat it as a same-source sibling.
    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert incoming_file.read_bytes() == b"replacement-raw"
    assert db.get_photo(incoming_pid)["folder_id"] == original_fid


def test_move_photos_provenance_cleared_when_source_folder_deleted(tmp_path):
    """When a source folder is deleted after some of its photos were moved
    out, any destination-photo rows that still reference the deleted path
    via ``last_move_source_folder_path`` must have that provenance cleared.
    Otherwise a new unrelated folder later appearing at the same path — for
    example a removable card re-mounted at ``/Volumes/CARD`` after the old
    scan was dropped — would compare equal to the stale reference and slip
    a same-stem developed-render collision past the guard in
    ``move_photos``.
    """
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    reusable_path = tmp_path / "CARD"
    destination = tmp_path / "archive"
    reusable_path.mkdir()
    original_fid = db.add_folder(str(reusable_path), name="CARD")
    original_file = reusable_path / "IMG.JPG"
    original_file.write_bytes(b"original-jpeg")
    original_pid = db.add_photo(
        folder_id=original_fid, filename=original_file.name,
        extension=".jpg", file_size=original_file.stat().st_size,
        file_mtime=1.0,
    )
    # Add a same-stem sibling that stays behind so the source stem is not
    # drained by the move below — otherwise ``move_photos`` correctly
    # expires the moved row's provenance immediately (covered by
    # ``test_move_photos_provenance_cleared_when_source_stem_drained``)
    # and there is nothing left for ``delete_folder``'s cascade to clear.
    sibling_file = reusable_path / "IMG.NEF"
    sibling_file.write_bytes(b"sibling-raw")
    db.add_photo(
        folder_id=original_fid, filename=sibling_file.name,
        extension=".nef", file_size=sibling_file.stat().st_size,
        file_mtime=1.0,
    )
    db.add_workspace_folder(ws_id, original_fid)

    first = move_photos(db, [original_pid], str(destination))
    assert first["moved"] == 1
    assert first["errors"] == []
    # The destination row records the source path as provenance while a
    # same-stem sibling remains behind.
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (original_pid,),
        ).fetchone()["last_move_source_folder_path"] == str(reusable_path)
    )

    # Drop the original source folder — the on-disk path is still there
    # (imagine the card being re-inserted with different content later),
    # but the earlier scan is gone from the catalog.
    db.delete_folder(original_fid)

    # Provenance must be cleared so a new folder at the reused path can't
    # inherit the earlier claim.
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (original_pid,),
        ).fetchone()["last_move_source_folder_path"] is None
    )

    # A brand-new folder appears at the same path with an unrelated photo
    # that happens to share the stem.
    replacement_fid = db.add_folder(str(reusable_path), name="CARD")
    db.add_workspace_folder(ws_id, replacement_fid)
    incoming_file = reusable_path / "IMG.CR3"
    incoming_file.write_bytes(b"replacement-raw")
    incoming_pid = db.add_photo(
        folder_id=replacement_fid, filename=incoming_file.name,
        extension=".cr3", file_size=incoming_file.stat().st_size,
        file_mtime=2.0,
    )

    result = move_photos(db, [incoming_pid], str(destination))

    # The new folder shares the deleted folder's path, but that stale
    # provenance was invalidated on delete, so the collision guard sees an
    # unknown origin for the existing stem and rejects rather than merging
    # unrelated content into one destination folder+stem.
    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert incoming_file.read_bytes() == b"replacement-raw"
    assert db.get_photo(incoming_pid)["folder_id"] == replacement_fid


def test_move_photos_provenance_cleared_when_missing_folder_merges_into_existing(tmp_path):
    """When a missing source folder is relocated onto a path already tracked
    by another folder, ``_merge_into_existing`` deletes the source folder
    row and frees its old path. Any destination photo still carrying that
    old path in ``last_move_source_folder_path`` must have that provenance
    cleared. Otherwise, a new unrelated folder scanned at that freed path
    later would compare equal to the stale reference in ``move_photos`` and
    slip a same-stem developed-render collision past the guard — the same
    class of bug fixed for ``delete_folder``, applied to the merge path.
    """
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    reusable_path = tmp_path / "CARD"
    destination = tmp_path / "archive"
    reusable_path.mkdir()
    original_fid = db.add_folder(str(reusable_path), name="CARD")
    original_file = reusable_path / "IMG.JPG"
    original_file.write_bytes(b"original-jpeg")
    original_pid = db.add_photo(
        folder_id=original_fid, filename=original_file.name,
        extension=".jpg", file_size=original_file.stat().st_size,
        file_mtime=1.0,
    )
    # Same-stem sibling that stays behind so the source stem is not drained
    # by the move — otherwise ``move_photos`` would clear provenance
    # immediately (covered by the drain test) and the merge cleanup under
    # test would have nothing to clear.
    sibling_file = reusable_path / "IMG.NEF"
    sibling_file.write_bytes(b"sibling-raw")
    db.add_photo(
        folder_id=original_fid, filename=sibling_file.name,
        extension=".nef", file_size=sibling_file.stat().st_size,
        file_mtime=1.0,
    )
    db.add_workspace_folder(ws_id, original_fid)

    first = move_photos(db, [original_pid], str(destination))
    assert first["moved"] == 1
    assert first["errors"] == []
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (original_pid,),
        ).fetchone()["last_move_source_folder_path"] == str(reusable_path)
    )

    # Simulate the missing-folder-merge flow: the original card folder goes
    # missing, then the user relocates that missing entry onto a path that
    # another folder already tracks (e.g. a replacement archive folder).
    target_path = tmp_path / "REPLACEMENT"
    target_path.mkdir()
    target_fid = db.add_folder(str(target_path), name="REPLACEMENT")
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?",
        (original_fid,),
    )
    db.conn.commit()
    # Delete the on-disk source directory so relocate_folder's
    # revalidation doesn't refresh the source instead of merging.
    sibling_file.unlink()
    reusable_path.rmdir()

    db.relocate_folder(original_fid, str(target_path))

    # The original source folder row must be gone (merged into target).
    assert db.conn.execute(
        "SELECT id FROM folders WHERE id = ?", (original_fid,)
    ).fetchone() is None
    # Provenance must be cleared so a new folder at the freed path can't
    # inherit the stale claim.
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (original_pid,),
        ).fetchone()["last_move_source_folder_path"] is None
    )

    # A brand-new unrelated folder now appears at the freed path with a
    # same-stem photo but a different extension.
    reusable_path.mkdir()
    replacement_fid = db.add_folder(str(reusable_path), name="CARD")
    db.add_workspace_folder(ws_id, replacement_fid)
    incoming_file = reusable_path / "IMG.CR3"
    incoming_file.write_bytes(b"replacement-raw")
    incoming_pid = db.add_photo(
        folder_id=replacement_fid, filename=incoming_file.name,
        extension=".cr3", file_size=incoming_file.stat().st_size,
        file_mtime=2.0,
    )

    result = move_photos(db, [incoming_pid], str(destination))

    # The collision guard now sees an unknown origin for the existing stem
    # (provenance was cleared by the merge) and rejects rather than
    # merging unrelated content into one destination folder+stem.
    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert incoming_file.read_bytes() == b"replacement-raw"
    assert db.get_photo(incoming_pid)["folder_id"] == replacement_fid


def test_move_provenance_rebased_for_child_cascaded_by_missing_merge(tmp_path):
    """Missing-root merges rebase provenance for surviving child folders."""
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    old_root = tmp_path / "OLD"
    old_child = old_root / "child"
    old_child.mkdir(parents=True)
    new_root = tmp_path / "NEW"
    new_child = new_root / "child"
    new_child.mkdir(parents=True)
    destination = tmp_path / "archive"

    old_root_id = db.add_folder(str(old_root), name="OLD")
    child_id = db.add_folder(
        str(old_child), name="child", parent_id=old_root_id,
    )
    new_root_id = db.add_folder(str(new_root), name="NEW")

    moved_file = old_child / "IMG.JPG"
    sibling_file = old_child / "IMG.CR3"
    moved_file.write_bytes(b"jpeg")
    sibling_file.write_bytes(b"raw")
    moved_pid = db.add_photo(
        folder_id=child_id, filename=moved_file.name, extension=".jpg",
        file_size=4, file_mtime=1.0,
    )
    db.add_photo(
        folder_id=child_id, filename=sibling_file.name, extension=".cr3",
        file_size=3, file_mtime=2.0,
    )

    first = move_photos(db, [moved_pid], str(destination))
    assert first["moved"] == 1
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (moved_pid,),
        ).fetchone()["last_move_source_folder_path"] == str(old_child)
    )

    # The old root is now missing. Relocating it onto an already-tracked
    # root merges the root row while cascading the surviving child row onto
    # the real child directory beneath the target.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id IN (?, ?)",
        (old_root_id, child_id),
    )
    db.conn.commit()
    sibling_file.unlink()
    old_child.rmdir()
    old_root.rmdir()

    cascaded = db.relocate_folder(old_root_id, str(new_root))

    assert cascaded == [{
        "id": child_id,
        "old_path": str(old_child),
        "new_path": str(new_child),
    }]
    assert db.conn.execute(
        "SELECT path, parent_id FROM folders WHERE id = ?", (child_id,),
    ).fetchone()["parent_id"] == new_root_id
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (moved_pid,),
        ).fetchone()["last_move_source_folder_path"] == str(new_child)
    )

    # Reuse the freed old child path with unrelated same-stem content. The
    # rebased provenance must make the destination collision guard reject it.
    old_child.mkdir(parents=True)
    replacement_id = db.add_folder(str(old_child), name="child")
    incoming = old_child / "IMG.NEF"
    incoming.write_bytes(b"replacement")
    incoming_pid = db.add_photo(
        folder_id=replacement_id, filename=incoming.name, extension=".nef",
        file_size=incoming.stat().st_size, file_mtime=3.0,
    )

    result = move_photos(db, [incoming_pid], str(destination))

    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert incoming.exists()
    assert db.get_photo(incoming_pid)["folder_id"] == replacement_id


def test_move_photos_provenance_rebased_when_source_folder_moved(tmp_path):
    """Renaming/moving a source folder via ``move_folder_path`` (e.g. the
    whole-folder move flow) frees the old path for reuse. Without cascading
    the rename into ``photos.last_move_source_folder_path``, a new unrelated
    folder later scanned at the freed path would compare equal to the stale
    stored origin and slip past the same-stem developed-render collision
    guard in ``move_photos``, letting two unrelated destination rows share
    the developed-output lookup by folder+stem. The cascade must rebase the
    stored provenance so it still points at the renamed folder — preserving
    the shared-render relationship with siblings that stay behind — and
    stops matching against any future occupant of the freed path.
    """
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    reusable_path = tmp_path / "CARD"
    destination = tmp_path / "archive"
    reusable_path.mkdir()
    original_fid = db.add_folder(str(reusable_path), name="CARD")
    original_file = reusable_path / "IMG.JPG"
    original_file.write_bytes(b"original-jpeg")
    original_pid = db.add_photo(
        folder_id=original_fid, filename=original_file.name,
        extension=".jpg", file_size=original_file.stat().st_size,
        file_mtime=1.0,
    )
    # Same-stem sibling that stays behind — see analogous note in the
    # source-folder-deleted test above; without it, ``move_photos``
    # correctly expires provenance immediately and the cascade under test
    # has nothing to rebase.
    sibling_file = reusable_path / "IMG.NEF"
    sibling_file.write_bytes(b"sibling-raw")
    db.add_photo(
        folder_id=original_fid, filename=sibling_file.name,
        extension=".nef", file_size=sibling_file.stat().st_size,
        file_mtime=1.0,
    )
    db.add_workspace_folder(ws_id, original_fid)

    first = move_photos(db, [original_pid], str(destination))
    assert first["moved"] == 1
    assert first["errors"] == []
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (original_pid,),
        ).fetchone()["last_move_source_folder_path"] == str(reusable_path)
    )

    # Rename the original source folder in the catalog (mirrors what the
    # whole-folder move flow does before the on-disk directory is moved
    # out). The old path is now free for reuse.
    renamed_path = tmp_path / "CARD.bak"
    reusable_path.rename(renamed_path)
    db.move_folder_path(original_fid, str(renamed_path), new_name="CARD.bak")

    # The provenance must have been rebased to the new path, not left
    # pointing at the freed one.
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (original_pid,),
        ).fetchone()["last_move_source_folder_path"] == str(renamed_path)
    )

    # A brand-new folder appears at the freed path with an unrelated photo
    # that happens to share the stem.
    reusable_path.mkdir()
    replacement_fid = db.add_folder(str(reusable_path), name="CARD")
    db.add_workspace_folder(ws_id, replacement_fid)
    incoming_file = reusable_path / "IMG.CR3"
    incoming_file.write_bytes(b"replacement-raw")
    incoming_pid = db.add_photo(
        folder_id=replacement_fid, filename=incoming_file.name,
        extension=".cr3", file_size=incoming_file.stat().st_size,
        file_mtime=2.0,
    )

    result = move_photos(db, [incoming_pid], str(destination))

    # The new folder shares the freed path, but the provenance was rebased
    # onto the renamed folder, so the collision guard rejects rather than
    # merging two unrelated content trees onto one destination folder+stem.
    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert incoming_file.read_bytes() == b"replacement-raw"
    assert db.get_photo(incoming_pid)["folder_id"] == replacement_fid


def test_move_photos_provenance_cleared_when_source_stem_drained(tmp_path):
    """When the last same-stem sibling is moved out of a source folder, any
    destination rows still carrying that source path in
    ``last_move_source_folder_path`` must have the provenance expired.
    Otherwise a later rescan/import of an unrelated ``IMG.*`` into the same
    source path could match ``existing_origin == src_dir`` in the collision
    guard and slip into the destination, where developed-output lookup is
    keyed by destination folder + stem only and the incoming photo would
    display/export the old row's developed render.
    """
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    source_path = tmp_path / "CARD"
    destination = tmp_path / "archive"
    source_path.mkdir()
    src_fid = db.add_folder(str(source_path), name="CARD")
    src_file = source_path / "IMG.JPG"
    src_file.write_bytes(b"original-jpeg")
    src_pid = db.add_photo(
        folder_id=src_fid, filename=src_file.name,
        extension=".jpg", file_size=src_file.stat().st_size,
        file_mtime=1.0,
    )
    db.add_workspace_folder(ws_id, src_fid)

    first = move_photos(db, [src_pid], str(destination))
    assert first["moved"] == 1
    assert first["errors"] == []

    # The source folder is now drained of same-stem photos, so the
    # destination row must not still claim that source as its provenance
    # (otherwise a rescan into the same path could inherit its render).
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (src_pid,),
        ).fetchone()["last_move_source_folder_path"] is None
    )

    # A brand-new unrelated photo lands in the same source folder path
    # (imagine a card wiped and re-populated). Its stem happens to match
    # the earlier row, but they don't share a developed render.
    incoming_file = source_path / "IMG.CR3"
    incoming_file.write_bytes(b"replacement-raw")
    incoming_pid = db.add_photo(
        folder_id=src_fid, filename=incoming_file.name,
        extension=".cr3", file_size=incoming_file.stat().st_size,
        file_mtime=2.0,
    )

    result = move_photos(db, [incoming_pid], str(destination))

    # The collision guard now sees an unknown origin for the existing stem
    # (provenance was expired when the source drained) and rejects rather
    # than merging unrelated rows into one destination folder+stem.
    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert incoming_file.read_bytes() == b"replacement-raw"
    assert db.get_photo(incoming_pid)["folder_id"] == src_fid


def test_delete_photos_expires_provenance_when_source_stem_drains(tmp_path):
    """Deleting the final source sibling expires moved-row provenance."""
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    source_path = tmp_path / "CARD"
    destination = tmp_path / "archive"
    source_path.mkdir()
    src_fid = db.add_folder(str(source_path), name="CARD")
    db.add_workspace_folder(ws_id, src_fid)

    raw = source_path / "IMG.CR3"
    jpeg = source_path / "IMG.JPG"
    raw.write_bytes(b"raw")
    jpeg.write_bytes(b"jpeg")
    raw_pid = db.add_photo(
        folder_id=src_fid, filename=raw.name, extension=".cr3",
        file_size=3, file_mtime=1.0,
    )
    jpeg_pid = db.add_photo(
        folder_id=src_fid, filename=jpeg.name, extension=".jpg",
        file_size=4, file_mtime=2.0,
    )

    first = move_photos(db, [raw_pid], str(destination))
    assert first["moved"] == 1
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (raw_pid,),
        ).fetchone()["last_move_source_folder_path"] == str(source_path)
    )

    deleted = db.delete_photos([jpeg_pid])
    assert deleted["deleted"] == 1
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (raw_pid,),
        ).fetchone()["last_move_source_folder_path"] is None
    )

    jpeg.unlink()
    incoming = source_path / "IMG.NEF"
    incoming.write_bytes(b"replacement")
    incoming_pid = db.add_photo(
        folder_id=src_fid, filename=incoming.name, extension=".nef",
        file_size=incoming.stat().st_size, file_mtime=3.0,
    )

    result = move_photos(db, [incoming_pid], str(destination))

    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert incoming.exists()
    assert db.get_photo(incoming_pid)["folder_id"] == src_fid


def test_move_photos_provenance_cleared_across_date_fanout(tmp_path):
    """A single source folder can fan same-stem siblings out to multiple
    date-organized destinations. When the last sibling drains the source,
    the provenance on the earlier siblings (moved to a different date
    destination) must also be expired — otherwise a later unrelated
    ``IMG.*`` reappearing at the source path could slip into whichever
    destination still carries the stale claim.
    """
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    source_path = tmp_path / "CARD"
    dest_a = tmp_path / "archive" / "2026-07-19"
    dest_b = tmp_path / "archive" / "2026-07-20"
    source_path.mkdir()
    src_fid = db.add_folder(str(source_path), name="CARD")
    file_a = source_path / "IMG.JPG"
    file_a.write_bytes(b"jpeg-bytes")
    file_b = source_path / "IMG.CR3"
    file_b.write_bytes(b"raw-bytes")
    pid_a = db.add_photo(
        folder_id=src_fid, filename=file_a.name, extension=".jpg",
        file_size=file_a.stat().st_size, file_mtime=1.0,
    )
    pid_b = db.add_photo(
        folder_id=src_fid, filename=file_b.name, extension=".cr3",
        file_size=file_b.stat().st_size, file_mtime=2.0,
    )
    db.add_workspace_folder(ws_id, src_fid)

    # First sibling moves to date A (source still has the other sibling —
    # provenance should be preserved on this call).
    first = move_photos(db, [pid_a], str(dest_a))
    assert first["moved"] == 1
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (pid_a,),
        ).fetchone()["last_move_source_folder_path"] == str(source_path)
    )

    # Second sibling moves to date B and drains the source folder. Both
    # destination rows now need their provenance expired.
    second = move_photos(db, [pid_b], str(dest_b))
    assert second["moved"] == 1
    for pid in (pid_a, pid_b):
        assert (
            db.conn.execute(
                "SELECT last_move_source_folder_path "
                "FROM photos WHERE id = ?",
                (pid,),
            ).fetchone()["last_move_source_folder_path"] is None
        )


def test_move_photos_provenance_rebased_when_missing_folder_relocated(tmp_path):
    """Renaming a missing source folder via ``Database.relocate_folder`` (the
    "remap missing folder to a new location" flow) frees the old path for
    reuse. Without cascading the rebase into
    ``photos.last_move_source_folder_path``, a later unrelated folder scanned
    at the freed path would compare equal to the stale stored origin and
    slip past the same-stem developed-render collision guard in
    ``move_photos``, letting two unrelated destination rows share the
    developed-output lookup by folder+stem. The cascade must rebase the
    stored provenance onto the new path — preserving the shared-render
    relationship with any sibling that stays behind — and stop matching
    against any future occupant of the freed path.
    """
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    reusable_path = tmp_path / "CARD"
    destination = tmp_path / "archive"
    reusable_path.mkdir()
    original_fid = db.add_folder(str(reusable_path), name="CARD")
    original_file = reusable_path / "IMG.JPG"
    original_file.write_bytes(b"original-jpeg")
    original_pid = db.add_photo(
        folder_id=original_fid, filename=original_file.name,
        extension=".jpg", file_size=original_file.stat().st_size,
        file_mtime=1.0,
    )
    # Same-stem sibling that stays behind so the destination row keeps its
    # provenance after the move — otherwise ``move_photos`` correctly
    # expires it immediately and the ``relocate_folder`` cascade has
    # nothing to rebase.
    sibling_file = reusable_path / "IMG.NEF"
    sibling_file.write_bytes(b"sibling-raw")
    db.add_photo(
        folder_id=original_fid, filename=sibling_file.name,
        extension=".nef", file_size=sibling_file.stat().st_size,
        file_mtime=1.0,
    )
    db.add_workspace_folder(ws_id, original_fid)

    first = move_photos(db, [original_pid], str(destination))
    assert first["moved"] == 1
    assert first["errors"] == []
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (original_pid,),
        ).fetchone()["last_move_source_folder_path"] == str(reusable_path)
    )

    # Mark the folder as missing (its on-disk path is gone) and remap it
    # via ``relocate_folder`` to a fresh location. This is the flow used
    # when a user tells the app "this folder now lives here". The old
    # path is now free for reuse.
    relocated_path = tmp_path / "CARD_RELOCATED"
    reusable_path.rename(relocated_path)
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?",
        (original_fid,),
    )
    db.conn.commit()
    db.relocate_folder(original_fid, str(relocated_path))

    # The provenance must have been rebased onto the new path, not left
    # pointing at the freed one.
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (original_pid,),
        ).fetchone()["last_move_source_folder_path"] == str(relocated_path)
    )

    # A brand-new unrelated folder appears at the freed path with an
    # unrelated photo whose stem matches.
    reusable_path.mkdir()
    replacement_fid = db.add_folder(str(reusable_path), name="CARD")
    db.add_workspace_folder(ws_id, replacement_fid)
    incoming_file = reusable_path / "IMG.CR3"
    incoming_file.write_bytes(b"replacement-raw")
    incoming_pid = db.add_photo(
        folder_id=replacement_fid, filename=incoming_file.name,
        extension=".cr3", file_size=incoming_file.stat().st_size,
        file_mtime=2.0,
    )

    result = move_photos(db, [incoming_pid], str(destination))

    # The collision guard rejects the incoming photo because its source
    # path does not match the rebased provenance on the existing
    # destination row.
    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert incoming_file.read_bytes() == b"replacement-raw"
    assert db.get_photo(incoming_pid)["folder_id"] == replacement_fid


def test_move_photos_rejects_case_folded_stem_collision(tmp_path, monkeypatch):
    """On case-insensitive destinations (Windows, default macOS APFS), two
    unrelated source folders can contain ``IMG.CR3`` and ``img.NEF`` and
    both originals land in the same date folder. Their ``*.jpg`` developed
    renders both want a case-only variant of the same filename, so exactly
    one wins; the second photo's row would then serve the first's render.
    The same-stem destination collision guard in ``move_photos`` must fold
    the stem to the destination filesystem's case sensitivity before
    consulting the origin map, so this collision is refused up front.
    """
    import move as move_module
    from move import move_photos

    monkeypatch.setattr(
        move_module, "_is_case_insensitive_path", lambda _: True,
    )

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    source_a = tmp_path / "cardA"
    source_b = tmp_path / "cardB"
    destination = tmp_path / "archive"
    source_a.mkdir()
    source_b.mkdir()

    fid_a = db.add_folder(str(source_a), name="cardA")
    fid_b = db.add_folder(str(source_b), name="cardB")
    db.add_workspace_folder(ws_id, fid_a)
    db.add_workspace_folder(ws_id, fid_b)

    file_a = source_a / "IMG.CR3"
    file_a.write_bytes(b"raw-a")
    pid_a = db.add_photo(
        folder_id=fid_a, filename=file_a.name, extension=".cr3",
        file_size=file_a.stat().st_size, file_mtime=1.0,
    )
    file_b = source_b / "img.NEF"
    file_b.write_bytes(b"raw-b")
    pid_b = db.add_photo(
        folder_id=fid_b, filename=file_b.name, extension=".nef",
        file_size=file_b.stat().st_size, file_mtime=2.0,
    )

    result = move_photos(db, [pid_a, pid_b], str(destination))

    # First photo moves, second is rejected because its case-folded stem
    # collides with the existing destination stem from a different source.
    assert result["moved"] == 1
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    # The winning photo is the one processed first.
    assert db.get_photo(pid_a)["folder_id"] != fid_a
    assert db.get_photo(pid_b)["folder_id"] == fid_b
    assert file_b.read_bytes() == b"raw-b"


def test_move_photos_folds_stems_for_configured_developed_volume(
    tmp_path, monkeypatch,
):
    """A case-folding developed volume governs render stem collisions."""
    import move as move_module
    from move import move_photos

    destination = tmp_path / "case-sensitive-archive"
    developed_dir = tmp_path / "case-folding-developed"
    developed_dir.mkdir()

    def fake_case_probe(path):
        return os.path.normpath(path) == os.path.normpath(developed_dir)

    monkeypatch.setattr(
        move_module, "_is_case_insensitive_path", fake_case_probe,
    )

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    source_a = tmp_path / "cardA"
    source_b = tmp_path / "cardB"
    source_a.mkdir()
    source_b.mkdir()
    fid_a = db.add_folder(str(source_a), name="cardA")
    fid_b = db.add_folder(str(source_b), name="cardB")
    db.add_workspace_folder(ws_id, fid_a)
    db.add_workspace_folder(ws_id, fid_b)

    file_a = source_a / "IMG.CR3"
    file_b = source_b / "img.NEF"
    file_a.write_bytes(b"raw-a")
    file_b.write_bytes(b"raw-b")
    pid_a = db.add_photo(
        folder_id=fid_a, filename=file_a.name, extension=".cr3",
        file_size=file_a.stat().st_size, file_mtime=1.0,
    )
    pid_b = db.add_photo(
        folder_id=fid_b, filename=file_b.name, extension=".nef",
        file_size=file_b.stat().st_size, file_mtime=2.0,
    )

    result = move_photos(
        db, [pid_a, pid_b], str(destination),
        developed_dir=str(developed_dir),
    )

    assert result["moved"] == 1
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert db.get_photo(pid_a)["folder_id"] != fid_a
    assert db.get_photo(pid_b)["folder_id"] == fid_b
    assert file_b.exists()


def test_move_photos_rejects_same_source_case_only_stem_collision(
    tmp_path, monkeypatch,
):
    """Same-source provenance only permits an exact shared render stem."""
    import move as move_module
    from move import move_photos

    monkeypatch.setattr(
        move_module, "_is_case_insensitive_path", lambda _: True,
    )

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    source = tmp_path / "case-sensitive-source"
    destination = tmp_path / "case-folding-destination"
    source.mkdir()
    fid = db.add_folder(str(source), name="source")
    files = {
        "IMG.CR3": b"raw-upper",
        "IMG.JPG": b"jpeg-upper",
        "img.NEF": b"raw-lower",
    }
    pids = {}
    for index, (filename, content) in enumerate(files.items(), start=1):
        path = source / filename
        path.write_bytes(content)
        pids[filename] = db.add_photo(
            folder_id=fid, filename=filename,
            extension=os.path.splitext(filename)[1].lower(),
            file_size=len(content), file_mtime=float(index),
        )

    first = move_photos(db, [pids["IMG.CR3"]], str(destination))
    assert first["moved"] == 1
    # The exact-stem IMG.JPG sibling keeps the source provenance alive.
    assert (
        db.conn.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?",
            (pids["IMG.CR3"],),
        ).fetchone()["last_move_source_folder_path"] == str(source)
    )

    result = move_photos(db, [pids["img.NEF"]], str(destination))

    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert "developed render stem" in result["errors"][0]
    assert (source / "img.NEF").exists()
    assert db.get_photo(pids["img.NEF"])["folder_id"] == fid


def test_move_photos_allows_case_folded_stem_on_case_sensitive_destination(
    tmp_path, monkeypatch,
):
    """The case-folded guard must NOT punish case-sensitive destinations,
    where ``IMG.CR3`` and ``img.NEF`` legitimately coexist as distinct
    files with distinct renders. Both photos should move successfully.
    """
    import move as move_module
    from move import move_photos

    monkeypatch.setattr(
        move_module, "_is_case_insensitive_path", lambda _: False,
    )

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    source_a = tmp_path / "cardA"
    source_b = tmp_path / "cardB"
    destination = tmp_path / "archive"
    source_a.mkdir()
    source_b.mkdir()

    fid_a = db.add_folder(str(source_a), name="cardA")
    fid_b = db.add_folder(str(source_b), name="cardB")
    db.add_workspace_folder(ws_id, fid_a)
    db.add_workspace_folder(ws_id, fid_b)

    file_a = source_a / "IMG.CR3"
    file_a.write_bytes(b"raw-a")
    pid_a = db.add_photo(
        folder_id=fid_a, filename=file_a.name, extension=".cr3",
        file_size=file_a.stat().st_size, file_mtime=1.0,
    )
    file_b = source_b / "img.NEF"
    file_b.write_bytes(b"raw-b")
    pid_b = db.add_photo(
        folder_id=fid_b, filename=file_b.name, extension=".nef",
        file_size=file_b.stat().st_size, file_mtime=2.0,
    )

    result = move_photos(db, [pid_a, pid_b], str(destination))

    assert result["moved"] == 2
    assert result["errors"] == []


def test_move_photos_refuses_destination_that_is_a_file(tmp_path):
    """Regression: ``os.makedirs(destination, exist_ok=True)`` raises
    ``FileExistsError`` when the path exists as a regular file, so a
    date-organized move that lands on such a path would crash the batch
    with an opaque error. ``move_photos`` must detect that up front and
    return a structured error instead.
    """
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    (src / "photo.jpg").write_bytes(b"x")
    pid = db.add_photo(
        folder_id=fid, filename="photo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )

    blocked = tmp_path / "blocked"
    blocked.write_bytes(b"not a directory")

    result = move_photos(
        db=db, photo_ids=[pid], destination=str(blocked),
    )

    assert result["moved"] == 0
    assert result["errors"]
    assert any("not a directory" in err for err in result["errors"])
    # The photo row stays at the original folder and the on-disk source
    # file is untouched.
    assert db.get_photo(pid)["folder_id"] == fid
    assert (src / "photo.jpg").exists()
    assert blocked.read_bytes() == b"not a directory"


@pytest.mark.skipif(sys.platform == "win32", reason="symlink setup is POSIX-only")
def test_move_photos_refuses_broken_symlink_destination(tmp_path):
    """The worker returns structured errors for a dangling destination."""
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "src"
    src.mkdir()
    fid = db.add_folder(str(src), name="src")
    photo = src / "photo.jpg"
    photo.write_bytes(b"x")
    pid = db.add_photo(
        folder_id=fid, filename=photo.name, extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    blocked = tmp_path / "blocked"
    blocked.symlink_to(tmp_path / "disconnected-volume", target_is_directory=True)

    result = move_photos(db, [pid], str(blocked))

    assert result["moved"] == 0
    assert any("not a directory" in error for error in result["errors"])
    assert photo.read_bytes() == b"x"
    assert os.path.lexists(blocked)


def test_relocate_stem_files_cleans_up_partial_copy_on_failure(
    tmp_path, monkeypatch,
):
    """Regression: if ``shutil.copy2`` creates the destination file and
    then fails (full disk, flaky mount), the partial file must be
    removed. Otherwise ``_iter_developed_outputs`` picks up the
    truncated render at the new key and serves it to exports/full-res
    instead of falling back to the intact source or the RAW.
    """
    from export import _relocate_stem_files

    old_dir = tmp_path / "old"
    old_dir.mkdir()
    (old_dir / "img.jpg").write_bytes(b"payload-full-content")
    new_dir = tmp_path / "new"

    def failing_copy2(src, dst, *a, **kw):
        # Simulate a real full-disk / flaky-mount failure: only part of
        # the source bytes make it to the destination before the error.
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, "wb") as fh:
            fh.write(b"partial")
        raise OSError("no space left on device")

    monkeypatch.setattr("export.shutil.copy2", failing_copy2)

    relocated = _relocate_stem_files(
        str(old_dir), str(new_dir), "img", listing_cache={},
        preserve_source=True,
    )
    assert relocated == 0
    # The partial destination file must be gone so the fallback path in
    # _iter_developed_outputs doesn't pick it up.
    assert not (new_dir / "img.jpg").exists()
    # The source render stayed in place.
    assert (old_dir / "img.jpg").read_bytes() == b"payload-full-content"


def test_relocate_stem_files_keeps_complete_copy_when_unlink_fails(
    tmp_path, monkeypatch,
):
    """Regression: on cross-filesystem moves, ``shutil.move`` copies the
    file and then unlinks the source. If only the unlink fails (locked
    source, read-only source dir), the destination is already complete
    and must be preserved — otherwise exports miss the render even
    though we did write it successfully.
    """
    from export import _relocate_stem_files

    old_dir = tmp_path / "old"
    old_dir.mkdir()
    source = old_dir / "img.jpg"
    source.write_bytes(b"payload")
    new_dir = tmp_path / "new"

    real_rename = os.rename
    real_unlink = os.unlink

    def cross_device_rename(src, dst):
        if os.fspath(src) == str(source):
            raise OSError("cross-device rename")
        return real_rename(src, dst)

    def locked_source_unlink(path):
        if os.fspath(path) == str(source):
            raise PermissionError("permission denied removing source")
        return real_unlink(path)

    monkeypatch.setattr(os, "rename", cross_device_rename)
    monkeypatch.setattr(os, "unlink", locked_source_unlink)

    listing_cache = {}
    relocated = _relocate_stem_files(
        str(old_dir), str(new_dir), "img", listing_cache=listing_cache,
        preserve_source=False,
    )
    assert relocated == 1
    # The complete destination copy stays in place so
    # _iter_developed_outputs still finds the render at the new key.
    assert (new_dir / "img.jpg").read_bytes() == b"payload"
    assert source.read_bytes() == b"payload"
    # The listing cache records the successful destination so later
    # same-stem calls can reuse it.
    assert any(v == str(new_dir / "img.jpg")
               for v in listing_cache.values())


def test_move_photos_reports_cleanup_error_after_commit(move_env, monkeypatch):
    """A post-commit os.remove failure must not roll back the catalog and
    must not abort remaining photos in the batch.

    Before the fix, os.remove on a locked or read-only source file raised
    out of the loop after the ``UPDATE photos`` commit — the catalog for
    the current photo already pointed at the destination, its developed
    render was still under the old folder key, and every subsequent photo
    in the batch was skipped.
    """
    import move as move_mod

    env = move_env

    original_remove = move_mod.os.remove
    target_src = os.path.normcase(os.path.normpath(str(env["src"] / "bird1.jpg")))

    def failing_remove(path):
        if os.path.normcase(os.path.normpath(path)) == target_src:
            raise OSError("permission denied")
        return original_remove(path)

    monkeypatch.setattr(move_mod.os, "remove", failing_remove)

    result = move_mod.move_photos(
        db=env["db"],
        photo_ids=[env["p1"], env["p2"]],
        destination=str(env["dst"]),
    )

    # The batch completes: bird1 counts as moved (destination has the file
    # and the catalog is repointed) and bird2 is unaffected. bird1's
    # leftover original is reported as a per-photo error.
    assert result["moved"] == 2
    assert any("bird1.jpg" in err for err in result["errors"])
    assert (env["dst"] / "bird1.jpg").exists()
    assert (env["dst"] / "bird2.jpg").exists()
    # Catalog points at the new folder for both.
    p1_row = env["db"].get_photo(env["p1"])
    p2_row = env["db"].get_photo(env["p2"])
    assert p1_row["folder_id"] == env["fid_dst"]
    assert p2_row["folder_id"] == env["fid_dst"]


def test_move_photos_rebases_developed_before_source_cleanup(
    move_env, monkeypatch,
):
    """Regression: even when os.remove of the source fails, the developed
    render must already be relocated to the new folder key so full-res /
    export lookups don't silently fall back to RAW.
    """
    import move as move_mod
    from export import developed_folder_key

    env = move_env

    developed = env["tmp_path"] / "developed"
    developed.mkdir()
    old_key = developed_folder_key(str(env["src"]))
    new_key = developed_folder_key(str(env["dst"]))
    (developed / old_key).mkdir()
    (developed / old_key / "bird1.jpg").write_bytes(b"bird1-dev")

    original_remove = move_mod.os.remove
    target_src = os.path.normcase(os.path.normpath(str(env["src"] / "bird1.jpg")))

    def failing_remove(path):
        if os.path.normcase(os.path.normpath(path)) == target_src:
            raise OSError("permission denied")
        return original_remove(path)

    monkeypatch.setattr(move_mod.os, "remove", failing_remove)

    move_mod.move_photos(
        db=env["db"],
        photo_ids=[env["p1"]],
        destination=str(env["dst"]),
        developed_dir=str(developed),
    )

    # Developed render followed the catalog even though source cleanup
    # blew up mid-loop.
    assert (developed / new_key / "bird1.jpg").read_bytes() == b"bird1-dev"


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


def test_move_photos_dest_folder_nests_under_ancestor(move_env):
    """A new destination under an existing folder gets parent_id set to that
    ancestor so it nests in the browse tree instead of floating as a root."""
    from move import move_photos

    env = move_env
    # Destination is a brand-new subfolder of the existing `dst` folder.
    sub_dst = env["dst"] / "sub"
    sub_dst.mkdir()

    move_photos(db=env["db"], photo_ids=[env["p1"]], destination=str(sub_dst))
    row = env["db"].conn.execute(
        "SELECT parent_id FROM folders WHERE path = ?", (str(sub_dst),)
    ).fetchone()
    assert row["parent_id"] == env["fid_dst"]


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


def test_move_folder_can_rename_during_move(move_env):
    """An explicit destination name moves and renames in one safe operation."""
    from move import move_folder

    env = move_env
    result = move_folder(
        db=env["db"],
        folder_id=env["fid_src"],
        destination=str(env["dst"]),
        destination_name="2026-07-12",
    )

    landing = env["dst"] / "2026-07-12"
    assert result["errors"] == []
    assert (landing / "bird1.jpg").exists()
    assert not env["src"].exists()
    folder = env["db"].conn.execute(
        "SELECT path, name FROM folders WHERE id = ?", (env["fid_src"],)
    ).fetchone()
    assert folder["path"] == str(landing)
    assert folder["name"] == "2026-07-12"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows strips trailing spaces from path components, so a ' shoot ' "
    "directory can't exist on disk — the untrimmed-name invariant is a POSIX concern.",
)
def test_move_folder_no_op_rename_preserves_untrimmed_source_name(tmp_path):
    """A no-op rename lands at the source's raw name — spaces and all.

    When the user leaves the Folder name field unchanged, the UI sends
    ``destination_name=""`` so the backend keeps the source folder name
    verbatim. If move_folder trims that fallback, the copy lands at
    ``/archive/shoot`` while preflight showed ``/archive/ shoot `` — the
    catalog then points at a folder that doesn't match what the user
    approved and could silently merge with a different existing folder.
    """
    from move import move_folder

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src = tmp_path / " shoot "
    src.mkdir()
    (src / "bird.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 100)
    dst = tmp_path / "archive"
    dst.mkdir()

    fid = db.add_folder(str(src), name=" shoot ")
    db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                 file_size=102, file_mtime=1.0)

    result = move_folder(
        db=db,
        folder_id=fid,
        destination=str(dst),
        destination_name="",
    )

    landing = dst / " shoot "
    assert result["errors"] == []
    assert landing.is_dir()
    assert (landing / "bird.jpg").exists()
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (fid,)
    ).fetchone()
    assert folder["path"] == str(landing)


def test_move_folder_no_op_rename_uses_source_leaf_when_name_missing(tmp_path):
    """A nameless folder row whose path ends in '/' must still land at its leaf.

    Legacy/relocated rows can carry an empty ``name`` alongside a ``path``
    stored with a trailing separator. ``os.path.basename("/photos/shoot/")``
    is ``""``, so without stripping the separator the no-op rename fallback
    collapses to ``""`` and the copy lands directly in the selected parent
    (potentially merging with a different folder). Preflight and
    ``resolve_folder_dest`` already ``rstrip("/\\")`` before basename();
    ``move_folder`` must too.
    """
    from move import move_folder

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src = tmp_path / "shoot"
    src.mkdir()
    (src / "bird.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 100)
    dst = tmp_path / "archive"
    dst.mkdir()

    fid = db.add_folder(str(src), name="shoot")
    # Simulate a legacy row: blank name, trailing separator on path.
    db.conn.execute(
        "UPDATE folders SET name = '', path = ? WHERE id = ?",
        (str(src) + "/", fid),
    )
    db.conn.commit()

    db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                 file_size=102, file_mtime=1.0)

    result = move_folder(
        db=db,
        folder_id=fid,
        destination=str(dst),
        destination_name="",
    )

    landing = dst / "shoot"
    assert result["errors"] == []
    assert landing.is_dir()
    assert (landing / "bird.jpg").exists()
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (fid,)
    ).fetchone()
    assert folder["path"] == str(landing)


def test_move_folder_rejects_destination_name_with_path_segments(move_env):
    """The rename field cannot escape the separately selected parent."""
    from move import move_folder

    env = move_env
    result = move_folder(
        db=env["db"],
        folder_id=env["fid_src"],
        destination=str(env["dst"]),
        destination_name="../somewhere-else",
    )

    assert result["moved"] == 0
    assert "without slashes" in result["errors"][0]
    assert env["src"].exists()


def test_move_folder_rejects_drive_qualified_destination_name(move_env):
    """A Windows drive-qualified leaf like C:shoot must not escape the parent.

    os.path.join(r"D:\\archive", "C:shoot") returns the drive-relative path
    "C:shoot" on Windows, so accepting a colon-bearing leaf would drop the
    copy — and repoint catalog_path — outside the selected destination.
    """
    from move import move_folder

    env = move_env
    result = move_folder(
        db=env["db"],
        folder_id=env["fid_src"],
        destination=str(env["dst"]),
        destination_name="C:shoot",
    )

    assert result["moved"] == 0
    assert "colons" in result["errors"][0]
    assert env["src"].exists()


def test_normalize_destination_name_rejects_colon():
    """Drive-qualified and colon-containing leaves are rejected everywhere."""
    import pytest
    from move import normalize_destination_name

    for bad in ("C:shoot", "D:\\archive", "foo:bar", ":", "bird:cage/nest"):
        with pytest.raises(ValueError):
            normalize_destination_name(bad)

    # Valid single-component names still pass through.
    assert normalize_destination_name("2026-07-12") == "2026-07-12"
    assert normalize_destination_name("") == ""
    assert normalize_destination_name(None) == ""


def test_move_folder_reports_cleanup_error_after_commit(move_env, monkeypatch):
    """Catalog repoints first, so a post-commit rmtree failure is committed.

    If rmtree(src_path) raises after move_folder_path has already pointed
    the catalog at the destination, the archive IS published — the files
    exist at the destination and the catalog resolves there. Surfacing
    that as a normal ``errors`` entry would tell the caller the move
    failed (the pipeline reports "results remain in staging") even though
    the data is safely at final_destination and the freshly created
    tracked folder row is in the catalog. Returning ``cleanup_error``
    separately lets callers warn about leftover originals without
    misreporting the move.
    """
    import move as move_mod

    env = move_env

    def raise_on_rmtree(path):
        raise OSError("permission denied")

    monkeypatch.setattr(move_mod.shutil, "rmtree", raise_on_rmtree)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    assert result["errors"] == []
    assert result["moved"] >= 1
    assert result.get("cleanup_error") == "permission denied"
    # Catalog now points at the new destination (commit step ran before
    # rmtree) — verifying we don't roll that back on the cleanup failure.
    folder = env["db"].conn.execute(
        "SELECT path FROM folders WHERE id = ?", (env["fid_src"],)
    ).fetchone()
    assert folder["path"] == str(env["dst"] / "src")


def test_move_folder_no_cleanup_error_on_clean_run(move_env):
    """When rmtree succeeds, ``cleanup_error`` is not present.

    Locks in that the new key is opt-in and the existing happy-path
    return shape is unchanged for the typical case.
    """
    from move import move_folder

    env = move_env
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    assert "cleanup_error" not in result
    assert result["errors"] == []


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


def test_preview_merge_counts_copy_and_skip(move_env):
    """preview_merge classifies every source file (sidecars included) as copy
    or skip by name, matching what rsync --ignore-existing actually does."""
    from move import preview_merge

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    # bird1.jpg already present at the destination -> skip; bird1.xmp and
    # bird2.jpg are missing -> copy.
    (landing / "bird1.jpg").write_bytes((env["src"] / "bird1.jpg").read_bytes())

    preview = preview_merge(str(env["src"]), str(landing))
    assert preview["will_skip"] == 1
    assert preview["will_copy"] == 2
    assert preview["will_block"] == 0
    assert preview["source_total"] == 3


def test_preview_merge_blocks_unverifiable_destination_entries(move_env, tmp_path):
    """A source file whose destination entry is something the post-copy
    verifier rejects — a symlink, a directory, or a path that resolves to
    the same inode as the source — must surface as ``will_block``, not
    ``will_skip``. rsync --ignore-existing would silently skip these by
    name, but _first_missing_source_file then refuses to delete the
    originals and the move aborts with "Verification failed". Reporting
    them as "already present and will be left untouched" would tell the
    user the merge is a no-op resume when it actually wouldn't complete.
    """
    from move import preview_merge

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()

    # bird1.jpg at the destination is a symlink — the verifier's islink
    # check rejects this even when the link target's bytes match.
    real_bird1 = tmp_path / "real_bird1.jpg"
    real_bird1.write_bytes((env["src"] / "bird1.jpg").read_bytes())
    os.symlink(str(real_bird1), str(landing / "bird1.jpg"))

    # bird1.xmp at the destination is a directory, not a file — the
    # verifier's isfile check rejects.
    (landing / "bird1.xmp").mkdir()

    # bird2.jpg missing -> a normal will_copy. Establishes that blocks
    # don't displace genuine copies.
    preview = preview_merge(str(env["src"]), str(landing))
    assert preview["will_block"] == 2
    assert preview["will_copy"] == 1
    assert preview["will_skip"] == 0
    assert preview["source_total"] == 3


def test_preview_merge_blocks_samefile_via_symlinked_parent(move_env, tmp_path):
    """A destination path that resolves to the same inode as the source
    file (e.g. the destination's parent is a symlink back into the source
    tree) is rejected by the verifier's samefile probe. preview_merge must
    classify it as ``will_block`` so the merge dialog doesn't claim the
    merge is a no-op when it would in fact fail to verify and abort.

    The trap this catches: rmtree(src) after a merge that "verified" via
    an aliased destination would destroy the only on-disk copy.
    """
    from move import preview_merge

    env = move_env
    # Add a nested source file that we can reach at the destination via a
    # symlinked parent (not a symlinked leaf — the islink branch already
    # catches that case; samefile guards the parent-alias path).
    sub = env["src"] / "nested"
    sub.mkdir()
    (sub / "bird3.jpg").write_bytes(b"x")

    landing = env["dst"] / "src"
    landing.mkdir()
    # landing/nested -> src/nested: when preview_merge computes dst_file =
    # landing/nested/bird3.jpg, path resolution follows the symlinked
    # parent so the leaf itself is a regular file (islink check is False),
    # but the inode matches the source — the case _first_missing_source_file
    # protects against by calling samefile.
    os.symlink(str(env["src"] / "nested"), str(landing / "nested"))

    dst_leaf = landing / "nested" / "bird3.jpg"
    assert os.path.samefile(str(env["src"] / "nested" / "bird3.jpg"), str(dst_leaf))
    assert not os.path.islink(str(dst_leaf))  # parent is the symlink, not the leaf

    preview = preview_merge(str(env["src"]), str(landing))
    # bird1.jpg, bird1.xmp, bird2.jpg at top-level: all missing -> copy.
    # nested/bird3.jpg: samefile with the source -> blocked.
    assert preview["will_block"] == 1
    assert preview["will_skip"] == 0
    assert preview["will_copy"] == 3
    assert preview["source_total"] == 4


def test_preview_merge_counts_directory_symlinks(move_env, tmp_path):
    """A directory symlink under the source is one transfer item: rsync -a /
    the shutil fallback recreate it as a symlink without descending. Omitting
    it would make the confirm dialog undercount and, for a source that's just
    a symlinked subdir, claim 0 files would transfer."""
    from move import preview_merge

    env = move_env
    # Real directory the symlinked subdir points at — kept outside the source
    # tree so its contents don't count on their own. The number of files it
    # holds is irrelevant: the preview must not descend.
    link_target = tmp_path / "linked_tree"
    link_target.mkdir()
    (link_target / "unused.jpg").write_bytes(b"x")
    os.symlink(str(link_target), str(env["src"] / "extras"))

    landing = env["dst"] / "src"
    landing.mkdir()
    preview = preview_merge(str(env["src"]), str(landing))
    # 3 source files + 1 directory symlink, none present at destination.
    assert preview["will_copy"] == 4
    assert preview["will_skip"] == 0
    assert preview["source_total"] == 4

    # Re-create the symlink at the destination — now the preview must classify
    # it as a skip rather than a copy, mirroring rsync --ignore-existing.
    os.symlink(str(link_target), str(landing / "extras"))
    preview = preview_merge(str(env["src"]), str(landing))
    assert preview["will_copy"] == 3
    assert preview["will_skip"] == 1
    assert preview["source_total"] == 4


def test_preview_merge_blocks_source_file_symlinks_with_missing_dest(move_env, tmp_path):
    """A source file that is itself a symlink, with nothing yet at the
    destination, must surface as ``will_block`` — not ``will_copy``.

    rsync -a (and the shutil fallback's os.symlink) recreate it as a
    symlink at the destination rather than materializing a regular file,
    and ``_first_missing_source_file`` then rejects the freshly-created
    symlink via its islink check. The merge aborts at the verify step
    after creating the link. Reporting it as "will be copied" would be a
    false promise: the dialog must warn instead so the user can choose
    not to commit to a merge that deterministically fails.
    """
    from move import preview_merge

    env = move_env
    # Source file symlink pointing outside the source tree. The link target
    # itself doesn't matter — the merge would create a symlink at the
    # destination either way, and the verifier rejects on islink alone.
    link_target = tmp_path / "real_bird3.jpg"
    link_target.write_bytes(b"x")
    os.symlink(str(link_target), str(env["src"] / "bird3.jpg"))

    landing = env["dst"] / "src"
    landing.mkdir()
    preview = preview_merge(str(env["src"]), str(landing))
    # 3 plain source files (bird1.jpg, bird1.xmp, bird2.jpg) are missing
    # at the destination — those are honest copies. bird3.jpg is a source
    # symlink with no destination entry — would-fail-verify after copy.
    assert preview["will_copy"] == 3
    assert preview["will_block"] == 1
    assert preview["will_skip"] == 0
    assert preview["source_total"] == 4


def test_preview_merge_is_name_only(move_env):
    """A same-name destination file counts as a skip even when its bytes
    differ — rsync --ignore-existing skips by name, and the differing-content
    case is caught separately as a hard conflict at merge time."""
    from move import preview_merge

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    # Same name, different content: still classified as a skip by the preview.
    (landing / "bird1.jpg").write_bytes(b"totally different bytes")

    preview = preview_merge(str(env["src"]), str(landing))
    assert preview["will_skip"] == 1
    assert preview["will_copy"] == 2


def test_move_folder_merge_never_overwrites_differing_dest_file(move_env):
    """A same-name destination file with DIFFERENT content (different size)
    is a hard conflict: the merge aborts before copying, leaving both the
    destination file and the originals untouched."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    # User's own file sharing bird1's name but with different content/size.
    (landing / "bird1.jpg").write_bytes(b"USER DATA - do not clobber")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Conflict" in e for e in result["errors"])
    # Pre-existing destination file untouched, originals preserved.
    assert (landing / "bird1.jpg").read_bytes() == b"USER DATA - do not clobber"
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_detects_same_size_different_content(move_env):
    """The dangerous case: a destination file with the SAME size but different
    bytes must still be detected as a conflict (size alone is insufficient),
    so the source is never deleted in favor of the wrong destination bytes."""
    from move import move_folder

    env = move_env
    src_bytes = (env["src"] / "bird1.jpg").read_bytes()
    landing = env["dst"] / "src"
    landing.mkdir()
    # Same length as the source, different content.
    decoy = bytes((b + 1) % 256 for b in src_bytes)
    assert len(decoy) == len(src_bytes) and decoy != src_bytes
    (landing / "bird1.jpg").write_bytes(decoy)

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Conflict" in e for e in result["errors"])
    # Destination decoy untouched, source preserved (no silent data loss).
    assert (landing / "bird1.jpg").read_bytes() == decoy
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_refuses_tracked_destination(move_env):
    """Merging into a destination Vireo already tracks as a folder is refused
    (a correct tracked-tree merge is out of scope and would dangle descendant
    paths). Originals are preserved."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    for fn in ("bird1.jpg", "bird1.xmp", "bird2.jpg"):
        (landing / fn).write_bytes((env["src"] / fn).read_bytes())
    # Destination already exists as its own folder row in the DB.
    env["db"].add_folder(str(landing), name="src")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    # Source intact.
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_refuses_tracked_descendant(move_env):
    """A tracked folder *below* the (untracked) destination must also block the
    merge — its path would collide when the source's children cascade onto it."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    (landing / "sub").mkdir(parents=True)
    # A tracked folder below the destination root.
    env["db"].add_folder(str(landing / "sub"), name="sub")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_refuses_destination_inside_tracked_ancestor(move_env):
    """Moving into a subfolder of a tracked root would create overlapping roots."""
    from move import move_folder

    env = move_env
    destination = env["dst"] / "Archive"

    result = move_folder(
        db=env["db"],
        folder_id=env["fid_src"],
        destination=str(destination),
        reject_tracked_ancestor=True,
    )

    assert result["moved"] == 0
    assert any("inside a folder Vireo already manages" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()
    assert not destination.exists()


def test_move_folder_refuses_tracked_merge_by_default(move_env):
    """Regression guard: without allow_tracked_merge, merging into a tracked
    destination is still refused (default behaviour byte-for-byte unchanged)."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    for fn in ("bird1.jpg", "bird1.xmp", "bird2.jpg"):
        (landing / fn).write_bytes((env["src"] / fn).read_bytes())
    # Destination already exists as its own folder row in the DB.
    env["db"].add_folder(str(landing), name="src")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]),
        merge=True,
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    # Source intact — nothing merged.
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merges_into_tracked_when_allowed(move_env):
    """With allow_tracked_merge=True the staged tree's files land in the
    existing archive on disk, the result reports merge counts, and the catalog
    has no leftover staged folder rows (they fold into the archive)."""
    from move import move_folder

    env = move_env
    db = env["db"]

    # The archive destination is the path the staged 'src' folder lands at when
    # placed inside 'dst' (move_folder preserves the source folder name).
    landing = env["dst"] / "src"
    landing.mkdir()
    # Prior shoot already in the archive on disk + in the catalog as a tracked
    # workspace-root folder.
    (landing / "prior.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 50)
    fid_archive = db.add_folder(str(landing), name="src")
    db.add_photo(folder_id=fid_archive, filename="prior.jpg", extension=".jpg",
                 file_size=52, file_mtime=3.0)

    staged_prefix = str(env["src"])

    result = move_folder(
        db=db, folder_id=env["fid_src"], destination=str(env["dst"]),
        merge=True, allow_tracked_merge=True,
    )

    assert result["errors"] == []
    assert result["moved"] >= 1
    # Staged files merged onto disk into the existing archive.
    assert (landing / "bird1.jpg").exists()
    assert (landing / "bird2.jpg").exists()
    assert (landing / "prior.jpg").exists()
    # Staging cleaned up.
    assert not env["src"].exists()

    # Merge counts surfaced. The two staged photos fold into the existing
    # archive folder; the prior photo is untouched.
    assert result["merged_into_existing"] == str(landing)
    merge = result["merge"]
    assert merge["new_photos"] == 2
    assert merge["merged_folders"] >= 1
    assert merge["already_present"] == 0

    # No leftover staged folder rows — the staged tree folded into the archive.
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path LIKE ?", (staged_prefix + "%",)
    ).fetchone() is None
    # The archive folder now holds all three photos under one row.
    names = {r["filename"] for r in db.conn.execute(
        "SELECT filename FROM photos WHERE folder_id = ?", (fid_archive,))}
    assert names == {"prior.jpg", "bird1.jpg", "bird2.jpg"}


def test_move_folder_merge_alias_destination_uses_stored_tracked_path(
        move_env, monkeypatch):
    """Regression: when the archive destination is a symlink/case-only alias of
    an already-tracked folder, the exact-overlap merge must reconcile onto the
    STORED tracked path, not the aliased ``catalog_path``. Otherwise
    ``merge_staged_tree_into_archive``'s exact ``WHERE path = ?`` lookups miss
    the existing row and create a SECOND folder row for the same on-disk
    archive.

    Simulated on Linux the same way as the case-alias refusal tests:
      1. Symlink so two distinct path strings share an inode.
      2. realpath/normcase patched to no-ops so the string-based tracked check
         misses and the samefile fallback in ``_path_equal_or_descends`` folds
         the alias to the tracked row.
    """
    from move import move_folder

    env = move_env
    db = env["db"]

    # The real archive folder, tracked at its real path with a prior shoot.
    real_dst = env["tmp_path"] / "realdst"
    landing = real_dst / "src"
    landing.mkdir(parents=True)
    (landing / "prior.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 50)
    fid_archive = db.add_folder(str(landing), name="src")
    db.add_photo(folder_id=fid_archive, filename="prior.jpg", extension=".jpg",
                 file_size=52, file_mtime=3.0)

    alias_dst = env["tmp_path"] / "alias_dst"
    try:
        os.symlink(str(real_dst), str(alias_dst))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    monkeypatch.setattr(os.path, "normcase", lambda p: p)

    # destination=alias_dst → resolved dest = alias_dst/src, an alias of the
    # tracked realdst/src. The merge is accepted (exact overlap via samefile)
    # and must fold into the existing row, not create a second one.
    result = move_folder(
        db=db, folder_id=env["fid_src"], destination=str(alias_dst),
        merge=True, allow_tracked_merge=True,
    )

    assert result["errors"] == []
    # The reconciliation base is the STORED tracked path, so the existing
    # archive row absorbs the staged photos — exactly ONE folder row for the
    # on-disk archive, no alias-path duplicate.
    archive_rows = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", (str(landing),)
    ).fetchall()
    assert len(archive_rows) == 1
    assert archive_rows[0]["id"] == fid_archive
    # No folder row was created under the alias path.
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path = ?", (str(alias_dst / "src"),)
    ).fetchone() is None
    # All three photos live under the single archive row.
    names = {r["filename"] for r in db.conn.execute(
        "SELECT filename FROM photos WHERE folder_id = ?", (fid_archive,))}
    assert names == {"prior.jpg", "bird1.jpg", "bird2.jpg"}
    # Staging folded away.
    assert not env["src"].exists()


def test_move_folder_ancestor_merge_reconciles_onto_stored_ancestor(
        move_env, monkeypatch):
    """Regression: when the destination sits INSIDE a tracked folder that was
    matched via a symlink or case-only alias, the ancestor-merge branch must
    rebase the reconciliation onto the STORED ancestor path, not the
    aliased ``catalog_path``. Otherwise ``merge_staged_tree_into_archive``'s
    exact ``WHERE path = ?`` parent lookups miss the stored ancestor row and
    land the staged root under an alias-prefixed path with ``parent_id=NULL``,
    spawning a parallel row set outside the managed archive tree.

    Simulated on Linux the same way as
    ``test_move_folder_merge_alias_destination_uses_stored_tracked_path``:
    a symlink so two path strings share an inode, plus ``realpath``/
    ``normcase`` patched to no-ops so ``_tracked_destination_ancestor`` misses
    the string-prefix check and falls into the samefile walk-up.
    """
    from move import move_folder

    env = move_env
    db = env["db"]

    # Real tracked ancestor archive with no rows below it yet.
    real_archive = env["tmp_path"] / "realarch"
    real_archive.mkdir()
    fid_ancestor = db.add_folder(str(real_archive), name="realarch")

    # Alias destination pointing at the same on-disk directory.
    alias_archive = env["tmp_path"] / "aliasarch"
    try:
        os.symlink(str(real_archive), str(alias_archive))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    monkeypatch.setattr(os.path, "normcase", lambda p: p)

    # destination=alias_archive -> resolved dest = alias_archive/src. The
    # ancestor probe only matches ``real_archive`` via the samefile walk-up,
    # so ``catalog_path`` is the alias-prefixed ``alias_archive/src``.
    result = move_folder(
        db=db, folder_id=env["fid_src"], destination=str(alias_archive),
        reject_tracked_ancestor=True, allow_tracked_merge=True,
    )

    assert result["errors"] == []
    assert result["merged_into_existing"] == str(real_archive)

    # The staged folder was reconciled onto STORED ancestor + "src", NOT the
    # alias-prefixed ``aliasarch/src``.
    stored_landing = str(real_archive / "src")
    alias_landing = str(alias_archive / "src")
    stored_row = db.conn.execute(
        "SELECT id, parent_id FROM folders WHERE path = ?", (stored_landing,)
    ).fetchone()
    assert stored_row is not None
    # Parent resolves to the stored ancestor row — not NULL (which would
    # mean an alias-prefixed row floating outside the managed tree).
    assert stored_row["parent_id"] == fid_ancestor
    # No alias-prefixed row was created.
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path = ?", (alias_landing,)
    ).fetchone() is None
    # Files land on disk under the real archive (same inode either way).
    assert (real_archive / "src" / "bird1.jpg").exists()
    assert (real_archive / "src" / "bird2.jpg").exists()


def test_move_folder_merge_relocates_developed_dir_onto_reconciled_base(
        move_env, monkeypatch):
    """Regression: on the exact-overlap merge path with a symlink/case-only
    alias destination, ``developed_folder_key`` hashes the STORED tracked
    path (that's what the catalog stores after reconciliation), so the
    developed-dir relocation must move outputs onto the same reconciled
    base. Relocating onto the aliased ``catalog_path`` would leave renders
    under a hash exports never read from, silently falling back to RAW
    after import.
    """
    from export import developed_folder_key
    from move import move_folder

    env = move_env
    db = env["db"]

    # Real archive folder, tracked at its real path.
    real_dst = env["tmp_path"] / "realdst2"
    landing = real_dst / "src"
    landing.mkdir(parents=True)
    fid_archive = db.add_folder(str(landing), name="src")

    alias_dst = env["tmp_path"] / "alias_dst2"
    try:
        os.symlink(str(real_dst), str(alias_dst))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    # Pre-existing developed subdir keyed off the STAGED source path (that's
    # where renders lived before this move ran). Also pre-create the target
    # keyed off the ALIAS so a bug that relocates onto the alias would look
    # correct on disk but rot the export path — force the assertion to
    # distinguish "moved to alias key" from "moved to stored key".
    developed = env["tmp_path"] / "developed"
    developed.mkdir()
    old_key = developed_folder_key(str(env["src"]))
    (developed / old_key).mkdir()
    (developed / old_key / "bird1.jpg").write_bytes(b"developed-bytes")

    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    monkeypatch.setattr(os.path, "normcase", lambda p: p)

    result = move_folder(
        db=db, folder_id=env["fid_src"], destination=str(alias_dst),
        merge=True, allow_tracked_merge=True,
        developed_dir=str(developed),
    )
    assert result["errors"] == []

    # The developed subdir moved to the STORED-path key (what export reads
    # from the catalog after reconciliation), not the alias-path key.
    stored_key = developed_folder_key(str(landing))
    alias_key = developed_folder_key(str(alias_dst / "src"))
    assert stored_key != alias_key
    assert (developed / stored_key / "bird1.jpg").read_bytes() == b"developed-bytes"
    assert not (developed / alias_key).exists()
    # Old (staged-path) key is gone.
    assert not (developed / old_key).exists()
    # Catalog still has one archive row — the stored one.
    assert db.conn.execute(
        "SELECT COUNT(*) c FROM folders WHERE path = ?", (str(landing),)
    ).fetchone()["c"] == 1
    assert db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", (str(landing),)
    ).fetchone()["id"] == fid_archive


def test_move_folder_refuses_descendant_tracked_overlap_even_when_merging(move_env):
    """Regression: ``allow_tracked_merge`` must only accept the "tracked row
    IS the destination" case, not a tracked row that sits STRICTLY BELOW the
    resolved destination. The descendant case is "wrap a fresh parent around
    an existing tracked subtree" (e.g. ``/Photos/USA/2024`` scanned as its
    own workspace root, then a fresh staged tree lands at ``/Photos/USA``);
    the reconciliation would rebase the staged tree onto the wrapper path
    while leaving the pre-existing tracked descendant's parentage untouched,
    creating two overlapping catalog subtrees managing the same on-disk area.
    Refuse before any copy — same guard as the default-off case."""
    from move import move_folder

    env = move_env
    db = env["db"]

    # Landing resolves to <dst>/src. Register a tracked row STRICTLY BELOW
    # that landing so ``_tracked_destination_overlap`` returns a descendant
    # (not an exact match). The row doesn't need to exist on disk — the
    # overlap probe is folder-row based.
    landing = env["dst"] / "src"
    inner = landing / "existing-shoot"
    inner_fid = db.add_folder(str(inner), name="existing-shoot")

    result = move_folder(
        db=db, folder_id=env["fid_src"], destination=str(env["dst"]),
        merge=True, allow_tracked_merge=True,
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    # Source intact — the guard fires before any copy or delete.
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()
    # Landing was never materialized and no wrapper folder row was created
    # for it. The pre-existing tracked descendant row is untouched.
    assert not landing.exists()
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path = ?", (str(landing),)
    ).fetchone() is None
    assert db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", (str(inner),)
    ).fetchone()["id"] == inner_fid


def test_tracked_destination_overlap_prefers_exact_match_over_descendant(move_env):
    """Regression: when both an exact-match tracked row (destination itself)
    and a strict-descendant tracked row exist, ``_tracked_destination_overlap``
    must return the EXACT match. Without the preference, SQLite returns rows in
    arbitrary order (insertion / rowid by default); a descendant row inserted
    BEFORE its later-scanned parent would come back first, and the caller's
    ``tracked_is_destination`` check would then reject the exact-match merge as
    the unsupported "wrap around a tracked subfolder" case."""
    from move import _tracked_destination_overlap

    env = move_env
    db = env["db"]
    landing = env["dst"] / "src"

    # Insert the DESCENDANT row first so a plain unordered scan returns it
    # before the exact-match row that arrives later. Neither row needs to
    # exist on disk — the overlap probe is folder-row based.
    desc_fid = db.add_folder(str(landing / "existing-shoot"),
                             name="existing-shoot")
    exact_fid = db.add_folder(str(landing), name="src")
    # Confirm the intended insertion order (descendant has the lower rowid,
    # so a naive SELECT would hit it first).
    assert desc_fid < exact_fid

    row = _tracked_destination_overlap(db, env["fid_src"], str(landing))
    assert row is not None
    assert row["id"] == exact_fid
    assert row["path"] == str(landing)


def test_move_folder_accepts_exact_tracked_merge_despite_descendant_row(move_env):
    """End-to-end regression for the ordering fix: ``move_folder`` with
    ``allow_tracked_merge=True`` must accept an EXACT tracked destination
    (destination IS the tracked row) even when a strict-descendant tracked row
    also exists and was inserted first. Before the exact-match preference in
    ``_tracked_destination_overlap`` the descendant row could be returned first
    and get treated as the unsupported "wrap" case, refusing the merge."""
    from move import move_folder

    env = move_env
    db = env["db"]
    landing = env["dst"] / "src"
    landing.mkdir()

    # Descendant tracked row first (lower rowid than the exact-match row),
    # then the exact-match row for the landing path itself.
    db.add_folder(str(landing / "existing-shoot"), name="existing-shoot")
    fid_archive = db.add_folder(str(landing), name="src")

    result = move_folder(
        db=db, folder_id=env["fid_src"], destination=str(env["dst"]),
        merge=True, allow_tracked_merge=True,
    )
    # The exact-match tracked row means this is the accept-as-merge case, NOT
    # the descendant-only "wrap" case. Merge succeeds, no errors.
    assert result["errors"] == []
    merge = result.get("merge")
    assert merge is not None
    # Two staged photos, neither collides with anything in the empty archive:
    # both become newly-archived rows.
    assert merge["new_photos"] == 2
    # The two staged photos are now catalog-parented on the archive folder.
    assert db.conn.execute(
        "SELECT COUNT(*) c FROM photos WHERE folder_id = ?",
        (fid_archive,),
    ).fetchone()["c"] == 2


def test_move_folder_merge_moved_excludes_already_present(move_env):
    """On the merge path, identical-filename collisions are dropped as
    ``already_present`` and must NOT be counted in ``result['moved']``.
    ``moved`` reflects photos actually added to the archive
    (== merge['new_photos']), not every staged source photo."""
    from move import move_folder

    env = move_env
    db = env["db"]

    # The staged 'src' folder lands at <dst>/src. Pre-seed that archive folder
    # with a byte-identical copy of one staged photo (bird1.jpg) so it collides
    # by filename AND content -> dropped as already_present (rsync
    # --ignore-existing keeps the archive copy; the catalog drops the staged
    # row). bird1.jpg in the fixture is b"\xff\xd8" + b"\x00"*100.
    landing = env["dst"] / "src"
    landing.mkdir()
    identical_bytes = (env["src"] / "bird1.jpg").read_bytes()
    (landing / "bird1.jpg").write_bytes(identical_bytes)
    fid_archive = db.add_folder(str(landing), name="src")
    # Matching ``file_hash`` on both sides is required for the merge to
    # treat the collision as real (see merge_staged_tree_into_archive's
    # hash-only invariant — size alone can't distinguish a real collision
    # from a rsync-copied phantom in the post-copy path). Pre-seed both
    # rows with the same hash, and stamp the staged fixture row with it
    # too so the drop path fires.
    db.add_photo(folder_id=fid_archive, filename="bird1.jpg", extension=".jpg",
                 file_size=len(identical_bytes), file_mtime=3.0,
                 file_hash="BIRD1HASH")
    db.conn.execute(
        "UPDATE photos SET file_hash = ? WHERE folder_id = ? AND filename = ?",
        ("BIRD1HASH", env["fid_src"], "bird1.jpg"),
    )
    db.conn.commit()

    result = move_folder(
        db=db, folder_id=env["fid_src"], destination=str(env["dst"]),
        merge=True, allow_tracked_merge=True,
    )

    assert result["errors"] == []
    merge = result["merge"]
    # bird1.jpg collides (identical) -> already_present; bird2.jpg is new.
    assert merge["already_present"] >= 1
    assert merge["new_photos"] == 1
    # moved must exclude the already_present photo.
    assert result["moved"] == merge["new_photos"]
    # The archive still has exactly one bird1.jpg row (no duplicate).
    assert db.conn.execute(
        "SELECT COUNT(*) c FROM photos WHERE folder_id = ? AND filename = ?",
        (fid_archive, "bird1.jpg"),
    ).fetchone()["c"] == 1


def test_move_folder_merge_reports_dropped_ids_for_cache_cleanup(move_env):
    """Regression: cached thumbnails/previews/working copies are keyed by
    ``photos.id``. When the merge drops a staged photo as
    ``already_present``, the freed rowid would leave orphan cache files on
    disk (SQLite reuses rowids, so a future import that lands on the same
    id inherits stale imagery). ``move_folder`` must surface the freed
    staged ids at ``result['dropped_photo_ids']`` so the pipeline archive
    stage can hand them to ``cleanup_cached_files_for_deleted_photos``.

    Kept OFF ``result['merge']`` — that dict is serialized into the
    archive-stage summary/API payload, and internal photo ids are not
    part of the user-facing shape."""
    from move import move_folder

    env = move_env
    db = env["db"]

    # Same setup as ``moved_excludes_already_present`` — bird1.jpg is
    # byte-identical between staged src and pre-seeded archive landing, so
    # the merge drops the staged bird1.jpg row.
    landing = env["dst"] / "src"
    landing.mkdir()
    identical_bytes = (env["src"] / "bird1.jpg").read_bytes()
    (landing / "bird1.jpg").write_bytes(identical_bytes)
    fid_archive = db.add_folder(str(landing), name="src")
    # Matching ``file_hash`` on both rows exercises the real-collision
    # drop path (see merge_staged_tree_into_archive's hash-only invariant).
    db.add_photo(folder_id=fid_archive, filename="bird1.jpg",
                 extension=".jpg",
                 file_size=len(identical_bytes), file_mtime=3.0,
                 file_hash="BIRD1HASH")
    db.conn.execute(
        "UPDATE photos SET file_hash = ? WHERE folder_id = ? AND filename = ?",
        ("BIRD1HASH", env["fid_src"], "bird1.jpg"),
    )
    db.conn.commit()

    staged_bird1_pid = db.conn.execute(
        "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
        (env["fid_src"], "bird1.jpg"),
    ).fetchone()["id"]

    result = move_folder(
        db=db, folder_id=env["fid_src"], destination=str(env["dst"]),
        merge=True, allow_tracked_merge=True,
    )

    assert result["errors"] == []
    assert staged_bird1_pid in result.get("dropped_photo_ids", [])
    # The user-facing merge dict stays clean of internal ids.
    assert "dropped_photo_ids" not in result["merge"]


def test_move_folder_refuses_missing_tracked_destination_before_copy(move_env):
    """A stale tracked destination row must block the move even when the
    resolved destination does not currently exist on disk."""
    from move import move_folder

    env = move_env
    landing = env["dst"] / "src"
    assert not landing.exists()
    env["db"].add_folder(str(landing), name="src")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )

    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()
    assert not landing.exists()


def test_move_folder_refuses_missing_tracked_descendant_before_copy(move_env):
    """A stale tracked descendant row would collide when source children
    cascade to the destination path, so it must block before any copy."""
    from move import move_folder

    env = move_env
    source_child = env["src"] / "sub"
    source_child.mkdir()
    env["db"].add_folder(str(source_child), name="sub")

    landing = env["dst"] / "src"
    tracked_child = landing / "sub"
    assert not landing.exists()
    env["db"].add_folder(str(tracked_child), name="sub")

    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )

    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()
    assert source_child.exists()
    assert not landing.exists()


def test_move_folder_merge_refuses_symlinked_tracked_destination(move_env):
    """A tracked folder reached via a symlink alias must be detected by the
    tracked-destination check (canonical realpath compare), not slip past a
    raw-string match."""
    from move import move_folder

    env = move_env
    real_dst = env["tmp_path"] / "realdst"
    landing = real_dst / "src"
    landing.mkdir(parents=True)
    env["db"].add_folder(str(landing), name="src")  # tracked at its real path

    link = env["tmp_path"] / "dstlink"
    try:
        os.symlink(str(real_dst), str(link))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    # Destination via the alias resolves (realpath) to the tracked landing.
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(link), merge=True
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_refuses_symlinked_self_destination(move_env):
    """A destination that is a symlink alias of the source's parent resolves
    (via realpath) to the source itself and must be refused, not no-op-copied
    then deleted."""
    from move import move_folder

    env = move_env
    link = env["tmp_path"] / "alias"
    try:
        os.symlink(str(env["src"].parent), str(link))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    # destination = alias → resolved dest == alias/src, realpath == src path
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(link), merge=True
    )
    assert result["moved"] == 0
    assert any("overlaps the source" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_merge_refuses_case_alias_self_destination(move_env, monkeypatch):
    """On a case-insensitive POSIX filesystem (default macOS APFS),
    os.path.normcase is a no-op and os.path.realpath does not fold case, so
    a destination that differs from the source only by case still resolves
    to the same inode but string-compares unequal. The overlap guard must
    fall back to os.path.samefile (device + inode) so the merge is refused
    before shutil.rmtree deletes the only copy of the source files.

    Simulated on Linux by:
      1. Using a symlink so two distinct path strings share an inode.
      2. Patching realpath/normcase to no-ops so the existing string-based
         check doesn't pre-empt the samefile fallback we want to exercise.
    """
    from move import move_folder

    env = move_env
    alias_parent = env["tmp_path"] / "alias_parent"
    try:
        os.symlink(str(env["src"].parent), str(alias_parent))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    monkeypatch.setattr(os.path, "normcase", lambda p: p)

    # destination=alias_parent → resolved dest = alias_parent/src; samefile
    # against env["src"] returns True because the symlink shares the inode.
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(alias_parent), merge=True
    )
    assert result["moved"] == 0
    assert any("overlaps the source" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()


def test_move_folder_merge_refuses_case_alias_tracked_destination(move_env, monkeypatch):
    """On a case-insensitive POSIX filesystem (default macOS APFS), a tracked
    folder reached via a case-only alias must also be refused. realpath +
    normcase don't fold case there, so the tracked-destination string compare
    misses; the samefile fallback in `_path_equal_or_descends` catches it and
    keeps two folder rows from managing the same on-disk tree.

    Simulated on Linux the same way as the self-destination case-alias test:
      1. Symlink so two distinct path strings share an inode.
      2. realpath/normcase patched to no-ops so the string-based check doesn't
         pre-empt the samefile fallback we want to exercise.
    """
    from move import move_folder

    env = move_env
    real_dst = env["tmp_path"] / "realdst"
    landing = real_dst / "src"
    landing.mkdir(parents=True)
    # Pre-populate landing so dest_exists is True and the tracked check runs.
    (landing / "bird1.jpg").write_bytes((env["src"] / "bird1.jpg").read_bytes())
    env["db"].add_folder(str(landing), name="src")  # tracked at the real path

    alias_dst = env["tmp_path"] / "alias_dst"
    try:
        os.symlink(str(real_dst), str(alias_dst))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(os.path, "realpath", lambda p: p)
    monkeypatch.setattr(os.path, "normcase", lambda p: p)

    # destination=alias_dst → resolved dest = alias_dst/src; string-compares
    # unequal to the tracked landing path, but samefile makes them collapse.
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(alias_dst), merge=True
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    # Source intact.
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()


def test_move_folder_refuses_case_alias_missing_tracked_destination(move_env, monkeypatch):
    """On a case-insensitive POSIX filesystem (default macOS APFS), a stale
    tracked folder row whose path differs from the resolved destination only
    by case must still be refused even when both leaves are missing on disk.
    realpath/normcase don't fold case there and samefile has nothing to
    compare across two non-existing paths, so without the FS case-insensitivity
    probe, _path_equal_or_descends would let the move copy first and leave two
    folder rows managing the same on-disk tree.

    Simulated on Linux by patching the case-insensitivity probe to True so the
    case-folded fallback runs without requiring an actual case-insensitive FS.
    """
    import move as move_mod

    env = move_env
    # User asks to move src into a missing destination whose path differs only
    # by case from a stale tracked DB row. Both parent and leaf are absent so
    # samefile has nothing to compare, even on a case-insensitive filesystem.
    dest_input = env["tmp_path"] / "missing_dest"
    assert not dest_input.exists()
    stale_parent = env["tmp_path"] / "MISSING_DEST"
    assert not stale_parent.exists()
    # On a case-insensitive FS this is the same directory as the resolved
    # landing /tmp_path/missing_dest/src, but neither path exists on disk.
    stale_landing = stale_parent / "src"
    assert not stale_landing.exists()
    env["db"].add_folder(str(stale_landing), name="src")

    monkeypatch.setattr(move_mod, "_is_case_insensitive_path", lambda p: True)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(dest_input)
    )
    assert result["moved"] == 0
    assert any("already manage" in e for e in result["errors"])
    # Source untouched, no copy started.
    assert (env["src"] / "bird1.jpg").exists()
    assert not stale_landing.exists()
    assert not stale_parent.exists()
    assert not dest_input.exists()


@pytest.mark.skipif(
    _TMP_FOLDS_CASE,
    reason="Scenario requires a case-sensitive parent FS so the "
           "case-flipped ancestor names resolve to distinct directories.",
)
def test_path_equal_or_descends_case_fold_scoped_to_ci_root(tmp_path):
    """The case-folded fallback in `_path_equal_or_descends` must only fold
    the suffix below the probed case-insensitive root — not the whole path.
    Otherwise a tracked row whose path differs from the resolved destination
    in a component ABOVE the case-folding subtree (a case-insensitive APFS
    or CIFS volume mounted at /mnt/photos under a case-sensitive Linux root
    FS: stale row /MNT/photos/dst/src vs move into /mnt/photos/dst/src) would
    falsely collapse as overlap and refuse a valid move, even though `/MNT`
    is a distinct directory from `/mnt` on the parent (case-sensitive) FS.
    Case-only differences BELOW the root must still collapse."""
    import move as move_mod

    ci_root = tmp_path / "ci_root"
    ci_root.mkdir()
    ci_root_real = os.path.realpath(str(ci_root))
    dest = str(ci_root / "dst" / "src")

    # Above the root — a case-flipped component on the case-sensitive parent
    # FS is a genuinely distinct path. Must NOT register as overlap.
    above_root = str(tmp_path / "CI_ROOT" / "dst" / "src")
    assert move_mod._path_equal_or_descends(
        above_root, dest, case_insensitive_root=ci_root_real
    ) is False

    # Below the root — case-only difference on the case-folding FS aliases
    # to the same on-disk directory. Must register as overlap.
    below_root = str(ci_root / "DST" / "src")
    assert move_mod._path_equal_or_descends(
        below_root, dest, case_insensitive_root=ci_root_real
    ) is True

    # Sibling of the root on the parent FS — distinct path, must NOT
    # register even when both paths happen to case-fold to the same string
    # when lowercased in full.
    sibling = str(tmp_path / "ci_root2" / "dst" / "src")
    assert move_mod._path_equal_or_descends(
        sibling, dest, case_insensitive_root=ci_root_real
    ) is False


@pytest.mark.skipif(
    _TMP_FOLDS_CASE,
    reason="Sibling-root assertion requires a case-sensitive parent FS so "
           "the case-flipped root spelling is a distinct on-disk directory.",
)
def test_path_equal_or_descends_folds_root_case_alias_in_candidate(
    tmp_path, monkeypatch,
):
    """The probed case-insensitive root itself can be spelled differently
    by case in a stale candidate row (e.g. row `/Photos/DST/src` against
    move target `/Photos/dst/src`, where `/Photos/dst` is the deepest
    existing case-folding ancestor). The literal `startswith(root)` check
    rejected such a row before the suffix was folded, so
    `_tracked_destination_overlap` missed the stale row and a move could
    leave two folder rows managing the same on-disk tree. The fix matches
    the root prefix case-insensitively and confirms via samefile that the
    candidate's variant is the same on-disk directory."""
    import move as move_mod

    base = tmp_path / "Photos"
    base.mkdir()
    root_dir = base / "dst"
    root_dir.mkdir()
    root_real = os.path.realpath(str(root_dir))

    # Move target's missing leaf under the existing case-folding root.
    dest = str(root_dir / "src")
    # Stale tracked row whose root-level segment is a case-only alias of
    # `root_dir`. Missing on disk on the case-sensitive Linux CI host.
    stale = str(base / "DST" / "src")

    real_samefile = move_mod._samefile_or_false

    def fake_samefile(a, b):
        # Mimic case-folding FS at `base`: two children of `base` whose
        # basenames differ only by case resolve to the same inode, even
        # when one of them doesn't actually exist on the underlying
        # case-sensitive host FS. Fall through to the real probe for
        # everything else so unrelated paths still behave normally.
        if (os.path.dirname(a) == str(base) and os.path.dirname(b) == str(base)
                and os.path.basename(a).lower() == os.path.basename(b).lower()):
            return True
        return real_samefile(a, b)

    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile)

    assert move_mod._path_equal_or_descends(
        stale, dest, case_insensitive_root=root_real
    ) is True

    # Sanity: an ABOVE-root case variant (parent FS case-sensitive) must
    # still be rejected — the samefile check on the candidate's root-level
    # prefix is what distinguishes the two scenarios.
    sibling_root = str(tmp_path / "PHOTOS" / "dst" / "src")
    assert move_mod._path_equal_or_descends(
        sibling_root, dest, case_insensitive_root=root_real
    ) is False


@pytest.mark.skipif(
    os.name == "nt",
    reason="POSIX filesystem-root scenario; Windows uses drive-rooted paths",
)
def test_path_equal_or_descends_handles_case_fold_root_at_filesystem_root():
    """When the probed case-insensitive root is the filesystem root itself
    (a writable case-insensitive POSIX volume mounted at `/`, or any
    destination whose first missing component sits directly under `/`),
    `root + os.sep` doubles to "//" and no real path starts with it.
    Without the trailing-separator handling, the case-folded compare is
    silently skipped and a stale tracked row like `/photos/src` slips past
    `_tracked_destination_overlap` against a move into `/Photos/src` —
    leaving two folder rows managing the same on-disk tree once the copy
    starts.
    """
    import move as move_mod

    # Both paths missing on disk (typical stale-row + fresh-move scenario).
    # The existence-based samefile checks early-return None for missing
    # paths, so the case-folded suffix compare is the only path that
    # catches this — with case_insensitive_root="/", the root prefix is the
    # bare separator and the suffix strip leaves the full path minus "/".
    assert move_mod._path_equal_or_descends(
        "/__vireo_missing_for_test/PHOTOS/src",
        "/__vireo_missing_for_test/photos/src",
        case_insensitive_root="/",
    ) is True

    # Equal paths under the root (canonical match through the case-folded
    # fallback when both leaves are missing).
    assert move_mod._path_equal_or_descends(
        "/__vireo_missing_for_test/photos/src",
        "/__vireo_missing_for_test/photos/src",
        case_insensitive_root="/",
    ) is True

    # Descendant of a case-aliased ancestor — the candidate's longer suffix
    # must still match the ancestor's via the prefix compare.
    assert move_mod._path_equal_or_descends(
        "/__vireo_missing_for_test/Photos/dst/inner",
        "/__vireo_missing_for_test/photos/dst",
        case_insensitive_root="/",
    ) is True

    # Genuinely distinct paths (not just case-different) still return False
    # — the case-fold-at-root edge must not cause over-folding.
    assert move_mod._path_equal_or_descends(
        "/__vireo_missing_for_test/other/src",
        "/__vireo_missing_for_test/photos/src",
        case_insensitive_root="/",
    ) is False


@pytest.mark.skipif(
    _TMP_FOLDS_CASE,
    reason="Scenario requires a case-sensitive parent FS so the "
           "above-root case-flipped row is a distinct on-disk path.",
)
def test_tracked_destination_overlap_skips_rows_outside_ci_root(move_env, monkeypatch):
    """End-to-end: `_tracked_destination_overlap` must NOT return a stale
    row whose path differs from the destination above the case-insensitive
    boundary. Before the fix, the full-path `.lower()` fallback collapsed
    /MNT/photos/dst/src with /mnt/photos/dst/src on a Linux box with a
    case-insensitive subvolume mounted at /mnt/photos and refused valid
    moves into that subvolume as "already managed"."""
    import move as move_mod

    env = move_env
    ci_root = env["tmp_path"] / "ci_root"
    ci_root.mkdir()
    dest = str(ci_root / "dst" / "src")
    assert not os.path.exists(dest)

    above_root = str(env["tmp_path"] / "CI_ROOT" / "dst" / "src")
    env["db"].add_folder(above_root, name="src")

    # Force the probe True so the case-fold branch is exercised regardless
    # of the host FS (CI tends to be case-sensitive ext4).
    monkeypatch.setattr(move_mod, "_is_case_insensitive_path", lambda p: True)

    assert move_mod._tracked_destination_overlap(
        env["db"], env["fid_src"], dest
    ) is None


def test_tracked_destination_overlap_caches_case_insensitivity_probe(move_env, monkeypatch):
    """The FS case-insensitivity probe is os.listdir-backed and re-running it
    per tracked-folder row turns the preflight guard into
    O(tracked_folders × destination_entries) before any copy starts. The
    overlap check must compute it once for the resolved destination and
    reuse that result for every row it scans.
    """
    import move as move_mod

    env = move_env
    # Many unrelated tracked rows. Each forces the missing-leaves fallback
    # in _path_equal_or_descends (realpath compare unequal, both leaves
    # missing or ancestor missing), which is the branch that calls the
    # probe — so without caching, the probe fires once per row.
    dest_input = env["tmp_path"] / "missing_dest_cache_probe"
    assert not dest_input.exists()
    for i in range(8):
        env["db"].add_folder(
            str(env["tmp_path"] / f"unrelated_{i}" / "leaf"), name="leaf"
        )

    calls = []
    real_probe = move_mod._is_case_insensitive_path

    def counting_probe(path):
        calls.append(path)
        return real_probe(path)

    monkeypatch.setattr(move_mod, "_is_case_insensitive_path", counting_probe)

    move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(dest_input)
    )

    # Count probes whose argument is the resolved destination. move_folder
    # resolves `destination` to `destination/<source_folder_name>` before
    # the overlap check, so the probed path is the landing dir, not the raw
    # input. The expected pattern with caching is 2 probes: one from
    # _destination_overlaps_source(src, dest)'s symmetric check against
    # dest, and one cached probe from _tracked_destination_overlap. Without
    # caching, the row loop would add 8 more (one per added row) — the
    # regression we're guarding against. The exact bound matters: bumping
    # the row count must not bump the probe count.
    resolved_dest = os.path.realpath(str(dest_input / "src"))
    dest_probes = [p for p in calls if os.path.realpath(p) == resolved_dest]
    # Lower bound on POSIX: the destination probe MUST be exercised at
    # least once. Without this, the upper-bound assertion passes vacuously
    # if a future refactor skips the resolved-destination probe entirely.
    # Windows short-circuits to True without listing, so no probe fires.
    if os.name != "nt":
        assert len(dest_probes) >= 1, (
            f"expected at least one resolved-destination probe on POSIX, "
            f"got 0 (all calls: {calls})"
        )
    assert len(dest_probes) <= 2, (
        f"expected dest probe to be cached (≤2 calls), "
        f"got {len(dest_probes)} (all calls: {calls})"
    )


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_probes_inside_target_fs(tmp_path, monkeypatch):
    """The case-insensitivity probe must test the FS at the deepest existing
    ancestor by case-flipping a CHILD entry, not by case-flipping the
    ancestor's own basename. Otherwise a case-sensitive APFS volume mounted
    at /Volumes/Photos under default case-insensitive macOS HFS+ is wrongly
    classified case-insensitive (the basename probe asks /Volumes how it
    resolves "Photos", which is case-insensitive, instead of asking the
    mounted volume how it resolves its own children), and valid moves into
    the volume get refused as overlapping a stale case-only alias.
    """
    import move as move_mod

    mount = tmp_path / "Mount"
    mount.mkdir()
    # Letter-named child so a child probe has something to flip.
    (mount / "child").mkdir()
    mount_str = str(mount)

    def fake_samefile(a, b):
        # Mimic the misleading scenario: case-flipping the mount point's
        # own basename in its parent directory collapses (the parent FS
        # folds case), while case-flipping a child name under the mount
        # does not (the mount's FS is case-sensitive).
        names = {os.path.basename(a), os.path.basename(b)}
        return names == {"Mount", "mOUNT"}

    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_samefile)

    # Deepest existing ancestor of the missing leaf is `mount`. The fixed
    # probe must look inside `mount` and report case-sensitive (False).
    # The pre-fix code probed `os.path.basename(mount)` in its parent and
    # returned True — the regression we're guarding against.
    assert move_mod._is_case_insensitive_path(
        os.path.join(mount_str, "missing", "sub")
    ) is False


def test_is_case_insensitive_path_detects_case_insensitive_fs(tmp_path, monkeypatch):
    """When the deepest existing ancestor's FS folds case, the probe must
    return True — confirmed via the temp-dir probe, since `samefile` True
    on a pre-existing child name could be a user-created hard link / symlink
    alias rather than real case folding."""
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "child").touch()
    target_str = str(target)

    def fake_samefile(a, b):
        # Mimic case-insensitive FS at `target`: any two child names
        # (including the temp probe dir we create) that differ only by
        # case resolve to the same inode.
        return (
            os.path.dirname(a) == target_str
            and os.path.dirname(b) == target_str
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    # Child probe alone is no longer authoritative for True — patch both
    # entry points so the temp-dir confirmation also sees the case fold.
    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_samefile)
    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is True


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_inconclusive_child_probe_keeps_scanning(
    tmp_path, monkeypatch,
):
    """A letter-bearing child whose case-flipped probe raises (broken
    symlink, permission/race) must not abort the scan as case-sensitive —
    only a definitive `samefile` False (two distinct entries) does that.
    Inconclusive Nones are skipped; True is non-authoritative and confirmed
    via the temp-dir probe below.
    """
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "broken").touch()
    (target / "child").touch()
    target_str = str(target)

    def fake_tristate(a, b):
        names = {os.path.basename(a).lower(), os.path.basename(b).lower()}
        if "broken" in names:
            # Mimic stat raising on the flipped spelling of a broken entry.
            return None
        # `child`/`CHILD` looks like an alias on a case-insensitive FS, but
        # the new logic doesn't trust child True — the result comes from the
        # temp-probe path below.
        return os.path.basename(a).lower() == os.path.basename(b).lower()

    def fake_samefile_or_false(a, b):
        # Temp-probe confirmation: mimic case-insensitive FS at `target`.
        return (
            os.path.dirname(a) == target_str
            and os.path.dirname(b) == target_str
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_tristate)
    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile_or_false)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is True


@pytest.mark.skipif(
    os.name == "nt" or _TMP_FOLDS_CASE,
    reason="Setup creates `foo` and `FOO` as distinct sibling files; only "
           "possible on a case-sensitive parent FS (not Windows / macOS APFS).",
)
def test_is_case_insensitive_path_child_alias_confirmed_by_temp_probe(
    tmp_path, monkeypatch,
):
    """On a case-SENSITIVE POSIX FS, `samefile` can be True for two distinct
    child entries when they are hard links or symlink aliases (e.g. `foo`
    and `FOO` both pointing to the same inode by user choice). Trusting that
    child True would misclassify Linux ext4 as case-insensitive and let
    `_tracked_destination_overlap` refuse a valid move into `dst/src` just
    because a stale row `Dst/src` exists. The temp-dir probe is the
    confirmation: it creates a fresh entry whose flipped spelling can't be
    a pre-existing alias.
    """
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "foo").touch()
    (target / "FOO").touch()  # pretend these are aliases for samefile
    target_str = str(target)

    def fake_tristate(a, b):
        # Pre-existing children look aliased (matches a user hard-link /
        # symlink setup on a case-sensitive FS).
        names = {os.path.basename(a), os.path.basename(b)}
        if names == {"foo", "FOO"}:
            return True
        return None

    # Temp probe uses the real `_samefile_or_false`; on real case-sensitive
    # POSIX tmp_path the probe and its flipped spelling do NOT resolve to
    # the same inode, so the final answer is False.
    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_tristate)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is False

    # Probe dir cleaned up — only the originals remain.
    assert sorted(os.listdir(target)) == ["FOO", "foo"]


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_child_scan_short_circuits_on_definitive_false(
    tmp_path, monkeypatch,
):
    """When the temp probe can't write (read-only ancestor), the child-scan
    fallback returns False as soon as it sees a case-twin pair with
    distinct inodes (`samefile` == False) — impossible on a case-folding
    FS — without statting every other entry under the ancestor."""
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "alpha").touch()
    (target / "ALPHA").touch()
    target_str = str(target)

    def fake_mkdtemp(*_a, **_kw):
        # Simulate a read-only ancestor so the child-scan fallback runs.
        raise OSError("read-only — exercise child-scan fallback")

    def fake_tristate(a, b):
        # Mimic real case-sensitive FS: alpha and ALPHA are distinct files
        # and samefile returns False on the pair.
        names = {os.path.basename(a), os.path.basename(b)}
        if names == {"alpha", "ALPHA"}:
            return False
        return None

    monkeypatch.setattr(move_mod.tempfile, "mkdtemp", fake_mkdtemp)
    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_tristate)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is False


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_temp_probe_runs_before_child_scan(
    tmp_path, monkeypatch,
):
    """The temp probe is conclusive in both directions with O(1) syscalls,
    so it must run first when the ancestor is writable — the child-scan
    fallback is read-only and only worthwhile when mkdtemp can't write.
    Before this ordering, `move_folder()` re-stat'd every entry under the
    destination's deepest existing ancestor on every move (the per-entry
    samefile probe is inconclusive on a typical case-sensitive directory
    with no case-twin children), turning the preflight guard into
    O(entries) wasted syscalls per move on big photo trees.
    """
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    # Many letter-bearing entries — the OLD scan-first order would call
    # _samefile_tristate once per entry before reaching the temp probe.
    for i in range(50):
        (target / f"entry_{i}").touch()

    call_order = []
    real_mkdtemp = move_mod.tempfile.mkdtemp
    real_tristate = move_mod._samefile_tristate

    def tracked_mkdtemp(*a, **kw):
        call_order.append("mkdtemp")
        return real_mkdtemp(*a, **kw)

    def tracked_tristate(a, b):
        # Only count per-entry child-scan probes against pre-existing
        # entries; ignore the temp probe's own samefile call (which goes
        # through _samefile_or_false → _samefile_tristate).
        if not (os.path.basename(a).startswith(".vireo_case_probe_")
                or os.path.basename(b).startswith(".vireo_case_probe_")):
            call_order.append("tristate")
        return real_tristate(a, b)

    monkeypatch.setattr(move_mod.tempfile, "mkdtemp", tracked_mkdtemp)
    monkeypatch.setattr(move_mod, "_samefile_tristate", tracked_tristate)

    # Real tmp_path is case-sensitive on Linux CI; the temp probe creates
    # a fresh dir whose flipped name doesn't exist, so samefile raises and
    # `_samefile_or_false` returns False. That answer is final, with zero
    # child-scan calls — the regression we're guarding against.
    move_mod._is_case_insensitive_path(
        os.path.join(str(target), "missing_leaf")
    )

    assert "mkdtemp" in call_order, "temp probe should have run"
    assert "tristate" not in call_order, (
        f"child-scan probes should not run when temp probe succeeds; "
        f"call order was {call_order}"
    )


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_all_inconclusive_falls_back_to_temp_probe(
    tmp_path, monkeypatch,
):
    """When every letter-bearing child's probe is inconclusive (every
    flipped spelling raises), the scan must fall through to the temp-dir
    probe rather than declaring the FS case-sensitive off the inconclusive
    children. Mirrors the no-letter-children fallback for the case where
    letter children exist but can't be resolved.
    """
    import move as move_mod

    target = tmp_path / "dir"
    target.mkdir()
    (target / "broken1").touch()
    (target / "broken2").touch()
    target_str = str(target)

    def fake_tristate(a, b):
        # Every child probe inconclusive — but the temp-probe fallback
        # uses `_samefile_or_false`, which we leave alone so the real
        # probe-dir path runs. With the case-insensitive simulation
        # below, the temp probe should return True.
        return None

    def fake_samefile_or_false(a, b):
        # Mimic case-insensitive FS at `target`: anything whose dirname
        # is `target` and whose basenames match case-folded is "same".
        return (
            os.path.dirname(a) == target_str
            and os.path.dirname(b) == target_str
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    monkeypatch.setattr(move_mod, "_samefile_tristate", fake_tristate)
    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile_or_false)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing_leaf")
    ) is True

    # Probe dir cleaned up — only the originals remain.
    assert sorted(os.listdir(target)) == ["broken1", "broken2"]


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_no_letter_children_falls_back_to_temp_probe(
    tmp_path, monkeypatch,
):
    """When no existing child has a letter to flip, the probe creates its
    own temp dir inside the ancestor and asks samefile whether the
    case-flipped spelling resolves to the same inode. The probe result is
    returned, and the probe dir must be cleaned up so the destination is left
    as the function found it."""
    import move as move_mod

    target = tmp_path / "digits"
    target.mkdir()
    (target / "123").touch()
    (target / "456").mkdir()
    monkeypatch.setattr(move_mod, "_samefile_or_false", lambda _a, _b: False)

    assert move_mod._is_case_insensitive_path(
        os.path.join(str(target), "missing")
    ) is False

    # No leftover probe dir — listdir still sees only the originals.
    assert sorted(os.listdir(target)) == ["123", "456"]


def test_is_case_insensitive_path_empty_ancestor_detects_case_insensitive_fs(
    tmp_path, monkeypatch,
):
    """A fresh case-insensitive POSIX destination tree (default macOS APFS at
    /Volumes/Photos with nothing in it yet) must still be classified as
    case-insensitive, so a stale tracked row like /Photos/Dst/src vs a
    move into /Photos/dst/src is still caught by the case-folded overlap
    check. Before the temp-probe fallback, an empty ancestor returned
    False unconditionally and let the stale alias slip through, leaving
    two folder rows managing the same on-disk tree.
    """
    import move as move_mod

    target = tmp_path / "empty_dest_root"
    target.mkdir()
    target_str = str(target)

    def fake_samefile(a, b):
        # Mimic case-insensitive FS at `target`: any two sibling names
        # under it that differ only by case resolve to the same inode.
        return (
            os.path.dirname(a) == target_str
            and os.path.dirname(b) == target_str
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile)

    assert move_mod._is_case_insensitive_path(
        os.path.join(target_str, "missing", "sub")
    ) is True

    # Probe dir cleaned up — only originals (none, here) remain.
    assert os.listdir(target) == []


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_empty_ancestor_unwritable_returns_false(tmp_path):
    """If the deepest existing ancestor is empty AND read-only, the
    temp-probe fallback can't write a probe dir. Return False rather than
    guessing — spuriously folding case could collapse two genuinely
    distinct paths."""
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root bypasses dir-write permissions")
    import move as move_mod

    target = tmp_path / "ro_empty"
    target.mkdir()
    original_mode = target.stat().st_mode
    os.chmod(target, 0o500)  # readable+executable, not writable
    try:
        assert move_mod._is_case_insensitive_path(
            os.path.join(str(target), "missing")
        ) is False
    finally:
        os.chmod(target, original_mode)


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows paths are treated case-insensitive without POSIX probing",
)
def test_is_case_insensitive_path_unreadable_ancestor_falls_back_to_temp_probe(
    tmp_path, monkeypatch,
):
    """A +wx (drop-box) ancestor without read permission can still receive
    a fresh temp dir, so an os.listdir denial must be inconclusive — fall
    through to the temp probe instead of declaring the FS case-sensitive.
    Otherwise a stale tracked row like /Photos/Dst/src can slip past the
    case-folded overlap check on a case-insensitive POSIX destination."""
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root bypasses dir-read permissions")
    import move as move_mod

    target = tmp_path / "dropbox"
    target.mkdir()
    target_str = str(target)

    def fake_samefile(a, b):
        return (
            os.path.dirname(a) == os.path.dirname(b)
            and os.path.basename(a).lower() == os.path.basename(b).lower()
        )

    monkeypatch.setattr(move_mod, "_samefile_or_false", fake_samefile)

    original_mode = target.stat().st_mode
    os.chmod(target, 0o300)  # write+execute, not readable
    try:
        with pytest.raises(OSError):
            os.listdir(target_str)
        assert move_mod._is_case_insensitive_path(
            os.path.join(target_str, "missing", "sub")
        ) is True
    finally:
        os.chmod(target, original_mode)

    # Probe dir cleaned up.
    assert os.listdir(target) == []


def test_move_folder_refuses_self_overlapping_destination(move_env):
    """A move whose resolved destination overlaps the source (here, the
    source's own parent → resolved dest == source) must be refused rather
    than no-op-copy then delete the source."""
    from move import move_folder

    env = move_env
    parent = str(env["src"].parent)  # resolve_folder_dest(...) == src path
    result = move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=parent, merge=True
    )
    assert result["moved"] == 0
    assert any("overlaps" in e for e in result["errors"])
    # Source folder fully intact.
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()


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
    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Verification failed" in e for e in result["errors"])
    # Originals preserved, pre-existing destination file untouched
    assert (env["src"] / "bird1.jpg").exists()
    assert sentinel.exists()


def test_move_folder_merge_rejects_symlinked_dest_file(move_env, monkeypatch):
    """A destination entry that is a symlink to the source file must fail
    verification, not be accepted as an independent copy. Otherwise rsync
    --ignore-existing leaves the symlink alone and the post-copy
    rmtree(src_path) would destroy the symlink's target — the only copy."""
    import move as move_mod

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    # Place real copies of the files we DON'T want the test to trip over,
    # so verification reaches the symlinked entry rather than failing on
    # the first plain-missing file.
    (landing / "bird1.xmp").write_bytes((env["src"] / "bird1.xmp").read_bytes())
    (landing / "bird2.jpg").write_bytes((env["src"] / "bird2.jpg").read_bytes())
    # bird1.jpg at dest is a SYMLINK back to the source file.
    try:
        os.symlink(str(env["src"] / "bird1.jpg"), str(landing / "bird1.jpg"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    # Force rsync to no-op (it would normally honor --ignore-existing and
    # leave the symlink anyway; this just removes the dependency on rsync).
    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Verification failed" in e and "bird1.jpg" in e
               for e in result["errors"])
    # The source file MUST still exist — that's the safety property.
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird1.jpg").read_bytes() != b""


def test_move_folder_merge_rejects_symlinked_dest_subdir(move_env, monkeypatch):
    """A destination subdirectory that is a symlink back into the source
    tree must also fail verification. os.path.isfile/getsize on the joined
    path would silently follow the link to the source's own bytes and
    pass — and then rmtree(src_path) would destroy the only copy."""
    import move as move_mod

    env = move_env
    # Reshape source: put bird1 inside a real subdirectory.
    sub_src = env["src"] / "sub"
    sub_src.mkdir()
    (env["src"] / "bird1.jpg").rename(sub_src / "bird1.jpg")
    (env["src"] / "bird1.xmp").rename(sub_src / "bird1.xmp")

    landing = env["dst"] / "src"
    landing.mkdir()
    # Real copy of the file in src root (bird2) so verification reaches
    # the symlinked subdirectory entries.
    (landing / "bird2.jpg").write_bytes((env["src"] / "bird2.jpg").read_bytes())
    # The "sub" dir at the destination is a SYMLINK back into the source.
    try:
        os.symlink(str(sub_src), str(landing / "sub"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Verification failed" in e for e in result["errors"])
    # Source files still present at the real path — not destroyed via the
    # symlinked-parent shortcut.
    assert (sub_src / "bird1.jpg").exists()


def test_move_folder_merge_rejects_broken_symlink(move_env, monkeypatch):
    """A broken symlink at the destination (lexists True, isfile False) must
    also fail verification, not be silently accepted as missing-then-fine."""
    import move as move_mod

    env = move_env
    landing = env["dst"] / "src"
    landing.mkdir()
    (landing / "bird1.xmp").write_bytes((env["src"] / "bird1.xmp").read_bytes())
    (landing / "bird2.jpg").write_bytes((env["src"] / "bird2.jpg").read_bytes())
    # Broken symlink — target doesn't exist.
    try:
        os.symlink(str(env["tmp_path"] / "nope.jpg"), str(landing / "bird1.jpg"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]), merge=True
    )
    assert result["moved"] == 0
    assert any("Verification failed" in e for e in result["errors"])
    assert (env["src"] / "bird1.jpg").exists()


def test_resolve_folder_dest():
    """resolve_folder_dest places the folder inside the destination."""
    from move import resolve_folder_dest

    # Compare against os.path.join so the expectation is platform-correct
    # (Windows joins with a backslash).
    assert resolve_folder_dest("/a/birds", "birds", "/nas/photos") == \
        os.path.join("/nas/photos", "birds")
    # Falls back to basename when name is empty
    assert resolve_folder_dest("/a/birds/", "", "/nas/photos") == \
        os.path.join("/nas/photos", "birds")
    # An explicit final name supports rename-while-moving.
    assert resolve_folder_dest(
        "/a/12", "12", "/nas/photos/2026", "2026-07-12"
    ) == os.path.join("/nas/photos/2026", "2026-07-12")


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


def test_move_folder_reports_phases_and_per_file_progress(move_env):
    """move_folder streams the move through named phases and reports each
    file as it is copied, instead of one progress call at the end."""
    from move import move_folder

    env = move_env
    calls = []
    result = move_folder(
        db=env["db"],
        folder_id=env["fid_src"],
        destination=str(env["dst"]),
        progress_cb=lambda cur, tot, fn, phase: calls.append(
            (cur, tot, fn, phase)
        ),
    )
    assert result["errors"] == []
    phases = [c[3] for c in calls]
    # The move walks through these phases in order.
    for expected in ("Checking destination", "Copying files",
                     "Verifying copy", "Updating catalog",
                     "Removing originals", "Done"):
        assert expected in phases, f"missing phase {expected!r} in {phases}"

    # The copy phase reports a real denominator (3 files: 2 jpgs + 1 xmp)
    # and at least one per-file update naming the file being copied.
    copy_calls = [c for c in calls if c[3] == "Copying files"]
    assert copy_calls, "no copy-phase progress reported"
    total = copy_calls[-1][1]
    assert total == 3
    named = [c for c in copy_calls if c[0] > 0 and c[2]]
    assert named, "no per-file progress during copy"
    assert all(c[1] == total for c in copy_calls)


def test_move_folder_progress_shutil_fallback(move_env, monkeypatch):
    """When rsync is unavailable, the shutil fallback still reports per-file
    copy progress through the same phase contract."""
    import move as move_mod

    def _no_rsync(*a, **k):
        raise FileNotFoundError("rsync")

    monkeypatch.setattr(move_mod.subprocess, "Popen", _no_rsync)

    env = move_env
    calls = []
    result = move_mod.move_folder(
        db=env["db"],
        folder_id=env["fid_src"],
        destination=str(env["dst"]),
        progress_cb=lambda cur, tot, fn, phase: calls.append(
            (cur, tot, fn, phase)
        ),
    )
    assert result["errors"] == []
    assert (env["dst"] / "src" / "bird1.jpg").exists()
    copy_calls = [c for c in calls if c[3] == "Copying files" and c[0] > 0]
    assert copy_calls, "shutil fallback reported no per-file progress"
    assert copy_calls[-1][1] == 3  # same 3-file denominator


def test_move_folder_prefers_discovered_gnu_rsync(move_env, monkeypatch):
    """Local NAS moves use discovered GNU rsync instead of a Finder app's
    bare ``rsync`` resolving to macOS openrsync."""
    import move as move_mod

    env = move_env
    captured = {}
    monkeypatch.setattr(move_mod.sys, "platform", "darwin")
    monkeypatch.setattr(
        move_mod, "resolve_rsync_bin", lambda configured="": "/gnu/rsync",
    )

    def fake_run(*args, **kwargs):
        captured["rsync_bin"] = kwargs.get("rsync_bin")
        return 1, "simulated failure", False

    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_run)
    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"],
        destination=str(env["dst"]),
    )

    assert captured["rsync_bin"] == "/gnu/rsync"
    assert result["moved"] == 0


def test_move_folder_windows_skips_discovered_rsync(move_env, monkeypatch):
    """Native Windows paths must not be passed to auto-discovered POSIX rsync."""
    import move as move_mod

    env = move_env
    captured = {}
    monkeypatch.setattr(move_mod.sys, "platform", "win32")

    def unexpected_resolve(configured=""):
        raise AssertionError("Windows local moves must not discover rsync")

    monkeypatch.setattr(move_mod, "resolve_rsync_bin", unexpected_resolve)

    def fake_run(*args, **kwargs):
        captured["rsync_bin"] = kwargs.get("rsync_bin")
        return 1, "simulated failure", False

    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_run)
    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"],
        destination=str(env["dst"]),
    )

    assert captured["rsync_bin"] == "rsync"
    assert result["moved"] == 0


def test_move_folder_stall_preserves_rsync_diagnostic(move_env, monkeypatch):
    """A watchdog timeout includes stderr's filename/root cause instead of
    replacing it with a generic 30-minute stall message."""
    import move as move_mod

    env = move_env
    monkeypatch.setattr(
        move_mod,
        "_run_rsync_streamed",
        lambda *args, **kwargs: (
            -9,
            "rsync: DSC_2042.NEF: file truncated while hashing\n",
            True,
        ),
    )

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"],
        destination=str(env["dst"]),
    )

    assert result["moved"] == 0
    assert "stalled" in result["errors"][0]
    assert "DSC_2042.NEF" in result["errors"][0]
    assert "file truncated while hashing" in result["errors"][0]


def test_move_folder_shutil_fallback_preserves_dir_symlink(move_env, monkeypatch):
    """The shutil fallback (rsync unavailable) must not silently drop a
    symlinked subdirectory. os.walk doesn't recurse into one, so without
    explicit handling its contents would never reach the destination yet the
    count verification would still pass and delete the originals. The symlink
    must be recreated at the destination, matching rsync -a."""
    import move as move_mod

    env = move_env
    # A symlinked subdirectory inside the source pointing at an external dir.
    external = env["tmp_path"] / "external"
    external.mkdir()
    (external / "target.txt").write_text("payload")
    try:
        os.symlink(str(external), str(env["src"] / "linkdir"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(move_mod.subprocess, "Popen",
                        lambda *a, **k: (_ for _ in ()).throw(
                            FileNotFoundError("rsync")))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    assert result["errors"] == []
    landing = env["dst"] / "src"
    link = landing / "linkdir"
    # The symlink is preserved as a symlink, still resolving to the payload.
    assert link.is_symlink()
    assert (link / "target.txt").read_text() == "payload"
    # External target untouched; source removed after a verified copy.
    assert (external / "target.txt").exists()
    assert not env["src"].exists()


def test_move_folder_shutil_fallback_aborts_on_unreadable_subdir(move_env, monkeypatch):
    """If the shutil fallback can't read a source subdirectory, it must abort
    the move rather than silently skip the subtree. The count verification
    would otherwise also skip it (same default walk), match, and delete the
    originals leaving the destination incomplete."""
    import move as move_mod

    env = move_env
    sub = env["src"] / "sub"
    sub.mkdir()
    (sub / "nest.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 30)

    monkeypatch.setattr(move_mod.subprocess, "Popen",
                        lambda *a, **k: (_ for _ in ()).throw(
                            FileNotFoundError("rsync")))

    # Force os.walk to surface a scandir error for the subdirectory, as it
    # would on a permission failure (default os.walk swallows this).
    real_walk = move_mod.os.walk

    def _walk_with_error(path, *a, **k):
        onerror = k.get("onerror")
        for root, dirs, files in real_walk(path, *a, **k):
            if onerror is not None and os.path.basename(root) == "sub":
                onerror(OSError(13, "Permission denied", str(sub)))
            yield root, dirs, files

    monkeypatch.setattr(move_mod.os, "walk", _walk_with_error)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    # Move aborted, originals preserved.
    assert result["moved"] == 0
    assert any("Copy failed" in e for e in result["errors"])
    assert env["src"].exists()
    assert (env["src"] / "bird1.jpg").exists()


def test_move_folder_shutil_fallback_preserves_dir_mode(move_env, monkeypatch):
    """The shutil fallback must preserve directory permissions on a fresh
    move (matching rsync -a / the old copytree). os.makedirs alone would
    drop a private 0700 folder down to the umask default before the source
    is deleted, permanently losing the metadata."""
    import move as move_mod

    if os.name == "nt":
        pytest.skip("POSIX directory modes not meaningful on Windows")

    env = move_env
    sub = env["src"] / "private"
    sub.mkdir()
    (sub / "secret.jpg").write_bytes(b"\xff\xd8" + b"\x00" * 20)
    os.chmod(str(sub), 0o700)

    monkeypatch.setattr(move_mod.subprocess, "Popen",
                        lambda *a, **k: (_ for _ in ()).throw(
                            FileNotFoundError("rsync")))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    assert result["errors"] == []
    dest_sub = env["dst"] / "src" / "private"
    assert dest_sub.is_dir()
    assert (os.stat(str(dest_sub)).st_mode & 0o777) == 0o700
    assert not env["src"].exists()


def test_move_folder_shutil_fallback_preserves_file_symlink(move_env, monkeypatch):
    """The shutil fallback must preserve a symlinked file as a symlink rather
    than dereferencing it through copy2 (matching rsync -a). Dereferencing
    would silently replace the link with its target's bytes and could write
    through a symlinked destination entry."""
    import move as move_mod

    env = move_env
    external = env["tmp_path"] / "ext_target.txt"
    external.write_text("external payload")
    try:
        os.symlink(str(external), str(env["src"] / "alias.txt"))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this platform")

    monkeypatch.setattr(move_mod.subprocess, "Popen",
                        lambda *a, **k: (_ for _ in ()).throw(
                            FileNotFoundError("rsync")))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"])
    )
    assert result["errors"] == []
    dest_link = env["dst"] / "src" / "alias.txt"
    assert dest_link.is_symlink()
    # samefile compares by inode/device, so it tolerates the `\\?\` extended-length
    # prefix Windows stamps onto absolute symlink targets returned by os.readlink.
    assert os.path.samefile(str(dest_link), str(external))
    # External target untouched; source removed after a verified copy.
    assert external.read_text() == "external payload"
    assert not env["src"].exists()


def test_move_folder_fresh_move_detects_late_source_file(move_env, monkeypatch):
    """A file added to the source after the upfront file count but before
    verification — a concurrent writer race rsync's scan missed — must be
    detected before `shutil.rmtree(src_path)` would silently delete it.

    Trusting the upfront `total_files` as the source count makes this
    silently incorrect: src now holds total_files+1 files, rsync copied
    only total_files of them, dst_count equals total_files, and a stale
    count compare passes — then rmtree wipes the new source file.
    Verification must re-examine the source against the destination
    instead of reusing the pre-copy count."""
    import shutil as _shutil

    import move as move_mod

    env = move_env
    landing = env["dst"] / "src"
    assert not landing.exists()  # fresh move (no merge path)

    def _copy_then_inject(src_path, dest_path, *args, **kwargs):
        # Stand in for rsync: copy the tree as it looked when scanning began,
        # then simulate a concurrent writer adding a new file to the source
        # before verification runs.
        _shutil.copytree(src_path, dest_path)
        (env["src"] / "late_arrival.jpg").write_bytes(b"new content")
        return 0, "", False

    monkeypatch.setattr(move_mod, "_run_rsync_streamed", _copy_then_inject)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid_src"], destination=str(env["dst"]),
    )
    # The move must abort — assert the safety property (originals survive,
    # late arrival is not silently deleted) without coupling to the exact
    # error wording. A count-mismatch fix surfaces this differently from a
    # per-source-file presence check; both are valid signals.
    assert result["moved"] == 0
    assert result["errors"], "expected an error explaining why verification failed"
    assert (env["src"] / "late_arrival.jpg").exists(), \
        "the late source file was silently deleted"
    assert (env["src"] / "late_arrival.jpg").read_bytes() == b"new content"
    # The pre-existing source files must also still be present — a fresh
    # move that aborts at verification preserves originals.
    assert (env["src"] / "bird1.jpg").exists()
    assert (env["src"] / "bird2.jpg").exists()


class _FakeStderr:
    """rsync stderr that's empty: iterating it yields no lines, so the drain
    loop (for line in proc.stderr) ends immediately."""

    def __iter__(self):
        return iter(())


class _SilentStdout:
    """stdout that never yields a line and blocks until the process is
    killed — simulates a wedged rsync producing no progress."""

    def __init__(self, killed):
        self._killed = killed

    def __iter__(self):
        return self

    def __next__(self):
        # Block until the watchdog kills the process, then end the stream.
        self._killed.wait(timeout=5)
        raise StopIteration


class _StreamingStdout:
    """stdout that emits ``n`` file lines with a small gap between each, then
    ends — simulates a slow-but-progressing transfer."""

    def __init__(self, n, gap):
        self._remaining = n
        self._gap = gap

    def __iter__(self):
        return self

    def __next__(self):
        if self._remaining <= 0:
            raise StopIteration
        time.sleep(self._gap)
        self._remaining -= 1
        return f"DSC_{self._remaining}.NEF\n"


class _FakeProc:
    def __init__(self, stdout, returncode=0):
        self.killed = threading.Event()
        self.stdout = stdout
        self.stderr = _FakeStderr()
        self.returncode = returncode

    def kill(self):
        self.returncode = -9
        self.killed.set()

    def wait(self):
        # The streamed read loop only reaches wait() after stdout iteration
        # has ended, so the fake "process" is already done — return at once.
        return self.returncode


def test_rsync_stall_watchdog_kills_silent_process(monkeypatch):
    """A rsync that produces no output for longer than the stall window is
    treated as wedged and killed, with timed_out=True — even though no
    total-runtime cap was hit."""
    import move as move_mod

    proc_holder = {}

    def _fake_popen(*_a, **_k):
        proc = _FakeProc.__new__(_FakeProc)
        proc.killed = threading.Event()
        proc.stdout = _SilentStdout(proc.killed)
        proc.stderr = _FakeStderr()
        proc.returncode = 0
        proc_holder["proc"] = proc
        return proc

    monkeypatch.setattr(move_mod.subprocess, "Popen", _fake_popen)

    rc, stderr, timed_out = move_mod._run_rsync_streamed(
        "/src", "/dst", ["--checksum"], 10, None, stall_timeout=0.3,
    )
    assert timed_out is True
    assert proc_holder["proc"].killed.is_set()


def test_rsync_streamed_runs_as_long_as_it_progresses(monkeypatch):
    """A slow transfer that keeps emitting files past the stall window is NOT
    killed: each transferred file resets the stall clock, so a copy can run
    far longer than stall_timeout as long as it keeps moving data."""
    import move as move_mod

    def _fake_popen(*_a, **_k):
        # 12 files, one every 0.1s = 1.2s total — comfortably past the
        # 1.0s stall window, so the runtime exceeds it and the watchdog
        # would fire if progress didn't reset the clock. The 10x gap of
        # per-file jitter margin (0.1s emit vs 1.0s window) keeps the
        # test robust against CI runner scheduling stalls that pushed
        # the previous 0.1s-vs-0.3s spacing over the edge on macOS.
        return _FakeProc(_StreamingStdout(n=12, gap=0.1))

    monkeypatch.setattr(move_mod.subprocess, "Popen", _fake_popen)

    seen = []
    rc, stderr, timed_out = move_mod._run_rsync_streamed(
        "/src", "/dst", ["--ignore-existing"], 12,
        lambda cur, tot, name, phase: seen.append(name),
        stall_timeout=1.0,
    )
    assert timed_out is False
    assert rc == 0
    assert len(seen) == 12  # progress reported for every transferred file


@pytest.mark.skipif(not hasattr(os, "openpty"), reason="pty is POSIX-only")
def test_rsync_streamed_survives_block_buffered_rsync(tmp_path):
    """A real child that block-buffers stdout (as Apple's openrsync does when
    writing to a pipe) must still stream per-file lines to the parent and
    survive the stall watchdog. Regression test: over a pipe, openrsync's
    --out-format lines all arrive in one burst at exit, so the watchdog saw
    pure silence and killed healthy NAS transfers slower than the stall
    window. The pty keeps the child line-buffered."""
    import move as move_mod

    fake_rsync = tmp_path / "fake_rsync"
    fake_rsync.write_text(
        "#!/usr/bin/env python3\n"
        "import sys, time\n"
        "for i in range(4):\n"
        # No flush: block-buffered when stdout is a pipe, line-buffered
        # when stdout is a tty — same behavior split as openrsync.
        "    sys.stdout.write('DSC_%04d.NEF\\n' % i)\n"
        "    time.sleep(0.4)\n"
    )
    fake_rsync.chmod(0o755)

    seen = []
    rc, stderr, timed_out = move_mod._run_rsync_streamed(
        str(tmp_path / "src"), str(tmp_path / "dst"), [], 4,
        lambda cur, tot, name, phase: seen.append(name),
        rsync_bin=str(fake_rsync),
        # Total runtime (4 x 0.4s = 1.6s) exceeds the stall window, so the
        # watchdog only stays quiet if lines genuinely stream one by one.
        stall_timeout=1.2,
    )
    assert timed_out is False
    assert rc == 0
    assert seen == [f"DSC_{i:04d}.NEF" for i in range(4)]


@pytest.mark.skipif(not hasattr(os, "openpty"), reason="pty is POSIX-only")
def test_rsync_streamed_closes_pty_fds_when_popen_fails(monkeypatch):
    """If Popen raises after os.openpty() succeeded (e.g. a bad rsync_bin),
    both pty fds must be closed before the exception propagates — otherwise
    every failed invocation leaks two fds and eventually exhausts the table.
    """
    import move as move_mod

    opened = []
    real_openpty = os.openpty

    def _tracking_openpty():
        master, slave = real_openpty()
        opened.extend((master, slave))
        return master, slave

    monkeypatch.setattr(move_mod.os, "openpty", _tracking_openpty)

    def _boom(*_a, **_k):
        raise FileNotFoundError("no such rsync binary")

    monkeypatch.setattr(move_mod.subprocess, "Popen", _boom)

    with pytest.raises(FileNotFoundError):
        move_mod._run_rsync_streamed(
            "/src", "/dst", [], 4, None, rsync_bin="/nonexistent/rsync",
        )

    assert len(opened) == 2  # openpty ran, so there are fds to worry about
    for fd in opened:
        with pytest.raises(OSError):
            os.fstat(fd)  # closed: fstat on a closed fd raises EBADF
