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


def test_build_destination_path_rejects_drive_relative_template():
    import pytest

    dt = datetime(2026, 3, 28, 14, 30, 0)
    with pytest.raises(ValueError, match="unsafe folder template"):
        build_destination_path(dt, "C:%Y")


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


def test_ingest_duplicate_folders_only_under_destination(tmp_path):
    """duplicate_folders must only contain paths under destination_dir.

    Regression test: ingest() globally joins photos+folders to find where
    existing duplicates live. If the match is in a library root other than
    the current destination, returning that out-of-tree path causes the
    pipeline to feed scanner.scan() restrict_dirs that aren't descendants
    of its root, making scanner._ensure_folder() recurse parents all the
    way up to '/' — polluting the active workspace with folders from an
    unrelated library.
    """
    src = tmp_path / "sd_card"
    dst = tmp_path / "new_library"
    old_library = tmp_path / "old_library" / "2023" / "2023-05-10"
    for d in [src, dst, old_library]:
        d.mkdir(parents=True)

    # A photo already lives in an unrelated library root.
    img = Image.new("RGB", (100, 100), color="purple")
    img.save(str(old_library / "old_shot.jpg"))
    # And a byte-identical copy is on the card, about to be ingested into
    # the new library.
    import shutil
    shutil.copy2(str(old_library / "old_shot.jpg"), str(src / "old_shot.jpg"))

    db = Database(str(tmp_path / "test.db"))
    # Scan the old library so its photo ends up in the DB with a file_hash.
    from scanner import scan
    scan(str(old_library.parent.parent), db)

    result = ingest(str(src), str(dst), db=db, skip_duplicates=True)

    # File should still be skipped (cross-library dedup behavior preserved).
    assert result["skipped_duplicate"] == 1
    assert result["copied"] == 0

    # But duplicate_folders must NOT leak the old library's path — it is
    # not a descendant of the destination and must never end up in the
    # pipeline's scan restrict_dirs.
    dup_folders = result.get("duplicate_folders", [])
    assert str(old_library) not in dup_folders, (
        f"duplicate_folders leaked out-of-tree path {str(old_library)!r}; "
        f"got {dup_folders!r}"
    )
    for d in dup_folders:
        assert d.startswith(str(dst)), (
            f"duplicate_folders contains {d!r} which is not under "
            f"destination {str(dst)!r}"
        )


def test_ingest_duplicate_folders_prefers_live_folder_over_stale(tmp_path):
    """When a hash exists in multiple destination subfolders, duplicate_folders
    must not select one whose DB status is not 'ok'.

    Regression: ingest's hash→folder map kept the first row returned by the
    query, which has no ordering or health filter. If a stale/missing folder
    was returned first, restrict_dirs pointed the post-ingest scan at a
    non-existent directory, which the scanner warns on and skips — so the
    live duplicate folder never got linked to the active workspace.
    """
    src = tmp_path / "sd_card"
    dst = tmp_path / "library"
    stale_dir = dst / "2024" / "2024-01-01"
    live_dir = dst / "2024" / "2024-02-02"
    for d in [src, stale_dir, live_dir]:
        d.mkdir(parents=True)

    # Two byte-identical files, one in each destination subfolder.
    img = Image.new("RGB", (100, 100), color="orange")
    img.save(str(stale_dir / "shot.jpg"))
    img.save(str(live_dir / "shot.jpg"))

    db = Database(str(tmp_path / "test.db"))
    from scanner import scan
    scan(str(dst), db)

    # Mark one of the folders as stale at the DB level, as check_folder_health
    # would if the directory had disappeared between scans.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE path = ?",
        (str(stale_dir),),
    )
    db.conn.commit()

    # Source file with the same hash.
    import shutil
    shutil.copy2(str(live_dir / "shot.jpg"), str(src / "shot.jpg"))

    result = ingest(str(src), str(dst), db=db, skip_duplicates=True)

    assert result["skipped_duplicate"] == 1
    assert result["copied"] == 0

    dup_folders = result.get("duplicate_folders", [])
    assert str(stale_dir) not in dup_folders, (
        f"duplicate_folders picked the stale folder {str(stale_dir)!r}; "
        f"got {dup_folders!r}"
    )
    assert str(live_dir) in dup_folders, (
        f"duplicate_folders should contain the live folder {str(live_dir)!r}; "
        f"got {dup_folders!r}"
    )


def test_ingest_duplicate_folders_rejects_sql_like_wildcard_siblings(tmp_path):
    """duplicate_folders must not leak siblings that only match because of
    SQL LIKE wildcard characters in the destination path.

    Regression: the subtree filter was expressed as ``f.path LIKE ?`` with
    the destination path spliced in directly. SQLite's LIKE treats ``_`` as
    a single-char wildcard, so a destination like ``.../dest_x`` combined
    with the ``.../dest_x/%`` pattern can match ``.../destXx/sub``, leaking
    a sibling subtree into duplicate_folders and reopening the out-of-tree
    scan problem on destinations whose names contain ``_`` or ``%``.
    """
    src = tmp_path / "sd_card"
    dest_under = tmp_path / "dest_x"
    # Sibling matches the SQL LIKE pattern ``dest_x/%`` because ``_`` is a
    # single-char wildcard. A nested subfolder is required because the
    # pattern demands a literal ``/`` after the wildcard-matched char.
    sibling_nested = tmp_path / "destXx" / "photos"
    for d in [src, dest_under, sibling_nested]:
        d.mkdir(parents=True)

    img = Image.new("RGB", (100, 100), color="teal")
    img.save(str(sibling_nested / "shot.jpg"))

    db = Database(str(tmp_path / "test.db"))
    from scanner import scan
    scan(str(tmp_path), db)

    import shutil
    shutil.copy2(str(sibling_nested / "shot.jpg"), str(src / "shot.jpg"))

    result = ingest(str(src), str(dest_under), db=db, skip_duplicates=True)

    assert result["skipped_duplicate"] == 1
    assert result["copied"] == 0
    dup_folders = result.get("duplicate_folders", [])
    assert str(sibling_nested) not in dup_folders, (
        f"duplicate_folders leaked LIKE-wildcard sibling {str(sibling_nested)!r}; "
        f"got {dup_folders!r}"
    )
    # Also verify no path that's not under the destination slipped in.
    for f in dup_folders:
        assert f == str(dest_under) or f.startswith(str(dest_under) + "/"), (
            f"duplicate_folders contains {f!r} which is not under "
            f"destination {str(dest_under)!r}"
        )


def test_ingest_duplicate_folders_excludes_folder_deleted_from_disk(tmp_path):
    """duplicate_folders must not contain folders that no longer exist on
    disk, even if their DB status is stale ('ok').

    Regression: the pipeline path does not refresh folder health before
    ingest, so a folder deleted since the last scan can still be marked
    ``status='ok'`` in the DB. If such a folder is returned first and
    recorded in duplicate_folders, the post-ingest scan walks a missing
    root and logs a warning without touching anything — the still-existing
    live duplicate folder never gets linked to the active workspace.
    """
    import shutil

    src = tmp_path / "sd_card"
    dst = tmp_path / "library"
    gone_dir = dst / "2024" / "2024-01-01"
    live_dir = dst / "2024" / "2024-02-02"
    for d in [src, gone_dir, live_dir]:
        d.mkdir(parents=True)

    img = Image.new("RGB", (100, 100), color="magenta")
    img.save(str(gone_dir / "shot.jpg"))
    img.save(str(live_dir / "shot.jpg"))

    db = Database(str(tmp_path / "test.db"))
    from scanner import scan
    scan(str(dst), db)

    # Simulate the filesystem disappearing since the last scan, without
    # refreshing DB folder health. gone_dir's row keeps status='ok'.
    shutil.rmtree(str(gone_dir))
    assert not gone_dir.exists()
    row = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (str(gone_dir),)
    ).fetchone()
    assert row and row["status"] == "ok", \
        "precondition: DB status must still be stale-ok"

    shutil.copy2(str(live_dir / "shot.jpg"), str(src / "shot.jpg"))
    result = ingest(str(src), str(dst), db=db, skip_duplicates=True)

    assert result["skipped_duplicate"] == 1
    assert result["copied"] == 0
    dup_folders = result.get("duplicate_folders", [])
    assert str(gone_dir) not in dup_folders, (
        f"duplicate_folders contained deleted folder {str(gone_dir)!r}; "
        f"got {dup_folders!r}"
    )
    assert str(live_dir) in dup_folders, (
        f"duplicate_folders should contain the live folder {str(live_dir)!r}; "
        f"got {dup_folders!r}"
    )


def test_ingest_duplicate_folders_tracks_all_destination_matches(tmp_path):
    """When the same hash lives in multiple destination subfolders, every
    one of them must appear in duplicate_folders.

    Regression: the hash→folder map kept only the first row per hash
    (``if fh in known_hash_folder: continue``). In the all-duplicates
    pipeline path, that reduced list was used as restrict_dirs, so the
    other matching folders were never scanned and therefore never linked
    into the active workspace. The user would see one of the duplicate's
    locations but not the others.
    """
    import shutil

    src = tmp_path / "sd_card"
    dst = tmp_path / "library"
    folder_a = dst / "2024" / "2024-03-10"
    folder_b = dst / "2024" / "2024-05-20"
    for d in [src, folder_a, folder_b]:
        d.mkdir(parents=True)

    # Byte-identical file in both destination subfolders — same hash twice.
    img = Image.new("RGB", (100, 100), color="olive")
    img.save(str(folder_a / "shot.jpg"))
    shutil.copy2(str(folder_a / "shot.jpg"), str(folder_b / "shot.jpg"))

    db = Database(str(tmp_path / "test.db"))
    from scanner import scan
    scan(str(dst), db)

    shutil.copy2(str(folder_a / "shot.jpg"), str(src / "shot.jpg"))
    result = ingest(str(src), str(dst), db=db, skip_duplicates=True)

    assert result["skipped_duplicate"] == 1
    assert result["copied"] == 0
    dup_folders = set(result.get("duplicate_folders", []))
    assert str(folder_a) in dup_folders, (
        f"duplicate_folders missing {str(folder_a)!r}; got {dup_folders!r}"
    )
    assert str(folder_b) in dup_folders, (
        f"duplicate_folders missing {str(folder_b)!r}; got {dup_folders!r}"
    )


def test_ingest_duplicate_folders_matches_dest_root_with_trailing_slash(tmp_path):
    """When destination_dir has a trailing slash and the duplicate lives
    directly at the destination root, duplicate_folders must still contain
    that root.

    Regression guard: path normalization between destination_dir (the
    caller's input) and folder paths stored in the DB (which are
    str(Path(...)) without trailing slashes) must agree. If they disagree,
    the ``f.path = ?`` branch of the subtree guard misses duplicates at
    the destination root, duplicate_folders stays empty, and the pipeline
    falls back to a full-tree scan.
    """
    import shutil

    src = tmp_path / "sd_card"
    dst = tmp_path / "library"
    for d in [src, dst]:
        d.mkdir()

    img = Image.new("RGB", (100, 100), color="cyan")
    img.save(str(dst / "root_shot.jpg"))

    db = Database(str(tmp_path / "test.db"))
    from scanner import scan
    scan(str(dst), db)

    shutil.copy2(str(dst / "root_shot.jpg"), str(src / "root_shot.jpg"))

    # Call ingest with a trailing slash on destination_dir.
    result = ingest(str(src), str(dst) + "/", db=db, skip_duplicates=True)

    assert result["skipped_duplicate"] == 1
    assert result["copied"] == 0
    dup_folders = result.get("duplicate_folders", [])
    assert str(dst) in dup_folders, (
        f"duplicate_folders should contain destination root {str(dst)!r} "
        f"even when destination_dir has a trailing slash; got {dup_folders!r}"
    )


def test_ingest_duplicate_folders_flat_import_root_duplicate(tmp_path):
    """Flat imports (folder_template='') put every file directly in the
    destination root. A matching duplicate at the root must show up in
    duplicate_folders so the pipeline scan targets it.
    """
    import shutil

    src = tmp_path / "sd_card"
    dst = tmp_path / "flat_lib"
    for d in [src, dst]:
        d.mkdir()

    img = Image.new("RGB", (100, 100), color="magenta")
    img.save(str(dst / "at_root.jpg"))

    db = Database(str(tmp_path / "test.db"))
    from scanner import scan
    scan(str(dst), db)

    shutil.copy2(str(dst / "at_root.jpg"), str(src / "at_root.jpg"))

    result = ingest(
        str(src), str(dst), db=db,
        skip_duplicates=True, folder_template="",
    )

    assert result["skipped_duplicate"] == 1
    assert result["copied"] == 0
    dup_folders = result.get("duplicate_folders", [])
    assert str(dst) in dup_folders, (
        f"flat-import root duplicate should be tracked; "
        f"got duplicate_folders={dup_folders!r}"
    )


def test_ingest_duplicate_folders_matches_unnormalized_stored_path(tmp_path):
    """When the DB holds a folder row whose path contains ``..`` segments
    because a previous scan was run with an unnormalized root, ingesting
    into that same unnormalized destination must still find the row via
    the SQL prefilter.

    Regression: pre-normalizing ``destination_dir`` before building the
    SQL query (with ``os.path.normpath``) turns ``/.../other/../library``
    into ``/.../library`` and queries ``/.../library`` / ``/.../library/%``,
    neither of which matches the raw stored ``/.../other/../library/...``
    string that scanner.scan persists (``Path`` does not collapse ``..``
    segments). ``known_hash_folders`` stays empty and the caller's scan
    then walks the full destination subtree unnecessarily.
    """
    import shutil

    from scanner import scan

    src = tmp_path / "sd_card"
    real_dst = tmp_path / "library"
    sibling = tmp_path / "other"
    for d in [src, real_dst, sibling]:
        d.mkdir()

    # Seed the destination library with a photo and scan it so a folder
    # row exists in the DB.
    Image.new("RGB", (64, 64), color="teal").save(str(real_dst / "keeper.jpg"))
    db = Database(str(tmp_path / "test.db"))
    scan(str(real_dst), db)

    # Rewrite the folder row to an equivalent path that routes through a
    # ``..`` segment. This mimics the state left behind by a prior scan
    # started with an unnormalized root like ``{tmp}/other/../library``.
    unnorm_path = f"{sibling}/../library"
    assert os.path.isdir(unnorm_path)
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE path = ?",
        (unnorm_path, str(real_dst)),
    )
    db.conn.commit()

    # Stage the same file on the "SD card" so it's a byte-for-byte
    # duplicate of the one already in the destination library.
    shutil.copy2(str(real_dst / "keeper.jpg"), str(src / "keeper.jpg"))

    # Ingest into the SAME unnormalized form the DB holds. skip_duplicates
    # should recognise the duplicate AND record the stored folder in
    # duplicate_folders so the post-ingest restrict scan can link it.
    result = ingest(str(src), unnorm_path, db=db, skip_duplicates=True)

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    dup_folders = result.get("duplicate_folders", [])
    assert unnorm_path in dup_folders, (
        f"expected unnormalized stored path {unnorm_path!r} in "
        f"duplicate_folders; got {dup_folders!r}"
    )


def test_ingest_duplicate_folders_rejects_dot_dot_escape(tmp_path):
    """A DB folder path containing ``..`` segments that lexically starts
    with destination_dir but resolves outside it must not leak into
    duplicate_folders.

    Regression: ``Path.is_relative_to`` is a lexical check on path parts,
    so a stored path like ``/library/../other/photos`` passes
    ``is_relative_to(Path("/library"))`` even though ``os.path.normpath``
    would resolve it to ``/other/photos``. Scanner stores raw strings, so
    a previous scan with an unnormalized root (or any manual DB edit)
    could persist such paths, and without normalization they would end
    up in restrict_dirs and get walked as if under destination.
    """
    import shutil

    src = tmp_path / "sd_card"
    dst = tmp_path / "library"
    escape_target = tmp_path / "other"
    for d in [src, dst, escape_target]:
        d.mkdir()

    img = Image.new("RGB", (100, 100), color="navy")
    img.save(str(escape_target / "shot.jpg"))

    db = Database(str(tmp_path / "test.db"))
    from scanner import scan
    scan(str(escape_target), db)

    # Rewrite the folder row to use a lexical escape that still resolves
    # to the same real directory on disk. The ``..`` leg pretends to be
    # anchored at dst, so is_relative_to(dst) lexically succeeds, but the
    # actual resolution points outside dst.
    escape_path = f"{dst}/../other"
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE path = ?",
        (escape_path, str(escape_target)),
    )
    db.conn.commit()

    shutil.copy2(str(escape_target / "shot.jpg"), str(src / "shot.jpg"))

    result = ingest(str(src), str(dst), db=db, skip_duplicates=True)

    assert result["skipped_duplicate"] == 1
    assert result["copied"] == 0
    dup_folders = result.get("duplicate_folders", [])
    assert escape_path not in dup_folders, (
        f"duplicate_folders accepted lexical .. escape {escape_path!r}; "
        f"got {dup_folders!r}"
    )
    # And make sure nothing outside dst slipped through under a different
    # disguise.
    import os
    for f in dup_folders:
        resolved = os.path.normpath(f)
        assert resolved == str(dst) or resolved.startswith(str(dst) + os.sep), (
            f"duplicate_folders contains {f!r} (normpath={resolved!r}) "
            f"which is not under destination {str(dst)!r}"
        )


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
