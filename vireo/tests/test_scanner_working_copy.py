"""Working copy extraction for large JPEGs."""
import os

from PIL import Image


def _make_jpeg(path, width, height):
    img = Image.new("RGB", (width, height), (128, 128, 128))
    img.save(path, "JPEG", quality=85)


def test_extract_working_copy_for_large_jpeg(tmp_path, monkeypatch):
    """A JPEG larger than working_copy_max_size gets a working copy created."""
    import config as cfg
    from db import Database
    from scanner import _extract_working_copies

    # Force a small max to avoid making huge fixture images.
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "photos"
    folder.mkdir()
    src = folder / "big.jpg"
    _make_jpeg(str(src), 2000, 1500)  # larger than 1000 cap

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    folder_id = db.add_folder(str(folder))
    photo_id = db.add_photo(
        folder_id, "big.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=2000, height=1500,
    )

    _extract_working_copies(db, str(vireo_dir))

    wc_path = vireo_dir / "working" / f"{photo_id}.jpg"
    assert wc_path.exists(), "working copy should be created for large JPEG"
    with Image.open(wc_path) as img:
        assert max(img.size) == 1000

    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id=?", (photo_id,)
    ).fetchone()
    assert row["working_copy_path"] == f"working/{photo_id}.jpg"


def test_no_jpeg_working_copy_when_max_size_zero(tmp_path, monkeypatch):
    """working_copy_max_size=0 disables JPEG working-copy extraction.

    Zero is the "full resolution" sentinel; without the guard the SQL
    predicate ``p.width > 0 OR p.height > 0`` matches every JPEG with known
    dimensions and produces an expensive full-size duplicate for each.
    """
    import config as cfg
    from db import Database
    from scanner import _extract_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 0, "working_copy_quality": 90})

    folder = tmp_path / "photos"
    folder.mkdir()
    src = folder / "big.jpg"
    _make_jpeg(str(src), 2000, 1500)

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    folder_id = db.add_folder(str(folder))
    photo_id = db.add_photo(
        folder_id, "big.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=2000, height=1500,
    )

    _extract_working_copies(db, str(vireo_dir))

    wc_path = vireo_dir / "working" / f"{photo_id}.jpg"
    assert not wc_path.exists()
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id=?", (photo_id,)
    ).fetchone()
    assert row["working_copy_path"] is None


def _seed_large_jpeg(db, folder, filename):
    """Make a large JPEG on disk, register it in `db`, return photo_id."""
    src = folder / filename
    _make_jpeg(str(src), 2000, 1500)
    folder_id = db.add_folder(str(folder))
    return db.add_photo(
        folder_id, filename, ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=2000, height=1500,
    )


def test_extract_working_copies_scope_restricts_to_given_folders(tmp_path, monkeypatch):
    """When `scope` is given, only photos in those folders get working copies."""
    import config as cfg
    from db import Database
    from scanner import _extract_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder_a = tmp_path / "a"
    folder_a.mkdir()
    folder_b = tmp_path / "b"
    folder_b.mkdir()

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    a_id = _seed_large_jpeg(db, folder_a, "a.jpg")
    b_id = _seed_large_jpeg(db, folder_b, "b.jpg")

    _extract_working_copies(db, str(vireo_dir), scope=[str(folder_a)])

    assert (vireo_dir / "working" / f"{a_id}.jpg").exists()
    assert not (vireo_dir / "working" / f"{b_id}.jpg").exists()

    rows = {
        r["id"]: r["working_copy_path"]
        for r in db.conn.execute(
            "SELECT id, working_copy_path FROM photos WHERE id IN (?, ?)",
            (a_id, b_id),
        ).fetchall()
    }
    assert rows[a_id] == f"working/{a_id}.jpg"
    assert rows[b_id] is None


def test_extract_working_copies_scope_matches_subtrees(tmp_path, monkeypatch):
    """Scope entries match their subtree — a photo in a subfolder is included."""
    import config as cfg
    from db import Database
    from scanner import _extract_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    parent = tmp_path / "parent"
    parent.mkdir()
    child = parent / "2026-04-20"
    child.mkdir()
    sibling = tmp_path / "sibling"
    sibling.mkdir()

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    child_id = _seed_large_jpeg(db, child, "c.jpg")
    sibling_id = _seed_large_jpeg(db, sibling, "s.jpg")

    _extract_working_copies(db, str(vireo_dir), scope=[str(parent)])

    assert (vireo_dir / "working" / f"{child_id}.jpg").exists()
    assert not (vireo_dir / "working" / f"{sibling_id}.jpg").exists()


def test_extract_working_copies_empty_scope_is_noop(tmp_path, monkeypatch):
    """scope=[] → nothing is extracted, even with eligible photos present."""
    import config as cfg
    from db import Database
    from scanner import _extract_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "photos"
    folder.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    photo_id = _seed_large_jpeg(db, folder, "big.jpg")

    _extract_working_copies(db, str(vireo_dir), scope=[])

    assert not (vireo_dir / "working" / f"{photo_id}.jpg").exists()
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id=?", (photo_id,)
    ).fetchone()
    assert row["working_copy_path"] is None


def test_subtree_like_pattern_posix():
    """Unix separator: LIKE pattern is the path followed by `/%`."""
    from scanner import _subtree_like_pattern
    assert _subtree_like_pattern("/photos/2024", sep="/") == "/photos/2024/%"


def test_subtree_like_pattern_windows_escapes_separator():
    r"""On Windows, the trailing `\` must be escape-doubled so `%` remains the
    wildcard under ``LIKE ? ESCAPE '\'`` — otherwise subtree matching silently
    matches only the exact folder.

    Input path `C:\a\b` with sep `\`:
      * every literal `\` in the path is doubled → `C:\\a\\b`
      * the trailing separator is also doubled → `\\`
      * the wildcard `%` is appended unescaped.
    """
    from scanner import _subtree_like_pattern
    assert _subtree_like_pattern("C:\\a\\b", sep="\\") == "C:\\\\a\\\\b\\\\%"


def test_subtree_like_pattern_escapes_literal_wildcards():
    """`_` and `%` inside folder names are escaped so they match literally."""
    from scanner import _subtree_like_pattern
    assert _subtree_like_pattern("/a/2024_06", sep="/") == "/a/2024\\_06/%"
    assert _subtree_like_pattern("/a/50%off", sep="/") == "/a/50\\%off/%"


def test_subtree_like_pattern_normalizes_trailing_separator():
    """Trailing separator in the scope path must not produce a double separator.

    Before this guard, `/photos/` produced `"//%"` and the root path `"/"`
    produced `"//%"` — neither matches any real descendant path.
    """
    from scanner import _subtree_like_pattern
    assert _subtree_like_pattern("/photos/", sep="/") == "/photos/%"
    assert _subtree_like_pattern("/photos///", sep="/") == "/photos/%"
    assert _subtree_like_pattern("/", sep="/") == "/%"
    assert _subtree_like_pattern("C:\\a\\", sep="\\") == "C:\\\\a\\\\%"


def test_extract_working_copies_scope_escapes_like_wildcards(tmp_path, monkeypatch):
    """An underscore in a scope path must not match unrelated siblings.

    SQLite LIKE treats `_` and `%` as wildcards. Without escaping, scoping to
    ``/photos/2024_06`` would also match a sibling like ``/photos/2024A06``.
    """
    import config as cfg
    from db import Database
    from scanner import _extract_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    wanted = tmp_path / "2024_06"
    wanted.mkdir()
    # Sibling whose path would match the naive `2024_06/%` pattern because `_`
    # is a LIKE wildcard. Both folders end in a directory separator boundary
    # so the tail matches a single arbitrary character.
    sibling = tmp_path / "2024A06"
    sibling.mkdir()
    (sibling / "sub").mkdir()

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    wanted_id = _seed_large_jpeg(db, wanted, "w.jpg")
    sibling_sub = sibling / "sub"
    sibling_id = _seed_large_jpeg(db, sibling_sub, "s.jpg")

    _extract_working_copies(db, str(vireo_dir), scope=[str(wanted)])

    assert (vireo_dir / "working" / f"{wanted_id}.jpg").exists()
    assert not (vireo_dir / "working" / f"{sibling_id}.jpg").exists(), (
        "wildcard `_` in wanted path leaked into sibling match"
    )


def test_scan_non_recursive_scopes_working_copies_to_root_only(tmp_path, monkeypatch):
    """scan(..., recursive=False) must not backfill working copies in subfolders.

    Regression: without honoring `recursive`, the derived scope used a subtree
    match that touched photos the caller explicitly chose not to walk.
    """
    import config as cfg
    from db import Database
    from scanner import scan

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))

    root = tmp_path / "scan"
    root.mkdir()
    # A large JPEG sitting at the scan root (on disk + to-be-scanned).
    _make_jpeg(str(root / "top.jpg"), 2000, 1500)

    # A pre-existing subfolder photo already in the DB that the caller does
    # NOT want touched because `recursive=False`.
    sub = root / "sub"
    sub.mkdir()
    sub_id = _seed_large_jpeg(db, sub, "in_sub.jpg")

    scan(str(root), db, recursive=False, vireo_dir=str(vireo_dir))

    top_row = db.conn.execute(
        "SELECT id, working_copy_path FROM photos WHERE filename='top.jpg'"
    ).fetchone()
    assert top_row is not None
    assert top_row["working_copy_path"] == f"working/{top_row['id']}.jpg"

    # Subfolder photo is outside the non-recursive scan; must NOT be touched.
    assert not (vireo_dir / "working" / f"{sub_id}.jpg").exists()
    sub_wc = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id=?", (sub_id,)
    ).fetchone()["working_copy_path"]
    assert sub_wc is None


def test_scan_scopes_working_copies_to_scan_root(tmp_path, monkeypatch):
    """scan() with a root only extracts working copies for photos under that root.

    Regression: before the fix, scan backfilled working copies library-wide,
    so a fresh import triggered full-size extraction for every pre-existing
    large JPEG in the DB — slow and unrelated to what was just scanned.
    """
    import config as cfg
    from db import Database
    from scanner import scan

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    # Pre-existing large JPEG in the DB, in a folder OUTSIDE the scan root.
    outside = tmp_path / "outside"
    outside.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    outside_id = _seed_large_jpeg(db, outside, "pre.jpg")

    # New folder inside the scan root with its own large JPEG on disk.
    scan_root = tmp_path / "scan"
    scan_root.mkdir()
    new_file = scan_root / "new.jpg"
    _make_jpeg(str(new_file), 2000, 1500)

    scan(str(scan_root), db, vireo_dir=str(vireo_dir))

    # The photo inside the scan root gets a working copy.
    inside_row = db.conn.execute(
        "SELECT id, working_copy_path FROM photos WHERE filename='new.jpg'"
    ).fetchone()
    assert inside_row is not None
    assert inside_row["working_copy_path"] == f"working/{inside_row['id']}.jpg"

    # The pre-existing photo outside the scan root is NOT touched.
    assert not (vireo_dir / "working" / f"{outside_id}.jpg").exists()
    outside_wc = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id=?", (outside_id,)
    ).fetchone()["working_copy_path"]
    assert outside_wc is None


def test_no_working_copy_for_small_jpeg(tmp_path, monkeypatch):
    """A JPEG within the cap does NOT get a working copy."""
    import config as cfg
    from db import Database
    from scanner import _extract_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "photos"
    folder.mkdir()
    src = folder / "small.jpg"
    _make_jpeg(str(src), 800, 600)  # below 1000

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    folder_id = db.add_folder(str(folder))
    photo_id = db.add_photo(
        folder_id, "small.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=800, height=600,
    )

    _extract_working_copies(db, str(vireo_dir))

    wc_path = vireo_dir / "working" / f"{photo_id}.jpg"
    assert not wc_path.exists()
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id=?", (photo_id,)
    ).fetchone()
    assert row["working_copy_path"] is None
