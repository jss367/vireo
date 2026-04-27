"""Working copy extraction for large JPEGs."""
import os

from PIL import Image


def _make_jpeg(path, width, height):
    img = Image.new("RGB", (width, height), (128, 128, 128))
    img.save(path, "JPEG", quality=85)


def _wait_for_backfill_terminal(runner, timeout=60.0, poll=0.05):
    """Poll runner.list_jobs() until a working_copy_backfill job appears
    and reaches a terminal status (``completed`` / ``failed``). Returns
    the job dict.

    Generous default timeout because full-suite test runs accumulate
    daemon threads, write-lock contention, and FS pressure that can
    stretch an otherwise sub-second backfill past tighter deadlines —
    causing order-dependent flakes (passes in isolation, fails after
    2k+ tests have run). Successful runs return immediately on the
    poll-rate cadence; the timeout only matters on actual hangs.

    Distinguishes "job never appeared" from "job never completed" so
    failures point at the right cause.
    """
    import time
    deadline = time.time() + timeout
    last_seen = None
    while time.time() < deadline:
        backfill_jobs = [
            j for j in runner.list_jobs()
            if j["type"] == "working_copy_backfill"
        ]
        if backfill_jobs:
            last_seen = backfill_jobs[0]
            if last_seen["status"] in ("completed", "failed"):
                return last_seen
        time.sleep(poll)
    if last_seen is None:
        raise AssertionError(
            f"working_copy_backfill job never appeared in runner.list_jobs() "
            f"within {timeout}s — kickoff likely never fired"
        )
    raise AssertionError(
        f"working_copy_backfill job appeared but did not reach terminal "
        f"status within {timeout}s; last seen status="
        f"{last_seen.get('status')!r}"
    )


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



# ---------------------------------------------------------------------------
# Candidate-count helper (drives the startup gate + before/after totals)
# ---------------------------------------------------------------------------


def test_candidate_count_excludes_small_jpegs(tmp_path, monkeypatch):
    """A row that the extractor would skip must not show up as a candidate.

    Small JPEGs (under ``working_copy_max_size``) are intentionally left
    without working copies, so a library of only small JPEGs has zero
    backfill work to do.
    """
    import config as cfg
    from db import Database
    from scanner import working_copy_backfill_candidate_count

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "photos"
    folder.mkdir()
    src = folder / "small.jpg"
    _make_jpeg(str(src), 800, 600)

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(folder))
    db.add_photo(
        folder_id, "small.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=800, height=600,
    )

    assert working_copy_backfill_candidate_count(db) == 0


def test_candidate_count_includes_large_jpeg(tmp_path, monkeypatch):
    """An oversized JPEG is a real backfill candidate."""
    import config as cfg
    from db import Database
    from scanner import working_copy_backfill_candidate_count

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "photos"
    folder.mkdir()
    src = folder / "big.jpg"
    _make_jpeg(str(src), 2000, 1500)

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(folder))
    db.add_photo(
        folder_id, "big.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=2000, height=1500,
    )

    assert working_copy_backfill_candidate_count(db) == 1


def test_candidate_count_includes_raw(tmp_path, monkeypatch):
    """RAW photos are always candidates regardless of recorded dimensions."""
    import config as cfg
    from db import Database
    from scanner import working_copy_backfill_candidate_count

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "photos"
    folder.mkdir()
    raw = folder / "shot.nef"
    raw.write_bytes(b"\x00" * 16)  # contents irrelevant for the SELECT

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(folder))
    db.add_photo(
        folder_id, "shot.nef", ".nef",
        file_size=os.path.getsize(str(raw)),
        file_mtime=os.path.getmtime(str(raw)),
        width=None, height=None,
    )

    assert working_copy_backfill_candidate_count(db) == 1


# ---------------------------------------------------------------------------
# Library-wide backfill (used by the startup self-healing job)
# ---------------------------------------------------------------------------


def test_backfill_processes_legacy_null_working_copy_path(tmp_path, monkeypatch):
    """``backfill_working_copies`` covers photos imported before the feature.

    Simulates a row that exists with ``working_copy_path=NULL`` from a prior
    scan that never had ``vireo_dir`` passed in. The new startup pass must
    pick it up library-wide (no ``scope`` argument).
    """
    import config as cfg
    from db import Database
    from scanner import backfill_working_copies

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

    result = backfill_working_copies(db, str(vireo_dir))

    # Both photos should now have a working copy on disk and in the DB.
    assert (vireo_dir / "working" / f"{a_id}.jpg").exists()
    assert (vireo_dir / "working" / f"{b_id}.jpg").exists()

    rows = {
        r["id"]: r["working_copy_path"]
        for r in db.conn.execute(
            "SELECT id, working_copy_path FROM photos WHERE id IN (?, ?)",
            (a_id, b_id),
        ).fetchall()
    }
    assert rows[a_id] == f"working/{a_id}.jpg"
    assert rows[b_id] == f"working/{b_id}.jpg"

    assert result["candidates"] == 2
    assert result["remaining"] == 0
    assert result["with_working_copy"] == 2


def test_backfill_skips_already_extracted(tmp_path, monkeypatch):
    """A photo that already has working_copy_path is not re-processed."""
    import config as cfg
    from db import Database
    from scanner import backfill_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "a"
    folder.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    pid = _seed_large_jpeg(db, folder, "a.jpg")

    # First pass creates the working copy.
    backfill_working_copies(db, str(vireo_dir))
    wc_path = vireo_dir / "working" / f"{pid}.jpg"
    first_mtime = wc_path.stat().st_mtime

    # Second pass: the row is no longer a candidate.
    result = backfill_working_copies(db, str(vireo_dir))
    assert result["candidates"] == 0
    # File is untouched (no rewrite).
    assert wc_path.stat().st_mtime == first_mtime


def test_backfill_failure_marker_prevents_retry_loop(tmp_path, monkeypatch):
    """A row whose extraction fails is marked and skipped on the next pass.

    Without this guard, every startup would re-attempt every broken file —
    an O(N) waste on each restart.
    """
    import config as cfg
    from db import Database
    from scanner import backfill_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "a"
    folder.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))

    # Register a row whose source file does NOT exist on disk —
    # extract_working_copy will fail.
    folder_id = db.add_folder(str(folder))
    pid = db.add_photo(
        folder_id, "missing.jpg", ".jpg",
        file_size=1000, file_mtime=42.0,
        width=2000, height=1500,
    )

    calls = {"n": 0}
    real = None

    import scanner as _scanner_mod

    def counting_extract(*args, **kwargs):
        calls["n"] += 1
        return False  # always fail

    monkeypatch.setattr(_scanner_mod, "extract_working_copy", counting_extract)

    backfill_working_copies(db, str(vireo_dir))
    assert calls["n"] == 1, "first pass should call extract once"

    row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_at,"
        " working_copy_failed_mtime FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row["working_copy_path"] is None
    assert row["working_copy_failed_at"] is not None
    assert row["working_copy_failed_mtime"] == 42.0

    # Second pass: candidate query must skip this row.
    backfill_working_copies(db, str(vireo_dir))
    assert calls["n"] == 1, "second pass must NOT retry a marked failure"


def test_backfill_failure_retries_when_mtime_changes(tmp_path, monkeypatch):
    """A user-replaced file (different mtime) clears the failure gate."""
    import config as cfg
    from db import Database
    from scanner import backfill_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "a"
    folder.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    folder_id = db.add_folder(str(folder))
    pid = db.add_photo(
        folder_id, "missing.jpg", ".jpg",
        file_size=1000, file_mtime=42.0,
        width=2000, height=1500,
    )

    calls = {"n": 0}

    import scanner as _scanner_mod

    def counting_extract(*args, **kwargs):
        calls["n"] += 1
        return False

    monkeypatch.setattr(_scanner_mod, "extract_working_copy", counting_extract)

    backfill_working_copies(db, str(vireo_dir))
    assert calls["n"] == 1

    # Simulate the user replacing the file: mtime changes.
    db.conn.execute(
        "UPDATE photos SET file_mtime=? WHERE id=?", (99.0, pid),
    )
    db.conn.commit()

    backfill_working_copies(db, str(vireo_dir))
    assert calls["n"] == 2, "mtime change must clear the failure gate"


def test_backfill_failure_retries_after_grace_period_elapses(tmp_path, monkeypatch):
    """A stale failure marker is bypassed even when the file mtime is unchanged.

    Regression: gating retries solely on ``working_copy_failed_mtime ==
    file_mtime`` permanently suppressed retries for transient failures
    (external drive temporarily disconnected at startup, brief I/O
    blip, etc.). Files whose source bytes never change would never get a
    second chance once that first failure was recorded — undermining the
    self-healing intent. The predicate now also bypasses the gate when the
    failure timestamp is older than the configured grace period.
    """
    import config as cfg
    from db import Database
    from scanner import backfill_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "a"
    folder.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))

    pid = _seed_large_jpeg(db, folder, "a.jpg")
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (pid,)
    ).fetchone()["file_mtime"]

    # Pretend the previous failure was recorded 48 hours ago against the
    # SAME file_mtime. Mtime equality alone would suppress the retry
    # forever; the time-based escape should override it.
    db.conn.execute(
        "UPDATE photos SET working_copy_failed_at = datetime('now', '-48 hours'),"
        " working_copy_failed_mtime = ?"
        " WHERE id = ?",
        (file_mtime, pid),
    )
    db.conn.commit()

    backfill_working_copies(db, str(vireo_dir))

    row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_at,"
        " working_copy_failed_mtime FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row["working_copy_path"] == f"working/{pid}.jpg"
    assert row["working_copy_failed_at"] is None
    assert row["working_copy_failed_mtime"] is None
    assert (vireo_dir / "working" / f"{pid}.jpg").exists()


def test_backfill_failure_does_not_retry_within_grace_period(tmp_path, monkeypatch):
    """A recent failure with unchanged mtime is still suppressed.

    Counterpart to ``test_backfill_failure_retries_after_grace_period_elapses``:
    the time-based escape must only trigger once enough time has passed —
    otherwise we'd be back to the original retry-loop problem on every
    restart.
    """
    import config as cfg
    from db import Database
    from scanner import backfill_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "a"
    folder.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    folder_id = db.add_folder(str(folder))
    pid = db.add_photo(
        folder_id, "missing.jpg", ".jpg",
        file_size=1000, file_mtime=42.0,
        width=2000, height=1500,
    )
    # Record a very recent failure (~1 minute ago) against the same mtime.
    db.conn.execute(
        "UPDATE photos SET working_copy_failed_at = datetime('now', '-1 minute'),"
        " working_copy_failed_mtime = ?"
        " WHERE id = ?",
        (42.0, pid),
    )
    db.conn.commit()

    calls = {"n": 0}

    import scanner as _scanner_mod

    def counting_extract(*args, **kwargs):
        calls["n"] += 1
        return False

    monkeypatch.setattr(_scanner_mod, "extract_working_copy", counting_extract)

    backfill_working_copies(db, str(vireo_dir))

    assert calls["n"] == 0, (
        "fresh failure marker (within grace period) must suppress retry"
    )


def test_backfill_success_clears_prior_failure_marker(tmp_path, monkeypatch):
    """After a successful extraction, failure columns are reset."""
    import config as cfg
    from db import Database
    from scanner import backfill_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "a"
    folder.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))

    pid = _seed_large_jpeg(db, folder, "a.jpg")
    # Pretend a previous backfill failed against an older mtime.
    db.conn.execute(
        "UPDATE photos SET working_copy_failed_at=datetime('now'),"
        " working_copy_failed_mtime=?"
        " WHERE id=?",
        (1.0, pid),
    )
    db.conn.commit()

    backfill_working_copies(db, str(vireo_dir))

    row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_at,"
        " working_copy_failed_mtime FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row["working_copy_path"] == f"working/{pid}.jpg"
    assert row["working_copy_failed_at"] is None
    assert row["working_copy_failed_mtime"] is None


def test_backfill_progress_callback_streams_per_row(tmp_path, monkeypatch):
    """``progress_callback`` is invoked once per row with (current, total)."""
    import config as cfg
    from db import Database
    from scanner import backfill_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "a"
    folder.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))

    ids = [_seed_large_jpeg(db, folder, f"p{i}.jpg") for i in range(3)]

    events = []
    backfill_working_copies(
        db, str(vireo_dir),
        progress_callback=lambda c, t: events.append((c, t)),
    )

    assert events == [(1, 3), (2, 3), (3, 3)]
    for pid in ids:
        assert (vireo_dir / "working" / f"{pid}.jpg").exists()


def test_backfill_cancel_check_aborts_loop(tmp_path, monkeypatch):
    """``cancel_check`` returning True stops the loop after the current row."""
    import config as cfg
    from db import Database
    from scanner import backfill_working_copies

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    folder = tmp_path / "a"
    folder.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))

    ids = [_seed_large_jpeg(db, folder, f"p{i}.jpg") for i in range(5)]

    # Cancel on second iteration. cancel_check fires *before* the row, so
    # row 0 runs, cancel is requested, row 1 sees cancel and aborts.
    state = {"n": 0}

    def cancel_check():
        state["n"] += 1
        return state["n"] >= 2

    backfill_working_copies(db, str(vireo_dir), cancel_check=cancel_check)

    completed = sum(
        1 for pid in ids
        if (vireo_dir / "working" / f"{pid}.jpg").exists()
    )
    assert completed == 1, f"expected 1 completion before cancel, got {completed}"


# ---------------------------------------------------------------------------
# scan() inline extraction — the new-imports path
# ---------------------------------------------------------------------------


def test_scan_records_failure_marker_for_unreadable_file(tmp_path, monkeypatch):
    """When inline extraction fails during scan(), the row carries a marker.

    Confirms the inline path (not just backfill) records failures, so the
    next backfill pass will respect the marker rather than retrying.
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
    src = root / "big.jpg"
    _make_jpeg(str(src), 2000, 1500)

    # Force extraction to fail on the post-scan pass.
    import scanner as _scanner_mod
    monkeypatch.setattr(_scanner_mod, "extract_working_copy", lambda *a, **k: False)

    scan(str(root), db, vireo_dir=str(vireo_dir))

    row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_at,"
        " working_copy_failed_mtime, file_mtime FROM photos"
        " WHERE filename='big.jpg'"
    ).fetchone()
    assert row["working_copy_path"] is None
    assert row["working_copy_failed_at"] is not None
    assert row["working_copy_failed_mtime"] == row["file_mtime"]


def test_scan_progress_callback_not_clobbered_by_working_copy_phase(tmp_path, monkeypatch):
    """The post-scan WC phase must not overwrite scan totals via progress_callback.

    Regression: ``_extract_working_copies`` was called with the same
    ``progress_callback`` the scan loop used to report per-file totals. In
    callers like the import job (vireo/app.py) the callback writes
    ``current``/``total`` into a shared ``job["progress"]`` dict that
    downstream phases read for the scan count. Passing the callback in
    again caused the WC phase to overwrite that total with the
    working-copy total — visually jumping the bar backward and feeding
    the wrong scan_count to later phases.
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
    # Two scanned files; only one is large enough to need a working copy.
    # Without the fix, the WC phase emits (1, 1) and would clobber the
    # scan's final (2, 2).
    _make_jpeg(str(root / "small.jpg"), 600, 400)
    _make_jpeg(str(root / "big.jpg"), 2000, 1500)

    events = []

    def progress_cb(current, total):
        events.append((current, total))

    scan(str(root), db, progress_callback=progress_cb, vireo_dir=str(vireo_dir))

    assert events, "scan should report progress for the scan loop"
    # The last reported total must be the SCAN total (2 files), not the
    # working-copy total (1 file). Any (_, 1) appearing after a (_, 2)
    # would mean the WC phase overwrote the scan totals.
    seen_scan_total = False
    for _current, total in events:
        if total == 2:
            seen_scan_total = True
        elif seen_scan_total and total == 1:
            raise AssertionError(
                f"Working-copy phase clobbered scan totals: {events}"
            )
    assert seen_scan_total, f"never observed scan total of 2: {events}"

    # Sanity check: the working copy itself was still produced.
    big_row = db.conn.execute(
        "SELECT id, working_copy_path FROM photos WHERE filename='big.jpg'"
    ).fetchone()
    assert big_row["working_copy_path"] == f"working/{big_row['id']}.jpg"


# ---------------------------------------------------------------------------
# Startup self-healing kickoff (app.create_app -> ephemeral JobRunner job)
# ---------------------------------------------------------------------------


def test_startup_backfill_skips_when_no_candidates(tmp_path, monkeypatch):
    """If no photo needs work, no working_copy_backfill job is started."""
    import os

    import config as cfg
    import models
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    from app import create_app
    from db import Database

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    Database(db_path)  # create empty DB with workspace

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")

    # Drive the kickoff synchronously instead of waiting for the 5s Timer.
    app._kickoff_working_copy_backfill()

    backfill_jobs = [
        j for j in app._job_runner.list_jobs()
        if j["type"] == "working_copy_backfill"
    ]
    assert backfill_jobs == []


def test_startup_backfill_skips_for_small_jpeg_only_library(tmp_path, monkeypatch):
    """Small JPEGs (under working_copy_max_size) are intentionally not extracted.

    The startup gate must skip them rather than launching a no-op backfill on
    every restart. Regression test for a library that contains only small
    JPEGs — naive ``working_copy_path IS NULL`` check would fire forever.
    """
    import os

    import config as cfg
    import models
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    from app import create_app
    from db import Database

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "test.db")

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "small.jpg"
    _make_jpeg(str(src), 800, 600)  # below the 1000 cap

    db = Database(db_path)
    folder_id = db.add_folder(str(photos_dir))
    db.add_photo(
        folder_id, "small.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=800, height=600,
    )
    db.conn.close()

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir), api_token="t")
    app._kickoff_working_copy_backfill()

    backfill_jobs = [
        j for j in app._job_runner.list_jobs()
        if j["type"] == "working_copy_backfill"
    ]
    assert backfill_jobs == [], (
        "small-JPEG-only library should not trigger working_copy_backfill"
    )


def test_startup_backfill_runs_when_candidates_exist(tmp_path, monkeypatch):
    """A photo with NULL working_copy_path triggers an ephemeral backfill job
    that produces the working copy and completes successfully.
    """
    import os

    import config as cfg
    import models
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    from app import create_app
    from db import Database

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "test.db")

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "big.jpg"
    _make_jpeg(str(src), 2000, 1500)

    db = Database(db_path)
    folder_id = db.add_folder(str(photos_dir))
    pid = db.add_photo(
        folder_id, "big.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=2000, height=1500,
    )
    db.conn.close()

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir), api_token="t")
    app._kickoff_working_copy_backfill()

    job = _wait_for_backfill_terminal(app._job_runner)
    assert job["status"] == "completed", f"job: {job}"
    assert job.get("ephemeral") is True

    # The working copy actually exists.
    assert (vireo_dir / "working" / f"{pid}.jpg").exists()

    # The DB row was updated with the working copy path.
    db2 = Database(db_path)
    row = db2.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["working_copy_path"] == f"working/{pid}.jpg"


def test_startup_backfill_does_not_persist_to_history(tmp_path, monkeypatch):
    """Ephemeral backfill job must NOT land in job_history.

    Otherwise every restart adds a noise row.
    """
    import os

    import config as cfg
    import models
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 1000, "working_copy_quality": 90})

    from app import create_app
    from db import Database

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "test.db")

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "big.jpg"
    _make_jpeg(str(src), 2000, 1500)

    db = Database(db_path)
    folder_id = db.add_folder(str(photos_dir))
    db.add_photo(
        folder_id, "big.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=2000, height=1500,
    )
    db.conn.close()

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir), api_token="t")
    app._kickoff_working_copy_backfill()

    _wait_for_backfill_terminal(app._job_runner)

    db2 = Database(db_path)
    rows = db2.conn.execute(
        "SELECT id FROM job_history WHERE type='working_copy_backfill'"
    ).fetchall()
    assert rows == [], "ephemeral job must not persist to history"
