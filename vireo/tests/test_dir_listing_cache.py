"""Directory listings reused while a folder's modification time holds.

The new-images walk and the automatic missing-originals scan re-read every
library folder on a timer. These tests pin that an unchanged folder is not
read from disk again, and that every kind of change a folder can see still
reaches the answer: a file added, removed or renamed (the folder's mtime
moves), a new subfolder, a symlink whose target vanished elsewhere (the
folder's mtime does not move), and an explicit recheck.
"""

import os
import sys
import time
from types import SimpleNamespace

import dir_listing_cache
import pytest
from dir_listing_cache import DirListingCache, ListingPass, read_directory
from image_loader import safe_scan_walk
from PIL import Image

HOUR = 3600


@pytest.fixture(autouse=True)
def _clear_shared_caches():
    from new_images import get_shared_cache

    dir_listing_cache.get_shared().clear()
    get_shared_cache().clear()
    yield
    dir_listing_cache.get_shared().clear()
    get_shared_cache().clear()


def _touch_image(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(path, "JPEG")


def _age(*dirs):
    """Backdate folders an hour, out of the cache's too-recent window, the
    way a real library folder last written days ago sits."""
    old = time.time() - HOUR
    for path in dirs:
        os.utime(path, (old, old))


def _age_tree(root):
    for dirpath, _dirnames, _filenames in os.walk(root):
        _age(dirpath)


@pytest.fixture
def scandir_calls(monkeypatch):
    """Record every directory read from disk."""
    calls = []
    real = os.scandir

    def counting(path="."):
        calls.append(os.path.normpath(os.fspath(path)))
        return real(path)

    monkeypatch.setattr(os, "scandir", counting)
    return calls


def _st(mtime_ns, ino=1, dev=1):
    return SimpleNamespace(st_mtime_ns=mtime_ns, st_ino=ino, st_dev=dev)


# --- DirListingCache ---------------------------------------------------------


def test_listing_is_reused_while_mtime_and_inode_hold():
    cache = DirListingCache(wall_clock=lambda: 10_000.0)
    st = _st(5_000 * 10**9)
    cache.store("/lib/a", st, 10_000.0, [("x.jpg", False, False), ("sub", True, False)])

    listing = cache.lookup("/lib/a/", st)
    assert listing.names == ("x.jpg", "sub")
    assert listing.dirs == frozenset({"sub"})
    assert cache.lookup("/lib/a", _st(5_000 * 10**9 + 1)) is None, "mtime moved"
    assert cache.lookup("/lib/a", st) is None, "a stale entry is dropped"


def test_a_replaced_folder_with_the_same_mtime_is_read_again():
    cache = DirListingCache()
    cache.store("/lib/a", _st(5_000 * 10**9, ino=7), 10_000.0, [("x.jpg", False, False)])
    assert cache.lookup("/lib/a", _st(5_000 * 10**9, ino=8)) is None


def test_a_remounted_volume_at_the_same_path_is_read_again():
    """Inodes are only unique within a device: a NAS or removable drive
    remounted at the same path can present the same ``st_ino`` and
    ``st_mtime_ns`` as the previous volume while listing different files, so
    the device id is part of the cached folder's identity."""
    cache = DirListingCache()
    cache.store("/lib/a", _st(5_000 * 10**9, ino=7, dev=1), 10_000.0, [("x.jpg", False, False)])
    assert cache.lookup("/lib/a", _st(5_000 * 10**9, ino=7, dev=2)) is None


def test_a_folder_changed_moments_before_the_read_is_not_trusted():
    """Coarse-timestamp filesystems (exFAT cards record 2s) can take a second
    change inside the same tick, so a folder modified within the racy window
    of the read is read again next time."""
    cache = DirListingCache(racy_seconds=10)
    st = _st(int(9_995 * 10**9))
    cache.store("/lib/a", st, 10_000.0, [("x.jpg", False, False)])
    assert cache.lookup("/lib/a", st) is None
    cache.store("/lib/a", st, 10_006.0, [("x.jpg", False, False)])
    assert cache.lookup("/lib/a", st) is not None


def test_listings_expire_after_the_backstop_age():
    now = {"t": 100.0}
    cache = DirListingCache(max_age_seconds=HOUR, monotonic=lambda: now["t"])
    st = _st(1)
    cache.store("/lib/a", st, 10_000.0, [])
    now["t"] += HOUR - 1
    assert cache.lookup("/lib/a", st) is not None
    now["t"] += 2
    assert cache.lookup("/lib/a", st) is None


def test_the_cache_is_bounded():
    cache = DirListingCache(max_directories=2)
    for name in ("a", "b", "c"):
        cache.store(f"/lib/{name}", _st(1), 10_000.0, [])
    assert cache.lookup("/lib/a", _st(1)) is None
    assert cache.lookup("/lib/c", _st(1)) is not None


def test_clear_drops_a_store_from_a_pass_started_before_it():
    """A pass whose ``scandir`` was already in flight when the user hit
    "Check again" must not repopulate the cache with what is now a stale
    listing: :meth:`clear` bumps a generation counter, and a :meth:`store`
    tagged with an older generation is refused."""
    cache = DirListingCache()
    st = _st(5_000 * 10**9)

    gen = cache.snapshot_generation()
    cache.clear()
    cache.store(
        "/lib/a", st, 10_000.0,
        [("stale.jpg", False, False)], generation=gen,
    )
    assert cache.lookup("/lib/a", st) is None

    fresh_gen = cache.snapshot_generation()
    cache.store(
        "/lib/a", st, 10_000.0,
        [("fresh.jpg", False, False)], generation=fresh_gen,
    )
    listing = cache.lookup("/lib/a", st)
    assert listing is not None and listing.names == ("fresh.jpg",)


def test_clear_also_drops_a_racy_store_from_an_earlier_pass():
    """The racy-window branch of :meth:`store` deletes the entry rather than
    writing one, but the same generation guard applies: a stale pass must
    not evict a listing recorded after :meth:`clear` bumped the generation."""
    cache = DirListingCache(racy_seconds=10)
    fresh_st = _st(int(9_995 * 10**9))
    stale_st = _st(int(9_999 * 10**9))

    gen = cache.snapshot_generation()
    cache.clear()
    # A fresh pass writes a good listing at the current generation.
    cache.store(
        "/lib/a", fresh_st, 10_006.0,
        [("fresh.jpg", False, False)], generation=cache.snapshot_generation(),
    )
    assert cache.lookup("/lib/a", fresh_st) is not None
    # A stale pass finishes with a racy read; without the guard this would
    # evict the fresh listing above.
    cache.store(
        "/lib/a", stale_st, 10_000.0,
        [("stale.jpg", False, False)], generation=gen,
    )
    assert cache.lookup("/lib/a", fresh_st) is not None


# --- read_directory / ListingPass --------------------------------------------


def test_read_directory_reuses_an_unchanged_folder(tmp_path, scandir_calls):
    (tmp_path / "a.jpg").write_bytes(b"x")
    _age(tmp_path)
    cache = DirListingCache()

    first = ListingPass(cache)
    assert read_directory(str(tmp_path), first).names == ("a.jpg",)
    assert (first.read, first.unchanged) == (1, 0)

    second = ListingPass(cache)
    assert read_directory(str(tmp_path), second).names == ("a.jpg",)
    assert (second.read, second.unchanged) == (0, 1)
    assert scandir_calls == [str(tmp_path)]

    (tmp_path / "b.jpg").write_bytes(b"x")
    third = ListingPass(cache)
    assert sorted(read_directory(str(tmp_path), third).names) == ["a.jpg", "b.jpg"]
    assert (third.read, third.unchanged) == (1, 0)


def test_a_recheck_reads_every_folder_and_refreshes_the_cache(tmp_path, scandir_calls):
    (tmp_path / "a.jpg").write_bytes(b"x")
    _age(tmp_path)
    cache = DirListingCache()
    read_directory(str(tmp_path), ListingPass(cache))

    recheck = ListingPass(cache, reuse=False)
    read_directory(str(tmp_path), recheck)
    assert (recheck.read, recheck.unchanged) == (1, 0)
    assert len(scandir_calls) == 2

    later = ListingPass(cache)
    read_directory(str(tmp_path), later)
    assert later.unchanged == 1


def test_a_pass_in_flight_when_the_cache_is_cleared_does_not_repopulate_it(
    tmp_path, monkeypatch,
):
    """A recheck's :meth:`clear` fires while an automatic pass is still
    reading its directory; the pass then calls :meth:`store` with the
    listing it captured before the clear. The generation the pass snapshotted
    at :meth:`ListingPass.begin` no longer matches, so :meth:`store` drops the
    write — a subsequent reuse-enabled pass reads the folder fresh instead of
    inheriting a stale listing."""
    (tmp_path / "a.jpg").write_bytes(b"x")
    _age(tmp_path)
    cache = DirListingCache()

    in_flight = ListingPass(cache)
    token, listing = in_flight.begin(str(tmp_path))
    assert listing is None
    entries = [("a.jpg", False, False)]

    cache.clear()  # user hit "Check again" mid-scan

    in_flight.finish(str(tmp_path), token, entries)
    assert cache.lookup(str(tmp_path), token[0]) is None

    later = ListingPass(cache)
    fresh = read_directory(str(tmp_path), later)
    assert fresh.names == ("a.jpg",)
    assert (later.read, later.unchanged) == (1, 0)


# --- safe_scan_walk ----------------------------------------------------------


def _walk(root, listing_pass):
    return sorted(
        (dirpath, sorted(dirnames), sorted(filenames))
        for dirpath, dirnames, filenames in safe_scan_walk(
            str(root), listing_pass=listing_pass,
        )
    )


def test_walk_reads_only_changed_folders_and_sees_every_change(tmp_path, scandir_calls):
    root = tmp_path / "lib"
    for rel in ("a/1.jpg", "a/2.jpg", "b/3.jpg", "b/deep/4.jpg"):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
    _age_tree(root)
    cache = DirListingCache()

    fresh = _walk(root, ListingPass(cache))
    assert fresh == _walk(root, None)
    scandir_calls.clear()

    unchanged = ListingPass(cache)
    assert _walk(root, unchanged) == fresh
    assert scandir_calls == []
    assert (unchanged.read, unchanged.unchanged) == (0, 4)

    # A file lands two levels down, and a new folder appears under ``a``.
    (root / "b" / "deep" / "5.jpg").write_bytes(b"x")
    (root / "a" / "new").mkdir()
    (root / "a" / "new" / "6.jpg").write_bytes(b"x")
    changed = ListingPass(cache)
    walked = _walk(root, changed)
    assert walked == _walk(root, None)
    assert (str(root / "b" / "deep"), [], ["4.jpg", "5.jpg"]) in walked
    assert (str(root / "a" / "new"), [], ["6.jpg"]) in walked
    assert sorted(scandir_calls) == sorted([
        str(root / "a"), str(root / "a" / "new"), str(root / "b" / "deep"),
        # The two changed folders, the new one, and the walk without a pass.
        str(root), str(root / "a"), str(root / "a" / "new"), str(root / "b"),
        str(root / "b" / "deep"),
    ])
    assert changed.unchanged == 2  # ``lib`` and ``b``

    os.remove(root / "a" / "1.jpg")
    os.rename(root / "b" / "3.jpg", root / "b" / "3b.jpg")
    assert _walk(root, ListingPass(cache)) == _walk(root, None)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX symlinks")
def test_replayed_listing_still_excludes_other_app_bundles(tmp_path):
    root = tmp_path / "lib"
    root.mkdir()
    (root / "a.jpg").write_bytes(b"x")
    (root / "Photos Library.photoslibrary").mkdir()
    (root / "LibraryAlias").symlink_to(tmp_path / "Other Library.photoslibrary")
    _age(root, root / "Photos Library.photoslibrary")
    cache = DirListingCache()

    fresh = _walk(root, ListingPass(cache))
    replay_pass = ListingPass(cache)
    replayed = _walk(root, replay_pass)
    assert replay_pass.unchanged == 1
    assert replayed == fresh == [(str(root), [], ["a.jpg"])]


def test_walk_stat_failure_reports_through_onerror(tmp_path):
    errors = []
    gone = tmp_path / "gone"
    list(safe_scan_walk(str(gone), onerror=errors.append, listing_pass=ListingPass()))
    assert len(errors) == 1
    assert errors[0].filename == str(gone)


# --- new-images walk ---------------------------------------------------------


@pytest.fixture
def db_with_workspace(tmp_path):
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    return db, ws_id, tmp_path


def test_new_images_walk_reuses_unchanged_folders(db_with_workspace, scandir_calls):
    from new_images import count_new_images_for_workspace

    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "day1" / "a.jpg"))
    _touch_image(str(root / "day2" / "b.jpg"))
    db.add_folder(str(root), name="shoot")
    _age_tree(root)
    cache = DirListingCache()

    first = count_new_images_for_workspace(db, ws_id, listing_cache=cache)
    assert first["new_count"] == 2
    assert (first["folders_read"], first["folders_unchanged"]) == (3, 0)

    second = count_new_images_for_workspace(db, ws_id, listing_cache=cache)
    assert second["new_count"] == 2
    assert (second["folders_read"], second["folders_unchanged"]) == (0, 3)

    _touch_image(str(root / "day2" / "c.jpg"))
    scandir_calls.clear()
    third = count_new_images_for_workspace(db, ws_id, listing_cache=cache)
    assert third["new_count"] == 3
    assert str(root / "day2" / "c.jpg") in third["sample"]
    assert scandir_calls == [str(root / "day2")]
    assert (third["folders_read"], third["folders_unchanged"]) == (1, 2)


def test_new_images_reused_listing_follows_the_catalog(db_with_workspace):
    """A cached listing holds names, not the answer: importing a file changes
    the count on the next pass even though no folder changed."""
    from new_images import count_new_images_for_workspace

    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "a.jpg"))
    _touch_image(str(root / "b.jpg"))
    folder_id = db.add_folder(str(root), name="shoot")
    _age(root)
    cache = DirListingCache()
    assert count_new_images_for_workspace(db, ws_id, listing_cache=cache)["new_count"] == 2

    db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    result = count_new_images_for_workspace(db, ws_id, listing_cache=cache)
    assert result["new_count"] == 1
    assert result["folders_unchanged"] == 1


def test_new_images_without_a_cache_reads_every_folder(db_with_workspace):
    from new_images import count_new_images_for_workspace

    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "a.jpg"))
    db.add_folder(str(root), name="shoot")
    for _ in range(2):
        result = count_new_images_for_workspace(db, ws_id)
        assert (result["folders_read"], result["folders_unchanged"]) == (1, 0)


def test_check_again_clears_remembered_listings(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    os.makedirs(tmp_path / "thumbs")
    db = Database(db_path)
    db.set_active_workspace(db.ensure_default_workspace())
    app = create_app(db_path=db_path, thumb_cache_dir=str(tmp_path / "thumbs"))

    shared = dir_listing_cache.get_shared()
    shared.store(str(tmp_path), _st(1), time.time(), [])
    assert shared.lookup(str(tmp_path), _st(1)) is not None
    resp = app.test_client().post("/api/workspaces/active/new-images/recheck")
    assert resp.get_json()["rechecked"] is True
    assert shared.lookup(str(tmp_path), _st(1)) is None


# --- missing originals -------------------------------------------------------


def _photo(db, folder_id, filename):
    return db.add_photo(
        folder_id=folder_id, filename=filename,
        extension=os.path.splitext(filename)[1], file_size=1, file_mtime=1.0,
    )


@pytest.fixture
def db(tmp_path):
    from db import Database

    database = Database(str(tmp_path / "test.db"))
    database.set_active_workspace(database.ensure_default_workspace())
    return database


def test_missing_scan_reuses_unchanged_folders_and_sees_deletions(db, tmp_path, scandir_calls):
    folder = tmp_path / "shoot"
    folder.mkdir()
    fid = db.add_folder(str(folder))
    for name in ("a.jpg", "b.jpg"):
        (folder / name).write_bytes(b"x")
        _photo(db, fid, name)
    _age(folder)
    cache = DirListingCache()

    first = ListingPass(cache)
    assert db.get_missing_photos(listing_pass=first) == []
    assert (first.read, first.unchanged) == (1, 0)

    second = ListingPass(cache)
    scandir_calls.clear()
    assert db.get_missing_photos(listing_pass=second) == []
    assert scandir_calls == []
    assert second.unchanged == 1

    os.remove(folder / "b.jpg")
    third = ListingPass(cache)
    assert [r["filename"] for r in db.get_missing_photos(listing_pass=third)] == ["b.jpg"]
    assert third.read == 1


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX symlinks")
def test_missing_scan_checks_symlink_targets_live_on_a_reused_listing(db, tmp_path):
    """Deleting a symlink's target elsewhere leaves the link's own folder
    untouched, so the listing is reused; the target is still checked."""
    folder = tmp_path / "shoot"
    elsewhere = tmp_path / "elsewhere"
    folder.mkdir()
    elsewhere.mkdir()
    (elsewhere / "real.jpg").write_bytes(b"x")
    os.symlink(elsewhere / "real.jpg", folder / "link.jpg")
    fid = db.add_folder(str(folder))
    _photo(db, fid, "link.jpg")
    _age(folder)
    cache = DirListingCache()
    assert db.get_missing_photos(listing_pass=ListingPass(cache)) == []

    os.remove(elsewhere / "real.jpg")
    reused = ListingPass(cache)
    assert [r["filename"] for r in db.get_missing_photos(listing_pass=reused)] == ["link.jpg"]
    assert reused.unchanged == 1


def test_missing_scan_with_a_listing_pass_matches_the_direct_read(db, tmp_path):
    folder = tmp_path / "shoot"
    sub = folder / "sub"
    sub.mkdir(parents=True)
    fid = db.add_folder(str(folder))
    fsub = db.add_folder(str(sub), parent_id=fid, workspace_root=False)
    (folder / "here.jpg").write_bytes(b"x")
    for folder_id, name in ((fid, "here.jpg"), (fid, "gone.jpg"), (fsub, "gone2.jpg")):
        _photo(db, folder_id, name)
    _age(folder, sub)
    direct = db.get_missing_photos()
    cache = DirListingCache()
    for _ in range(2):
        via_pass = db.get_missing_photos(listing_pass=ListingPass(cache))
        assert [r["id"] for r in via_pass] == [r["id"] for r in direct]
