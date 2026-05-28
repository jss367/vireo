# vireo/tests/test_thumbnails.py
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image


def _make_jpeg(path, w=200, h=150):
    Image.new("RGB", (w, h), (100, 100, 100)).save(str(path), "JPEG", quality=85)


def test_generate_thumbnail_creates_jpeg(tmp_path):
    """generate_thumbnail creates a JPEG thumbnail file."""
    from thumbnails import generate_thumbnail

    src = str(tmp_path / "source.jpg")
    Image.new('RGB', (2000, 1500), color='red').save(src)

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)

    result = generate_thumbnail(1, src, cache_dir)
    assert result is not None
    assert os.path.exists(result)

    # Verify it's a valid JPEG
    with Image.open(result) as img:
        assert img.format == 'JPEG'
        assert max(img.size) <= 400


def test_generate_thumbnail_skips_existing(tmp_path):
    """generate_thumbnail skips if thumbnail already exists."""
    from thumbnails import generate_thumbnail

    src = str(tmp_path / "source.jpg")
    Image.new('RGB', (200, 100)).save(src)

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)

    # Create first
    path1 = generate_thumbnail(1, src, cache_dir)
    mtime1 = os.path.getmtime(path1)

    # Should skip and return existing path
    path2 = generate_thumbnail(1, src, cache_dir)
    mtime2 = os.path.getmtime(path2)
    assert path1 == path2
    assert mtime1 == mtime2


def test_get_thumb_path_returns_none_if_missing(tmp_path):
    """get_thumb_path returns None if thumbnail doesn't exist."""
    from thumbnails import get_thumb_path

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)

    assert get_thumb_path(999, cache_dir) is None


def test_get_thumb_path_returns_path_if_exists(tmp_path):
    """get_thumb_path returns the path if thumbnail exists."""
    from thumbnails import generate_thumbnail, get_thumb_path

    src = str(tmp_path / "source.jpg")
    Image.new('RGB', (200, 100)).save(src)

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)
    generate_thumbnail(42, src, cache_dir)

    result = get_thumb_path(42, cache_dir)
    assert result is not None
    assert os.path.exists(result)


def test_generate_all_uses_working_copy(tmp_path):
    """generate_all uses working copy instead of original for RAW photos."""
    from db import Database
    from thumbnails import generate_all

    vireo_dir = tmp_path / "vireo"
    working_dir = vireo_dir / "working"
    working_dir.mkdir(parents=True)
    thumb_dir = vireo_dir / "thumbnails"

    # Create a working copy (simulating extracted JPEG)
    wc = working_dir / "1.jpg"
    Image.new("RGB", (4096, 2731), color=(0, 255, 0)).save(str(wc), "JPEG")

    db = Database(str(vireo_dir / "test.db"))
    folder_id = db.add_folder("/fake/photos")
    photo_id = db.add_photo(folder_id, "test.nef", ".nef", 1000, 1.0)
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        ("working/1.jpg", photo_id),
    )
    db.conn.commit()

    generate_all(db, str(thumb_dir), vireo_dir=str(vireo_dir))

    assert os.path.exists(os.path.join(str(thumb_dir), f"{photo_id}.jpg"))


def test_generate_all_routes_through_canonical_helper(tmp_path, monkeypatch):
    """generate_all calls get_canonical_image_path to resolve the source."""
    import thumbnails
    from db import Database

    # Fixture: one photo with no working copy
    folder = tmp_path / "photos"
    folder.mkdir()
    src = folder / "a.jpg"
    _make_jpeg(src)

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    fid = db.add_folder(str(folder))
    db.add_photo(
        fid, "a.jpg", ".jpg",
        file_size=os.path.getsize(src),
        file_mtime=os.path.getmtime(src),
        width=200, height=150,
    )

    mock_helper = MagicMock(return_value=str(src))
    monkeypatch.setattr(thumbnails, "get_canonical_image_path", mock_helper)

    thumb_dir = vireo_dir / "thumbs"
    thumbnails.generate_all(db, str(thumb_dir), vireo_dir=str(vireo_dir))

    assert mock_helper.called, \
        "generate_all should route source-path resolution through get_canonical_image_path"


def test_generate_all_creates_missing(tmp_path):
    """generate_all generates thumbnails for photos without them."""
    from db import Database
    from thumbnails import generate_all

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name='root')

    # Create two source images
    for name in ['a.jpg', 'b.jpg']:
        Image.new('RGB', (300, 200)).save(str(tmp_path / name))
        db.add_photo(folder_id=fid, filename=name, extension='.jpg',
                     file_size=100, file_mtime=1.0)

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)

    # Need folder path lookup for generate_all
    progress = []
    generate_all(db, cache_dir, progress_callback=lambda c, t: progress.append((c, t)))

    assert len(progress) == 2
    assert os.path.exists(os.path.join(cache_dir, "1.jpg"))
    assert os.path.exists(os.path.join(cache_dir, "2.jpg"))


def test_generate_all_records_thumb_path_in_db(tmp_path):
    """After generate_all, photos.thumb_path must reflect the generated file
    so the dashboard's coverage query (`thumb_path IS NOT NULL`) shows the
    thumbnail as produced. The on-disk JPEG alone is not enough."""
    from db import Database
    from thumbnails import generate_all

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name='root')
    for name in ['a.jpg', 'b.jpg']:
        Image.new('RGB', (300, 200)).save(str(tmp_path / name))
        db.add_photo(folder_id=fid, filename=name, extension='.jpg',
                     file_size=100, file_mtime=1.0)

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)
    generate_all(db, cache_dir)

    rows = db.conn.execute(
        "SELECT id, thumb_path FROM photos ORDER BY id"
    ).fetchall()
    assert all(r["thumb_path"] is not None for r in rows), (
        f"All photos should have thumb_path set; got {[dict(r) for r in rows]}"
    )
    # Stored value should identify the file by photo id, not as a brittle
    # absolute path that breaks if cache_dir moves.
    assert rows[0]["thumb_path"] == "1.jpg"
    assert rows[1]["thumb_path"] == "2.jpg"


def test_generate_all_does_not_record_thumb_path_on_failure(tmp_path, monkeypatch):
    """If generate_thumbnail returns None (failure), thumb_path stays NULL —
    we don't want the dashboard to falsely report coverage."""
    import thumbnails as thumbnails_mod
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name='root')
    Image.new('RGB', (300, 200)).save(str(tmp_path / "a.jpg"))
    db.add_photo(folder_id=fid, filename="a.jpg", extension='.jpg',
                 file_size=100, file_mtime=1.0)

    monkeypatch.setattr(
        thumbnails_mod, "generate_thumbnail",
        lambda photo_id, src, cache_dir, size=400, quality=85: None,
    )

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)
    thumbnails_mod.generate_all(db, cache_dir)

    row = db.conn.execute("SELECT thumb_path FROM photos").fetchone()
    assert row["thumb_path"] is None


def test_backfill_thumb_paths_sets_path_for_existing_files(tmp_path):
    """Library-wide backfill should mark photos whose thumbnail JPEG exists on
    disk but whose thumb_path is NULL (the dashboard-coverage repair pass)."""
    from db import Database
    from thumbnails import backfill_thumb_paths

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name='root')
    for name in ['a.jpg', 'b.jpg', 'c.jpg']:
        db.add_photo(folder_id=fid, filename=name, extension='.jpg',
                     file_size=100, file_mtime=1.0)

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)
    # Only photos 1 and 3 have on-disk thumbnails.
    Image.new('RGB', (50, 50)).save(str(tmp_path / "thumbs" / "1.jpg"))
    Image.new('RGB', (50, 50)).save(str(tmp_path / "thumbs" / "3.jpg"))

    result = backfill_thumb_paths(db, cache_dir)

    rows = {r["id"]: r["thumb_path"] for r in db.conn.execute(
        "SELECT id, thumb_path FROM photos"
    ).fetchall()}
    assert rows[1] == "1.jpg"
    assert rows[2] is None
    assert rows[3] == "3.jpg"
    assert result["set"] == 2


def test_backfill_thumb_paths_clears_path_for_missing_files(tmp_path):
    """If a photo has thumb_path set but the file is gone (user wiped the
    cache), the backfill should clear the column so the dashboard reflects
    on-disk reality. Otherwise drift persists between disk and DB."""
    from db import Database
    from thumbnails import backfill_thumb_paths

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name='root')
    db.add_photo(folder_id=fid, filename="a.jpg", extension='.jpg',
                 file_size=100, file_mtime=1.0)
    # Pretend a previous run set this; the file no longer exists on disk.
    db.conn.execute("UPDATE photos SET thumb_path='1.jpg' WHERE id=1")
    db.conn.commit()

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)

    result = backfill_thumb_paths(db, cache_dir)

    row = db.conn.execute("SELECT thumb_path FROM photos").fetchone()
    assert row["thumb_path"] is None
    assert result["cleared"] == 1


def test_backfill_thumb_paths_skips_when_already_synced(tmp_path):
    """No-op when every photo's thumb_path matches disk — the steady-state
    case after the first backfill run."""
    from db import Database
    from thumbnails import backfill_thumb_paths

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name='root')
    db.add_photo(folder_id=fid, filename="a.jpg", extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET thumb_path='1.jpg' WHERE id=1")
    db.conn.commit()

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)
    Image.new('RGB', (50, 50)).save(str(tmp_path / "thumbs" / "1.jpg"))

    result = backfill_thumb_paths(db, cache_dir)
    assert result["set"] == 0
    assert result["cleared"] == 0


def test_thumb_path_backfill_candidate_count_zero_when_synced(tmp_path):
    """Startup gate count: returns 0 when nothing needs work, so the kickoff
    can skip spawning a job entirely."""
    from db import Database
    from thumbnails import thumb_path_backfill_candidate_count

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name='root')
    db.add_photo(folder_id=fid, filename="a.jpg", extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET thumb_path='1.jpg' WHERE id=1")
    db.conn.commit()

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)
    Image.new('RGB', (50, 50)).save(str(tmp_path / "thumbs" / "1.jpg"))

    assert thumb_path_backfill_candidate_count(db, cache_dir) == 0


def test_thumb_path_backfill_candidate_count_counts_unsynced(tmp_path):
    """Both stale-NULL (file exists but column empty) and stale-NOT-NULL
    (column set but file missing) photos count as candidates."""
    from db import Database
    from thumbnails import thumb_path_backfill_candidate_count

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name='root')
    for name in ['a.jpg', 'b.jpg']:
        db.add_photo(folder_id=fid, filename=name, extension='.jpg',
                     file_size=100, file_mtime=1.0)

    cache_dir = str(tmp_path / "thumbs")
    os.makedirs(cache_dir)
    # Photo 1: file exists, column NULL  -> needs setting.
    Image.new('RGB', (50, 50)).save(str(tmp_path / "thumbs" / "1.jpg"))
    # Photo 2: column set, file missing  -> needs clearing.
    db.conn.execute("UPDATE photos SET thumb_path='2.jpg' WHERE id=2")
    db.conn.commit()

    assert thumb_path_backfill_candidate_count(db, cache_dir) == 2


# -- Route-level self-heal tests --
#
# The /thumbnails/<id>.jpg route must regenerate on miss instead of
# 404-ing whenever the photo still exists in the DB and the source
# image is readable. A blank card on the encounter grid is a UX
# black-box (CORE_PHILOSOPHY: no black boxes), and the project's
# self-healing-app rule (feedback_self_healing_app.md) says the app
# should detect and repair broken cache state, not surface it.

def _make_app_with_real_photo(tmp_path, monkeypatch, filename="bird.jpg"):
    """Build a Flask app + DB with one real photo whose source file exists.

    Returns (app, db, photo_id, thumb_dir).
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(
        models, "CONFIG_PATH", str(tmp_path / "models.json"),
    )

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / filename
    Image.new("RGB", (800, 600), (180, 90, 40)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename=filename, extension=".jpg",
        file_size=os.path.getsize(src),
        file_mtime=os.path.getmtime(src),
        width=800, height=600,
    )

    app = create_app(
        db_path=db_path, thumb_cache_dir=str(thumb_dir),
        api_token="test-token-123",
    )
    return app, db, pid, str(thumb_dir)


def test_serve_thumbnail_regenerates_on_cache_miss(tmp_path, monkeypatch):
    """When the thumbnail JPEG is missing on disk but the photo exists,
    the route must regenerate it and serve it — never 404 — and the
    file must persist for next time. This is the encounter-grid card
    self-heal path: a wiped or never-populated cache should not leave
    blank tiles."""
    app, db, pid, thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)
    thumb_file = os.path.join(thumb_dir, f"{pid}.jpg")
    assert not os.path.exists(thumb_file), "precondition: cache miss"

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 200, (
        f"thumbnail route should self-heal on miss, got {resp.status_code}"
    )
    assert resp.mimetype in ("image/jpeg", "image/jpg")
    # Body must be a real JPEG (SOI marker = FF D8).
    assert resp.data[:2] == b"\xff\xd8", "response body is not a JPEG"

    # Persisted: the file is now on disk for future requests.
    assert os.path.exists(thumb_file), "regenerated thumbnail must be saved to disk"

    # And photos.thumb_path is set so the dashboard's coverage query
    # reflects the heal.
    row = db.conn.execute(
        "SELECT thumb_path FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["thumb_path"] == f"{pid}.jpg"


def test_serve_thumbnail_serves_cached_without_regenerating(tmp_path, monkeypatch):
    """When the file is already on disk, the route must not re-decode the
    source — that would burn CPU on every encounter-grid scroll."""
    app, _db, pid, thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)

    # Pre-populate with a sentinel image whose bytes we recognise.
    thumb_file = os.path.join(thumb_dir, f"{pid}.jpg")
    Image.new("RGB", (50, 50), (1, 2, 3)).save(thumb_file, "JPEG", quality=70)
    sentinel_size = os.path.getsize(thumb_file)

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 200
    # The cached file must have been served unmodified.
    assert os.path.getsize(thumb_file) == sentinel_size


def test_serve_thumbnail_404s_for_deleted_photo(tmp_path, monkeypatch):
    """When the photo is gone from the DB (stale URL from a cached
    pipeline_results JSON), the route correctly 404s. There is nothing
    to regenerate."""
    app, db, pid, _thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)
    db.conn.execute("DELETE FROM photos WHERE id=?", (pid,))
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 404


def test_serve_thumbnail_regenerates_when_cached_predates_source(
    tmp_path, monkeypatch,
):
    """Reused photo IDs leave behind cached thumbnails belonging to the
    previous tenant of that ID. ``photos.id`` is INTEGER PRIMARY KEY
    *without* AUTOINCREMENT, so SQLite reuses the highest freed rowids
    on the next insert. Combined with delete paths that don't clean up
    the on-disk cache (``audit.remove_orphans``, folder consolidation,
    companion-pair dedup in the scanner), a thumbnail at ``<id>.jpg``
    can persist after its original photo is gone, and a *different*
    photo later inserted at the same ID gets the stale image served on
    every grid scroll. The route must compare the cached thumbnail's
    mtime against the photo's ``file_mtime`` and regenerate when the
    cache predates the source — anchoring correctness in the data
    rather than depending on every delete site to clean up.
    """
    app, db, pid, thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)

    # Pre-populate a sentinel thumbnail (the "stale" tenant). Backdate it
    # so its mtime is older than the photo's file_mtime — this is the
    # condition the route must detect.
    thumb_file = os.path.join(thumb_dir, f"{pid}.jpg")
    Image.new("RGB", (50, 50), (1, 2, 3)).save(thumb_file, "JPEG", quality=70)
    sentinel_size = os.path.getsize(thumb_file)
    photo_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (pid,)
    ).fetchone()["file_mtime"]
    stale_mtime = photo_mtime - 86400  # one day older than the source
    os.utime(thumb_file, (stale_mtime, stale_mtime))

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 200
    assert resp.data[:2] == b"\xff\xd8", "response body is not a JPEG"
    # The stale sentinel was 50x50 at quality 70; a regenerated thumbnail
    # of the 800x600 source at quality 85 produces a different file size.
    # Comparing sizes is enough to prove the file was rewritten — we
    # don't need to round-trip through PIL.
    assert os.path.getsize(thumb_file) != sentinel_size, (
        "stale thumbnail was served unchanged; the route did not "
        "detect that the cached file predates the source"
    )
    # And the regenerated thumb's mtime is now >= file_mtime, so a
    # subsequent request hits the fast path.
    assert os.path.getmtime(thumb_file) >= photo_mtime


def test_serve_thumbnail_handles_race_between_exists_and_getmtime(
    tmp_path, monkeypatch,
):
    """The cache-hit path checks ``os.path.exists`` then calls
    ``os.path.getmtime`` separately. If the cached file vanishes between
    those two syscalls — concurrent ``Clear cache`` from Settings, a
    parallel regeneration that unlinked the stale file, an external
    cleanup process — ``getmtime`` raises ``FileNotFoundError``. The
    route must catch that and treat the request as a cache miss, not
    propagate to Flask's global handler as a 500.

    Simulated by monkeypatching ``os.path.getmtime`` to raise unconditionally
    once. The route must respond 200 (regenerated) — and never 500 —
    proving the race is handled.
    """
    import app as app_module

    app, _db, pid, thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)

    # Stage a cached file so the ``os.path.exists`` branch is taken,
    # but make ``getmtime`` raise as if the file disappeared between
    # the two syscalls. Patching the symbol the route uses
    # (``app_module.os.path.getmtime``) reaches inside the request
    # thread without affecting test infrastructure.
    thumb_file = os.path.join(thumb_dir, f"{pid}.jpg")
    Image.new("RGB", (50, 50), (1, 2, 3)).save(thumb_file, "JPEG", quality=70)

    real_getmtime = app_module.os.path.getmtime
    raised = {"count": 0}

    def racing_getmtime(path):
        if path == thumb_file and raised["count"] == 0:
            raised["count"] += 1
            raise FileNotFoundError(2, "File not found", path)
        return real_getmtime(path)

    monkeypatch.setattr(app_module.os.path, "getmtime", racing_getmtime)

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 200, (
        f"race between exists() and getmtime() leaked a {resp.status_code} "
        f"to the user; the route should treat the missing file as a cache "
        f"miss and self-heal"
    )
    assert resp.data[:2] == b"\xff\xd8", "response body is not a JPEG"
    assert raised["count"] == 1, "test setup did not exercise the race"


def test_serve_thumbnail_does_not_loop_on_future_dated_source(
    tmp_path, monkeypatch,
):
    """``photos.file_mtime`` can legitimately be in the future — files
    copied from a machine with clock skew, archives that preserve
    future filesystem timestamps, NEFs whose embedded metadata reflects
    a different timezone. A naive ``cached_mtime < file_mtime`` check
    treats every request as stale because the regenerated thumbnail's
    own mtime defaults to ``time.time()``, which is still less than the
    stored future ``file_mtime``. The route must break the loop after
    regeneration so a subsequent request hits the fast path instead of
    re-decoding the source on every grid scroll.
    """
    import time as _time

    app, db, pid, thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)
    # Set the photo's file_mtime to one day in the future, simulating
    # the clock-skew / preserved-future-timestamp case.
    future_mtime = _time.time() + 86400
    db.conn.execute(
        "UPDATE photos SET file_mtime=? WHERE id=?", (future_mtime, pid),
    )
    db.conn.commit()

    thumb_file = os.path.join(thumb_dir, f"{pid}.jpg")
    assert not os.path.exists(thumb_file), "precondition: cache miss"

    client = app.test_client()
    # First request triggers regeneration (cache miss → self-heal path).
    resp1 = client.get(f"/thumbnails/{pid}.jpg")
    assert resp1.status_code == 200
    assert os.path.exists(thumb_file)
    after_first = os.path.getmtime(thumb_file)

    # Second request must NOT regenerate. If the freshness check still
    # fires for future-dated sources, the route would unlink the file
    # and re-encode the source, bumping the mtime to a new ``now``.
    resp2 = client.get(f"/thumbnails/{pid}.jpg")
    assert resp2.status_code == 200
    after_second = os.path.getmtime(thumb_file)
    assert after_second == after_first, (
        "thumbnail regenerated a second time despite being just-written; "
        "future-dated photos.file_mtime causes an infinite regeneration "
        "loop on every request"
    )


def test_serve_thumbnail_serves_cached_when_thumb_newer_than_source(
    tmp_path, monkeypatch,
):
    """The stale-cache guard must not regress the common case: a thumbnail
    generated *after* its source file was last modified is fresh and
    must be served without re-decoding. Otherwise every grid scroll
    burns CPU on a thumbnail re-render."""
    app, db, pid, thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)

    thumb_file = os.path.join(thumb_dir, f"{pid}.jpg")
    Image.new("RGB", (50, 50), (1, 2, 3)).save(thumb_file, "JPEG", quality=70)
    sentinel_size = os.path.getsize(thumb_file)
    photo_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (pid,)
    ).fetchone()["file_mtime"]
    fresh_mtime = photo_mtime + 60  # generated after the source was written
    os.utime(thumb_file, (fresh_mtime, fresh_mtime))

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 200
    assert os.path.getsize(thumb_file) == sentinel_size, (
        "fresh cached thumbnail was rewritten — the stale-cache guard "
        "is firing on cache hits where it shouldn't"
    )


def test_serve_thumbnail_404s_for_non_numeric_filename(tmp_path, monkeypatch):
    """Garbage URLs (e.g. /thumbnails/foo.jpg) must 404 cleanly without
    raising — defends the route against arbitrary path probes."""
    app, *_ = _make_app_with_real_photo(tmp_path, monkeypatch)
    client = app.test_client()
    resp = client.get("/thumbnails/not-a-number.jpg")
    assert resp.status_code == 404


def test_serve_thumbnail_prefers_working_copy_over_source(tmp_path, monkeypatch):
    """When a working copy exists, the self-heal must use it instead of
    the original. This matters for RAW formats whose original cannot be
    decoded by libraw — the working copy was extracted from the embedded
    JPEG at scan time and is the only readable source."""
    app, db, pid, thumb_dir = _make_app_with_real_photo(
        tmp_path, monkeypatch, filename="bird.jpg",
    )

    # Replace the original on disk with garbage so any attempt to decode
    # it fails. The working copy is the only readable source.
    photos_row = db.conn.execute(
        "SELECT f.path, p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (pid,),
    ).fetchone()
    original_path = os.path.join(photos_row["path"], photos_row["filename"])
    with open(original_path, "wb") as f:
        f.write(b"not a real image")

    # Stage a working copy at the canonical location.
    vireo_dir = os.path.dirname(thumb_dir)
    wc_dir = os.path.join(vireo_dir, "working")
    os.makedirs(wc_dir, exist_ok=True)
    wc_path = os.path.join(wc_dir, f"{pid}.jpg")
    Image.new("RGB", (1024, 768), (50, 200, 50)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 200, (
        "self-heal should fall through to the working copy when the "
        "original is unreadable"
    )
    assert resp.data[:2] == b"\xff\xd8"
    assert os.path.exists(os.path.join(thumb_dir, f"{pid}.jpg"))


def test_serve_thumbnail_404s_when_source_is_unreadable(tmp_path, monkeypatch):
    """When neither the original nor a working copy is readable, the
    route must 404 (not 500). generate_thumbnail logs the cause."""
    app, db, pid, _thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)

    # Make the only source unreadable.
    photos_row = db.conn.execute(
        "SELECT f.path, p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (pid,),
    ).fetchone()
    original_path = os.path.join(photos_row["path"], photos_row["filename"])
    with open(original_path, "wb") as f:
        f.write(b"not a real image")

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")
    assert resp.status_code == 404


def test_serve_thumbnail_skips_recent_failed_raw_working_copy(
    tmp_path, monkeypatch,
):
    """A RAW row that already failed working-copy extraction at the current
    mtime must not retry RAW decode in the thumbnail request thread.
    """
    import thumbnails

    app, db, pid, _thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (pid,),
    ).fetchone()
    with open(os.path.join(folder["path"], "bad.NEF"), "wb") as f:
        f.write(b"not a decodable raw")
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (pid,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, pid),
    )
    db.conn.commit()

    called = {"generate": False}

    def fail_if_called(*_args, **_kwargs):
        called["generate"] = True
        raise AssertionError("thumbnail request retried failed RAW decode")

    monkeypatch.setattr(thumbnails, "generate_thumbnail", fail_if_called)

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 404
    assert called["generate"] is False


def test_serve_thumbnail_refreshes_failure_marker_when_stale_retry_fails(
    tmp_path, monkeypatch,
):
    """A stale RAW failure may retry once; if thumbnail regeneration still
    fails, the route should refresh the marker so later requests fail fast."""
    import thumbnails

    app, db, pid, _thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (pid,),
    ).fetchone()
    with open(os.path.join(folder["path"], "bad.NEF"), "wb") as f:
        f.write(b"not a decodable raw")
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (pid,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now', '-48 hours'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, pid),
    )
    db.conn.commit()

    monkeypatch.setattr(
        thumbnails, "generate_thumbnail", lambda *args, **kwargs: None,
    )

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 404
    row = db.conn.execute(
        """SELECT working_copy_failed_mtime,
                  (julianday('now') - julianday(working_copy_failed_at))
                  * 24 * 60 * 60 AS age_seconds
           FROM photos WHERE id=?""",
        (pid,),
    ).fetchone()
    assert row["working_copy_failed_mtime"] == file_mtime
    assert row["age_seconds"] is not None and row["age_seconds"] < 60


def test_serve_thumbnail_resolves_when_folder_status_is_missing(tmp_path, monkeypatch):
    """The self-heal must use the photo's actual folder path even when
    the folder's status is ``'missing'``. ``get_folder_tree()`` filters
    out ``'missing'`` folders, which would leave the canonical-path
    helper with an empty mapping and silently fall back to
    ``os.path.join('', photo['filename'])`` — a CWD-relative path.

    A request can land on this code path while a network mount briefly
    flaps (the health loop flips status to ``'missing'``) or before the
    next health pass clears a transient state. Resolving CWD-relative
    is wrong in either case: at best the request 404s nondeterministically,
    at worst it persists a thumbnail derived from an unrelated file with
    the same basename in the server's working directory.
    """
    app, db, pid, thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)

    # Mark the photo's folder as missing — but the source file is still
    # on disk (the canonical case is a flapping mount, not a real
    # delete). The route must still resolve via the actual folder path.
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()

    # Sanity: get_folder_tree (the prior lookup) would now omit this
    # folder, forcing the buggy CWD fallback.
    assert db.get_folder_tree() == [], (
        "precondition: folder is filtered out of the workspace tree once "
        "marked missing — the bug we're guarding against"
    )

    # Pollute CWD with a same-named file that contains different bytes
    # so a CWD-relative resolution would silently feed unrelated pixels
    # into the thumbnail (and persist it to disk under photo_id.jpg).
    cwd_decoy_dir = tmp_path / "cwd"
    cwd_decoy_dir.mkdir()
    decoy = cwd_decoy_dir / "bird.jpg"
    Image.new("RGB", (800, 600), (10, 250, 10)).save(str(decoy), "JPEG")
    monkeypatch.chdir(str(cwd_decoy_dir))

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")

    assert resp.status_code == 200, (
        "self-heal must resolve via the photo's folder regardless of "
        f"folder status, got {resp.status_code}"
    )
    assert resp.data[:2] == b"\xff\xd8"
    assert os.path.exists(os.path.join(thumb_dir, f"{pid}.jpg"))


def test_serve_thumbnail_does_not_unlink_cross_workspace_cache(
    tmp_path, monkeypatch,
):
    """The stale-cache guard fires before any workspace check, so a
    request for a photo outside the active workspace can still drive
    the unlink-and-regen branch — destroying another workspace's
    cached thumbnail as a side effect, even though the request
    ultimately 404s. The fix is to validate workspace access before
    mutating the cache: stale-detection is fine, but the on-disk
    unlink must not happen until we know the active workspace owns
    the photo. Otherwise a single ill-aimed grid scroll on
    workspace B could wipe workspace A's thumbnails.
    """
    app, db, pid, thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)

    # Stage a stale cached file for the photo so the staleness guard
    # would normally fire (cached_mtime older than file_mtime).
    thumb_file = os.path.join(thumb_dir, f"{pid}.jpg")
    Image.new("RGB", (50, 50), (1, 2, 3)).save(thumb_file, "JPEG", quality=70)
    photo_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (pid,)
    ).fetchone()["file_mtime"]
    stale_mtime = photo_mtime - 86400
    os.utime(thumb_file, (stale_mtime, stale_mtime))

    # Switch to a workspace that doesn't have the photo's folder linked.
    other_ws = db.create_workspace("Other")
    db.update_workspace(other_ws, last_opened_at="2030-01-01T00:00:00Z")
    db.set_active_workspace(other_ws)

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")
    assert resp.status_code == 404
    # The other workspace's cached thumbnail must survive the request.
    assert os.path.exists(thumb_file), (
        "cross-workspace request unlinked another workspace's cached "
        "thumbnail; the staleness guard must not mutate cache files "
        "for photos the active workspace cannot see"
    )


def test_serve_thumbnail_does_not_pin_stale_when_unlink_fails(
    tmp_path, monkeypatch,
):
    """If ``os.remove`` fails on the stale file (Windows file lock,
    permissions, antivirus quarantine), ``generate_thumbnail`` short-
    circuits because its destination still exists, so the stale JPEG
    is never re-encoded. Aligning the file's mtime to the source's
    ``file_mtime`` afterward would mark the stale image as fresh
    forever — pinning the wrong image until manual cache clear.

    The route must detect this and either skip the mtime alignment
    (so the next request retries) or refuse to call ``generate_thumbnail``
    when the stale file is still on disk. Either way: a request that
    cannot rewrite the file must NOT advance its mtime.
    """
    import app as app_module

    app, db, pid, thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)

    # Stage a stale cached file. Give it sentinel bytes so we can
    # confirm the stale content is still on disk afterward.
    thumb_file = os.path.join(thumb_dir, f"{pid}.jpg")
    Image.new("RGB", (50, 50), (1, 2, 3)).save(thumb_file, "JPEG", quality=70)
    sentinel_bytes = open(thumb_file, "rb").read()
    photo_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (pid,)
    ).fetchone()["file_mtime"]
    stale_mtime = photo_mtime - 86400
    os.utime(thumb_file, (stale_mtime, stale_mtime))

    # Make ``os.remove`` fail when called on this thumbnail — simulating
    # a file lock or permission denial. Other unlink calls in the test
    # process must keep working.
    real_remove = app_module.os.remove

    def failing_remove(path, *args, **kwargs):
        if path == thumb_file:
            raise PermissionError(13, "Permission denied", path)
        return real_remove(path, *args, **kwargs)

    monkeypatch.setattr(app_module.os, "remove", failing_remove)

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")
    assert resp.status_code == 200

    # The on-disk file must still be the stale sentinel — generate_thumbnail
    # short-circuited because os.remove couldn't drop the file.
    assert open(thumb_file, "rb").read() == sentinel_bytes
    # And critically, its mtime must NOT have been advanced to the
    # source's file_mtime — that would falsely mark it fresh and pin
    # the wrong image indefinitely. Allow equality with the original
    # stale_mtime; the route is permitted to leave it untouched.
    assert os.path.getmtime(thumb_file) < photo_mtime, (
        "stale thumbnail was pinned as fresh after a failed unlink; "
        "next request will not retry the regen and the wrong image "
        "stays cached forever"
    )


def test_serve_thumbnail_404s_for_photo_outside_active_workspace(tmp_path, monkeypatch):
    """The route must 404 when the photo exists in the DB but its folder
    isn't linked to the active workspace. Without workspace scoping,
    ``get_canonical_image_path`` would receive an empty folders dict
    (``get_folder_tree`` is workspace-scoped) and fall back to a CWD-
    relative path — which could read or persist a thumbnail derived from
    an unrelated same-named file on the server. Same-filename collision
    across workspaces is not exotic: every Lightroom export produces
    files like ``DSC_0001.jpg``."""
    app, db, pid, _thumb_dir = _make_app_with_real_photo(tmp_path, monkeypatch)

    # Spin up a second workspace that does NOT have the photo's folder
    # linked, and make it the active one. The photo row still exists,
    # but it is invisible from this workspace. Persisting
    # ``last_opened_at`` is what the per-request ``Database`` instance
    # uses to pick the active workspace on init, so the route inherits
    # the switch.
    other_ws = db.create_workspace("Other")
    db.update_workspace(other_ws, last_opened_at="2030-01-01T00:00:00Z")
    db.set_active_workspace(other_ws)

    client = app.test_client()
    resp = client.get(f"/thumbnails/{pid}.jpg")
    assert resp.status_code == 404, (
        "thumbnail self-heal must not serve a photo that the active "
        "workspace cannot see"
    )
