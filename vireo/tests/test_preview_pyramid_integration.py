"""End-to-end: scan -> working copy -> thumbnail -> preview -> eviction."""
import os

from PIL import Image


def _make_jpeg(path, w, h, color=(200, 100, 50)):
    Image.new("RGB", (w, h), color).save(str(path), "JPEG", quality=85)


def test_full_pyramid_cycle(tmp_path, monkeypatch):
    """Exercise the full preview pyramid: scan-time working copy extraction for
    large JPEGs, canonical-helper-backed thumbnail generation, preview generation
    with LRU tracking, /full <-> /preview alias behavior, and quota-triggered
    eviction.
    """
    monkeypatch.setenv("HOME", str(tmp_path))

    import config as cfg
    from app import create_app
    from db import Database
    from scanner import _extract_working_copies
    from thumbnails import generate_all as gen_thumbs

    # Route config through the temp dir BEFORE creating the Database so every
    # code path picks up the small working_copy_max_size cap.
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({
        **cfg.DEFAULTS,
        "working_copy_max_size": 1000,
        "working_copy_quality": 90,
        "preview_max_size": 1920,
    })

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    folder = tmp_path / "photos"
    folder.mkdir()
    big = folder / "big.jpg"
    small = folder / "small.jpg"
    _make_jpeg(big, 1500, 1000)  # above 1000 cap
    _make_jpeg(small, 800, 600)  # below cap

    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()

    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(folder), name="photos")
    pid_big = db.add_photo(
        folder_id=fid, filename="big.jpg", extension=".jpg",
        file_size=os.path.getsize(big), file_mtime=os.path.getmtime(big),
        width=1500, height=1000,
    )
    pid_small = db.add_photo(
        folder_id=fid, filename="small.jpg", extension=".jpg",
        file_size=os.path.getsize(small), file_mtime=os.path.getmtime(small),
        width=800, height=600,
    )

    # 1. Scanner creates a working copy for the big JPEG only.
    _extract_working_copies(db, str(vireo_dir))
    assert (vireo_dir / "working" / f"{pid_big}.jpg").exists(), (
        "large JPEG should get a working copy"
    )
    assert not (vireo_dir / "working" / f"{pid_small}.jpg").exists(), (
        "small JPEG should NOT get a working copy"
    )

    # 2. Thumbnails are generated (via canonical helper path).
    gen_thumbs(db, str(thumb_dir), vireo_dir=str(vireo_dir))
    assert (thumb_dir / f"{pid_big}.jpg").exists()
    assert (thumb_dir / f"{pid_small}.jpg").exists()

    # 3. Preview endpoint generates the file and records an LRU row.
    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    client = app.test_client()
    resp = client.get(f"/photos/{pid_big}/preview?size=1920")
    assert resp.status_code == 200
    assert db.preview_cache_get(pid_big, 1920) is not None, (
        "preview_cache row should exist after /preview generates"
    )
    assert (vireo_dir / "previews" / f"{pid_big}_1920.jpg").exists()

    # 4. /full is an alias for /preview?size=<preview_max_size>.
    full_bytes = client.get(f"/photos/{pid_big}/full").data
    preview_bytes = client.get(f"/photos/{pid_big}/preview?size=1920").data
    assert full_bytes == preview_bytes, "/full must return the same bytes as /preview at preview_max_size"

    # 5. Shrinking the quota via /api/config immediately drains the cache.
    resp = client.post("/api/config", json={"preview_cache_max_mb": 0})
    assert resp.status_code == 200
    assert db.preview_cache_total_bytes() == 0, (
        "saving preview_cache_max_mb=0 should evict every tracked entry"
    )
    assert not (vireo_dir / "previews" / f"{pid_big}_1920.jpg").exists(), (
        "the on-disk preview file should be removed when its row is evicted"
    )
