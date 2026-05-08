# vireo/tests/test_audit.py
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image


def test_check_drift_detects_xmp_change(tmp_path):
    """check_drift detects when XMP was modified after scan."""
    from audit import check_drift
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))
    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Sparrow'},
        hierarchical_keywords=set(),
    )

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Modify XMP after scan
    time.sleep(0.05)
    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Cardinal'},
        hierarchical_keywords=set(),
    )

    drifts = check_drift(db)
    assert len(drifts) >= 1
    assert drifts[0]['filename'] == 'bird.jpg'


def test_check_orphans_detects_deleted_file(tmp_path):
    """check_orphans finds DB entries with no file on disk."""
    from audit import check_orphans
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, 'bird.jpg')
    Image.new('RGB', (100, 100)).save(img_path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Delete file after scan
    os.unlink(img_path)

    orphans = check_orphans(db)
    assert len(orphans) == 1
    assert orphans[0]['filename'] == 'bird.jpg'


def test_check_untracked_finds_new_files(tmp_path):
    """check_untracked finds files on disk not in the DB."""
    from audit import check_untracked
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'known.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Add new file after scan
    Image.new('RGB', (200, 200)).save(os.path.join(root, 'new_file.jpg'))

    untracked = check_untracked(db, [root])
    assert len(untracked) == 1
    assert 'new_file.jpg' in untracked[0]['path']


def test_remove_orphans(tmp_path):
    """remove_orphans deletes DB entries for missing files."""
    from audit import remove_orphans
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/gone', name='gone')
    pid = db.add_photo(folder_id=fid, filename='missing.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)

    remove_orphans(db, [pid])

    photo = db.get_photo(pid)
    assert photo is None


def test_remove_orphans_endpoint_unlinks_cached_thumbnail(
    tmp_path, monkeypatch,
):
    """The /api/audit/remove-orphans endpoint must remove cached
    thumbnails for the photos it drops. Without this cleanup, the
    next photo to inherit the same SQLite rowid (``photos.id`` is
    INTEGER PRIMARY KEY without AUTOINCREMENT, so deleted IDs at the
    high end are reused on the next insert) would inherit the orphaned
    JPEG and the user would see the wrong photo on the encounter
    grid.
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

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/gone", name="gone")
    pid = db.add_photo(
        folder_id=fid, filename="missing.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Stage the cached thumbnail that will become orphaned by the
    # remove-orphans call. This mirrors a real-world state where the
    # source file vanished from disk but the cached JPEG persists.
    thumb_file = thumb_dir / f"{pid}.jpg"
    Image.new("RGB", (50, 50), (1, 2, 3)).save(str(thumb_file), "JPEG")
    assert thumb_file.exists(), "precondition: cached thumb staged"

    app = create_app(
        db_path=db_path, thumb_cache_dir=str(thumb_dir),
        api_token="test-token-123",
    )
    client = app.test_client()
    resp = client.post(
        "/api/audit/remove-orphans",
        json={"photo_ids": [pid]},
    )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["removed"] == 1

    # The DB row is gone …
    assert db.get_photo(pid) is None
    # … and so is the cached thumbnail. A future photo that inherits
    # this ID will get a fresh thumbnail from its own source.
    assert not thumb_file.exists(), (
        "remove-orphans left a cached thumbnail behind; the next photo "
        "to inherit this rowid will be served the orphaned JPEG"
    )
