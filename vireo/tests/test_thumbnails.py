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
