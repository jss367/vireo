# vireo/tests/test_thumbnails.py
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image


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
