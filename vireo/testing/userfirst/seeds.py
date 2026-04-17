"""Pre-built seed functions for user-first test scenarios.

Each seed is a callable ``(db_path, thumb_dir, photos_root)`` that populates
the database and creates placeholder thumbnails so the app starts with
realistic data visible in the UI.
"""
import os
import sys

# The Database class lives in vireo/ and expects ``from db import Database``.
# Mirror conftest.py's approach: ensure vireo/ is on sys.path.
_VIREO_DIR = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
if _VIREO_DIR not in sys.path:
    sys.path.insert(0, _VIREO_DIR)


def _make_thumb(thumb_dir, photo_id):
    """Create a 100x100 placeholder JPEG thumbnail for *photo_id*."""
    from PIL import Image

    path = os.path.join(thumb_dir, f"{photo_id}.jpg")
    if not os.path.exists(path):
        Image.new("RGB", (100, 100), color=(80, 120, 80)).save(path)


def browse_seed(db_path, thumb_dir, photos_root):
    """Minimal seed: workspace, 3 folders, ~10 photos with keywords and ratings.

    Creates tiny placeholder thumbnails so the browse grid renders cards.
    """
    from db import Database

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    # Use photos_root when available so validate_db_folders passes;
    # fall back to synthetic paths for headless CI.
    base = photos_root if photos_root else "/test/photos"

    f1 = db.add_folder(os.path.join(base, "wildlife"), name="wildlife")
    f2 = db.add_folder(os.path.join(base, "landscapes"), name="landscapes")
    f3 = db.add_folder(
        os.path.join(base, "wildlife", "birds"), name="birds", parent_id=f1
    )

    photos = []
    specs = [
        # (folder, filename, ext, size, mtime, timestamp, rating, flag)
        (f1, "eagle01.jpg", ".jpg", 5000, 1.0, "2024-03-10T08:00:00", 5, None),
        (f1, "eagle02.jpg", ".jpg", 4800, 2.0, "2024-03-10T08:01:00", 4, None),
        (f3, "robin01.jpg", ".jpg", 3200, 3.0, "2024-04-05T07:30:00", 3, None),
        (f3, "robin02.nef", ".nef", 25000, 4.0, "2024-04-05T07:31:00", 0, None),
        (f3, "sparrow01.jpg", ".jpg", 2800, 5.0, "2024-05-12T06:00:00", 0, None),
        (f2, "sunset01.jpg", ".jpg", 6000, 6.0, "2024-06-20T19:45:00", 2, "flagged"),
        (f2, "mountain01.jpg", ".jpg", 7000, 7.0, "2024-07-04T10:00:00", 0, None),
        (f1, "hawk01.jpg", ".jpg", 4500, 8.0, "2024-08-15T11:00:00", 1, None),
        (f3, "finch01.jpg", ".jpg", 3100, 9.0, "2024-09-01T16:00:00", 0, "rejected"),
        (f1, "heron01.jpg", ".jpg", 5200, 10.0, "2024-10-22T14:30:00", 0, None),
    ]

    for folder_id, fname, ext, size, mtime, ts, rating, flag in specs:
        pid = db.add_photo(
            folder_id=folder_id,
            filename=fname,
            extension=ext,
            file_size=size,
            file_mtime=mtime,
            timestamp=ts,
        )
        photos.append(pid)
        if rating:
            db.update_photo_rating(pid, rating)
        if flag:
            db.update_photo_flag(pid, flag)

    # Add keywords and tag some photos
    k_eagle = db.add_keyword("Eagle", is_species=True)
    k_robin = db.add_keyword("Robin", is_species=True)
    k_sparrow = db.add_keyword("Sparrow", is_species=True)
    k_wildlife = db.add_keyword("Wildlife")

    db.tag_photo(photos[0], k_eagle)
    db.tag_photo(photos[1], k_eagle)
    db.tag_photo(photos[2], k_robin)
    db.tag_photo(photos[3], k_robin)
    db.tag_photo(photos[4], k_sparrow)
    db.tag_photo(photos[0], k_wildlife)
    db.tag_photo(photos[2], k_wildlife)

    # Create thumbnails
    os.makedirs(thumb_dir, exist_ok=True)
    for pid in photos:
        _make_thumb(thumb_dir, pid)

    db.conn.close()
