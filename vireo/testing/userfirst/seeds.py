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


def misses_seed(db_path, thumb_dir, photos_root):
    """Seed: three photos pre-flagged as misses (one per category).

    Exercises the /misses page and its bulk-reject flow without requiring a
    real pipeline run (which would need MegaDetector/SAM2 weights). The
    fixture sets the miss_* booleans directly, mimicking what miss_stage
    writes after classify_miss.
    """
    from db import Database

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    base = photos_root if photos_root else "/test/photos"
    folder_id = db.add_folder(os.path.join(base, "misses_fixture"), name="misses_fixture")

    ts = "2026-04-22T10:00:00+00:00"
    specs = [
        ("no_subject", "ns01.jpg", "2026-04-22T09:00:00", {"miss_no_subject": 1}),
        ("clipped",    "clip01.jpg", "2026-04-22T09:00:01", {"miss_clipped": 1}),
        ("oof",        "oof01.jpg",  "2026-04-22T09:00:02", {"miss_oof": 1}),
    ]

    photos = []
    for _cat, fname, photo_ts, flags in specs:
        pid = db.add_photo(
            folder_id=folder_id,
            filename=fname,
            extension=".jpg",
            file_size=4000,
            file_mtime=float(len(photos) + 1),
            timestamp=photo_ts,
        )
        photos.append(pid)
        col = next(iter(flags))
        db.conn.execute(
            f"UPDATE photos SET {col}=1, miss_computed_at=? WHERE id=?",
            (ts, pid),
        )
        # Write a primary detection to the canonical `detections` table so
        # the /misses cards can render bbox overlays. no_subject gets a
        # low-confidence detection (matches the pipeline behavior).
        db.save_detections(
            pid,
            [{"box": {"x": 0.35, "y": 0.35, "w": 0.2, "h": 0.2},
              "confidence": 0.10 if _cat == "no_subject" else 0.85,
              "category": "animal"}],
        )
    db.conn.commit()

    os.makedirs(thumb_dir, exist_ok=True)
    for pid in photos:
        _make_thumb(thumb_dir, pid)

    db.conn.close()


def orphan_folder_seed(db_path, thumb_dir, photos_root):
    """Seed: a child folder whose parent is linked-then-unlinked.

    Reproduces the condition that caused #597 — ``folders.parent_id`` points
    at a folder that is not linked to the active workspace. Before the
    ``get_folder_tree`` fix, the child was invisible in the browse sidebar
    (stranded under an unreachable parent bucket). After the fix, the
    child reparents to root and renders.
    """
    from db import Database

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    base = photos_root if photos_root else "/test/photos"

    # Parent is linked during creation (add_folder auto-links), child is added
    # with parent_id pointing at it, then the parent is unlinked.
    parent_id = db.add_folder(os.path.join(base, "archive"), name="archive")
    child_id = db.add_folder(
        os.path.join(base, "archive", "2024"), name="2024", parent_id=parent_id
    )
    # Independent folder that's always a root — control to prove the tree renders.
    linked_root_id = db.add_folder(os.path.join(base, "inbox"), name="inbox")

    # Unlink the parent so it's a "ghost" parent of the child.
    db.remove_workspace_folder(ws_id, parent_id)

    # Give the orphan child one photo so the grid isn't empty when filtered.
    photos = []
    for folder_id, fname, ts in (
        (child_id, "archive2024_01.jpg", "2024-01-15T10:00:00"),
        (linked_root_id, "inbox_01.jpg", "2024-11-01T09:00:00"),
    ):
        pid = db.add_photo(
            folder_id=folder_id,
            filename=fname,
            extension=".jpg",
            file_size=5000,
            file_mtime=1.0,
            timestamp=ts,
        )
        photos.append(pid)

    os.makedirs(thumb_dir, exist_ok=True)
    for pid in photos:
        _make_thumb(thumb_dir, pid)

    db.conn.close()
