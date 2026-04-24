# vireo/tests/test_scanner.py
import json
import os
import shutil
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image

requires_exiftool = pytest.mark.skipif(
    shutil.which("exiftool") is None,
    reason="exiftool not installed",
)


def _create_test_images(root, structure):
    """Create test image files in a directory structure.

    Args:
        root: base directory path
        structure: dict of {relative_path: [filenames]}
    """
    for rel_path, filenames in structure.items():
        folder = os.path.join(root, rel_path) if rel_path else root
        os.makedirs(folder, exist_ok=True)
        for fname in filenames:
            img = Image.new('RGB', (200, 100), color='green')
            img.save(os.path.join(folder, fname))


def test_scan_discovers_folders(tmp_path):
    """scan() creates folder entries for all directories containing images."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['root.jpg'],
        '2024': ['a.jpg'],
        '2024/January': ['b.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    folders = db.get_folder_tree()
    paths = [f['path'] for f in folders]
    assert root in paths
    assert os.path.join(root, '2024') in paths
    assert os.path.join(root, '2024', 'January') in paths


def test_scan_discovers_photos(tmp_path):
    """scan() creates photo entries for all image files."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['img1.jpg', 'img2.jpg'],
        'sub': ['img3.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert filenames == {'img1.jpg', 'img2.jpg', 'img3.jpg'}


def test_scan_non_recursive_only_finds_root_photos(tmp_path):
    """scan(recursive=False) only finds photos in the root folder, not subfolders."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['root.jpg'],
        'sub': ['sub.jpg'],
        'sub/deep': ['deep.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db, recursive=False)

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert filenames == {'root.jpg'}


def test_scan_reads_dimensions(tmp_path):
    """scan() reads image dimensions."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img = Image.new('RGB', (640, 480), color='blue')
    img.save(os.path.join(root, 'test.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos()
    assert photos[0]['width'] == 640
    assert photos[0]['height'] == 480


def test_scan_records_file_mtime(tmp_path):
    """scan() records file modification time."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, 'test.jpg')
    Image.new('RGB', (100, 100)).save(img_path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos()
    assert photos[0]['file_mtime'] is not None
    assert photos[0]['file_mtime'] > 0


def test_scan_progress_callback(tmp_path):
    """scan() calls progress callback with (current, total)."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg', 'c.jpg']})

    db = Database(str(tmp_path / "test.db"))
    progress = []
    scan(root, db, progress_callback=lambda cur, tot: progress.append((cur, tot)))

    assert len(progress) == 4
    assert progress[0] == (0, 3)   # initial discovery report
    assert progress[-1] == (3, 3)


def test_scan_ignores_non_image_files(tmp_path):
    """scan() skips files that aren't images."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'photo.jpg'))
    with open(os.path.join(root, 'notes.txt'), 'w') as f:
        f.write('not an image')
    with open(os.path.join(root, '.hidden.jpg'), 'w') as f:
        f.write('hidden')

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos()
    assert len(photos) == 1
    assert photos[0]['filename'] == 'photo.jpg'


def test_scan_updates_folder_counts(tmp_path):
    """scan() updates photo_count on folders."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['a.jpg', 'b.jpg'],
        'sub': ['c.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    folders = db.get_folder_tree()
    root_folder = [f for f in folders if f['path'] == root][0]
    sub_folder = [f for f in folders if f['name'] == 'sub'][0]
    assert root_folder['photo_count'] == 2
    assert sub_folder['photo_count'] == 1


def test_scan_imports_xmp_keywords(tmp_path):
    """scan() reads XMP sidecars and imports keywords into the database."""
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))

    # Create XMP sidecar with keywords
    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Northern cardinal', 'Birds'},
        hierarchical_keywords={'Birds|Northern cardinal'},
    )

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos()
    assert len(photos) == 1
    keywords = db.get_photo_keywords(photos[0]['id'])
    kw_names = {k['name'] for k in keywords}
    assert 'Northern cardinal' in kw_names
    assert 'Birds' in kw_names


def test_scan_imports_hierarchical_keywords(tmp_path):
    """scan() creates keyword hierarchy from lr:hierarchicalSubject."""
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))

    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Black kite'},
        hierarchical_keywords={'Birds|Raptors|Black kite'},
    )

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    tree = db.get_keyword_tree()
    names = {k['name'] for k in tree}
    assert 'Birds' in names
    assert 'Raptors' in names
    assert 'Black kite' in names

    # Verify hierarchy: Raptors parent is Birds
    raptors = [k for k in tree if k['name'] == 'Raptors'][0]
    birds = [k for k in tree if k['name'] == 'Birds'][0]
    assert raptors['parent_id'] == birds['id']


def test_incremental_scan_skips_unchanged(tmp_path):
    """Incremental scan skips files that haven't changed since last scan."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'old.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Add a new file
    time.sleep(0.05)
    Image.new('RGB', (200, 200)).save(os.path.join(root, 'new.jpg'))

    # Track what gets processed
    processed = []
    scan(root, db, incremental=True,
         progress_callback=lambda cur, tot: processed.append(cur))

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert 'old.jpg' in filenames
    assert 'new.jpg' in filenames


def test_incremental_scan_detects_xmp_changes(tmp_path):
    """Incremental scan re-reads XMP when xmp_mtime changes."""
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

    # Verify initial keyword
    photos = db.get_photos()
    kws = db.get_photo_keywords(photos[0]['id'])
    assert {k['name'] for k in kws} == {'Sparrow'}

    # Modify XMP - add a keyword
    time.sleep(0.05)
    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Cardinal'},
        hierarchical_keywords=set(),
    )

    scan(root, db, incremental=True)

    # Should now have both keywords (merge from XMP)
    kws = db.get_photo_keywords(photos[0]['id'])
    kw_names = {k['name'] for k in kws}
    assert 'Sparrow' in kw_names
    assert 'Cardinal' in kw_names


@requires_exiftool
def test_scan_populates_exif_data(tmp_path):
    """scan() populates the exif_data JSON column when extract_full_metadata is on."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img = Image.new('RGB', (640, 480), color='blue')
    img.save(os.path.join(root, 'test.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    row = db.conn.execute("SELECT exif_data FROM photos LIMIT 1").fetchone()
    assert row["exif_data"] is not None
    meta = json.loads(row["exif_data"])
    assert isinstance(meta, dict)
    # Should have at least a File group
    assert "File" in meta


def test_scan_pairs_raw_and_jpeg(tmp_path):
    """When a folder has IMG.cr3 and IMG.jpg, they become one photo with companion_path."""
    from db import Database
    from scanner import scan

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    # Create a JPEG
    Image.new("RGB", (200, 100), color="green").save(str(img_dir / "IMG_001.jpg"))
    # Create a fake raw file with the same base name
    with open(str(img_dir / "IMG_001.cr3"), "wb") as f:
        f.write(b"\x00" * 200)

    db = Database(str(tmp_path / "test.db"))
    scan(str(img_dir), db)

    photos = db.conn.execute("SELECT filename, companion_path FROM photos").fetchall()
    # Should be one photo record, not two
    assert len(photos) == 1

    photo = photos[0]
    # Raw is primary, JPEG is companion
    assert photo["filename"] == "IMG_001.cr3"
    assert photo["companion_path"] == "IMG_001.jpg"


def test_scan_late_arriving_raw_pairs_with_existing_jpeg(tmp_path):
    """Importing raws after JPEGs matches them to existing photo records."""
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from db import Database
    from scanner import scan

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    from PIL import Image

    # First scan: JPEG only
    Image.new("RGB", (200, 100), color="green").save(str(img_dir / "IMG_001.jpg"))
    db = Database(str(tmp_path / "test.db"))
    scan(str(img_dir), db)

    photos_before = db.conn.execute("SELECT id FROM photos").fetchall()
    assert len(photos_before) == 1

    # Add metadata to the JPEG record (simulating user edits before raw arrives)
    jpeg_id = photos_before[0]["id"]
    db.conn.execute(
        "UPDATE photos SET rating = 4, flag = 'flagged', timestamp = '2024-06-15T10:30:00' WHERE id = ?",
        (jpeg_id,),
    )
    # Add a keyword to the JPEG
    kw_id = db.add_keyword("Robin")
    db.tag_photo(jpeg_id, kw_id)
    db.conn.commit()

    # Now add the raw file and rescan
    with open(str(img_dir / "IMG_001.cr3"), "wb") as f:
        f.write(b"\x00" * 200)
    scan(str(img_dir), db)

    photos_after = db.conn.execute(
        "SELECT id, filename, companion_path, rating, flag, timestamp FROM photos"
    ).fetchall()
    # Still one photo — raw becomes primary, JPEG becomes companion
    assert len(photos_after) == 1
    assert photos_after[0]["filename"] == "IMG_001.cr3"
    assert photos_after[0]["companion_path"] == "IMG_001.jpg"

    # Metadata should have been transferred from the JPEG record
    assert photos_after[0]["rating"] == 4
    assert photos_after[0]["flag"] == "flagged"
    assert photos_after[0]["timestamp"] == "2024-06-15T10:30:00"

    # Keywords should have been transferred
    raw_id = photos_after[0]["id"]
    keywords = db.get_photo_keywords(raw_id)
    kw_names = {k["name"] for k in keywords}
    assert "Robin" in kw_names


def test_pairing_merges_predictions_without_unique_violation(tmp_path):
    """Pairing raw+JPEG deduplicates predictions that would violate UNIQUE(photo_id, model, workspace_id)."""
    from db import Database
    from scanner import scan

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    # First scan: JPEG only
    Image.new("RGB", (200, 100), color="green").save(str(img_dir / "IMG_001.jpg"))
    db = Database(str(tmp_path / "test.db"))
    scan(str(img_dir), db)

    jpeg_id = db.conn.execute("SELECT id FROM photos").fetchone()["id"]

    # Classify the JPEG — create detection then add a prediction
    det_ids = db.save_detections(jpeg_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.85, "bioclip")

    # Now add the raw file and rescan — this creates a new photo record for the raw,
    # then the classify job also runs on the raw (simulated here)
    with open(str(img_dir / "IMG_001.cr3"), "wb") as f:
        f.write(b"\x00" * 200)
    scan(str(img_dir), db)

    # At this point, the raw should have picked up the JPEG's prediction.
    # There were two records (raw + jpeg), both classified with same model/workspace,
    # and pairing merged them without IntegrityError.
    photos = db.conn.execute("SELECT id, filename FROM photos").fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"

    raw_id = photos[0]["id"]
    preds = db.conn.execute(
        """SELECT pr.species, pr.confidence FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           WHERE d.photo_id = ?""",
        (raw_id,),
    ).fetchall()
    assert len(preds) == 1
    assert preds[0]["species"] == "Robin"


def test_pairing_merges_duplicate_predictions_keeps_higher_confidence(tmp_path):
    """When both raw and JPEG have predictions, both detections transfer to primary."""
    from db import Database

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    # Create both files
    Image.new("RGB", (200, 100), color="green").save(str(img_dir / "IMG_001.jpg"))
    with open(str(img_dir / "IMG_001.cr3"), "wb") as f:
        f.write(b"\x00" * 200)

    db = Database(str(tmp_path / "test.db"))
    # Scan — this creates both records, then pairs them. But we need BOTH to have
    # predictions before pairing. So: scan once (creates paired result), undo pairing
    # manually to set up the scenario, then re-pair.
    # Instead: create photos manually, add predictions, then run pairing.
    from scanner import _pair_raw_jpeg_companions

    fid = db.add_folder(str(img_dir), name="photos")
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG_001.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG_001.cr3", extension=".cr3",
                          file_size=2000, file_mtime=1.0)

    # Both classified with same model — JPEG has higher confidence
    jpeg_det = db.save_detections(jpeg_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    raw_det = db.save_detections(raw_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(jpeg_det[0], "Robin", 0.95, "bioclip")
    db.add_prediction(raw_det[0], "Robin", 0.70, "bioclip")

    # Run pairing — should NOT raise IntegrityError
    _pair_raw_jpeg_companions(db)

    photos = db.conn.execute("SELECT id, filename, companion_path FROM photos").fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"

    preds = db.conn.execute(
        """SELECT pr.species, pr.confidence FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           WHERE d.photo_id = ?""",
        (photos[0]["id"],),
    ).fetchall()
    # Both detections (and their predictions) transfer to the primary photo.
    # UNIQUE(detection_id, model) doesn't conflict since detection IDs differ.
    assert len(preds) == 2
    confidences = sorted(p["confidence"] for p in preds)
    assert confidences == [0.70, 0.95]


def test_pairing_transfers_inat_submissions(tmp_path):
    """Pairing raw+JPEG transfers iNat submissions from companion to primary."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(img_dir), name="photos")
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG_001.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG_001.cr3", extension=".cr3",
                          file_size=2000, file_mtime=1.0)

    # JPEG was submitted to iNaturalist
    db.record_inat_submission(jpeg_id, observation_id=12345,
                              observation_url="https://inaturalist.org/observations/12345")

    # Verify submission exists
    subs_before = db.get_inat_submissions([jpeg_id])
    assert jpeg_id in subs_before

    # Run pairing
    _pair_raw_jpeg_companions(db)

    photos = db.conn.execute("SELECT id, filename FROM photos").fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"

    # Submission should be on the raw (primary) now, not lost
    raw_id_after = photos[0]["id"]
    subs_after = db.get_inat_submissions([raw_id_after])
    assert raw_id_after in subs_after
    assert subs_after[raw_id_after]["observation_id"] == 12345


def test_pairing_deduplicates_inat_submissions(tmp_path):
    """When both raw and JPEG have iNat submissions for the same observation, pairing doesn't crash."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(img_dir), name="photos")
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG_001.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG_001.cr3", extension=".cr3",
                          file_size=2000, file_mtime=1.0)

    # Both photos submitted for the same observation (e.g., user submitted JPEG,
    # then raw was auto-submitted via a script)
    db.record_inat_submission(jpeg_id, observation_id=12345,
                              observation_url="https://inaturalist.org/observations/12345")
    db.record_inat_submission(raw_id, observation_id=12345,
                              observation_url="https://inaturalist.org/observations/12345")
    # JPEG also has a different observation
    db.record_inat_submission(jpeg_id, observation_id=67890,
                              observation_url="https://inaturalist.org/observations/67890")

    # Should NOT raise IntegrityError
    _pair_raw_jpeg_companions(db)

    photos = db.conn.execute("SELECT id, filename FROM photos").fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"

    raw_id_after = photos[0]["id"]
    # Both observations should be preserved on the primary
    subs = db.conn.execute(
        "SELECT observation_id FROM inat_submissions WHERE photo_id = ? ORDER BY observation_id",
        (raw_id_after,),
    ).fetchall()
    obs_ids = [s["observation_id"] for s in subs]
    assert 12345 in obs_ids
    assert 67890 in obs_ids


def test_scan_stores_file_hash(tmp_path):
    """Scanning a folder computes and stores SHA-256 file_hash for each photo."""
    from db import Database
    from scanner import scan

    # Create a test image
    img_dir = tmp_path / "photos"
    img_dir.mkdir()
    img = Image.new("RGB", (200, 100), color="green")
    img.save(str(img_dir / "test.jpg"))

    db = Database(str(tmp_path / "test.db"))
    scan(str(img_dir), db)

    photo = db.conn.execute("SELECT file_hash FROM photos LIMIT 1").fetchone()
    assert photo["file_hash"] is not None
    assert len(photo["file_hash"]) == 64  # SHA-256 hex digest length


def test_extract_dimensions_raw_skips_exif_thumbnail_size():
    """For RAW files, ExifImageWidth/Height is the embedded JPEG thumbnail (e.g. 160x120),
    not the actual image. _extract_dimensions should return the real dimensions from
    File:ImageWidth/Height instead."""
    from scanner import _extract_dimensions

    # Simulate ExifTool output for a Nikon NEF file:
    # EXIF:ExifImageWidth/Height = 160x120 (embedded thumbnail)
    # File:ImageWidth/Height = 8256x5504 (actual RAW image)
    exif_group = {
        "ExifImageWidth": 160,
        "ExifImageHeight": 120,
        "ImageWidth": 160,
        "ImageHeight": 120,
    }
    file_group = {
        "ImageWidth": 8256,
        "ImageHeight": 5504,
    }

    width, height = _extract_dimensions(exif_group, file_group, extension=".nef")

    assert width == 8256, f"Expected actual RAW width 8256, got {width} (embedded thumbnail)"
    assert height == 5504, f"Expected actual RAW height 5504, got {height} (embedded thumbnail)"


def test_extract_dimensions_jpeg_still_uses_exif():
    """For JPEG files, ExifImageWidth/Height should still be the first priority."""
    from scanner import _extract_dimensions

    exif_group = {
        "ExifImageWidth": 6000,
        "ExifImageHeight": 4000,
    }
    file_group = {
        "ImageWidth": 6000,
        "ImageHeight": 4000,
    }

    width, height = _extract_dimensions(exif_group, file_group, extension=".jpg")

    assert width == 6000
    assert height == 4000


def test_extract_dimensions_raw_falls_back_to_exif_imagewidth():
    """For RAW files without File dimensions, EXIF:ImageWidth (non-ExifImageWidth) is used."""
    from scanner import _extract_dimensions

    exif_group = {
        "ExifImageWidth": 160,
        "ExifImageHeight": 120,
        "ImageWidth": 8256,
        "ImageHeight": 5504,
    }
    file_group = {}

    width, height = _extract_dimensions(exif_group, file_group, extension=".nef")

    # Should skip ExifImageWidth (thumbnail) but still find ImageWidth
    assert width == 8256
    assert height == 5504


def test_extract_dimensions_all_raw_extensions():
    """All supported RAW extensions should skip ExifImageWidth/Height."""
    from scanner import _extract_dimensions

    raw_exts = [".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"]

    for ext in raw_exts:
        exif_group = {"ExifImageWidth": 160, "ExifImageHeight": 120}
        file_group = {"ImageWidth": 8256, "ImageHeight": 5504}

        width, height = _extract_dimensions(exif_group, file_group, extension=ext)
        assert width == 8256, f"Failed for {ext}: got width {width}"
        assert height == 5504, f"Failed for {ext}: got height {height}"


def test_pair_raw_jpeg_transfers_gps_and_metadata(tmp_path):
    """Pairing raw+JPEG transfers GPS, exif_data, and focal_length from companion."""
    import json

    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    # JPEG has GPS and metadata, RAW does not
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.nef", extension=".nef",
                          file_size=25000000, file_mtime=1.0)

    exif_json = json.dumps({"EXIF": {"Make": "Nikon", "Model": "Z9", "ISO": 400}})
    db.conn.execute(
        "UPDATE photos SET latitude=32.88, longitude=-117.25, exif_data=?, focal_length=400.0 WHERE id=?",
        (exif_json, jpeg_id),
    )
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute(
        "SELECT filename, latitude, longitude, exif_data, focal_length FROM photos"
    ).fetchone()
    assert photo["filename"] == "IMG.nef"
    assert photo["latitude"] == 32.88
    assert photo["longitude"] == -117.25
    assert photo["focal_length"] == 400.0
    meta = json.loads(photo["exif_data"])
    assert meta["EXIF"]["Make"] == "Nikon"


def test_pair_raw_jpeg_keeps_primary_gps_when_present(tmp_path):
    """If RAW already has GPS, companion GPS is not overwritten."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.cr3", extension=".cr3",
                          file_size=20000000, file_mtime=1.0)

    # Both have GPS but different coords — primary should keep its own
    db.conn.execute(
        "UPDATE photos SET latitude=40.0, longitude=-74.0 WHERE id=?", (jpeg_id,))
    db.conn.execute(
        "UPDATE photos SET latitude=32.0, longitude=-117.0 WHERE id=?", (raw_id,))
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute("SELECT latitude, longitude FROM photos").fetchone()
    assert photo["latitude"] == 32.0
    assert photo["longitude"] == -117.0


def test_pair_raw_jpeg_transfers_zero_gps_from_companion(tmp_path):
    """A companion with latitude=0.0 (equator) should be transferred to a RAW that has no GPS."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.nef", extension=".nef",
                          file_size=25000000, file_mtime=1.0)

    # JPEG is on the equator/prime meridian (0.0, 0.0) — falsy but valid
    db.conn.execute(
        "UPDATE photos SET latitude=0.0, longitude=0.0 WHERE id=?", (jpeg_id,))
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute("SELECT filename, latitude, longitude FROM photos").fetchone()
    assert photo["filename"] == "IMG.nef"
    assert photo["latitude"] == 0.0
    assert photo["longitude"] == 0.0


def test_pair_raw_jpeg_does_not_overwrite_zero_primary_gps(tmp_path):
    """A RAW with latitude=0.0 (equator) should NOT be overwritten by companion GPS."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.cr3", extension=".cr3",
                          file_size=20000000, file_mtime=1.0)

    # JPEG has non-zero GPS, RAW sits at equator (0.0, 0.0) — must not be overwritten
    db.conn.execute(
        "UPDATE photos SET latitude=51.5, longitude=-0.1 WHERE id=?", (jpeg_id,))
    db.conn.execute(
        "UPDATE photos SET latitude=0.0, longitude=0.0 WHERE id=?", (raw_id,))
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute("SELECT latitude, longitude FROM photos").fetchone()
    assert photo["latitude"] == 0.0
    assert photo["longitude"] == 0.0


def test_scan_extracts_working_copy_for_raw(tmp_path, monkeypatch):
    """Scanning a RAW file creates a working copy JPEG."""
    import scanner
    from db import Database

    # Set up vireo dir structure
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    # Create a fake NEF file
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    nef_file = photo_dir / "IMG_001.nef"
    nef_file.write_bytes(b"fake raw data")

    # Mock ExifTool to return empty metadata
    monkeypatch.setattr(scanner, "extract_metadata", lambda paths: {})

    # Mock extract_working_copy to actually create a file (simulates success)
    def fake_extract(source, output, max_size=4096, quality=92):
        os.makedirs(os.path.dirname(output), exist_ok=True)
        Image.new("RGB", (4096, 2731)).save(output, "JPEG")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    photos = db.get_photos(per_page=999999)
    assert len(photos) == 1
    assert photos[0]["working_copy_path"] is not None
    assert os.path.exists(os.path.join(str(vireo_dir), photos[0]["working_copy_path"]))


def test_scan_skips_working_copy_for_jpeg(tmp_path, monkeypatch):
    """Scanning a JPEG file does not create a working copy."""
    import scanner
    from db import Database

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    jpg_file = photo_dir / "IMG_001.jpg"
    Image.new("RGB", (3000, 2000)).save(str(jpg_file), "JPEG")

    monkeypatch.setattr(scanner, "extract_metadata", lambda paths: {})

    # Mock extract_working_copy -- should never be called for JPEGs
    calls = []

    def fake_extract(source, output, max_size=4096, quality=92):
        calls.append(source)
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    photos = db.get_photos(per_page=999999)
    assert len(photos) == 1
    assert photos[0]["working_copy_path"] is None
    assert len(calls) == 0


def test_scan_uses_companion_jpeg_for_working_copy(tmp_path, monkeypatch):
    """When RAW+JPEG pair exists, working copy is extracted from the companion JPEG."""
    import scanner
    from db import Database

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()

    # Create RAW + JPEG pair
    nef_file = photo_dir / "IMG_001.nef"
    nef_file.write_bytes(b"fake raw data")
    jpg_file = photo_dir / "IMG_001.jpg"
    Image.new("RGB", (6000, 4000), color=(255, 0, 0)).save(str(jpg_file), "JPEG")

    # Mock ExifTool
    monkeypatch.setattr(scanner, "extract_metadata", lambda paths: {})

    # Track which source file extract_working_copy is called with
    sources_used = []

    def fake_extract(source, output, max_size=4096, quality=92):
        sources_used.append(source)
        os.makedirs(os.path.dirname(output), exist_ok=True)
        Image.new("RGB", (4096, 2731)).save(output, "JPEG")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    # After companion pairing, the RAW should have a working copy
    photos = db.get_photos(per_page=999999)
    raw_photos = [p for p in photos if p["extension"] == ".nef"]
    assert len(raw_photos) == 1
    assert raw_photos[0]["working_copy_path"] is not None

    # Verify the companion JPEG was used as the source, not the RAW file
    assert len(sources_used) == 1
    assert sources_used[0].endswith("IMG_001.jpg"), (
        f"Expected companion JPEG as source, got: {sources_used[0]}"
    )


# --- _extract_timestamp tests ---

def test_extract_timestamp_subsec():
    """_extract_timestamp includes sub-second precision from SubSecTimeOriginal."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00", "SubSecTimeOriginal": "123"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00.123000"


def test_extract_timestamp_no_subsec():
    """_extract_timestamp works without SubSecTimeOriginal."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00"


def test_extract_timestamp_garbage_subsec():
    """_extract_timestamp ignores non-numeric SubSecTimeOriginal."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00", "SubSecTimeOriginal": "abc"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00"


def test_extract_timestamp_subsec_fallback():
    """_extract_timestamp falls back to SubSecTime when SubSecTimeOriginal is absent."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00", "SubSecTime": "50"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00.500000"


def test_extract_timestamp_subsec_long():
    """_extract_timestamp truncates sub-second values longer than 6 digits."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00", "SubSecTimeOriginal": "12345678"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00.123456"


# --- Incremental rescan metadata_missing heuristic tests ---

def _setup_scanned_photo(tmp_path, pil_size=(640, 480)):
    """Create a JPEG, run a fresh scan, return (db, photo_id, image_path)."""
    import scanner
    from db import Database

    root = str(tmp_path / "photos")
    os.makedirs(root)
    image_path = os.path.join(root, "photo.jpg")
    Image.new("RGB", pil_size, color="green").save(image_path, "JPEG")

    db = Database(str(tmp_path / "test.db"))
    # Mock ExifTool so the first scan populates exif_data with real
    # dimensions, independent of whether exiftool is installed.
    def fake_extract(paths, restricted_tags=None):
        return {
            p: {"File": {"ImageWidth": pil_size[0], "ImageHeight": pil_size[1]},
                "EXIF": {}, "Composite": {}}
            for p in paths
        }
    import metadata
    original = metadata.extract_metadata
    metadata.extract_metadata = fake_extract
    scanner.extract_metadata = fake_extract
    try:
        scanner.scan(root, db)
    finally:
        metadata.extract_metadata = original
        scanner.extract_metadata = original

    row = db.conn.execute(
        "SELECT id FROM photos WHERE filename='photo.jpg'"
    ).fetchone()
    return db, root, image_path, row["id"]


def test_incremental_rescan_reextracts_when_timestamp_null(tmp_path, monkeypatch):
    """Incremental scan re-processes a photo whose timestamp is NULL
    and exif_data is NULL (existing behavior — regression guard)."""
    import scanner

    db, root, image_path, pid = _setup_scanned_photo(tmp_path)

    # Simulate broken state: timestamp lost, dims wrong, exif_data cleared.
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, width=100, height=100, "
        "exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    def fake_extract(paths, restricted_tags=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    scanner.scan(root, db, incremental=True)

    row = db.conn.execute(
        "SELECT width, height, exif_data FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["width"] == 640  # repopulated from fake ExifTool
    assert row["height"] == 480
    assert row["exif_data"] is not None


def test_incremental_rescan_reextracts_when_raw_dims_suspect(tmp_path, monkeypatch):
    """Incremental scan re-processes a row where extension is RAW and
    width < 1000 (the 160x120 embedded-thumb bug), even when timestamp
    is populated — provided exif_data is NULL so the guard doesn't block."""
    import scanner

    db, root, image_path, pid = _setup_scanned_photo(tmp_path)

    # Simulate broken state: fake RAW extension with thumbnail dims and
    # populated timestamp. exif_data=NULL so the exif_extracted guard
    # doesn't block re-extraction.
    db.conn.execute(
        "UPDATE photos SET extension='.nef', width=160, height=120, "
        "timestamp='2020-01-01T12:00:00', exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    def fake_extract(paths, restricted_tags=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    scanner.scan(root, db, incremental=True)

    row = db.conn.execute(
        "SELECT width, height, exif_data FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["width"] == 640
    assert row["height"] == 480
    assert row["exif_data"] is not None


def test_incremental_rescan_skips_small_jpeg_dims_not_raw(tmp_path, monkeypatch):
    """Incremental scan does NOT re-process a non-RAW row with suspicious
    small dimensions. The dims heuristic is RAW-specific so JPEGs, PNGs,
    etc. that are legitimately tiny aren't re-extracted repeatedly."""
    import scanner

    db, root, image_path, pid = _setup_scanned_photo(tmp_path)

    # Simulate small-dims on a non-RAW extension; timestamp populated so
    # the NULL-timestamp branch doesn't fire either.
    db.conn.execute(
        "UPDATE photos SET extension='.jpg', width=160, height=120, "
        "timestamp='2020-01-01T12:00:00', exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    called_with = []
    def fake_extract(paths, restricted_tags=None):
        called_with.append(list(paths))
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    scanner.scan(root, db, incremental=True)

    # width stays at the synthetic broken value because we didn't reprocess.
    row = db.conn.execute(
        "SELECT width, height FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["width"] == 160
    assert row["height"] == 120
    # And extract_metadata was never called with this file.
    assert all(image_path not in batch for batch in called_with)


def test_scan_restrict_files_ignores_files_not_in_list(tmp_path, monkeypatch):
    """When scan is called with restrict_files, files in restrict_dirs
    that are not in the list are left untouched — even if they're brand
    new and not yet in the DB. This prevents the pipeline's repair path
    from ingesting new files as a side effect of fixing broken metadata."""
    import scanner
    from db import Database

    root = str(tmp_path / "photos")
    os.makedirs(root)
    existing_file = os.path.join(root, "existing.jpg")
    Image.new("RGB", (640, 480), color="green").save(existing_file, "JPEG")

    db = Database(str(tmp_path / "test.db"))

    def fake_extract(paths, restricted_tags=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    # Seed the DB with only the existing file, then force broken state.
    scanner.scan(root, db)
    pid = db.conn.execute(
        "SELECT id FROM photos WHERE filename='existing.jpg'"
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    # NOW add an untracked file to the same folder (after initial scan).
    new_file = os.path.join(root, "new_untracked.jpg")
    Image.new("RGB", (640, 480), color="blue").save(new_file, "JPEG")

    # Second scan with restrict_files constrained to the existing file only.
    scanner.scan(
        root, db,
        incremental=True,
        restrict_dirs=[root],
        restrict_files={existing_file},
    )

    # new_untracked.jpg should NOT have been ingested.
    filenames = [p["filename"] for p in db.get_photos(per_page=999999)]
    assert "new_untracked.jpg" not in filenames
    assert "existing.jpg" in filenames


def test_incremental_rescan_respects_exif_extracted_guard(tmp_path, monkeypatch):
    """Incremental scan does NOT re-process a row when exif_data is
    populated, even if the row otherwise looks broken. The guard prevents
    retry loops on photos where ExifTool has already produced output
    (e.g. files with genuinely missing EXIF timestamps)."""
    import scanner

    db, root, image_path, pid = _setup_scanned_photo(tmp_path)

    # Broken-looking state, but exif_data is populated (ExifTool already
    # ran once). Scanner must skip this row.
    db.conn.execute(
        "UPDATE photos SET extension='.nef', width=160, height=120, "
        "timestamp='2020-01-01T12:00:00', "
        "exif_data='{\"File\":{}}' WHERE id=?", (pid,)
    )
    db.conn.commit()

    called_with = []
    def fake_extract(paths, restricted_tags=None):
        called_with.append(list(paths))
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    scanner.scan(root, db, incremental=True)

    row = db.conn.execute(
        "SELECT width, height FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["width"] == 160
    assert row["height"] == 120
    assert all(image_path not in batch for batch in called_with)


def test_resolve_worker_count_tiny_batch_is_sequential():
    """Batches below 8 files always use 1 worker."""
    from scanner import _resolve_worker_count
    assert _resolve_worker_count(list(range(7))) == 1


def test_resolve_worker_count_capped_by_batch_size(monkeypatch):
    """Worker count never exceeds the batch size."""
    import config as cfg
    import scanner

    monkeypatch.setattr(cfg, "get", lambda _k: 0)
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 32)
    # 10 files on a 32-core box should top out at 10 workers.
    assert scanner._resolve_worker_count(list(range(10))) == 10


def test_resolve_worker_count_clamps_to_windows_limit(monkeypatch):
    """On Windows, ProcessPoolExecutor rejects max_workers > 61, so clamp."""
    import config as cfg
    import scanner

    monkeypatch.setattr(cfg, "get", lambda _k: 0)
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 128)
    monkeypatch.setattr(scanner.sys, "platform", "win32")
    # Batch is large enough that it wouldn't otherwise clamp the count.
    workers = scanner._resolve_worker_count(list(range(200)))
    assert workers == scanner._WINDOWS_MAX_WORKERS == 61


def test_resolve_worker_count_clamps_configured_value_on_windows(monkeypatch):
    """Explicit scan_workers above 61 is still clamped on Windows."""
    import config as cfg
    import scanner

    monkeypatch.setattr(cfg, "get", lambda _k: 96)
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 128)
    monkeypatch.setattr(scanner.sys, "platform", "win32")
    assert scanner._resolve_worker_count(list(range(200))) == 61


def test_resolve_worker_count_no_windows_cap_on_posix(monkeypatch):
    """The 61-worker cap must not apply on non-Windows platforms."""
    import config as cfg
    import scanner

    monkeypatch.setattr(cfg, "get", lambda _k: 0)
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 128)
    monkeypatch.setattr(scanner.sys, "platform", "linux")
    assert scanner._resolve_worker_count(list(range(200))) == 128


# -- scan resilience: retry on locked DB, mark folder partial on abort --


class _FlakyConn:
    """Connection proxy that injects commit failures for testing.

    sqlite3.Connection.commit is read-only at the instance level, so tests
    that need to simulate transient commit failures wrap the real connection
    in this proxy. All other attributes pass through to the real connection
    so code that calls ``conn.execute(...)`` etc. behaves identically.
    """

    def __init__(self, real, fail_on_calls):
        """fail_on_calls: dict {call_number: exception_to_raise}."""
        self._real = real
        self._fail_on_calls = dict(fail_on_calls)
        self._call_count = 0

    def commit(self):
        self._call_count += 1
        exc = self._fail_on_calls.get(self._call_count)
        if exc is not None:
            raise exc
        return self._real.commit()

    # sqlite3.Connection is used as a context manager in db.py
    # (``with self.conn:`` for transactions). Python bypasses ``__getattr__``
    # for dunder lookups, so we must forward these explicitly. Route commit
    # through our own method so the fail injection still fires.
    def __enter__(self):
        self._real.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            try:
                self.commit()
            except BaseException:
                self._real.rollback()
                raise
        else:
            self._real.rollback()
        return False

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_scan_retries_on_database_is_locked(tmp_path):
    """If a commit hits 'database is locked', scan retries instead of aborting.

    busy_timeout covers most cases, but a retry wrapper handles the tail where
    a contended DB exceeds the timeout mid-scan. Without it, a single transient
    lock aborts the whole scan and leaves the folder partially populated.
    """
    import sqlite3

    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # First two commits raise 'database is locked'; subsequent commits succeed.
    locked = sqlite3.OperationalError("database is locked")
    db.conn = _FlakyConn(db.conn, fail_on_calls={1: locked, 2: locked})

    scanner_mod.scan(root, db)

    filenames = {
        p["filename"]
        for p in db.conn.execute("SELECT filename FROM photos").fetchall()
    }
    assert filenames == {"a.jpg", "b.jpg"}, (
        f"expected both photos persisted after retries, got {filenames}"
    )


def test_scan_marks_folder_partial_on_unrecoverable_failure(tmp_path):
    """When scan can't recover, the folder is marked 'partial' before raising.

    Visible state: user sees the folder in its UI with a 'partial' badge and
    knows to rescan, instead of believing the folder is fully imported.
    """
    import sqlite3

    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg', 'c.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # Second commit raises a non-lock OperationalError that retry won't
    # swallow. Scan must mark the folder partial and re-raise.
    db.conn = _FlakyConn(
        db.conn,
        fail_on_calls={2: sqlite3.OperationalError("disk I/O error")},
    )

    with pytest.raises(sqlite3.OperationalError):
        scanner_mod.scan(root, db)

    # Unwrap proxy for the final assertion.
    real_conn = db.conn._real
    row = real_conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row is not None, "folder row should exist despite aborted scan"
    assert row["status"] == "partial", (
        f"expected folder.status='partial' after mid-scan failure, got {row['status']!r}"
    )


def test_partial_folder_is_visible_in_folder_tree(tmp_path):
    """Folders flagged 'partial' must still render in the browse-page tree.

    get_folder_tree() historically required status='ok'. After marking a
    folder partial we need it to STILL appear so the user can see the badge
    and initiate a rescan — otherwise 'partial' silently hides the folder.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/p")
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (fid,))
    db.conn.commit()

    tree = db.get_folder_tree()
    ids = {row["id"] for row in tree}
    assert fid in ids, "partial folder should still appear in get_folder_tree"
    # Status should be queryable so the UI can render the badge.
    partial_row = next(row for row in tree if row["id"] == fid)
    assert partial_row["status"] == "partial"


def test_successful_scan_clears_partial_flag(tmp_path):
    """A successful rescan of a previously-partial folder restores 'ok'."""
    import sqlite3

    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # First scan: fail partway through to leave the folder 'partial'.
    db.conn = _FlakyConn(
        db.conn,
        fail_on_calls={2: sqlite3.OperationalError("disk I/O error")},
    )
    with pytest.raises(sqlite3.OperationalError):
        scanner_mod.scan(root, db)
    real_conn = db.conn._real
    row = real_conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row["status"] == "partial"

    # Second scan: succeed and clear the flag.
    db.conn = real_conn
    scanner_mod.scan(root, db)
    row = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row["status"] == "ok", (
        f"successful rescan should flip partial → ok, got {row['status']!r}"
    )


def test_successful_scan_clears_partial_on_touched_subfolders(tmp_path):
    """Recursive scan clears 'partial' on subfolders the scan actually touched.

    The exception path flags every touched subfolder as partial; the success
    path must reset those same subfolders so a user who rescans after a
    failure sees a clean tree.
    """
    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['root.jpg'],
        'sub': ['nested.jpg'],
    })
    db = Database(str(tmp_path / "test.db"))

    scanner_mod.scan(root, db)
    # Mark both folders partial to simulate a prior mid-scan abort.
    db.conn.execute("UPDATE folders SET status = 'partial'")
    db.conn.commit()

    scanner_mod.scan(root, db)

    rows = db.conn.execute(
        "SELECT path, status FROM folders ORDER BY path"
    ).fetchall()
    statuses = {r["path"]: r["status"] for r in rows}
    assert statuses[root] == "ok", (
        f"root should be cleared back to ok, got {statuses[root]!r}"
    )
    sub_path = os.path.join(root, "sub")
    assert statuses[sub_path] == "ok", (
        f"touched subfolder should be cleared back to ok, "
        f"got {statuses[sub_path]!r}"
    )


def test_partial_folder_photos_remain_visible_in_queries(tmp_path):
    """Photos in 'partial' folders must stay queryable through read paths.

    Before this fix, `f.status = 'ok'` joins across `db.py` excluded photos
    from partial folders, so an interrupted scan could make already-imported
    photos disappear from the UI.
    """
    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['seen.jpg']})
    db = Database(str(tmp_path / "test.db"))

    scanner_mod.scan(root, db)
    db.conn.execute(
        "UPDATE folders SET status = 'partial' WHERE path = ?", (root,)
    )
    db.conn.commit()

    # Photo queries should still return the photo.
    photos = db.get_photos(per_page=100)
    filenames = {p["filename"] for p in photos}
    assert "seen.jpg" in filenames, (
        f"photo in partial folder should remain visible, got {filenames}"
    )

    # And coverage stats should still count it.
    stats = db.get_coverage_stats()
    assert stats["total"] >= 1, (
        f"coverage should count photos in partial folders, "
        f"got total={stats['total']!r}"
    )


def test_successful_noop_incremental_scan_clears_partial(tmp_path):
    """A no-op incremental rescan must still clear 'partial' on scoped folders.

    If the success-path reset is gated only on the main loop's
    ``touched_folder_ids`` set, a successful incremental scan that processes
    zero files (all photos unchanged) leaves ``status='partial'`` stuck and
    the folder hidden from ``status='ok'`` read paths. The reset must also
    run for the outer scan scope (root + restrict_dirs).
    """
    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # Initial clean scan so photos are indexed.
    scanner_mod.scan(root, db)

    # Simulate the after-failure state: folder got flagged 'partial' by a
    # prior aborted scan even though all photo rows are already present.
    db.conn.execute(
        "UPDATE folders SET status = 'partial' WHERE path = ?", (root,)
    )
    db.conn.commit()

    # Incremental rescan — no files changed, so the main loop touches zero
    # folders. The scan scope fallback should still clear 'partial'.
    scanner_mod.scan(root, db, incremental=True)

    row = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row["status"] == "ok", (
        f"no-op incremental scan should flip partial → ok, got {row['status']!r}"
    )


def test_pre_pass_failure_marks_folder_partial(tmp_path):
    """A non-retryable DB error during the pre-pass XMP commit flags the folder.

    Pre-pass XMP re-imports commit before the main scan loop begins. If that
    commit raises a non-transient error, the scan aborts with the folder row
    still ``status='ok'`` unless the pre-pass is wrapped in the same partial-
    status recovery path as the main loop.
    """
    import sqlite3

    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # Initial clean scan so the photo row exists for incremental mode.
    scanner_mod.scan(root, db)

    # Touch the XMP sidecar so the pre-pass re-imports keywords and commits.
    xmp_path = os.path.join(root, "a.xmp")
    with open(xmp_path, "w") as f:
        f.write("<x:xmpmeta xmlns:x='adobe:ns:meta/'></x:xmpmeta>")
    # Make the existing row's xmp_mtime stale so pre-pass treats it as changed.
    db.conn.execute("UPDATE photos SET xmp_mtime = 0 WHERE filename = 'a.jpg'")
    db.conn.commit()

    # First commit after scan starts is the pre-pass XMP UPDATE. Raise a
    # non-retryable OperationalError there.
    db.conn = _FlakyConn(
        db.conn,
        fail_on_calls={1: sqlite3.OperationalError("disk I/O error")},
    )

    with pytest.raises(sqlite3.OperationalError):
        scanner_mod.scan(root, db, incremental=True)

    real_conn = db.conn._real
    row = real_conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row is not None
    assert row["status"] == "partial", (
        f"expected folder.status='partial' after pre-pass failure, got {row['status']!r}"
    )


def test_rescan_invalidates_stale_thumbnail_when_file_content_changes(tmp_path):
    """When a file's content changes on disk, re-scan must invalidate the
    stale thumbnail so the next serve regenerates from fresh pixels.

    Regression test for the _D851925.NEF bug where a photo's thumbnail showed
    a bird but the full image (derived from the current file) was a squirrel:
    the source had been replaced, file_hash was updated on re-scan, but the
    thumbnail cache was never invalidated.
    """
    from db import Database
    from scanner import scan
    from thumbnails import generate_thumbnail

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "photo.jpg")
    # Original content: solid red
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    cache_dir = vireo_dir / "thumbnails"
    cache_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))

    photos = db.get_photos(per_page=100)
    assert len(photos) == 1
    photo_id = photos[0]["id"]
    original_hash = db.conn.execute(
        "SELECT file_hash FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()[0]
    assert original_hash is not None

    thumb_path = str(cache_dir / f"{photo_id}.jpg")
    generate_thumbnail(photo_id, img_path, str(cache_dir))
    assert os.path.exists(thumb_path)

    # Replace file content (same filename, different pixels → new hash + new mtime)
    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(img_path, "JPEG")
    # Ensure file_mtime differs from the DB value so incremental scan re-processes.
    new_mtime = os.path.getmtime(img_path)
    db_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()[0]
    assert new_mtime != db_mtime

    # Re-scan: scanner must detect the content change and drop the stale thumbnail.
    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    updated_hash = db.conn.execute(
        "SELECT file_hash FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()[0]
    assert updated_hash != original_hash, "sanity: scanner should have updated file_hash"

    assert not os.path.exists(thumb_path), (
        "Scanner must invalidate the cached thumbnail when file content changes; "
        "leaving it on disk is how thumbnail/full-image mismatches get baked in."
    )


def test_rescan_invalidates_preview_cache_rows_when_file_content_changes(tmp_path):
    """preview_cache LRU rows must be removed alongside preview files when
    a photo's content changes, or total_bytes accounting reports ghost
    bytes for files that no longer exist and quota eviction starts
    targeting valid previews.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "photo.jpg")
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))

    photo_id = db.get_photos(per_page=100)[0]["id"]

    # Seed a preview file + accounting row, as /photos/<id>/preview would.
    preview_file = preview_dir / f"{photo_id}_1920.jpg"
    Image.new("RGB", (1920, 1440), color=(255, 0, 0)).save(str(preview_file), "JPEG")
    file_bytes = preview_file.stat().st_size
    db.preview_cache_insert(photo_id, 1920, file_bytes)
    assert db.preview_cache_total_bytes() == file_bytes

    # Replace source pixels → new file_hash → invalidation should fire.
    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(img_path, "JPEG")
    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    assert not preview_file.exists(), "preview file should be deleted"
    assert db.preview_cache_get(photo_id, 1920) is None, (
        "preview_cache row must be deleted alongside the file; "
        "leaving it inflates preview_cache_total_bytes and triggers "
        "unnecessary eviction of valid previews."
    )
    assert db.preview_cache_total_bytes() == 0


def test_rescan_keeps_preview_cache_row_when_file_unlink_fails(tmp_path, monkeypatch):
    """If a preview file can't be deleted (e.g. locked on Windows), the
    matching preview_cache row must stay. Otherwise the serve path's
    lazy-adoption shortcut (app.py ~L8131) re-adopts the stale file on
    the next /photos/<id>/preview and hands out pre-change content, and
    quota eviction stops accounting for the leaked bytes.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "photo.jpg")
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))
    photo_id = db.get_photos(per_page=100)[0]["id"]

    preview_file = preview_dir / f"{photo_id}_1920.jpg"
    Image.new("RGB", (1920, 1440), color=(255, 0, 0)).save(str(preview_file), "JPEG")
    file_bytes = preview_file.stat().st_size
    db.preview_cache_insert(photo_id, 1920, file_bytes)

    # Simulate the preview file being un-removable (locked, ACL, etc.).
    real_remove = os.remove
    stuck = str(preview_file)

    def selective_remove(path):
        if os.fspath(path) == stuck:
            raise PermissionError("simulated lock")
        real_remove(path)

    monkeypatch.setattr(os, "remove", selective_remove)

    # Force a content-change rescan so invalidation fires.
    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(img_path, "JPEG")
    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    assert preview_file.exists(), "sanity: stuck preview file should remain"
    assert db.preview_cache_get(photo_id, 1920) is not None, (
        "When preview unlink fails, the cache row must stay so quota "
        "accounting keeps the leaked bytes visible and the serve path "
        "does not lazy-adopt stale content."
    )


def test_rescan_sweeps_untracked_preview_files(tmp_path):
    """Legacy preview files that pre-date preview_cache accounting (no
    row) must still be cleaned up when a photo's content changes.
    Otherwise app.py's lazy-adoption path (~L8131) re-adopts them on
    the next /photos/<id>/preview request and hands out pre-change
    bytes.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "photo.jpg")
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))
    photo_id = db.get_photos(per_page=100)[0]["id"]

    # Seed an *untracked* preview (file exists, no preview_cache row).
    untracked = preview_dir / f"{photo_id}_800.jpg"
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(str(untracked), "JPEG")
    assert db.preview_cache_get(photo_id, 800) is None

    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(img_path, "JPEG")
    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    assert not untracked.exists(), (
        "Untracked preview files for invalidated photos must be swept "
        "or serve's lazy-adoption path re-adopts stale pre-change bytes."
    )


def test_rescan_preview_sweep_is_batched_not_per_photo(tmp_path, monkeypatch):
    """The untracked-preview sweep must run at most once per scan,
    not once per invalidated photo. Per-photo os.listdir on the
    preview dir turns large rescans into O(N × M) work.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    paths = []
    for i in range(3):
        p = os.path.join(root, f"photo_{i}.jpg")
        Image.new("RGB", (800, 600), color=(255, i * 40, 0)).save(p, "JPEG")
        paths.append(p)

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))

    # Replace all three files' contents so each invalidation fires.
    time.sleep(0.05)
    for i, p in enumerate(paths):
        Image.new("RGB", (800, 600), color=(0, 0, 255 - i * 30)).save(p, "JPEG")

    preview_listings = []
    real_listdir = os.listdir

    def counting_listdir(path):
        if str(path).rstrip(os.sep).endswith("previews"):
            preview_listings.append(str(path))
        return real_listdir(path)

    monkeypatch.setattr(os, "listdir", counting_listdir)

    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    assert len(preview_listings) <= 1, (
        f"previews/ must be enumerated at most once per scan regardless "
        f"of how many photos were invalidated; got {len(preview_listings)} "
        f"listings across 3 invalidations."
    )


def test_audit_import_untracked_invalidates_stale_thumbnail(tmp_path):
    """audit.import_untracked calls scan() without an explicit vireo_dir.
    Invalidation must still fire so stale thumbnails don't survive a
    re-import of a folder that contains content-changed files. Exercises
    the db._db_path fallback inside _invalidate_derived_caches.
    """
    from audit import import_untracked
    from db import Database
    from scanner import scan
    from thumbnails import generate_thumbnail

    root = tmp_path / "photos"
    root.mkdir()
    img = root / "photo.jpg"
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(str(img), "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    (vireo_dir / "thumbnails").mkdir()

    db = Database(str(vireo_dir / "vireo.db"))
    scan(str(root), db, vireo_dir=str(vireo_dir))

    photo_id = db.get_photos(per_page=100)[0]["id"]
    thumb_path = vireo_dir / "thumbnails" / f"{photo_id}.jpg"
    generate_thumbnail(photo_id, str(img), str(vireo_dir / "thumbnails"))
    assert thumb_path.exists()

    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(str(img), "JPEG")

    # audit.import_untracked does scan(d, db, incremental=True) without vireo_dir.
    import_untracked(db, [str(img)])

    assert not thumb_path.exists(), (
        "Invalidation must fire even when the caller omits vireo_dir; "
        "otherwise audit-driven rescans bake stale thumbnails into the cache."
    )


def test_preview_sweep_chunks_large_photo_id_sets(tmp_path):
    """_sweep_untracked_previews_for_photos must chunk its IN (...) query.

    Older SQLite builds cap bound parameters at 999
    (SQLITE_MAX_VARIABLE_NUMBER). A rescan that invalidates thousands
    of photos would otherwise raise ``too many SQL variables`` during
    post-processing and fail the whole scan.
    """
    import scanner
    from db import Database

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    (vireo_dir / "previews").mkdir()

    db = Database(str(vireo_dir / "test.db"))

    real_conn = db.conn
    max_params_seen = 0

    class TrackingConn:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, params=()):
            nonlocal max_params_seen
            if isinstance(params, (list, tuple)):
                max_params_seen = max(max_params_seen, len(params))
            return self._inner.execute(sql, params)

        def executemany(self, sql, seq):
            return self._inner.executemany(sql, seq)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    db.conn = TrackingConn(real_conn)

    scanner._sweep_untracked_previews_for_photos(
        db, str(vireo_dir), list(range(1, 2001)),
    )

    assert max_params_seen <= 999, (
        f"Preview sweep sent {max_params_seen} bound parameters in one "
        f"query; would crash on SQLite builds with "
        f"SQLITE_MAX_VARIABLE_NUMBER=999."
    )


def test_rescan_regenerates_working_copy_when_file_content_changes(tmp_path):
    """When a large JPEG's content changes, re-scan must invalidate the stale
    working copy so the subsequent extraction reflects current pixels."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "big.jpg")
    # Larger than the default working_copy_max_size (4096) so a working copy is extracted.
    Image.new("RGB", (5000, 3000), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))

    photos = db.get_photos(per_page=100)
    assert len(photos) == 1
    photo_id = photos[0]["id"]
    assert photos[0]["working_copy_path"] is not None
    wc_path = vireo_dir / photos[0]["working_copy_path"]
    assert wc_path.exists()

    # Record what the working copy looks like now (top-left pixel = red).
    with Image.open(wc_path) as img:
        assert img.convert("RGB").getpixel((0, 0))[0] > 200  # red channel dominant

    # Replace file content with a very different image.
    time.sleep(0.05)
    Image.new("RGB", (5000, 3000), color=(0, 0, 255)).save(img_path, "JPEG")

    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    # Working copy should point to a regenerated file whose pixels match the new source.
    photos = db.get_photos(per_page=100)
    wc_rel = photos[0]["working_copy_path"]
    assert wc_rel is not None, "Scanner must re-set working_copy_path after invalidation"
    wc_path = vireo_dir / wc_rel
    assert wc_path.exists()
    with Image.open(wc_path) as img:
        pixel = img.convert("RGB").getpixel((0, 0))
    assert pixel[2] > 200 and pixel[0] < 100, (
        f"Working copy pixel {pixel} does not reflect updated (blue) source; "
        "stale extraction from pre-change content was served."
    )
