# vireo/tests/test_scanner.py
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image


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

    assert len(progress) == 3
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

    # Classify the JPEG — add a prediction
    db.add_prediction(
        photo_id=jpeg_id,
        species="Robin",
        confidence=0.85,
        model="bioclip",
    )

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
        "SELECT species, confidence FROM predictions WHERE photo_id = ?",
        (raw_id,),
    ).fetchall()
    assert len(preds) == 1
    assert preds[0]["species"] == "Robin"


def test_pairing_merges_duplicate_predictions_keeps_higher_confidence(tmp_path):
    """When both raw and JPEG have predictions for the same model, keep higher confidence."""
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
    db.add_prediction(photo_id=jpeg_id, species="Robin", confidence=0.95, model="bioclip")
    db.add_prediction(photo_id=raw_id, species="Robin", confidence=0.70, model="bioclip")

    # Run pairing — should NOT raise IntegrityError
    _pair_raw_jpeg_companions(db)

    photos = db.conn.execute("SELECT id, filename, companion_path FROM photos").fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"

    preds = db.conn.execute(
        "SELECT species, confidence FROM predictions WHERE photo_id = ?",
        (photos[0]["id"],),
    ).fetchall()
    # Should keep the higher-confidence prediction
    assert len(preds) == 1
    assert preds[0]["confidence"] == 0.95


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
