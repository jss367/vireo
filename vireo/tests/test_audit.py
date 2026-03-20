# vireo/tests/test_audit.py
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))

from PIL import Image


def test_check_drift_detects_xmp_change(tmp_path):
    """check_drift detects when XMP was modified after scan."""
    from db import Database
    from scanner import scan
    from audit import check_drift
    from xmp_writer import write_xmp_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))
    write_xmp_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Sparrow'},
        hierarchical_keywords=set(),
    )

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Modify XMP after scan
    time.sleep(0.05)
    write_xmp_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Cardinal'},
        hierarchical_keywords=set(),
    )

    drifts = check_drift(db)
    assert len(drifts) >= 1
    assert drifts[0]['filename'] == 'bird.jpg'


def test_check_orphans_detects_deleted_file(tmp_path):
    """check_orphans finds DB entries with no file on disk."""
    from db import Database
    from scanner import scan
    from audit import check_orphans

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
    from db import Database
    from scanner import scan
    from audit import check_untracked

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
    from db import Database
    from audit import remove_orphans

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/gone', name='gone')
    pid = db.add_photo(folder_id=fid, filename='missing.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)

    remove_orphans(db, [pid])

    photo = db.get_photo(pid)
    assert photo is None
