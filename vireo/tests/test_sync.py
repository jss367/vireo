# vireo/tests/test_sync.py
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))

from PIL import Image


def _setup_photo_with_xmp(tmp_path, db, keywords=None):
    """Create a photo file, XMP sidecar, and DB entry. Returns (photo_id, xmp_path)."""
    from xmp_writer import write_xmp_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root, exist_ok=True)

    fid = db.add_folder(root, name='photos')
    img_path = os.path.join(root, 'bird.jpg')
    Image.new('RGB', (100, 100)).save(img_path)

    xmp_path = os.path.join(root, 'bird.xmp')
    write_xmp_sidecar(xmp_path, flat_keywords=keywords or set(), hierarchical_keywords=set())

    pid = db.add_photo(folder_id=fid, filename='bird.jpg', extension='.jpg',
                       file_size=100, file_mtime=os.path.getmtime(img_path),
                       xmp_mtime=os.path.getmtime(xmp_path))
    return pid, xmp_path


def test_sync_to_xmp_writes_keyword_add(tmp_path):
    """sync_to_xmp writes keyword_add changes to XMP sidecars."""
    from db import Database
    from sync import sync_to_xmp
    from compare import read_xmp_keywords

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)

    # Queue a keyword_add change
    db.queue_change(pid, 'keyword_add', 'Northern cardinal')

    result = sync_to_xmp(db)
    assert result['synced'] == 1
    assert result['failed'] == 0

    # Verify XMP was written
    keywords = read_xmp_keywords(xmp_path)
    assert 'Northern cardinal' in keywords

    # Pending changes should be cleared
    assert len(db.get_pending_changes()) == 0


def test_sync_to_xmp_writes_rating(tmp_path):
    """sync_to_xmp writes rating changes to XMP sidecars."""
    from db import Database
    from sync import sync_to_xmp
    from xml.etree import ElementTree as ET

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)

    db.queue_change(pid, 'rating', '4')

    result = sync_to_xmp(db)
    assert result['synced'] == 1

    # Verify xmp:Rating was written
    tree = ET.parse(xmp_path)
    root = tree.getroot()
    ns_xmp = "http://ns.adobe.com/xap/1.0/"
    desc = root.find('.//{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description')
    rating = desc.get(f'{{{ns_xmp}}}Rating')
    assert rating == '4'


def test_sync_to_xmp_handles_missing_file(tmp_path):
    """sync_to_xmp tracks failures when XMP file path doesn't exist."""
    from db import Database
    from sync import sync_to_xmp

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/nonexistent', name='gone')
    pid = db.add_photo(folder_id=fid, filename='missing.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.queue_change(pid, 'keyword_add', 'Test')

    result = sync_to_xmp(db)
    assert result['failed'] == 1
    assert len(result['failures']) == 1

    # Pending changes should still be there for retry
    assert len(db.get_pending_changes()) == 1


def test_sync_from_xmp_updates_db(tmp_path):
    """sync_from_xmp reconciles DB keywords to the current XMP keywords."""
    from db import Database
    from sync import sync_from_xmp
    from xmp_writer import write_xmp_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={'Sparrow'})

    # Import initial keyword
    kid = db.add_keyword('Sparrow')
    db.tag_photo(pid, kid)

    # Replace the XMP sidecar with a different keyword set.
    os.remove(xmp_path)
    write_xmp_sidecar(xmp_path, flat_keywords={'Cardinal'}, hierarchical_keywords=set())

    sync_from_xmp(db, [pid])

    keywords = db.get_photo_keywords(pid)
    kw_names = {k['name'] for k in keywords}
    assert 'Cardinal' in kw_names
    assert 'Sparrow' not in kw_names


def test_sync_from_xmp_preserves_keyword_when_only_case_differs(tmp_path):
    """Case-only differences between DB and XMP keyword names should not drop the tag."""
    from db import Database
    from sync import sync_from_xmp
    from xmp_writer import write_xmp_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={'sparrow'})

    kid = db.add_keyword('Sparrow')
    db.tag_photo(pid, kid)

    os.remove(xmp_path)
    write_xmp_sidecar(xmp_path, flat_keywords={'sparrow'}, hierarchical_keywords=set())

    sync_from_xmp(db, [pid])

    keywords = db.get_photo_keywords(pid)
    assert {k['name'] for k in keywords} == {'Sparrow'}


def test_sync_to_xmp_reports_unsupported_flag_changes(tmp_path):
    """Legacy flag pending changes remain queued and are reported as unsupported."""
    from db import Database
    from sync import sync_to_xmp
    from xml.etree import ElementTree as ET

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)

    db.queue_change(pid, 'flag', 'flagged')

    before = ET.tostring(ET.parse(xmp_path).getroot(), encoding='unicode')
    result = sync_to_xmp(db)
    after = ET.tostring(ET.parse(xmp_path).getroot(), encoding='unicode')

    assert result['synced'] == 0
    assert result['failed'] == 1
    assert result['failures'][0]['error'] == 'unsupported change type: flag'
    assert before == after
    assert len(db.get_pending_changes()) == 1
