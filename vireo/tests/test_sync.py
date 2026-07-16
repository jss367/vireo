# vireo/tests/test_sync.py
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image


def _setup_photo_with_xmp(tmp_path, db, keywords=None):
    """Create a photo file, XMP sidecar, and DB entry. Returns (photo_id, xmp_path)."""
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root, exist_ok=True)

    fid = db.add_folder(root, name='photos')
    img_path = os.path.join(root, 'bird.jpg')
    Image.new('RGB', (100, 100)).save(img_path)

    xmp_path = os.path.join(root, 'bird.xmp')
    write_sidecar(xmp_path, flat_keywords=keywords or set(), hierarchical_keywords=set())

    pid = db.add_photo(folder_id=fid, filename='bird.jpg', extension='.jpg',
                       file_size=100, file_mtime=os.path.getmtime(img_path),
                       xmp_mtime=os.path.getmtime(xmp_path))
    return pid, xmp_path


def test_sync_to_xmp_writes_keyword_add(tmp_path):
    """sync_to_xmp writes keyword_add changes to XMP sidecars."""
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords

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
    keywords = read_keywords(xmp_path)
    assert 'Northern cardinal' in keywords

    # Pending changes should be cleared
    assert len(db.get_pending_changes()) == 0


def test_sync_to_xmp_keyword_add_canonicalizes_existing_variant(tmp_path):
    """A keyword_add against a sidecar that already contains a normalized-
    equivalent variant should end up with one clean <rdf:li>, not two.

    Regression: write_sidecar() dedupes by exact-string set difference, so
    queuing ``keyword_add: apapane`` for a photo whose sidecar carries a
    legacy ``‘apapane`` used to append a second entry that sync_from_xmp
    would then never clean up. sync_to_xmp now strips add-equivalent
    variants first so the sidecar canonicalizes to the clean spelling.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(
        tmp_path, db, keywords={'‘apapane'},
    )

    db.queue_change(pid, 'keyword_add', 'apapane')

    result = sync_to_xmp(db)
    assert result['synced'] == 1
    assert result['failed'] == 0

    keywords = read_keywords(xmp_path)
    assert keywords == {'apapane'}
    assert len(db.get_pending_changes()) == 0


def test_sync_to_xmp_keyword_add_preserves_hierarchies_with_matching_segment(
    tmp_path,
):
    """A flat keyword_add must not delete an unrelated hierarchy whose leaf
    happens to share the added keyword.

    Regression: sync canonicalizes sidecar variants of an added keyword by
    stripping add-equivalents before writing. Using the default
    remove_keywords semantics (which drop any hierarchy whose segment
    matches) would delete `Animals|Birds|Hawk` when the user adds a flat
    `Hawk`, wiping the entire hierarchical tree from the sidecar.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_hierarchical_keywords, read_keywords, write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    # Seed the sidecar with a hierarchy whose leaf matches what we're about
    # to add flat. write_sidecar accepts both bags in one call.
    write_sidecar(
        xmp_path,
        flat_keywords=set(),
        hierarchical_keywords={'Animals|Birds|Hawk'},
    )

    db.queue_change(pid, 'keyword_add', 'Hawk')

    result = sync_to_xmp(db)
    assert result['synced'] == 1
    assert result['failed'] == 0

    assert 'Hawk' in read_keywords(xmp_path)
    assert 'Animals|Birds|Hawk' in read_hierarchical_keywords(xmp_path)


def test_sync_to_xmp_writes_rating(tmp_path):
    """sync_to_xmp writes rating changes to XMP sidecars."""
    from xml.etree import ElementTree as ET

    from db import Database
    from sync import sync_to_xmp

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


def test_sync_to_xmp_writes_edit_recipe(tmp_path):
    """sync_to_xmp writes Vireo edit recipes to XMP sidecars."""
    from db import Database
    from sync import sync_to_xmp

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)

    db.queue_change(
        pid,
        "edit_recipe",
        '{"crop":{"h":0.8,"w":0.7,"x":0.1,"y":0.1},"version":1}',
    )

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert result["failed"] == 0
    content = open(xmp_path).read()
    assert "vireo:editRecipe" in content
    assert "&quot;crop&quot;" in content
    assert len(db.get_pending_changes()) == 0


def test_sync_to_xmp_clears_edit_recipe_marker(tmp_path):
    """An empty edit_recipe change removes Vireo's XMP recipe marker."""
    from db import Database
    from sync import sync_to_xmp
    from xmp import write_edit_recipe

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    write_edit_recipe(xmp_path, '{"rotation":90,"version":1}')

    db.queue_change(pid, "edit_recipe", "")
    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert "vireo:editRecipe" not in open(xmp_path).read()


def test_sync_to_xmp_limits_sync_to_selected_change_ids(tmp_path):
    """sync_to_xmp can write only the checked pending changes."""
    from xml.etree import ElementTree as ET

    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)

    db.queue_change(pid, "keyword_add", "Northern cardinal")
    db.queue_change(pid, "rating", "4")
    pending = db.get_pending_changes()
    ids_by_type = {c["change_type"]: c["id"] for c in pending}

    result = sync_to_xmp(db, change_ids=[ids_by_type["keyword_add"]])

    assert result["synced"] == 1
    assert result["failed"] == 0
    assert "Northern cardinal" in read_keywords(xmp_path)

    desc = ET.parse(xmp_path).getroot().find(
        ".//{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description"
    )
    assert desc.get("{http://ns.adobe.com/xap/1.0/}Rating") is None

    remaining = db.get_pending_changes()
    assert [(c["change_type"], c["value"]) for c in remaining] == [("rating", "4")]


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
    from xmp import write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={'Sparrow'})

    # Import initial keyword
    kid = db.add_keyword('Sparrow')
    db.tag_photo(pid, kid)

    # Replace the XMP sidecar with a different keyword set.
    os.remove(xmp_path)
    write_sidecar(xmp_path, flat_keywords={'Cardinal'}, hierarchical_keywords=set())

    sync_from_xmp(db, [pid])

    keywords = db.get_photo_keywords(pid)
    kw_names = {k['name'] for k in keywords}
    assert 'Cardinal' in kw_names
    assert 'Sparrow' not in kw_names


def test_sync_from_xmp_preserves_keyword_when_only_case_differs(tmp_path):
    """Case-only differences between DB and XMP keyword names should not drop the tag."""
    from db import Database
    from sync import sync_from_xmp
    from xmp import write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={'sparrow'})

    kid = db.add_keyword('Sparrow')
    db.tag_photo(pid, kid)

    os.remove(xmp_path)
    write_sidecar(xmp_path, flat_keywords={'sparrow'}, hierarchical_keywords=set())

    sync_from_xmp(db, [pid])

    keywords = db.get_photo_keywords(pid)
    assert {k['name'] for k in keywords} == {'Sparrow'}


def test_sync_from_xmp_preserves_tag_when_only_edge_quote_differs(tmp_path):
    """A stray edge-quote variant in XMP should match the clean DB spelling.

    Regression: prior to normalizing both sides of the diff, an XMP file
    containing '‘apapane' compared against a DB row stored as 'apapane'
    would land in "add ‘apapane" (a no-op via add_keyword's normalize
    fallback) followed by "remove apapane" (the DB name isn't in the raw
    XMP set), leaving the photo untagged.
    """
    from db import Database
    from sync import sync_from_xmp
    from xmp import write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={'apapane'})

    kid = db.add_keyword('apapane')
    db.tag_photo(pid, kid)

    os.remove(xmp_path)
    write_sidecar(
        xmp_path, flat_keywords={'‘apapane'}, hierarchical_keywords=set(),
    )

    sync_from_xmp(db, [pid])

    keywords = db.get_photo_keywords(pid)
    assert {k['name'] for k in keywords} == {'apapane'}


def test_sync_from_xmp_skips_xmp_keywords_that_normalize_to_empty(tmp_path):
    """A sidecar keyword that normalizes to empty (e.g. a lone quote) must
    be ignored, not aborted. add_keyword now raises ValueError on
    empty-after-normalization input, so without the pre-filter one
    malformed edge-quote entry would kill the entire sidecar reconcile
    and leave every other keyword unsynced.
    """
    from db import Database
    from sync import sync_from_xmp
    from xmp import write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={'Sparrow'})

    kid = db.add_keyword('Sparrow')
    db.tag_photo(pid, kid)

    os.remove(xmp_path)
    # A lone smart quote normalizes to empty; a real second keyword sits
    # alongside it. The malformed entry must be silently skipped and the
    # real one still applied.
    write_sidecar(
        xmp_path,
        flat_keywords={'Sparrow', 'Cardinal', '“”'},
        hierarchical_keywords=set(),
    )

    sync_from_xmp(db, [pid])

    keywords = db.get_photo_keywords(pid)
    assert {k['name'] for k in keywords} == {'Sparrow', 'Cardinal'}


def test_sync_to_xmp_reports_unsupported_flag_changes_when_disabled(tmp_path, monkeypatch):
    """Flag pending changes remain queued when XMP flag sync is disabled."""
    from xml.etree import ElementTree as ET

    import config as cfg
    from db import Database
    from sync import sync_to_xmp

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    config["sync_flags_to_xmp"] = False
    cfg.save(config)

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


def test_sync_to_xmp_writes_flag_when_enabled(tmp_path, monkeypatch):
    """sync_to_xmp writes xmpDM:pick when flag sync is enabled."""
    from xml.etree import ElementTree as ET

    import config as cfg
    from db import Database
    from sync import sync_to_xmp

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    config["sync_flags_to_xmp"] = True
    cfg.save(config)

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)

    db.queue_change(pid, 'flag', 'rejected')

    result = sync_to_xmp(db)

    assert result['synced'] == 1
    assert result['failed'] == 0
    assert len(db.get_pending_changes()) == 0

    tree = ET.parse(xmp_path)
    desc = tree.getroot().find(
        './/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description'
    )
    pick = desc.get('{http://ns.adobe.com/xmp/1.0/DynamicMedia/}pick')
    assert pick == '-1'


def test_sync_to_xmp_treats_legacy_null_flag_as_none(tmp_path, monkeypatch):
    """Legacy queued NULL flag values should clear XMP pick state."""
    from xml.etree import ElementTree as ET

    import config as cfg
    from db import Database
    from sync import sync_to_xmp

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    config["sync_flags_to_xmp"] = True
    cfg.save(config)

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)

    db.queue_change(pid, 'flag', None)

    result = sync_to_xmp(db)

    assert result['synced'] == 1
    assert result['failed'] == 0
    assert len(db.get_pending_changes()) == 0

    tree = ET.parse(xmp_path)
    desc = tree.getroot().find(
        './/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description'
    )
    pick = desc.get('{http://ns.adobe.com/xmp/1.0/DynamicMedia/}pick')
    assert pick == '0'


def test_sync_to_xmp_writes_effective_location(tmp_path, monkeypatch):
    """location changes write effective coordinates into the sidecar."""
    from xml.etree import ElementTree as ET

    import config as cfg
    from db import Database
    from sync import sync_to_xmp

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = True
    cfg.save(config)

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)

    kid = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) "
        "VALUES (?, 'location', ?, ?)",
        ("Paris Airbnb", 48.8566, 2.3522),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, kid),
    )
    db.conn.commit()
    db.queue_change(pid, "location", "effective")

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert result["failed"] == 0
    assert len(db.get_pending_changes()) == 0

    desc = ET.parse(xmp_path).getroot().find(
        './/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description'
    )
    assert desc.get('{http://ns.adobe.com/exif/1.0/}GPSLatitude') == '48,51.396000N'
    assert desc.get('{http://ns.adobe.com/exif/1.0/}GPSLongitude') == '2,21.132000E'
    assert desc.get('{https://vireo.app/ns/1.0/}gpsSource') == 'keyword'


def test_sync_to_xmp_removes_stale_vireo_location_when_effective_location_missing(tmp_path, monkeypatch):
    """Clearing a Vireo-assigned location removes only Vireo-authored GPS."""
    import config as cfg
    from db import Database
    from sync import sync_to_xmp
    from xmp import write_gps_location

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = True
    cfg.save(config)

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    write_gps_location(xmp_path, 48.8566, 2.3522, source="keyword")
    db.queue_change(pid, "location", "effective")

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    with open(xmp_path) as f:
        content = f.read()
    assert "GPSLatitude" not in content
    assert "GPSLongitude" not in content
    assert "vireo:gpsSource" not in content


def test_sync_to_xmp_clears_location_change_without_writing_when_disabled(tmp_path, monkeypatch):
    """Turning the setting off before sync prevents queued GPS writes."""
    import config as cfg
    from db import Database
    from sync import sync_to_xmp

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = False
    cfg.save(config)

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)

    kid = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) "
        "VALUES (?, 'location', ?, ?)",
        ("Paris Airbnb", 48.8566, 2.3522),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, kid),
    )
    db.conn.commit()
    db.queue_change(pid, "location", "effective")

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert result["failed"] == 0
    assert len(db.get_pending_changes()) == 0
    with open(xmp_path) as f:
        content = f.read()
    assert "GPSLatitude" not in content
    assert "GPSLongitude" not in content


def test_sync_to_xmp_disabled_location_change_removes_stale_vireo_gps(tmp_path, monkeypatch):
    """Disabling assigned-location writes still cleans up Vireo-authored GPS."""
    import config as cfg
    from db import Database
    from sync import sync_to_xmp
    from xmp import write_gps_location

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = False
    cfg.save(config)

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    write_gps_location(xmp_path, 48.8566, 2.3522, source="keyword")
    db.queue_change(pid, "location", "effective")

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert result["failed"] == 0
    assert len(db.get_pending_changes()) == 0
    with open(xmp_path) as f:
        content = f.read()
    assert "GPSLatitude" not in content
    assert "GPSLongitude" not in content
    assert "vireo:gpsSource" not in content


def test_sync_to_xmp_location_cleanup_does_not_write_exif_fallback(tmp_path, monkeypatch):
    """Assigned-location sync cleanup should not preserve Vireo GPS via EXIF fallback."""
    import config as cfg
    from db import Database
    from sync import sync_to_xmp
    from xmp import write_gps_location

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = True
    cfg.save(config)

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    db.conn.execute(
        "UPDATE photos SET latitude=?, longitude=? WHERE id=?",
        (40.7829, -73.9654, pid),
    )
    db.conn.commit()
    write_gps_location(xmp_path, 48.8566, 2.3522, source="keyword")
    db.queue_change(pid, "location", "effective")

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    with open(xmp_path) as f:
        content = f.read()
    assert "GPSLatitude" not in content
    assert "GPSLongitude" not in content
    assert "vireo:gpsSource" not in content


def test_sync_to_xmp_add_survives_normalized_remove_for_same_photo(tmp_path):
    """A rename queues both remove `‘apapane` and add `apapane` on the same
    photo. remove_keywords compares by normalized match key, so the newly
    written clean `<rdf:li>` and the pre-existing quoted variant BOTH match
    the remove key. sync_to_xmp must apply the remove BEFORE the add so the
    resulting sidecar carries the clean spelling instead of ending up empty.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={"‘apapane"})

    db.queue_change(pid, "keyword_remove", "‘apapane")
    db.queue_change(pid, "keyword_add", "apapane")

    result = sync_to_xmp(db)
    assert result["synced"] == 1
    assert result["failed"] == 0

    kw = read_keywords(xmp_path)
    assert "apapane" in kw
    assert "‘apapane" not in kw
    assert len(db.get_pending_changes()) == 0


def test_sync_to_xmp_selected_add_pulls_in_paired_legacy_remove(tmp_path):
    """When the sync panel filters change_ids to only the keyword_add half
    of a rename (add `apapane` + legacy remove `‘apapane` for the same
    photo), sync_to_xmp must pull the paired remove into the same batch.

    Regression: both remove_keywords() (for the paired remove) and the
    add-canonicalization pass compare by normalized match key. Syncing
    only the add still runs add-canonicalization -- stripping the legacy
    `<rdf:li>` before writing the clean spelling. If the paired remove is
    left pending and later synced on its own, normalized removal matches
    the clean `<rdf:li>` too and the keyword disappears entirely even
    though both syncs reported success.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={"‘apapane"})

    db.queue_change(pid, "keyword_remove", "‘apapane")
    db.queue_change(pid, "keyword_add", "apapane")
    pending = db.get_pending_changes()
    ids_by_type = {c["change_type"]: c["id"] for c in pending}

    result = sync_to_xmp(db, change_ids=[ids_by_type["keyword_add"]])
    assert result["failed"] == 0

    kw = read_keywords(xmp_path)
    assert kw == {"apapane"}

    remaining = db.get_pending_changes()
    assert remaining == []


def test_sync_to_xmp_normalized_rename_preserves_unrelated_hierarchy(tmp_path):
    """A normalization-only rename queued as ``keyword_remove('‘Birds')`` +
    ``keyword_add('Birds')`` on the same photo must not strip an unrelated
    hierarchy like ``Animals|Birds|Hawk``.

    Regression: ``remove_keywords()`` compares each pipe-delimited hierarchy
    segment by normalized match key, so a naive hierarchical remove of the
    legacy variant matches the clean ``Birds`` segment inside the unrelated
    hierarchy and drops the whole ``Animals|Birds|Hawk`` entry from
    ``lr:hierarchicalSubject``. Applying flat-only removal for the paired
    remove keeps the hierarchy intact while still canonicalizing the flat
    legacy ``<rdf:li>‘Birds</rdf:li>`` to ``Birds`` in ``dc:subject``.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_hierarchical_keywords, read_keywords, write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    # Sidecar carries the legacy flat variant AND an unrelated hierarchy
    # whose middle segment happens to normalize to the same key as the
    # remove target.
    write_sidecar(
        xmp_path,
        flat_keywords={'‘Birds'},
        hierarchical_keywords={'Animals|Birds|Hawk'},
    )

    db.queue_change(pid, 'keyword_remove', '‘Birds')
    db.queue_change(pid, 'keyword_add', 'Birds')

    result = sync_to_xmp(db)
    assert result['synced'] == 1
    assert result['failed'] == 0

    # Flat legacy variant is gone, clean spelling is written.
    flat = read_keywords(xmp_path)
    assert 'Birds' in flat
    assert '‘Birds' not in flat
    # Unrelated hierarchy survives -- was NOT stripped by the paired
    # remove even though `Birds` segment normalizes to the remove key.
    assert 'Animals|Birds|Hawk' in read_hierarchical_keywords(xmp_path)


def test_sync_from_xmp_preserves_cross_slot_homonyms(tmp_path):
    """Cross-slot same-text keywords must both survive a sidecar reconcile.

    A photo can legitimately carry two distinct DB rows sharing the same
    normalized text in different slots (e.g. a taxonomy ``Robin`` and an
    individual ``Robin`` — SQLite's UNIQUE(name, parent_id) treats NULL
    parents as distinct, and the dedup boundary elsewhere in the codebase
    is (name, parent_id, type)). A single flat ``Robin`` in the sidecar
    cannot disambiguate between the homonyms, so reconciliation must keep
    both tags rather than untag one arbitrarily.
    """
    from db import Database
    from sync import sync_from_xmp
    from xmp import write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={'Robin'})

    # Two top-level rows with the same normalized text but different
    # types. Insert directly so both rows survive add_keyword's peer
    # promotion.
    db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, ?)",
        ('Robin', 'taxonomy'),
    )
    db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, ?)",
        ('Robin', 'individual'),
    )
    db.conn.commit()
    rows = db.conn.execute(
        "SELECT id, type FROM keywords WHERE name = 'Robin' "
        "AND parent_id IS NULL"
    ).fetchall()
    kid_by_type = {row['type']: row['id'] for row in rows}
    assert set(kid_by_type) == {'taxonomy', 'individual'}

    db.tag_photo(pid, kid_by_type['taxonomy'])
    db.tag_photo(pid, kid_by_type['individual'])

    os.remove(xmp_path)
    write_sidecar(xmp_path, flat_keywords={'Robin'}, hierarchical_keywords=set())

    sync_from_xmp(db, [pid])

    keywords = db.get_photo_keywords(pid)
    # Both distinct-slot homonyms must survive; sidecar reconciliation
    # cannot pick between them.
    surviving_ids = {kw['id'] for kw in keywords}
    assert kid_by_type['taxonomy'] in surviving_ids
    assert kid_by_type['individual'] in surviving_ids
