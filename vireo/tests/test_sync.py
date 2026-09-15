# vireo/tests/test_sync.py
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
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


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("history", ["none", "undo", "redo"])
def test_rating_return_to_previous_value_syncs_latest(tmp_path, db, batch, history):
    from services.photo_review import PhotoReviewService
    from sync import sync_to_xmp
    from xmp import read_sync_preview_metadata

    db.set_active_workspace(db.ensure_default_workspace())
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    service = PhotoReviewService(db)
    for rating in (1, 2, 1):
        if batch:
            service.set_ratings([pid], rating)
        else:
            service.set_rating(pid, rating)
    if history != "none":
        db.undo_last_edit()
    if history == "redo":
        db.redo_last_undo()

    expected_rating = 2 if history == "undo" else 1
    assert db.get_photo(pid)["rating"] == expected_rating
    assert sync_to_xmp(db) == {"synced": 1, "failed": 0, "failures": [],
                               "ok": True, "errors": []}
    assert read_sync_preview_metadata(xmp_path)["rating"] == str(expected_rating)
    assert not db.get_pending_changes()


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


def test_sync_to_xmp_keyword_remove_flat_leaves_matching_hierarchy(tmp_path):
    """``keyword_remove_flat`` strips the flat ``dc:subject`` entry only.

    ``repair_duplicate_photo_species`` queues this variant when a
    surviving hierarchical leaf's parent chain still carries the
    detached root's spelling (e.g. root ``Verdin`` detached while
    ``Verdin|Desert Verdin`` is preserved). A regular
    ``keyword_remove`` would run ``remove_keywords`` in hierarchical
    mode and strip that preserved hierarchy — dropping the very
    ``lr:hierarchicalSubject`` line the repair kept — whereas
    ``keyword_remove_flat`` cleans only the stale ``dc:subject`` line.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_hierarchical_keywords, read_keywords, write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    write_sidecar(
        xmp_path,
        flat_keywords={'Verdin', 'Desert Verdin'},
        hierarchical_keywords={'Verdin|Desert Verdin'},
    )

    db.queue_change(pid, 'keyword_remove_flat', 'Verdin')

    result = sync_to_xmp(db)
    assert result['synced'] == 1
    assert result['failed'] == 0

    # Flat ``Verdin`` is gone; the hierarchy stays intact.
    flat = read_keywords(xmp_path)
    assert 'Verdin' not in flat
    assert 'Desert Verdin' in flat
    assert 'Verdin|Desert Verdin' in read_hierarchical_keywords(xmp_path)
    assert len(db.get_pending_changes()) == 0


def test_sync_to_xmp_clears_sibling_workspace_flat_removals(tmp_path):
    """One global sidecar write retires equivalent workspace queue rows.

    Retirement is visible in every workspace owning a shared folder, but the
    XMP file itself is shared. After one workspace applies the removal, a
    sibling must not retain a stale row that can delete a later manual re-add.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords, write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws1 = db.ensure_default_workspace()
    ws2 = db.create_workspace("sibling")
    db.set_active_workspace(ws1)
    pid, xmp_path = _setup_photo_with_xmp(
        tmp_path, db, keywords={"Wildlife"},
    )
    folder_id = db.get_photo(pid)["folder_id"]
    db.add_workspace_folder(ws1, folder_id)
    db.add_workspace_folder(ws2, folder_id)
    db.queue_change(
        pid, "keyword_remove_flat", "Wildlife", workspace_id=ws1,
    )
    db.queue_change(
        pid, "keyword_remove_flat", "Wildlife", workspace_id=ws2,
    )

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert "Wildlife" not in read_keywords(xmp_path)
    assert db.conn.execute(
        """SELECT 1 FROM pending_changes
           WHERE photo_id = ? AND change_type = 'keyword_remove_flat'""",
        (pid,),
    ).fetchone() is None

    # A later manual re-add must survive visiting and syncing the sibling.
    write_sidecar(
        xmp_path, flat_keywords={"Wildlife"}, hierarchical_keywords=set(),
    )
    db.set_active_workspace(ws2)
    assert sync_to_xmp(db)["synced"] == 0
    assert "Wildlife" in read_keywords(xmp_path)


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


def test_sync_to_xmp_writes_rating_after_edit_recipe_creates_sidecar(tmp_path):
    """A selected edit write creates XMP before the same-photo rating runs."""
    from xml.etree import ElementTree as ET

    from db import Database
    from sync import sync_to_xmp

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    os.remove(xmp_path)

    db.queue_change(pid, "rating", "5")
    db.queue_change(pid, "edit_recipe", '{"exposure":0.5}')

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert result["failed"] == 0
    desc = ET.parse(xmp_path).getroot().find(
        ".//{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description"
    )
    assert desc.get("{http://ns.adobe.com/xap/1.0/}Rating") == "5"
    assert desc.get("{https://vireo.app/ns/1.0/}editRecipe") == (
        '{"exposure":0.5}'
    )


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


def test_sync_from_xmp_locks_before_reading_sidecar(tmp_path, monkeypatch):
    """Reconciliation must hold the writer lock through its XMP read."""
    import sync
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, _xmp_path = _setup_photo_with_xmp(
        tmp_path, db, keywords={"Wildlife"},
    )

    original_read_keywords = sync.read_keywords
    observed_transactions = []

    def read_keywords_under_lock(path):
        observed_transactions.append(db.conn.in_transaction)
        return original_read_keywords(path)

    monkeypatch.setattr(sync, "read_keywords", read_keywords_under_lock)

    sync.sync_from_xmp(db, [pid])

    assert observed_transactions == [True]
    assert db.conn.in_transaction is False


def test_sync_from_xmp_does_not_restore_keyword_with_pending_removal(tmp_path):
    """A stale sidecar cannot resurrect metadata awaiting a sync removal."""
    from db import Database
    from sync import sync_from_xmp

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, _xmp_path = _setup_photo_with_xmp(
        tmp_path, db, keywords={"Wildlife"},
    )
    db.queue_change(pid, "keyword_remove_flat", "Wildlife")

    sync_from_xmp(db, [pid])

    assert db.get_photo_keywords(pid) == []


def test_sync_from_xmp_flat_removal_preserves_same_name_hierarchy(tmp_path):
    """A flat-only removal must not detach a same-name nested keyword."""
    from db import Database
    from sync import sync_from_xmp
    from xmp import write_sidecar

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    write_sidecar(
        xmp_path,
        flat_keywords={"Wildlife"},
        hierarchical_keywords={"Animals|Wildlife"},
    )
    flat = db.add_keyword("Wildlife")
    parent = db.add_keyword("Animals")
    nested = db.add_keyword("Wildlife", parent_id=parent)
    db.tag_photo(pid, flat)
    db.tag_photo(pid, nested)
    db.queue_change(pid, "keyword_remove_flat", "Wildlife")

    sync_from_xmp(db, [pid])

    keyword_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert flat not in keyword_ids
    assert nested in keyword_ids


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


def test_sync_to_xmp_writes_rating_after_location_creates_sidecar(
    tmp_path, monkeypatch,
):
    """A selected GPS write creates XMP before the same-photo rating runs."""
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
    os.remove(xmp_path)
    location_id = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) "
        "VALUES ('Tallahassee', 'location', 30.4383, -84.2807)"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, location_id),
    )
    db.conn.commit()

    db.queue_change(pid, "rating", "4")
    db.queue_change(pid, "location", "effective")

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert result["failed"] == 0
    desc = ET.parse(xmp_path).getroot().find(
        ".//{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description"
    )
    assert desc.get("{http://ns.adobe.com/xap/1.0/}Rating") == "4"
    assert desc.get("{https://vireo.app/ns/1.0/}gpsSource") == "keyword"


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


def test_sync_result_reports_partial_failure_to_the_job_layer(tmp_path, monkeypatch):
    """A run that fails on most photos must not land in history as a success."""
    import xmp as xmp_module
    from db import Database
    from sync import sync_to_xmp

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    pid, _ = _setup_photo_with_xmp(tmp_path, db)
    db.queue_change(pid, 'keyword_add', 'Test')

    def refuse(tree, xmp_path):
        raise PermissionError(13, "Permission denied")

    # Fail where the NAS actually failed: publishing the tree. Patching a
    # single writer would miss a sync that batches its mutations into one
    # publish.
    monkeypatch.setattr(xmp_module, "_write_tree_atomic", refuse)

    result = sync_to_xmp(db)
    assert result["synced"] == 0
    assert result["failed"] == 1
    assert result["ok"] is False
    assert result["errors"] and "Permission denied" in result["errors"][0]
    # The count belongs in the summary so one NAS-wide cause reads as one line.
    assert "(1 photo)" in result["errors"][0]
    assert len(db.get_pending_changes()) == 1


def test_sync_failure_reasons_are_deduplicated(tmp_path, monkeypatch):
    """Thousands of identical failures collapse into one named cause."""
    from db import Database
    from sync import sync_to_xmp

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    # An uncreated child of tmp_path is guaranteed absent; a fixed
    # /nonexistent path is not, and on hosts where it exists the test would
    # silently exercise a different code path.
    missing_dir = tmp_path / 'gone'
    fid = db.add_folder(str(missing_dir), name='gone')
    for i in range(3):
        pid = db.add_photo(folder_id=fid, filename=f'missing{i}.jpg',
                           extension='.jpg', file_size=100, file_mtime=1.0)
        db.queue_change(pid, 'keyword_add', 'Test')

    result = sync_to_xmp(db)
    assert result["failed"] == 3
    assert result["ok"] is False
    assert len(result["errors"]) == 1
    assert "(3 photos)" in result["errors"][0]


def test_sync_failure_reason_ignores_per_photo_filename(tmp_path, monkeypatch):
    """One NAS-wide EACCES on many sidecars must collapse to one reason.

    ``str(OSError)`` appends the offending filename
    (``[Errno 13] Permission denied: '/path/to/DSC_0001.xmp'``), so counting
    raw error strings would treat every per-photo failure as a distinct
    reason and reduce the summary back to one-line-per-photo. The summary
    must strip the filename so the count captures the whole batch under one
    cause.
    """
    import xmp as xmp_module
    from db import Database
    from sync import sync_to_xmp

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    pids = []
    for i in range(4):
        pid, _ = _setup_photo_with_xmp(tmp_path / f"lot{i}", db)
        db.queue_change(pid, 'keyword_add', 'Test')
        pids.append(pid)

    def refuse_with_filename(tree, xmp_path):
        raise PermissionError(13, "Permission denied", xmp_path)

    monkeypatch.setattr(xmp_module, "_write_tree_atomic", refuse_with_filename)

    result = sync_to_xmp(db)

    assert result["synced"] == 0
    assert result["failed"] == len(pids)
    assert result["ok"] is False
    # Sanity: the underlying per-photo error strings really do differ.
    raw_errors = {f["error"] for f in result["failures"]}
    assert len(raw_errors) == len(pids), raw_errors
    # The summary collapses them to one cause and reports the full count.
    assert len(result["errors"]) == 1, result["errors"]
    reason = result["errors"][0]
    assert "Permission denied" in reason
    assert f"({len(pids)} photos)" in reason
    # And it names the cause without leaking any per-photo path.
    for f in result["failures"]:
        # Each failure keeps its raw path-bearing error for the detail log,
        # but the summary line must not include the specific filename.
        assert f["error"] != reason
        assert ".xmp" not in reason


def test_sync_result_counts_distinct_photos_not_records():
    """Two failure records for the same photo count the photo once.

    A photo with two queued unsupported changes of the same type produces
    two identical failure records; the summary reports 'photos', not
    records, so the two must collapse to one.
    """
    from sync import _sync_result

    failures = [
        {"photo_id": 1, "error": "unsupported change type: flag"},
        {"photo_id": 1, "error": "unsupported change type: flag"},
        {"photo_id": 2, "error": "unsupported change type: flag"},
    ]
    result = _sync_result(synced=0, failures=failures)
    # The raw failures are preserved for the detail log.
    assert result["failed"] == 3
    # But the summary counts distinct photos per reason: photo 1 once, plus
    # photo 2, so the reason applies to two photos, not three.
    assert result["errors"] == ["unsupported change type: flag (2 photos)"]


def test_sync_panel_does_not_report_zero_for_a_resultless_failure():
    """A crashed sync must not render as "Wrote 0, failed on 0".

    When ``sync_to_xmp`` raises before returning -- a failing
    ``get_pending_changes`` or ``clear_pending`` -- the JobRunner completes the
    job with a null result. Defaulting the counters to zero would state a
    count the run never established, and if ``clear_pending`` is what failed,
    sidecars had already been written. The guard is on the counters rather
    than on ``event.status`` so a resultless completed or cancelled event
    cannot reach the counter branches either.
    """
    from pathlib import Path

    panel = (Path(__file__).parents[1] / "templates/_sync_panel.html").read_text(encoding="utf-8")
    handler = panel[panel.index("onComplete: function(event)"):]
    handler = handler[:handler.index("onError:")]
    assert "typeof result.synced === 'number'" in handler
    assert "typeof result.failed === 'number'" in handler
    assert "Sync failed" in handler
    # The JobRunner's failure contract, not just the error list.
    assert "event.failure && event.failure.message" in handler
    # One tooltip assignment, so a stale failure detail cannot survive a
    # later clean run and the two sites cannot drift apart.
    assert handler.count("status.title") == 1


def test_sync_panel_clears_stale_success_when_pending_remains():
    """A "Synced N photos!" success stays green until checkPendingSync clears it.

    ``runVisibleSync`` syncs only the checked change types, so the unchecked
    ones intentionally stay queued. The panel must not leave the green
    success message next to a nonzero pending count -- the two together read
    as a UI bug. Match the state we set on the element (``dataset.syncState``)
    rather than the specific words, so future copy tweaks to the success
    branch cannot desync the check.
    """
    from pathlib import Path

    panel = (Path(__file__).parents[1] / "templates/_sync_panel.html").read_text(encoding="utf-8")

    # The success branches (``Synced!`` / ``Synced N photos!`` /
    # ``Nothing to sync.``) mark the element as success.
    complete_handler = panel[panel.index("onComplete: function(event)"):]
    complete_handler = complete_handler[:complete_handler.index("onError:")]
    assert "status.dataset.syncState = 'success'" in complete_handler

    # ``checkPendingSync`` clears any stale success when pending remains,
    # so the marker approach must be used there too. The old literal-text
    # match (``status.textContent === 'Synced!'``) is gone: it silently
    # missed the ``Synced N photos!`` case runVisibleSync produces.
    check_fn = panel[panel.index("async function checkPendingSync"):]
    check_fn = check_fn[:check_fn.index("async function runSync")]
    assert "status.dataset.syncState === 'success'" in check_fn
    assert "status.textContent === 'Synced!'" not in check_fn


def test_sync_panel_cancelled_summary_includes_failures():
    """A cancelled run that already saw failures must surface them.

    ``api_job_sync`` does not poll cancellation inside ``sync_to_xmp``'s
    per-photo loop, so a cancel requested mid-run still returns a structured
    result containing both writes and failures while the JobRunner emits
    ``status: "cancelled"``. Reporting only the written count would present
    a NAS-wide permission failure as an ordinary cancellation.
    """
    from pathlib import Path

    panel = (Path(__file__).parents[1] / "templates/_sync_panel.html").read_text(encoding="utf-8")
    handler = panel[panel.index("onComplete: function(event)"):]
    handler = handler[:handler.index("onError:")]

    cancelled = handler[handler.index("event.status === 'cancelled'"):]
    # Take just the cancelled branch, up to the next ``else if`` / ``else``.
    cancelled = cancelled[:cancelled.index("} else if (failed === 0")]

    # Both the failed count and the first reason must be reported when
    # failures occurred before cancellation took hold.
    assert "failed.toLocaleString()" in cancelled
    assert "reasons[0]" in cancelled
    # And the colour must switch to danger so the summary is not read as an
    # ordinary user-requested cancellation.
    assert "var(--danger)" in cancelled


# ── One publish per sidecar, in parallel ────────────────────────────────

def _count_publishes(monkeypatch):
    """Record every sidecar publish, delegating to the real writer."""
    import xmp as xmp_module

    published = []
    original = xmp_module._write_tree_atomic

    def counting(tree, xmp_path):
        published.append(str(xmp_path))
        return original(tree, xmp_path)

    monkeypatch.setattr(xmp_module, "_write_tree_atomic", counting)
    return published


def test_sync_publishes_each_sidecar_once(tmp_path, monkeypatch):
    """Every change type a photo queued lands in a single publish.

    Each writer used to parse and republish the sidecar on its own, so a
    photo with keywords, a flag and a rating paid the full temp-file /
    fsync / ACL-copy / rename cost three times. On an SMB-mounted NAS that
    is most of the runtime.
    """
    import config as cfg
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords, read_sync_preview_metadata

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({"sync_flags_to_xmp": True})

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={"Wildlife"})
    db.queue_change(pid, "keyword_add", "Osprey")
    db.queue_change(pid, "keyword_remove", "Wildlife")
    db.queue_change(pid, "rating", "4")
    db.queue_change(pid, "flag", "flagged")

    published = _count_publishes(monkeypatch)
    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert published == [str(xmp_path)]
    # And the single published tree carries every queued change.
    assert read_keywords(xmp_path) == {"Osprey"}
    metadata = read_sync_preview_metadata(xmp_path)
    assert metadata["rating"] == "4"
    assert metadata["flag"] == "flagged"
    assert db.get_pending_changes() == []


def test_sync_skips_publishing_an_already_correct_sidecar(tmp_path, monkeypatch):
    """Re-queuing metadata the sidecar already carries costs a read, not a write."""
    from db import Database
    from sync import sync_to_xmp

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    pid, xmp_path = _setup_photo_with_xmp(tmp_path, db, keywords={"Osprey"})
    db.queue_change(pid, "keyword_add", "Osprey")
    db.queue_change(pid, "rating", "3")
    assert sync_to_xmp(db)["synced"] == 1

    before = os.path.getmtime(xmp_path)
    db.queue_change(pid, "keyword_add", "Osprey")
    db.queue_change(pid, "rating", "3")

    published = _count_publishes(monkeypatch)
    result = sync_to_xmp(db)

    # The change is still retired -- the sidecar already says what it asks for.
    assert result["synced"] == 1
    assert result["failed"] == 0
    assert published == []
    assert os.path.getmtime(xmp_path) == before
    assert db.get_pending_changes() == []


def test_sync_writes_independent_sidecars_concurrently(tmp_path):
    """Two photos in different folders are published at the same time.

    Sidecar writes are network-latency bound, so the run must overlap them.
    The barrier only clears when two writers are inside the publish at once;
    a serial loop leaves each waiting alone until it times out.
    """
    import threading

    import xmp as xmp_module
    from db import Database
    from sync import sync_to_xmp

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    for i in range(2):
        pid, _ = _setup_photo_with_xmp(tmp_path / f"lot{i}", db)
        db.queue_change(pid, "keyword_add", "Osprey")

    barrier = threading.Barrier(2, timeout=30)
    original = xmp_module._write_tree_atomic

    def publish_together(tree, xmp_path):
        barrier.wait()
        return original(tree, xmp_path)

    xmp_module._write_tree_atomic = publish_together
    try:
        result = sync_to_xmp(db)
    finally:
        xmp_module._write_tree_atomic = original

    assert result["failed"] == 0, result["errors"]
    assert result["synced"] == 2


def test_sync_keeps_photos_sharing_a_sidecar_on_one_writer(tmp_path):
    """A RAW and a JPEG share one .xmp; their changes must not clobber.

    Both photos resolve to ``bird.xmp``. Published in parallel, each would
    load the pre-change tree and the loser's keyword would vanish.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    jpeg_id, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    folder_id = db.get_photo(jpeg_id)["folder_id"]
    raw_id = db.add_photo(folder_id=folder_id, filename="bird.nef",
                          extension=".nef", file_size=100, file_mtime=1.0)
    db.queue_change(jpeg_id, "keyword_add", "Osprey")
    db.queue_change(raw_id, "keyword_add", "Kestrel")

    result = sync_to_xmp(db)

    assert result["synced"] == 2
    assert read_keywords(xmp_path) == {"Osprey", "Kestrel"}


def test_sync_resolves_paths_once_per_run_and_folder(tmp_path, monkeypatch):
    """Path resolution and the offline-folder probe scale with folders, not photos.

    Resolving a sidecar per photo re-ran the recursive folder-tree CTE and a
    full photo-detail SELECT every time, and probed the same folder once per
    photo -- a network round trip each on a NAS.
    """
    from db import Database
    from sync import sync_to_xmp

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    pid, _ = _setup_photo_with_xmp(tmp_path, db)
    folder_id = db.get_photo(pid)["folder_id"]
    db.queue_change(pid, "keyword_add", "Osprey")
    for i in range(4):
        sibling = db.add_photo(folder_id=folder_id, filename=f"more{i}.jpg",
                               extension=".jpg", file_size=100, file_mtime=1.0)
        db.queue_change(sibling, "keyword_add", "Osprey")

    tree_calls = []
    original_tree = db.get_folder_tree
    monkeypatch.setattr(
        db, "get_folder_tree",
        lambda: (tree_calls.append(1), original_tree())[1],
    )
    isdir_calls = []
    original_isdir = os.path.isdir
    monkeypatch.setattr(
        os.path, "isdir",
        lambda path: (isdir_calls.append(path), original_isdir(path))[1],
    )

    result = sync_to_xmp(db)

    assert result["synced"] == 5
    assert len(tree_calls) == 1
    assert len([p for p in isdir_calls if str(tmp_path) in str(p)]) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="symlink creation needs elevation")
def test_sync_serializes_photos_reached_through_folder_alias(tmp_path):
    """A folder symlink and its target resolve to one sidecar, on one worker.

    Two photos in two folder rows that ``realpath``-collapse to the same
    directory used to hash to different ``by_sidecar`` keys, so parallel
    workers parsed one .xmp, mutated it, and each atomically rewrote it --
    the last writer's tree won and the other photo's keyword vanished even
    though both changes were reported synced.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    jpeg_id, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    real_folder = os.path.dirname(xmp_path)
    alias_folder = str(tmp_path / "alias")
    os.symlink(real_folder, alias_folder)
    alias_fid = db.add_folder(alias_folder, name="alias")
    raw_id = db.add_photo(
        folder_id=alias_fid, filename="bird.nef", extension=".nef",
        file_size=100, file_mtime=1.0,
    )
    db.queue_change(jpeg_id, "keyword_add", "Osprey")
    db.queue_change(raw_id, "keyword_add", "Kestrel")

    result = sync_to_xmp(db)

    assert result["synced"] == 2, result["errors"]
    assert read_keywords(xmp_path) == {"Osprey", "Kestrel"}


def test_sync_serializes_case_variant_sidecars_without_merging_them(tmp_path):
    """Case-variant sidecar paths share a worker but keep their own files.

    ``Bird.CR3`` and ``BIRD.JPG`` in one folder derive sidecar paths ending in
    ``Bird.xmp`` and ``BIRD.xmp`` -- distinct strings, but the same file on
    APFS, SMB and NTFS. They must never be published in parallel, or one
    photo's tree overwrites the other's while both report synced.

    Both properties are asserted without depending on the host filesystem's
    case behaviour, because that is exactly what the code no longer probes:
    the two writes must land on one worker thread, and each photo's keyword
    must reach the path that photo names. On a case-insensitive host those
    two paths are one file holding both keywords; on a case-sensitive host
    they are two files holding one each. Either way nothing is lost and
    nothing is written into the wrong sidecar.
    """
    import threading

    import xmp as xmp_module
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords, write_sidecar

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    a_id, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    folder = os.path.dirname(xmp_path)
    folder_id = db.get_photo(a_id)["folder_id"]
    # Rename the sidecar and the photo so it looks like the mixed-case source.
    mixed_path = os.path.join(folder, "Bird.xmp")
    os.rename(xmp_path, mixed_path)
    db.conn.execute(
        "UPDATE photos SET filename = ?, extension = ? WHERE id = ?",
        ("Bird.CR3", ".CR3", a_id),
    )
    b_id = db.add_photo(
        folder_id=folder_id, filename="BIRD.JPG", extension=".JPG",
        file_size=100, file_mtime=1.0,
    )
    upper_path = os.path.join(folder, "BIRD.xmp")
    write_sidecar(upper_path, flat_keywords=set(), hierarchical_keywords=set())
    db.queue_change(a_id, "keyword_add", "Osprey")
    db.queue_change(b_id, "keyword_add", "Kestrel")

    threads = []
    original = xmp_module._write_tree_atomic

    def record_thread(tree, path):
        threads.append(threading.current_thread().name)
        return original(tree, path)

    xmp_module._write_tree_atomic = record_thread
    try:
        result = sync_to_xmp(db)
    finally:
        xmp_module._write_tree_atomic = original

    assert result["synced"] == 2, result["errors"]
    # One group means one worker: the publishes cannot have overlapped.
    assert len(set(threads)) == 1, threads
    # And each photo's change reached the path that photo names.
    assert "Osprey" in read_keywords(mixed_path)
    assert "Kestrel" in read_keywords(upper_path)


@pytest.mark.skipif(sys.platform == "win32", reason="symlink creation needs elevation")
def test_sync_serializes_photos_whose_sidecars_are_symlinked_together(tmp_path):
    """A sidecar that is a symlink to another photo's sidecar is one file.

    These two paths differ in basename, so grouping puts them in different
    buckets and the pool runs them concurrently -- but ``_write_tree_atomic``
    resolves the link before replacing, so both publish over the same file.
    Without the per-target lock each editor parses the pre-change tree and
    the last writer discards the other photo's keyword while both report
    synced.
    """
    import time

    import xmp as xmp_module
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    target_id, target_xmp = _setup_photo_with_xmp(tmp_path, db)
    folder = os.path.dirname(target_xmp)
    folder_id = db.get_photo(target_id)["folder_id"]
    link_id = db.add_photo(folder_id=folder_id, filename="hawk.jpg",
                           extension=".jpg", file_size=100, file_mtime=1.0)
    os.symlink(target_xmp, os.path.join(folder, "hawk.xmp"))

    db.queue_change(target_id, "keyword_add", "Osprey")
    db.queue_change(link_id, "keyword_add", "Kestrel")

    # Widen the window between parse and publish so an unserialized run
    # would reliably lose one of the two keywords.
    original = xmp_module._write_tree_atomic

    def slow_publish(tree, path):
        time.sleep(0.05)
        return original(tree, path)

    xmp_module._write_tree_atomic = slow_publish
    try:
        result = sync_to_xmp(db)
    finally:
        xmp_module._write_tree_atomic = original

    assert result["synced"] == 2, result["errors"]
    assert read_keywords(target_xmp) == {"Osprey", "Kestrel"}


def test_sync_reports_a_malformed_queue_row_without_aborting_the_run(tmp_path):
    """One unplannable photo fails alone; the rest of the run still writes.

    ``_plan_photo_sync`` calls ``int(value)`` for a rating, and the schema
    permits a NULL or non-numeric value. Planning moved out of the per-photo
    try when the writes were hoisted onto a pool, which let one legacy row
    abort every other photo's sidecar.
    """
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    bad_id, _ = _setup_photo_with_xmp(tmp_path / "bad", db)
    good_id, good_xmp = _setup_photo_with_xmp(tmp_path / "good", db)
    db.queue_change(bad_id, "rating", "not-a-number")
    db.queue_change(good_id, "keyword_add", "Osprey")

    result = sync_to_xmp(db)

    assert result["synced"] == 1
    assert result["failed"] == 1
    assert result["ok"] is False
    assert result["failures"][0]["photo_id"] == bad_id
    assert read_keywords(good_xmp) == {"Osprey"}
    # The good photo's row is retired; the malformed one stays queued.
    assert [c["photo_id"] for c in db.get_pending_changes()] == [bad_id]


def test_sync_serializes_folder_rows_that_differ_only_in_case(tmp_path):
    """Two folder rows spelled differently in case are one directory.

    A share mounted case-insensitively can be catalogued twice --
    ``/mnt/Photos`` and ``/mnt/PHOTOS`` -- and ``realpath`` preserves each
    spelling while ``normcase`` does nothing off Windows, so folding only the
    basename left the two spellings in separate groups holding separate
    locks. Their photos could then publish over the same sidecar at once.
    """
    import threading

    import xmp as xmp_module
    from db import Database
    from sync import sync_to_xmp
    from xmp import read_keywords, write_sidecar

    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    a_id, xmp_a = _setup_photo_with_xmp(tmp_path, db)
    lower_folder = os.path.dirname(xmp_a)
    upper_folder = os.path.join(os.path.dirname(lower_folder), "PHOTOS")
    # The same directory on a case-insensitive host, a sibling on a
    # case-sensitive one; either way the two rows must share a worker.
    os.makedirs(upper_folder, exist_ok=True)
    upper_fid = db.add_folder(upper_folder, name="PHOTOS")
    # Same basename as the photo in the lower-case row, so the two sidecar
    # paths differ only in the folder's spelling.
    b_id = db.add_photo(folder_id=upper_fid, filename="bird.nef",
                        extension=".nef", file_size=100, file_mtime=1.0)
    xmp_b = os.path.join(upper_folder, "bird.xmp")
    write_sidecar(xmp_b, flat_keywords=set(), hierarchical_keywords=set())

    db.queue_change(a_id, "keyword_add", "Osprey")
    db.queue_change(b_id, "keyword_add", "Kestrel")

    threads = []
    original = xmp_module._write_tree_atomic

    def record_thread(tree, path):
        threads.append(threading.current_thread().name)
        return original(tree, path)

    xmp_module._write_tree_atomic = record_thread
    try:
        result = sync_to_xmp(db)
    finally:
        xmp_module._write_tree_atomic = original

    assert result["synced"] == 2, result["errors"]
    assert len(set(threads)) == 1, threads
    assert "Osprey" in read_keywords(xmp_a)
    assert "Kestrel" in read_keywords(xmp_b)


def test_sync_clears_rows_that_predate_the_change_token_column(tmp_path):
    """`IN (NULL)` matches nothing, so a token-only clear would never clear them.

    change_token is a nullable TEXT column with no backfill, and a real
    catalog still holds rows queued before it existed. Clearing those by
    token would leave them queued forever, rewritten by every later sync.
    """
    import sync
    from db import Database

    db = Database(str(tmp_path / "t.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    photo, _xmp_path = _setup_photo_with_xmp(tmp_path, db)
    db.conn.execute(
        "INSERT INTO pending_changes (photo_id, change_type, value, change_token, workspace_id) "
        "VALUES (?, 'keyword_add', 'Osprey', NULL, ?)",
        (photo, db._ws_id()),
    )
    db.conn.commit()

    result = sync.sync_to_xmp(db)
    assert result["ok"], result
    assert result["synced"] == 1, result
    assert db.count_pending_changes() == 0, "a NULL-token row was left queued"
    db.close()


@pytest.mark.parametrize("order,expected", [
    (("keyword_add", "keyword_remove"), set()),
    (("keyword_remove", "keyword_add"), {"Osprey"}),
])
def test_sync_resolves_same_keyword_add_and_remove_to_the_later_one(tmp_path, order, expected):
    """A genuine add-then-remove of one term is not a normalization rename.

    _remove_planned_keywords pairs any add/remove sharing a normalized key,
    strips the legacy spelling and writes the term back -- right for a rename
    (`'apapane` -> `apapane`, two spellings), wrong for the same string twice,
    where it turned a removal into a no-op. The pair became reachable once
    the keyword endpoints stopped cancelling a queued opposite for photos
    under a pre-transfer NAS sync.
    """
    import sync
    from db import Database
    from xmp import read_keywords, write_sidecar

    db = Database(str(tmp_path / "t.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    photo, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    write_sidecar(xmp_path, {"Osprey"}, set())
    for kind in order:
        db.queue_change(photo, kind, "Osprey")

    result = sync.sync_to_xmp(db)
    assert result["ok"], result
    assert (read_keywords(xmp_path) or set()) == expected
    # Both rows clear either way: the losing intent was superseded, not
    # dropped unapplied.
    assert db.count_pending_changes() == 0
    db.close()


def test_sync_writes_sidecar_under_a_workspace_that_cannot_see_the_folder(tmp_path):
    """`folder_scope="global"` reads the folder map from the whole catalog.

    The pre-transfer NAS sync activates the queue owner's workspace so its
    ``sync_flags_to_xmp`` / ``write_assigned_location_to_xmp`` settings apply
    to the edits made there, but a staging folder can be unlinked from that
    workspace (linked only to a sibling) between the queue and the send. The
    default per-workspace resolution would then abort the run with "folder
    not accessible" though the file is right there.
    """
    import sync
    from db import Database
    from xmp import read_keywords

    db = Database(str(tmp_path / "t.db"))
    owner = db.ensure_default_workspace()
    db.set_active_workspace(owner)
    photo, xmp_path = _setup_photo_with_xmp(tmp_path, db)
    db.queue_change(photo, "keyword_add", "Osprey")

    # Unlink the folder from the owner. It stays a catalog row that a sibling
    # workspace still sees, but ``get_folder_tree()`` under the owner returns
    # nothing for it.
    db.conn.execute(
        "DELETE FROM workspace_folders WHERE workspace_id = ?", (owner,))
    db.conn.commit()

    per_workspace = sync.sync_to_xmp(db)
    # Per-workspace resolution reports the exact failure the option 1 fix
    # addresses -- the run cannot find the folder to write into.
    assert per_workspace["synced"] == 0, per_workspace
    assert any("folder not accessible" in f.get("error", "")
               for f in per_workspace["failures"]), per_workspace
    # And the pending row stays queued: nothing was written, so nothing clears.
    assert db.count_pending_changes() == 1

    result = sync.sync_to_xmp(db, folder_scope="global")
    assert result["ok"], result
    assert result["synced"] == 1, result
    assert "Osprey" in (read_keywords(xmp_path) or set())
    assert db.count_pending_changes() == 0
    db.close()
