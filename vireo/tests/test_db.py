# vireo/tests/test_db.py
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_create_tables(tmp_path):
    """Database creates all tables on init."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Verify all tables exist
    tables = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = {r['name'] for r in tables}
    assert 'folders' in table_names
    assert 'photos' in table_names
    assert 'keywords' in table_names
    assert 'photo_keywords' in table_names
    assert 'collections' in table_names
    assert 'pending_changes' in table_names


def test_edit_history_tables_exist(tmp_path):
    """edit_history and edit_history_items tables exist after init."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    tables = [r['name'] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    assert 'edit_history' in tables
    assert 'edit_history_items' in tables


def test_photos_table_has_exif_data_column(tmp_path):
    """photos table has exif_data JSON column."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Column exists (either in CREATE or via ALTER)
    col = db.conn.execute("SELECT exif_data FROM photos LIMIT 0").fetchall()
    assert col is not None  # No exception means column exists


def test_working_copy_path_column_exists(tmp_path):
    """The photos table has a working_copy_path column after migration."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    row = db.conn.execute("SELECT working_copy_path FROM photos LIMIT 0").description
    assert row[0][0] == "working_copy_path"


def test_add_and_get_folder(tmp_path):
    """add_folder creates a folder, get_folder_tree returns it."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos/2024', name='2024')
    assert fid is not None

    tree = db.get_folder_tree()
    assert len(tree) == 1
    assert tree[0]['path'] == '/photos/2024'
    assert tree[0]['name'] == '2024'


def test_folder_hierarchy(tmp_path):
    """Folders can have parent-child relationships."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    parent_id = db.add_folder('/photos', name='photos')
    child_id = db.add_folder('/photos/2024', name='2024', parent_id=parent_id)

    tree = db.get_folder_tree()
    assert len(tree) == 2
    child = [f for f in tree if f['name'] == '2024'][0]
    assert child['parent_id'] == parent_id


def test_add_and_get_photo(tmp_path):
    """add_photo creates a photo, get_photo retrieves it."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(
        folder_id=fid,
        filename='DSC_0001.NEF',
        extension='.nef',
        file_size=25000000,
        file_mtime=1700000000.0,
        timestamp='2024-01-15T10:30:00',
        width=6000,
        height=4000,
    )
    assert pid is not None

    photo = db.get_photo(pid)
    assert photo['filename'] == 'DSC_0001.NEF'
    assert photo['extension'] == '.nef'
    assert photo['folder_id'] == fid
    assert photo['rating'] == 0
    assert photo['flag'] == 'none'


def test_get_photos_pagination(tmp_path):
    """get_photos supports pagination."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    for i in range(25):
        db.add_photo(folder_id=fid, filename=f'IMG_{i:04d}.jpg', extension='.jpg',
                     file_size=1000, file_mtime=1700000000.0 + i)

    page1 = db.get_photos(page=1, per_page=10)
    assert len(page1) == 10
    page2 = db.get_photos(page=2, per_page=10)
    assert len(page2) == 10
    page3 = db.get_photos(page=3, per_page=10)
    assert len(page3) == 5


def test_get_photos_filter_by_folder(tmp_path):
    """get_photos can filter by folder_id."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    f1 = db.add_folder('/photos/a', name='a')
    f2 = db.add_folder('/photos/b', name='b')
    db.add_photo(folder_id=f1, filename='a1.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=f2, filename='b1.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=f2, filename='b2.jpg', extension='.jpg', file_size=100, file_mtime=1.0)

    results = db.get_photos(folder_id=f2)
    assert len(results) == 2
    assert all(r['folder_id'] == f2 for r in results)


def test_folder_subtree_does_not_expand_when_root_is_inactive(tmp_path):
    """A stale/crafted folder_id for an out-of-workspace root must not expand.

    Tree: A(inactive) -> B(active). Passing A should not pull B in — otherwise
    a stale request could surface photos from active descendants of a folder
    that no longer belongs to the current workspace.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    active_ws = db._ws_id()
    other_ws = db.create_workspace('Other')
    a = db.add_folder('/a', name='a')
    b = db.add_folder('/a/b', name='b', parent_id=a)
    # Detach A from the active workspace; B stays.
    db.remove_workspace_folder(active_ws, a)
    db.add_workspace_folder(other_ws, a)

    db.add_photo(folder_id=b, filename='b.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)

    # Only A itself comes back (no expansion into B).
    assert db.get_folder_subtree_ids(a) == [a]
    # And since A itself isn't in the active workspace, the photo query returns nothing.
    assert db.get_photos(folder_id=a) == []


def test_folder_subtree_does_not_cross_workspace_boundary(tmp_path):
    """Expansion stops at folders removed from the active workspace.

    Tree: A (active) -> B (not active) -> C (active). Filtering by A should
    NOT include C even though C is in the active workspace, because the
    intermediate B is detached from A in the active workspace's tree.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    active_ws = db._ws_id()
    other_ws = db.create_workspace('Other')
    a = db.add_folder('/a', name='a')
    b = db.add_folder('/a/b', name='b', parent_id=a)
    c = db.add_folder('/a/b/c', name='c', parent_id=b)
    # Move B out of the active workspace; A and C stay.
    db.remove_workspace_folder(active_ws, b)
    db.add_workspace_folder(other_ws, b)

    db.add_photo(folder_id=a, filename='a.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=c, filename='c.jpg', extension='.jpg',
                 file_size=100, file_mtime=2.0)

    assert db.get_folder_subtree_ids(a) == [a]
    results = db.get_photos(folder_id=a)
    assert len(results) == 1
    assert results[0]['filename'] == 'a.jpg'


def test_get_photos_folder_filter_includes_descendants(tmp_path):
    """get_photos(folder_id=parent) includes photos from descendant folders."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    root = db.add_folder('/photos', name='photos')
    year = db.add_folder('/photos/2024', name='2024', parent_id=root)
    leaf = db.add_folder('/photos/2024/01-15', name='01-15', parent_id=year)
    sibling = db.add_folder('/other', name='other')

    db.add_photo(folder_id=root, filename='top.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=year, filename='mid.jpg', extension='.jpg', file_size=100, file_mtime=2.0)
    db.add_photo(folder_id=leaf, filename='deep.jpg', extension='.jpg', file_size=100, file_mtime=3.0)
    db.add_photo(folder_id=sibling, filename='other.jpg', extension='.jpg', file_size=100, file_mtime=4.0)

    assert len(db.get_photos(folder_id=root)) == 3
    assert len(db.get_photos(folder_id=year)) == 2
    assert len(db.get_photos(folder_id=leaf)) == 1
    assert len(db.get_photos(folder_id=sibling)) == 1


def test_count_filtered_photos_folder_includes_descendants(tmp_path):
    """count_filtered_photos(folder_id=parent) counts descendant photos."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    root = db.add_folder('/p', name='p')
    child = db.add_folder('/p/c', name='c', parent_id=root)
    db.add_photo(folder_id=child, filename='x.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=child, filename='y.jpg', extension='.jpg', file_size=100, file_mtime=2.0)

    assert db.count_filtered_photos(folder_id=root) == 2
    assert db.count_filtered_photos(folder_id=child) == 2


def test_browse_summary_folder_includes_descendants(tmp_path):
    """get_browse_summary(folder_id=parent) counts descendants in filtered_total."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    root = db.add_folder('/p', name='p')
    child = db.add_folder('/p/c', name='c', parent_id=root)
    db.add_photo(folder_id=child, filename='x.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=root, filename='y.jpg', extension='.jpg', file_size=100, file_mtime=2.0)

    summary = db.get_browse_summary(folder_id=root)
    assert summary['filtered_total'] == 2


def test_browse_summary_top_species_filters_to_latest_fingerprint(tmp_path):
    """get_browse_summary's top-species ranking must pin to the most
    recent labels_fingerprint per (detection, classifier_model).
    Otherwise a stale higher-confidence row from an old label set wins
    ROW_NUMBER() and `/api/browse/summary` reports the wrong species.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/p', name='p')
    pid = db.add_photo(folder_id=fid, filename='a.jpg',
                       extension='.jpg', file_size=100, file_mtime=1.0)
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    # Stale fingerprint — HIGHER confidence + 'Finch'.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Finch', 0.99, '2026-01-01')",
        (det_id,),
    )
    # Current fingerprint — lower confidence + 'Robin'.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Robin', 0.7, '2026-04-25')",
        (det_id,),
    )
    db.conn.commit()

    summary = db.get_browse_summary()
    species_list = [(s["species"], s["count"]) for s in summary["top_species"]]
    assert species_list == [("Robin", 1)], (
        f"top_species must be the active-fingerprint Robin (lower "
        f"confidence) — not stale-fingerprint Finch (higher); "
        f"got {species_list}"
    )


def test_calendar_data_folder_includes_descendants(tmp_path):
    """get_calendar_data(folder_id=parent) counts descendants."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    root = db.add_folder('/p', name='p')
    child = db.add_folder('/p/c', name='c', parent_id=root)
    db.add_photo(folder_id=child, filename='x.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-15T00:00:00')
    db.add_photo(folder_id=child, filename='y.jpg', extension='.jpg', file_size=100,
                 file_mtime=2.0, timestamp='2024-06-15T00:01:00')

    data = db.get_calendar_data(year=2024, folder_id=root)
    assert data['days'].get('2024-06-15') == 2


def test_geolocated_photos_folder_includes_descendants(tmp_path):
    """get_geolocated_photos(folder_id=parent) includes descendant-folder photos."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    root = db.add_folder('/p', name='p')
    child = db.add_folder('/p/c', name='c', parent_id=root)
    pid1 = db.add_photo(folder_id=child, filename='x.jpg', extension='.jpg',
                        file_size=100, file_mtime=1.0)
    pid2 = db.add_photo(folder_id=root, filename='y.jpg', extension='.jpg',
                        file_size=100, file_mtime=2.0)
    db.conn.execute("UPDATE photos SET latitude = 10.0, longitude = 20.0 WHERE id = ?", (pid1,))
    db.conn.execute("UPDATE photos SET latitude = 11.0, longitude = 21.0 WHERE id = ?", (pid2,))
    db.conn.commit()

    photos = db.get_geolocated_photos(folder_id=root)
    assert len(photos) == 2


def test_get_photos_filter_by_rating(tmp_path):
    """get_photos can filter by minimum rating."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.update_photo_rating(p1, 3)
    db.update_photo_rating(p2, 5)

    results = db.get_photos(rating_min=4)
    assert len(results) == 1
    assert results[0]['filename'] == 'b.jpg'


def test_get_photos_filter_by_date_range(tmp_path):
    """get_photos can filter by date range."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='jan.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-01-15T10:00:00')
    db.add_photo(folder_id=fid, filename='jun.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-15T10:00:00')
    db.add_photo(folder_id=fid, filename='dec.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-12-15T10:00:00')

    results = db.get_photos(date_from='2024-03-01', date_to='2024-09-01')
    assert len(results) == 1
    assert results[0]['filename'] == 'jun.jpg'


def test_get_photos_sort(tmp_path):
    """get_photos supports different sort orders."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-01T00:00:00')
    db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-01-01T00:00:00')

    by_name = db.get_photos(sort='name')
    assert by_name[0]['filename'] == 'a.jpg'

    by_date = db.get_photos(sort='date')
    assert by_date[0]['filename'] == 'a.jpg'  # earlier date first

    by_date_desc = db.get_photos(sort='date_desc')
    assert by_date_desc[0]['filename'] == 'b.jpg'


def test_sort_date_tiebreaker(tmp_path):
    """Photos with identical timestamps sort by filename as tiebreaker."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    # Insert in non-alphabetical order
    db.add_photo(folder_id=fid, filename='IMG_003.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-15T14:30:00')
    db.add_photo(folder_id=fid, filename='IMG_001.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-15T14:30:00')
    db.add_photo(folder_id=fid, filename='IMG_002.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-15T14:30:00')

    by_date = db.get_photos(sort='date')
    assert [p['filename'] for p in by_date] == ['IMG_001.jpg', 'IMG_002.jpg', 'IMG_003.jpg']

    by_date_desc = db.get_photos(sort='date_desc')
    assert [p['filename'] for p in by_date_desc] == ['IMG_001.jpg', 'IMG_002.jpg', 'IMG_003.jpg']


def test_date_filter_inclusive_with_subsec(tmp_path):
    """date_to filter includes photos with sub-second timestamps."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-15T23:59:59.500000')
    db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-15T12:00:00')

    # Bare date bound should include both photos
    photos = db.get_photos(date_to='2024-06-15')
    assert len(photos) == 2

    # Second-precision bound should still include sub-second photo
    photos = db.get_photos(date_to='2024-06-15T23:59:59')
    assert len(photos) == 2

    # Bound before the sub-second photo should exclude it
    photos = db.get_photos(date_to='2024-06-15T23:59:58')
    assert len(photos) == 1
    assert photos[0]['filename'] == 'b.jpg'


def test_inclusive_date_to_edge_cases():
    """_inclusive_date_to handles non-string and short fractional inputs."""
    from db import _inclusive_date_to

    # Non-string input returns None (fail closed)
    assert _inclusive_date_to(20240615) is None
    assert _inclusive_date_to(True) is None

    # Short fractional seconds are padded with 9s
    assert _inclusive_date_to("2024-06-15T23:59:59.5") == "2024-06-15T23:59:59.599999"
    assert _inclusive_date_to("2024-06-15T23:59:59.50") == "2024-06-15T23:59:59.509999"
    assert _inclusive_date_to("2024-06-15T23:59:59.500") == "2024-06-15T23:59:59.500999"

    # Already 6 digits — unchanged
    assert _inclusive_date_to("2024-06-15T23:59:59.500000") == "2024-06-15T23:59:59.500000"

    # None passthrough
    assert _inclusive_date_to(None) is None


def test_update_photo_rating(tmp_path):
    """update_photo_rating changes the rating."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.update_photo_rating(pid, 4)
    photo = db.get_photo(pid)
    assert photo['rating'] == 4


def test_update_photo_flag(tmp_path):
    """update_photo_flag changes the flag."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.update_photo_flag(pid, 'flagged')
    photo = db.get_photo(pid)
    assert photo['flag'] == 'flagged'


def test_keyword_hierarchy(tmp_path):
    """Keywords support parent-child hierarchy."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    birds = db.add_keyword('Birds')
    raptors = db.add_keyword('Raptors', parent_id=birds)
    hawk = db.add_keyword('Red-tailed hawk', parent_id=raptors)

    # Keywords need photos in the workspace to appear in the tree
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.tag_photo(pid, birds)
    db.tag_photo(pid, raptors)
    db.tag_photo(pid, hawk)

    tree = db.get_keyword_tree()
    assert len(tree) == 3

    hawk_row = [k for k in tree if k['name'] == 'Red-tailed hawk'][0]
    assert hawk_row['parent_id'] == raptors


def test_tag_and_untag_photo(tmp_path):
    """tag_photo and untag_photo manage photo-keyword associations."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    kid = db.add_keyword('Cardinal')

    db.tag_photo(pid, kid)
    keywords = db.get_photo_keywords(pid)
    assert len(keywords) == 1
    assert keywords[0]['name'] == 'Cardinal'

    db.untag_photo(pid, kid)
    keywords = db.get_photo_keywords(pid)
    assert len(keywords) == 0


def test_pending_changes_queue(tmp_path):
    """queue_change adds entries, get_pending_changes reads them, clear_pending removes them."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)

    db.queue_change(pid, 'rating', '4')
    db.queue_change(pid, 'keyword_add', 'Cardinal')

    changes = db.get_pending_changes()
    assert len(changes) == 2

    db.clear_pending([c['id'] for c in changes])
    assert len(db.get_pending_changes()) == 0


def test_get_photos_keyword_search(tmp_path):
    """get_photos can filter by keyword name."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='cardinal.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='sparrow.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    k1 = db.add_keyword('Cardinal')
    k2 = db.add_keyword('Sparrow')
    db.tag_photo(p1, k1)
    db.tag_photo(p2, k2)

    results = db.get_photos(keyword='Cardinal')
    assert len(results) == 1
    assert results[0]['filename'] == 'cardinal.jpg'


def test_add_keyword_idempotent(tmp_path):
    """add_keyword returns existing id if keyword already exists."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    id1 = db.add_keyword('Birds')
    id2 = db.add_keyword('Birds')
    assert id1 == id2


def test_collection_crud(tmp_path):
    """add_collection, get_collections work."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    rules = [{"field": "rating", "op": ">=", "value": 4}]
    cid = db.add_collection('Best Photos', json.dumps(rules))
    assert cid is not None

    colls = db.get_collections()
    assert len(colls) == 1
    assert colls[0]['name'] == 'Best Photos'


def test_collection_photos_rating_rule(tmp_path):
    """get_collection_photos filters by rating rule."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='good.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='bad.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.update_photo_rating(p1, 5)
    db.update_photo_rating(p2, 2)

    rules = [{"field": "rating", "op": ">=", "value": 4}]
    cid = db.add_collection('Best', json.dumps(rules))

    photos = db.get_collection_photos(cid)
    assert len(photos) == 1
    assert photos[0]['filename'] == 'good.jpg'


def test_collection_photos_keyword_rule(tmp_path):
    """get_collection_photos filters by keyword contains rule."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='sparrow.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    k1 = db.add_keyword('Red-tailed hawk')
    k2 = db.add_keyword('Song sparrow')
    db.tag_photo(p1, k1)
    db.tag_photo(p2, k2)

    rules = [{"field": "keyword", "op": "contains", "value": "hawk"}]
    cid = db.add_collection('Hawks', json.dumps(rules))

    photos = db.get_collection_photos(cid)
    assert len(photos) == 1
    assert photos[0]['filename'] == 'hawk.jpg'


def test_collection_untagged_rule(tmp_path):
    """get_collection_photos filters by keyword_count equals 0."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='tagged.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='untagged.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    kid = db.add_keyword('Bird')
    db.tag_photo(p1, kid)

    rules = [{"field": "keyword_count", "op": "equals", "value": 0}]
    cid = db.add_collection('Untagged', json.dumps(rules))

    photos = db.get_collection_photos(cid)
    assert len(photos) == 1
    assert photos[0]['filename'] == 'untagged.jpg'


def test_collection_recent_days_rule(tmp_path):
    """get_collection_photos filters by timestamp recent_days."""
    import json
    from datetime import datetime, timedelta

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')

    recent_ts = (datetime.now() - timedelta(days=5)).isoformat()
    old_ts = (datetime.now() - timedelta(days=60)).isoformat()

    db.add_photo(folder_id=fid, filename='recent.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0, timestamp=recent_ts)
    db.add_photo(folder_id=fid, filename='old.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0, timestamp=old_ts)

    rules = [{"field": "timestamp", "op": "recent_days", "value": 30}]
    cid = db.add_collection('Recent', json.dumps(rules))

    photos = db.get_collection_photos(cid)
    assert len(photos) == 1
    assert photos[0]['filename'] == 'recent.jpg'


def test_collection_timestamp_between_subsec(tmp_path):
    """Collection timestamp 'between' rule includes sub-second photos."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')

    db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0, timestamp='2024-06-15T23:59:59.500000')
    db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0, timestamp='2024-06-15T12:00:00')

    rules = [{"field": "timestamp", "op": "between",
              "value": ["2024-06-15", "2024-06-15T23:59:59"]}]
    cid = db.add_collection('June 15', json.dumps(rules))

    photos = db.get_collection_photos(cid)
    assert len(photos) == 2


def test_collection_has_species_rule(tmp_path):
    """get_collection_photos filters by has_species rule."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='classified.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='location_only.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p3 = db.add_photo(folder_id=fid, filename='no_tags.jpg', extension='.jpg', file_size=100, file_mtime=1.0)

    k_species = db.add_keyword('Northern cardinal', is_species=True)
    k_location = db.add_keyword('The Park', is_species=False)
    db.tag_photo(p1, k_species)
    db.tag_photo(p2, k_location)

    # Needs classification: no species keyword
    rules = [{"field": "has_species", "op": "equals", "value": 0}]
    cid = db.add_collection('Needs Classification', json.dumps(rules))

    photos = db.get_collection_photos(cid)
    filenames = {p['filename'] for p in photos}
    assert 'location_only.jpg' in filenames
    assert 'no_tags.jpg' in filenames
    assert 'classified.jpg' not in filenames


def test_has_subject_rule_matches_photos_without_subject_keywords(tmp_path, monkeypatch):
    """has_subject==0 returns photos that have no keyword whose type is in
    the workspace's subject_types set."""
    import json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='p2.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    db.tag_photo(p1, scene_kid)  # p1 is identified, p2 is not

    cid = db.add_collection(
        "Needs Subject",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 0}]),
    )
    photos = db.get_collection_photos(cid, per_page=999)
    pids = {p["id"] for p in photos}
    assert pids == {p2}


def test_has_subject_rule_value_one_matches_identified_photos(tmp_path, monkeypatch):
    """has_subject==1 is the inverse — only identified photos match."""
    import json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='p2.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    db.tag_photo(p1, scene_kid)

    cid = db.add_collection(
        "Has Subject",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 1}]),
    )
    photos = db.get_collection_photos(cid, per_page=999)
    pids = {p["id"] for p in photos}
    assert pids == {p1}


def test_has_subject_rule_empty_subject_types_value_one_matches_no_photos(tmp_path, monkeypatch):
    """When subject_types is empty, has_subject==1 should match no photos
    (no type counts as 'identifying')."""
    import json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace(ws_id, config_overrides={"subject_types": []})

    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    # Tag with a genre keyword anyway — but with empty subject_types, no
    # type counts as identifying, so this photo still does not match.
    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    db.tag_photo(p1, scene_kid)

    cid = db.add_collection(
        "Has Subject (empty types)",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 1}]),
    )
    photos = db.get_collection_photos(cid, per_page=999)
    assert photos == []


def test_add_keyword_is_species(tmp_path):
    """add_keyword with is_species=True marks the keyword."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword('Song sparrow', is_species=True)
    row = db.conn.execute("SELECT is_species FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row['is_species'] == 1


def test_add_keyword_updates_is_species(tmp_path):
    """add_keyword updates is_species on existing keyword if newly marked."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword('Song sparrow', is_species=False)
    row = db.conn.execute("SELECT is_species FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row['is_species'] == 0

    kid2 = db.add_keyword('Song sparrow', is_species=True)
    assert kid2 == kid
    row = db.conn.execute("SELECT is_species FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row['is_species'] == 1


def test_mark_species_keywords_sets_type_taxonomy(tmp_path):
    """mark_species_keywords sets both is_species=1 AND type='taxonomy'.

    Regression test: previously it only set is_species, so the UI keyword
    type filter (which reads the `type` column) stayed on 'general' for
    keywords imported via XMP sync or manual add.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # A keyword that arrived without is_species=True (e.g., from XMP sync).
    kid = db.add_keyword('Green heron')
    row = db.conn.execute(
        "SELECT is_species, type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row['is_species'] == 0
    assert row['type'] == 'general'

    class FakeTaxonomy:
        def lookup(self, name):
            if name.lower() == 'green heron':
                return {"taxon_id": 5017, "scientific_name": "Butorides virescens"}
            return None

        def is_taxon(self, name):
            return self.lookup(name) is not None

    updated = db.mark_species_keywords(FakeTaxonomy())
    assert updated == 1

    row = db.conn.execute(
        "SELECT is_species, type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row['is_species'] == 1
    assert row['type'] == 'taxonomy'


def test_mark_species_keywords_links_local_taxon_id(tmp_path):
    """mark_species_keywords links keywords.taxon_id when taxa table has a match."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Seed a row in the local taxa table so the inat_id lookup succeeds.
    db.conn.execute(
        "INSERT INTO taxa (inat_id, name, rank, kingdom) VALUES (?, ?, ?, ?)",
        (5017, 'Butorides virescens', 'species', 'Animalia'),
    )
    db.conn.commit()
    taxa_id = db.conn.execute(
        "SELECT id FROM taxa WHERE inat_id = 5017"
    ).fetchone()['id']

    kid = db.add_keyword('Green heron')

    class FakeTaxonomy:
        def lookup(self, name):
            if name.lower() == 'green heron':
                return {"taxon_id": 5017, "scientific_name": "Butorides virescens"}
            return None

        def is_taxon(self, name):
            return self.lookup(name) is not None

    db.mark_species_keywords(FakeTaxonomy())

    row = db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row['taxon_id'] == taxa_id


def test_mark_species_keywords_fixes_type_taxonomy_with_is_species_zero(tmp_path):
    """A keyword with type='taxonomy' but is_species=0 gets is_species=1.

    This state is reachable via API-driven type edits or legacy drift.
    Species-only flows filter on is_species=1, so leaving it at 0 while
    type is 'taxonomy' hides the keyword from those flows.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword('Green heron')
    db.conn.execute(
        "UPDATE keywords SET is_species = 0, type = 'taxonomy' WHERE id = ?", (kid,)
    )
    db.conn.commit()

    class FakeTaxonomy:
        def lookup(self, name):
            if name.lower() == 'green heron':
                return {"taxon_id": 5017}
            return None

        def is_taxon(self, name):
            return self.lookup(name) is not None

    updated = db.mark_species_keywords(FakeTaxonomy())
    assert updated == 1
    row = db.conn.execute(
        "SELECT is_species, type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row['is_species'] == 1
    assert row['type'] == 'taxonomy'


def test_mark_species_keywords_backfills_taxon_id_on_existing_taxonomy(tmp_path):
    """Keywords already typed 'taxonomy' but with taxon_id=NULL get linked.

    Covers the Gadwall/Black-crowned-night-heron case: keywords added via
    the classifier path with is_species=True (which also set
    type='taxonomy') before the local taxa table was populated. A later
    taxonomy download should attach taxon_id to those existing rows.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute(
        "INSERT INTO taxa (inat_id, name, rank, kingdom) VALUES (?, ?, ?, ?)",
        (6924, 'Mareca strepera', 'species', 'Animalia'),
    )
    db.conn.commit()
    taxa_id = db.conn.execute(
        "SELECT id FROM taxa WHERE inat_id = 6924"
    ).fetchone()['id']

    # Simulate classifier-added keyword: type='taxonomy' but taxon_id=NULL.
    kid = db.add_keyword('Gadwall', is_species=True)
    row = db.conn.execute(
        "SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row['type'] == 'taxonomy'
    assert row['taxon_id'] is None

    class FakeTaxonomy:
        def lookup(self, name):
            if name.lower() == 'gadwall':
                return {"taxon_id": 6924}
            return None

        def is_taxon(self, name):
            return self.lookup(name) is not None

    updated = db.mark_species_keywords(FakeTaxonomy())
    assert updated == 1

    row = db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row['taxon_id'] == taxa_id


def test_mark_species_keywords_retypes_when_is_species_already_set(tmp_path):
    """A keyword with is_species=1 but type!='taxonomy' still gets retyped.

    Defends against data-drift bugs where the two columns got out of sync.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword('Green heron')
    # Simulate the pre-fix state: is_species set by a prior (now-removed)
    # backfill pass that didn't update `type`.
    db.conn.execute(
        "UPDATE keywords SET is_species = 1, type = 'general' WHERE id = ?", (kid,)
    )
    db.conn.commit()

    class FakeTaxonomy:
        def lookup(self, name):
            if name.lower() == 'green heron':
                return {"taxon_id": 5017}
            return None

        def is_taxon(self, name):
            return self.lookup(name) is not None

    updated = db.mark_species_keywords(FakeTaxonomy())
    assert updated == 1
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row['type'] == 'taxonomy'


def test_default_collections_created(tmp_path):
    """create_default_collections creates default collections."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    db.create_default_collections()

    colls = db.get_collections()
    names = {c['name'] for c in colls}
    assert 'All Photos' in names
    assert 'Needs Identification' in names
    assert 'Untagged' in names
    assert 'Flagged' in names
    assert 'Recent Import' in names


def test_default_collections_idempotent(tmp_path):
    """create_default_collections doesn't duplicate if called twice."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    db.create_default_collections()
    db.create_default_collections()

    colls = db.get_collections()
    assert len(colls) == 5


def test_default_collections_adds_missing(tmp_path):
    """create_default_collections adds new defaults alongside existing collections."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    # Create just one collection manually
    db.add_collection('Flagged', json.dumps([{"field": "flag", "op": "equals", "value": "flagged"}]))
    db.create_default_collections()

    colls = db.get_collections()
    names = {c['name'] for c in colls}
    assert 'All Photos' in names
    assert 'Needs Identification' in names
    assert 'Untagged' in names
    assert 'Recent Import' in names
    assert len(colls) == 5  # no duplicate Flagged


def test_default_collection_uses_has_subject_for_new_workspaces(tmp_path):
    """A newly-created workspace gets 'Needs Identification' (not 'Needs
    Classification') with the has_subject==0 rule."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    db.create_default_collections()

    cols = {c["name"]: json.loads(c["rules"]) for c in db.get_collections()}
    assert "Needs Identification" in cols
    assert "Needs Classification" not in cols
    assert cols["Needs Identification"] == [
        {"field": "has_subject", "op": "equals", "value": 0}
    ]


def test_existing_needs_classification_collection_migrated_idempotently(tmp_path):
    """A workspace pre-populated with the legacy default gets renamed; running
    the migration again is a no-op."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    # Force-create the legacy state.
    db.add_collection(
        "Needs Classification",
        json.dumps([{"field": "has_species", "op": "equals", "value": 0}]),
    )
    db.migrate_default_subject_collection()
    db.migrate_default_subject_collection()  # idempotent

    cols = {c["name"]: json.loads(c["rules"]) for c in db.get_collections()}
    assert "Needs Identification" in cols
    assert "Needs Classification" not in cols
    assert cols["Needs Identification"] == [
        {"field": "has_subject", "op": "equals", "value": 0}
    ]


def test_migration_skips_user_customized_collection(tmp_path):
    """If 'Needs Classification' exists with a non-default rule (the user
    edited it), leave it alone."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    custom = [{"field": "rating", "op": ">=", "value": 3}]
    db.add_collection("Needs Classification", json.dumps(custom))
    db.migrate_default_subject_collection()

    cols = {c["name"]: json.loads(c["rules"]) for c in db.get_collections()}
    assert "Needs Classification" in cols
    assert cols["Needs Classification"] == custom


def test_upgrade_path_no_duplicate_collection(tmp_path):
    """Regression: on an upgraded DB with the legacy 'Needs Classification'
    default, running the startup sequence (migrate-then-seed) leaves a
    single 'Needs Identification' collection — not a duplicate alongside
    the legacy one."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    # Force-create the legacy state that an upgraded DB would have.
    db.add_collection(
        "Needs Classification",
        json.dumps([{"field": "has_species", "op": "equals", "value": 0}]),
    )

    # Mirror the create_app order: migrate first, then seed defaults.
    db.migrate_default_subject_collection()
    db.create_default_collections()

    cols = {c["name"]: json.loads(c["rules"]) for c in db.get_collections()}
    assert "Needs Classification" not in cols
    assert "Needs Identification" in cols
    assert cols["Needs Identification"] == [
        {"field": "has_subject", "op": "equals", "value": 0}
    ]
    # Sanity: total default-collection count is 5, not 6 (no duplicate).
    default_names = {"All Photos", "Needs Identification", "Untagged",
                     "Flagged", "Recent Import"}
    assert default_names.issubset(cols.keys())


def test_default_genre_keywords_inserted(tmp_path):
    """Database init populates the default genre keywords."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    rows = db.conn.execute(
        "SELECT name FROM keywords WHERE type = 'genre' ORDER BY name"
    ).fetchall()
    assert [r["name"] for r in rows] == ["Abstract", "Architecture", "Landscape", "Sunset", "Wildlife"]


def test_default_genre_keywords_idempotent(tmp_path):
    """Calling ensure_default_genre_keywords multiple times doesn't duplicate."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.ensure_default_genre_keywords()
    db.ensure_default_genre_keywords()
    n = db.conn.execute(
        "SELECT COUNT(*) AS n FROM keywords WHERE type = 'genre'"
    ).fetchone()["n"]
    assert n == 5


def test_all_photos_collection_returns_all_photos(tmp_path):
    """The default 'All Photos' collection matches every photo in the workspace."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=2.0)
    db.add_photo(folder_id=fid, filename='c.jpg', extension='.jpg', file_size=100, file_mtime=3.0)

    db.create_default_collections()
    all_photos = next(c for c in db.get_collections() if c['name'] == 'All Photos')

    photos = db.get_collection_photos(all_photos['id'])
    assert {p['filename'] for p in photos} == {'a.jpg', 'b.jpg', 'c.jpg'}
    assert db.count_collection_photos(all_photos['id']) == 3


# --- Helper to set up a workspace with photos ---

def _make_workspace_with_photos(tmp_path, photo_overrides=None):
    """Create a db with a workspace, folder, and photos. Returns (db, photo_ids).

    photo_overrides is a list of dicts with column overrides per photo.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(ws_id, fid)
    overrides = photo_overrides or [{}]
    photo_ids = []
    for i, ov in enumerate(overrides):
        pid = db.add_photo(
            folder_id=fid,
            filename=ov.get('filename', f'IMG_{i:04d}.jpg'),
            extension='.jpg',
            file_size=1000,
            file_mtime=1000.0,
            timestamp=ov.get('timestamp'),
        )
        # Apply column overrides via direct UPDATE
        for col, val in ov.items():
            if col not in ('filename', 'timestamp'):
                db.conn.execute(f"UPDATE photos SET {col} = ? WHERE id = ?", (val, pid))
        db.conn.commit()
        photo_ids.append(pid)
    return db, photo_ids


# --- Cluster 1: Pipeline Feature Counts ---

def test_get_pipeline_feature_counts_empty(tmp_path):
    """Returns zeros when no photos have pipeline features."""
    db, _ = _make_workspace_with_photos(tmp_path, [{}])
    counts = db.get_pipeline_feature_counts()
    assert counts['masks'] == 0
    assert counts['detections'] == 0
    assert counts['sharpness'] == 0


def test_get_pipeline_feature_counts_with_data(tmp_path):
    """Returns correct counts for each pipeline feature."""
    db, pids = _make_workspace_with_photos(tmp_path, [
        {'mask_path': '/mask/1.png', 'subject_tenengrad': 42.0},
        {'mask_path': '/mask/2.png'},
        {},
        {},
    ])
    # Create detections for first two photos (replaces old detection_box column)
    db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 100, "h": 100}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.save_detections(pids[2], [
        {"box": {"x": 10, "y": 10, "w": 50, "h": 50}, "confidence": 0.8, "category": "animal"}
    ], detector_model="MDV6")
    counts = db.get_pipeline_feature_counts()
    assert counts['masks'] == 2
    assert counts['detections'] == 2
    assert counts['sharpness'] == 1


def test_get_pipeline_feature_counts_workspace_scoped(tmp_path):
    """Only counts photos in the active workspace's folders."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws1 = db.ensure_default_workspace()
    db.set_active_workspace(ws1)

    f1 = db.add_folder('/photos1', name='photos1')
    db.add_workspace_folder(ws1, f1)
    db.add_photo(folder_id=f1, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET mask_path = '/m' WHERE filename = 'a.jpg'")
    db.conn.commit()

    # Create second workspace with different folder
    ws2 = db.create_workspace('WS2')
    f2 = db.add_folder('/photos2', name='photos2')
    db.add_workspace_folder(ws2, f2)
    db.set_active_workspace(ws2)
    db.add_photo(folder_id=f2, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)

    # WS2 should have 0 masks
    counts = db.get_pipeline_feature_counts()
    assert counts['masks'] == 0

    # WS1 should have 1 mask
    db.set_active_workspace(ws1)
    counts = db.get_pipeline_feature_counts()
    assert counts['masks'] == 1


# --- Cluster 2: Dashboard Stats ---

def test_get_dashboard_stats_empty(tmp_path):
    """Returns sensible defaults when workspace has photos but no metadata."""
    db, _ = _make_workspace_with_photos(tmp_path, [{}])
    stats = db.get_dashboard_stats()
    assert stats['top_keywords'] == []
    assert stats['photos_by_month'] == []
    assert stats['classified_count'] == 0
    assert stats['detected_count'] == 0


def test_get_dashboard_stats_with_data(tmp_path):
    """Returns correct aggregations across all stat types."""
    db, pids = _make_workspace_with_photos(tmp_path, [
        {'timestamp': '2024-06-15T10:30:00', 'rating': 3, 'flag': 'flagged',
         'quality_score': 0.85},
        {'timestamp': '2024-06-20T14:00:00', 'rating': 5, 'flag': 'none',
         'quality_score': 0.42},
        {'timestamp': '2024-07-01T08:00:00', 'rating': 3, 'flag': 'none'},
    ])

    # Add keywords
    kid = db.add_keyword('Robin', is_species=True)
    db.tag_photo(pids[0], kid)
    db.tag_photo(pids[1], kid)

    # Add a detection and prediction
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Robin', 0.95, 'test')

    stats = db.get_dashboard_stats()

    # top_keywords: Robin with 2 photos. Wildlife is auto-added by the
    # first-species rule, so it also appears with 2 photos.
    by_name = {k['name']: k['photo_count'] for k in stats['top_keywords']}
    assert by_name.get('Robin') == 2
    assert by_name.get('Wildlife') == 2

    # photos_by_month: 2 in 2024-06, 1 in 2024-07
    months = {r['month']: r['count'] for r in stats['photos_by_month']}
    assert months['2024-06'] == 2
    assert months['2024-07'] == 1

    # rating_distribution
    ratings = {r['rating']: r['count'] for r in stats['rating_distribution']}
    assert ratings[3] == 2
    assert ratings[5] == 1

    # classified_count
    assert stats['classified_count'] == 1

    # detected_count (photos with detections)
    assert stats['detected_count'] == 1

    # photos_by_hour
    hours = {r['hour']: r['count'] for r in stats['photos_by_hour']}
    assert hours[10] == 1
    assert hours[14] == 1
    assert hours[8] == 1


# --- Cluster 2b: Coverage Stats ---

def test_get_coverage_stats_empty_workspace(tmp_path):
    """Totals are zero when the workspace has no photos at all."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    stats = db.get_coverage_stats()
    assert stats['total'] == 0
    for key in ('thumbnail', 'phash', 'quality', 'detected', 'classified',
                'gps', 'file_hash', 'working_copy', 'mask', 'rating'):
        assert stats[key] == 0, f"{key} should be 0 on empty workspace"


def test_get_coverage_stats_counts_each_stage(tmp_path):
    """Each pipeline stage is counted independently based on its column."""
    db, pids = _make_workspace_with_photos(tmp_path, [
        {'thumb_path': '/t/1.jpg', 'phash': 'abc', 'quality_score': 0.9,
         'latitude': 10.0, 'longitude': 20.0, 'file_hash': 'h1',
         'working_copy_path': '/wc/1.jpg', 'mask_path': '/m/1.png',
         'subject_tenengrad': 1.5, 'bg_tenengrad': 0.2,
         'eye_x': 0.5, 'burst_id': 'b1',
         'rating': 4, 'exif_data': '{}',
         'timestamp': '2024-01-01T00:00:00'},
        {'thumb_path': '/t/2.jpg', 'phash': 'def',
         'timestamp': '2024-02-01T00:00:00'},
        {},  # Nothing set
    ])
    # Classifier embedding lives in photo_embeddings now, not on photos.
    db.upsert_photo_embedding(pids[0], 'test', b'e')
    # Add a detection + prediction for the first photo only.
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4},
         "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Robin', 0.95, 'test')

    stats = db.get_coverage_stats()
    assert stats['total'] == 3
    assert stats['thumbnail'] == 2
    assert stats['phash'] == 2
    assert stats['quality'] == 1
    assert stats['gps'] == 1
    assert stats['file_hash'] == 1
    assert stats['working_copy'] == 1
    assert stats['mask'] == 1
    assert stats['subject_sharpness'] == 1
    assert stats['bg_sharpness'] == 1
    assert stats['eye'] == 1
    assert stats['label_embedding'] == 1
    assert stats['burst'] == 1
    assert stats['rating'] == 1  # rating > 0
    assert stats['exif'] == 1
    assert stats['timestamp'] == 2
    assert stats['detected'] == 1
    assert stats['classified'] == 1


def test_coverage_stats_scoped_to_active_workspace(tmp_path):
    """Photos and detections in other workspaces must not leak in."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws1 = db.ensure_default_workspace()
    ws2 = db.create_workspace('Other')
    db.set_active_workspace(ws1)
    fid1 = db.add_folder('/ws1', name='ws1')
    db.add_workspace_folder(ws1, fid1)
    db.add_photo(folder_id=fid1, filename='a.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)
    db.set_active_workspace(ws2)
    fid2 = db.add_folder('/ws2', name='ws2')
    db.add_workspace_folder(ws2, fid2)
    db.add_photo(folder_id=fid2, filename='b.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid2, filename='c.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)
    db.set_active_workspace(ws1)
    assert db.get_coverage_stats()['total'] == 1
    db.set_active_workspace(ws2)
    assert db.get_coverage_stats()['total'] == 2


def test_get_folder_coverage_stats_per_folder_totals(tmp_path):
    """Returned rows are one per workspace folder with correct per-folder totals."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid_a = db.add_folder('/A', name='A')
    fid_b = db.add_folder('/B', name='B')
    db.add_workspace_folder(ws_id, fid_a)
    db.add_workspace_folder(ws_id, fid_b)
    pa = db.add_photo(folder_id=fid_a, filename='1.jpg', extension='.jpg',
                      file_size=1, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET thumb_path = '/t/1.jpg', "
                    "phash = 'x' WHERE id = ?", (pa,))
    db.add_photo(folder_id=fid_b, filename='2.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid_b, filename='3.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)
    db.conn.commit()

    folders = db.get_folder_coverage_stats()
    by_path = {f['path']: f for f in folders}
    assert by_path['/A']['total'] == 1
    assert by_path['/A']['thumbnail'] == 1
    assert by_path['/A']['phash'] == 1
    assert by_path['/B']['total'] == 2
    assert by_path['/B']['thumbnail'] == 0
    assert by_path['/B']['phash'] == 0


# --- Cluster 3: Prediction Management ---

def test_get_group_predictions(tmp_path):
    """Returns predictions with photo data for a group."""
    db, pids = _make_workspace_with_photos(tmp_path, [
        {'quality_score': 0.9}, {'quality_score': 0.5},
    ])
    det_ids0 = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    det_ids1 = db.save_detections(pids[1], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.8, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids0[0], species='Robin', confidence=0.95,
                      model='test', group_id='g1')
    db.add_prediction(det_ids1[0], species='Robin', confidence=0.80,
                      model='test', group_id='g1')

    results = db.get_group_predictions('g1')
    assert len(results) == 2
    # Should be ordered by quality_score DESC
    assert results[0]['quality_score'] == 0.9
    assert results[1]['quality_score'] == 0.5
    # Should include photo fields
    assert 'filename' in dict(results[0])


def test_get_group_predictions_includes_alternatives(tmp_path):
    """Each primary row includes per-detection alternatives sorted by confidence."""
    db, pids = _make_workspace_with_photos(tmp_path, [
        {'quality_score': 0.9}, {'quality_score': 0.5},
    ])
    det0 = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    det1 = db.save_detections(pids[1], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.8, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det0[0], species='Robin', confidence=0.95, model='test', group_id='g1')
    db.add_prediction(det0[0], species='Sparrow', confidence=0.30, model='test', status='alternative')
    db.add_prediction(det0[0], species='Wren', confidence=0.10, model='test', status='alternative')
    db.add_prediction(det1[0], species='Robin', confidence=0.80, model='test', group_id='g1')
    db.add_prediction(det1[0], species='Finch', confidence=0.25, model='test', status='alternative')

    results = db.get_group_predictions('g1')
    assert len(results) == 2
    row0 = dict(results[0])
    row1 = dict(results[1])
    # Alternatives attached per detection, sorted desc by confidence
    assert [a['species'] for a in row0['alternatives']] == ['Sparrow', 'Wren']
    assert [a['species'] for a in row1['alternatives']] == ['Finch']
    assert row0['alternatives'][0]['confidence'] == 0.30


def test_get_group_predictions_alternatives_filtered_by_model(tmp_path):
    """Alternatives from a different classifier model must not leak in."""
    db, pids = _make_workspace_with_photos(tmp_path, [{'quality_score': 0.9}])
    det = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det[0], species='Robin', confidence=0.95, model='modelA', group_id='g1')
    db.add_prediction(det[0], species='Sparrow', confidence=0.4, model='modelA', status='alternative')
    # Alternative from a different model on the same detection — must be excluded
    db.add_prediction(det[0], species='Eagle', confidence=0.9, model='modelB', status='alternative')

    results = db.get_group_predictions('g1')
    alts = [a['species'] for a in dict(results[0])['alternatives']]
    assert alts == ['Sparrow']


def test_get_group_predictions_handles_large_group(tmp_path):
    """Very large burst groups must not blow up SQLite's expression depth."""
    size = 1005
    photos = [{'quality_score': 0.5} for _ in range(size)]
    db, pids = _make_workspace_with_photos(tmp_path, photos)
    for pid in pids:
        det = db.save_detections(pid, [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
        ], detector_model="MDV6")
        db.add_prediction(det[0], species='Robin', confidence=0.9, model='test', group_id='g1')

    results = db.get_group_predictions('g1')
    assert len(results) == size
    assert all(dict(r)['alternatives'] == [] for r in results)


def test_get_group_predictions_alternatives_keyed_by_detection_and_model(tmp_path):
    """If the same detection has primaries from multiple models in one group,
    each primary gets only its own model's alternatives."""
    db, pids = _make_workspace_with_photos(tmp_path, [{'quality_score': 0.9}])
    det = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det[0], species='Robin', confidence=0.95, model='modelA', group_id='g1')
    db.add_prediction(det[0], species='Sparrow', confidence=0.4, model='modelA', status='alternative')
    db.add_prediction(det[0], species='Eagle', confidence=0.90, model='modelB', group_id='g1')
    db.add_prediction(det[0], species='Hawk', confidence=0.3, model='modelB', status='alternative')

    results = [dict(r) for r in db.get_group_predictions('g1')]
    by_model = {r['model']: [a['species'] for a in r['alternatives']] for r in results}
    assert by_model == {'modelA': ['Sparrow'], 'modelB': ['Hawk']}


def test_update_predictions_status_by_photo(tmp_path):
    """Updates prediction status for all predictions of a photo."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species='Robin', confidence=0.95, model='test')

    db.update_predictions_status_by_photo(pids[0], 'accepted')

    preds = db.get_predictions(photo_ids=[pids[0]])
    assert preds[0]['status'] == 'accepted'


def test_ungroup_prediction(tmp_path):
    """Removes a prediction from its group."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species='Robin', confidence=0.95,
                      model='test', group_id='g1')
    pred = db.get_predictions(photo_ids=[pids[0]])[0]

    db.ungroup_prediction(pred['id'])

    updated = db.get_predictions(photo_ids=[pids[0]])[0]
    assert updated['group_id'] is None


def test_get_existing_prediction_photo_ids(tmp_path):
    """Returns set of photo_ids that have predictions for a model."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}, {}])
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species='Robin', confidence=0.9, model='bioclip')

    result = db.get_existing_prediction_photo_ids('bioclip')
    assert result == {pids[0]}

    result = db.get_existing_prediction_photo_ids('other-model')
    assert result == set()


def test_get_prediction_for_photo_keyed_by_fingerprint(tmp_path):
    """When labels_fingerprint is given, it must scope the lookup.

    Cache identity is (detection, model, fingerprint, species), so the
    single-photo fetch used by the classify-skip path must honor it;
    otherwise it would return a row from a different label set and
    propagate incorrect species metadata into downstream grouping.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    # Two predictions on the same detection, same model, different fingerprints.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence) VALUES (?, 'bioclip', 'aaa', 'Robin', 0.9)",
        (det_ids[0],),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence) VALUES (?, 'bioclip', 'bbb', 'Sparrow', 0.85)",
        (det_ids[0],),
    )
    db.conn.commit()

    assert db.get_prediction_for_photo(
        pids[0], 'bioclip', labels_fingerprint='aaa',
    )['species'] == 'Robin'
    assert db.get_prediction_for_photo(
        pids[0], 'bioclip', labels_fingerprint='bbb',
    )['species'] == 'Sparrow'
    # Absent fingerprint → no row
    assert db.get_prediction_for_photo(
        pids[0], 'bioclip', labels_fingerprint='ccc',
    ) is None


def test_query_move_rule_matches_has_predictions_honors_threshold(tmp_path):
    """has_predictions must match the UI's read-time threshold view: a
    photo whose only prediction sits on a below-threshold detection
    should NOT count as having predictions.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}, {}])
    # Photo 0: above-threshold detection + prediction → "has predictions" = True
    det_a = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    db.add_prediction(det_a, species='Robin', confidence=0.9, model='bioclip')

    # Photo 1: ONLY a below-threshold detection (default 0.2) with a
    # prediction. Must NOT count as "has predictions".
    det_b = db.save_detections(pids[1], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.05, "category": "animal"}
    ], detector_model="MDV6")[0]
    db.add_prediction(det_b, species='Sparrow', confidence=0.9, model='bioclip')

    has = db.query_move_rule_matches({"has_predictions": True})
    assert has == [pids[0]], (
        f"has_predictions=True must apply detector_confidence floor; "
        f"expected only photo 0, got {has}"
    )
    none = db.query_move_rule_matches({"has_predictions": False})
    assert pids[1] in none, (
        f"Photo with only below-threshold predictions must match "
        f"has_predictions=False; got {none}"
    )


def test_query_move_rule_matches_has_predictions(tmp_path):
    """The has_predictions move-rule criterion must work post-refactor.

    Predictions no longer carry photo_id/workspace_id; the EXISTS subquery
    now routes through detections.photo_id instead. Previously this raised
    `no such column: pr.photo_id` on any preview/apply of a rule using the
    "Has predictions" criterion.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}, {}])
    # Photo 0 has a prediction; photo 1 does not.
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species='Robin', confidence=0.9, model='bioclip')

    hits = db.query_move_rule_matches({"has_predictions": True})
    assert hits == [pids[0]]
    misses = db.query_move_rule_matches({"has_predictions": False})
    assert misses == [pids[1]]


def test_dashboard_stats_classified_count_honors_threshold_and_fingerprint(tmp_path):
    """get_dashboard_stats' classified_count and prediction_status must
    apply the same detector_confidence floor as detected_count (otherwise
    the dashboard shows "3 detected, 7 classified" after raising the
    threshold), and must scope to the most recent labels_fingerprint per
    (detection, classifier_model) so stale-label predictions don't drift
    the dashboard away from the active labeling context.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}, {}])
    # Photo A: high-confidence detection + current-fingerprint prediction.
    det_a = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Robin', 0.9, '2026-04-24')",
        (det_a,),
    )
    # Stale-fingerprint prediction on the SAME detection — must not
    # inflate classified_count or prediction_status beyond the one photo.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Finch', 0.95, '2026-01-01')",
        (det_a,),
    )

    # Photo B: LOW-confidence detection (below default 0.2 threshold) with
    # its own current-fingerprint prediction — must be excluded by the
    # threshold filter.
    det_b = db.save_detections(pids[1], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.05, "category": "animal"}
    ], detector_model="MDV6")[0]
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Sparrow', 0.9, '2026-04-24')",
        (det_b,),
    )
    db.conn.commit()

    stats = db.get_dashboard_stats()
    # detected_count applies threshold → only photo A qualifies.
    assert stats["detected_count"] == 1
    # classified_count must match — NOT 2 (ignored threshold) and NOT 3
    # (also mixed fingerprints on photo A).
    assert stats["classified_count"] == 1, (
        f"classified_count ({stats['classified_count']}) must honor "
        f"detector_confidence + fingerprint; expected 1 like detected_count"
    )
    # prediction_status: one pending row for photo A's current-fingerprint
    # prediction. Stale-fingerprint row and below-threshold row excluded.
    status_counts = {r["status"]: r["count"] for r in stats["prediction_status"]}
    assert status_counts.get("pending", 0) == 1, (
        f"prediction_status must exclude stale fingerprint and below-threshold "
        f"rows; got {status_counts}"
    )


def test_species_clusters_endpoint_filters_to_active_fingerprint(tmp_path, monkeypatch):
    """/api/species/<name>/clusters must not mix stale and current-label
    rows when a detection has been classified under multiple fingerprints.
    """
    import os

    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-2", "name": "bioclip-2",
        "model_str": "hf-hub:imageomics/bioclip-2",
        "model_type": "bioclip", "downloaded": True,
    })
    from app import create_app

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    from db import Database
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder("/p", name="p")
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)
    import numpy as np
    emb = np.zeros(512, dtype=np.float32).tobytes()
    # /clusters joins photo_embeddings on pr.classifier_model, so the cached
    # embedding must be keyed on the same model the predictions below use.
    db.upsert_photo_embedding(pid, "bioclip-2", emb)
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    # Two fingerprints on the same detection with the same species.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Robin', 0.95, '2026-01-01')",
        (det_id,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Robin', 0.80, '2026-04-24')",
        (det_id,),
    )
    db.conn.commit()

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir,
                     api_token="t")
    client = app.test_client()
    resp = client.get("/api/species/Robin/clusters")
    assert resp.status_code == 200
    data = resp.get_json()
    # Photo must appear exactly once (deduped to the current fingerprint),
    # not twice (one per fingerprint row).
    assert data["total_photos"] == 1, (
        f"Expected one photo (deduped to current fingerprint), got "
        f"{data['total_photos']}"
    )


def test_species_clusters_endpoint_filters_to_active_classifier_model(
    tmp_path, monkeypatch,
):
    """/api/species/<name>/clusters must constrain predictions to a single
    classifier model — otherwise a workspace whose detection has predictions
    from two models (e.g. BioCLIP-2 and BioCLIP-3) clusters vectors from
    two different model spaces and shows the same photo twice (one prediction
    row per model).
    """
    import os

    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-3", "name": "bioclip-3",
        "model_str": "hf-hub:imageomics/bioclip-3",
        "model_type": "bioclip", "downloaded": True,
    })
    from app import create_app

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    from db import Database
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder("/p", name="p")
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)
    import numpy as np
    emb_old = np.zeros(512, dtype=np.float32).tobytes()
    emb_new = np.ones(512, dtype=np.float32).tobytes()
    # Same photo has embeddings under both models — exactly the state Phase 1
    # is meant to support — but /clusters must pick one.
    db.upsert_photo_embedding(pid, "bioclip-2", emb_old)
    db.upsert_photo_embedding(pid, "bioclip-3", emb_new)
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    # One prediction row per model on the same detection + species.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Robin', 0.95, '2026-01-01')",
        (det_id,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-3', 'fp-new', 'Robin', 0.80, '2026-04-24')",
        (det_id,),
    )
    db.conn.commit()

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir,
                     api_token="t")
    client = app.test_client()
    resp = client.get("/api/species/Robin/clusters")
    assert resp.status_code == 200
    data = resp.get_json()
    # The photo must appear exactly once (filtered to the active classifier
    # model), not twice (one per classifier model row).
    assert data["total_photos"] == 1, (
        f"Expected one photo (filtered to active classifier model), got "
        f"{data['total_photos']}"
    )


def test_clear_detections_also_clears_detector_runs(tmp_path):
    """clear_detections must wipe the matching detector_runs entry, or a
    failed reclassify (clear, then model init crash) would leave a stale
    "done" run key behind and the next non-reclassify pass would skip
    detection forever, leaving the photo without boxes.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="megadetector-v6")
    db.record_detector_run(pids[0], "megadetector-v6", box_count=1)
    assert pids[0] in db.get_detector_run_photo_ids("megadetector-v6")

    db.clear_detections(pids[0])

    assert pids[0] not in db.get_detector_run_photo_ids("megadetector-v6"), (
        "clear_detections left a stale run key — _detect_subjects would "
        "skip this photo on the next non-reclassify pass."
    )


def test_clear_predictions_scopes_by_fingerprint(tmp_path):
    """When a fingerprint is supplied, clear_predictions must delete only
    that label set's rows and leave predictions under other fingerprints
    untouched. Also wipes the matching classifier_runs so next pass
    re-runs inference.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_id = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="bioclip-2", labels_fingerprint="fp-a")
    db.add_prediction(det_id, species="Sparrow", confidence=0.85,
                      model="bioclip-2", labels_fingerprint="fp-b")
    db.record_classifier_run(det_id, "bioclip-2", "fp-a", prediction_count=1)
    db.record_classifier_run(det_id, "bioclip-2", "fp-b", prediction_count=1)

    db.clear_predictions(model="bioclip-2", labels_fingerprint="fp-a")

    remaining = {r["species"] for r in db.conn.execute(
        "SELECT species FROM predictions WHERE detection_id=?", (det_id,)
    ).fetchall()}
    assert remaining == {"Sparrow"}, "fp-b row must be untouched"

    run_keys = db.get_classifier_run_keys(det_id)
    assert ("bioclip-2", "fp-a") not in run_keys, \
        "fp-a classifier_runs key must be cleared or next pass will skip"
    assert ("bioclip-2", "fp-b") in run_keys, "fp-b run key must be preserved"


def test_clear_predictions_no_model_clears_classifier_runs(tmp_path):
    """clear_predictions() with no model must also wipe classifier_runs for
    affected detections. Otherwise the (detection, model, fingerprint) skip
    gate treats them as already classified and the next non-reclassify pass
    leaves those photos permanently without predictions.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_id = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="bioclip-2", labels_fingerprint="fp-a")
    db.add_prediction(det_id, species="Sparrow", confidence=0.85,
                      model="other-model", labels_fingerprint="fp-b")
    db.record_classifier_run(det_id, "bioclip-2", "fp-a", prediction_count=1)
    db.record_classifier_run(det_id, "other-model", "fp-b", prediction_count=1)

    db.clear_predictions()

    remaining = db.conn.execute(
        "SELECT COUNT(*) FROM predictions WHERE detection_id=?",
        (det_id,),
    ).fetchone()[0]
    assert remaining == 0, "All predictions for the detection must be deleted"

    run_keys = db.get_classifier_run_keys(det_id)
    assert run_keys == set(), (
        "All classifier_runs entries for the detection must be cleared so "
        "the next non-reclassify pass actually re-runs inference"
    )


def test_get_predictions_filters_to_latest_fingerprint(tmp_path):
    """get_predictions() must return only the most recent fingerprint per
    (detection, classifier_model). Mixing stale and current fingerprints
    contaminates /api/predictions and /api/predictions/compare with
    duplicate/conflicting species after a label-set change.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_id = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    # Stale fingerprint with HIGHER confidence than current — confidence-only
    # ranking would surface it; the latest-fingerprint filter must exclude it.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Finch', 0.95, '2026-01-01')",
        (det_id,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Robin', 0.80, '2026-04-24')",
        (det_id,),
    )
    db.conn.commit()

    rows = db.get_predictions(photo_ids=[pids[0]])
    species_seen = {r["species"] for r in rows}
    assert species_seen == {"Robin"}, (
        "get_predictions returned stale-fingerprint species — would mix "
        "old and current label sets in /api/predictions."
    )


def test_move_folders_moves_prediction_review(tmp_path):
    """Moving folders between workspaces must carry prediction_review rows
    with them — otherwise accepted/rejected/group metadata is silently
    dropped and the target workspace reads everything as 'pending'.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_src = db.create_workspace("src")
    ws_tgt = db.create_workspace("tgt")
    fid = db.add_folder("/p", name="p")
    db.add_workspace_folder(ws_src, fid)

    db._active_workspace_id = ws_src
    pid = db.add_photo(fid, "a.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="bioclip-2", labels_fingerprint="fp-x",
                      status="accepted", individual="Ruby")
    pred_id = db.conn.execute(
        "SELECT id FROM predictions WHERE species='Robin'"
    ).fetchone()["id"]
    # Sanity: review row exists in source ws.
    assert db.get_review_status(pred_id, ws_src) == "accepted"

    db.move_folders_to_workspace(ws_src, ws_tgt, [fid])

    # Review row must have followed the folder.
    assert db.get_review_status(pred_id, ws_tgt) == "accepted"
    assert db.get_review_status(pred_id, ws_src) == "pending", \
        "source ws should no longer claim this review row"
    # Individual carried over.
    row = db.conn.execute(
        "SELECT individual FROM prediction_review "
        "WHERE prediction_id=? AND workspace_id=?",
        (pred_id, ws_tgt),
    ).fetchone()
    assert row["individual"] == "Ruby"


def test_get_top_prediction_for_photo_scoped_to_current_fingerprint(tmp_path):
    """get_top_prediction_for_photo must return the highest-confidence row
    from the *current* fingerprint only — a stale fingerprint with higher
    confidence must NOT win. Used by iNat prefill/submit to avoid sending
    a taxon from an old label set.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_id = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    # Stale fingerprint wins by confidence but is from an old label set.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Finch', 0.95, '2026-01-01')",
        (det_id,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Robin', 0.80, '2026-04-24')",
        (det_id,),
    )
    db.conn.commit()

    pred = db.get_top_prediction_for_photo(pids[0])
    assert pred is not None
    assert pred["species"] == "Robin", (
        "Helper returned stale-fingerprint species despite higher "
        "confidence — would prefill iNat with the wrong taxon."
    )


def test_get_top_prediction_for_photo_respects_detector_confidence(tmp_path):
    """get_top_prediction_for_photo must drop predictions whose backing
    detection is below the supplied detector_confidence floor — otherwise
    iNat prefill/submit can use species from detections the UI threshold
    is supposed to hide (e.g. lower threshold to classify, then raise it).
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    # Two detections on the same photo: one above the upcoming threshold
    # with a LOWER-confidence prediction, one below with a HIGHER-
    # confidence prediction. Without the floor, the below-threshold
    # detection's species wins by confidence.
    det_high_conf, det_low_conf = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
        {"box": {"x": 1, "y": 1, "w": 1, "h": 1}, "confidence": 0.05, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_high_conf, species="Robin", confidence=0.4,
                      model="bioclip-2", labels_fingerprint="fp")
    db.add_prediction(det_low_conf, species="Finch", confidence=0.95,
                      model="bioclip-2", labels_fingerprint="fp")

    # Without a floor: confidence-only ranking returns the (now-hidden)
    # below-threshold detection's species.
    pred_unfiltered = db.get_top_prediction_for_photo(pids[0])
    assert pred_unfiltered is not None
    assert pred_unfiltered["species"] == "Finch"

    # With the floor matching a typical UI threshold, only the above-
    # threshold detection is eligible, so we get the visible species.
    pred = db.get_top_prediction_for_photo(
        pids[0], min_detector_confidence=0.2,
    )
    assert pred is not None
    assert pred["species"] == "Robin", (
        "Helper returned a species from a below-threshold detection — "
        "iNat would submit a taxon from a detection the UI hides."
    )

    # If the floor excludes every detection, the helper returns None
    # rather than falling back to a hidden detection.
    assert db.get_top_prediction_for_photo(
        pids[0], min_detector_confidence=0.99,
    ) is None


def test_update_prediction_group_info_scoped_to_fingerprint(tmp_path):
    """Group metadata must land on the primary row for the ACTIVE label
    fingerprint, not whichever fingerprint happens to have higher
    confidence on the same detection.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_id = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]

    # Stale fingerprint has HIGHER confidence than current — so the old
    # (by-confidence-only) SELECT would pick the wrong one.
    db.add_prediction(det_id, species="Finch", confidence=0.95,
                      model="bioclip-2", labels_fingerprint="fp-old")
    pred_old = db.conn.execute(
        "SELECT id FROM predictions WHERE labels_fingerprint='fp-old'"
    ).fetchone()["id"]
    db.add_prediction(det_id, species="Robin", confidence=0.80,
                      model="bioclip-2", labels_fingerprint="fp-new")
    pred_new = db.conn.execute(
        "SELECT id FROM predictions WHERE labels_fingerprint='fp-new'"
    ).fetchone()["id"]

    db.update_prediction_group_info(
        detection_id=det_id, model="bioclip-2",
        group_id="g1", vote_count=2, total_votes=3, individual=None,
        labels_fingerprint="fp-new",
    )

    # Only the fp-new row got the group metadata.
    new_row = db.conn.execute(
        "SELECT group_id FROM prediction_review WHERE prediction_id=?",
        (pred_new,),
    ).fetchone()
    old_row = db.conn.execute(
        "SELECT group_id FROM prediction_review WHERE prediction_id=?",
        (pred_old,),
    ).fetchone()
    assert new_row is not None and new_row["group_id"] == "g1"
    assert old_row is None, "stale-fingerprint row must not receive group metadata"


def test_accept_prediction_sibling_rejection_scoped_to_fingerprint(tmp_path):
    """Accepting a prediction under one labels_fingerprint must not mark
    predictions with OTHER fingerprints on the same detection as rejected.
    Each label set should have independent review state.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    ws = db._active_workspace_id
    det_id = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]

    # Two predictions on the same detection, different fingerprints.
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="bioclip-2", labels_fingerprint="fp-old")
    pred_old = db.conn.execute(
        "SELECT id FROM predictions WHERE labels_fingerprint='fp-old'"
    ).fetchone()["id"]

    db.add_prediction(det_id, species="Blue Jay", confidence=0.85,
                      model="bioclip-2", labels_fingerprint="fp-new")
    pred_new = db.conn.execute(
        "SELECT id FROM predictions WHERE labels_fingerprint='fp-new'"
    ).fetchone()["id"]

    # Accept the fp-new prediction. The fp-old one must remain pending.
    db.accept_prediction(pred_new)

    assert db.get_review_status(pred_new, ws) == "accepted"
    assert db.get_review_status(pred_old, ws) == "pending", \
        "sibling rejection must not cross fingerprints"


def test_get_group_predictions_alternatives_scoped_to_fingerprint(tmp_path):
    """Group alternatives must not mix across fingerprints — a detection
    classified under two label sets should only show the current set's
    alternatives in the group-review UI.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    ws = db._active_workspace_id
    det_id = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]

    # Current fingerprint: primary + alternative.
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="bioclip-2", labels_fingerprint="fp-new",
                      group_id="g1")
    pred_primary = db.conn.execute(
        "SELECT id FROM predictions WHERE species='Robin'"
    ).fetchone()["id"]
    db.add_prediction(det_id, species="Sparrow", confidence=0.3,
                      model="bioclip-2", labels_fingerprint="fp-new",
                      status="alternative")

    # Stale fingerprint: primary + alternative on the SAME detection.
    db.add_prediction(det_id, species="Finch", confidence=0.8,
                      model="bioclip-2", labels_fingerprint="fp-old")
    db.add_prediction(det_id, species="Warbler", confidence=0.2,
                      model="bioclip-2", labels_fingerprint="fp-old",
                      status="alternative")

    group = db.get_group_predictions("g1")
    assert len(group) == 1
    assert group[0]["species"] == "Robin"
    alt_species = {a["species"] for a in group[0]["alternatives"]}
    assert alt_species == {"Sparrow"}, \
        f"expected only current-fingerprint alternative, got {alt_species}"


def test_add_prediction_duplicate_does_not_corrupt_review(tmp_path):
    """When add_prediction is called twice with the same unique key and the
    second call carries review metadata, the upsert into prediction_review
    must target the EXISTING prediction_id — not whatever cur.lastrowid
    happens to hold after the INSERT OR IGNORE is skipped.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}, {}])
    # Two distinct detections so we have two prediction ids to confuse.
    det1 = db.save_detections(pids[0], [
        {"box": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")[0]
    det2 = db.save_detections(pids[1], [
        {"box": {"x": 0.5, "y": 0.5, "w": 0.5, "h": 0.5}, "confidence": 0.85, "category": "animal"}
    ], detector_model="MDV6")[0]

    # First prediction on det1 — no review state, fingerprint="x".
    db.add_prediction(det1, species="Robin", confidence=0.9,
                      model="bioclip-2", labels_fingerprint="x")
    pred1_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id=?", (det1,)
    ).fetchone()["id"]

    # Unrelated prediction on det2 to move cur.lastrowid forward.
    db.add_prediction(det2, species="Sparrow", confidence=0.8,
                      model="bioclip-2", labels_fingerprint="x")

    # Re-add the SAME (det1, model, fp, species) with review metadata.
    # INSERT OR IGNORE should skip; the upsert must target pred1_id, not
    # the most-recent-insert id (which would be det2's prediction).
    db.add_prediction(
        det1, species="Robin", confidence=0.9,
        model="bioclip-2", labels_fingerprint="x",
        status="accepted", individual="Ruby",
    )
    rev_rows = db.conn.execute(
        "SELECT prediction_id, status, individual FROM prediction_review "
        "WHERE status = 'accepted'"
    ).fetchall()
    assert len(rev_rows) == 1
    assert rev_rows[0]["prediction_id"] == pred1_id
    assert rev_rows[0]["individual"] == "Ruby"


def test_get_photos_missing_masks_folder_ids_scoped_to_workspace(tmp_path):
    """The folder_ids branch must enforce workspace scoping so stray folder
    ids from another workspace don't leak photos into the result.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")
    fa = db.add_folder("/a", name="a")
    fb = db.add_folder("/b", name="b")
    db.add_workspace_folder(ws_a, fa)
    db.add_workspace_folder(ws_b, fb)

    db._active_workspace_id = ws_a
    pa = db.add_photo(fa, "x.jpg", extension=".jpg",
                      file_size=100, file_mtime=1.0)
    db._active_workspace_id = ws_b
    pb = db.add_photo(fb, "y.jpg", extension=".jpg",
                      file_size=100, file_mtime=2.0)

    # Both photos have detections.
    db.save_detections(pa, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.save_detections(pb, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")

    # Active ws = A. Even if caller passes B's folder id by mistake,
    # results must not include B's photo.
    db._active_workspace_id = ws_a
    hits = db.get_photos_missing_masks(folder_ids=[fa, fb])
    hit_ids = {h["id"] for h in hits}
    assert pa in hit_ids
    assert pb not in hit_ids, "workspace B photo must not leak into workspace A"


def test_clear_predictions_without_collection_photo_ids(tmp_path):
    """The no-collection branch must bind every workspace_id placeholder it
    uses. A bug where the list of bound params had fewer entries than the
    ?-count in the SQL would raise sqlite3.ProgrammingError.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}, {}])
    det_ids = db.save_detections(
        pids[0],
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )
    db.add_prediction(det_ids[0], species='Robin', confidence=0.9, model='bioclip')

    # Must not raise. Both with and without the model filter.
    db.clear_predictions()
    assert db.get_existing_prediction_photo_ids('bioclip') == set()

    # Restore and try the model branch.
    db.add_prediction(det_ids[0], species='Robin', confidence=0.9, model='bioclip')
    db.clear_predictions(model='bioclip')
    assert db.get_existing_prediction_photo_ids('bioclip') == set()


def test_get_existing_prediction_photo_ids_keyed_by_fingerprint(tmp_path):
    """When a labels_fingerprint is given, the lookup scopes to that fingerprint.

    The cache identity of a prediction is (detection, model, fingerprint,
    species), so the photo-level short-circuit in classify_job must key on
    fingerprint too — otherwise changing the workspace's label set would
    leave stale predictions unprocessed.
    """
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    # Prediction was produced with label set fp="aaa"
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence) VALUES (?, 'bioclip', 'aaa', 'Robin', 0.9)",
        (det_ids[0],),
    )
    db.conn.commit()

    # Same fingerprint → photo is skipped
    assert db.get_existing_prediction_photo_ids(
        'bioclip', labels_fingerprint='aaa',
    ) == {pids[0]}
    # Different fingerprint (label set changed) → photo is NOT skipped
    assert db.get_existing_prediction_photo_ids(
        'bioclip', labels_fingerprint='bbb',
    ) == set()
    # No fingerprint passed → pre-refactor behavior (skip any model match)
    assert db.get_existing_prediction_photo_ids('bioclip') == {pids[0]}


def test_get_prediction_for_photo(tmp_path):
    """Returns species and confidence for a photo's prediction by model."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species='Robin', confidence=0.95, model='bioclip')

    row = db.get_prediction_for_photo(pids[0], 'bioclip')
    assert row['species'] == 'Robin'
    assert row['confidence'] == 0.95

    assert db.get_prediction_for_photo(pids[0], 'other') is None


def test_get_and_upsert_photo_embedding(tmp_path):
    """Stores and retrieves a photo embedding keyed on model."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])

    assert db.get_photo_embedding(pids[0], "BioCLIP") is None

    db.upsert_photo_embedding(pids[0], "BioCLIP", b'\x01\x02\x03\x04')

    result = db.get_photo_embedding(pids[0], "BioCLIP")
    assert result == b'\x01\x02\x03\x04'


def test_upsert_photo_embedding_replaces_same_model(tmp_path):
    """Upsert with the same (model, variant) overwrites the previous blob."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])

    db.upsert_photo_embedding(pids[0], "BioCLIP", b'\x01\x02')
    db.upsert_photo_embedding(pids[0], "BioCLIP", b'\x03\x04')

    assert db.get_photo_embedding(pids[0], "BioCLIP") == b'\x03\x04'


def test_photo_embeddings_per_model_isolation(tmp_path):
    """Two models for the same photo coexist; neither overwrites the other."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])

    db.upsert_photo_embedding(pids[0], "BioCLIP", b'\x01')
    db.upsert_photo_embedding(pids[0], "BioCLIP-2", b'\x02')

    assert db.get_photo_embedding(pids[0], "BioCLIP") == b'\x01'
    assert db.get_photo_embedding(pids[0], "BioCLIP-2") == b'\x02'


def test_photo_embeddings_variant_isolation(tmp_path):
    """Different variants for the same model coexist."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])

    db.upsert_photo_embedding(pids[0], "BioCLIP", b'\xaa', variant='v1')
    db.upsert_photo_embedding(pids[0], "BioCLIP", b'\xbb', variant='v2')

    assert db.get_photo_embedding(pids[0], "BioCLIP", variant='v1') == b'\xaa'
    assert db.get_photo_embedding(pids[0], "BioCLIP", variant='v2') == b'\xbb'


def test_update_prediction_group_info(tmp_path):
    """Updates group info on an existing prediction."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    det_ids = db.save_detections(pids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species='Robin', confidence=0.95, model='bioclip')

    db.update_prediction_group_info(
        detection_id=det_ids[0], model='bioclip',
        group_id='g1', vote_count=3, total_votes=5, individual='[{"species":"Robin"}]',
    )

    pred = db.get_predictions(photo_ids=[pids[0]])[0]
    assert pred['group_id'] == 'g1'
    assert pred['vote_count'] == 3
    assert pred['total_votes'] == 5


def test_is_keyword_species(tmp_path):
    """Checks if a keyword is marked as species."""
    db, _ = _make_workspace_with_photos(tmp_path, [{}])
    kid_species = db.add_keyword('Robin', is_species=True)
    kid_location = db.add_keyword('The Park', is_species=False)

    assert db.is_keyword_species(kid_species) is True
    assert db.is_keyword_species(kid_location) is False


# --- Cluster 5: Calendar Data ---

def _make_calendar_db(tmp_path):
    """Create a db with workspace, folders, and photos suitable for calendar tests."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos/2024', name='2024')
    fid2 = db.add_folder('/photos/2024/January', name='January', parent_id=fid)

    p1 = db.add_photo(folder_id=fid, filename='bird1.jpg', extension='.jpg',
                      file_size=1000, file_mtime=1.0, timestamp='2024-01-15T10:00:00')
    p2 = db.add_photo(folder_id=fid2, filename='bird2.jpg', extension='.jpg',
                      file_size=2000, file_mtime=2.0, timestamp='2024-01-20T14:00:00')
    p3 = db.add_photo(folder_id=fid, filename='bird3.jpg', extension='.jpg',
                      file_size=3000, file_mtime=3.0, timestamp='2024-06-10T09:00:00')

    db.update_photo_rating(p1, 3)
    db.update_photo_rating(p3, 5)

    return db


def test_get_calendar_data_basic(tmp_path):
    """get_calendar_data returns daily counts and year bounds."""
    db = _make_calendar_db(tmp_path)
    data = db.get_calendar_data(year=2024)
    assert data["year"] == 2024
    assert "2024-01-15" in data["days"]
    assert "2024-06-10" in data["days"]
    assert data["days"]["2024-01-15"] == 1
    assert data["min_year"] == 2024
    assert data["max_year"] == 2024


def test_get_calendar_data_filters(tmp_path):
    """get_calendar_data respects folder_id and rating_min filters."""
    db = _make_calendar_db(tmp_path)
    folders = db.get_folder_tree()
    jan = [f for f in folders if f["name"] == "January"][0]
    data = db.get_calendar_data(year=2024, folder_id=jan["id"])
    assert list(data["days"].keys()) == ["2024-01-20"]

    data = db.get_calendar_data(year=2024, rating_min=4)
    assert list(data["days"].keys()) == ["2024-06-10"]


def test_get_calendar_data_empty_year(tmp_path):
    """get_calendar_data returns empty days for a year with no photos."""
    db = _make_calendar_db(tmp_path)
    data = db.get_calendar_data(year=2020)
    assert data["days"] == {}
    assert data["year"] == 2020


def test_get_geolocated_photos_excludes_null_coords(tmp_path):
    """get_geolocated_photos only returns photos with lat/lon."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='geo.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='nogeo.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    # Set GPS on p1 only
    db.conn.execute("UPDATE photos SET latitude=37.77, longitude=-122.42 WHERE id=?", (p1,))
    db.conn.commit()

    results = db.get_geolocated_photos()
    assert len(results) == 1
    assert results[0]['filename'] == 'geo.jpg'
    assert results[0]['latitude'] == 37.77
    assert results[0]['longitude'] == -122.42


def test_get_geolocated_photos_workspace_scoped(tmp_path):
    """get_geolocated_photos respects workspace scoping."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=1.0, longitude=2.0 WHERE id=?", (p1,))
    db.conn.commit()

    # Create a second workspace without this folder
    ws2 = db.create_workspace('Other')
    db.set_active_workspace(ws2)
    results = db.get_geolocated_photos()
    assert len(results) == 0


def test_get_geolocated_photos_filters(tmp_path):
    """get_geolocated_photos applies rating and date filters."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0, timestamp='2024-01-15T10:00:00')
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0, timestamp='2024-06-15T10:00:00')
    db.conn.execute("UPDATE photos SET latitude=1.0, longitude=2.0 WHERE id IN (?,?)", (p1, p2))
    db.conn.commit()
    db.update_photo_rating(p1, 2)
    db.update_photo_rating(p2, 5)

    # Filter by rating
    results = db.get_geolocated_photos(rating_min=4)
    assert len(results) == 1
    assert results[0]['filename'] == 'b.jpg'

    # Filter by date range
    results = db.get_geolocated_photos(date_from='2024-03-01')
    assert len(results) == 1
    assert results[0]['filename'] == 'b.jpg'


def test_get_geolocated_photos_with_species(tmp_path):
    """get_geolocated_photos includes species from accepted predictions."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=?", (p1,))
    db.conn.commit()
    det_ids = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    pred = db.get_predictions(photo_ids=[p1])
    db.accept_prediction(pred[0]['id'])

    results = db.get_geolocated_photos()
    assert len(results) == 1
    assert results[0]['species'] == 'Red-tailed Hawk'


def test_get_geolocated_photos_no_prediction_species_null(tmp_path):
    """get_geolocated_photos returns species=None when no accepted prediction."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='mystery.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=?", (p1,))
    db.conn.commit()

    results = db.get_geolocated_photos()
    assert len(results) == 1
    assert results[0]['species'] is None


def test_get_geolocated_photos_species_filter(tmp_path):
    """get_geolocated_photos filters by species when provided."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='heron.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id IN (?,?)", (p1, p2))
    db.conn.commit()
    det_ids1 = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    det_ids2 = db.save_detections(p2, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids1[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    db.add_prediction(det_ids2[0], 'Great Blue Heron', 0.90, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1, p2])
    for pr in preds:
        db.accept_prediction(pr['id'])

    results = db.get_geolocated_photos(species='Red-tailed Hawk')
    assert len(results) == 1
    assert results[0]['filename'] == 'hawk.jpg'

    results = db.get_geolocated_photos(species='Great Blue Heron')
    assert len(results) == 1
    assert results[0]['filename'] == 'heron.jpg'


def test_get_geolocated_photos_species_filter_multi_species(tmp_path):
    """Filter matches any species tag on the photo, not only the alphabetical first."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    p1 = db.add_photo(folder_id=fid, filename='both.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=?", (p1,))
    db.conn.commit()
    det1 = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.95, "category": "animal"}
    ], detector_model="MDV6")
    det2 = db.save_detections(p1, [
        {"box": {"x": 0.5, "y": 0.5, "w": 0.3, "h": 0.4}, "confidence": 0.60, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det1[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    db.add_prediction(det2[0], "Sparrow", 0.60, 'bioclip')
    for pr in db.get_predictions(photo_ids=[p1]):
        db.accept_prediction(pr['id'])

    # Photo is tagged with both; either filter value must return it,
    # and the species label in the returned row must match the filter.
    rows = db.get_geolocated_photos(species='Red-tailed Hawk')
    assert len(rows) == 1
    assert rows[0]['species'] == 'Red-tailed Hawk'
    rows = db.get_geolocated_photos(species='Sparrow')
    assert len(rows) == 1
    assert rows[0]['species'] == 'Sparrow'
    assert db.get_geolocated_photos(species='Cardinal') == []


def test_get_accepted_species(tmp_path):
    """get_accepted_species returns distinct marker species from geolocated photos."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='heron.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    # Both photos need GPS to appear in species list
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id IN (?,?)", (p1, p2))
    db.conn.commit()
    det_ids1 = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    det_ids2 = db.save_detections(p2, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids1[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    db.add_prediction(det_ids2[0], 'Great Blue Heron', 0.90, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1, p2])
    for pr in preds:
        db.accept_prediction(pr['id'])

    species = db.get_accepted_species()
    assert 'Great Blue Heron' in species
    assert 'Red-tailed Hawk' in species
    assert len(species) == 2


def test_get_accepted_species_excludes_non_geolocated(tmp_path):
    """get_accepted_species excludes species from photos without GPS."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    # p1 has no GPS coordinates
    det_ids = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1])
    # Review status now lives in the workspace-scoped prediction_review
    # table; absent rows are 'pending'.
    db.set_review_status(preds[0]['id'], db._active_workspace_id, 'accepted')

    species = db.get_accepted_species()
    assert species == []


def test_get_accepted_species_excludes_non_accepted(tmp_path):
    """get_accepted_species only includes accepted predictions, not pending."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=?", (p1,))
    db.conn.commit()
    det_ids = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Red-tailed Hawk', 0.95, 'bioclip')

    species = db.get_accepted_species()
    assert species == []


def test_get_accepted_species_multiple_species_per_photo(tmp_path):
    """get_accepted_species returns all distinct species keywords tagged on photos."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=?", (p1,))
    db.conn.commit()
    # Two detections for the same photo, each with different species
    det_ids1 = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.95, "category": "animal"}
    ], detector_model="MDV6")
    det_ids2 = db.save_detections(p1, [
        {"box": {"x": 0.5, "y": 0.5, "w": 0.3, "h": 0.4}, "confidence": 0.60, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids1[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    db.add_prediction(det_ids2[0], 'Cooper\'s Hawk', 0.60, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1])
    for pr in preds:
        db.accept_prediction(pr['id'])

    species = db.get_accepted_species()
    # Both species keywords tagged on the photo appear, alphabetical.
    assert species == ["Cooper's Hawk", 'Red-tailed Hawk']


def test_count_photos_without_gps(tmp_path):
    """count_photos_without_gps counts photos missing GPS coordinates."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='geo.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='nogeo1.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='nogeo2.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE filename='geo.jpg'")
    db.conn.commit()

    assert db.count_photos_without_gps() == 2


def test_taxa_table_exists(tmp_path):
    """The taxa table is created with expected columns."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute(
        "SELECT id, inat_id, name, common_name, rank, parent_id, kingdom "
        "FROM taxa LIMIT 0"
    )


def test_taxa_common_names_table_exists(tmp_path):
    """The taxa_common_names table is created."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute(
        "SELECT taxon_id, name, locale FROM taxa_common_names LIMIT 0"
    )


def test_informal_groups_tables_exist(tmp_path):
    """The informal_groups and informal_group_taxa tables are created."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute("SELECT id, name FROM informal_groups LIMIT 0")
    db.conn.execute(
        "SELECT group_id, taxon_id FROM informal_group_taxa LIMIT 0"
    )


def test_keywords_type_column_exists(tmp_path):
    """Keywords table has type column defaulting to 'general'."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword("test")
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "general"


def test_keywords_location_columns_exist(tmp_path):
    """Keywords table has latitude and longitude columns."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword("Central Park")
    db.conn.execute(
        "UPDATE keywords SET latitude = 40.7829, longitude = -73.9654 WHERE id = ?",
        (kid,),
    )
    db.conn.commit()
    row = db.conn.execute(
        "SELECT latitude, longitude FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert abs(row["latitude"] - 40.7829) < 0.001
    assert abs(row["longitude"] - (-73.9654)) < 0.001


def test_keywords_taxon_id_column_exists(tmp_path):
    """Keywords table has taxon_id column."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Insert a taxon first
    db.conn.execute(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (1, 'Animalia', 'Animals', 'kingdom', 'Animalia')"
    )
    tid = db.conn.execute("SELECT id FROM taxa WHERE inat_id = 1").fetchone()["id"]
    kid = db.add_keyword("Animals")
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
        (tid, kid),
    )
    db.conn.commit()
    row = db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["taxon_id"] == tid


def test_is_species_migrated_to_taxonomy_type(tmp_path):
    """Existing is_species=1 keywords get type='taxonomy' after migration."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword("Cardinal", is_species=True)
    row = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1


def test_add_keyword_individual_type(tmp_path):
    """A keyword can be created with type='individual' via direct SQL update."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword("John Doe")
    db.conn.execute(
        "UPDATE keywords SET type = 'individual' WHERE id = ?", (kid,)
    )
    db.conn.commit()
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "individual"


def test_migrate_legacy_keyword_types_renames_people_descriptive_event(tmp_path):
    """Pre-existing keywords of types 'people', 'descriptive', 'event' get
    migrated to 'individual', 'general', 'general' respectively. Idempotent."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    # Force-create legacy rows via direct SQL (bypassing add_keyword's validation).
    db.conn.execute("INSERT INTO keywords (name, type) VALUES (?, 'people')", ("John",))
    db.conn.execute("INSERT INTO keywords (name, type) VALUES (?, 'descriptive')", ("blurry",))
    db.conn.execute("INSERT INTO keywords (name, type) VALUES (?, 'event')", ("wedding",))
    db.conn.commit()
    db.migrate_legacy_keyword_types()
    db.migrate_legacy_keyword_types()  # idempotent
    rows = db.conn.execute(
        "SELECT name, type FROM keywords WHERE name IN ('John', 'blurry', 'wedding') ORDER BY name"
    ).fetchall()
    types = {r["name"]: r["type"] for r in rows}
    assert types == {"John": "individual", "blurry": "general", "wedding": "general"}


def test_keyword_tree_includes_type(tmp_path):
    """get_keyword_tree returns the type field for each keyword."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    kid = db.add_keyword('Cardinal', is_species=True)
    db.tag_photo(pid, kid)

    tree = db.get_keyword_tree()
    assert len(tree) >= 1
    cardinal = [k for k in tree if k['name'] == 'Cardinal'][0]
    assert cardinal['type'] == 'taxonomy'


def test_photo_keywords_includes_type(tmp_path):
    """get_photo_keywords returns the type field for each keyword."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    # Use a name that isn't pre-seeded as a default genre keyword.
    kid = db.add_keyword('MyTag')
    db.tag_photo(pid, kid)

    keywords = db.get_photo_keywords(pid)
    assert len(keywords) == 1
    assert keywords[0]['type'] == 'general'


def test_photo_embeddings_table_exists(tmp_path):
    """photo_embeddings table is created on init with the expected columns."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(photo_embeddings)")}
    assert {"photo_id", "model", "variant", "embedding", "created_at"} <= cols


def test_photos_embedding_columns_dropped(tmp_path):
    """photos.embedding and photos.embedding_model are no longer present."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(photos)")}
    assert "embedding" not in cols
    assert "embedding_model" not in cols


def test_get_photos_with_embedding_filters_by_model(tmp_path):
    """get_photos_with_embedding returns only workspace photos with matching model."""
    import numpy as np
    db, pids = _make_workspace_with_photos(tmp_path, [{}, {}, {}])
    emb1 = np.random.randn(512).astype(np.float32).tobytes()
    emb2 = np.random.randn(512).astype(np.float32).tobytes()
    db.upsert_photo_embedding(pids[0], "BioCLIP", emb1)
    db.upsert_photo_embedding(pids[1], "BioCLIP-2", emb2)
    # pids[2] has no embedding

    results = db.get_photos_with_embedding("BioCLIP")
    assert len(results) == 1
    assert results[0][0] == pids[0]
    assert results[0][1] == emb1


def test_get_photos_with_embedding_excludes_other_workspaces(tmp_path):
    """Only photos in the active workspace are returned."""
    import numpy as np
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    ws_b = db.create_workspace("Other")
    # add_folder auto-links to the active workspace, so switch before each.
    db.set_active_workspace(ws_a)
    fid_a = db.add_folder('/a', name='a')
    db.set_active_workspace(ws_b)
    fid_b = db.add_folder('/b', name='b')
    pid_a = db.add_photo(folder_id=fid_a, filename='a.jpg', extension='.jpg',
                         file_size=1, file_mtime=1.0)
    pid_b = db.add_photo(folder_id=fid_b, filename='b.jpg', extension='.jpg',
                         file_size=1, file_mtime=1.0)
    emb = np.zeros(8, dtype=np.float32).tobytes()
    db.upsert_photo_embedding(pid_a, "BioCLIP", emb)
    db.upsert_photo_embedding(pid_b, "BioCLIP", emb)

    db.set_active_workspace(ws_a)
    results_a = db.get_photos_with_embedding("BioCLIP")
    assert [r[0] for r in results_a] == [pid_a]

    db.set_active_workspace(ws_b)
    results_b = db.get_photos_with_embedding("BioCLIP")
    assert [r[0] for r in results_b] == [pid_b]


def test_migration_from_legacy_embedding_columns(tmp_path):
    """Legacy photos.(embedding, embedding_model) data migrates into photo_embeddings on open."""
    from db import Database
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    fid = db.add_folder('/photos', name='photos')
    p_with_model = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                                file_size=1, file_mtime=1.0)
    p_no_model = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg',
                              file_size=1, file_mtime=1.0)

    # Simulate a pre-Phase-1 database by re-adding the legacy columns and
    # populating them. Closing+reopening triggers the migration.
    db.conn.execute("ALTER TABLE photos ADD COLUMN embedding BLOB")
    db.conn.execute("ALTER TABLE photos ADD COLUMN embedding_model TEXT")
    db.conn.execute(
        "UPDATE photos SET embedding=?, embedding_model=? WHERE id=?",
        (b'\x01\x02\x03', 'BioCLIP', p_with_model),
    )
    db.conn.execute(
        "UPDATE photos SET embedding=?, embedding_model=NULL WHERE id=?",
        (b'\x04\x05\x06', p_no_model),
    )
    # Clear any embeddings already migrated by Database.__init__.
    db.conn.execute("DELETE FROM photo_embeddings")
    db.conn.commit()
    db.conn.close()

    db2 = Database(db_path)

    cols = {row[1] for row in db2.conn.execute("PRAGMA table_info(photos)")}
    assert "embedding" not in cols
    assert "embedding_model" not in cols

    rows = db2.conn.execute(
        "SELECT photo_id, model, embedding FROM photo_embeddings ORDER BY photo_id"
    ).fetchall()
    # Only the row with a non-NULL model is migrated; the other is dropped.
    assert len(rows) == 1
    assert rows[0]["photo_id"] == p_with_model
    assert rows[0]["model"] == "BioCLIP"
    assert rows[0]["embedding"] == b'\x01\x02\x03'


# -- Edit history --


def _make_db_with_photos(tmp_path, n=3):
    """Helper: create a Database with n photos and return (db, photo_ids)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pids = []
    for i in range(n):
        pid = db.add_photo(folder_id=fid, filename=f'IMG_{i:04d}.jpg',
                           extension='.jpg', file_size=1000,
                           file_mtime=1700000000.0 + i)
        pids.append(pid)
    return db, pids


def test_record_edit_single(tmp_path):
    """record_edit stores a single-photo edit with before/after values."""
    db, pids = _make_db_with_photos(tmp_path)
    pid = pids[0]

    db.record_edit(
        action_type='rating',
        description='Set rating to 5',
        new_value='5',
        items=[{'photo_id': pid, 'old_value': '0', 'new_value': '5'}],
    )

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'rating'
    assert history[0]['description'] == 'Set rating to 5'
    assert history[0]['is_batch'] == 0
    assert history[0]['item_count'] == 1


def test_record_edit_batch(tmp_path):
    """record_edit stores a batch edit with multiple items."""
    db, pids = _make_db_with_photos(tmp_path)

    items = [
        {'photo_id': pids[0], 'old_value': '3', 'new_value': '5'},
        {'photo_id': pids[1], 'old_value': '0', 'new_value': '5'},
    ]
    db.record_edit(
        action_type='rating',
        description='Set rating to 5 on 2 photos',
        new_value='5',
        items=items,
        is_batch=True,
    )

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['is_batch'] == 1
    assert history[0]['item_count'] == 2


def test_get_edit_history_order(tmp_path):
    """get_edit_history returns most recent first."""
    db, pids = _make_db_with_photos(tmp_path)
    pid = pids[0]

    db.record_edit('rating', 'First edit', '1',
                   [{'photo_id': pid, 'old_value': '0', 'new_value': '1'}])
    db.record_edit('rating', 'Second edit', '2',
                   [{'photo_id': pid, 'old_value': '1', 'new_value': '2'}])

    history = db.get_edit_history()
    assert history[0]['description'] == 'Second edit'
    assert history[1]['description'] == 'First edit'


def test_get_edit_history_pagination(tmp_path):
    """get_edit_history supports limit and offset."""
    db, pids = _make_db_with_photos(tmp_path)
    pid = pids[0]

    for i in range(5):
        db.record_edit('rating', f'Edit {i}', str(i),
                       [{'photo_id': pid, 'old_value': str(i), 'new_value': str(i+1)}])

    page1 = db.get_edit_history(limit=2, offset=0)
    page2 = db.get_edit_history(limit=2, offset=2)
    assert len(page1) == 2
    assert len(page2) == 2
    assert page1[0]['description'] == 'Edit 4'
    assert page2[0]['description'] == 'Edit 2'


def test_undo_last_edit_rating(tmp_path):
    """undo_last_edit restores photo rating and removes the history entry."""
    db, pids = _make_db_with_photos(tmp_path)
    pid = pids[0]
    original_rating = db.get_photo(pid)['rating']

    db.update_photo_rating(pid, 5)
    db.record_edit('rating', 'Set rating to 5', '5',
                   [{'photo_id': pid, 'old_value': str(original_rating), 'new_value': '5'}])

    result = db.undo_last_edit()
    assert result is not None
    assert result['description'] == 'Set rating to 5'
    assert db.get_photo(pid)['rating'] == original_rating
    assert len(db.get_edit_history()) == 0


def test_undo_last_edit_flag(tmp_path):
    """undo_last_edit restores photo flag."""
    db, pids = _make_db_with_photos(tmp_path)
    pid = pids[0]

    db.update_photo_flag(pid, 'flagged')
    db.record_edit('flag', 'Set flag to flagged', 'flagged',
                   [{'photo_id': pid, 'old_value': 'none', 'new_value': 'flagged'}])

    result = db.undo_last_edit()
    assert db.get_photo(pid)['flag'] == 'none'


def test_undo_last_edit_keyword_add(tmp_path):
    """undo_last_edit removes keyword that was added."""
    db, pids = _make_db_with_photos(tmp_path)
    pid = pids[0]
    kid = db.add_keyword('Eagle')
    db.tag_photo(pid, kid)
    db.record_edit('keyword_add', 'Added keyword "Eagle"', str(kid),
                   [{'photo_id': pid, 'old_value': '', 'new_value': str(kid)}])

    db.undo_last_edit()
    keywords = db.get_photo_keywords(pid)
    assert not any(k['name'] == 'Eagle' for k in keywords)


def test_undo_last_edit_keyword_remove(tmp_path):
    """undo_last_edit re-adds keyword that was removed."""
    db, pids = _make_db_with_photos(tmp_path)
    pid = pids[0]
    kid = db.add_keyword('Hawk')
    db.tag_photo(pid, kid)
    # Now remove it and record the edit
    db.untag_photo(pid, kid)
    db.record_edit('keyword_remove', 'Removed keyword "Hawk"', str(kid),
                   [{'photo_id': pid, 'old_value': str(kid), 'new_value': ''}])

    db.undo_last_edit()
    keywords = db.get_photo_keywords(pid)
    assert any(k['id'] == kid for k in keywords)


def test_undo_last_edit_batch(tmp_path):
    """undo_last_edit restores all photos in a batch operation."""
    db, pids = _make_db_with_photos(tmp_path)
    original_ratings = {}
    for pid in pids[:2]:
        original_ratings[pid] = db.get_photo(pid)['rating']

    items = []
    for pid, old_r in original_ratings.items():
        db.update_photo_rating(pid, 5)
        items.append({'photo_id': pid, 'old_value': str(old_r), 'new_value': '5'})
    db.record_edit('rating', 'Set rating to 5 on 2 photos', '5', items, is_batch=True)

    db.undo_last_edit()
    for pid, old_r in original_ratings.items():
        assert db.get_photo(pid)['rating'] == old_r


def test_undo_last_edit_empty(tmp_path):
    """undo_last_edit returns None when no history exists."""
    db, pids = _make_db_with_photos(tmp_path)
    assert db.undo_last_edit() is None


def test_photos_has_file_hash_and_companion_path(tmp_path):
    """Photos table has file_hash and companion_path columns after migration."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    row = db.conn.execute(
        "SELECT file_hash, companion_path FROM photos LIMIT 0"
    ).description
    col_names = [r[0] for r in row]
    assert "file_hash" in col_names
    assert "companion_path" in col_names


def test_photos_has_eye_focus_columns(tmp_path):
    """Photos table has eye_x, eye_y, eye_conf, eye_tenengrad columns."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(photos)")}
    assert "eye_x" in cols
    assert "eye_y" in cols
    assert "eye_conf" in cols
    assert "eye_tenengrad" in cols


def test_update_photo_eye_fields_roundtrip(tmp_path):
    """update_photo_pipeline_features persists eye_* fields."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid,
        filename="eye.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=1.0,
    )
    db.update_photo_pipeline_features(
        pid,
        eye_x=123.4,
        eye_y=56.7,
        eye_conf=0.82,
        eye_tenengrad=18450.2,
    )
    row = db.conn.execute(
        "SELECT eye_x, eye_y, eye_conf, eye_tenengrad FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert (row[0], row[1], row[2], row[3]) == (123.4, 56.7, 0.82, 18450.2)


def test_update_photo_eye_fields_accept_null(tmp_path):
    """update_photo_pipeline_features accepts explicit None for eye_* fields."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid,
        filename="eye.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=1.0,
    )
    # First set some values
    db.update_photo_pipeline_features(
        pid, eye_x=1.0, eye_y=2.0, eye_conf=0.5, eye_tenengrad=9.0
    )
    # Then clear them
    db.update_photo_pipeline_features(
        pid, eye_x=None, eye_y=None, eye_conf=None, eye_tenengrad=None
    )
    row = db.conn.execute(
        "SELECT eye_x, eye_y, eye_conf, eye_tenengrad FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert (row[0], row[1], row[2], row[3]) == (None, None, None, None)


def test_list_photos_for_eye_keypoint_stage_prefers_routable_prediction(tmp_path):
    """When the top-confidence prediction lacks taxonomy_class and
    scientific_name, a lower-confidence prediction with routable taxonomy
    info must be chosen instead — otherwise ``_resolve_keypoint_model``
    gets a non-routable row and the stage skips the photo.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)

    pid = db.add_photo(
        fid,
        "mammal.jpg",
        ".jpg",
        1000,
        1.0,
        width=800,
        height=600,
    )
    db.update_photo_pipeline_features(pid, mask_path=str(tmp_path / "mask.png"))

    det_ids = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8}, "confidence": 0.95}],
        detector_model="MegaDetector",
    )
    # Top-confidence prediction has species but no taxonomy_class/scientific_name.
    db.add_prediction(
        det_ids[0],
        species="Unknown top",
        confidence=0.99,
        model="bioclip-2.5",
        category="match",
    )
    # Lower-confidence prediction carries full taxonomy.
    db.add_prediction(
        det_ids[0],
        species="Vulpes vulpes",
        confidence=0.55,
        model="bioclip-2.5",
        category="match",
        taxonomy={
            "class": "Mammalia",
            "scientific_name": "Vulpes vulpes",
        },
    )

    rows = db.list_photos_for_eye_keypoint_stage()
    assert len(rows) == 1
    assert rows[0]["taxonomy_class"] == "Mammalia"
    assert rows[0]["scientific_name"] == "Vulpes vulpes"


def test_list_photos_for_eye_keypoint_stage_keeps_confidence_order_when_routable(tmp_path):
    """When multiple predictions carry routable taxonomy info, the
    highest-confidence one wins — the routability preference must not
    override confidence among routable rows.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)

    pid = db.add_photo(
        fid,
        "bird.jpg",
        ".jpg",
        1000,
        1.0,
        width=800,
        height=600,
    )
    db.update_photo_pipeline_features(pid, mask_path=str(tmp_path / "mask.png"))

    det_ids = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8}, "confidence": 0.95}],
        detector_model="MegaDetector",
    )
    db.add_prediction(
        det_ids[0],
        species="Turdus migratorius",
        confidence=0.92,
        model="bioclip-2.5",
        category="match",
        taxonomy={"class": "Aves", "scientific_name": "Turdus migratorius"},
    )
    db.add_prediction(
        det_ids[0],
        species="Corvus corax",
        confidence=0.55,
        model="bioclip-2.5",
        category="match",
        taxonomy={"class": "Aves", "scientific_name": "Corvus corax"},
    )

    rows = db.list_photos_for_eye_keypoint_stage()
    assert len(rows) == 1
    assert rows[0]["scientific_name"] == "Turdus migratorius"


def test_list_photos_for_eye_keypoint_stage_filters_to_active_fingerprint(tmp_path):
    """When a detection has cached predictions from multiple label-set
    fingerprints, the eye-keypoint candidate query must pick from the
    most recent one — otherwise a stale high-confidence row can drive
    `_resolve_keypoint_model` with outdated taxonomy and route the
    stage to the wrong model.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)
    pid = db.add_photo(fid, "a.jpg", ".jpg", 1000, 1.0, width=800, height=600)
    db.update_photo_pipeline_features(pid, mask_path=str(tmp_path / "mask.png"))
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.95}
    ], detector_model="MegaDetector")[0]
    # Stale fingerprint — high confidence + bird taxonomy.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, taxonomy_class, "
        "scientific_name, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Robin', 0.99, 'Aves', "
        "'Turdus migratorius', '2026-01-01')",
        (det_id,),
    )
    # Current fingerprint — lower confidence + mammal taxonomy.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, taxonomy_class, "
        "scientific_name, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Vulpes vulpes', 0.6, 'Mammalia', "
        "'Vulpes vulpes', '2026-04-25')",
        (det_id,),
    )
    db.conn.commit()

    rows = db.list_photos_for_eye_keypoint_stage()
    assert len(rows) == 1
    # Must pick the current-fingerprint row, not the stale higher-confidence one.
    assert rows[0]["taxonomy_class"] == "Mammalia", (
        f"Stale-fingerprint Robin row drove the eye-keypoint stage; "
        f"expected current-fingerprint Vulpes; got {rows[0]['taxonomy_class']}"
    )


def test_list_photos_for_eye_keypoint_stage_scopes_to_photo_ids(tmp_path):
    """When ``photo_ids`` is provided, only those photos are returned even
    if other eligible photos exist in the workspace. Empty iterables return
    no rows without hitting the DB.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)

    pid_a = db.add_photo(
        fid, "a.jpg", ".jpg", 1000, 1.0, width=800, height=600,
    )
    pid_b = db.add_photo(
        fid, "b.jpg", ".jpg", 1000, 2.0, width=800, height=600,
    )
    for pid in (pid_a, pid_b):
        db.update_photo_pipeline_features(pid, mask_path=str(tmp_path / "mask.png"))
        det_ids = db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8}, "confidence": 0.9}],
            detector_model="MegaDetector",
        )
        db.add_prediction(
            det_ids[0], species="Vulpes vulpes", confidence=0.9,
            model="bioclip-2.5", category="match",
            taxonomy={"class": "Mammalia", "scientific_name": "Vulpes vulpes"},
        )

    # No filter: both photos.
    assert {r["id"] for r in db.list_photos_for_eye_keypoint_stage()} == {pid_a, pid_b}
    # Scoped to one photo.
    rows = db.list_photos_for_eye_keypoint_stage(photo_ids=[pid_a])
    assert [r["id"] for r in rows] == [pid_a]
    # Empty iterable short-circuits to [].
    assert db.list_photos_for_eye_keypoint_stage(photo_ids=set()) == []


def test_add_keyword_auto_detects_taxonomy(tmp_path):
    """add_keyword auto-detects taxonomy type when name matches a taxon."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Insert a taxon into the taxa table
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank) VALUES (1, 'Cardinalis cardinalis', 'Northern Cardinal', 'species')"
    )
    db.conn.commit()
    kid = db.add_keyword("Northern Cardinal")
    row = db.conn.execute("SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] == 1


def test_add_keyword_auto_detects_taxonomy_via_scientific_name(tmp_path):
    """add_keyword auto-detects taxonomy type when name matches a scientific name."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank) VALUES (1, 'Cardinalis cardinalis', 'Northern Cardinal', 'species')"
    )
    db.conn.commit()
    kid = db.add_keyword("Cardinalis cardinalis")
    row = db.conn.execute("SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] == 1


def test_add_keyword_auto_detects_taxonomy_via_alt_common_name(tmp_path):
    """add_keyword auto-detects taxonomy type via taxa_common_names table."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank) VALUES (1, 'Cardinalis cardinalis', 'Northern Cardinal', 'species')"
    )
    db.conn.execute(
        "INSERT INTO taxa_common_names (taxon_id, name) VALUES (1, 'Redbird')"
    )
    db.conn.commit()
    kid = db.add_keyword("Redbird")
    row = db.conn.execute("SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] == 1


def test_add_keyword_no_auto_detect_for_general(tmp_path):
    """add_keyword defaults to general when name doesn't match a taxon."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword("favorite")
    row = db.conn.execute("SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["type"] == "general"
    assert row["taxon_id"] is None


def test_database_supports_in_memory_sqlite():
    """Database init succeeds for SQLite's special in-memory path."""
    from db import Database

    db = Database(":memory:")
    row = db.conn.execute(
        "SELECT name FROM workspaces WHERE id = ?",
        (db._active_workspace_id,),
    ).fetchone()

    assert row is not None
    assert row["name"] == "Default"


def test_detections_table_exists(tmp_path):
    """The detections table should exist with expected columns.

    ``workspace_id`` was dropped when detections became global (cached per
    photo, not per workspace); the per-workspace scoping now happens via
    ``workspace_folders`` joins at read time.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    row = db.conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='detections'"
    ).fetchone()
    assert row is not None
    schema = row[0].lower()
    assert "photo_id" in schema
    assert "workspace_id" not in schema
    assert "box_x" in schema
    assert "box_y" in schema
    assert "box_w" in schema
    assert "box_h" in schema
    assert "detector_confidence" in schema
    assert "category" in schema
    assert "detector_model" in schema


def test_predictions_references_detection_id(tmp_path):
    """The predictions table should reference detection_id, not photo_id."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    row = db.conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='predictions'"
    ).fetchone()
    schema = row[0].lower()
    assert "detection_id" in schema
    # photo_id should NOT be a direct column anymore
    # (it's accessed via JOIN through detections)
    assert "photo_id" not in schema


def test_save_detections(tmp_path):
    """save_detections should insert rows and return detection IDs."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(folder_id=fid, filename="elk.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    detections = [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.95, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.6, "w": 0.2, "h": 0.3}, "confidence": 0.80, "category": "animal"},
    ]
    ids = db.save_detections(pid, detections, detector_model="MDV6-yolov9-c")
    assert len(ids) == 2
    rows = db.conn.execute(
        "SELECT * FROM detections WHERE photo_id = ? ORDER BY id",
        (pid,),
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["box_x"] == 0.1
    assert rows[1]["box_x"] == 0.5


def test_save_detections_replaces_existing(tmp_path):
    """Second save for the same (photo, model) wipes prior rows — idempotent."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0)

    det_a = {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    det_b = {"box": {"x": 0.2, "y": 0.2, "w": 0.5, "h": 0.5}, "confidence": 0.7, "category": "animal"}

    # First run: two boxes
    ids_v1 = db.save_detections(photo_id, [det_a, det_b], detector_model="megadetector-v6")
    assert len(ids_v1) == 2

    # Second run on same (photo, model): one box — the old rows should be gone
    ids_v2 = db.save_detections(photo_id, [det_a], detector_model="megadetector-v6")
    rows = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ? AND detector_model = ?",
        (photo_id, "megadetector-v6"),
    ).fetchall()
    assert {r["id"] for r in rows} == set(ids_v2)


def test_save_detections_is_global(tmp_path):
    """Detections written in workspace A are visible when B is active — the table is global."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_a, folder_id)
    db.add_workspace_folder(ws_b, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0)

    db._active_workspace_id = ws_a
    db.save_detections(photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6")

    db._active_workspace_id = ws_b
    rows = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ?", (photo_id,),
    ).fetchall()
    # Global cache: workspace B sees the row written from A
    assert len(rows) == 1


def test_get_detections_threshold_filter(tmp_path, monkeypatch):
    """get_detections filters by min_conf, resolving from workspace-effective config when None."""
    import config as cfg
    from db import Database

    # Isolate config from the user's ~/.vireo/config.json so the default
    # detector_confidence (0.2) is what the test actually resolves.
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    db.save_detections(
        photo_id,
        [
            {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.05, "category": "animal"},
            {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.4, "category": "animal"},
        ],
        detector_model="megadetector-v6",
    )

    # min_conf=0: returns everything
    rows = db.get_detections(photo_id, min_conf=0)
    assert len(rows) == 2

    # min_conf=0.2: only the 0.4 row
    rows = db.get_detections(photo_id, min_conf=0.2)
    assert len(rows) == 1
    assert rows[0]["detector_confidence"] == 0.4

    # min_conf=None pulls from workspace-effective config (default 0.2 → 1 row)
    rows = db.get_detections(photo_id)
    assert len(rows) == 1
    assert rows[0]["detector_confidence"] == 0.4


def test_get_detections_cross_workspace_read(tmp_path):
    """Detections written in workspace A are readable from workspace B — table is global."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_a, folder_id)
    db.add_workspace_folder(ws_b, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )

    db._active_workspace_id = ws_a
    db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )

    db._active_workspace_id = ws_b
    rows = db.get_detections(photo_id, min_conf=0)
    assert len(rows) == 1
    assert rows[0]["detector_confidence"] == 0.9


def test_get_detections_for_photos_threshold_filter(tmp_path, monkeypatch):
    """Batch get_detections_for_photos filters by min_conf across photos and reads cross-workspace."""
    import config as cfg
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/tmp/p")
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_a, fid)
    db.add_workspace_folder(ws_b, fid)
    p1 = db.add_photo(fid, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    p2 = db.add_photo(fid, "b.jpg", extension=".jpg", file_size=100, file_mtime=2.0)

    db._active_workspace_id = ws_a
    db.save_detections(p1, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.05, "category": "animal"},
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.6, "category": "animal"},
    ], detector_model="megadetector-v6")
    db.save_detections(p2, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.1, "category": "animal"},
    ], detector_model="megadetector-v6")

    # min_conf=0: both photos, all three rows
    result = db.get_detections_for_photos([p1, p2], min_conf=0)
    assert len(result[p1]) == 2
    assert len(result[p2]) == 1

    # min_conf=0.5: only p1's 0.6 row; p2 has nothing above threshold → omitted
    result = db.get_detections_for_photos([p1, p2], min_conf=0.5)
    assert set(result.keys()) == {p1}
    assert len(result[p1]) == 1
    assert result[p1][0]["confidence"] == 0.6

    # min_conf=None resolves from workspace-effective config (default 0.2):
    # p1 keeps the 0.6, p2's 0.1 is filtered out.
    result = db.get_detections_for_photos([p1, p2])
    assert set(result.keys()) == {p1}
    assert len(result[p1]) == 1

    # Cross-workspace: read from B, see A's writes.
    db._active_workspace_id = ws_b
    result = db.get_detections_for_photos([p1, p2], min_conf=0)
    assert len(result[p1]) == 2
    assert len(result[p2]) == 1


def test_get_detections_for_photo(tmp_path):
    """get_detections should return all detections for a photo in current workspace."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(folder_id=fid, filename="elk.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    detections = [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.95, "category": "animal"},
    ]
    db.save_detections(pid, detections, detector_model="MDV6")
    result = db.get_detections(pid)
    assert len(result) == 1
    assert result[0]["box_x"] == 0.1
    assert result[0]["detector_model"] == "MDV6"


def test_get_detections_for_photos_batch(tmp_path):
    """get_detections_for_photos should return all detections grouped by photo_id."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    p1 = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg", file_size=100, file_mtime=2.0)
    p3 = db.add_photo(folder_id=fid, filename="c.jpg", extension=".jpg", file_size=100, file_mtime=3.0)
    db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.8, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2}, "confidence": 0.95, "category": "animal"},
    ], detector_model="MDV6")
    db.save_detections(p2, [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.1, "h": 0.1}, "confidence": 0.7, "category": "person"},
    ], detector_model="MDV6")

    result = db.get_detections_for_photos([p1, p2, p3])

    assert set(result.keys()) == {p1, p2}
    assert len(result[p1]) == 2
    assert result[p1][0]["confidence"] == 0.95
    assert result[p1][1]["confidence"] == 0.8
    assert result[p1][0]["x"] == 0.5
    assert result[p1][0]["category"] == "animal"
    assert len(result[p2]) == 1
    assert result[p2][0]["category"] == "person"


def test_get_detections_for_photos_empty(tmp_path):
    """get_detections_for_photos with empty input returns empty dict."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    assert db.get_detections_for_photos([]) == {}


def test_get_detections_for_photos_is_global(tmp_path):
    """get_detections_for_photos reads detections globally — table is no longer workspace-scoped."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    db.add_workspace_folder(db._ws_id(), fid)
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")

    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)

    # Global cache: workspace "Other" sees detections written under the default workspace.
    result = db.get_detections_for_photos([pid], min_conf=0)
    assert len(result[pid]) == 1
    assert result[pid][0]["confidence"] == 0.9


def test_get_predictions_for_detection_filters(tmp_path):
    """get_predictions_for_detection filters by min_classifier_conf and labels_fingerprint."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(photo_id, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]

    for sp, conf, fp in [("Robin", 0.8, "abc"), ("Sparrow", 0.3, "abc"),
                          ("Robin", 0.9, "xyz")]:
        db.conn.execute(
            """INSERT INTO predictions (detection_id, classifier_model,
                                         labels_fingerprint, species, confidence)
               VALUES (?, 'bioclip-2', ?, ?, ?)""",
            (det_id, fp, sp, conf),
        )
    db.conn.commit()

    # All three rows when unfiltered
    assert len(db.get_predictions_for_detection(det_id, min_classifier_conf=0)) == 3
    # Only >= 0.5
    assert len(db.get_predictions_for_detection(det_id, min_classifier_conf=0.5)) == 2
    # Filter by fingerprint
    by_abc = db.get_predictions_for_detection(
        det_id, labels_fingerprint="abc", min_classifier_conf=0
    )
    assert {r["species"] for r in by_abc} == {"Robin", "Sparrow"}


def test_clear_detections(tmp_path):
    """clear_detections should remove detections and cascade to predictions."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(folder_id=fid, filename="elk.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.conn.execute(
        """INSERT INTO predictions
             (detection_id, classifier_model, labels_fingerprint, species, confidence)
           VALUES (?, ?, ?, ?, ?)""",
        (det_ids[0], "bioclip", "legacy", "Elk", 0.9),
    )
    db.conn.commit()
    db.clear_detections(pid)
    assert db.conn.execute("SELECT COUNT(*) FROM detections WHERE photo_id = ?", (pid,)).fetchone()[0] == 0
    assert db.conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0


def test_add_prediction_with_detection(tmp_path):
    """add_prediction should accept detection_id and store prediction."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(folder_id=fid, filename="elk.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Elk", confidence=0.92, model="bioclip")
    preds = db.get_predictions()
    assert len(preds) == 1
    assert preds[0]["species"] == "Elk"
    assert preds[0]["confidence"] == 0.92


def test_add_prediction_rejects_null_detection_id(tmp_path):
    """add_prediction must reject None detection_id to prevent orphans
    that are invisible to workspace-scoped queries."""
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    with pytest.raises(ValueError, match="non-null detection_id"):
        db.add_prediction(None, species="Elk", confidence=0.9, model="bioclip")


def test_get_predictions_includes_photo_and_box(tmp_path):
    """get_predictions should include photo filename and bounding box from detection."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(folder_id=fid, filename="elk.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Elk", confidence=0.9, model="bioclip")
    preds = db.get_predictions()
    assert preds[0]["filename"] == "elk.jpg"
    assert preds[0]["box_x"] == 0.1
    assert preds[0]["photo_id"] == pid


def test_accept_prediction_tags_photo(tmp_path):
    """accept_prediction should add species keyword to the photo."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(folder_id=fid, filename="elk.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Elk", confidence=0.9, model="bioclip")
    preds = db.get_predictions()
    result = db.accept_prediction(preds[0]["id"])
    assert result["species"] == "Elk"
    kws = db.get_photo_keywords(pid)
    assert any(k["name"] == "Elk" for k in kws)


def test_get_existing_detection_photo_ids(tmp_path):
    """get_existing_detection_photo_ids shim returns photo IDs where the default
    detector model has run (delegates to get_detector_run_photo_ids)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid1 = db.add_photo(folder_id=fid, filename="elk.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    pid2 = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    db.save_detections(pid1, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")
    db.record_detector_run(pid1, "megadetector-v6", box_count=1)
    result = db.get_existing_detection_photo_ids()
    assert pid1 in result
    assert pid2 not in result


def test_get_detector_run_photo_ids_excludes_torn_state(tmp_path):
    """A detector_runs row with box_count>0 but no matching detections is a
    torn state left behind by a reclassify that cleared detections and then
    failed before writing new rows. That photo must NOT be treated as cached —
    otherwise the next non-reclassify run skips detection and the photo is
    permanently stranded on full-image fallback.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    torn = db.add_photo(folder_id=fid, filename="torn.jpg", extension=".jpg",
                        file_size=100, file_mtime=1.0)
    empty = db.add_photo(folder_id=fid, filename="empty.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    ok = db.add_photo(folder_id=fid, filename="ok.jpg", extension=".jpg",
                      file_size=100, file_mtime=1.0)

    # Torn: run recorded with boxes, but detections cleared (simulates a
    # reclassify that wiped rows then crashed before re-detecting).
    db.save_detections(torn, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="megadetector-v6")
    db.record_detector_run(torn, "megadetector-v6", box_count=1)
    db.clear_detections(torn)

    # Legit empty scene: run recorded with box_count=0, no detections.
    db.record_detector_run(empty, "megadetector-v6", box_count=0)

    # Consistent: run recorded and detection rows present.
    db.save_detections(ok, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="megadetector-v6")
    db.record_detector_run(ok, "megadetector-v6", box_count=1)

    result = db.get_detector_run_photo_ids("megadetector-v6")
    assert torn not in result, "torn state must be re-detected, not skipped"
    assert empty in result, "legit empty scenes must stay cached"
    assert ok in result, "consistent cached runs must stay cached"


def test_write_detection_batch_is_atomic_under_commit_failure(tmp_path):
    """If commit raises during write_detection_batch, neither detections nor
    detector_runs may hold partial state. Both inserts must commit together
    or not at all — the contract that prevents the torn writes described in
    issue #654 (detections without a matching detector_runs row, or vice
    versa for empty scenes).
    """
    import pytest
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )

    detections = [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
    ]

    # sqlite3.Connection.commit is read-only at the C level, so we wrap the
    # connection with a thin proxy whose commit() raises. Everything else
    # delegates to the real connection so the helper's INSERTs still hit
    # the underlying DB.
    real_conn = db.conn

    class FailingCommitConn:
        def __init__(self, inner):
            self._inner = inner

        def commit(self):
            raise RuntimeError("simulated commit failure")

        def __getattr__(self, name):
            return getattr(self._inner, name)

    db.conn = FailingCommitConn(real_conn)
    try:
        with pytest.raises(RuntimeError, match="simulated commit failure"):
            db.write_detection_batch(
                photo_id, "megadetector-v6", detections,
            )
    finally:
        db.conn = real_conn

    # If the helper did not roll back, an in-progress transaction is still
    # holding the half-written state. Attempting a commit here would expose
    # any leaked rows below.
    import contextlib
    with contextlib.suppress(Exception):
        db.conn.commit()

    det_count = db.conn.execute(
        "SELECT COUNT(*) AS c FROM detections WHERE photo_id = ?",
        (photo_id,),
    ).fetchone()["c"]
    run_count = db.conn.execute(
        "SELECT COUNT(*) AS c FROM detector_runs WHERE photo_id = ?",
        (photo_id,),
    ).fetchone()["c"]

    assert det_count == 0, (
        f"detections must roll back when commit fails; got {det_count} rows"
    )
    assert run_count == 0, (
        f"detector_runs must roll back when commit fails; got {run_count} rows"
    )


def test_write_detection_batch_writes_both_tables_atomically(tmp_path):
    """Successful write_detection_batch persists detection rows + the matching
    detector_runs row, returns the new detection IDs, and box_count reflects
    the number of detections written.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )

    detections = [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.4, "y": 0.4, "w": 0.2, "h": 0.2},
         "confidence": 0.7, "category": "animal"},
    ]
    ids = db.write_detection_batch(photo_id, "megadetector-v6", detections)
    assert len(ids) == 2

    rows = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ? AND detector_model = ?",
        (photo_id, "megadetector-v6"),
    ).fetchall()
    assert {r["id"] for r in rows} == set(ids)

    run = db.conn.execute(
        "SELECT box_count FROM detector_runs WHERE photo_id = ? AND detector_model = ?",
        (photo_id, "megadetector-v6"),
    ).fetchone()
    assert run is not None, "detector_runs row must exist after batch write"
    assert run["box_count"] == 2


def test_write_detection_batch_records_empty_scene(tmp_path):
    """Empty detections must still write a detector_runs row with box_count=0
    so future passes skip the photo as a known empty scene.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )

    ids = db.write_detection_batch(photo_id, "megadetector-v6", [])
    assert ids == []
    run = db.conn.execute(
        "SELECT box_count FROM detector_runs WHERE photo_id = ? AND detector_model = ?",
        (photo_id, "megadetector-v6"),
    ).fetchone()
    assert run is not None
    assert run["box_count"] == 0
    assert photo_id in db.get_detector_run_photo_ids("megadetector-v6")


def test_multiple_predictions_per_detection(tmp_path):
    """Multiple species predictions can be stored for the same detection."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    db.add_workspace_folder(ws_id, fid)
    pid = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="megadetector-v6")
    det_id = det_ids[0]

    db.add_prediction(detection_id=det_id, species="Robin", confidence=0.85,
                      model="test-model", category="new")
    db.add_prediction(detection_id=det_id, species="Sparrow", confidence=0.10,
                      model="test-model", category="new")
    db.add_prediction(detection_id=det_id, species="Finch", confidence=0.05,
                      model="test-model", category="new")

    preds = db.get_predictions()
    assert len(preds) == 3
    species = {p["species"] for p in preds}
    assert species == {"Robin", "Sparrow", "Finch"}


def test_alternative_predictions_filtered_from_pending(tmp_path):
    """get_predictions with status='pending' excludes alternatives."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    db.add_workspace_folder(ws_id, fid)
    pid = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="megadetector-v6")
    det_id = det_ids[0]

    db.add_prediction(detection_id=det_id, species="Robin", confidence=0.85,
                      model="test-model", status="pending")
    db.add_prediction(detection_id=det_id, species="Sparrow", confidence=0.10,
                      model="test-model", status="alternative")
    db.add_prediction(detection_id=det_id, species="Finch", confidence=0.05,
                      model="test-model", status="alternative")

    # All predictions
    all_preds = db.get_predictions()
    assert len(all_preds) == 3

    # Pending only — should return just the top-1
    pending = db.get_predictions(status="pending")
    assert len(pending) == 1
    assert pending[0]["species"] == "Robin"

    # Alternatives only
    alts = db.get_predictions(status="alternative")
    assert len(alts) == 2


# -- Folder health / missing folder tests --


def test_folder_status_column_exists(tmp_path):
    """Folders table has a status column defaulting to 'ok'."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.ensure_default_workspace()
    db.set_active_workspace(1)
    fid = db.add_folder("/photos/test", name="test")
    row = db.conn.execute("SELECT status FROM folders WHERE id = ?", (fid,)).fetchone()
    assert row["status"] == "ok"


def test_check_folder_health_marks_missing(tmp_path):
    """check_folder_health sets status='missing' for non-existent folders."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    real_dir = str(tmp_path / "real")
    os.makedirs(real_dir)
    fid_real = db.add_folder(real_dir, name="real")
    fid_gone = db.add_folder("/nonexistent/folder", name="gone")

    changed = db.check_folder_health()
    assert changed == 1

    row = db.conn.execute("SELECT status FROM folders WHERE id = ?", (fid_gone,)).fetchone()
    assert row["status"] == "missing"
    row = db.conn.execute("SELECT status FROM folders WHERE id = ?", (fid_real,)).fetchone()
    assert row["status"] == "ok"


def test_check_folder_health_recovers(tmp_path):
    """check_folder_health sets status back to 'ok' when folder reappears."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = str(tmp_path / "comeback")
    fid = db.add_folder(folder, name="comeback")
    # Mark missing manually
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid,))
    db.conn.commit()

    # Folder doesn't exist yet -> stays missing
    db.check_folder_health()
    assert db.conn.execute("SELECT status FROM folders WHERE id = ?", (fid,)).fetchone()["status"] == "missing"

    # Create folder -> recovery
    os.makedirs(folder)
    db.check_folder_health()
    assert db.conn.execute("SELECT status FROM folders WHERE id = ?", (fid,)).fetchone()["status"] == "ok"


def test_check_folder_health_preserves_partial_when_path_exists(tmp_path):
    """check_folder_health must NOT overwrite 'partial' with 'ok'.

    Regression: the app runs this health check in a 10-minute background loop.
    If it blindly sets every existing folder to 'ok', a folder flagged
    'partial' by a failed scan gets auto-cleared before the user has a chance
    to rescan, and the UI badge silently disappears. Only a successful rescan
    should clear partial.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = str(tmp_path / "partial_folder")
    os.makedirs(folder)
    fid = db.add_folder(folder, name="partial_folder")
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (fid,))
    db.conn.commit()

    changed = db.check_folder_health()
    assert changed == 0, "partial folder on disk should not change status"
    status = db.conn.execute(
        "SELECT status FROM folders WHERE id = ?", (fid,)
    ).fetchone()["status"]
    assert status == "partial"


def test_check_folder_health_partial_becomes_missing_when_path_gone(tmp_path):
    """A 'partial' folder whose path disappears still flips to 'missing'.

    Rescanning a vanished directory can't recover the data, so the usual
    missing-folder UX (relocate or remove) is more useful than keeping the
    partial badge.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid = db.add_folder("/nope/partial_gone", name="partial_gone")
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (fid,))
    db.conn.commit()

    changed = db.check_folder_health()
    assert changed == 1
    status = db.conn.execute(
        "SELECT status FROM folders WHERE id = ?", (fid,)
    ).fetchone()["status"]
    assert status == "missing"


def test_get_missing_folders(tmp_path):
    """get_missing_folders returns missing folders with photo counts."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid = db.add_folder("/gone/folder", name="gone")
    db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                 file_size=1000, file_mtime=1.0)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid,))
    db.conn.commit()

    missing = db.get_missing_folders()
    assert len(missing) == 1
    assert missing[0]["path"] == "/gone/folder"
    assert missing[0]["photo_count"] == 1


def test_get_missing_folders_scoped_to_active_workspace(tmp_path):
    """Missing folders from other workspaces must not leak into the active one."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    ws_b = db.create_workspace("Other")

    db.set_active_workspace(ws_a)
    fid_a = db.add_folder("/gone/in_a", name="in_a")

    db.set_active_workspace(ws_b)
    fid_b = db.add_folder("/gone/in_b", name="in_b")

    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()

    db.set_active_workspace(ws_a)
    missing = db.get_missing_folders()
    assert [row["path"] for row in missing] == ["/gone/in_a"]

    db.set_active_workspace(ws_b)
    missing = db.get_missing_folders()
    assert [row["path"] for row in missing] == ["/gone/in_b"]


def test_relocate_folder(tmp_path):
    """relocate_folder updates path and sets status to 'ok'."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid = db.add_folder("/old/path", name="photos")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid,))
    db.conn.commit()

    new_path = str(tmp_path / "new_location")
    os.makedirs(new_path)
    db.relocate_folder(fid, new_path)

    row = db.conn.execute("SELECT path, status FROM folders WHERE id = ?", (fid,)).fetchone()
    assert row["path"] == new_path
    assert row["status"] == "ok"


def test_relocate_folder_cascade(tmp_path):
    """relocate_folder returns child folders that also exist at new location."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    parent = db.add_folder("/old/root", name="root")
    child = db.add_folder("/old/root/sub", name="sub", parent_id=parent)
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()

    new_root = str(tmp_path / "new_root")
    os.makedirs(os.path.join(new_root, "sub"))

    cascaded = db.relocate_folder(parent, new_root)
    assert len(cascaded) == 1
    assert cascaded[0]["id"] == child

    row = db.conn.execute("SELECT path, status FROM folders WHERE id = ?", (child,)).fetchone()
    assert row["path"] == os.path.join(new_root, "sub")
    assert row["status"] == "ok"


def test_relocate_folder_merge_into_existing(tmp_path):
    """relocate_folder merges photos into existing folder when paths conflict."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    existing_path = str(tmp_path / "existing")
    os.makedirs(existing_path)

    # Create two folders: one missing, one existing at target path
    fid_missing = db.add_folder("/old/path", name="photos")
    fid_existing = db.add_folder(existing_path, name="existing")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_missing,))
    db.conn.commit()

    # Create photo_a.jpg on disk so it will be found during merge
    (tmp_path / "existing" / "photo_a.jpg").write_bytes(b"\xff\xd8")

    # Add photos to both folders
    # Missing folder: photo_a.jpg (exists on disk), photo_b.jpg (duplicate), photo_d.jpg (NOT on disk)
    pid_a = db.add_photo(fid_missing, "photo_a.jpg", ".jpg", 1000, 1.0)
    pid_b_missing = db.add_photo(fid_missing, "photo_b.jpg", ".jpg", 1000, 1.0)
    pid_d = db.add_photo(fid_missing, "photo_d.jpg", ".jpg", 1000, 1.0)
    # Existing folder has photo_b.jpg (will win) and photo_c.jpg
    pid_b_existing = db.add_photo(fid_existing, "photo_b.jpg", ".jpg", 2000, 2.0)
    pid_c = db.add_photo(fid_existing, "photo_c.jpg", ".jpg", 1000, 1.0)

    cascaded = db.relocate_folder(fid_missing, existing_path)

    # Missing folder should be deleted
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid_missing,)).fetchone() is None

    # photo_a exists on disk at target — should be reassigned to existing folder
    row_a = db.conn.execute("SELECT folder_id FROM photos WHERE id = ?", (pid_a,)).fetchone()
    assert row_a["folder_id"] == fid_existing

    # photo_b from missing folder should be deleted (duplicate in target)
    assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid_b_missing,)).fetchone() is None

    # photo_d does NOT exist on disk at target — should be deleted
    assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid_d,)).fetchone() is None

    # photo_b and photo_c in existing folder should be untouched
    assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid_b_existing,)).fetchone() is not None
    assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid_c,)).fetchone() is not None

    assert cascaded == []


def test_relocate_folder_merge_revalidates_source_path(tmp_path):
    """relocate_folder rejects merge if source path came back on disk."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    source_path = str(tmp_path / "source")
    target_path = str(tmp_path / "target")
    os.makedirs(source_path)
    os.makedirs(target_path)

    fid_source = db.add_folder(source_path, name="source")
    fid_target = db.add_folder(target_path, name="target")
    # Mark source as missing, but the directory still exists on disk (simulating reconnected drive)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_source,))
    db.conn.commit()

    import pytest
    with pytest.raises(ValueError, match="already tracked"):
        db.relocate_folder(fid_source, target_path)

    # Source should be refreshed to ok, not deleted
    row = db.conn.execute("SELECT status FROM folders WHERE id = ?", (fid_source,)).fetchone()
    assert row is not None
    assert row["status"] == "ok"


def test_relocate_folder_rejects_conflict_for_ok_folder(tmp_path):
    """relocate_folder raises ValueError when source folder is not missing."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    dir_a = str(tmp_path / "folder_a")
    dir_b = str(tmp_path / "folder_b")
    os.makedirs(dir_a)
    os.makedirs(dir_b)

    fid_a = db.add_folder(dir_a, name="a")
    fid_b = db.add_folder(dir_b, name="b")
    # folder_a is NOT missing — status is 'ok'

    import pytest
    with pytest.raises(ValueError, match="already tracked"):
        db.relocate_folder(fid_a, dir_b)

    # Both folders should remain unchanged
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid_a,)).fetchone() is not None
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid_b,)).fetchone() is not None


def test_relocate_folder_merge_updates_photo_count(tmp_path):
    """_merge_into_existing recomputes photo_count on the target folder."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    existing_path = str(tmp_path / "existing")
    os.makedirs(existing_path)

    fid_missing = db.add_folder("/old/path", name="missing")
    fid_existing = db.add_folder(existing_path, name="existing")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_missing,))
    db.conn.commit()

    # Create files on disk so they survive the existence check
    (tmp_path / "existing" / "photo_a.jpg").write_bytes(b"\xff\xd8")

    db.add_photo(fid_missing, "photo_a.jpg", ".jpg", 1000, 1.0)
    db.add_photo(fid_existing, "photo_b.jpg", ".jpg", 1000, 1.0)

    db.relocate_folder(fid_missing, existing_path)

    row = db.conn.execute("SELECT photo_count FROM folders WHERE id = ?", (fid_existing,)).fetchone()
    assert row["photo_count"] == 2


def test_relocate_folder_merge_marks_target_ok(tmp_path):
    """_merge_into_existing sets target folder status to ok."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    existing_path = str(tmp_path / "existing")
    os.makedirs(existing_path)

    fid_missing = db.add_folder("/old/path", name="missing")
    fid_existing = db.add_folder(existing_path, name="existing")
    # Mark both as missing
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()

    db.relocate_folder(fid_missing, existing_path)

    row = db.conn.execute("SELECT status FROM folders WHERE id = ?", (fid_existing,)).fetchone()
    assert row["status"] == "ok"


def test_relocate_folder_merge_reparents_children(tmp_path):
    """_merge_into_existing reparents child folders to the target folder."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    existing_path = str(tmp_path / "existing")
    os.makedirs(existing_path)

    fid_missing = db.add_folder("/old/path", name="missing")
    fid_existing = db.add_folder(existing_path, name="existing")
    # Child folder whose parent is the missing folder
    fid_child = db.add_folder("/old/path/sub", name="sub", parent_id=fid_missing)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_missing,))
    db.conn.commit()

    db.relocate_folder(fid_missing, existing_path)

    # Source folder should be gone
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid_missing,)).fetchone() is None

    # Child folder should now point to the target as its parent
    row = db.conn.execute("SELECT parent_id FROM folders WHERE id = ?", (fid_child,)).fetchone()
    assert row["parent_id"] == fid_existing


def test_relocate_folder_merge_preserves_workspace_links(tmp_path):
    """_merge_into_existing transfers workspace visibility to the target folder."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws1 = db.ensure_default_workspace()
    db.set_active_workspace(ws1)

    existing_path = str(tmp_path / "existing")
    os.makedirs(existing_path)

    # Create source folder in ws1
    fid_missing = db.add_folder("/old/path", name="missing")

    # Create a second workspace and add target folder to it (NOT to ws1)
    ws2 = db.conn.execute(
        "INSERT INTO workspaces (name) VALUES (?)", ("Second",)
    ).lastrowid
    db.conn.commit()
    fid_existing = db.conn.execute(
        "INSERT INTO folders (path, name) VALUES (?, ?)", (existing_path, "existing")
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (ws2, fid_existing),
    )
    db.conn.commit()

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_missing,))
    db.conn.commit()

    db.relocate_folder(fid_missing, existing_path)

    # Target folder should now be visible in BOTH workspaces
    ws_links = db.conn.execute(
        "SELECT workspace_id FROM workspace_folders WHERE folder_id = ? ORDER BY workspace_id",
        (fid_existing,),
    ).fetchall()
    ws_ids = {row["workspace_id"] for row in ws_links}
    assert ws1 in ws_ids, "target folder should be visible in source's workspace"
    assert ws2 in ws_ids, "target folder should retain its original workspace"


def test_relocate_folder_cascade_skips_duplicate(tmp_path):
    """relocate_folder skips cascading a child if its target path is already tracked."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    parent = db.add_folder("/old/root", name="root")
    child = db.add_folder("/old/root/sub", name="sub", parent_id=parent)
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()

    new_root = str(tmp_path / "new_root")
    child_target = os.path.join(new_root, "sub")
    os.makedirs(child_target)

    # Add a folder that already occupies the child's target path
    db.add_folder(child_target, name="conflict")

    cascaded = db.relocate_folder(parent, new_root)
    # Child should NOT be in cascaded list since its target is already taken
    assert len(cascaded) == 0

    # Child should remain unchanged
    row = db.conn.execute("SELECT path, status FROM folders WHERE id = ?", (child,)).fetchone()
    assert row["path"] == "/old/root/sub"
    assert row["status"] == "missing"


def test_relocate_folder_cascade_skips_descendants_of_conflict(tmp_path):
    """relocate_folder skips descendants when their ancestor's path conflicts."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    parent = db.add_folder("/old/root", name="root")
    child = db.add_folder("/old/root/sub", name="sub", parent_id=parent)
    grand = db.add_folder("/old/root/sub/grand", name="grand", parent_id=child)
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()

    new_root = str(tmp_path / "new_root")
    child_target = os.path.join(new_root, "sub")
    grand_target = os.path.join(new_root, "sub", "grand")
    os.makedirs(grand_target)  # creates child_target too

    # Add a folder that conflicts with the child's target path
    db.add_folder(child_target, name="conflict")

    cascaded = db.relocate_folder(parent, new_root)
    # Neither child nor grandchild should be cascaded
    assert len(cascaded) == 0

    # Both should remain unchanged
    for fid, expected_path in [(child, "/old/root/sub"), (grand, "/old/root/sub/grand")]:
        row = db.conn.execute("SELECT path, status FROM folders WHERE id = ?", (fid,)).fetchone()
        assert row["path"] == expected_path
        assert row["status"] == "missing"


def test_delete_folder(tmp_path):
    """delete_folder removes folder and its photos from the database."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid = db.add_folder("/delete/me", name="me")
    pid = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0)

    result = db.delete_folder(fid)
    assert result["deleted_photos"] == 1

    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid,)).fetchone() is None
    assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid,)).fetchone() is None


def test_missing_folder_photos_hidden_from_browse(tmp_path):
    """Photos in missing folders don't appear in get_photos or count_photos."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid_ok = db.add_folder("/ok/folder", name="ok")
    fid_gone = db.add_folder("/gone/folder", name="gone")
    db.add_photo(folder_id=fid_ok, filename="visible.jpg", extension=".jpg",
                 file_size=1000, file_mtime=1.0, timestamp="2024-01-01T00:00:00")
    db.add_photo(folder_id=fid_gone, filename="hidden.jpg", extension=".jpg",
                 file_size=1000, file_mtime=1.0, timestamp="2024-01-01T00:00:00")

    # Both visible before marking missing
    assert db.count_photos() == 2
    assert len(db.get_photos()) == 2

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_gone,))
    db.conn.commit()

    # Only one visible after marking missing
    assert db.count_photos() == 1
    photos = db.get_photos()
    assert len(photos) == 1
    assert photos[0]["filename"] == "visible.jpg"


def test_missing_folder_hidden_from_folder_tree(tmp_path):
    """Missing folders don't appear in get_folder_tree."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid_ok = db.add_folder("/ok/folder", name="ok")
    fid_gone = db.add_folder("/gone/folder", name="gone")

    assert len(db.get_folder_tree()) == 2

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_gone,))
    db.conn.commit()

    tree = db.get_folder_tree()
    assert len(tree) == 1
    assert tree[0]["name"] == "ok"


def test_folder_tree_orphan_parent_becomes_root(tmp_path):
    """If a folder's parent_id points to a folder not linked to the active
    workspace, get_folder_tree returns the folder with parent_id=None so the
    browse sidebar renders it at root instead of hiding it under an unreachable
    parent bucket."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    # Parent folder exists in the folders table but is NOT linked to the
    # active workspace (simulated by detaching it after add_folder auto-links).
    fid_parent = db.add_folder("/photos", name="photos")
    db.remove_workspace_folder(ws, fid_parent)

    # Child references the unlinked parent.
    fid_child = db.add_folder("/photos/2024", name="2024", parent_id=fid_parent)

    tree = db.get_folder_tree()
    assert len(tree) == 1
    assert tree[0]["id"] == fid_child
    assert tree[0]["parent_id"] is None


def test_folder_tree_linked_parent_preserved(tmp_path):
    """When the parent is linked to the workspace, parent_id is unchanged."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid_parent = db.add_folder("/photos", name="photos")
    fid_child = db.add_folder("/photos/2024", name="2024", parent_id=fid_parent)

    tree = db.get_folder_tree()
    child = [f for f in tree if f["id"] == fid_child][0]
    assert child["parent_id"] == fid_parent


def test_folder_tree_walks_past_unlinked_ancestor(tmp_path):
    """If a folder's immediate parent is not linked but a grandparent is,
    parent_id is rewritten to the nearest linked ancestor."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid_gp = db.add_folder("/photos", name="photos")
    fid_mid = db.add_folder("/photos/2024", name="2024", parent_id=fid_gp)
    # Detach the middle folder from the workspace so it acts as a gap.
    db.remove_workspace_folder(ws, fid_mid)
    fid_leaf = db.add_folder("/photos/2024/trip", name="trip", parent_id=fid_mid)

    tree = db.get_folder_tree()
    paths = {f["id"]: f["parent_id"] for f in tree}
    assert fid_gp in paths
    assert fid_mid not in paths  # not linked -> not returned
    assert paths[fid_leaf] == fid_gp


def test_folder_tree_walks_past_missing_ancestor(tmp_path):
    """A folder whose parent is linked but has status!='ok' should be walked
    past — the missing ancestor is already filtered from the tree, so the
    child should reparent to the next linked+ok ancestor instead of dangling."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid_gp = db.add_folder("/photos", name="photos")
    fid_mid = db.add_folder("/photos/2024", name="2024", parent_id=fid_gp)
    fid_leaf = db.add_folder("/photos/2024/trip", name="trip", parent_id=fid_mid)

    # Mark the middle folder as missing on disk — it stays linked to the
    # workspace but is filtered out by get_folder_tree's status='ok' clause.
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_mid,))
    db.conn.commit()

    tree = db.get_folder_tree()
    ids = {f["id"] for f in tree}
    assert fid_mid not in ids
    leaf_row = [f for f in tree if f["id"] == fid_leaf][0]
    assert leaf_row["parent_id"] == fid_gp


def test_folder_tree_null_parent_stays_null(tmp_path):
    """A top-level folder (parent_id NULL) stays at root."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid = db.add_folder("/photos", name="photos")
    tree = db.get_folder_tree()
    assert len(tree) == 1
    assert tree[0]["id"] == fid
    assert tree[0]["parent_id"] is None


def test_folder_tree_orphan_resolution_is_workspace_scoped(tmp_path):
    """The parent_id rewrite considers the ACTIVE workspace's links — the
    same folder linked to two workspaces can have different effective parents
    depending on which workspace is active."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")

    # Build hierarchy in workspace A: parent AND child linked.
    db.set_active_workspace(ws_a)
    fid_parent = db.add_folder("/photos", name="photos")
    fid_child = db.add_folder("/photos/2024", name="2024", parent_id=fid_parent)

    # Link only the child into workspace B (not the parent).
    db.add_workspace_folder(ws_b, fid_child)

    # In A: parent is linked -> preserved.
    db.set_active_workspace(ws_a)
    tree_a = db.get_folder_tree()
    child_a = [f for f in tree_a if f["id"] == fid_child][0]
    assert child_a["parent_id"] == fid_parent

    # In B: parent is not linked -> reparented to None.
    db.set_active_workspace(ws_b)
    tree_b = db.get_folder_tree()
    assert len(tree_b) == 1
    assert tree_b[0]["id"] == fid_child
    assert tree_b[0]["parent_id"] is None


def test_missing_folder_hidden_from_collection(tmp_path):
    """Photos in missing folders don't appear in collection queries."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid_ok = db.add_folder("/ok/folder", name="ok")
    fid_gone = db.add_folder("/gone/folder", name="gone")
    p1 = db.add_photo(folder_id=fid_ok, filename="visible.jpg", extension=".jpg",
                      file_size=1000, file_mtime=1.0, timestamp="2024-01-01T00:00:00")
    p2 = db.add_photo(folder_id=fid_gone, filename="hidden.jpg", extension=".jpg",
                      file_size=1000, file_mtime=1.0, timestamp="2024-01-01T00:00:00")

    # Create a collection that matches all photos
    rules = json.dumps([{"field": "photo_ids", "value": [p1, p2]}])
    db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("test", rules, ws),
    )
    db.conn.commit()
    coll_id = db.conn.execute("SELECT id FROM collections WHERE name = 'test'").fetchone()["id"]

    assert db.count_collection_photos(coll_id) == 2

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_gone,))
    db.conn.commit()

    assert db.count_collection_photos(coll_id) == 1
    photos = db.get_collection_photos(coll_id)
    assert len(photos) == 1
    assert photos[0]["filename"] == "visible.jpg"


# -- Move rules --

def test_create_move_rule(db):
    """Creating a move rule stores name, destination, and criteria."""
    rule_id = db.create_move_rule("Archive hawks", "/nas/archive", {"rating_min": 3, "species": ["Red-tailed Hawk"]})
    assert rule_id is not None
    rule = db.get_move_rule(rule_id)
    assert rule["name"] == "Archive hawks"
    assert rule["destination"] == "/nas/archive"
    import json
    assert json.loads(rule["criteria"]) == {"rating_min": 3, "species": ["Red-tailed Hawk"]}


def test_list_move_rules(db):
    """Listing rules returns all saved rules."""
    db.create_move_rule("Rule A", "/dest/a", {})
    db.create_move_rule("Rule B", "/dest/b", {"flag": "flagged"})
    rules = db.list_move_rules()
    assert len(rules) == 2
    names = {r["name"] for r in rules}
    assert names == {"Rule A", "Rule B"}


def test_update_move_rule(db):
    """Updating a rule changes its fields."""
    rid = db.create_move_rule("Old name", "/old", {})
    db.update_move_rule(rid, name="New name", destination="/new", criteria={"rating_min": 5})
    rule = db.get_move_rule(rid)
    assert rule["name"] == "New name"
    assert rule["destination"] == "/new"


def test_delete_move_rule(db):
    """Deleting a rule removes it."""
    rid = db.create_move_rule("Temp", "/tmp", {})
    db.delete_move_rule(rid)
    assert db.get_move_rule(rid) is None


def test_batch_update_photo_folder(db):
    """batch_update_photo_folder moves photos to a new folder in one transaction."""
    fid1 = db.add_folder("/src", name="src")
    fid2 = db.add_folder("/dst", name="dst")
    p1 = db.add_photo(folder_id=fid1, filename="a.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid1, filename="b.jpg", extension=".jpg", file_size=200, file_mtime=2.0)
    db.batch_update_photo_folder([p1, p2], fid2)
    photo1 = db.get_photo(p1)
    photo2 = db.get_photo(p2)
    assert photo1["folder_id"] == fid2
    assert photo2["folder_id"] == fid2


def test_move_folder_path_cascade(db):
    """move_folder_path updates parent and all child folder paths."""
    fid = db.add_folder("/local/2024", name="2024")
    cid = db.add_folder("/local/2024/march", name="march", parent_id=fid)
    gcid = db.add_folder("/local/2024/march/birds", name="birds", parent_id=cid)
    db.move_folder_path(fid, "/nas/photos/2024")
    parent = db.conn.execute("SELECT path FROM folders WHERE id = ?", (fid,)).fetchone()
    child = db.conn.execute("SELECT path FROM folders WHERE id = ?", (cid,)).fetchone()
    grandchild = db.conn.execute("SELECT path FROM folders WHERE id = ?", (gcid,)).fetchone()
    assert parent["path"] == "/nas/photos/2024"
    assert child["path"] == "/nas/photos/2024/march"
    assert grandchild["path"] == "/nas/photos/2024/march/birds"


def test_check_filename_collisions(db):
    """check_filename_collisions detects conflicts at destination folder."""
    fid1 = db.add_folder("/src", name="src")
    fid2 = db.add_folder("/dst", name="dst")
    db.add_photo(folder_id=fid1, filename="bird.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    p_dst = db.add_photo(folder_id=fid2, filename="bird.jpg", extension=".jpg", file_size=200, file_mtime=2.0)
    p_src = db.conn.execute("SELECT id FROM photos WHERE folder_id = ? AND filename = 'bird.jpg'", (fid1,)).fetchone()["id"]
    collisions = db.check_filename_collisions([p_src], fid2)
    assert len(collisions) == 1
    assert collisions[0]["filename"] == "bird.jpg"


# --- Highlights candidates ---


def test_get_highlights_candidates(tmp_path):
    """get_highlights_candidates returns photos with quality scores, species, and embeddings."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/test/folder', name='folder')
    # Insert photos with varying quality scores
    for i, qs in enumerate([0.9, 0.7, 0.5, 0.3, None]):
        pid = db.add_photo(
            folder_id=fid, filename=f'img{i}.jpg', extension='.jpg',
            file_size=1000, file_mtime=1000.0,
        )
        db.conn.execute(
            "UPDATE photos SET quality_score = ? WHERE id = ?", (qs, pid)
        )
        if qs is not None and qs >= 0.5:
            # Add a detection + accepted prediction for photos with decent quality.
            # Detections are global (no workspace_id); the predictions table
            # dropped ``model``/``status`` — review state lives in
            # ``prediction_review``.
            did = db.conn.execute(
                "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
                (pid,),
            ).lastrowid
            pred_id = db.conn.execute(
                "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
                "VALUES (?, 'test', ?, 0.95)",
                (did, f"Species{i}"),
            ).lastrowid
            db.conn.execute(
                "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
                "VALUES (?, ?, 'accepted')",
                (pred_id, db._ws_id()),
            )
    db.conn.commit()

    # min_quality=0.5 should return 3 photos (0.9, 0.7, 0.5), excluding None and 0.3
    results = db.get_highlights_candidates(folder_id=fid, min_quality=0.5)
    assert len(results) == 3
    # Should be ordered by quality_score DESC
    scores = [r["quality_score"] for r in results]
    assert scores == sorted(scores, reverse=True)
    # Each result should have species field (may be None for unclassified)
    assert all("species" in dict(r) for r in results)


def test_get_highlights_candidates_includes_descendants(tmp_path):
    """get_highlights_candidates(folder_id=parent) includes photos from descendant folders."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    root = db.add_folder('/p', name='p')
    child = db.add_folder('/p/c', name='c', parent_id=root)
    p1 = db.add_photo(folder_id=root, filename='top.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=child, filename='deep.jpg', extension='.jpg',
                      file_size=100, file_mtime=2.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id IN (?, ?)", (p1, p2))
    db.conn.commit()

    results = db.get_highlights_candidates(folder_id=root, min_quality=0.0)
    assert len(results) == 2


def test_get_highlights_candidates_skips_missing_descendants(tmp_path):
    """Photos in descendant folders marked 'missing' are excluded (match count_filtered_photos)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    root = db.add_folder('/p', name='p')
    ok_child = db.add_folder('/p/ok', name='ok', parent_id=root)
    missing_child = db.add_folder('/p/gone', name='gone', parent_id=root)
    p_ok = db.add_photo(folder_id=ok_child, filename='a.jpg', extension='.jpg',
                        file_size=100, file_mtime=1.0)
    p_gone = db.add_photo(folder_id=missing_child, filename='b.jpg', extension='.jpg',
                          file_size=100, file_mtime=2.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id IN (?, ?)", (p_ok, p_gone))
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (missing_child,))
    db.conn.commit()

    candidates = db.get_highlights_candidates(folder_id=root, min_quality=0.0)
    assert len(candidates) == 1
    assert candidates[0]["filename"] == 'a.jpg'
    # Consistent with count_filtered_photos
    assert db.count_filtered_photos(folder_id=root) == 1


def test_get_highlights_candidates_excludes_rejected(tmp_path):
    """Flagged-rejected photos are excluded."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/test/folder', name='folder')
    good_pid = db.add_photo(
        folder_id=fid, filename='good.jpg', extension='.jpg',
        file_size=1000, file_mtime=1000.0,
    )
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id = ?", (good_pid,))
    bad_pid = db.add_photo(
        folder_id=fid, filename='bad.jpg', extension='.jpg',
        file_size=1000, file_mtime=1000.0,
    )
    db.conn.execute("UPDATE photos SET quality_score = 0.9, flag = 'rejected' WHERE id = ?", (bad_pid,))
    db.conn.commit()

    results = db.get_highlights_candidates(folder_id=fid, min_quality=0.0)
    assert len(results) == 1
    assert results[0]["filename"] == "good.jpg"


def test_get_highlights_candidates_workspace_wide(tmp_path):
    """folder_id=None pulls candidates from every folder in the active workspace.

    A photoshoot commonly spans multiple dated folders (Vireo auto-organizes
    imports by EXIF capture date). Passing folder_id=None blends all of them.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Two sibling folders, both in the active workspace, each with a scored photo.
    f1 = db.add_folder('/shoot/2024-01-15', name='2024-01-15')
    f2 = db.add_folder('/shoot/2024-01-16', name='2024-01-16')
    p1 = db.add_photo(folder_id=f1, filename='day1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=f2, filename='day2.jpg', extension='.jpg',
                      file_size=100, file_mtime=2.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id IN (?, ?)", (p1, p2))
    db.conn.commit()

    results = db.get_highlights_candidates(folder_id=None, min_quality=0.0)
    filenames = {r["filename"] for r in results}
    assert filenames == {"day1.jpg", "day2.jpg"}


def test_get_highlights_candidates_workspace_wide_isolates_workspaces(tmp_path):
    """folder_id=None must not leak photos from folders in other workspaces."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    active_ws = db._ws_id()
    other_ws = db.create_workspace('Other')
    # Folder in the active workspace.
    f_active = db.add_folder('/active', name='active')
    # Folder only in the other workspace.
    f_other = db.add_folder('/other', name='other')
    db.remove_workspace_folder(active_ws, f_other)
    db.add_workspace_folder(other_ws, f_other)
    p_active = db.add_photo(folder_id=f_active, filename='a.jpg', extension='.jpg',
                            file_size=100, file_mtime=1.0)
    p_other = db.add_photo(folder_id=f_other, filename='b.jpg', extension='.jpg',
                           file_size=100, file_mtime=2.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id IN (?, ?)",
                    (p_active, p_other))
    db.conn.commit()

    results = db.get_highlights_candidates(folder_id=None, min_quality=0.0)
    filenames = {r["filename"] for r in results}
    assert filenames == {"a.jpg"}


def test_get_highlights_candidates_workspace_wide_respects_min_quality_and_rejected(tmp_path):
    """Workspace-wide pool still honors min_quality and excludes rejected photos."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    f1 = db.add_folder('/a', name='a')
    f2 = db.add_folder('/b', name='b')
    # Above threshold in f1; below threshold in f2; rejected in f1.
    p_keep = db.add_photo(folder_id=f1, filename='keep.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    p_low = db.add_photo(folder_id=f2, filename='low.jpg', extension='.jpg',
                         file_size=100, file_mtime=2.0)
    p_reject = db.add_photo(folder_id=f1, filename='reject.jpg', extension='.jpg',
                            file_size=100, file_mtime=3.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id = ?", (p_keep,))
    db.conn.execute("UPDATE photos SET quality_score = 0.2 WHERE id = ?", (p_low,))
    db.conn.execute("UPDATE photos SET quality_score = 0.9, flag = 'rejected' WHERE id = ?",
                    (p_reject,))
    db.conn.commit()

    results = db.get_highlights_candidates(folder_id=None, min_quality=0.5)
    filenames = [r["filename"] for r in results]
    assert filenames == ["keep.jpg"]


# --- Folders with quality data ---


def test_get_folders_with_quality_data(tmp_path):
    """Returns only folders that have photos with quality scores."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Folder with quality data
    fid1 = db.add_folder('/scored', name='scored')
    pid1 = db.add_photo(
        folder_id=fid1, filename='a.jpg', extension='.jpg',
        file_size=1000, file_mtime=1000.0,
    )
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id = ?", (pid1,))
    # Folder without quality data
    fid2 = db.add_folder('/noscores', name='noscores')
    db.add_photo(
        folder_id=fid2, filename='b.jpg', extension='.jpg',
        file_size=1000, file_mtime=1000.0,
    )
    db.conn.commit()

    folders = db.get_folders_with_quality_data()
    assert len(folders) == 1
    assert folders[0]["name"] == "scored"
    assert folders[0]["photo_count"] > 0


def test_get_folders_with_quality_data_rolls_up_subtree(tmp_path):
    """Parent folder's photo_count reflects scored photos in descendant folders."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    root = db.add_folder('/p', name='p')
    child = db.add_folder('/p/c', name='c', parent_id=root)
    # Scored photo lives only in the child
    pid = db.add_photo(folder_id=child, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id = ?", (pid,))
    db.conn.commit()

    folders = db.get_folders_with_quality_data()
    # Both the parent and the child appear; each counts the single scored photo
    by_name = {f["name"]: f for f in folders}
    assert by_name["p"]["photo_count"] == 1
    assert by_name["c"]["photo_count"] == 1


def test_get_folders_with_quality_data_scopes_to_active_workspace(tmp_path):
    """When a descendant belongs to another workspace, its scored photos are excluded."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Parent lives in the active (Default) workspace; child is moved to a second workspace.
    active_ws = db._ws_id()
    other_ws = db.create_workspace('Other')
    root = db.add_folder('/p', name='p')
    child = db.add_folder('/p/c', name='c', parent_id=root)
    pid = db.add_photo(folder_id=child, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id = ?", (pid,))
    # Move the child folder to the other workspace only.
    db.remove_workspace_folder(active_ws, child)
    db.add_workspace_folder(other_ws, child)
    db.conn.commit()

    folders = db.get_folders_with_quality_data()
    by_name = {f["name"]: f for f in folders}
    # Parent should NOT inherit the child's scored photo, since the child is
    # no longer in the active workspace. get_highlights_candidates would
    # return 0 candidates for the parent, so the dropdown count must match.
    assert "p" not in by_name
    assert db.get_highlights_candidates(folder_id=root, min_quality=0.0) == []


def test_get_folders_with_quality_data_stops_at_inactive_ancestor(tmp_path):
    """Rollup cannot propagate across an inactive intermediate folder.

    Tree: A(active) -> B(inactive) -> C(active with scored photo). A must
    NOT show a rolled-up count sourced from C, since get_folder_subtree_ids
    stops at B and get_highlights_candidates(A) returns nothing.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    active_ws = db._ws_id()
    other_ws = db.create_workspace('Other')
    a = db.add_folder('/a', name='a')
    b = db.add_folder('/a/b', name='b', parent_id=a)
    c = db.add_folder('/a/b/c', name='c', parent_id=b)
    pid = db.add_photo(folder_id=c, filename='x.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id = ?", (pid,))
    # Detach B from the active workspace.
    db.remove_workspace_folder(active_ws, b)
    db.add_workspace_folder(other_ws, b)
    db.conn.commit()

    folders = db.get_folders_with_quality_data()
    by_name = {f["name"]: f for f in folders}
    # C still shows up (its own photo counts).
    assert by_name["c"]["photo_count"] == 1
    # A must NOT inherit C's count through the inactive B.
    assert "a" not in by_name
    # Sanity: candidate API agrees.
    assert db.get_highlights_candidates(folder_id=a, min_quality=0.0) == []


def test_get_folders_with_quality_data_skips_missing_descendants(tmp_path):
    """A parent folder does not count photos in descendant folders marked 'missing'."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    root = db.add_folder('/p', name='p')
    ok_child = db.add_folder('/p/ok', name='ok', parent_id=root)
    missing_child = db.add_folder('/p/gone', name='gone', parent_id=root)
    p_ok = db.add_photo(folder_id=ok_child, filename='a.jpg', extension='.jpg',
                        file_size=100, file_mtime=1.0)
    p_gone = db.add_photo(folder_id=missing_child, filename='b.jpg', extension='.jpg',
                          file_size=100, file_mtime=2.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id IN (?, ?)", (p_ok, p_gone))
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (missing_child,))
    db.conn.commit()

    folders = db.get_folders_with_quality_data()
    by_name = {f["name"]: f for f in folders}
    assert by_name["p"]["photo_count"] == 1  # only the ok-child photo
    assert "gone" not in by_name


def test_color_labels_table_exists(tmp_path):
    """photo_color_labels table is created on init."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    tables = {r['name'] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert 'photo_color_labels' in tables


def test_set_color_label(tmp_path):
    """set_color_label stores a color for a photo in the active workspace."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.set_color_label(pid, 'red')
    assert db.get_color_label(pid) == 'red'


def test_set_color_label_replaces(tmp_path):
    """Setting a new color replaces the old one."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.set_color_label(pid, 'red')
    db.set_color_label(pid, 'blue')
    assert db.get_color_label(pid) == 'blue'


def test_remove_color_label(tmp_path):
    """remove_color_label deletes the label."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.set_color_label(pid, 'green')
    db.remove_color_label(pid)
    assert db.get_color_label(pid) is None


def test_color_label_invalid_color(tmp_path):
    """set_color_label rejects invalid colors."""
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    with pytest.raises(ValueError):
        db.set_color_label(pid, 'orange')


def test_color_label_workspace_scoped(tmp_path):
    """Color labels are per-workspace — same photo can have different labels in different workspaces."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)

    # Default workspace
    ws1 = db._active_workspace_id
    db.set_color_label(pid, 'red')

    # Create second workspace and add the folder
    ws2 = db.create_workspace('Second')
    db.set_active_workspace(ws2)
    db.add_workspace_folder(ws2, fid)
    db.set_color_label(pid, 'blue')

    # Verify each workspace has its own label
    assert db.get_color_label(pid) == 'blue'
    db.set_active_workspace(ws1)
    assert db.get_color_label(pid) == 'red'


def test_batch_set_color_label(tmp_path):
    """batch_set_color_label sets label on multiple photos."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.batch_set_color_label([p1, p2], 'yellow')
    assert db.get_color_label(p1) == 'yellow'
    assert db.get_color_label(p2) == 'yellow'


def test_batch_remove_color_label(tmp_path):
    """batch_set_color_label with None removes labels."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.batch_set_color_label([p1, p2], 'yellow')
    db.batch_set_color_label([p1, p2], None)
    assert db.get_color_label(p1) is None
    assert db.get_color_label(p2) is None


def test_get_photos_filter_by_color_label(tmp_path):
    """get_photos can filter by color label."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p3 = db.add_photo(folder_id=fid, filename='c.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.set_color_label(p1, 'red')
    db.set_color_label(p2, 'blue')

    results = db.get_photos(color_label='red')
    assert len(results) == 1
    assert results[0]['filename'] == 'a.jpg'


def test_count_filtered_photos_with_color_label(tmp_path):
    """count_filtered_photos respects color_label filter."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.set_color_label(p1, 'green')

    count = db.count_filtered_photos(color_label='green')
    assert count == 1


def test_get_photos_filter_color_label_combined_with_rating(tmp_path):
    """color_label + rating_min combined filter works (regression: param ordering)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p3 = db.add_photo(folder_id=fid, filename='c.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.update_photo_rating(p1, 4)
    db.update_photo_rating(p2, 4)
    db.update_photo_rating(p3, 2)
    db.set_color_label(p1, 'red')
    db.set_color_label(p3, 'red')

    # Only p1 has both rating >= 4 AND color_label red
    results = db.get_photos(rating_min=4, color_label='red')
    assert len(results) == 1
    assert results[0]['filename'] == 'a.jpg'

    count = db.count_filtered_photos(rating_min=4, color_label='red')
    assert count == 1


def test_collection_color_label_rule(tmp_path):
    """Collections support color_label rules."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.set_color_label(p1, 'red')
    db.set_color_label(p2, 'blue')

    rules = json.dumps([{"field": "color_label", "op": "equals", "value": "red"}])
    cid = db.add_collection("Reds", rules)
    photos = db.get_collection_photos(cid)
    assert len(photos) == 1
    assert photos[0]['filename'] == 'a.jpg'


def test_collection_color_label_not_equals_rule(tmp_path):
    """Collections support color_label 'is not' rule."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p3 = db.add_photo(folder_id=fid, filename='c.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.set_color_label(p1, 'red')
    db.set_color_label(p2, 'blue')

    rules = json.dumps([{"field": "color_label", "op": "is not", "value": "red"}])
    cid = db.add_collection("Not Red", rules)
    photos = db.get_collection_photos(cid)
    filenames = {p['filename'] for p in photos}
    assert 'a.jpg' not in filenames
    assert 'b.jpg' in filenames
    assert 'c.jpg' in filenames


def test_undo_color_label(tmp_path):
    """Undo reverts a color label change."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.set_color_label(pid, 'red')
    db.record_edit('color_label', 'Set color to red', 'red',
                   [{'photo_id': pid, 'old_value': '', 'new_value': 'red'}])

    result = db.undo_last_edit()
    assert result is not None
    assert db.get_color_label(pid) is None


def test_redo_color_label(tmp_path):
    """Redo re-applies a color label change."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.set_color_label(pid, 'red')
    db.record_edit('color_label', 'Set color to red', 'red',
                   [{'photo_id': pid, 'old_value': '', 'new_value': 'red'}])

    db.undo_last_edit()
    assert db.get_color_label(pid) is None

    db.redo_last_undo()
    assert db.get_color_label(pid) == 'red'


def test_dino_embedding_variant_column_exists(tmp_path):
    """The photos table has a dino_embedding_variant column."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute("SELECT dino_embedding_variant FROM photos LIMIT 0")


def test_update_photo_embeddings_stores_variant(tmp_path):
    """update_photo_embeddings persists the DINOv2 variant alongside the blobs."""
    import numpy as np
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    pid = db.add_photo(fid, "a.jpg", ".jpg", 100, 1.0)

    blob = np.ones(1024, dtype=np.float32).tobytes()
    db.update_photo_embeddings(
        pid,
        dino_subject_embedding=blob,
        dino_global_embedding=blob,
        variant="vit-l14",
    )
    row = db.conn.execute(
        "SELECT dino_embedding_variant FROM photos WHERE id = ?", (pid,)
    ).fetchone()
    assert row["dino_embedding_variant"] == "vit-l14"


def test_update_photo_embeddings_rewrite_updates_variant(tmp_path):
    """Re-embedding a photo with a new variant overwrites the stored variant."""
    import numpy as np
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    pid = db.add_photo(fid, "a.jpg", ".jpg", 100, 1.0)

    db.update_photo_embeddings(
        pid,
        dino_subject_embedding=np.ones(768, dtype=np.float32).tobytes(),
        dino_global_embedding=np.ones(768, dtype=np.float32).tobytes(),
        variant="vit-b14",
    )
    db.update_photo_embeddings(
        pid,
        dino_subject_embedding=np.ones(1024, dtype=np.float32).tobytes(),
        dino_global_embedding=np.ones(1024, dtype=np.float32).tobytes(),
        variant="vit-l14",
    )
    row = db.conn.execute(
        "SELECT dino_embedding_variant FROM photos WHERE id = ?", (pid,)
    ).fetchone()
    assert row["dino_embedding_variant"] == "vit-l14"


def test_preview_cache_table_exists(tmp_path):
    """Database creates preview_cache table on init."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    row = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='preview_cache'"
    ).fetchone()
    assert row is not None

    # Verify schema columns
    cols = {r["name"] for r in db.conn.execute("PRAGMA table_info(preview_cache)").fetchall()}
    assert cols == {"photo_id", "size", "bytes", "last_access_at"}

    # Verify index
    idx = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='preview_cache_last_access'"
    ).fetchone()
    assert idx is not None


def test_preview_cache_insert_and_touch(tmp_path):
    """Insert a row, then touch updates last_access_at."""
    import time

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Need a real photo row due to FK
    folder_id = db.add_folder("/tmp/test")
    photo_id = db.add_photo(
        folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0
    )

    t0 = time.time()
    db.preview_cache_insert(photo_id, size=1920, bytes_=12345)

    row = db.conn.execute(
        "SELECT bytes, last_access_at FROM preview_cache WHERE photo_id=? AND size=?",
        (photo_id, 1920),
    ).fetchone()
    assert row["bytes"] == 12345
    assert row["last_access_at"] >= t0

    # Sleep a tiny bit, touch, confirm timestamp advances
    time.sleep(0.05)
    db.preview_cache_touch(photo_id, size=1920)
    row2 = db.conn.execute(
        "SELECT last_access_at FROM preview_cache WHERE photo_id=? AND size=?",
        (photo_id, 1920),
    ).fetchone()
    assert row2["last_access_at"] > row["last_access_at"]


def test_preview_cache_total_bytes(tmp_path):
    """total_bytes sums all rows."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/test")
    p1 = db.add_photo(
        folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0
    )
    p2 = db.add_photo(
        folder_id, "b.jpg", ".jpg", file_size=100, file_mtime=1.0
    )

    assert db.preview_cache_total_bytes() == 0
    db.preview_cache_insert(p1, 1920, 100)
    db.preview_cache_insert(p2, 2560, 200)
    assert db.preview_cache_total_bytes() == 300


def test_preview_cache_delete(tmp_path):
    """Delete removes the row."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/test")
    p1 = db.add_photo(
        folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0
    )
    db.preview_cache_insert(p1, 1920, 100)
    db.preview_cache_delete(p1, 1920)
    assert db.preview_cache_total_bytes() == 0


def test_preview_cache_oldest_first(tmp_path):
    """Iterating in LRU order returns oldest first."""
    import time

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/test")
    p1 = db.add_photo(
        folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0
    )
    p2 = db.add_photo(
        folder_id, "b.jpg", ".jpg", file_size=100, file_mtime=1.0
    )

    db.preview_cache_insert(p1, 1920, 100)
    time.sleep(0.05)
    db.preview_cache_insert(p2, 1920, 200)

    rows = db.preview_cache_oldest_first()
    assert [(r["photo_id"], r["size"]) for r in rows] == [(p1, 1920), (p2, 1920)]


def test_new_cache_tables_exist(tmp_path):
    """detector_runs, classifier_runs, labels_fingerprints, prediction_review are created."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    tables = {r['name'] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert 'detector_runs' in tables
    assert 'classifier_runs' in tables
    assert 'labels_fingerprints' in tables
    assert 'prediction_review' in tables


def test_record_detector_run_and_lookup(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    db._active_workspace_id = db.create_workspace("WS")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )

    # Initially: no runs recorded
    assert db.get_detector_run_photo_ids("megadetector-v6") == set()

    db.record_detector_run(photo_id, "megadetector-v6", box_count=0)
    assert db.get_detector_run_photo_ids("megadetector-v6") == {photo_id}

    # Re-recording is idempotent / updates box_count
    db.record_detector_run(photo_id, "megadetector-v6", box_count=3)
    row = db.conn.execute(
        "SELECT box_count FROM detector_runs WHERE photo_id=? AND detector_model=?",
        (photo_id, "megadetector-v6"),
    ).fetchone()
    assert row["box_count"] == 3


def test_detector_run_is_not_workspace_scoped(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_a, folder_id)
    db.add_workspace_folder(ws_b, folder_id)
    photo_id = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )

    db._active_workspace_id = ws_a
    # Save the matching detection rows alongside the run so the cached-run
    # state is consistent — get_detector_run_photo_ids excludes torn states
    # where box_count>0 has no matching detections.
    db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9,
         "category": "animal"},
        {"box": {"x": 0.4, "y": 0.4, "w": 0.2, "h": 0.2}, "confidence": 0.8,
         "category": "animal"},
    ], detector_model="megadetector-v6")
    db.record_detector_run(photo_id, "megadetector-v6", box_count=2)

    db._active_workspace_id = ws_b
    assert photo_id in db.get_detector_run_photo_ids("megadetector-v6")


def test_record_classifier_run_and_lookup(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    db._active_workspace_id = db.create_workspace("WS")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Need a detection row to reference:
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    det_id = det_ids[0]

    assert db.get_classifier_run_keys(det_id) == set()

    db.record_classifier_run(det_id, "bioclip-2", "abc123", prediction_count=5)
    assert db.get_classifier_run_keys(det_id) == {("bioclip-2", "abc123")}


def test_upsert_labels_fingerprint(tmp_path):
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.upsert_labels_fingerprint(
        fingerprint="abc123",
        display_name="California birds",
        sources=["/labels/ca-birds.txt"],
        label_count=423,
    )
    row = db.conn.execute(
        "SELECT * FROM labels_fingerprints WHERE fingerprint=?", ("abc123",)
    ).fetchone()
    assert row["display_name"] == "California birds"
    assert json.loads(row["sources_json"]) == ["/labels/ca-birds.txt"]
    assert row["label_count"] == 423

    # Upsert is idempotent
    db.upsert_labels_fingerprint("abc123", "California birds (v2)",
                                  ["/labels/ca-birds-v2.txt"], 500)
    row = db.conn.execute(
        "SELECT display_name, label_count FROM labels_fingerprints WHERE fingerprint=?",
        ("abc123",),
    ).fetchone()
    assert row["display_name"] == "California birds (v2)"
    assert row["label_count"] == 500


def test_review_status_absence_is_pending(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("WS")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    # Predictions now carry ``classifier_model`` (the old ``model`` was renamed).
    pred_id = db.conn.execute(
        """INSERT INTO predictions (detection_id, classifier_model, species, confidence)
           VALUES (?, 'bioclip-2', 'Robin', 0.8)""",
        (det_ids[0],),
    ).lastrowid
    db.conn.commit()

    # No row in prediction_review yet → pending
    assert db.get_review_status(pred_id, ws) == "pending"

    db.set_review_status(pred_id, ws, status="approved")
    assert db.get_review_status(pred_id, ws) == "approved"

    db.set_review_status(pred_id, ws, status="rejected")
    assert db.get_review_status(pred_id, ws) == "rejected"


def test_predictions_has_labels_fingerprint(tmp_path):
    """Fresh DB's predictions table includes the labels_fingerprint column."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    cols = {r[1] for r in db.conn.execute(
        "PRAGMA table_info(predictions)"
    ).fetchall()}
    assert "labels_fingerprint" in cols


def test_predictions_has_new_unique_and_no_legacy_columns(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    cols = {r[1] for r in db.conn.execute(
        "PRAGMA table_info(predictions)"
    ).fetchall()}
    # Legacy review/workspace columns are gone
    for legacy in ("status", "reviewed_at", "individual", "group_id",
                   "vote_count", "total_votes", "workspace_id"):
        assert legacy not in cols, f"legacy column {legacy} still present"
    # New unique constraint on (detection_id, classifier_model, labels_fingerprint, species)
    indexes = db.conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='predictions'"
    ).fetchall()
    assert any(
        "labels_fingerprint" in (idx["sql"] or "") and "species" in (idx["sql"] or "")
        for idx in indexes
    )


def test_miss_columns_present(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    row = db.conn.execute(
        "SELECT miss_no_subject, miss_clipped, miss_oof, miss_computed_at "
        "FROM photos LIMIT 0"
    ).description
    names = {c[0] for c in row}
    assert names == {
        "miss_no_subject", "miss_clipped", "miss_oof", "miss_computed_at",
    }


def test_list_misses_returns_flagged_photos_only(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")
    p1 = db.add_photo(
        folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0
    )
    p2 = db.add_photo(
        folder_id, "b.jpg", ".jpg", file_size=100, file_mtime=2.0
    )
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1, miss_computed_at='2026-04-22' "
        "WHERE id=?", (p1,)
    )
    db.conn.commit()

    misses = db.list_misses()
    ids = [m["id"] for m in misses]
    assert p1 in ids
    assert p2 not in ids


def test_list_misses_filters_by_category(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")
    p_clip = db.add_photo(
        folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0
    )
    p_oof = db.add_photo(
        folder_id, "b.jpg", ".jpg", file_size=100, file_mtime=2.0
    )
    db.conn.execute("UPDATE photos SET miss_clipped=1 WHERE id=?", (p_clip,))
    db.conn.execute("UPDATE photos SET miss_oof=1     WHERE id=?", (p_oof,))
    db.conn.commit()

    assert [m["id"] for m in db.list_misses(category="clipped")] == [p_clip]
    assert [m["id"] for m in db.list_misses(category="oof")] == [p_oof]


def test_clear_miss_flag_on_photo(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")
    p = db.add_photo(
        folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0
    )
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1, miss_oof=1 WHERE id=?", (p,)
    )
    db.conn.commit()

    db.clear_miss_flag(p, "clipped")
    row = db.conn.execute(
        "SELECT miss_clipped, miss_oof FROM photos WHERE id=?", (p,)
    ).fetchone()
    assert row["miss_clipped"] == 0
    assert row["miss_oof"] == 1


def test_bulk_reject_category_sets_flag_rejected(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")
    p1 = db.add_photo(
        folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0
    )
    p2 = db.add_photo(
        folder_id, "b.jpg", ".jpg", file_size=100, file_mtime=2.0
    )
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1 WHERE id IN (?, ?)", (p1, p2)
    )
    db.conn.commit()

    affected = db.bulk_reject_miss_category("clipped")
    assert len(affected) == 2
    assert {a["photo_id"] for a in affected} == {p1, p2}
    for pid in (p1, p2):
        flag = db.conn.execute(
            "SELECT flag FROM photos WHERE id=?", (pid,)
        ).fetchone()["flag"]
        assert flag == "rejected"


def test_misses_helpers_exclude_already_rejected_photos(tmp_path):
    """Neither list_misses nor bulk_reject should touch photos already rejected.

    The exclusion clause (flag IS NULL OR flag != 'rejected') is load-bearing:
    a photo that's already been rejected must not show up again as a miss
    (it's done) and must not inflate the bulk-reject rowcount (it's not news).
    """
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")
    p_miss = db.add_photo(
        folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0
    )
    p_already = db.add_photo(
        folder_id, "b.jpg", ".jpg", file_size=100, file_mtime=2.0
    )
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1 WHERE id IN (?, ?)",
        (p_miss, p_already),
    )
    db.conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (p_already,))
    db.conn.commit()

    listed = [m["id"] for m in db.list_misses(category="clipped")]
    assert p_miss in listed
    assert p_already not in listed

    affected = db.bulk_reject_miss_category("clipped")
    # only p_miss got rejected; p_already was already rejected
    assert len(affected) == 1
    assert affected[0]["photo_id"] == p_miss
    flag_already = db.conn.execute(
        "SELECT flag FROM photos WHERE id=?", (p_already,)
    ).fetchone()["flag"]
    assert flag_already == "rejected"  # unchanged


def test_list_misses_since_filter(tmp_path):
    """`since` restricts results to photos whose miss_computed_at >= since.

    Used by the pipeline-review "Review misses" step to scope the grid to
    photos from the current pipeline run.
    """
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")
    p_old = db.add_photo(
        folder_id, "old.jpg", ".jpg", file_size=100, file_mtime=1.0
    )
    p_new = db.add_photo(
        folder_id, "new.jpg", ".jpg", file_size=100, file_mtime=2.0
    )
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1, miss_computed_at='2026-04-20T00:00:00+00:00' "
        "WHERE id=?", (p_old,),
    )
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1, miss_computed_at='2026-04-22T10:00:00+00:00' "
        "WHERE id=?", (p_new,),
    )
    db.conn.commit()

    all_ids = [m["id"] for m in db.list_misses(category="clipped")]
    assert p_old in all_ids and p_new in all_ids

    recent = [m["id"] for m in db.list_misses(category="clipped",
                                              since="2026-04-21T00:00:00+00:00")]
    assert recent == [p_new]

    grouped = db.list_misses(since="2026-04-21T00:00:00+00:00")
    assert [m["id"] for m in grouped] == [p_new]


def test_list_misses_scoped_to_active_workspace(tmp_path):
    """Misses in folders linked only to workspace A must not appear or get
    rejected when workspace B is active."""
    from db import Database
    db = Database(str(tmp_path / "m.db"))

    ws_a = db.ensure_default_workspace()
    ws_b = db.create_workspace("Other")

    db.set_active_workspace(ws_a)
    fa = db.add_folder("/tmp/a", name="a")
    p_a = db.add_photo(fa, "a.jpg", ".jpg", file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET miss_clipped=1 WHERE id=?", (p_a,))
    db.conn.commit()

    db.set_active_workspace(ws_b)
    fb = db.add_folder("/tmp/b", name="b")
    p_b = db.add_photo(fb, "b.jpg", ".jpg", file_size=100, file_mtime=2.0)
    db.conn.execute("UPDATE photos SET miss_clipped=1 WHERE id=?", (p_b,))
    db.conn.commit()

    # Workspace B sees only its own miss.
    ids_b = [m["id"] for m in db.list_misses(category="clipped")]
    assert ids_b == [p_b]

    # Bulk reject in B must not touch A's photo.
    affected = db.bulk_reject_miss_category("clipped")
    assert len(affected) == 1
    assert affected[0]["photo_id"] == p_b
    flag_a = db.conn.execute(
        "SELECT flag FROM photos WHERE id=?", (p_a,)
    ).fetchone()["flag"]
    assert flag_a != "rejected"
    flag_b = db.conn.execute(
        "SELECT flag FROM photos WHERE id=?", (p_b,)
    ).fetchone()["flag"]
    assert flag_b == "rejected"

    # Switching back to A should still reveal its untouched miss.
    db.set_active_workspace(ws_a)
    ids_a = [m["id"] for m in db.list_misses(category="clipped")]
    assert ids_a == [p_a]


def test_list_misses_joins_primary_detection_from_detections_table(tmp_path):
    """list_misses must source detection_box/detection_conf from the
    canonical `detections` table (highest-confidence row per photo, workspace-
    scoped), not the legacy photos.detection_* columns that aren't populated
    by normal pipeline runs."""
    import json as _json

    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")
    p = db.add_photo(folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET miss_clipped=1 WHERE id=?", (p,))
    db.conn.commit()
    # Two detections; the primary is the higher-confidence one.
    db.save_detections(
        p,
        [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.1, "h": 0.1},
             "confidence": 0.40, "category": "animal"},
            {"box": {"x": 0.35, "y": 0.35, "w": 0.2, "h": 0.2},
             "confidence": 0.85, "category": "animal"},
        ],
        detector_model="megadetector-v6",
    )

    misses = db.list_misses(category="clipped")
    assert len(misses) == 1
    m = misses[0]
    assert m["detection_conf"] == 0.85
    box = _json.loads(m["detection_box"])
    assert box == {"x": 0.35, "y": 0.35, "w": 0.2, "h": 0.2}


def test_list_misses_returns_null_detection_when_no_detections(tmp_path):
    """A no_subject miss has no detection — detection_box/conf are None."""
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")
    p = db.add_photo(folder_id, "a.jpg", ".jpg", file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET miss_no_subject=1 WHERE id=?", (p,))
    db.conn.commit()

    misses = db.list_misses(category="no_subject")
    assert len(misses) == 1
    assert misses[0]["detection_box"] is None
    assert misses[0]["detection_conf"] is None


def test_bulk_reject_miss_category_scoped_by_since(tmp_path):
    """Bulk reject must honor the same `since` filter as list_misses so
    clicking "Reject all" on /misses?since=... doesn't silently reject
    older misses from prior pipeline runs that aren't shown on screen."""
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")

    p_old = db.add_photo(folder_id, "old.jpg", ".jpg", file_size=100, file_mtime=1.0)
    p_new = db.add_photo(folder_id, "new.jpg", ".jpg", file_size=100, file_mtime=2.0)
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1, miss_computed_at=? WHERE id=?",
        ("2026-04-10T00:00:00+00:00", p_old),
    )
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1, miss_computed_at=? WHERE id=?",
        ("2026-04-22T10:00:00+00:00", p_new),
    )
    db.conn.commit()

    # Since filter matches only the new miss.
    affected = db.bulk_reject_miss_category("clipped", since="2026-04-20T00:00:00+00:00")
    assert len(affected) == 1
    assert affected[0]["photo_id"] == p_new

    flag_old = db.conn.execute(
        "SELECT flag FROM photos WHERE id=?", (p_old,)
    ).fetchone()["flag"]
    flag_new = db.conn.execute(
        "SELECT flag FROM photos WHERE id=?", (p_new,)
    ).fetchone()["flag"]
    assert flag_old != "rejected"
    assert flag_new == "rejected"

    # Without since, the old miss is now eligible.
    affected2 = db.bulk_reject_miss_category("clipped")
    assert len(affected2) == 1
    assert affected2[0]["photo_id"] == p_old
    flag_old2 = db.conn.execute(
        "SELECT flag FROM photos WHERE id=?", (p_old,)
    ).fetchone()["flag"]
    assert flag_old2 == "rejected"


def test_bulk_reject_miss_category_preserves_null_flag_in_old_value(tmp_path):
    """old_value must be None (not "") for rows with NULL flag, so undo
    can restore the original NULL rather than writing a non-canonical
    empty string that bypasses flag validation (none/flagged/rejected
    or NULL)."""
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")

    p_null = db.add_photo(folder_id, "null.jpg", ".jpg", file_size=100, file_mtime=1.0)
    p_none = db.add_photo(folder_id, "none.jpg", ".jpg", file_size=100, file_mtime=2.0)
    p_flagged = db.add_photo(folder_id, "flagged.jpg", ".jpg", file_size=100, file_mtime=3.0)

    # p_null forced to NULL flag (add_photo defaults the column to 'none');
    # p_none explicitly "none"; p_flagged "flagged".
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1 WHERE id IN (?, ?, ?)",
        (p_null, p_none, p_flagged),
    )
    db.conn.execute("UPDATE photos SET flag=NULL      WHERE id=?", (p_null,))
    db.conn.execute("UPDATE photos SET flag='none'    WHERE id=?", (p_none,))
    db.conn.execute("UPDATE photos SET flag='flagged' WHERE id=?", (p_flagged,))
    db.conn.commit()

    affected = db.bulk_reject_miss_category("clipped")
    by_id = {a["photo_id"]: a for a in affected}

    assert by_id[p_null]["old_value"] is None
    assert by_id[p_none]["old_value"] == "none"
    assert by_id[p_flagged]["old_value"] == "flagged"


def test_list_misses_chunks_detection_lookup_over_sqlite_var_limit(tmp_path):
    """With >999 flagged misses, the detections IN (...) clause would exceed
    SQLite's SQLITE_MAX_VARIABLE_NUMBER. list_misses must chunk the lookup
    so /api/misses doesn't raise ``OperationalError: too many SQL variables``
    for workspaces with many flagged photos."""
    from db import Database
    db = Database(str(tmp_path / "m.db"))
    folder_id = db.add_folder("/tmp/fake")

    # Insert 1100 photos, all flagged as clipped, each with one detection.
    # This pushes the IN clause well past the default 999-var limit and would
    # crash without chunking.
    N = 1100
    photo_ids = []
    for i in range(N):
        pid = db.add_photo(
            folder_id, f"p{i:04d}.jpg", ".jpg",
            file_size=100, file_mtime=float(i + 1),
        )
        photo_ids.append(pid)
    db.conn.executemany(
        "UPDATE photos SET miss_clipped=1 WHERE id=?",
        [(pid,) for pid in photo_ids],
    )
    db.conn.commit()
    for pid in photo_ids:
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
              "confidence": 0.9, "category": "animal"}],
            detector_model="megadetector-v6",
        )

    misses = db.list_misses(category="clipped")
    assert len(misses) == N
    # Every row must have a detection attached (proving chunking visited all).
    assert all(m["detection_conf"] == 0.9 for m in misses)
    assert all(m["detection_box"] is not None for m in misses)


def test_clear_miss_flag_scoped_to_active_workspace(tmp_path):
    """clear_miss_flag must refuse to touch a photo from another workspace."""
    import pytest
    from db import Database
    db = Database(str(tmp_path / "m.db"))

    ws_a = db._active_workspace_id
    ws_b = db.create_workspace("Other")

    db.set_active_workspace(ws_a)
    fa = db.add_folder("/tmp/a", name="a")
    p_a = db.add_photo(fa, "a.jpg", ".jpg", file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET miss_clipped=1 WHERE id=?", (p_a,))
    db.conn.commit()

    db.set_active_workspace(ws_b)
    with pytest.raises(ValueError):
        db.clear_miss_flag(p_a, "clipped")

    # A's miss flag must still be set.
    row = db.conn.execute(
        "SELECT miss_clipped FROM photos WHERE id=?", (p_a,)
    ).fetchone()
    assert row["miss_clipped"] == 1


def test_new_image_snapshots_tables_exist(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    tables = {
        r["name"]
        for r in db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "new_image_snapshots" in tables
    assert "new_image_snapshot_files" in tables


def test_create_and_get_new_images_snapshot(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    paths = ["/tmp/a/IMG_001.JPG", "/tmp/b/IMG_002.JPG"]
    snap_id = db.create_new_images_snapshot(paths)
    assert isinstance(snap_id, int)

    snap = db.get_new_images_snapshot(snap_id)
    assert snap is not None
    assert snap["file_count"] == 2
    assert snap["workspace_id"] == ws_id
    assert sorted(snap["file_paths"]) == sorted(paths)


def test_get_snapshot_from_different_workspace_returns_none(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    other_ws = db.create_workspace("Other")
    paths = ["/tmp/a/IMG_001.JPG"]
    snap_id = db.create_new_images_snapshot(paths)
    db.set_active_workspace(other_ws)
    assert db.get_new_images_snapshot(snap_id) is None


def test_snapshot_deleted_with_workspace(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    throwaway_ws = db.create_workspace("Throwaway")
    db.set_active_workspace(throwaway_ws)
    snap_id = db.create_new_images_snapshot(["/tmp/a.jpg"])
    db.delete_workspace(throwaway_ws)
    row = db.conn.execute(
        "SELECT id FROM new_image_snapshots WHERE id = ?", (snap_id,)
    ).fetchone()
    assert row is None


def test_create_snapshot_empty_paths(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    snap_id = db.create_new_images_snapshot([])
    snap = db.get_new_images_snapshot(snap_id)
    assert snap["file_count"] == 0
    assert snap["file_paths"] == []


# -- busy_timeout / lock resilience --


def test_busy_timeout_pragma_is_set(tmp_path):
    """Each connection explicitly sets PRAGMA busy_timeout to at least 30s.

    Python's sqlite3 default (5000ms) is too tight under load — heavy
    pipeline writes can hold the writer lock longer than that. A real-world
    scan against a busy DB hit 'database is locked' with the implicit default.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    timeout_ms = db.conn.execute("PRAGMA busy_timeout").fetchone()[0]
    assert timeout_ms >= 30000


def test_concurrent_writers_wait_rather_than_fail(tmp_path):
    """A second writer queues behind the first instead of crashing.

    Repro of the failure mode that killed 3 of 4 parallel scans in prod: the
    default 5s busy_timeout wasn't enough under load. With a longer timeout
    explicitly set, the second writer waits and succeeds.
    """
    import sqlite3
    import threading
    import time

    from db import Database

    db_path = str(tmp_path / "concurrent.db")
    db = Database(db_path)
    db.add_folder("/a")

    blocker = sqlite3.connect(db_path, check_same_thread=False)
    blocker.execute("BEGIN IMMEDIATE")
    blocker.execute("UPDATE folders SET name = 'held' WHERE path = '/a'")

    def release_after_delay():
        time.sleep(0.8)
        blocker.commit()
        blocker.close()

    threading.Thread(target=release_after_delay, daemon=True).start()

    start = time.time()
    db.add_folder("/b")
    elapsed = time.time() - start

    assert elapsed >= 0.5, f"writer did not wait for lock (elapsed={elapsed:.2f}s)"
    assert elapsed < 5.0, f"writer waited too long (elapsed={elapsed:.2f}s)"
    paths = {r["path"] for r in db.conn.execute("SELECT path FROM folders").fetchall()}
    assert {"/a", "/b"}.issubset(paths)


def test_folder_status_partial_value_allowed(tmp_path):
    """folders.status accepts 'partial' as a value — scan abort marker."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/x")
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (fid,))
    db.conn.commit()
    row = db.conn.execute(
        "SELECT status FROM folders WHERE id = ?", (fid,)
    ).fetchone()
    assert row["status"] == "partial"


def test_get_folder_tree_includes_partial_folders_with_status(tmp_path):
    """Partial folders must stay in the tree so the browse sidebar can render
    a badge and the user can rescan. Also, the returned rows must carry
    ``status`` so the UI can tell ok from partial. Missing folders are still
    excluded — they have their own ``get_missing_folders`` path.
    """
    from db import Database

    db = Database(str(tmp_path / "tree.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    ok_id = db.add_folder("/ok", name="ok")
    partial_id = db.add_folder("/partial", name="partial")
    missing_id = db.add_folder("/missing", name="missing")
    db.conn.execute(
        "UPDATE folders SET status = 'partial' WHERE id = ?", (partial_id,)
    )
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (missing_id,)
    )
    db.conn.commit()

    rows = {row["id"]: dict(row) for row in db.get_folder_tree()}

    assert ok_id in rows, "ok folder must appear in tree"
    assert partial_id in rows, (
        "partial folder must appear in tree so badge renders and rescan works"
    )
    assert missing_id not in rows, "missing folder must not appear in tree"
    assert rows[ok_id]["status"] == "ok"
    assert rows[partial_id]["status"] == "partial"


def test_keyword_types_constant():
    """KEYWORD_TYPES contains exactly the five valid enum values."""
    from db import KEYWORD_TYPES
    assert frozenset({"taxonomy", "individual", "location", "genre", "general"}) == KEYWORD_TYPES


def test_add_keyword_accepts_valid_types(tmp_path):
    """add_keyword stores the requested type when it's a valid enum value."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    kid = db.add_keyword("Charlie", kw_type="individual")
    row = db.conn.execute("SELECT type FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["type"] == "individual"


def test_add_keyword_rejects_unknown_type(tmp_path):
    """add_keyword raises ValueError for unknown type values."""
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    with pytest.raises(ValueError, match="invalid keyword type"):
        db.add_keyword("BadType", kw_type="alien")


def test_add_keyword_explicit_taxonomy_for_unknown_name_skips_taxa_lookup(tmp_path):
    """Explicit kw_type='taxonomy' bypasses taxa-table lookup; taxon_id stays NULL.
    is_species is auto-corrected to 1 to match the type."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    kid = db.add_keyword("DefinitelyNotASpecies42", kw_type="taxonomy")
    row = db.conn.execute(
        "SELECT type, taxon_id, is_species FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] is None
    assert row["is_species"] == 1  # auto-set by the new reconciliation


def test_add_keyword_is_species_with_non_taxonomy_type_raises(tmp_path):
    """is_species=True paired with a non-'taxonomy' kw_type is inconsistent and
    must raise ValueError rather than silently producing a mismatched row."""
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    with pytest.raises(ValueError, match="is_species=True requires kw_type='taxonomy'"):
        db.add_keyword("Mismatch", is_species=True, kw_type="general")


def test_add_keyword_explicit_taxonomy_sets_is_species(tmp_path):
    """Explicit kw_type='taxonomy' (without is_species) auto-sets is_species=1
    so the legacy column stays in sync with the type."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    kid = db.add_keyword("Some Bird", kw_type="taxonomy")
    row = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1


def test_add_keyword_existing_general_upgrades_to_requested_type(tmp_path):
    """An existing 'general' keyword should be upgraded to the requested type
    when add_keyword is called again with an explicit kw_type."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    # Use a name that isn't pre-seeded as a default genre keyword.
    kid1 = db.add_keyword("Cityscape")
    row1 = db.conn.execute("SELECT type FROM keywords WHERE id = ?", (kid1,)).fetchone()
    assert row1["type"] == "general"

    kid2 = db.add_keyword("Cityscape", kw_type="genre")
    assert kid2 == kid1
    row2 = db.conn.execute("SELECT type FROM keywords WHERE id = ?", (kid2,)).fetchone()
    assert row2["type"] == "genre"


def test_add_keyword_existing_general_upgrades_to_taxonomy_sets_is_species(tmp_path):
    """Upgrading an existing 'general' keyword to 'taxonomy' should also
    flip is_species to 1 so the legacy column stays consistent."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    kid1 = db.add_keyword("Mystery Bird")
    row1 = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid1,)
    ).fetchone()
    assert row1["type"] == "general"
    assert row1["is_species"] == 0

    kid2 = db.add_keyword("Mystery Bird", kw_type="taxonomy")
    assert kid2 == kid1
    row2 = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid2,)
    ).fetchone()
    assert row2["type"] == "taxonomy"
    assert row2["is_species"] == 1


def test_get_subject_types_returns_default(tmp_path, monkeypatch):
    """A fresh workspace with no overrides falls back to the global default."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    assert db.get_subject_types() == {"taxonomy", "individual", "genre"}


def test_get_subject_types_honors_workspace_override(tmp_path, monkeypatch):
    """Workspace config_overrides for subject_types take precedence over the default."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace(ws_id, config_overrides={"subject_types": ["taxonomy"]})
    assert db.get_subject_types() == {"taxonomy"}


def test_get_subject_types_drops_unknown_values(tmp_path, monkeypatch):
    """Unknown type strings in the workspace override are silently dropped."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace(ws_id, config_overrides={"subject_types": ["taxonomy", "alien"]})
    assert db.get_subject_types() == {"taxonomy"}


def test_filter_out_subject_tagged_excludes_tagged_photos(tmp_path):
    """filter_out_subject_tagged drops photos that have any keyword whose
    type is in the supplied set."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))

    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='p2.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    gen_kid = db.add_keyword("note", kw_type="general")
    db.tag_photo(p1, scene_kid)
    db.tag_photo(p2, gen_kid)

    kept = db.filter_out_subject_tagged([p1, p2], {"genre"})
    assert kept == [p2]


def test_filter_out_subject_tagged_empty_set_returns_all(tmp_path):
    """An empty subject_types set means no type counts as 'identifying',
    so every input photo is kept."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    assert db.filter_out_subject_tagged([p1], set()) == [p1]


def test_filter_out_subject_tagged_empty_photo_ids_returns_empty(tmp_path):
    """An empty photo_ids list short-circuits to an empty result."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    assert db.filter_out_subject_tagged([], {"genre"}) == []


def test_filter_out_subject_tagged_preserves_input_order(tmp_path):
    """Output preserves the input order of photo_ids for the kept rows."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))

    fid = db.add_folder('/photos', name='photos')
    # Insert several photos; we'll then ask filter_out_subject_tagged
    # to keep them all (no subject tags) but in a specific input order.
    p_a = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    p_b = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    p_c = db.add_photo(folder_id=fid, filename='c.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)

    # No subject-tagged photos here — all three should be kept.
    # Provide ids in non-sorted order to confirm the helper preserves input order.
    ordered = [p_c, p_a, p_b]
    assert db.filter_out_subject_tagged(ordered, {"genre"}) == ordered


def test_tag_photo_with_first_taxonomy_keyword_adds_wildlife_genre(tmp_path):
    """Tagging a photo with its first taxonomy keyword auto-adds the
    Wildlife genre keyword."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    species_kid = db.add_keyword("Northern cardinal", is_species=True)

    db.tag_photo(p1, species_kid)

    # Photo should now have BOTH the species keyword AND Wildlife genre.
    rows = db.conn.execute(
        """SELECT k.name, k.type FROM photo_keywords pk
           JOIN keywords k ON k.id = pk.keyword_id
           WHERE pk.photo_id = ?
           ORDER BY k.name""",
        (p1,),
    ).fetchall()
    by_name = {r["name"]: r["type"] for r in rows}
    assert by_name == {
        "Northern cardinal": "taxonomy",
        "Wildlife": "genre",
    }


def test_tag_photo_second_species_does_not_re_add_wildlife(tmp_path):
    """If a photo already has at least one taxonomy keyword, tagging a
    second species does NOT touch the Wildlife genre keyword (so user
    removal sticks)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    sp_a = db.add_keyword("Robin", is_species=True)
    sp_b = db.add_keyword("Sparrow", is_species=True)

    db.tag_photo(p1, sp_a)  # Wildlife auto-added
    # User manually removes Wildlife
    wildlife_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre'"
    ).fetchone()["id"]
    db.untag_photo(p1, wildlife_id)

    db.tag_photo(p1, sp_b)  # second species — should NOT re-add Wildlife

    has_wildlife = db.conn.execute(
        """SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?""",
        (p1, wildlife_id),
    ).fetchone() is not None
    assert has_wildlife is False


def test_tag_photo_with_non_taxonomy_does_not_add_wildlife(tmp_path):
    """Tagging with a non-species keyword (location, individual, etc.)
    does NOT add Wildlife."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    loc_kid = db.add_keyword("Park", kw_type="location")

    db.tag_photo(p1, loc_kid)

    n = db.conn.execute(
        "SELECT COUNT(*) AS n FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.name = 'Wildlife'",
        (p1,),
    ).fetchone()["n"]
    assert n == 0


def test_tag_photo_re_adds_wildlife_after_all_species_removed_and_new_added(tmp_path):
    """Sticky removal only sticks while at least one taxonomy keyword
    exists. Removing all species and tagging a new one re-adds Wildlife."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    sp_a = db.add_keyword("Robin", is_species=True)
    sp_b = db.add_keyword("Sparrow", is_species=True)
    db.tag_photo(p1, sp_a)  # Wildlife added
    wildlife_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre'"
    ).fetchone()["id"]
    db.untag_photo(p1, wildlife_id)
    # Remove the only species
    db.untag_photo(p1, sp_a)

    # Tag a new species — this is the "first species again" case
    db.tag_photo(p1, sp_b)

    has_wildlife = db.conn.execute(
        """SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?""",
        (p1, wildlife_id),
    ).fetchone() is not None
    assert has_wildlife is True


def test_backfill_auto_wildlife_for_existing_species_tagged_photos(tmp_path):
    """The one-shot backfill adds Wildlife to every photo with a species
    keyword that doesn't already have Wildlife. Idempotent."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='p2.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p3 = db.add_photo(folder_id=fid, filename='p3.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    sp_a = db.add_keyword("Robin", is_species=True)
    # Bypass tag_photo so the auto-Wildlife rule does NOT fire — this
    # simulates a pre-existing DB.
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, sp_a),
    )
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p2, sp_a),
    )
    db.conn.commit()

    db.backfill_wildlife_genre()
    db.backfill_wildlife_genre()  # idempotent

    wildlife_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre'"
    ).fetchone()["id"]

    # p1, p2 should have Wildlife. p3 (no species) should not.
    def has_wildlife(pid):
        return db.conn.execute(
            "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (pid, wildlife_id),
        ).fetchone() is not None
    assert has_wildlife(p1) is True
    assert has_wildlife(p2) is True
    assert has_wildlife(p3) is False
