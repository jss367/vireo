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


def test_default_collections_created(tmp_path):
    """create_default_collections creates default collections."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    db.create_default_collections()

    colls = db.get_collections()
    names = {c['name'] for c in colls}
    assert 'Needs Classification' in names
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
    assert len(colls) == 4


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
    assert 'Needs Classification' in names
    assert 'Untagged' in names
    assert 'Recent Import' in names
    assert len(colls) == 4  # no duplicate Flagged


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
    db, _ = _make_workspace_with_photos(tmp_path, [
        {'mask_path': '/mask/1.png', 'detection_box': '0,0,100,100', 'subject_tenengrad': 42.0},
        {'mask_path': '/mask/2.png'},
        {'detection_box': '10,10,50,50'},
        {},
    ])
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
         'quality_score': 0.85, 'detection_conf': 0.9},
        {'timestamp': '2024-06-20T14:00:00', 'rating': 5, 'flag': 'none',
         'quality_score': 0.42, 'detection_conf': 0.0},
        {'timestamp': '2024-07-01T08:00:00', 'rating': 3, 'flag': 'none'},
    ])

    # Add keywords
    kid = db.add_keyword('Robin', is_species=True)
    db.tag_photo(pids[0], kid)
    db.tag_photo(pids[1], kid)

    # Add a prediction
    db.add_prediction(photo_id=pids[0], species='Robin', confidence=0.95, model='test')

    stats = db.get_dashboard_stats()

    # top_keywords: Robin with 2 photos
    assert len(stats['top_keywords']) == 1
    assert stats['top_keywords'][0]['name'] == 'Robin'
    assert stats['top_keywords'][0]['photo_count'] == 2

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

    # detected_count (detection_conf > 0)
    assert stats['detected_count'] == 1

    # photos_by_hour
    hours = {r['hour']: r['count'] for r in stats['photos_by_hour']}
    assert hours[10] == 1
    assert hours[14] == 1
    assert hours[8] == 1


# --- Cluster 3: Prediction Management ---

def test_get_group_predictions(tmp_path):
    """Returns predictions with photo data for a group."""
    db, pids = _make_workspace_with_photos(tmp_path, [
        {'quality_score': 0.9}, {'quality_score': 0.5},
    ])
    db.add_prediction(photo_id=pids[0], species='Robin', confidence=0.95,
                      model='test', group_id='g1')
    db.add_prediction(photo_id=pids[1], species='Robin', confidence=0.80,
                      model='test', group_id='g1')

    results = db.get_group_predictions('g1')
    assert len(results) == 2
    # Should be ordered by quality_score DESC
    assert results[0]['quality_score'] == 0.9
    assert results[1]['quality_score'] == 0.5
    # Should include photo fields
    assert 'filename' in dict(results[0])


def test_update_predictions_status_by_photo(tmp_path):
    """Updates prediction status for all predictions of a photo."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    db.add_prediction(photo_id=pids[0], species='Robin', confidence=0.95, model='test')

    db.update_predictions_status_by_photo(pids[0], 'accepted')

    preds = db.get_predictions(photo_ids=[pids[0]])
    assert preds[0]['status'] == 'accepted'


def test_ungroup_prediction(tmp_path):
    """Removes a prediction from its group."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    db.add_prediction(photo_id=pids[0], species='Robin', confidence=0.95,
                      model='test', group_id='g1')
    pred = db.get_predictions(photo_ids=[pids[0]])[0]

    db.ungroup_prediction(pred['id'])

    updated = db.get_predictions(photo_ids=[pids[0]])[0]
    assert updated['group_id'] is None


def test_get_existing_prediction_photo_ids(tmp_path):
    """Returns set of photo_ids that have predictions for a model."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}, {}])
    db.add_prediction(photo_id=pids[0], species='Robin', confidence=0.9, model='bioclip')

    result = db.get_existing_prediction_photo_ids('bioclip')
    assert result == {pids[0]}

    result = db.get_existing_prediction_photo_ids('other-model')
    assert result == set()


def test_get_prediction_for_photo(tmp_path):
    """Returns species and confidence for a photo's prediction by model."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    db.add_prediction(photo_id=pids[0], species='Robin', confidence=0.95, model='bioclip')

    row = db.get_prediction_for_photo(pids[0], 'bioclip')
    assert row['species'] == 'Robin'
    assert row['confidence'] == 0.95

    assert db.get_prediction_for_photo(pids[0], 'other') is None


def test_get_and_store_photo_embedding(tmp_path):
    """Stores and retrieves a photo embedding."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])

    assert db.get_photo_embedding(pids[0]) is None

    db.store_photo_embedding(pids[0], b'\x01\x02\x03\x04')

    result = db.get_photo_embedding(pids[0])
    assert result == b'\x01\x02\x03\x04'


def test_update_prediction_group_info(tmp_path):
    """Updates group info on an existing prediction."""
    db, pids = _make_workspace_with_photos(tmp_path, [{}])
    db.add_prediction(photo_id=pids[0], species='Robin', confidence=0.95, model='bioclip')

    db.update_prediction_group_info(
        photo_id=pids[0], model='bioclip',
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
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=?", (p1,))
    db.conn.commit()
    db.add_prediction(p1, 'Red-tailed Hawk', 0.95, 'bioclip')
    # Accept the prediction
    pred = db.get_predictions(photo_ids=[p1])
    db.conn.execute("UPDATE predictions SET status='accepted' WHERE id=?", (pred[0]['id'],))
    db.conn.commit()

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
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='heron.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id IN (?,?)", (p1, p2))
    db.conn.commit()
    db.add_prediction(p1, 'Red-tailed Hawk', 0.95, 'bioclip')
    db.add_prediction(p2, 'Great Blue Heron', 0.90, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1, p2])
    for pr in preds:
        db.conn.execute("UPDATE predictions SET status='accepted' WHERE id=?", (pr['id'],))
    db.conn.commit()

    results = db.get_geolocated_photos(species='Red-tailed Hawk')
    assert len(results) == 1
    assert results[0]['filename'] == 'hawk.jpg'

    results = db.get_geolocated_photos(species='Great Blue Heron')
    assert len(results) == 1
    assert results[0]['filename'] == 'heron.jpg'


def test_get_accepted_species(tmp_path):
    """get_accepted_species returns distinct marker species from geolocated photos."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='heron.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    # Both photos need GPS to appear in species list
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id IN (?,?)", (p1, p2))
    db.conn.commit()
    db.add_prediction(p1, 'Red-tailed Hawk', 0.95, 'bioclip')
    db.add_prediction(p2, 'Great Blue Heron', 0.90, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1, p2])
    for pr in preds:
        db.conn.execute("UPDATE predictions SET status='accepted' WHERE id=?", (pr['id'],))
    db.conn.commit()

    species = db.get_accepted_species()
    assert 'Great Blue Heron' in species
    assert 'Red-tailed Hawk' in species
    assert len(species) == 2


def test_get_accepted_species_excludes_non_geolocated(tmp_path):
    """get_accepted_species excludes species from photos without GPS."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    # p1 has no GPS coordinates
    db.add_prediction(p1, 'Red-tailed Hawk', 0.95, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1])
    db.conn.execute("UPDATE predictions SET status='accepted' WHERE id=?", (preds[0]['id'],))
    db.conn.commit()

    species = db.get_accepted_species()
    assert species == []


def test_get_accepted_species_excludes_non_accepted(tmp_path):
    """get_accepted_species only includes accepted predictions, not pending."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=?", (p1,))
    db.conn.commit()
    db.add_prediction(p1, 'Red-tailed Hawk', 0.95, 'bioclip')

    species = db.get_accepted_species()
    assert species == []


def test_get_accepted_species_uses_top_confidence(tmp_path):
    """get_accepted_species returns only the highest-confidence species per photo."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=?", (p1,))
    db.conn.commit()
    db.add_prediction(p1, 'Red-tailed Hawk', 0.95, 'bioclip')
    db.add_prediction(p1, 'Cooper\'s Hawk', 0.60, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1])
    for pr in preds:
        db.conn.execute("UPDATE predictions SET status='accepted' WHERE id=?", (pr['id'],))
    db.conn.commit()

    species = db.get_accepted_species()
    # Only the top-confidence species should appear
    assert species == ['Red-tailed Hawk']


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


def test_add_keyword_people_type(tmp_path):
    """A keyword can be created with type='people' via direct SQL update."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword("John Doe")
    db.conn.execute(
        "UPDATE keywords SET type = 'people' WHERE id = ?", (kid,)
    )
    db.conn.commit()
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "people"


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
    kid = db.add_keyword('Sunset')
    db.tag_photo(pid, kid)

    keywords = db.get_photo_keywords(pid)
    assert len(keywords) == 1
    assert keywords[0]['type'] == 'general'


def test_embedding_model_column_exists(tmp_path):
    """The photos table has an embedding_model column."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute("SELECT embedding_model FROM photos LIMIT 0")


def test_store_photo_embedding_with_model(tmp_path):
    """store_photo_embedding saves model name alongside the embedding."""
    import numpy as np
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.conn.execute("INSERT INTO folders (path, name) VALUES ('/tmp', 'tmp')").lastrowid
    pid = db.conn.execute("INSERT INTO photos (folder_id, filename) VALUES (?, 'a.jpg')", (fid,)).lastrowid
    db.conn.commit()
    emb = np.random.randn(512).astype(np.float32)
    db.store_photo_embedding(pid, emb.tobytes(), model="BioCLIP")
    row = db.conn.execute("SELECT embedding_model FROM photos WHERE id = ?", (pid,)).fetchone()
    assert row["embedding_model"] == "BioCLIP"


def test_get_embeddings_by_model(tmp_path):
    """get_embeddings_by_model returns only photos with matching model."""
    import numpy as np
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.conn.execute("INSERT INTO folders (path, name) VALUES ('/tmp', 'tmp')").lastrowid
    # Link folder to workspace
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._active_workspace_id, fid),
    )
    emb1 = np.random.randn(512).astype(np.float32)
    emb2 = np.random.randn(512).astype(np.float32)
    p1 = db.conn.execute("INSERT INTO photos (folder_id, filename) VALUES (?, 'a.jpg')", (fid,)).lastrowid
    p2 = db.conn.execute("INSERT INTO photos (folder_id, filename) VALUES (?, 'b.jpg')", (fid,)).lastrowid
    p3 = db.conn.execute("INSERT INTO photos (folder_id, filename) VALUES (?, 'c.jpg')", (fid,)).lastrowid
    db.store_photo_embedding(p1, emb1.tobytes(), model="BioCLIP")
    db.store_photo_embedding(p2, emb2.tobytes(), model="BioCLIP-2")
    # p3 has no embedding
    db.conn.commit()

    results = db.get_embeddings_by_model("BioCLIP")
    assert len(results) == 1
    assert results[0][0] == p1


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
    """The detections table should exist with expected columns."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    row = db.conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='detections'"
    ).fetchone()
    assert row is not None
    schema = row[0].lower()
    assert "photo_id" in schema
    assert "workspace_id" in schema
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
    rows = db.conn.execute("SELECT * FROM detections WHERE photo_id = ?", (pid,)).fetchall()
    assert len(rows) == 2
    assert rows[0]["box_x"] == 0.1
    assert rows[1]["box_x"] == 0.5


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


def test_clear_detections(tmp_path):
    """clear_detections should remove detections and cascade to predictions."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(folder_id=fid, filename="elk.jpg", extension=".jpg", file_size=100, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    # Insert prediction via raw SQL since add_prediction is not yet refactored
    # for the detection-based schema (Task 3)
    db.conn.execute(
        "INSERT INTO predictions (detection_id, species, confidence, model, status) VALUES (?, ?, ?, ?, ?)",
        (det_ids[0], "Elk", 0.9, "bioclip", "pending"),
    )
    db.conn.commit()
    db.clear_detections(pid)
    assert db.conn.execute("SELECT COUNT(*) FROM detections WHERE photo_id = ?", (pid,)).fetchone()[0] == 0
    assert db.conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
