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
    from db import Database
    import json
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
    from db import Database
    import json
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
    from db import Database
    import json
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
    from db import Database
    import json
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
    from db import Database
    from datetime import datetime, timedelta
    import json
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
    from db import Database
    import json
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
    from db import Database
    import json
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
