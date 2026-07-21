# vireo/tests/test_db.py
import os
import sys

import pytest

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


def test_old_predictions_schema_raises_incompatible(tmp_path):
    """A pre-`classifier_model` database fails with a typed, actionable error.

    Reproduces the real-world crash: a database from an older Vireo has a
    `predictions` table built around a `model` column. The current schema's
    `CREATE TABLE IF NOT EXISTS predictions` silently skips that existing
    table, then `idx_predictions_identity` references the absent
    `classifier_model` column and raises `no such column: classifier_model`.
    `Database.__init__` must convert that opaque OperationalError into an
    IncompatibleDatabaseError carrying the db path and original cause.
    """
    import sqlite3

    import pytest
    from db import Database, IncompatibleDatabaseError

    db_path = str(tmp_path / "old.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE predictions (
            id           INTEGER PRIMARY KEY,
            detection_id INTEGER,
            species      TEXT,
            model        TEXT,
            UNIQUE(detection_id, model, species)
        );
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(IncompatibleDatabaseError) as excinfo:
        Database(db_path)
    assert excinfo.value.db_path == db_path
    assert "classifier_model" in (excinfo.value.cause or "")
    # The message is user-facing remediation context, not a bare stack trace.
    assert "incompatible older version" in str(excinfo.value)


def test_fresh_database_does_not_raise_incompatible(tmp_path):
    """A fresh database initializes cleanly — the guard never false-fires."""
    from db import Database

    # No exception == pass. Sanity-check a current-schema table came through.
    db = Database(str(tmp_path / "fresh.db"))
    cols = [r[1] for r in db.conn.execute("PRAGMA table_info(predictions)").fetchall()]
    assert "classifier_model" in cols


def test_insert_into_stale_table_raises_incompatible(tmp_path, monkeypatch):
    """A stale-schema failure surfacing as ``has no column named …`` is caught.

    SQLite reports a different OperationalError shape when an INSERT/UPDATE
    targets an existing-but-stale table that's missing a newly added column:
    ``table <name> has no column named <col>`` rather than
    ``no such column: …``. ``_create_tables`` has INSERT paths into long-lived
    tables (e.g. backfill writes to ``db_meta``) that can hit this when the
    on-disk shape is older than the current build expects, so the guard must
    classify this third spelling as an incompatible-database failure too —
    otherwise the raw OperationalError escapes, ``main()`` never emits the
    structured ``incompatible_database`` stderr, and the desktop launcher
    shows the generic "Sidecar did not become healthy" timeout instead of
    the actionable remediation.
    """
    import sqlite3

    import db as db_module
    import pytest
    from db import Database, IncompatibleDatabaseError

    def fake_create(self):
        raise sqlite3.OperationalError(
            "table db_meta has no column named value"
        )

    monkeypatch.setattr(db_module.Database, "_create_tables", fake_create)

    with pytest.raises(IncompatibleDatabaseError) as excinfo:
        Database(str(tmp_path / "stale.db"))
    assert excinfo.value.db_path == str(tmp_path / "stale.db")
    assert "has no column named" in (excinfo.value.cause or "")
    assert "incompatible older version" in str(excinfo.value)


def test_non_schema_operational_error_propagates(tmp_path, monkeypatch):
    """Environmental OperationalErrors propagate as-is, not as IncompatibleDatabaseError.

    The guard is for stale-schema failures ("no such column/table: …"). Other
    OperationalErrors — file locked, read-only, disk full, I/O error — are
    recoverable environmental problems and must surface accurately, otherwise
    the user gets misleading "back up and remove your DB" remediation for a
    perfectly good database that just needs a different fix.
    """
    import sqlite3

    import db as db_module
    import pytest
    from db import Database, IncompatibleDatabaseError

    def fake_create(self):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(db_module.Database, "_create_tables", fake_create)

    with pytest.raises(sqlite3.OperationalError) as excinfo:
        Database(str(tmp_path / "locked.db"))
    assert "locked" in str(excinfo.value)
    assert not isinstance(excinfo.value, IncompatibleDatabaseError)


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


def test_get_photos_filter_by_flag(tmp_path):
    """get_photos and count_filtered_photos can filter picks and rejects."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    picked = db.add_photo(folder_id=fid, filename='pick.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0,
                          timestamp='2024-01-01T10:00:00')
    rejected = db.add_photo(folder_id=fid, filename='reject.jpg', extension='.jpg',
                            file_size=100, file_mtime=2.0,
                            timestamp='2024-01-02T10:00:00')
    db.add_photo(folder_id=fid, filename='plain.jpg', extension='.jpg',
                 file_size=100, file_mtime=3.0,
                 timestamp='2024-01-03T10:00:00')
    db.update_photo_flag(picked, 'flagged')
    db.update_photo_flag(rejected, 'rejected')

    picks = db.get_photos(flag='flagged')
    rejects = db.get_photos(flag='rejected')

    assert [p['filename'] for p in picks] == ['pick.jpg']
    assert [p['filename'] for p in rejects] == ['reject.jpg']
    assert db.count_filtered_photos(flag='flagged') == 1
    assert db.count_filtered_photos(flag='rejected') == 1
    assert db.get_browse_summary(flag='flagged')['filtered_total'] == 1
    assert db.get_calendar_data(2024, flag='rejected')['days'] == {'2024-01-02': 1}


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


def test_get_photos_date_sort_puts_missing_timestamps_last(tmp_path):
    """Undated photos should not appear before the oldest captured photo."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='undated.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp=None)
    db.add_photo(folder_id=fid, filename='newer.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-01T00:00:00')
    db.add_photo(folder_id=fid, filename='older.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-01-01T00:00:00')

    by_date = db.get_photos(sort='date')
    assert [p['filename'] for p in by_date] == ['older.jpg', 'newer.jpg', 'undated.jpg']

    by_date_desc = db.get_photos(sort='date_desc')
    assert [p['filename'] for p in by_date_desc] == ['newer.jpg', 'older.jpg', 'undated.jpg']

    ids_by_date = db.get_photo_ids(sort='date')
    assert ids_by_date == [p['id'] for p in by_date]


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


def test_get_photos_keyword_multi_token(tmp_path):
    """Multi-token keyword search requires every whitespace-separated token to
    match (in the filename or some keyword), so "red bill" finds the
    hyphenated keyword "Red-billed leiothrix" without an exact substring."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p3 = db.add_photo(folder_id=fid, filename='c.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.tag_photo(p1, db.add_keyword('Red-billed leiothrix'))
    db.tag_photo(p2, db.add_keyword('Northern Cardinal'))
    # p3 holds each token in a *separate* keyword.
    db.tag_photo(p3, db.add_keyword('Reddish'))
    db.tag_photo(p3, db.add_keyword('Billboard'))

    # "red bill" matches the hyphenated single keyword (p1) and the
    # split-across-two-keywords photo (p3); token order is irrelevant.
    assert {r['filename'] for r in db.get_photos(keyword='red bill')} == {'a.jpg', 'c.jpg'}
    assert {r['filename'] for r in db.get_photos(keyword='bill red')} == {'a.jpg', 'c.jpg'}

    # Every token must match: no photo has both a "red*" and a "cardinal*" tag.
    assert db.get_photos(keyword='red cardinal') == []

    # A token can be satisfied by a different keyword on the same photo.
    assert {r['filename'] for r in db.get_photos(keyword='northern card')} == {'b.jpg'}

    # count_filtered_photos and get_photo_ids agree with get_photos.
    assert db.count_filtered_photos(keyword='red bill') == 2
    assert len(db.get_photo_ids(keyword='red bill')) == 2


def test_get_photos_keyword_whole_word_excludes_embedded_token(tmp_path):
    """Whole-word keyword search matches separators, not embedded substrings."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    western = db.add_photo(
        folder_id=fid, filename='western-gull.jpg', extension='.jpg',
        file_size=100, file_mtime=1.0,
    )
    common = db.add_photo(
        folder_id=fid, filename='common-tern.jpg', extension='.jpg',
        file_size=100, file_mtime=1.0,
    )
    file_only = db.add_photo(
        folder_id=fid, filename='tern_001.jpg', extension='.jpg',
        file_size=100, file_mtime=1.0,
    )
    eastern = db.add_photo(
        folder_id=fid, filename='eastern-phoebe.jpg', extension='.jpg',
        file_size=100, file_mtime=1.0,
    )
    db.tag_photo(western, db.add_keyword('Western Gull'))
    db.tag_photo(common, db.add_keyword('Common Tern'))
    db.tag_photo(eastern, db.add_keyword('Eastern Phoebe'))

    assert {
        r['filename'] for r in db.get_photos(keyword='tern')
    } == {'western-gull.jpg', 'common-tern.jpg', 'tern_001.jpg', 'eastern-phoebe.jpg'}

    whole_word = db.get_photos(keyword='tern', keyword_whole_word=True)
    assert {r['filename'] for r in whole_word} == {'common-tern.jpg', 'tern_001.jpg'}
    assert db.count_filtered_photos(keyword='tern', keyword_whole_word=True) == 2
    assert set(db.get_photo_ids(keyword='tern', keyword_whole_word=True)) == {
        common,
        file_only,
    }


def test_get_photos_keyword_match_case(tmp_path):
    """Match-case keyword search distinguishes otherwise identical tokens."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(
        folder_id=fid, filename='Common-Tern.jpg', extension='.jpg',
        file_size=100, file_mtime=1.0,
    )
    db.add_photo(
        folder_id=fid, filename='common-tern.jpg', extension='.jpg',
        file_size=100, file_mtime=1.0,
    )

    assert {
        r['filename'] for r in db.get_photos(keyword='Tern')
    } == {'Common-Tern.jpg', 'common-tern.jpg'}
    assert {
        r['filename'] for r in db.get_photos(keyword='Tern', keyword_match_case=True)
    } == {'Common-Tern.jpg'}
    assert {
        r['filename']
        for r in db.get_photos(
            keyword='Tern',
            keyword_match_case=True,
            keyword_whole_word=True,
        )
    } == {'Common-Tern.jpg'}


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


def test_collection_photos_date_sort_puts_missing_timestamps_last(tmp_path):
    """Collection Browse order follows the main Browse date order."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='undated.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp=None)
    db.add_photo(folder_id=fid, filename='newer.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-06-01T00:00:00')
    db.add_photo(folder_id=fid, filename='older.jpg', extension='.jpg', file_size=100,
                 file_mtime=1.0, timestamp='2024-01-01T00:00:00')

    rules = [{"field": "rating", "op": ">=", "value": 0}]
    cid = db.add_collection('All', json.dumps(rules))

    photos = db.get_collection_photos(cid)
    assert [p['filename'] for p in photos] == ['older.jpg', 'newer.jpg', 'undated.jpg']
    assert db.get_collection_photo_ids(cid) == [p['id'] for p in photos]


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


def test_count_photos_for_rules_unsaved(tmp_path):
    """count_photos_for_rules evaluates a rules list directly (without
    persisting it to the collections table) so the smart-collection modal
    can show a live match count as the user edits rules.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='good.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='ok.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p3 = db.add_photo(folder_id=fid, filename='bad.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.update_photo_rating(p1, 5)
    db.update_photo_rating(p2, 4)
    db.update_photo_rating(p3, 1)

    # No rules -> matches every photo in the workspace.
    assert db.count_photos_for_rules([]) == 3

    # rating >= 4 -> two of three.
    assert db.count_photos_for_rules(
        [{"field": "rating", "op": ">=", "value": 4}]
    ) == 2

    # No saved collection row was created.
    assert len(db.get_collections()) == 0


def test_smart_collection_can_select_photos_with_jpeg_companions(tmp_path):
    """Paired JPEG availability is a first-class smart-collection rule even
    though the pair remains one RAW-primary catalog record."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    paired = db.add_photo(
        folder_id=fid, filename="paired.nef", extension=".nef",
        file_size=100, file_mtime=1.0,
    )
    db.add_photo(
        folder_id=fid, filename="raw-only.nef", extension=".nef",
        file_size=100, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET companion_path='paired.jpg' WHERE id=?", (paired,),
    )
    db.conn.commit()

    yes = [{"field": "has_jpeg_companion", "op": "equals", "value": 1}]
    no = [{"field": "has_jpeg_companion", "op": "equals", "value": 0}]
    assert db.count_photos_for_rules(yes) == 1
    assert db.count_photos_for_rules(no) == 1


def test_count_photos_for_rules_rejects_malformed_input(tmp_path):
    """The preview helper raises on input that isn't a list of rule dicts —
    the API route relies on this to return a 400 instead of 500.
    """
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    with pytest.raises(ValueError):
        db.count_photos_for_rules("not a list")
    with pytest.raises(ValueError):
        db.count_photos_for_rules([{"op": "is", "value": 5}])  # missing field
    with pytest.raises(ValueError):
        db.count_photos_for_rules(["not a dict"])
    # Reject value types SQLite cannot bind as a parameter — without this
    # the preview route surfaces a sqlite3.InterfaceError as a 500.
    with pytest.raises(ValueError):
        db.count_photos_for_rules(
            [{"field": "rating", "op": ">=", "value": {"nested": 1}}]
        )
    with pytest.raises(ValueError):
        db.count_photos_for_rules(
            [{"field": "photo_ids", "op": "is", "value": [1, {"nested": 1}]}]
        )
    # Scalar-only fields must reject list values; otherwise SQLite raises
    # ProgrammingError ("type 'list' is not supported") at bind time.
    for field, op in [
        ("rating", ">="),
        ("flag", "is"),
        ("extension", "is"),
        ("color_label", "is"),
    ]:
        with pytest.raises(ValueError):
            db.count_photos_for_rules(
                [{"field": field, "op": op, "value": [1]}]
            )
    # ...but list-accepting fields still work.
    assert db.count_photos_for_rules(
        [{"field": "photo_ids", "op": "is", "value": [1, 2, 3]}]
    ) == 0
    assert db.count_photos_for_rules(
        [{"field": "timestamp", "op": "between",
          "value": ["2020-01-01", "2020-12-31"]}]
    ) == 0


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
    """Collection ``between`` upper: bare-date bound covers the whole day
    (sub-second photos included); a precise-instant bound is treated as
    an exact instant (sub-second photos after it are excluded)."""
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

    # Bare-date upper covers the full named day, including sub-second
    # photos in ``23:59:59``.
    bare_rules = [{"field": "timestamp", "op": "between",
                   "value": ["2024-06-15", "2024-06-15"]}]
    cid_bare = db.add_collection('June 15 bare', json.dumps(bare_rules))
    assert len(db.get_collection_photos(cid_bare)) == 2

    # A precise-instant upper ``2024-06-15T23:59:59`` is treated as
    # exactly that instant — a photo at ``23:59:59.500000`` is strictly
    # after it and must be excluded, matching the semantics of ``<=``
    # on precise instants.
    precise_rules = [{"field": "timestamp", "op": "between",
                      "value": ["2024-06-15", "2024-06-15T23:59:59"]}]
    cid_precise = db.add_collection('June 15 precise', json.dumps(precise_rules))
    matched = {p['filename'] for p in db.get_collection_photos(cid_precise)}
    assert matched == {'b.jpg'}


def test_collection_timestamp_rules_match_ui_shape(tmp_path):
    """Date rule values arrive in the shape the rule modal now serializes:
    'between' as a [YYYY-MM-DD, YYYY-MM-DD] list, 'recent_days' as an int."""
    import json
    from datetime import datetime, timedelta

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')

    # Five photos spanning more than 30 days, including ones on the bare
    # date boundaries the UI's <input type="date"> emits.
    db.add_photo(folder_id=fid, filename='before.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0,
                 timestamp='2024-06-09T23:59:59')
    db.add_photo(folder_id=fid, filename='start.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0,
                 timestamp='2024-06-10T00:00:01')
    db.add_photo(folder_id=fid, filename='middle.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0,
                 timestamp='2024-06-15T12:00:00')
    db.add_photo(folder_id=fid, filename='end.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0,
                 timestamp='2024-06-20T23:59:59.500000')
    db.add_photo(folder_id=fid, filename='after.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0,
                 timestamp='2024-06-21T00:00:01')

    # 'between' with bare YYYY-MM-DD bounds (the literal output of two
    # <input type="date"> fields). Both endpoints must be inclusive,
    # including sub-second timestamps on the upper bound.
    between_rules = [{
        "field": "timestamp", "op": "between",
        "value": ["2024-06-10", "2024-06-20"],
    }]
    cid_between = db.add_collection('Mid June', json.dumps(between_rules))
    matched = {p['filename'] for p in db.get_collection_photos(cid_between)}
    assert matched == {'start.jpg', 'middle.jpg', 'end.jpg'}

    # 'recent_days' with an integer value (the literal output of the
    # number input + parseInt in saveCollection()).
    recent_ts = (datetime.now() - timedelta(days=3)).isoformat()
    old_ts = (datetime.now() - timedelta(days=20)).isoformat()
    db.add_photo(folder_id=fid, filename='days_recent.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0, timestamp=recent_ts)
    db.add_photo(folder_id=fid, filename='days_old.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0, timestamp=old_ts)

    recent_rules = [{"field": "timestamp", "op": "recent_days", "value": 7}]
    cid_recent = db.add_collection('Last week', json.dumps(recent_rules))
    matched_recent = {p['filename'] for p in db.get_collection_photos(cid_recent)}
    assert 'days_recent.jpg' in matched_recent
    assert 'days_old.jpg' not in matched_recent


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


def test_collection_wildlife_excluded_rule(tmp_path):
    """Collections can filter photos marked Not Wildlife."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='wild.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='pet.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.update_photo_wildlife_excluded(p2, True)

    cid = db.add_collection(
        "Wildlife Only",
        json.dumps([{"field": "wildlife_excluded", "op": "equals", "value": 0}]),
    )
    pids = {p["id"] for p in db.get_collection_photos(cid, per_page=999)}
    assert pids == {p1}


def test_default_needs_identification_excludes_not_wildlife(tmp_path, monkeypatch):
    """The default Needs Identification collection omits photos explicitly
    marked Not Wildlife, even when they have no subject keyword."""
    import json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    needs_id = db.add_photo(folder_id=fid, filename='needs.jpg',
                            extension='.jpg', file_size=100, file_mtime=1.0)
    not_wildlife = db.add_photo(folder_id=fid, filename='pet.jpg',
                                extension='.jpg', file_size=100, file_mtime=1.0)
    identified = db.add_photo(folder_id=fid, filename='identified.jpg',
                              extension='.jpg', file_size=100, file_mtime=1.0)
    db.update_photo_wildlife_excluded(not_wildlife, True)
    genre_kid = db.add_keyword("Wildlife", kw_type="genre")
    db.tag_photo(identified, genre_kid)

    db.create_default_collections()
    cid = next(c["id"] for c in db.get_collections()
               if c["name"] == "Needs Identification")
    rules = json.loads(next(c["rules"] for c in db.get_collections()
                            if c["id"] == cid))

    pids = {p["id"] for p in db.get_collection_photos(cid, per_page=999)}
    assert rules == [
        {"field": "has_subject", "op": "equals", "value": 0},
        {"field": "wildlife_excluded", "op": "equals", "value": 0},
    ]
    assert pids == {needs_id}


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


def test_has_subject_rule_counts_legacy_is_species_when_taxonomy_in_subject_types(tmp_path, monkeypatch):
    """Regression: on upgraded DBs, species rows can briefly carry
    ``is_species=1`` with a non-taxonomy ``type`` (e.g. 'general') until
    the background ``mark_species_keywords`` pass retypes them. The
    has_subject rule must treat those legacy rows as identifying when
    'taxonomy' is in subject_types — otherwise already-identified photos
    show up in 'Needs Identification' during that window.
    """
    import json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace(ws_id, config_overrides={"subject_types": ["taxonomy"]})

    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='p2.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    # Plant a legacy-shaped species keyword on p1: is_species=1, type='general'.
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 1)",
        ("Robin",),
    )
    legacy_sp = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, legacy_sp),
    )
    db.conn.commit()

    # has_subject==0 must EXCLUDE p1 (it's identified by the legacy species
    # row) and include only p2.
    cid_unident = db.add_collection(
        "Needs Identification",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 0}]),
    )
    pids_unident = {p["id"] for p in db.get_collection_photos(cid_unident, per_page=999)}
    assert pids_unident == {p2}, (
        "has_subject==0 with 'taxonomy' in subject_types must exclude photos "
        "tagged with legacy is_species=1 keywords whose type hasn't been "
        "retyped to 'taxonomy' yet."
    )

    # has_subject==1 must INCLUDE p1.
    cid_ident = db.add_collection(
        "Identified",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 1}]),
    )
    pids_ident = {p["id"] for p in db.get_collection_photos(cid_ident, per_page=999)}
    assert pids_ident == {p1}


def test_has_subject_rule_ignores_legacy_is_species_when_taxonomy_excluded(tmp_path, monkeypatch):
    """Counter-test: when 'taxonomy' is NOT in subject_types, the
    legacy-species fallback must not fire — a photo with only an
    is_species=1 keyword should still register as not-identified."""
    import json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace(ws_id, config_overrides={"subject_types": ["genre"]})

    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 1)",
        ("Robin",),
    )
    legacy_sp = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, legacy_sp),
    )
    db.conn.commit()

    cid = db.add_collection(
        "Needs Identification",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 0}]),
    )
    pids = {p["id"] for p in db.get_collection_photos(cid, per_page=999)}
    assert pids == {p1}, (
        "When 'taxonomy' is excluded from subject_types, an is_species=1 "
        "keyword must not satisfy has_subject — only the configured types do."
    )


def test_has_subject_rule_rejects_invalid_op_when_subject_types_empty(tmp_path, monkeypatch):
    """When subject_types is empty, a malformed has_subject rule (e.g.
    ``op='contains'``) must still surface as a ValueError so the API
    layer returns 400 — not silently drop the rule with ``None, []``.
    Mirrors the operator guard the other advertised boolean fields get
    via ``_boolean_predicate``.
    """
    import config as cfg
    import pytest
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace(ws_id, config_overrides={"subject_types": []})

    with pytest.raises(ValueError):
        db._build_query_from_rules(
            [{"field": "has_subject", "op": "contains", "value": 1}]
        )


def test_has_subject_rule_empty_subject_types_value_zero_matches_all(tmp_path, monkeypatch):
    """When subject_types is empty, ``has_subject is false`` should match
    every photo (nothing counts as identifying, so nothing has a subject).
    Prior implementation returned ``None, []`` here — semantically the
    same for a lone rule, but the new fail-closed guard routes through
    ``_boolean_predicate`` so we assert the affirmative-matches-all
    branch too.
    """
    import json

    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace(ws_id, config_overrides={"subject_types": []})

    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='p2.jpg', extension='.jpg',
                 file_size=200, file_mtime=2.0)

    cid = db.add_collection(
        "Missing Subject (empty types)",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 0}]),
    )
    photos = db.get_collection_photos(cid, per_page=999)
    assert len(photos) == 2


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


def test_mark_species_keywords_preserves_explicit_location_homonym(tmp_path):
    """A location name that is also a taxon must remain a location.

    ``California`` is both a geographic name and a plant genus. Retyping a
    location hierarchy node as taxonomy makes subsequent Google Place chain
    upserts fail with ``name_conflict``.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    country_id = db.add_keyword("United States", kw_type="location")
    location_id = db.add_keyword(
        "California", parent_id=country_id, kw_type="location",
    )

    class FakeTaxonomy:
        def lookup(self, name):
            if name == "California":
                return {"taxon_id": 123, "scientific_name": "California"}
            return None

    updated = db.mark_species_keywords(FakeTaxonomy())

    assert updated == 0
    row = db.conn.execute(
        "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?",
        (location_id,),
    ).fetchone()
    assert dict(row) == {
        "type": "location",
        "is_species": 0,
        "taxon_id": None,
    }

    leaf_id = db.upsert_place_chain({
        "place_id": "test-california-park",
        "name": "Test California Park",
        "types": ["park"],
        "lat": 32.9,
        "lng": -117.2,
        "address_components": [
            {"name": "United States", "types": ["country"]},
            {
                "name": "California",
                "types": ["administrative_area_level_1"],
            },
        ],
    })
    leaf = db.conn.execute(
        "SELECT type, parent_id FROM keywords WHERE id = ?", (leaf_id,),
    ).fetchone()
    assert dict(leaf) == {"type": "location", "parent_id": location_id}


def test_repair_misclassified_location_ancestors_restores_legacy_homonym(
    tmp_path,
):
    """Repair a California row already corrupted before type preservation."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    country_id = db.add_keyword("United States", kw_type="location")
    state_id = db.add_keyword(
        "California", parent_id=country_id, kw_type="taxonomy",
    )
    db.add_keyword(
        "San Diego County", parent_id=state_id, kw_type="location",
    )

    repaired = db.repair_misclassified_location_ancestors()

    assert repaired == 1
    row = db.conn.execute(
        "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?",
        (state_id,),
    ).fetchone()
    assert dict(row) == {
        "type": "location",
        "is_species": 0,
        "taxon_id": None,
    }
    assert db.repair_misclassified_location_ancestors() == 0


def test_repair_misclassified_location_ancestors_restores_adjacent_nodes(
    tmp_path,
):
    """Repair adjacent taxonomy rows within an existing location hierarchy."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    country_id = db.add_keyword("United States", kw_type="location")
    state_id = db.add_keyword(
        "California", parent_id=country_id, kw_type="taxonomy",
    )
    county_id = db.add_keyword(
        "San Diego County", parent_id=state_id, kw_type="taxonomy",
    )
    db.add_keyword(
        "San Diego", parent_id=county_id, kw_type="location",
    )
    db.conn.execute(
        "UPDATE keywords SET place_id = ?, is_species = 1 WHERE id = ?",
        ("san-diego-county-region", county_id),
    )

    repaired = db.repair_misclassified_location_ancestors()

    assert repaired == 2
    rows = db.conn.execute(
        "SELECT name, type, place_id, is_species FROM keywords "
        "WHERE id IN (?, ?) ORDER BY name",
        (state_id, county_id),
    ).fetchall()
    assert [dict(row) for row in rows] == [
        {
            "name": "California",
            "type": "location",
            "place_id": None,
            "is_species": 0,
        },
        {
            "name": "San Diego County",
            "type": "location",
            "place_id": "san-diego-county-region",
            "is_species": 0,
        },
    ]
    assert db.repair_misclassified_location_ancestors() == 0


def test_repair_misclassified_location_ancestors_restores_root_chain(tmp_path):
    """Repair a taxonomy-typed country at the root of a location hierarchy."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    country_id = db.add_keyword("United States", kw_type="taxonomy")
    state_id = db.add_keyword(
        "California", parent_id=country_id, kw_type="taxonomy",
    )
    db.add_keyword(
        "San Diego County", parent_id=state_id, kw_type="location",
    )

    assert db.repair_misclassified_location_ancestors() == 2
    rows = db.conn.execute(
        "SELECT type FROM keywords WHERE id IN (?, ?) ORDER BY id",
        (country_id, state_id),
    ).fetchall()
    assert [row["type"] for row in rows] == ["location", "location"]


def test_place_upsert_repairs_adjacent_location_ancestors_on_demand(
    tmp_path,
):
    """Adjacent stale taxonomy nodes must not block map assignment."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    country_id = db.add_keyword("United States", kw_type="location")
    state_id = db.add_keyword(
        "California", parent_id=country_id, kw_type="taxonomy",
    )
    county_id = db.add_keyword(
        "San Diego County", parent_id=state_id, kw_type="taxonomy",
    )
    db.add_keyword(
        "San Diego", parent_id=county_id, kw_type="location",
    )

    leaf_id = db.upsert_place_chain({
        "place_id": "nearby-park",
        "name": "Nearby Park",
        "types": ["park"],
        "lat": 32.75,
        "lng": -117.0,
        "address_components": [
            {"name": "United States", "types": ["country"]},
            {
                "name": "California",
                "types": ["administrative_area_level_1"],
            },
            {
                "name": "San Diego County",
                "types": ["administrative_area_level_2"],
            },
        ],
    })

    ancestors = db.conn.execute(
        "SELECT name, type, is_species, taxon_id FROM keywords "
        "WHERE id IN (?, ?) ORDER BY name",
        (state_id, county_id),
    ).fetchall()
    leaf = db.conn.execute(
        "SELECT type, parent_id FROM keywords WHERE id = ?", (leaf_id,),
    ).fetchone()
    assert [dict(row) for row in ancestors] == [
        {
            "name": "California",
            "type": "location",
            "is_species": 0,
            "taxon_id": None,
        },
        {
            "name": "San Diego County",
            "type": "location",
            "is_species": 0,
            "taxon_id": None,
        },
    ]
    assert dict(leaf) == {"type": "location", "parent_id": county_id}


def test_place_upsert_reuses_misclassified_administrative_leaf(tmp_path):
    """Selecting an admin region heals and enriches its hierarchy row."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    country_id = db.add_keyword("United States", kw_type="location")
    state_id = db.add_keyword(
        "California", parent_id=country_id, kw_type="taxonomy",
    )
    county_id = db.add_keyword(
        "San Diego County", parent_id=state_id, kw_type="location",
    )

    selected_id = db.upsert_place_chain({
        "place_id": "california-region",
        "name": "California",
        "types": ["administrative_area_level_1"],
        "lat": 36.7783,
        "lng": -119.4179,
        "address_components": [
            {"name": "United States", "types": ["country"]},
            {
                "name": "California",
                "types": ["administrative_area_level_1"],
            },
        ],
    })

    assert selected_id == state_id
    state = db.conn.execute(
        "SELECT type, place_id, latitude, longitude FROM keywords WHERE id = ?",
        (state_id,),
    ).fetchone()
    assert dict(state) == {
        "type": "location",
        "place_id": "california-region",
        "latitude": 36.7783,
        "longitude": -119.4179,
    }
    assert db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE name = ? AND parent_id = ?",
        ("California", country_id),
    ).fetchone()[0] == 1

    park_id = db.upsert_place_chain({
        "place_id": "nearby-park",
        "name": "Nearby Park",
        "types": ["park"],
        "lat": 32.75,
        "lng": -117.0,
        "address_components": [
            {"name": "United States", "types": ["country"]},
            {
                "name": "California",
                "types": ["administrative_area_level_1"],
            },
            {
                "name": "San Diego County",
                "types": ["administrative_area_level_2"],
            },
        ],
    })

    park = db.conn.execute(
        "SELECT parent_id FROM keywords WHERE id = ?", (park_id,),
    ).fetchone()
    assert park["parent_id"] == county_id


def test_place_upsert_restores_place_bearing_legacy_location(tmp_path):
    """Re-selecting a legacy place id restores its location type directly."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    country_id = db.add_keyword("United States", kw_type="location")
    state_id = db.add_keyword(
        "California", parent_id=country_id, kw_type="taxonomy",
    )
    db.conn.execute(
        "UPDATE keywords SET place_id = ?, is_species = 1 WHERE id = ?",
        ("california-region", state_id),
    )

    selected_id = db.upsert_place_chain({
        "place_id": "california-region",
        "name": "California",
        "types": ["administrative_area_level_1"],
        "lat": 36.7783,
        "lng": -119.4179,
        "address_components": [
            {"name": "United States", "types": ["country"]},
            {
                "name": "California",
                "types": ["administrative_area_level_1"],
            },
        ],
    })

    assert selected_id == state_id
    state = db.conn.execute(
        "SELECT type, is_species, taxon_id, latitude, longitude "
        "FROM keywords WHERE id = ?",
        (state_id,),
    ).fetchone()
    assert dict(state) == {
        "type": "location",
        "is_species": 0,
        "taxon_id": None,
        "latitude": 36.7783,
        "longitude": -119.4179,
    }


def test_place_upsert_reuses_root_administrative_location(tmp_path):
    """Selecting a country enriches its existing root instead of duplicating it."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    country_id = db.add_keyword("United States", kw_type="location")

    selected_id = db.upsert_place_chain({
        "place_id": "united-states-country",
        "name": "United States",
        "types": ["country"],
        "lat": 39.8283,
        "lng": -98.5795,
        "address_components": [
            {"name": "United States", "types": ["country"]},
        ],
    })

    assert selected_id == country_id
    assert db.conn.execute(
        "SELECT COUNT(*) FROM keywords "
        "WHERE name = ? AND parent_id IS NULL AND type = 'location'",
        ("United States",),
    ).fetchone()[0] == 1
    country = db.conn.execute(
        "SELECT place_id, latitude, longitude FROM keywords WHERE id = ?",
        (country_id,),
    ).fetchone()
    assert dict(country) == {
        "place_id": "united-states-country",
        "latitude": 39.8283,
        "longitude": -98.5795,
    }


def test_place_upsert_repairs_root_administrative_location(tmp_path):
    """Selecting a legacy taxonomy root heals it without creating a duplicate."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    country_id = db.add_keyword("United States", kw_type="taxonomy")
    state_id = db.add_keyword(
        "California", parent_id=country_id, kw_type="location",
    )

    selected_id = db.upsert_place_chain({
        "place_id": "united-states-country",
        "name": "United States",
        "types": ["country"],
        "lat": 39.8283,
        "lng": -98.5795,
        "address_components": [
            {"name": "United States", "types": ["country"]},
        ],
    })

    assert selected_id == country_id
    country = db.conn.execute(
        "SELECT type, place_id FROM keywords WHERE id = ?", (country_id,),
    ).fetchone()
    assert dict(country) == {
        "type": "location",
        "place_id": "united-states-country",
    }
    assert db.conn.execute(
        "SELECT parent_id FROM keywords WHERE id = ?", (state_id,),
    ).fetchone()["parent_id"] == country_id
    assert db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE name = ? AND parent_id IS NULL",
        ("United States",),
    ).fetchone()[0] == 1


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
    assert 'GPS Without Location Keyword' in names


def test_default_collections_idempotent(tmp_path):
    """create_default_collections doesn't duplicate if called twice."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    db.create_default_collections()
    db.create_default_collections()

    colls = db.get_collections()
    assert len(colls) == 6


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
    assert 'GPS Without Location Keyword' in names
    assert len(colls) == 6  # no duplicate Flagged


def test_default_collections_for_all_workspaces_adds_missing_defaults(tmp_path):
    """Startup seeding covers non-active workspaces in upgraded databases."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws1 = db.ensure_default_workspace()
    ws2 = db.create_workspace("ws-b")

    # Simulate an upgraded multi-workspace database where only one workspace
    # already had the older default set. The all-workspaces startup pass should
    # add the new GPS/location-keyword default everywhere without duplicating Flagged.
    db.conn.execute(
        "INSERT INTO collections (workspace_id, name, rules) VALUES (?, ?, ?)",
        (
            ws2,
            "Flagged",
            json.dumps([{"field": "flag", "op": "equals", "value": "flagged"}]),
        ),
    )
    db.conn.commit()

    db.set_active_workspace(ws1)
    db.create_default_collections_for_all_workspaces()

    for ws in (ws1, ws2):
        rows = db.conn.execute(
            "SELECT name FROM collections WHERE workspace_id = ?", (ws,),
        ).fetchall()
        names = [r["name"] for r in rows]
        assert "GPS Without Location Keyword" in names
        assert names.count("Flagged") == 1


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
        {"field": "has_subject", "op": "equals", "value": 0},
        {"field": "wildlife_excluded", "op": "equals", "value": 0},
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
        {"field": "has_subject", "op": "equals", "value": 0},
        {"field": "wildlife_excluded", "op": "equals", "value": 0},
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


def test_existing_needs_identification_default_adds_not_wildlife_filter(tmp_path):
    """Existing default Needs Identification collections skip Not Wildlife
    photos after migration."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    db.add_collection(
        "Needs Identification",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 0}]),
    )

    updated = db.migrate_default_needs_identification_collection()

    cols = {c["name"]: json.loads(c["rules"]) for c in db.get_collections()}
    assert updated == 1
    assert cols["Needs Identification"] == [
        {"field": "has_subject", "op": "equals", "value": 0},
        {"field": "wildlife_excluded", "op": "equals", "value": 0},
    ]


def test_needs_identification_not_wildlife_migration_skips_custom_rules(tmp_path):
    """A user-customized Needs Identification collection should not be
    overwritten by the default-rule migration."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    custom = [{"field": "rating", "op": ">=", "value": 3}]
    db.add_collection("Needs Identification", json.dumps(custom))

    updated = db.migrate_default_needs_identification_collection()

    cols = {c["name"]: json.loads(c["rules"]) for c in db.get_collections()}
    assert updated == 0
    assert cols["Needs Identification"] == custom


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
        {"field": "has_subject", "op": "equals", "value": 0},
        {"field": "wildlife_excluded", "op": "equals", "value": 0},
    ]
    # Sanity: default collections include one Needs Identification, not a
    # duplicate alongside the legacy Needs Classification.
    default_names = {"All Photos", "Needs Identification", "Untagged",
                     "Flagged", "Recent Import", "GPS Without Location Keyword"}
    assert default_names.issubset(cols.keys())


def test_default_gps_without_location_keyword_collection_matches_expected_photos(tmp_path):
    """The default GPS/location-keyword collection surfaces geotagged photos
    that still need Vireo's structured location keyword."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    needs_location = db.add_photo(
        folder_id=fid, filename='gps-no-location.jpg',
        extension='.jpg', file_size=100, file_mtime=1.0,
    )
    no_gps = db.add_photo(
        folder_id=fid, filename='no-gps.jpg',
        extension='.jpg', file_size=100, file_mtime=1.0,
    )
    has_location = db.add_photo(
        folder_id=fid, filename='gps-with-location.jpg',
        extension='.jpg', file_size=100, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET latitude = 1.0, longitude = 2.0 WHERE id IN (?, ?)",
        (needs_location, has_location),
    )
    loc_kw = db.add_keyword("Yosemite", kw_type="location")
    db.tag_photo(has_location, loc_kw)

    db.create_default_collections()
    cid = next(c["id"] for c in db.get_collections()
               if c["name"] == "GPS Without Location Keyword")

    pids = {p["id"] for p in db.get_collection_photos(cid, per_page=999)}
    assert pids == {needs_location}


def test_has_location_keyword_rule_and_no_location_information_definition(tmp_path):
    """No Location Information means no EXIF GPS and no location keyword."""
    import json

    from db import NO_LOCATION_INFORMATION_RULES, Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    no_location = db.add_photo(
        folder_id=fid, filename='no-location.jpg',
        extension='.jpg', file_size=100, file_mtime=1.0,
    )
    gps_no_keyword = db.add_photo(
        folder_id=fid, filename='gps-no-keyword.jpg',
        extension='.jpg', file_size=100, file_mtime=1.0,
    )
    no_gps_with_keyword = db.add_photo(
        folder_id=fid, filename='keyword-only.jpg',
        extension='.jpg', file_size=100, file_mtime=1.0,
    )
    gps_with_keyword = db.add_photo(
        folder_id=fid, filename='gps-keyword.jpg',
        extension='.jpg', file_size=100, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET latitude = 1.0, longitude = 2.0 WHERE id IN (?, ?)",
        (gps_no_keyword, gps_with_keyword),
    )
    loc_kw = db.add_keyword("Yosemite", kw_type="location")
    db.tag_photo(no_gps_with_keyword, loc_kw)
    db.tag_photo(gps_with_keyword, loc_kw)

    cid = db.add_collection("No Location Information", json.dumps(NO_LOCATION_INFORMATION_RULES))

    pids = {p["id"] for p in db.get_collection_photos(cid, per_page=999)}
    assert pids == {no_location}


def test_migrate_default_location_collections_renames_and_fixes_exact_legacy_rules(tmp_path):
    """Clarify old location collection names without touching customized rules."""
    import json

    from db import (
        GPS_WITHOUT_LOCATION_KEYWORD_RULES,
        NO_LOCATION_INFORMATION_RULES,
        Database,
    )

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    db.add_collection("Needs Location", json.dumps(GPS_WITHOUT_LOCATION_KEYWORD_RULES))
    db.add_collection(
        "Needs Location",
        json.dumps({"mode": "all", "rules": GPS_WITHOUT_LOCATION_KEYWORD_RULES}),
    )
    db.add_collection(
        "No Location",
        json.dumps({
            "mode": "all",
            "rules": [
                {"field": "location_keyword_missing", "op": "equals", "value": 0},
            ],
        }),
    )
    db.add_collection("No Location", json.dumps([{"field": "rating", "op": ">=", "value": 3}]))

    updated = db.migrate_default_location_collections()

    rows = {
        c["name"]: json.loads(c["rules"])
        for c in db.get_collections()
        if c["name"] != "No Location"
    }
    custom = [
        json.loads(c["rules"])
        for c in db.get_collections()
        if c["name"] == "No Location"
    ]
    assert updated == 3
    assert rows["GPS Without Location Keyword"] == GPS_WITHOUT_LOCATION_KEYWORD_RULES
    assert rows["No Location Information"] == NO_LOCATION_INFORMATION_RULES
    assert custom == [[{"field": "rating", "op": ">=", "value": 3}]]


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


def test_dashboard_scope_combines_folder_collection_and_dates(tmp_path):
    """Dashboard, coverage, and Browse use the same intersected scope."""
    import json

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    park = db.add_folder("/photos/park", name="park")
    yard = db.add_folder("/photos/yard", name="yard")
    db.add_workspace_folder(ws_id, park)
    db.add_workspace_folder(ws_id, yard)

    march = db.add_photo(
        folder_id=park, filename="march.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2024-03-10T08:00:00",
    )
    june_park = db.add_photo(
        folder_id=park, filename="june-park.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2024-06-10T08:00:00",
    )
    june_yard = db.add_photo(
        folder_id=yard, filename="june-yard.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2024-06-11T08:00:00",
    )
    collection_id = db.add_collection(
        "June picks",
        json.dumps([{"field": "photo_ids", "value": [march, june_park, june_yard]}]),
    )

    scope = {
        "collection_id": collection_id,
        "date_from": "2024-06-01",
        "date_to": "2024-06-30",
    }
    stats = db.get_dashboard_stats(**scope)
    assert stats["total_photos"] == 2
    assert stats["folder_count"] == 2
    assert stats["photos_by_month"] == [{"month": "2024-06", "count": 2}]
    assert db.get_coverage_stats(**scope)["total"] == 2
    assert {row["path"] for row in db.get_folder_coverage_stats(**scope)} == {
        "/photos/park", "/photos/yard",
    }

    browse = db.get_photos(folder_id=yard, **scope)
    assert [photo["id"] for photo in browse] == [june_yard]
    assert db.get_photo_ids(folder_id=yard, **scope) == [june_yard]
    assert db.count_filtered_photos(folder_id=yard, **scope) == 1


def test_dashboard_collection_scope_preserves_offline_photos(tmp_path):
    """Dashboard totals for a collection scope keep photos in offline folders.

    The unscoped Dashboard intentionally counts photos in missing folders
    (metadata-only aggregates like total_photos, photos_by_month, etc.).
    Scoping by a collection whose rules match those photos must not
    silently drop them via the collection subquery's folder-status filter.
    """
    import json

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    ok_folder = db.add_folder("/photos/ok", name="ok")
    gone_folder = db.add_folder("/photos/gone", name="gone")
    db.add_workspace_folder(ws_id, ok_folder)
    db.add_workspace_folder(ws_id, gone_folder)

    visible = db.add_photo(
        folder_id=ok_folder, filename="here.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2024-06-10T08:00:00",
    )
    offline = db.add_photo(
        folder_id=gone_folder, filename="offline.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2024-06-11T08:00:00",
    )
    collection_id = db.add_collection(
        "All photos",
        json.dumps([{"field": "photo_ids", "value": [visible, offline]}]),
    )

    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (gone_folder,),
    )
    db.conn.commit()

    stats = db.get_dashboard_stats(collection_id=collection_id)
    assert stats["total_photos"] == 2, (
        "Dashboard totals must count the offline photo when scoped by "
        f"a collection that matches it; got {stats['total_photos']}"
    )
    assert stats["accessible_photos"] == 1
    assert stats["missing_folder_count"] == 1
    assert stats["attention"]["unclassified"] == 1
    assert stats["attention"]["missing_location"] == 1
    months = {row["month"]: row["count"] for row in stats["photos_by_month"]}
    assert months.get("2024-06") == 2

    # Coverage still restricts to accessible folders in its outer join, so
    # the collection scope must not further shrink that count either.
    assert db.get_coverage_stats(collection_id=collection_id)["total"] == 1

    # Regression guard: Browse and pipeline callers must still filter out
    # photos in offline folders even when the collection rules match them.
    assert db.count_collection_photos(collection_id) == 1
    assert [p["id"] for p in db.get_collection_photos(collection_id)] == [visible]


def test_dashboard_attention_counts_actionable_gaps_in_scope(tmp_path):
    """Needs Attention cards report preview, sync, location, and duplicate work."""
    db, pids = _make_workspace_with_photos(tmp_path, [
        {"timestamp": "2024-06-01T08:00:00", "file_hash": "same"},
        {"timestamp": "2024-06-02T08:00:00", "file_hash": "same"},
        {"timestamp": "2024-07-01T08:00:00", "file_hash": "other"},
    ])
    det_ids = db.save_detections(pids[0], [{
        "box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4},
        "confidence": 0.9,
        "category": "animal",
    }], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.95, "test")
    db.preview_cache_insert(pids[0], 1920, 100)
    db.conn.execute(
        "INSERT INTO pending_changes "
        "(photo_id, change_type, value, change_token, workspace_id) "
        "VALUES (?, 'rating', '4', 'token', ?)",
        (pids[1], db._ws_id()),
    )
    db.conn.commit()

    attention = db.get_dashboard_stats(
        date_from="2024-06-01", date_to="2024-06-30",
    )["attention"]
    assert attention == {
        "unclassified": 1,
        "missing_location": 2,
        "missing_previews": 1,
        "preview_size": 1920,
        "preview_enabled": True,
        "pending_sync": 1,
        "duplicate_groups": 1,
    }


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


def test_folder_coverage_keeps_zero_match_folders_with_scopes(tmp_path):
    """Photo filters keep in-scope folders visible while folder scope narrows rows."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    folder_a = db.add_folder("/A", name="A")
    folder_b = db.add_folder("/B", name="B")
    db.add_workspace_folder(ws_id, folder_a)
    db.add_workspace_folder(ws_id, folder_b)
    db.add_photo(
        folder_id=folder_a, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2024-01-01T00:00:00",
    )
    db.add_photo(
        folder_id=folder_b, filename="b.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp="2024-01-02T00:00:00",
    )

    no_matches = db.get_folder_coverage_stats(date_from="2024-02-01")
    assert {row["path"]: row["total"] for row in no_matches} == {
        "/A": 0,
        "/B": 0,
    }

    folder_scoped = db.get_folder_coverage_stats(
        folder_id=folder_a, date_from="2024-02-01",
    )
    assert [(row["path"], row["total"]) for row in folder_scoped] == [
        ("/A", 0),
    ]


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


def test_get_geolocated_photos_keyword_search_options(tmp_path):
    """Map keyword filtering uses the shared whole-word and case options."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='western.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='tern.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=1.0, longitude=2.0 WHERE id IN (?,?)", (p1, p2))
    db.conn.commit()
    db.tag_photo(p1, db.add_keyword('Western Tanager'))
    db.tag_photo(p2, db.add_keyword('Common Tern'))

    assert {
        r['filename'] for r in db.get_geolocated_photos(keyword='tern')
    } == {'western.jpg', 'tern.jpg'}
    assert {
        r['filename']
        for r in db.get_geolocated_photos(keyword='tern', keyword_whole_word=True)
    } == {'tern.jpg'}
    assert {
        r['filename']
        for r in db.get_geolocated_photos(keyword='Tern', keyword_match_case=True)
    } == {'tern.jpg'}


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
    det_ids = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.95, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.3, "h": 0.4}, "confidence": 0.60, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    db.add_prediction(det_ids[1], "Sparrow", 0.60, 'bioclip')
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


def test_get_geolocated_photos_falls_back_to_keyword_coords(tmp_path):
    """Photos without EXIF GPS but tagged with a location keyword that has
    coords are returned with the keyword's coords and coord_source='keyword'."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(folder_id=fid, filename='no_gps.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    # No EXIF GPS — leave latitude/longitude as NULL.

    # Create a type='location' keyword with coords.
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) VALUES (?, 'location', ?, ?)",
        ('Central Park', 40.7829, -73.9654),
    )
    kid = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, kid),
    )
    db.conn.commit()

    rows = db.get_geolocated_photos()
    assert len(rows) == 1
    r = rows[0]
    assert r['filename'] == 'no_gps.jpg'
    assert r['latitude'] == 40.7829
    assert r['longitude'] == -73.9654
    assert r['coord_source'] == 'keyword'
    assert r['keyword_location_name'] == 'Central Park'


def test_get_geolocated_photos_prefers_exif_over_keyword(tmp_path):
    """When a photo has both EXIF coords AND a location keyword, EXIF wins."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(folder_id=fid, filename='both.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET latitude=?, longitude=? WHERE id=?",
                    (37.7749, -122.4194, pid))

    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) VALUES (?, 'location', ?, ?)",
        ('New York', 40.7128, -74.0060),
    )
    kid = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, kid),
    )
    db.conn.commit()

    rows = db.get_geolocated_photos()
    assert len(rows) == 1
    r = rows[0]
    assert r['latitude'] == 37.7749
    assert r['longitude'] == -122.4194
    assert r['coord_source'] == 'exif'
    assert r['keyword_location_name'] is None


def test_get_geolocated_photos_excludes_photos_with_neither(tmp_path):
    """Photos with neither EXIF coords nor a coord-bearing location keyword
    should not appear in results."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    # Photo 1: no GPS, no keyword link at all.
    p1 = db.add_photo(folder_id=fid, filename='lonely.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    # Photo 2: no GPS, has a location keyword but the keyword has no coords
    # (free-text fallback).
    p2 = db.add_photo(folder_id=fid, filename='free_text.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'location')",
        ('the dog park',),
    )
    kid = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p2, kid),
    )
    db.conn.commit()

    rows = db.get_geolocated_photos()
    assert rows == []


def test_get_geolocated_photos_with_partial_exif_uses_keyword_pair(tmp_path):
    """A photo with only one EXIF axis populated must NOT mix EXIF lat with
    keyword lng (or vice versa) — that produces a wrong marker location.
    The fallback decision is paired: either both EXIF axes win, or both
    keyword axes win."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(folder_id=fid, filename='partial.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    # Partial EXIF: latitude present, longitude missing (sometimes seen on
    # corrupt EXIF or when only one half of GPS lat/lng was decoded).
    db.conn.execute(
        "UPDATE photos SET latitude=?, longitude=NULL WHERE id=?",
        (37.7749, pid),
    )
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) "
        "VALUES (?, 'location', ?, ?)",
        ('Central Park', 40.7829, -73.9654),
    )
    kid = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, kid),
    )
    db.conn.commit()

    rows = db.get_geolocated_photos()
    assert len(rows) == 1
    r = rows[0]
    # Coords must come as a pair from the keyword (not 37.7749 + -73.9654,
    # which would put the marker in the middle of the Pacific).
    assert r['latitude'] == 40.7829, "expected paired keyword lat, got mixed"
    assert r['longitude'] == -73.9654, "expected paired keyword lng, got mixed"
    assert r['coord_source'] == 'keyword'
    assert r['keyword_location_name'] == 'Central Park'


def test_get_effective_photo_location_prefers_exif(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='both.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute(
        "UPDATE photos SET latitude=?, longitude=? WHERE id=?",
        (37.7749, -122.4194, pid),
    )
    kid = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) "
        "VALUES (?, 'location', ?, ?)",
        ('Paris Airbnb', 48.8566, 2.3522),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, kid),
    )
    db.conn.commit()

    loc = db.get_effective_photo_location(pid)
    assert loc["source"] == "exif"
    assert loc["latitude"] == 37.7749
    assert loc["longitude"] == -122.4194
    assert loc["keyword_location_name"] is None


def test_get_effective_photo_location_falls_back_to_keyword_pair(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='assigned.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute(
        "UPDATE photos SET latitude=?, longitude=NULL WHERE id=?",
        (37.7749, pid),
    )
    kid = db.conn.execute(
        "INSERT INTO keywords (name, type, place_id, latitude, longitude) "
        "VALUES (?, 'location', ?, ?, ?)",
        ('Paris Airbnb', 'place_123', 48.8566, 2.3522),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, kid),
    )
    db.conn.commit()

    loc = db.get_effective_photo_location(pid)
    assert loc["source"] == "keyword"
    assert loc["latitude"] == 48.8566
    assert loc["longitude"] == 2.3522
    assert loc["keyword_location_name"] == "Paris Airbnb"
    assert loc["place_id"] == "place_123"


def test_get_effective_photo_location_enforces_active_workspace(tmp_path):
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='geo.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute(
        "UPDATE photos SET latitude=?, longitude=? WHERE id=?",
        (37.7749, -122.4194, pid),
    )
    db.conn.commit()

    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)

    with pytest.raises(ValueError, match="active workspace"):
        db.get_effective_photo_location(pid)


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
    det_ids = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.95, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.3, "h": 0.4}, "confidence": 0.60, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    db.add_prediction(det_ids[1], 'Cooper\'s Hawk', 0.60, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1])
    for pr in preds:
        db.accept_prediction(pr['id'])

    species = db.get_accepted_species()
    # Both species keywords tagged on the photo appear, alphabetical.
    assert species == ["Cooper's Hawk", 'Red-tailed Hawk']


def test_get_accepted_species_includes_keyword_coord_photos(tmp_path):
    """A photo without EXIF GPS but with a 'location'-type keyword that has
    coords must still surface its species in the dropdown — get_geolocated_photos
    renders such photos as map markers, so the filter list has to match."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    # No EXIF GPS — only a location keyword with coords.
    loc_id = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) "
        "VALUES (?, 'location', ?, ?)",
        ("Central Park", 40.785091, -73.968285),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, loc_id),
    )
    db.conn.commit()

    det_ids = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1])
    for pr in preds:
        db.accept_prediction(pr['id'])

    assert db.get_accepted_species() == ['Red-tailed Hawk']


def test_get_accepted_species_ignores_coordless_location_keywords(tmp_path):
    """A 'location'-type keyword *without* coords (free-text "the meadow")
    is not enough — get_geolocated_photos won't render the photo, so the
    species filter shouldn't list it either."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)
    p1 = db.add_photo(folder_id=fid, filename='hawk.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    # Free-text location keyword: no coords.
    loc_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'location')",
        ("the meadow behind the cabin",),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, loc_id),
    )
    db.conn.commit()

    det_ids = db.save_detections(p1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Red-tailed Hawk', 0.95, 'bioclip')
    preds = db.get_predictions(photo_ids=[p1])
    for pr in preds:
        db.accept_prediction(pr['id'])

    assert db.get_accepted_species() == []


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


def test_count_photos_without_gps_excludes_keyword_coord_photos(tmp_path):
    """A photo with no EXIF GPS but a coord-bearing location keyword IS
    plottable on the map — count_photos_without_gps must not count it,
    so total_with_gps stays >= len(get_geolocated_photos())."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(db._active_workspace_id, fid)

    # Photo 1: full EXIF GPS — definitely plottable.
    db.add_photo(folder_id=fid, filename='exif.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.conn.execute(
        "UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE filename='exif.jpg'"
    )
    # Photo 2: no EXIF, but tagged with a coord-bearing location keyword.
    p2 = db.add_photo(folder_id=fid, filename='via_keyword.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) "
        "VALUES ('Central Park', 'location', 40.78, -73.96)",
    )
    kid = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p2, kid),
    )
    # Photo 3: no EXIF, tagged with a free-text (coordless) location keyword.
    p3 = db.add_photo(folder_id=fid, filename='free_text.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES ('the dog park', 'location')",
    )
    free_kid = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p3, free_kid),
    )
    # Photo 4: nothing — truly without GPS.
    db.add_photo(folder_id=fid, filename='lonely.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.conn.commit()

    # Photos 3 and 4 are not plottable (no EXIF, no coord-bearing keyword).
    assert db.count_photos_without_gps() == 2

    # Sanity: get_geolocated_photos returns the two plottable photos
    # (Photo 1 via EXIF, Photo 2 via keyword fallback). Total of plottable
    # (= 4 photos - 2 without_gps = 2) matches len(get_geolocated_photos).
    geo = db.get_geolocated_photos()
    assert len(geo) == 2


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


def test_migration_adds_missing_miss_classifier_columns(tmp_path):
    """DBs created before the miss-classifier columns existed must have
    miss_no_subject/miss_clipped/miss_oof/miss_computed_at added on open —
    otherwise PHOTO_COLS-based queries fail with 'no such column'."""
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)

    # Simulate a pre-miss-classifier DB by dropping the columns and
    # closing. Reopening must trigger the ALTER TABLE fallback.
    for col in ("miss_no_subject", "miss_clipped", "miss_oof",
                "miss_computed_at"):
        db.conn.execute(f"ALTER TABLE photos DROP COLUMN {col}")
    db.conn.commit()
    db.conn.close()

    db2 = Database(db_path)
    cols = {row[1] for row in db2.conn.execute("PRAGMA table_info(photos)")}
    for expected in (
        "miss_no_subject", "miss_clipped", "miss_oof", "miss_computed_at"
    ):
        assert expected in cols, f"migration failed to add {expected}"

    # PHOTO_COLS-based queries must succeed against the migrated DB.
    row = db2.conn.execute(
        f"SELECT {Database.PHOTO_COLS} FROM photos"
    ).fetchone()
    assert row is not None
    assert row["miss_no_subject"] is None
    assert row["miss_clipped"] is None
    assert row["miss_oof"] is None


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


def test_add_keyword_auto_detects_taxonomy_with_smart_apostrophe(tmp_path):
    """Smart-apostrophe user text matches straight-apostrophe taxa names."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank) "
        "VALUES (1, 'Sayornis saya', ?, 'species')",
        ("Say's Phoebe",),
    )
    db.conn.commit()

    kid = db.add_keyword("Say’s phoebe")
    row = db.conn.execute(
        "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
    assert row["taxon_id"] == 1


def test_add_keyword_promotes_existing_general_smart_apostrophe_taxon(tmp_path):
    """A legacy general row is promoted when auto-detect can now resolve it."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword("Say’s phoebe")
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank) "
        "VALUES (1, 'Sayornis saya', ?, 'species')",
        ("Say's Phoebe",),
    )
    db.conn.commit()

    assert db.add_keyword("Say’s phoebe") == kid
    row = db.conn.execute(
        "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
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
        "SELECT * FROM detections WHERE photo_id = ?",
        (pid,),
    ).fetchall()
    assert len(rows) == 2
    # Content-addressed IDs aren't insertion-ordered, so compare as a set.
    assert {round(r["box_x"], 4) for r in rows} == {0.1, 0.5}


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


def test_replace_prediction_preserves_equivalent_hierarchy_target(tmp_path):
    """Replacing stale species leaves an equivalent hierarchical target in
    place instead of flattening it to the root keyword."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos")
    pid = db.add_photo(fid, "verdin.jpg", ".jpg", 100, 1.0)
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    stale = db.add_keyword("Sparrow", is_species=True)
    db.tag_photo(pid, nested)
    db.tag_photo(pid, stale)
    detection_id = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(detection_id, "Verdin", 0.95, "bioclip")
    prediction_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (detection_id,),
    ).fetchone()["id"]

    result = db.accept_prediction(prediction_id, replace_species=True)

    root = result["keyword_id"]
    tagged = {row["id"] for row in db.get_photo_keywords(pid)}
    assert nested in tagged
    assert root not in tagged
    assert stale not in tagged


def test_replace_prediction_records_affected_when_only_removals_occur(tmp_path):
    """Replace Keywords on a photo that already carries the accepted target
    via a hierarchy leaf must still record the photo in ``affected`` when
    stale species are removed.

    ``/api/predictions/<id>/replace-keywords`` builds its edit-history items
    from ``result['affected']`` alone, so a photo that gets stale species
    stripped but no new tag added must still surface; otherwise the removed
    species has no audit entry and undo cannot restore it.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos")
    pid = db.add_photo(fid, "verdin.jpg", ".jpg", 100, 1.0)
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    stale = db.add_keyword("Sparrow", is_species=True)
    db.tag_photo(pid, nested)
    db.tag_photo(pid, stale)
    detection_id = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(detection_id, "Verdin", 0.95, "bioclip")
    prediction_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (detection_id,),
    ).fetchone()["id"]

    result = db.accept_prediction(prediction_id, replace_species=True)

    # Even though no new tag was added (hierarchy leaf already satisfies the
    # target), the stale Sparrow removal must be visible in ``affected`` so
    # the API layer can record it.
    assert len(result["affected"]) == 1
    entry = result["affected"][0]
    assert entry["photo_id"] == pid
    assert entry["old_species"] == ["Sparrow"]
    # And the keyword_remove pending change was actually queued.
    removed = {
        (c["photo_id"], c["value"])
        for c in db.get_pending_changes()
        if c["change_type"] == "keyword_remove"
    }
    assert (pid, "Sparrow") in removed


def test_replace_prediction_migrates_curation_from_canonical_root_of_alias(tmp_path):
    """When Replace Keywords strips a hierarchy alias whose taxon has curation
    keyed on the canonical root spelling, highlights and representative
    preferences must migrate onto the newly-tagged species.

    ``repair_duplicate_photo_species`` leaves the alias leaf attached (say
    ``Desert Verdin`` under ``Penduline tits``) and detaches the top-level
    ``Verdin`` — but curation was preserved under the canonical root name
    ``Verdin`` because that is where ``_canonical_curation_species`` keeps
    it. Renaming the curation from only the raw alias ``Desert Verdin``
    would leave ``species_highlights`` / ``photo_preferences`` stranded
    under ``Verdin`` after the photo is retagged to a different species.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    taxa = _seed_taxa(
        db,
        [
            (2912, "Auriparus flaviceps", "Verdin"),
            (19860, "Campylorhynchus brunneicapillus", "Cactus Wren"),
        ],
    )
    fid = db.add_folder("/photos")
    pid = db.add_photo(fid, "verdin.jpg", ".jpg", 100, 1.0)
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Desert Verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], nested),
    )
    # Root species row exists (canonical curation key) but is NOT tagged on
    # the photo — the post-repair layout after the redundant root was
    # detached.
    db.add_keyword("Verdin", is_species=True)
    db.conn.commit()
    db.tag_photo(pid, nested)

    # Curation lives under the canonical root spelling that repair kept as
    # the species key, not under the raw hierarchy alias name.
    db.conn.execute(
        """INSERT INTO photo_preferences
             (workspace_id, purpose, species, photo_id)
           VALUES (?, ?, ?, ?)""",
        (ws_id, "highlights", "Verdin", pid),
    )
    db.conn.execute(
        """INSERT INTO species_highlights
             (workspace_id, species, photo_id, rank,
              created_at, updated_at)
           VALUES (?, ?, ?, 1, datetime('now'), datetime('now'))""",
        (ws_id, "Verdin", pid),
    )
    db.conn.commit()

    detection_id = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(detection_id, "Cactus Wren", 0.95, "bioclip")
    prediction_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (detection_id,),
    ).fetchone()["id"]

    result = db.accept_prediction(prediction_id, replace_species=True)

    names = {k["name"] for k in db.get_photo_keywords(pid)}
    assert "Cactus Wren" in names
    assert "Desert Verdin" not in names

    # Sidecar remove still uses the raw alias name (XMP carries it, not the
    # canonical root spelling).
    removed = {
        (c["photo_id"], c["value"])
        for c in db.get_pending_changes()
        if c["change_type"] == "keyword_remove"
    }
    assert (pid, "Desert Verdin") in removed
    assert result["affected"][0]["old_species"] == ["Desert Verdin"]

    # Curation migrated from the canonical root Verdin onto Cactus Wren.
    pref_species = {
        row["species"] for row in db.conn.execute(
            "SELECT species FROM photo_preferences WHERE photo_id = ?",
            (pid,),
        ).fetchall()
    }
    assert "Cactus Wren" in pref_species
    assert "Verdin" not in pref_species

    hl_species = {
        row["species"] for row in db.conn.execute(
            "SELECT species FROM species_highlights WHERE photo_id = ?",
            (pid,),
        ).fetchall()
    }
    assert "Cactus Wren" in hl_species
    assert "Verdin" not in hl_species


def test_replace_prediction_removes_ambiguous_legacy_homonym_row(tmp_path):
    """When Replace Keywords accepts a linked target and the photo carries
    an unlinked same-key homonym row, the ambiguous row must be treated as
    a stale species and removed.

    Regression: the target-species check treated any NULL-taxon same-key row
    as equivalent to the linked target, so with a legacy ``Robin`` alongside
    taxonomy ``robin``/``ROBIN`` bound to different taxa, the legacy row was
    kept while the correct linked target was added, leaving the wrong
    species attached.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(
        db,
        [
            (18001, "Erithacus rubecula", "European Robin"),
            (18002, "Turdus migratorius", "American Robin"),
        ],
    )
    fid = db.add_folder("/photos")
    pid = db.add_photo(fid, "robin.jpg", ".jpg", 100, 1.0)

    # Two distinct linked species keywords share the NOCASE match key
    # "robin" but resolve to different taxa. add_keyword dedupes case-
    # insensitively so INSERT directly to preserve both rows.
    european = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('robin', 'taxonomy', 1, ?)",
        (taxa["European Robin"],),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Robin', 'taxonomy', 1, ?)",
        (taxa["American Robin"],),
    )
    # An unlinked, typed legacy row with the same match key. It could be
    # either species; Replace Keywords must not treat it as authoritatively
    # equivalent to the target and must strip it as a stale species.
    legacy = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('ROBIN', 'taxonomy', 1, NULL)"
    ).lastrowid
    db.tag_photo(pid, legacy)
    db.conn.commit()

    detection_id = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(detection_id, "robin", 0.95, "bioclip")
    prediction_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (detection_id,),
    ).fetchone()["id"]

    result = db.accept_prediction(prediction_id, replace_species=True)

    tagged = {row["id"] for row in db.get_photo_keywords(pid)}
    assert european in tagged, (
        "Linked target must be attached even though a same-key legacy row "
        "existed"
    )
    assert legacy not in tagged, (
        "Ambiguous NULL-taxon same-key row must be removed — its identity "
        "cannot be assumed to equal the linked target"
    )
    assert len(result["affected"]) == 1
    entry = result["affected"][0]
    assert entry["photo_id"] == pid
    assert "ROBIN" in entry["old_species"]


def test_replace_prediction_unlinked_target_preserves_linked_homonym(tmp_path):
    """When the accept target is an unlinked legacy species row and the
    photo also carries a distinct LINKED same-key homonym, Replace Keywords
    must not fold the linked row into the target.

    Regression: the target-species check treated any same-key row as the
    target in the unlinked-target branch, so a linked ``robin`` on the
    photo was silently kept as "already the target" (excluded from
    ``to_remove``) while the intended unlinked target was added alongside,
    leaving the wrong species attached.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(
        db,
        [
            (18101, "Turdus migratorius", "American Robin"),
        ],
    )
    fid = db.add_folder("/photos")
    pid = db.add_photo(fid, "robin.jpg", ".jpg", 100, 1.0)

    # An unlinked legacy species keyword — this is the accept target.
    legacy_target = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Robin', 'taxonomy', 1, NULL)"
    ).lastrowid
    # A distinct linked same-key row exists in the catalog and is
    # attached to the photo. Its identity is a different species; the
    # unlinked target cannot claim it.
    linked_homonym = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('robin', 'taxonomy', 1, ?)",
        (taxa["American Robin"],),
    ).lastrowid
    db.tag_photo(pid, linked_homonym)
    db.conn.commit()

    detection_id = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(detection_id, "Robin", 0.95, "bioclip")
    prediction_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (detection_id,),
    ).fetchone()["id"]

    # ``add_keyword`` NOCASE-dedupes among same-parent taxonomy rows and
    # tie-breaks by id, so the earlier-inserted ``legacy_target`` wins and
    # becomes the resolved accept target ``kid``. The replace-species
    # branch must recognise that the linked ``robin`` on the photo is a
    # distinct species, not the unlinked target — and untag it.
    db.accept_prediction(prediction_id, replace_species=True)

    tagged = {row["id"] for row in db.get_photo_keywords(pid)}
    assert legacy_target in tagged, (
        "Unlinked accept target must be attached to the photo"
    )
    assert linked_homonym not in tagged, (
        "Linked same-key homonym is a distinct species and must not be "
        "folded into the unlinked target: it must be included in "
        "to_remove and untagged from the photo"
    )


def test_accept_subject_species_preserves_existing_tag_and_accepts_models(tmp_path):
    """An additional subject species is added without replacing the original.

    Agreeing predictions from different models on the same detection are
    resolved together.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/photos")
    photo_id = db.add_photo(
        folder_id=folder_id, filename="ducks.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    wigeon_id = db.add_keyword("American Wigeon", is_species=True)
    db.tag_photo(photo_id, wigeon_id)
    detection_id = db.save_detections(
        photo_id,
        [{
            "box": {"x": 0.5, "y": 0.4, "w": 0.2, "h": 0.2},
            "confidence": 0.6,
            "category": "animal",
        }],
        detector_model="MDV6",
    )[0]
    db.add_prediction(
        detection_id, "Blue-winged Teal", 0.91, "bioclip",
        labels_fingerprint="fp",
    )
    db.add_prediction(
        detection_id, "Blue-winged Teal", 0.87, "inat",
        labels_fingerprint="fp",
    )
    target = next(
        row for row in db.get_predictions()
        if row["classifier_model"] == "bioclip"
    )

    result = db.accept_subject_species(target["id"])

    assert result["species"] == "Blue-winged Teal"
    assert len(result["prediction_ids"]) == 2
    # Each underlying accept_prediction call reports an entry: the first
    # tags Blue-winged Teal (``changed_tag=True``), the second sees the
    # photo already carrying it and records a status-only accept
    # (``changed_tag=False``) so the aggregate accept-subject history
    # can still reverse every sibling status on undo.
    assert len(result["affected"]) == 2
    changed = [a["changed_tag"] for a in result["affected"]]
    assert changed.count(True) == 1
    assert changed.count(False) == 1
    assert {row["name"] for row in db.get_photo_keywords(photo_id)} >= {
        "American Wigeon", "Blue-winged Teal",
    }
    statuses = {
        row["classifier_model"]: row["status"]
        for row in db.get_predictions(photo_ids=[photo_id])
    }
    assert statuses == {"bioclip": "accepted", "inat": "accepted"}


def test_replace_species_preserves_other_subject_on_multi_detection_photo(tmp_path):
    """Replacing one subject's species must not strip a species that belongs
    to a different detection (subject) on the same photo.

    Regression: accept_prediction(replace_species=True) deleted every species
    keyword on the photo, so correcting the teal box's ID wiped the American
    Wigeon confirmed on the other box.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid, filename="ducks.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    wigeon = db.add_keyword("American Wigeon", is_species=True)
    stale_teal = db.add_keyword("Green-winged Teal", is_species=True)
    db.tag_photo(pid, wigeon)
    db.tag_photo(pid, stale_teal)

    d_wigeon, d_teal = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.6, "y": 0.5, "w": 0.2, "h": 0.2},
         "confidence": 0.8, "category": "animal"},
    ], detector_model="MDV6")
    # The wigeon box keeps a live prediction naming the wigeon; the teal box's
    # real identity is Blue-winged Teal.
    db.add_prediction(
        d_wigeon, "American Wigeon", 0.99, "bioclip", labels_fingerprint="fp",
    )
    db.add_prediction(
        d_teal, "Blue-winged Teal", 0.95, "bioclip", labels_fingerprint="fp",
    )
    teal_pred = next(
        r for r in db.get_predictions(photo_ids=[pid])
        if r["detection_id"] == d_teal
    )

    result = db.accept_prediction(teal_pred["id"], replace_species=True)

    names = {k["name"] for k in db.get_photo_keywords(pid)}
    assert "Blue-winged Teal" in names        # this subject's corrected ID
    assert "American Wigeon" in names          # other subject preserved
    assert "Green-winged Teal" not in names    # this subject's stale ID gone
    # The stripped species is reported (and only it), so the sidecar remove is
    # queued for the stale teal but not the still-tagged wigeon.
    assert result["affected"][0]["old_species"] == ["Green-winged Teal"]
    removed = {
        (c["photo_id"], c["value"])
        for c in db.get_pending_changes()
        if c["change_type"] == "keyword_remove"
    }
    assert (pid, "Green-winged Teal") in removed
    assert (pid, "American Wigeon") not in removed


def test_replace_species_ignores_stale_fingerprint_predictions_on_neighbour(tmp_path):
    """The protected-species query must only consider the neighbouring
    detection's *current* labels_fingerprint. A re-classified detection can
    still carry pending predictions from an older label set naming a species
    that the current run no longer produces; those stale rows must not
    shield an obsolete species keyword from replace_species.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid, filename="ducks.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # The photo carries a stale species tag left over from when the neighbour
    # was previously classified as Green-winged Teal.
    fresh_wigeon = db.add_keyword("American Wigeon", is_species=True)
    stale_teal = db.add_keyword("Green-winged Teal", is_species=True)
    db.tag_photo(pid, fresh_wigeon)
    db.tag_photo(pid, stale_teal)

    d_target, d_neighbour = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.6, "y": 0.5, "w": 0.2, "h": 0.2},
         "confidence": 0.8, "category": "animal"},
    ], detector_model="MDV6")
    # Neighbour's *old* fingerprint still has a pending row naming the stale
    # species — the kind of row get_predictions() filters out.
    db.add_prediction(
        d_neighbour, "Green-winged Teal", 0.90, "bioclip",
        labels_fingerprint="fp_old",
    )
    # Neighbour's *current* fingerprint names the wigeon; the target box was
    # classified as the wrong teal and needs correction to Blue-winged Teal.
    db.add_prediction(
        d_neighbour, "American Wigeon", 0.95, "bioclip",
        labels_fingerprint="fp_new",
    )
    db.add_prediction(
        d_target, "Blue-winged Teal", 0.92, "bioclip",
        labels_fingerprint="fp_new",
    )
    target_pred = next(
        r for r in db.get_predictions(photo_ids=[pid])
        if r["detection_id"] == d_target
    )

    result = db.accept_prediction(target_pred["id"], replace_species=True)

    names = {k["name"] for k in db.get_photo_keywords(pid)}
    assert "Blue-winged Teal" in names
    assert "American Wigeon" in names          # protected by current fp
    assert "Green-winged Teal" not in names    # stale fp must not protect it
    assert "Green-winged Teal" in result["affected"][0]["old_species"]
    removed = {
        (c["photo_id"], c["value"])
        for c in db.get_pending_changes()
        if c["change_type"] == "keyword_remove"
    }
    assert (pid, "Green-winged Teal") in removed


def test_replace_species_normalizes_protected_species_before_matching(tmp_path):
    """The neighbour's *live* prediction species must be folded through the
    same keyword-normalization as the photo's stored keyword before deciding
    what to protect.

    Regression: predictions.species stores the raw model output (e.g.
    `‘apapane` with a leading edge quote), while add_keyword normalizes on
    write so the photo actually carries the clean keyword `apapane`. A
    naive `lower(trim(species))` fold on the SQL side would leave the raw
    edge quote in place, `keyword.name.strip().lower() not in protected`
    would be True, and replace_species would queue a keyword_remove for
    the still-live neighbouring subject.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid, filename="ducks.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Photo carries the neighbour's species under the normalized (clean)
    # spelling — the shape add_keyword actually writes.
    apapane = db.add_keyword("apapane", is_species=True)
    stale = db.add_keyword("Green-winged Teal", is_species=True)
    db.tag_photo(pid, apapane)
    db.tag_photo(pid, stale)

    d_target, d_neighbour = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.6, "y": 0.5, "w": 0.2, "h": 0.2},
         "confidence": 0.8, "category": "animal"},
    ], detector_model="MDV6")
    # Neighbour's live prediction still carries the raw edge-quote form
    # that predictions.species records verbatim.
    db.add_prediction(
        d_neighbour, "‘apapane", 0.95, "bioclip",
        labels_fingerprint="fp",
    )
    db.add_prediction(
        d_target, "Blue-winged Teal", 0.92, "bioclip",
        labels_fingerprint="fp",
    )
    target_pred = next(
        r for r in db.get_predictions(photo_ids=[pid])
        if r["detection_id"] == d_target
    )

    result = db.accept_prediction(target_pred["id"], replace_species=True)

    names = {k["name"] for k in db.get_photo_keywords(pid)}
    assert "Blue-winged Teal" in names
    assert "apapane" in names                  # protected across normalization
    assert "Green-winged Teal" not in names    # this subject's stale ID gone
    assert result["affected"][0]["old_species"] == ["Green-winged Teal"]
    removed = {
        (c["photo_id"], c["value"])
        for c in db.get_pending_changes()
        if c["change_type"] == "keyword_remove"
    }
    assert (pid, "Green-winged Teal") in removed
    assert (pid, "apapane") not in removed


def test_replace_species_ignores_alternative_prediction_on_neighbour(tmp_path):
    """The protected set must exclude neighbour predictions whose review
    status is ``'alternative'`` — Compare drops those rows explicitly, so a
    stale species keyword that only survives via an alternative row is not
    "still tagged by another subject", it's just an out-of-band species
    tag that replace should strip.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid, filename="ducks.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Photo carries a stale species tag whose only backing on the neighbour
    # is an 'alternative' prediction row.
    stale = db.add_keyword("Green-winged Teal", is_species=True)
    db.tag_photo(pid, stale)

    d_target, d_neighbour = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.6, "y": 0.5, "w": 0.2, "h": 0.2},
         "confidence": 0.8, "category": "animal"},
    ], detector_model="MDV6")
    # Neighbour: primary picks a wigeon, alternative names the stale teal.
    db.add_prediction(
        d_neighbour, "American Wigeon", 0.95, "bioclip",
        labels_fingerprint="fp",
    )
    db.add_prediction(
        d_neighbour, "Green-winged Teal", 0.40, "bioclip",
        status="alternative", labels_fingerprint="fp",
    )
    # Target box: wrong teal, needs correction to Blue-winged Teal.
    db.add_prediction(
        d_target, "Blue-winged Teal", 0.92, "bioclip",
        labels_fingerprint="fp",
    )
    target_pred = next(
        r for r in db.get_predictions(photo_ids=[pid])
        if r["detection_id"] == d_target and r["status"] != "alternative"
    )

    result = db.accept_prediction(target_pred["id"], replace_species=True)

    names = {k["name"] for k in db.get_photo_keywords(pid)}
    assert "Blue-winged Teal" in names
    assert "Green-winged Teal" not in names    # alternative must not protect it
    assert "Green-winged Teal" in result["affected"][0]["old_species"]
    removed = {
        (c["photo_id"], c["value"])
        for c in db.get_pending_changes()
        if c["change_type"] == "keyword_remove"
    }
    assert (pid, "Green-winged Teal") in removed


def test_replace_species_ignores_below_threshold_neighbour(tmp_path):
    """The protected set must exclude neighbour predictions whose detection
    sits below the workspace's ``detector_confidence`` threshold — Compare
    treats those as dormant and hides them, so a stale species keyword that
    only survives via a below-threshold neighbour is not "another visible
    subject" and replace must strip it.
    """
    import config as cfg
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({"detector_confidence": 0.5})

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid, filename="ducks.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Photo carries a stale species tag whose only backing on the neighbour
    # is a below-threshold detection.
    stale = db.add_keyword("Green-winged Teal", is_species=True)
    db.tag_photo(pid, stale)

    d_target, d_dormant = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
        # Below the 0.5 workspace threshold — Compare treats as dormant.
        {"box": {"x": 0.6, "y": 0.5, "w": 0.2, "h": 0.2},
         "confidence": 0.2, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(
        d_dormant, "Green-winged Teal", 0.95, "bioclip",
        labels_fingerprint="fp",
    )
    db.add_prediction(
        d_target, "Blue-winged Teal", 0.92, "bioclip",
        labels_fingerprint="fp",
    )
    target_pred = next(
        r for r in db.get_predictions(photo_ids=[pid])
        if r["detection_id"] == d_target
    )

    result = db.accept_prediction(target_pred["id"], replace_species=True)

    names = {k["name"] for k in db.get_photo_keywords(pid)}
    assert "Blue-winged Teal" in names
    # Dormant (below-threshold) neighbour must not protect the stale tag.
    assert "Green-winged Teal" not in names
    assert "Green-winged Teal" in result["affected"][0]["old_species"]
    removed = {
        (c["photo_id"], c["value"])
        for c in db.get_pending_changes()
        if c["change_type"] == "keyword_remove"
    }
    assert (pid, "Green-winged Teal") in removed


def test_replace_species_preserves_neighbour_above_threshold(tmp_path):
    """Companion to the below-threshold test: an above-threshold neighbour
    with a non-alternative prediction MUST still protect its species even
    when the workspace's detector_confidence is set high enough that the
    default 0.2 would have hidden neighbours in the earlier test. This
    guards against the visibility filter accidentally protecting nothing.
    """
    import config as cfg
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({"detector_confidence": 0.5})

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid, filename="ducks.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    wigeon = db.add_keyword("American Wigeon", is_species=True)
    stale = db.add_keyword("Green-winged Teal", is_species=True)
    db.tag_photo(pid, wigeon)
    db.tag_photo(pid, stale)

    d_target, d_wigeon = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
        # Comfortably above the 0.5 workspace threshold.
        {"box": {"x": 0.6, "y": 0.5, "w": 0.2, "h": 0.2},
         "confidence": 0.85, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(
        d_wigeon, "American Wigeon", 0.99, "bioclip",
        labels_fingerprint="fp",
    )
    db.add_prediction(
        d_target, "Blue-winged Teal", 0.92, "bioclip",
        labels_fingerprint="fp",
    )
    target_pred = next(
        r for r in db.get_predictions(photo_ids=[pid])
        if r["detection_id"] == d_target
    )

    db.accept_prediction(target_pred["id"], replace_species=True)

    names = {k["name"] for k in db.get_photo_keywords(pid)}
    assert "Blue-winged Teal" in names
    assert "American Wigeon" in names          # visible neighbour still protects
    assert "Green-winged Teal" not in names


def test_replace_species_protects_taxonomy_ancestor_of_neighbour_prediction(
    tmp_path, monkeypatch,
):
    """A broader taxonomy keyword (e.g. Anatidae) supported by a neighbouring
    subject's prediction under the taxonomy — not just by exact text — must
    survive replace_species and its curation must not be migrated onto the
    new species.

    Regression: the protected set only contained ``keyword_match_key(pr.species)``
    so a multi-detection photo carrying a family-level keyword like Anatidae
    alongside the neighbour's American Wigeon still had Anatidae deleted when
    a different box was replaced, and the ``rename_*_species`` migration then
    rebound Anatidae's curation onto the corrected species — the wrong
    subject.
    """
    import json as _json

    import taxonomy as taxonomy_mod
    from db import Database
    from taxonomy import Taxonomy

    # Minimal taxonomy: Anatidae family + three species inside it. Enough for
    # Compare's relationship rules to fire:
    #   * Anatidae vs American Wigeon -> ancestor / refinement (must protect)
    #   * Green-winged Teal vs American Wigeon -> sibling / conflict (must not)
    tax_path = tmp_path / "taxonomy.json"
    with open(tax_path, "w") as f:
        _json.dump({
            "last_updated": "2026-07-18",
            "source": "test",
            "taxa_by_common": {
                "american wigeon": {
                    "taxon_id": 1,
                    "scientific_name": "Mareca americana",
                    "common_name": "American Wigeon",
                    "rank": "species",
                    "lineage_names": [
                        "Animalia", "Chordata", "Aves", "Anseriformes",
                        "Anatidae", "Mareca", "Mareca americana",
                    ],
                    "lineage_ranks": [
                        "kingdom", "phylum", "class", "order",
                        "family", "genus", "species",
                    ],
                },
                "green-winged teal": {
                    "taxon_id": 2,
                    "scientific_name": "Anas crecca",
                    "common_name": "Green-winged Teal",
                    "rank": "species",
                    "lineage_names": [
                        "Animalia", "Chordata", "Aves", "Anseriformes",
                        "Anatidae", "Anas", "Anas crecca",
                    ],
                    "lineage_ranks": [
                        "kingdom", "phylum", "class", "order",
                        "family", "genus", "species",
                    ],
                },
                "wood duck": {
                    "taxon_id": 3,
                    "scientific_name": "Aix sponsa",
                    "common_name": "Wood Duck",
                    "rank": "species",
                    "lineage_names": [
                        "Animalia", "Chordata", "Aves", "Anseriformes",
                        "Anatidae", "Aix", "Aix sponsa",
                    ],
                    "lineage_ranks": [
                        "kingdom", "phylum", "class", "order",
                        "family", "genus", "species",
                    ],
                },
                "anatidae": {
                    "taxon_id": 4,
                    "scientific_name": "Anatidae",
                    "common_name": "Anatidae",
                    "rank": "family",
                    "lineage_names": [
                        "Animalia", "Chordata", "Aves", "Anseriformes",
                        "Anatidae",
                    ],
                    "lineage_ranks": [
                        "kingdom", "phylum", "class", "order", "family",
                    ],
                },
            },
            "taxa_by_scientific": {
                "anatidae": {
                    "taxon_id": 4,
                    "scientific_name": "Anatidae",
                    "common_name": "Anatidae",
                    "rank": "family",
                    "lineage_names": [
                        "Animalia", "Chordata", "Aves", "Anseriformes",
                        "Anatidae",
                    ],
                    "lineage_ranks": [
                        "kingdom", "phylum", "class", "order", "family",
                    ],
                },
            },
        }, f)
    fake_tax = Taxonomy(str(tax_path))
    monkeypatch.setattr(
        taxonomy_mod, "load_local_taxonomy", lambda: fake_tax,
    )

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid, filename="ducks.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Photo carries the neighbour's species AND a broader family taxonomy
    # keyword — the shape a photo picks up from a prior curation pass. It
    # also carries an unrelated stale species that only the target box's
    # (soon-to-be-replaced) old identity supported.
    wigeon = db.add_keyword("American Wigeon", is_species=True)
    anatidae = db.add_keyword("Anatidae", kw_type="taxonomy")
    stale_teal = db.add_keyword("Green-winged Teal", is_species=True)
    db.tag_photo(pid, wigeon)
    db.tag_photo(pid, anatidae)
    db.tag_photo(pid, stale_teal)

    # Seed curation state on the family keyword so we can verify it is NOT
    # migrated onto the new species when Anatidae survives replace.
    ws_id = db._ws_id()
    db.conn.execute(
        """INSERT INTO photo_preferences
             (workspace_id, purpose, species, photo_id)
           VALUES (?, ?, ?, ?)""",
        (ws_id, "highlights", "Anatidae", pid),
    )
    db.conn.commit()

    d_target, d_neighbour = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.6, "y": 0.5, "w": 0.2, "h": 0.2},
         "confidence": 0.85, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(
        d_neighbour, "American Wigeon", 0.99, "bioclip",
        labels_fingerprint="fp",
    )
    db.add_prediction(
        d_target, "Wood Duck", 0.92, "bioclip",
        labels_fingerprint="fp",
    )
    target_pred = next(
        r for r in db.get_predictions(photo_ids=[pid])
        if r["detection_id"] == d_target
    )

    result = db.accept_prediction(target_pred["id"], replace_species=True)

    names = {k["name"] for k in db.get_photo_keywords(pid)}
    assert "Wood Duck" in names                # target's corrected identity
    assert "American Wigeon" in names           # exact-text neighbour survives
    assert "Anatidae" in names                  # taxonomy ancestor survives
    assert "Green-winged Teal" not in names     # target's stale ID gone
    # Anatidae must not appear in old_species: it wasn't stripped, so no
    # keyword_remove was queued and the curation migration was skipped.
    assert "Anatidae" not in result["affected"][0]["old_species"]
    assert "Green-winged Teal" in result["affected"][0]["old_species"]
    removed = {
        (c["photo_id"], c["value"])
        for c in db.get_pending_changes()
        if c["change_type"] == "keyword_remove"
    }
    assert (pid, "Green-winged Teal") in removed
    assert (pid, "Anatidae") not in removed
    assert (pid, "American Wigeon") not in removed
    # Anatidae's curation stayed on Anatidae — not silently rebound to the
    # newly-added Wood Duck.
    prefs = db.conn.execute(
        "SELECT species FROM photo_preferences WHERE photo_id = ?",
        (pid,),
    ).fetchall()
    pref_species = {row["species"] for row in prefs}
    assert "Anatidae" in pref_species
    assert "Wood Duck" not in pref_species


def test_accept_prediction_queues_normalized_species(tmp_path):
    """When the prediction's species carries stray edge quotes (e.g.
    `‘apapane`), accept_prediction must tag the photo with the normalized
    row AND queue the pending sidecar keyword_add / return the payload
    using the stored (clean) spelling. Without this, the DB tag points to
    `apapane` while pending changes and the response payload use
    `‘apapane`, so a later delete queues the clean name, pending changes
    stop cancelling, and XMP sync persists the stray-quote label."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="‘apapane",
                      confidence=0.9, model="bioclip")
    pred = db.get_predictions()[0]

    result = db.accept_prediction(pred["id"])
    # The stored keyword name is the normalized (clean) spelling.
    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (result["keyword_id"],)
    ).fetchone()
    assert row["name"] == "apapane"
    # Response payload uses the stored spelling too.
    assert result["species"] == "apapane"
    # Pending keyword_add uses the clean spelling — a later remove of the
    # stored keyword can then cancel the queued add.
    pending = db.get_pending_changes()
    add_values = [
        c["value"] for c in pending if c["change_type"] == "keyword_add"
    ]
    assert "apapane" in add_values
    assert "‘apapane" not in add_values


def test_accept_prediction_commit_false_preserves_caller_transaction_on_error(
    tmp_path, monkeypatch,
):
    """accept_prediction(_commit=False) leaves rollback to the caller."""
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid,
        filename="elk.jpg",
        extension=".jpg",
        file_size=100,
        file_mtime=1.0,
    )
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Elk", confidence=0.9, model="bioclip")
    pred = db.get_predictions()[0]

    db.conn.execute(
        "INSERT INTO keywords (name, is_species) VALUES ('Outer Marker', 0)"
    )

    def fail_add_keyword(*args, **kwargs):
        raise RuntimeError("simulated accept failure")

    monkeypatch.setattr(db, "add_keyword", fail_add_keyword)
    with pytest.raises(RuntimeError, match="simulated accept failure"):
        db.accept_prediction(pred["id"], _commit=False)

    assert db.conn.execute(
        "SELECT 1 FROM keywords WHERE name = 'Outer Marker'"
    ).fetchone() is not None
    db.conn.rollback()


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


def test_write_detection_batch_ids_are_stable_for_same_content(tmp_path):
    """IDs are derived from content, not table state. Even after other rows
    are inserted (so auto-rowid recycling no longer hides the bug), the same
    (photo, model, detections) must produce the same IDs.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_a = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    photo_b = db.add_photo(
        folder_id=folder_id, filename="b.jpg", extension=".jpg",
        file_size=200, file_mtime=2.0,
    )

    detections = [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.4, "y": 0.4, "w": 0.2, "h": 0.2},
         "confidence": 0.7, "category": "animal"},
    ]
    ids1 = db.write_detection_batch(photo_a, "megadetector-v6", detections)
    # Insert into a different photo so the table is non-empty when photo_a's
    # rows are deleted-and-reinserted — defeats auto-rowid's "reset to 1
    # when table empty" recycling that hides the bug.
    db.write_detection_batch(photo_b, "megadetector-v6", detections)
    ids2 = db.write_detection_batch(photo_a, "megadetector-v6", detections)
    assert ids1 == ids2, (
        f"same content must produce same IDs across rewrites; got {ids1} vs {ids2}"
    )

    count = db.conn.execute(
        "SELECT COUNT(*) AS c FROM detections WHERE photo_id = ? AND detector_model = ?",
        (photo_a, "megadetector-v6"),
    ).fetchone()["c"]
    assert count == len(detections), "second write must not duplicate rows"


def test_write_detection_batch_retires_stale_rows(tmp_path):
    """Detections the new run no longer produces must be deleted, even when
    other detections from the same (photo, model) are unchanged. Predictions
    against retired detections CASCADE-delete; predictions against retained
    detections survive.
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

    a = {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"}
    b = {"box": {"x": 0.3, "y": 0.3, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"}
    c = {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2},
         "confidence": 0.9, "category": "animal"}
    ids_abc = db.write_detection_batch(photo_id, "megadetector-v6", [a, b, c])
    id_a, id_b, id_c = ids_abc

    # Plant a prediction against every detection. The retained ones
    # (id_a, id_c) must survive the second write; only the retired one
    # (id_b) should cascade-disappear. This pins the cascade contract:
    # a bad implementation that DELETEs all rows and reinserts the kept
    # ones would still produce the same `remaining` set below, but it
    # would wipe id_a and id_c's predictions too.
    for det_id, species in [(id_a, "A"), (id_b, "B"), (id_c, "C")]:
        db.conn.execute(
            """INSERT INTO predictions
                 (detection_id, classifier_model, labels_fingerprint, species, confidence)
               VALUES (?, 'classifier-v1', 'legacy', ?, 0.95)""",
            (det_id, species),
        )
    db.conn.commit()

    ids_ac = db.write_detection_batch(photo_id, "megadetector-v6", [a, c])
    assert ids_ac == [id_a, id_c], "stable IDs for retained boxes"

    remaining = {r["id"] for r in db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ? AND detector_model = ?",
        (photo_id, "megadetector-v6"),
    ).fetchall()}
    assert remaining == {id_a, id_c}, f"stale id {id_b} must be deleted"

    remaining_predictions = {
        (r["detection_id"], r["species"]) for r in db.conn.execute(
            "SELECT detection_id, species FROM predictions"
        ).fetchall()
    }
    assert remaining_predictions == {(id_a, "A"), (id_c, "C")}, (
        "predictions on retained detections must survive; only id_b's "
        f"prediction should have cascaded away, got {remaining_predictions}"
    )


def test_write_detection_batch_second_writer_does_not_cascade_predictions(tmp_path):
    """The race: pipeline A writes detections, classify writes predictions
    against them, pipeline B writes the same detections again. With
    auto-rowid IDs, B's DELETE CASCADEs A's predictions (data loss). With
    content-addressed IDs, B's UPSERT matches A's rows so the FK targets
    survive.
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
        {"box": {"x": 0.10, "y": 0.10, "w": 0.20, "h": 0.20},
         "confidence": 0.9, "category": "animal"},
    ]

    # Pipeline A: writes detections, then writes a prediction against the
    # detection it just produced.
    ids_a = db.write_detection_batch(photo_id, "megadetector-v6", detections)
    det_id = ids_a[0]
    db.conn.execute(
        """INSERT INTO predictions
             (detection_id, classifier_model, labels_fingerprint, species, confidence, category)
           VALUES (?, 'classifier-v1', 'legacy', 'cardinal', 0.95, 'animal')""",
        (det_id,),
    )
    db.conn.commit()

    pred_count_before = db.conn.execute(
        "SELECT COUNT(*) AS c FROM predictions WHERE detection_id = ?",
        (det_id,),
    ).fetchone()["c"]
    assert pred_count_before == 1

    # Pipeline B: writes the same detections. With auto-rowid this would
    # DELETE A's row and CASCADE-delete A's prediction.
    ids_b = db.write_detection_batch(photo_id, "megadetector-v6", detections)
    assert ids_b == ids_a, "concurrent writer must produce same IDs"

    pred_count_after = db.conn.execute(
        "SELECT COUNT(*) AS c FROM predictions WHERE detection_id = ?",
        (det_id,),
    ).fetchone()["c"]
    assert pred_count_after == 1, (
        "B's write must not CASCADE-delete A's prediction; "
        f"got {pred_count_after} predictions remaining"
    )


def test_write_detection_batch_conflict_refreshes_box_fields(tmp_path):
    """If a replacement box falls in the same quantized ID bucket, the row's
    exact stored coordinates must still update to the latest detector output.
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

    first = {
        "box": {"x": 0.10001, "y": 0.20001, "w": 0.30001, "h": 0.40001},
        "confidence": 0.70,
        "category": "animal",
    }
    second = {
        "box": {"x": 0.10002, "y": 0.20002, "w": 0.30002, "h": 0.40002},
        "confidence": 0.91,
        "category": "animal",
    }

    ids1 = db.write_detection_batch(photo_id, "megadetector-v6", [first])
    ids2 = db.write_detection_batch(photo_id, "megadetector-v6", [second])
    assert ids2 == ids1, "sub-quantization box drift should keep the same id"

    row = db.conn.execute(
        "SELECT box_x, box_y, box_w, box_h, detector_confidence, category"
        " FROM detections WHERE id = ?",
        (ids1[0],),
    ).fetchone()
    assert dict(row) == {
        "box_x": second["box"]["x"],
        "box_y": second["box"]["y"],
        "box_w": second["box"]["w"],
        "box_h": second["box"]["h"],
        "detector_confidence": second["confidence"],
        "category": "animal",
    }


def test_write_detection_batch_deduplicates_same_batch_ids(tmp_path):
    """One detector batch can contain two boxes in the same quantized ID bucket.
    Persist and count the unique detection once, using the strongest row.
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

    low = {
        "box": {"x": 0.10001, "y": 0.20001, "w": 0.30001, "h": 0.40001},
        "confidence": 0.50,
        "category": "animal",
    }
    high = {
        "box": {"x": 0.10002, "y": 0.20002, "w": 0.30002, "h": 0.40002},
        "confidence": 0.95,
        "category": "animal",
    }

    ids = db.write_detection_batch(photo_id, "megadetector-v6", [low, high])
    assert len(ids) == 1

    row = db.conn.execute(
        "SELECT box_x, detector_confidence FROM detections WHERE id = ?",
        (ids[0],),
    ).fetchone()
    assert row["box_x"] == high["box"]["x"]
    assert row["detector_confidence"] == high["confidence"]
    run = db.conn.execute(
        "SELECT box_count FROM detector_runs WHERE photo_id = ? AND detector_model = ?",
        (photo_id, "megadetector-v6"),
    ).fetchone()
    assert run["box_count"] == 1


def test_pairing_recomputes_detection_ids_so_photo_id_reuse_is_safe(tmp_path):
    """Regression: when raw+jpeg pairing moves a detection to the primary photo,
    its content-addressed id MUST be recomputed against the primary's photo_id.

    Otherwise a later photo that reuses the companion's freed rowid (SQLite
    INTEGER PRIMARY KEY without AUTOINCREMENT recycles ids) could produce a
    detection whose computed id collides with the stale-moved row, and the
    UPSERT would silently update the wrong photo's detection.
    """
    from db import Database
    from detection_id import detection_id as compute_id
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, fid)
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG_1.jpg", extension=".jpg",
                           file_size=1, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG_1.cr3", extension=".cr3",
                          file_size=1, file_mtime=1.0)

    box = {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}
    det_ids = db.write_detection_batch(jpeg_id, "MDV6", [
        {"box": box, "confidence": 0.9, "category": "animal"},
    ])
    db.add_prediction(det_ids[0], "Robin", 0.95, "bioclip")

    _pair_raw_jpeg_companions(db)

    # After pairing: the surviving detection's id is computed against the
    # primary (raw) photo's id, not the companion's.
    expected_new_id = compute_id(
        raw_id, "MDV6", (box["x"], box["y"], box["w"], box["h"]), "animal",
    )
    rows = db.conn.execute(
        "SELECT id, photo_id FROM detections WHERE photo_id = ?", (raw_id,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["id"] == expected_new_id, (
        "moved detection's id must be recomputed against the new photo_id"
    )

    # Prediction followed the rehash.
    pred = db.conn.execute(
        "SELECT species FROM predictions WHERE detection_id = ?",
        (expected_new_id,),
    ).fetchone()
    assert pred is not None and pred["species"] == "Robin"


def test_pairing_redirects_classifier_runs_so_cache_gate_still_hits(tmp_path):
    """Regression for Codex P2 on PR #912: after pair-up rehashes a
    detection's id, the classifier_runs row must follow.

    `get_classifier_run_keys(detection_id)` is the gate that decides
    whether classify_photos can serve cached predictions. If the run-key
    row stays pointed at the old (companion) id, paired-then-rehashed
    photos look unclassified to the gate and get re-classified
    unnecessarily on every subsequent classify pass.
    """
    from db import Database
    from detection_id import detection_id as compute_id
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, fid)
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG_1.jpg", extension=".jpg",
                           file_size=1, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG_1.cr3", extension=".cr3",
                          file_size=1, file_mtime=1.0)

    box = {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}
    det_ids = db.write_detection_batch(jpeg_id, "MDV6", [
        {"box": box, "confidence": 0.9, "category": "animal"},
    ])
    old_det_id = det_ids[0]
    db.add_prediction(old_det_id, "Robin", 0.95, "bioclip")
    # Record the classifier_run row that gates non-reclassify reruns.
    db.conn.execute(
        """INSERT INTO classifier_runs
             (detection_id, classifier_model, labels_fingerprint, prediction_count)
           VALUES (?, 'bioclip', 'fp-x', 1)""",
        (old_det_id,),
    )
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    expected_new_id = compute_id(
        raw_id, "MDV6", (box["x"], box["y"], box["w"], box["h"]), "animal",
    )
    # The classifier_runs row must now point at the rehashed detection id —
    # otherwise the non-reclassify gate would treat the paired photo as
    # unclassified and rerun the classifier needlessly.
    keys = db.get_classifier_run_keys(expected_new_id)
    assert ("bioclip", "fp-x") in keys, (
        f"classifier_runs must follow rehash; new id keys: {keys}"
    )
    # And nothing left pointing at the stale old id (CASCADE cleanup).
    stale = db.conn.execute(
        "SELECT 1 FROM classifier_runs WHERE detection_id = ?", (old_det_id,),
    ).fetchone()
    assert stale is None, "stale classifier_runs row must not survive"


def test_pairing_preserves_review_when_duplicate_prediction_collapses(tmp_path):
    """When a companion prediction loses the duplicate collapse, its manual
    review state must move to the surviving primary prediction.
    """
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, fid)
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG_1.jpg", extension=".jpg",
                           file_size=1, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG_1.cr3", extension=".cr3",
                          file_size=1, file_mtime=1.0)

    box = {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}
    jpeg_det = db.write_detection_batch(jpeg_id, "MDV6", [
        {"box": box, "confidence": 0.9, "category": "animal"},
    ])[0]
    raw_det = db.write_detection_batch(raw_id, "MDV6", [
        {"box": box, "confidence": 0.9, "category": "animal"},
    ])[0]
    db.add_prediction(jpeg_det, "Robin", 0.95, "bioclip", status="accepted")
    db.add_prediction(raw_det, "Robin", 0.90, "bioclip")

    _pair_raw_jpeg_companions(db)

    row = db.conn.execute(
        """SELECT pr_rev.status
             FROM predictions pr
             JOIN prediction_review pr_rev ON pr_rev.prediction_id = pr.id
            WHERE pr.detection_id IN (
                  SELECT id FROM detections WHERE photo_id = ?
            )
              AND pr.species = 'Robin'
              AND pr.classifier_model = 'bioclip'
              AND pr_rev.workspace_id = ?""",
        (raw_id, ws),
    ).fetchone()
    assert row is not None
    assert row["status"] == "accepted"


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


def test_get_missing_photos_returns_photos_with_missing_source(tmp_path):
    """get_missing_photos returns photos whose source file is gone but folder remains."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = tmp_path / "photos"
    folder.mkdir()
    fid = db.add_folder(str(folder), name="photos")

    here = folder / "present.jpg"
    here.write_bytes(b"x")
    pid_present = db.add_photo(folder_id=fid, filename="present.jpg",
                               extension=".jpg", file_size=1, file_mtime=1.0)
    pid_gone = db.add_photo(folder_id=fid, filename="gone.NEF",
                            extension=".nef", file_size=1, file_mtime=1.0,
                            timestamp="2024-03-08T10:00:00")

    missing = db.get_missing_photos()
    ids = [row["id"] for row in missing]
    assert ids == [pid_gone]
    row = missing[0]
    assert row["filename"] == "gone.NEF"
    assert row["folder_path"] == str(folder)
    assert row["timestamp"] == "2024-03-08T10:00:00"


def test_get_missing_photos_excludes_photos_in_missing_folders(tmp_path):
    """Photos in folders flagged 'missing' are surfaced by get_missing_folders;
    don't double-count them here."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid = db.add_folder("/gone/folder", name="gone")
    db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid,))
    db.conn.commit()

    assert db.get_missing_photos() == []


def test_get_missing_photos_skips_folder_whose_root_is_offline(tmp_path):
    """Folders whose path no longer resolves on disk must be skipped wholesale.

    Regression: folder status only flips to 'missing' when the 10-minute
    health loop runs. When a volume is unmounted, get_missing_photos used
    to classify every photo in that folder as a ghost, and the UI offered
    bulk delete — which would wipe library rows for a drive that's just
    temporarily offline. Treat 'folder root missing on disk' the same as
    'folder marked missing in DB' so we never surface those rows.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    # Folder is 'ok' in DB but its path doesn't exist (unmounted volume).
    fid = db.add_folder("/Volumes/never_mounted/dir", name="offline")
    assert db.conn.execute(
        "SELECT status FROM folders WHERE id = ?", (fid,)
    ).fetchone()["status"] == "ok"
    db.add_photo(folder_id=fid, filename="bird.NEF", extension=".nef",
                 file_size=1, file_mtime=1.0)

    assert db.get_missing_photos() == []


def test_get_missing_photos_scoped_to_active_workspace(tmp_path):
    """Missing photos in other workspaces don't leak into the active one."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    ws_b = db.create_workspace("Other")

    folder_a = tmp_path / "a"
    folder_a.mkdir()
    folder_b = tmp_path / "b"
    folder_b.mkdir()

    db.set_active_workspace(ws_a)
    fid_a = db.add_folder(str(folder_a), name="a")
    db.add_photo(folder_id=fid_a, filename="ghost_a.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)

    db.set_active_workspace(ws_b)
    fid_b = db.add_folder(str(folder_b), name="b")
    db.add_photo(folder_id=fid_b, filename="ghost_b.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)

    db.set_active_workspace(ws_a)
    assert [row["filename"] for row in db.get_missing_photos()] == ["ghost_a.jpg"]
    db.set_active_workspace(ws_b)
    assert [row["filename"] for row in db.get_missing_photos()] == ["ghost_b.jpg"]


def test_get_missing_photos_does_not_stat_every_photo(tmp_path, monkeypatch):
    """Photos in the same folder must share a single readdir, not N stats.

    On a large library (tens of thousands of photos) the per-photo
    ``os.path.exists`` was the bottleneck — multi-minute scans over
    network volumes. One ``os.scandir`` per folder + an in-memory set
    lookup is the contract for *present* files. Missing candidates pay one
    fallback ``os.path.exists`` to honor FS-specific case rules without
    unconditional case-folding, which is bounded by the number of misses
    rather than the total photo count.
    """
    import os

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = tmp_path / "shoot"
    folder.mkdir()
    fid = db.add_folder(str(folder), name="shoot")

    for i in range(50):
        name = f"present_{i:03d}.jpg"
        (folder / name).write_bytes(b"x")
        db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                     file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename="gone.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)

    real_exists = os.path.exists
    exists_calls = 0

    def counted_exists(path):
        nonlocal exists_calls
        exists_calls += 1
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", counted_exists)

    missing = db.get_missing_photos()

    assert [row["filename"] for row in missing] == ["gone.jpg"]
    # 1 fallback stat for the single missing photo. Crucially, the 50
    # present photos share one scandir and pay zero stats — that's the
    # perf contract this test guards.
    assert exists_calls <= 1, (
        f"get_missing_photos called os.path.exists {exists_calls} times for "
        "1 missing photo; should be at most one stat per miss (no per-present-photo stats)"
    )


def test_get_missing_photos_ignores_progress_callback_errors(tmp_path):
    """Progress callback failures must not abort missing-original detection."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = tmp_path / "shoot"
    folder.mkdir()
    fid = db.add_folder(str(folder), name="shoot")
    db.add_photo(folder_id=fid, filename="gone.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)

    def broken_progress(_payload):
        raise RuntimeError("progress sink unavailable")

    missing = db.get_missing_photos(progress_callback=broken_progress)

    assert [row["filename"] for row in missing] == ["gone.jpg"]


def test_get_missing_photos_reports_periodic_photo_progress(tmp_path):
    """Large single-folder scans should emit progress before the final callback."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = tmp_path / "shoot"
    folder.mkdir()
    fid = db.add_folder(str(folder), name="shoot")
    for i in range(401):
        db.add_photo(folder_id=fid, filename=f"gone_{i:03d}.jpg",
                     extension=".jpg", file_size=1, file_mtime=1.0)

    events = []
    missing = db.get_missing_photos(progress_callback=events.append)

    assert len(missing) == 401
    considered = [event["photos_considered"] for event in events]
    assert 200 in considered
    assert 400 in considered
    assert considered[-1] == 401


def test_get_missing_photos_handles_unicode_normalization(tmp_path):
    """A photo row stored as NFC must not be reported missing if the file
    on disk has the same visible name in NFD bytes (or vice versa).

    Why: macOS APFS stores filenames in their original normalization form
    but compares with normalization, so ``os.path.exists("café.jpg" NFC)``
    used to find the NFD file. Switching to ``listdir`` + Python set
    membership is byte-exact, so without explicit normalization the check
    would falsely flag the file as missing.
    """
    import unicodedata

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = tmp_path / "shoot"
    folder.mkdir()
    fid = db.add_folder(str(folder), name="shoot")

    nfc_name = unicodedata.normalize("NFC", "café_dawn.jpg")
    nfd_name = unicodedata.normalize("NFD", "café_dawn.jpg")
    assert nfc_name != nfd_name  # sanity: the two encodings differ at byte level

    # File on disk: NFD bytes.
    (folder / nfd_name).write_bytes(b"x")
    # DB row: NFC bytes.
    db.add_photo(folder_id=fid, filename=nfc_name, extension=".jpg",
                 file_size=1, file_mtime=1.0)

    assert db.get_missing_photos() == []


def test_get_missing_photos_defers_case_to_filesystem(tmp_path):
    """Case-only mismatches must follow the underlying filesystem's rules.

    On case-insensitive volumes (APFS default, NTFS) a row inserted as
    ``IMG_1234.NEF`` should resolve a file written as ``IMG_1234.nef`` — the
    user sees them as the same file in Finder/Explorer. On case-sensitive
    volumes (ext4, most network mounts) they are genuinely distinct files
    and the row should surface as missing. Unconditionally case-folding the
    set-membership check would silently collapse distinct files on
    case-sensitive volumes, so the implementation falls back to
    ``os.path.exists`` on a NFC miss and lets the kernel arbitrate.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = tmp_path / "shoot"
    folder.mkdir()
    fid = db.add_folder(str(folder), name="shoot")

    (folder / "IMG_1234.nef").write_bytes(b"x")
    db.add_photo(folder_id=fid, filename="IMG_1234.NEF", extension=".nef",
                 file_size=1, file_mtime=1.0)

    fs_case_insensitive = os.path.exists(str(folder / "IMG_1234.NEF"))
    if fs_case_insensitive:
        # APFS/NTFS: kernel resolves the case-mismatched name; not missing.
        assert db.get_missing_photos() == []
    else:
        # ext4/XFS/most network mounts: distinct names ARE distinct files.
        # The row IS missing — the prior os.path.exists would have agreed.
        assert [row["filename"] for row in db.get_missing_photos()] == ["IMG_1234.NEF"]


def test_get_missing_photos_distinguishes_case_on_case_sensitive_fs(tmp_path):
    """On a case-sensitive volume two filenames differing only in case are
    distinct files; the missing-originals scan must not collapse them.

    Regression guard for the prior unconditional ``.lower()`` normalization,
    which made ``A.jpg`` and ``a.jpg`` share a lookup key. With the kernel
    arbitrating case via ``os.path.exists``, both rows are correctly tracked
    and only the absent one surfaces as missing.
    """
    import os as _os

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = tmp_path / "shoot"
    folder.mkdir()
    fid = db.add_folder(str(folder), name="shoot")

    # Probe FS — only meaningful on case-sensitive volumes.
    (folder / "_probe").write_bytes(b"")
    case_insensitive = _os.path.exists(str(folder / "_PROBE"))
    (folder / "_probe").unlink()
    if case_insensitive:
        import pytest
        pytest.skip("filesystem is case-insensitive; case-distinct names alias")

    (folder / "shot.jpg").write_bytes(b"x")  # lower-case file present
    db.add_photo(folder_id=fid, filename="shot.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename="SHOT.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)

    # Only the genuinely-absent SHOT.jpg should surface; the lower-case row
    # must not be incorrectly aliased away.
    assert [row["filename"] for row in db.get_missing_photos()] == ["SHOT.jpg"]


def test_get_missing_photos_treats_broken_symlink_as_missing(tmp_path):
    """A symlink whose target no longer exists must surface as missing.

    Regression guard for the listdir → set-membership switch: ``os.listdir``
    returns the symlink basename even when the target is gone, but the prior
    ``os.path.exists(src)`` followed the symlink and returned False. Filter
    broken symlinks during scandir so libraries that track originals via
    symlinked paths still see them in the cleanup flow.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder = tmp_path / "shoot"
    folder.mkdir()
    fid = db.add_folder(str(folder), name="shoot")

    target = tmp_path / "originals" / "raw_001.NEF"
    target.parent.mkdir()
    target.write_bytes(b"x")
    link_path = folder / "raw_001.NEF"
    os.symlink(str(target), str(link_path))
    db.add_photo(folder_id=fid, filename="raw_001.NEF", extension=".nef",
                 file_size=1, file_mtime=1.0)

    # Symlink target intact: photo present.
    assert db.get_missing_photos() == []

    # Target deleted: symlink basename still appears in scandir, but the
    # photo should now surface as missing.
    target.unlink()
    assert [row["filename"] for row in db.get_missing_photos()] == ["raw_001.NEF"]


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


def test_nearest_ancestor_folder_id(tmp_path):
    """nearest_ancestor_folder_id returns the longest proper-ancestor row."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    usa = db.add_folder("/vol/USA", name="USA")
    usa_2026 = db.add_folder("/vol/USA/2026", name="2026", parent_id=usa)

    # Longest ancestor wins over the shallower one.
    assert db.nearest_ancestor_folder_id("/vol/USA/2026/2026-05-30") == usa_2026
    # Falls back to the nearest existing ancestor when the immediate one has
    # no row.
    assert db.nearest_ancestor_folder_id("/vol/USA/gap/leaf") == usa
    # No ancestor row -> None (folder is a root).
    assert db.nearest_ancestor_folder_id("/elsewhere/x") is None
    # A row is never its own ancestor.
    assert db.nearest_ancestor_folder_id("/vol/USA", exclude_id=usa) is None


def test_relocate_folder_relinks_parent_by_path(tmp_path):
    """Relocating a folder re-derives parent_id from its new path so it does
    not stay nested under its pre-move parent (browse-tree mis-nesting bug)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    pics_2026 = db.add_folder("/pics/2026", name="2026")
    usa = db.add_folder("/vol/USA", name="USA")
    # A date folder that originally lived under /pics/2026.
    date = db.add_folder("/pics/2026/2026-05-30", name="2026-05-30",
                         parent_id=pics_2026)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (date,))
    db.conn.commit()

    # User moved it onto the USA volume and re-points Vireo at the new path.
    db.relocate_folder(date, "/vol/USA/2026-05-30")

    row = db.conn.execute(
        "SELECT parent_id FROM folders WHERE id = ?", (date,)
    ).fetchone()
    # Re-linked to its real new ancestor, not left pointing at /pics/2026.
    assert row["parent_id"] == usa


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


def test_relocate_folder_merge_preserves_non_root_workspace_link(tmp_path):
    """_merge_into_existing must not promote materialized descendants to roots."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws1 = db.ensure_default_workspace()
    db.set_active_workspace(ws1)

    existing_path = str(tmp_path / "existing")
    os.makedirs(existing_path)

    fid_missing = db.add_folder("/old/path", name="missing")
    fid_existing = db.conn.execute(
        "INSERT INTO folders (path, name) VALUES (?, ?)", (existing_path, "existing")
    ).lastrowid
    db.conn.execute(
        """UPDATE workspace_folders
           SET is_root = 0
           WHERE workspace_id = ? AND folder_id = ?""",
        (ws1, fid_missing),
    )
    db.conn.commit()

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_missing,))
    db.conn.commit()

    db.relocate_folder(fid_missing, existing_path)

    row = db.conn.execute(
        """SELECT is_root FROM workspace_folders
           WHERE workspace_id = ? AND folder_id = ?""",
        (ws1, fid_existing),
    ).fetchone()
    assert row is not None
    assert row["is_root"] == 0


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


def test_relocate_folder_cascade_skips_mixed_separator_descendants_of_conflict(tmp_path):
    """Conflicted ancestors must be processed before mixed-separator descendants."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    parent = db.add_folder("C:/old/root", name="root")
    child = db.add_folder("C:\\old\\root\\sub", name="sub", parent_id=parent)
    grand = db.add_folder("C:/old/root/sub/grand", name="grand", parent_id=child)
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()

    new_root = str(tmp_path / "new_root")
    child_target = os.path.join(new_root, "sub")
    grand_target = os.path.join(new_root, "sub", "grand")
    os.makedirs(grand_target)
    db.add_folder(child_target, name="conflict")

    cascaded = db.relocate_folder(parent, new_root)
    assert cascaded == []

    for fid, expected_path in [
        (child, "C:\\old\\root\\sub"),
        (grand, "C:/old/root/sub/grand"),
    ]:
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


def test_delete_folder_with_descendants(tmp_path):
    """delete_folder removes the whole subtree — folders.parent_id has no ON
    DELETE action, so deleting a non-leaf folder row alone would trip the FK
    after its photos were already deleted."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    parent = db.add_folder("/tree", name="tree")
    child = db.add_folder("/tree/sub", name="sub", parent_id=parent)
    grand = db.add_folder("/tree/sub/deep", name="deep", parent_id=child)
    pids = [
        db.add_photo(folder_id=fid, filename=f"bird{fid}.jpg", extension=".jpg",
                     file_size=1000, file_mtime=1.0)
        for fid in (parent, child, grand)
    ]

    result = db.delete_folder(parent)
    assert result["deleted_photos"] == 3

    for fid in (parent, child, grand):
        assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid,)).fetchone() is None
        assert db.conn.execute(
            "SELECT folder_id FROM workspace_folders WHERE folder_id = ?", (fid,)
        ).fetchone() is None
    for pid in pids:
        assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid,)).fetchone() is None


def test_delete_folder_keeps_descendant_rooted_in_other_workspace(tmp_path):
    """Deleting a folder in one workspace must not destroy a descendant that
    another workspace imported as its own root (is_root = 1) — that subtree
    is still reachable there. The kept head is reparented to NULL and only
    unlinked from the deleting workspace."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    db.set_active_workspace(ws_a)

    parent = db.add_folder("/tree", name="tree")
    child = db.add_folder("/tree/sub", name="sub", parent_id=parent,
                          workspace_root=False)
    grand = db.add_folder("/tree/sub/deep", name="deep", parent_id=child,
                          workspace_root=False)
    other = db.add_folder("/tree/other", name="other", parent_id=parent,
                          workspace_root=False)

    # Workspace B imports /tree/sub as its own root.
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_b, child, is_root=True)

    pid_parent = db.add_photo(folder_id=parent, filename="p.jpg", extension=".jpg",
                              file_size=1000, file_mtime=1.0)
    pid_child = db.add_photo(folder_id=child, filename="c.jpg", extension=".jpg",
                             file_size=1000, file_mtime=1.0)
    pid_grand = db.add_photo(folder_id=grand, filename="g.jpg", extension=".jpg",
                             file_size=1000, file_mtime=1.0)
    pid_other = db.add_photo(folder_id=other, filename="o.jpg", extension=".jpg",
                             file_size=1000, file_mtime=1.0)

    # Prime A's new-images cache: the delete changes what A can see (kept
    # subtree unlinked), so the cached count must be dropped.
    db._new_images_cache.set(db._db_path, ws_a, {"new_count": 7})

    result = db.delete_folder(parent)
    # Only the photos outside B's root are deleted.
    assert result["deleted_photos"] == 2

    # A's cached new-images payload is invalidated by the delete.
    assert db._new_images_cache.get(db._db_path, ws_a) is None

    # Parent and the unshared sibling are gone, with their photos.
    for fid in (parent, other):
        assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid,)).fetchone() is None
    for pid in (pid_parent, pid_other):
        assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid,)).fetchone() is None

    # B's subtree survives: folder rows, photos, and B's links.
    row = db.conn.execute("SELECT parent_id FROM folders WHERE id = ?", (child,)).fetchone()
    assert row is not None
    assert row["parent_id"] is None  # reparented — old parent row is gone
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (grand,)).fetchone() is not None
    for pid in (pid_child, pid_grand):
        assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid,)).fetchone() is not None
    assert db.conn.execute(
        "SELECT is_root FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (ws_b, child),
    ).fetchone()["is_root"] == 1
    assert db.conn.execute(
        "SELECT folder_id FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (ws_b, grand),
    ).fetchone() is not None

    # Workspace A no longer sees any of it.
    assert db.conn.execute(
        "SELECT folder_id FROM workspace_folders WHERE workspace_id = ?", (ws_a,)
    ).fetchall() == []


def test_delete_folder_keeps_target_rooted_in_other_workspace(tmp_path):
    """Deleting a folder that another workspace imported as its own root
    must not delete anything — the folder row, subtree, and photos survive;
    only the deleting workspace's links are removed."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    db.set_active_workspace(ws_a)

    parent = db.add_folder("/tree", name="tree")
    child = db.add_folder("/tree/sub", name="sub", parent_id=parent,
                          workspace_root=False)

    # Workspace B imports /tree itself as its own root.
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_b, parent, is_root=True)
    db.add_workspace_folder(ws_b, child, is_root=False)

    pid_parent = db.add_photo(folder_id=parent, filename="p.jpg", extension=".jpg",
                              file_size=1000, file_mtime=1.0)
    pid_child = db.add_photo(folder_id=child, filename="c.jpg", extension=".jpg",
                             file_size=1000, file_mtime=1.0)

    # Prime A's new-images cache: the unlink-only path must still drop it,
    # since the folder no longer contributes to A's backlog.
    db._new_images_cache.set(db._db_path, ws_a, {"new_count": 7})

    result = db.delete_folder(parent)
    assert result["deleted_photos"] == 0
    assert result["files"] == []

    # A's cached new-images payload is invalidated by the unlink.
    assert db._new_images_cache.get(db._db_path, ws_a) is None

    # Folder rows and photos all survive, parent chain intact.
    row = db.conn.execute(
        "SELECT parent_id FROM folders WHERE id = ?", (child,)
    ).fetchone()
    assert row is not None
    assert row["parent_id"] == parent
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (parent,)).fetchone() is not None
    for pid in (pid_parent, pid_child):
        assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid,)).fetchone() is not None

    # B's links survive, including the root flag.
    assert db.conn.execute(
        "SELECT is_root FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (ws_b, parent),
    ).fetchone()["is_root"] == 1
    assert db.conn.execute(
        "SELECT folder_id FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (ws_b, child),
    ).fetchone() is not None

    # Workspace A no longer sees any of it.
    assert db.conn.execute(
        "SELECT folder_id FROM workspace_folders WHERE workspace_id = ?", (ws_a,)
    ).fetchall() == []


def test_delete_folder_keeps_target_covered_by_other_workspace_ancestor_root(tmp_path):
    """Deleting a folder whose only foreign link is scanner-materialized
    (is_root = 0) must not delete anything: that link means another
    workspace reaches the folder through a root ancestor outside the
    deleted subtree. Any foreign link protects — not just is_root = 1."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    db.set_active_workspace(ws_a)

    parent = db.add_folder("/photos", name="photos")
    child = db.add_folder("/photos/2024", name="2024", parent_id=parent,
                          workspace_root=False)

    # Workspace B imports /photos as its root; the scanner materializes the
    # descendant /photos/2024 as a non-root link.
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_b, parent, is_root=True)
    db.add_workspace_folder(ws_b, child, is_root=False)

    pid_child = db.add_photo(folder_id=child, filename="c.jpg", extension=".jpg",
                             file_size=1000, file_mtime=1.0)

    # Workspace A deletes /photos/2024 — B still sees it via its /photos root.
    result = db.delete_folder(child)
    assert result["deleted_photos"] == 0
    assert result["files"] == []

    # Folder row and photo survive, parent chain intact.
    row = db.conn.execute(
        "SELECT parent_id FROM folders WHERE id = ?", (child,)
    ).fetchone()
    assert row is not None
    assert row["parent_id"] == parent
    assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid_child,)).fetchone() is not None

    # B's links survive.
    assert db.conn.execute(
        "SELECT is_root FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (ws_b, parent),
    ).fetchone()["is_root"] == 1
    assert db.conn.execute(
        "SELECT folder_id FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (ws_b, child),
    ).fetchone() is not None

    # Workspace A no longer links the deleted subtree, but keeps /photos.
    assert db.conn.execute(
        "SELECT folder_id FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (ws_a, child),
    ).fetchone() is None
    assert db.conn.execute(
        "SELECT folder_id FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (ws_a, parent),
    ).fetchone() is not None


def test_delete_folder_includes_path_only_descendants(tmp_path):
    """Legacy databases can hold descendants whose parent_id is NULL even
    though their path lives under the deleted folder. The deletion walk
    must collect the subtree by path (like the workspace link/unlink
    paths), not by parent_id, or those rows and their photos survive."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    parent = db.add_folder("/photos", name="photos")
    # Legacy shape: path is under /photos but parent_id was never set.
    child = db.add_folder("/photos/2024", name="2024")
    assert db.conn.execute(
        "SELECT parent_id FROM folders WHERE id = ?", (child,)
    ).fetchone()["parent_id"] is None

    pids = [
        db.add_photo(folder_id=fid, filename=f"bird{fid}.jpg", extension=".jpg",
                     file_size=1000, file_mtime=1.0)
        for fid in (parent, child)
    ]

    result = db.delete_folder(parent)
    assert result["deleted_photos"] == 2

    for fid in (parent, child):
        assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid,)).fetchone() is None
        assert db.conn.execute(
            "SELECT folder_id FROM workspace_folders WHERE folder_id = ?", (fid,)
        ).fetchone() is None
    for pid in pids:
        assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid,)).fetchone() is None


def test_delete_folder_keeps_path_only_descendant_linked_in_other_workspace(tmp_path):
    """A path-only legacy descendant that another workspace links must be
    preserved by the same foreign-link protection as parent_id-linked
    descendants: its row and photos survive, only the deleting workspace's
    links go."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    db.set_active_workspace(ws_a)

    parent = db.add_folder("/photos", name="photos")
    # Legacy shape: under /photos by path, parent_id NULL.
    child = db.add_folder("/photos/2024", name="2024", workspace_root=False)

    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_b, child, is_root=True)

    pid_parent = db.add_photo(folder_id=parent, filename="p.jpg", extension=".jpg",
                              file_size=1000, file_mtime=1.0)
    pid_child = db.add_photo(folder_id=child, filename="c.jpg", extension=".jpg",
                             file_size=1000, file_mtime=1.0)

    result = db.delete_folder(parent)
    assert result["deleted_photos"] == 1

    # /photos and its photo are gone.
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (parent,)).fetchone() is None
    assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid_parent,)).fetchone() is None

    # B's legacy-shaped subtree survives with its photo and link.
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (child,)).fetchone() is not None
    assert db.conn.execute("SELECT id FROM photos WHERE id = ?", (pid_child,)).fetchone() is not None
    assert db.conn.execute(
        "SELECT is_root FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (ws_b, child),
    ).fetchone()["is_root"] == 1

    # Workspace A no longer sees any of it.
    assert db.conn.execute(
        "SELECT folder_id FROM workspace_folders WHERE workspace_id = ?", (ws_a,)
    ).fetchall() == []


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


def test_count_photos_in_workspace_includes_missing(tmp_path):
    """count_photos_in_workspace counts photos regardless of folder status,
    so the dashboard's headline total stays honest when a drive unmounts.

    count_photos() filters to ok/partial folders (right for browse/cull/etc.,
    where you can only act on accessible photos). The dashboard wants the
    full inventory, including photos whose folders are temporarily offline.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid_ok = db.add_folder("/ok/folder", name="ok")
    fid_gone = db.add_folder("/gone/folder", name="gone")
    db.add_photo(folder_id=fid_ok, filename="visible.jpg", extension=".jpg",
                 file_size=1000, file_mtime=1.0)
    db.add_photo(folder_id=fid_gone, filename="hidden.jpg", extension=".jpg",
                 file_size=1000, file_mtime=1.0)

    assert db.count_photos() == 2
    assert db.count_photos_in_workspace() == 2

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_gone,))
    db.conn.commit()

    # count_photos hides missing-folder photos, but the total inventory does not
    assert db.count_photos() == 1
    assert db.count_photos_in_workspace() == 2


def test_count_keywords_in_workspace_includes_missing(tmp_path):
    """count_keywords_in_workspace counts keywords regardless of folder
    status, so the dashboard's Keywords headline agrees with its Top
    Species / Other Keywords charts (both populated from photos in any
    folder, including 'missing'). Without this, the two widgets contradict
    each other on the same page when a drive unmounts.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0)
    k1 = db.add_keyword("Cardinal")
    k2 = db.add_keyword("Sparrow")
    db.tag_photo(pid, k1)
    db.tag_photo(pid, k2)

    assert db.count_keywords() == 2
    assert db.count_keywords_in_workspace() == 2

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid,))
    db.conn.commit()

    # count_keywords filters missing folders; the inventory-wide count does not
    assert db.count_keywords() == 0
    assert db.count_keywords_in_workspace() == 2


def test_count_photos_in_workspace_scoped_to_active_workspace(tmp_path):
    """count_photos_in_workspace is workspace-scoped — photos in other
    workspaces' folders don't bleed into the count.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    ws_b = db.create_workspace("OtherWS")

    db.set_active_workspace(ws_a)
    fid = db.add_folder("/some/folder", name="folder")
    db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                 file_size=1000, file_mtime=1.0)

    assert db.count_photos_in_workspace() == 1
    db.set_active_workspace(ws_b)
    assert db.count_photos_in_workspace() == 0


def test_dashboard_stats_metadata_survives_missing_folders(tmp_path):
    """get_dashboard_stats's pure-DB aggregates (top_keywords, photos_by_month,
    rating_dist, flag_dist) read metadata that doesn't depend on disk access,
    so they should still populate when folders are flagged 'missing'. Without
    this, unmounting a drive blanks the entire stats page even though all the
    underlying data is still in the database.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    fid = db.add_folder("/photos", name="photos")
    p1 = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                      file_size=1000, file_mtime=1.0,
                      timestamp="2024-01-15T12:00:00")
    p2 = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                      file_size=1000, file_mtime=1.0,
                      timestamp="2024-02-15T12:00:00")
    db.update_photo_rating(p1, 4)
    db.update_photo_rating(p2, 3)

    # Sanity: stats populated when folder is ok.
    stats = db.get_dashboard_stats()
    assert len(stats["photos_by_month"]) == 2
    assert len(stats["rating_distribution"]) >= 1

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid,))
    db.conn.commit()

    # Metadata aggregates still populated — they don't depend on disk presence.
    stats = db.get_dashboard_stats()
    assert len(stats["photos_by_month"]) == 2, \
        "photos_by_month went empty when folder marked missing"
    assert len(stats["rating_distribution"]) >= 1, \
        "rating_distribution went empty when folder marked missing"


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
    assert os.path.normpath(child["path"]) == os.path.normpath("/nas/photos/2024/march")
    assert os.path.normpath(grandchild["path"]) == os.path.normpath("/nas/photos/2024/march/birds")


def test_bulk_photo_id_apis_chunk_param_lists(tmp_path):
    """Select-all on a large library produces id lists beyond
    SQLITE_MAX_VARIABLE_NUMBER (32766 on modern builds) — every bulk-id
    API must chunk or stage rather than inline one placeholder per id."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)

    huge = [pid] + list(range(10_000_000, 10_033_000))  # > 32766 ids

    db.batch_update_photo_rating(huge, 4, verify_workspace=False)
    db.batch_update_photo_flag(huge, "flagged", verify_workspace=False)
    # The set path inserts per-id (no IN clause); only the lookup and
    # removal paths take id lists into one statement.
    db.batch_set_color_label([pid], "red")
    labels = db.get_color_labels_for_photos(huge)
    db.batch_set_color_label(huge, None)

    photo = db.get_photo(pid)
    assert photo["rating"] == 4
    assert photo["flag"] == "flagged"
    assert labels == {pid: "red"}
    assert db.get_color_labels_for_photos([pid]) == {}  # removal applied

    # Scope-clause consumers and the reclassify purge must not raise either.
    counts = db.count_real_detections_in_scope(photo_ids=huge, min_conf=0.2)
    assert counts is not None
    db.clear_predictions(model="some-model", collection_photo_ids=huge)

    # Downstream lookups invoked by export/pipeline jobs after the outer
    # query/scope chunks must also chunk — the export job feeds the entire
    # filtered id list into get_photos_by_ids + get_species_keywords_for_photos,
    # and pipeline.load_photo_features feeds the entire scoped id list into
    # get_detections_for_photos twice.
    photos_map = db.get_photos_by_ids(huge)
    assert pid in photos_map  # the one real row survives the chunked select
    species_map = db.get_species_keywords_for_photos(huge)
    assert species_map == {}  # no keywords attached, but no OperationalError
    det_map = db.get_detections_for_photos(huge, min_conf=0)
    assert det_map == {}


def test_move_folder_path_does_not_touch_wildcard_siblings(db):
    """LIKE treats _ and % as wildcards — moving /pics/my_dir must not
    rewrite the unrelated sibling /pics/myXdir's children."""
    fid = db.add_folder("/pics/my_dir", name="my_dir")
    sib = db.add_folder("/pics/myXdir", name="myXdir")
    sib_child = db.add_folder("/pics/myXdir/sub", name="sub", parent_id=sib)

    db.move_folder_path(fid, "/dest/dir")

    moved = db.conn.execute("SELECT path FROM folders WHERE id = ?", (fid,)).fetchone()
    sibling = db.conn.execute("SELECT path FROM folders WHERE id = ?", (sib,)).fetchone()
    sibling_child = db.conn.execute("SELECT path FROM folders WHERE id = ?", (sib_child,)).fetchone()
    assert moved["path"] == "/dest/dir"
    assert sibling["path"] == "/pics/myXdir"
    assert sibling_child["path"] == "/pics/myXdir/sub"


def test_move_folder_path_is_case_sensitive(db):
    """Path cascades must not use SQLite LIKE's ASCII case folding."""
    fid = db.add_folder("/Photos/2024", name="2024")
    child = db.add_folder("/Photos/2024/trip", name="trip", parent_id=fid)
    sib = db.add_folder("/photos/2024", name="lower-2024")
    sib_child = db.add_folder("/photos/2024/sibling", name="sibling", parent_id=sib)

    db.move_folder_path(fid, "/Archive/2024")

    moved = db.conn.execute("SELECT path FROM folders WHERE id = ?", (fid,)).fetchone()
    moved_child = db.conn.execute("SELECT path FROM folders WHERE id = ?", (child,)).fetchone()
    sibling = db.conn.execute("SELECT path FROM folders WHERE id = ?", (sib,)).fetchone()
    sibling_child = db.conn.execute("SELECT path FROM folders WHERE id = ?", (sib_child,)).fetchone()
    assert moved["path"] == "/Archive/2024"
    assert os.path.normpath(moved_child["path"]) == os.path.normpath("/Archive/2024/trip")
    assert sibling["path"] == "/photos/2024"
    assert sibling_child["path"] == "/photos/2024/sibling"


def test_merge_staged_tree_new_subfolders(db):
    """Staged tree merged under an existing tracked base: new date folders
    are repointed under the base, parent_id fixed, workspace linked, and the
    base's existing photos are untouched."""
    ws = db._active_workspace_id

    # Existing tracked archive base with one prior shoot, linked as a root.
    base_id = db.add_folder("/arch/USA", name="USA")
    old_id = db.add_folder("/arch/USA/2025/2025-01-01", name="2025-01-01",
                           parent_id=base_id)
    db.add_photo(folder_id=old_id, filename="old.raf", extension=".raf",
                 file_size=100, file_mtime=1.0)
    db.add_workspace_folder(ws, base_id, is_root=True)

    # Staged tree (post-rsync the files already live at /arch/USA/...). Created
    # with workspace_root=False so the staged rows are NOT pre-linked as roots.
    # The intermediate year folder is a real catalog row (a scan would create
    # it) so the reparenting chain is exercised end to end.
    stage_root = db.add_folder("/stage/USA", name="USA", workspace_root=False)
    stage_year = db.add_folder("/stage/USA/2026", name="2026",
                               parent_id=stage_root, workspace_root=False)
    stage_leaf = db.add_folder("/stage/USA/2026/2026-06-30", name="2026-06-30",
                               parent_id=stage_year, workspace_root=False)
    db.add_photo(folder_id=stage_leaf, filename="new.raf", extension=".raf",
                 file_size=200, file_mtime=2.0)

    db.merge_staged_tree_into_archive(stage_root, "/arch/USA")

    # New leaf now lives under the base, parented to its (new) target parent.
    leaf = db.conn.execute(
        "SELECT id, parent_id FROM folders WHERE path = ?",
        ("/arch/USA/2026/2026-06-30",),
    ).fetchone()
    assert leaf is not None
    year = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", ("/arch/USA/2026",),
    ).fetchone()
    assert year is not None
    assert leaf["parent_id"] == year["id"]

    # The staged root and leaf rows are gone (folded into the existing base).
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path = ?", ("/stage/USA",)
    ).fetchone() is None
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path LIKE '/stage/%'"
    ).fetchone() is None

    # The new photo moved with the folder; the old photo is untouched.
    assert db.conn.execute(
        "SELECT folder_id FROM photos WHERE filename = ?", ("new.raf",)
    ).fetchone()["folder_id"] == leaf["id"]
    assert db.conn.execute(
        "SELECT folder_id FROM photos WHERE filename = ?", ("old.raf",)
    ).fetchone()["folder_id"] == old_id

    # New leaf is linked to the workspace as a non-root (base is the root).
    row = db.conn.execute(
        "SELECT is_root FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
        (ws, leaf["id"]),
    ).fetchone()
    assert row is not None and row["is_root"] == 0


def test_merge_staged_tree_downgrades_staged_root_leaf(db):
    """Regression: a staged photo-bearing leaf that the scanner registered as
    its OWN workspace root (workspace_folders.is_root=1) must be demoted to a
    plain descendant when it is folded under the existing archive base.

    This reproduces the real trigger: the staging scan restricts to the leaf
    dir, so add_folder links it as a root. add_workspace_folder's INSERT OR
    IGNORE cannot downgrade that pre-existing is_root=1 row, so the merge must
    UPDATE it to 0 explicitly — otherwise a stray second workspace root is left
    inside the archive. The prior new-subfolders test seeds the staged rows with
    workspace_root=False, so it would pass even with the fix removed; this one
    genuinely exercises the downgrade."""
    ws = db._active_workspace_id

    # Existing tracked archive base, linked as the workspace root.
    base_id = db.add_folder("/arch/USA", name="USA")
    db.add_workspace_folder(ws, base_id, is_root=True)

    # Staged tree post-rsync. The leaf is registered as its OWN root, exactly as
    # the staging scanner does (restrict_dirs => is_root=1 on the scanned leaf).
    stage_root = db.add_folder("/stage/USA", name="USA", workspace_root=False)
    stage_year = db.add_folder("/stage/USA/2026", name="2026",
                               parent_id=stage_root, workspace_root=False)
    stage_leaf = db.add_folder("/stage/USA/2026/2026-06-30", name="2026-06-30",
                               parent_id=stage_year, workspace_root=True)
    db.add_photo(folder_id=stage_leaf, filename="new.raf", extension=".raf",
                 file_size=200, file_mtime=2.0)

    # Precondition: the staged leaf really IS a workspace root before the merge,
    # so this test genuinely exercises the downgrade path.
    pre = db.conn.execute(
        "SELECT is_root FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
        (ws, stage_leaf),
    ).fetchone()
    assert pre is not None and pre["is_root"] == 1

    db.merge_staged_tree_into_archive(stage_root, "/arch/USA")

    # The folded leaf now lives under the archive as a NON-root descendant.
    leaf = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?",
        ("/arch/USA/2026/2026-06-30",),
    ).fetchone()
    assert leaf is not None
    leaf_link = db.conn.execute(
        "SELECT is_root FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
        (ws, leaf["id"]),
    ).fetchone()
    assert leaf_link is not None and leaf_link["is_root"] == 0

    # The archive base is the single remaining workspace root.
    roots = db.conn.execute(
        "SELECT folder_id FROM workspace_folders "
        "WHERE workspace_id=? AND is_root=1",
        (ws,),
    ).fetchall()
    assert [r["folder_id"] for r in roots] == [base_id]


def test_merge_staged_tree_existing_folder_and_collision(db, tmp_path):
    """Staged folder folds into a pre-existing target date-folder: a
    same-filename staged photo (even one carrying keyword links) whose archived
    file REALLY exists on disk is dropped as already-archived, a genuinely-new
    one is reparented, and the counts use the settled photo-count names.
    Regression guard for the photo_keywords FK that has no ON DELETE CASCADE —
    deleting the collided photo without first clearing its keyword rows would
    raise FOREIGN KEY constraint failed."""
    ws = db._active_workspace_id

    # Real on-disk archive so the collision drop's "archived file exists" check
    # sees the byte-identical file rsync --ignore-existing preserved. Both the
    # staged and archived rows carry the SAME file_hash — a real collision (as
    # opposed to a phantom-row replacement) only reaches the merge when the
    # bytes on both sides are identical, because the upstream content-conflict
    # check would abort the whole move otherwise.
    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026" / "2026-06-30"
    date_dir.mkdir(parents=True)
    (date_dir / "dup.raf").write_bytes(b"archived-dup")
    (date_dir / "keep.raf").write_bytes(b"archived-keep")

    base_id = db.add_folder(str(arch), name="USA")
    year_id = db.add_folder(str(arch / "2026"), name="2026", parent_id=base_id)
    date_id = db.add_folder(str(date_dir), name="2026-06-30", parent_id=year_id)
    db.add_photo(folder_id=date_id, filename="dup.raf", extension=".raf",
                 file_size=12, file_mtime=1.0, file_hash="DUPHASH")
    db.add_photo(folder_id=date_id, filename="keep.raf", extension=".raf",
                 file_size=13, file_mtime=1.0, file_hash="KEEPHASH")
    db.add_workspace_folder(ws, base_id, is_root=True)

    # Staged tree post-rsync. The intermediate year folder is a real row so the
    # reparent chain is exercised; the leaf already exists at the target.
    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA", workspace_root=False)
    stage_year = db.add_folder(str(stage / "2026"), name="2026",
                               parent_id=stage_root, workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026" / "2026-06-30"),
                               name="2026-06-30",
                               parent_id=stage_year, workspace_root=False)
    dup_pid = db.add_photo(folder_id=stage_leaf, filename="dup.raf",
                           extension=".raf", file_size=12, file_mtime=2.0,
                           file_hash="DUPHASH")
    db.add_photo(folder_id=stage_leaf, filename="fresh.raf", extension=".raf",
                 file_size=200, file_mtime=2.0, file_hash="FRESHHASH")

    # Attach a keyword to the COLLIDED staged photo so the delete must clear
    # photo_keywords first; without the fix this raises a FK error.
    kw = db.add_keyword("Osprey")
    db.tag_photo(dup_pid, kw)

    counts = db.merge_staged_tree_into_archive(stage_root, str(arch))

    # No duplicate dup.raf row; fresh.raf reparented into the existing folder.
    names = {r["filename"] for r in db.conn.execute(
        "SELECT filename FROM photos WHERE folder_id = ?", (date_id,))}
    assert names == {"dup.raf", "keep.raf", "fresh.raf"}
    assert db.conn.execute(
        "SELECT COUNT(*) c FROM photos WHERE filename='dup.raf'"
    ).fetchone()["c"] == 1
    # The dropped staged photo's keyword links are gone too.
    assert db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ?", (dup_pid,)
    ).fetchone() is None

    # Settled count names with correct values: 1 new photo (fresh.raf), no new
    # folders (every staged folder mapped to an existing target — root, year,
    # and leaf all pre-exist under the archive), 3 merged folders, 1 dropped.
    assert counts["new_photos"] == 1
    assert counts["new_folders"] == 0
    assert counts["merged_folders"] == 3
    assert counts["already_present"] == 1

    # All staged rows folded away.
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path LIKE ?", (str(stage) + "%",)
    ).fetchone() is None


def test_merge_staged_tree_links_archive_to_active_workspace(db):
    """Regression: when the pre-existing archive base is linked only to a
    DIFFERENT workspace, merging staged photos into it must link the base
    (and its subtree) to the active workspace too. Workspace-scoped photo
    queries join ``workspace_folders`` on ``p.folder_id``; without the link,
    the else-branch UPDATE moves photos onto a ``target["id"]`` that has no
    ``workspace_folders`` row for the active ws, and every merged-in photo
    silently vanishes from ws-scoped views even though the import reports
    success. Reproduces via two workspaces: the archive lives under ``other``
    only, and ``active`` runs the merge."""
    # Archive lives under the default workspace; ``active`` never scans it.
    other_ws = db._active_workspace_id
    base_id = db.add_folder("/arch/USA", name="USA")
    date_id = db.add_folder("/arch/USA/2025-06-30", name="2025-06-30",
                            parent_id=base_id, workspace_root=False)
    db.add_photo(folder_id=date_id, filename="prior.raf", extension=".raf",
                 file_size=100, file_mtime=1.0)

    # Switch to a fresh workspace — the one running the pipeline.
    active_ws = db.create_workspace("Active")
    db.set_active_workspace(active_ws)

    # Precondition: the active ws cannot see the archive yet.
    assert db.conn.execute(
        "SELECT 1 FROM workspace_folders "
        "WHERE workspace_id=? AND folder_id=?",
        (active_ws, base_id),
    ).fetchone() is None

    # Staged tree post-rsync; a real scan restrict_dirs registers the leaf
    # as its own root, but the base_id-invisible visibility bug reproduces
    # regardless of that flag, so keep the setup minimal.
    stage_root = db.add_folder("/stage/USA", name="USA", workspace_root=False)
    stage_leaf = db.add_folder("/stage/USA/2025-06-30", name="2025-06-30",
                               parent_id=stage_root, workspace_root=False)
    new_pid = db.add_photo(folder_id=stage_leaf, filename="new.raf",
                           extension=".raf", file_size=200, file_mtime=2.0)

    db.merge_staged_tree_into_archive(stage_root, "/arch/USA")

    # Archive base is now the active workspace's root; the subtree pull in
    # add_workspace_folder linked the existing date folder along with it.
    base_link = db.conn.execute(
        "SELECT is_root FROM workspace_folders "
        "WHERE workspace_id=? AND folder_id=?",
        (active_ws, base_id),
    ).fetchone()
    assert base_link is not None and base_link["is_root"] == 1
    assert db.conn.execute(
        "SELECT 1 FROM workspace_folders "
        "WHERE workspace_id=? AND folder_id=?",
        (active_ws, date_id),
    ).fetchone() is not None

    # The other workspace's link on the archive base is preserved — the
    # is_root UPDATE in add_workspace_folder is scoped by workspace_id.
    other_link = db.conn.execute(
        "SELECT is_root FROM workspace_folders "
        "WHERE workspace_id=? AND folder_id=?",
        (other_ws, base_id),
    ).fetchone()
    assert other_link is not None and other_link["is_root"] == 1

    # The merged-in photo landed on the existing date folder and is now
    # visible via the exact workspace_folders join every scoped query uses.
    row = db.conn.execute(
        """SELECT p.filename FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE p.id = ? AND wf.workspace_id = ?""",
        (new_pid, active_ws),
    ).fetchone()
    assert row is not None and row["filename"] == "new.raf"


def test_merge_staged_tree_new_descendant_roots_tracked_ancestor_for_active_ws(
        db):
    """Regression: when the archive destination is a BRAND-NEW subfolder
    inside a tracked archive that belongs only to a DIFFERENT workspace,
    the merge used to skip the ``if archive_row:`` linking block (the new
    subfolder has no folder row yet), then the reconciliation loop
    reparented the staged root onto the new archive path and explicitly
    demoted it to ``is_root=0``. The active workspace was left with NO
    ``is_root=1`` row covering the merged tree, and
    ``get_workspace_folder_roots()`` (which filters on ``is_root=1``)
    returned nothing — the newly-archived photos silently disappeared from
    the active workspace even though the import reported success.

    The fix roots the deepest tracked ancestor in the active workspace
    when no root ancestor is present, so the merged tree has a visible
    anchor."""
    # Archive tracked only under the default ws.
    other_ws = db._active_workspace_id
    tracked_id = db.add_folder("/arch", name="arch")
    db.add_workspace_folder(other_ws, tracked_id, is_root=True)

    # Switch to a fresh workspace with NO link to /arch and NO root
    # ancestor above it.
    active_ws = db.create_workspace("Active")
    db.set_active_workspace(active_ws)
    assert db.conn.execute(
        "SELECT 1 FROM workspace_folders "
        "WHERE workspace_id=? AND folder_id=?",
        (active_ws, tracked_id),
    ).fetchone() is None

    # Staged tree destined for a NEW subfolder inside /arch — no folder row
    # for /arch/NewShoot exists yet.
    stage_root = db.add_folder("/stage/NewShoot", name="NewShoot",
                               workspace_root=False)
    stage_leaf = db.add_folder("/stage/NewShoot/2026-06-30", name="2026-06-30",
                               parent_id=stage_root, workspace_root=False)
    new_pid = db.add_photo(folder_id=stage_leaf, filename="new.raf",
                           extension=".raf", file_size=200, file_mtime=2.0)

    db.merge_staged_tree_into_archive(stage_root, "/arch/NewShoot")

    # The tracked ancestor /arch is now a root of the active workspace, so
    # get_workspace_folder_roots() has an anchor for the merged tree.
    ancestor_link = db.conn.execute(
        "SELECT is_root FROM workspace_folders "
        "WHERE workspace_id=? AND folder_id=?",
        (active_ws, tracked_id),
    ).fetchone()
    assert ancestor_link is not None and ancestor_link["is_root"] == 1

    # The merged photo is visible via the workspace_folders join every
    # ws-scoped query uses.
    row = db.conn.execute(
        """SELECT p.filename FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE p.id = ? AND wf.workspace_id = ?""",
        (new_pid, active_ws),
    ).fetchone()
    assert row is not None and row["filename"] == "new.raf"

    # The other workspace's root on /arch is preserved (is_root scoping in
    # add_workspace_folder is per-workspace).
    other_link = db.conn.execute(
        "SELECT is_root FROM workspace_folders "
        "WHERE workspace_id=? AND folder_id=?",
        (other_ws, tracked_id),
    ).fetchone()
    assert other_link is not None and other_link["is_root"] == 1


def test_merge_staged_tree_ancestor_base_stays_non_root(db):
    """Regression: when the active workspace already has a managed root ANCESTOR
    of the archive base (import into an existing subfolder — root ``/Photos``,
    base ``/Photos/USA``), the merge must LINK the base to the workspace but NOT
    root it. Rooting it would leave ``/Photos`` and ``/Photos/USA`` as two
    overlapping workspace roots. When there is no root ancestor the base IS the
    root (covered by the other merge tests)."""
    ws = db._active_workspace_id

    # Existing workspace root ancestor.
    root_id = db.add_folder("/Photos", name="Photos")
    base_id = db.add_folder("/Photos/USA", name="USA", parent_id=root_id)
    db.add_workspace_folder(ws, root_id, is_root=True)

    # Staged tree lands at a genuinely-new date subfolder inside the base.
    stage_root = db.add_folder("/stage/USA", name="USA", workspace_root=False)
    stage_leaf = db.add_folder("/stage/USA/2026-06-30", name="2026-06-30",
                               parent_id=stage_root, workspace_root=False)
    db.add_photo(folder_id=stage_leaf, filename="new.raf", extension=".raf",
                 file_size=200, file_mtime=2.0)

    db.merge_staged_tree_into_archive(stage_root, "/Photos/USA")

    # The base is linked to the workspace but as a NON-root.
    base_link = db.conn.execute(
        "SELECT is_root FROM workspace_folders "
        "WHERE workspace_id=? AND folder_id=?", (ws, base_id),
    ).fetchone()
    assert base_link is not None and base_link["is_root"] == 0

    # /Photos remains the SOLE workspace root — no duplicate overlapping root.
    roots = [r["folder_id"] for r in db.conn.execute(
        "SELECT folder_id FROM workspace_folders "
        "WHERE workspace_id=? AND is_root=1", (ws,))]
    assert roots == [root_id]


def test_merge_staged_tree_materializes_missing_intermediate_parent(db):
    """Regression: when the archive destination is nested inside a tracked
    root but an intermediate archive folder has never been scanned (e.g.
    ``/Photos`` tracked and the user imports to ``/Photos/2026/NewShoot``),
    the storage preflight creates ``/Photos/2026`` on disk via rsync's
    parent-dir setup but never opens a folder row for it. Without the fix
    the reconciliation repoints the staged root to ``/Photos/2026/NewShoot``
    with ``parent_id=NULL`` — floating it outside the managed archive tree
    and breaking every parent-based subtree operation (cascade path
    renames, ``_folder_subtree_ids_by_path``, tree UI). The merge must
    materialize the missing intermediate rows top-down so the staged
    root's ``parent_id`` resolves to the freshly-created ``/Photos/2026``
    row, and each intermediate must be linked to the active workspace as
    a non-root (they sit under the existing tracked root).
    """
    ws = db._active_workspace_id

    # Existing tracked archive root — a single folder row, no descendants
    # under it in the catalog. The intermediate ``/Photos/2026`` was
    # never scanned.
    root_id = db.add_folder("/Photos", name="Photos")
    db.add_workspace_folder(ws, root_id, is_root=True)

    # Staged tree that will land at /Photos/2026/NewShoot after rsync.
    stage_root = db.add_folder("/stage/NewShoot", name="NewShoot",
                               workspace_root=False)
    stage_leaf = db.add_folder("/stage/NewShoot/2026-06-30",
                               name="2026-06-30",
                               parent_id=stage_root, workspace_root=False)
    db.add_photo(folder_id=stage_leaf, filename="new.raf", extension=".raf",
                 file_size=100, file_mtime=1.0)

    db.merge_staged_tree_into_archive(stage_root, "/Photos/2026/NewShoot")

    # The intermediate archive folder now has a real folder row parented
    # under the tracked root — not floating with parent_id=NULL.
    mid = db.conn.execute(
        "SELECT id, parent_id FROM folders WHERE path = ?",
        ("/Photos/2026",),
    ).fetchone()
    assert mid is not None, (
        "missing intermediate /Photos/2026 should be materialized so the "
        "reparented staged root has a real parent row to point at"
    )
    assert mid["parent_id"] == root_id

    # The staged root, now repointed to the archive destination, is
    # correctly parented under the freshly-created intermediate.
    dest = db.conn.execute(
        "SELECT parent_id FROM folders WHERE path = ?",
        ("/Photos/2026/NewShoot",),
    ).fetchone()
    assert dest is not None
    assert dest["parent_id"] == mid["id"]

    # /Photos remains the SOLE workspace root — no duplicate overlapping
    # root created by the materialized intermediates or the reparented
    # staged root.
    roots = [r["folder_id"] for r in db.conn.execute(
        "SELECT folder_id FROM workspace_folders "
        "WHERE workspace_id=? AND is_root=1", (ws,))]
    assert roots == [root_id]

    # The intermediate is linked to the active workspace as a non-root so
    # workspace-scoped queries over the tree include it.
    mid_link = db.conn.execute(
        "SELECT is_root FROM workspace_folders "
        "WHERE workspace_id=? AND folder_id=?", (ws, mid["id"]),
    ).fetchone()
    assert mid_link is not None and mid_link["is_root"] == 0


def test_merge_staged_tree_intermediate_insert_survives_race(db):
    """Regression: two concurrent local-processing jobs targeting siblings
    inside the same tracked archive (e.g. ``/Photos/2026/A`` and
    ``/Photos/2026/B`` while only ``/Photos`` is tracked) can both snapshot
    the shared intermediate ``/Photos/2026`` as missing during walk-up, then
    race to insert it. The archive-destination reservation lets them run
    because the leaf paths don't overlap. Without ``INSERT OR IGNORE`` +
    requery, the loser would raise ``IntegrityError`` on the intermediate's
    ``folders.path`` UNIQUE constraint AFTER all staging/processing work is
    already done, stranding the merge. The reconciliation must instead fall
    back to the winner's row and finish parenting the staged tree under it.
    """
    ws = db._active_workspace_id

    # Tracked archive root; the shared intermediate /Photos/2026 has never
    # been scanned (no folder row for it yet).
    root_id = db.add_folder("/Photos", name="Photos")
    db.add_workspace_folder(ws, root_id, is_root=True)

    # Staged tree that will land at /Photos/2026/A after rsync.
    stage_root = db.add_folder("/stage/A", name="A", workspace_root=False)
    db.add_photo(folder_id=stage_root, filename="a.raf", extension=".raf",
                 file_size=100, file_mtime=1.0)

    # Simulate the race: the walk-up probe above has already snapshotted
    # /Photos/2026 as missing; here we sneak in a competing insert JUST
    # before the reconciliation's own INSERT fires, standing in for the
    # winning concurrent job's commit. Without INSERT OR IGNORE the loser's
    # plain INSERT would raise here. Wrap the connection (sqlite3.Connection
    # is C-implemented and doesn't allow assigning to .execute directly).
    real_conn = db.conn

    class _RacingConn:
        race_inserted = False

        def execute(self_wrap, sql, params=()):
            if (not _RacingConn.race_inserted
                    and isinstance(sql, str)
                    and sql.startswith("INSERT OR IGNORE INTO folders")):
                real_conn.execute(
                    "INSERT INTO folders (path, name, parent_id) "
                    "VALUES (?, ?, ?)",
                    ("/Photos/2026", "2026", root_id),
                )
                _RacingConn.race_inserted = True
            return real_conn.execute(sql, params)

        def __getattr__(self_wrap, name):
            return getattr(real_conn, name)

    db.conn = _RacingConn()
    try:
        db.merge_staged_tree_into_archive(stage_root, "/Photos/2026/A")
    finally:
        db.conn = real_conn

    # The race was actually triggered (guards against a future refactor that
    # renames the INSERT and silently no-ops this simulation).
    assert _RacingConn.race_inserted, (
        "expected the reconciliation to run INSERT OR IGNORE for the missing "
        "intermediate row — the simulation didn't fire, so the race is no "
        "longer exercised"
    )

    # Exactly one /Photos/2026 row survives (the winner's), and the staged
    # tree ended up parented under it — no duplicates, no NULL parent, no
    # UNIQUE-constraint crash.
    mid_rows = db.conn.execute(
        "SELECT id, parent_id FROM folders WHERE path = ?",
        ("/Photos/2026",),
    ).fetchall()
    assert len(mid_rows) == 1
    mid_id = mid_rows[0]["id"]
    assert mid_rows[0]["parent_id"] == root_id
    dest = db.conn.execute(
        "SELECT parent_id FROM folders WHERE path = ?",
        ("/Photos/2026/A",),
    ).fetchone()
    assert dest is not None and dest["parent_id"] == mid_id


def test_merge_staged_tree_missing_target_file_keeps_new_photo(db, tmp_path):
    """Defensive: a filename collision against a STALE target row whose file is
    missing on disk must NOT drop the staged photo as ``already_present``. The
    upstream content-conflict check couldn't fire (no dest file), so rsync
    copied the new staged bytes; dropping the staged row would leave those bytes
    represented by the phantom row's hash/metadata and lose the new photo.
    Instead the phantom row is removed and the staged photo reparented in its
    place, so the surviving row describes the real on-disk file."""
    ws = db._active_workspace_id

    # Real on-disk archive, but the archived file is ABSENT (stale catalog row).
    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-06-30"
    date_dir.mkdir(parents=True)
    # NOTE: no missing.raf written to date_dir — the row is stale.

    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-06-30", parent_id=base_id)
    stale_pid = db.add_photo(folder_id=date_id, filename="missing.raf",
                             extension=".raf", file_size=100, file_mtime=1.0,
                             file_hash="STALEHASH")
    db.add_workspace_folder(ws, base_id, is_root=True)

    # Staged photo shares the basename but is the freshly-copied real file.
    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA", workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026-06-30"), name="2026-06-30",
                               parent_id=stage_root, workspace_root=False)
    new_pid = db.add_photo(folder_id=stage_leaf, filename="missing.raf",
                           extension=".raf", file_size=200, file_mtime=2.0,
                           file_hash="NEWHASH")

    counts = db.merge_staged_tree_into_archive(stage_root, str(arch))

    # The staged photo survived and now lives on the archive date folder — it
    # was NOT dropped as already_present.
    assert counts["already_present"] == 0
    assert counts["new_photos"] == 1
    surviving = db.conn.execute(
        "SELECT id, folder_id, file_hash FROM photos WHERE filename='missing.raf'"
    ).fetchall()
    assert len(surviving) == 1
    assert surviving[0]["id"] == new_pid
    assert surviving[0]["folder_id"] == date_id
    assert surviving[0]["file_hash"] == "NEWHASH"
    # The stale phantom row is gone (replaced by the real bytes' row).
    assert db.conn.execute(
        "SELECT 1 FROM photos WHERE id = ?", (stale_pid,)
    ).fetchone() is None


def test_merge_staged_tree_phantom_row_replaces_when_file_on_disk_after_rsync(
    db, tmp_path,
):
    """Regression for the exact production ordering: ``move_folder`` runs
    rsync ``--ignore-existing`` FIRST, then calls
    ``merge_staged_tree_into_archive``. If the target row was a phantom
    (its file missing before rsync), rsync creates the file at the target
    path from the staged bytes — so by the time the merge's collision
    check runs, ``os.path.exists`` returns True in the phantom case too
    and can't tell it apart from a real byte-identical collision. Left
    unfixed, the staged photo would be dropped as ``already_present`` and
    the freshly-copied bytes would be represented by the phantom row's
    stale hash/metadata — the newly-imported photo silently disappears
    behind the stale row.

    Distinguish real collision from phantom by the target row's
    bytes-identity (``file_hash``) instead of raw file existence.
    """
    ws = db._active_workspace_id

    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-06-30"
    date_dir.mkdir(parents=True)
    # The archived file IS on disk — but only because rsync just copied it
    # in from staging over an empty slot. Its bytes are the STAGED bytes;
    # the target catalog row was a phantom that had lost track of the
    # deleted original.
    (date_dir / "collide.raf").write_bytes(b"fresh-staged-bytes")

    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-06-30",
                            parent_id=base_id)
    phantom_pid = db.add_photo(folder_id=date_id, filename="collide.raf",
                               extension=".raf", file_size=100,
                               file_mtime=1.0, file_hash="STALEHASH")
    db.add_workspace_folder(ws, base_id, is_root=True)

    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA", workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026-06-30"), name="2026-06-30",
                               parent_id=stage_root, workspace_root=False)
    new_pid = db.add_photo(folder_id=stage_leaf, filename="collide.raf",
                           extension=".raf", file_size=200, file_mtime=2.0,
                           file_hash="NEWHASH")

    counts = db.merge_staged_tree_into_archive(stage_root, str(arch))

    # The freshly-copied bytes are represented by the STAGED row (which
    # accurately describes them), not the stale phantom row.
    assert counts["already_present"] == 0
    assert counts["new_photos"] == 1
    surviving = db.conn.execute(
        "SELECT id, folder_id, file_hash FROM photos "
        "WHERE filename='collide.raf'"
    ).fetchall()
    assert len(surviving) == 1
    assert surviving[0]["id"] == new_pid
    assert surviving[0]["folder_id"] == date_id
    assert surviving[0]["file_hash"] == "NEWHASH"
    # The phantom row is gone; its cache files are reported for cleanup.
    assert db.conn.execute(
        "SELECT 1 FROM photos WHERE id = ?", (phantom_pid,)
    ).fetchone() is None
    assert phantom_pid in counts["dropped_photo_ids"]


def test_merge_staged_tree_defaults_to_phantom_when_hashes_missing(
    db, tmp_path,
):
    """When either row's ``file_hash`` is unset (older catalog rows, or a
    scan that failed to hash), the collision guard has no safe post-copy
    signal — ``file_size`` alone can coincidentally match a phantom row's
    stored size (empty XMP sidecars, small metadata files), and rehashing
    the on-disk file always matches the staged hash in the phantom case
    (rsync wrote the staged bytes). Default to phantom-replacement so the
    freshly-imported pipeline output survives; the alternative (silently
    dropping the new photo behind a same-size stale row) is the worse
    failure mode. Regression for the Codex finding at db.py:3221."""
    ws = db._active_workspace_id

    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-06-30"
    date_dir.mkdir(parents=True)
    # The archived file IS on disk (rsync landed the staged bytes there),
    # and its size EQUALS the phantom row's recorded size — the exact
    # coincidence the old size-fallback would have false-flagged as a
    # real collision, silently dropping the newly-imported photo.
    (date_dir / "collide.raf").write_bytes(b"x" * 100)

    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-06-30",
                            parent_id=base_id)
    # Phantom row: no file_hash (older catalog row that pre-dates
    # hashing) and a recorded size that HAPPENS to equal the freshly-
    # copied staged file's size.
    phantom_pid = db.add_photo(folder_id=date_id, filename="collide.raf",
                               extension=".raf", file_size=100,
                               file_mtime=1.0)
    db.add_workspace_folder(ws, base_id, is_root=True)

    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA", workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026-06-30"), name="2026-06-30",
                               parent_id=stage_root, workspace_root=False)
    # Staged photo also lacks a recorded hash (the unhashed-scan case).
    new_pid = db.add_photo(folder_id=stage_leaf, filename="collide.raf",
                           extension=".raf", file_size=100,
                           file_mtime=2.0)

    counts = db.merge_staged_tree_into_archive(stage_root, str(arch))

    # Without hash-based verification the guard defaults to phantom, so
    # the staged (newly-imported) photo is preserved and the unverifiable
    # target row is dropped.
    assert counts["already_present"] == 0
    assert counts["new_photos"] == 1
    surviving = db.conn.execute(
        "SELECT id FROM photos WHERE filename='collide.raf'"
    ).fetchall()
    assert len(surviving) == 1
    assert surviving[0]["id"] == new_pid
    assert db.conn.execute(
        "SELECT 1 FROM photos WHERE id = ?", (phantom_pid,)
    ).fetchone() is None
    # The phantom's cache files must still be reported for cleanup so a
    # rowid re-use can't inherit stale imagery.
    assert phantom_pid in counts["dropped_photo_ids"]


def test_merge_staged_tree_case_alias_collision_on_case_insensitive_volume(
    db, tmp_path, monkeypatch,
):
    """On a case-insensitive archive volume (default macOS APFS, Windows
    NTFS) an existing catalog row ``IMG.RAF`` and a staged ``img.raf`` are
    the same on-disk file: rsync ``--ignore-existing`` treats them as a
    match and skips the staged copy. Without a case-aware collision check
    the staged row would be reparented onto the target folder, leaving TWO
    catalog rows in one folder for the same file (SQLite text-equality is
    case-sensitive so UNIQUE(folder_id, filename) doesn't fire to catch
    the mistake). The staged row must be dropped as ``already_present``.
    """
    ws = db._active_workspace_id

    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-06-30"
    date_dir.mkdir(parents=True)
    # Only the upper-case archived file exists on disk. The staged
    # lower-case name would resolve to this same file on a case-folding
    # volume.
    (date_dir / "IMG.RAF").write_bytes(b"archived-bytes")

    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-06-30",
                            parent_id=base_id)
    archived_pid = db.add_photo(folder_id=date_id, filename="IMG.RAF",
                                extension=".raf", file_size=100,
                                file_mtime=1.0, file_hash="SAMEBYTES")
    db.add_workspace_folder(ws, base_id, is_root=True)

    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA", workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026-06-30"), name="2026-06-30",
                               parent_id=stage_root, workspace_root=False)
    # A real case-alias collision only reaches the merge step when the bytes
    # are identical (the upstream content-conflict check aborts the whole
    # move otherwise), so the two rows describe the same bytes and share a
    # hash. Modelling that here keeps the collision check's hash comparison
    # honest: mismatched hashes would (correctly) be treated as a phantom
    # row now, so a case-alias test with fake mismatching hashes would not
    # represent the production invariant it's asserting.
    staged_pid = db.add_photo(folder_id=stage_leaf, filename="img.raf",
                              extension=".raf", file_size=100,
                              file_mtime=2.0, file_hash="SAMEBYTES")

    # Force the merge to treat the target volume as case-insensitive
    # regardless of the CI host's actual filesystem behavior (Linux ext4
    # is case-sensitive, so the underlying ``_case_insensitive_root``
    # probe would otherwise return None and the fix wouldn't be exercised
    # in this test environment).
    import move
    monkeypatch.setattr(move, "_case_insensitive_root", lambda p: "/")
    # And make ``os.path.exists`` treat the case-alias lookup as present,
    # since a real case-insensitive volume would resolve
    # ``.../IMG.raf`` / ``.../img.raf`` back to the same on-disk file.
    real_exists = os.path.exists
    def case_insensitive_exists(p):
        if real_exists(p):
            return True
        # Fall back to a case-insensitive directory listing.
        parent = os.path.dirname(p)
        base = os.path.basename(p).lower()
        try:
            entries = os.listdir(parent)
        except OSError:
            return False
        return any(e.lower() == base for e in entries)
    monkeypatch.setattr("db.os.path.exists", case_insensitive_exists)

    counts = db.merge_staged_tree_into_archive(stage_root, str(arch))

    # The staged row was dropped, not reparented — one row remains for the
    # archived file.
    rows = db.conn.execute(
        "SELECT id, filename FROM photos WHERE folder_id = ?", (date_id,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["id"] == archived_pid
    assert rows[0]["filename"] == "IMG.RAF"
    assert db.conn.execute(
        "SELECT 1 FROM photos WHERE id = ?", (staged_pid,)
    ).fetchone() is None
    assert counts["already_present"] == 1
    assert counts["new_photos"] == 0


def test_merge_staged_tree_intra_staged_case_alias_collision(
    db, tmp_path, monkeypatch,
):
    """Regression: two staged files whose filenames differ only in case
    land in the same staged folder (e.g. staged on a case-sensitive disk,
    archiving to a case-insensitive volume like APFS/SMB). rsync
    ``--ignore-existing`` writes the FIRST file and silently skips the
    second, so the second row's bytes never land on disk. Without an
    intra-staged claim tracker, both rows get reparented into the target
    folder — SQLite text-equality is case-sensitive so UNIQUE(folder_id,
    filename) doesn't fire, and the catalog ends up with two rows for one
    on-disk file (losing the second image). The second staged row must be
    dropped as ``already_present`` instead of reparented.
    """
    ws = db._active_workspace_id

    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-06-30"
    date_dir.mkdir(parents=True)
    # Empty target folder — no pre-existing catalog rows and (initially)
    # no on-disk photos. The archive-side folder rows exist but the merge
    # tracker must catch the intra-staged collision itself.
    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-06-30",
                            parent_id=base_id)
    db.add_workspace_folder(ws, base_id, is_root=True)

    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA", workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026-06-30"), name="2026-06-30",
                               parent_id=stage_root, workspace_root=False)
    # Two staged photos with case-differing filenames. Different hashes
    # model the realistic case where the two source files have DIFFERENT
    # bytes — rsync writes only one of them to disk and the other row
    # describes bytes that never landed. Even if the file hashes matched,
    # only one on-disk file can exist on a case-insensitive volume so a
    # second catalog row is still wrong.
    first_pid = db.add_photo(folder_id=stage_leaf, filename="IMG.RAF",
                             extension=".raf", file_size=100,
                             file_mtime=1.0, file_hash="FIRSTBYTES")
    second_pid = db.add_photo(folder_id=stage_leaf, filename="img.raf",
                              extension=".raf", file_size=200,
                              file_mtime=2.0, file_hash="OTHERBYTES")

    # Force the merge to treat the target volume as case-insensitive
    # regardless of the CI host's actual filesystem behavior.
    import move
    monkeypatch.setattr(move, "_case_insensitive_root", lambda p: "/")

    counts = db.merge_staged_tree_into_archive(stage_root, str(arch))

    # Exactly one photo row ends up in the target folder — the first
    # staged row was reparented, the second was dropped as
    # ``already_present`` (and its id reported for cache cleanup).
    rows = db.conn.execute(
        "SELECT id, filename FROM photos WHERE folder_id = ?", (date_id,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["id"] == first_pid
    assert rows[0]["filename"] == "IMG.RAF"
    assert db.conn.execute(
        "SELECT 1 FROM photos WHERE id = ?", (second_pid,)
    ).fetchone() is None
    assert counts["already_present"] == 1
    assert counts["new_photos"] == 1
    assert second_pid in counts["dropped_photo_ids"]


def test_merge_staged_tree_intra_staged_case_alias_case_sensitive_volume(
    db, tmp_path, monkeypatch,
):
    """On a case-sensitive archive volume (Linux ext4), two staged files
    ``IMG.RAF`` and ``img.raf`` are distinct on-disk files and both should
    be reparented into the target folder — the intra-staged claim tracker
    must NOT trigger. Guards against a regression where the tracker
    normalizes with ``str.casefold`` unconditionally and drops legitimate
    distinct files on case-sensitive targets.
    """
    ws = db._active_workspace_id

    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-06-30"
    date_dir.mkdir(parents=True)
    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-06-30",
                            parent_id=base_id)
    db.add_workspace_folder(ws, base_id, is_root=True)

    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA", workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026-06-30"), name="2026-06-30",
                               parent_id=stage_root, workspace_root=False)
    upper_pid = db.add_photo(folder_id=stage_leaf, filename="IMG.RAF",
                             extension=".raf", file_size=100,
                             file_mtime=1.0, file_hash="UPPERBYTES")
    lower_pid = db.add_photo(folder_id=stage_leaf, filename="img.raf",
                             extension=".raf", file_size=200,
                             file_mtime=2.0, file_hash="LOWERBYTES")

    # Case-sensitive target: ``_case_insensitive_root`` returns None.
    import move
    monkeypatch.setattr(move, "_case_insensitive_root", lambda p: None)

    counts = db.merge_staged_tree_into_archive(stage_root, str(arch))

    rows = {r["filename"]: r["id"] for r in db.conn.execute(
        "SELECT id, filename FROM photos WHERE folder_id = ? ORDER BY filename",
        (date_id,),
    ).fetchall()}
    assert rows == {"IMG.RAF": upper_pid, "img.raf": lower_pid}
    assert counts["already_present"] == 0
    assert counts["new_photos"] == 2


def test_merge_staged_tree_rolls_back_on_error(db, monkeypatch):
    """Regression: ``merge_staged_tree_into_archive`` runs a long sequence of
    UPDATE/DELETE statements before a single final commit. An exception
    partway through the reconciliation loop must ``rollback()`` — otherwise
    the pending mutations sit on the connection and a later unrelated commit
    silently persists a half-merged catalog. Matches the pattern used by
    ``delete_folder``, ``move_folders_to_workspace``, and
    ``_merge_duplicate_keywords_pass``.

    The concrete pending-mutation this test forces is a photo reparent from
    a staged else-branch iteration (existing target folder). The else-branch
    contains no ``add_workspace_folder`` call, so its ``UPDATE photos SET
    folder_id`` is genuinely pending until the end-of-body commit — a raise
    before that commit must roll it back."""
    ws = db._active_workspace_id

    # Existing tracked archive base + a pre-existing date folder so the
    # first staged-folder iteration hits the else-branch (target exists),
    # not the if-branch (which would commit mid-loop through
    # ``add_workspace_folder`` and defeat the rollback demonstration).
    base_id = db.add_folder("/arch/USA", name="USA")
    existing_date_id = db.add_folder("/arch/USA/2025-12-25",
                                     name="2025-12-25", parent_id=base_id)
    db.add_photo(folder_id=existing_date_id, filename="old.raf",
                 extension=".raf", file_size=100, file_mtime=1.0)
    db.add_workspace_folder(ws, base_id, is_root=True)

    # Staged tree with two else-branch iterations. The stage_root maps to
    # ``/arch/USA`` (base_id, exists), and the stage_leaf maps to
    # ``/arch/USA/2025-12-25`` (existing_date_id, exists). We seed the
    # stage_leaf photo so iteration 1 does its pending
    # ``UPDATE photos SET folder_id`` first, then iteration 2 blows up on
    # the ``_case_insensitive_root`` probe.
    stage_root = db.add_folder("/stage/USA", name="USA",
                               workspace_root=False)
    staged_root_pid = db.add_photo(
        folder_id=stage_root, filename="root_new.raf",
        extension=".raf", file_size=150, file_mtime=1.5,
    )
    stage_leaf = db.add_folder("/stage/USA/2025-12-25",
                               name="2025-12-25", parent_id=stage_root,
                               workspace_root=False)
    staged_leaf_pid = db.add_photo(
        folder_id=stage_leaf, filename="leaf_new.raf",
        extension=".raf", file_size=200, file_mtime=2.0,
    )

    # Poison ``_case_insensitive_root`` to succeed on the FIRST call (so
    # iteration 1 completes its pending photo reparent) and raise on the
    # SECOND (so iteration 2 crashes AFTER iteration 1's pending
    # ``UPDATE photos SET folder_id`` is queued but before the end-of-body
    # commit).
    import move as move_mod
    real_ci_root = move_mod._case_insensitive_root
    call_count = {"n": 0}

    def _flaky(path):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise RuntimeError("case-fold probe blew up mid-merge")
        return real_ci_root(path)

    monkeypatch.setattr(move_mod, "_case_insensitive_root", _flaky)

    import pytest as _pytest
    with _pytest.raises(RuntimeError, match="case-fold probe blew up"):
        db.merge_staged_tree_into_archive(stage_root, "/arch/USA")

    # Iteration 1's pending ``UPDATE photos SET folder_id = base_id`` was
    # rolled back: the staged photo is still on stage_root, NOT on base_id.
    # Without the try/except + rollback, the pending UPDATE would sit on the
    # connection and any later commit on the same connection would persist
    # it — a silent half-merge.
    assert db.conn.execute(
        "SELECT folder_id FROM photos WHERE id = ?", (staged_root_pid,)
    ).fetchone()["folder_id"] == stage_root
    # Iteration 2's staged photo is untouched (the raise fired before its
    # photo loop even ran).
    assert db.conn.execute(
        "SELECT folder_id FROM photos WHERE id = ?", (staged_leaf_pid,)
    ).fetchone()["folder_id"] == stage_leaf
    # Neither staged folder was folded into the archive — stage_root's row
    # still lives under its staging path, not deleted or reparented.
    assert db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", ("/stage/USA",)
    ).fetchone()["id"] == stage_root
    # The pre-existing archived photo is untouched.
    old_photo = db.conn.execute(
        "SELECT folder_id FROM photos WHERE filename = ?", ("old.raf",)
    ).fetchone()
    assert old_photo["folder_id"] == existing_date_id
    # Confirm the poison actually fired twice (i.e. we did reach the raise
    # via iteration 2, so iteration 1's pending mutation genuinely happened
    # and the rollback is what returned staged_root_pid to stage_root).
    assert call_count["n"] == 2


def test_merge_staged_tree_new_folder_reparent_rolls_back_on_later_error(
        db, monkeypatch):
    """Regression: the new-folder branch (``target is None``) used to call
    ``self.add_workspace_folder(...)`` — which commits the connection — so
    a later exception's ``rollback()`` could not undo the preceding
    ``UPDATE folders SET path = ?, parent_id = ?`` on the same staged row.
    That left a half-merged catalog: the staged folder's path pointed at
    the archive even though the merge as a whole was rolled back.

    The fix is a non-committing helper (``_add_workspace_folder_no_commit``)
    used inside the loop. This test exercises specifically the mid-loop
    new-folder path followed by a later else-branch crash and verifies the
    new-folder iteration's UPDATE is fully rolled back — a companion to
    ``test_merge_staged_tree_rolls_back_on_error`` (which covers the
    else-branch photo reparent)."""
    ws = db._active_workspace_id

    # Existing tracked archive base + one pre-existing descendant so the
    # deepest staged iteration hits the else-branch (crash point).
    base_id = db.add_folder("/arch/USA", name="USA")
    existing_id = db.add_folder("/arch/USA/existing_leaf",
                                name="existing_leaf", parent_id=base_id)
    db.add_workspace_folder(ws, base_id, is_root=True)

    # Three staged folders ordered by length so iteration order is:
    #   1. stage_root       → target=/arch/USA (EXISTS, else)
    #   2. stage_new        → target=/arch/USA/new_leaf (NEW, if-branch — the
    #                          iteration whose rollback we're verifying)
    #   3. stage_existing   → target=/arch/USA/existing_leaf (EXISTS, else —
    #                          the iteration we poison to force the raise)
    stage_root = db.add_folder("/stage/USA", name="USA",
                               workspace_root=False)
    stage_new = db.add_folder("/stage/USA/new_leaf", name="new_leaf",
                              parent_id=stage_root, workspace_root=False)
    stage_new_pid = db.add_photo(folder_id=stage_new, filename="new.raf",
                                 extension=".raf",
                                 file_size=100, file_mtime=1.0)
    stage_existing = db.add_folder("/stage/USA/existing_leaf",
                                   name="existing_leaf",
                                   parent_id=stage_root, workspace_root=False)
    db.add_photo(folder_id=stage_existing, filename="dup.raf",
                 extension=".raf", file_size=100, file_mtime=1.0)

    # Poison ``_case_insensitive_root`` to succeed on the first call (else
    # iteration 1, stage_root → base) and raise on the second call
    # (else iteration 3, stage_existing → existing_leaf). Iteration 2
    # (if-branch, stage_new → new target) queues an UPDATE folders SET
    # path/parent_id + a workspace-folder link BETWEEN those two calls — the
    # raise fires with iteration 2's mutations still uncommitted, so a
    # correct rollback must revert stage_new's path.
    import move as move_mod
    real_ci_root = move_mod._case_insensitive_root
    calls = {"n": 0}

    def _flaky(path):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise RuntimeError("case-fold probe blew up mid-merge")
        return real_ci_root(path)

    monkeypatch.setattr(move_mod, "_case_insensitive_root", _flaky)

    import pytest as _pytest
    with _pytest.raises(RuntimeError, match="case-fold probe blew up"):
        db.merge_staged_tree_into_archive(stage_root, "/arch/USA")

    # Poison actually fired on iteration 3 (2nd call) — confirms iteration 2
    # completed BEFORE the raise, so its UPDATE is exactly what should have
    # rolled back.
    assert calls["n"] == 2

    # stage_new's path UPDATE + parent_id UPDATE was rolled back: still
    # ``/stage/USA/new_leaf``, parented to stage_root, not to base_id.
    # Without the fix, the mid-loop commit inside ``add_workspace_folder``
    # would have persisted the path change and this row would sit at
    # ``/arch/USA/new_leaf`` with base_id as parent.
    row = db.conn.execute(
        "SELECT path, parent_id FROM folders WHERE id = ?", (stage_new,)
    ).fetchone()
    assert row["path"] == "/stage/USA/new_leaf"
    assert row["parent_id"] == stage_root
    # No archive row at the target path was left behind.
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path = ?", ("/arch/USA/new_leaf",)
    ).fetchone() is None
    # The staged photo still lives under stage_new.
    assert db.conn.execute(
        "SELECT folder_id FROM photos WHERE id = ?", (stage_new_pid,)
    ).fetchone()["folder_id"] == stage_new


def test_merge_staged_tree_restores_missing_archive_base_status(db):
    """Regression: an existing archive row marked ``status='missing'`` (e.g.
    a health scan ran while the drive was unmounted) must be flipped back to
    ``ok`` when the merge succeeds, otherwise the ws-scoped photo queries
    (which filter ``status IN ('ok', 'partial')``) hide the newly-merged
    photos even though the import reported success."""
    ws = db._active_workspace_id
    base_id = db.add_folder("/arch/USA", name="USA")
    db.add_workspace_folder(ws, base_id, is_root=True)
    # Simulate a prior health scan that saw the drive unmounted.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (base_id,))
    db.conn.commit()

    stage_root = db.add_folder("/stage/USA", name="USA",
                               workspace_root=False)
    db.add_photo(folder_id=stage_root, filename="new.raf",
                 extension=".raf", file_size=100, file_mtime=1.0)

    db.merge_staged_tree_into_archive(stage_root, "/arch/USA")

    assert db.conn.execute(
        "SELECT status FROM folders WHERE id = ?", (base_id,)
    ).fetchone()["status"] == "ok"


def test_merge_staged_tree_restores_missing_target_folder_status(
        db, tmp_path):
    """Regression: same as ``restores_missing_archive_base_status`` but for
    an existing DESCENDANT target folder that the merge folds staged photos
    into. Any lingering ``missing`` from an older health scan must flip to
    ``ok`` after the merge so photos aren't hidden."""
    ws = db._active_workspace_id
    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-01-01"
    date_dir.mkdir(parents=True)

    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-01-01",
                            parent_id=base_id)
    db.add_workspace_folder(ws, base_id, is_root=True)
    # Mark the DESCENDANT folder as missing.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (date_id,))
    db.conn.commit()

    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA",
                               workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026-01-01"), name="2026-01-01",
                               parent_id=stage_root, workspace_root=False)
    db.add_photo(folder_id=stage_leaf, filename="fresh.raf",
                 extension=".raf", file_size=100, file_mtime=1.0)

    db.merge_staged_tree_into_archive(stage_root, str(arch))

    assert db.conn.execute(
        "SELECT status FROM folders WHERE id = ?", (date_id,)
    ).fetchone()["status"] == "ok"


def test_merge_staged_tree_partial_archive_base_status_preserved(db):
    """The status restore only migrates ``missing`` → ``ok``. A folder
    marked ``partial`` (some verified photos missing on disk) still has
    unverified state and should remain ``partial`` after the merge — the
    merge only proves the newly-added files exist, not the earlier ones."""
    ws = db._active_workspace_id
    base_id = db.add_folder("/arch/USA", name="USA")
    db.add_workspace_folder(ws, base_id, is_root=True)
    db.conn.execute(
        "UPDATE folders SET status = 'partial' WHERE id = ?", (base_id,))
    db.conn.commit()

    stage_root = db.add_folder("/stage/USA", name="USA",
                               workspace_root=False)
    db.add_photo(folder_id=stage_root, filename="new.raf",
                 extension=".raf", file_size=100, file_mtime=1.0)

    db.merge_staged_tree_into_archive(stage_root, "/arch/USA")

    assert db.conn.execute(
        "SELECT status FROM folders WHERE id = ?", (base_id,)
    ).fetchone()["status"] == "partial"


def test_merge_staged_tree_reports_dropped_photo_ids_for_collisions(
        db, tmp_path):
    """Regression: cached thumbnails/previews/working copies are keyed off
    ``photos.id``. When the merge drops a staged photo as ``already_present``
    (identical file already archived), the freed photo id would leave orphan
    cache files on disk; SQLite reuses freed rowids so a later import that
    lands on the same id would inherit stale imagery. The merge must report
    the dropped ids up to the caller so it can clean the on-disk cache."""
    ws = db._active_workspace_id
    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-01-01"
    date_dir.mkdir(parents=True)
    # The archived file must exist on disk for the collision to be treated
    # as ``already_present`` (a phantom target row is instead REPLACED).
    (date_dir / "dup.raf").write_bytes(b"archived")

    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-01-01",
                            parent_id=base_id)
    # Matching ``file_hash`` on both sides is required for the collision
    # guard to treat this as a real collision — the row's byte-identity
    # claim (hash) has to match the staged photo's for the drop to be
    # safe in the post-copy path. See merge_staged_tree_into_archive's
    # hash-only invariant.
    db.add_photo(folder_id=date_id, filename="dup.raf", extension=".raf",
                 file_size=8, file_mtime=1.0, file_hash="DUPHASH")
    db.add_workspace_folder(ws, base_id, is_root=True)

    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA",
                               workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026-01-01"), name="2026-01-01",
                               parent_id=stage_root, workspace_root=False)
    dup_pid = db.add_photo(folder_id=stage_leaf, filename="dup.raf",
                           extension=".raf", file_size=8, file_mtime=2.0,
                           file_hash="DUPHASH")

    counts = db.merge_staged_tree_into_archive(stage_root, str(arch))

    assert counts["already_present"] == 1
    assert dup_pid in counts["dropped_photo_ids"]


def test_merge_staged_tree_reports_dropped_photo_ids_for_phantom_target(
        db, tmp_path):
    """Regression: when the target folder has a filename-collision row whose
    on-disk file is MISSING (a phantom row from a prior stale state), the
    merge deletes the phantom and reparents the staged photo. The phantom's
    freed photo id must also be reported as dropped so its cache files get
    cleaned."""
    ws = db._active_workspace_id
    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-01-01"
    date_dir.mkdir(parents=True)
    # NOTE: don't create dup.raf — the phantom target row points at a
    # non-existent archived file.

    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-01-01",
                            parent_id=base_id)
    phantom_pid = db.add_photo(folder_id=date_id, filename="dup.raf",
                               extension=".raf",
                               file_size=8, file_mtime=1.0)
    db.add_workspace_folder(ws, base_id, is_root=True)

    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA",
                               workspace_root=False)
    stage_leaf = db.add_folder(str(stage / "2026-01-01"), name="2026-01-01",
                               parent_id=stage_root, workspace_root=False)
    db.add_photo(folder_id=stage_leaf, filename="dup.raf",
                 extension=".raf", file_size=8, file_mtime=2.0)

    counts = db.merge_staged_tree_into_archive(stage_root, str(arch))

    # No ``already_present`` this time (the collision was a phantom, and the
    # staged bytes represent the surviving row).
    assert counts["already_present"] == 0
    # The phantom id was freed and must be reported for cache cleanup.
    assert phantom_pid in counts["dropped_photo_ids"]


def test_folder_under_rule_excludes_siblings_and_escapes_wildcards(tmp_path, monkeypatch):
    """'folder under /photos/2023' must match that folder and its
    descendants only — not the sibling /photos/2023-trip — and a _ in the
    value must not act as a LIKE wildcard."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    f_2023 = db.add_folder("/photos/2023", name="2023")
    f_sub = db.add_folder("/photos/2023/trip", name="trip", parent_id=f_2023)
    f_sib = db.add_folder("/photos/2023-trip", name="2023-trip")
    f_us = db.add_folder("/photos/my_dir", name="my_dir")
    f_usx = db.add_folder("/photos/myXdir", name="myXdir")
    for fid, name in [(f_2023, "a"), (f_sub, "b"), (f_sib, "c"), (f_us, "d"), (f_usx, "e")]:
        db.add_photo(folder_id=fid, filename=f"{name}.jpg", extension=".jpg",
                     file_size=100, file_mtime=1.0)

    under_2023 = [{"field": "folder", "op": "under", "value": "/photos/2023"}]
    assert db.count_photos_for_rules(under_2023) == 2  # folder itself + descendant

    not_under_2023 = [{"field": "folder", "op": "not_under", "value": "/photos/2023"}]
    assert db.count_photos_for_rules(not_under_2023) == 3

    under_us = [{"field": "folder", "op": "under", "value": "/photos/my_dir"}]
    assert db.count_photos_for_rules(under_us) == 1  # not /photos/myXdir


def test_folder_under_rule_matches_backslash_paths(tmp_path, monkeypatch):
    """Windows libraries store folder paths with backslash separators
    (``str(Path(...))`` in scanner.scan). The 'folder under' rule must
    still match descendants of a backslash-delimited root and exclude
    siblings; a LIKE pattern that hard-codes '/%' would silently miss
    every Windows descendant."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    f_root = db.add_folder("C:\\Photos\\2023", name="2023")
    f_sub = db.add_folder("C:\\Photos\\2023\\trip", name="trip", parent_id=f_root)
    f_sib = db.add_folder("C:\\Photos\\2023-trip", name="2023-trip")
    for fid, name in [(f_root, "a"), (f_sub, "b"), (f_sib, "c")]:
        db.add_photo(folder_id=fid, filename=f"{name}.jpg", extension=".jpg",
                     file_size=100, file_mtime=1.0)

    # Forward-slash rule value still applies to a Windows library because
    # both sides are normalized to '/'.
    under = [{"field": "folder", "op": "under", "value": "C:/Photos/2023"}]
    assert db.count_photos_for_rules(under) == 2  # root + descendant, not sibling

    # Backslash rule values match symmetrically (normalized before escaping).
    under_bs = [{"field": "folder", "op": "under", "value": "C:\\Photos\\2023"}]
    assert db.count_photos_for_rules(under_bs) == 2

    not_under = [{"field": "folder", "op": "not_under", "value": "C:/Photos/2023"}]
    assert db.count_photos_for_rules(not_under) == 1  # only the sibling

    under_sib = [{"field": "folder", "op": "under", "value": "C:/Photos/2023-trip"}]
    assert db.count_photos_for_rules(under_sib) == 1


def test_folder_legacy_is_op_resolves_like_under(tmp_path, monkeypatch):
    """Folder collections saved with the pre-'under' vocabulary use op 'is'
    (and 'is not'). Those legacy ops must resolve as aliases for
    'under'/'not_under' instead of raising 'unsupported field/op', which
    previously 500'd the whole /api/collections list and blanked every
    collection dropdown."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    f_2023 = db.add_folder("/photos/2023", name="2023")
    f_sub = db.add_folder("/photos/2023/trip", name="trip", parent_id=f_2023)
    f_sib = db.add_folder("/photos/2023-trip", name="2023-trip")
    for fid, name in [(f_2023, "a"), (f_sub, "b"), (f_sib, "c")]:
        db.add_photo(folder_id=fid, filename=f"{name}.jpg", extension=".jpg",
                     file_size=100, file_mtime=1.0)

    legacy_is = [{"field": "folder", "op": "is", "value": "/photos/2023"}]
    assert db.count_photos_for_rules(legacy_is) == 2  # folder + descendant

    legacy_is_not = [{"field": "folder", "op": "is not", "value": "/photos/2023"}]
    assert db.count_photos_for_rules(legacy_is_not) == 1  # only the sibling

    # 'equals' is the third legacy alias; it also resolves like 'under'
    # (the folder plus its descendants), matching how the rule behaved
    # when the older UI wrote it.
    legacy_equals = [{"field": "folder", "op": "equals", "value": "/photos/2023"}]
    assert db.count_photos_for_rules(legacy_equals) == 2

    # count_collection_photos (used by /api/collections) must not raise.
    import json
    cid = db.add_collection("Legacy Folder", json.dumps(legacy_is))
    assert db.count_collection_photos(cid) == 2


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
    assert all("folder_name" in dict(r) for r in results)
    assert all("folder_path" in dict(r) for r in results)
    assert all("keyword_names" in dict(r) for r in results)


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


def test_get_highlights_candidates_photo_id_restricts_query(tmp_path):
    """photo_id filter returns only that photo's row, ignoring workspace peers."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    f = db.add_folder('/shoot', name='shoot')
    p1 = db.add_photo(folder_id=f, filename='a.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=f, filename='b.jpg', extension='.jpg',
                      file_size=100, file_mtime=2.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id IN (?, ?)",
                    (p1, p2))
    db.conn.commit()

    only = db.get_highlights_candidates(folder_id=None, min_quality=0.0,
                                        photo_id=p1)
    assert [r["id"] for r in only] == [p1]

    # Non-existent id yields no rows without erroring.
    assert db.get_highlights_candidates(folder_id=None, min_quality=0.0,
                                        photo_id=999999) == []


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


def test_get_highlights_candidates_returns_predicted_species(tmp_path):
    """Photos with no accepted species but a classifier prediction
    expose ``predicted_species`` and ``predicted_confidence`` columns."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/p', name='p')
    pid = db.add_photo(folder_id=fid, filename='bird.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.6 WHERE id = ?", (pid,))
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'test', 'ʻApapane', 0.82)",
        (did,),
    )
    db.conn.commit()

    rows = db.get_highlights_candidates(folder_id=fid, min_quality=0.0)
    assert len(rows) == 1
    assert rows[0]["predicted_species"] == "ʻApapane"
    assert abs(rows[0]["predicted_confidence"] - 0.82) < 1e-6
    # No accepted keyword → species is None
    assert rows[0]["species"] is None


def test_get_highlights_candidates_predicted_picks_highest_confidence(tmp_path):
    """When a photo has multiple detections with different predictions,
    the highest-confidence one is returned."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/p', name='p')
    pid = db.add_photo(folder_id=fid, filename='two.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.5 WHERE id = ?", (pid,))
    d1 = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    d2 = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'Boring Bird', 0.40)",
        (d1,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'Cool Bird', 0.90)",
        (d2,),
    )
    db.conn.commit()

    rows = db.get_highlights_candidates(folder_id=fid, min_quality=0.0)
    assert rows[0]["predicted_species"] == "Cool Bird"
    assert abs(rows[0]["predicted_confidence"] - 0.90) < 1e-6


def test_get_highlights_candidates_predicted_excludes_rejected(tmp_path):
    """Predictions the user rejected in the active workspace do not
    appear as the fallback species."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/p', name='p')
    pid = db.add_photo(folder_id=fid, filename='r.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.5 WHERE id = ?", (pid,))
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    pred_id = db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'Wrong Bird', 0.95)",
        (did,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'rejected')",
        (pred_id, db._ws_id()),
    )
    db.conn.commit()

    rows = db.get_highlights_candidates(folder_id=fid, min_quality=0.0)
    assert rows[0]["predicted_species"] is None
    assert rows[0]["predicted_confidence"] is None


def test_get_highlights_candidates_predicted_uses_latest_fingerprint(tmp_path):
    """A reclassified detection (new labels_fingerprint) should bucket the
    photo under the current classifier results, not an older high-confidence
    prediction that the rest of the app no longer surfaces.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/p', name='p')
    pid = db.add_photo(folder_id=fid, filename='reclass.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.5 WHERE id = ?", (pid,))
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    # Older prediction: higher confidence but stale labels_fingerprint.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence, created_at) "
        "VALUES (?, 'm', 'OLD', 'Stale Bird', 0.95, '2025-01-01 00:00:00')",
        (did,),
    )
    # Newer prediction: lower confidence but current labels_fingerprint.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence, created_at) "
        "VALUES (?, 'm', 'NEW', 'Fresh Bird', 0.60, '2026-01-01 00:00:00')",
        (did,),
    )
    db.conn.commit()

    rows = db.get_highlights_candidates(folder_id=fid, min_quality=0.0)
    assert rows[0]["predicted_species"] == "Fresh Bird"
    assert abs(rows[0]["predicted_confidence"] - 0.60) < 1e-6


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


def test_collection_group_any_and_none_rules(tmp_path):
    """Smart collections support grouped OR and NONE logic."""
    import json

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='five.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='pick.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p3 = db.add_photo(folder_id=fid, filename='other.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET rating = 5 WHERE id = ?", (p1,))
    db.conn.execute("UPDATE photos SET flag = 'flagged' WHERE id = ?", (p2,))
    db.conn.commit()

    any_rules = {
        "mode": "any",
        "rules": [
            {"field": "rating", "op": ">=", "value": 5},
            {"field": "flag", "op": "equals", "value": "flagged"},
        ],
    }
    cid = db.add_collection("Five or Pick", json.dumps(any_rules))
    assert [p["filename"] for p in db.get_collection_photos(cid, per_page=10)] == [
        "five.jpg",
        "pick.jpg",
    ]

    none_rules = {
        "mode": "none",
        "rules": [
            {"field": "rating", "op": ">=", "value": 5},
            {"field": "flag", "op": "equals", "value": "flagged"},
        ],
    }
    cid = db.add_collection("Neither", json.dumps(none_rules))
    assert [p["filename"] for p in db.get_collection_photos(cid, per_page=10)] == [
        "other.jpg",
    ]


def test_collection_vireo_native_fields(tmp_path):
    """Smart collections can target Vireo AI/quality workflow fields."""
    import json

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='match.jpg', extension='.jpg', file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='miss.jpg', extension='.jpg', file_size=100, file_mtime=1.0)

    db.conn.execute(
        "UPDATE photos SET quality_score = 0.91, mask_path = ?, "
        "active_mask_variant = 'sam2-large', latitude = 1.0, longitude = 2.0 "
        "WHERE id = ?",
        (str(tmp_path / "mask.png"), p1),
    )
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.20, latitude = 3.0, longitude = 4.0 "
        "WHERE id = ?",
        (p2,),
    )
    loc_kw = db.add_keyword("Yosemite", kw_type="location")
    db.tag_photo(p2, loc_kw)
    db.conn.execute(
        "INSERT INTO inat_submissions (photo_id, observation_id, observation_url) "
        "VALUES (?, 123, 'https://example.test/obs/123')",
        (p1,),
    )
    det_id = db.save_detections(
        p1,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.95, "category": "animal"}],
        "megadetector-v6",
    )[0]
    db.add_prediction(det_id, "Red Fox", 0.93, "bioclip", status="accepted")

    rules = {
        "mode": "all",
        "rules": [
            {"field": "quality_score", "op": ">=", "value": 0.9},
            {"field": "has_mask", "op": "equals", "value": 1},
            {"field": "active_mask_variant", "op": "equals", "value": "sam2-large"},
            {"field": "has_gps", "op": "equals", "value": 1},
            {"field": "location_keyword_missing", "op": "equals", "value": 1},
            {"field": "inat_submitted", "op": "equals", "value": 1},
            {"field": "prediction_confidence", "op": ">=", "value": 0.9},
            {"field": "classifier_model", "op": "equals", "value": "bioclip"},
            {"field": "prediction_status", "op": "equals", "value": "accepted"},
        ],
    }
    cid = db.add_collection("Vireo Native", json.dumps(rules))
    photos = db.get_collection_photos(cid, per_page=10)
    assert [p["filename"] for p in photos] == ["match.jpg"]


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


def test_migration_adds_miss_columns_to_existing_photos_table(tmp_path):
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    folder_id = db.add_folder("/photos", name="photos")
    db.add_photo(
        folder_id=folder_id,
        filename="legacy.jpg",
        extension=".jpg",
        file_size=100,
        file_mtime=1.0,
    )

    missing_cols = {
        "miss_no_subject", "miss_clipped", "miss_oof", "miss_computed_at",
    }
    rows = db.conn.execute("PRAGMA table_info(photos)").fetchall()
    keep = [row for row in rows if row["name"] not in missing_cols]
    definitions = []
    for row in keep:
        definition = f'"{row["name"]}" {row["type"]}'
        if row["pk"]:
            definition += " PRIMARY KEY"
        if row["notnull"]:
            definition += " NOT NULL"
        if row["dflt_value"] is not None:
            definition += f' DEFAULT {row["dflt_value"]}'
        definitions.append(definition)
    column_list = ", ".join(f'"{row["name"]}"' for row in keep)

    db.conn.execute("ALTER TABLE photos RENAME TO photos_current")
    db.conn.execute(f"CREATE TABLE photos ({', '.join(definitions)})")
    db.conn.execute(
        f"INSERT INTO photos ({column_list}) SELECT {column_list} FROM photos_current"
    )
    db.conn.execute("DROP TABLE photos_current")
    db.conn.commit()
    db.conn.close()

    db2 = Database(db_path)
    cols = {row["name"] for row in db2.conn.execute("PRAGMA table_info(photos)")}
    assert missing_cols.issubset(cols)

    photos = db2.get_photos()
    assert len(photos) == 1
    assert photos[0]["miss_no_subject"] is None
    assert photos[0]["miss_clipped"] is None
    assert photos[0]["miss_oof"] is None


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


def test_create_new_images_snapshot_file_count_matches_unique_paths(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    paths = ["/tmp/a/IMG_001.JPG", "/tmp/a/IMG_001.JPG", "/tmp/b/IMG_002.JPG"]

    snap_id = db.create_new_images_snapshot(paths)
    snap = db.get_new_images_snapshot(snap_id)

    assert snap["file_count"] == 2
    assert snap["file_paths"] == ["/tmp/a/IMG_001.JPG", "/tmp/b/IMG_002.JPG"]


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


def test_add_keyword_taxonomy_preserves_individual_type_and_is_species(tmp_path):
    """If an existing keyword has a deliberate non-general type (e.g.
    'individual'), a later add_keyword(..., kw_type='taxonomy') must NOT
    silently flip its type or stamp is_species=1. Otherwise auto-wildlife,
    backfill, and subject filters that include `OR is_species=1` would
    treat that individual row as a species.

    The current contract: deliberate non-general rows are NOT candidates
    for the typed-lookup. Caller asking for kw_type='taxonomy' on a name
    that exists only as 'individual' falls through to INSERT, creating
    a NEW taxonomy row alongside the preserved individual one
    (duplicates by name across types are intentional; lookups
    disambiguate by kw_type).
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))

    kid_individual = db.add_keyword("Charlie", kw_type="individual")
    row1 = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid_individual,)
    ).fetchone()
    assert row1["type"] == "individual"
    assert row1["is_species"] == 0

    kid_taxonomy = db.add_keyword("Charlie", kw_type="taxonomy")
    # A new row is created — the original individual row was not a
    # candidate for the typed lookup.
    assert kid_taxonomy != kid_individual

    # Original 'individual' row is unchanged.
    row1_after = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid_individual,)
    ).fetchone()
    assert row1_after["type"] == "individual", "deliberate type must be preserved"
    assert row1_after["is_species"] == 0, (
        "is_species must NOT be set on a preserved non-taxonomy row"
    )

    # New row has the requested taxonomy type with is_species=1.
    row2 = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid_taxonomy,)
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


def test_filter_out_wildlife_excluded_drops_marked_photos(tmp_path):
    """Explicit Not Wildlife state is independent of keyword subject tagging."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws = db.create_workspace("ws")
    db.set_active_workspace(ws)
    fid = db.add_folder('/photos', name='photos')
    db.add_workspace_folder(ws, fid)
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='p2.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    db.update_photo_wildlife_excluded(p1, True)

    assert db.filter_out_wildlife_excluded([p1, p2]) == [p2]


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


def test_get_subject_types_drops_non_string_entries(tmp_path, monkeypatch):
    """Regression: config_overrides may contain malformed JSON entries since
    api_update_workspace persists arbitrary values. Membership against a
    frozenset raises TypeError on unhashable input; the helper must filter
    non-strings before testing membership."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    # Persist deliberately-malformed subject_types via the workspace
    # config (mirrors what api_update_workspace would let through).
    db.update_workspace(ws_id, config_overrides={
        "subject_types": ["taxonomy", ["nested"], {"obj": 1}, 42, None, "genre"],
    })
    # Must not raise. Returns the string-and-valid subset.
    assert db.get_subject_types() == {"taxonomy", "genre"}


def test_update_keyword_rejects_non_string_type(tmp_path):
    """Regression: update_keyword(type=...) must raise ValueError (not
    TypeError) on non-hashable JSON values, so api_update_keyword's
    existing ValueError catch still translates to 400 instead of 500."""
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    kid = db.add_keyword("Tag", kw_type="general")
    with pytest.raises(ValueError, match="Invalid keyword type"):
        db.update_keyword(kid, type=[])
    with pytest.raises(ValueError, match="Invalid keyword type"):
        db.update_keyword(kid, type={"x": 1})
    with pytest.raises(ValueError, match="Invalid keyword type"):
        db.update_keyword(kid, type=42)


def test_filter_out_subject_tagged_chunks_large_input(tmp_path, monkeypatch):
    """Regression: photo_ids larger than the SQLite bind-var limit must be
    chunked, not passed as a single oversized IN clause. The classify job
    sources up to ~1M photos via get_collection_photos(per_page=999999),
    which would trip 'too many SQL variables' on builds with the historical
    999-var cap.

    Force a tiny chunk size so we exercise the chunking path with a small
    number of real photos, then assert filtering correctness across the
    boundary.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')

    # 25 photos, alternating tagged/untagged so the result spans multiple
    # chunks under our forced chunk size of 10.
    monkeypatch.setattr(Database, "_FILTER_SUBJECT_CHUNK", 10)
    genre_kid = db.add_keyword("Landscape", kw_type="genre")
    pids = []
    for i in range(25):
        p = db.add_photo(
            folder_id=fid, filename=f"p{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=float(i),
        )
        pids.append(p)
        # Tag every even-index photo with the genre keyword.
        if i % 2 == 0:
            db.tag_photo(p, genre_kid)

    kept = db.filter_out_subject_tagged(pids, {"genre"})
    expected = [p for i, p in enumerate(pids) if i % 2 == 1]
    assert kept == expected, (
        f"Chunking corrupted result. Expected odd-index photos, got {kept}"
    )


def test_filter_out_subject_tagged_excludes_legacy_is_species_when_taxonomy_requested(tmp_path):
    """Regression: ``filter_out_subject_tagged`` must drop photos tagged
    with a legacy species keyword (``is_species=1`` with non-taxonomy
    ``type``) when 'taxonomy' is in the requested subject_types. Upgraded
    DBs carry these rows until the background ``mark_species_keywords``
    pass retypes them; without this guard, the classify job would
    re-classify already-identified photos during that window.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='p2.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    # Plant a legacy species keyword on p1 (is_species=1, type='general').
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 1)",
        ("Robin",),
    )
    legacy_sp = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, legacy_sp),
    )
    db.conn.commit()

    kept = db.filter_out_subject_tagged([p1, p2], {"taxonomy"})
    assert kept == [p2], (
        "Legacy is_species=1 rows must count as taxonomy-tagged so upgraded "
        "DBs don't re-run classify on already-identified photos."
    )


def test_filter_out_subject_tagged_ignores_legacy_is_species_when_taxonomy_excluded(tmp_path):
    """Counter-test: when 'taxonomy' is not in the requested subject_types,
    is_species=1 must NOT exclude photos — only keywords whose type
    matches the requested set count."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 1)",
        ("Robin",),
    )
    legacy_sp = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, legacy_sp),
    )
    db.conn.commit()

    kept = db.filter_out_subject_tagged([p1], {"genre"})
    assert kept == [p1], (
        "is_species=1 must only count when 'taxonomy' is one of the "
        "requested subject_types; otherwise the legacy-species fallback "
        "would over-filter."
    )


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


def test_backfill_wildlife_genre_matches_legacy_is_species_rows(tmp_path):
    """Regression: on upgraded DBs, species rows can carry legacy
    ``is_species=1`` but a non-taxonomy ``type`` (e.g. NULL, 'general')
    until the background ``mark_species_keywords`` pass retypes them.

    The backfill runs at startup before that pass, so a query that joined
    only on ``type='taxonomy'`` matched zero rows yet still wrote the
    one-shot marker — and never re-ran. The backfill must also match
    ``is_species=1`` so upgraded photos get Wildlife on first startup,
    independent of background normalization timing.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    # Force a legacy-shaped species keyword via direct SQL: is_species=1
    # but type='general' (the shape an upgraded DB has before the
    # background mark_species_keywords pass converts type to 'taxonomy').
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 1)",
        ("Robin",),
    )
    sp = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, sp),
    )
    db.conn.commit()

    db.backfill_wildlife_genre()

    wildlife_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre'"
    ).fetchone()["id"]
    has_wildlife = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (p1, wildlife_id),
    ).fetchone() is not None
    assert has_wildlife is True, (
        "Backfill must match is_species=1 rows so upgraded DBs get Wildlife "
        "added on first startup, even before mark_species_keywords retypes "
        "legacy species keywords to type='taxonomy'."
    )


def test_backfill_wildlife_genre_after_mark_picks_up_plaintext_species(tmp_path):
    """Regression: plain-text species tags on upgraded DBs start as
    ``is_species=0`` and ``type != 'taxonomy'``; only ``mark_species_keywords``
    can retype them via taxonomy lookup. The Wildlife backfill is one-shot via
    ``wildlife_backfill_done`` and only matches ``type='taxonomy' OR is_species=1``,
    so it MUST run after ``mark_species_keywords`` — otherwise it sets the
    marker on a zero-row scan and never retries.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    # Plain-text species tag: type='general', is_species=0 — neither flag set.
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 0)",
        ("Robin",),
    )
    sp = cur.lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, sp),
    )
    db.conn.commit()

    class FakeTaxonomy:
        def lookup(self, name):
            return {"taxon_id": 1} if name == "Robin" else None

    # Order matches startup: mark first, then backfill.
    db.mark_species_keywords(FakeTaxonomy())
    db.backfill_wildlife_genre()

    wildlife_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre'"
    ).fetchone()["id"]
    has_wildlife = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (p1, wildlife_id),
    ).fetchone() is not None
    assert has_wildlife is True, (
        "Plain-text species tags must be retyped by mark_species_keywords "
        "before backfill_wildlife_genre runs, so the backfill scan sees them."
    )


def test_tag_photo_legacy_is_species_keyword_adds_wildlife_genre(tmp_path):
    """Regression: auto-Wildlife must fire for legacy species rows whose
    ``type`` hasn't been retyped to ``taxonomy`` yet (is_species=1 with
    non-taxonomy ``type``). On upgraded databases ``mark_species_keywords``
    runs in a background thread, so newly-tagged photos can briefly hit
    keywords that satisfy ``is_species=1`` but not ``type='taxonomy'``."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)

    # Plant a legacy-shaped species keyword: is_species=1 but type='general'
    # (the shape an upgraded DB has before mark_species_keywords retypes it).
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 1)",
        ("Robin",),
    )
    sp = cur.lastrowid
    db.conn.commit()

    db.tag_photo(p1, sp)

    wildlife_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre'"
    ).fetchone()["id"]
    has_wildlife = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (p1, wildlife_id),
    ).fetchone() is not None
    assert has_wildlife is True, (
        "Auto-Wildlife must trigger for legacy is_species=1 rows whose type "
        "hasn't been normalized yet — otherwise photos tagged during the "
        "background mark_species_keywords window permanently miss Wildlife."
    )


def test_tag_photo_legacy_species_sticky_removal_holds(tmp_path):
    """Regression complement: sticky removal must still hold across the
    mixed (legacy + retyped) species states. After removing Wildlife, a
    second species tag — even one that uses the legacy is_species=1 shape
    — must NOT re-add Wildlife as long as another species keyword is
    already attached."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    sp_a = db.add_keyword("Robin", is_species=True)
    db.tag_photo(p1, sp_a)  # First species — Wildlife auto-added.
    wildlife_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre'"
    ).fetchone()["id"]
    db.untag_photo(p1, wildlife_id)  # User removes Wildlife.

    # Plant a second species with the legacy shape.
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 1)",
        ("Sparrow",),
    )
    sp_b = cur.lastrowid
    db.conn.commit()
    db.tag_photo(p1, sp_b)

    has_wildlife = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (p1, wildlife_id),
    ).fetchone() is not None
    assert has_wildlife is False, (
        "Adding a second (legacy-shaped) species must respect the user's "
        "Wildlife removal — the count query must consider both 'taxonomy' "
        "and is_species=1 rows so existing species are seen."
    )


def test_tag_photo_no_op_re_tag_does_not_re_add_removed_wildlife(tmp_path):
    """Regression: re-tagging an already-tagged species (a no-op INSERT OR
    IGNORE) must NOT re-fire the auto-Wildlife rule, so user-removed
    Wildlife stays removed."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    sp = db.add_keyword("Robin", is_species=True)

    db.tag_photo(p1, sp)  # First species tag — Wildlife auto-added.
    wildlife_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre'"
    ).fetchone()["id"]
    db.untag_photo(p1, wildlife_id)  # User removes Wildlife.

    # Re-tag the same species (e.g. user clicks the keyword chip twice).
    # INSERT OR IGNORE is a no-op; auto-Wildlife must NOT fire.
    db.tag_photo(p1, sp)

    has_wildlife = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (p1, wildlife_id),
    ).fetchone() is not None
    assert has_wildlife is False, (
        "Re-tagging an already-attached species must not undo a user's "
        "Wildlife removal."
    )


def test_ensure_default_genre_keywords_upgrades_existing_general_wildlife(tmp_path):
    """Regression: an upgraded DB with an existing 'Wildlife' general keyword
    must have it promoted to type='genre' so auto-tagging finds it."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Wipe the genre defaults that __init__ seeded (simulate a DB that
    # never went through this code path) and force-create a 'general'
    # Wildlife row, mirroring an upgraded DB where the user hand-tagged.
    db.conn.execute("DELETE FROM keywords WHERE type = 'genre'")
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 0)",
        ("Wildlife",),
    )
    db.conn.commit()

    db.ensure_default_genre_keywords()

    row = db.conn.execute(
        "SELECT type FROM keywords WHERE name = 'Wildlife' AND parent_id IS NULL"
    ).fetchone()
    assert row["type"] == "genre", (
        "An existing 'general' Wildlife should be promoted to 'genre' so "
        "auto-Wildlife and the backfill find a canonical genre row."
    )
    # Other defaults should also be present (the seed loop still runs).
    n = db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE type = 'genre'"
    ).fetchone()[0]
    assert n >= 5  # Wildlife + Landscape + Sunset + Architecture + Abstract


def test_init_normalizes_legacy_wildlife_before_seeding_genres(tmp_path):
    """Regression: an upgraded DB where 'Wildlife' has a legacy type like
    'descriptive' must end up with type='genre' after Database.__init__,
    not stuck on 'general' (which the auto-Wildlife / backfill queries
    can't find via WHERE name='Wildlife' AND type='genre')."""
    from db import Database
    # First instantiation runs the schema setup AND the seeds. Tear that
    # down to simulate a pre-genre-feature DB: re-create the Wildlife
    # row as 'descriptive', clear all genre rows.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    db.conn.execute("DELETE FROM keywords WHERE type = 'genre'")
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'descriptive', 0)",
        ("Wildlife",),
    )
    db.conn.commit()
    db.conn.close()

    # Re-open: __init__ runs migrate_legacy_keyword_types BEFORE
    # ensure_default_genre_keywords, so Wildlife should be normalized to
    # 'genre' instead of getting stuck on 'general'.
    db2 = Database(db_path)
    rows = db2.conn.execute(
        "SELECT type FROM keywords WHERE name = 'Wildlife' AND parent_id IS NULL"
    ).fetchall()
    types = {r["type"] for r in rows}
    assert "genre" in types, (
        f"Legacy 'descriptive' Wildlife must end up as 'genre' after "
        f"__init__; got types={types}. The auto-Wildlife trigger and "
        f"backfill query depend on this."
    )


def test_ensure_default_genre_keywords_preserves_non_general_user_types(tmp_path):
    """If a user deliberately created e.g. an 'individual' Wildlife (a pet
    named Wildlife), ensure_default_genre_keywords must NOT clobber that."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.conn.execute("DELETE FROM keywords WHERE type = 'genre'")
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'individual', 0)",
        ("Wildlife",),
    )
    db.conn.commit()

    db.ensure_default_genre_keywords()

    rows = db.conn.execute(
        "SELECT type FROM keywords WHERE name = 'Wildlife' ORDER BY type"
    ).fetchall()
    types = sorted(r["type"] for r in rows)
    # The user's 'individual' row is preserved AND a canonical 'genre'
    # row is created alongside it. Duplicates BY NAME across different
    # types are intentional — disambiguation in add_keyword's lookup
    # (ORDER BY (type=?) DESC) ensures kw_type-typed callers get the
    # right row deterministically.
    assert types == ["genre", "individual"], (
        f"Expected both 'genre' (canonical) and 'individual' (user's) "
        f"Wildlife rows; got types={types}"
    )
    # Other genre defaults still seeded
    n_other = db.conn.execute(
        """SELECT COUNT(*) FROM keywords
           WHERE type = 'genre' AND name IN
                ('Landscape', 'Sunset', 'Architecture', 'Abstract')"""
    ).fetchone()[0]
    assert n_other == 4


def test_add_keyword_is_species_with_no_kw_type_treats_as_taxonomy(tmp_path):
    """Regression: species-accept flows (prediction accept, group apply)
    call add_keyword(name, is_species=True) without kw_type. With a
    same-name non-taxonomy row pre-existing (e.g. an 'individual' Robin
    that's a person), the lookup must treat is_species=True as
    kw_type='taxonomy' so the individual row isn't a candidate.

    Otherwise the species accept would return the individual row id,
    no taxonomy row is created, and the photo gets tagged with a
    non-taxonomy keyword that breaks species-specific behavior (taxon
    linkage, species workflows, subject_types semantics)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))

    # User has 'Robin' typed as 'individual' (their kid).
    kid_individual = db.add_keyword("Robin", kw_type="individual")

    # Classifier accept-prediction flow calls add_keyword with
    # is_species=True and no kw_type.
    kid_taxonomy = db.add_keyword("Robin", is_species=True)

    assert kid_taxonomy != kid_individual, (
        "is_species=True must NOT return a same-name 'individual' row. "
        "The species-accept flow needs a fresh taxonomy row."
    )
    row_taxonomy = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid_taxonomy,)
    ).fetchone()
    assert row_taxonomy["type"] == "taxonomy"
    assert row_taxonomy["is_species"] == 1

    # Original 'individual' Robin row is unchanged.
    row_individual = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid_individual,)
    ).fetchone()
    assert row_individual["type"] == "individual"
    assert row_individual["is_species"] == 0


def test_add_keyword_is_species_promotes_general_row(tmp_path):
    """A pre-existing 'general' Robin (legacy plain-text tag) is
    promoted to taxonomy by is_species=True (with no kw_type) — the
    typed-lookup includes 'general' as a promotable candidate."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))

    kid = db.add_keyword("Robin", kw_type="general")
    kid2 = db.add_keyword("Robin", is_species=True)
    assert kid2 == kid, "general row should be promoted in place"
    row = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1


def test_add_keyword_kw_type_falls_through_when_only_other_types_exist(tmp_path):
    """Regression: when kw_type is supplied but no same-type or 'general'
    row exists, add_keyword must NOT silently return a non-promotable
    other-typed row (e.g. 'location' or 'individual'). It must fall
    through to INSERT a new row with the requested type, so callers
    deterministically get back a row that matches kw_type.

    Reachable via POST /api/photos/<id>/keywords and /api/batch/keyword
    with an explicit type. Without this, the photo would be tagged with
    a wrong-typed row and stay stuck in 'Needs Identification'."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))

    # User has 'Backyard' typed as 'location' (a place they care about).
    # No 'general' or 'genre' Backyard exists.
    kid_location = db.add_keyword("Backyard", kw_type="location")

    # Caller asks for genre Backyard (e.g. via the lightbox flow tagging
    # a photo as a Backyard scene). The location row must NOT be returned.
    kid_genre = db.add_keyword("Backyard", kw_type="genre")
    assert kid_genre != kid_location, (
        "add_keyword('Backyard', kw_type='genre') returned the existing "
        "location row instead of creating a new genre row."
    )

    row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (kid_genre,)
    ).fetchone()
    assert row["type"] == "genre", (
        f"Expected the new row to have type='genre'; got {row['type']!r}"
    )

    # Original 'location' Backyard is unchanged.
    row_loc = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (kid_location,)
    ).fetchone()
    assert row_loc["type"] == "location"


def test_add_keyword_no_kw_type_prefers_canonical_genre_over_location(tmp_path):
    """Regression: when kw_type is omitted, the case-insensitive lookup
    must prefer the most structured interpretation. A user with both a
    hand-tagged 'location' Landscape and a canonical 'genre' Landscape
    should get the genre row when calling add_keyword('Landscape')
    with no kw_type — otherwise the photo would be tagged with the
    location row and stay stuck in 'Needs Identification' under the
    default subject_types."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    # Wipe the genre defaults; force-create a 'location' Landscape FIRST,
    # then a 'genre' Landscape — so id-ASC alone would pick location.
    db.conn.execute("DELETE FROM keywords WHERE type = 'genre'")
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'location', 0)",
        ("Landscape",),
    )
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'genre', 0)",
        ("Landscape",),
    )
    db.conn.commit()

    # No kw_type supplied. Must deterministically return the 'genre' row.
    kid = db.add_keyword("Landscape")
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "genre", (
        f"add_keyword('Landscape') without kw_type must prefer 'genre' "
        f"over 'location'; got {row['type']!r}"
    )


def test_add_keyword_no_kw_type_prefers_taxonomy_over_general(tmp_path):
    """The type-priority order picks taxonomy first when present. A
    species keyword that was once tagged plain-text and later promoted
    via mark_species_keywords (creating a parallel 'general' row that
    stuck around) should still be picked as the taxonomy row."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    # 'general' Robin first (older id), 'taxonomy' Robin second.
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 0)",
        ("Robin",),
    )
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'taxonomy', 1)",
        ("Robin",),
    )
    db.conn.commit()

    kid = db.add_keyword("Robin")
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "taxonomy"


def test_ensure_default_genre_keywords_seeds_canonical_row_despite_typed_collision(tmp_path):
    """Regression: the seed must guarantee a canonical genre row for each
    default name even when a user has previously tagged a same-name
    keyword with a different type (e.g. 'Landscape' as 'location').

    Otherwise the lightbox 'Not Wildlife' flow would call add_keyword(
    name='Landscape', kw_type='genre'), find the existing 'location' row,
    fail to upgrade it (only 'general' is upgraded), and tag the photo
    with a non-genre keyword — leaving it stuck in 'Needs Identification'
    under the default subject types.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Wipe the genre defaults and force-create same-name rows with
    # different types to simulate an upgraded user library.
    db.conn.execute("DELETE FROM keywords WHERE type = 'genre'")
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 0)",
        ("landscape",),  # lowercase — exercises COLLATE NOCASE
    )
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'location', 0)",
        ("Sunset",),  # user has a "Sunset" location they care about
    )
    db.conn.commit()

    db.ensure_default_genre_keywords()
    db.ensure_default_genre_keywords()  # idempotent re-run

    # 'landscape' (general) is PROMOTED to 'genre' via the in-place
    # UPDATE — no duplicate.
    rows = db.conn.execute(
        "SELECT type FROM keywords WHERE name = 'landscape' COLLATE NOCASE"
    ).fetchall()
    assert sorted(r["type"] for r in rows) == ["genre"], (
        f"'general' Landscape should be promoted in place to 'genre'; "
        f"got types={[r['type'] for r in rows]}"
    )

    # 'Sunset' (location) is PRESERVED, AND a canonical 'genre' Sunset
    # is created alongside it.
    rows = db.conn.execute(
        "SELECT type FROM keywords WHERE name = 'Sunset' COLLATE NOCASE"
    ).fetchall()
    assert sorted(r["type"] for r in rows) == ["genre", "location"], (
        f"Expected both 'genre' (canonical) and 'location' (user's) "
        f"Sunset rows; got types={[r['type'] for r in rows]}"
    )

    # add_keyword's lookup must deterministically pick the genre row
    # when kw_type='genre' is supplied (otherwise the route flow gets
    # the wrong-typed row non-deterministically).
    sunset_id = db.add_keyword("Sunset", kw_type="genre")
    sunset_type = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (sunset_id,)
    ).fetchone()["type"]
    assert sunset_type == "genre", (
        f"add_keyword('Sunset', kw_type='genre') must return the genre "
        f"row, got type={sunset_type!r}"
    )

    # Other defaults exist exactly once.
    for name in ("Architecture", "Abstract", "Wildlife"):
        n = db.conn.execute(
            "SELECT COUNT(*) FROM keywords WHERE name = ? COLLATE NOCASE",
            (name,),
        ).fetchone()[0]
        assert n == 1, f"Expected exactly one '{name}' row; got {n}"


def test_migrate_default_subject_collection_covers_all_workspaces(tmp_path):
    """Regression: the migration must rename the legacy collection in EVERY
    workspace, not just the currently-active one. Multi-workspace upgrades
    otherwise leave non-active workspaces stuck on has_species."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws1 = db.ensure_default_workspace()
    ws2 = db.create_workspace("ws-b")

    legacy_rule = json.dumps([{"field": "has_species", "op": "equals", "value": 0}])
    # Force-create the legacy collection in BOTH workspaces by direct SQL,
    # bypassing the active-workspace gating in add_collection.
    for ws in (ws1, ws2):
        db.conn.execute(
            "INSERT INTO collections (workspace_id, name, rules) VALUES (?, ?, ?)",
            (ws, "Needs Classification", legacy_rule),
        )
    db.conn.commit()

    # Active workspace is ws1; migration should cover ws2 as well.
    db.set_active_workspace(ws1)
    db.migrate_default_subject_collection()

    for ws in (ws1, ws2):
        row = db.conn.execute(
            "SELECT name, rules FROM collections "
            "WHERE workspace_id = ? AND name IN ('Needs Classification', 'Needs Identification')",
            (ws,),
        ).fetchone()
        assert row is not None, f"workspace {ws} lost its default collection"
        assert row["name"] == "Needs Identification", (
            f"workspace {ws} still has legacy 'Needs Classification' name"
        )
        assert json.loads(row["rules"]) == [
            {"field": "has_subject", "op": "equals", "value": 0},
            {"field": "wildlife_excluded", "op": "equals", "value": 0},
        ]


def test_backfill_wildlife_genre_runs_only_once(tmp_path):
    """Regression: backfill must be gated by a db_meta marker so subsequent
    startups don't re-add user-removed Wildlife."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.create_workspace("ws"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='p1.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    sp = db.add_keyword("Robin", is_species=True)
    # Direct-SQL tag so auto-Wildlife doesn't fire — simulates a
    # pre-auto-Wildlife DB where this photo had only Robin.
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p1, sp),
    )
    db.conn.commit()

    db.backfill_wildlife_genre()  # First run: adds Wildlife to p1.
    wildlife_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Wildlife' AND type = 'genre'"
    ).fetchone()["id"]
    # Marker should now be set
    assert db.get_meta("wildlife_backfill_done") == "1"

    # User removes Wildlife (sticky-removal intent).
    db.untag_photo(p1, wildlife_id)

    # Subsequent backfill calls (simulating subsequent app starts) must
    # short-circuit without re-adding Wildlife.
    db.backfill_wildlife_genre()
    db.backfill_wildlife_genre()
    has_wildlife = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (p1, wildlife_id),
    ).fetchone() is not None
    assert has_wildlife is False, (
        "Backfill must not re-add Wildlife on subsequent startups; the "
        "user's sticky removal would be silently undone."
    )

    # Force flag exists for tests.
    db.backfill_wildlife_genre(force=True)
    has_wildlife = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (p1, wildlife_id),
    ).fetchone() is not None
    assert has_wildlife is True, "force=True must override the marker gate."


def test_keywords_has_place_id_column_and_unique_index(db):
    import sqlite3

    import pytest

    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(keywords)").fetchall()}
    assert "place_id" in cols

    db.conn.execute(
        "INSERT INTO keywords (name, type, place_id) VALUES (?, ?, ?)",
        ("Central Park", "location", "ChIJ_test_1"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.conn.execute(
            "INSERT INTO keywords (name, type, place_id) VALUES (?, ?, ?)",
            ("Different Name", "location", "ChIJ_test_1"),
        )

    db.conn.execute(
        "INSERT INTO keywords (name, type, place_id) VALUES (?, ?, NULL)",
        ("free text 1", "location"),
    )
    db.conn.execute(
        "INSERT INTO keywords (name, type, place_id) VALUES (?, ?, NULL)",
        ("free text 2", "location"),
    )


def test_place_reverse_geocode_cache_table_exists(db):
    cols = {
        row[1]
        for row in db.conn.execute(
            "PRAGMA table_info(place_reverse_geocode_cache)"
        ).fetchall()
    }
    assert cols >= {"lat_grid", "lng_grid", "place_id", "response", "fetched_at"}


def _central_park_details():
    """Canned Place Details payload (matches places.place_details() shape)."""
    return {
        "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
        "name": "Central Park",
        "lat": 40.7829,
        "lng": -73.9654,
        "address_components": [
            # Google's response order: narrowest → broadest.
            {"name": "Manhattan", "short_name": "Manhattan",
             "types": ["sublocality_level_1", "sublocality", "political"]},
            {"name": "New York", "short_name": "New York",
             "types": ["locality", "political"]},
            {"name": "New York", "short_name": "NY",
             "types": ["administrative_area_level_1", "political"]},
            {"name": "United States", "short_name": "US",
             "types": ["country", "political"]},
        ],
    }


def test_upsert_place_chain_creates_leaf_with_coords_and_place_id(db):
    details = _central_park_details()
    leaf_id = db.upsert_place_chain(details)
    leaf = db.conn.execute(
        "SELECT name, type, place_id, latitude, longitude, parent_id "
        "FROM keywords WHERE id = ?",
        (leaf_id,),
    ).fetchone()
    assert leaf is not None
    assert leaf["name"] == "Central Park"
    assert leaf["type"] == "location"
    assert leaf["place_id"] == details["place_id"]
    assert leaf["latitude"] == details["lat"]
    assert leaf["longitude"] == details["lng"]
    # Parent chain populated, so leaf must have a non-null parent_id.
    assert leaf["parent_id"] is not None


def test_upsert_place_chain_walks_full_parent_chain(db):
    details = _central_park_details()
    leaf_id = db.upsert_place_chain(details)

    # Walk leaf → root, collecting names + place_ids.
    chain = []
    cur_id = leaf_id
    while cur_id is not None:
        row = db.conn.execute(
            "SELECT id, name, parent_id, type, place_id, latitude, longitude "
            "FROM keywords WHERE id = ?",
            (cur_id,),
        ).fetchone()
        chain.append(row)
        cur_id = row["parent_id"]

    # Leaf + 4 address_components → 5 total.
    assert len(chain) == 5
    # Order is leaf → narrowest parent → … → broadest.
    names = [r["name"] for r in chain]
    assert names == [
        "Central Park",
        "Manhattan",
        "New York",       # locality
        "New York",       # state (admin_area_1)
        "United States",
    ]
    # Only the leaf carries place_id and coords.
    leaf, *parents = chain
    assert leaf["place_id"] == details["place_id"]
    assert leaf["latitude"] == details["lat"]
    for p in parents:
        assert p["type"] == "location"
        assert p["place_id"] is None
        assert p["latitude"] is None
        assert p["longitude"] is None
    # Root (broadest) must have NULL parent_id.
    assert chain[-1]["parent_id"] is None


def test_upsert_place_chain_filters_address_fragments(db):
    """Street numbers, routes, and postal codes should not become keywords."""
    details = {
        "place_id": "ChIJ_Address_Fragment_Test",
        "name": "123 Main St",
        "lat": 37.1,
        "lng": -122.2,
        "address_components": [
            {"name": "123", "short_name": "123", "types": ["street_number"]},
            {"name": "Main St", "short_name": "Main St", "types": ["route"]},
            {"name": "Mountain View", "short_name": "Mountain View", "types": ["locality"]},
            {"name": "Santa Clara County", "short_name": "Santa Clara County",
             "types": ["administrative_area_level_2"]},
            {"name": "California", "short_name": "CA",
             "types": ["administrative_area_level_1"]},
            {"name": "United States", "short_name": "US", "types": ["country"]},
            {"name": "94043", "short_name": "94043", "types": ["postal_code"]},
        ],
    }

    leaf_id = db.upsert_place_chain(details)
    names = []
    cur_id = leaf_id
    while cur_id is not None:
        row = db.conn.execute(
            "SELECT name, parent_id FROM keywords WHERE id = ?",
            (cur_id,),
        ).fetchone()
        names.append(row["name"])
        cur_id = row["parent_id"]

    assert names == [
        "123 Main St",
        "Mountain View",
        "Santa Clara County",
        "California",
        "United States",
    ]
    all_location_names = {
        row["name"]
        for row in db.conn.execute(
            "SELECT name FROM keywords WHERE type = 'location'"
        ).fetchall()
    }
    assert "123" not in all_location_names
    assert "94043" not in all_location_names
    assert "Main St" not in all_location_names


def test_upsert_place_chain_preserves_lower_admin_levels(db):
    """Administrative levels 6 and 7 are valid location parents."""
    details = {
        "place_id": "ChIJ_Admin_Level_7_Test",
        "name": "Village Square",
        "lat": 48.1,
        "lng": 11.2,
        "address_components": [
            {"name": "Village Square", "short_name": "Village Square",
             "types": ["point_of_interest"]},
            {"name": "Quarter Seven", "short_name": "Q7",
             "types": ["administrative_area_level_7"]},
            {"name": "District Six", "short_name": "D6",
             "types": ["administrative_area_level_6"]},
            {"name": "Region Five", "short_name": "R5",
             "types": ["administrative_area_level_5"]},
            {"name": "Germany", "short_name": "DE", "types": ["country"]},
            {"name": "12", "short_name": "12", "types": ["street_number"]},
            {"name": "10115", "short_name": "10115", "types": ["postal_code"]},
        ],
    }

    leaf_id = db.upsert_place_chain(details)
    names = []
    cur_id = leaf_id
    while cur_id is not None:
        row = db.conn.execute(
            "SELECT name, parent_id FROM keywords WHERE id = ?",
            (cur_id,),
        ).fetchone()
        names.append(row["name"])
        cur_id = row["parent_id"]

    assert names == [
        "Village Square",
        "Quarter Seven",
        "District Six",
        "Region Five",
        "Germany",
    ]


def test_upsert_place_chain_preserves_same_named_admin_parent(db):
    """Leaf-name filtering must not drop broader same-named parents."""
    details = {
        "place_id": "ChIJ_New_York_City_Test",
        "name": "New York",
        "types": ["locality", "political"],
        "lat": 40.7128,
        "lng": -74.0060,
        "address_components": [
            {"name": "New York", "short_name": "New York", "types": ["locality"]},
            {"name": "New York County", "short_name": "New York County",
             "types": ["administrative_area_level_2"]},
            {"name": "New York", "short_name": "NY",
             "types": ["administrative_area_level_1"]},
            {"name": "United States", "short_name": "US", "types": ["country"]},
        ],
    }

    leaf_id = db.upsert_place_chain(details)
    names = []
    cur_id = leaf_id
    while cur_id is not None:
        row = db.conn.execute(
            "SELECT name, parent_id FROM keywords WHERE id = ?",
            (cur_id,),
        ).fetchone()
        names.append(row["name"])
        cur_id = row["parent_id"]

    assert names == [
        "New York",
        "New York County",
        "New York",
        "United States",
    ]


def test_upsert_place_chain_preserves_same_named_poi_parent(db):
    """POI leaves should not remove same-named geographic parents."""
    details = {
        "place_id": "ChIJ_Manhattan_Venue_Test",
        "name": "Manhattan",
        "types": ["point_of_interest", "establishment"],
        "lat": 40.75,
        "lng": -73.99,
        "address_components": [
            {"name": "Manhattan", "short_name": "Manhattan",
             "types": ["sublocality_level_1", "sublocality", "political"]},
            {"name": "New York", "short_name": "New York",
             "types": ["locality", "political"]},
            {"name": "New York", "short_name": "NY",
             "types": ["administrative_area_level_1", "political"]},
            {"name": "United States", "short_name": "US", "types": ["country"]},
        ],
    }

    leaf_id = db.upsert_place_chain(details)
    names = []
    cur_id = leaf_id
    while cur_id is not None:
        row = db.conn.execute(
            "SELECT name, parent_id FROM keywords WHERE id = ?",
            (cur_id,),
        ).fetchone()
        names.append(row["name"])
        cur_id = row["parent_id"]

    assert names == [
        "Manhattan",
        "Manhattan",
        "New York",
        "New York",
        "United States",
    ]


def test_upsert_place_chain_is_idempotent(db):
    details = _central_park_details()
    first_id = db.upsert_place_chain(details)
    before_count = db.conn.execute(
        "SELECT COUNT(*) AS n FROM keywords"
    ).fetchone()["n"]

    second_id = db.upsert_place_chain(details)
    after_count = db.conn.execute(
        "SELECT COUNT(*) AS n FROM keywords"
    ).fetchone()["n"]

    assert second_id == first_id
    assert after_count == before_count


def test_upsert_place_chain_shares_parents_across_leaves(db):
    """Two leaves under the same NY/USA chain must reuse parent rows."""
    central_park = _central_park_details()
    times_square = {
        "place_id": "ChIJmQJIxlVYwokRLgeuocVOGVU",
        "name": "Times Square",
        "lat": 40.7580,
        "lng": -73.9855,
        "address_components": list(central_park["address_components"]),
    }
    cp_leaf = db.upsert_place_chain(central_park)
    ts_leaf = db.upsert_place_chain(times_square)

    assert cp_leaf != ts_leaf

    # Both leaves should chain up to the same broadest ancestor (United States).
    def root_of(kid):
        cur = kid
        while True:
            row = db.conn.execute(
                "SELECT parent_id FROM keywords WHERE id = ?", (cur,),
            ).fetchone()
            if row["parent_id"] is None:
                return cur
            cur = row["parent_id"]

    assert root_of(cp_leaf) == root_of(ts_leaf)
    # Total keywords: 4 shared parents + 2 leaves = 6 (no duplicate parents).
    total = db.conn.execute(
        "SELECT COUNT(*) AS n FROM keywords WHERE type='location'"
    ).fetchone()["n"]
    assert total == 6


def _make_photo(db, filename="loc.jpg"):
    """Helper: create a folder + a single photo, return its id."""
    fid = db.add_folder(f"/photos/{filename}-folder", name="loc")
    return db.add_photo(
        folder_id=fid, filename=filename, extension=".jpg",
        file_size=1000, file_mtime=1.0,
    )


def test_set_photo_location_replaces_existing(db):
    """set_photo_location removes any existing 'location' link before inserting."""
    pid = _make_photo(db)
    # Pre-existing location keyword link.
    old_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'location')",
        ("Old Park",),
    ).lastrowid
    new_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'location')",
        ("New Park",),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, old_id),
    )
    db.conn.commit()

    db.set_photo_location(pid, new_id)

    rows = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid,),
    ).fetchall()
    keyword_ids = {r["keyword_id"] for r in rows}
    assert keyword_ids == {new_id}, "old location link must be removed; new one present"


def test_set_photo_location_preserves_non_location_keywords(db):
    """set_photo_location must only touch 'location' links, not other tag types."""
    pid = _make_photo(db)
    general_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'general')",
        ("Bird",),
    ).lastrowid
    loc_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'location')",
        ("Park",),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, general_id),
    )
    db.conn.commit()

    db.set_photo_location(pid, loc_id)

    rows = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid,),
    ).fetchall()
    keyword_ids = {r["keyword_id"] for r in rows}
    assert keyword_ids == {general_id, loc_id}


def test_clear_photo_location_removes_links_but_keeps_keyword_rows(db):
    """clear_photo_location deletes the link only, never the keyword row."""
    pid = _make_photo(db)
    loc_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'location')",
        ("Park",),
    ).lastrowid
    general_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'general')",
        ("Bird",),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, loc_id),
    )
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, general_id),
    )
    db.conn.commit()

    db.clear_photo_location(pid)

    # Location link gone, general link survives.
    rows = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid,),
    ).fetchall()
    assert {r["keyword_id"] for r in rows} == {general_id}

    # Both keyword rows still exist.
    kept = db.conn.execute(
        "SELECT id FROM keywords WHERE id IN (?, ?)", (loc_id, general_id),
    ).fetchall()
    assert len(kept) == 2


def test_get_or_create_text_location_creates_new(db):
    """First call creates, second with same name returns same id."""
    first = db.get_or_create_text_location("the dog park")
    row = db.conn.execute(
        "SELECT name, type, place_id, parent_id, latitude, longitude "
        "FROM keywords WHERE id = ?",
        (first,),
    ).fetchone()
    assert row["name"] == "the dog park"
    assert row["type"] == "location"
    assert row["place_id"] is None
    assert row["parent_id"] is None
    assert row["latitude"] is None
    assert row["longitude"] is None

    second = db.get_or_create_text_location("the dog park")
    assert second == first


def test_get_or_create_text_location_strips_whitespace_and_rejects_empty(db):
    """Whitespace stripped; empty/whitespace-only raises ValueError."""
    import pytest

    a = db.get_or_create_text_location("  Café Park  ")
    b = db.get_or_create_text_location("Café Park")
    assert a == b
    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (a,),
    ).fetchone()
    assert row["name"] == "Café Park"

    with pytest.raises(ValueError):
        db.get_or_create_text_location("")
    with pytest.raises(ValueError):
        db.get_or_create_text_location("   ")


def test_link_keyword_to_place_attaches_metadata(db):
    """An existing free-text keyword gets place_id, coords, and parent chain."""
    # Create a free-text "Central Park" with a photo tagged.
    pid = _make_photo(db)
    free_id = db.get_or_create_text_location("Central Park")
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, free_id),
    )
    db.conn.commit()

    details = _central_park_details()
    result = db.link_keyword_to_place(free_id, details)

    assert result["merged"] is False
    assert result["keyword_id"] == free_id

    row = db.conn.execute(
        "SELECT name, type, place_id, latitude, longitude, parent_id "
        "FROM keywords WHERE id = ?",
        (free_id,),
    ).fetchone()
    assert row["place_id"] == details["place_id"]
    assert row["latitude"] == details["lat"]
    assert row["longitude"] == details["lng"]
    assert row["name"] == "Central Park"
    # Parent chain attached.
    assert row["parent_id"] is not None

    # Photo link preserved.
    pk = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (pid, free_id),
    ).fetchone()
    assert pk is not None


def test_link_keyword_to_place_merges_on_existing_place_id(db):
    """If the place_id already belongs to another keyword, merge into it."""
    # First: create the canonical place-bearing keyword via upsert_place_chain.
    details = _central_park_details()
    canonical_id = db.upsert_place_chain(details)

    # Tag photo A with the canonical row.
    pid_a = _make_photo(db, "a.jpg")
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid_a, canonical_id),
    )

    # Now: a separate free-text "Central Park (free)" keyword, tagged on photo B.
    pid_b = _make_photo(db, "b.jpg")
    free_id = db.get_or_create_text_location("Central Park (free)")
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid_b, free_id),
    )
    db.conn.commit()

    # Link the free-text keyword to the same place_id → should merge into canonical.
    result = db.link_keyword_to_place(free_id, details)

    assert result["merged"] is True
    assert result["keyword_id"] == canonical_id

    # Old free-text row gone.
    gone = db.conn.execute(
        "SELECT id FROM keywords WHERE id = ?", (free_id,),
    ).fetchone()
    assert gone is None

    # Both photos now linked to canonical.
    rows = db.conn.execute(
        "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
        (canonical_id,),
    ).fetchall()
    photo_ids = {r["photo_id"] for r in rows}
    assert photo_ids == {pid_a, pid_b}

    # No leftover links to the old free-text id.
    stale = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE keyword_id = ?", (free_id,),
    ).fetchone()
    assert stale is None


def test_link_keyword_to_place_self_merge_via_non_leaf_parent(db):
    """If the chain reuses the target keyword as a non-leaf ancestor, the
    link must be a no-op (returning ``merged=False``) rather than reparenting
    the keyword onto one of its own descendants and creating a cycle.

    Scenario: user has a free-text "United States" keyword and tries to link
    it to Central Park. The chain walk discovers and reuses the user's
    "United States" row at the country (broadest) level, then walks deeper
    through NY-state, NY-city, Manhattan. Without the full-chain self-merge
    guard, the UPDATE would set the user's row to ``name='Central Park',
    parent_id=Manhattan_id`` — making the user's row a child of its own
    great-grandchild.
    """
    pid = _make_photo(db)
    free_us_id = db.get_or_create_text_location("United States")
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, free_us_id),
    )
    db.conn.commit()

    details = _central_park_details()
    result = db.link_keyword_to_place(free_us_id, details)

    # No-op result.
    assert result == {"keyword_id": free_us_id, "merged": False}

    # Original keyword unchanged: still "United States", no place_id, no parent.
    row = db.conn.execute(
        "SELECT name, type, place_id, parent_id FROM keywords WHERE id = ?",
        (free_us_id,),
    ).fetchone()
    assert row is not None
    assert row["name"] == "United States"
    assert row["type"] == "location"
    assert row["place_id"] is None
    assert row["parent_id"] is None

    # Photo still tagged with the original keyword.
    pk = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (pid, free_us_id),
    ).fetchone()
    assert pk is not None


def test_link_keyword_to_place_raises_on_missing_id(db):
    """A non-existent keyword id must raise ValueError, not silently no-op
    while leaving orphan parent rows behind."""
    import pytest

    details = _central_park_details()
    with pytest.raises(ValueError, match="999999"):
        db.link_keyword_to_place(999999, details)


def test_link_keyword_to_place_rejects_non_location_keyword(db):
    """Linking a place to a non-'location' keyword must raise ValueError.
    Otherwise the globally-unique place_id would get attached to (say) a
    species or general keyword, and later location upserts would resolve to
    that non-location row, after which set_photo_location rejects it and
    the place is stuck until manual cleanup."""
    import pytest

    general_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'general')",
        ("Bird",),
    ).lastrowid
    db.conn.commit()

    details = _central_park_details()
    with pytest.raises(ValueError, match="not 'location'"):
        db.link_keyword_to_place(general_id, details)

    # The non-location keyword must be left untouched — no place_id, no
    # coords, no reparenting onto a freshly-created chain.
    row = db.conn.execute(
        "SELECT type, place_id, latitude, longitude, parent_id "
        "FROM keywords WHERE id = ?",
        (general_id,),
    ).fetchone()
    assert row["type"] == "general"
    assert row["place_id"] is None
    assert row["latitude"] is None
    assert row["longitude"] is None
    assert row["parent_id"] is None


def test_link_keyword_to_place_disambiguates_name_collision(db):
    """The UPDATE in link_keyword_to_place can fail on UNIQUE(name,
    parent_id) — not just UNIQUE(place_id) — when a different row with the
    same (name, parent_id) already exists (e.g., an earlier upsert created
    one homonymous place at the slot). The handler must distinguish from
    a place_id collision and disambiguate the new row's name rather than
    re-raising as 500."""
    # First, upsert a place chain that creates "Riverside Park" with
    # place_id=A under "New York" (state).
    details_a = {
        "place_id": "ChIJ_Park_A",
        "name": "Riverside Park",
        "lat": 40.80, "lng": -73.97,
        "address_components": [
            {"name": "New York", "short_name": "NY",
             "types": ["administrative_area_level_1", "political"]},
        ],
    }
    leaf_a = db.upsert_place_chain(details_a)

    # Now create a free-text "Riverside Park" with no parent (different
    # slot, so the upsert above didn't touch it).
    free_id = db.get_or_create_text_location("Riverside Park")
    assert free_id != leaf_a

    # Link the free-text row to a DIFFERENT Google place (B) whose chain
    # ALSO lands under "New York" — the UPDATE will try to set the same
    # (name="Riverside Park", parent_id=NY) as A and collide.
    details_b = {
        "place_id": "ChIJ_Park_B",
        "name": "Riverside Park",
        "lat": 40.85, "lng": -73.95,
        "address_components": [
            {"name": "New York", "short_name": "NY",
             "types": ["administrative_area_level_1", "political"]},
        ],
    }
    result = db.link_keyword_to_place(free_id, details_b)

    # Should NOT have re-raised. Should NOT have merged (A and B are
    # distinct places). free_id keeps its identity but with a
    # disambiguated name.
    assert result["merged"] is False
    assert result["keyword_id"] == free_id

    free_row = db.conn.execute(
        "SELECT name, place_id FROM keywords WHERE id = ?", (free_id,),
    ).fetchone()
    assert free_row["place_id"] == "ChIJ_Park_B"
    # Disambiguated name keeps the original name as a prefix.
    assert "Riverside Park" in free_row["name"]
    assert free_row["name"] != "Riverside Park", \
        "name must have been disambiguated, not left as the colliding value"

    # The original "Riverside Park" (place A) is untouched.
    a_row = db.conn.execute(
        "SELECT name, place_id FROM keywords WHERE id = ?", (leaf_a,),
    ).fetchone()
    assert a_row["name"] == "Riverside Park"
    assert a_row["place_id"] == "ChIJ_Park_A"


def test_link_keyword_to_place_merge_reparents_with_child_name_collision(db):
    """During the merge path's reparent of children onto the canonical row,
    a child whose (name) collides with an existing child of the canonical
    would violate UNIQUE(name, parent_id) on a bulk UPDATE. Disambiguate
    the migrating child's name on clash rather than 500ing."""
    # Canonical place row Q, with an existing child "Boathouse".
    q_details = {
        "place_id": "ChIJ_Q",
        "name": "Q Park",
        "lat": 40.0, "lng": -73.0,
        "address_components": [],
    }
    canonical_id = db.upsert_place_chain(q_details)
    canonical_child_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES ('Boathouse', 'location', ?)",
        (canonical_id,),
    ).lastrowid

    # Old keyword P (free-text), with its own "Boathouse" child.
    p_id = db.get_or_create_text_location("Q Park")
    p_child_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES ('Boathouse', 'location', ?)",
        (p_id,),
    ).lastrowid
    db.conn.commit()

    # Link P → triggers merge. The reparent step would naively try to set
    # both Boathouse rows' parent_id = canonical_id, colliding on
    # UNIQUE(name, parent_id). Fix: per-child loop with disambiguation.
    result = db.link_keyword_to_place(p_id, q_details)
    assert result["merged"] is True
    assert result["keyword_id"] == canonical_id

    # The original canonical child keeps its name; the migrating child got
    # disambiguated.
    canonical_child = db.conn.execute(
        "SELECT name, parent_id FROM keywords WHERE id = ?", (canonical_child_id,),
    ).fetchone()
    assert canonical_child["name"] == "Boathouse"
    assert canonical_child["parent_id"] == canonical_id

    migrated_child = db.conn.execute(
        "SELECT name, parent_id FROM keywords WHERE id = ?", (p_child_id,),
    ).fetchone()
    assert migrated_child is not None, "migrating child must survive"
    assert migrated_child["parent_id"] == canonical_id
    assert migrated_child["name"] != "Boathouse", \
        "migrating child must have been disambiguated, not left to clash"
    assert "Boathouse" in migrated_child["name"]


def test_location_edits_are_non_undoable(db):
    """location_set / location_clear / location_link entries land in the
    edit history (auditable) but are skipped by undo so the user doesn't
    click Undo and see no state change while older edits become the next
    undo target."""
    # Pre-record one undoable edit so the undo cursor can be tested.
    pid = _make_photo(db)
    db.record_edit('rating', 'First edit', '1',
                   [{'photo_id': pid, 'old_value': '0', 'new_value': '1'}])

    # Now record location_set / clear / link audit entries (newest = link).
    db.record_edit('location_set', 'set location: Test', '1',
                   [{'photo_id': pid, 'old_value': '', 'new_value': '1'}])
    db.record_edit('location_clear', 'cleared location', '',
                   [{'photo_id': pid, 'old_value': '', 'new_value': ''}])
    db.record_edit('location_link', 'linked', '1', [])

    # Undo MUST step over the three location_* entries straight to the
    # rating edit, even though they're newer.
    entry = db.undo_last_edit()
    assert entry is not None
    assert entry['action_type'] == 'rating', (
        "undo must skip location_* entries; "
        f"got action_type={entry['action_type']!r} instead"
    )
    """When the merge path fires (place_id already owned by a canonical
    row), the old keyword may have descendants pointing to it via
    parent_id. SQLite's self-referential FK on keywords.parent_id (with
    foreign_keys=ON) blocks DELETE FROM keywords if any child still
    references the old row. The merge must reparent descendants onto the
    canonical row before deleting."""

    # Step 1: pre-create a canonical place row Q with a known place_id.
    q_details = {
        "place_id": "ChIJ_Q",
        "name": "Q Park",
        "lat": 40.0, "lng": -73.0,
        "address_components": [],
    }
    canonical_id = db.upsert_place_chain(q_details)

    # Step 2: create a free-text keyword P (no place_id), and give it a
    # child keyword C that points to P via parent_id.
    p_id = db.get_or_create_text_location("Q Park")
    c_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'location', ?)",
        ("Q Park West", p_id),
    ).lastrowid
    db.conn.commit()

    # Step 3: link P to the same place_id as the canonical row Q.
    # This triggers the merge path — P's photos (none here) move onto Q,
    # and P must be deleted. With descendants still pointing to P, the
    # naive DELETE fails on the self-FK. After the fix, C is reparented
    # to Q first.
    result = db.link_keyword_to_place(p_id, q_details)

    assert result["merged"] is True
    assert result["keyword_id"] == canonical_id

    # P must be gone.
    p_row = db.conn.execute(
        "SELECT id FROM keywords WHERE id = ?", (p_id,),
    ).fetchone()
    assert p_row is None, "old keyword should have been deleted post-merge"

    # C must still exist and now point to Q (the canonical row).
    c_row = db.conn.execute(
        "SELECT name, parent_id FROM keywords WHERE id = ?", (c_id,),
    ).fetchone()
    assert c_row is not None, "child keyword must survive the merge"
    assert c_row["parent_id"] == canonical_id, \
        "child must be reparented onto the canonical row"


def test_set_photo_location_rejects_non_location_keyword(db):
    """Passing a non-'location' keyword must raise ValueError; otherwise the
    DELETE would strip real location links and the INSERT would add a
    keyword that clear_photo_location can't clean up."""
    import pytest

    pid = _make_photo(db)
    general_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'general')",
        ("Bird",),
    ).lastrowid
    db.conn.commit()

    with pytest.raises(ValueError, match="not 'location'"):
        db.set_photo_location(pid, general_id)

    # Also a missing id must raise.
    with pytest.raises(ValueError, match="does not exist"):
        db.set_photo_location(pid, 999999)


def test_upsert_place_chain_raises_on_cross_type_collision(db):
    """A pre-existing non-location keyword on the same (name, parent_id) must
    raise a clear error rather than silently merging or surfacing a raw
    sqlite3.IntegrityError. The user's existing tags must not be corrupted.

    Note: SQLite's UNIQUE(name, parent_id) doesn't fire when parent_id is
    NULL (NULL != NULL in SQL), so the collision can only occur on
    non-root chain levels. We pre-build the "United States" → "New York"
    (state) → "New York" (locality) location parents directly, then plant
    a 'general' keyword "Manhattan" under the locality. The next
    upsert_place_chain walk will try to INSERT a 'location' "Manhattan"
    in the same slot, triggering the UNIQUE(name, parent_id) collision.
    """
    import pytest

    us_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'location', NULL)",
        ("United States",),
    ).lastrowid
    state_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'location', ?)",
        ("New York", us_id),
    ).lastrowid
    locality_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'location', ?)",
        ("New York", state_id),
    ).lastrowid
    # Plant a 'general' "Manhattan" under the locality New York row.
    db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'general', ?)",
        ("Manhattan", locality_id),
    )
    db.conn.commit()

    # Chain walk wants a 'location' "Manhattan" under the same locality →
    # UNIQUE(name, parent_id) blows up; we must surface a clear RuntimeError.
    with pytest.raises(RuntimeError, match="Manhattan"):
        db.upsert_place_chain(_central_park_details())


def test_upsert_one_keyword_handles_homonymous_places(db):
    """Two distinct Google places with different place_ids that share the
    same (name, parent_id) chain slot must both succeed. The current
    ON CONFLICT(place_id) handler doesn't catch the (name, parent_id)
    UNIQUE constraint, so the second insert previously surfaced as a 500
    from the API. Disambiguate by appending a short place_id suffix to
    the second row's name.
    """
    # Realistic-ish setup: two Google places that share the same name AND
    # the same parent. Rare in practice (parents usually disambiguate), but
    # Google can return the same address_components for distinct points.
    state_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'location', NULL)",
        ("New York",),
    ).lastrowid
    db.conn.commit()

    first_id = db._upsert_one_keyword(
        name="Riverside Park",
        parent_id=state_id,
        place_id="ChIJ_Riverside_A",
        latitude=40.80,
        longitude=-73.97,
    )
    second_id = db._upsert_one_keyword(
        name="Riverside Park",
        parent_id=state_id,
        place_id="ChIJ_Riverside_B",
        latitude=40.85,
        longitude=-73.95,
    )
    assert first_id != second_id, "homonymous places must be distinct rows"
    rows = db.conn.execute(
        "SELECT id, name, place_id FROM keywords "
        "WHERE place_id IN ('ChIJ_Riverside_A', 'ChIJ_Riverside_B') "
        "ORDER BY id"
    ).fetchall()
    assert len(rows) == 2
    # First row keeps its plain name; second is disambiguated.
    assert rows[0]["name"] == "Riverside Park"
    assert rows[0]["place_id"] == "ChIJ_Riverside_A"
    assert rows[1]["place_id"] == "ChIJ_Riverside_B"
    # Disambiguated name should be different from the first; format is an
    # implementation detail, but it must contain enough of the place_id to
    # make collisions astronomically unlikely.
    assert rows[1]["name"] != "Riverside Park"
    assert "Riverside Park" in rows[1]["name"]


def test_upsert_one_keyword_homonymous_idempotent(db):
    """Re-upserting the SAME (place_id) after disambiguation still hits
    the ON CONFLICT(place_id) path and returns the same id (no extra rows)."""
    state_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'location', NULL)",
        ("New York",),
    ).lastrowid
    db.conn.commit()
    db._upsert_one_keyword(
        name="Riverside Park", parent_id=state_id,
        place_id="ChIJ_Riverside_A", latitude=40.80, longitude=-73.97,
    )
    second_id = db._upsert_one_keyword(
        name="Riverside Park", parent_id=state_id,
        place_id="ChIJ_Riverside_B", latitude=40.85, longitude=-73.95,
    )
    # Re-upsert with the same place_id (B) — should hit ON CONFLICT(place_id)
    # and return the SAME id without trying the disambiguation path again.
    second_id_again = db._upsert_one_keyword(
        name="Riverside Park", parent_id=state_id,
        place_id="ChIJ_Riverside_B", latitude=40.85, longitude=-73.95,
    )
    assert second_id == second_id_again
    n_rows = db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE place_id LIKE 'ChIJ_Riverside_%'"
    ).fetchone()[0]
    assert n_rows == 2, "no extra rows from idempotent re-upsert"


# --- Task 7: reverse-geocode cache get/put -----------------------------------


def test_reverse_geocode_cache_round_trips_put_then_get(db):
    """Putting a value and then getting it back returns matching fields."""
    response_json = '{"place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4", "name": "Sydney"}'
    db.reverse_geocode_cache_put(
        -33.8688, 151.2093, "ChIJN1t_tDeuEmsRUsoyG83frY4", response_json,
    )
    hit = db.reverse_geocode_cache_get(-33.8688, 151.2093)
    assert hit is not None
    assert hit["place_id"] == "ChIJN1t_tDeuEmsRUsoyG83frY4"
    assert hit["response"] == response_json


def test_reverse_geocode_cache_returns_none_on_miss(db):
    """Getting a never-put coord returns None (distinct from cached negative)."""
    assert db.reverse_geocode_cache_get(40.0, -73.0) is None


def test_reverse_geocode_cache_grid_collapses_nearby_coords(db):
    """Coords that round to the same ~110m grid cell share a cache entry,
    while coords that round to a different cell are kept separate."""
    db.reverse_geocode_cache_put(
        40.7820, -73.9650, "ChIJ_central_park", '{"name":"Central Park"}',
    )
    # 40.7821, -73.9648 → both round to the same int(round(x*1000)) cell
    hit = db.reverse_geocode_cache_get(40.7821, -73.9648)
    assert hit is not None
    assert hit["place_id"] == "ChIJ_central_park"
    assert hit["response"] == '{"name":"Central Park"}'

    # 40.7830, -73.9650 → different lat_grid (40783 vs 40782)
    db.reverse_geocode_cache_put(
        40.7830, -73.9650, "ChIJ_other", '{"name":"Other"}',
    )
    other_hit = db.reverse_geocode_cache_get(40.7830, -73.9650)
    assert other_hit is not None
    assert other_hit["place_id"] == "ChIJ_other"
    # Original cell unchanged.
    orig_hit = db.reverse_geocode_cache_get(40.7820, -73.9650)
    assert orig_hit["place_id"] == "ChIJ_central_park"


def test_reverse_geocode_cache_caches_negative_result(db):
    """A null place_id (Google returned no match) is still cached. The
    cache must distinguish 'we asked Google and got no match' from 'we
    never asked' — get returns the row, not None."""
    db.reverse_geocode_cache_put(0.0, 0.0, None, "{}")
    hit = db.reverse_geocode_cache_get(0.0, 0.0)
    assert hit is not None
    assert hit["place_id"] is None
    assert hit["response"] == "{}"


def test_reverse_geocode_cache_put_overwrites_existing(db):
    """Re-putting at the same grid cell overwrites the prior value."""
    db.reverse_geocode_cache_put(
        12.345, 67.890, "ChIJ_first", '{"v":1}',
    )
    db.reverse_geocode_cache_put(
        12.345, 67.890, "ChIJ_second", '{"v":2}',
    )
    hit = db.reverse_geocode_cache_get(12.345, 67.890)
    assert hit is not None
    assert hit["place_id"] == "ChIJ_second"
    assert hit["response"] == '{"v":2}'


# -- update_keyword: rename re-runs taxonomy auto-detection --
#
# Regression tests for: when a keyword name is changed (e.g. fixing a typo
# like "Lesser scaub" -> "Lesser Scaup"), the same auto-detection logic
# that add_keyword uses on insert must re-fire so the keyword gets
# re-typed as 'taxonomy' with a linked taxon_id. Manual user overrides
# (any non-'general' type, or explicit type/taxon_id kwargs) win over
# auto-detection.


def _seed_taxa(db, rows):
    """Insert minimal taxa rows for keyword auto-detect tests.

    rows: list of (inat_id, scientific_name, common_name) tuples.
    Returns dict mapping common_name -> taxa.id (local PK).
    """
    out = {}
    for inat_id, sci, common in rows:
        cur = db.conn.execute(
            "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
            "VALUES (?, ?, ?, 'species', 'Animalia')",
            (inat_id, sci, common),
        )
        out[common] = cur.lastrowid
    db.conn.commit()
    return out


def test_update_keyword_rename_general_to_matching_taxon_auto_retypes(tmp_path):
    """Renaming a 'general' keyword to a name matching a taxon retypes it
    as 'taxonomy' and links taxon_id. This is the typo-fix path."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(7000, "Aythya affinis", "Lesser Scaup")])

    # User adds a typo'd keyword that does NOT match any taxon.
    kid = db.add_keyword("Lesser scaub")
    pre = db.conn.execute(
        "SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert pre["type"] == "general"
    assert pre["taxon_id"] is None

    # User fixes the typo via update_keyword.
    db.update_keyword(kid, name="Lesser Scaup")

    row = db.conn.execute(
        "SELECT name, type, taxon_id, is_species FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["name"] == "Lesser Scaup"
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] == taxa["Lesser Scaup"]
    # Mirror add_keyword's invariant: an auto-promoted taxonomy keyword
    # backed by a matched taxon must have is_species=1, otherwise
    # species-only queries (filtering on is_species=1) would silently
    # exclude it.
    assert row["is_species"] == 1


def test_update_keyword_rename_general_to_matching_taxon_visible_to_species_query(tmp_path):
    """After auto-promoting a renamed 'general' keyword to 'taxonomy',
    species-only queries (which filter on is_species=1) must include it.
    Regression for the case where update_keyword set type/taxon_id but
    forgot to flip is_species, leaving auto-promoted keywords invisible
    to get_species_keywords_for_photos."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(7000, "Aythya affinis", "Lesser Scaup")])

    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    kid = db.add_keyword("Lesser scaub")  # typo, no taxon match
    db.tag_photo(pid, kid)

    # Before rename: general keyword, species query returns nothing.
    assert db.get_species_keywords_for_photos([pid]) == {}

    db.update_keyword(kid, name="Lesser Scaup")

    # After rename: auto-promoted to taxonomy + is_species=1, so the
    # species query now includes it.
    assert db.get_species_keywords_for_photos([pid]) == {pid: ["Lesser Scaup"]}


def test_get_species_keywords_includes_taxonomy_typed_without_is_species(tmp_path):
    """Legacy/upgraded data may carry species tags typed 'taxonomy' but with
    is_species=0. get_species_keywords_for_photos must still surface them so
    the Compare page does not misclassify already-tagged photos as 'new'
    (mirrors the is_species OR type='taxonomy' definition accept_prediction
    uses)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))

    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    kid = db.add_keyword("Lesser Scaup")
    db.tag_photo(pid, kid)
    db.conn.execute(
        "UPDATE keywords SET is_species = 0, type = 'taxonomy' WHERE id = ?",
        (kid,),
    )
    db.conn.commit()

    assert db.get_species_keywords_for_photos([pid]) == {pid: ["Lesser Scaup"]}


def test_species_queries_use_taxon_rank_and_dedupe_hierarchy_by_taxon(tmp_path):
    """A hierarchy ancestor is taxonomy, not a species badge; duplicate
    hierarchy/root leaves for one species collapse by taxon identity."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [
        (2912, "Auriparus flaviceps", "Verdin"),
    ])
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank) "
        "VALUES (38595, 'Remizidae', 'Penduline tits', 'family')"
    )
    db.conn.commit()

    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    birds = db.add_keyword("1Birds")
    family = db.add_keyword("Penduline tits", parent_id=birds)
    nested = db.add_keyword("Verdin", parent_id=family)
    root = db.add_keyword("Verdin", is_species=True)
    db.tag_photo(pid, family)
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)

    assert db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (root,)
    ).fetchone()["taxon_id"] == taxa["Verdin"]
    assert db.is_keyword_species(family) is False
    assert db.is_keyword_species(nested) is True
    assert db.get_species_keywords_for_photos([pid]) == {pid: ["Verdin"]}
    assert db.get_photos_with_equivalent_species([pid], root) == {pid}

    rows = {row["id"]: row for row in db.get_keywords_for_photos([pid])[pid]}
    assert rows[family]["taxon_rank"] == "family"
    assert rows[nested]["taxon_rank"] == "species"


def test_equivalent_species_matches_unlinked_legacy_hierarchy_by_name(tmp_path):
    """A linked root target still matches a typed legacy hierarchy leaf whose
    taxon link has not yet been backfilled."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = NULL, is_species = 1, "
        "type = 'taxonomy' WHERE id = ?",
        (nested,),
    )
    db.tag_photo(pid, nested)

    assert db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (root,),
    ).fetchone()["taxon_id"] == taxa["Verdin"]
    assert db.get_photos_with_equivalent_species([pid], root) == {pid}


def test_equivalent_species_skips_unlinked_row_when_homonym_taxon_exists(tmp_path):
    """When two distinct taxonomy rows share a NOCASE match key but link to
    different taxa (legacy `Robin` vs taxonomy `robin`), an unlinked same-key
    row on a photo must not satisfy the target species — the row is ambiguous
    and treating it as equivalent would let accept/confirm skip tagging the
    intended species keyword."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(
        db,
        [
            (18001, "Erithacus rubecula", "European Robin"),
            (18002, "Turdus migratorius", "American Robin"),
        ],
    )
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Two intentionally-distinct linked species keywords share the NOCASE
    # match key "robin" but resolve to different taxa. add_keyword dedupes
    # case-insensitively so INSERT directly to preserve both rows.
    european = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('robin', 'taxonomy', 1, ?)",
        (taxa["European Robin"],),
    ).lastrowid
    american = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Robin', 'taxonomy', 1, ?)",
        (taxa["American Robin"],),
    ).lastrowid
    # An unlinked, typed legacy row with the same match key. The user
    # tagged this row before taxonomy was populated; it could be either
    # species, so equivalence lookups must not accept it as the target.
    legacy = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('ROBIN', 'taxonomy', 1, NULL)"
    ).lastrowid
    db.tag_photo(pid, legacy)

    # Neither linked target should treat the unlinked homonym as equivalent.
    assert db.get_photos_with_equivalent_species([pid], european) == set()
    assert db.get_photos_with_equivalent_species([pid], american) == set()

    # Once the ambiguous legacy row is linked to the taxon that the target
    # resolves to, equivalence is authoritative again.
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["American Robin"], legacy),
    )
    assert db.get_photos_with_equivalent_species([pid], american) == {pid}
    assert db.get_photos_with_equivalent_species([pid], european) == set()


def test_equivalent_species_unlinked_target_skips_linked_homonym(tmp_path):
    """When the accept target is an unlinked legacy species keyword and a
    distinct taxonomy-linked keyword shares its NOCASE match key (e.g.
    upgraded/offline catalog with legacy ``Robin`` alongside taxonomy
    ``robin``), the name-only fallback must not treat the linked homonym on
    a photo as the target. Folding them would let accept/confirm skip
    ``tag_photo``/``queue_change`` for the unlinked target while marking the
    prediction accepted, leaving the intended keyword row absent."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(19001, "Turdus migratorius", "American Robin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Distinct linked taxonomy row that intentionally shares the match key
    # with the unlinked legacy target below.
    linked = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('robin', 'taxonomy', 1, ?)",
        (taxa["American Robin"],),
    ).lastrowid
    # An intentionally-preserved legacy row with no taxon_id — the target.
    legacy_target = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Robin', 'taxonomy', 1, NULL)"
    ).lastrowid
    # The photo carries the LINKED homonym, not the legacy target.
    db.tag_photo(pid, linked)

    # Accepting/confirming the unlinked legacy target must not fold in the
    # linked homonym row, or the confirm would skip tagging.
    assert db.get_photos_with_equivalent_species(
        [pid], legacy_target,
    ) == set()

    # If the same photo actually carries the legacy target row, that
    # exact-row match is still recognized.
    other = db.add_photo(
        folder_id=fid, filename="b.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.tag_photo(other, legacy_target)
    assert db.get_photos_with_equivalent_species(
        [pid, other], legacy_target,
    ) == {other}


def test_species_display_name_resolves_hierarchy_alias_through_taxon(tmp_path):
    """A differently-spelled hierarchy leaf canonicalizes to the root species
    row linked to the same unique taxon."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Desert Verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], nested),
    )
    db.add_keyword("Verdin", is_species=True)
    db.conn.commit()

    assert db.resolve_species_display_name("Desert Verdin") == "Verdin"


def test_species_keywords_canonicalize_hierarchy_leaf_to_root_spelling(tmp_path):
    """After repair leaves only a differently-spelled hierarchy leaf, the
    per-photo species list must still surface the canonical root spelling so
    `_attach_species_representatives` can match `species_representatives`
    rows keyed on that root. Otherwise browse/review/pipeline cards drop the
    representative badge for a taxon that is still eligibly attached."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], nested),
    )
    db.add_keyword("Verdin", is_species=True)
    db.conn.commit()
    db.tag_photo(pid, nested)

    # Only the leaf is attached (repair detached the redundant root), but
    # both root and leaf share the taxon — the returned name must be the
    # canonical root spelling used by species_representatives.
    assert db.get_species_keywords_for_photos([pid]) == {pid: ["Verdin"]}


def test_species_keywords_preserves_attached_root_alias_spelling(tmp_path):
    """When a photo carries a top-level species alias root for a taxon that
    also has a differently-spelled sibling root row, the returned name must
    be the *attached* root's stored spelling, not whichever sibling root
    happens to sort first. Curation writes still go through
    ``resolve_species_display_name()``, which preserves an exact root-name
    match, so Browse/Compare and representative attachment would otherwise
    report the sibling root while representative/highlight rows are keyed
    under the actually attached alias. Only hierarchy leaves need the root
    fallback; attached root rows should keep their own stored spelling."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Create the sibling root first so it has the lower id and would win a
    # naive ``ORDER BY id`` canonicalization.
    db.add_keyword("Verdin", is_species=True)
    sci_root = db.add_keyword("Auriparus flaviceps", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
        (taxa["Verdin"], sci_root),
    )
    db.conn.commit()
    db.tag_photo(pid, sci_root)

    # The attached row's own stored spelling wins so downstream curation
    # lookups match the actually-attached name.
    assert db.get_species_keywords_for_photos([pid]) == {
        pid: ["Auriparus flaviceps"]
    }


def test_species_keywords_preserves_unlinked_case_variant_spellings(tmp_path):
    """A photo carrying two NULL-taxon species rows that differ only by
    SQLite NOCASE spelling (root ``Foo`` plus hierarchy leaf ``foo``) must
    surface both names. The repair path deliberately leaves those attached
    because unlinked eligibility compares exact ``k.name``; folding them
    by ``keyword_match_key`` here would drop the root spelling, so
    ``_attach_species_representatives``/highlight lookups miss the
    curation stored under ``Foo``."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Something")
    nested = db.add_keyword("foo", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, "
        "taxon_id = NULL WHERE id = ?",
        (nested,),
    )
    root = db.add_keyword("Foo", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = NULL WHERE id = ?", (root,)
    )
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)

    # Both distinct spellings survive the dedup so downstream lookups
    # (species_representatives, species_highlights) can match either key.
    result = db.get_species_keywords_for_photos([pid])
    assert set(result[pid]) == {"Foo", "foo"}


def test_photo_life_list_species_canonicalizes_hierarchy_leaf_to_root_spelling(
    tmp_path,
):
    """After repair leaves only a differently-spelled hierarchy leaf, the
    photo-detail life-list species must still surface the canonical root
    spelling. Otherwise ``api_photo_detail`` compares the leaf spelling
    against ``get_species_representative_lists`` (keyed on the root) and the
    lightbox/context menu drops the "current representative" flag and offers
    to set it again for a taxon that is still eligibly attached."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], nested),
    )
    db.add_keyword("Verdin", is_species=True)
    db.conn.commit()
    db.tag_photo(pid, nested)

    assert db.get_photo_life_list_species(pid) == ["Verdin"]


def test_photo_life_list_species_preserves_attached_root_alias(tmp_path):
    """When a photo is tagged with an attached top-level alias for a taxon
    (for example ``Auriparus flaviceps``) and another root row for the same
    taxon (``Verdin``) exists but is not attached, ``api_photo_detail`` must
    report the actually attached spelling. Curation writes preserve exact
    root-name matches, so rewriting the attached alias to the arbitrarily
    first same-taxon root would make representative/highlight state keyed to
    the attached name appear missing (or be updated under the wrong species).
    Only hierarchy leaves should fall back to the canonical root spelling."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Root alias (attached to the photo) must survive dedup; ``add_keyword``
    # would collapse the second same-key row, so create the two roots with
    # distinct spellings and back-link both to the same taxon directly.
    alias = db.add_keyword("Auriparus flaviceps", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], alias),
    )
    other_root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], other_root),
    )
    db.conn.commit()
    db.tag_photo(pid, alias)

    assert db.get_photo_life_list_species(pid) == ["Auriparus flaviceps"]


def test_species_display_name_uses_leaf_spelling_when_no_root_exists(tmp_path):
    """When a linked hierarchy leaf's canonical taxon has no top-level
    root keyword (for example a hierarchy-only accept whose top-level
    ``Black Phoebe`` was never created), ``resolve_species_display_name``
    must return the matched leaf's stored spelling. The case-convention
    fallback would otherwise mint ``Black Phoebe`` from a submitted
    ``black phoebe`` while the photo only carries the leaf ``black
    phoebe``, so highlight / preference eligibility (still keyed on
    exact ``k.name`` for hierarchy-only tags) would drop the bucket on
    reload."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(
        db, [(9876, "Sayornis nigricans", "Black Phoebe")]
    )
    parent = db.add_keyword("Tyrant flycatchers")
    leaf = db.add_keyword("black phoebe", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Black Phoebe"], leaf),
    )
    db.conn.commit()

    assert db.resolve_species_display_name("black phoebe") == "black phoebe"


def test_photo_life_list_species_preserves_linked_homonyms(tmp_path):
    """When a photo carries two linked species rows whose canonical
    roots share a SQLite-NOCASE key but point at different taxa (for
    example a legacy ``Robin`` bound to the American robin taxon and a
    taxonomy ``robin`` bound to the European robin taxon), both must
    surface in the per-photo life-list species so ``api_photo_detail``
    can match either against its ``species_representative_lists`` /
    ``species_highlights`` state. Folding by an ASCII-case-fold match
    key would hide one taxon even though its keyword remains
    attached."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    taxa = _seed_taxa(
        db,
        [
            (5001, "Turdus migratorius", "American Robin"),
            (5002, "Erithacus rubecula", "European Robin"),
        ],
    )
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # ``add_keyword`` collapses NOCASE duplicates through its typed
    # lookup, so create the two homonym rows directly to reproduce a
    # legacy catalog that preserved both distinct spellings.
    upper = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES (?, 'taxonomy', 1, ?)",
        ("Robin", taxa["American Robin"]),
    ).lastrowid
    lower = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES (?, 'taxonomy', 1, ?)",
        ("robin", taxa["European Robin"]),
    ).lastrowid
    db.conn.commit()
    db.tag_photo(pid, upper)
    db.tag_photo(pid, lower)

    assert set(db.get_photo_life_list_species(pid)) == {"Robin", "robin"}


def test_photo_life_list_species_preserves_unlinked_case_variants(tmp_path):
    """A photo carrying two NULL-taxon species rows that differ only by
    SQLite-NOCASE spelling (root ``Foo`` plus hierarchy leaf ``foo``,
    both preserved by the duplicate-repair path) must surface both
    names. Unlinked highlight / representative curation compares by
    exact ``k.name``; folding by ASCII case-fold here would drop one
    spelling and strand its curation lookups even though the keyword
    remains attached."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Something")
    nested = db.add_keyword("foo", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, "
        "taxon_id = NULL WHERE id = ?",
        (nested,),
    )
    root = db.add_keyword("Foo", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = NULL WHERE id = ?", (root,)
    )
    db.conn.commit()
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)

    assert set(db.get_photo_life_list_species(pid)) == {"Foo", "foo"}


def test_repair_duplicate_photo_species_keeps_hierarchical_association(tmp_path):
    """The one-shot repair detaches only the redundant root association."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    root = db.add_keyword("Verdin", is_species=True)
    # Simulate the legacy confirmed-species insert path, which left root
    # taxonomy rows unlinked even when the local taxon was known.
    db.conn.execute("UPDATE keywords SET taxon_id = NULL WHERE id = ?", (root,))
    db.conn.commit()
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.queue_change(pid, "keyword_add", "Verdin")
    keyword_edit_id = db.record_edit(
        "keyword_add", 'Confirmed species "Verdin"', str(root),
        [{"photo_id": pid, "old_value": "", "new_value": str(root)}],
    )
    rating_edit_id = db.record_edit(
        "rating", "Changed rating", "5",
        [{
            "photo_id": pid,
            "old_value": str(root),
            "new_value": str(root),
        }],
    )
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    tagged_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert nested in tagged_ids
    assert root not in tagged_ids
    assert db.conn.execute(
        "SELECT 1 FROM keywords WHERE id = ?", (root,)
    ).fetchone() is not None
    assert not [
        row for row in db.get_pending_changes()
        if row["photo_id"] == pid and row["value"] == "Verdin"
    ]
    history_item = db.conn.execute(
        "SELECT new_value FROM edit_history_items WHERE edit_id = ?",
        (keyword_edit_id,),
    ).fetchone()
    assert history_item is None
    rating_item = db.conn.execute(
        "SELECT old_value, new_value FROM edit_history_items WHERE edit_id = ?",
        (rating_edit_id,),
    ).fetchone()
    assert dict(rating_item) == {
        "old_value": str(root),
        "new_value": str(root),
    }
    assert db.repair_duplicate_photo_species() == 0


def test_repair_duplicate_photo_species_drops_root_redo_history(tmp_path):
    """Undo/redo after repair must not recreate the detached root species."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    root = db.add_keyword("Verdin", is_species=True)
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.record_edit(
        "keyword_add", 'Confirmed species "Verdin"', str(root),
        [{"photo_id": pid, "old_value": "", "new_value": str(root)}],
    )
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    assert db.get_edit_history() == []
    assert db.undo_last_edit() is None
    assert db.redo_last_undo() is None
    tagged_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert nested in tagged_ids
    assert root not in tagged_ids


def test_repair_duplicate_photo_species_preserves_no_tag_prediction_accepts(tmp_path):
    """A ``prediction_accept`` recorded with a ``no_tag`` JSON old_value
    already skips tag mutations on undo/redo (see the ``_skip_tag_undo``
    / ``_skip_tag_redo`` branches keyed by ``old_meta['no_tag']``), so
    keeping the item cannot reattach the detached root. Deleting it
    would erase the only audit/undo record of the accepted prediction-
    status flip — the repair must leave those items intact."""
    import json as _json

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    root = db.add_keyword("Verdin", is_species=True)
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    # Simulate an accept where the photo already carried the equivalent
    # hierarchy leaf: the API records ``prediction_accept`` with
    # ``old_value`` = JSON payload including ``no_tag: true``. Its
    # ``new_value`` still points at the root keyword id so it would
    # match the repair's numeric filter without the no_tag guard.
    payload = _json.dumps({"prediction_id": 42, "no_tag": True})
    no_tag_edit_id = db.record_edit(
        "prediction_accept", 'Accepted "Verdin"', str(root),
        [{"photo_id": pid, "old_value": payload,
          "new_value": str(root)}],
    )
    # A regular tag-mutating prediction_accept for the same root must
    # still be dropped so its undo/redo cannot reattach the detached
    # root.
    regular_edit_id = db.record_edit(
        "prediction_accept", 'Accepted "Verdin" (tagged)', str(root),
        [{"photo_id": pid, "old_value": "17",
          "new_value": str(root)}],
    )
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    tagged_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert nested in tagged_ids
    assert root not in tagged_ids
    # The no_tag accept survives — its audit/undo path is intact.
    surviving = db.conn.execute(
        "SELECT old_value, new_value FROM edit_history_items "
        "WHERE edit_id = ? AND photo_id = ?",
        (no_tag_edit_id, pid),
    ).fetchone()
    assert surviving is not None
    assert surviving["old_value"] == payload
    assert surviving["new_value"] == str(root)
    # The tag-mutating accept is dropped so redo can't reattach root.
    dropped = db.conn.execute(
        "SELECT 1 FROM edit_history_items "
        "WHERE edit_id = ? AND photo_id = ?",
        (regular_edit_id, pid),
    ).fetchone()
    assert dropped is None


def test_repair_duplicate_photo_species_guards_against_unlinked_homonyms(tmp_path):
    """A legacy NULL-taxon leaf whose key collides with a linked root but
    represents a different species (e.g. legacy ``Robin`` alongside
    taxonomy ``Robin`` linked to a different taxon) must not be folded
    into the linked root's group and cause the repair to detach the
    accepted taxonomy species."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(
        db,
        [
            (100, "Erithacus rubecula", "Robin"),
            (200, "Turdus migratorius", "American Robin"),
        ],
    )
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Linked accepted species (Erithacus rubecula / Robin).
    root_linked = db.add_keyword("Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["Robin"], root_linked),
    )
    # A DIFFERENT species (American Robin) also exists in the catalog under
    # the same normalized display key — legacy hierarchy leaves under the
    # same name were common before disambiguation.
    other_root = db.add_keyword("Robin", parent_id=None, is_species=True)
    # Manually make the "other_root" a taxonomy-linked homonym pointing at
    # a different taxon. add_keyword may already have merged into the
    # existing "Robin" row; if so, seed a separate linked keyword.
    other_root_row = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ? AND parent_id IS NULL "
        "AND id != ?",
        ("Robin", root_linked),
    ).fetchone()
    if other_root_row is None:
        db.conn.execute(
            "INSERT INTO keywords (name, is_species, type, taxon_id) "
            "VALUES (?, 1, 'taxonomy', ?)",
            ("Robin (alt)", taxa["American Robin"]),
        )
        other_row = db.conn.execute(
            "SELECT id FROM keywords WHERE name = ?", ("Robin (alt)",)
        ).fetchone()
        # Restore the same match_key so the ambiguity guard triggers.
        db.conn.execute(
            "UPDATE keywords SET name = ? WHERE id = ?",
            ("Robin", other_row["id"]),
        )
    parent = db.add_keyword("Songbirds")
    # An unlinked hierarchy leaf named "Robin" under Songbirds — intent
    # ambiguous, could be either taxon.
    unlinked_leaf = db.add_keyword("Robin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
        "taxon_id = NULL WHERE id = ?",
        (unlinked_leaf,),
    )
    db.tag_photo(pid, root_linked)
    db.tag_photo(pid, unlinked_leaf)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    # The repair must leave the linked root attached; the unlinked leaf
    # cannot safely be folded into the linked group because the key is a
    # known homonym.
    db.repair_duplicate_photo_species()
    tagged_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert root_linked in tagged_ids


def test_repair_duplicate_photo_species_deletes_json_species_replace_history(tmp_path):
    """species_replace items can store ``old_value`` as JSON with
    ``keyword_ids``. A bare-string equality misses those, so undo/redo
    of that entry would re-tag the detached root. The repair must parse
    JSON payloads and delete species_replace items that reference any
    of the removed root keyword ids."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute("UPDATE keywords SET taxon_id = NULL WHERE id = ?", (root,))
    db.conn.commit()
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    # Craft a species_replace item whose old_value is a JSON payload
    # referencing the redundant root via ``keyword_ids``.
    import json as _json
    new_kw = db.add_keyword("Sparrow", is_species=True)
    edit_id = db.record_edit(
        "species_replace", "swap species", str(new_kw),
        [{
            "photo_id": pid,
            "old_value": _json.dumps({"keyword_ids": [root, 999]}),
            "new_value": str(new_kw),
        }],
    )
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    # The species_replace item that referenced the detached root via JSON
    # must be dropped so undo/redo cannot re-tag the root.
    remaining = db.conn.execute(
        "SELECT COUNT(*) AS n FROM edit_history_items WHERE edit_id = ?",
        (edit_id,),
    ).fetchone()["n"]
    assert remaining == 0


def test_repair_duplicate_photo_species_queues_sidecar_remove_for_orphaned_alias(tmp_path):
    """When the surviving hierarchy leaf is spelled differently from the
    detached root (e.g. root ``Verdin`` and nested ``Desert Verdin`` for
    the same taxon), a previously synced ``dc:subject: Verdin`` in the
    sidecar would let a later re-scan re-attach the root. The repair must
    queue a ``keyword_remove`` for the orphaned root spelling so
    ``sync_to_xmp`` can clear it."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Birds")
    nested = db.add_keyword(
        "Desert Verdin", parent_id=parent, is_species=True,
    )
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id IN (?, ?)",
        (taxa["Verdin"], nested, root),
    )
    db.conn.commit()
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    tagged_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert nested in tagged_ids
    assert root not in tagged_ids

    pending = [
        row for row in db.get_pending_changes()
        if row["photo_id"] == pid
    ]
    removes = [
        row for row in pending
        if row["change_type"] == "keyword_remove"
        and row["value"] == "Verdin"
    ]
    assert len(removes) == 1, (
        f"expected a queued keyword_remove for 'Verdin', got: {pending}"
    )
    # The surviving alias must not be scheduled for removal.
    assert not [
        row for row in pending
        if row["change_type"] == "keyword_remove"
        and row["value"] == "Desert Verdin"
    ]


def test_repair_duplicate_photo_species_skips_sidecar_remove_when_survivor_matches(tmp_path):
    """When the surviving hierarchy leaf shares a normalized name with
    the detached root (both ``Verdin``), the scanner's per-photo dedup
    already skips the flat entry on re-import — queueing a keyword_remove
    would let ``sync_to_xmp`` hierarchically strip the surviving
    ``Birds|Verdin`` from the sidecar too."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    root = db.add_keyword("Verdin", is_species=True)
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    tagged_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert nested in tagged_ids
    assert root not in tagged_ids
    assert not [
        row for row in db.get_pending_changes()
        if row["photo_id"] == pid and row["change_type"] == "keyword_remove"
    ]


def test_repair_duplicate_photo_species_skips_remove_when_survivor_ancestor_matches(tmp_path):
    """When the surviving hierarchy leaf has a distinct leaf name but the
    detached root's spelling matches one of its ancestor segments (e.g.
    root ``Verdin`` detached while ``Verdin|Desert Verdin`` is kept), the
    repair must NOT queue a plain ``keyword_remove`` for ``Verdin``:
    ``sync_to_xmp`` applies keyword_remove hierarchically (``remove_keywords``
    strips any ``lr:hierarchicalSubject`` whose segment matches), so the
    next sync would delete the very ``Verdin|Desert Verdin`` hierarchy the
    repair kept in the DB. Queue a ``keyword_remove_flat`` instead so the
    stale ``dc:subject: Verdin`` still gets stripped — otherwise the
    scanner reimports it on the next XMP scan (its per-photo dedup only
    considers attached leaf names, not ancestor segments) and recreates
    the duplicate root."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Surviving hierarchy: Verdin -> Desert Verdin. The leaf is what
    # gets tagged, but the parent chain includes the ``Verdin`` segment.
    hier_parent = db.add_keyword("Verdin")
    nested = db.add_keyword(
        "Desert Verdin", parent_id=hier_parent, is_species=True,
    )
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id IN (?, ?)",
        (taxa["Verdin"], nested, root),
    )
    db.conn.commit()
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    tagged_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert nested in tagged_ids
    assert root not in tagged_ids

    pending = [
        row for row in db.get_pending_changes()
        if row["photo_id"] == pid
    ]
    assert not [
        row for row in pending
        if row["change_type"] == "keyword_remove"
        and row["value"] == "Verdin"
    ], (
        f"expected no plain keyword_remove for 'Verdin' (it appears as "
        f"an ancestor segment of the preserved 'Verdin|Desert Verdin' "
        f"hierarchy — a hierarchical sync remove would strip that "
        f"preserved entry from the sidecar), got: {pending}"
    )
    # A flat-only cleanup must still be queued so ``sync_to_xmp`` strips
    # the stale ``dc:subject: Verdin`` line without touching the
    # preserved hierarchical entry. Without it, the next XMP scan
    # reimports ``Verdin`` and reattaches the duplicate root.
    flat_removes = [
        row for row in pending
        if row["change_type"] == "keyword_remove_flat"
        and row["value"] == "Verdin"
    ]
    assert flat_removes, (
        f"expected a queued keyword_remove_flat for 'Verdin' to strip "
        f"the stale flat root from dc:subject without touching the "
        f"preserved 'Verdin|Desert Verdin' hierarchy, got: {pending}"
    )


def test_repair_duplicate_photo_species_cancels_unsynced_root_add(tmp_path):
    """When a still-unsynced ``keyword_add`` for the detached root name
    is pending, the repair must cancel it and NOT queue a
    ``keyword_remove`` — the sidecar has not received the flat root, so
    there is nothing to strip and a stale remove would only cause
    unnecessary XMP churn."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Birds")
    nested = db.add_keyword(
        "Desert Verdin", parent_id=parent, is_species=True,
    )
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id IN (?, ?)",
        (taxa["Verdin"], nested, root),
    )
    db.conn.commit()
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.queue_change(pid, "keyword_add", "Verdin")
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    pending = [
        row for row in db.get_pending_changes()
        if row["photo_id"] == pid
    ]
    assert not [row for row in pending if row["value"] == "Verdin"], (
        f"expected the pending Verdin add to be cancelled with no "
        f"replacement remove, got: {pending}"
    )


def test_repair_duplicate_photo_species_preserves_leaf_pending_add(tmp_path):
    """A pending ``keyword_add`` for a surviving hierarchy leaf must not
    be cancelled when the repair detaches the redundant root. Cancelling
    the leaf's add would leave ``sync_to_xmp`` writing only the root
    cleanup and never surfacing the preserved hierarchy in XMP; a
    subsequent scan would then see the DB and sidecar diverge."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Birds")
    nested = db.add_keyword(
        "Desert Verdin", parent_id=parent, is_species=True,
    )
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id IN (?, ?)",
        (taxa["Verdin"], nested, root),
    )
    db.conn.commit()
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    # The leaf's add is still pending sidecar sync when repair fires.
    db.queue_change(pid, "keyword_add", "Desert Verdin")
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    pending = [
        row for row in db.get_pending_changes()
        if row["photo_id"] == pid
    ]
    leaf_adds = [
        row for row in pending
        if row["change_type"] == "keyword_add"
        and row["value"] == "Desert Verdin"
    ]
    assert len(leaf_adds) == 1, (
        f"expected the preserved leaf's pending keyword_add to survive "
        f"the repair, got: {pending}"
    )


def test_repair_duplicate_photo_species_queues_sidecar_remove_in_photo_workspace(tmp_path):
    """When the photo's folder lives only in a workspace other than the
    active one, the sidecar ``keyword_remove`` must be queued under that
    workspace. ``get_pending_changes`` filters by the active workspace,
    so a remove queued under the wrong workspace would leave the stale
    root spelling in the sidecar for the real workspace's next sync."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    active_ws = db._ws_id()
    other_ws = db.create_workspace("Other")
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    # Move the folder out of the active workspace and into "Other".
    db.remove_workspace_folder(active_ws, fid)
    db.add_workspace_folder(other_ws, fid)
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Birds")
    nested = db.add_keyword(
        "Desert Verdin", parent_id=parent, is_species=True,
    )
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id IN (?, ?)",
        (taxa["Verdin"], nested, root),
    )
    db.conn.commit()
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1

    # No remove should appear in the active workspace's queue…
    assert not [
        row for row in db.get_pending_changes()
        if row["photo_id"] == pid
        and row["change_type"] == "keyword_remove"
        and row["value"] == "Verdin"
    ], (
        "expected no queued remove in the (empty) active workspace"
    )

    # …but the "Other" workspace (which actually contains the photo)
    # must have exactly one queued keyword_remove for the orphaned root.
    db.set_active_workspace(other_ws)
    removes = [
        row for row in db.get_pending_changes()
        if row["photo_id"] == pid
        and row["change_type"] == "keyword_remove"
        and row["value"] == "Verdin"
    ]
    assert len(removes) == 1, (
        f"expected a queued keyword_remove for 'Verdin' in the "
        f"photo's workspace, got: {removes}"
    )


def test_repair_duplicate_photo_species_queues_sidecar_remove_in_every_photo_workspace(tmp_path):
    """When a photo's folder is shared across multiple workspaces, the
    sidecar keyword_remove must be queued in each so any workspace that
    later syncs strips the orphaned root spelling from the sidecar."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    active_ws = db._ws_id()
    other_ws = db.create_workspace("Other")
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    db.add_workspace_folder(other_ws, fid)
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Birds")
    nested = db.add_keyword(
        "Desert Verdin", parent_id=parent, is_species=True,
    )
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id IN (?, ?)",
        (taxa["Verdin"], nested, root),
    )
    db.conn.commit()
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1

    for ws_id in (active_ws, other_ws):
        db.set_active_workspace(ws_id)
        removes = [
            row for row in db.get_pending_changes()
            if row["photo_id"] == pid
            and row["change_type"] == "keyword_remove"
            and row["value"] == "Verdin"
        ]
        assert len(removes) == 1, (
            f"workspace {ws_id} should have exactly one queued "
            f"keyword_remove for 'Verdin', got: {removes}"
        )


def test_repair_duplicate_photo_species_waits_for_local_taxa(tmp_path):
    """An empty taxa table must not consume the one-shot repair marker."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 0
    assert db.get_meta(db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY) is None


def test_repair_duplicate_photo_species_keeps_curation_on_root_key(tmp_path):
    """Hierarchy spelling differences do not move curation away from the
    canonical root species key used by subsequent API requests."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], nested),
    )
    root = db.add_keyword("Verdin", is_species=True)
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.add_species_highlight("Verdin", pid)
    db.set_photo_preference("highlights", "Verdin", pid)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    for table in (
        "species_highlights", "photo_preferences", "species_representatives",
    ):
        species = {
            row["species"] for row in db.conn.execute(
                f"SELECT species FROM {table} WHERE photo_id = ?", (pid,),
            ).fetchall()
        }
        assert species == {"Verdin"}


def test_repair_duplicate_photo_species_preserves_curation_eligibility(tmp_path):
    """Root-key curation stays eligible after repair when the surviving
    hierarchy leaf's stored spelling differs from the root.

    ``get_species_representative_lists(eligible_only=True)`` and the
    life-list preference validator previously required an exact
    ``k.name = sr.species`` match. After repair detaches the redundant
    root row but leaves the hierarchical leaf attached with a differently
    spelled name, an existing "Verdin" representative would be silently
    dropped and updating the preserved root-key preference would fail
    eligibility even though the same taxon is still attached via
    hierarchy.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], nested),
    )
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["Verdin"], root),
    )
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.set_species_representative("Verdin", pid)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1

    # Root keyword row is no longer attached, only the differently-spelled
    # hierarchy leaf remains.
    attached_names = {
        row["name"] for row in db.conn.execute(
            """SELECT k.name FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
               WHERE pk.photo_id = ?""",
            (pid,),
        ).fetchall()
    }
    assert "verdin" in attached_names
    assert "Verdin" not in attached_names

    # The stored representative under the canonical "Verdin" key must
    # still surface under eligible_only=True even though only the lower-
    # cased hierarchy leaf is attached.
    eligible = db.get_species_representative_lists(eligible_only=True)
    assert eligible.get("Verdin") == [pid]
    assert db.get_species_representatives(eligible_only=True) == {"Verdin": pid}

    # The life-list preference validator that gates /api/photo-preferences
    # writes must accept the same photo for the preserved root key so a
    # user updating their "Verdin" representative doesn't get an
    # eligibility error after repair.
    from app import create_app  # noqa: WPS433 (test-scoped import)

    app = create_app(str(tmp_path / "test.db"), str(tmp_path / "thumbs"))
    with app.test_client() as client:
        resp = client.post(
            "/api/photo-preferences",
            json={
                "purpose": "species_representative",
                "species": "Verdin",
                "photo_id": pid,
            },
        )
        assert resp.status_code == 200, resp.get_json()


def test_repair_duplicate_photo_species_skips_unlinked_case_variants(tmp_path):
    """Unlinked (NULL-taxon) same-key duplicates with different spellings
    must stay attached. Curation/eligibility for unlinked species keys is
    compared with exact ``k.name`` — there is no taxon fallback that maps
    a differently-spelled leaf back to the root spelling. Detaching root
    ``Foo`` while only leaf ``foo`` remains would strand the
    representative/highlight stored under ``Foo``. Same-spelling unlinked
    duplicates are still repaired because the surviving leaf carries the
    exact root name.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    # A species row is required so the repair's early-exit does not fire;
    # the seeded taxon is unrelated to the unlinked "Foo" keywords under
    # test.
    _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Something")
    # Unlinked hierarchy leaf spelled "foo"; taxonomy-typed without a
    # taxon link (mark_species_keywords stamps this on legacy upgrades).
    nested = db.add_keyword("foo", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, "
        "taxon_id = NULL WHERE id = ?",
        (nested,),
    )
    # Unlinked root spelled "Foo" — different spelling, same match_key.
    root = db.add_keyword("Foo", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = NULL WHERE id = ?", (root,)
    )
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    # Curation stored under the root spelling — the exact string the
    # unlinked eligibility queries compare against.
    db.set_species_representative("Foo", pid)
    db.add_species_highlight("Foo", pid)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    # The unlinked case-variant duplicate is left alone — there is no
    # safe canonicalization when both rows have NULL taxon_id.
    assert db.repair_duplicate_photo_species() == 0
    attached_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert root in attached_ids, (
        "unlinked root spelling must stay attached so exact-name curation "
        "keeps applying"
    )
    assert nested in attached_ids

    # Root-spelled representative and highlight remain eligible because
    # the exact ``k.name = 'Foo'`` row is still attached.
    assert db.get_species_representatives(eligible_only=True) == {"Foo": pid}
    highlights = db.get_species_highlights(eligible_only=True)
    assert pid in highlights.get("Foo", {})


def test_repair_duplicate_photo_species_repairs_unlinked_same_spelling(tmp_path):
    """Unlinked duplicates whose root and surviving leaf share the same
    exact spelling are still repaired: the leaf carries the same
    ``k.name`` after the root is detached, so exact-name eligibility keeps
    matching.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Something")
    nested = db.add_keyword("Foo", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, "
        "taxon_id = NULL WHERE id = ?",
        (nested,),
    )
    root = db.add_keyword("Foo", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = NULL WHERE id = ?", (root,)
    )
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.set_species_representative("Foo", pid)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1
    attached_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert nested in attached_ids
    assert root not in attached_ids
    # The surviving leaf carries the same "Foo" name, so root-spelled
    # curation still resolves.
    assert db.get_species_representatives(eligible_only=True) == {"Foo": pid}


def test_update_keyword_rename_general_no_match_stays_general(tmp_path):
    """Renaming a 'general' keyword to a name with no taxon match leaves
    it as 'general' with NULL taxon_id."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(7000, "Aythya affinis", "Lesser Scaup")])

    kid = db.add_keyword("Misc thing")
    db.update_keyword(kid, name="Still misc")

    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Still misc"
    assert row["type"] == "general"
    assert row["taxon_id"] is None


def test_update_keyword_rename_taxonomy_to_different_taxon_updates_taxon_id(tmp_path):
    """Renaming a keyword that's already 'taxonomy' to a name matching a
    DIFFERENT taxon updates taxon_id and keeps type='taxonomy'."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [
        (7000, "Aythya affinis", "Lesser Scaup"),
        (7001, "Aythya marila", "Greater Scaup"),
    ])

    kid = db.add_keyword("Lesser Scaup")
    pre = db.conn.execute(
        "SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert pre["type"] == "taxonomy"
    assert pre["taxon_id"] == taxa["Lesser Scaup"]

    db.update_keyword(kid, name="Greater Scaup")

    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Greater Scaup"
    assert row["type"] == "taxonomy"
    # Documented behavior: when the new name matches a different taxon,
    # taxon_id is updated to point at the new match.
    assert row["taxon_id"] == taxa["Greater Scaup"]


def test_update_keyword_rename_taxonomy_to_unknown_name_keeps_taxon_id(tmp_path):
    """Renaming a 'taxonomy' keyword to a name with no taxon match keeps
    type='taxonomy' and leaves taxon_id as-is (don't drop the link)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(7000, "Aythya affinis", "Lesser Scaup")])

    kid = db.add_keyword("Lesser Scaup")
    pre_taxon_id = taxa["Lesser Scaup"]
    assert db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()["taxon_id"] == pre_taxon_id

    db.update_keyword(kid, name="Some custom name")

    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Some custom name"
    # type is preserved (not 'general'), taxon_id is preserved.
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] == pre_taxon_id


def test_update_keyword_rename_location_keyword_preserves_type_and_taxon_id(tmp_path):
    """User intent wins: a manually-typed 'location' keyword that's
    renamed to a string matching a taxon must NOT be re-classified as
    'taxonomy'. type and taxon_id stay intact."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(7000, "Aythya affinis", "Lesser Scaup")])

    # Add a general keyword, then user marks it as 'location'.
    kid = db.add_keyword("Backyard")
    db.update_keyword(kid, type="location")
    pre = db.conn.execute(
        "SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert pre["type"] == "location"
    assert pre["taxon_id"] is None

    # User renames it to a name that happens to match a taxon.
    db.update_keyword(kid, name="Lesser Scaup")

    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Lesser Scaup"
    assert row["type"] == "location", "manual 'location' must not be auto-overridden"
    assert row["taxon_id"] is None


def test_update_keyword_no_name_change_does_not_touch_type(tmp_path):
    """Updates that don't include 'name' don't trigger auto-detect logic
    and behave like the pre-fix update_keyword."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(7000, "Aythya affinis", "Lesser Scaup")])

    kid = db.add_keyword("Backyard")
    # Just change type — name stays 'Backyard'.
    db.update_keyword(kid, type="location")
    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Backyard"
    assert row["type"] == "location"
    assert row["taxon_id"] is None


def test_update_keyword_explicit_location_type_clears_species_flag(tmp_path):
    """Demoting a taxonomy homonym without a merge peer clears is_species.

    The Keywords UI sends only ``type`` for this edit. Leaving the legacy flag
    set would make species queries continue to treat the location as taxonomy
    even though taxonomy marking now preserves explicit non-taxonomy types.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword("California", kw_type="taxonomy")
    before = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid,),
    ).fetchone()
    assert dict(before) == {"type": "taxonomy", "is_species": 1}

    effective_id = db.update_keyword(kid, type="location")

    assert effective_id == kid
    after = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid,),
    ).fetchone()
    assert dict(after) == {"type": "location", "is_species": 0}


def test_update_keyword_noop_general_type_preserves_legacy_species_flag(tmp_path):
    """Re-selecting General must not erase a legacy species marker."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Robin', 'general', 1)"
    )
    kid = cur.lastrowid
    db.conn.commit()

    effective_id = db.update_keyword(kid, type="general")

    assert effective_id == kid
    row = db.conn.execute(
        "SELECT type, is_species FROM keywords WHERE id = ?", (kid,),
    ).fetchone()
    assert dict(row) == {"type": "general", "is_species": 1}


def test_update_keyword_explicit_type_and_taxon_id_kwargs_win(tmp_path):
    """Caller-supplied type and taxon_id win over auto-detection. Used by
    the bulk-type-apply UI path."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [
        (7000, "Aythya affinis", "Lesser Scaup"),
        (7001, "Aythya marila", "Greater Scaup"),
    ])

    kid = db.add_keyword("Misc")  # general, no taxon
    # Caller renames AND explicitly sets type+taxon_id to a different taxon.
    # Auto-detect would pick "Lesser Scaup" -> taxa["Lesser Scaup"], but
    # the explicit kwargs must override.
    db.update_keyword(
        kid,
        name="Lesser Scaup",
        type="taxonomy",
        taxon_id=taxa["Greater Scaup"],
    )
    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Lesser Scaup"
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] == taxa["Greater Scaup"]


def test_update_keyword_rename_with_explicit_non_taxonomy_type_skips_taxon_link(tmp_path):
    """Combined rename + explicit non-taxonomy type must not auto-fill
    taxon_id. Otherwise the row ends up as type='location' with a
    taxonomy link, which violates the docstring's precedence ('explicit
    kwargs win') and creates inconsistent state for callers that send
    combined name/type updates from the API."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    _seed_taxa(db, [(7000, "Aythya affinis", "Lesser Scaup")])

    kid = db.add_keyword("Lesser scaub")  # general, no taxon match
    pre = db.conn.execute(
        "SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert pre["type"] == "general"
    assert pre["taxon_id"] is None

    # Caller renames AND explicitly types as 'location'. Even though
    # "Lesser Scaup" matches a taxon, the effective type is 'location',
    # so taxon_id MUST NOT be auto-filled.
    db.update_keyword(kid, name="Lesser Scaup", type="location")

    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Lesser Scaup"
    assert row["type"] == "location"
    assert row["taxon_id"] is None, (
        "explicit non-taxonomy type must suppress taxon_id auto-fill"
    )


def test_update_keyword_rename_taxonomy_to_explicit_non_taxonomy_type_does_not_relink(tmp_path):
    """When changing a 'taxonomy' keyword's type to something else AND
    renaming, do not auto-refresh taxon_id to the new name's match.
    The existing taxon_id is left untouched (no auto-fill, no auto-clear)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [
        (7000, "Aythya affinis", "Lesser Scaup"),
        (7001, "Aythya marila", "Greater Scaup"),
    ])

    kid = db.add_keyword("Lesser Scaup")
    pre_taxon_id = taxa["Lesser Scaup"]
    assert db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()["taxon_id"] == pre_taxon_id

    # Rename to a name that matches a different taxon, but explicitly
    # demote type to 'location'. taxon_id should NOT auto-refresh to
    # taxa["Greater Scaup"] because the effective type isn't 'taxonomy'.
    db.update_keyword(kid, name="Greater Scaup", type="location")

    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Greater Scaup"
    assert row["type"] == "location"
    assert row["taxon_id"] == pre_taxon_id, (
        "explicit non-taxonomy type must suppress taxon_id auto-refresh"
    )


def test_update_keyword_rename_with_empty_taxa_table_no_op(tmp_path):
    """If the taxa table is empty (user hasn't downloaded taxonomy yet),
    rename succeeds without error and without auto-reclassification."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # No _seed_taxa call — taxa table is empty.

    kid = db.add_keyword("Lesser scaub")
    db.update_keyword(kid, name="Lesser Scaup")

    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Lesser Scaup"
    assert row["type"] == "general"
    assert row["taxon_id"] is None


def test_update_keyword_idempotent_name_update_does_not_auto_retype(tmp_path):
    """Sending the same name (idempotent PUT) must NOT trigger
    auto-detection. A pre-existing 'general' keyword whose name happens
    to match a taxon stays 'general' when a client re-sends the full
    keyword object with no actual rename — auto-detection only fires on
    a real name change."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))

    # Add a 'general' keyword BEFORE the taxa table is populated so it
    # doesn't get auto-typed on insert. This mirrors a realistic
    # scenario: user added "Lesser Scaup" as a freeform tag before
    # downloading taxonomy.
    kid = db.add_keyword("Lesser Scaup")
    pre = db.conn.execute(
        "SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert pre["type"] == "general"
    assert pre["taxon_id"] is None

    # Now populate taxonomy.
    _seed_taxa(db, [(7000, "Aythya affinis", "Lesser Scaup")])

    # Idempotent update: same name. Must NOT auto-promote to 'taxonomy'.
    db.update_keyword(kid, name="Lesser Scaup")

    row = db.conn.execute(
        "SELECT name, type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Lesser Scaup"
    assert row["type"] == "general", (
        "no actual name change — auto-detection must not fire"
    )
    assert row["taxon_id"] is None


def test_update_keyword_rename_normalizes_edge_quotes(tmp_path):
    """Rename must apply the same normalization as add_keyword so a
    request like `‘apapane` stores `apapane`. Without this the PUT path
    bypasses the duplicate-prevention contract enforced on insert."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        kid = db.add_keyword("apapane")
        db.update_keyword(kid, name="‘apapane")

        row = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (kid,)
        ).fetchone()
        assert row["name"] == "apapane"
    finally:
        db.close()


def test_update_keyword_rename_rejects_empty_after_normalization(tmp_path):
    """A rename whose normalized value is empty (quote-only input) must
    raise ValueError, mirroring add_keyword. Otherwise the PUT path
    would store an invisible/invalid keyword row."""
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        kid = db.add_keyword("Real Keyword")

        with pytest.raises(ValueError):
            db.update_keyword(kid, name="'")
        with pytest.raises(ValueError):
            db.update_keyword(kid, name="“”")

        # Original name still in place.
        row = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (kid,)
        ).fetchone()
        assert row["name"] == "Real Keyword"
    finally:
        db.close()


def test_update_keyword_rename_preserves_okina(tmp_path):
    """Legitimate leading okina (U+02BB) must survive rename normalization
    the same way it does through add_keyword — species names such as
    'ʻApapane' are the point of that carve-out."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        kid = db.add_keyword("Placeholder")
        db.update_keyword(kid, name="ʻApapane")

        row = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (kid,)
        ).fetchone()
        assert row["name"] == "ʻApapane"
    finally:
        db.close()


def test_update_keyword_same_name_retype_merges_into_peer(tmp_path):
    """A PUT that normalizes the name back to the current stored value but
    changes the type (e.g. `{name: "‘apapane", type: "taxonomy"}` on a
    general `apapane` row) must run the same peer/collision check that a
    real rename does. Otherwise the UPDATE silently produces two
    taxonomy rows that normalize to the same key at the same slot,
    because UNIQUE(name, parent_id) doesn't constrain NULL parents.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        # Top-level taxonomy `apapane` already exists.
        cur = db.conn.execute(
            "INSERT INTO keywords (name, parent_id, is_species, type) "
            "VALUES (?, NULL, 1, 'taxonomy')",
            ("apapane",),
        )
        taxonomy_id = cur.lastrowid
        # A separate top-level general `apapane` row also exists.
        general_id = db.add_keyword("apapane", kw_type="general")
        assert general_id != taxonomy_id

        # PUT-style retype whose name normalizes back to the current stored
        # value (`‘apapane` → `apapane`) but whose type moves to
        # 'taxonomy'. Must merge into the existing taxonomy peer rather
        # than promoting the general row and leaving two taxonomy rows.
        effective_id = db.update_keyword(
            general_id, name="‘apapane", type="taxonomy"
        )
        assert effective_id == taxonomy_id

        # Exactly one top-level taxonomy `apapane` row must remain.
        # Stored names are always normalized, so a plain name comparison
        # is exact.
        rows = db.conn.execute(
            "SELECT id FROM keywords "
            "WHERE name = 'apapane' COLLATE NOCASE "
            "AND parent_id IS NULL AND type = 'taxonomy'"
        ).fetchall()
        assert [r["id"] for r in rows] == [taxonomy_id]
        # The old general row must be gone (merged away).
        gone = db.conn.execute(
            "SELECT id FROM keywords WHERE id = ?", (general_id,)
        ).fetchone()
        assert gone is None
    finally:
        db.close()


def test_update_keyword_type_only_put_merges_into_normalized_peer(tmp_path):
    """A type-only PUT (no `name` kwarg) must still run the same-slot peer
    check. Otherwise, changing a general `apapane` row's type to
    `taxonomy` via the Browse/Keywords type dropdown while a taxonomy
    `apapane` peer already exists at the same (NULL) parent leaves two
    same-name taxonomy rows at the top level (UNIQUE(name, parent_id)
    treats NULL parents as distinct), so later calls bind to either id at
    random.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        # Clean top-level taxonomy row (stored rows are always normalized).
        cur = db.conn.execute(
            "INSERT INTO keywords (name, parent_id, is_species, type) "
            "VALUES (?, NULL, 1, 'taxonomy')",
            ("apapane",),
        )
        taxonomy_id = cur.lastrowid
        # General peer at the same slot.
        general_id = db.add_keyword("apapane", kw_type="general")
        assert general_id != taxonomy_id

        # Type-only PUT: the dropdown changes just the type, no name.
        effective_id = db.update_keyword(general_id, type="taxonomy")
        assert effective_id == taxonomy_id

        # Exactly one top-level taxonomy row must remain for the name, and
        # it must be the pre-existing taxonomy row.
        rows = db.conn.execute(
            "SELECT id FROM keywords "
            "WHERE name = 'apapane' COLLATE NOCASE "
            "AND parent_id IS NULL AND type = 'taxonomy'"
        ).fetchall()
        assert [r["id"] for r in rows] == [taxonomy_id]
        # The old general row must be gone (merged away).
        gone = db.conn.execute(
            "SELECT id FROM keywords WHERE id = ?", (general_id,)
        ).fetchone()
        assert gone is None
    finally:
        db.close()


def test_update_keyword_retype_to_nontaxonomy_does_not_leak_is_species(tmp_path):
    """Retyping a taxonomy row to a non-taxonomy type that already has a
    same-name peer must not copy is_species=1 (or the taxon link) onto the
    surviving peer. Otherwise species queries — `is_species = 1 OR type =
    'taxonomy'` — would keep matching photos tagged with the survivor
    (e.g. an ``individual`` row), silently mis-tagging every photo already
    on it. Guards `_merge_keyword_into`'s destination-side flag propagation.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        # Simulate a real folder + photos so tag rows are well-formed.
        fid = db.add_folder("/photos", name="photos")
        pid_src = db.add_photo(
            folder_id=fid, filename="a.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        pid_dst = db.add_photo(
            folder_id=fid, filename="b.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )

        # Taxonomy `Robin` (species=1) at the top level.
        cur = db.conn.execute(
            "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id) "
            "VALUES (?, NULL, 1, 'taxonomy', NULL)",
            ("Robin",),
        )
        taxonomy_id = cur.lastrowid
        db.tag_photo(pid_src, taxonomy_id)

        # Existing individual `Robin` peer at the same slot, is_species=0.
        cur = db.conn.execute(
            "INSERT INTO keywords (name, parent_id, is_species, type) "
            "VALUES (?, NULL, 0, 'individual')",
            ("Robin",),
        )
        individual_id = cur.lastrowid
        db.tag_photo(pid_dst, individual_id)

        # Retype the taxonomy row to 'individual'. update_keyword must merge
        # into the individual peer and return its id.
        effective_id = db.update_keyword(taxonomy_id, type="individual")
        assert effective_id == individual_id

        # The taxonomy row is gone.
        assert db.conn.execute(
            "SELECT id FROM keywords WHERE id = ?", (taxonomy_id,)
        ).fetchone() is None

        # Critically: the surviving individual row must NOT have inherited
        # is_species=1 or a taxon_id from the taxonomy source.
        survivor = db.conn.execute(
            "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?",
            (individual_id,),
        ).fetchone()
        assert survivor["type"] == "individual"
        assert survivor["is_species"] == 0, (
            "is_species must not leak into a non-taxonomy destination"
        )
        assert survivor["taxon_id"] is None, (
            "taxon_id must not leak into a non-taxonomy destination"
        )

        # Photos from both sides are retained on the survivor.
        tagged = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (individual_id,),
            ).fetchall()
        }
        assert tagged == {pid_src, pid_dst}
    finally:
        db.close()


def test_update_keyword_retype_legacy_general_is_species_does_not_leak(tmp_path):
    """Legacy `type='general', is_species=1` rows upgraded from pre-invariant
    databases still count as species-bearing to the rest of the app
    (`is_species = 1 OR type = 'taxonomy'`). Retyping such a row into an
    existing individual/general peer must not stamp is_species=1 or a taxon
    link onto the non-taxonomy survivor. Extends
    ``test_update_keyword_retype_to_nontaxonomy_does_not_leak_is_species``
    to cover the general-source variant flagged by Codex.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        fid = db.add_folder("/photos", name="photos")
        pid_src = db.add_photo(
            folder_id=fid, filename="a.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        pid_dst = db.add_photo(
            folder_id=fid, filename="b.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )

        # Legacy general row with is_species=1 and a taxon link — the shape
        # upgraded DBs can carry before mark_species_keywords retypes them.
        cur = db.conn.execute(
            "INSERT INTO taxa (name, rank) VALUES (?, ?)",
            ("Turdus migratorius", "species"),
        )
        taxon_row_id = cur.lastrowid
        cur = db.conn.execute(
            "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id) "
            "VALUES (?, NULL, 1, 'general', ?)",
            ("Robin", taxon_row_id),
        )
        general_id = cur.lastrowid
        db.tag_photo(pid_src, general_id)

        # Existing individual `Robin` peer at the same slot, no species flag.
        cur = db.conn.execute(
            "INSERT INTO keywords (name, parent_id, is_species, type) "
            "VALUES (?, NULL, 0, 'individual')",
            ("Robin",),
        )
        individual_id = cur.lastrowid
        db.tag_photo(pid_dst, individual_id)

        # Retype the legacy general row to 'individual'. update_keyword must
        # merge into the individual peer.
        effective_id = db.update_keyword(general_id, type="individual")
        assert effective_id == individual_id

        assert db.conn.execute(
            "SELECT id FROM keywords WHERE id = ?", (general_id,)
        ).fetchone() is None

        # Critically: is_species and taxon_id must NOT leak onto the
        # non-taxonomy survivor even though the source was type='general'
        # (not 'taxonomy').
        survivor = db.conn.execute(
            "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?",
            (individual_id,),
        ).fetchone()
        assert survivor["type"] == "individual"
        assert survivor["is_species"] == 0, (
            "legacy is_species=1 general source must not stamp survivor"
        )
        assert survivor["taxon_id"] is None

        tagged = {
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
                (individual_id,),
            ).fetchall()
        }
        assert tagged == {pid_src, pid_dst}
    finally:
        db.close()


def test_merge_keyword_into_retargets_edit_history(tmp_path):
    """When _merge_keyword_into deletes the source keyword row, any
    edit_history entry referring to it as a keyword id must be retargeted
    onto the survivor. Otherwise undo of a recent keyword_add / _remove /
    prediction_accept / species_replace looks up the deleted id, finds
    nothing, and marks the entry undone without reversing the tag — the
    photo keeps the survivor's tag with no way to undo it. Covers the
    Codex finding on the one-shot normalization migration.
    """
    import json as _json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        fid = db.add_folder("/photos", name="photos")
        pid = db.add_photo(
            folder_id=fid, filename="a.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        pid_replace = db.add_photo(
            folder_id=fid, filename="b.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )

        # Survivor and about-to-be-merged source of the same type.
        keep_id = db.add_keyword("Robin", kw_type="general")
        merge_id = db.add_keyword("robin variant", kw_type="general")
        # Both photos are tagged with merge_id so undo has something to hit.
        db.tag_photo(pid, merge_id)
        db.tag_photo(pid_replace, merge_id)

        # 1) A bare-id keyword_add edit whose new_value points at merge_id.
        eid_add = db.record_edit(
            "keyword_add", "Added keyword", str(merge_id),
            [{"photo_id": pid, "old_value": "", "new_value": str(merge_id)}],
        )
        # 2) A keyword_remove edit whose item.old_value carries the bare id.
        db.tag_photo(pid, merge_id)  # ensure tagged before recording remove
        eid_remove = db.record_edit(
            "keyword_remove", "Removed keyword", str(merge_id),
            [{"photo_id": pid, "old_value": str(merge_id), "new_value": ""}],
        )
        # 3) A species_replace edit with JSON metadata carrying keyword_id +
        #    keyword_ids that reference merge_id.
        payload = _json.dumps(
            {"keyword_id": merge_id, "keyword_ids": [merge_id]},
            sort_keys=True,
        )
        eid_replace = db.record_edit(
            "species_replace", "Replaced species", str(merge_id),
            [{
                "photo_id": pid_replace,
                "old_value": payload,
                "new_value": str(merge_id),
            }],
        )

        # Merge merge_id into keep_id (mirrors the migration's convergence
        # loop, and update_keyword's retype-into-peer path).
        db._merge_keyword_into(merge_id, keep_id)
        db.conn.commit()

        # edit_history.new_value retargeted for all three action types.
        for eid in (eid_add, eid_remove, eid_replace):
            row = db.conn.execute(
                "SELECT new_value FROM edit_history WHERE id = ?", (eid,)
            ).fetchone()
            assert row["new_value"] == str(keep_id), (
                f"edit_history #{eid}.new_value not retargeted"
            )

        # Bare-id item columns retargeted.
        add_item = db.conn.execute(
            "SELECT old_value, new_value FROM edit_history_items "
            "WHERE edit_id = ?", (eid_add,),
        ).fetchone()
        assert add_item["new_value"] == str(keep_id)

        remove_item = db.conn.execute(
            "SELECT old_value, new_value FROM edit_history_items "
            "WHERE edit_id = ?", (eid_remove,),
        ).fetchone()
        assert remove_item["old_value"] == str(keep_id)

        # JSON payload rewritten in place.
        replace_item = db.conn.execute(
            "SELECT old_value, new_value FROM edit_history_items "
            "WHERE edit_id = ?", (eid_replace,),
        ).fetchone()
        assert replace_item["new_value"] == str(keep_id)
        parsed = _json.loads(replace_item["old_value"])
        assert parsed["keyword_id"] == keep_id
        assert parsed["keyword_ids"] == [keep_id]
    finally:
        db.close()


def test_merge_keyword_into_dedupes_keyword_ids_after_retarget(tmp_path):
    """A species_replace payload whose keyword_ids list already contains
    the destination id must not end up with a duplicate after retarget.
    Otherwise undo would re-tag the survivor twice for the same keyword id.
    """
    import json as _json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        fid = db.add_folder("/photos", name="photos")
        pid = db.add_photo(
            folder_id=fid, filename="a.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )

        keep_id = db.add_keyword("Robin", kw_type="general")
        merge_id = db.add_keyword("robin variant", kw_type="general")
        db.tag_photo(pid, merge_id)

        payload = _json.dumps(
            {"keyword_id": merge_id, "keyword_ids": [keep_id, merge_id]},
            sort_keys=True,
        )
        eid = db.record_edit(
            "species_replace", "Replaced species", str(merge_id),
            [{
                "photo_id": pid,
                "old_value": payload,
                "new_value": str(merge_id),
            }],
        )

        db._merge_keyword_into(merge_id, keep_id)
        db.conn.commit()

        replace_item = db.conn.execute(
            "SELECT old_value FROM edit_history_items WHERE edit_id = ?",
            (eid,),
        ).fetchone()
        parsed = _json.loads(replace_item["old_value"])
        assert parsed["keyword_ids"] == [keep_id], (
            "duplicate destination id after retarget"
        )
    finally:
        db.close()


def test_merge_keyword_into_preserves_prediction_accept_old_value(tmp_path):
    """`_merge_keyword_into`'s bare-string rewrite of
    edit_history_items.old_value must skip prediction_accept entries.
    api_accept_prediction records item.old_value = str(prediction_id),
    and _edit_prediction_id falls back to that raw value. If the merged
    keyword id happens to equal a stored prediction id, a blanket rewrite
    would silently retarget undo/redo onto a different prediction.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        fid = db.add_folder("/photos", name="photos")
        pid = db.add_photo(
            folder_id=fid, filename="a.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )

        keep_id = db.add_keyword("Sparrow", kw_type="general")
        merge_id = db.add_keyword("sparrow variant", kw_type="general")

        # Contrive a prediction_accept edit whose item.old_value (the
        # prediction id per api_accept_prediction) numerically equals the
        # keyword row about to be merged — the exact collision the
        # CodeRabbit finding calls out. The prediction row itself is not
        # required for the rewrite pass; the retarget operates purely on
        # edit_history_items.
        prediction_id = merge_id
        eid = db.record_edit(
            "prediction_accept", "Accepted prediction", str(merge_id),
            [{
                "photo_id": pid,
                "old_value": str(prediction_id),
                "new_value": str(merge_id),
            }],
        )

        db.tag_photo(pid, merge_id)
        db._merge_keyword_into(merge_id, keep_id)
        db.conn.commit()

        item = db.conn.execute(
            "SELECT old_value, new_value FROM edit_history_items "
            "WHERE edit_id = ?", (eid,),
        ).fetchone()
        # new_value (keyword id) IS retargeted onto the survivor.
        assert item["new_value"] == str(keep_id)
        # old_value is the prediction id — MUST stay unchanged even
        # though it numerically equals src_id.
        assert item["old_value"] == str(merge_id), (
            "prediction_accept.old_value (prediction id) was incorrectly "
            "rewritten as if it were a keyword id"
        )
    finally:
        db.close()


def test_merge_keyword_into_preserves_preexisting_survivor_tag(tmp_path):
    """When a `keyword_add(src_id)` edit references a photo that already
    carried dst_id at merge time, retargeting the edit onto dst_id would
    let a later undo call untag_photo(dst_id) and strip the user's
    survivor tag — which was never part of that add. `_merge_keyword_into`
    must drop such items so undo iterates only over the items whose photos
    did NOT pre-existingly hold the survivor. Covers the Codex finding on
    the retargeting pass.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        ws = db.ensure_default_workspace()
        db.set_active_workspace(ws)
        fid = db.add_folder("/photos", name="photos")
        pid_had_both = db.add_photo(
            folder_id=fid, filename="both.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        pid_only_src = db.add_photo(
            folder_id=fid, filename="src.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )

        keep_id = db.add_keyword("Robin", kw_type="general")
        merge_id = db.add_keyword("robin variant", kw_type="general")

        # pid_had_both already carries the survivor before the add.
        db.tag_photo(pid_had_both, keep_id)
        # Both photos then get tagged with the variant that will be merged.
        db.tag_photo(pid_had_both, merge_id)
        db.tag_photo(pid_only_src, merge_id)

        # A batch keyword_add edit records the variant tag on both photos.
        eid = db.record_edit(
            "keyword_add", "Added variant to 2 photos", str(merge_id),
            [
                {"photo_id": pid_had_both, "old_value": "",
                 "new_value": str(merge_id)},
                {"photo_id": pid_only_src, "old_value": "",
                 "new_value": str(merge_id)},
            ],
            is_batch=True,
        )

        db._merge_keyword_into(merge_id, keep_id)
        db.conn.commit()

        # The item for pid_had_both must be gone — retargeting it would
        # let undo strip the survivor tag that pre-existed.
        remaining = db.conn.execute(
            "SELECT photo_id, new_value FROM edit_history_items "
            "WHERE edit_id = ? ORDER BY photo_id", (eid,),
        ).fetchall()
        remaining_pids = [r["photo_id"] for r in remaining]
        assert pid_had_both not in remaining_pids, (
            "keyword_add item for a photo that pre-existingly held the "
            "survivor tag should be dropped, not retargeted"
        )
        assert pid_only_src in remaining_pids
        # Surviving item is retargeted onto the survivor id.
        [only_src_item] = [r for r in remaining if r["photo_id"] == pid_only_src]
        assert only_src_item["new_value"] == str(keep_id)

        # End-to-end: undo the edit and confirm the pre-existing survivor
        # tag on pid_had_both is preserved.
        db.undo_last_edit()
        keep_tags_had_both = {
            r["id"] for r in db.get_photo_keywords(pid_had_both)
        }
        assert keep_id in keep_tags_had_both, (
            "undo removed the pre-existing survivor tag from pid_had_both"
        )
        # And pid_only_src, whose add was legitimately retargeted, no
        # longer carries the survivor tag after undo.
        keep_tags_only_src = {
            r["id"] for r in db.get_photo_keywords(pid_only_src)
        }
        assert keep_id not in keep_tags_only_src
    finally:
        db.close()


def test_merge_keyword_into_preserves_preexisting_survivor_for_keyword_remove(tmp_path):
    """A `keyword_remove(src_id)` edit retargeted onto dst_id must not let
    redo strip a pre-existing survivor tag. undo of keyword_remove tags on
    entry.new_value (INSERT OR IGNORE — no-op if dst pre-existed), but
    redo calls untag_photo(pid, entry.new_value); if entry.new_value was
    retargeted to dst_id and pid already carried dst_id, redo removes
    the user's survivor tag.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        ws = db.ensure_default_workspace()
        db.set_active_workspace(ws)
        fid = db.add_folder("/photos", name="photos")
        pid_had_both = db.add_photo(
            folder_id=fid, filename="both.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        pid_only_src = db.add_photo(
            folder_id=fid, filename="src.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        keep_id = db.add_keyword("Robin", kw_type="general")
        merge_id = db.add_keyword("robin variant", kw_type="general")
        db.tag_photo(pid_had_both, keep_id)
        db.tag_photo(pid_had_both, merge_id)
        db.tag_photo(pid_only_src, merge_id)

        # Record a keyword_remove edit for the variant on both photos.
        # keyword_remove convention: item.old_value = str(kid), new_value = ''.
        eid = db.record_edit(
            "keyword_remove", "Removed variant from 2 photos", str(merge_id),
            [
                {"photo_id": pid_had_both, "old_value": str(merge_id),
                 "new_value": ""},
                {"photo_id": pid_only_src, "old_value": str(merge_id),
                 "new_value": ""},
            ],
            is_batch=True,
        )
        # Simulate the untag the original edit performed.
        db.untag_photo(pid_had_both, merge_id)
        db.untag_photo(pid_only_src, merge_id)

        db._merge_keyword_into(merge_id, keep_id)
        db.conn.commit()

        remaining_pids = [
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM edit_history_items WHERE edit_id = ?",
                (eid,),
            ).fetchall()
        ]
        assert pid_had_both not in remaining_pids, (
            "keyword_remove item for a photo that pre-existingly held the "
            "survivor tag should be dropped so redo does not remove it"
        )
        assert pid_only_src in remaining_pids

        # Undo (tags with entry.new_value=dst_id; no-op for pid_had_both
        # because survivor was already there).
        db.undo_last_edit()
        assert keep_id in {
            r["id"] for r in db.get_photo_keywords(pid_had_both)
        }
        # Redo (would untag survivor from pid_had_both without the fix).
        db.redo_last_undo()
        assert keep_id in {
            r["id"] for r in db.get_photo_keywords(pid_had_both)
        }, "redo of keyword_remove stripped the pre-existing survivor tag"
    finally:
        db.close()


def test_merge_keyword_into_preserves_preexisting_survivor_for_prediction_accept(tmp_path):
    """A `prediction_accept(src_id)` edit retargeted onto dst_id must not
    let undo strip a pre-existing survivor tag. prediction_accept shares
    the keyword_add branch in _apply_undo — the untag_photo call would
    remove the survivor. The migration drops such items; prediction-
    status restoration for those specific items is intentionally
    sacrificed to preserve the user's tag (see _merge_keyword_into).
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        ws = db.ensure_default_workspace()
        db.set_active_workspace(ws)
        fid = db.add_folder("/photos", name="photos")
        pid_had_both = db.add_photo(
            folder_id=fid, filename="both.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        pid_only_src = db.add_photo(
            folder_id=fid, filename="src.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        keep_id = db.add_keyword("Robin", kw_type="general")
        merge_id = db.add_keyword("robin variant", kw_type="general")
        db.tag_photo(pid_had_both, keep_id)
        db.tag_photo(pid_had_both, merge_id)
        db.tag_photo(pid_only_src, merge_id)

        # prediction_accept convention:
        # entry.new_value = str(kid), item.old_value = str(pred_id),
        # item.new_value = str(kid).
        eid = db.record_edit(
            "prediction_accept", "Accepted prediction for 2 photos", str(merge_id),
            [
                {"photo_id": pid_had_both, "old_value": "42",
                 "new_value": str(merge_id)},
                {"photo_id": pid_only_src, "old_value": "43",
                 "new_value": str(merge_id)},
            ],
            is_batch=True,
        )

        db._merge_keyword_into(merge_id, keep_id)
        db.conn.commit()

        remaining_pids = [
            r["photo_id"] for r in db.conn.execute(
                "SELECT photo_id FROM edit_history_items WHERE edit_id = ?",
                (eid,),
            ).fetchall()
        ]
        assert pid_had_both not in remaining_pids, (
            "prediction_accept item for a photo that pre-existingly held "
            "the survivor tag should be dropped so undo does not untag it"
        )
        assert pid_only_src in remaining_pids

        db.undo_last_edit()
        assert keep_id in {
            r["id"] for r in db.get_photo_keywords(pid_had_both)
        }, "undo of prediction_accept stripped the pre-existing survivor tag"
    finally:
        db.close()


def test_add_photo_retries_on_database_is_locked(tmp_path):
    """The INSERT inside add_photo must retry transient 'database is locked'.

    Observed in production: a long-running cull held the writer lock for
    minutes; the active scan's next ``add_photo`` call timed out at the
    INSERT (not the commit), aborting the whole pipeline. ``commit_with_retry``
    only protects the commit phase; statement-level retries cover the
    INSERT itself.
    """
    import sqlite3

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")

    real_conn = db.conn
    fail_remaining = {"n": 2}

    class _LockyExecuteConn:
        """Proxy that injects 'database is locked' on the photos INSERT a few
        times. sqlite3.Connection.execute is read-only at the instance level
        so we must wrap the whole connection."""
        def __init__(self, real):
            self._real = real
        def execute(self, sql, params=()):
            if fail_remaining["n"] > 0 and "INSERT OR IGNORE INTO photos" in sql:
                fail_remaining["n"] -= 1
                raise sqlite3.OperationalError("database is locked")
            return self._real.execute(sql, params)
        def __getattr__(self, name):
            return getattr(self._real, name)

    db.conn = _LockyExecuteConn(real_conn)
    try:
        pid = db.add_photo(
            folder_id=fid,
            filename="DSC_0001.NEF",
            extension=".nef",
            file_size=1000,
            file_mtime=1.0,
        )
    finally:
        db.conn = real_conn

    assert pid is not None, "add_photo must succeed after transient lock retries"
    assert fail_remaining["n"] == 0, "the flaky executor should have been hit"
    photo = db.get_photo(pid)
    assert photo["filename"] == "DSC_0001.NEF"


def _add_one_detection(db, photo_id, detector_model="test-det", conf=0.9):
    """Append a single detection without wiping prior rows for the same
    (photo, model). save_detections is destructive per (photo, model), so
    use direct SQL when we want multiple detections on one photo.
    """
    cur = db.conn.execute(
        """INSERT INTO detections
             (photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category)
           VALUES (?, ?, 0.0, 0.0, 1.0, 1.0, ?, 'animal')""",
        (photo_id, detector_model, conf),
    )
    db.conn.commit()
    return cur.lastrowid


def test_count_classifier_runs_filters_by_model_and_fingerprint(tmp_path):
    """count_classifier_runs returns the number of distinct photos in the
    given id list where every qualifying detection has a classifier_runs
    row matching the given (model, fingerprint). Photo-scoped to match
    the classify loop's ``cached`` bucket: a photo is only "cached" when
    no detection would need fresh inference."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")
    p1 = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)
    p2 = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)
    p3 = db.add_photo(folder_id=fid, filename="c.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)

    d1 = _add_one_detection(db, p1)
    d2 = _add_one_detection(db, p2)
    d3 = _add_one_detection(db, p3)

    db.record_classifier_run(d1, "BioCLIP-2.5", "fp-a", prediction_count=1)
    db.record_classifier_run(d2, "BioCLIP-2.5", "fp-b", prediction_count=1)
    db.record_classifier_run(d3, "iNat21", "fp-a", prediction_count=1)

    # Only p1 matches (BioCLIP-2.5 + fp-a).
    assert db.count_classifier_runs(
        [p1, p2, p3], "BioCLIP-2.5", "fp-a"
    ) == 1

    # Empty input returns 0.
    assert db.count_classifier_runs([], "BioCLIP-2.5", "fp-a") == 0

    # Photo with multiple detections, only one cached, does NOT count:
    # the runtime would still need to infer the uncached detection, and the
    # photo would end up in ``count`` (inferred), not ``cached``.
    d2b = _add_one_detection(db, p2)
    db.record_classifier_run(d2b, "BioCLIP-2.5", "fp-a", prediction_count=1)
    assert db.count_classifier_runs(
        [p1, p2, p3], "BioCLIP-2.5", "fp-a"
    ) == 1  # only p1 — p2 still has d2 (fp-b) with no matching run key

    # Once every qualifying detection on p2 has a matching run key, p2
    # counts too.
    db.record_classifier_run(d2, "BioCLIP-2.5", "fp-a", prediction_count=1)
    assert db.count_classifier_runs(
        [p1, p2, p3], "BioCLIP-2.5", "fp-a"
    ) == 2  # p1 and p2


def test_count_classifier_runs_chunks_large_id_lists(tmp_path):
    """count_classifier_runs returns the correct count even when the
    photo id list exceeds SQLITE_MAX_VARIABLE_NUMBER (default 999)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")

    # 1500 photos, each with one detection and one classifier_runs row
    # for (BioCLIP-2.5, fp-a). Above the 999 SQLite variable cap.
    photo_ids = []
    for i in range(1500):
        pid = db.add_photo(
            folder_id=fid, filename=f"p_{i:05d}.jpg", extension=".jpg",
            file_size=1, file_mtime=1.0, timestamp=None,
            width=1, height=1,
        )
        did = _add_one_detection(db, pid)
        db.record_classifier_run(did, "BioCLIP-2.5", "fp-a", prediction_count=1)
        photo_ids.append(pid)

    assert db.count_classifier_runs(
        photo_ids, "BioCLIP-2.5", "fp-a"
    ) == 1500


def test_count_classifier_runs_excludes_full_image_and_below_threshold(tmp_path):
    """count_classifier_runs mirrors the runtime gate: full-image rows and
    sub-threshold detections are excluded so prior full-image classifier
    runs and stale low-confidence boxes don't inflate cached_estimate."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")
    p1 = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)
    p2 = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)
    p3 = db.add_photo(folder_id=fid, filename="c.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)

    # p1: only a full-image detection has a matching run key. Should NOT
    # be counted (runtime gate skips full-image rows when picking primary).
    d1_full = _add_one_detection(db, p1, detector_model="full-image", conf=0.9)
    db.record_classifier_run(d1_full, "BioCLIP-2.5", "fp-a", prediction_count=1)

    # p2: only a below-threshold detection has a matching run key. Should
    # NOT be counted (runtime gate filters at detector_confidence >= 0.2).
    d2_low = _add_one_detection(db, p2, conf=0.05)
    db.record_classifier_run(d2_low, "BioCLIP-2.5", "fp-a", prediction_count=1)

    # p3: a normal above-threshold non-full-image detection has a matching
    # run key. Should be counted.
    d3 = _add_one_detection(db, p3, conf=0.9)
    db.record_classifier_run(d3, "BioCLIP-2.5", "fp-a", prediction_count=1)

    assert db.count_classifier_runs(
        [p1, p2, p3], "BioCLIP-2.5", "fp-a",
    ) == 1


def test_count_classifier_runs_ignores_non_animal_detections(tmp_path):
    """Non-animal detector boxes (person, vehicle) are skipped by the
    classify loop before inference — so an uncached non-animal box on an
    otherwise cache-served photo must not force that photo out of the
    ``cached`` bucket. Mirrors pipeline_job.py's ``category == 'animal'``
    filter on both the cache- and DB-read detection paths.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")
    p1 = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)
    p2 = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)

    # p1: one animal detection (cached) + one uncached person detection.
    # The person box gets skipped at runtime so p1 IS fully cache-served.
    d1_animal = _add_one_detection(db, p1, conf=0.9)
    db.record_classifier_run(d1_animal, "BioCLIP-2.5", "fp-a", prediction_count=1)
    db.conn.execute(
        """INSERT INTO detections
             (photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category)
           VALUES (?, 'test-det', 0.0, 0.0, 1.0, 1.0, 0.9, 'person')""",
        (p1,),
    )
    db.conn.commit()

    # p2: only a person detection has a matching run key. Person boxes
    # aren't classifier targets, so p2 has zero classifiable detections —
    # it falls through to the empty-detections/full-image branch and is
    # not counted here.
    cur = db.conn.execute(
        """INSERT INTO detections
             (photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category)
           VALUES (?, 'test-det', 0.0, 0.0, 1.0, 1.0, 0.9, 'vehicle')""",
        (p2,),
    )
    db.conn.commit()
    db.record_classifier_run(cur.lastrowid, "BioCLIP-2.5", "fp-a", prediction_count=1)

    assert db.count_classifier_runs(
        [p1, p2], "BioCLIP-2.5", "fp-a",
    ) == 1


def test_all_nav_ids_covers_every_page():
    from db import ALL_NAV_IDS
    expected = {
        "import",
        "pipeline", "jobs", "pipeline_review", "pipeline_rapid_review", "review", "cull",
        "misses", "highlights", "life_list", "browse", "edit", "map", "location_review", "variants",
        "dashboard", "storage", "audit", "move", "compare",
        "settings", "workspace", "lightroom", "shortcuts",
        "keywords", "duplicates", "logs",
    }
    assert expected == ALL_NAV_IDS


def test_default_tabs_are_direct_navigation():
    from db import DEFAULT_TABS
    assert DEFAULT_TABS == [
        "import", "browse", "pipeline", "pipeline_review",
        "review", "cull", "jobs", "highlights", "misses", "storage", "settings",
    ]


def test_photo_masks_table_exists(tmp_path):
    """photo_masks table must exist on a fresh DB."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    cols = {row[1] for row in db.conn.execute(
        "PRAGMA table_info(photo_masks)"
    ).fetchall()}
    assert {
        "photo_id", "variant", "path", "created_at",
        "detector_model", "prompt_x", "prompt_y", "prompt_w", "prompt_h",
        "subject_size", "subject_tenengrad", "bg_tenengrad", "crop_complete",
    } <= cols


def test_photo_masks_pk_is_photo_and_variant(tmp_path):
    """(photo_id, variant) is the primary key — same variant twice fails."""
    import sqlite3

    import pytest
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (1, 'sam2-small', '/p1', 0, 'unknown', -1, -1, -1, -1)"
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.conn.execute(
            "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
            "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
            "VALUES (1, 'sam2-small', '/p2', 1, 'unknown', -1, -1, -1, -1)"
        )


def test_photos_has_active_mask_variant_column(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("SELECT active_mask_variant FROM photos LIMIT 0")


def test_upsert_photo_mask_inserts_and_replaces(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )

    db.upsert_photo_mask(
        photo_id=1, variant="sam2-small", path="/m/1.sam2-small.png",
        detector_model="megadetector-v6",
        prompt_x=10, prompt_y=20, prompt_w=100, prompt_h=200,
        subject_size=20000, subject_tenengrad=1.5,
        bg_tenengrad=0.3, crop_complete=1.0,
    )
    row = db.conn.execute(
        "SELECT path, prompt_x FROM photo_masks WHERE photo_id=1 AND variant='sam2-small'"
    ).fetchone()
    assert row["path"] == "/m/1.sam2-small.png"
    assert row["prompt_x"] == 10

    # Re-upsert with new prompt — row replaced
    db.upsert_photo_mask(
        photo_id=1, variant="sam2-small", path="/m/1.sam2-small.png",
        detector_model="megadetector-v6",
        prompt_x=11, prompt_y=20, prompt_w=100, prompt_h=200,
        subject_size=21000, subject_tenengrad=1.5,
        bg_tenengrad=0.3, crop_complete=1.0,
    )
    row = db.conn.execute(
        "SELECT prompt_x, subject_size FROM photo_masks WHERE photo_id=1 AND variant='sam2-small'"
    ).fetchone()
    assert row["prompt_x"] == 11
    assert row["subject_size"] == 21000
    # Still exactly one row for this (photo, variant)
    n = db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks WHERE photo_id=1 AND variant='sam2-small'"
    ).fetchone()[0]
    assert n == 1


def test_get_photo_mask_returns_row_or_none(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    assert db.get_photo_mask(1, "sam2-small") is None

    db.upsert_photo_mask(
        photo_id=1, variant="sam2-small", path="/p", detector_model="md",
        prompt_x=1, prompt_y=2, prompt_w=3, prompt_h=4,
    )
    m = db.get_photo_mask(1, "sam2-small")
    assert m["path"] == "/p"
    assert m["detector_model"] == "md"
    assert m["prompt_x"] == 1


def test_list_masks_for_photo(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.upsert_photo_mask(
        photo_id=1, variant="sam2-small", path="/a",
        detector_model="md", prompt_x=1, prompt_y=2, prompt_w=3, prompt_h=4,
    )
    db.upsert_photo_mask(
        photo_id=1, variant="sam2-large", path="/b",
        detector_model="md", prompt_x=1, prompt_y=2, prompt_w=3, prompt_h=4,
    )
    variants = sorted(m["variant"] for m in db.list_masks_for_photo(1))
    assert variants == ["sam2-large", "sam2-small"]


def test_set_active_mask_variant_denormalizes(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.upsert_photo_mask(
        photo_id=1, variant="sam2-large", path="/m/1.sam2-large.png",
        detector_model="md", prompt_x=1, prompt_y=2, prompt_w=3, prompt_h=4,
        subject_size=12345, subject_tenengrad=2.0,
        bg_tenengrad=0.5, crop_complete=0.9,
    )
    db.set_active_mask_variant(1, "sam2-large")
    row = db.conn.execute(
        "SELECT mask_path, active_mask_variant, subject_size, "
        "subject_tenengrad, bg_tenengrad, crop_complete FROM photos WHERE id=1"
    ).fetchone()
    assert row["mask_path"] == "/m/1.sam2-large.png"
    assert row["active_mask_variant"] == "sam2-large"
    assert row["subject_size"] == 12345
    assert row["subject_tenengrad"] == 2.0


def test_set_active_mask_variant_missing_row_raises(tmp_path):
    import pytest
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    with pytest.raises(ValueError):
        db.set_active_mask_variant(1, "sam3-small")


def test_delete_masks_for_variant_removes_files_and_rows(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (2, 1, 'b.jpg')"
    )

    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    p1 = masks_dir / "1.sam2-small.png"
    p1.write_bytes(b"x")
    p2 = masks_dir / "2.sam2-small.png"
    p2.write_bytes(b"y")
    db.upsert_photo_mask(
        1, "sam2-small", str(p1),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    db.upsert_photo_mask(
        2, "sam2-small", str(p2),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )

    deleted = db.delete_masks_for_variant("sam2-small")
    assert deleted == 2
    assert not p1.exists() and not p2.exists()
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks WHERE variant='sam2-small'"
    ).fetchone()[0] == 0


def test_delete_masks_for_variant_refuses_active(tmp_path):
    import pytest
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    p = masks_dir / "1.sam2-small.png"
    p.write_bytes(b"x")
    db.upsert_photo_mask(
        1, "sam2-small", str(p),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    db.set_active_mask_variant(1, "sam2-small")
    with pytest.raises(ValueError):
        db.delete_masks_for_variant("sam2-small")


def test_delete_inactive_masks(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    pa = masks_dir / "1.sam2-small.png"
    pa.write_bytes(b"a")
    pb = masks_dir / "1.sam2-large.png"
    pb.write_bytes(b"b")
    db.upsert_photo_mask(
        1, "sam2-small", str(pa),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    db.upsert_photo_mask(
        1, "sam2-large", str(pb),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    db.set_active_mask_variant(1, "sam2-large")
    n = db.delete_inactive_masks()
    assert n == 1
    assert not pa.exists()
    assert pb.exists()
    remaining = {m["variant"] for m in db.list_masks_for_photo(1)}
    assert remaining == {"sam2-large"}


def test_delete_inactive_masks_skips_photos_with_no_active(tmp_path):
    """Photos whose active_mask_variant IS NULL must not lose their
    masks to delete-inactive: that's the partial-state case where a
    prior pipeline crashed between upsert_photo_mask and
    set_active_mask_variant. The user has to promote a variant to
    active first before the cleanup will touch the photo."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    p = masks_dir / "1.sam2-small.png"
    p.write_bytes(b"x")
    db.upsert_photo_mask(
        1, "sam2-small", str(p),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    # Note: NO set_active_mask_variant call — leaves active NULL.
    n = db.delete_inactive_masks()
    assert n == 0
    assert p.exists()
    assert {m["variant"] for m in db.list_masks_for_photo(1)} == {"sam2-small"}


def test_find_stale_masks(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 10, 20, 100, 200, 0.9, 'animal')"
    )
    # Mask was made from the same prompt → not stale
    db.upsert_photo_mask(
        1, "sam2-small", "/p",
        detector_model="megadetector-v6",
        prompt_x=10, prompt_y=20, prompt_w=100, prompt_h=200,
    )
    assert db.find_stale_masks() == []

    # Insert a mask whose prompt no longer matches the current detection
    db.upsert_photo_mask(
        1, "sam2-large", "/q",
        detector_model="megadetector-v6",
        prompt_x=99, prompt_y=20, prompt_w=100, prompt_h=200,
    )
    stale = db.find_stale_masks()
    assert {(s["photo_id"], s["variant"]) for s in stale} == {(1, "sam2-large")}


def test_find_stale_masks_compares_against_primary_detection_only(tmp_path):
    """Mask validity hangs off the *primary* detection — the
    highest-confidence non-``full-image`` row. A leftover secondary
    box (or a row from another retained detector model) that still
    matches the mask's stored prompt must NOT keep the mask out of
    the stale set; otherwise stale cache entries linger after
    detector/model changes.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    # The current primary (highest-confidence non-full-image) is the
    # 0.95 box at (200, 200, 50, 50). The 0.30 row at the OLD prompt
    # coordinates is a leftover secondary that should NOT save the
    # cached mask from being marked stale.
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 200, 200, 50, 50, 0.95, 'animal')"
    )
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 10, 20, 100, 200, 0.30, 'animal')"
    )
    db.upsert_photo_mask(
        1, "sam2-small", "/p",
        detector_model="megadetector-v6",
        prompt_x=10, prompt_y=20, prompt_w=100, prompt_h=200,
    )
    stale = db.find_stale_masks()
    assert {(s["photo_id"], s["variant"]) for s in stale} == {
        (1, "sam2-small")
    }, (
        "mask whose prompt only matches a secondary detection must be stale"
    )

    # Sanity: the mask for the current primary's prompt is still fresh.
    db.upsert_photo_mask(
        1, "sam2-large", "/q",
        detector_model="megadetector-v6",
        prompt_x=200, prompt_y=200, prompt_w=50, prompt_h=50,
    )
    stale = db.find_stale_masks()
    assert {(s["photo_id"], s["variant"]) for s in stale} == {
        (1, "sam2-small")
    }


def test_find_stale_masks_preserves_real_precision_bbox(tmp_path):
    """detections.box_* are normalized REAL values in [0, 1].  An older
    revision int()-truncated those to populate prompt_*, which collapsed
    every prompt to (0, 0, 0, 0) and meant the staleness query matched
    any cached mask against any current detection.  A REAL prompt that
    matches the current primary detection must be fresh; a REAL prompt
    that doesn't match must be stale.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    # Normalized bbox (x, y, w, h) — typical detector output.
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 0.123, 0.456, 0.300, 0.400, 0.9, 'animal')"
    )
    # Mask was made from the same REAL prompt → not stale.
    db.upsert_photo_mask(
        1, "sam2-small", "/p",
        detector_model="megadetector-v6",
        prompt_x=0.123, prompt_y=0.456, prompt_w=0.300, prompt_h=0.400,
    )
    assert db.find_stale_masks() == []

    # Mask whose prompt matches what the prior int() truncation would
    # have written. The actual detection has moved (any normalized
    # value), so this mask must be stale.
    db.upsert_photo_mask(
        1, "sam2-large", "/q",
        detector_model="megadetector-v6",
        prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    stale = db.find_stale_masks()
    assert {(s["photo_id"], s["variant"]) for s in stale} == {
        (1, "sam2-large")
    }


def test_find_stale_masks_applies_detector_confidence_floor(tmp_path):
    """The staleness check has to honor the workspace's
    ``detector_confidence`` floor: detections below the floor are
    invisible to extraction (it skips them), so a cached mask whose
    prompt only matches a below-floor box must be marked stale even
    though its coordinates technically still appear in ``detections``.
    Otherwise raising the floor leaves stale masks active and reused.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    # Photo 1's only non-full-image detection is below the new floor.
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 0.10, 0.20, 0.30, 0.40, 0.15, 'animal')"
    )
    db.upsert_photo_mask(
        1, "sam2-small", "/p",
        detector_model="megadetector-v6",
        prompt_x=0.10, prompt_y=0.20, prompt_w=0.30, prompt_h=0.40,
    )
    # Photo 2 has a fresh, above-floor detection that matches its mask.
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (2, 1, 'b.jpg')"
    )
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (2, 'megadetector-v6', 0.50, 0.50, 0.20, 0.20, 0.90, 'animal')"
    )
    db.upsert_photo_mask(
        2, "sam2-small", "/q",
        detector_model="megadetector-v6",
        prompt_x=0.50, prompt_y=0.50, prompt_w=0.20, prompt_h=0.20,
    )

    # No floor: photo 1's mask matches its (low-confidence) detection,
    # so neither mask is stale. Preserves prior behavior for callers
    # that don't pass a threshold.
    assert db.find_stale_masks() == []

    # Floor at 0.5: photo 1's only detection (0.15) is invisible, so
    # there's no primary detection for the mask to match against and
    # the mask is stale. Photo 2's mask matches its 0.90 detection and
    # stays fresh.
    stale = db.find_stale_masks(detector_confidence=0.5)
    assert {(s["photo_id"], s["variant"]) for s in stale} == {
        (1, "sam2-small")
    }


def test_find_stale_masks_floor_drops_below_threshold_match(tmp_path):
    """A cached mask whose prompt only matches a below-floor detection
    (and the photo has no above-floor detections at all) must be stale
    once the floor is applied — extraction wouldn't run for that photo
    so the mask can never be regenerated.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    # Two below-floor detections; the cached mask's prompt matches the
    # higher-confidence one. Without a floor it'd be the "primary" and
    # the mask would look fresh; with the floor it disappears.
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 0.10, 0.10, 0.10, 0.10, 0.40, 'animal')"
    )
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 0.20, 0.20, 0.10, 0.10, 0.30, 'animal')"
    )
    db.upsert_photo_mask(
        1, "sam2-small", "/p",
        detector_model="megadetector-v6",
        prompt_x=0.10, prompt_y=0.10, prompt_w=0.10, prompt_h=0.10,
    )
    # Without floor: the 0.40 box is the primary, its prompt equals
    # the cached one → fresh.
    assert db.find_stale_masks() == []
    # With floor 0.5: nothing visible, cached mask is stale.
    stale = db.find_stale_masks(detector_confidence=0.5)
    assert {(s["photo_id"], s["variant"]) for s in stale} == {
        (1, "sam2-small")
    }


def test_delete_stale_masks_honors_detector_confidence(tmp_path):
    """``delete_stale_masks`` has to forward the threshold so the
    deleted set matches the count surfaced on the storage card. If it
    didn't, the user would press *Delete stale* and end up with a
    non-zero leftover.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 0.10, 0.10, 0.10, 0.10, 0.20, 'animal')"
    )
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    p = masks_dir / "1.sam2-small.png"
    p.write_bytes(b"x")
    db.upsert_photo_mask(
        1, "sam2-small", str(p),
        detector_model="megadetector-v6",
        prompt_x=0.10, prompt_y=0.10, prompt_w=0.10, prompt_h=0.10,
    )
    # Without a floor the (matching, low-confidence) detection keeps
    # the mask fresh.
    assert db.delete_stale_masks() == 0
    assert p.exists()
    # With a floor above the detection's confidence the mask becomes
    # stale and gets removed.
    assert db.delete_stale_masks(detector_confidence=0.5) == 1
    assert not p.exists()


def test_min_detector_confidence_no_overrides(tmp_path):
    """With no per-workspace overrides, the cross-workspace minimum is
    just the global default. The Default workspace auto-created by
    Database.__init__ has no config_overrides, so this is the
    out-of-the-box state.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    assert db.min_detector_confidence_across_workspaces(
        {"detector_confidence": 0.3}
    ) == 0.3
    # Falls back to 0.2 if the global config doesn't define it.
    assert db.min_detector_confidence_across_workspaces({}) == 0.2


def test_min_detector_confidence_picks_lowest_override(tmp_path):
    """The global storage view must use the most permissive floor
    across workspaces. If one workspace overrides
    detector_confidence=0.1 and another sticks with 0.5, the global
    minimum is 0.1 — masks valid under 0.1 must not be deleted just
    because the active workspace happens to be the strict one.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    permissive = db.create_workspace(
        "permissive", config_overrides={"detector_confidence": 0.1}
    )
    strict = db.create_workspace(
        "strict", config_overrides={"detector_confidence": 0.5}
    )
    # Active workspace shouldn't matter — try both.
    db.set_active_workspace(strict)
    assert db.min_detector_confidence_across_workspaces(
        {"detector_confidence": 0.3}
    ) == 0.1
    db.set_active_workspace(permissive)
    assert db.min_detector_confidence_across_workspaces(
        {"detector_confidence": 0.3}
    ) == 0.1


def test_min_detector_confidence_includes_unoverridden_workspaces(tmp_path):
    """A workspace without an override still counts at the global
    default. If global=0.2 and one workspace overrides up to 0.5 but
    others don't override at all, the cross-workspace min is 0.2 (the
    unoverridden workspaces' effective value), not 0.5.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.create_workspace(
        "strict", config_overrides={"detector_confidence": 0.5}
    )
    # The auto-created Default workspace has no overrides → uses 0.2.
    assert db.min_detector_confidence_across_workspaces(
        {"detector_confidence": 0.2}
    ) == 0.2


def test_legacy_mask_backfill_is_resumable(tmp_path):
    """If startup crashes after backfilling some legacy mask rows but
    before completing, the next startup must finish the rest.  The
    earlier outer ``if total_unknown_rows == 0`` guard caused the
    remaining photos to be skipped forever, leaving orphan mask_path
    values that the variant-aware APIs and cleanup logic couldn't see.
    """
    from db import Database
    db_path = str(tmp_path / "v.db")
    db = Database(db_path)
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename, mask_path) "
        "VALUES (1, 1, 'a.jpg', '/m/1.png')"
    )
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename, mask_path) "
        "VALUES (2, 1, 'b.jpg', '/m/2.png')"
    )
    db.conn.commit()
    db.close()

    # First open: everything backfills.
    db = Database(db_path)
    backfilled = {
        r[0] for r in db.conn.execute(
            "SELECT photo_id FROM photo_masks WHERE variant='unknown'"
        )
    }
    assert backfilled == {1, 2}

    # Simulate a partial crash: one of the unknown rows was inserted
    # but the other never made it (rolled back, killed mid-loop, etc.).
    db.conn.execute(
        "DELETE FROM photo_masks WHERE photo_id=2 AND variant='unknown'"
    )
    db.conn.execute(
        "UPDATE photos SET active_mask_variant=NULL WHERE id=2"
    )
    db.conn.commit()
    db.close()

    # Next startup must finish the missing photo, not stop just because
    # *some* unknown rows already exist.
    db = Database(db_path)
    backfilled = {
        r[0] for r in db.conn.execute(
            "SELECT photo_id FROM photo_masks WHERE variant='unknown'"
        )
    }
    assert backfilled == {1, 2}, (
        "second startup must re-finish the legacy mask backfill for "
        "photos missing photo_masks rows"
    )
    active = {
        r[0]: r[1] for r in db.conn.execute(
            "SELECT id, active_mask_variant FROM photos"
        )
    }
    assert active == {1: "unknown", 2: "unknown"}


def test_delete_stale_masks(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 10, 20, 100, 200, 0.9, 'animal')"
    )
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    fresh = masks_dir / "1.sam2-small.png"
    fresh.write_bytes(b"f")
    stale_path = masks_dir / "1.sam2-large.png"
    stale_path.write_bytes(b"s")
    db.upsert_photo_mask(
        1, "sam2-small", str(fresh),
        detector_model="megadetector-v6",
        prompt_x=10, prompt_y=20, prompt_w=100, prompt_h=200,
    )
    db.upsert_photo_mask(
        1, "sam2-large", str(stale_path),
        detector_model="megadetector-v6",
        prompt_x=99, prompt_y=20, prompt_w=100, prompt_h=200,
    )
    deleted = db.delete_stale_masks()
    assert deleted == 1
    assert fresh.exists()
    assert not stale_path.exists()
    assert {m["variant"] for m in db.list_masks_for_photo(1)} == {"sam2-small"}


def test_find_stale_masks_breaks_primary_ties_deterministically(tmp_path):
    """When two non-full-image detections share the maximum confidence,
    extraction (``ORDER BY detector_confidence DESC, id ASC``) picks the
    smaller-id row as the primary. ``find_stale_masks`` has to agree —
    otherwise a mask whose prompt matches the *other* tied row could be
    treated as fresh while extraction is regenerating from the chosen
    primary, leaving stale cache entries that drift between the two
    paths.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    # Two detections tied on detector_confidence. Insertion order makes
    # det id=1 the deterministic primary (smaller id wins on tie).
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 0.10, 0.10, 0.10, 0.10, 0.80, 'animal')"
    )
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 0.20, 0.20, 0.20, 0.20, 0.80, 'animal')"
    )

    # Mask matching the LATER tied row (the one extraction does NOT
    # pick) must be stale: extraction would not regenerate from it.
    db.upsert_photo_mask(
        1, "sam2-small", "/p",
        detector_model="megadetector-v6",
        prompt_x=0.20, prompt_y=0.20, prompt_w=0.20, prompt_h=0.20,
    )
    stale = db.find_stale_masks()
    assert {(s["photo_id"], s["variant"]) for s in stale} == {
        (1, "sam2-small")
    }

    # Mask matching the FIRST tied row (the deterministic primary) is
    # fresh.
    db.upsert_photo_mask(
        1, "sam2-large", "/q",
        detector_model="megadetector-v6",
        prompt_x=0.10, prompt_y=0.10, prompt_w=0.10, prompt_h=0.10,
    )
    stale = db.find_stale_masks()
    assert {(s["photo_id"], s["variant"]) for s in stale} == {
        (1, "sam2-small")
    }


def test_delete_masks_refuses_paths_outside_masks_dir(tmp_path):
    """``delete_masks_for_variant`` / ``delete_inactive_masks`` /
    ``delete_stale_masks`` feed ``photo_masks.path`` straight into
    ``os.remove``. A corrupted or migrated row pointing outside the
    masks directory must not let the user-triggerable storage cleanup
    endpoints unlink arbitrary files. Mirrors the realpath containment
    already enforced by ``/api/masks/<pid>/<variant>.png``.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()

    # File outside the masks directory — must be untouched.
    outside = tmp_path / "evil.png"
    outside.write_bytes(b"important")
    db.upsert_photo_mask(
        1, "sam2-small", str(outside),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )

    db.delete_masks_for_variant("sam2-small")
    assert outside.exists(), "file outside masks dir was deleted"

    # Same protection on delete_inactive_masks.
    db.upsert_photo_mask(
        1, "sam2-small", str(outside),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    inside = masks_dir / "1.sam2-large.png"
    inside.write_bytes(b"in")
    db.upsert_photo_mask(
        1, "sam2-large", str(inside),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    db.set_active_mask_variant(1, "sam2-large")
    db.delete_inactive_masks()
    assert outside.exists(), "delete_inactive_masks unlinked outside path"
    # Active variant inside masks dir is preserved.
    assert inside.exists()

    # And on delete_stale_masks: a stale row pointing outside is
    # unlinked from the DB but the file is left alone.
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'md', 9, 9, 9, 9, 0.9, 'animal')"
    )
    stale_outside = tmp_path / "stale.png"
    stale_outside.write_bytes(b"stale")
    db.upsert_photo_mask(
        1, "sam2-small", str(stale_outside),
        detector_model="md", prompt_x=1, prompt_y=1, prompt_w=1, prompt_h=1,
    )
    db.delete_stale_masks()
    assert stale_outside.exists(), "delete_stale_masks unlinked outside path"


def test_mask_variants_summary(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    for pid in (1, 2, 3):
        db.conn.execute(
            "INSERT INTO photos(id, folder_id, filename) VALUES (?, 1, ?)",
            (pid, f"p{pid}.jpg"),
        )
    md = tmp_path / "masks"
    md.mkdir()
    for pid, var, size in [
        (1, "sam2-small", 100),
        (2, "sam2-small", 200),
        (1, "sam2-large", 500),
        (3, "sam3-small", 700),
    ]:
        p = md / f"{pid}.{var}.png"
        p.write_bytes(b"x" * size)
        db.upsert_photo_mask(
            pid, var, str(p),
            detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
        )
    db.set_active_mask_variant(1, "sam2-large")

    summary = {s["variant"]: s for s in db.mask_variants_summary()}
    assert summary["sam2-small"]["count"] == 2
    assert summary["sam2-small"]["bytes"] == 300
    assert summary["sam2-large"]["count"] == 1
    assert summary["sam2-large"]["active_count"] == 1
    assert summary["sam3-small"]["active_count"] == 0


def test_mask_variant_coverage_is_workspace_scoped(tmp_path):
    """mask_variant_coverage returns per-variant counts of distinct photos
    in the active workspace that have a row for that variant. Photos
    outside the workspace are excluded even though photo_masks is global.
    """
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    ws_in = db.create_workspace("In")
    ws_out = db.create_workspace("Out")

    f_in = db.add_folder("/in", name="in")
    f_out = db.add_folder("/out", name="out")
    db.add_workspace_folder(ws_in, f_in)
    db.add_workspace_folder(ws_out, f_out)

    p1 = db.add_photo(folder_id=f_in, filename="a.jpg", extension=".jpg",
                      file_size=1, file_mtime=1.0)
    p2 = db.add_photo(folder_id=f_in, filename="b.jpg", extension=".jpg",
                      file_size=1, file_mtime=1.0)
    p3 = db.add_photo(folder_id=f_out, filename="c.jpg", extension=".jpg",
                      file_size=1, file_mtime=1.0)

    db.upsert_photo_mask(p1, "sam2-small", "/p/a.small.png",
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)
    db.upsert_photo_mask(p1, "sam2-large", "/p/a.large.png",
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)
    db.upsert_photo_mask(p2, "sam2-small", "/p/b.small.png",
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)
    # p3 lives outside ws_in — its sam2-large row must NOT be counted.
    db.upsert_photo_mask(p3, "sam2-large", "/p/c.large.png",
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)

    db.set_active_mask_variant(p1, "sam2-large")

    db.set_active_workspace(ws_in)
    cov = {c["variant"]: c for c in db.mask_variant_coverage()}
    assert cov["sam2-small"]["count"] == 2
    assert cov["sam2-small"]["active_count"] == 0
    assert cov["sam2-large"]["count"] == 1  # p3 excluded
    assert cov["sam2-large"]["active_count"] == 1

    db.set_active_workspace(ws_out)
    cov_out = {c["variant"]: c for c in db.mask_variant_coverage()}
    assert cov_out["sam2-large"]["count"] == 1
    assert "sam2-small" not in cov_out


def test_existing_masks_migrate_to_unknown_variant(tmp_path):
    """A photos row with mask_path set on a pre-migration DB gets a
    photo_masks row with variant='unknown' and prompt=-1."""
    import sqlite3
    db_path = tmp_path / "v.db"

    # Build a DB with the old shape, no photo_masks table. Include the
    # columns CREATE INDEX statements reference (timestamp, file_hash) so
    # _create_tables doesn't fail on a freshly-empty schema.
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT)")
    conn.execute(
        "CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER, "
        "filename TEXT, timestamp TEXT, file_hash TEXT, rating INTEGER, "
        "mask_path TEXT, subject_size REAL, subject_tenengrad REAL, "
        "bg_tenengrad REAL, crop_complete REAL)"
    )
    conn.execute("INSERT INTO folders(id, path) VALUES (1, '/tmp')")
    conn.execute(
        "INSERT INTO photos(id, folder_id, filename, mask_path, "
        "subject_tenengrad, crop_complete) "
        "VALUES (1, 1, 'a.jpg', '/m/1.png', 1.5, 0.9)"
    )
    # Photo without a mask_path — should NOT get a photo_masks row.
    conn.execute(
        "INSERT INTO photos(id, folder_id, filename) "
        "VALUES (2, 1, 'b.jpg')"
    )
    conn.commit()
    conn.close()

    from db import Database
    db = Database(str(db_path))

    row = db.conn.execute(
        "SELECT * FROM photo_masks WHERE photo_id=1"
    ).fetchone()
    assert row is not None
    assert row["variant"] == "unknown"
    assert row["detector_model"] == "unknown"
    assert row["prompt_x"] == -1
    assert row["prompt_y"] == -1
    assert row["prompt_w"] == -1
    assert row["prompt_h"] == -1
    assert row["path"] == "/m/1.png"
    assert row["subject_tenengrad"] == 1.5
    assert row["crop_complete"] == 0.9
    # And photos.active_mask_variant is set
    av = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=1"
    ).fetchone()[0]
    assert av == "unknown"

    # Photo without mask_path: no photo_masks row, no active_mask_variant.
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks WHERE photo_id=2"
    ).fetchone()[0] == 0
    av2 = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=2"
    ).fetchone()[0]
    assert av2 is None


def test_mask_migration_is_idempotent(tmp_path):
    """Re-opening the DB does not re-insert migrated 'unknown' rows."""
    import sqlite3
    db_path = tmp_path / "v.db"

    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT)")
    conn.execute(
        "CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER, "
        "filename TEXT, timestamp TEXT, file_hash TEXT, rating INTEGER, "
        "mask_path TEXT, subject_size REAL, subject_tenengrad REAL, "
        "bg_tenengrad REAL, crop_complete REAL)"
    )
    conn.execute("INSERT INTO folders(id, path) VALUES (1, '/tmp')")
    conn.execute(
        "INSERT INTO photos(id, folder_id, filename, mask_path) "
        "VALUES (1, 1, 'a.jpg', '/m/1.png')"
    )
    conn.commit()
    conn.close()

    from db import Database
    db = Database(str(db_path))
    db.conn.close()

    # Re-open — migration must not insert a second row.
    db2 = Database(str(db_path))
    n = db2.conn.execute(
        "SELECT COUNT(*) FROM photo_masks WHERE photo_id=1"
    ).fetchone()[0]
    assert n == 1


def test_photo_masks_subject_size_is_real(tmp_path):
    """Fresh DBs declare photo_masks.subject_size as REAL since the
    feature stores a fraction in [0, 1]."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    cols = {row[1]: row[2] for row in db.conn.execute(
        "PRAGMA table_info(photo_masks)"
    ).fetchall()}
    assert cols["subject_size"] == "REAL"


# -- Pipeline-status-makeover Phase 1: fingerprint columns + writes -----------
#
# These tests cover the per-stage staleness columns added by the
# pipeline-status-makeover (see docs/plans/2026-05-01-pipeline-status-makeover-design.md).
# The aggregate `pipeline_stage_counts()` helper that originally lived here
# was dropped after PR #745 landed `vireo/pipeline_plan.py` covering the same
# territory; only the staleness primitives remain.

def test_photos_has_eye_kp_fingerprint_column(tmp_path):
    """photos.eye_kp_fingerprint must exist on a fresh DB."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("SELECT eye_kp_fingerprint FROM photos LIMIT 0")


def test_photos_eye_kp_fingerprint_migrates_on_old_db(tmp_path):
    """Opening a DB without the column adds it (idempotent migration)."""
    from db import Database
    p = str(tmp_path / "v.db")
    db = Database(p)
    db.conn.execute("ALTER TABLE photos DROP COLUMN eye_kp_fingerprint")
    db.conn.commit()
    db.close()
    db2 = Database(p)
    db2.conn.execute("SELECT eye_kp_fingerprint FROM photos LIMIT 0")


def test_eye_kp_fingerprint_backfilled_on_migration(tmp_path):
    """One-shot backfill: photos with eye_tenengrad NOT NULL get the
    current fingerprint; rows without eye data stay NULL. Repeated DB
    opens don't re-stamp."""
    from db import Database
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    p = str(tmp_path / "v.db")
    db = Database(p)
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename, eye_tenengrad, "
        "eye_kp_fingerprint) "
        "VALUES (1, 1, 'a.jpg', 12.5, NULL), (2, 1, 'b.jpg', NULL, NULL)"
    )
    # Wipe the migration marker so reopening reruns the backfill once.
    db.conn.execute("DELETE FROM db_meta WHERE key='eye_kp_fingerprint_backfill'")
    db.conn.commit()
    db.close()
    db2 = Database(p)
    rows = {r["id"]: r["eye_kp_fingerprint"]
            for r in db2.conn.execute(
                "SELECT id, eye_kp_fingerprint FROM photos ORDER BY id"
            ).fetchall()}
    assert rows[1] == EYE_KP_FINGERPRINT_VERSION
    assert rows[2] is None
    # Marker now set; reopening must NOT touch already-populated rows.
    db2.conn.execute(
        "UPDATE photos SET eye_kp_fingerprint = 'mutated' WHERE id = 1"
    )
    db2.conn.commit()
    db2.close()
    db3 = Database(p)
    after = db3.conn.execute(
        "SELECT eye_kp_fingerprint FROM photos WHERE id = 1"
    ).fetchone()[0]
    assert after == "mutated"  # backfill skipped on second open


def test_update_photo_pipeline_features_stamps_eye_kp_fingerprint(tmp_path):
    """Passing eye_kp_fingerprint to update_photo_pipeline_features writes it."""
    from db import Database
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.conn.commit()
    db.update_photo_pipeline_features(
        1, eye_x=0.5, eye_y=0.5, eye_conf=0.9, eye_tenengrad=12.0,
        eye_kp_fingerprint=EYE_KP_FINGERPRINT_VERSION,
    )
    fp = db.conn.execute(
        "SELECT eye_kp_fingerprint FROM photos WHERE id=1"
    ).fetchone()[0]
    assert fp == EYE_KP_FINGERPRINT_VERSION


def test_update_photo_pipeline_features_skips_eye_kp_fingerprint_when_unset(tmp_path):
    """If eye_kp_fingerprint is not passed, it stays NULL (doesn't get clobbered)."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename, eye_kp_fingerprint) "
        "VALUES (1, 1, 'a.jpg', 'preexisting')"
    )
    db.conn.commit()
    db.update_photo_pipeline_features(1, eye_x=0.5, eye_y=0.5)
    fp = db.conn.execute(
        "SELECT eye_kp_fingerprint FROM photos WHERE id=1"
    ).fetchone()[0]
    assert fp == "preexisting"


def test_workspaces_has_group_state_columns(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces LIMIT 0"
    )


def test_set_workspace_group_state(tmp_path):
    """set_workspace_group_state writes both columns atomically."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    ws_id = db._active_workspace_id
    assert ws_id is not None
    db.set_workspace_group_state(ws_id, fingerprint="abc123", when_ts=1714579200)
    row = db.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_grouped_at"] == 1714579200
    assert row["last_group_fingerprint"] == "abc123"


def test_set_workspace_group_state_overwrites(tmp_path):
    """Calling set_workspace_group_state again replaces both values."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    ws_id = db._active_workspace_id
    db.set_workspace_group_state(ws_id, fingerprint="old", when_ts=1)
    db.set_workspace_group_state(ws_id, fingerprint="new", when_ts=2)
    row = db.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_grouped_at"] == 2
    assert row["last_group_fingerprint"] == "new"


def test_get_workspace_extensions_returns_distinct_lowercased(tmp_path):
    """Extensions are returned distinct, lowercased, sorted, scoped to ws."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='b.JPG', extension='.JPG',
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='c.nef', extension='.nef',
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='d.cr2', extension='.cr2',
                 file_size=1, file_mtime=1.0)

    exts = db.get_workspace_extensions()
    # Lowercased (so .jpg and .JPG collapse), distinct, sorted.
    assert exts == ['.cr2', '.jpg', '.nef']


def test_get_workspace_extensions_scoped_to_active_workspace(tmp_path):
    """Extensions from another workspace's folders must not leak in."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    default_ws = db._active_workspace_id
    f_default = db.add_folder('/a', name='a')
    db.add_photo(folder_id=f_default, filename='x.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)

    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    f_other = db.add_folder('/b', name='b')
    db.add_photo(folder_id=f_other, filename='y.nef', extension='.nef',
                 file_size=1, file_mtime=1.0)

    # Active = other_ws
    assert db.get_workspace_extensions() == ['.nef']

    db.set_active_workspace(default_ws)
    assert db.get_workspace_extensions() == ['.jpg']


def test_get_workspace_extensions_skips_null_and_empty(tmp_path):
    """Photos with NULL/empty extension don't surface as an empty option."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/p', name='p')
    db.add_photo(folder_id=fid, filename='ok.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)
    # Force a NULL/empty row directly — add_photo doesn't normally let
    # this happen but historical scans may have produced rows with no
    # extension and we don't want them to render as a blank dropdown
    # option that would silently match nothing in _build_collection_query.
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size, file_mtime) "
        "VALUES (?, 'no_ext', NULL, 1, 1.0)", (fid,)
    )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size, file_mtime) "
        "VALUES (?, 'empty_ext', '', 1, 1.0)", (fid,)
    )
    db.conn.commit()

    assert db.get_workspace_extensions() == ['.jpg']


def test_collection_extension_rule_matches_case_insensitively(tmp_path):
    """Extension rule must match photos regardless of stored case.

    The dropdown only offers lowercased options, but older imports can
    leave .JPG (mixed case) in the photos table. SQLite's = is
    case-sensitive, so without LOWER() on both sides a rule saved as
    .jpg silently misses .JPG photos.
    """
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    db.add_photo(folder_id=fid, filename='lower.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='upper.JPG', extension='.JPG',
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='raw.nef', extension='.nef',
                 file_size=1, file_mtime=1.0)

    cid = db.add_collection(
        'JPGs', json.dumps([{"field": "extension", "op": "is", "value": ".jpg"}])
    )
    names = sorted(p['filename'] for p in db.get_collection_photos(cid))
    assert names == ['lower.jpg', 'upper.JPG']

    cid_not = db.add_collection(
        'NotJPG', json.dumps([{"field": "extension", "op": "is not", "value": ".jpg"}])
    )
    names_not = sorted(p['filename'] for p in db.get_collection_photos(cid_not))
    assert names_not == ['raw.nef']


def test_get_workspace_extensions_excludes_missing_folders(tmp_path):
    """An extension only present in a missing folder must not appear in the
    dropdown.

    `_build_collection_query` joins folders with `status IN ('ok',
    'partial')`, so an extension surfaced from a missing-folder photo
    would silently match zero rows when used in a rule — exactly the
    failure mode this dropdown exists to prevent.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid_ok = db.add_folder('/ok', name='ok')
    fid_gone = db.add_folder('/gone', name='gone')
    db.add_photo(folder_id=fid_ok, filename='a.jpg', extension='.jpg',
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid_gone, filename='b.cr2', extension='.cr2',
                 file_size=1, file_mtime=1.0)

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_gone,))
    db.conn.commit()

    # .cr2 lives only in the missing folder — must be filtered out.
    assert db.get_workspace_extensions() == ['.jpg']


# -- Regression tests: unbounded IN clauses, inat ordering, override guards --


def _cap_sqlite_vars(db, cap=999):
    """Emulate the historical SQLITE_MAX_VARIABLE_NUMBER=999 cap so an
    unchunked IN clause fails deterministically even on modern builds."""
    import sqlite3
    db.conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, cap)


def test_eye_keypoint_stage_chunks_large_photo_id_scope(tmp_path):
    """Pipeline callers pass the full resolved collection scope as
    photo_ids; a single IN clause would exceed the bind-var cap for big
    collections. Must route through _scope_clause like its count siblings."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)

    pids = []
    for i in range(2):
        pid = db.add_photo(fid, f"p{i}.jpg", ".jpg", 1000, float(i + 1),
                           width=800, height=600)
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
        pids.append(pid)

    _cap_sqlite_vars(db)
    scope = pids + list(range(1_000_000, 1_001_200))  # 1202 ids > 999 cap
    rows = db.list_photos_for_eye_keypoint_stage(photo_ids=scope)
    assert {r["id"] for r in rows} == set(pids)


def test_get_predictions_chunks_large_photo_id_list(tmp_path):
    """/api/predictions passes full-collection id lists. Chunked queries
    must merge while preserving the confidence-DESC ordering across
    chunk boundaries."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')

    def make_photo(name, conf):
        pid = db.add_photo(folder_id=fid, filename=name, extension='.jpg',
                           file_size=100, file_mtime=1.0)
        det = db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}],
            detector_model="MegaDetector",
        )[0]
        db.add_prediction(det, species=name, confidence=conf, model="bioclip")
        return pid

    p_low = make_photo("low.jpg", 0.3)
    p_high = make_photo("high.jpg", 0.9)

    _cap_sqlite_vars(db)
    # Put the high-confidence photo in the *last* chunk so an
    # append-without-resort implementation would order it after p_low.
    scope = [p_low] + list(range(1_000_000, 1_001_200)) + [p_high]
    rows = db.get_predictions(photo_ids=scope)
    assert [r["photo_id"] for r in rows] == [p_high, p_low]
    confs = [r["confidence"] for r in rows]
    assert confs == sorted(confs, reverse=True)


def test_get_keywords_for_photos_chunks_and_dedups_large_input(tmp_path):
    """Same /api/predictions source feeds get_keywords_for_photos with the
    full collection scope; must chunk, and duplicated input ids must not
    double-append keywords."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    kid = db.add_keyword("Robin", kw_type="general")
    db.tag_photo(pid, kid)

    _cap_sqlite_vars(db)
    # pid appears twice, in what would be different chunks.
    scope = [pid] + list(range(1_000_000, 1_001_200)) + [pid]
    result = db.get_keywords_for_photos(scope)
    assert list(result.keys()) == [pid]
    assert [k["name"] for k in result[pid]] == ["Robin"]


def test_delete_photos_chunks_large_resolve_list(tmp_path):
    """api_audit_remove_missing passes a raw request-body id list straight
    through; the initial resolve SELECT must chunk like the rest of the
    method already does."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pids = [
        db.add_photo(folder_id=fid, filename=f"p{i}.jpg", extension='.jpg',
                     file_size=100, file_mtime=1.0)
        for i in range(3)
    ]

    _cap_sqlite_vars(db)
    result = db.delete_photos(pids + list(range(1_000_000, 1_001_200)))
    assert result["deleted"] == 3
    assert set(result["ids"]) == set(pids)


def test_apply_duplicate_resolution_chunks_large_group(tmp_path):
    """duplicate_scan.py documents that one duplicate group can exceed the
    bind-var cap and chunks its own reads; the apply path must chunk both
    the resolve SELECT and the loser-flag UPDATE."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path / "photos"), name='photos')
    # 1100 photos in one group: 1099 losers > the legacy 999-var cap, so
    # the rejected-flag UPDATE must chunk too.
    pids = [
        db.add_photo(folder_id=fid, filename=f"dup{i:04d}.jpg", extension='.jpg',
                     file_size=100, file_mtime=float(i + 1))
        for i in range(1100)
    ]

    _cap_sqlite_vars(db)
    result = db.apply_duplicate_resolution(pids)
    assert result["winner_id"] in pids
    assert result["rejected"] == 1099
    flagged = db.conn.execute(
        "SELECT COUNT(*) AS n FROM photos WHERE flag = 'rejected'"
    ).fetchone()["n"]
    assert flagged == 1099


def test_collection_photo_ids_rule_supports_large_selection(tmp_path):
    """A static collection created from a large selection used to bind one
    parameter per id, making every query against the collection fail
    permanently. Integer ids are inlined as literals instead."""
    import json

    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                      file_size=100, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg',
                      file_size=100, file_mtime=2.0)

    ids = [p1, p2] + list(range(1_000_000, 1_001_200))
    cid = db.add_collection(
        'Big selection', json.dumps([{"field": "photo_ids", "value": ids}])
    )

    _cap_sqlite_vars(db)
    assert db.count_collection_photos(cid) == 2
    assert {p["id"] for p in db.get_collection_photos(cid)} == {p1, p2}
    # Composability: the leaf still works inside a rule group with siblings.
    assert db.count_photos_for_rules([
        {"field": "photo_ids", "value": ids},
        {"field": "rating", "op": ">=", "value": 0},
    ]) == 2


def test_get_inat_submissions_returns_newest_and_chunks(tmp_path):
    """Each photo must map to its most recent submission (the old dict
    comprehension over DESC-ordered rows kept the OLDEST), and the photo_id
    IN clause must chunk."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute(
        "INSERT INTO inat_submissions (photo_id, observation_id, observation_url, submitted_at) "
        "VALUES (?, 111, 'https://inat/111', '2025-01-01 00:00:00')",
        (pid,),
    )
    db.conn.execute(
        "INSERT INTO inat_submissions (photo_id, observation_id, observation_url, submitted_at) "
        "VALUES (?, 222, 'https://inat/222', '2026-01-01 00:00:00')",
        (pid,),
    )
    db.conn.commit()

    subs = db.get_inat_submissions([pid])
    assert subs[pid]["observation_id"] == 222

    _cap_sqlite_vars(db)
    subs = db.get_inat_submissions([pid] + list(range(1_000_000, 1_001_200)))
    assert subs[pid]["observation_id"] == 222


def test_workspace_active_labels_survive_non_dict_overrides(tmp_path):
    """api_update_workspace can persist non-dict config_overrides JSON; the
    active-labels accessors must fall back like get_effective_config and
    get_subject_types do, not raise AttributeError/TypeError."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id

    for bad in (["not", "a", "dict"], "just a string", 42):
        db.update_workspace(ws_id, config_overrides=bad)
        assert db.get_workspace_active_labels() is None
        # Setter must replace the junk rather than crash on item assignment.
        db.set_workspace_active_labels(["birds.txt"])
        assert db.get_workspace_active_labels() == ["birds.txt"]


def test_edit_presets_crud_strips_geometry(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    assert db.list_edit_presets() == []

    preset = db.save_edit_preset(
        "High-ISO forest",
        {
            "rotation": 90,
            "crop": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
            "adjustments": {"exposure": 0.5, "noise_reduction": 40},
        },
    )
    assert preset["name"] == "High-ISO forest"
    assert preset["recipe"] == {
        "version": 1,
        "adjustments": {"exposure": 0.5, "noise_reduction": 40.0},
    }

    listed = db.list_edit_presets()
    assert len(listed) == 1
    assert listed[0]["id"] == preset["id"]
    assert listed[0]["recipe"]["adjustments"]["noise_reduction"] == 40.0


def test_edit_preset_upserts_by_trimmed_name(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))

    first = db.save_edit_preset("Backlit  ", {"adjustments": {"exposure": 1}})
    second = db.save_edit_preset(
        " Backlit", {"adjustments": {"shadows": 30}}
    )

    assert first["name"] == "Backlit"
    assert second["id"] == first["id"]
    listed = db.list_edit_presets()
    assert len(listed) == 1
    assert listed[0]["recipe"]["adjustments"] == {"shadows": 30.0}


def test_edit_presets_list_sorted_by_name(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    for name in ("zebra dusk", "Backlit", "high-ISO forest"):
        db.save_edit_preset(name, {"adjustments": {"contrast": 10}})

    names = [p["name"] for p in db.list_edit_presets()]
    assert names == sorted(names, key=str.casefold)


def test_edit_preset_rejects_empty_or_geometry_only(tmp_path):
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))

    with pytest.raises(ValueError):
        db.save_edit_preset("Nothing", {})
    with pytest.raises(ValueError):
        db.save_edit_preset("Geometry only", {"rotation": 90})
    with pytest.raises(ValueError):
        db.save_edit_preset("Zeroed", {"adjustments": {"exposure": 0}})
    assert db.list_edit_presets() == []


def test_edit_preset_rejects_blank_or_overlong_name(tmp_path):
    import pytest
    from db import Database
    db = Database(str(tmp_path / "test.db"))

    with pytest.raises(ValueError):
        db.save_edit_preset("   ", {"adjustments": {"exposure": 1}})
    with pytest.raises(ValueError):
        db.save_edit_preset("x" * 200, {"adjustments": {"exposure": 1}})


def test_delete_edit_preset(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    preset = db.save_edit_preset("Doomed", {"adjustments": {"exposure": 1}})

    assert db.delete_edit_preset(preset["id"]) is True
    assert db.delete_edit_preset(preset["id"]) is False
    assert db.list_edit_presets() == []


def test_import_tab_in_nav_registries(tmp_path):
    """import/process split PR 3 + import-page-routing PR: the Import
    tab must be in every server registry, and must be the leftmost/
    first pinned page in DEFAULT_TABS since Import is now the natural
    starting workflow for new workspaces."""
    from db import ALL_NAV_IDS, DEFAULT_TABS, Database

    assert "import" in ALL_NAV_IDS
    assert DEFAULT_TABS[0] == "import", DEFAULT_TABS

    db = Database(str(tmp_path / "t.db"))
    db.set_tabs(["import", "browse"])
    assert db.get_tabs()[:2] == ["import", "browse"]


def test_existing_workspaces_gain_import_tab(tmp_path):
    """A pre-split workspace tabs row gets Import inserted leftmost on init."""
    import json as json_mod

    from db import Database

    db_path = str(tmp_path / "m.db")
    db = Database(db_path)
    ws = db._active_workspace_id
    old = ["browse", "pipeline", "review"]
    db.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json_mod.dumps(old), ws),
    )
    # A real pre-split DB was written by a version that never set
    # PRAGMA user_version, so it reads back as 0. The fresh Database
    # above already bumped it to 4; reset it so the second init runs
    # every guarded tabs migration from scratch.
    db.conn.execute("PRAGMA user_version = 0")
    db.conn.commit()
    db.close()

    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    tabs = db2.get_tabs()
    assert "import" in tabs
    assert tabs[0] == "import"


def test_import_tab_migration_not_reapplied_after_unpin(tmp_path):
    """Once the import-tab migration has run, a subsequent unpin must
    stay unpinned — the migration is guarded by PRAGMA user_version so
    every ``Database`` re-open doesn't silently re-add ``import``.
    """
    import json as json_mod

    from db import Database

    db_path = str(tmp_path / "m.db")
    db = Database(db_path)
    ws = db._active_workspace_id
    # User unpins ``import`` after the migration has already run.
    db.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json_mod.dumps(["browse", "pipeline", "review"]), ws),
    )
    db.conn.commit()
    db.close()

    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    assert "import" not in db2.get_tabs()


def test_existing_workspaces_gain_storage_tab(tmp_path):
    """Existing saved tabs get Storage once so moved cache controls stay visible."""
    import json as json_mod

    from db import Database

    db_path = str(tmp_path / "m.db")
    db = Database(db_path)
    ws = db._active_workspace_id
    old = ["browse", "pipeline", "review", "settings"]
    db.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json_mod.dumps(old), ws),
    )
    db.conn.execute("PRAGMA user_version = 1")
    db.conn.commit()
    db.close()

    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    tabs = db2.get_tabs()
    assert "storage" in tabs
    assert tabs.index("storage") == tabs.index("settings") - 1


def test_storage_tab_migration_not_reapplied_after_unpin(tmp_path):
    """Once the storage-tab migration has run, a later unpin stays unpinned."""
    import json as json_mod

    from db import Database

    db_path = str(tmp_path / "m.db")
    db = Database(db_path)
    ws = db._active_workspace_id
    db.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json_mod.dumps(["browse", "pipeline", "review", "settings"]), ws),
    )
    db.conn.commit()
    db.close()

    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    assert "storage" not in db2.get_tabs()


# ---------------------------------------------------------------------------
# Life list explorer (taxonomic completeness) — shared taxa seeding helper
# ---------------------------------------------------------------------------

def _seed_bird_taxonomy(db):
    """Insert a tiny Aves subtree: class Aves > 2 orders > families > genera > species.
    Returns dict of name -> taxa id."""
    rows = [
        # (inat_id, name, common_name, rank, parent_name, kingdom)
        (3,     "Aves",           "Birds",         "class",   None,             "Animalia"),
        (7251,  "Passeriformes",  "Perching Birds","order",   "Aves",           "Animalia"),
        (67566, "Passerellidae",  "New World Sparrows","family","Passeriformes", "Animalia"),
        (9100,  "Melospiza",      None,            "genus",   "Passerellidae",  "Animalia"),
        (9101,  "Melospiza melodia","Song Sparrow","species", "Melospiza",      "Animalia"),
        (9102,  "Melospiza georgiana","Swamp Sparrow","species","Melospiza",    "Animalia"),
        (9200,  "Zonotrichia",    None,            "genus",   "Passerellidae",  "Animalia"),
        (9201,  "Zonotrichia albicollis","White-throated Sparrow","species","Zonotrichia","Animalia"),
        (4000,  "Anseriformes",   "Waterfowl",     "order",   "Aves",           "Animalia"),
        (4100,  "Anatidae",       "Ducks",         "family",  "Anseriformes",   "Animalia"),
        (4200,  "Anas",           None,            "genus",   "Anatidae",       "Animalia"),
        (4201,  "Anas platyrhynchos","Mallard",    "species", "Anas",           "Animalia"),
    ]
    ids = {}
    for inat_id, name, common, rank, parent, kingdom in rows:
        parent_id = ids.get(parent)
        cur = db.conn.execute(
            "INSERT INTO taxa (inat_id, name, common_name, rank, parent_id, kingdom)"
            " VALUES (?,?,?,?,?,?)",
            (inat_id, name, common, rank, parent_id, kingdom),
        )
        ids[name] = cur.lastrowid
    db.conn.commit()
    return ids


def test_get_default_explorer_root_finds_aves(db):
    assert db.get_explorer_root() is None  # no taxonomy yet
    ids = _seed_bird_taxonomy(db)
    root = db.get_explorer_root()
    assert root["id"] == ids["Aves"]
    assert root["name"] == "Aves"
    assert root["rank"] == "class"


def test_life_list_taxon_ids_scope(db):
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                      file_size=1, file_mtime=1.0, timestamp='2024-01-01T00:00:00')
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg',
                      file_size=1, file_mtime=2.0, timestamp='2024-01-02T00:00:00')
    # Matched species keyword (linked to Song Sparrow taxon)
    k1 = db.add_keyword('Song Sparrow')
    db.tag_photo(p1, k1)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k1))
    db.conn.commit()
    # Unmatched species keyword (is_species but no taxon_id)
    k2 = db.add_keyword('Mystery Warbler')
    db.tag_photo(p2, k2)
    db.conn.execute("UPDATE keywords SET is_species=1 WHERE id=?", (k2,))
    db.conn.commit()

    found = db.get_life_list_taxon_ids()
    assert found == {ids['Melospiza melodia']}
    unmatched = db.get_life_list_unmatched_species()
    assert 'Mystery Warbler' in unmatched


def test_life_list_taxon_ids_excludes_non_species_matches(db):
    # A taxonomy tag that resolves to a genus (or any rank above species) must
    # NOT show up as a "found" taxon — the explorer only counts at species rank
    # and would otherwise silently drop the match. It should surface in the
    # unmatched list instead so the "not counted" honesty footnote is accurate.
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=1, file_mtime=1.0)
    # Tag with a keyword that links to the Melospiza *genus*, not a species.
    k = db.add_keyword('Melospiza sp.')
    db.tag_photo(p, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza'], k))
    db.conn.commit()

    assert db.get_life_list_taxon_ids() == set()
    unmatched = db.get_life_list_unmatched_species()
    assert 'Melospiza sp.' in unmatched


def test_life_list_taxon_ids_excludes_rejected(db):
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=1, file_mtime=1.0)
    k = db.add_keyword('Song Sparrow')
    db.tag_photo(p, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k))
    db.conn.commit()
    db.update_photo_flag(p, 'rejected')
    assert db.get_life_list_taxon_ids() == set()


def test_get_taxon_subtree(db):
    ids = _seed_bird_taxonomy(db)
    rows = db.get_taxon_subtree(ids['Aves'])
    by_name = {r['name']: r for r in rows}
    assert by_name['Aves']['rank'] == 'class'
    assert by_name['Melospiza melodia']['rank'] == 'species'
    # parent linkage preserved
    assert by_name['Passeriformes']['parent_id'] == ids['Aves']
    assert by_name['Melospiza']['parent_id'] == ids['Passerellidae']
    # subtree of a genus is just its species + itself
    sub = {r['name'] for r in db.get_taxon_subtree(ids['Melospiza'])}
    assert sub == {'Melospiza', 'Melospiza melodia', 'Melospiza georgiana'}


def test_get_classes_for_taxa(db):
    ids = _seed_bird_taxonomy(db)
    classes = db.get_classes_for_taxa({ids['Melospiza melodia']})
    assert [c['name'] for c in classes] == ['Aves']
    assert db.get_classes_for_taxa(set()) == []


def test_get_classes_for_taxa_chunks_large_id_lists(db):
    """`/api/life-list/explorer` passes the whole life-list `found` set, which can
    exceed SQLite's bound-parameter limit — the query must chunk and merge."""
    from vireo.db import _SQLITE_PARAM_CHUNK_SIZE
    ids = _seed_bird_taxonomy(db)
    # A pile of non-existent taxon ids (well above the chunk size) plus one real
    # species id. A single un-chunked IN () would exceed SQLite's parameter cap;
    # the chunked implementation must still find Aves via the real id.
    n_fillers = _SQLITE_PARAM_CHUNK_SIZE * 3 + 25
    seed = {10_000_000 + i for i in range(n_fillers)}
    seed.add(ids['Melospiza melodia'])
    classes = db.get_classes_for_taxa(seed)
    assert [c['name'] for c in classes] == ['Aves']


def test_best_photo_by_taxon(db):
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p1 = db.add_photo(folder_id=fid, filename='low.jpg', extension='.jpg',
                      file_size=1, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='high.jpg', extension='.jpg',
                      file_size=1, file_mtime=2.0)
    # No update_photo_quality_score helper in db.py; set the column directly.
    db.conn.execute("UPDATE photos SET quality_score=? WHERE id=?", (0.2, p1))
    db.conn.execute("UPDATE photos SET quality_score=? WHERE id=?", (0.9, p2))
    db.conn.commit()
    k = db.add_keyword('Song Sparrow')
    db.tag_photo(p1, k)
    db.tag_photo(p2, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k))
    db.conn.commit()
    best = db.get_life_list_best_photo_by_taxon([ids['Melospiza melodia']])
    assert best[ids['Melospiza melodia']]['filename'] == 'high.jpg'


def test_apply_case_convention_preserves_mixed_case_eponym(tmp_path):
    """The 'lower' convention must not mangle mixed-case first words:
    `McKay's bunting` keeps its internal capital, ALL-CAPS label-file
    spellings sentence-case, plain words capitalize as before."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        conv = db._apply_case_convention
        assert conv("McKay's bunting", "lower") == "McKay's bunting"
        assert conv("MALLARD DUCK", "lower") == "Mallard duck"
        assert conv("black phoebe", "lower") == "Black phoebe"
        assert conv("mallard", "lower") == "Mallard"
    finally:
        db.close()


def test_curation_setters_canonicalize_species_casing(tmp_path):
    """Curation writes with a prediction-cased label key on the stored
    keyword spelling, and removal finds the row under any casing."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    try:
        ws_id = db.ensure_default_workspace()
        db.set_active_workspace(ws_id)
        fid = db.add_folder('/photos', name='photos')
        pid = db.add_photo(
            folder_id=fid, filename='a.jpg', extension='.jpg',
            file_size=100, file_mtime=1.0,
        )
        db.conn.execute(
            "INSERT INTO keywords (name, type, is_species) "
            "VALUES ('Common waxbill', 'taxonomy', 1)"
        )
        db.conn.commit()

        db.set_photo_preference("life_list", "Common Waxbill", pid)
        prefs = db.conn.execute(
            "SELECT DISTINCT species FROM photo_preferences"
        ).fetchall()
        assert {r["species"] for r in prefs} == {"Common waxbill"}

        rank = db.add_species_highlight("Common Waxbill", pid)
        assert rank == 1
        rows = db.conn.execute(
            "SELECT species FROM species_highlights"
        ).fetchall()
        assert [r["species"] for r in rows] == ["Common waxbill"]

        # Re-adding under yet another casing reuses the canonical row
        # instead of appending a second rank.
        assert db.add_species_highlight("COMMON WAXBILL", pid) == 1

        removed = db.remove_species_highlight("Common Waxbill", pid)
        assert removed == 1
    finally:
        db.close()


def test_add_species_keyword_prefers_species_rank_over_genus_homonym(tmp_path):
    """Adding an is_species=True keyword whose name matches both a species
    and a higher-rank homonym (e.g. species Puma vs genus Puma) must bind
    the row to the species-rank taxon. Otherwise Life List, Compare, and
    Explorer queries — which filter on ``t.rank = 'species'`` — silently
    drop photos carrying the just-added keyword.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (46272, "Puma", "Puma", "genus"),
            (41963, "Puma concolor", "Puma", "species"),
        ],
    )
    db.conn.commit()
    species_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'species'"
    ).fetchone()["id"]

    kid = db.add_keyword("Puma", is_species=True)
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert dict(row) == {
        "is_species": 1, "type": "taxonomy", "taxon_id": species_taxon,
    }


def test_add_general_keyword_promoted_to_species_prefers_species_rank(tmp_path):
    """When add_keyword auto-detects a general name as taxonomy via a
    taxa lookup, the resulting is_species=1 row must still land on a
    species-rank taxon so the rank-filtered queries don't drop it.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (46272, "Puma", "Puma", "genus"),
            (41963, "Puma concolor", "Puma", "species"),
        ],
    )
    db.conn.commit()
    species_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'species'"
    ).fetchone()["id"]

    kid = db.add_keyword("Puma")
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
    assert row["taxon_id"] == species_taxon


def test_rename_keyword_to_matching_taxon_prefers_species_rank(tmp_path):
    """Renaming a general keyword to a name that matches both a species
    and a higher-rank homonym auto-promotes to taxonomy on the
    species-rank taxon so rank-filtered queries still see the row.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (46272, "Puma", "Puma", "genus"),
            (41963, "Puma concolor", "Puma", "species"),
        ],
    )
    db.conn.commit()
    species_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'species'"
    ).fetchone()["id"]

    kid = db.add_keyword("Cougar")
    db.update_keyword(kid, name="Puma")

    row = db.conn.execute(
        "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
    assert row["taxon_id"] == species_taxon


def test_add_species_rebinds_existing_row_from_higher_rank_to_species(tmp_path):
    """A pre-existing keyword row already bound to a higher-rank taxon
    (e.g. a legacy catalog that stamped ``Puma`` with the genus taxon
    before ``prefer_species`` existed) must be rebound to the species-
    rank taxon on the next species accept. Without the rebind the row
    stays flagged is_species/taxonomy while its taxon_id points at the
    genus, and every downstream ``t.rank = 'species'`` filter (Life
    List, Compare, Explorer) silently drops photos carrying it.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (46272, "Puma", "Puma", "genus"),
            (41963, "Puma concolor", "Puma", "species"),
        ],
    )
    db.conn.commit()
    genus_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'genus'"
    ).fetchone()["id"]
    species_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'species'"
    ).fetchone()["id"]

    # Legacy row: correctly typed taxonomy species but bound to the
    # non-species-rank genus taxon.
    kid = db.add_keyword("Puma", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (genus_taxon, kid),
    )
    db.conn.commit()
    pre = db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert pre["taxon_id"] == genus_taxon

    # Accepting the species again should rebind onto the species-rank
    # taxon rather than leaving the higher-rank homonym in place.
    same = db.add_keyword("Puma", is_species=True)
    assert same == kid
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["is_species"] == 1
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] == species_taxon


def test_add_species_preserves_species_taxon_over_species_taxon(tmp_path):
    """A row already bound to a species-rank taxon must NOT be rebound
    when a different species-rank taxon shares the name — same-NOCASE
    homonyms preserved by the migration are genuinely different species,
    so the existing binding is authoritative.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, 'species', 'Animalia')",
        [
            (7000, "Aythya affinis", "Robin"),
            (7001, "Turdus migratorius", "Robin"),
        ],
    )
    db.conn.commit()
    original_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE inat_id = 7000"
    ).fetchone()["id"]

    kid = db.add_keyword("Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (original_taxon, kid),
    )
    db.conn.commit()

    same = db.add_keyword("Robin", is_species=True)
    assert same == kid
    row = db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["taxon_id"] == original_taxon


def test_promote_general_row_with_higher_rank_taxon_rebinds_to_species(tmp_path):
    """A legacy 'general' row already carrying a higher-rank taxon_id
    is promoted to taxonomy on the next taxonomy lookup and its
    taxon_id must be rebound to the species-rank homonym. Without the
    rebind the promoted row would be stamped is_species=1/taxonomy but
    still bound to the genus, and Life List/Compare rank filters would
    drop photos tagged with it.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (46272, "Puma", "Puma", "genus"),
            (41963, "Puma concolor", "Puma", "species"),
        ],
    )
    db.conn.commit()
    genus_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'genus'"
    ).fetchone()["id"]
    species_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'species'"
    ).fetchone()["id"]

    # Simulate a legacy general row bound to the genus taxon. add_keyword
    # normally auto-promotes on insert, so bypass it with direct SQL.
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Puma', 'general', 0, ?)",
        (genus_taxon,),
    )
    kid = cur.lastrowid
    db.conn.commit()

    same = db.add_keyword("Puma")
    assert same == kid
    row = db.conn.execute(
        "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
    assert row["taxon_id"] == species_taxon


def test_add_species_prefers_alternate_common_name_over_higher_rank_direct(tmp_path):
    """When a submitted label collides with a higher-rank taxon on the
    direct ``taxa`` lookup but also appears in ``taxa_common_names`` as an
    alternate for a species-rank taxon, the species-rank alternate must
    win. Otherwise the new rank filters (Life List, Compare) silently drop
    every photo carrying the accepted keyword even though the accept
    reported success.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    # Direct match on `Boa` is the genus; the species Boa constrictor
    # registers `Boa` as an alternate English name via taxa_common_names.
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank, kingdom) "
        "VALUES (1, 'Boa', 'Boa', 'genus', 'Animalia')"
    )
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank, kingdom) "
        "VALUES (2, 'Boa constrictor', 'Boa Constrictor', 'species', 'Animalia')"
    )
    db.conn.execute(
        "INSERT INTO taxa_common_names (taxon_id, name) VALUES (2, 'Boa')"
    )
    db.conn.commit()

    kid = db.add_keyword("Boa", is_species=True)
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
    assert row["taxon_id"] == 2


def test_add_species_leaves_taxon_null_when_only_higher_rank_matches(tmp_path):
    """When a species-typed add finds only a higher-rank taxon (no
    species-rank direct or common-name match), ``taxon_id`` must be left
    NULL. Binding the row to a genus/family would satisfy the caller's
    reconciliation (``is_species = 1``/``type = 'taxonomy'`` still get
    stamped) while making the row invisible to every rank-filtered reader
    (Life List, Compare, Explorer, highlight/preference eligibility) that
    restricts to ``t.rank = 'species' OR t.rank IS NULL``.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (10001, "Corvidae", "Corvids", "family"),
        ],
    )
    db.conn.commit()

    kid = db.add_keyword("Corvidae", is_species=True)
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
    assert row["taxon_id"] is None


def test_add_species_preserves_higher_rank_taxon_when_no_species_match(tmp_path):
    """A pre-existing keyword row bound to a valid higher-rank taxon
    (e.g. a ``Corvidae`` family observation) must keep that
    ``taxon_id`` when the next species-typed accept cannot find a
    species-rank match. ``get_life_list_candidates`` and
    ``get_life_list_locations`` now surface linked higher-rank
    identifications so their ``taxon_rank`` / ``scientific_name`` /
    ``taxonomic_class`` metadata powers the new genus / family /
    class Life List filters; clearing the ``taxon_id`` here would
    strip that metadata on the next normal keyword edit and silently
    break those filters — mirroring the preservation guarantee that
    :meth:`mark_species_keywords` gives on startup.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (10001, "Corvidae", "Corvids", "family"),
        ],
    )
    db.conn.commit()
    family_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'family'"
    ).fetchone()["id"]

    # Fully typed taxonomy row linked to a valid higher-rank taxon.
    # ``add_keyword`` is species-preferring on insert, so bypass it
    # with direct SQL to simulate the upgraded catalog.
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Corvidae', 'taxonomy', 1, ?)",
        (family_taxon,),
    )
    kid = cur.lastrowid
    db.conn.commit()

    # Re-adding as a taxonomy keyword must NOT clear the family link.
    same = db.add_keyword("Corvidae", is_species=True)
    assert same == kid
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["is_species"] == 1
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] == family_taxon

    # The same guarantee must hold for the kw_type='taxonomy' variant
    # taken by other callers.
    same_typed = db.add_keyword("Corvidae", kw_type="taxonomy")
    assert same_typed == kid
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["is_species"] == 1
    assert row["type"] == "taxonomy"
    assert row["taxon_id"] == family_taxon


def test_mark_species_keywords_rebinds_higher_rank_taxonomy_link(tmp_path):
    """A keyword row already typed ``taxonomy`` and linked by the old
    species-agnostic lookup to a genus/family taxon must be rebound to a
    species-rank taxon on the next ``mark_species_keywords`` pass. Without
    the rebind, upgraded catalogs keep the higher-rank binding and the new
    ``t.rank = 'species'`` filter (Life List, Compare, Explorer) silently
    drops every photo carrying the accepted keyword.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (46272, "Puma", "Puma", "genus"),
            (41963, "Puma concolor", "Puma", "species"),
        ],
    )
    db.conn.commit()
    genus_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'genus'"
    ).fetchone()["id"]
    species_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'species'"
    ).fetchone()["id"]

    # Simulate a legacy row: fully typed taxonomy but bound to the
    # non-species-rank genus taxon. add_keyword normally auto-promotes on
    # insert, so bypass it with direct SQL.
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Puma', 'taxonomy', 1, ?)",
        (genus_taxon,),
    )
    kid = cur.lastrowid
    db.conn.commit()

    class FakeTaxonomy:
        def lookup(self, name):
            if name.lower() == "puma":
                return {"taxon_id": 41963}
            return None

        def is_taxon(self, name):
            return self.lookup(name) is not None

    updated = db.mark_species_keywords(FakeTaxonomy())
    assert updated == 1
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
    assert row["taxon_id"] == species_taxon


def test_mark_species_keywords_leaves_taxon_null_when_only_higher_rank_available(tmp_path):
    """A legacy accepted species keyword (``type='taxonomy'``,
    ``is_species=1``, ``taxon_id=NULL``) whose name only resolves to a
    higher-rank taxon (genus/family) must NOT be bound to that
    non-species taxon on the next ``mark_species_keywords`` pass. Binding
    the row to a genus/family would satisfy the marking pass while making
    the row invisible to every rank-filtered reader (Life List, Compare,
    highlight/preference eligibility) that restricts to
    ``t.rank = 'species' OR t.rank IS NULL``, so upgraded catalogs would
    silently lose every photo carrying the accepted keyword.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (46272, "Puma", "Puma", "genus"),
        ],
    )
    db.conn.commit()

    # Simulate an accepted species keyword created before the local taxa
    # table was populated: fully typed as species but with a NULL taxon
    # link. add_keyword's species path already refuses to bind to a
    # higher-rank taxon, so bypass it with direct SQL.
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Puma', 'taxonomy', 1, NULL)",
    )
    kid = cur.lastrowid
    db.conn.commit()

    class FakeTaxonomy:
        # Only the genus is known — no species-rank alternative exists.
        def lookup(self, name):
            if name.lower() == "puma":
                return {"taxon_id": 46272}
            return None

        def is_taxon(self, name):
            return self.lookup(name) is not None

    updated = db.mark_species_keywords(FakeTaxonomy())
    assert updated == 0
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
    assert row["taxon_id"] is None


def test_mark_species_keywords_keeps_species_taxon_when_only_higher_rank_available(tmp_path):
    """``mark_species_keywords`` must not clobber an existing species-rank
    binding just because the taxonomy lookup only resolves to a higher-rank
    homonym. A row already correctly bound stays bound and no update fires.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (46272, "Puma", "Puma", "genus"),
            (41963, "Puma concolor", "Puma", "species"),
        ],
    )
    db.conn.commit()
    species_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'species'"
    ).fetchone()["id"]

    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Puma', 'taxonomy', 1, ?)",
        (species_taxon,),
    )
    kid = cur.lastrowid
    db.conn.commit()

    class FakeTaxonomy:
        # Taxonomy only knows the genus id here — the rebind must NOT fire
        # because there is no species-rank alternative.
        def lookup(self, name):
            if name.lower() == "puma":
                return {"taxon_id": 46272}
            return None

        def is_taxon(self, name):
            return self.lookup(name) is not None

    updated = db.mark_species_keywords(FakeTaxonomy())
    assert updated == 0
    row = db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["taxon_id"] == species_taxon


def test_mark_species_keywords_preserves_higher_rank_taxon_when_no_species_match(tmp_path):
    """A fully typed taxonomy row already linked to a higher-rank taxon
    (genus/family/class) must retain that link on the next
    ``mark_species_keywords`` pass when no species-rank replacement is
    available. ``get_life_list_candidates`` and ``get_life_list_locations``
    now surface linked higher-rank identifications so their
    ``taxon_rank`` / ``scientific_name`` / ``taxonomic_class`` metadata
    powers the new genus / family / class Life List filters; clearing
    the ``taxon_id`` on startup would strip that metadata and silently
    break those filters after the first restart.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (10001, "Corvidae", "Corvids", "family"),
        ],
    )
    db.conn.commit()
    family_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'family'"
    ).fetchone()["id"]

    # Fully typed taxonomy row linked to a valid higher-rank taxon (a
    # ``Corvidae`` family observation the user has accepted). add_keyword
    # is species-preferring on insert, so bypass it with direct SQL to
    # simulate the upgraded catalog.
    cur = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) "
        "VALUES ('Corvidae', 'taxonomy', 1, ?)",
        (family_taxon,),
    )
    kid = cur.lastrowid
    db.conn.commit()

    class FakeTaxonomy:
        # Only the family is known — no species-rank alternative exists.
        def lookup(self, name):
            if name.lower() == "corvidae":
                return {"taxon_id": 10001}
            return None

        def is_taxon(self, name):
            return self.lookup(name) is not None

    updated = db.mark_species_keywords(FakeTaxonomy())
    assert updated == 0
    row = db.conn.execute(
        "SELECT is_species, type, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1
    assert row["taxon_id"] == family_taxon


def test_repair_duplicate_photo_species_preserves_highlight_eligibility(tmp_path):
    """After ``repair_duplicate_photo_species`` detaches the redundant
    root, ``get_species_highlights(eligible_only=True)`` must still
    surface highlights stored under the canonical root name for a
    photo whose only remaining species keyword is a differently-cased
    hierarchy leaf (``verdin`` vs ``Verdin``). The taxon-linked
    fallback matches the leaf back to the root without loosening the
    ambiguous-homonym boundary.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], nested),
    )
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["Verdin"], root),
    )
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    # Highlight stored under the canonical root spelling — the same
    # string curation is keyed on and the photo carried via the root
    # tag before repair detached it.
    db.add_species_highlight("Verdin", pid)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()

    assert db.repair_duplicate_photo_species() == 1

    # Only the differently-spelled hierarchy leaf remains attached.
    attached_names = {
        row["name"] for row in db.conn.execute(
            """SELECT k.name FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
               WHERE pk.photo_id = ?""",
            (pid,),
        ).fetchall()
    }
    assert "verdin" in attached_names
    assert "Verdin" not in attached_names

    # Highlight under the canonical root name still applies.
    highlights = db.get_species_highlights(eligible_only=True)
    assert pid in highlights.get("Verdin", {}), (
        "highlight under root spelling must remain eligible for a photo "
        "whose only species keyword is a differently-spelled hierarchy leaf"
    )
    # Same result via the single-species selector used by the UI.
    single = db.get_species_highlights(species="Verdin", eligible_only=True)
    assert pid in single.get("Verdin", {})


def test_get_species_highlights_canonicalizes_prediction_alias(tmp_path):
    """``resolve_species_display_name`` routes a linked hierarchy alias
    (e.g. ``Desert Verdin`` bound to the ``Verdin`` species taxon) to the
    canonical root spelling, so ``add_species_highlight`` /
    ``_collect_highlight_buckets`` store an unconfirmed prediction's
    highlight under ``Verdin``. The prediction-fallback branch of
    ``get_species_highlights(eligible_only=True)`` must apply the same
    canonicalization when it looks up the photo's top prediction —
    otherwise the highlight vanishes on reload because ``pr.species``
    is still the raw ``Desert Verdin`` label.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.9 WHERE id = ?", (pid,)
    )
    # Root "Verdin" + hierarchy leaf "Desert Verdin" both bound to the
    # same species taxon. resolve_species_display_name canonicalizes
    # "Desert Verdin" through the shared taxon to root "Verdin".
    root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["Verdin"], root),
    )
    parent = db.add_keyword("Penduline tits")
    leaf = db.add_keyword("Desert Verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], leaf),
    )

    # Photo has NO species keyword tagged (unconfirmed) but has a top
    # classifier prediction labelled with the hierarchy alias.
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) "
        "VALUES (?, 'test', 'fp1', 'Desert Verdin', 0.95)",
        (did,),
    )
    db.conn.commit()

    # add_species_highlight canonicalizes the label via
    # resolve_species_display_name — the leaf's linked taxon resolves
    # back to root "Verdin", so the highlight lands under "Verdin".
    db.add_species_highlight("Desert Verdin", pid)
    stored = db.conn.execute(
        "SELECT species FROM species_highlights WHERE photo_id = ?", (pid,)
    ).fetchone()
    assert stored["species"] == "Verdin"

    # Prediction-fallback eligibility must canonicalize pr.species the
    # same way, so the stored highlight remains eligible on reload.
    highlights = db.get_species_highlights(eligible_only=True)
    assert pid in highlights.get("Verdin", {}), (
        "highlight canonicalized to root spelling must still match a photo "
        "whose top prediction is the linked hierarchy alias"
    )
    single = db.get_species_highlights(species="Verdin", eligible_only=True)
    assert pid in single.get("Verdin", {})


def test_get_species_highlights_prediction_fallback_rejects_wrong_taxon(tmp_path):
    """Canonicalizing pr.species through the leaf's taxon must not
    loosen the taxon boundary: an unconfirmed photo whose top prediction
    is a leaf linked to taxon A must NOT satisfy a highlight stored
    under a canonical root name resolving to taxon B, even when the
    strings would collide after case-folding.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [
        (2912, "Auriparus flaviceps", "Verdin"),
        (7000, "Turdus migratorius", "American Robin"),
    ])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.9 WHERE id = ?", (pid,)
    )
    # Leaf "Desert Verdin" bound to the Verdin species taxon.
    parent = db.add_keyword("Penduline tits")
    leaf = db.add_keyword("Desert Verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], leaf),
    )
    verdin_root = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["Verdin"], verdin_root),
    )
    # Unrelated root "American Robin" bound to a different species taxon.
    robin_root = db.add_keyword("American Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["American Robin"], robin_root),
    )
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) "
        "VALUES (?, 'test', 'fp1', 'Desert Verdin', 0.95)",
        (did,),
    )
    # A highlight under a different root — must not become eligible for
    # a photo whose top prediction canonicalizes to another root.
    db.add_species_highlight("American Robin", pid)
    db.conn.commit()

    highlights = db.get_species_highlights(
        species="American Robin", eligible_only=True,
    )
    assert "American Robin" not in highlights, (
        "prediction-fallback canonicalization must respect the taxon "
        "boundary — a highlight under 'American Robin' must not be "
        "eligible for a photo whose top prediction resolves to 'Verdin'"
    )


def test_get_species_highlights_prediction_fallback_skips_ambiguous_alias(tmp_path):
    """When multiple linked hierarchy leaves share a predicted label but point
    at DIFFERENT taxa, the prediction-fallback canonicalization must NOT pick
    an arbitrary root for the alias — that would silently promote an
    ambiguous prediction into a specific root bucket. ``_collect_highlight_buckets``
    keeps such ambiguous labels raw, so the eligibility reload path must too:
    highlights saved under either root name must not become eligible for a
    photo whose top prediction is the ambiguous shared label.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [
        (30001, "Turdus migratorius", "American Robin"),
        (30002, "Erithacus rubecula", "European Robin"),
    ])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.9 WHERE id = ?", (pid,)
    )
    # Two hierarchy leaves both named "Robin" but linked to distinct taxa.
    parent_a = db.add_keyword("Old World")
    leaf_a = db.add_keyword("Robin", parent_id=parent_a)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["European Robin"], leaf_a),
    )
    parent_b = db.add_keyword("New World")
    leaf_b = db.add_keyword("Robin", parent_id=parent_b)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["American Robin"], leaf_b),
    )
    # Corresponding roots for each linked taxon.
    american_root = db.add_keyword("American Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["American Robin"], american_root),
    )
    european_root = db.add_keyword("European Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["European Robin"], european_root),
    )
    # A prediction on the photo carries the ambiguous shared label.
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) "
        "VALUES (?, 'test', 'fp1', 'Robin', 0.95)",
        (did,),
    )
    # Simulate highlights saved directly under each root's canonical name.
    db.conn.execute(
        "INSERT INTO species_highlights (species, photo_id, rank, workspace_id) "
        "VALUES ('American Robin', ?, 0, 1)",
        (pid,),
    )
    db.conn.execute(
        "INSERT INTO species_highlights (species, photo_id, rank, workspace_id) "
        "VALUES ('European Robin', ?, 0, 1)",
        (pid,),
    )
    db.conn.commit()

    highlights = db.get_species_highlights(eligible_only=True)
    assert pid not in highlights.get("American Robin", {}), (
        "ambiguous prediction label must not silently canonicalize to an "
        "arbitrary root — the 'American Robin' highlight must not become "
        "eligible when the top prediction 'Robin' resolves to multiple taxa"
    )
    assert pid not in highlights.get("European Robin", {}), (
        "ambiguous prediction label must not silently canonicalize to an "
        "arbitrary root — the 'European Robin' highlight must not become "
        "eligible when the top prediction 'Robin' resolves to multiple taxa"
    )


def test_get_species_highlights_rejects_taxon_mismatch(tmp_path):
    """The new taxon-linked fallback must only match the leaf back to a
    root whose taxon_id shares identity with the leaf's taxon_id. A
    photo whose only species keyword links to taxon A must not satisfy
    a highlight whose canonical root name resolves to taxon B, even if
    the leaf keyword's spelling happens to match the root name.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [
        (2912, "Auriparus flaviceps", "Verdin"),
        (7000, "Turdus migratorius", "American Robin"),
    ])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Photo carries only a Verdin hierarchy leaf.
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["Verdin"], nested),
    )
    db.tag_photo(pid, nested)
    # Root named "American Robin" bound to that species' taxon — its
    # taxon_id does NOT match the leaf's taxon, so the taxon-linked
    # fallback must reject the pairing.
    robin_root = db.add_keyword("American Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["American Robin"], robin_root),
    )
    db.add_species_highlight("American Robin", pid)
    db.conn.commit()

    highlights = db.get_species_highlights(
        species="American Robin", eligible_only=True,
    )
    assert "American Robin" not in highlights, (
        "highlight stored under one species must not become eligible "
        "for a photo whose only species keyword links to a different taxon"
    )


def test_get_species_highlights_prediction_fallback_prefers_same_name_root(tmp_path):
    """The prediction-only highlight fallback must mirror
    ``resolve_species_display_name``'s root-first preference: when a
    top-level species root shares the prediction label with a hierarchy
    leaf that points at a different taxon, the reload canonicalization
    must return the root's stored spelling, not the leaf's root spelling.

    Without this, ``add_species_highlight`` stores the highlight under
    the root name (case 1) but the eligibility reload canonicalizes
    through the mismatched leaf's taxon and drops the saved highlight.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    taxa = _seed_taxa(db, [
        (40001, "Turdus migratorius", "American Robin"),
        (40002, "Erithacus rubecula", "European Robin"),
    ])
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.9 WHERE id = ?", (pid,)
    )
    # Top-level species root named "Robin" linked to the American Robin
    # taxon (this is the row ``resolve_species_display_name`` case 1
    # returns for the label "Robin").
    root = db.add_keyword("Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["American Robin"], root),
    )
    # A hierarchy leaf also named "Robin" but linked to a DIFFERENT taxon
    # (European Robin), plus a canonical root for that taxon whose
    # stored spelling differs from the prediction label.
    parent = db.add_keyword("Old World")
    leaf = db.add_keyword("Robin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (taxa["European Robin"], leaf),
    )
    european_root = db.add_keyword("European Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (taxa["European Robin"], european_root),
    )
    # The photo carries only the American Robin root — its taxon matches
    # the "Robin" root and highlights saved under "Robin" belong to it.
    db.tag_photo(pid, root)
    # A prediction on the photo carries the shared label "Robin"; the
    # reload canonicalization runs against this string.
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) "
        "VALUES (?, 'test', 'fp1', 'Robin', 0.95)",
        (did,),
    )
    # ``add_species_highlight`` funnels through
    # ``resolve_species_display_name``, which returns "Robin" (root-first)
    # for the label "Robin".
    db.add_species_highlight("Robin", pid)
    db.conn.commit()

    highlights = db.get_species_highlights(
        species="Robin", eligible_only=True,
    )
    assert pid in highlights.get("Robin", {}), (
        "highlight stored under root 'Robin' must remain eligible when "
        "the prediction label 'Robin' also matches a hierarchy leaf "
        "pointing at a different taxon — the reload path must prefer the "
        "same-name root rather than canonicalizing through the leaf"
    )


def test_curation_eligibility_higher_rank_taxonomy_homonym(tmp_path):
    """A linked higher-rank taxonomy keyword (e.g. a genus row named
    ``Puma``) is eligible for the identification-name-keyed
    representative curation row that shares its stored spelling, but
    not for the species-only ordered-highlights row keyed on the same
    name.

    Representative eligibility mirrors the widened
    :meth:`get_life_list_candidates`, :meth:`get_photo_life_list_species`,
    and ``_photo_can_be_life_list_preference`` write path — the Life
    List renders higher-rank identifications so their photos must
    survive the ``eligible_only=True`` read that ``GET /api/photos/<id>``
    uses to decide whether the shared Set-Representative button is
    current. ``get_species_highlights`` remains species-only, because
    the Highlights page still only surfaces species buckets.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    db.conn.executemany(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (?, ?, ?, ?, 'Animalia')",
        [
            (46272, "Puma", "Puma", "genus"),
            (41963, "Puma concolor", "Puma", "species"),
        ],
    )
    db.conn.commit()
    genus_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'genus'"
    ).fetchone()["id"]

    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # A pre-existing keyword row named "Puma" bound to the genus (as if
    # stamped by a legacy catalog before ``prefer_species`` existed and
    # not yet rebound). ``mark_species_keywords`` would rebind it during
    # a startup pass, but until then it stays higher-rank while still
    # flagged is_species/taxonomy.
    kid = db.add_keyword("Puma")
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 1, taxon_id = ? "
        "WHERE id = ?",
        (genus_taxon, kid),
    )
    db.tag_photo(pid, kid)

    # Curation rows stored under the "Puma" identification key.
    db.set_species_representative("Puma", pid)
    db.add_species_highlight("Puma", pid)
    db.conn.commit()

    # Representative eligibility matches the widened Life List siblings:
    # the higher-rank homonym is a valid representative for the "Puma"
    # entry the user actually renders under on the Life List.
    reps = db.get_species_representative_lists(eligible_only=True)
    assert reps.get("Puma") == [pid], (
        "higher-rank homonym 'Puma' must satisfy 'Puma' representative "
        "eligibility, matching the widened life-list write path"
    )
    assert db.get_species_representatives(eligible_only=True) == {"Puma": pid}

    # Highlights remain species-only. The Highlights page surfaces
    # species buckets, so a genus-rank keyword named 'Puma' must not
    # keep the species 'Puma' highlight eligible.
    highlights = db.get_species_highlights(eligible_only=True)
    assert pid not in highlights.get("Puma", {}), (
        "genus-rank taxonomy row named 'Puma' must not satisfy species "
        "'Puma' highlight eligibility"
    )
    single = db.get_species_highlights(species="Puma", eligible_only=True)
    assert pid not in single.get("Puma", {})

    # Sanity check: when the row is rebound to the species-rank taxon,
    # even the species-only highlight eligibility flips on.
    species_taxon = db.conn.execute(
        "SELECT id FROM taxa WHERE rank = 'species'"
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (species_taxon, kid),
    )
    db.conn.commit()

    reps_after = db.get_species_representative_lists(eligible_only=True)
    assert reps_after.get("Puma") == [pid]
    highlights_after = db.get_species_highlights(eligible_only=True)
    assert pid in highlights_after.get("Puma", {})


def test_accept_prediction_replace_migrates_each_case_variant_curation_row(tmp_path):
    """Replace Keywords must migrate curation for every distinct removed
    row's exact spelling. Legacy ``Robin`` and taxonomy ``robin`` coexist
    as separate keyword rows (the ``UNIQUE(name, parent_id)`` index on
    ``keywords`` is case-sensitive), and both can carry
    ``species_highlights`` / ``photo_preferences`` rows keyed by the
    exact stored name. Deduping the curation-source list by
    ``keyword_match_key`` — an ASCII-only NOCASE fold — collapses those
    two spellings into one, so only one migrates onto the new species
    and the other stays stranded under a species keyword the photo no
    longer carries after ``replace_species``.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._ws_id()
    fid = db.add_folder("/photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Two DISTINCT NULL-taxon species keyword rows differing only by
    # ASCII case. Insert directly to bypass ``add_keyword``'s NOCASE
    # dedupe and confirm ``keywords`` allows both to coexist.
    upper_id = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) "
        "VALUES ('Robin', NULL, 1, 'general')"
    ).lastrowid
    lower_id = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) "
        "VALUES ('robin', NULL, 1, 'general')"
    ).lastrowid
    db.tag_photo(pid, upper_id)
    db.tag_photo(pid, lower_id)

    # Curation rows keyed to BOTH exact stored names. ``species_highlights``
    # has PK (workspace_id, species, photo_id) with case-sensitive TEXT,
    # so both can coexist.
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'Robin', ?, 0)",
        (ws_id, pid),
    )
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'robin', ?, 1)",
        (ws_id, pid),
    )
    db.conn.commit()

    detection_id = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(detection_id, "Sparrow", 0.95, "bioclip")
    pred_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?",
        (detection_id,),
    ).fetchone()["id"]

    db.accept_prediction(pred_id, replace_species=True)

    # Both stale Robin rows must have been detached.
    tagged_ids = {row["id"] for row in db.get_photo_keywords(pid)}
    assert upper_id not in tagged_ids
    assert lower_id not in tagged_ids

    remaining_species = {
        row["species"] for row in db.conn.execute(
            "SELECT species FROM species_highlights "
            "WHERE workspace_id = ? AND photo_id = ?",
            (ws_id, pid),
        ).fetchall()
    }
    # Under the ``keyword_match_key`` dedup only one of ``Robin`` /
    # ``robin`` would land in curation_sources, so the other exact
    # spelling would still appear here. Neither may survive: both
    # highlight rows must migrate onto the new species.
    assert "Robin" not in remaining_species, (
        "'Robin' highlight row must have migrated onto 'Sparrow'"
    )
    assert "robin" not in remaining_species, (
        "'robin' highlight row must have migrated onto 'Sparrow' — "
        "an ASCII case-fold dedup would collapse 'Robin' and 'robin' "
        "so only one of the two distinct spellings got renamed"
    )
    assert "Sparrow" in remaining_species


# ---------------------------------------------------------------------------
# Universal filter engine (Phase 1) — new fields, ops, and value suggestions.
# Design: docs/plans/2026-07-19-universal-filters-design.md
# ---------------------------------------------------------------------------


def _filter_db(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    return db, fid


def test_universal_filter_numeric_fields_and_between(tmp_path):
    db, fid = _filter_db(tmp_path)
    small = db.add_photo(folder_id=fid, filename='small.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    big = db.add_photo(folder_id=fid, filename='big.jpg', extension='.jpg',
                       file_size=9000, file_mtime=1.0)
    db.conn.execute(
        "UPDATE photos SET width=8640, height=5760, focal_length=600, "
        "iso=3200, aperture=6.3, shutter_speed=0.0004 WHERE id=?", (big,))
    db.conn.execute(
        "UPDATE photos SET width=4000, height=3000, focal_length=300, "
        "iso=200, aperture=2.8, shutter_speed=0.008 WHERE id=?", (small,))
    db.conn.commit()

    count = db.count_photos_for_rules
    assert count([{"field": "file_size", "op": ">", "value": 500}]) == 1
    assert count([{"field": "width", "op": ">=", "value": 8000}]) == 1
    assert count([{"field": "height", "op": "<", "value": 4000}]) == 1
    assert count([{"field": "iso", "op": "between", "value": [100, 400]}]) == 1
    assert count([{"field": "focal_length", "op": "between", "value": [200, 700]}]) == 2
    assert count([{"field": "aperture", "op": "is", "value": 2.8}]) == 1
    assert count([{"field": "shutter_speed", "op": "<", "value": 0.001}]) == 1
    # rating between rides the same generalized numeric path
    db.update_photo_rating(small, 2)
    db.update_photo_rating(big, 5)
    assert count([{"field": "rating", "op": "between", "value": [4, 5]}]) == 1


def test_universal_filter_filename_text_ops(tmp_path):
    db, fid = _filter_db(tmp_path)
    db.add_photo(folder_id=fid, filename='Owl_2101.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename='eagle_2102.cr3', extension='.cr3',
                 file_size=100, file_mtime=1.0)

    count = db.count_photos_for_rules
    assert count([{"field": "filename", "op": "contains", "value": "owl"}]) == 1
    assert count([{"field": "filename", "op": "contains", "value": "owl",
                   "case": True}]) == 0
    assert count([{"field": "filename", "op": "contains", "value": "Owl",
                   "case": True}]) == 1
    assert count([{"field": "filename", "op": "not_contains", "value": "owl"}]) == 1
    assert count([{"field": "filename", "op": "starts_with", "value": "eagle"}]) == 1
    assert count([{"field": "filename", "op": "ends_with", "value": ".cr3"}]) == 1
    assert count([{"field": "filename", "op": "is", "value": "owl_2101.jpg"}]) == 1
    assert count([{"field": "filename", "op": "is", "value": "owl_2101.jpg",
                   "case": True}]) == 0
    # LIKE wildcards in user input must be literal
    assert count([{"field": "filename", "op": "contains", "value": "%"}]) == 0


def test_universal_filter_flag_null_is_unflagged(tmp_path):
    """Legacy rows store NULL for unflagged; the Unflagged chip must still
    return them (plan step 4 regression guard)."""
    db, fid = _filter_db(tmp_path)
    null_flag = db.add_photo(folder_id=fid, filename='legacy.jpg', extension='.jpg',
                             file_size=100, file_mtime=1.0)
    picked = db.add_photo(folder_id=fid, filename='picked.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET flag=NULL WHERE id=?", (null_flag,))
    db.conn.execute("UPDATE photos SET flag='flagged' WHERE id=?", (picked,))
    db.conn.commit()

    count = db.count_photos_for_rules
    assert count([{"field": "flag", "op": "is", "value": "none"}]) == 1
    assert count([{"field": "flag", "op": "is not", "value": "none"}]) == 1
    assert count([{"field": "flag", "op": "in", "value": ["none", "flagged"]}]) == 2
    assert count([{"field": "flag", "op": "in", "value": ["rejected"]}]) == 0
    assert count([{"field": "flag", "op": "not_in", "value": ["flagged"]}]) == 1


def test_universal_filter_in_not_in_enums(tmp_path):
    db, fid = _filter_db(tmp_path)
    red = db.add_photo(folder_id=fid, filename='red.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    yellow = db.add_photo(folder_id=fid, filename='yellow.cr3', extension='.cr3',
                          file_size=100, file_mtime=1.0)
    plain = db.add_photo(folder_id=fid, filename='plain.nef', extension='.nef',
                         file_size=100, file_mtime=1.0)
    ws = db._ws_id()
    db.conn.execute(
        "INSERT INTO photo_color_labels(photo_id, workspace_id, color) VALUES (?,?,?)",
        (red, ws, 'red'))
    db.conn.execute(
        "INSERT INTO photo_color_labels(photo_id, workspace_id, color) VALUES (?,?,?)",
        (yellow, ws, 'yellow'))
    db.conn.commit()

    count = db.count_photos_for_rules
    assert count([{"field": "color_label", "op": "in", "value": ["red", "yellow"]}]) == 2
    assert count([{"field": "color_label", "op": "in", "value": ["green"]}]) == 0
    # not_in: photos without any label from the set — unlabeled photos match
    assert count([{"field": "color_label", "op": "not_in", "value": ["red"]}]) == 2
    assert count([{"field": "extension", "op": "in", "value": [".jpg", ".NEF"]}]) == 2
    assert count([{"field": "extension", "op": "not_in", "value": [".cr3"]}]) == 2
    # empty selections: in [] matches nothing, not_in [] excludes nothing
    assert count([{"field": "flag", "op": "in", "value": []}]) == 0
    assert count([{"field": "flag", "op": "not_in", "value": []}]) == 3


def test_universal_filter_empty_in_preserves_any_none_semantics(tmp_path):
    """Constant-true/false leaves must stay in their group; dropping them
    would invert any/none semantics (prototype review regression)."""
    db, fid = _filter_db(tmp_path)
    p = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=100, file_mtime=1.0)
    db.update_photo_rating(p, 5)

    count = db.count_photos_for_rules
    # any(in [], rating >= 4): first clause false, second true -> matches
    assert count({"mode": "any", "rules": [
        {"field": "flag", "op": "in", "value": []},
        {"field": "rating", "op": ">=", "value": 4},
    ]}) == 1
    # none(not_in []): the clause is always true -> none-group matches nothing
    assert count({"mode": "none", "rules": [
        {"field": "flag", "op": "not_in", "value": []},
    ]}) == 0


def test_universal_filter_timestamp_recent_and_comparisons(tmp_path):
    from datetime import UTC, datetime, timedelta
    db, fid = _filter_db(tmp_path)
    fresh = db.add_photo(folder_id=fid, filename='fresh.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    old = db.add_photo(folder_id=fid, filename='old.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    now = datetime.now(UTC)
    # Scanner writes ``datetime.isoformat()``, so timestamps carry the ``T``
    # separator in real DBs. The recent-cutoff must be formatted to match;
    # a space-separated cutoff against a T-separated timestamp is a lexical
    # mismatch where every photo on the cutoff day slips past ``>=`` (``T``
    # sorts after space), regressing "last N days" by nearly a day.
    db.conn.execute("UPDATE photos SET timestamp=? WHERE id=?",
                    ((now - timedelta(days=2)).isoformat(timespec='seconds'), fresh))
    db.conn.execute("UPDATE photos SET timestamp=? WHERE id=?",
                    ('2020-06-01T12:00:00', old))
    db.conn.commit()

    count = db.count_photos_for_rules
    assert count([{"field": "timestamp", "op": "recent",
                   "value": {"n": 7, "unit": "days"}}]) == 1
    assert count([{"field": "timestamp", "op": "recent",
                   "value": {"n": 1, "unit": "days"}}]) == 0
    assert count([{"field": "timestamp", "op": "recent",
                   "value": {"n": 10, "unit": "years"}}]) == 2
    assert count([{"field": "timestamp", "op": ">=", "value": "2021-01-01"}]) == 1
    assert count([{"field": "timestamp", "op": "<", "value": "2021-01-01"}]) == 1
    # <= is inclusive of the named day
    assert count([{"field": "timestamp", "op": "<=", "value": "2020-06-01"}]) == 1


def test_universal_filter_timestamp_precise_upper_bounds_stay_exact(tmp_path):
    """``<=`` and ``between`` upper bounds only pad bare ``YYYY-MM-DD``.

    A precise-instant upper ``2024-01-01T12:00:00`` means exactly that
    instant, not the whole clock second — padding it to ``.999999``
    would spuriously include ``12:00:00.500000`` photos that are strictly
    after the requested instant. Bare-date bounds still cover the full
    named day so the UI's ``<input type="date">`` output stays inclusive.
    """
    db, fid = _filter_db(tmp_path)
    early = db.add_photo(folder_id=fid, filename='early.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    on_second = db.add_photo(folder_id=fid, filename='on.jpg', extension='.jpg',
                             file_size=100, file_mtime=1.0)
    subsec = db.add_photo(folder_id=fid, filename='subsec.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET timestamp=? WHERE id=?",
                    ('2024-01-01T11:59:59', early))
    db.conn.execute("UPDATE photos SET timestamp=? WHERE id=?",
                    ('2024-01-01T12:00:00', on_second))
    db.conn.execute("UPDATE photos SET timestamp=? WHERE id=?",
                    ('2024-01-01T12:00:00.500000', subsec))
    db.conn.commit()

    count = db.count_photos_for_rules
    # ``<=`` on a precise instant is exact: matches the pre-instant photo
    # and the instant itself, excludes ``.500000`` which is strictly after.
    assert count([{"field": "timestamp", "op": "<=",
                   "value": "2024-01-01T12:00:00"}]) == 2
    # Same for ``between`` — precise upper excludes sub-second photos
    # in the same clock second.
    assert count([{"field": "timestamp", "op": "between",
                   "value": ["2024-01-01T00:00:00",
                             "2024-01-01T12:00:00"]}]) == 2
    # Bare-date upper still covers the whole day.
    assert count([{"field": "timestamp", "op": "<=",
                   "value": "2024-01-01"}]) == 3
    assert count([{"field": "timestamp", "op": "between",
                   "value": ["2024-01-01", "2024-01-01"]}]) == 3


def test_universal_filter_recent_cutoff_matches_iso_separator(tmp_path):
    """Regression: the ``recent`` cutoff must use the same ``T`` separator
    as stored timestamps. Before the fix the cutoff came from SQLite's
    ``datetime('now', ?)`` which returns ``YYYY-MM-DD HH:MM:SS`` (space
    separator); a lexical ``>=`` against ``YYYY-MM-DDTHH:MM:SS`` treated
    every photo on the cutoff day as recent because ``T`` (0x54) sorts
    after ``' '`` (0x20). Place the photo on the cutoff day *before* the
    cutoff clock time so the mismatch flips the answer.
    """
    from datetime import UTC, datetime, timedelta
    db, fid = _filter_db(tmp_path)
    boundary = db.add_photo(folder_id=fid, filename='boundary.jpg',
                            extension='.jpg', file_size=100, file_mtime=1.0)
    # 5-days-ago at 00:05 UTC. The ``recent 5 days`` cutoff lands 5 days
    # ago at the *current* clock time — so the photo is on the cutoff day
    # but strictly earlier than the cutoff, and its true age is > 5 days.
    # Correct answer: excluded. Bug behavior: the T/space mismatch makes
    # the photo lexically sort after the space-separated cutoff and the
    # rule wrongly includes it. Skip if the test happens to run in the
    # first minute of a UTC day (where cutoff clock time is <= 00:05 and
    # the boundary is legitimately within the window).
    now = datetime.now(UTC)
    if now.hour == 0 and now.minute <= 5:
        pytest.skip("cutoff clock time <= photo time — inconclusive boundary")
    boundary_ts = (
        (now - timedelta(days=5))
        .replace(hour=0, minute=5, second=0, microsecond=0)
        .isoformat(timespec='seconds')
    )
    db.conn.execute("UPDATE photos SET timestamp=? WHERE id=?",
                    (boundary_ts, boundary))
    db.conn.commit()

    count = db.count_photos_for_rules
    # A photo whose true age is > 5 days must not match "recent 5 days".
    assert count([{"field": "timestamp", "op": "recent",
                   "value": {"n": 5, "unit": "days"}}]) == 0
    # ``recent_days`` shares the cutoff formatting — guard it too.
    assert count([{"field": "timestamp", "op": "recent_days",
                   "value": 5}]) == 0
    # A wider window still matches, sanity-check.
    assert count([{"field": "timestamp", "op": "recent",
                   "value": {"n": 30, "unit": "days"}}]) == 1


def test_universal_filter_species_any_match(tmp_path):
    """A multi-species photo matches when ANY of its species matches."""
    db, fid = _filter_db(tmp_path)
    multi = db.add_photo(folder_id=fid, filename='multi.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    single = db.add_photo(folder_id=fid, filename='single.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    heron = db.add_keyword('Great Blue Heron', is_species=True)
    egret = db.add_keyword('Snowy Egret', is_species=True)
    plain = db.add_keyword('wetlands')
    db.tag_photo(multi, heron)
    db.tag_photo(multi, egret)
    db.tag_photo(single, heron)
    db.tag_photo(single, plain)

    count = db.count_photos_for_rules
    assert count([{"field": "species", "op": "is", "value": "Snowy Egret"}]) == 1
    assert count([{"field": "species", "op": "is", "value": "Great Blue Heron"}]) == 2
    assert count([{"field": "species", "op": "contains", "value": "egret"}]) == 1
    assert count([{"field": "species", "op": "is not", "value": "Snowy Egret"}]) == 1
    # non-species keywords never match the species field
    assert count([{"field": "species", "op": "contains", "value": "wetlands"}]) == 0


def test_universal_filter_species_contains_escapes_like_metacharacters(tmp_path):
    """``contains``/``not_contains`` on species must treat ``%``/``_`` in the
    value as literal characters — otherwise a rule like
    ``{"field":"species","op":"contains","value":"%"}`` matches every
    species-tagged photo instead of only species whose name literally
    contains ``%``. The other text/folder filters already escape LIKE
    metacharacters; species did not, so a client could bypass the filter
    by passing a wildcard."""
    db, fid = _filter_db(tmp_path)
    only = db.add_photo(folder_id=fid, filename='only.jpg', extension='.jpg',
                        file_size=100, file_mtime=1.0)
    heron = db.add_keyword('Great Blue Heron', is_species=True)
    db.tag_photo(only, heron)

    count = db.count_photos_for_rules
    # A bare ``%`` used to match every species-tagged photo; after the fix
    # it matches only species whose name literally contains ``%``.
    assert count([{"field": "species", "op": "contains", "value": "%"}]) == 0
    assert count([{"field": "species", "op": "contains", "value": "_"}]) == 0
    # ``not_contains`` with a wildcard used to exclude every species-tagged
    # photo; the escaped form leaves them included (nothing literally
    # contains ``%``).
    assert count([{"field": "species", "op": "not_contains", "value": "%"}]) == 1
    # Sanity: normal literal substring still works.
    assert count([{"field": "species", "op": "contains", "value": "Heron"}]) == 1


def test_universal_filter_keyword_contains_escapes_like_metacharacters(tmp_path):
    """``keyword contains`` must treat ``%``/``_`` in the value as literal
    characters, matching the species/filename/camera text rules. Otherwise a
    request like ``{"field":"keyword","op":"contains","value":"%"}`` matches
    every keyworded photo — the exact wildcard-bypass hole the registry now
    advertises to clients through ``/api/filters/fields``."""
    db, fid = _filter_db(tmp_path)
    tagged = db.add_photo(folder_id=fid, filename='tagged.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    untagged = db.add_photo(folder_id=fid, filename='untagged.jpg', extension='.jpg',
                            file_size=100, file_mtime=1.0)
    k = db.add_keyword('Red-tailed hawk')
    db.tag_photo(tagged, k)

    count = db.count_photos_for_rules
    # A bare ``%``/``_`` used to match every keyworded photo; after the fix
    # they only match keywords literally containing the metacharacter.
    assert count([{"field": "keyword", "op": "contains", "value": "%"}]) == 0
    assert count([{"field": "keyword", "op": "contains", "value": "_"}]) == 0
    # ``not_contains`` with a bare wildcard used to exclude every keyworded
    # photo; escaped, it leaves them included and excludes only the
    # untagged photo (no keyword row exists to match).
    assert count([{"field": "keyword", "op": "not_contains", "value": "%"}]) == 2
    # Sanity: literal substrings still match normally.
    assert count([{"field": "keyword", "op": "contains", "value": "hawk"}]) == 1
    assert count([{"field": "keyword", "op": "contains", "value": "sparrow"}]) == 0


def test_universal_filter_species_matches_root_via_taxon_of_hierarchy_leaf(tmp_path):
    """Species rules resolve through taxon identity — a photo tagged only
    with a hierarchy leaf whose linked taxon has a same-taxon top-level root
    still matches the root's name. Mirrors how
    ``get_species_keywords_for_photos`` canonicalizes to the root spelling,
    so the universal filter agrees with the species names shown in Browse,
    Compare, and life-list views."""
    db, fid = _filter_db(tmp_path)
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    pid = db.add_photo(folder_id=fid, filename='leaf.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    other = db.add_photo(folder_id=fid, filename='other.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    # A photo tagged only with a hierarchy leaf whose taxon links to the
    # top-level root "Verdin". Leaf spelling ("Auriparus flaviceps") is
    # distinct from root ("Verdin"), so a raw ``k.name`` predicate cannot
    # rescue the match — canonicalization through ``taxon_id`` is required.
    parent = db.add_keyword("Penduline tits")
    leaf = db.add_keyword("Auriparus flaviceps", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type='taxonomy', is_species=1, taxon_id=? "
        "WHERE id=?", (taxa["Verdin"], leaf))
    db.add_keyword("Verdin", is_species=True)  # auto-links taxon_id
    db.conn.commit()
    db.tag_photo(pid, leaf)

    # Sanity check: the canonical species surfaced elsewhere is "Verdin".
    assert db.get_species_keywords_for_photos([pid]) == {pid: ["Verdin"]}

    count = db.count_photos_for_rules
    # `is`/`equals` on the root name matches the taxon-linked leaf.
    assert count([{"field": "species", "op": "is", "value": "Verdin"}]) == 1
    assert count([{"field": "species", "op": "equals", "value": "Verdin"}]) == 1
    # `contains` also follows the root-name canonicalization: the leaf name
    # does not contain "erdi", but the root name does.
    assert count([{"field": "species", "op": "contains", "value": "erdi"}]) == 1
    # A non-matching species stays non-matching.
    assert count([{"field": "species", "op": "is", "value": "Nope"}]) == 0
    # `is not` / `not_contains` correctly exclude the taxon-matched photo
    # and include the untagged one (2 photos - 1 match = 1).
    assert count([{"field": "species", "op": "is not", "value": "Verdin"}]) == 1
    assert count([{"field": "species", "op": "not_contains", "value": "erdi"}]) == 1
    _ = other  # keep the second photo alive for the negative assertions.


def test_universal_filter_workflow_fields(tmp_path):
    db, fid = _filter_db(tmp_path)
    edited = db.add_photo(folder_id=fid, filename='edited.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    indexed = db.add_photo(folder_id=fid, filename='indexed.jpg', extension='.jpg',
                           file_size=100, file_mtime=1.0)
    burst = db.add_photo(folder_id=fid, filename='burst.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    db.set_photo_edit_recipe(edited, {"rotation": 90})
    db.conn.execute(
        "INSERT INTO photo_embeddings(photo_id, model, variant, embedding) "
        "VALUES (?,?,?,?)", (indexed, 'clip-vit', '', b'\x01'))
    db.conn.execute("UPDATE photos SET burst_id='B42' WHERE id=?", (burst,))
    db.conn.execute("UPDATE photos SET file_hash='abc123' WHERE id=?", (burst,))
    db.conn.commit()

    count = db.count_photos_for_rules
    assert count([{"field": "has_edits", "op": "is", "value": 1}]) == 1
    assert count([{"field": "has_edits", "op": "is", "value": 0}]) == 2
    assert count([{"field": "has_visual_index", "op": "is", "value": 1}]) == 1
    assert count([{"field": "has_visual_index", "op": "is", "value": 1,
                   "model": "clip-vit"}]) == 1
    assert count([{"field": "has_visual_index", "op": "is", "value": 1,
                   "model": "other-model"}]) == 0
    # An unsupported op on a boolean field must raise, not silently return
    # a truthy predicate — the API layer catches ValueError → 400 so
    # malformed requests surface as validation errors instead of a 200
    # with unfiltered rows. Every registry-advertised boolean field must
    # fail-closed the same way, not just ``has_visual_index``.
    for bad in (
        {"field": "has_visual_index", "op": "contains", "value": 1},
        {"field": "has_edits", "op": "contains", "value": 1},
        {"field": "in_burst", "op": "starts_with", "value": 1},
        {"field": "has_gps", "op": "between", "value": [0, 1]},
        {"field": "has_location_keyword", "op": "contains", "value": 1},
        {"field": "is_duplicate", "op": ">=", "value": 1},
    ):
        with pytest.raises(ValueError):
            count([bad])
    # A non-boolean value must also fail-closed: without this guard, values
    # outside ``_truthy``'s whitelist (True/1/"1"/"true") silently fall to
    # the negative branch, so ``has_gps is "yes"`` would quietly return the
    # ``is false`` set instead of the reject-as-400 the other malformed
    # boolean rules get.
    for bad in (
        {"field": "has_edits", "op": "is", "value": "yes"},
        {"field": "has_gps", "op": "is", "value": "maybe"},
        {"field": "has_visual_index", "op": "is not", "value": "sometimes"},
        {"field": "in_burst", "op": "equals", "value": 2},
    ):
        with pytest.raises(ValueError):
            count([bad])
    assert count([{"field": "in_burst", "op": "is", "value": 1}]) == 1
    assert count([{"field": "burst_id", "op": "is", "value": "B42"}]) == 1
    assert count([{"field": "duplicate_group", "op": "is", "value": "abc123"}]) == 1


def test_is_duplicate_sees_cross_workspace_partners(tmp_path):
    """``is_duplicate`` must match a photo whose only duplicate lives in
    another workspace — otherwise Browse hides members that the Duplicates
    workflow (``find_duplicate_groups``, which is catalog-wide by
    ``file_hash``) will still act on.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    active_ws = db._ws_id()
    other_ws = db.create_workspace('Other')

    active_folder = db.add_folder('/active', name='active')
    other_folder = db.add_folder('/other', name='other')
    db.remove_workspace_folder(active_ws, other_folder)
    db.add_workspace_folder(other_ws, other_folder)

    here = db.add_photo(folder_id=active_folder, filename='here.jpg',
                        extension='.jpg', file_size=100, file_mtime=1.0)
    there = db.add_photo(folder_id=other_folder, filename='there.jpg',
                         extension='.jpg', file_size=100, file_mtime=2.0)
    lone = db.add_photo(folder_id=active_folder, filename='lone.jpg',
                        extension='.jpg', file_size=100, file_mtime=3.0)
    db.conn.execute("UPDATE photos SET file_hash='shared' WHERE id IN (?, ?)",
                    (here, there))
    db.conn.execute("UPDATE photos SET file_hash='unique' WHERE id=?", (lone,))
    db.conn.commit()

    count = db.count_photos_for_rules
    # ``here`` is only visible in the active workspace; ``there`` sits in the
    # other workspace. Under the old workspace-scoped subquery, ``here`` would
    # count as 0 duplicates. Catalog-wide, it must show as a duplicate member.
    assert count([{"field": "is_duplicate", "op": "is", "value": 1}]) == 1
    assert count([{"field": "is_duplicate", "op": "is", "value": 0}]) == 1

    # Rejecting the cross-workspace partner drops the pair, matching
    # find_duplicate_groups' rejected-flag filter.
    db.conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (there,))
    db.conn.commit()
    assert count([{"field": "is_duplicate", "op": "is", "value": 1}]) == 0
    assert count([{"field": "is_duplicate", "op": "is", "value": 0}]) == 2


def test_universal_filter_prediction_rules_pin_current_fingerprint(tmp_path):
    """Universal-filter rules that consult ``predictions`` (prediction_status,
    prediction_confidence, classifier_model, taxonomy_*) must only match the
    most recent ``labels_fingerprint`` per (detection, classifier_model) —
    matching how the dashboard and review UI decide which prediction row is
    "current". Without pinning, an older accepted row keeps the photo in
    ``prediction_status is accepted`` after a rerun classifier writes a new
    fingerprint with a different (still-pending) verdict.
    """
    db, fid = _filter_db(tmp_path)
    ws_id = db._ws_id()
    photo = db.add_photo(folder_id=fid, filename='p.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    det = db.save_detections(photo, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="MDV6")[0]
    # Stale-fingerprint prediction accepted under an old label set.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Robin', 0.9, '2026-01-01')",
        (det,),
    )
    stale_pred = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id=? AND labels_fingerprint='fp-old'",
        (det,),
    ).fetchone()["id"]
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status, reviewed_at) "
        "VALUES (?, ?, 'accepted', '2026-01-02')",
        (stale_pred, ws_id),
    )
    # Current-fingerprint prediction — still pending.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Finch', 0.55, '2026-04-24')",
        (det,),
    )
    db.conn.commit()

    count = db.count_photos_for_rules
    # Accept lives only on the stale fingerprint → filter must exclude it.
    assert count([{"field": "prediction_status", "op": "is",
                   "value": "accepted"}]) == 0
    # Current fingerprint is pending → visible.
    assert count([{"field": "prediction_status", "op": "is",
                   "value": "pending"}]) == 1
    # Confidence + classifier_model + taxonomy filters read pred.* so all
    # ride the same pin: the stale 0.9 must not satisfy a >=0.8 rule.
    assert count([{"field": "prediction_confidence", "op": ">=",
                   "value": 0.8}]) == 0
    assert count([{"field": "prediction_confidence", "op": ">=",
                   "value": 0.5}]) == 1


def test_universal_filter_prediction_rules_gate_by_detector_confidence(tmp_path, monkeypatch):
    """Universal-filter rules that consult ``predictions`` (prediction_status,
    prediction_confidence, classifier_model, taxonomy_*) must apply the
    workspace-effective ``detector_confidence`` floor so a prediction on a
    below-threshold hidden detection can't satisfy a rule that Browse would
    show no detection context for. Mirrors get_detections_for_photos() and
    the dashboard prediction counters, which both filter by the threshold.
    """
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({"detector_confidence": 0.5})

    db, fid = _filter_db(tmp_path)
    # Photo A: above-threshold detection carrying a current prediction —
    # should match every advertised prediction rule.
    a = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=100, file_mtime=1.0)
    det_a = db.save_detections(a, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="MDV6")[0]
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp', 'Robin', 0.85, '2026-01-01')",
        (det_a,),
    )
    # Photo B: ONLY a below-threshold detection with an otherwise-matching
    # prediction — Browse hides the detection at threshold=0.5, so the
    # prediction filters must hide the photo too.
    b = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg',
                     file_size=100, file_mtime=1.0)
    det_b = db.save_detections(b, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.05,
         "category": "animal"},
    ], detector_model="MDV6")[0]
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp', 'Robin', 0.85, '2026-01-01')",
        (det_b,),
    )
    db.conn.commit()

    count = db.count_photos_for_rules
    # prediction_confidence, classifier_model, and prediction_status all
    # route through the shared _prediction_exists helper — one gate covers
    # them all. Photo A qualifies; Photo B is hidden by the threshold.
    assert count([{"field": "prediction_confidence", "op": ">=",
                   "value": 0.8}]) == 1
    assert count([{"field": "classifier_model", "op": "is",
                   "value": "bioclip-2"}]) == 1
    assert count([{"field": "prediction_status", "op": "is",
                   "value": "pending"}]) == 1


def test_universal_filter_prediction_rules_ignore_alternative_rows(tmp_path):
    """Filters that represent the displayed prediction — prediction_confidence,
    classifier_model, taxonomy_* — must ignore runner-up predictions stored
    with ``prediction_review.status = 'alternative'``. /api/predictions drops
    alternatives from top-level results (app.py:12386-12388) and Compare
    hides them, so a top pick at 0.95 with an alternative at 0.10 must not
    satisfy ``prediction_confidence <= 0.2``.
    """
    db, fid = _filter_db(tmp_path)
    ws_id = db._ws_id()
    photo = db.add_photo(folder_id=fid, filename='p.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    det = db.save_detections(photo, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="MDV6")[0]
    # Displayed top prediction — high confidence.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp', 'Robin', 0.95, '2026-01-01')",
        (det,),
    )
    # Runner-up alternative — low confidence, marked alternative in review.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp', 'Sparrow', 0.10, '2026-01-01')",
        (det,),
    )
    alt_pred = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id=? AND species='Sparrow'",
        (det,),
    ).fetchone()["id"]
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status, reviewed_at) "
        "VALUES (?, ?, 'alternative', '2026-01-02')",
        (alt_pred, ws_id),
    )
    db.conn.commit()

    count = db.count_photos_for_rules
    # The alternative sits at 0.10 but must not drag the photo into a
    # <=0.2 confidence bucket — the displayed 0.95 pick is what matters.
    assert count([{"field": "prediction_confidence", "op": "<=",
                   "value": 0.2}]) == 0
    # The displayed pick still passes a >=0.9 rule.
    assert count([{"field": "prediction_confidence", "op": ">=",
                   "value": 0.9}]) == 1
    # classifier_model still resolves via the top pick (still present, not
    # excluded — alternative sits under the same model).
    assert count([{"field": "classifier_model", "op": "is",
                   "value": "bioclip-2"}]) == 1


def test_universal_filter_classifier_model_contains_escapes_like_metacharacters(tmp_path):
    """``classifier_model contains`` must treat ``%`` / ``_`` in the value as
    literal characters, matching the other advertised text contains rules.
    Otherwise ``{"field":"classifier_model","op":"contains","value":"%"}``
    would match every classified photo — the wildcard-bypass hole the
    registry now advertises to clients through ``/api/filters/fields``."""
    db, fid = _filter_db(tmp_path)
    photo = db.add_photo(folder_id=fid, filename='p.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    det = db.save_detections(photo, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="MDV6")[0]
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp1', 'Robin', 0.9, '2026-01-01')",
        (det,),
    )
    db.conn.commit()

    count = db.count_photos_for_rules
    # A bare ``%`` / ``_`` used to match every classified photo; escaped,
    # they only match model strings that literally contain the metacharacter.
    assert count([{"field": "classifier_model", "op": "contains",
                   "value": "%"}]) == 0
    assert count([{"field": "classifier_model", "op": "contains",
                   "value": "_"}]) == 0
    # Sanity: a literal substring still matches.
    assert count([{"field": "classifier_model", "op": "contains",
                   "value": "bioclip"}]) == 1


def test_universal_filter_validation_errors(tmp_path):
    import pytest
    db, _ = _filter_db(tmp_path)
    count = db.count_photos_for_rules

    with pytest.raises(ValueError):
        count([{"field": "flag", "op": "in", "value": "none"}])  # not a list
    with pytest.raises(ValueError):
        count([{"field": "iso", "op": "between", "value": [100]}])  # wrong arity
    with pytest.raises(ValueError):
        count([{"field": "timestamp", "op": "recent", "value": 30}])  # not a dict
    with pytest.raises(ValueError):
        count([{"field": "timestamp", "op": "recent",
                "value": {"n": 0, "unit": "days"}}])
    with pytest.raises(ValueError):
        count([{"field": "timestamp", "op": "recent",
                "value": {"n": 3, "unit": "fortnights"}}])
    with pytest.raises(ValueError):
        count([{"field": "no_such_field", "op": "is", "value": 1}])


def test_registry_ops_all_compile(tmp_path):
    """Every field/op combination the registry advertises must build SQL —
    the registry and the engine share this test so they cannot drift."""
    from filter_fields import FILTER_FIELDS
    db, _ = _filter_db(tmp_path)

    sample_values = {
        "text": "x", "number": 1, "rating": 3, "date": "2024-01-01",
        "boolean": 1, "enum": None, "folder": "/photos",
    }
    for key, spec in FILTER_FIELDS.items():
        for op in spec["ops"]:
            value = sample_values[spec["type"]]
            if spec["type"] == "enum":
                value = (spec.get("values") or [".jpg"])[0]
            if op in ("in", "not_in"):
                value = [value]
            elif op == "between":
                value = ["2024-01-01", "2024-12-31"] if spec["type"] == "date" else [0, 5]
            elif op == "recent":
                value = {"n": 7, "unit": "days"}
            rule = {"field": key, "op": op, "value": value}
            count = db.count_photos_for_rules([rule])
            assert isinstance(count, int), f"{key}/{op} failed"


def test_exif_backfill_migration_and_idempotence(tmp_path):
    import json as _json

    from db import Database
    path = str(tmp_path / "test.db")
    db, fid = _filter_db(tmp_path)
    pid = db.add_photo(folder_id=fid, filename='sony.arw', extension='.arw',
                       file_size=100, file_mtime=1.0)
    exif = {
        "EXIF": {"Make": "Sony", "Model": "ILCE-1", "FNumber": 6.3,
                 "ExposureTime": 0.0004, "ISO": 800},
        "Composite": {"LensID": "FE 200-600mm F5.6-6.3 G OSS"},
    }
    db.conn.execute(
        "UPDATE photos SET exif_data=?, camera_make=NULL, camera_model=NULL, "
        "lens=NULL, aperture=NULL, shutter_speed=NULL, iso=NULL WHERE id=?",
        (_json.dumps(exif), pid))
    # Simulate a pre-backfill database so reopening runs the migration.
    db.conn.execute("DELETE FROM db_meta WHERE key='exif_summary_backfill_v1'")
    db.conn.commit()
    db.close()

    db2 = Database(path)
    row = db2.conn.execute(
        "SELECT camera_make, camera_model, lens, aperture, shutter_speed, iso "
        "FROM photos WHERE id=?", (pid,)).fetchone()
    assert row["camera_make"] == "Sony"
    assert row["camera_model"] == "ILCE-1"
    assert row["lens"] == "FE 200-600mm F5.6-6.3 G OSS"
    assert row["aperture"] == 6.3
    assert row["shutter_speed"] == 0.0004
    assert row["iso"] == 800
    # The promoted columns are filterable.
    ws_id = db2.ensure_default_workspace()
    db2.set_active_workspace(ws_id)
    assert db2.count_photos_for_rules(
        [{"field": "camera_model", "op": "contains", "value": "ilce"}]) == 1

    # Idempotence: with the marker set, reopening must not overwrite.
    db2.conn.execute("UPDATE photos SET camera_make='UserEdited' WHERE id=?", (pid,))
    db2.conn.commit()
    db2.close()
    db3 = Database(path)
    row = db3.conn.execute(
        "SELECT camera_make FROM photos WHERE id=?", (pid,)).fetchone()
    assert row["camera_make"] == "UserEdited"
    db3.close()


def test_query_photos_sort_and_paging(tmp_path):
    db, fid = _filter_db(tmp_path)
    for i, (name, rating) in enumerate([('a.jpg', 1), ('b.jpg', 5), ('c.jpg', 3)]):
        pid = db.add_photo(folder_id=fid, filename=name, extension='.jpg',
                           file_size=100, file_mtime=1.0,
                           timestamp=f'2024-01-0{i + 1} 10:00:00')
        db.update_photo_rating(pid, rating)

    rows = db.query_photos([], sort="rating")
    assert [r["filename"] for r in rows] == ['b.jpg', 'c.jpg', 'a.jpg']
    rows = db.query_photos([], sort="name", page=2, per_page=2)
    assert [r["filename"] for r in rows] == ['c.jpg']
    rows = db.query_photos([{"field": "rating", "op": ">=", "value": 3}], sort="name")
    assert [r["filename"] for r in rows] == ['b.jpg', 'c.jpg']


def test_get_filter_field_values_counts_respect_rules(tmp_path):
    import pytest
    db, fid = _filter_db(tmp_path)
    ids = []
    for name, model, rating in [
        ('a.jpg', 'Sony A1', 5), ('b.jpg', 'Sony A1', 1),
        ('c.jpg', 'Canon R5', 5), ('d.jpg', None, 5),
    ]:
        pid = db.add_photo(folder_id=fid, filename=name, extension='.jpg',
                           file_size=100, file_mtime=1.0)
        db.update_photo_rating(pid, rating)
        if model:
            db.conn.execute("UPDATE photos SET camera_model=? WHERE id=?", (model, pid))
        ids.append(pid)
    db.conn.commit()

    # Unfiltered: counts over the whole workspace, NULLs excluded.
    values = db.get_filter_field_values("camera_model")
    assert values == [
        {"value": "Sony A1", "count": 2},
        {"value": "Canon R5", "count": 1},
    ]
    # Sibling rules constrain the counts (facet semantics).
    values = db.get_filter_field_values(
        "camera_model", rules=[{"field": "rating", "op": ">=", "value": 4}])
    assert values == [
        {"value": "Canon R5", "count": 1},
        {"value": "Sony A1", "count": 1},
    ]
    # Typeahead narrowing.
    values = db.get_filter_field_values("camera_model", q="son")
    assert values == [{"value": "Sony A1", "count": 2}]
    # Keyword and species values come from the keyword tables.
    heron = db.add_keyword('Great Blue Heron', is_species=True)
    db.tag_photo(ids[0], heron)
    assert db.get_filter_field_values("species") == [
        {"value": "Great Blue Heron", "count": 1}]
    # Non-suggest fields are rejected.
    with pytest.raises(ValueError):
        db.get_filter_field_values("rating")


def test_get_filter_field_values_counts_wrap_or_rules(tmp_path):
    """Facet counts under a top-level ``any`` (OR) rule group must not leak
    rows that satisfy the first OR branch but fail the facet's own predicate.

    ``_build_query_from_rules`` returns ``WHERE (A) OR (B)`` for
    ``{mode: any}``; appending ``AND {facet}`` would bind to the last OR
    branch by SQL precedence (``(A) OR ((B) AND facet)``), letting
    branch-A rows with a NULL/non-matching facet field still inflate the
    suggestion count. The facet must wrap the existing rule condition so
    every matched row also satisfies the ``value IS NOT NULL``/typeahead
    predicates the suggestion helper appends.
    """
    db, fid = _filter_db(tmp_path)
    # Two-branch tree: rating >= 5  OR  camera_model = "Nikon Z9".
    # Sony has rating 5 (matches branch A) but a distinct model. Under the
    # buggy binding, Sony would leak into the Nikon-branch AND camera_model
    # facet — but only Nikon should count under "Nikon Z9".
    for name, model, rating in [
        ('sony_hi.jpg', 'Sony A1', 5),      # matches branch A only
        ('nikon_lo.jpg', 'Nikon Z9', 1),    # matches branch B only
        ('nikon_hi.jpg', 'Nikon Z9', 5),    # matches both branches
        ('nomodel.jpg', None, 5),           # matches branch A, NULL camera
    ]:
        pid = db.add_photo(folder_id=fid, filename=name, extension='.jpg',
                           file_size=100, file_mtime=1.0)
        db.update_photo_rating(pid, rating)
        if model:
            db.conn.execute(
                "UPDATE photos SET camera_model=? WHERE id=?", (model, pid))
    db.conn.commit()

    rules = {"mode": "any", "rules": [
        {"field": "rating", "op": ">=", "value": 5},
        {"field": "camera_model", "op": "is", "value": "Nikon Z9"},
    ]}
    values = {v["value"]: v["count"]
              for v in db.get_filter_field_values("camera_model", rules=rules)}
    # Three photos total match the OR tree (sony_hi, nikon_lo, nikon_hi);
    # the NULL-camera one is excluded by the facet's IS NOT NULL guard.
    # Counts must match what ``camera_model is <val>`` would return when
    # combined with the sibling OR tree — one Sony, two Nikon.
    assert values == {"Sony A1": 1, "Nikon Z9": 2}
    # Typeahead ``son`` combined with the OR tree must also stay honest —
    # the ``AND LIKE ?`` clause the helper appends must apply to every
    # matched row, not just the last OR branch.
    values = {v["value"]: v["count"]
              for v in db.get_filter_field_values(
                  "camera_model", rules=rules, q="son")}
    assert values == {"Sony A1": 1}


def test_get_filter_field_values_folder_counts_over_subtree(tmp_path):
    """Folder suggestions must aggregate over subtrees so counts match the
    ``folder under=<path>`` operator the rule engine implements. A parent
    folder with no direct photos but matching descendants must still be
    suggested with its subtree count; a folder mixing direct and nested
    photos must not undercount."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    root = db.add_folder('/photos', name='photos')
    year = db.add_folder('/photos/2024', name='2024', parent_id=root)
    month = db.add_folder('/photos/2024/01', name='01', parent_id=year)
    other = db.add_folder('/vacation', name='vacation')
    # /photos itself has NO direct photos; only descendants.
    db.add_photo(folder_id=year, filename='y1.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=month, filename='m1.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=month, filename='m2.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=other, filename='v1.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)

    values = {v["value"]: v["count"] for v in db.get_filter_field_values("folder")}
    # Parent with only descendants is still suggestable with subtree count.
    assert values.get('/photos') == 3
    # /photos/2024: 1 direct + 2 in /01 subtree.
    assert values.get('/photos/2024') == 3
    # /photos/2024/01: 2 direct.
    assert values.get('/photos/2024/01') == 2
    # Separate subtree counts independently.
    assert values.get('/vacation') == 1
    # Cross-check: each suggestion's count matches what a `folder under`
    # rule would return.
    for path, count in values.items():
        rule_count = db.count_photos_for_rules(
            [{"field": "folder", "op": "under", "value": path}])
        assert rule_count == count, f"mismatch for {path!r}: rule={rule_count} suggest={count}"

    # Typeahead narrows to matching folder paths.
    values = {v["value"]: v["count"]
              for v in db.get_filter_field_values("folder", q="2024")}
    assert set(values) == {'/photos/2024', '/photos/2024/01'}

    # Sibling rules constrain the counts (facet semantics preserved).
    values = {v["value"]: v["count"] for v in db.get_filter_field_values(
        "folder",
        rules=[{"field": "filename", "op": "starts_with", "value": "m"}],
    )}
    # Only m1.jpg / m2.jpg match; they live under /photos/2024/01.
    assert values.get('/photos') == 2
    assert values.get('/photos/2024') == 2
    assert values.get('/photos/2024/01') == 2
    assert '/vacation' not in values


def test_get_filter_field_values_species_canonicalizes_hierarchy_leaf(tmp_path):
    """Species typeahead resolves hierarchy leaves through ``taxon_id`` to
    the same-taxon top-level root's name, so suggestions match the values
    ``get_species_keywords_for_photos`` shows in Browse, Compare, and
    life-list views. Otherwise a user editing a Species rule would see the
    raw leaf spelling (``Desert Verdin``) that never appears elsewhere."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    # Photo p_leaf: tagged only with a hierarchy leaf linked to Verdin taxon.
    p_leaf = db.add_photo(folder_id=fid, filename='leaf.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    parent = db.add_keyword("Penduline tits")
    leaf = db.add_keyword("Desert Verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type='taxonomy', is_species=1, taxon_id=? "
        "WHERE id=?", (taxa["Verdin"], leaf))
    # Photo p_root: tagged with the top-level root directly (auto-links).
    p_root = db.add_photo(folder_id=fid, filename='root.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    root_kid = db.add_keyword("Verdin", is_species=True)
    db.tag_photo(p_leaf, leaf)
    db.tag_photo(p_root, root_kid)
    db.conn.commit()

    # Suggestions canonicalize the leaf to the root spelling; both photos
    # count under the single canonical name.
    assert db.get_filter_field_values("species") == [
        {"value": "Verdin", "count": 2}]
    # The raw leaf spelling is never suggested — the canonical root wins.
    assert db.get_filter_field_values("species", q="Desert") == []
    # Typeahead on the root name returns the canonical suggestion.
    assert db.get_filter_field_values("species", q="Verd") == [
        {"value": "Verdin", "count": 2}]
    # Cross-check: the suggested value works with the `species is` rule
    # that the UI would fire off — both photos are matched (rule engine
    # already canonicalizes through taxon).
    assert db.count_photos_for_rules(
        [{"field": "species", "op": "is", "value": "Verdin"}]) == 2


def test_get_filter_field_values_species_preserves_attached_root_spelling(tmp_path):
    """When a taxon has multiple top-level species roots and a photo is
    tagged with the non-MIN(id) root, ``get_species_keywords_for_photos``
    intentionally keeps the attached root's stored spelling (its
    ``is_root`` guard). The typeahead must agree — if it always rewrites
    to the MIN(id) root, the species name Browse shows would disappear
    from suggestions and a typeahead query for that displayed root would
    return nothing."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos', name='photos')
    taxa = _seed_taxa(db, [(9999, "Corvus brachyrhynchos", "American Crow")])
    # Two top-level roots for the same taxon: one earlier, one later.
    root_early = db.add_keyword("American Crow", is_species=True)
    root_late = db.add_keyword("crow (american)", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id=? WHERE id=?",
        (taxa["American Crow"], root_late),
    )
    # Photo tagged with the LATE (non-MIN-id) root only.
    p_late = db.add_photo(folder_id=fid, filename='late.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    db.tag_photo(p_late, root_late)
    # Photo tagged with a hierarchy leaf under the same taxon — still
    # canonicalizes to the MIN(id) root (existing behavior).
    p_leaf = db.add_photo(folder_id=fid, filename='leaf.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    parent = db.add_keyword("Corvids")
    leaf = db.add_keyword("Northeastern Crow", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type='taxonomy', is_species=1, taxon_id=? "
        "WHERE id=?", (taxa["American Crow"], leaf))
    db.tag_photo(p_leaf, leaf)
    db.conn.commit()

    values = {v["value"]: v["count"]
              for v in db.get_filter_field_values("species")}
    # The attached late-root spelling must survive as its own suggestion —
    # matches what ``get_species_keywords_for_photos`` reports for p_late
    # (Browse shows ``crow (american)``, so the typeahead must offer it).
    assert values.get("crow (american)") == 1
    # The hierarchy-leaf photo still canonicalizes to the MIN(id) root
    # (``American Crow``), matching the leaf-only test above.
    assert values.get("American Crow") == 1
    assert "Northeastern Crow" not in values
    # Typeahead on the attached root spelling finds the photo instead of
    # returning nothing.
    late_hits = db.get_filter_field_values("species", q="crow (")
    assert {v["value"] for v in late_hits} == {"crow (american)"}


def test_get_filter_field_values_folder_escapes_like_metacharacters(tmp_path):
    """Folder suggestion counts must not treat stored folder paths as LIKE
    patterns. A folder such as ``/photos/my_dir`` uses ``_`` — a single-char
    LIKE wildcard — so an unescaped subtree join would also match photos in
    ``/photos/myXdir`` and overcount what selecting the suggestion actually
    filters to."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    literal = db.add_folder('/photos/my_dir', name='my_dir')
    look_alike = db.add_folder('/photos/myXdir', name='myXdir')
    db.add_photo(folder_id=literal, filename='a.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=look_alike, filename='b.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)

    values = {v["value"]: v["count"] for v in db.get_filter_field_values("folder")}
    # ``/photos/my_dir`` covers only its own direct photo — the ``_`` in the
    # stored path must not act as a LIKE wildcard against ``myXdir``. Cross-
    # check with what the ``folder under`` rule engine returns.
    assert values.get('/photos/my_dir') == 1
    assert values.get('/photos/myXdir') == 1
    for path in ('/photos/my_dir', '/photos/myXdir'):
        rule_count = db.count_photos_for_rules(
            [{"field": "folder", "op": "under", "value": path}])
        assert rule_count == values.get(path), (
            f"mismatch for {path!r}: rule={rule_count} suggest={values.get(path)}"
        )


def test_get_filter_field_values_folder_root_with_trailing_separator(tmp_path):
    """A workspace root stored with a trailing separator (Windows drive root
    ``D:\\`` or POSIX ``/``) must still count its subtree correctly. The
    rule engine's ``folder under`` op strips trailing separators via
    ``_path_for_subtree_match``, so the facet must do the same on both
    sides — otherwise concatenating the LIKE prefix produces ``D://%`` /
    ``//%`` which matches nothing, and the suggested root counts 0 while
    ``folder under=<root>`` actually returns every photo on the drive."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    # POSIX root stored with trailing '/', descendants without.
    root = db.add_folder('/', name='root')
    child = db.add_folder('/dcim', name='dcim', parent_id=root)
    db.add_photo(folder_id=root, filename='at_root.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=child, filename='d1.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)
    db.add_photo(folder_id=child, filename='d2.jpg', extension='.jpg',
                 file_size=100, file_mtime=1.0)

    values = {v["value"]: v["count"] for v in db.get_filter_field_values("folder")}
    # ``/`` covers the direct photo plus both descendants.
    assert values.get('/') == 3
    assert values.get('/dcim') == 2
    # Cross-check: the suggested count matches what ``folder under`` returns.
    for path, count in values.items():
        rule_count = db.count_photos_for_rules(
            [{"field": "folder", "op": "under", "value": path}])
        assert rule_count == count, (
            f"mismatch for {path!r}: rule={rule_count} suggest={count}"
        )


def test_get_filter_field_values_camera_folds_case(tmp_path):
    """Camera-field suggestions must fold by case: the corresponding rule
    engine text ops use ``LOWER(column) = LOWER(?)`` for camera_make/
    camera_model/lens (no ``case_toggle`` on the registry), so raw
    value-splitting facet counts would show ``Sony A1`` and ``sony a1`` as
    two 1-count entries while selecting either fires a case-insensitive
    filter that returns both photos. The suggestion count must equal the
    rule count for the value it becomes."""
    db, fid = _filter_db(tmp_path)
    ids = []
    for name, model in [
        ('a.jpg', 'Sony A1'), ('b.jpg', 'sony a1'), ('c.jpg', 'SONY A1'),
        ('d.jpg', 'Canon R5'),
    ]:
        pid = db.add_photo(folder_id=fid, filename=name, extension='.jpg',
                           file_size=100, file_mtime=1.0)
        db.conn.execute("UPDATE photos SET camera_model=? WHERE id=?", (model, pid))
        ids.append(pid)
    db.conn.commit()

    values = db.get_filter_field_values("camera_model")
    # One row per case-folded model, count aggregates all case variants.
    by_lower = {v["value"].lower(): v["count"] for v in values}
    assert by_lower == {"sony a1": 3, "canon r5": 1}
    # Cross-check: the suggested count matches what a ``camera_model is <suggested>``
    # rule would return for the case-insensitive engine.
    for entry in values:
        rule_count = db.count_photos_for_rules(
            [{"field": "camera_model", "op": "is", "value": entry["value"]}])
        assert rule_count == entry["count"], (
            f"mismatch for {entry['value']!r}: rule={rule_count} "
            f"suggest={entry['count']}"
        )
    # Typeahead narrowing still works case-insensitively.
    values = db.get_filter_field_values("camera_model", q="SONY")
    assert len(values) == 1
    assert values[0]["value"].lower() == "sony a1"
    assert values[0]["count"] == 3


def test_universal_filter_species_matches_by_displayed_root_name(tmp_path):
    """Species rules match a photo by the species name shown in the UI —
    the canonical root spelling from ``get_species_keywords_for_photos``
    (and ``/api/filters/values``) — never by a same-taxon hierarchy leaf
    that never surfaces there.

    A photo tagged only with the root ``Verdin`` is displayed as
    ``Verdin``. A rule ``species is "Auriparus flaviceps"`` (or
    ``contains "flaviceps"``) targets the leaf spelling and must not
    pull the root-tagged photo in just because that leaf exists for the
    same taxon — otherwise the filter contradicts what
    ``get_species_keywords_for_photos`` and the values typeahead
    advertise, and the ``is not`` / ``not_contains`` inverses exclude
    the photo unexpectedly.
    """
    db, fid = _filter_db(tmp_path)
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    pid = db.add_photo(folder_id=fid, filename='v.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    root_kw = db.add_keyword("Verdin", is_species=True)
    parent = db.add_keyword("Penduline tits")
    leaf = db.add_keyword("Auriparus flaviceps", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type='taxonomy', is_species=1, taxon_id=? "
        "WHERE id=?", (taxa["Verdin"], leaf))
    db.conn.commit()
    db.tag_photo(pid, root_kw)

    # Sanity check: displayed species is the root spelling ``Verdin``,
    # not the leaf ``Auriparus flaviceps``.
    assert db.get_species_keywords_for_photos([pid]) == {pid: ["Verdin"]}

    count = db.count_photos_for_rules
    # The displayed name matches.
    assert count([{"field": "species", "op": "is", "value": "Verdin"}]) == 1
    # A same-taxon hierarchy leaf that never surfaces in the UI does not.
    assert count([{"field": "species", "op": "is",
                   "value": "Auriparus flaviceps"}]) == 0
    assert count([{"field": "species", "op": "contains",
                   "value": "flaviceps"}]) == 0
    # The ``is not`` / ``not_contains`` inverses correctly keep the
    # root-tagged photo included when the query targets a leaf spelling
    # the photo is not displayed under.
    assert count([{"field": "species", "op": "is not",
                   "value": "Auriparus flaviceps"}]) == 1
    assert count([{"field": "species", "op": "not_contains",
                   "value": "flaviceps"}]) == 1


def test_universal_filter_species_matches_rootless_hierarchy_leaf(tmp_path):
    """When a hierarchy leaf's taxon has no top-level root row in
    ``keywords`` (repair detached the ``Verdin`` root and left only the
    ``Desert Verdin`` leaf), ``get_species_keywords_for_photos`` and the
    values typeahead both fall back to the leaf's own ``k.name``. The
    species filter must fall back the same way — otherwise
    ``species is "Desert Verdin"`` silently excludes the photo (and
    ``is not`` silently includes it) even though the leaf name is what
    the rest of the app surfaces.
    """
    db, fid = _filter_db(tmp_path)
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    pid = db.add_photo(folder_id=fid, filename='leaf.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    other = db.add_photo(folder_id=fid, filename='other.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    parent = db.add_keyword("Penduline tits")
    leaf = db.add_keyword("Desert Verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type='taxonomy', is_species=1, taxon_id=? "
        "WHERE id=?", (taxa["Verdin"], leaf))
    db.conn.commit()
    db.tag_photo(pid, leaf)
    # No top-level ``Verdin`` root exists — this is the rootless-leaf
    # shape the repair pass leaves behind.
    assert db.conn.execute(
        "SELECT COUNT(*) FROM keywords "
        "WHERE parent_id IS NULL AND taxon_id=?", (taxa["Verdin"],)
    ).fetchone()[0] == 0

    # The displayed species falls back to the leaf's own spelling.
    assert db.get_species_keywords_for_photos([pid]) == {pid: ["Desert Verdin"]}

    count = db.count_photos_for_rules
    # The leaf name matches ``is`` / ``equals`` / ``contains``.
    assert count([{"field": "species", "op": "is",
                   "value": "Desert Verdin"}]) == 1
    assert count([{"field": "species", "op": "equals",
                   "value": "Desert Verdin"}]) == 1
    assert count([{"field": "species", "op": "contains",
                   "value": "Verdin"}]) == 1
    # And the inverses correctly exclude the leaf-tagged photo.
    assert count([{"field": "species", "op": "is not",
                   "value": "Desert Verdin"}]) == 1
    assert count([{"field": "species", "op": "not_contains",
                   "value": "Verdin"}]) == 1
    _ = other


def test_universal_filter_species_multi_root_isolates_attached_spelling(tmp_path):
    """When a taxon has multiple top-level roots (say ``Verdin`` and the
    sibling alias ``Auriparus flaviceps``), a photo tagged only with one
    of them must match rules for that spelling only — never for the
    sibling root's spelling. ``/api/filters/values`` groups attached
    roots by their own ``kv.name`` (its ``kv.parent_id IS NULL`` branch),
    so if the filter matched any same-taxon root, selecting one
    suggestion would return photos the count advertises under the other.

    A hierarchy leaf photo for the same taxon must still match the
    canonical MIN(id) root spelling — that's what
    ``get_species_keywords_for_photos`` and the typeahead surface for
    leaves — but not the sibling root spelling.
    """
    db, fid = _filter_db(tmp_path)
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    # Create ``Verdin`` first so it wins MIN(id) — the sibling root
    # ``Auriparus flaviceps`` gets a higher id.
    verdin_root = db.add_keyword("Verdin", is_species=True)
    sci_root = db.add_keyword("Auriparus flaviceps", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET type='taxonomy', is_species=1, taxon_id=? "
        "WHERE id=?", (taxa["Verdin"], sci_root))
    parent = db.add_keyword("Penduline tits")
    leaf = db.add_keyword("Desert Verdin", parent_id=parent)
    db.conn.execute(
        "UPDATE keywords SET type='taxonomy', is_species=1, taxon_id=? "
        "WHERE id=?", (taxa["Verdin"], leaf))
    db.conn.commit()

    p_verdin = db.add_photo(folder_id=fid, filename='v.jpg', extension='.jpg',
                            file_size=100, file_mtime=1.0)
    p_sci = db.add_photo(folder_id=fid, filename='s.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    p_leaf = db.add_photo(folder_id=fid, filename='l.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    db.tag_photo(p_verdin, verdin_root)
    db.tag_photo(p_sci, sci_root)
    db.tag_photo(p_leaf, leaf)

    # Sanity: display keeps each attached root's own spelling and
    # canonicalizes the hierarchy leaf to MIN(id) root.
    displayed = db.get_species_keywords_for_photos([p_verdin, p_sci, p_leaf])
    assert displayed[p_verdin] == ["Verdin"]
    assert displayed[p_sci] == ["Auriparus flaviceps"]
    assert displayed[p_leaf] == ["Verdin"]

    count = db.count_photos_for_rules
    # ``species is "Verdin"`` matches only the photos surfaced as
    # ``Verdin`` — the attached-``Verdin`` photo and the canonicalized
    # leaf — never the sibling-root-tagged photo.
    assert count([{"field": "species", "op": "is", "value": "Verdin"}]) == 2
    # ``species is "Auriparus flaviceps"`` matches only the attached
    # sibling root, not the ``Verdin``-tagged nor the leaf-tagged photos
    # (the leaf displays as ``Verdin`` via MIN(id) canonicalization).
    assert count([{"field": "species", "op": "is",
                   "value": "Auriparus flaviceps"}]) == 1


def test_exif_backfill_migration_clears_empty_marker_for_rescan(tmp_path):
    """Rows whose ``exif_data`` is the ``'{}'`` marker were scanned with
    ``extract_full_metadata=False`` before the promoted EXIF columns
    existed, so there is nothing to backfill from JSON. The scanner's
    incremental pre-pass treats any non-NULL ``exif_data`` as already
    extracted, so leaving the marker in place would keep camera/lens/iso
    NULL until a user manually forces a full non-incremental scan. The
    migration must clear those rows back to NULL so the next incremental
    scan re-runs ExifTool and populates the promoted columns.
    """
    from db import Database
    path = str(tmp_path / "test.db")
    db, fid = _filter_db(tmp_path)
    empty = db.add_photo(folder_id=fid, filename='empty.jpg', extension='.jpg',
                         file_size=100, file_mtime=1.0)
    populated = db.add_photo(folder_id=fid, filename='sony.arw', extension='.arw',
                             file_size=100, file_mtime=1.0)
    import json as _json
    exif = {"EXIF": {"Make": "Sony", "Model": "ILCE-1"}}
    db.conn.execute(
        "UPDATE photos SET exif_data='{}', camera_make=NULL, "
        "camera_model=NULL WHERE id=?", (empty,))
    db.conn.execute(
        "UPDATE photos SET exif_data=?, camera_make=NULL, "
        "camera_model=NULL WHERE id=?", (_json.dumps(exif), populated))
    # Simulate pre-backfill.
    db.conn.execute("DELETE FROM db_meta WHERE key='exif_summary_backfill_v1'")
    db.conn.commit()
    db.close()

    db2 = Database(path)
    # Populated row is backfilled from its stored JSON.
    row = db2.conn.execute(
        "SELECT exif_data, camera_make, camera_model FROM photos WHERE id=?",
        (populated,)).fetchone()
    assert row["camera_make"] == "Sony"
    assert row["camera_model"] == "ILCE-1"
    # Empty-marker row is cleared so the next incremental scan re-extracts
    # ExifTool for it (the pre-pass keys on ``exif_data IS NOT NULL``).
    row = db2.conn.execute(
        "SELECT exif_data, camera_make, camera_model FROM photos WHERE id=?",
        (empty,)).fetchone()
    assert row["exif_data"] is None
    assert row["camera_make"] is None
    assert row["camera_model"] is None
    db2.close()

    # Second re-open is a no-op (marker set): a fresh '{}' written after
    # the migration ran must not be cleared on subsequent opens —
    # otherwise the scanner's own ``COALESCE(exif_data, '{}')`` write
    # would bounce right back to NULL every time.
    db3 = Database(path)
    db3.conn.execute(
        "UPDATE photos SET exif_data='{}' WHERE id=?", (empty,))
    db3.conn.commit()
    db3.close()
    db4 = Database(path)
    row = db4.conn.execute(
        "SELECT exif_data FROM photos WHERE id=?", (empty,)).fetchone()
    assert row["exif_data"] == '{}'
    db4.close()


def test_universal_filter_has_species_matches_taxonomy_type_keyword(tmp_path):
    """``has_species`` must accept species stored as
    ``type='taxonomy'`` with ``is_species=0`` — the shape upgraded/legacy
    photos carry and that the species filter,
    ``get_species_keywords_for_photos``, and Browse all treat as species.
    A plain ``k.is_species = 1`` check disagreed: the same photo would
    appear under ``species is Verdin`` yet fail ``has_species is true``.
    Also validates the newer ``is_species=1`` flag path so both storage
    shapes count toward the "Has species" chip.
    """
    db, fid = _filter_db(tmp_path)
    taxa = _seed_taxa(db, [(2912, "Auriparus flaviceps", "Verdin")])
    p_legacy = db.add_photo(folder_id=fid, filename='legacy.jpg', extension='.jpg',
                            file_size=100, file_mtime=1.0)
    p_flag = db.add_photo(folder_id=fid, filename='flag.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    p_none = db.add_photo(folder_id=fid, filename='none.jpg', extension='.jpg',
                          file_size=100, file_mtime=1.0)
    # Legacy shape: type='taxonomy', is_species=0, taxon linked to a species.
    legacy_kw = db.add_keyword("Verdin")
    db.conn.execute(
        "UPDATE keywords SET type='taxonomy', is_species=0, taxon_id=? "
        "WHERE id=?", (taxa["Verdin"], legacy_kw))
    db.conn.commit()
    db.tag_photo(p_legacy, legacy_kw)
    # Newer shape: is_species=1 flag.
    flag_kw = db.add_keyword("Great Blue Heron", is_species=True)
    db.tag_photo(p_flag, flag_kw)

    count = db.count_photos_for_rules
    # Sanity: the species filter already accepts the legacy shape.
    assert count([{"field": "species", "op": "is", "value": "Verdin"}]) == 1
    # Both storage shapes count toward has_species=true; only p_none
    # (untagged) matches has_species=false.
    assert count([{"field": "has_species", "op": "is", "value": 1}]) == 2
    assert count([{"field": "has_species", "op": "is", "value": 0}]) == 1
    _ = p_none  # keep the untagged photo alive for the negative branch.
