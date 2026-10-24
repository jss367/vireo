"""Maintenance actions leave durable, workspace-scoped audit history."""

import json
import sqlite3
from pathlib import Path

import pytest
from db import Database


def _photo(db, tmp_path):
    pid = db.photos.list_page()[0]['id']
    folder = db.photos.get(pid)['folder_id']
    db.conn.execute('UPDATE folders SET path = ? WHERE id = ?', (str(tmp_path), folder))
    db.commit()
    return pid, tmp_path / db.photos.get(pid)['filename']


def _entry(client, action):
    entries = client.get('/api/edit-history').json
    entry = next(row for row in entries if row['action_type'] == action)
    assert 'cannot undo' in entry['description']
    assert client.get('/api/undo/status').json['available'] is False
    assert client.get('/api/redo/status').json['available'] is False
    return entry


def _duplicates(db):
    pids = [row['id'] for row in db.photos.list_page()][:2]
    ids = []
    for name, pid in zip(('Heron', 'heron'), pids, strict=True):
        kid = db.conn.execute("INSERT INTO keywords(name, type) VALUES (?, 'general')", (name,)).lastrowid
        ids.append(kid)
        db.tag_photo(pid, kid)
    return ids


def test_remove_orphans_history_survives_deletion_and_reopen(app_and_db, tmp_path):
    app, db = app_and_db
    pid, path = _photo(db, tmp_path)
    client = app.test_client()
    response = client.post('/api/audit/remove-orphans', json={'photo_ids': [pid, pid]})
    assert response.json == {'ok': True, 'removed': 1}
    entry = _entry(client, 'audit_remove_orphans')
    payload = json.loads(entry['new_value'])
    assert payload['removed'] == 1
    assert payload['photos'][0]['photo_id'] == pid
    assert payload['photos'][0]['filename'] == path.name
    assert db.photos.get(pid) is None
    assert not (Path(app.config['THUMB_CACHE_DIR']) / f'{pid}.jpg').exists()
    with Database(db._db_path) as reopened:
        assert reopened.edit_history.list_recent()[0]['id'] == entry['id']
    other = db.create_workspace('Other')
    db.set_active_workspace(other)
    assert db.edit_history.list_recent() == []


@pytest.mark.parametrize('direction', ['use_db', 'use_xmp'])
def test_audit_resolve_records_actual_metadata_or_queued_writes(app_and_db, tmp_path, direction):
    from xmp import write_sidecar

    app, db = app_and_db
    pid, path = _photo(db, tmp_path)
    if direction == 'use_xmp':
        write_sidecar(str(path.with_suffix('.xmp')), flat_keywords={'Heron'}, hierarchical_keywords=set())
    client = app.test_client()
    response = client.post('/api/audit/resolve', json={'photo_id': pid, 'direction': direction})
    assert response.status_code == 200, response.json
    entry = _entry(client, 'audit_resolve')
    assert json.loads(entry['new_value']) == {'photo_id': pid, 'direction': direction}
    item = db.conn.execute('SELECT * FROM edit_history_items WHERE edit_id = ?', (entry['id'],)).fetchone()
    before, after = (json.loads(item[key]) for key in ('old_value', 'new_value'))
    assert before != after
    if direction == 'use_xmp':
        assert {row['name'] for row in after['keywords']} == {'Heron'}
    else:
        assert after['pending_changes']


def test_keyword_cleanup_records_count_and_skips_empty_repeat(app_and_db):
    app, db = app_and_db
    keep, duplicate = _duplicates(db)
    client = app.test_client()
    assert client.post('/api/keywords/clean').json == {'ok': True, 'merged': 1}
    entry = _entry(client, 'keyword_clean')
    assert json.loads(entry['new_value']) == {'merged': 1}
    assert db.conn.execute('SELECT id FROM keywords WHERE id = ?', (duplicate,)).fetchone() is None
    assert db.conn.execute('SELECT id FROM keywords WHERE id = ?', (keep,)).fetchone()
    assert client.post('/api/keywords/clean').json['merged'] == 0
    assert len(client.get('/api/edit-history').json) == 1


@pytest.mark.parametrize('operation', ['orphans', 'clean', 'use_db', 'use_xmp'])
def test_history_failure_rolls_back_maintenance(app_and_db, tmp_path, monkeypatch, operation):
    from xmp import write_sidecar

    app, db = app_and_db
    pid, path = _photo(db, tmp_path)
    if operation == 'clean':
        _duplicates(db)
    if operation == 'use_xmp':
        write_sidecar(str(path.with_suffix('.xmp')), flat_keywords={'Heron'}, hierarchical_keywords=set())
    tables = ('photos', 'keywords', 'photo_keywords', 'pending_changes', 'collections')
    before = {table: [tuple(row) for row in db.conn.execute(f'SELECT * FROM {table} ORDER BY 1, 2')]
              for table in tables}

    def fail(self, *args, **kwargs):
        # Another connection must not see the change before history succeeds.
        with sqlite3.connect(db._db_path) as reader:
            for table in tables:
                assert reader.execute(f'SELECT * FROM {table} ORDER BY 1, 2').fetchall() == before[table]
        raise RuntimeError('history failed')

    monkeypatch.setattr(Database, 'record_edit', fail)
    url, body = {
        'orphans': ('/api/audit/remove-orphans', {'photo_ids': [pid]}),
        'clean': ('/api/keywords/clean', {}),
        'use_db': ('/api/audit/resolve', {'photo_id': pid, 'direction': 'use_db'}),
        'use_xmp': ('/api/audit/resolve', {'photo_id': pid, 'direction': 'use_xmp'}),
    }[operation]
    assert app.test_client().post(url, json=body).status_code == 500
    for table in tables:
        assert [tuple(row) for row in db.conn.execute(f'SELECT * FROM {table} ORDER BY 1, 2')] == before[table]
    assert not db.edit_history.list_recent()
    if operation == 'orphans':
        assert (Path(app.config['THUMB_CACHE_DIR']) / f'{pid}.jpg').exists()


@pytest.mark.parametrize('sidecar', ['missing', 'corrupt'])
def test_skipped_xmp_repair_has_no_history(app_and_db, tmp_path, sidecar):
    app, db = app_and_db
    pid, path = _photo(db, tmp_path)
    if sidecar == 'corrupt':
        path.with_suffix('.xmp').write_text('not XML')
    before = db.photo_keywords.get_for_photo(pid)
    response = app.test_client().post('/api/audit/resolve', json={'photo_id': pid, 'direction': 'use_xmp'})
    assert response.status_code == (409 if sidecar == 'corrupt' else 200)
    assert db.photo_keywords.get_for_photo(pid) == before
    assert db.edit_history.list_recent() == []


def test_empty_maintenance_preserves_redo(app_and_db, tmp_path):
    app, db = app_and_db
    pid, path = _photo(db, tmp_path)
    path.write_bytes(b'existing original')
    client = app.test_client()
    client.post(f'/api/photos/{pid}/rating', json={'rating': 1})
    assert client.post('/api/undo').status_code == 200
    assert client.get('/api/redo/status').json['available']
    for url, body in (
        ('/api/audit/remove-orphans', {'photo_ids': [pid]}),
        ('/api/keywords/clean', {}),
        ('/api/audit/resolve', {'photo_id': pid, 'direction': 'use_xmp'}),
    ):
        assert client.post(url, json=body).status_code == 200
        assert client.get('/api/redo/status').json['available']
    assert client.get('/api/edit-history').json == []


def test_bulk_repair_keeps_history_for_completed_photos_on_later_failure(app_and_db, monkeypatch):
    import audit

    app, db = app_and_db
    pids = [row['id'] for row in db.photos.list_page()][:2]
    original = audit.resolve_drift

    def resolve(db, pid, direction):
        if pid == pids[1]:
            raise RuntimeError('second repair failed')
        return original(db, pid, direction)

    monkeypatch.setattr(audit, 'check_drift', lambda db: [{'photo_id': pid, 'resolutions': ['use_db', 'use_xmp']} for pid in pids])
    monkeypatch.setattr(audit, 'resolve_drift', resolve)
    client = app.test_client()
    assert client.post('/api/audit/resolve-all', json={'direction': 'use_db'}).status_code == 500
    entry = _entry(client, 'audit_resolve')
    assert json.loads(entry['new_value'])['photo_id'] == pids[0]
    assert len(client.get('/api/edit-history').json) == 1


def test_undo_skips_maintenance_and_replays_the_previous_photo_edit(app_and_db):
    app, db = app_and_db
    _duplicates(db)
    pid = db.photos.list_page()[0]['id']
    original = db.photos.get(pid)['rating']
    client = app.test_client()
    assert client.post(f'/api/photos/{pid}/rating', json={'rating': 1}).status_code == 200
    assert client.post('/api/keywords/clean').json['merged'] == 1
    assert client.get('/api/undo/status').json['count'] == 1
    assert client.post('/api/undo').status_code == 200
    assert db.photos.get(pid)['rating'] == original
    assert client.post('/api/redo').status_code == 200
    assert db.photos.get(pid)['rating'] == 1
    assert any(row['action_type'] == 'keyword_clean' for row in client.get('/api/edit-history').json)


@pytest.mark.parametrize('direction', ['use_db', 'use_xmp'])
def test_bulk_repair_records_each_changed_photo(app_and_db, tmp_path, monkeypatch, direction):
    import audit
    from xmp import write_sidecar

    app, db = app_and_db
    photos = db.photos.list_page()
    for photo in photos:
        folder = tmp_path / str(photo['folder_id'])
        folder.mkdir(exist_ok=True)
        db.conn.execute('UPDATE folders SET path = ? WHERE id = ?', (str(folder), photo['folder_id']))
        if direction == 'use_xmp':
            sidecar = (folder / photo['filename']).with_suffix('.xmp')
            write_sidecar(str(sidecar), flat_keywords={'Heron'}, hierarchical_keywords=set())
    db.commit()
    # The untagged third photo has no catalog keywords to queue with use_db.
    pids = [photo['id'] for photo in photos[:2]]
    monkeypatch.setattr(audit, 'check_drift', lambda db: [{'photo_id': pid, 'resolutions': ['use_db', 'use_xmp']} for pid in pids])
    client = app.test_client()
    response = client.post('/api/audit/resolve-all', json={'direction': direction})
    assert response.status_code == 200, response.json
    entries = client.get('/api/edit-history').json
    assert len(entries) == 2
    assert {json.loads(entry['new_value'])['photo_id'] for entry in entries} == set(pids)
    assert all(entry['action_type'] == 'audit_resolve' for entry in entries)
    assert client.get('/api/undo/status').json['available'] is False


def test_maintenance_history_honors_retention_limit(app_and_db, monkeypatch):
    import config as cfg

    app, db = app_and_db
    _duplicates(db)
    original_get = cfg.get
    monkeypatch.setattr(cfg, 'get', lambda key, *args, **kwargs: 1 if key == 'max_edit_history' else original_get(key, *args, **kwargs))
    pid = db.photos.list_page()[0]['id']
    client = app.test_client()
    assert client.post('/api/audit/resolve', json={'photo_id': pid, 'direction': 'use_db'}).status_code == 200
    assert client.post('/api/keywords/clean').json['merged'] == 1
    entries = client.get('/api/edit-history').json
    assert len(entries) == 1
    assert entries[0]['action_type'] == 'keyword_clean'


@pytest.mark.parametrize('direction', ['use_db', 'use_xmp'])
def test_unchanged_repair_does_not_duplicate_history(app_and_db, tmp_path, direction):
    from xmp import write_sidecar

    app, db = app_and_db
    pid, path = _photo(db, tmp_path)
    if direction == 'use_xmp':
        write_sidecar(str(path.with_suffix('.xmp')), flat_keywords={'Heron'}, hierarchical_keywords=set())
    client = app.test_client()
    body = {'photo_id': pid, 'direction': direction}
    assert client.post('/api/audit/resolve', json=body).status_code == 200
    assert len(client.get('/api/edit-history').json) == 1
    assert client.post('/api/audit/resolve', json=body).status_code == 200
    assert len(client.get('/api/edit-history').json) == 1


def test_skipped_xmp_repair_blocks_concurrent_writes_until_history_compared(app_and_db, tmp_path, monkeypatch):
    import audit
    from repositories.audit import AuditRepository

    app, db = app_and_db
    pid, path = _photo(db, tmp_path)
    path.with_suffix('.xmp').write_text('not XML')
    # Simulate a sidecar becoming unreadable after the route's availability check.
    monkeypatch.setattr(audit, 'drift_for_photo', lambda db, pid: None)
    original = AuditRepository.drift_history_state
    snapshots = []

    def state(repository, photo_id):
        if snapshots:
            # The after-snapshot must not admit an unrelated metadata write
            # and then misattribute it to this skipped repair.
            with sqlite3.connect(db._db_path, timeout=0) as writer:
                with pytest.raises(sqlite3.OperationalError, match='locked'):
                    writer.execute('UPDATE photos SET xmp_mtime = 123 WHERE id = ?', (photo_id,))
        snapshots.append(photo_id)
        return original(repository, photo_id)

    monkeypatch.setattr(AuditRepository, 'drift_history_state', state)
    response = app.test_client().post('/api/audit/resolve', json={'photo_id': pid, 'direction': 'use_xmp'})
    assert response.status_code == 200
    assert snapshots == [pid, pid]
    assert db.edit_history.list_recent() == []


def test_bulk_repair_skips_corrupt_sidecars_and_records_only_completed_repairs(app_and_db, tmp_path):
    from xmp import write_sidecar

    app, db = app_and_db
    photos = db.photos.list_page()
    paths = []
    for photo in photos:
        folder = tmp_path / str(photo['folder_id'])
        folder.mkdir(exist_ok=True)
        db.conn.execute('UPDATE folders SET path = ? WHERE id = ?', (str(folder), photo['folder_id']))
        paths.append((folder / photo['filename']).with_suffix('.xmp'))
    db.commit()
    paths[0].write_text('not XML')
    write_sidecar(str(paths[1]), flat_keywords={'Heron'}, hierarchical_keywords=set())
    first_pid, second_pid = (photo['id'] for photo in photos[:2])
    first_keywords = db.photo_keywords.get_for_photo(first_pid)
    client = app.test_client()
    response = client.post('/api/audit/resolve-all', json={'direction': 'use_xmp'})
    assert response.status_code == 200, response.json
    assert response.json['resolved'] == 1
    assert response.json['skipped'] == 1
    assert response.json['warning']
    entries = client.get('/api/edit-history').json
    assert len(entries) == 1
    assert json.loads(entries[0]['new_value']) == {'photo_id': second_pid, 'direction': 'use_xmp'}
    assert db.photo_keywords.get_for_photo(first_pid) == first_keywords
    # The catalog direction is allowed for a corrupt sidecar with known
    # keywords. Record the queued replacement, without claiming an XMP read.
    response = client.post('/api/audit/resolve', json={'photo_id': first_pid, 'direction': 'use_db'})
    assert response.status_code == 200, response.json
    entries = client.get('/api/edit-history').json
    assert len(entries) == 2
    assert json.loads(entries[0]['new_value']) == {'photo_id': first_pid, 'direction': 'use_db'}
    assert 'Queued catalog keywords' in entries[0]['description']
