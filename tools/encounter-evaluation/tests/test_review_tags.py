"""Integration checks against real Vireo writers, using disposable libraries."""
import json

import pytest

from encounter_eval.common import write_json
from encounter_eval.library import prepare
from encounter_eval.review import build_queue, connect, save_review
from encounter_eval.review_tags import sync_review_tags


@pytest.fixture
def writable_queue(tmp_path):
    from db import Database

    path = tmp_path/'library.db'
    db = Database(str(path))
    ws = db._ws_id()
    folder = db.add_folder('/example')
    db.add_workspace_folder(ws, folder)
    pid = db.add_photo(folder, 'bird.jpg', '.jpg', 100, 1, timestamp='2026-01-01T12:00:00', file_hash='original')
    db.conn.executemany('INSERT INTO taxa(id,inat_id,name,common_name,rank) VALUES(?,?,?,?,?)', [
        (1, 101, 'Tringa erythropus', 'Spotted Redshank', 'species'),
        (2, 102, 'Plegadis falcinellus', 'Glossy Ibis', 'species'),
        (3, 103, 'Plegadis', 'Plegadis', 'genus'),
        (4, 104, 'Another taxon', 'Glossy Ibis', 'species'),
    ])
    db.conn.commit()
    old = db.add_keyword('Spotted Redshank', is_species=True, source_taxon_id=101)
    genre = db.add_keyword('Favorite', kw_type='genre')
    genus = db.add_keyword('Plegadis', kw_type='taxonomy')
    db.conn.execute('UPDATE keywords SET taxon_id=3 WHERE id=?', (genus,))
    for kid in [old, genre, genus]:
        db.tag_photo(pid, kid)
    run = tmp_path/'run'
    manifest = prepare(path, run, workspace=ws)
    for entry in manifest['sessions']:
        entry['partition'] = 'train'
    write_json(run/'manifest.json', manifest)
    queue = tmp_path/'reviews.sqlite'
    build_queue(run, queue, path)
    yield queue, db, pid, old, {genre, genus}
    db.close()


def save_and_sync(queue, pid, taxa, *, complete=True, revision=0):
    with connect(queue) as conn:
        save_review(conn, pid, taxa=taxa, complete=complete, revision=revision, update_vireo_tags=True)
        return sync_review_tags(conn, pid)


def keywords(db, pid):
    return {r['id'] for r in db.get_photo_keywords(pid)}


def test_complete_review_replaces_species_preserves_other_tags_and_queues_xmp(writable_queue):
    queue, db, pid, old, preserved = writable_queue
    result = save_and_sync(queue, pid, ['inat:102'])
    assert result['status'] == 'applied', result
    assert old not in keywords(db, pid)
    assert preserved <= keywords(db, pid)
    added = db.conn.execute('''SELECT k.source_taxon_id,pk.source FROM keywords k
        JOIN photo_keywords pk ON pk.keyword_id=k.id WHERE pk.photo_id=? AND k.source_taxon_id=102''', (pid,)).fetchone()
    assert tuple(added) == (102, 'manual')
    pending = {(r['change_type'], r['value']) for r in db.get_pending_changes()}
    assert ('keyword_remove', 'Spotted Redshank') in pending
    stored_name = db.conn.execute('SELECT name FROM keywords WHERE source_taxon_id=102').fetchone()[0]
    assert ('keyword_add', stored_name) in pending
    assert len(db.get_edit_history()) == 2
    with connect(queue) as conn:
        assert sync_review_tags(conn, pid)['status'] == 'applied'
    assert len(db.get_edit_history()) == 2
    # Each change uses existing undo handlers.
    assert db.undo_last_edit()
    assert db.undo_last_edit()
    assert keywords(db, pid) == preserved | {old}


def test_partial_review_adds_and_complete_empty_removes_only_species(writable_queue):
    queue, db, pid, old, preserved = writable_queue
    assert save_and_sync(queue, pid, ['inat:102'], complete=False)['status'] == 'applied'
    assert preserved | {old} <= keywords(db, pid)
    assert len(keywords(db, pid)) == len(preserved) + 2
    assert save_and_sync(queue, pid, [], revision=1)['status'] == 'applied'
    assert keywords(db, pid) == preserved


def test_homonyms_use_selected_identity(writable_queue):
    queue, db, pid, old, preserved = writable_queue
    assert save_and_sync(queue, pid, ['inat:104'])['status'] == 'applied'
    assert db.conn.execute('''SELECT k.source_taxon_id FROM keywords k JOIN photo_keywords pk ON pk.keyword_id=k.id
        WHERE pk.photo_id=? AND k.source_taxon_id IS NOT NULL''', (pid,)).fetchone()[0] == 104


def test_library_failure_rolls_back_tags_but_retains_reference_and_retry(writable_queue, monkeypatch):
    from db import Database

    queue, db, pid, old, preserved = writable_queue
    original = Database.record_edit
    def fail(*args, **kwargs):
        raise OSError('simulated disk failure')
    monkeypatch.setattr(Database, 'record_edit', fail)
    result = save_and_sync(queue, pid, ['inat:102'])
    assert result['status'] == 'pending'
    assert 'simulated disk failure' in result['error']
    assert keywords(db, pid) == preserved | {old}
    assert not db.get_pending_changes()
    with connect(queue) as conn:
        assert json.loads(conn.execute('SELECT taxa FROM reviews').fetchone()[0]) == ['inat:102']
        monkeypatch.setattr(Database, 'record_edit', original)
        assert sync_review_tags(conn, pid)['status'] == 'applied'


def test_retry_after_library_commit_does_not_repeat_or_overwrite_later_edits(writable_queue, monkeypatch):
    from encounter_eval import review_tags

    queue, db, pid, old, preserved = writable_queue
    original = review_tags._apply_tags
    def interrupt_after_commit(*args):
        original(*args)
        raise OSError('simulated interruption before review acknowledgement')
    monkeypatch.setattr(review_tags, '_apply_tags', interrupt_after_commit)
    assert save_and_sync(queue, pid, ['inat:102'])['status'] == 'pending'
    added = (keywords(db, pid) - preserved).pop()
    db.untag_photo(pid, added)  # An explicit later user edit must survive retry.
    monkeypatch.setattr(review_tags, '_apply_tags', original)
    with connect(queue) as conn:
        assert sync_review_tags(conn, pid)['status'] == 'applied'
    assert keywords(db, pid) == preserved
    assert len(db.get_edit_history()) == 2


@pytest.mark.parametrize('change', ['identity', 'hash_cleared', 'workspace'])
def test_tag_updates_reject_changed_photo_or_workspace(writable_queue, change):
    queue, db, pid, old, preserved = writable_queue
    if change == 'identity':
        db.conn.execute("UPDATE photos SET file_hash='replacement' WHERE id=?", (pid,))
    elif change == 'hash_cleared':
        db.conn.execute('UPDATE photos SET file_hash=NULL WHERE id=?', (pid,))
    else:
        db.conn.execute('DELETE FROM workspace_folders')
    db.conn.commit()
    result = save_and_sync(queue, pid, ['inat:102'])
    assert result['status'] == 'pending'
    assert 'no longer matches' in result['error']
    assert keywords(db, pid) == preserved | {old}
    assert not db.get_pending_changes()


def test_http_save_updates_tags_and_reports_retriable_failure(writable_queue, monkeypatch):
    import threading
    from urllib.parse import parse_qs, urlparse
    from urllib.request import Request, urlopen

    from db import Database

    from encounter_eval.review_server import make_server

    queue, db, pid, old, preserved = writable_queue
    server = make_server(queue, update_vireo_tags=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    parsed = urlparse(server.review_url)
    base = f'{parsed.scheme}://{parsed.netloc}'
    headers = {'Authorization': 'Bearer '+parse_qs(parsed.query)['token'][0], 'Content-Type':'application/json'}
    def request(path, data=None):
        with urlopen(Request(base+path, headers=headers, data=json.dumps(data).encode() if data else None)) as response:
            return json.load(response)
    try:
        assert request('/api/status')['update_vireo_tags'] is True
        original = Database.record_edit
        def fail(*args, **kwargs):
            raise OSError('library temporarily unavailable')
        monkeypatch.setattr(Database, 'record_edit', fail)
        answer = request('/api/review', {'photo_id': pid, 'taxa':['inat:102'], 'complete':True})
        assert answer['revision'] == 1
        assert answer['tag_sync']['status'] == 'pending'
        assert request('/api/status')['tag_updates_pending'] == 1
        assert request(f'/api/photo/{pid}')['tag_sync']['error'] == 'library temporarily unavailable'
        assert keywords(db, pid) == preserved | {old}
        monkeypatch.setattr(Database, 'record_edit', original)
        answer = request('/api/retry-tags', {'photo_id':pid})
        assert answer['tag_sync']['status'] == 'applied'
        assert request('/api/status')['tag_updates_pending'] == 0
        assert old not in keywords(db, pid)
        assert preserved <= keywords(db, pid)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_individually_shared_photo_can_sync_review_tags(writable_queue):
    queue, db, pid, old, preserved = writable_queue
    db.grant_workspace_photos(db._ws_id(), [pid])
    db.conn.execute('DELETE FROM workspace_folders')
    db.conn.commit()
    result = save_and_sync(queue, pid, ['inat:102'])
    assert result['status'] == 'applied', result
    assert old not in keywords(db, pid)
    assert preserved <= keywords(db, pid)
    assert db.get_pending_changes()
