"""Identity decisions survive each catalog collapse, or reject atomically."""
from pathlib import Path

import pytest
from db import Database
from services import photo_ingest
from services.identity_alternatives import resolve_alternatives, save_alternatives


@pytest.fixture
def db(tmp_path):
    value = Database(str(tmp_path / 'collapse.db'))
    yield value
    value.close()


def _photo(db, folder, filename):
    return photo_ingest.add_photo(db, folder_id=folder, filename=filename,
                                  extension=Path(filename).suffix, file_size=5, file_mtime=1)


def _fixture(db, tmp_path, path):
    target_path = tmp_path / 'target'
    target_path.mkdir()
    target = db.add_folder(str(target_path))
    source = db.add_folder(str(tmp_path / 'source')) if path == 'folder' else target
    survivor = _photo(db, target, 'bird.jpg' if path == 'folder' else 'bird.raf')
    losing = _photo(db, source, 'bird.jpg')
    for filename in ('bird.jpg', 'bird.raf'):
        (target_path / filename).write_bytes(b'bytes')
    return source, target, survivor, losing, target_path


@pytest.mark.parametrize('path', ['folder', 'companion'])
@pytest.mark.parametrize('status', ['unresolved', 'resolved'])
@pytest.mark.parametrize('drained', [False, True])
def test_collapse_preserves_identity_tags_provenance_and_replay(db, tmp_path, path, status, drained):
    source, target, survivor, losing, target_path = _fixture(db, tmp_path, path)
    ws = db.require_workspace_id()
    sharing = db.create_workspace('Shared collapse')
    db.set_active_workspace(ws)
    db.add_workspace_folder(sharing, source)
    note = db.add_keyword('Unrelated note')
    db.tag_photo(survivor, note, source='manual')
    db.photo_review.set_flag(survivor, 'rejected')
    with db._commits_held():
        saved = save_alternatives(db, [losing], ['Heron', 'Egret'], 'Distant frame', 'Bill')
        edited = resolve_alternatives(db, [losing], 'Heron') if status == 'resolved' else saved
    if drained:
        db.conn.execute('DELETE FROM pending_changes')
        db.commit()
    from sync import sync_from_xmp, sync_to_xmp
    from xmp import read_keywords, write_sidecar
    sidecar = str(target_path / 'bird.xmp')
    write_sidecar(sidecar, flat_keywords={'Unrelated note'}, hierarchical_keywords=set())
    pending = [dict(r) for r in db.conn.execute("SELECT * FROM pending_changes WHERE photo_id=? AND change_type LIKE 'keyword_%'", (losing,))]
    db.conn.execute('UPDATE pending_changes SET sync_started=1 WHERE photo_id=?', (losing,))
    db.commit()
    before = db.identity_alternatives.for_photos([losing], include_resolved=True)[losing]
    if path == 'folder':
        db.folders.merge_into_existing(source, target, str(target_path))
    else:
        from scanner.companions import _pair_raw_jpeg_companions
        assert _pair_raw_jpeg_companions(db) == {losing: survivor}
    assert db.photos.get(losing) is None
    assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == before
    links = db.identity_alternatives.candidate_links([survivor], ['Heron', 'Egret']).get(survivor, [])
    assert bool(links) == (status == 'resolved')
    if links:
        assert [row['source'] for row in links] == ['manual']
    assert db.photos.get(survivor)['flag'] == 'rejected'
    assert note in {r['id'] for r in db.photo_keywords.get_for_photos([survivor])[survivor]}
    for row in pending:
        moved = db.conn.execute('SELECT photo_id,change_token,sync_started FROM pending_changes WHERE id=?', (row['id'],)).fetchone()
        assert moved is not None and moved['photo_id'] == survivor
        assert moved['change_token'] != row['change_token'] and moved['sync_started'] == 1
        from services.pending_changes import clear_pending_by_token
        clear_pending_by_token(db, [row['change_token']])
        assert db.conn.execute('SELECT 1 FROM pending_changes WHERE change_token=?', (moved['change_token'],)).fetchone()
    for action in (None, 'undo', 'redo'):
        db.set_active_workspace(ws)
        if action:
            result = db.undo_last_edit() if action == 'undo' else db.redo_last_undo()
            assert result['id'] == edited
        db.set_active_workspace(sharing)
        assert sync_to_xmp(db)['failed'] == 0
        assert 'Unrelated note' in read_keywords(sidecar)
        expected = (status == 'resolved' and action != 'undo')
        assert ('Heron' in read_keywords(sidecar)) == expected
        sync_from_xmp(db, [survivor])
        assert bool(db.identity_alternatives.candidate_links([survivor], ['Heron', 'Egret'])) == expected
        assert db.photos.get(survivor)['flag'] == 'rejected'
    db.set_active_workspace(ws)
    assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == before
    db.set_active_workspace(sharing)
    assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == before


@pytest.mark.parametrize('path', ['folder', 'companion'])
@pytest.mark.parametrize('conflict', ['decision', 'confirmation'])
def test_collapse_conflicts_keep_pair_data_and_folder_transaction_untouched(db, tmp_path, path, conflict):
    from repositories.identity_alternatives import IdentityMergeConflict
    source, target, survivor, losing, target_path = _fixture(db, tmp_path, path)
    with db._commits_held():
        save_alternatives(db, [losing], ['Heron', 'Egret'], 'Original evidence', 'Bill')
        if conflict == 'decision':
            save_alternatives(db, [survivor], ['Heron', 'Egret'], 'Other evidence', 'Call')
        else:
            keyword = db.add_keyword('Heron', is_species=True, _commit=False)
            db.tag_photo(survivor, keyword, source='manual', _commit=False)
    # A folder rejection must preserve unrelated writes in its outer transaction.
    db.conn.execute('UPDATE photos SET rating=4 WHERE id=?', (survivor,))
    tables = ['photos', 'folders', 'photo_keywords', 'photo_identity_alternatives', 'pending_changes', 'edit_history_items']
    before = {table: [tuple(r) for r in db.conn.execute(f'SELECT * FROM {table}')] for table in tables}
    if path == 'folder':
        with pytest.raises(IdentityMergeConflict):
            db.folders.merge_into_existing(source, target, str(target_path))
    else:
        from scanner.companions import _pair_raw_jpeg_companions
        assert _pair_raw_jpeg_companions(db) == {}
    for table in tables:
        assert [tuple(r) for r in db.conn.execute(f'SELECT * FROM {table}')] == before[table]
    db.conn.rollback()
    assert (db.photos.get(survivor)['rating'] != 4) == (path == 'folder')


@pytest.mark.parametrize('path', ['folder', 'companion'])
def test_collapse_keeps_current_tags_for_redo_only_identity_history(db, tmp_path, path):
    source, target, survivor, losing, target_path = _fixture(db, tmp_path, path)
    keyword = db.add_keyword('Heron', is_species=True)
    db.tag_photo(losing, keyword, source='accept')
    with db._commits_held():
        saved = save_alternatives(db, [losing], ['Heron', 'Egret'], 'Redo evidence', 'Bill')
    db.undo_last_edit()
    if path == 'folder':
        db.folders.merge_into_existing(source, target, str(target_path))
    else:
        from scanner.companions import _pair_raw_jpeg_companions
        _pair_raw_jpeg_companions(db)
    assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (survivor, keyword)).fetchone()[0] == 'accept'
    assert db.redo_last_undo()['id'] == saved
    assert db.identity_alternatives.for_photos([survivor])[survivor]['reason'] == 'Redo evidence'
    db.undo_last_edit()
    assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (survivor, keyword)).fetchone()[0] == 'accept'


@pytest.mark.parametrize('path', ['delete', 'audit'])
def test_real_deletion_cascades_identity_and_history_only_for_deleted_photos(db, tmp_path, path):
    _, _, survivor, losing, _ = _fixture(db, tmp_path, 'folder')
    with db._commits_held():
        save_alternatives(db, [losing, survivor], ['Heron', 'Egret'], 'Independent evidence', 'Bill')
        resolve_alternatives(db, [losing, survivor], 'Heron')
    if path == 'delete':
        db.delete_photos([losing])
    else:
        db.audit.remove_orphan_rows([losing])
        db.commit()
    assert db.photos.get(losing) is None
    assert not db.conn.execute('SELECT 1 FROM photo_identity_alternatives WHERE photo_id=?', (losing,)).fetchone()
    assert not db.conn.execute('SELECT 1 FROM edit_history_items WHERE photo_id=?', (losing,)).fetchone()
    assert survivor in db.identity_alternatives.for_photos([survivor], include_resolved=True)
    db.undo_last_edit()
    db.redo_last_undo()


@pytest.mark.parametrize('failure', [RuntimeError, KeyboardInterrupt, GeneratorExit])
def test_identity_replay_cleanup_rolls_back_and_keeps_writer_lock(db, tmp_path, failure):
    import sqlite3
    _, _, survivor, _, _ = _fixture(db, tmp_path, 'folder')
    original = db.photos.get(survivor)['rating']
    with pytest.raises(failure), db._edit_history_repository().identity_replay():
        db.conn.execute('UPDATE photos SET rating=5 WHERE id=?', (survivor,))
        raise failure('Replay interrupted')
    assert db.photos.get(survivor)['rating'] == original
    assert db.conn.in_transaction
    other = sqlite3.connect(db._db_path, timeout=0)
    try:
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            other.execute('UPDATE photos SET rating=1 WHERE id=?', (survivor,))
    finally:
        other.close()
    db.conn.rollback()


@pytest.mark.parametrize('same_hash', [True, False])
def test_staged_identity_intents_cannot_be_acknowledged_by_old_path_writer(db, tmp_path, same_hash):
    from services.pending_changes import clear_pending_by_token
    from test_db_moves_merge import _collision_tree
    tree = _collision_tree(db, tmp_path, same_hash=same_hash)
    db.add_workspace_folder(db.require_workspace_id(), tree['stage_root'])
    losing, survivor = (tree['staged'], tree['survivor']) if same_hash else (tree['survivor'], tree['staged'])
    with db._commits_held():
        save_alternatives(db, [losing], ['Heron', 'Egret'], 'Moved decision', 'Bill')
        resolve_alternatives(db, [losing], 'Heron')
    before = [dict(row) for row in db.conn.execute('SELECT * FROM pending_changes WHERE photo_id=?', (losing,))]
    assert before
    db.conn.execute('UPDATE pending_changes SET sync_started=1 WHERE photo_id=?', (losing,))
    db.commit()
    counts = db.merge_staged_tree_into_archive(tree['stage_root'], tree['arch'])
    clear_pending_by_token(db, [row['change_token'] for row in before])
    for row in before:
        replacement = db.conn.execute('SELECT * FROM pending_changes WHERE id=?', (row['id'],)).fetchone()
        assert replacement['photo_id'] == survivor
        assert replacement['change_token'] != row['change_token']
        assert replacement['sync_started'] == 1
        if same_hash:
            assert replacement['change_token'] in counts['preserved_off_staging_identities']


@pytest.mark.parametrize('commit', [False, True])
def test_folder_identity_conflict_rolls_back_earlier_duplicate_carries(db, tmp_path, commit):
    from repositories.identity_alternatives import IdentityMergeConflict
    source, target, survivor, losing, target_path = _fixture(db, tmp_path, 'folder')
    keyword = db.add_keyword('Earlier embedded note')
    db.tag_photo(losing, keyword, source='manual')
    db.photo_keywords.record_embedded_offered_keys(losing, ['earlier embedded note'])
    later_target = _photo(db, target, 'later.jpg')
    later_source = _photo(db, source, 'later.jpg')
    with db._commits_held():
        save_alternatives(db, [later_source], ['Heron', 'Egret'], 'Source evidence', 'Bill')
        save_alternatives(db, [later_target], ['Heron', 'Egret'], 'Target evidence', 'Call')
    assert keyword not in {row['id'] for row in db.photo_keywords.get_for_photos([survivor]).get(survivor, [])}
    before = [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords ORDER BY photo_id,keyword_id')]
    with pytest.raises(IdentityMergeConflict):
        db.folders.merge_into_existing(source, target, str(target_path), commit=commit)
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords ORDER BY photo_id,keyword_id')] == before
    assert db.photos.get(losing) is not None
    assert db.photos.get(later_source) is not None
