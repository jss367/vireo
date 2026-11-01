"""Automatic pairing skips only conflicting identities, preserving scan progress."""
import json
import logging

import pytest
from db import Database
from PIL import Image
from repositories.identity_alternatives import IdentityMergeConflict
from scanner import ScanOptions, scan
from services import photo_ingest, workspace_folders
from services.identity_alternatives import resolve_alternatives, save_alternatives


def _catalog(tmp_path, conflict_first, *, conflicting=True):
    db = Database(str(tmp_path / 'pairing.db'))
    existing = tmp_path / 'existing'
    existing.mkdir()
    folder = workspace_folders.add_folder(db, str(existing))
    good_stem, bad_stem = ('b-good', 'a-conflict') if conflict_first else ('a-good', 'b-conflict')
    pairs = {}
    for key, stem in [('good', good_stem), ('bad', bad_stem)]:
        pair = [photo_ingest.add_photo(db, folder_id=folder, filename=stem + extension, extension=extension, file_size=1, file_mtime=1) for extension in ('.raf', '.jpg')]
        pairs[key] = pair
    if conflicting:
        with db.transaction():
            save_alternatives(db, [pairs['bad'][0]], ['Heron', 'Egret'], 'RAW evidence', 'Bill')
            save_alternatives(db, [pairs['bad'][1]], ['Heron', 'Egret'], 'JPEG evidence', 'Call')
    sharing = db.create_workspace('Shared pair review')
    db.set_active_workspace(1)
    workspace_folders.add_workspace_folder(db, sharing, folder)
    members = sum(pairs.values(), [])
    collection = db.conn.execute('INSERT INTO collections(name,rules,workspace_id) VALUES (?,?,?)',
                                 ('Pairs', json.dumps([{'field': 'photo_ids', 'value': members}]), 1)).lastrowid
    db.commit()
    root = tmp_path / 'unrelated-scan'
    root.mkdir()
    Image.new('RGB', (40, 30)).save(root / 'new.jpg')
    return db, pairs, sharing, collection, root


def _pair_state(db, ids):
    marks = ','.join('?' for _ in ids)
    result = {'photos': [tuple(row) for row in db.conn.execute(f'SELECT * FROM photos WHERE id IN ({marks}) ORDER BY id', ids)]}
    for table in ('photo_keywords', 'photo_identity_alternatives', 'pending_changes', 'edit_history_items'):
        result[table] = [tuple(row) for row in db.conn.execute(f'SELECT * FROM {table} WHERE photo_id IN ({marks}) ORDER BY photo_id', ids)]
    return result


def _members(db, collection):
    return json.loads(db.conn.execute('SELECT rules FROM collections WHERE id=?', (collection,)).fetchone()[0])[0]['value']


@pytest.mark.parametrize('conflict_first', [False, True])
def test_unrelated_scan_completes_keeps_conflicting_pair_and_merges_other_pairs(tmp_path, caplog, conflict_first):
    db, pairs, sharing, collection, root = _catalog(tmp_path, conflict_first)
    try:
        before = _pair_state(db, pairs['bad'])
        with caplog.at_level(logging.WARNING):
            result = scan(str(root), db, options=ScanOptions(skip_working_copies=True))
        assert result['indexed'] == 1
        assert _pair_state(db, pairs['bad']) == before
        assert db.photos.get(pairs['good'][1]) is None
        assert db.photos.get(pairs['good'][0])['companion_path']
        assert set(_members(db, collection)) == {*pairs['bad'], pairs['good'][0]}
        db.set_active_workspace(sharing)
        assert set(pairs['bad']) <= set(db.photo_visibility.visible_photo_ids(pairs['bad']))
        assert any(record.levelno >= logging.WARNING and 'separate' in record.getMessage()
                   and all(str(pid) in record.getMessage() for pid in pairs['bad']) for record in caplog.records)
    finally:
        db.close()


@pytest.mark.parametrize('domain_error', [False, True])
def test_late_pair_failure_restores_collection_state_and_discards_deferred_actions(tmp_path, monkeypatch, domain_error):
    import scanner.companions as companions
    db, pairs, _, collection, root = _catalog(tmp_path, False, conflicting=False)
    with db.transaction():
        save_alternatives(db, [pairs['bad'][1]], ['Heron', 'Egret'], 'Retain late-failure evidence', 'Call')
        resolve_alternatives(db, [pairs['bad'][1]], 'Heron')
        db.conn.execute('UPDATE pending_changes SET sync_started=1 WHERE photo_id=?', (pairs['bad'][1],))
    before = _pair_state(db, sum(pairs.values(), []))
    original_members = _members(db, collection)
    bad_before = _pair_state(db, pairs['bad'])
    original = companions._merge_companion_into_primary
    actions = []

    def fail_after_delete(db, primary, companion, vireo_dir, thumb_cache_dir, post_commit_fs_actions, photo_rows):
        result = original(db, primary, companion, vireo_dir, thumb_cache_dir, post_commit_fs_actions, photo_rows)
        post_commit_fs_actions.append(lambda: actions.append(companion['id']))
        if companion['id'] == pairs['bad'][1]:
            error = IdentityMergeConflict if domain_error else RuntimeError
            raise error('injected after deletion')
        return result

    monkeypatch.setattr(companions, '_merge_companion_into_primary', fail_after_delete)
    try:
        if domain_error:
            assert companions._pair_raw_jpeg_companions(db) == {pairs['good'][1]: pairs['good'][0]}
            assert _pair_state(db, pairs['bad']) == bad_before
            assert all(db.photos.get(pid) is not None for pid in pairs['bad'])
            assert set(_members(db, collection)) == {*pairs['bad'], pairs['good'][0]}
            assert actions == [pairs['good'][1]]
        else:
            with pytest.raises(RuntimeError, match='after deletion'):
                scan(str(root), db, options=ScanOptions(skip_working_copies=True))
            assert _pair_state(db, sum(pairs.values(), [])) == before
            assert _members(db, collection) == original_members
            assert actions == []
    finally:
        db.close()
