"""Identity-bearing folder and companion collapses retain current decisions."""

import pytest
from db import Database
from repositories.identity_alternatives import IdentityMergeConflict
from scanner.companions import _pair_raw_jpeg_companions
from services import photo_ingest, workspace_folders
from services.edit_history import redo_last_undo, undo_last_edit
from services.identity_alternatives import resolve_alternatives, save_alternatives

CANDIDATES = ['Tropical Kingbird', "Couch's Kingbird"]


@pytest.fixture
def catalog(tmp_path):
    with Database(str(tmp_path / 'collapse.db')) as db:
        yield db


def photo(db, folder, filename):
    return photo_ingest.add_photo(db, folder_id=folder, filename=filename,
                                  extension='.' + filename.rsplit('.', 1)[1],
                                  file_size=5, file_mtime=1.0, timestamp='2026-01-01T10:00:00')


def folder_twins(db, tmp_path):
    target_path = tmp_path / 'target'
    target_path.mkdir()
    target = workspace_folders.add_folder(db, str(target_path))
    source = workspace_folders.add_folder(db, str(tmp_path / 'source'))
    survivor = photo(db, target, 'bird.raf')
    losing = photo(db, source, 'bird.raf')
    return source, target, losing, survivor, str(target_path)


def rows(db):
    tables = ['photos', 'folders', 'photo_identity_alternatives', 'photo_keywords',
              'pending_changes', 'edit_history', 'edit_history_items', 'workspace_photos']
    return {table: sorted(tuple(row) for row in db.conn.execute(f'SELECT * FROM {table}')) for table in tables}


@pytest.mark.parametrize('synced', [False, True])
@pytest.mark.parametrize('active', [False, True])
def test_folder_merge_keeps_resolved_tags_provenance_pending_and_history(catalog, tmp_path, synced, active):
    db = catalog
    source, target, losing, survivor, path = folder_twins(db, tmp_path)
    first = db.require_workspace_id()
    second = db.create_workspace('Shared identification')
    db.photo_visibility.grant(second, [losing])
    unrelated = db.add_keyword('American Robin', is_species=True)
    db.tag_photo(survivor, unrelated, source='manual')
    db.commit()
    db.photo_review.set_flag(survivor, 'rejected')
    db.commit()
    with db.transaction():
        save_alternatives(db, [losing], CANDIDATES, 'Uncertain calls', 'Audio')
        edit = resolve_alternatives(db, [losing], CANDIDATES[0])
    before = db.identity_alternatives.for_photos([losing], include_resolved=True)[losing]
    if synced:
        db.conn.execute('DELETE FROM pending_changes')
        db.commit()
    if not active:
        db.set_active_workspace(None)
    db.folders.merge_into_existing(source, target, path)
    assert db.active_workspace_id == (first if active else None)
    db.set_active_workspace(first)
    assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == before
    links = db.photo_keywords.get_for_photos([survivor])[survivor]
    candidate = next(row for row in links if row['name'] == CANDIDATES[0])
    assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (survivor, candidate['id'])).fetchone()[0] == 'manual'
    assert unrelated in {row['id'] for row in links}
    assert db.photos.get(survivor)['flag'] == 'rejected'
    assert not db.conn.execute('SELECT 1 FROM pending_changes WHERE photo_id=?', (losing,)).fetchone()
    assert {row[0] for row in db.conn.execute("SELECT workspace_id FROM pending_changes WHERE photo_id=? AND change_type='keyword_add' AND value=?", (survivor, CANDIDATES[0]))} == {first, second}
    assert undo_last_edit(db)['id'] == edit
    assert db.identity_alternatives.for_photos()[survivor]['status'] == 'unresolved'
    assert redo_last_undo(db)['id'] == edit
    assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == before
    assert db.photos.get(survivor)['flag'] == 'rejected'
    db.set_active_workspace(second)
    assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == before


def test_folder_merge_conflict_rolls_back_earlier_duplicate_and_all_pending(catalog, tmp_path):
    db = catalog
    source, target, losing, survivor, path = folder_twins(db, tmp_path)
    losing2 = photo(db, source, 'other.raf')
    survivor2 = photo(db, target, 'other.raf')
    with db.transaction():
        save_alternatives(db, [losing], CANDIDATES, 'First evidence', 'Audio')
        resolve_alternatives(db, [losing], CANDIDATES[0])
        save_alternatives(db, [losing2], CANDIDATES, 'Other evidence', 'Audio')
        save_alternatives(db, [survivor2], CANDIDATES, 'Distinct evidence', 'Video')
    before = rows(db)
    with pytest.raises(IdentityMergeConflict, match='conflicting identity'):
        db.folders.merge_into_existing(source, target, path)
    assert not db.conn.in_transaction
    assert rows(db) == before
    assert db.photos.get(losing) is not None and db.photos.get(survivor) is not None


@pytest.mark.parametrize('resolved', [False, True])
def test_companion_pairing_keeps_identity_tags_and_normal_history(catalog, tmp_path, resolved):
    db = catalog
    folder = workspace_folders.add_folder(db, str(tmp_path))
    primary, companion = photo(db, folder, 'bird.raf'), photo(db, folder, 'bird.jpg')
    unrelated = db.add_keyword('American Robin', is_species=True)
    db.tag_photo(primary, unrelated)
    db.commit()
    with db.transaction():
        edit = save_alternatives(db, [companion], CANDIDATES, 'Uncertain call', 'Audio')
        if resolved:
            edit = resolve_alternatives(db, [companion], CANDIDATES[0])
    before = db.identity_alternatives.for_photos([companion], include_resolved=True)[companion]
    db.conn.execute('DELETE FROM pending_changes')
    db.commit()
    assert _pair_raw_jpeg_companions(db) == {companion: primary}
    assert db.identity_alternatives.for_photos([primary], include_resolved=True)[primary] == before
    assert unrelated in {row['id'] for row in db.photo_keywords.get_for_photos([primary])[primary]}
    assert undo_last_edit(db)['id'] == edit
    assert redo_last_undo(db)['id'] == edit
    assert db.identity_alternatives.for_photos([primary], include_resolved=True)[primary] == before


def test_companion_identity_conflict_keeps_pair_and_commits_earlier_pair(catalog, tmp_path):
    db = catalog
    folder = workspace_folders.add_folder(db, str(tmp_path))
    raw, jpeg = photo(db, folder, 'first.raf'), photo(db, folder, 'first.jpg')
    raw2, jpeg2 = photo(db, folder, 'second.raf'), photo(db, folder, 'second.jpg')
    with db.transaction():
        save_alternatives(db, [jpeg], CANDIDATES, 'First review', 'Audio')
        resolve_alternatives(db, [jpeg], CANDIDATES[0])
        save_alternatives(db, [raw2], CANDIDATES, 'RAW evidence', 'Audio')
        save_alternatives(db, [jpeg2], CANDIDATES, 'JPEG evidence', 'Video')
    before_photos = [db.photos.get(pid) for pid in (raw2, jpeg2)]
    tables = ['photo_identity_alternatives', 'photo_keywords', 'pending_changes', 'edit_history_items']
    before = {table: [tuple(row) for row in db.conn.execute(f'SELECT * FROM {table} WHERE photo_id IN (?,?) ORDER BY photo_id', (raw2, jpeg2))] for table in tables}
    assert _pair_raw_jpeg_companions(db) == {jpeg: raw}
    assert not db.conn.in_transaction
    assert [db.photos.get(pid) for pid in (raw2, jpeg2)] == before_photos
    for table in tables:
        assert [tuple(row) for row in db.conn.execute(f'SELECT * FROM {table} WHERE photo_id IN (?,?) ORDER BY photo_id', (raw2, jpeg2))] == before[table]
    assert db.photos.get(raw)['companion_path'] == 'first.jpg'
    assert db.photos.get(jpeg) is None


@pytest.mark.parametrize('kind', ['folder', 'companion'])
def test_collapse_does_not_reintroduce_candidate_outvoted_by_newer_visible_review(catalog, tmp_path, kind):
    db = catalog
    if kind == 'folder':
        source, target, losing, survivor, path = folder_twins(db, tmp_path)
    else:
        folder = workspace_folders.add_folder(db, str(tmp_path))
        survivor, losing = photo(db, folder, 'bird.raf'), photo(db, folder, 'bird.jpg')
    keyword = db.add_keyword(CANDIDATES[0], is_species=True)
    db.tag_photo(losing, keyword)
    db.commit()
    db.queue_change(losing, 'keyword_add', CANDIDATES[0])
    db.commit()
    db.conn.execute("UPDATE pending_changes SET created_at='2020-01-01' WHERE photo_id=?", (losing,))
    db.commit()
    with db.transaction():
        save_alternatives(db, [survivor], CANDIDATES, 'Newer uncertain review', 'Audio')
    if kind == 'folder':
        db.folders.merge_into_existing(source, target, path)
    else:
        assert _pair_raw_jpeg_companions(db) == {losing: survivor}
    assert not db.identity_alternatives.candidate_links([survivor], CANDIDATES)
    assert db.identity_alternatives.for_photos()[survivor]['reason'] == 'Newer uncertain review'
    assert not db.conn.execute("SELECT 1 FROM pending_changes WHERE photo_id=? AND change_type='keyword_add' AND value=?", (survivor, CANDIDATES[0])).fetchone()


def test_folder_collapse_repairs_alias_intents_in_both_visible_workspaces(catalog, tmp_path):
    from sync import sync_to_xmp
    from xmp import read_keywords

    db = catalog
    source, target, losing, survivor, path = folder_twins(db, tmp_path)
    (tmp_path / 'target' / 'bird.raf').write_bytes(b'bytes')
    first = db.require_workspace_id()
    second = db.create_workspace('Shared alias review')
    db.photo_visibility.grant(second, [losing])
    db.conn.execute("INSERT INTO taxa(id,inat_id,name,common_name,rank) VALUES (93001,99999,'Tyrannus melancholicus','Tropical Kingbird','species')")
    scientific = db.add_keyword('Tyrannus melancholicus', is_species=True)
    db.commit()
    scientific_name = db.keywords.name_of(scientific)
    with db.transaction():
        save_alternatives(db, [losing], CANDIDATES, 'Uncertain', 'Audio')
        db.tag_photo(losing, scientific, source='manual')
        resolve_alternatives(db, [losing], CANDIDATES[0])
    db.queue_change(survivor, 'keyword_add', CANDIDATES[0], workspace_id=first)
    db.commit()
    db.queue_change(losing, 'keyword_remove', CANDIDATES[0], workspace_id=second)
    db.commit()
    old_token = db.conn.execute("SELECT change_token FROM pending_changes WHERE photo_id=? AND change_type='keyword_add' AND value=?", (losing, scientific_name)).fetchone()[0]
    db.conn.execute("UPDATE pending_changes SET sync_started=1 WHERE change_token=?", (old_token,))
    db.commit()
    db.folders.merge_into_existing(source, target, path)
    assert not db.conn.execute('SELECT 1 FROM pending_changes WHERE change_token=?', (old_token,)).fetchone()
    assert not db.conn.execute("SELECT 1 FROM pending_changes WHERE photo_id=? AND change_type='keyword_add' AND value=?", (survivor, CANDIDATES[0])).fetchone()
    assert {row[0] for row in db.conn.execute("SELECT workspace_id FROM pending_changes WHERE photo_id=? AND change_type='keyword_add' AND value=? AND sync_started=1", (survivor, scientific_name))} == {first, second}
    assert db.active_workspace_id == first
    for workspace in (second, first):
        db.set_active_workspace(workspace)
        assert sync_to_xmp(db)['failed'] == 0
        assert scientific_name in read_keywords(str(tmp_path / 'target' / 'bird.xmp'))
        assert CANDIDATES[0] not in read_keywords(str(tmp_path / 'target' / 'bird.xmp'))
