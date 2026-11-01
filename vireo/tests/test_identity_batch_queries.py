"""Bound full-catalog identity resolution work for a multi-photo decision."""
import pytest
from db import Database
from services import photo_ingest, workspace_folders
from services.edit_history import redo_last_undo, undo_last_edit
from services.identity_alternatives import resolve_alternatives, save_alternatives


@pytest.mark.parametrize('operation', ['save', 'resolve', 'undo', 'redo'])
def test_identity_batch_bounds_full_keyword_scans(tmp_path, operation):
    db = Database(str(tmp_path / 'batch.db'))
    try:
        folder = workspace_folders.add_folder(db, str(tmp_path / 'photos'))
        photos = [photo_ingest.add_photo(db, folder_id=folder, filename=f'{i}.jpg', extension='.jpg', file_size=1, file_mtime=1) for i in range(20)]
        db.conn.executemany("INSERT INTO keywords(name,type,is_species) VALUES (?,'taxonomy',1)", [(f'Catalog species {i}',) for i in range(2000)])
        db.commit()
        if operation != 'save':
            with db.transaction():
                save_alternatives(db, photos, ['Heron', 'Egret'], 'Distant frames', 'Bill')
        if operation in ('undo', 'redo'):
            with db.transaction():
                resolve_alternatives(db, photos, 'Heron')
        if operation == 'redo':
            undo_last_edit(db)
        scans = []
        db.conn.set_trace_callback(lambda sql: scans.append(sql) if "FROM keywords k " in sql and "k.is_species=1 OR k.type='taxonomy'" in sql else None)
        if operation == 'save':
            with db.transaction():
                save_alternatives(db, photos, ['Heron', 'Egret'], 'Distant frames', 'Bill')
        elif operation == 'resolve':
            with db.transaction():
                resolve_alternatives(db, photos, 'Heron')
        elif operation == 'undo':
            undo_last_edit(db)
        else:
            redo_last_undo(db)
        db.conn.set_trace_callback(None)
        assert len(scans) <= 10, f'{operation}: {len(scans)} full scans for 20 photos and 2000 species'
    finally:
        db.close()


def test_resolution_refreshes_identity_after_keyword_creation_in_same_transaction(tmp_path):
    db = Database(str(tmp_path / 'creation.db'))
    try:
        folder = workspace_folders.add_folder(db, str(tmp_path / 'photos'))
        photo = photo_ingest.add_photo(db, folder_id=folder, filename='bird.jpg', extension='.jpg', file_size=1, file_mtime=1)
        common = db.add_keyword('Choice')  # promoted by resolution, not a species yet
        alias = db.add_keyword('Scientific alias', is_species=True, source_taxon_id=99001)
        db.tag_photo(photo, alias, source='accept')
        db.commit()
        with db.transaction():
            # Warm name resolution before either the taxonomy or promoted keyword exists.
            save_alternatives(db, [photo], ['Choice', 'Other'], 'Distant frame', 'Bill')
            db.conn.execute("INSERT INTO taxa(id,inat_id,name,common_name,rank) VALUES (99001,99001,'Scientific alias','Choice','species')")
            resolve_alternatives(db, [photo], 'Choice')
        row = db.conn.execute('SELECT is_species,taxon_id FROM keywords WHERE id=?', (common,)).fetchone()
        assert row['is_species'] and row['taxon_id'] == 99001
        # Resolution must see the newly authoritative identity and retain its
        # existing scientific association rather than attach a redundant root.
        assert [row['keyword_id'] for row in db.conn.execute('SELECT keyword_id FROM photo_keywords WHERE photo_id=?', (photo,))] == [alias]
        for action in ((lambda: undo_last_edit(db)), (lambda: redo_last_undo(db))):
            action()
            assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (photo, alias)).fetchone()[0] == 'accept'
    finally:
        db.close()


def test_batch_identity_reuse_keeps_each_photo_association_check_live(tmp_path, monkeypatch):
    from repositories.identity_alternatives import IdentityAlternativesRepository
    from services.identity_alternatives import IdentityHistoryStale
    db = Database(str(tmp_path / 'live.db'))
    try:
        folder = workspace_folders.add_folder(db, str(tmp_path / 'photos'))
        photos = [photo_ingest.add_photo(db, folder_id=folder, filename=f'{i}.jpg', extension='.jpg', file_size=1, file_mtime=1) for i in range(2)]
        keyword = db.add_keyword('Heron', is_species=True)
        db.commit()
        original = IdentityAlternativesRepository.set_state

        def change_next_photo(repository, photo_id, state):
            original(repository, photo_id, state)
            if photo_id == photos[0]:
                db.tag_photo(photos[1], keyword, source='manual')
                db.commit()

        monkeypatch.setattr(IdentityAlternativesRepository, 'set_state', change_next_photo)
        with pytest.raises(IdentityHistoryStale), db.transaction():
            save_alternatives(db, photos, ['Heron', 'Egret'], 'Distant frames', 'Bill')
        assert db.identity_alternatives.for_photos(photos) == {}
        assert not db.conn.execute('SELECT 1 FROM photo_keywords WHERE photo_id IN (?, ?)', photos).fetchone()
    finally:
        db.close()


@pytest.mark.parametrize('rank', ['genus', 'family'])
def test_higher_rank_candidate_name_uses_species_alias_and_preserves_metadata(tmp_path, rank):
    from sync import sync_from_xmp, sync_to_xmp
    from test_sync import _setup_photo_with_xmp
    from xmp import read_keywords
    db = Database(str(tmp_path / 'rank.db'))
    try:
        photo, sidecar = _setup_photo_with_xmp(tmp_path, db, keywords={'Puma', 'Puma concolor', 'Unrelated note'})
        first = db.require_workspace_id()
        second = db.create_workspace('Shared homonym lookup')
        db.commit()
        db.set_active_workspace(first)
        workspace_folders.add_workspace_folder(db, second, db.photos.get(photo)['folder_id'])
        db.conn.execute('INSERT INTO taxa(id,inat_id,name,rank) VALUES (99001,101,?,?)', ('Puma', rank))
        db.conn.execute("INSERT INTO taxa(id,inat_id,name,common_name,rank) VALUES (99002,102,'Puma concolor','Cougar','species')")
        db.conn.execute("INSERT INTO taxa_common_names(taxon_id,name) VALUES (99002,'Puma')")
        higher = db.add_keyword('Puma', kw_type='taxonomy')
        db.commit()
        db.conn.execute('UPDATE keywords SET taxon_id=99001,source_taxon_id=101,is_species=0 WHERE id=?', (higher,))
        scientific = db.add_keyword('Puma concolor', is_species=True)
        note = db.add_keyword('Unrelated note')
        db.tag_photo(photo, higher, source='manual')
        db.tag_photo(photo, scientific, source='accept')
        db.tag_photo(photo, note, source='manual')
        db.commit()
        db.photo_review.set_flag(photo, 'rejected')
        db.commit()
        with db.transaction():
            saved = save_alternatives(db, [photo], ['Puma', 'Bobcat'], 'Ambiguous silhouette', 'Bill')
        assert not db.conn.execute('SELECT 1 FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (photo, scientific)).fetchone()
        for action in (None, 'undo', 'redo'):
            db.set_active_workspace(first)
            if action:
                result = undo_last_edit(db) if action == 'undo' else redo_last_undo(db)
                assert result['id'] == saved
            db.set_active_workspace(second)
            assert sync_to_xmp(db)['failed'] == 0
            assert {'Puma', 'Unrelated note'} <= read_keywords(sidecar)
            assert ('Puma concolor' in read_keywords(sidecar)) == (action == 'undo')
            sync_from_xmp(db, [photo])
            assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (photo, higher)).fetchone()[0] == 'manual'
            assert db.photos.get(photo)['flag'] == 'rejected'
            if action == 'undo':
                assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (photo, scientific)).fetchone()[0] == 'accept'
    finally:
        db.close()


def test_photo_collapse_without_identity_names_skips_taxonomy_scans(tmp_path):
    from repositories.identity_alternatives import remap_identity_keyword_intents

    with Database(str(tmp_path / 'ordinary-collapse.db')) as db:
        folder = workspace_folders.add_folder(db, str(tmp_path / 'photos'))
        photos = [photo_ingest.add_photo(db, folder_id=folder, filename=f'{i}.jpg',
                                         extension='.jpg', file_size=1, file_mtime=1) for i in range(2)]
        db.conn.executemany("INSERT INTO keywords(name,type,is_species) VALUES (?,'taxonomy',1)",
                            [(f'Catalog species {i}',) for i in range(2000)])
        db.queue_change(photos[0], 'keyword_add', 'Catalog species 1')
        db.commit()
        before = [tuple(row) for row in db.conn.execute('SELECT * FROM pending_changes')]
        scans = []
        db.conn.set_trace_callback(lambda sql: scans.append(sql) if 'FROM keywords k ' in sql else None)
        moved = remap_identity_keyword_intents(db.conn, photos[0], photos[1], [])
        db.conn.set_trace_callback(None)
        assert moved == {}
        assert scans == []
        assert [tuple(row) for row in db.conn.execute('SELECT * FROM pending_changes')] == before
