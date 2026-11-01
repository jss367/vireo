"""Carried authoritative aliases must reach the destination sidecar."""
from functools import partial
from pathlib import Path

import pytest
from db import Database
from scanner.companions import _pair_raw_jpeg_companions
from services import moves_merge, workspace_folders
from services.edit_history import redo_last_undo, undo_last_edit
from services.identity_alternatives import resolve_alternatives, save_alternatives
from sync import sync_from_xmp, sync_to_xmp
from test_db_moves_merge import _collision_tree
from test_identity_photo_collapses import folder_twins, photo
from xmp import read_keywords, write_sidecar


def _path(db, photo_id):
    row = db.conn.execute('SELECT f.path,p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?', (photo_id,)).fetchone()
    return Path(row['path']) / row['filename']


def _collision(db, tmp_path, mode, monkeypatch):
    if mode in ('staged', 'phantom'):
        tree = _collision_tree(db, tmp_path, same_hash=mode == 'staged')
        losing, survivor = ((tree['staged'], tree['survivor']) if mode == 'staged' else (tree['survivor'], tree['staged']))
        target = tmp_path / 'arch/day/dup.raf'
        collapse = partial(moves_merge.merge_staged_tree_into_archive, db, tree['stage_root'], tree['arch'])
    elif mode == 'intra_staged':
        import move
        archive = tmp_path / 'archive'
        stage = tmp_path / 'stage'
        archive.mkdir()
        workspace_folders.add_folder(db, str(archive), workspace_root=True)
        stage_root = workspace_folders.add_folder(db, str(stage), workspace_root=True)
        survivor, losing = photo(db, stage_root, 'A.raf'), photo(db, stage_root, 'a.raf')
        target = archive / 'A.raf'
        monkeypatch.setattr(move, '_case_insensitive_root', lambda path: True)
        collapse = partial(moves_merge.merge_staged_tree_into_archive, db, stage_root, str(archive))
    elif mode == 'folder':
        source, destination, losing, survivor, target_dir = folder_twins(db, tmp_path)
        target = Path(target_dir) / 'bird.raf'
        collapse = partial(db.folders.merge_into_existing, source, destination, target_dir)
    else:
        folder = workspace_folders.add_folder(db, str(tmp_path / 'paired'))
        survivor, losing = photo(db, folder, 'bird.raf'), photo(db, folder, 'bird.jpg')
        target = _path(db, survivor)
        collapse = partial(_pair_raw_jpeg_companions, db)
    return losing, survivor, target, collapse


def _alias(db, kind):
    if kind == 'source_only':
        left, right = db.add_keyword('Common hierarchy'), db.add_keyword('Scientific hierarchy')
        common = db.add_keyword('Common alias', parent_id=left, is_species=True, source_taxon_id=88001)
        alias = db.add_keyword('Scientific alias', parent_id=right, is_species=True, source_taxon_id=88001)
        db.commit()
        return db.keywords.name_of(common), alias
    db.conn.execute("INSERT INTO taxa(id,inat_id,name,common_name,rank) VALUES (88001,?,'Tyrannus melancholicus','Tropical Kingbird','species')", (None if kind == 'local_only' else 88001,))
    db.conn.execute("INSERT INTO taxa_common_names(taxon_id,name) VALUES (88001,'Yellow Kingbird')")
    label = 'Yellow Kingbird' if kind == 'alternate_common' else 'Tyrannus melancholicus'
    return 'Tropical Kingbird', db.add_keyword(label, is_species=True)


@pytest.mark.parametrize('mode', ['staged', 'phantom', 'intra_staged', 'folder', 'companion'])
@pytest.mark.parametrize('alias_kind', ['scientific', 'alternate_common', 'source_only', 'local_only'])
def test_drained_alias_confirmation_survives_collision_sync_reimport_and_history(tmp_path, monkeypatch, mode, alias_kind):
    with Database(str(tmp_path / 'aliases.db')) as db:
        first = db.require_workspace_id()
        losing, survivor, destination, collapse = _collision(db, tmp_path, mode, monkeypatch)
        source = _path(db, losing)
        for path in (source, destination):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'bytes')
            write_sidecar(str(path.with_suffix('.xmp')), flat_keywords={'Unrelated note', 'Higher family'}, hierarchical_keywords=set())
        second = db.create_workspace('Shared alias repair')
        hidden = db.create_workspace('Unrelated hidden scope')
        db.set_active_workspace(first)
        db.photo_visibility.grant(first, [losing, survivor])
        db.photo_visibility.grant(second, [losing, survivor])
        candidate, alias = _alias(db, alias_kind)
        db.conn.execute("INSERT INTO taxa(id,name,rank) VALUES (88002,'Higher family','family')")
        higher = db.add_keyword('Higher family', kw_type='taxonomy')
        db.commit()
        db.conn.execute('UPDATE keywords SET taxon_id=88002 WHERE id=?', (higher,))
        note = db.add_keyword('Unrelated note')
        db.tag_photo(survivor, higher, source='manual')
        db.tag_photo(survivor, note, source='manual')
        db.commit()
        db.photo_review.set_flag(survivor, 'rejected')
        db.commit()
        with db.transaction():
            save_alternatives(db, [losing], [candidate, 'Uncataloged other bird'], 'Retain the call evidence', 'Audio')
            # Normal resolution retains an already attached equivalent spelling.
            db.tag_photo(losing, alias, source='accept')
            edit = resolve_alternatives(db, [losing], candidate)
        alias_name = db.keywords.name_of(alias)
        state = db.identity_alternatives.for_photos([losing], include_resolved=True)[losing]
        for workspace in (first, second):
            db.set_active_workspace(workspace)
            changes = [row['id'] for row in db.conn.execute('SELECT id FROM pending_changes WHERE photo_id=? AND workspace_id=?', (losing, workspace))]
            assert sync_to_xmp(db, change_ids=changes)['failed'] == 0
        assert alias_name in read_keywords(str(source.with_suffix('.xmp')))
        assert not db.conn.execute('SELECT 1 FROM pending_changes WHERE photo_id=?', (losing,)).fetchone()
        # The arrival's sidecar can lack a previously synced source alias.
        write_sidecar(str(destination.with_suffix('.xmp')), flat_keywords={'Unrelated note', 'Higher family'}, hierarchical_keywords=set())
        db.set_active_workspace(first)
        collapse()
        assert db.photos.get(losing) is None
        assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == state
        additions = {row['workspace_id'] for row in db.conn.execute("SELECT workspace_id FROM pending_changes WHERE photo_id=? AND change_type='keyword_add' AND value=?", (survivor, alias_name))}
        assert additions == {first, second}
        assert hidden not in additions
        for action in (None, 'undo', 'redo'):
            db.set_active_workspace(first)
            if action:
                result = undo_last_edit(db) if action == 'undo' else redo_last_undo(db)
                assert result['id'] == edit
            db.set_active_workspace(second)
            assert sync_to_xmp(db)['failed'] == 0
            assert {alias_name, 'Unrelated note', 'Higher family'} <= read_keywords(str(destination.with_suffix('.xmp')))
            sync_from_xmp(db, [survivor])
            assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (survivor, alias)).fetchone()[0] == 'accept'
            assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (survivor, higher)).fetchone()[0] == 'manual'
            assert db.photos.get(survivor)['flag'] == 'rejected'
        assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == state
