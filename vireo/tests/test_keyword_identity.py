import json

import pytest
from db import Database
from keyword_identity import (
    grouped_keywords,
    keyword_paths,
    location_candidates,
    merge_keywords,
    path_key,
    preview_keyword_merge,
    reconcile_location,
)


@pytest.fixture
def catalog(tmp_path):
    db = Database(str(tmp_path / 'identity.db'))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder = db.add_folder(str(tmp_path / 'photos'), name='Photos')
    db.add_workspace_folder(ws, folder)
    photos = [db.add_photo(folder_id=folder, filename=f'{i}.jpg', extension='.jpg',
                           file_size=10, file_mtime=1,
                           timestamp=f'2024-0{6 if i < 2 else 7}-15T10:00:00')
              for i in range(3)]
    yield db, photos
    db.close()


def species_pair(db):
    taxon = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) VALUES ('Testus bird', 'Test bird', 'species', 123)"
    ).lastrowid
    parent = db.add_keyword('Imported birds')
    root = db.add_keyword('Test bird', is_species=True)
    leaf = db.add_keyword('Test Bird', parent_id=parent, is_species=True)
    db.conn.execute('UPDATE keywords SET taxon_id = ? WHERE id IN (?, ?)', (taxon, root, leaf))
    db.conn.commit()
    return root, leaf


def place_pair(db, name='Lake Hodges', nested=False):
    parent = db.add_keyword('Imported places') if nested else None
    source = db.add_keyword(name, parent_id=parent, kw_type='general')
    target = db.upsert_place_chain({
        'place_id': 'test-place', 'name': name, 'lat': 33.06, 'lng': -117.11,
        'address_components': [{'name': 'San Diego', 'types': ['locality']}],
    })
    return source, target, parent


def test_identity_counts_filters_and_paths(catalog):
    db, photos = catalog
    root, leaf = species_pair(db)
    db.tag_photo(photos[0], root)
    db.tag_photo(photos[0], leaf)
    db.tag_photo(photos[1], root)
    db.tag_photo(photos[2], leaf)
    group = next(g for g in grouped_keywords(db) if g['identity'] == 'inat:123')
    assert group['photo_count'] == 3
    assert {m['id'] for m in group['members']} == {root, leaf}
    assert group['paths'] == [['Test bird'], ['Imported birds', 'Test Bird']]
    stats = db.get_dashboard_stats(date_to='2024-06-30')['top_keywords']
    assert len(stats) == 1
    assert stats[0]['identity'] == group['identity']
    assert stats[0]['photo_count'] == 2
    assert db.get_dashboard_stats()['keyword_count'] == 1
    assert db.count_keywords_in_workspace() == 1
    rules = [{'field': 'keyword_identity', 'op': 'equals', 'value': group['identity']},
             {'field': 'timestamp', 'op': '<=', 'value': '2024-06-30'}]
    assert set(db.query_photo_ids(rules)) == set(photos[:2])
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()
    assert db.get_dashboard_stats()['top_keywords'][0]['photo_count'] == 3


def test_unproven_same_names_and_conflicting_source_taxa_stay_separate(catalog):
    db, photos = catalog
    root, leaf = species_pair(db)
    db.conn.execute('UPDATE keywords SET source_taxon_id = 999 WHERE id = ?', (leaf,))
    for photo, kid in zip(photos, [root, leaf, db.add_keyword('Test bird', kw_type='general')], strict=True):
        db.tag_photo(photo, kid)
    assert len(db.get_dashboard_stats()['top_keywords']) == 3
    parent = db.add_keyword('Another place')
    a = db.add_keyword('Springfield', kw_type='location')
    b = db.add_keyword('Springfield', parent_id=parent, kw_type='location')
    db.tag_photo(photos[0], a)
    db.tag_photo(photos[0], b)
    assert len([k for k in db.get_dashboard_stats()['top_keywords'] if k['name'] == 'Springfield']) == 2


def test_dashboard_uses_a_label_tagged_in_the_selected_date_range(catalog):
    db, photos = catalog
    root, leaf = species_pair(db)
    db.tag_photo(photos[0], root)
    db.tag_photo(photos[2], leaf)
    entry, = db.get_dashboard_stats(date_from='2024-07-01')['top_keywords']
    assert entry['name'] == 'Test Bird'
    assert entry['id'] == leaf
    assert entry['photo_count'] == 1


def test_dashboard_does_not_borrow_species_labels_from_other_workspaces(catalog):
    db, photos = catalog
    workspace = db._ws_id()
    root, leaf = species_pair(db)
    db.tag_photo(photos[0], leaf)
    other = db.create_workspace('Another vocabulary')
    db.set_active_workspace(other)
    folder = db.add_folder('/other-vocabulary', name='Other')
    db.add_workspace_folder(other, folder)
    photo = db.add_photo(folder_id=folder, filename='other.jpg', extension='.jpg', file_size=10, file_mtime=1)
    db.tag_photo(photo, root)
    db.set_active_workspace(workspace)
    entry, = db.get_dashboard_stats()['top_keywords']
    assert entry['id'] == leaf
    assert entry['name'] == 'Test Bird'
    assert entry['name'] == next(g for g in grouped_keywords(db) if g['identity'] == entry['identity'])['name']


@pytest.mark.parametrize('nested', [False, True])
def test_location_reconciliation_preserves_provenance_and_import_path(catalog, nested):
    db, photos = catalog
    source, target, parent = place_pair(db, nested=nested)
    db.tag_photo(photos[0], source, source='manual')
    db.tag_photo(photos[0], target, source='accept')
    db.tag_photo(photos[1], source)
    db.tag_photo(photos[2], target)
    preview = location_candidates(db)
    assert len(preview) == 1
    assert preview[0]['source_count'] == 2
    assert preview[0]['combined_count'] == 3
    reconcile_location(db, source, target)
    assert db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (source,)).fetchone() is None
    assert db.add_keyword('Lake Hodges', parent_id=parent, _resolve_alias=True) == target
    assert db.get_assigned_photo_location(photos[1])['place_id'] == 'test-place'
    assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?',
                           (photos[0], target)).fetchone()[0] == 'manual'
    assert db.conn.execute("SELECT COUNT(*) FROM pending_changes WHERE change_type = 'location'").fetchone()[0] == 2
    group = next(g for g in grouped_keywords(db) if g['id'] == target)
    assert preview[0]['source_path'] in group['paths']
    assert group['photo_count'] == 3
    assert location_candidates(db) == []
    # Remember the resolution after reopening, not just within an import run.
    path = db.conn.execute('PRAGMA database_list').fetchone()['file']
    with Database(path) as reopened:
        assert reopened.add_keyword('Lake Hodges', parent_id=parent, _resolve_alias=True) == target


def test_reconciliation_rejects_changed_or_out_of_workspace_candidates(catalog):
    db, photos = catalog
    source, target, _ = place_pair(db)
    db.tag_photo(photos[0], source)
    child = db.add_keyword('A sublocation', parent_id=source)
    with pytest.raises(ValueError):
        reconcile_location(db, source, target)
    assert db.conn.execute('SELECT COUNT(*) FROM keyword_import_aliases').fetchone()[0] == 0
    db.conn.execute('DELETE FROM keywords WHERE id = ?', (child,))
    db.conn.commit()
    other = db.create_workspace('Other')
    db.set_active_workspace(other)
    with pytest.raises(ValueError):
        reconcile_location(db, source, target)


def test_reconciliation_rolls_back_alias_and_tags_if_queueing_fails(catalog, monkeypatch):
    db, photos = catalog
    source, target, _ = place_pair(db)
    db.tag_photo(photos[0], source)
    def fail(*args, **kwargs):
        raise RuntimeError('queue failed')
    monkeypatch.setattr(db, 'queue_change', fail)
    with pytest.raises(RuntimeError, match='queue failed'):
        reconcile_location(db, source, target)
    assert db.conn.execute('SELECT COUNT(*) FROM keyword_import_aliases').fetchone()[0] == 0
    assert db.conn.execute('SELECT keyword_id FROM photo_keywords WHERE photo_id = ?', (photos[0],)).fetchone()[0] == source


def test_catalog_import_does_not_attach_flat_and_hierarchical_leaf(catalog, monkeypatch):
    from importer import execute_import
    db, photos = catalog
    row = db.conn.execute('SELECT f.path, p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?',
                          (photos[0],)).fetchone()
    monkeypatch.setattr('importer.read_catalog', lambda *args, **kwargs: {
        row['path'] + '/' + row['filename']: {
            'flat_keywords': {'Lake Hodges'},
            'hierarchical_keywords': {'Imported places|Lake Hodges'},
        },
    })
    execute_import(['dummy.lrcat'], db, write_xmp=False)
    leaves = [k for k in db.get_photo_keywords(photos[0]) if k['name'] == 'Lake Hodges']
    assert len(leaves) == 1
    assert leaves[0]['parent_id'] is not None
    target = db.upsert_place_chain({'place_id': 'test-place', 'name': 'Lake Hodges', 'lat': 33, 'lng': -117,
                                    'address_components': []})
    reconcile_location(db, leaves[0]['id'], target)
    execute_import(['dummy.lrcat'], db, write_xmp=False)
    assert [k['id'] for k in db.get_photo_keywords(photos[0])] == [target]


def test_identity_and_reconciliation_api(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    photos = [r['id'] for r in db.conn.execute('SELECT id FROM photos ORDER BY id LIMIT 2')]
    source, target, _ = place_pair(db)
    db.tag_photo(photos[0], source)
    assert client.get('/api/keywords/identities').status_code == 200
    preview = client.get('/api/keywords/location-matches').get_json()
    assert any(c['source_id'] == source for c in preview)
    assert client.post('/api/keywords/reconcile-location', json={'source_id': True, 'target_id': target}).status_code == 400
    response = client.post('/api/keywords/reconcile-location', json={'source_id': source, 'target_id': target})
    assert response.status_code == 200
    assert client.post('/api/keywords/reconcile-location', json={'source_id': source, 'target_id': target}).status_code == 400


def test_reconciliation_accepts_an_existing_descendant_location(catalog):
    db, photos = catalog
    source, target, _ = place_pair(db)
    child = db.upsert_place_chain({'name': 'Picnic area', 'place_id': 'picnic-place',
                                   'lat': 33.1, 'lng': -117.1, 'address_components': []})
    db.conn.execute('UPDATE keywords SET parent_id = ? WHERE id = ?', (target, child))
    db.conn.commit()
    db.tag_photo(photos[0], source)
    db.set_photo_location(photos[0], child)
    assert location_candidates(db)[0]['conflicting_photo_count'] == 0
    reconcile_location(db, source, target)
    assert {k['id'] for k in db.get_photo_keywords(photos[0])} == {target, child}
    assert db.get_assigned_photo_location(photos[0])['place_id'] == 'picnic-place'


def test_reconciliation_does_not_override_a_different_assigned_place(catalog):
    db, photos = catalog
    source, target, _ = place_pair(db)
    other = db.upsert_place_chain({'name': 'Another lake', 'place_id': 'another-place',
                                  'lat': 40, 'lng': -100, 'address_components': []})
    db.tag_photo(photos[0], source)
    db.set_photo_location(photos[0], other)
    assert location_candidates(db)[0]['conflicting_photo_count'] == 1
    with pytest.raises(ValueError, match='different linked place'):
        reconcile_location(db, source, target)
    assert db.get_assigned_photo_location(photos[0])['place_id'] == 'another-place'


@pytest.mark.parametrize('nested', [False, True])
@pytest.mark.parametrize('flat_present', [False, True])
def test_rescanning_and_syncing_renamed_place_keep_confirmed_import_alias(catalog, nested, flat_present):
    from pathlib import Path

    from scanner import _import_keywords_for_photo
    from sync import sync_from_xmp
    from xmp import write_sidecar

    db, photos = catalog
    source, target, parent = place_pair(db, nested=nested)
    db.tag_photo(photos[0], source)
    reconcile_location(db, source, target)
    db.update_keyword(target, name='Lake Hodges Preserve')
    folder = Path(db.conn.execute('SELECT path FROM folders LIMIT 1').fetchone()[0])
    folder.mkdir()
    sidecar = folder / '0.xmp'
    hierarchy = 'Imported places|Lake Hodges' if nested else 'Lake Hodges'
    write_sidecar(str(sidecar), {'Lake Hodges'} if flat_present else set(), {hierarchy})
    _import_keywords_for_photo(db, photos[0], str(sidecar))
    assert {k['id'] for k in db.get_photo_keywords(photos[0])} == {target}
    sync_from_xmp(db, [photos[0]])
    assert {k['id'] for k in db.get_photo_keywords(photos[0])} == {target}
    db.untag_photo(photos[0], target)
    sync_from_xmp(db, [photos[0]])
    assert {k['id'] for k in db.get_photo_keywords(photos[0])} == {target}


@pytest.mark.parametrize('nested', [False, True])
def test_manual_keyword_addition_does_not_resolve_import_aliases(catalog, nested):
    db, photos = catalog
    source, target, parent = place_pair(db, nested=nested)
    db.tag_photo(photos[0], source)
    reconcile_location(db, source, target)
    manual = db.add_keyword('Lake Hodges', parent_id=parent)
    db.tag_photo(photos[1], manual)
    assert manual != target
    keyword = db.conn.execute('SELECT type, place_id FROM keywords WHERE id = ?', (manual,)).fetchone()
    assert keyword['type'] == 'general'
    assert keyword['place_id'] is None
    assert db.get_assigned_photo_location(photos[1]) is None
    assert db.add_keyword('Lake Hodges', parent_id=parent, _resolve_alias=True) == target


def test_api_add_keyword_does_not_attach_reconciled_place_for_homonym(app_and_db):
    """Codex flagged the /api/photos/<id>/keywords entry point: an untyped
    manual add of a homonym must not silently attach the reconciled place
    and reassign the photo's map/GPS location. Exercise the HTTP boundary
    to catch a future regression where the handler grows an opt-in flag.
    """
    app, db = app_and_db
    client = app.test_client()
    source, target, _ = place_pair(db)
    seed_photo, target_photo = [
        r['id'] for r in db.conn.execute(
            'SELECT id FROM photos ORDER BY id LIMIT 2'
        )
    ]
    db.tag_photo(seed_photo, source)
    reconcile_location(db, source, target)
    before = {k['id'] for k in db.get_photo_keywords(target_photo)}
    response = client.post(
        f'/api/photos/{target_photo}/keywords',
        json={'name': 'Lake Hodges'},
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload['ok'] is True
    kid = payload['keyword_id']
    assert kid != target
    row = db.conn.execute(
        'SELECT type, place_id FROM keywords WHERE id = ?', (kid,),
    ).fetchone()
    assert row['place_id'] is None
    assert row['type'] != 'location'
    tagged = {k['id'] for k in db.get_photo_keywords(target_photo)}
    assert target not in tagged
    assert tagged - before == {kid}


def test_leaf_import_alias_does_not_reparent_new_subtrees(catalog):
    db, photos = catalog
    source, target, _ = place_pair(db)
    db.tag_photo(photos[0], source)
    reconcile_location(db, source, target)
    imported_parent = db.add_keyword('Lake Hodges', _resolve_alias=False)
    child = db.add_keyword('Picnic area', parent_id=imported_parent)
    assert imported_parent != target
    assert db.conn.execute('SELECT parent_id FROM keywords WHERE id = ?', (child,)).fetchone()[0] == imported_parent


def test_linking_a_place_to_an_existing_place_preserves_import_aliases(catalog):
    db, photos = catalog
    source, target, parent = place_pair(db, nested=True)
    db.tag_photo(photos[0], source)
    reconcile_location(db, source, target)
    details = {'name': 'Lake Hodges Preserve', 'place_id': 'corrected-place',
               'lat': 33, 'lng': -117, 'address_components': []}
    survivor = db.upsert_place_chain(details)
    result = db.link_keyword_to_place(target, details)
    assert result == {'keyword_id': survivor, 'merged': True}
    assert db.conn.execute('SELECT keyword_id FROM keyword_import_aliases').fetchone()[0] == survivor
    assert db.add_keyword('Lake Hodges', parent_id=parent, _resolve_alias=True) == survivor
    assert {k['id'] for k in db.get_photo_keywords(photos[0])} == {survivor}


@pytest.mark.parametrize('reader', ['scanner', 'sync', 'catalog'])
@pytest.mark.parametrize('existing_place', [False, True])
def test_import_rejects_conflicting_confirmed_locations_before_changing_tags(catalog, monkeypatch, reader, existing_place):
    from pathlib import Path

    from importer import execute_import
    from scanner import _import_keywords_for_photo
    from sync import sync_from_xmp
    from xmp import write_sidecar

    db, photos = catalog
    targets = []
    for index, state in enumerate(['Illinois', 'Missouri']):
        parent = db.add_keyword(state)
        source = db.add_keyword('Springfield', parent_id=parent)
        target = db.upsert_place_chain({'name': 'Springfield', 'place_id': f'springfield-{index}',
                                        'lat': 38 + index, 'lng': -90, 'address_components': []})
        db.tag_photo(photos[index], source)
        reconcile_location(db, source, target)
        targets.append(target)
    original = db.add_keyword('Original keyword')
    db.tag_photo(photos[2], original)
    if existing_place:
        db.tag_photo(photos[2], targets[0])
    before = {k['id'] for k in db.get_photo_keywords(photos[2])}
    hierarchy = {'Missouri|Springfield'} if existing_place else {'Illinois|Springfield', 'Missouri|Springfield'}
    # Including Springfield keeps an existing same-name location during
    # Sync from XMP too; adding the other alias must not leave both assigned.
    flat = {'Springfield', 'Do not partially import'}
    folder = Path(db.conn.execute('SELECT path FROM folders LIMIT 1').fetchone()[0])
    folder.mkdir()
    sidecar = folder / '2.xmp'
    write_sidecar(str(sidecar), flat, hierarchy)
    if reader == 'catalog':
        monkeypatch.setattr('importer.read_catalog', lambda *args, **kwargs: {
            str(folder / '2.jpg'): {'flat_keywords': flat, 'hierarchical_keywords': hierarchy},
        })
        result = execute_import(['dummy.lrcat'], db, write_xmp=False)
        assert result['failed'] == 1
        assert result['imported'] == 0
    else:
        with pytest.raises(ValueError, match='different linked places'):
            if reader == 'scanner':
                _import_keywords_for_photo(db, photos[2], str(sidecar))
            else:
                sync_from_xmp(db, [photos[2]])
    assert {k['id'] for k in db.get_photo_keywords(photos[2])} == before


def _sidecar_with_marker(path, *, marker, owned, flat, hierarchical):
    """Write a sidecar with an explicit Vireo location-keyword ownership marker."""
    from xmp import (
        LOCATION_KEYWORDS_MARKER,
        LOCATION_KEYWORDS_OWNED,
        SidecarEditor,
    )

    editor = SidecarEditor(str(path))
    editor.add_keywords(flat_keywords=set(flat), hierarchical_keywords=set(hierarchical))
    desc = editor._description()
    desc.set(LOCATION_KEYWORDS_MARKER, marker)
    if owned is not None:
        desc.set(LOCATION_KEYWORDS_OWNED, owned)
    editor._dirty = True
    editor.commit()


def test_drop_stale_vireo_location_keywords_keeps_a_users_flat_leaf(catalog, tmp_path):
    """A queued change does not drop a flat leaf Vireo never claimed to own."""
    from keyword_identity import drop_stale_vireo_location_keywords

    db, photos = catalog
    db.queue_change(photos[0], 'location', 'effective')
    sidecar = tmp_path / 'photo.xmp'
    _sidecar_with_marker(
        sidecar,
        marker='United States|California|Kumeyaay Lake',
        owned='hier',
        flat={'Kumeyaay Lake', 'House finch'},
        hierarchical={'United States|California|Kumeyaay Lake', 'Birds|House finch'},
    )
    flat, hier = drop_stale_vireo_location_keywords(
        db, photos[0], str(sidecar),
        {'Kumeyaay Lake', 'House finch'},
        ['United States|California|Kumeyaay Lake', 'Birds|House finch'],
    )
    assert flat == {'Kumeyaay Lake', 'House finch'}
    assert 'United States|California|Kumeyaay Lake' not in hier
    assert 'Birds|House finch' in hier


def test_drop_stale_vireo_location_keywords_keeps_a_users_hierarchy(catalog, tmp_path):
    """A queued change does not drop a hierarchy Vireo never claimed to own."""
    from keyword_identity import drop_stale_vireo_location_keywords

    db, photos = catalog
    db.queue_change(photos[0], 'location', 'effective')
    sidecar = tmp_path / 'photo.xmp'
    _sidecar_with_marker(
        sidecar,
        marker='United States|California|Kumeyaay Lake',
        owned='flat',
        flat={'Kumeyaay Lake'},
        hierarchical={'United States|California|Kumeyaay Lake'},
    )
    flat, hier = drop_stale_vireo_location_keywords(
        db, photos[0], str(sidecar),
        {'Kumeyaay Lake'},
        ['United States|California|Kumeyaay Lake'],
    )
    assert flat == set()
    assert hier == ['United States|California|Kumeyaay Lake']


def test_drop_stale_vireo_location_keywords_legacy_marker_drops_both(catalog, tmp_path):
    """A sidecar written before the ownership record keeps the old behaviour."""
    from keyword_identity import drop_stale_vireo_location_keywords

    db, photos = catalog
    db.queue_change(photos[0], 'location', 'effective')
    sidecar = tmp_path / 'photo.xmp'
    _sidecar_with_marker(
        sidecar,
        marker='United States|California|Kumeyaay Lake',
        owned=None,  # legacy: companion attribute absent
        flat={'Kumeyaay Lake'},
        hierarchical={'United States|California|Kumeyaay Lake'},
    )
    flat, hier = drop_stale_vireo_location_keywords(
        db, photos[0], str(sidecar),
        {'Kumeyaay Lake'},
        ['United States|California|Kumeyaay Lake'],
    )
    assert flat == set()
    assert hier == []

def test_manual_merge_preview_and_cross_workspace_sidecar_updates(catalog):
    db, photos = catalog
    source = db.add_keyword('Wing St. Canyon', kw_type='location')
    target = db.add_keyword('Wing Street Canyon', kw_type='location')
    db.update_keyword(target, latitude=32.7447, longitude=-117.2186)
    db.tag_photo(photos[0], source, source='manual')
    db.tag_photo(photos[0], target, source='accept')
    db.tag_photo(photos[1], source)
    db.tag_photo(photos[2], target)
    ws = db._ws_id()
    other = db.create_workspace('Shared photos')
    folder = db.conn.execute('SELECT folder_id FROM photos WHERE id = ?', (photos[0],)).fetchone()[0]
    db.add_workspace_folder(other, folder)
    selection = [source, target]
    preview = preview_keyword_merge(db, selection, target)
    assert preview['combined_count'] == 3
    assert (preview['latitude'], preview['longitude']) == (32.7447, -117.2186)
    assert db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (source,)).fetchone()
    assert not db.conn.execute('SELECT 1 FROM pending_changes').fetchone()
    merge_keywords(db, selection, target, preview['preview_token'])
    assert not db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (source,)).fetchone()
    assert {r['photo_id'] for r in db.conn.execute('SELECT photo_id FROM photo_keywords WHERE keyword_id = ?', (target,))} == set(photos)
    assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?', (photos[0], target)).fetchone()[0] == 'manual'
    for workspace in (ws, other):
        pending = {(r['photo_id'], r['change_type'], r['value']) for r in db.conn.execute(
            'SELECT * FROM pending_changes WHERE workspace_id = ?', (workspace,))}
        for photo in photos[:2]:
            assert (photo, 'keyword_remove_flat', 'Wing St. Canyon') in pending
            assert (photo, 'keyword_add', 'Wing Street Canyon') in pending
        for photo in photos:
            assert (photo, 'location', 'effective') in pending


def test_manual_merge_general_into_linked_place_remembers_import(catalog):
    db, photos = catalog
    source, target, parent = place_pair(db, name='Whatcom Falls Park', nested=True)
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    preview = preview_keyword_merge(db, [source, target], target)
    merge_keywords(db, [source, target], target, preview['preview_token'])
    assert db.add_keyword('Whatcom Falls Park', parent_id=parent, _resolve_alias=True) == target
    assert db.get_assigned_photo_location(photos[0])['place_id'] == 'test-place'


def test_manual_merge_multiple_keywords_and_coordinate_pair(catalog):
    db, photos = catalog
    ids = [db.add_keyword(name, kw_type='location') for name in ('Short', 'Long', 'Alternative')]
    for photo, kid in zip(photos, ids, strict=True):
        db.tag_photo(photo, kid)
    db.update_keyword(ids[0], latitude=99)
    db.update_keyword(ids[1], latitude=32, longitude=-117)
    preview = preview_keyword_merge(db, ids, ids[0])
    assert (preview['latitude'], preview['longitude']) == (32, -117)
    merge_keywords(db, ids, ids[0], preview['preview_token'])
    row = db.conn.execute('SELECT * FROM keywords WHERE id = ?', (ids[0],)).fetchone()
    assert (row['latitude'], row['longitude']) == (32, -117)
    assert all(db.get_photo_keywords(p)[0]['id'] == ids[0] for p in photos)


def test_manual_merge_stale_preview_and_rollback(catalog, monkeypatch):
    db, photos = catalog
    source, target, _ = place_pair(db)
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    ids = [source, target]
    preview = preview_keyword_merge(db, ids, target)
    db.tag_photo(photos[2], source)
    with pytest.raises(ValueError, match='changed'):
        merge_keywords(db, ids, target, preview['preview_token'])
    preview = preview_keyword_merge(db, ids, target)
    def fail(*args, **kwargs):
        raise RuntimeError('queue failed')
    monkeypatch.setattr(db, 'queue_change', fail)
    with pytest.raises(RuntimeError, match='queue failed'):
        merge_keywords(db, ids, target, preview['preview_token'])
    assert db.get_photo_keywords(photos[0])[0]['id'] == source
    assert not db.conn.execute('SELECT 1 FROM keyword_import_aliases').fetchone()
    assert not db.conn.execute('SELECT 1 FROM pending_changes').fetchone()


@pytest.mark.parametrize('case', ['species', 'photo_conflict', 'alias', 'workspace', 'overlap'])
def test_manual_merge_rejects_unsafe_selection(catalog, case):
    db, photos = catalog
    source, target, _ = place_pair(db)
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    if case == 'species':
        db.conn.execute('UPDATE keywords SET is_species = 1 WHERE id = ?', (source,))
    elif case in ('photo_conflict', 'alias'):
        other = db.upsert_place_chain({'place_id': 'other-place', 'name': 'Other lake', 'lat': 34, 'lng': -116, 'address_components': []})
        if case == 'photo_conflict':
            db.tag_photo(photos[0], other)
        else:
            from keyword_identity import path_key
            db.conn.execute('INSERT INTO keyword_import_aliases VALUES (?, ?, ?)', (path_key(['Lake Hodges']), '["Lake Hodges"]', other))
    elif case == 'workspace':
        db.set_active_workspace(db.create_workspace('Empty'))
    elif case == 'overlap':
        # The source's subtree contains the row being kept, so absorbing it
        # would write the survivor as its own parent.
        db.conn.execute('UPDATE keywords SET parent_id = ? WHERE id = ?', (source, target))
    db.conn.commit()
    with pytest.raises(ValueError):
        preview_keyword_merge(db, [source, target], target)
    assert db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (source,)).fetchone()


def _nested_place_branches(db):
    """A junk-rooted duplicate branch beside a clean one, as geocoding leaves it."""
    junk = db.add_keyword('92004', kw_type='location')
    stray = db.add_keyword('Borrego Springs', parent_id=junk, kw_type='location')
    road = db.add_keyword('Unnamed Road', parent_id=stray, kw_type='location')
    trailhead = db.add_keyword('Trailhead', parent_id=road, kw_type='location')
    county = db.add_keyword('San Diego County', kw_type='location')
    kept = db.add_keyword('Borrego Springs', parent_id=county, kw_type='location')
    return stray, kept, road, trailhead


def test_manual_merge_reparents_a_whole_subtree(catalog):
    """A duplicate branch merges children and all; the preview's predicted
    paths are the paths the write path actually produces, and every photo
    tagged on a moved descendant gets its sidecar hierarchy rewritten even
    though its own flat keyword never changed."""
    db, photos = catalog
    stray, kept, road, trailhead = _nested_place_branches(db)
    db.tag_photo(photos[0], trailhead)
    db.tag_photo(photos[1], kept)
    preview = preview_keyword_merge(db, [stray, kept], kept)

    assert [(c['id'], c['outcome'], c['to_path']) for c in preview['children']] == [
        (road, 'move', ['San Diego County', 'Borrego Springs', 'Unnamed Road']),
    ]
    assert preview['children'][0]['photo_count'] == 1
    predicted = {int(k): v[1] for k, v in preview['path_changes'].items()}
    merge_keywords(db, [stray, kept], kept, preview['preview_token'])

    rows = [dict(r) for r in db.conn.execute('SELECT * FROM keywords')]
    actual = keyword_paths(rows)
    assert all(actual[kid] == path for kid, path in predicted.items())
    assert actual[trailhead] == [
        'San Diego County', 'Borrego Springs', 'Unnamed Road', 'Trailhead']
    assert not db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (stray,)).fetchone()

    merges = [json.loads(r['value']) for r in db.conn.execute(
        "SELECT value FROM pending_changes WHERE photo_id = ? AND change_type = 'keyword_merge'",
        (photos[0],))]
    moved = next(m for m in merges if m['target_id'] == trailhead)
    assert moved['source_path'] == [
        '92004', 'Borrego Springs', 'Unnamed Road', 'Trailhead']
    assert moved['target_path'] == actual[trailhead]
    assert db.conn.execute(
        "SELECT 1 FROM pending_changes WHERE photo_id = ? AND change_type = 'location'",
        (photos[0],)).fetchone()


def test_manual_merge_child_collisions_match_the_write_path(catalog):
    """The preview replays the merge's own collision rules, so what it says
    about each colliding child is what the merge does: same type collapses,
    a distinct Google place under the same name survives under a suffix."""
    db, photos = catalog
    old = db.add_keyword('Park', kw_type='location')
    new = db.add_keyword('Parc', kw_type='location')
    twin_old = db.add_keyword('Trail', parent_id=old, kw_type='location')
    twin_new = db.add_keyword('Trail', parent_id=new, kw_type='location')
    place_old = db.add_keyword('Overlook', parent_id=old, kw_type='location')
    place_new = db.add_keyword('Overlook', parent_id=new, kw_type='location')
    db.conn.execute("UPDATE keywords SET place_id = 'place-aaaaaaaa' WHERE id = ?", (place_old,))
    db.conn.execute("UPDATE keywords SET place_id = 'place-bbbbbbbb' WHERE id = ?", (place_new,))
    db.conn.commit()
    db.tag_photo(photos[0], twin_old)
    db.tag_photo(photos[1], twin_new)
    db.tag_photo(photos[2], place_old)

    preview = preview_keyword_merge(db, [old, new], new)
    outcomes = {c['id']: (c['outcome'], c['new_name']) for c in preview['children']}
    assert outcomes[twin_old] == ('merge', 'Trail')
    assert outcomes[place_old] == ('rename', 'Overlook (aaaaaaaa)')

    merge_keywords(db, [old, new], new, preview['preview_token'])
    assert not db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (twin_old,)).fetchone()
    assert {r['photo_id'] for r in db.conn.execute(
        'SELECT photo_id FROM photo_keywords WHERE keyword_id = ?', (twin_new,))} == {
        photos[0], photos[1]}
    survivor = db.conn.execute(
        'SELECT name, parent_id FROM keywords WHERE id = ?', (place_old,)).fetchone()
    assert (survivor['name'], survivor['parent_id']) == ('Overlook (aaaaaaaa)', new)
    assert db.conn.execute(
        'SELECT place_id FROM keywords WHERE id = ?', (place_new,)).fetchone()[0] == 'place-bbbbbbbb'



def test_manual_merge_at_the_root_collapses_a_whole_duplicate_chain(catalog):
    """Geocoding can leave a second copy of a whole place chain under a junk
    root. Merging the two roots has to cascade all the way down in one pass,
    and each collapsing level has to name the keyword it lands in rather than
    reporting a destination it no longer has."""
    db, photos = catalog
    junk = db.add_keyword('92004', kw_type='location')
    stray = junk
    for name in ('United States', 'California', 'Borrego Springs'):
        stray = db.add_keyword(name, parent_id=stray, kw_type='location')
    trailhead = db.add_keyword('Trailhead', parent_id=stray, kw_type='location')
    kept = None
    for name in ('United States', 'California', 'Borrego Springs'):
        kept = db.add_keyword(name, parent_id=kept, kw_type='location')
    trail = db.add_keyword('Nature Trail', parent_id=kept, kw_type='location')
    db.tag_photo(photos[0], trailhead)
    db.tag_photo(photos[1], trail)
    stray_root = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'United States' AND parent_id = ?",
        (junk,)).fetchone()[0]
    kept_root = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'United States' AND parent_id IS NULL"
    ).fetchone()[0]

    preview = preview_keyword_merge(db, [stray_root, kept_root], kept_root)
    assert preview['removed_count'] == 3
    assert all(child['to_path'] for child in preview['children'])
    assert [(c['name'], c['outcome']) for c in preview['children']] == [
        ('California', 'merge'), ('Borrego Springs', 'merge'), ('Trailhead', 'move')]
    merge_keywords(db, [stray_root, kept_root], kept_root, preview['preview_token'])

    rows = [dict(r) for r in db.conn.execute('SELECT * FROM keywords')]
    actual = keyword_paths(rows)
    assert actual[trailhead] == ['United States', 'California', 'Borrego Springs', 'Trailhead']
    assert actual[trail] == ['United States', 'California', 'Borrego Springs', 'Nature Trail']
    # Only the emptied junk root is left behind; nothing else duplicated.
    assert sorted(' > '.join(actual[r['id']]) for r in rows
                  if r['type'] == 'location') == [
        '92004',
        'United States',
        'United States > California',
        'United States > California > Borrego Springs',
        'United States > California > Borrego Springs > Nature Trail',
        'United States > California > Borrego Springs > Trailhead',
    ]


def test_merging_parents_keeps_same_named_children_of_different_species(catalog):
    """Two branches can each hold a "Hummingbird" that means a different bird.
    Same name and same type is not the same keyword: collapsing them keeps the
    destination's taxon and drops the source's, so the migrating row's photos
    would come out tagged as the other species. Keep both instead, the way the
    distinct-Google-place collision already does."""
    db, photos = catalog
    anna = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte anna', 'Anna\u2019s Hummingbird', 'species', 5112)").lastrowid
    costa = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte costae', 'Costa\u2019s Hummingbird', 'species', 5113)").lastrowid
    old = db.add_keyword('Trip A')
    new = db.add_keyword('Trip B')
    old_bird = db.add_keyword('Hummingbird', parent_id=old, is_species=True)
    new_bird = db.add_keyword('Hummingbird', parent_id=new, is_species=True)
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
                    (anna, old_bird))
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
                    (costa, new_bird))
    db.conn.commit()
    db.tag_photo(photos[0], old_bird)
    db.tag_photo(photos[1], new_bird)

    preview = preview_keyword_merge(db, [old, new], new)
    assert [(c['name'], c['outcome']) for c in preview['children']] == [
        ('Hummingbird', 'rename')]
    merge_keywords(db, [old, new], new, preview['preview_token'])

    survivor = db.conn.execute(
        'SELECT name, parent_id, taxon_id FROM keywords WHERE id = ?', (old_bird,)).fetchone()
    assert (survivor['name'], survivor['parent_id'], survivor['taxon_id']) == (
        f'Hummingbird (id-{old_bird})', new, anna)
    assert db.conn.execute(
        'SELECT keyword_id FROM photo_keywords WHERE photo_id = ?',
        (photos[0],)).fetchone()['keyword_id'] == old_bird


def test_merging_parents_still_collapses_children_of_the_same_species(catalog):
    """The split above must not block the ordinary case: a local taxon_id and
    the matching iNat source_taxon_id name one species, so those children are
    duplicates and still collapse into one."""
    db, photos = catalog
    anna = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte anna', 'Anna\u2019s Hummingbird', 'species', 5112)").lastrowid
    old = db.add_keyword('Trip A')
    new = db.add_keyword('Trip B')
    old_bird = db.add_keyword('Hummingbird', parent_id=old, is_species=True)
    new_bird = db.add_keyword('Hummingbird', parent_id=new, is_species=True)
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', source_taxon_id = 5112 WHERE id = ?",
                    (old_bird,))
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
                    (anna, new_bird))
    db.conn.commit()
    db.tag_photo(photos[0], old_bird)
    db.tag_photo(photos[1], new_bird)

    preview = preview_keyword_merge(db, [old, new], new)
    assert [(c['name'], c['outcome']) for c in preview['children']] == [
        ('Hummingbird', 'merge')]
    merge_keywords(db, [old, new], new, preview['preview_token'])
    assert not db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (old_bird,)).fetchone()
    assert {r['photo_id'] for r in db.conn.execute(
        'SELECT photo_id FROM photo_keywords WHERE keyword_id = ?', (new_bird,))} == {
        photos[0], photos[1]}


def test_child_collapse_folds_source_taxon_id_onto_unlinked_survivor(catalog):
    """`keywords_claim_different_taxa` treats a bare `source_taxon_id` as
    identity, so it lets the collapse proceed when the destination has no
    claim of its own. The recursive merge then has to fold that iNat id
    onto the survivor -- otherwise the migrating row's only external
    taxon identity is deleted and its photos land on an unlinked species
    row."""
    db, photos = catalog
    old = db.add_keyword('Trip A')
    new = db.add_keyword('Trip B')
    old_bird = db.add_keyword('Hummingbird', parent_id=old, is_species=True)
    new_bird = db.add_keyword('Hummingbird', parent_id=new, is_species=True)
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', source_taxon_id = 5112 WHERE id = ?",
                    (old_bird,))
    db.conn.execute("UPDATE keywords SET type = 'taxonomy' WHERE id = ?",
                    (new_bird,))
    db.conn.commit()
    db.tag_photo(photos[0], old_bird)
    db.tag_photo(photos[1], new_bird)

    preview = preview_keyword_merge(db, [old, new], new)
    assert [(c['name'], c['outcome']) for c in preview['children']] == [
        ('Hummingbird', 'merge')]
    merge_keywords(db, [old, new], new, preview['preview_token'])

    assert not db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (old_bird,)).fetchone()
    survivor = db.conn.execute(
        'SELECT source_taxon_id, taxon_id FROM keywords WHERE id = ?',
        (new_bird,)).fetchone()
    assert (survivor['source_taxon_id'], survivor['taxon_id']) == (5112, None)
    assert {r['photo_id'] for r in db.conn.execute(
        'SELECT photo_id FROM photo_keywords WHERE keyword_id = ?', (new_bird,))} == {
        photos[0], photos[1]}


def test_merge_conflict_guard_covers_photos_tagged_only_on_a_moved_descendant(catalog):
    """The selected rows are not the whole blast radius -- a photo can be
    tagged only on a descendant the merge is about to move. If that photo also
    carries an unrelated linked place, the merge would queue it a location
    resync that exports the wrong coordinates, so it has to trip the guard."""
    db, photos = catalog
    stray = db.add_keyword('Stray', kw_type='location')
    leaf = db.add_keyword('Leaf', parent_id=stray, kw_type='location')
    kept = db.add_keyword('Kept', kw_type='location')
    db.conn.execute("UPDATE keywords SET place_id = 'keep-place' WHERE id = ?", (kept,))
    elsewhere = db.upsert_place_chain({
        'place_id': 'other-place', 'name': 'Elsewhere', 'lat': 50, 'lng': 10,
        'address_components': []})
    db.conn.commit()
    db.tag_photo(photos[0], leaf)
    db.tag_photo(photos[0], elsewhere)
    db.tag_photo(photos[1], kept)

    with pytest.raises(ValueError, match='different linked place'):
        preview_keyword_merge(db, [stray, kept], kept)
    assert db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (stray,)).fetchone()


def test_merge_rewrites_sidecars_for_children_collapsed_by_recursion(catalog):
    """A child that collapses into a same-named sibling stops existing, so a
    rewrite keyed to surviving rows skips its photos entirely: their sidecars
    keep the retired hierarchy and a later rescan recreates the branch that
    was just merged away. Point those photos at the sibling instead."""
    db, photos = catalog
    old = db.add_keyword('Trip A', kw_type='location')
    new = db.add_keyword('Trip B', kw_type='location')
    old_lake = db.add_keyword('Lake', parent_id=old, kw_type='location')
    new_lake = db.add_keyword('Lake', parent_id=new, kw_type='location')
    db.tag_photo(photos[0], old_lake)
    db.tag_photo(photos[1], new_lake)

    preview = preview_keyword_merge(db, [old, new], new)
    assert [(c['name'], c['outcome']) for c in preview['children']] == [('Lake', 'merge')]
    merge_keywords(db, [old, new], new, preview['preview_token'])

    merges = [json.loads(r['value']) for r in db.conn.execute(
        "SELECT value FROM pending_changes WHERE photo_id = ? AND change_type = 'keyword_merge'",
        (photos[0],))]
    assert {'source_path': ['Trip A', 'Lake'], 'target_id': new_lake,
            'target_path': ['Trip B', 'Lake']} in merges
    assert db.conn.execute(
        "SELECT 1 FROM pending_changes WHERE photo_id = ? AND change_type = 'location'",
        (photos[0],)).fetchone()
    # The retired path must resolve to the surviving sibling on re-import, or
    # the next scan rebuilds "Trip A > Lake" from the untouched sidecar.
    alias = db.conn.execute(
        'SELECT keyword_id FROM keyword_import_aliases WHERE path_key = ?',
        (path_key(['Trip A', 'Lake']),)).fetchone()
    assert alias['keyword_id'] == new_lake


def test_merge_rewrites_the_survivors_own_hierarchy_when_the_chooser_renames_it(catalog):
    """A photo tagged only with the retained row still has that row's OLD path
    in its sidecar after a chooser rename. The flat remove/add pair does not
    touch lr:hierarchicalSubject, so without a rewrite the retired hierarchy
    survives and a later scan can rebuild it."""
    db, photos = catalog
    county = db.add_keyword('San Diego County', kw_type='location')
    junk = db.add_keyword('92004', kw_type='location')
    stray = db.add_keyword('Borrego', parent_id=junk, kw_type='location')
    kept = db.add_keyword('Borrego', parent_id=county, kw_type='location')
    db.tag_photo(photos[0], kept)
    db.tag_photo(photos[1], stray)

    overrides = {'name': 'Borrego Springs'}
    preview = preview_keyword_merge(db, [stray, kept], kept, overrides)
    merge_keywords(db, [stray, kept], kept, preview['preview_token'], overrides)

    merges = [json.loads(r['value']) for r in db.conn.execute(
        "SELECT value FROM pending_changes WHERE photo_id = ? AND change_type = 'keyword_merge'",
        (photos[0],))]
    assert {'source_path': ['San Diego County', 'Borrego'], 'target_id': kept,
            'target_path': ['San Diego County', 'Borrego Springs']} in merges
    alias = db.conn.execute(
        'SELECT keyword_id FROM keyword_import_aliases WHERE path_key = ?',
        (path_key(['San Diego County', 'Borrego']),)).fetchone()
    assert alias['keyword_id'] == kept


def test_merging_a_location_away_resyncs_even_when_the_result_is_general(catalog):
    """The type chooser allows a location source to merge into a general
    target. The photos keep the survivor's general tag, but their sidecars
    still hold the vireo location marker, hierarchy and coordinates that the
    deleted location row exported -- only a `location` change clears those."""
    db, photos = catalog
    location = db.add_keyword('Wing Canyon', kw_type='location')
    general = db.add_keyword('Wing Cyn', kw_type='general')
    db.tag_photo(photos[0], location)
    db.tag_photo(photos[1], general)

    preview = preview_keyword_merge(db, [location, general], general)
    assert preview['resolved']['type'] == 'general'
    merge_keywords(db, [location, general], general, preview['preview_token'])

    resynced = {r['photo_id'] for r in db.conn.execute(
        "SELECT photo_id FROM pending_changes WHERE change_type = 'location'")}
    assert photos[0] in resynced


def test_merge_allows_a_linked_descendant_to_collapse_into_an_unlinked_twin(catalog):
    """A source descendant carrying a place, colliding with a same-named
    destination descendant that has none, collapses safely -- the merge hands
    its place to the survivor. The conflict guard reads pre-merge ids, so it
    has to count rows the plan absorbs as compatible or it rejects a perfectly
    valid subtree merge."""
    db, photos = catalog
    kept = db.add_keyword('Park', kw_type='location')
    db.conn.execute("UPDATE keywords SET place_id = 'kept-place' WHERE id = ?", (kept,))
    kept_lake = db.add_keyword('Lake', parent_id=kept, kw_type='location')
    stray = db.add_keyword('Parc', kw_type='location')
    stray_lake = db.add_keyword('Lake', parent_id=stray, kw_type='location')
    db.conn.execute("UPDATE keywords SET place_id = 'lake-place' WHERE id = ?", (stray_lake,))
    db.conn.commit()
    db.tag_photo(photos[0], stray_lake)
    db.tag_photo(photos[1], kept_lake)

    preview = preview_keyword_merge(db, [stray, kept], kept)
    assert [(c['name'], c['outcome']) for c in preview['children']] == [('Lake', 'merge')]
    merge_keywords(db, [stray, kept], kept, preview['preview_token'])
    # The absorbed row's place moved onto the surviving twin rather than vanishing.
    assert db.conn.execute(
        'SELECT place_id FROM keywords WHERE id = ?', (kept_lake,)).fetchone()[0] == 'lake-place'
    assert {r['photo_id'] for r in db.conn.execute(
        'SELECT photo_id FROM photo_keywords WHERE keyword_id = ?', (kept_lake,))} == {
        photos[0], photos[1]}


def _two_hummingbird_branches(db):
    """Two trips each holding a "Hummingbird" that means a different bird."""
    anna = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte anna', 'Anna bird', 'species', 5112)").lastrowid
    costa = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte costae', 'Costa bird', 'species', 5113)").lastrowid
    old = db.add_keyword('Trip A')
    new = db.add_keyword('Trip B')
    old_bird = db.add_keyword('Hummingbird', parent_id=old, is_species=True)
    new_bird = db.add_keyword('Hummingbird', parent_id=new, is_species=True)
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
                    (anna, old_bird))
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
                    (costa, new_bird))
    db.conn.commit()
    return old, new, old_bird, new_bird


def test_disambiguated_child_carries_its_dependent_names(catalog):
    """Keeping a colliding child under a suffixed name is still a rename. An
    unsynced keyword_add left on the old spelling would write a word the
    database no longer has, and the photos need the flat remove/add so their
    sidecars follow the row."""
    db, photos = catalog
    old, new, old_bird, new_bird = _two_hummingbird_branches(db)
    db.tag_photo(photos[0], old_bird)
    db.tag_photo(photos[1], new_bird)
    db.queue_change(photos[0], 'keyword_add', 'Hummingbird')

    preview = preview_keyword_merge(db, [old, new], new)
    merge_keywords(db, [old, new], new, preview['preview_token'])

    renamed = f'Hummingbird (id-{old_bird})'
    assert db.conn.execute(
        'SELECT name FROM keywords WHERE id = ?', (old_bird,)).fetchone()[0] == renamed
    pending = {(r['change_type'], r['value']) for r in db.conn.execute(
        'SELECT change_type, value FROM pending_changes WHERE photo_id = ?', (photos[0],))}
    assert ('keyword_add', renamed) in pending
    assert ('keyword_add', 'Hummingbird') not in pending
    assert ('keyword_remove_flat', 'Hummingbird') in pending


def test_disambiguation_skips_a_name_that_is_already_taken(catalog):
    """The suffix is not unique by construction -- a user can already own the
    exact name it produces. Colliding a second time raised a bare SQLite
    IntegrityError out of /api/keywords/merge."""
    db, photos = catalog
    old, new, old_bird, new_bird = _two_hummingbird_branches(db)
    squatter = db.add_keyword(f'Hummingbird (id-{old_bird})', parent_id=new)
    db.tag_photo(photos[0], old_bird)
    db.tag_photo(photos[1], new_bird)
    db.tag_photo(photos[2], squatter)

    preview = preview_keyword_merge(db, [old, new], new)
    planned = next(c['new_name'] for c in preview['children'] if c['id'] == old_bird)
    assert planned == f'Hummingbird (id-{old_bird}) 2'
    merge_keywords(db, [old, new], new, preview['preview_token'])

    assert sorted(r['name'] for r in db.conn.execute(
        'SELECT name FROM keywords WHERE parent_id = ?', (new,))) == [
        'Hummingbird', f'Hummingbird (id-{old_bird})', f'Hummingbird (id-{old_bird}) 2']
    # The squatter keeps its own row and photo; only the migrating child moved.
    assert db.conn.execute(
        'SELECT keyword_id FROM photo_keywords WHERE photo_id = ?',
        (photos[2],)).fetchone()['keyword_id'] == squatter


def test_merge_keeps_a_legacy_species_flag_a_retype_cleared(catalog):
    """A legacy ``general, is_species=1`` row absorbing a taxonomy row: the
    merge clears is_species on the way through so a retyped row cannot leak
    into species queries, and the chooser then restores a species link. The
    finalization has to set the flag from the resolved state, not leave the
    intermediate one -- a linked row that ``is_species = 1 OR type =
    'taxonomy'`` misses is invisible to every species surface."""
    db, photos = catalog
    anna = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte anna', 'Anna bird', 'species', 5112)").lastrowid
    legacy = db.add_keyword('Hummer', kw_type='general')
    db.conn.execute('UPDATE keywords SET is_species = 1 WHERE id = ?', (legacy,))
    taxonomy = db.add_keyword('Hummingbird', is_species=True)
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
                    (anna, taxonomy))
    db.conn.commit()
    db.tag_photo(photos[0], taxonomy)
    db.tag_photo(photos[1], legacy)

    preview = preview_keyword_merge(db, [taxonomy, legacy], legacy)
    assert preview['resolved']['taxon_id'] == anna
    merge_keywords(db, [taxonomy, legacy], legacy, preview['preview_token'])

    row = db.conn.execute(
        'SELECT type, is_species, taxon_id FROM keywords WHERE id = ?', (legacy,)).fetchone()
    assert (row['type'], row['taxon_id']) == ('general', anna)
    assert row['is_species'] == 1


def test_merge_does_not_give_a_chosen_place_another_places_coordinates(catalog):
    """A retained Google place owns its own point. Filling a coordinate-less
    chosen place from an unrelated row would put it somewhere it isn't -- and
    the merge exports the result as GPS to every tagged photo."""
    db, photos = catalog
    unlocated = db.add_keyword('Overlook', kw_type='location')
    located = db.add_keyword('Overlook Point', kw_type='location')
    db.conn.execute("UPDATE keywords SET place_id = 'place-a' WHERE id = ?", (unlocated,))
    db.conn.execute("UPDATE keywords SET place_id = 'place-b', latitude = 48.0, "
                    "longitude = 11.0 WHERE id = ?", (located,))
    db.conn.commit()
    db.tag_photo(photos[0], unlocated)
    db.tag_photo(photos[1], located)

    preview = preview_keyword_merge(db, [unlocated, located], located, {'place_id': 'place-a'})
    assert preview['resolved']['place_id'] == 'place-a'
    assert (preview['latitude'], preview['longitude']) == (None, None)
    # Dropping the link entirely is the case where a fallback is still right.
    unlinked = preview_keyword_merge(db, [unlocated, located], located, {'place_id': None})
    assert (unlinked['latitude'], unlinked['longitude']) == (48.0, 11.0)


def test_planner_folds_absorbed_identity_for_a_third_branch(catalog):
    """With three branches, the second same-named child is compared against a
    destination the first one already folded its place into. A simulation that
    skips that fold predicts a collapse the write path turns into a rename,
    and the aliases queued from the prediction would point the child's photos
    at the wrong sibling."""
    db, photos = catalog
    first = db.add_keyword('Trip A', kw_type='location')
    second = db.add_keyword('Trip B', kw_type='location')
    kept = db.add_keyword('Trip C', kw_type='location')
    first_lake = db.add_keyword('Lake', parent_id=first, kw_type='location')
    second_lake = db.add_keyword('Lake', parent_id=second, kw_type='location')
    kept_lake = db.add_keyword('Lake', parent_id=kept, kw_type='location')
    db.conn.execute("UPDATE keywords SET place_id = 'lake-a' WHERE id = ?", (first_lake,))
    db.conn.execute("UPDATE keywords SET place_id = 'lake-b' WHERE id = ?", (second_lake,))
    db.conn.commit()
    for photo, keyword in zip(photos, (first_lake, second_lake, kept_lake), strict=True):
        db.tag_photo(photo, keyword)

    preview = preview_keyword_merge(db, [first, second, kept], kept)
    predicted = {c['id']: c['outcome'] for c in preview['children']}
    assert predicted[first_lake] == 'merge'
    # lake-a folded onto the unlinked survivor, so lake-b is now a conflict.
    assert predicted[second_lake] == 'rename'
    merge_keywords(db, [first, second, kept], kept, preview['preview_token'])

    assert not db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (first_lake,)).fetchone()
    survivor = db.conn.execute(
        'SELECT name, parent_id FROM keywords WHERE id = ?', (second_lake,)).fetchone()
    assert (survivor['name'], survivor['parent_id']) == ('Lake (lake-b)', kept)
    assert db.conn.execute(
        'SELECT place_id FROM keywords WHERE id = ?', (kept_lake,)).fetchone()[0] == 'lake-a'


def test_species_options_group_by_resolved_identity(catalog):
    """One row linked by local taxon_id and another by the matching iNat
    source_taxon_id name the same species. Offering two indistinguishable
    choices would demand a decision that has no answer."""
    db, photos = catalog
    anna = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte anna', 'Anna bird', 'species', 5112)").lastrowid
    by_local = db.add_keyword('Hummer A', is_species=True)
    by_inat = db.add_keyword('Hummer B', is_species=True)
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
                    (anna, by_local))
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', source_taxon_id = 5112 WHERE id = ?",
                    (by_inat,))
    db.conn.commit()
    db.tag_photo(photos[0], by_local)
    db.tag_photo(photos[1], by_inat)

    preview = preview_keyword_merge(db, [by_local, by_inat], by_local)
    assert len(preview['options']['species']) == 1
    assert preview['requires_choice'] == []
    assert 'preview_token' in preview


def test_merge_refuses_to_steal_an_alias_owned_by_an_unrelated_keyword(catalog):
    """Every retired path gets aliased to wherever its photos land, so a
    moved descendant's old path can hijack a durable alias some other keyword
    already owns and silently redirect future imports."""
    db, photos = catalog
    stray = db.add_keyword('Stray', kw_type='location')
    leaf = db.add_keyword('Leaf', parent_id=stray, kw_type='location')
    kept = db.add_keyword('Kept', kw_type='location')
    unrelated = db.add_keyword('Somewhere else', kw_type='location')
    db.tag_photo(photos[0], leaf)
    db.tag_photo(photos[1], kept)
    db.tag_photo(photos[2], unrelated)
    db.conn.execute(
        'INSERT INTO keyword_import_aliases VALUES (?, ?, ?)',
        (path_key(['Stray', 'Leaf']), json.dumps(['Stray', 'Leaf']), unrelated))
    db.conn.commit()

    with pytest.raises(ValueError, match='already resolves to a different keyword'):
        preview_keyword_merge(db, [stray, kept], kept)
    assert db.conn.execute(
        'SELECT keyword_id FROM keyword_import_aliases WHERE path_key = ?',
        (path_key(['Stray', 'Leaf']),)).fetchone()['keyword_id'] == unrelated

def test_manual_merge_asks_which_link_to_keep_instead_of_refusing(catalog):
    """Two rows carrying different real-world identities have no honest
    default, so the preview names the field and withholds its token rather
    than silently retagging photos onto one of them."""
    db, photos = catalog
    first, second, _ = place_pair(db)
    other = db.upsert_place_chain({
        'place_id': 'other-place', 'name': 'Other lake', 'lat': 34, 'lng': -116,
        'address_components': []})
    db.tag_photo(photos[0], other)
    db.tag_photo(photos[1], second)

    preview = preview_keyword_merge(db, [other, second], second)
    assert preview['requires_choice'] == ['place']
    assert 'preview_token' not in preview
    assert {o['place_id'] for o in preview['options']['place']} == {'other-place', 'test-place'}
    with pytest.raises(ValueError, match='Choose which'):
        merge_keywords(db, [other, second], second, 'no-token')

    chosen = {'place_id': 'other-place'}
    preview = preview_keyword_merge(db, [other, second], second, chosen)
    assert preview['resolved']['place_id'] == 'other-place'
    # Coordinates follow the chosen place rather than the retained row's.
    assert (preview['latitude'], preview['longitude']) == (34, -116)
    merge_keywords(db, [other, second], second, preview['preview_token'], chosen)
    row = db.conn.execute(
        'SELECT place_id, latitude, longitude FROM keywords WHERE id = ?', (second,)).fetchone()
    assert (row['place_id'], row['latitude'], row['longitude']) == ('other-place', 34, -116)
    assert not db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (other,)).fetchone()


def test_manual_merge_asks_which_species_link_to_keep(catalog):
    db, photos = catalog
    source, target = species_pair(db)[:2]
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    db.conn.execute('UPDATE keywords SET source_taxon_id = 999 WHERE id = ?', (source,))
    db.conn.commit()

    preview = preview_keyword_merge(db, [source, target], target)
    assert preview['requires_choice'] == ['species']
    # The chooser has to say which species, not which row number -- and an
    # iNat id the local taxonomy lacks must not borrow the other option's name.
    assert {(o['source_taxon_id'], o['taxon_name'], o['taxon_common_name'])
            for o in preview['options']['species']} == {
        (None, 'Testus bird', 'Test bird'), (999, None, None)}
    pick = {'species': {'taxon_id': preview['options']['species'][0]['taxon_id'],
                        'source_taxon_id': 999}}
    preview = preview_keyword_merge(db, [source, target], target, pick)
    merge_keywords(db, [source, target], target, preview['preview_token'], pick)
    assert db.conn.execute(
        'SELECT source_taxon_id FROM keywords WHERE id = ?', (target,)).fetchone()[0] == 999


def test_manual_merge_mixes_spelling_and_place_across_rows(catalog):
    """The retained row supplies the record that survives, not every value on
    it: keeping one row's spelling alongside another's place link is the
    whole point of the chooser."""
    db, photos = catalog
    plain = db.add_keyword('Borrego Palm Cyn.', kw_type='location')
    linked = db.upsert_place_chain({
        'place_id': 'trailhead', 'name': 'Borrego Palm Canyon Trailhead',
        'lat': 33.27, 'lng': -116.42, 'address_components': []})
    db.tag_photo(photos[0], plain)
    db.tag_photo(photos[1], linked)

    overrides = {'name': 'Borrego Palm Cyn.', 'parent_id': None}
    preview = preview_keyword_merge(db, [plain, linked], linked, overrides)
    assert preview['resolved']['name'] == 'Borrego Palm Cyn.'
    assert preview['resolved']['place_id'] == 'trailhead'
    assert not preview['requires_choice']
    merge_keywords(db, [plain, linked], linked, preview['preview_token'], overrides)

    row = db.conn.execute(
        'SELECT name, place_id, latitude FROM keywords WHERE id = ?', (linked,)).fetchone()
    assert (row['name'], row['place_id'], row['latitude']) == (
        'Borrego Palm Cyn.', 'trailhead', 33.27)
    # The renamed survivor's photos must export the chosen spelling, and the
    # spelling the merge retired must come back out of the sidecars.
    pending = {(r['photo_id'], r['change_type'], r['value']) for r in db.conn.execute(
        'SELECT photo_id, change_type, value FROM pending_changes')}
    assert (photos[1], 'keyword_add', 'Borrego Palm Cyn.') in pending
    assert (photos[0], 'keyword_add', 'Borrego Palm Cyn.') in pending
    assert all(value != 'Borrego Palm Canyon Trailhead'
               for _, change_type, value in pending if change_type == 'keyword_add')


def test_manual_merge_rename_carries_pending_edits_and_curation(catalog):
    """A chooser rename is a rename: strings that mirror the keyword's name
    have to move with it or an unsynced add writes the retired spelling."""
    db, photos = catalog
    source = db.add_keyword('Wing St. Canyon', kw_type='location')
    target = db.add_keyword('Wing Street Canyon', kw_type='location')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    db.queue_change(photos[1], 'keyword_add', 'Wing Street Canyon')

    overrides = {'name': 'Wing Canyon'}
    preview = preview_keyword_merge(db, [source, target], target, overrides)
    merge_keywords(db, [source, target], target, preview['preview_token'], overrides)

    values = {r['value'] for r in db.conn.execute(
        "SELECT value FROM pending_changes WHERE change_type = 'keyword_add'")}
    assert values == {'Wing Canyon'}
    assert db.conn.execute(
        'SELECT name FROM keywords WHERE id = ?', (target,)).fetchone()[0] == 'Wing Canyon'


@pytest.mark.parametrize('overrides,message', [
    ({'name': '   '}, 'Enter a name'),
    ({'name': 'A|B'}, 'may not contain'),
    ({'parent_id': 424242}, 'offered parent'),
    ({'type': 'genre'}, 'types'),
    ({'place_id': 'not-selected'}, 'linked places'),
    ({'coordinates': {'latitude': 91, 'longitude': 0}}, 'between'),
    ({'coordinates': {'latitude': 'north', 'longitude': 0}}, 'numeric'),
    ({'coordinates': 33.2}, 'both a latitude'),
    ({'nonsense': 1}, 'Unrecognized'),
])
def test_manual_merge_rejects_unusable_overrides(catalog, overrides, message):
    db, photos = catalog
    source = db.add_keyword('Wing St. Canyon', kw_type='location')
    target = db.add_keyword('Wing Street Canyon', kw_type='location')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    with pytest.raises(ValueError, match=message):
        preview_keyword_merge(db, [source, target], target, overrides)


def test_manual_merge_coordinates_are_editable_and_clearable(catalog):
    db, photos = catalog
    source = db.add_keyword('Wing St. Canyon', kw_type='location')
    target = db.add_keyword('Wing Street Canyon', kw_type='location')
    db.update_keyword(target, latitude=32.7447, longitude=-117.2186)
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)

    overrides = {'coordinates': {'latitude': 33.0, 'longitude': -116.0}}
    preview = preview_keyword_merge(db, [source, target], target, overrides)
    assert (preview['latitude'], preview['longitude']) == (33.0, -116.0)
    merge_keywords(db, [source, target], target, preview['preview_token'], overrides)
    row = db.conn.execute(
        'SELECT latitude, longitude FROM keywords WHERE id = ?', (target,)).fetchone()
    assert (row['latitude'], row['longitude']) == (33.0, -116.0)


def test_manual_merge_preview_token_covers_the_chooser(catalog):
    """A token minted for one set of picks must not authorize another."""
    db, photos = catalog
    source = db.add_keyword('Wing St. Canyon', kw_type='location')
    target = db.add_keyword('Wing Street Canyon', kw_type='location')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    preview = preview_keyword_merge(db, [source, target], target)
    with pytest.raises(ValueError, match='changed'):
        merge_keywords(db, [source, target], target, preview['preview_token'],
                       {'name': 'Something else'})
    assert db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (source,)).fetchone()

def test_manual_merge_api_validation_and_revalidation(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    photos = [r['id'] for r in db.conn.execute('SELECT id FROM photos ORDER BY id LIMIT 2')]
    ids = [db.add_keyword(name, kw_type='general') for name in ('Merge first', 'Merge second')]
    for photo, kid in zip(photos, ids, strict=True):
        db.tag_photo(photo, kid)
    for invalid in (None, [], {}, {'keyword_ids': [True, ids[1]], 'target_id': ids[1]},
                    {'keyword_ids': ids, 'target_id': True},
                    {'keyword_ids': [ids[0], ids[0]], 'target_id': ids[0]},
                    {'keyword_ids': ids, 'target_id': 999999},
                    {'keyword_ids': [ids[0], 999999], 'target_id': ids[0]}):
        assert client.post('/api/keywords/merge-preview', json=invalid).status_code == 400
    body = {'keyword_ids': ids, 'target_id': ids[1]}
    assert client.post('/api/keywords/merge', json=body).status_code == 400
    preview = client.post('/api/keywords/merge-preview', json=body)
    assert preview.status_code == 200
    body['preview_token'] = preview.get_json()['preview_token']
    assert client.post('/api/keywords/merge', json=body).status_code == 200
    assert client.post('/api/keywords/merge', json=body).status_code == 400


def test_manual_merge_count_includes_retained_children_and_other_workspaces(catalog):
    db, photos = catalog
    source = db.add_keyword('Alias', kw_type='general')
    target = db.add_keyword('Keep', kw_type='general')
    child = db.add_keyword('Child', parent_id=target)
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], child)
    ws = db._ws_id()
    other = db.create_workspace('Other workspace')
    db.set_active_workspace(other)
    folder = db.add_folder('/other-merge-photos', name='Other photos')
    db.add_workspace_folder(other, folder)
    photo = db.add_photo(folder_id=folder, filename='elsewhere.jpg', extension='.jpg', file_size=1, file_mtime=1)
    db.tag_photo(photo, source)
    db.set_active_workspace(ws)
    preview = preview_keyword_merge(db, [source, target], target)
    assert preview['combined_count'] == 3
    merge_keywords(db, [source, target], target, preview['preview_token'])
    assert db.conn.execute('SELECT parent_id FROM keywords WHERE id = ?', (child,)).fetchone()[0] == target
    assert db.conn.execute('SELECT keyword_id FROM photo_keywords WHERE photo_id = ?', (photo,)).fetchone()[0] == target
    assert db.conn.execute("SELECT 1 FROM pending_changes WHERE photo_id = ? AND workspace_id = ? AND change_type = 'keyword_add'", (photo, other)).fetchone()


def test_manual_merge_sidecar_sync_keeps_unrelated_tags(catalog, tmp_path):
    from PIL import Image
    from sync import sync_to_xmp
    from xmp import read_keywords, write_sidecar

    db, photos = catalog
    directory = tmp_path / 'photos'
    directory.mkdir()
    Image.new('RGB', (2, 2)).save(directory / '0.jpg')
    sidecar = directory / '0.xmp'
    write_sidecar(str(sidecar), {'Wing St. Canyon', 'Unrelated'}, set())
    source = db.add_keyword('Wing St. Canyon', kw_type='location')
    target = db.add_keyword('Wing Street Canyon', kw_type='location')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    preview = preview_keyword_merge(db, [source, target], target)
    merge_keywords(db, [source, target], target, preview['preview_token'])
    changes = [r['id'] for r in db.get_pending_changes() if r['photo_id'] == photos[0]]
    result = sync_to_xmp(db, change_ids=changes)
    assert result['failed'] == 0
    assert result['synced'] == 1
    assert read_keywords(str(sidecar)) == {'Wing Street Canyon', 'Unrelated'}


@pytest.mark.parametrize('keyword_type', ['general', 'taxonomy', 'location', 'individual', 'genre'])
def test_manual_merge_same_name_hierarchy_survives_sync_and_rescan(catalog, tmp_path, keyword_type):
    from PIL import Image
    from scanner import _import_keywords_for_photo
    from sync import sync_from_xmp, sync_to_xmp
    from xmp import write_sidecar

    db, photos = catalog
    old_parent = db.add_keyword('Wrong parent')
    new_parent = db.add_keyword('Retained parent')
    source = db.add_keyword('Shared leaf', parent_id=old_parent, kw_type=keyword_type)
    target = db.add_keyword('Shared leaf', parent_id=new_parent, kw_type=keyword_type)
    unrelated = db.add_keyword('Unrelated')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[0], unrelated)
    db.tag_photo(photos[1], target)
    directory = tmp_path / 'photos'
    directory.mkdir()
    Image.new('RGB', (2, 2)).save(directory / '0.jpg')
    sidecar = str(directory / '0.xmp')
    write_sidecar(sidecar, {'Shared leaf', 'Unrelated'}, {'Wrong parent|Shared leaf'})
    preview = preview_keyword_merge(db, [source, target], target)
    merge_keywords(db, [source, target], target, preview['preview_token'])
    for after_sync in (False, True):
        if after_sync:
            changes = [r['id'] for r in db.get_pending_changes() if r['photo_id'] == photos[0]]
            assert sync_to_xmp(db, change_ids=changes)['failed'] == 0
        _import_keywords_for_photo(db, photos[0], sidecar)
        sync_from_xmp(db, [photos[0]])
        assert {k['id'] for k in db.get_photo_keywords(photos[0])} == {target, unrelated}
        assert not db.conn.execute('SELECT 1 FROM keywords WHERE name = ? AND parent_id = ?',
                                   ('Shared leaf', old_parent)).fetchone()


def test_manual_merge_import_aliases_keep_types_and_manual_additions_distinct(catalog):
    db, photos = catalog
    source = db.add_keyword('Imported label')
    target = db.add_keyword('Retained label')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    preview = preview_keyword_merge(db, [source, target], target)
    merge_keywords(db, [source, target], target, preview['preview_token'])
    assert db.add_keyword('Imported label', _resolve_alias=True) == target
    assert db.add_keyword('Imported label') != target
    typed = db.add_keyword('Imported label', kw_type='location', _resolve_alias=True)
    assert typed != target
    assert db.conn.execute('SELECT type FROM keywords WHERE id = ?', (typed,)).fetchone()[0] == 'location'
    path = db.conn.execute('PRAGMA database_list').fetchone()['file']
    with Database(path) as reopened:
        assert reopened.add_keyword('Imported label', _resolve_alias=True) == target


def test_catalog_import_resolves_multiple_non_location_merge_aliases(catalog, monkeypatch):
    from importer import execute_import

    db, photos = catalog
    expected = set()
    for name in ('First label', 'Second label'):
        old_parent = db.add_keyword('Old ' + name)
        new_parent = db.add_keyword('New ' + name)
        source = db.add_keyword(name, parent_id=old_parent)
        target = db.add_keyword(name, parent_id=new_parent)
        db.tag_photo(photos[0], source)
        db.tag_photo(photos[1], target)
        preview = preview_keyword_merge(db, [source, target], target)
        merge_keywords(db, [source, target], target, preview['preview_token'])
        expected.add(target)
    # Non-location aliases must not participate in the one-linked-place rule.
    place = db.upsert_place_chain({'place_id': 'real-place', 'name': 'A place', 'lat': 33, 'lng': -117,
                                   'address_components': []})
    db.tag_photo(photos[0], place)
    expected.add(place)
    row = db.conn.execute('SELECT f.path, p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?',
                          (photos[0],)).fetchone()
    monkeypatch.setattr('importer.read_catalog', lambda *args, **kwargs: {
        row['path'] + '/' + row['filename']: {
            'flat_keywords': {'First label', 'Second label'},
            'hierarchical_keywords': {'Old First label|First label', 'Old Second label|Second label'},
        },
    })
    execute_import(['dummy.lrcat'], db, write_xmp=False)
    assert {k['id'] for k in db.get_photo_keywords(photos[0])} == expected


def test_catalog_import_matches_windows_paths_across_separator_and_case(catalog, monkeypatch):
    """A Windows catalog path must find the photo the scanner stored.

    `read_catalog` returns Lightroom's own spelling — forward slashes, and
    whatever case the catalog recorded — while the scanner stores what
    `os.walk` produced. When those disagree the import silently skips the
    photo. Both normalizers are identities on POSIX, so `ntpath`'s are patched
    in to make the mismatch reachable from this runner at all.
    """
    import ntpath

    import importer
    from importer import execute_import

    db, _ = catalog
    monkeypatch.setattr(importer.os.path, 'normpath', ntpath.normpath)
    monkeypatch.setattr(importer.os.path, 'normcase', ntpath.normcase)
    folder = db.add_folder('D:\\Pictures\\Trip', name='Trip')
    db.add_workspace_folder(db._ws_id(), folder)
    photo = db.add_photo(folder_id=folder, filename='DSC_1.jpg', extension='.jpg',
                         file_size=10, file_mtime=1, timestamp='2024-06-15T10:00:00')
    monkeypatch.setattr('importer.read_catalog', lambda *args, **kwargs: {
        'd:/Pictures/Trip/DSC_1.jpg': {
            'flat_keywords': {'Osprey'},
            'hierarchical_keywords': set(),
        },
    })
    result = execute_import(['dummy.lrcat'], db, write_xmp=False)
    assert result['skipped'] == 0
    assert [k['name'] for k in db.get_photo_keywords(photo)] == ['Osprey']


def test_manual_merge_preserves_source_removal_on_target_only_photo(catalog, tmp_path):
    from PIL import Image
    from sync import sync_from_xmp, sync_to_xmp
    from xmp import read_keywords, write_sidecar

    db, photos = catalog
    source = db.add_keyword('Old spelling')
    target = db.add_keyword('Retained spelling')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], source)
    db.tag_photo(photos[1], target)
    db.untag_photo(photos[1], source)
    ws = db._ws_id()
    other = db.create_workspace('Shared sidecar')
    folder = db.conn.execute('SELECT folder_id FROM photos WHERE id = ?', (photos[1],)).fetchone()[0]
    db.add_workspace_folder(other, folder)
    for workspace in (ws, other):
        db.queue_change(photos[1], 'keyword_remove', 'Old spelling', workspace_id=workspace)
    before = [dict(r) for r in db.conn.execute('SELECT * FROM pending_changes ORDER BY id')]
    directory = tmp_path / 'photos'
    directory.mkdir()
    Image.new('RGB', (2, 2)).save(directory / '1.jpg')
    sidecar = str(directory / '1.xmp')
    write_sidecar(sidecar, {'Old spelling', 'Retained spelling'}, set())
    preview = preview_keyword_merge(db, [source, target], target)
    merge_keywords(db, [source, target], target, preview['preview_token'])
    assert [dict(r) for r in db.conn.execute('SELECT * FROM pending_changes WHERE photo_id = ? ORDER BY id',
                                           (photos[1],))] == before
    result = sync_to_xmp(db, change_ids=[r['id'] for r in before if r['workspace_id'] == ws])
    assert result['failed'] == 0
    assert read_keywords(sidecar) == {'Retained spelling'}
    sync_from_xmp(db, [photos[1]])
    assert {k['id'] for k in db.get_photo_keywords(photos[1])} == {target}


@pytest.mark.parametrize('remove_before_sync', [False, True])
def test_manual_merge_rewrites_exact_hierarchy_and_allows_later_removal(catalog, tmp_path, remove_before_sync):
    from PIL import Image
    from scanner import _import_keywords_for_photo
    from sync import sync_from_xmp, sync_to_xmp
    from xmp import read_hierarchical_keywords, read_keywords, write_sidecar

    db, photos = catalog
    old_parent = db.add_keyword('Old parent')
    new_parent = db.add_keyword('New parent')
    source = db.add_keyword('Old leaf', parent_id=old_parent)
    target = db.add_keyword('New leaf', parent_id=new_parent)
    other_parent = db.add_keyword('Things')
    homonym_parent = db.add_keyword('Old leaf', parent_id=other_parent)
    unrelated = db.add_keyword('Detail', parent_id=homonym_parent)
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[0], unrelated)
    db.tag_photo(photos[1], target)
    directory = tmp_path / 'photos'
    directory.mkdir()
    Image.new('RGB', (2, 2)).save(directory / '0.jpg')
    sidecar = str(directory / '0.xmp')
    write_sidecar(sidecar, {'Old leaf', 'Detail'}, {'Old parent|Old leaf', 'Things|Old leaf|Detail'})
    preview = preview_keyword_merge(db, [source, target], target)
    merge_keywords(db, [source, target], target, preview['preview_token'])
    def sync_photo():
        changes = [r['id'] for r in db.get_pending_changes() if r['photo_id'] == photos[0]]
        assert sync_to_xmp(db, change_ids=changes)['failed'] == 0
    if not remove_before_sync:
        sync_photo()
        assert read_keywords(sidecar) == {'New leaf', 'Detail'}
        assert set(read_hierarchical_keywords(sidecar)) == {'New parent|New leaf', 'Things|Old leaf|Detail'}
        _import_keywords_for_photo(db, photos[0], sidecar)
        sync_from_xmp(db, [photos[0]])
        assert {k['id'] for k in db.get_photo_keywords(photos[0])} == {target, unrelated}
    db.untag_photo(photos[0], target)
    db.remove_pending_changes(photos[0], 'keyword_add', 'New leaf')
    db.queue_change(photos[0], 'keyword_remove', 'New leaf')
    sync_photo()
    assert read_keywords(sidecar) == {'Detail'}
    assert set(read_hierarchical_keywords(sidecar)) == {'Things|Old leaf|Detail'}
    _import_keywords_for_photo(db, photos[0], sidecar)
    sync_from_xmp(db, [photos[0]])
    assert {k['id'] for k in db.get_photo_keywords(photos[0])} == {unrelated}


@pytest.mark.parametrize('cancel_add', [False, True])
@pytest.mark.parametrize('same_name', [False, True])
@pytest.mark.parametrize('target_present', [False, True])
def test_merge_flat_cleanup_survives_api_cancellation_and_partial_sync(app_and_db, tmp_path, cancel_add, same_name, target_present):
    from PIL import Image
    from scanner import _import_keywords_for_photo
    from sync import sync_from_xmp, sync_to_xmp
    from xmp import read_hierarchical_keywords, read_keywords, write_sidecar

    app, db = app_and_db
    client = app.test_client()
    directory = tmp_path / 'merge-photo'
    directory.mkdir()
    Image.new('RGB', (2, 2)).save(directory / 'photo.jpg')
    folder = db.add_folder(str(directory), name='Merge photo')
    db.add_workspace_folder(db._ws_id(), folder)
    photo = db.add_photo(folder_id=folder, filename='photo.jpg', extension='.jpg', file_size=1, file_mtime=1)
    old_parent = db.add_keyword('Old parent')
    new_parent = db.add_keyword('New parent')
    source = db.add_keyword('Old leaf', parent_id=old_parent)
    target_name = 'Old leaf' if same_name else 'New leaf'
    target = db.add_keyword(target_name, parent_id=new_parent)
    db.tag_photo(photo, source)
    db.tag_photo(1, target)
    sidecar = str(directory / 'photo.xmp')
    write_sidecar(sidecar, {'Old leaf'}, {'Old parent|Old leaf'})
    if target_present:
        db.tag_photo(photo, target)
        write_sidecar(sidecar, {target_name}, {'New parent|' + target_name})
    body = {'keyword_ids': [source, target], 'target_id': target}
    response = client.post('/api/keywords/merge-preview', json=body)
    assert response.status_code == 200
    body['preview_token'] = response.get_json()['preview_token']
    assert client.post('/api/keywords/merge', json=body).status_code == 200
    if cancel_add:
        assert client.delete(f'/api/photos/{photo}/keywords/{target}').status_code == 200
        # A background scan or an explicit sidecar read can happen before
        # pending edits are written; aliases must respect that removal too.
        _import_keywords_for_photo(db, photo, sidecar)
        sync_from_xmp(db, [photo])
        assert not db.get_photo_keywords(photo)
    pending = [dict(r) for r in db.get_pending_changes() if r['photo_id'] == photo]
    preview = client.get('/api/sync/preview').get_json()
    preview_photo = next(p for p in preview['photos'] if p['photo_id'] == photo)
    merge_change = next(c for c in preview_photo['changes'] if c['type'] == 'keyword_merge')
    assert merge_change['presentation']['before'] == 'Old parent → Old leaf'
    assert merge_change['presentation']['after'] == ('Removed' if cancel_add else 'New parent → ' + target_name)
    if cancel_add:
        assert not any(r['change_type'] == 'keyword_add' for r in pending)
        assert not any(r['change_type'] == 'keyword_remove' for r in pending)
        assert any(r['change_type'] == 'keyword_merge' for r in pending)
    cleanup = [r['id'] for r in pending if r['change_type'] == 'keyword_remove_flat'] or [r['id'] for r in pending]
    assert sync_to_xmp(db, change_ids=cleanup)['failed'] == 0
    assert read_keywords(sidecar) == (set() if cancel_add else {target_name})
    assert set(read_hierarchical_keywords(sidecar)) == (set() if cancel_add else {'New parent|' + target_name})
    _import_keywords_for_photo(db, photo, sidecar)
    sync_from_xmp(db, [photo])
    assert {k['id'] for k in db.get_photo_keywords(photo)} == (set() if cancel_add else {target})


def test_cancel_unrelated_homonym_add_preserves_existing_hierarchy(app_and_db, tmp_path):
    from sync import sync_from_xmp, sync_to_xmp
    from xmp import read_hierarchical_keywords, read_keywords, write_sidecar

    app, db = app_and_db
    client = app.test_client()
    directory = tmp_path / 'homonym-photo'
    directory.mkdir()
    folder = db.add_folder(str(directory), name='Homonym photo')
    db.add_workspace_folder(db._ws_id(), folder)
    photo = db.add_photo(folder_id=folder, filename='photo.jpg', extension='.jpg', file_size=1, file_mtime=1)
    people = db.add_keyword('People', kw_type='individual')
    individual = db.add_keyword('Robin', parent_id=people, kw_type='individual')
    source = db.add_keyword('Old Robin', kw_type='taxonomy')
    target = db.add_keyword('Robin', kw_type='taxonomy')
    db.tag_photo(1, source)
    db.tag_photo(1, target)
    preview = preview_keyword_merge(db, [source, target], target)
    merge_keywords(db, [source, target], target, preview['preview_token'])
    db.tag_photo(photo, individual)
    sidecar = str(directory / 'photo.xmp')
    write_sidecar(sidecar, {'Robin'}, {'People|Robin'})
    assert client.post(f'/api/photos/{photo}/keywords', json={'keyword_id': target}).status_code == 200
    assert client.delete(f'/api/photos/{photo}/keywords/{target}').status_code == 200
    assert not [r for r in db.get_pending_changes() if r['photo_id'] == photo]
    sync_to_xmp(db)
    assert read_keywords(sidecar) == {'Robin'}
    assert set(read_hierarchical_keywords(sidecar)) == {'People|Robin'}
    sync_from_xmp(db, [photo])
    assert {k['id'] for k in db.get_photo_keywords(photo)} == {individual}


def test_chained_merges_sync_together_when_only_latest_add_is_selected(catalog, tmp_path):
    from sync import sync_to_xmp
    from xmp import read_hierarchical_keywords, read_keywords, write_sidecar

    db, photos = catalog
    directory = tmp_path / 'photos'
    directory.mkdir()
    ids = []
    for name in ('First', 'Second', 'Third'):
        parent = db.add_keyword(name + ' parent')
        keyword = db.add_keyword(name, parent_id=parent)
        db.tag_photo(photos[0], keyword)
        ids.append(keyword)
    sidecar = str(directory / '0.xmp')
    write_sidecar(sidecar, {'First'}, {'First parent|First'})
    for source, target in zip(ids, ids[1:], strict=False):
        preview = preview_keyword_merge(db, [source, target], target)
        merge_keywords(db, [source, target], target, preview['preview_token'])
    additions = [r['id'] for r in db.get_pending_changes() if r['change_type'] == 'keyword_add']
    assert sync_to_xmp(db, change_ids=additions)['failed'] == 0
    assert read_keywords(sidecar) == {'Third'}
    assert set(read_hierarchical_keywords(sidecar)) == {'Third parent|Third'}
    assert not db.get_pending_changes()


@pytest.mark.parametrize('reader', ['scan', 'sync', 'catalog'])
def test_flat_only_import_resolves_unambiguous_merged_nested_leaf(catalog, tmp_path, monkeypatch, reader):
    from importer import execute_import
    from scanner import _import_keywords_for_photo
    from sync import sync_from_xmp
    from xmp import write_sidecar

    db, photos = catalog
    parent = db.add_keyword('Old parent')
    source = db.add_keyword('Old leaf', parent_id=parent)
    target = db.add_keyword('Retained leaf')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    preview = preview_keyword_merge(db, [source, target], target)
    merge_keywords(db, [source, target], target, preview['preview_token'])
    directory = tmp_path / 'photos'
    directory.mkdir()
    sidecar = str(directory / '2.xmp')
    write_sidecar(sidecar, {'Old leaf'}, set())
    if reader == 'scan':
        _import_keywords_for_photo(db, photos[2], sidecar)
    elif reader == 'sync':
        sync_from_xmp(db, [photos[2]])
    else:
        monkeypatch.setattr('importer.read_catalog', lambda *args, **kwargs: {
            str(directory / '2.jpg'): {
                'flat_keywords': {'Old leaf'}, 'hierarchical_keywords': set(),
            },
        })
        execute_import(['dummy.lrcat'], db, write_xmp=False)
    assert {k['id'] for k in db.get_photo_keywords(photos[2])} == {target}
    assert not db.conn.execute('SELECT 1 FROM keywords WHERE name = ?', ('Old leaf',)).fetchone()


def test_flat_merge_alias_does_not_override_ambiguous_live_identity(catalog):
    from keyword_identity import resolve_import_path

    db, photos = catalog
    parent = db.add_keyword('Old parent')
    source = db.add_keyword('Robin', parent_id=parent, kw_type='taxonomy')
    target = db.add_keyword('Bird', kw_type='taxonomy')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    preview = preview_keyword_merge(db, [source, target], target)
    merge_keywords(db, [source, target], target, preview['preview_token'])
    assert resolve_import_path(db, ['Robin']) == target
    people = db.add_keyword('People', kw_type='individual')
    db.add_keyword('Robin', parent_id=people, kw_type='individual')
    assert resolve_import_path(db, ['Robin']) is None
    assert resolve_import_path(db, ['Robin'], kw_type='taxonomy') == target
    assert resolve_import_path(db, ['Old parent', 'Robin']) == target


def test_manual_merge_rejects_case_insensitive_sibling_collision(catalog):
    """Every keyword lookup uses ``keyword_match_key`` and ``add_keyword``
    dedupes case-insensitively, but SQLite's own UNIQUE(name, parent_id) is
    BINARY so ``foo`` and ``Foo`` can already coexist under one parent from a
    legacy path. An override renaming the survivor to a name whose match key
    a sibling already owns has to be rejected at preview time -- the write
    path would otherwise leave two semantic peers no import could tell apart.
    """
    db, photos = catalog
    parent = db.add_keyword('Parent')
    source = db.add_keyword('foo', parent_id=parent)
    target = db.add_keyword('bar', parent_id=parent)
    # Bypass ``add_keyword``'s ``COLLATE NOCASE`` dedupe to plant a same-key
    # sibling; the underlying table constraint permits it.
    squatter = db.conn.execute(
        'INSERT INTO keywords(name, parent_id) VALUES (?, ?)', ('Foo', parent),
    ).lastrowid
    db.conn.commit()
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    db.tag_photo(photos[2], squatter)

    for override in ('foo', 'FOO'):
        with pytest.raises(ValueError, match='Another keyword already sits'):
            preview_keyword_merge(db, [source, target], target, {'name': override})
    # Sanity: an override with a genuinely free key still succeeds, so the
    # guard has not swallowed the normal case.
    ok = preview_keyword_merge(db, [source, target], target, {'name': 'baz'})
    assert ok['resolved']['name'] == 'baz'


def test_manual_merge_rejects_non_scalar_choice_payloads(catalog):
    """``overrides`` reaches this code from ``request.get_json``, so a
    malformed payload like ``{"type": {}}`` or ``{"place_id": []}`` can arrive
    with an unhashable value. A membership test against a set would raise
    ``TypeError`` and escape ``ValueError``-only handling as an HTTP 500;
    the caller expected a 400 with a clear message.
    """
    db, photos = catalog
    source = db.add_keyword('Wing St. Canyon', kw_type='location')
    target = db.add_keyword('Wing Street Canyon', kw_type='location')
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    for bad_type in ({}, [], 5):
        with pytest.raises(ValueError, match='types'):
            preview_keyword_merge(db, [source, target], target, {'type': bad_type})
    for bad_place in ({}, [], 5):
        with pytest.raises(ValueError, match='linked places'):
            preview_keyword_merge(db, [source, target], target, {'place_id': bad_place})


def test_species_option_preserves_complementary_taxon_links(catalog):
    """A row linked by local ``taxon_id`` and one linked by the matching iNat
    ``source_taxon_id`` name the same species and collapse into one option.
    That option has to carry BOTH raw links -- ``_apply_merge_overrides``
    otherwise writes the missing field back as ``None`` and drops either the
    local link or the source provenance."""
    db, photos = catalog
    anna = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte anna', 'Anna bird', 'species', 5112)").lastrowid
    by_local = db.add_keyword('Hummer A', is_species=True)
    by_inat = db.add_keyword('Hummer B', is_species=True)
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', taxon_id = ? WHERE id = ?",
                    (anna, by_local))
    db.conn.execute("UPDATE keywords SET type = 'taxonomy', source_taxon_id = 5112 WHERE id = ?",
                    (by_inat,))
    db.conn.commit()
    db.tag_photo(photos[0], by_local)
    db.tag_photo(photos[1], by_inat)

    preview = preview_keyword_merge(db, [by_local, by_inat], by_local)
    (option,) = preview['options']['species']
    assert option['taxon_id'] == anna
    assert option['source_taxon_id'] == 5112
    merge_keywords(db, [by_local, by_inat], by_local, preview['preview_token'])
    row = db.conn.execute(
        'SELECT taxon_id, source_taxon_id FROM keywords WHERE id = ?', (by_local,)
    ).fetchone()
    assert row['taxon_id'] == anna
    assert row['source_taxon_id'] == 5112


def test_merge_resyncs_photos_on_a_metadata_folded_child(catalog):
    """When two parent branches have colliding location children and the
    incoming child supplies a ``place_id`` or coordinates the retained child
    lacks, ``_merge_keyword_into`` folds that metadata onto the retained
    child. Photos tagged solely with the retained child now sit at a new
    catalog location; without a ``location`` resync their sidecars still
    hold the old (empty) location metadata."""
    db, photos = catalog
    kept = db.add_keyword('Park A', kw_type='location')
    stray = db.add_keyword('Park B', kw_type='location')
    kept_lake = db.add_keyword('Lake', parent_id=kept, kw_type='location')
    stray_lake = db.add_keyword('Lake', parent_id=stray, kw_type='location')
    db.conn.execute("UPDATE keywords SET place_id = 'lake-place', "
                    "latitude = 33.06, longitude = -117.11 WHERE id = ?", (stray_lake,))
    db.conn.commit()
    # Photo 0 is tagged solely with the retained (place-less) child.
    db.tag_photo(photos[0], kept_lake)
    # Photo 1 is tagged with the collapsing sibling that supplies the place.
    db.tag_photo(photos[1], stray_lake)

    preview = preview_keyword_merge(db, [stray, kept], kept)
    assert kept_lake in preview['metadata_folded_ids']
    merge_keywords(db, [stray, kept], kept, preview['preview_token'])

    # The retained child now carries the collapsing sibling's place.
    row = db.conn.execute(
        'SELECT place_id, latitude, longitude FROM keywords WHERE id = ?',
        (kept_lake,)).fetchone()
    assert row['place_id'] == 'lake-place'

    # Both photos need a ``location`` resync -- the one tagged solely with
    # the retained child too, or its sidecar keeps the pre-fold location.
    resynced = {r['photo_id'] for r in db.conn.execute(
        "SELECT photo_id FROM pending_changes WHERE change_type = 'location'")}
    assert photos[0] in resynced
    assert photos[1] in resynced


def test_manual_merge_case_variant_child_collapses_into_destination_sibling(catalog):
    """SQLite's UNIQUE(name, parent_id) is BINARY, so a source child named
    ``heron`` can reparent under a destination that already holds ``Heron``
    without hitting an IntegrityError. Every keyword lookup uses
    ``keyword_match_key`` though, so leaving both rows in place would create
    two semantic peers no import could tell apart. The planner has to catch
    the folded collision the way the exact-name path does, and the write
    path has to mirror the same outcome."""
    db, photos = catalog
    old = db.add_keyword('Trip A', kw_type='general')
    new = db.add_keyword('Trip B', kw_type='general')
    old_bird = db.add_keyword('heron', parent_id=old, kw_type='general')
    # Bypass ``add_keyword``'s ``COLLATE NOCASE`` dedupe to plant a
    # case-variant sibling; the underlying table constraint is BINARY so it
    # permits the raw INSERT.
    new_bird = db.conn.execute(
        'INSERT INTO keywords(name, parent_id, type) VALUES (?, ?, ?)',
        ('Heron', new, 'general'),
    ).lastrowid
    db.conn.commit()
    db.tag_photo(photos[0], old_bird)
    db.tag_photo(photos[1], new_bird)

    preview = preview_keyword_merge(db, [old, new], new)
    outcomes = {c['id']: (c['outcome'], c['new_name']) for c in preview['children']}
    assert outcomes[old_bird] == ('merge', 'heron')

    merge_keywords(db, [old, new], new, preview['preview_token'])
    # Only one Heron survives under the retained parent, carrying both photos.
    survivors = [dict(r) for r in db.conn.execute(
        "SELECT id, name FROM keywords WHERE parent_id = ?", (new,))]
    assert len(survivors) == 1
    assert {r['photo_id'] for r in db.conn.execute(
        'SELECT photo_id FROM photo_keywords WHERE keyword_id = ?',
        (survivors[0]['id'],))} == {photos[0], photos[1]}


def test_manual_merge_case_variant_child_disambiguates_when_taxa_differ(catalog):
    """When the case-folded collision names two rows that carry different
    taxa, the planner has to preserve them the same way an exact-name
    same-taxon collision does -- suffix the migrating child rather than
    collapsing it and silently retagging its photos as the other species."""
    db, photos = catalog
    anna = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte anna', 'Anna bird', 'species', 5112)").lastrowid
    costa = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) "
        "VALUES ('Calypte costae', 'Costa bird', 'species', 5113)").lastrowid
    old = db.add_keyword('Trip A')
    new = db.add_keyword('Trip B')
    # Raw INSERTs to bypass ``add_keyword``'s casing normalization: this
    # test needs the two branches to hold the case-variant spellings the
    # BINARY UNIQUE(name, parent_id) index still permits.
    old_bird = db.conn.execute(
        'INSERT INTO keywords(name, parent_id, type, is_species, taxon_id) '
        'VALUES (?, ?, ?, 1, ?)',
        ('hummingbird', old, 'taxonomy', anna),
    ).lastrowid
    new_bird = db.conn.execute(
        'INSERT INTO keywords(name, parent_id, type, is_species, taxon_id) '
        'VALUES (?, ?, ?, 1, ?)',
        ('Hummingbird', new, 'taxonomy', costa),
    ).lastrowid
    db.conn.commit()
    db.tag_photo(photos[0], old_bird)
    db.tag_photo(photos[1], new_bird)

    preview = preview_keyword_merge(db, [old, new], new)
    outcomes = {c['id']: (c['outcome'], c['new_name']) for c in preview['children']}
    assert outcomes[old_bird] == ('rename', f'hummingbird (id-{old_bird})')

    merge_keywords(db, [old, new], new, preview['preview_token'])
    # Both taxa survive under the retained parent, each keeping its photo.
    rows = {dict(r)['id']: dict(r) for r in db.conn.execute(
        "SELECT id, name, taxon_id FROM keywords WHERE parent_id = ?", (new,))}
    assert rows[new_bird]['taxon_id'] == costa
    assert rows[old_bird]['taxon_id'] == anna
    assert {r['photo_id'] for r in db.conn.execute(
        'SELECT photo_id FROM photo_keywords WHERE keyword_id = ?',
        (old_bird,))} == {photos[0]}
    assert {r['photo_id'] for r in db.conn.execute(
        'SELECT photo_id FROM photo_keywords WHERE keyword_id = ?',
        (new_bird,))} == {photos[1]}


def test_manual_merge_resyncs_descendants_when_survivor_crosses_location_boundary(catalog):
    """``get_photo_location_paths`` stops walking at the first non-location
    ancestor. Retyping the retained parent between 'location' and something
    else changes what that walk includes for every location descendant under
    it, even when their own textual paths are unchanged -- and
    ``_queue_moved_subtree_changes`` covers only descendants whose path
    changed. The merge has to queue a ``location`` resync for photos tagged
    directly with any location descendant in the retained subtree when the
    survivor crosses the boundary."""
    db, photos = catalog
    # Retained parent is currently 'location', but the chooser retypes it
    # to 'general'; its descendant Trail (also location) has photos.
    kept = db.add_keyword('Park', kw_type='location')
    trail = db.add_keyword('Trail', parent_id=kept, kw_type='location')
    stray = db.add_keyword('Yard', kw_type='general')
    db.tag_photo(photos[0], trail)
    db.tag_photo(photos[1], stray)

    preview = preview_keyword_merge(db, [kept, stray], kept, {'type': 'general'})
    assert preview['resolved']['type'] == 'general'
    merge_keywords(db, [kept, stray], kept, preview['preview_token'],
                   {'type': 'general'})

    resynced = {r['photo_id'] for r in db.conn.execute(
        "SELECT photo_id FROM pending_changes WHERE change_type = 'location'")}
    # The photo tagged only on the location descendant of the retyped
    # survivor is included: the walk now stops at the retyped parent, so
    # its exported location chain shifted even though Trail itself did not
    # move.
    assert photos[0] in resynced
