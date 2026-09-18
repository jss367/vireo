import pytest
from db import Database
from keyword_identity import grouped_keywords, location_candidates, reconcile_location


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
