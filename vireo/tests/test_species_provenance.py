"""Evidence belongs to a photo/species association and follows normal undo."""

import sqlite3

import pytest
from db import Database
from services import catalog_deletion, moves_merge, workspace_folders
from services.edit_history import redo_last_undo, undo_last_edit


def confirm(client, species, photos=(1,), **kwargs):
    response = client.post('/api/encounters/species', json={
        'species': species, 'photo_ids': list(photos), 'add': True, **kwargs,
    })
    assert response.status_code == 200, response.get_json()
    return response.get_json()


def assignments(client, pid=1):
    response = client.get(f'/api/photos/{pid}')
    assert response.status_code == 200
    return {a['species']: a for a in response.get_json()['species_assignments']}


def evidence(client, assignment, **body):
    return client.post(f"/api/photos/{assignment['photo_id']}/species/{assignment['keyword_id']}/provenance", json=body)


def test_mixed_species_evidence_reload_and_flags(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    db.photo_review.set_flag(1, 'flagged')
    db.commit()
    confirm(client, 'Robin', provenance={'method': 'direct_visual', 'reviewer': 'Reviewer',
                                      'evidence_source': 'Visible plumage'})
    confirm(client, 'Mallard', provenance={'method': 'adjacent_frames', 'supporting_photo_ids': [2],
                                        'reason': 'Same subject in the next frame'})
    actual = assignments(client)
    assert actual['Robin']['method'] == 'direct_visual'
    assert actual['Mallard']['method'] == 'adjacent_frames'
    assert actual['Mallard']['supporting_photo_ids'] == [2]
    assert actual['Mallard']['confidence'] is None
    assert actual['Mallard']['confidence_meaning'] == 'Not recorded'
    assert client.get('/api/photos/1').get_json()['flag'] == 'flagged'
    with Database(db._db_path) as reopened:
        reloaded = {a['species']: a for a in reopened.species_provenance.assignments(1)}
    assert reloaded == actual
    item = db.conn.execute('SELECT species_provenance FROM edit_history_items ORDER BY id DESC LIMIT 1').fetchone()
    assert 'adjacent_frames' in item[0]


def test_existing_manual_and_imports_are_unspecified(app_and_db):
    app, db = app_and_db
    manual = db.add_keyword('Manual bird', is_species=True)
    imported = db.add_keyword('Imported bird', is_species=True)
    db.tag_photo(1, manual)
    db.tag_photo(1, imported, source=None)
    db.commit()
    result = assignments(app.test_client())
    assert result['Manual bird']['method'] == 'manual_unspecified'
    assert result['Imported bird']['method'] == 'imported_unknown'


def test_update_existing_evidence_undo_redo_and_audit(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    confirm(client, 'Robin')
    original = assignments(client)['Robin']
    response = evidence(client, original, method='adjacent_frames', reviewer='Test reviewer',
                        reason='Same bird beside the branch', supporting_photo_ids=[2])
    assert response.status_code == 200
    updated = assignments(client)['Robin']
    assert updated['confidence'] is None
    entry = db.edit_history.list_recent()[0]
    assert entry['source'] == 'Test reviewer'
    assert entry['reason'] == 'Same bird beside the branch'
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Robin'] == original
    assert client.post('/api/redo').status_code == 200
    assert assignments(client)['Robin'] == updated


def test_replace_and_remove_preserve_provenance_on_undo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    confirm(client, 'Robin', provenance={'method': 'adjacent_frames', 'reason': 'Adjacent subject'})
    original = assignments(client)['Robin']
    from test_app import _seed_encounter_cache
    _seed_encounter_cache(app, db, [1], confirmed_species='Robin')
    response = client.post('/api/encounters/species', json={
        'species': 'Mallard', 'photo_ids': [1], 'previous_species': 'Robin',
        'provenance': {'method': 'direct_visual', 'reviewer': 'Human'},
    })
    assert response.status_code == 200, response.get_json()
    assert 'Robin' not in assignments(client)
    new = assignments(client)['Mallard']
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Robin'] == original
    assert client.post('/api/redo').status_code == 200
    assert assignments(client)['Mallard'] == new
    response = client.delete(f"/api/photos/1/keywords/{new['keyword_id']}")
    assert response.status_code == 200, response.get_json()
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Mallard'] == new


def test_mixed_new_and_existing_confirmation_undo(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    confirm(client, 'Robin')
    confirm(client, 'Robin', photos=(1, 2), provenance={'method': 'adjacent_frames', 'reason': 'Same bird'})
    assert assignments(client)['Robin']['method'] == 'adjacent_frames'
    assert assignments(client, 2)['Robin']['method'] == 'adjacent_frames'
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Robin']['method'] == 'manual_unspecified'
    assert 'Robin' not in assignments(client, 2)
    assert client.post('/api/redo').status_code == 200
    assert assignments(client)['Robin']['method'] == 'adjacent_frames'
    assert assignments(client, 2)['Robin']['method'] == 'adjacent_frames'


@pytest.mark.parametrize('value', [
    {}, {'method': 'bogus'}, {'method': 'adjacent_frames'},
    {'method': 'direct_visual', 'confidence': 0.8},
    {'method': 'direct_visual', 'confidence': 0.8, 'confidence_meaning': 'Not recorded'},
    {'method': 'model_proposed', 'confidence': 10**400, 'confidence_meaning': 'Invalid score'},
    {'method': 'model_proposed', 'confidence': True, 'confidence_meaning': 'Model score'},
    {'method': 'model_proposed', 'confidence': float('nan'), 'confidence_meaning': 'Model score'},
    {'method': 'direct_visual', 'supporting_photo_ids': [True]},
    {'method': 'adjacent_frames', 'supporting_photo_ids': [99999]},
    {'method': 'direct_visual', 'reviewer': 12},
    {'method': 'direct_visual', 'detection_id': 99999},
])
def test_invalid_evidence_is_atomic(app_and_db, value):
    app, db = app_and_db
    client = app.test_client()
    before = db.edit_history.list_recent()
    response = client.post('/api/encounters/species', json={
        'species': 'Robin', 'photo_ids': [1], 'add': True, 'provenance': value,
    })
    assert response.status_code == 400, response.get_json()
    assert 'Robin' not in assignments(client)
    assert db.edit_history.list_recent() == before


def test_migration_keeps_historical_labels_unspecified(app_and_db):
    import schema
    app, db = app_and_db
    kid = db.add_keyword('Old manual bird', is_species=True)
    db.tag_photo(1, kid)
    db.commit()
    with sqlite3.connect(db._db_path) as conn:
        conn.execute('ALTER TABLE photo_keywords DROP COLUMN species_provenance')
        conn.execute('ALTER TABLE edit_history_items DROP COLUMN species_provenance')
        conn.execute('PRAGMA user_version=24')
    schema.ensure_schema(db._db_path)
    schema.ensure_schema(db._db_path)
    assert assignments(app.test_client())['Old manual bird']['method'] == 'manual_unspecified'


def test_subject_scoping_and_confidence_meaning(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    did = db.detections.save(1, [{'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5},
                                  'confidence': .9, 'category': 'animal'}], detector_model='test')[0]
    db.commit()
    confirm(client, 'Robin', provenance={'method': 'model_proposed', 'detection_id': did,
                                      'confidence': .8, 'confidence_meaning': 'Uncalibrated model similarity score',
                                      'evidence_source': 'test-model'})
    actual = assignments(client)['Robin']
    assert actual['detection_id'] == did
    assert actual['confidence'] == .8
    assert actual['confidence_meaning'] == 'Uncalibrated model similarity score'
    response = client.post('/api/encounters/species', json={
        'species': 'Robin', 'photo_ids': [2], 'add': True,
        'provenance': {'method': 'direct_visual', 'detection_id': did},
    })
    assert response.status_code == 400
    assert 'Robin' not in assignments(client, 2)


def test_unknown_source_survives_remove_and_undo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword('Imported bird', is_species=True)
    db.tag_photo(1, kid, source=None)
    db.commit()
    original = assignments(client)['Imported bird']
    assert client.delete(f'/api/photos/1/keywords/{kid}').status_code == 200
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Imported bird'] == original


def test_history_replays_after_reopening_catalog(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    confirm(client, 'Robin', provenance={'method': 'adjacent_frames', 'reason': 'Same bird'})
    original = assignments(client)['Robin']
    with Database(db._db_path) as reopened:
        undo_last_edit(reopened)
        assert not reopened.species_provenance.assignments(1)
    with Database(db._db_path) as reopened:
        redo_last_undo(reopened)
    assert assignments(client)['Robin'] == original


def test_same_named_distinct_species_do_not_share_evidence(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    # Imported homonyms may legitimately coexist; normal add canonicalizes names.
    first = db.conn.execute(
        "INSERT INTO keywords(name, is_species, type) VALUES ('Robin', 1, 'general')"
    ).lastrowid
    second = db.conn.execute(
        "INSERT INTO keywords(name, is_species, type) VALUES ('robin', 1, 'taxonomy')"
    ).lastrowid
    db.conn.execute("INSERT INTO taxa(id, name, rank) VALUES (101, 'First robin', 'species')")
    db.conn.execute("INSERT INTO taxa(id, name, rank) VALUES (102, 'Other robin', 'species')")
    db.conn.execute('UPDATE keywords SET taxon_id=101 WHERE id=?', (first,))
    db.conn.execute('UPDATE keywords SET taxon_id=102 WHERE id=?', (second,))
    db.tag_photo(1, first)
    db.tag_photo(1, second)
    db.commit()
    assignment = next(a for a in db.species_provenance.assignments(1) if a['keyword_id'] == first)
    assert evidence(client, assignment, method='direct_visual', reason='First subject markings').status_code == 200
    actual = {a['keyword_id']: a for a in db.species_provenance.assignments(1)}
    assert actual[first]['method'] == 'direct_visual'
    assert actual[second]['method'] == 'manual_unspecified'
    assert db.photo_species.equivalent_assignment_ids([1], first) == {1: [first]}


def test_keyword_merge_retargets_evidence_and_undo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    confirm(client, 'Robin', provenance={'method': 'direct_visual', 'reason': 'Visible markings'})
    source = assignments(client)['Robin']
    assert evidence(client, source, method='adjacent_frames', reason='Compared adjacent frames').status_code == 200
    survivor = db.add_keyword('Surviving Robin', is_species=True)
    db.tag_photo(1, survivor, source='accept')
    db.commit()
    db._merge_keyword_into(source['keyword_id'], survivor)
    db.commit()
    merged = assignments(client)['Surviving Robin']
    assert merged['method'] == 'adjacent_frames'
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Surviving Robin']['method'] == 'direct_visual'
    assert client.post('/api/redo').status_code == 200
    assert assignments(client)['Surviving Robin']['method'] == 'adjacent_frames'
    assert client.delete(f'/api/photos/1/keywords/{survivor}').status_code == 200
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Surviving Robin']['method'] == 'adjacent_frames'


def test_sidecar_reconciliation_does_not_leak_into_later_undo(app_and_db):
    from sync import _untag_keywords_missing_from_sidecar

    app, db = app_and_db
    kid = db.add_keyword('Sidecar species', is_species=True)
    db.tag_photo(1, kid, provenance={'method': 'adjacent_frames', 'reason': 'Original evidence'})
    db.commit()
    assert db._species_provenance_before == {}
    _untag_keywords_missing_from_sidecar(db, 1, db.photo_keywords.get_for_photo(1), {}, set(), set())
    db.commit()
    assert db._species_provenance_before == {}
    new = {'method': 'direct_visual', 'reason': 'New independent confirmation'}
    db.tag_photo(1, kid, provenance=new)
    db.commit()
    client = app.test_client()
    confirm(client, 'Other bird')
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Sidecar species']['method'] == 'direct_visual'
    assert assignments(client)['Sidecar species']['reason'] == new['reason']
    assert client.post('/api/redo').status_code == 200
    assert assignments(client)['Sidecar species']['reason'] == new['reason']


def test_non_undoable_species_removals_do_not_accumulate_snapshots(db):
    from services import photo_ingest

    fid = workspace_folders.add_folder(db, '/photos')
    pid = photo_ingest.add_photo(db, fid, 'bird.jpg', '.jpg', 100, 1.0)
    for index in range(100):
        kid = db.add_keyword(f'Species {index}', is_species=True)
        db.tag_photo(pid, kid, provenance={'method': 'direct_visual', 'reason': 'x' * 4000})
        db.untag_photo(pid, kid)
    db.commit()
    assert db._species_provenance_before == {}


@pytest.mark.parametrize('operation', ['pairing', 'folder_collision', 'duplicate_resolution',
                                       'pending_edits'])
@pytest.mark.parametrize('existing_evidence', [False, True])
def test_photo_association_moves_preserve_evidence(db, tmp_path, operation, existing_evidence):
    from services import photo_ingest

    source_dir, target_dir = tmp_path / 'source', tmp_path / 'target'
    source_dir.mkdir()
    target_dir.mkdir()
    source_folder = workspace_folders.add_folder(db, str(source_dir))
    target_folder = workspace_folders.add_folder(db, str(target_dir))
    losing = photo_ingest.add_photo(db, source_folder, 'bird.jpg', '.jpg', 100, 1.0)
    survivor = photo_ingest.add_photo(db, target_folder, 'bird.jpg', '.jpg', 100, 1.0)
    kid = db.add_keyword('Mallard', is_species=True)
    db.commit()
    original = {'method': 'adjacent_frames', 'reviewer': 'Field reviewer',
                'reason': 'Same bird across frames', 'evidence_source': 'Burst review',
                'supporting_photo_ids': [survivor], 'confidence': None,
                'confidence_meaning': 'No calibrated value'}
    db.tag_photo(losing, kid, provenance=original)
    db.photo_keywords.record_embedded_offered_keys(losing, ['mallard'])
    db.commit()
    if existing_evidence:
        expected = {'method': 'direct_visual', 'reason': 'Survivor markings'}
        db.tag_photo(survivor, kid, provenance=expected)
        db.commit()
    else:
        expected = original
    if operation == 'pairing':
        db.keyword_provenance.carry_companion_keywords(losing, survivor)
    elif operation == 'folder_collision':
        db.folders.merge_into_existing(source_folder, target_folder, str(target_dir))
        db.commit()
        assert db.photos.get(losing) is None
    elif operation == 'duplicate_resolution':
        db._apply_winner_loser_merge(survivor, [losing])
        assert db.photos.get(losing)['flag'] == 'rejected'
    else:
        db.queue_change(losing, 'keyword_add', 'Mallard')
        db.moves_merge.reconcile_conflicting_keyword_edits(losing, survivor)
    db.commit()
    copied = db.species_provenance.assignments(survivor)[0]
    assert {key: copied[key] for key in expected} == expected
    assert db._species_provenance_before == {}


def test_copied_subject_evidence_keeps_origin_and_remains_editable(app_and_db):
    app, db = app_and_db
    did = db.detections.save(1, [{'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5},
                                 'confidence': .9, 'category': 'animal'}], detector_model='test')[0]
    db.commit()
    kid = db.add_keyword('Mallard', is_species=True)
    db.tag_photo(1, kid, provenance={'method': 'direct_visual', 'detection_id': did,
                                  'reason': 'Visible markings'})
    db.commit()
    db.keyword_provenance.carry_companion_keywords(1, 2)
    db.commit()
    client = app.test_client()
    copied = assignments(client, 2)['Mallard']
    db.keyword_provenance.carry_companion_keywords(2, 3)
    db.commit()
    assert assignments(client, 3)['Mallard']['subject_photo_id'] == 1
    assert copied['detection_id'] == did
    assert copied['subject_photo_id'] == 1
    response = evidence(client, copied, method='direct_visual', detection_id=did,
                        subject_photo_id=1, reason='Added a source explanation')
    assert response.status_code == 200, response.get_json()
    assert assignments(client, 2)['Mallard']['subject_photo_id'] == 1
    for invalid in ({'detection_id': did, 'subject_photo_id': 3},
                    {'detection_id': True, 'subject_photo_id': 1}):
        assert evidence(client, copied, method='direct_visual', **invalid).status_code == 400


def test_copied_historical_subject_can_be_preserved_after_source_deletion(app_and_db):
    app, db = app_and_db
    did = db.detections.save(1, [{'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5},
                                 'confidence': .9, 'category': 'animal'}], detector_model='test')[0]
    db.commit()
    kid = db.add_keyword('Mallard', is_species=True)
    db.tag_photo(1, kid, provenance={'method': 'direct_visual', 'detection_id': did, 'supporting_photo_ids': [1]})
    db.commit()
    db.keyword_provenance.carry_companion_keywords(1, 2)
    catalog_deletion.delete_photos(db, [1])
    db.commit()
    client = app.test_client()
    copied = assignments(client, 2)['Mallard']
    response = evidence(client, copied, method='direct_visual', detection_id=did,
                        subject_photo_id=1, reason='Historical subject reference', supporting_photo_ids=[1])
    assert response.status_code == 200, response.get_json()
    assert assignments(client, 2)['Mallard']['detection_id'] == did


    assert assignments(client, 2)['Mallard']['supporting_photo_ids'] == [1]
    assert evidence(client, copied, method='direct_visual', detection_id=did,
                    subject_photo_id=1, supporting_photo_ids=[1, 99999]).status_code == 400


@pytest.mark.parametrize('same_hash', [False, True])
def test_archive_photo_merge_preserves_evidence(db, tmp_path, same_hash):
    from test_db_moves_merge import _collision_tree

    tree = _collision_tree(db, tmp_path, same_hash=same_hash)
    losing, survivor = ((tree['staged'], tree['survivor']) if same_hash
                        else (tree['survivor'], tree['staged']))
    kid = db.add_keyword('Mallard', is_species=True)
    db.commit()
    original = {'method': 'adjacent_frames', 'reviewer': 'Reviewer',
                'reason': 'Adjacent frame', 'supporting_photo_ids': [survivor],
                'confidence': None, 'confidence_meaning': 'No calibrated value'}
    db.tag_photo(losing, kid, provenance=original)
    db.photo_keywords.record_embedded_offered_keys(losing, ['mallard'])
    db.commit()
    moves_merge.merge_staged_tree_into_archive(db, tree['stage_root'], tree['arch'])
    copied = next(a for a in db.species_provenance.assignments(survivor) if a['keyword_id'] == kid)
    assert {key: copied[key] for key in original} == original
    assert db.photos.get(losing) is None


def test_duplicate_resolution_carries_species_evidence_from_loser(app_and_db):
    """The winner inherits a loser's species evidence when it has none itself."""
    app, db = app_and_db
    client = app.test_client()
    # Give photo 2 (soon-to-be-loser) a reviewer-recorded identification.
    confirm(client, 'Robin', photos=(2,), provenance={
        'method': 'direct_visual', 'reviewer': 'Reviewer',
        'reason': 'Hand-identified on the losing frame'})
    loser_robin = assignments(client, 2)['Robin']
    # Winner has no Robin yet; resolve 2 → 1 and verify the evidence rides along.
    db._apply_winner_loser_merge(1, [2], winner_flag=None)
    db.commit()
    winner_robin = assignments(client)['Robin']
    assert winner_robin['method'] == 'direct_visual'
    assert winner_robin['reviewer'] == 'Reviewer'
    assert winner_robin['reason'] == 'Hand-identified on the losing frame'
    # The loser's evidence survived the row rewrite onto the winner.
    assert winner_robin['keyword_id'] == loser_robin['keyword_id']


def test_duplicate_resolution_keeps_winner_evidence_over_loser(app_and_db):
    """A winner's own evidence is not overwritten by the loser's."""
    app, db = app_and_db
    client = app.test_client()
    confirm(client, 'Robin', photos=(1,), provenance={
        'method': 'direct_visual', 'reviewer': 'Winner reviewer',
        'reason': 'Original call'})
    confirm(client, 'Robin', photos=(2,), provenance={
        'method': 'adjacent_frames', 'reviewer': 'Loser reviewer',
        'reason': 'Adjacent inference'})
    db._apply_winner_loser_merge(1, [2], winner_flag=None)
    db.commit()
    winner_robin = assignments(client)['Robin']
    assert winner_robin['method'] == 'direct_visual'
    assert winner_robin['reviewer'] == 'Winner reviewer'
    assert winner_robin['reason'] == 'Original call'


def test_shared_upsert_folds_species_provenance_in_sql(app_and_db):
    """The shared KEYWORD_SOURCE_CONFLICT_SQL clause carries the loser's evidence
    onto a surviving row that has none, and keeps the survivor's own when both have one."""
    import json

    import db as db_mod

    app, db = app_and_db
    kid = db.add_keyword('Robin', is_species=True)
    db.commit()
    loser_payload = json.dumps({'method': 'direct_visual', 'reviewer': 'Loser'}, sort_keys=True)
    # First row: survivor without evidence, loser incoming with evidence.
    db.conn.execute("INSERT INTO photo_keywords (photo_id, keyword_id, source) VALUES (?, ?, 'manual')",
                    (1, kid))
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id, source, species_provenance) "
        "VALUES (?, ?, 'manual', ?) " + db_mod.KEYWORD_SOURCE_CONFLICT_SQL,
        (1, kid, loser_payload),
    )
    actual = db.conn.execute(
        "SELECT species_provenance FROM photo_keywords WHERE photo_id=? AND keyword_id=?",
        (1, kid),
    ).fetchone()[0]
    assert actual == loser_payload
    # Second case: survivor already has its own evidence, incoming must not replace it.
    survivor_payload = json.dumps({'method': 'adjacent_frames', 'reviewer': 'Survivor'}, sort_keys=True)
    db.conn.execute(
        "UPDATE photo_keywords SET species_provenance=? WHERE photo_id=? AND keyword_id=?",
        (survivor_payload, 1, kid),
    )
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id, source, species_provenance) "
        "VALUES (?, ?, 'manual', ?) " + db_mod.KEYWORD_SOURCE_CONFLICT_SQL,
        (1, kid, loser_payload),
    )
    actual = db.conn.execute(
        "SELECT species_provenance FROM photo_keywords WHERE photo_id=? AND keyword_id=?",
        (1, kid),
    ).fetchone()[0]
    assert actual == survivor_payload


def test_sync_untag_does_not_leak_into_later_record_edit(app_and_db, tmp_path, monkeypatch):
    """A sync-triggered untag must not attach stale evidence to a later edit."""
    import os

    import sync as sync_mod

    app, db = app_and_db
    client = app.test_client()
    confirm(client, 'Robin', photos=(1,), provenance={
        'method': 'direct_visual', 'reviewer': 'Original reviewer',
        'reason': 'Recorded before reconcile'})
    assert db._species_provenance_before == {}
    # Simulate an XMP reconcile that drops the Robin keyword by writing an
    # empty-keywords sidecar beside photo 1 and running sync_from_xmp.
    folder_row = db.folders.get(db.photos.get(1)['folder_id'])
    folder_path = str(tmp_path / 'photos_folder')
    os.makedirs(folder_path, exist_ok=True)
    db.conn.execute("UPDATE folders SET path=? WHERE id=?", (folder_path, folder_row['id']))
    db.commit()
    xmp_path = os.path.join(folder_path, 'bird1.xmp')
    with open(xmp_path, 'w') as handle:
        handle.write(
            '<?xpacket begin="" id="W5M0MpCehiHzreSzNTczkc9d"?>'
            '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
            '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
            '<rdf:Description xmlns:dc="http://purl.org/dc/elements/1.1/">'
            '<dc:subject><rdf:Bag/></dc:subject>'
            '</rdf:Description></rdf:RDF></x:xmpmeta>'
            '<?xpacket end="w"?>'
        )
    sync_mod.sync_from_xmp(db, [1])
    # After a reconcile commit, no snapshot may linger on the Database.
    assert db._species_provenance_before == {}
    # A later unrelated species edit on the same photo must not pick up the
    # pre-reconcile Robin state; its edit-history row carries only its own
    # before/after values.
    confirm(client, 'Mallard', photos=(1,), provenance={
        'method': 'direct_visual', 'reviewer': 'Later reviewer', 'reason': 'New call'})
    newest = db.conn.execute(
        "SELECT species_provenance FROM edit_history_items ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]
    assert newest is not None
    assert 'Original reviewer' not in newest
    assert 'Recorded before reconcile' not in newest


def test_intermediate_commit_does_not_discard_an_explicit_undo_snapshot(app_and_db):
    app, db = app_and_db
    kid = db.add_keyword('Robin', is_species=True)
    db.tag_photo(1, kid, source='accept', provenance={'method': 'direct_visual', 'reason': 'Original'})
    db.commit()
    before = db.species_provenance.state(1, kid)
    db.untag_photo(1, kid, _record_history=True)
    db.commit()
    assert db._species_provenance_before
    db.record_edit('keyword_remove', 'Removed Robin', str(kid),
                   [{'photo_id': 1, 'old_value': '', 'new_value': str(kid)}])
    undo_last_edit(db)
    assert db.species_provenance.state(1, kid) == before


def test_bulk_species_additions_preserve_existing_evidence_through_undo(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    confirm(client, 'Robin', provenance={'method': 'adjacent_frames', 'reason': 'Earlier burst review'})
    original = assignments(client)['Robin']
    response = client.post('/api/encounters/species/bulk', json={'assignments': [
        {'species': 'Mallard', 'photo_ids': [1, 2]},
        {'species': 'Blue Jay', 'photo_ids': [1]},
    ]})
    assert response.status_code == 200, response.get_json()
    assert assignments(client)['Robin'] == original
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Robin'] == original
    assert 'Mallard' not in assignments(client)
    assert client.post('/api/redo').status_code == 200
    assert assignments(client)['Robin'] == original
    assert assignments(client)['Mallard']['method'] == 'manual_unspecified'


@pytest.mark.parametrize('direction', ['undo', 'redo'])
@pytest.mark.parametrize('failure', ['stale', 'database'])
def test_failed_identity_replay_cannot_contaminate_a_later_evidence_edit(app_and_db, monkeypatch, direction, failure):
    from repositories.identity_alternatives import IdentityAlternativesRepository
    from services.identity_alternatives import IdentityHistoryStale, resolve_alternatives, save_alternatives

    app, db = app_and_db
    candidate, other = 'Tropical Kingbird', "Couch's Kingbird"
    kid = db.add_keyword(candidate, is_species=True)
    db.commit()
    old_evidence = {'method': 'direct_visual', 'reason': 'Old evidence'}
    if direction == 'redo':
        for pid in (1, 2):
            db.tag_photo(pid, kid, provenance=old_evidence)
            db.commit()
    save_alternatives(db, [1, 2], [candidate, other], 'Need calls', 'Audio')
    db.commit()
    if direction == 'redo':
        undo_last_edit(db)
    else:
        resolve_alternatives(db, [1, 2], candidate)
        db.commit()
        db.tag_photo(1, kid, provenance=old_evidence)
        db.commit()
    if failure == 'stale':
        db.tag_photo(2, db.add_keyword(other, is_species=True))
        db.commit()
        expected_error = IdentityHistoryStale
    else:
        original = IdentityAlternativesRepository.set_state
        def fail_second(self, photo_id, state):
            if photo_id == 2:
                raise sqlite3.OperationalError('Injected replay failure')
            return original(self, photo_id, state)
        monkeypatch.setattr(IdentityAlternativesRepository, 'set_state', fail_second)
        expected_error = sqlite3.OperationalError
    db.commit()
    before = db.species_provenance.state(1, kid)
    with pytest.raises(expected_error):
        ((lambda: undo_last_edit(db)) if direction == 'undo' else (lambda: redo_last_undo(db)))()
    assert db.species_provenance.state(1, kid) == before
    assert db._species_provenance_before == {}
    # The evidence-only endpoint uses another connection; the later keyword
    # action reuses the Database that just experienced the failed replay.
    client = app.test_client()
    current = assignments(client)[candidate]
    assert evidence(client, current, method='direct_visual', reason='New evidence').status_code == 200
    independent = db.add_keyword('Independent species', is_species=True)
    db.tag_photo(1, independent)
    db.commit()
    db.record_edit('keyword_add', 'Independent edit', str(independent), [
        {'photo_id': 1, 'old_value': '', 'new_value': str(independent)},
    ])
    undo_last_edit(db)
    assert assignments(client)[candidate]['reason'] == 'New evidence'
