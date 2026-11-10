"""Reported plover/pipe patterns remain reviewable without inventing certainty."""

import sqlite3

import pytest
import schema
from canonical_schema import SCHEMA_VERSION
from db import Database


def detection(db, photo_id, score=0.9):
    return db.detections.save(photo_id, [{
        'box': {'x': .1, 'y': .2, 'w': .3, 'h': .4},
        'confidence': score, 'category': 'animal',
    }], detector_model='test-detector')[0]


def add(db, det, species, score, model='BioCLIP-2.5', **kwargs):
    db.predictions.add(det, species, score, model, **kwargs)
    db.commit()


def evidence_rows(app, **params):
    response = app.test_client().get('/api/predictions', query_string={'evidence': '1', **params})
    assert response.status_code == 200
    return response.get_json()['predictions']


def save(app, db, det, decision='not_enough_evidence', revision=0, **kwargs):
    return app.test_client().post(f'/api/detections/{det}/evidence-review', json={
        'decision': decision, 'observed_revision': revision,
        'observed_workspace_id': db.require_workspace_id(), **kwargs,
    })


def test_reported_plover_and_pipe_patterns_are_questionable(app_and_db):
    app, db = app_and_db
    photos = db.photos.list_page()
    plover, pipe = [detection(db, p['id']) for p in photos[:2]]
    # These reproduce reported score/name patterns, not an independent image assessment.
    db.conn.execute('UPDATE photos SET filename=? WHERE id=?', ('DSC_2542.NEF', photos[0]['id']))
    db.conn.execute('UPDATE photos SET filename=? WHERE id=?', ('DSC_3059.NEF', photos[1]['id']))
    add(db, plover, 'American Golden-Plover', .9994)
    add(db, plover, 'American Golden-Plover', .3651, 'reported second classifier (unidentified)')
    add(db, pipe, 'Common Poorwill', .8)
    add(db, pipe, 'Great Barracuda', .6, 'another classifier')
    rows = evidence_rows(app)
    plover_row = next(p for p in rows if p['detection_id'] == plover and p['model'] == 'BioCLIP-2.5')
    e = plover_row['evidence']
    assert not e['score']['calibrated_correctness']
    assert e['score']['kind'] == 'candidate_relative_softmax'
    assert e['score']['value'] == .9994
    assert 'not a probability' in e['score']['explanation']
    assert not e['disagreement']  # differing scores alone are not model disagreement
    assert any('High relative score' in r for r in e['reasons'])
    assert {p['species'] for p in e['model_candidates']} == {'American Golden-Plover'}
    pipe_row = next(p for p in rows if p['detection_id'] == pipe)
    assert pipe_row['evidence']['disagreement']
    assert any('Models propose different species' in r for r in pipe_row['evidence']['reasons'])
    assert save(app, db, pipe, 'invalid_subject').status_code == 200
    assert all(p['evidence']['subject']['decision'] == 'invalid_subject'
               for p in evidence_rows(app) if p['detection_id'] == pipe)


def test_disagreement_is_per_region_and_survives_row_filters(app_and_db):
    app, db = app_and_db
    photo_id = db.photos.list_page()[0]['id']
    first = detection(db, photo_id)
    # Same photo, different localized animal; model names alone cannot conflate them.
    second = db.detections.save(photo_id, [{
        'box': {'x': .6, 'y': .2, 'w': .2, 'h': .3}, 'confidence': .8, 'category': 'animal',
    }], detector_model='other-detector')[0]
    db.commit()
    add(db, first, 'Herring Gull', .8, 'one')
    add(db, second, 'Ring-billed Gull', .9, 'two')
    assert all(not p['evidence']['disagreement'] for p in evidence_rows(app))
    add(db, first, 'California Gull', .7, 'two')
    # Row-level rules hide model two; evidence still names its disagreement.
    rows = evidence_rows(app, rules='[{"field":"prediction_confidence","op":">=","value":0.75}]')
    row = next(p for p in rows if p['detection_id'] == first)
    assert row['evidence']['disagreement']
    assert {c['species'] for c in row['evidence']['model_candidates']} == {'Herring Gull', 'California Gull'}


def test_raw_match_uses_exact_run_and_model_scale(app_and_db):
    app, db = app_and_db
    det = detection(db, db.photos.list_page()[0]['id'])
    add(db, det, 'Old species', .9999, labels_fingerprint='old', match_score=.9)
    db.model_runs.record_classifier_match_score(det, 'BioCLIP-2.5', 'old', .9, score_kind='cosine')
    db.commit()
    add(db, det, 'Current species', .99, labels_fingerprint='new', match_score=.12)
    db.model_runs.record_classifier_match_score(det, 'BioCLIP-2.5', 'new', .12, label_count=100, score_kind='cosine')
    db.commit()
    db.workspaces.update(db.require_workspace_id(), config_overrides={
        'match_thresholds': {'BioCLIP-2.5': {'threshold': .3, 'score_kind': 'cosine'}},
    })
    db.commit()
    row = evidence_rows(app)[0]
    assert row['species'] == 'Current species'
    assert row['evidence']['score']['raw_value'] == .12
    assert row['evidence']['score']['raw_kind'] == 'cosine'
    assert row['evidence']['score']['label_count'] == 100
    assert row['evidence']['match']['state'] == 'unlisted'
    assert row['evidence']['score']['calibrated_correctness'] is False


def test_location_is_honest_context_and_never_an_exclusion(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    det = detection(db, pid)
    add(db, det, 'American Golden-Plover', .9994)
    assert evidence_rows(app)[0]['evidence']['geography'] == {
        'state': 'unknown', 'explanation': 'Geographic plausibility unknown: no usable catalog GPS.',
    }
    db.conn.execute('UPDATE photos SET latitude=0, longitude=0 WHERE id=?', (pid,))
    db.commit()
    geo = evidence_rows(app)[0]['evidence']['geography']
    assert geo['state'] == 'unknown' and geo['latitude'] == geo['longitude'] == 0
    assert 'Catalog GPS' in geo['source']
    assert 'missing sightings do not exclude' in geo['explanation']
    assert 'taxon_name=American+Golden-Plover' in geo['observations_url']


@pytest.mark.parametrize('decision', ['not_enough_evidence', 'invalid_subject', 'subject_valid', 'unreviewed'])
def test_assessments_preserve_predictions_tags_flags_and_can_be_revisited(app_and_db, decision):
    app, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    det = detection(db, pid)
    add(db, det, 'Common Poorwill', .99)
    db.photo_review.set_flag(pid, 'flagged')
    db.commit()
    before = [tuple(row) for row in db.conn.execute('SELECT * FROM predictions')]
    keywords = [tuple(row) for row in db.photo_keywords.get_for_photo(pid)]
    changes = [tuple(row) for row in db.conn.execute('SELECT * FROM pending_changes')]
    response = save(app, db, det, decision)
    assert response.status_code == 200
    assert response.get_json() == {'ok': True, 'decision': decision, 'revision': 1}
    assert db.photos.get(pid)['flag'] == 'flagged'
    assert [tuple(row) for row in db.photo_keywords.get_for_photo(pid)] == keywords
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM predictions')] == before
    assert db.predictions.get_rows()[0]['status'] == 'pending'
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM pending_changes')] == changes
    # New request/connection reads the durable workspace-local decision.
    assert evidence_rows(app)[0]['evidence']['subject']['decision'] == decision
    assert save(app, db, det, 'unreviewed', revision=1).status_code == 200
    assert evidence_rows(app)[0]['evidence']['subject']['decision'] == 'unreviewed'


def test_stale_or_foreign_assessment_cannot_overwrite_review(app_and_db):
    app, db = app_and_db
    det = detection(db, db.photos.list_page()[0]['id'])
    add(db, det, 'Common Poorwill', .99)
    assert save(app, db, det).status_code == 200
    assert save(app, db, det, 'subject_valid').status_code == 409
    assert save(app, db, det, revision=1, observed_workspace_id=-1).status_code == 409
    assert save(app, db, det + 9999).status_code == 404
    assert save(app, db, det, revision=True).status_code == 400
    assert save(app, db, det, 'made_up').status_code == 400
    assert evidence_rows(app)[0]['evidence']['subject']['decision'] == 'not_enough_evidence'


def test_assessments_are_workspace_local_and_cascade_with_detection(app_and_db):
    app, db = app_and_db
    photo = db.photos.list_page()[0]
    det = detection(db, photo['id'])
    add(db, det, 'Common Poorwill', .99)
    assert save(app, db, det).status_code == 200
    other_ws = db.create_workspace('Other review')
    db.commit()
    assert app.test_client().post(f'/api/workspaces/{other_ws}/activate').status_code == 200
    db.set_active_workspace(other_ws)
    assert save(app, db, det, observed_workspace_id=other_ws).status_code == 404
    db.workspace_folders.add_exact(other_ws, photo['folder_id'])
    db.commit()
    assert evidence_rows(app)[0]['evidence']['subject']['decision'] == 'unreviewed'
    assert save(app, db, det, 'subject_valid').status_code == 200
    assert db.conn.execute('SELECT COUNT(*) FROM detection_evidence_reviews').fetchone()[0] == 2
    db.conn.execute('DELETE FROM detections WHERE id=?', (det,))
    assert not db.conn.execute('SELECT * FROM detection_evidence_reviews').fetchall()


def test_evidence_lookup_covers_exactly_the_workspace_photos(app_and_db):
    """A photo granted on its own is in scope; one the workspace cannot see is not."""
    _, db = app_and_db
    granted, unseen = db.photos.list_page()[:2]
    ids = {}
    for name, photo in (('granted', granted), ('unseen', unseen)):
        det = detection(db, photo['id'])
        add(db, det, 'Common Poorwill', .9)
        ids[name] = db.conn.execute(
            'SELECT id FROM predictions WHERE detection_id = ?', (det,),
        ).fetchone()[0]
    other_ws = db.create_workspace('Other review')
    db.photo_visibility.grant(other_ws, [granted['id']])
    db.commit()
    assert set(db.predictions.evidence_context(list(ids.values()))) == set(ids.values())
    db.set_active_workspace(other_ws)
    assert set(db.predictions.evidence_context(list(ids.values()))) == {ids['granted']}


def test_evidence_lookup_reads_by_prediction_id_not_by_workspace(app_and_db):
    """Each chunk costs its own rows, however many photos the workspace holds.

    Review hands this the whole queue in chunks. A plan that walks the
    workspace's photos, or builds the visibility view, repeats that work for
    every chunk: a minute on an 80,000-photo workspace.
    """
    _, db = app_and_db
    det = detection(db, db.photos.list_page()[0]['id'])
    add(db, det, 'Common Poorwill', .9)
    prediction_id = db.conn.execute('SELECT id FROM predictions').fetchone()[0]
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        assert prediction_id in db.predictions.evidence_context([prediction_id])
    finally:
        db.conn.set_trace_callback(None)
    (lookup,) = [s for s in statements if 'FROM predictions pr' in s]
    plan = [row[3] for row in db.conn.execute('EXPLAIN QUERY PLAN ' + lookup)]
    assert plan[0] == 'SEARCH pr USING INTEGER PRIMARY KEY (rowid=?)'
    assert not [step for step in plan if 'CO-ROUTINE' in step or step.startswith('SCAN')]


def test_version_24_migration_adds_review_state_idempotently(tmp_path):
    path = str(tmp_path / 'previous.db')
    with Database(path):
        pass
    with sqlite3.connect(path) as conn:
        conn.execute('DROP TABLE detection_evidence_reviews')
        conn.execute('PRAGMA user_version=24')
        schema._apply_pending(conn)
        schema._apply_pending(conn)
        assert conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        assert not conn.execute('SELECT * FROM detection_evidence_reviews').fetchall()


def test_stale_accept_cannot_confirm_deferred_subject_or_hidden_burst_member(app_and_db):
    app, db = app_and_db
    photos = db.photos.list_page()
    det1, det2 = [detection(db, p['id']) for p in photos[:2]]
    for det in (det1, det2):
        add(db, det, 'Common Poorwill', .99, group_id='uncertain-burst')
    pred1 = db.conn.execute('SELECT id FROM predictions WHERE detection_id=?', (det1,)).fetchone()[0]
    assert save(app, db, det2).status_code == 200
    before = [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords')]
    # The clicked row itself is not deferred. Its group expansion is.
    response = app.test_client().post(f'/api/predictions/{pred1}/accept')
    assert response.status_code == 409
    assert response.get_json()['code'] == 'identification_deferred'
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords')] == before
    assert all(row['status'] == 'pending' for row in db.predictions.get_rows())
    assert save(app, db, det2, 'unreviewed', revision=1).status_code == 200
    assert app.test_client().post(f'/api/predictions/{pred1}/accept').status_code == 200


def test_bulk_accept_rolls_back_earlier_tags_when_a_target_is_deferred(app_and_db):
    app, db = app_and_db
    photos = db.photos.list_page()
    det1, det2 = [detection(db, p['id']) for p in photos[:2]]
    for det in (det1, det2):
        add(db, det, 'Common Poorwill', .99)
    # Isolate the deferral guard from the independent species-keyword conflict guard.
    db.conn.execute('DELETE FROM photo_keywords')
    db.commit()
    ids = [row['id'] for row in db.predictions.get_rows()]
    deferred_det = db.conn.execute('SELECT detection_id FROM predictions WHERE id=?', (ids[-1],)).fetchone()[0]
    assert save(app, db, deferred_det, 'invalid_subject').status_code == 200
    response = app.test_client().post('/api/predictions/batch-accept', json={'prediction_ids': ids})
    assert response.status_code == 409
    assert not db.conn.execute('SELECT * FROM photo_keywords').fetchall()
    assert all(row['status'] == 'pending' for row in db.predictions.get_rows())


def test_group_apply_cannot_tag_or_flag_deferred_subject(app_and_db):
    app, db = app_and_db
    photo = db.photos.list_page()[0]
    det = detection(db, photo['id'])
    add(db, det, 'Common Poorwill', .99)
    assert save(app, db, det).status_code == 200
    before = [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords')]
    response = app.test_client().post('/api/predictions/group/apply', json={
        'picks': [photo['id']], 'rejects': [], 'removed': [], 'species': 'Common Poorwill',
    })
    assert response.status_code == 409
    assert db.photos.get(photo['id'])['flag'] == photo['flag']
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords')] == before
    assert db.predictions.get_rows()[0]['status'] == 'pending'


def test_background_like_region_is_named_in_the_evidence(app_and_db):
    """A box the classifier scored like bark carries a background verdict
    and a review reason; a clean subject carries the opposite verdict; a
    run that never scored the prompts is marked unmeasured."""
    app, db = app_and_db
    photo_id = db.photos.list_page()[0]['id']
    # Three boxes in one save: a detection's id hashes its box, and a save
    # replaces the photo's rows for the detector.
    trunk, hawk, old, kinglet = db.detections.save(photo_id, [
        {'box': {'x': 0.0, 'y': 0.0, 'w': 0.4, 'h': 1.0}, 'confidence': .34, 'category': 'animal'},
        {'box': {'x': 0.4, 'y': 0.3, 'w': 0.3, 'h': 0.7}, 'confidence': .49, 'category': 'animal'},
        {'box': {'x': 0.7, 'y': 0.3, 'w': 0.2, 'h': 0.2}, 'confidence': .6, 'category': 'animal'},
        {'box': {'x': 0.7, 'y': 0.6, 'w': 0.3, 'h': 0.4}, 'confidence': .87, 'category': 'animal'},
    ], detector_model='test-detector')
    # A small bird in tamarisk: the prompt gap is inside the margin, but the
    # detector was confident, so the verdict trusts the box.
    add(db, kinglet, 'Ruby-crowned Kinglet', .9, match_score=.49)
    db.model_runs.record_classifier_match_score(
        kinglet, 'BioCLIP-2.5', 'legacy', .49, score_kind='cosine',
        background_score=.35, background_prompt='tamarisk')
    add(db, trunk, 'Elf Owl', .88, match_score=.41)
    db.model_runs.record_classifier_match_score(
        trunk, 'BioCLIP-2.5', 'legacy', .41, score_kind='cosine',
        background_score=.27, background_prompt='leaves')
    add(db, hawk, 'Red-shouldered Hawk', .52, match_score=.49)
    db.model_runs.record_classifier_match_score(
        hawk, 'BioCLIP-2.5', 'legacy', .49, score_kind='cosine',
        background_score=.09, background_prompt='twigs')
    add(db, old, 'Verdin', .7, match_score=.4)
    db.model_runs.record_classifier_match_score(old, 'BioCLIP-2.5', 'legacy', .4, score_kind='cosine')
    db.commit()
    by_species = {row['species']: row['evidence'] for row in evidence_rows(app)}
    owl = by_species['Elf Owl']
    assert owl['background']['state'] == 'background'
    assert owl['background']['background_prompt'] == 'leaves'
    assert any('background' in reason and 'leaves' in reason for reason in owl['reasons'])
    assert by_species['Red-shouldered Hawk']['background']['state'] == 'subject'
    assert not any('background' in reason for reason in by_species['Red-shouldered Hawk']['reasons'])
    assert by_species['Verdin']['background']['state'] == 'unavailable'
    bird = by_species['Ruby-crowned Kinglet']
    assert bird['background']['state'] == 'subject'
    assert bird['background']['detector_confidence'] == pytest.approx(.87)
    assert 'detector was 0.87 sure' in bird['background']['explanation']
    assert not any('background' in reason for reason in bird['reasons'])


@pytest.mark.parametrize('starting_version', [32, 33])
def test_background_migration_adds_columns_idempotently_from_either_v33_layout(
    tmp_path, starting_version,
):
    """Background columns land whether the catalog was stamped 33 by the sibling
    migration (missing both column sets) or came up through 32.

    The reverse — a pre-fix catalog stamped 33 by this branch where the
    background columns already exist — is covered by
    ``test_background_migration_is_a_noop_when_columns_already_exist``.
    """
    path = str(tmp_path / f'previous-v{starting_version}.db')
    with Database(path):
        pass
    with sqlite3.connect(path) as conn:
        conn.execute('ALTER TABLE classifier_match_scores DROP COLUMN background_score')
        conn.execute('ALTER TABLE classifier_match_scores DROP COLUMN background_prompt')
        conn.execute(f'PRAGMA user_version={starting_version}')
        schema._apply_pending(conn)
        schema._apply_pending(conn)
        assert conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        columns = {row[1] for row in conn.execute('PRAGMA table_info(classifier_match_scores)')}
        assert {'background_score', 'background_prompt'} <= columns


def test_background_migration_is_a_noop_when_columns_already_exist(tmp_path):
    path = str(tmp_path / 'pre-fix-v33.db')
    with Database(path):
        pass
    with sqlite3.connect(path) as conn:
        conn.execute('PRAGMA user_version=33')
        schema._apply_pending(conn)
        assert conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        columns = {row[1] for row in conn.execute('PRAGMA table_info(classifier_match_scores)')}
        assert {'background_score', 'background_prompt'} <= columns


def test_background_migration_repairs_alternate_v33_missing_keeper_columns(tmp_path):
    """A dev catalog stamped v33 by this branch before the restack has the
    background columns but no keeper columns, and ``_apply_pending`` would
    skip migration 33 on it. Migration 34 must idempotently repair the
    keeper columns too, preserving any existing rows."""
    path = str(tmp_path / 'alternate-v33.db')
    with Database(path):
        pass
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO current_flag_attribution "
            "(workspace_id, photo_id, flag, source, reason, description, created_at) "
            "VALUES (1, 1, 'rejected', 'cull', 'duplicate', 'kept sibling', '2026-01-01')"
        )
        conn.execute('ALTER TABLE current_flag_attribution DROP COLUMN keeper_photo_id')
        conn.execute('ALTER TABLE current_flag_attribution DROP COLUMN comparison')
        conn.execute('ALTER TABLE current_flag_attribution DROP COLUMN keeper_deleted_at')
        conn.execute('PRAGMA user_version=33')
        conn.commit()
        schema._apply_pending(conn)
        schema._apply_pending(conn)
        assert conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        attribution = {row[1] for row in conn.execute('PRAGMA table_info(current_flag_attribution)')}
        assert {'keeper_photo_id', 'comparison', 'keeper_deleted_at'} <= attribution
        background = {row[1] for row in conn.execute('PRAGMA table_info(classifier_match_scores)')}
        assert {'background_score', 'background_prompt'} <= background
        preserved = conn.execute(
            "SELECT reason, description FROM current_flag_attribution WHERE photo_id = 1"
        ).fetchone()
        assert preserved == ('duplicate', 'kept sibling')
