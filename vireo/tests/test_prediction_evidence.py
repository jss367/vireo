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
    add(db, det, 'Current species', .99, labels_fingerprint='new', match_score=.12)
    db.model_runs.record_classifier_match_score(det, 'BioCLIP-2.5', 'new', .12, label_count=100, score_kind='cosine')
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
    assert app.test_client().post(f'/api/workspaces/{other_ws}/activate').status_code == 200
    db.set_active_workspace(other_ws)
    assert save(app, db, det, observed_workspace_id=other_ws).status_code == 404
    db.workspace_folders.add_exact(other_ws, photo['folder_id'])
    assert evidence_rows(app)[0]['evidence']['subject']['decision'] == 'unreviewed'
    assert save(app, db, det, 'subject_valid').status_code == 200
    assert db.conn.execute('SELECT COUNT(*) FROM detection_evidence_reviews').fetchone()[0] == 2
    db.conn.execute('DELETE FROM detections WHERE id=?', (det,))
    assert not db.conn.execute('SELECT * FROM detection_evidence_reviews').fetchall()


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
