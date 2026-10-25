"""Cross-model Review cards and their exact, reversible decision scope."""

import pytest

BLUE = {"taxon_id": 138690, "scientific_name": "Cyanistes caeruleus"}


def _seed(db):
    photos = [p['id'] for p in db.photos.list_page()]
    db.conn.execute(
        "INSERT INTO taxa (inat_id, name, common_name, rank) VALUES (?, ?, ?, 'species')",
        (BLUE['taxon_id'], BLUE['scientific_name'], 'Blue Tit'),
    )
    detections = [db.detections.save(pid, [{
        'box': {'x': .1, 'y': .1, 'w': .3, 'h': .4}, 'confidence': .9,
        'category': 'animal',
    }], detector_model='MDV6')[0] for pid in photos]
    ids = []
    for model, name, indexes, group in [
        ('BioCLIP', 'Eurasian Blue Tit', (0, 1), 'bio'),
        ('iNat21', 'Blue Tit', (1, 2), 'inat'),
        ('third-model', 'Blue Tit', (2,), None),
    ]:
        for index in indexes:
            db.predictions.add(detections[index], name, .9, model,
                               labels_fingerprint='current', taxonomy=BLUE,
                               group_id=group, vote_count=2, total_votes=2)
            ids.append(db.conn.execute('SELECT max(id) FROM predictions').fetchone()[0])
    db.commit()
    return photos, detections, ids


def _body(db, ids):
    return {'review_prediction_ids': ids, 'observed_workspace_id': db.require_workspace_id()}


def _statuses(db, ids):
    return [db.predictions.current_status(pid) for pid in ids]


def test_overlapping_groups_and_singletons_share_one_card(app_and_db):
    app, db = app_and_db
    _, _, ids = _seed(db)
    rows = app.test_client().get('/api/predictions').get_json()['predictions']
    assert {p['id'] for p in rows} == set(ids)
    assert len({p['card_id'] for p in rows}) == 1
    assert len({p['consensus_species_key'] for p in rows}) == 1
    # The key is independent of the representative chosen by sorting.
    again = app.test_client().get('/api/predictions').get_json()['predictions']
    assert {p['card_id'] for p in rows} == {p['card_id'] for p in again}


@pytest.mark.parametrize('decision,status', [('accept', 'accepted'), ('reject', 'rejected')])
def test_card_decision_covers_union_and_undo_redo(app_and_db, decision, status):
    app, db = app_and_db
    photos, _, ids = _seed(db)
    client = app.test_client()
    before = {pid: {k['id'] for k in db.photo_keywords.get_for_photo(pid)} for pid in photos}
    response = client.post(f'/api/predictions/{ids[0]}/{decision}', json=_body(db, ids))
    assert response.status_code == 200, response.get_json()
    key = 'prediction_ids' if decision == 'accept' else 'rejected_prediction_ids'
    assert set(response.get_json()[key]) == set(ids)
    assert _statuses(db, ids) == [status] * len(ids)
    if decision == 'accept':
        assert all('Blue Tit' in {k['name'] for k in db.photo_keywords.get_for_photo(pid)} for pid in photos)
    assert client.post('/api/undo').status_code == 200
    assert _statuses(db, ids) == ['pending'] * len(ids)
    assert {pid: {k['id'] for k in db.photo_keywords.get_for_photo(pid)} for pid in photos} == before
    assert client.post('/api/redo').status_code == 200
    assert _statuses(db, ids) == [status] * len(ids)


@pytest.mark.parametrize('decision', ['accept', 'reject'])
def test_filtered_card_never_expands_outside_submitted_members(app_and_db, decision):
    app, db = app_and_db
    _, _, ids = _seed(db)
    response = app.test_client().post(f'/api/predictions/{ids[0]}/{decision}', json=_body(db, ids[:1]))
    assert response.status_code == 200
    assert _statuses(db, ids[1:]) == ['pending'] * (len(ids) - 1)


@pytest.mark.parametrize('decision', ['accept', 'reject'])
@pytest.mark.parametrize('changed', ['accepted', 'rejected', 'reviewed', 'superseded', 'workspace'])
def test_stale_card_is_refused_atomically(app_and_db, decision, changed):
    app, db = app_and_db
    _, detections, ids = _seed(db)
    body = _body(db, ids)
    if changed == 'superseded':
        db.predictions.add(detections[2], 'Blue Tit', .95, 'third-model',
                           labels_fingerprint='new', taxonomy=BLUE)
    elif changed == 'workspace':
        body['observed_workspace_id'] += 1
    else:
        db.update_prediction_status(ids[-1], changed)
    before = _statuses(db, ids)
    response = app.test_client().post(f'/api/predictions/{ids[0]}/{decision}', json=body)
    assert response.status_code == 409, response.get_json()
    assert _statuses(db, ids) == before
    assert db.conn.execute('SELECT count(*) FROM edit_history').fetchone()[0] == 0


def test_other_species_and_disjoint_bursts_stay_separate(app_and_db):
    app, db = app_and_db
    _, detections, ids = _seed(db)
    db.predictions.add(detections[0], 'Blue Tit', .95, 'other-species',
                       taxonomy={'taxon_id': 999, 'scientific_name': 'Other species'})
    other = db.conn.execute('SELECT max(id) FROM predictions').fetchone()[0]
    client = app.test_client()
    rows = {p['id']: p for p in client.get('/api/predictions').get_json()['predictions']}
    assert rows[other]['card_id'] != rows[ids[0]]['card_id']
    response = client.post(f'/api/predictions/{ids[0]}/accept', json=_body(db, [*ids, other]))
    assert response.status_code == 409
    assert _statuses(db, [*ids, other]) == ['pending'] * (len(ids) + 1)
    assert client.post(f'/api/predictions/{ids[0]}/accept', json=_body(db, ids)).status_code == 200
    assert db.predictions.current_status(other) == 'pending'


def test_deferred_member_blocks_whole_card_before_writes(app_and_db):
    app, db = app_and_db
    _, detections, ids = _seed(db)
    db.predictions.set_evidence_review(detections[2], 'not_enough_evidence')
    db.commit()
    response = app.test_client().post(f'/api/predictions/{ids[0]}/accept', json=_body(db, ids))
    assert response.status_code == 409
    assert _statuses(db, ids) == ['pending'] * len(ids)


def test_compare_accepts_aliases_only_on_same_detection(app_and_db):
    app, db = app_and_db
    _, _, ids = _seed(db)
    client = app.test_client()
    response = client.post(f'/api/predictions/{ids[1]}/accept-subject')
    assert response.status_code == 200
    assert set(response.get_json()['prediction_ids']) == {ids[1], ids[2]}
    assert _statuses(db, [ids[0], ids[3], ids[4]]) == ['pending'] * 3
    assert client.post('/api/undo').status_code == 200
    assert _statuses(db, ids) == ['pending'] * len(ids)


def test_same_species_without_overlap_is_not_merged(app_and_db):
    app, db = app_and_db
    _, _, ids = _seed(db)
    # Remove the iNat row on the shared photo, breaking the only bridge.
    db.conn.execute('DELETE FROM predictions WHERE id=?', (ids[2],))
    db.commit()
    rows = {p['id']: p for p in app.test_client().get('/api/predictions').get_json()['predictions']}
    assert rows[ids[0]]['card_id'] == rows[ids[1]]['card_id']
    assert rows[ids[3]]['card_id'] == rows[ids[4]]['card_id']
    assert rows[ids[0]]['card_id'] != rows[ids[3]]['card_id']


def test_decided_and_stale_rows_do_not_hide_pending_cards(app_and_db):
    app, db = app_and_db
    _, detections, ids = _seed(db)
    db.update_prediction_status(ids[1], 'accepted')
    db.predictions.add(detections[2], 'Blue Tit', .99, 'third-model',
                       labels_fingerprint='old', taxonomy=BLUE)
    stale = db.conn.execute('SELECT max(id) FROM predictions').fetchone()[0]
    db.conn.execute("UPDATE predictions SET created_at='2000-01-01' WHERE id=?", (stale,))
    db.commit()
    rows = {p['id']: p for p in app.test_client().get('/api/predictions').get_json()['predictions']}
    assert stale not in rows
    assert rows[ids[1]]['card_id'] != rows[ids[2]]['card_id']


def test_card_undo_preserves_existing_keyword_and_prior_alternative_decision(app_and_db):
    app, db = app_and_db
    photos, detections, ids = _seed(db)
    kid = db.add_keyword('Blue Tit', is_species=True)
    db.tag_photo(photos[1], kid)
    db.predictions.add(detections[1], 'Other bird', .1, 'BioCLIP',
                       labels_fingerprint='current', status='reviewed')
    earlier = db.conn.execute('SELECT max(id) FROM predictions').fetchone()[0]
    client = app.test_client()
    assert client.post(f'/api/predictions/{ids[0]}/accept', json=_body(db, ids)).status_code == 200
    assert db.predictions.current_status(earlier) == 'reviewed'
    assert client.post('/api/undo').status_code == 200
    assert db.predictions.current_status(earlier) == 'reviewed'
    assert kid in {k['id'] for k in db.photo_keywords.get_for_photo(photos[1])}
    assert _statuses(db, ids) == ['pending'] * len(ids)
    assert client.post('/api/redo').status_code == 200
    assert db.predictions.current_status(earlier) == 'reviewed'


@pytest.mark.parametrize('decision', ['accept', 'reject'])
def test_alternative_statuses_round_trip_with_card(app_and_db, decision):
    app, db = app_and_db
    _, detections, ids = _seed(db)
    db.predictions.add(detections[1], 'Other bird', .1, 'BioCLIP',
                       labels_fingerprint='current', status='alternative')
    alt = db.conn.execute('SELECT max(id) FROM predictions').fetchone()[0]
    client = app.test_client()
    assert client.post(f'/api/predictions/{ids[0]}/{decision}', json=_body(db, ids)).status_code == 200
    assert db.predictions.current_status(alt) == 'rejected'
    assert client.post('/api/undo').status_code == 200
    assert db.predictions.current_status(alt) == 'alternative'
    assert _statuses(db, ids) == ['pending'] * len(ids)
    assert client.post('/api/redo').status_code == 200
    assert db.predictions.current_status(alt) == 'rejected'


@pytest.mark.parametrize('decision', ['accept', 'reject'])
@pytest.mark.parametrize('invalid', [[], [True], ['1'], [999999]])
def test_invalid_card_payload_never_falls_back_to_group_expansion(app_and_db, decision, invalid):
    app, db = app_and_db
    _, _, ids = _seed(db)
    response = app.test_client().post(f'/api/predictions/{ids[0]}/{decision}', json=_body(db, invalid))
    assert response.status_code in (400, 409)
    assert _statuses(db, ids) == ['pending'] * len(ids)


def test_mid_card_failure_rolls_back_keywords_statuses_and_history(app_and_db, monkeypatch):
    from db import Database

    app, db = app_and_db
    photos, _, ids = _seed(db)
    before = {pid: {k['id'] for k in db.photo_keywords.get_for_photo(pid)} for pid in photos}
    real_accept = Database.accept_prediction

    def accept(database, prediction_id, **kwargs):
        if prediction_id == ids[2]:
            raise RuntimeError('second model failed')
        return real_accept(database, prediction_id, **kwargs)

    monkeypatch.setattr(Database, 'accept_prediction', accept)
    response = app.test_client().post(f'/api/predictions/{ids[0]}/accept', json=_body(db, ids))
    assert response.status_code == 500
    assert _statuses(db, ids) == ['pending'] * len(ids)
    assert {pid: {k['id'] for k in db.photo_keywords.get_for_photo(pid)} for pid in photos} == before
    assert db.conn.execute('SELECT count(*) FROM edit_history').fetchone()[0] == 0


def test_merging_uses_the_species_accept_applies_not_a_minority_frame_label(app_and_db):
    app, db = app_and_db
    photos, _, ids = _seed(db)
    db.conn.execute(
        "UPDATE predictions SET species='Other bird', source_taxon_id=999, scientific_name='Other bird' WHERE id=?",
        (ids[0],),
    )
    db.conn.execute(
        "UPDATE prediction_review SET individual=? WHERE group_id='bio'",
        ('{"Blue Tit": 2, "Other bird": 1}',),
    )
    db.commit()
    client = app.test_client()
    rows = client.get('/api/predictions').get_json()['predictions']
    assert len({p['card_id'] for p in rows}) == 1
    assert len({p['species_key'] for p in rows}) == 2
    assert client.post(f'/api/predictions/{ids[0]}/accept', json=_body(db, ids)).status_code == 200
    assert all('Other bird' not in {k['name'] for k in db.photo_keywords.get_for_photo(pid)} for pid in photos)


@pytest.mark.parametrize('decision', ['accept', 'reject'])
def test_visibility_loss_refuses_every_member(app_and_db, decision):
    app, db = app_and_db
    _, _, ids = _seed(db)
    db.conn.execute('DELETE FROM workspace_folders WHERE workspace_id=?', (db.require_workspace_id(),))
    db.commit()
    response = app.test_client().post(f'/api/predictions/{ids[0]}/{decision}', json=_body(db, ids))
    assert response.status_code == 409
    assert _statuses(db, ids) == ['pending'] * len(ids)
