"""Cross-model Review cards and their exact, reversible decision scope."""

import json

import pytest
from services import review_decisions

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
            db.commit()
            ids.append(db.conn.execute('SELECT max(id) FROM predictions').fetchone()[0])
    db.commit()
    return photos, detections, ids


def _body(db, ids):
    return {'review_prediction_ids': ids, 'observed_workspace_id': db.require_workspace_id()}


def _statuses(db, ids):
    return [db.predictions.current_status(pid) for pid in ids]


@pytest.mark.parametrize('decision', ['accept', 'reject'])
@pytest.mark.parametrize('body', [None, 123, True, 'review_prediction_ids', [], ['review_prediction_ids']])
def test_decision_requires_a_json_object_before_any_writes(app_and_db, decision, body):
    app, db = app_and_db
    _, _, ids = _seed(db)
    response = app.test_client().post(
        f'/api/predictions/{ids[0]}/{decision}', data=json.dumps(body), content_type='application/json',
    )
    assert response.status_code == 400, response.get_data(as_text=True)
    assert 'object' in response.get_json()['error']
    assert _statuses(db, ids) == ['pending'] * len(ids)
    assert db.conn.execute('SELECT count(*) FROM edit_history').fetchone()[0] == 0
    assert db.conn.execute('SELECT count(*) FROM pending_changes').fetchone()[0] == 0


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
        db.commit()
    elif changed == 'workspace':
        body['observed_workspace_id'] += 1
    else:
        review_decisions.update_prediction_status(db, ids[-1], changed)
        db.commit()
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
    db.commit()
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


@pytest.mark.parametrize('existing_alias', [None, 'Verdin', 'Auriparus flaviceps'])
@pytest.mark.parametrize('reverse_order', [False, True])
@pytest.mark.parametrize('sidecar_synced', [False, True])
def test_compare_legacy_aliases_undo_redo_tags_and_sidecar_intent(
    app_and_db, existing_alias, reverse_order, sidecar_synced,
):
    """History must undo the alias actually tagged, preserving pre-existing tags."""
    app, db = app_and_db
    photo_id = db.photos.list_page()[0]['id']
    db.conn.execute(
        "INSERT INTO taxa (inat_id, name, common_name, rank) "
        "VALUES (2912, 'Auriparus flaviceps', 'Verdin', 'species')",
    )
    db.commit()
    aliases = ['Verdin', 'Auriparus flaviceps']
    if reverse_order:
        aliases.reverse()
    keywords = {name: db.add_keyword(name, is_species=True) for name in aliases}
    if existing_alias:
        db.tag_photo(photo_id, keywords[existing_alias])
        db.commit()
    before = {k['id'] for k in db.photo_keywords.get_for_photo(photo_id)}
    detection_id = db.detections.save(photo_id, [{
        'box': {'x': .1, 'y': .1, 'w': .3, 'h': .4}, 'confidence': .9,
        'category': 'animal',
    }], detector_model='MDV6')[0]
    db.commit()
    ids = []
    for index, name in enumerate(aliases):
        db.predictions.add(detection_id, name, .9 - index * .1, f'custom-{index}',
                           labels_fingerprint='legacy')
        db.commit()
        ids.append(db.conn.execute('SELECT max(id) FROM predictions').fetchone()[0])
    assert all(row['source_taxon_id'] is None for row in db.predictions.get_rows_by_ids(ids))
    db.commit()

    def tags():
        return {k['id'] for k in db.photo_keywords.get_for_photo(photo_id)}

    def pending():
        return {tuple(row) for row in db.conn.execute(
            'SELECT change_type, value FROM pending_changes WHERE photo_id=?', (photo_id,),
        )}

    pending_before = pending()
    client = app.test_client()
    response = client.post(f'/api/predictions/{ids[0]}/accept-subject')
    assert response.status_code == 200, response.get_json()
    assert set(response.get_json()['prediction_ids']) == set(ids)
    assert _statuses(db, ids) == ['accepted', 'accepted']
    expected_tags = before if existing_alias else before | {keywords[aliases[0]]}
    assert tags() == expected_tags
    expected_pending = pending_before if existing_alias else pending_before | {('keyword_add', aliases[0])}
    assert pending() == expected_pending

    items = db.conn.execute(
        "SELECT i.new_value, i.old_value FROM edit_history_items i "
        "JOIN edit_history e ON e.id=i.edit_id WHERE e.action_type='prediction_accept'",
    ).fetchall()
    by_keyword = {int(item['new_value']): json.loads(item['old_value']) for item in items}
    assert set(by_keyword) == set(keywords.values())
    for index, alias in enumerate(aliases):
        meta = by_keyword[keywords[alias]]
        assert meta['prediction_ids'] == [ids[index]]
        assert bool(meta.get('no_tag')) == (bool(existing_alias) or index > 0)

    if sidecar_synced:
        db.conn.execute('DELETE FROM pending_changes WHERE photo_id=?', (photo_id,))
        db.commit()
        pending_before = set()
    assert client.post('/api/undo').status_code == 200
    assert tags() == before
    assert _statuses(db, ids) == ['pending', 'pending']
    undo_pending = pending_before
    if sidecar_synced and not existing_alias:
        undo_pending = {('keyword_remove', aliases[0])}
    assert pending() == undo_pending
    assert client.post('/api/redo').status_code == 200
    assert tags() == expected_tags
    assert _statuses(db, ids) == ['accepted', 'accepted']
    # If the accept was synced, redo cancels undo's unsynced removal:
    # the sidecar already contains the first alias, so no add is needed.
    redo_pending = pending_before
    if not existing_alias and not sidecar_synced:
        redo_pending = pending_before | {('keyword_add', aliases[0])}
    assert pending() == redo_pending


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
    review_decisions.update_prediction_status(db, ids[1], 'accepted')
    db.commit()
    db.predictions.add(detections[2], 'Blue Tit', .99, 'third-model',
                       labels_fingerprint='old', taxonomy=BLUE)
    db.commit()
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
    db.commit()
    db.predictions.add(detections[1], 'Other bird', .1, 'BioCLIP',
                       labels_fingerprint='current', status='reviewed')
    db.commit()
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
    db.commit()
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


@pytest.mark.parametrize('decision', ['accept', 'reject'])
@pytest.mark.parametrize('failure', ['member', 'history'])
def test_mid_card_failure_rolls_back_keywords_statuses_and_history(app_and_db, monkeypatch, decision, failure):
    from db import Database

    app, db = app_and_db
    photos, _, ids = _seed(db)
    # A failed new action must also preserve the redo stack it tried to clear.
    edit_id = db.record_edit('rating', 'prior edit', '1', [
        {'photo_id': photos[0], 'old_value': '0', 'new_value': '1'},
    ])
    db.commit()
    db.conn.execute('UPDATE edit_history SET undone=1 WHERE id=?', (edit_id,))
    db.commit()

    def snapshot():
        return {table: [tuple(row) for row in db.conn.execute(f'SELECT * FROM {table} ORDER BY rowid')]
                for table in ('keywords', 'photo_keywords', 'prediction_review', 'pending_changes',
                              'edit_history', 'edit_history_items')}

    before = snapshot()
    method = 'record_edit' if failure == 'history' else (
        'accept_prediction' if decision == 'accept' else 'update_prediction_status'
    )
    # A decision write goes through its review_decisions writer; history
    # through Database.record_edit.
    owner = Database if failure == 'history' else review_decisions
    real_write = getattr(owner, method)
    observations = []
    calls = 0

    def write(database, *args, **kwargs):
        nonlocal calls
        calls += 1
        if failure == 'member' and calls == 2:
            raise RuntimeError('second member failed')
        result = real_write(database, *args, **kwargs)
        # The independent fixture connection must still see the before-state;
        # no inner setter or history write may release the writer transaction.
        observations.append((database.in_transaction, snapshot() == before))
        if failure == 'history':
            raise RuntimeError('history failed after inserting its items')
        return result

    monkeypatch.setattr(owner, method, write)
    response = app.test_client().post(f'/api/predictions/{ids[0]}/{decision}', json=_body(db, ids))
    assert response.status_code == 500
    assert observations and all(locked and unpublished for locked, unpublished in observations)
    assert _statuses(db, ids) == ['pending'] * len(ids)
    assert snapshot() == before


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


def _seed_two_detections_same_photo(db, grouped=False):
    """One photo carrying two distinct detections of the same species.

    Two models classify each detection as the same taxon. The expected shape
    is two cards (one per detection), each spanning both models; a card
    decision must not touch the other detected subject.
    """
    photo = db.photos.list_page()[0]['id']
    db.conn.execute(
        "INSERT INTO taxa (inat_id, name, common_name, rank) VALUES (?, ?, ?, 'species')",
        (BLUE['taxon_id'], BLUE['scientific_name'], 'Blue Tit'),
    )
    left = db.detections.save(photo, [{
        'box': {'x': .05, 'y': .1, 'w': .3, 'h': .4}, 'confidence': .9,
        'category': 'animal',
    }], detector_model='MDV6')[0]
    right = db.detections.save(photo, [{
        'box': {'x': .55, 'y': .1, 'w': .3, 'h': .4}, 'confidence': .9,
        'category': 'animal',
    }], detector_model='MDV6-right')[0]
    db.commit()
    ids_by_detection: dict[int, list[int]] = {left: [], right: []}
    for detection in (left, right):
        for model, name in [('BioCLIP', 'Eurasian Blue Tit'), ('iNat21', 'Blue Tit')]:
            db.predictions.add(detection, name, .9, model,
                               labels_fingerprint='current', taxonomy=BLUE,
                               group_id=f'{model}-shared' if grouped else None)
            db.commit()
            ids_by_detection[detection].append(
                db.conn.execute('SELECT max(id) FROM predictions').fetchone()[0],
            )
    db.commit()
    return photo, ids_by_detection


@pytest.mark.parametrize('grouped', [False, True])
def test_two_detections_in_one_photo_stay_in_separate_cards(app_and_db, grouped):
    app, db = app_and_db
    _, ids_by_detection = _seed_two_detections_same_photo(db, grouped)
    left_ids, right_ids = ids_by_detection.values()
    rows = {p['id']: p for p in app.test_client().get('/api/predictions').get_json()['predictions']}
    assert {rows[pid]['card_id'] for pid in left_ids} == {rows[left_ids[0]]['card_id']}
    assert {rows[pid]['card_id'] for pid in right_ids} == {rows[right_ids[0]]['card_id']}
    assert rows[left_ids[0]]['card_id'] != rows[right_ids[0]]['card_id']


@pytest.mark.parametrize('reverse_order', [False, True])
def test_transitive_burst_links_cannot_merge_distinct_subjects_in_one_frame(reverse_order):
    from services.review_cards import review_card_components

    # Each model's burst visits each frame once, but they disagree on which
    # subject in frame 10 continues into the shared detection in frame 20.
    rows = [
        {'id': 1, 'photo_id': 10, 'detection_id': 100, 'model': 'bio', 'group_id': 'a'},
        {'id': 2, 'photo_id': 20, 'detection_id': 200, 'model': 'bio', 'group_id': 'a'},
        {'id': 3, 'photo_id': 20, 'detection_id': 200, 'model': 'inat', 'group_id': 'b'},
        {'id': 4, 'photo_id': 10, 'detection_id': 101, 'model': 'inat', 'group_id': 'b'},
    ]
    for row in rows:
        row.update(consensus_species_key='taxon:1', status='pending')
    if reverse_order:
        rows.reverse()
    components = review_card_components(rows)
    assert {frozenset(row['id'] for row in members) for members in components} == {
        frozenset({1}), frozenset({2, 3}), frozenset({4}),
    }


@pytest.mark.parametrize('decision,status', [('accept', 'accepted'), ('reject', 'rejected')])
@pytest.mark.parametrize('grouped', [False, True])
def test_decision_on_one_detection_leaves_the_other_subject_reviewable(
    app_and_db, decision, status, grouped,
):
    app, db = app_and_db
    _, ids_by_detection = _seed_two_detections_same_photo(db, grouped)
    left_ids, right_ids = ids_by_detection.values()
    client = app.test_client()
    response = client.post(
        f'/api/predictions/{left_ids[0]}/{decision}', json=_body(db, left_ids),
    )
    assert response.status_code == 200, response.get_json()
    key = 'prediction_ids' if decision == 'accept' else 'rejected_prediction_ids'
    assert set(response.get_json()[key]) == set(left_ids)
    assert _statuses(db, left_ids) == [status] * len(left_ids)
    # The other subject's predictions must remain pending and independently
    # decidable through its own card.
    assert _statuses(db, right_ids) == ['pending'] * len(right_ids)
    assert client.post(
        f'/api/predictions/{right_ids[0]}/{decision}', json=_body(db, right_ids),
    ).status_code == 200
    assert _statuses(db, right_ids) == [status] * len(right_ids)
    # Undo must restore only the most recent subject's decision, leaving the
    # earlier subject's decision intact.
    assert client.post('/api/undo').status_code == 200
    assert _statuses(db, right_ids) == ['pending'] * len(right_ids)
    assert _statuses(db, left_ids) == [status] * len(left_ids)
    # Redo re-applies that decision without touching the other subject.
    assert client.post('/api/redo').status_code == 200
    assert _statuses(db, right_ids) == [status] * len(right_ids)
    assert _statuses(db, left_ids) == [status] * len(left_ids)


@pytest.mark.parametrize('decision', ['accept', 'reject'])
@pytest.mark.parametrize('grouped', [False, True])
def test_submitting_both_subjects_as_one_card_is_refused(app_and_db, decision, grouped):
    app, db = app_and_db
    _, ids_by_detection = _seed_two_detections_same_photo(db, grouped)
    left_ids, right_ids = ids_by_detection.values()
    merged = [*left_ids, *right_ids]
    response = app.test_client().post(
        f'/api/predictions/{left_ids[0]}/{decision}', json=_body(db, merged),
    )
    assert response.status_code == 409
    assert _statuses(db, merged) == ['pending'] * len(merged)


@pytest.mark.parametrize('decision', ['accept', 'reject'])
def test_visibility_loss_refuses_every_member(app_and_db, decision):
    app, db = app_and_db
    _, _, ids = _seed(db)
    db.conn.execute('DELETE FROM workspace_folders WHERE workspace_id=?', (db.require_workspace_id(),))
    db.commit()
    response = app.test_client().post(f'/api/predictions/{ids[0]}/{decision}', json=_body(db, ids))
    assert response.status_code == 409
    assert _statuses(db, ids) == ['pending'] * len(ids)
