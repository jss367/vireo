"""Tests for /api/pipeline/group/state and /api/pipeline/group/apply.

These endpoints back the burst-review modal on the pipeline review page.
Apply must be diff-based (only writes when state actually changes), must
clear flags on photos moved to candidates, and must add the consensus
species keyword on picks while skipping photos that already have it.
"""


def _photo_ids(db):
    return [p['id'] for p in db.get_photos()]


def test_apply_flags_picks_rejects_and_ignores_species(app_and_db):
    """Flags-only contract: apply writes pick/reject flags but the endpoint
    no longer tags species (species confirmation moves to a dedicated path)."""
    app, db = app_and_db
    pids = _photo_ids(db)
    pick_id, reject_id = pids[0], pids[1]
    client = app.test_client()

    resp = client.post('/api/pipeline/group/apply', json={
        'picks': [pick_id],
        'rejects': [reject_id],
        'candidates': [],
        'species': 'Coyote',
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['ok'] is True

    assert db.get_photo(pick_id)['flag'] == 'flagged'
    assert db.get_photo(reject_id)['flag'] == 'rejected'

    # No species keyword is tagged on the pick.
    pick_kws = {k['name'] for k in db.get_photo_keywords(pick_id)}
    assert 'Coyote' not in pick_kws

    # Reject also has no species keyword.
    reject_kws = {k['name'] for k in db.get_photo_keywords(reject_id)}
    assert 'Coyote' not in reject_kws

    # Returned per-photo state matches what we just wrote.
    photos = data['photos']
    assert photos[str(pick_id)]['flag'] == 'flagged'
    assert photos[str(pick_id)]['has_species_keyword'] is False
    assert photos[str(reject_id)]['flag'] == 'rejected'
    assert photos[str(reject_id)]['has_species_keyword'] is False


def test_group_apply_ignores_species_and_tags_nothing(app_and_db):
    """Posting `species` applies flags but tags NO species keyword."""
    app, db = app_and_db
    pids = _photo_ids(db)
    p1, p2 = pids[0], pids[1]
    client = app.test_client()

    resp = client.post('/api/pipeline/group/apply', json={
        'picks': [p1],
        'rejects': [p2],
        'candidates': [],
        'species': 'Blue Jay',
    })
    assert resp.status_code == 200
    body = resp.get_json()
    # Flags still applied.
    assert body['photos'][str(p1)]['flag'] == 'flagged'
    assert body['photos'][str(p2)]['flag'] == 'rejected'
    # But NO species keyword was added to the pick.
    assert body['photos'][str(p1)]['has_species_keyword'] is False
    # And the DB has no species keyword on p1.
    kws = db.get_photo_keywords(p1)
    assert not any(k['name'] == 'Blue Jay' for k in kws)


def test_apply_clears_flag_when_photo_moved_to_candidates(app_and_db):
    """If a photo was rejected previously and the user moves it back to
    candidates, Apply & Close must clear the flag in the DB."""
    app, db = app_and_db
    pids = _photo_ids(db)
    pid = pids[0]
    db.update_photo_flag(pid, 'rejected')
    client = app.test_client()

    resp = client.post('/api/pipeline/group/apply', json={
        'picks': [],
        'rejects': [],
        'candidates': [pid],
        'species': '',
    })
    assert resp.status_code == 200
    assert db.get_photo(pid)['flag'] == 'none'


def test_apply_is_idempotent_when_state_unchanged(app_and_db):
    """Applying the same state twice doesn't double-record edit history."""
    app, db = app_and_db
    pids = _photo_ids(db)
    pid = pids[0]
    db.update_photo_flag(pid, 'flagged')
    client = app.test_client()

    # Snapshot edit history count.
    before = db.conn.execute(
        "SELECT COUNT(*) AS n FROM edit_history WHERE workspace_id = ?",
        (db._active_workspace_id,),
    ).fetchone()['n']

    resp = client.post('/api/pipeline/group/apply', json={
        'picks': [pid],
        'rejects': [],
        'candidates': [],
        'species': '',
    })
    assert resp.status_code == 200

    after = db.conn.execute(
        "SELECT COUNT(*) AS n FROM edit_history WHERE workspace_id = ?",
        (db._active_workspace_id,),
    ).fetchone()['n']
    assert after == before  # no flag transition → no history row


def test_apply_skips_keyword_when_already_applied(app_and_db):
    """If the photo already has the species keyword, Apply must not add a
    duplicate or queue a redundant XMP sync change."""
    app, db = app_and_db
    pids = _photo_ids(db)
    pid = pids[0]
    kid = db.add_keyword('Coyote', is_species=True)
    db.tag_photo(pid, kid)
    client = app.test_client()

    resp = client.post('/api/pipeline/group/apply', json={
        'picks': [pid],
        'rejects': [],
        'candidates': [],
        'species': 'Coyote',
    })
    assert resp.status_code == 200

    # Exactly one association — no duplicate.
    n = db.conn.execute(
        "SELECT COUNT(*) AS n FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (pid, kid),
    ).fetchone()['n']
    assert n == 1

    # No pending sync change for an already-applied keyword.
    pending = db.conn.execute(
        "SELECT COUNT(*) AS n FROM pending_changes "
        "WHERE photo_id = ? AND change_type = 'keyword_add' AND value = ? AND workspace_id = ?",
        (pid, 'Coyote', db._active_workspace_id),
    ).fetchone()['n']
    assert pending == 0


def test_apply_does_not_touch_keyword_pending_changes(app_and_db):
    """Flags-only contract: the endpoint no longer tags species, so it must not
    queue, cancel, or otherwise modify keyword pending changes. A pre-existing
    `keyword_remove` is left untouched, and no `keyword_add` is queued."""
    app, db = app_and_db
    pids = _photo_ids(db)
    pid = pids[0]
    # Pre-existing pending remove for "Coyote".
    db.queue_change(pid, 'keyword_remove', 'Coyote')
    client = app.test_client()

    resp = client.post('/api/pipeline/group/apply', json={
        'picks': [pid],
        'rejects': [],
        'candidates': [],
        'species': 'Coyote',
    })
    assert resp.status_code == 200

    pending = db.conn.execute(
        "SELECT change_type FROM pending_changes "
        "WHERE photo_id = ? AND value = 'Coyote' AND workspace_id = ?",
        (pid, db._active_workspace_id),
    ).fetchall()
    types = {row['change_type'] for row in pending}
    # The remove is left untouched and no add is queued in its place.
    assert 'keyword_remove' in types
    assert 'keyword_add' not in types


def test_apply_records_edit_history_for_undo(app_and_db):
    """Flag changes must be recorded in edit_history so undo works the same way
    it does on the regular review page. Flags-only: no `keyword_add` history is
    recorded by this endpoint anymore (species moves to a dedicated path)."""
    app, db = app_and_db
    pids = _photo_ids(db)
    pid = pids[0]
    client = app.test_client()

    resp = client.post('/api/pipeline/group/apply', json={
        'picks': [pid],
        'rejects': [],
        'candidates': [],
        'species': 'Coyote',
    })
    assert resp.status_code == 200

    rows = db.conn.execute(
        "SELECT action_type FROM edit_history WHERE workspace_id = ? ORDER BY id",
        (db._active_workspace_id,),
    ).fetchall()
    actions = [r['action_type'] for r in rows]
    assert 'flag' in actions
    assert 'keyword_add' not in actions


def test_apply_rejects_conflicting_zones(app_and_db):
    app, db = app_and_db
    pids = _photo_ids(db)
    pid = pids[0]
    client = app.test_client()

    resp = client.post('/api/pipeline/group/apply', json={
        'picks': [pid],
        'rejects': [pid],
        'candidates': [],
    })
    assert resp.status_code == 400


def test_apply_rejects_photo_outside_workspace(app_and_db):
    """Photo belonging to a folder not in the active workspace is rejected
    with 403, matching the existing predictions/group/apply endpoint."""
    app, db = app_and_db
    photos_in_default = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()
    pid = photos_in_default['id']

    # Create a second workspace with no folders and bump its last_opened_at
    # so the request-scoped Database picks it as the active workspace.
    other_ws = db.create_workspace('Other')
    db.update_workspace(other_ws, last_opened_at='2099-01-01T00:00:00')

    client = app.test_client()
    resp = client.post('/api/pipeline/group/apply', json={
        'picks': [pid],
        'rejects': [],
        'candidates': [],
    })
    assert resp.status_code == 403


def test_state_endpoint_returns_current_flags_and_keyword_status(app_and_db):
    app, db = app_and_db
    pids = _photo_ids(db)
    flagged_id, rejected_id, neutral_id = pids[0], pids[1], pids[2]
    db.update_photo_flag(flagged_id, 'flagged')
    db.update_photo_flag(rejected_id, 'rejected')
    kid = db.add_keyword('Coyote', is_species=True)
    db.tag_photo(flagged_id, kid)
    client = app.test_client()

    resp = client.post('/api/pipeline/group/state', json={
        'photo_ids': [flagged_id, rejected_id, neutral_id],
        'species': 'Coyote',
    })
    assert resp.status_code == 200
    data = resp.get_json()
    photos = data['photos']
    assert photos[str(flagged_id)]['flag'] == 'flagged'
    assert photos[str(flagged_id)]['has_species_keyword'] is True
    assert photos[str(rejected_id)]['flag'] == 'rejected'
    assert photos[str(rejected_id)]['has_species_keyword'] is False
    assert photos[str(neutral_id)]['flag'] == 'none'
    assert photos[str(neutral_id)]['has_species_keyword'] is False
    assert data['species_kid'] == kid


def test_state_endpoint_handles_unknown_species(app_and_db):
    """When the typed species doesn't exist as a keyword yet, has_species_keyword
    is always False (the keyword gets created on apply, not on state lookup)."""
    app, db = app_and_db
    pids = _photo_ids(db)
    client = app.test_client()
    resp = client.post('/api/pipeline/group/state', json={
        'photo_ids': pids,
        'species': 'NeverSeenBefore',
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['species_kid'] is None
    for pid in pids:
        assert data['photos'][str(pid)]['has_species_keyword'] is False


def test_state_endpoint_scopes_to_active_workspace(app_and_db):
    """The state endpoint must not leak flag/keyword status for photos that
    don't belong to the active workspace, matching the apply endpoint guard."""
    app, db = app_and_db
    photos_in_default = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()
    pid = photos_in_default['id']

    other_ws = db.create_workspace('Other')
    db.update_workspace(other_ws, last_opened_at='2099-01-01T00:00:00')

    client = app.test_client()
    resp = client.post('/api/pipeline/group/state', json={
        'photo_ids': [pid],
        'species': 'Coyote',
    })
    assert resp.status_code == 200
    data = resp.get_json()
    # Photo from a different workspace must not appear in the response.
    assert str(pid) not in data['photos']
    assert pid not in data['photos']


def test_state_endpoint_ignores_homonym_non_species_keyword(app_and_db):
    """A non-species keyword (e.g. an 'individual' tag) sharing the species name
    must NOT be reported as the species keyword. Otherwise has_species_keyword
    and the Apply-label preview would lie about what apply will write."""
    app, db = app_and_db
    pids = _photo_ids(db)
    pid = pids[0]

    # Pre-existing 'individual' tag named like the species we're about to type.
    individual_kid = db.add_keyword('Robin', kw_type='individual')
    db.tag_photo(pid, individual_kid)

    client = app.test_client()
    resp = client.post('/api/pipeline/group/state', json={
        'photo_ids': [pid],
        'species': 'Robin',
    })
    assert resp.status_code == 200
    data = resp.get_json()

    # The individual tag is not the species keyword. Either no species kid is
    # reported, or it's a different id from the individual.
    assert data['species_kid'] != individual_kid
    assert data['photos'][str(pid)]['has_species_keyword'] is False


def test_state_endpoint_gates_representative_on_current_eligibility(app_and_db):
    """A stored representative preference that's now stale (photo rejected or
    no longer carrying the species keyword) must NOT be reported as the
    representative on the group modal, matching how browse/highlights hide it.
    """
    app, db = app_and_db
    pids = _photo_ids(db)
    live_id, rejected_id, untagged_id = pids[0], pids[1], pids[2]

    # One species per photo so each failure mode is independently observable.
    kid_live = db.add_keyword('Coyote Live', is_species=True)
    kid_rejected = db.add_keyword('Coyote Rejected', is_species=True)
    kid_untagged = db.add_keyword('Coyote Untagged', is_species=True)
    db.tag_photo(live_id, kid_live)
    db.tag_photo(rejected_id, kid_rejected)
    db.tag_photo(untagged_id, kid_untagged)
    db.set_species_representative('Coyote Live', live_id)
    db.set_species_representative('Coyote Rejected', rejected_id)
    db.set_species_representative('Coyote Untagged', untagged_id)

    # Make each stale in one of the two ways the shared payload attachers
    # already filter on. The preference rows themselves remain intact.
    db.update_photo_flag(rejected_id, 'rejected')
    db.untag_photo(untagged_id, kid_untagged)

    client = app.test_client()

    # Eligible representative still lights up.
    resp = client.post('/api/pipeline/group/state', json={
        'photo_ids': [live_id],
        'species': 'Coyote Live',
    })
    assert resp.status_code == 200
    assert resp.get_json()['photos'][str(live_id)]['is_species_representative'] is True

    # Rejected photo whose preference row still points at it does NOT.
    resp = client.post('/api/pipeline/group/state', json={
        'photo_ids': [rejected_id],
        'species': 'Coyote Rejected',
    })
    assert resp.status_code == 200
    assert resp.get_json()['photos'][str(rejected_id)]['is_species_representative'] is False

    # Photo whose preference row still points at it but that no longer carries
    # the stored species keyword also does NOT.
    resp = client.post('/api/pipeline/group/state', json={
        'photo_ids': [untagged_id],
        'species': 'Coyote Untagged',
    })
    assert resp.status_code == 200
    assert resp.get_json()['photos'][str(untagged_id)]['is_species_representative'] is False
