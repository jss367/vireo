"""Grouping edits share persistent history with ordinary photo edits."""

import contextlib
import copy
import os

import pytest
from pipeline import load_results_raw, save_results_raw


def _seed(db):
    ids = sorted(p['id'] for p in db.get_photos())
    cache = {
        'photos': [{'id': pid, 'label': 'REVIEW', 'rating': 0} for pid in ids],
        'encounters': [{
            'photo_ids': ids, 'photo_count': len(ids), 'burst_count': 2,
            'bursts': [{'photo_ids': ids[:2]}, {'photo_ids': ids[2:]}],
            'species': [], 'species_predictions': [],
        }],
        'summary': {},
    }
    save_results_raw(cache, os.path.dirname(db._db_path), db._ws_id())
    return ids, cache


def _load(db):
    return load_results_raw(os.path.dirname(db._db_path), db._ws_id())


def _detach(client, kind='burst', **extra):
    response = client.post('/api/pipeline/detach-' + kind, json={
        'encounter_index': 0, 'burst_index': 0, **extra,
    })
    assert response.status_code == 200, response.get_json()


def test_grouping_and_flags_undo_redo_in_one_history(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, original = _seed(db)
    _detach(client, 'photo', photo_id=ids[0])
    first = _load(db)
    client.post('/api/batch/flag', json={'photo_ids': ids, 'flag': 'rejected'})
    _detach(client)
    last = _load(db)
    assert client.get('/api/undo/status').json['count'] == 3

    # A new client/page uses the same history; grouping never restores photos.
    client = app.test_client()
    assert client.post('/api/undo').status_code == 200
    assert _load(db)['encounters'] == first['encounters']
    assert all(db.get_photo(pid)['flag'] == 'rejected' for pid in ids)
    assert client.post('/api/undo').status_code == 200
    assert all(db.get_photo(pid)['flag'] == 'none' for pid in ids)
    assert client.post('/api/undo').status_code == 200
    assert _load(db)['encounters'] == original['encounters']
    assert client.get('/api/undo/status').json['available'] is False
    for _ in range(3):
        assert client.post('/api/redo').status_code == 200
    assert _load(db)['encounters'] == last['encounters']
    assert all(db.get_photo(pid)['flag'] == 'rejected' for pid in ids)


def test_grouping_restore_preserves_new_photo_metadata(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, original = _seed(db)
    _detach(client)
    updated = _load(db)
    updated['photos'][0]['rating'] = 5
    updated['photos'][0]['quality_composite'] = .95
    updated['miss_computed_at'] = 'new marker'
    save_results_raw(updated, os.path.dirname(db._db_path), db._ws_id())
    assert client.post('/api/undo').status_code == 200
    restored = _load(db)
    assert restored['encounters'] == original['encounters']
    assert restored['photos'] == updated['photos']
    assert restored['miss_computed_at'] == 'new marker'


def test_stale_grouping_entry_is_retired_and_preserves_newer_work(app_and_db):
    """A stale action is reported before an older, unrelated edit is undone."""
    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    # Older undoable edit that must remain reachable through undo.
    client.post('/api/batch/flag', json={'photo_ids': ids, 'flag': 'flagged'})
    _detach(client)
    updated = _load(db)
    # Structural change (a burst split) mirrors what reflow/regroup-live
    # would land: the cache no longer matches the detach entry's snapshot.
    updated['encounters'][0]['bursts'].append({'photo_ids': [ids[0]]})
    save_results_raw(updated, os.path.dirname(db._db_path), db._ws_id())

    # The advertised grouping action must not silently undo the older flag.
    advertised = client.get('/api/undo/status').json
    assert 'detach' in advertised['description'].lower()
    assert client.post('/api/undo').status_code == 409
    assert all(db.get_photo(pid)['flag'] == 'flagged' for pid in ids)
    refreshed = client.get('/api/undo/status').json
    assert refreshed['id'] != advertised['id']
    assert 'flag' in refreshed['description'].lower()
    response = client.post('/api/undo')
    assert response.status_code == 200, response.get_json()
    assert 'flag' in response.get_json()['undone'].lower()
    assert _load(db)['encounters'] == updated['encounters']
    assert all(db.get_photo(pid)['flag'] == 'none' for pid in ids)
    assert client.get('/api/undo/status').json['count'] == 0
    # Redo replays the flag edit; the retired detach never resurfaces.
    assert client.post('/api/redo').status_code == 200
    assert all(db.get_photo(pid)['flag'] == 'flagged' for pid in ids)
    assert client.get('/api/redo/status').json['available'] is False
    assert _load(db)['encounters'] == updated['encounters']


def test_grouping_undo_preserves_cleared_burst_override(app_and_db):
    """A ``clearBurstOverride`` clear survives the next grouping undo.

    ``clearBurstOverride`` posts ``species_override = None`` through
    ``/api/pipeline/save-cache`` and records no history entry. The next
    grouping undo therefore sees the cache as structurally unchanged
    (``_grouping_signature`` ignores ``species_override``) and would
    otherwise silently resurrect the pre-clear override from the
    detach's ``before`` snapshot. The clear must carry through instead.
    """
    app, db = app_and_db
    client = app.test_client()
    ids, original = _seed(db)
    # Seed a burst-level override the detach's ``before`` snapshot
    # captures, so restoring it without a preservation pass would
    # resurrect the old value.
    original_state = _load(db)
    original_state['encounters'][0]['bursts'][0]['species_override'] = {
        'species': 'Old override', 'confirmed': True,
    }
    save_results_raw(original_state, os.path.dirname(db._db_path), db._ws_id())
    original = _load(db)

    _detach(client)
    detached = _load(db)
    # The detached burst is the new encounter appended by the detach
    # handler — locate it by photo composition so this test survives any
    # future reordering.
    detached_photo_ids = ids[:2]
    detached_enc_idx = next(
        i for i, enc in enumerate(detached['encounters'])
        if enc.get('photo_ids') == detached_photo_ids
    )
    updated = copy.deepcopy(detached)
    updated['encounters'][detached_enc_idx]['bursts'][0]['species_override'] = None
    save_results_raw(updated, os.path.dirname(db._db_path), db._ws_id())

    assert client.post('/api/undo').status_code == 200
    restored = _load(db)
    # Structure returns to the pre-detach shape, but the user's clear on
    # that burst is preserved rather than silently overwritten by the
    # detach's stale before-snapshot value.
    assert len(restored['encounters']) == len(original['encounters'])
    matching_burst = next(
        b for b in restored['encounters'][0]['bursts']
        if b.get('photo_ids') == detached_photo_ids
    )
    assert matching_burst.get('species_override') is None


def test_grouping_undo_restores_summary_counts(app_and_db):
    """Restored encounter/burst counts stay in sync with the restored groups.

    ``/api/pipeline/page-init`` reads structural counts off ``summary``
    and the review page's ``updateSummaryBar()`` keeps them when the
    normal ``confirmed_count`` fields are present, so a stale summary
    left behind after undo shows the wrong encounter/burst totals.
    """
    app, db = app_and_db
    client = app.test_client()
    _seed(db)
    _detach(client)
    after_detach = _load(db)
    assert after_detach['summary']['encounter_count'] == 2
    assert after_detach['summary']['burst_count'] == 2

    assert client.post('/api/undo').status_code == 200
    restored = _load(db)
    assert restored['summary']['encounter_count'] == 1
    assert restored['summary']['burst_count'] == 2

    assert client.post('/api/redo').status_code == 200
    replayed = _load(db)
    assert replayed['summary']['encounter_count'] == 2
    assert replayed['summary']['burst_count'] == 2


def test_grouping_undo_tolerates_reverted_species_cache_state(app_and_db):
    """A species edit's leftover cache does not block a later grouping undo.

    ``/api/encounters/species`` writes ``confirmed_species`` /
    ``species_override`` into the cache but records its DB revert as an
    ordinary undoable edit. After that species edit is undone, its cache
    state remains — undo is strict LIFO, so a subsequent grouping undo is
    not overwriting active newer work and must succeed.
    """
    app, db = app_and_db
    client = app.test_client()
    ids, original = _seed(db)
    _detach(client)
    detached = _load(db)
    updated = copy.deepcopy(detached)
    updated['encounters'][0]['confirmed_species'] = 'New species'
    updated['encounters'][0]['species_confirmed'] = True
    updated['encounters'][0]['bursts'][0]['species_override'] = {
        'species': 'New species', 'confirmed': True,
    }
    save_results_raw(updated, os.path.dirname(db._db_path), db._ws_id())
    response = client.post('/api/undo')
    assert response.status_code == 200, response.get_json()
    assert _load(db)['encounters'] == original['encounters']


@pytest.mark.parametrize('operation', ['undo', 'redo'])
def test_failed_grouping_write_can_be_retried(app_and_db, monkeypatch, operation):
    app, db = app_and_db
    client = app.test_client()
    _seed(db)
    _detach(client)
    if operation == 'redo':
        assert client.post('/api/undo').status_code == 200
    before = copy.deepcopy(_load(db))
    with monkeypatch.context() as patch:
        def fail(*args, **kwargs):
            raise OSError('Disk unavailable')
        patch.setattr('pipeline.save_results_raw', fail)
        assert client.post('/api/' + operation).status_code == 500
    assert _load(db) == before
    assert client.get('/api/' + operation + '/status').json['available'] is True
    assert client.post('/api/' + operation).status_code == 200


def test_new_grouping_edit_clears_redo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    _detach(client)
    assert client.post('/api/undo').status_code == 200
    _detach(client, 'photo', photo_id=ids[0])
    assert client.get('/api/redo/status').json['available'] is False


def test_processing_lock_does_not_stall_undo_or_consume_history(app_and_db):
    from pipeline_locks import acquire_workspace_regroup

    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    _detach(client)
    client.post(f'/api/photos/{ids[0]}/flag', json={'flag': 'flagged'})
    with acquire_workspace_regroup(db._ws_id()):
        # Ordinary edits can still be undone during processing.
        assert client.post('/api/undo').status_code == 200
        # Grouping undo fails promptly and is retryable when the lock releases.
        assert client.post('/api/undo').status_code == 409
        assert client.get('/api/undo/status').json['count'] == 1
    assert client.post('/api/undo').status_code == 200


def test_cache_only_species_confirm_is_undone_before_older_grouping_edit(app_and_db):
    """A cache-only encounter confirmation is undoable in LIFO with a detach.

    ``/api/encounters/species`` writes ``confirmed_species`` /
    ``species_confirmed`` into the cache regardless of whether any photo's
    keyword tags change. When every submitted photo already carries the
    requested species keyword (``newly_tagged`` is empty), no
    ``keyword_add`` history row would be recorded. Without a dedicated
    entry, a preceding grouping edit stays newest and its undo silently
    discards the still-active confirmation because grouping signatures
    strip these species fields by design.

    A ``species_confirm_cache`` entry restores LIFO order: the first undo
    reverts the cache confirmation, the second undo reverts the detach
    with pristine species state.
    """
    app, db = app_and_db
    client = app.test_client()
    ids, original = _seed(db)
    kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ? COLLATE NOCASE", ('Cardinal',),
    ).fetchone()[0]
    # Pre-tag every seeded photo with Cardinal so a subsequent confirmation
    # produces no ``newly_tagged`` and no ``keyword_add`` history row.
    for pid in ids:
        # Already-tagged rows raise; ignore.
        with contextlib.suppress(Exception):
            db.tag_photo(pid, kid, source='manual')
    db.conn.commit()

    _detach(client)
    detached = _load(db)
    # Locate the pre-detach parent encounter that survived (still holds the
    # remaining burst) and confirm species on it. All its photos already
    # carry Cardinal, so this write is cache-only.
    parent_enc = next(
        enc for enc in detached['encounters']
        if len(enc.get('bursts') or []) > 0
        and set(enc.get('photo_ids') or []) != set(ids[:2])
    )
    response = client.post('/api/encounters/species', json={
        'species': 'Cardinal',
        'photo_ids': parent_enc['photo_ids'],
    })
    assert response.status_code == 200, response.get_json()

    confirmed = _load(db)
    matched = next(
        enc for enc in confirmed['encounters']
        if enc.get('photo_ids') == parent_enc['photo_ids']
    )
    assert matched.get('confirmed_species') == 'Cardinal'
    assert matched.get('species_confirmed') is True
    # Both edits are undoable: the detach and the cache-only confirmation.
    assert client.get('/api/undo/status').json['count'] == 2

    # First undo reverts the cache confirmation, not the detach.
    response = client.post('/api/undo')
    assert response.status_code == 200, response.get_json()
    after_first_undo = _load(db)
    reverted = next(
        enc for enc in after_first_undo['encounters']
        if enc.get('photo_ids') == parent_enc['photo_ids']
    )
    assert reverted.get('confirmed_species') is None
    assert not reverted.get('species_confirmed')
    # Structure still shows the detach — its history entry is next in line.
    assert len(after_first_undo['encounters']) == len(detached['encounters'])

    # Second undo reverts the detach itself.
    response = client.post('/api/undo')
    assert response.status_code == 200, response.get_json()
    assert _load(db)['encounters'] == original['encounters']

    # Both redos replay in order.
    assert client.post('/api/redo').status_code == 200
    assert client.post('/api/redo').status_code == 200
    replayed = _load(db)
    replayed_match = next(
        enc for enc in replayed['encounters']
        if enc.get('photo_ids') == parent_enc['photo_ids']
    )
    assert replayed_match.get('confirmed_species') == 'Cardinal'
    assert replayed_match.get('species_confirmed') is True


def test_cache_only_burst_confirm_that_auto_detaches_is_undoable(app_and_db):
    """A cache-only burst confirmation that triggers auto-detach records history.

    When every photo in a multi-burst encounter already carries a species
    different from the encounter's confirmed species, ``/api/encounters/species``
    would restructure the cache via ``auto_detach_burst_for_species`` without
    recording any keyword change. Before this fix the structural change was
    invisible to undo, so pressing Undo popped an unrelated older edit.
    The route now persists the auto-detach as a grouping edit so undo
    reverses the burst move and re-attaches it to its original encounter.
    """
    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    # Tag every photo with Cardinal so the first burst carries a species
    # that differs from the encounter's confirmed one.
    kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ? COLLATE NOCASE", ('Cardinal',),
    ).fetchone()[0]
    for pid in ids:
        with contextlib.suppress(Exception):
            db.tag_photo(pid, kid, source='manual')
    db.conn.commit()
    # Seed the cache so the encounter is confirmed as Sparrow — the confirm
    # below asks for Cardinal on burst 0, which triggers auto-detach.
    seeded = _load(db)
    seeded['encounters'][0]['confirmed_species'] = 'Sparrow'
    seeded['encounters'][0]['species_confirmed'] = True
    save_results_raw(seeded, os.path.dirname(db._db_path), db._ws_id())

    burst_photo_ids = ids[:2]
    response = client.post('/api/encounters/species', json={
        'species': 'Cardinal',
        'photo_ids': burst_photo_ids,
        'burst_index': 0,
    })
    assert response.status_code == 200, response.get_json()
    after_confirm = _load(db)
    # Auto-detach split the burst into its own encounter with Cardinal.
    assert len(after_confirm['encounters']) == 2
    assert client.get('/api/undo/status').json['count'] == 1

    assert client.post('/api/undo').status_code == 200, \
        'auto-detach must be reversible via undo'
    restored = _load(db)
    assert len(restored['encounters']) == 1
    assert sorted(restored['encounters'][0]['photo_ids']) == sorted(ids)
    assert restored['encounters'] == seeded['encounters']

    assert client.post('/api/redo').status_code == 200
    replayed = _load(db)
    assert replayed['encounters'] == after_confirm['encounters']


def test_cache_only_confirm_rolls_back_when_cache_write_fails(
    app_and_db, monkeypatch,
):
    """A failed cache save rolls back the species_confirm_cache history row.

    ``record_species_confirm_cache`` is committed with the rest of the
    request. If ``save_results_raw`` fails afterward, the DB row must roll
    back too — otherwise Undo would report a change that never landed on
    disk, and Redo could re-apply a confirmation the user never saw.
    """
    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ? COLLATE NOCASE", ('Cardinal',),
    ).fetchone()[0]
    for pid in ids:
        with contextlib.suppress(Exception):
            db.tag_photo(pid, kid, source='manual')
    db.conn.commit()

    before_count = client.get('/api/undo/status').json['count']
    before_cache = _load(db)
    with monkeypatch.context() as patch:
        def fail(*args, **kwargs):
            raise OSError('Disk full')
        patch.setattr('pipeline.save_results_raw', fail)
        response = client.post('/api/encounters/species', json={
            'species': 'Cardinal',
            'photo_ids': ids,
        })
        assert response.status_code == 500
    # The failed request must not leave a species_confirm_cache entry
    # sitting in undo history for a change that never happened.
    assert client.get('/api/undo/status').json['count'] == before_count
    assert _load(db) == before_cache


@pytest.mark.parametrize("undo_before_clear", [False, True])
@pytest.mark.parametrize("burst", [False, True])
def test_confirm_history_preserves_cleared_override(app_and_db, undo_before_clear, burst):
    """Undoing a cache-only burst confirmation preserves a later clear.

    After a cache-only burst confirmation, ``clearBurstOverride`` can
    write ``species_override = None`` through ``/api/pipeline/save-cache``
    without a history entry. Undoing the still-latest confirmation must
    not blindly restore the recorded previous override — that would
    silently resurrect a state the user cleared.
    """
    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ? COLLATE NOCASE", ('Cardinal',),
    ).fetchone()[0]
    for pid in ids:
        with contextlib.suppress(Exception):
            db.tag_photo(pid, kid, source='manual')
    db.conn.commit()
    # Seed a burst-level previous override so undo of the confirmation
    # would (before the fix) restore it and overwrite the user's clear.
    seeded = _load(db)
    if burst:
        seeded['encounters'][0]['bursts'][0]['species_override'] = {
            'species': 'Old', 'confirmed': True,
        }
    else:
        seeded['encounters'][0].update(confirmed_species='Old', species_confirmed=True)
    save_results_raw(seeded, os.path.dirname(db._db_path), db._ws_id())
    payload = {'species': 'Cardinal', 'photo_ids': ids[:2] if burst else ids}
    if burst:
        payload['burst_index'] = 0
    response = client.post('/api/encounters/species', json=payload)
    assert response.status_code == 200, response.get_json()
    if undo_before_clear:
        assert client.post('/api/undo').status_code == 200

    cleared = _load(db)
    if burst:
        cleared['encounters'][0]['bursts'][0]['species_override'] = None
    else:
        cleared['encounters'][0].update(confirmed_species=None, species_confirmed=False)
    save_results_raw(cleared, os.path.dirname(db._db_path), db._ws_id())
    cleared = _load(db)

    # A superseded confirmation is retired, never made redoable by a no-op undo.
    direction = 'redo' if undo_before_clear else 'undo'
    assert client.post('/api/' + direction).status_code == 409
    assert _load(db) == cleared
    assert client.get('/api/undo/status').json['available'] is False
    assert client.post('/api/redo').status_code == 400
    assert _load(db) == cleared


def test_grouping_history_is_workspace_scoped(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    _seed(db)
    _detach(client)
    original_workspace = db._ws_id()
    other = db.create_workspace('Another trip')
    # The app's request connections follow the active workspace stored in meta.
    assert client.post(f'/api/workspaces/{other}/activate', json={}).status_code == 200
    assert client.get('/api/undo/status').json['available'] is False
    assert client.post(f'/api/workspaces/{original_workspace}/activate', json={}).status_code == 200
    assert client.get('/api/undo/status').json['available'] is True
    assert client.post('/api/undo').status_code == 200


@pytest.mark.parametrize('cache_only', [False, True])
@pytest.mark.parametrize('auto_detach', [False, True])
def test_species_confirmation_restores_cache_on_commit_failure(
    app_and_db, monkeypatch, cache_only, auto_detach,
):
    """A failed DB commit restores the complete pre-confirmation cache."""
    from db import Database

    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    if cache_only:
        kid = db.add_keyword('Cardinal', is_species=True)
        for pid in ids:
            db.tag_photo(pid, kid)
    before = _load(db)
    if auto_detach:
        before['encounters'][0]['confirmed_species'] = 'Sparrow'
        before['encounters'][0]['species_confirmed'] = True
    save_results_raw(before, os.path.dirname(db._db_path), db._ws_id())
    keywords_before = {pid: db.get_photo_keywords(pid) for pid in ids}
    history_before = db.edit_history.list_recent()
    pending_before = [dict(row) for row in db.conn.execute('SELECT * FROM pending_changes')]
    original_init = Database.__init__

    class FailCommit:
        def __init__(self, conn):
            self.conn = conn

        def __getattr__(self, name):
            return getattr(self.conn, name)

        def commit(self):
            raise OSError('Commit failed')

    with monkeypatch.context() as patch:
        def fail_commit_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            self.conn = FailCommit(self.conn)

        patch.setattr(Database, '__init__', fail_commit_init)
        payload = {'species': 'Cardinal', 'photo_ids': ids[:2] if auto_detach else ids}
        if auto_detach:
            payload['burst_index'] = 0
        response = client.post('/api/encounters/species', json=payload)
        assert response.status_code == 500
    assert _load(db) == before
    assert db.edit_history.list_recent() == history_before
    assert {pid: db.get_photo_keywords(pid) for pid in ids} == keywords_before
    assert [dict(row) for row in db.conn.execute('SELECT * FROM pending_changes')] == pending_before
    assert client.post('/api/encounters/species', json=payload).status_code == 200


def test_species_confirmation_restores_cache_when_rollback_also_fails(
    app_and_db, monkeypatch, caplog,
):
    """A failed rollback still restores the cache and surfaces the commit error."""
    from db import Database

    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    before = _load(db)
    original_init = Database.__init__

    class FailCommitAndRollback:
        def __init__(self, conn):
            self.conn = conn

        def __getattr__(self, name):
            return getattr(self.conn, name)

        def commit(self):
            raise OSError('Commit failed')

        def rollback(self):
            raise RuntimeError('Rollback failed')

    with monkeypatch.context() as patch:
        def failing_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            self.conn = FailCommitAndRollback(self.conn)

        patch.setattr(Database, '__init__', failing_init)
        response = client.post('/api/encounters/species', json={
            'species': 'Cardinal', 'photo_ids': ids,
        })
        assert response.status_code == 500
    assert _load(db) == before
    assert 'Rollback failed after species confirmation failed' in caplog.text
    assert 'Commit failed' in caplog.text


@pytest.mark.parametrize('replacement', [False, True])
def test_keyword_changing_auto_detach_is_one_atomic_history_action(app_and_db, replacement):
    app, db = app_and_db
    client = app.test_client()
    ids, before = _seed(db)
    before['encounters'][0].update(confirmed_species='Sparrow', species_confirmed=True)
    if replacement:
        old_kid = db.add_keyword('Sparrow', is_species=True)
        for pid in ids[:2]:
            db.tag_photo(pid, old_kid)
    save_results_raw(before, os.path.dirname(db._db_path), db._ws_id())
    keywords_before = {pid: [row["id"] for row in db.get_photo_keywords(pid)] for pid in ids}
    assert client.post('/api/encounters/species', json={
        'species': 'Cardinal', 'photo_ids': ids[:2], 'burst_index': 0,
    }).status_code == 200
    after = _load(db)
    keywords_after = {pid: [row["id"] for row in db.get_photo_keywords(pid)] for pid in ids}
    assert len(after['encounters']) == 2
    assert client.get('/api/undo/status').json['count'] == 1
    assert client.post('/api/undo').status_code == 200
    assert _load(db)['encounters'] == before['encounters']
    assert {pid: [row["id"] for row in db.get_photo_keywords(pid)] for pid in ids} == keywords_before
    assert client.post('/api/redo').status_code == 200
    assert _load(db)['encounters'] == after['encounters']
    assert {pid: [row["id"] for row in db.get_photo_keywords(pid)] for pid in ids} == keywords_after


@pytest.mark.parametrize('change_flags', [False, True])
def test_group_review_removal_and_flags_share_one_history_action(app_and_db, change_flags):
    app, db = app_and_db
    client = app.test_client()
    ids, before = _seed(db)
    payload = {
        'picks': [ids[0]] if change_flags else [], 'rejects': [],
        'candidates': [] if change_flags else [ids[0]], 'removed': [ids[1]],
        'encounter_index': 0, 'burst_index': 0, 'expected_burst_photo_ids': ids[:2],
    }
    response = client.post('/api/pipeline/group/apply', json=payload)
    assert response.status_code == 200, response.json
    after = _load(db)
    assert after['encounters'][0]['burst_count'] == 3
    assert client.get('/api/undo/status').json['count'] == 1
    assert client.post('/api/undo').status_code == 200
    assert _load(db)['encounters'] == before['encounters']
    assert db.get_photo(ids[0])['flag'] == 'none'
    assert client.post('/api/redo').status_code == 200
    assert _load(db)['encounters'] == after['encounters']
    assert db.get_photo(ids[0])['flag'] == ('flagged' if change_flags else 'none')


def test_delayed_cache_snapshot_cannot_overwrite_undo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    _seed(db)
    _detach(client)
    delayed = _load(db)
    assert client.post('/api/undo').status_code == 200
    restored = _load(db)
    assert client.post('/api/pipeline/save-cache', json=delayed).status_code == 409
    assert _load(db) == restored
    assert client.get('/api/redo/status').json['available'] is True


def test_override_clear_is_checked_and_undoable(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, before = _seed(db)
    override = {'species': 'Cardinal', 'confirmed': True}
    before['encounters'][0]['bursts'][0]['species_override'] = override
    save_results_raw(before, os.path.dirname(db._db_path), db._ws_id())
    payload = {'clear_override': {
        'encounter_photo_ids': ids, 'burst_photo_ids': ids[:2], 'expected_override': override,
    }}
    assert client.post('/api/pipeline/save-cache', json=payload).status_code == 200
    assert _load(db)['encounters'][0]['bursts'][0]['species_override'] is None
    assert client.post('/api/pipeline/save-cache', json=payload).status_code == 409
    assert client.post('/api/undo').status_code == 200
    assert _load(db)['encounters'] == before['encounters']
    assert client.post('/api/redo').status_code == 200
    assert _load(db)['encounters'][0]['bursts'][0]['species_override'] is None


@pytest.mark.parametrize('undo', [False, True])
def test_combined_history_failure_rolls_back_photos_and_cache(app_and_db, monkeypatch, undo):
    from services import grouping_history

    app, db = app_and_db
    client = app.test_client()
    ids, before = _seed(db)
    before['encounters'][0].update(confirmed_species='Sparrow', species_confirmed=True)
    before['encounters'][0]['bursts'][0]['species_override'] = None
    save_results_raw(before, os.path.dirname(db._db_path), db._ws_id())
    assert client.post('/api/encounters/species', json={
        'species': 'Cardinal', 'photo_ids': ids[:2], 'burst_index': 0,
    }).status_code == 200
    if not undo:
        assert client.post('/api/undo').status_code == 200
    cache_before = _load(db)
    keywords_before = {pid: list(db.get_photo_keywords(pid)) for pid in ids}
    pending_before = list(db.conn.execute('SELECT * FROM pending_changes'))
    history_before = list(db.conn.execute('SELECT * FROM edit_history'))
    apply = grouping_history.apply_grouping_photo_edit

    def fail_after_photo_write(*args, **kwargs):
        apply(*args, **kwargs)
        raise OSError('Database write failed')

    route = '/api/undo' if undo else '/api/redo'
    with monkeypatch.context() as patch:
        patch.setattr(grouping_history, 'apply_grouping_photo_edit', fail_after_photo_write)
        assert client.post(route).status_code == 500
    assert _load(db) == cache_before
    assert {pid: list(db.get_photo_keywords(pid)) for pid in ids} == keywords_before
    assert list(db.conn.execute('SELECT * FROM pending_changes')) == pending_before
    assert list(db.conn.execute('SELECT * FROM edit_history')) == history_before
    assert client.post(route).status_code == 200
    if not undo:
        assert _load(db)['encounters'][-1]['bursts'][0]['species_override']['confirmed'] is True


def test_pipeline_results_refresh_species_from_database_after_confirm_and_undo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, cached = _seed(db)
    for pid in ids:
        for keyword in db.get_photo_keywords(pid):
            db.untag_photo(pid, keyword['id'])
    for photo in cached['photos']:
        photo['confirmed_species'] = 'Stale cached label'
    save_results_raw(cached, os.path.dirname(db._db_path), db._ws_id())
    assert all(p['confirmed_species'] is None for p in client.get('/api/pipeline/results').json['photos'])
    assert client.post('/api/encounters/species', json={
        'species': 'Cardinal', 'photo_ids': ids,
    }).status_code == 200
    assert all(p['confirmed_species'] == 'Cardinal' for p in client.get('/api/pipeline/results').json['photos'])
    assert client.post('/api/undo').status_code == 200
    assert all(p['confirmed_species'] is None for p in client.get('/api/pipeline/results').json['photos'])
    assert client.post('/api/redo').status_code == 200
    assert all(p['confirmed_species'] == 'Cardinal' for p in client.get('/api/pipeline/results').json['photos'])


def test_group_flags_leave_pipeline_suggestions_unchanged(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, cached = _seed(db)
    cached['photos'][0]['label'] = 'KEEP'
    cached['photos'][1]['label'] = 'REJECT'
    save_results_raw(cached, os.path.dirname(db._db_path), db._ws_id())
    expected = {p['id']: p['label'] for p in cached['photos']}
    assert client.post('/api/pipeline/group/apply', json={
        'picks': [ids[1]], 'rejects': [ids[0]], 'candidates': [],
    }).status_code == 200
    for operation in (None, '/api/undo', '/api/redo'):
        if operation:
            assert client.post(operation).status_code == 200
        assert {p['id']: p['label'] for p in _load(db)['photos']} == expected
        for endpoint in ('/api/pipeline/results', '/api/pipeline/page-init'):
            result = client.get(endpoint).json
            photos = result['results']['photos'] if 'results' in result else result['photos']
            assert {p['id']: p['label'] for p in photos} == expected


@pytest.mark.parametrize('undo', [False, True])
def test_stale_combined_photo_history_retains_writer_lock(app_and_db, monkeypatch, undo):
    import sqlite3

    from services import grouping_history

    app, db = app_and_db
    client = app.test_client()
    ids, before = _seed(db)
    before['encounters'][0].update(confirmed_species='Sparrow', species_confirmed=True)
    save_results_raw(before, os.path.dirname(db._db_path), db._ws_id())
    assert client.post('/api/encounters/species', json={
        'species': 'Cardinal', 'photo_ids': ids[:2], 'burst_index': 0,
    }).status_code == 200
    newer = _load(db)
    newer['encounters'][0]['bursts'].append({'photo_ids': []})
    save_results_raw(newer, os.path.dirname(db._db_path), db._ws_id())
    assert client.post('/api/undo').status_code == 409
    assert client.get('/api/undo/status').json['description'].startswith('Photo changes from:')
    if not undo:
        assert client.post('/api/undo').status_code == 200
    apply = grouping_history.apply_grouping_photo_edit
    checked = []

    def check_after_photo_write(request_db, entry, items, **kwargs):
        apply(request_db, entry, items, **kwargs)
        assert request_db.conn.in_transaction
        competitor = sqlite3.connect(db._db_path, timeout=0)
        try:
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                competitor.execute('BEGIN IMMEDIATE')
        finally:
            competitor.rollback()
            competitor.close()
        checked.append(True)

    monkeypatch.setattr(grouping_history, 'apply_grouping_photo_edit', check_after_photo_write)
    response = client.post('/api/undo' if undo else '/api/redo')
    assert response.status_code == 200
    assert checked == [True]
    assert _load(db) == newer


@pytest.mark.parametrize('burst', [False, True])
@pytest.mark.parametrize('replacement', [False, True])
def test_species_confirmation_without_split_restores_labels_and_keywords(app_and_db, burst, replacement):
    app, db = app_and_db
    client = app.test_client()
    ids, before = _seed(db)
    before['encounters'][0]['bursts'] = [{'photo_ids': ids, 'species_override': None}]
    before['encounters'][0]['burst_count'] = 1
    if replacement:
        kid = db.add_keyword('Sparrow', is_species=True)
        for pid in ids:
            db.tag_photo(pid, kid)
        before['encounters'][0].update(confirmed_species='Sparrow', species_confirmed=True)
    save_results_raw(before, os.path.dirname(db._db_path), db._ws_id())
    before = _load(db)
    keywords_before = {pid: [k['id'] for k in db.get_photo_keywords(pid)] for pid in ids}
    payload = {'species': 'Cardinal', 'photo_ids': ids}
    if burst:
        payload['burst_index'] = 0
    assert client.post('/api/encounters/species', json=payload).status_code == 200
    after = _load(db)
    assert len(after['encounters']) == 1
    keywords_after = {pid: [k['id'] for k in db.get_photo_keywords(pid)] for pid in ids}
    assert client.get('/api/undo/status').json['count'] == 1
    assert client.post('/api/undo').status_code == 200
    assert _load(db)['encounters'] == before['encounters']
    restored = client.get('/api/pipeline/page-init').json['results']
    assert restored['summary']['confirmed_count'] == before['summary']['confirmed_count']
    assert {pid: [k['id'] for k in db.get_photo_keywords(pid)] for pid in ids} == keywords_before
    assert client.post('/api/redo').status_code == 200
    assert _load(db)['encounters'] == after['encounters']
    assert {pid: [k['id'] for k in db.get_photo_keywords(pid)] for pid in ids} == keywords_after


def test_legacy_burst_photo_detach_is_undoable_and_clear_is_rejected(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, before = _seed(db)
    before['encounters'][0]['bursts'] = [ids[:2], ids[2:]]
    save_results_raw(before, os.path.dirname(db._db_path), db._ws_id())
    assert client.post('/api/pipeline/save-cache', json={'clear_override': {
        'encounter_photo_ids': ids, 'burst_photo_ids': ids[:2], 'expected_override': None,
    }}).status_code == 409
    _detach(client, 'photo', photo_id=ids[0])
    assert client.post('/api/undo').status_code == 200
    assert _load(db)['encounters'] == before['encounters']
    assert client.post('/api/redo').status_code == 200
    assert _load(db)['encounters'][0]['burst_count'] == 3


def test_stale_redo_reports_retirement_before_replaying_next_action(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    _detach(client)
    client.post('/api/batch/flag', json={'photo_ids': ids, 'flag': 'flagged'})
    assert client.post('/api/undo').status_code == 200
    assert client.post('/api/undo').status_code == 200
    newer = _load(db)
    newer['encounters'][0]['bursts'].append({'photo_ids': []})
    save_results_raw(newer, os.path.dirname(db._db_path), db._ws_id())
    assert 'detach' in client.get('/api/redo/status').json['description'].lower()
    assert client.post('/api/redo').status_code == 409
    assert all(db.get_photo(pid)['flag'] == 'none' for pid in ids)
    assert _load(db) == newer
    assert 'flag' in client.get('/api/redo/status').json['description'].lower()
    assert client.post('/api/redo').status_code == 200
    assert all(db.get_photo(pid)['flag'] == 'flagged' for pid in ids)
    assert _load(db) == newer


def _grouping_rows(db):
    return db.conn.execute(
        "SELECT eh.id, eh.new_value, p.payload "
        "FROM edit_history eh LEFT JOIN edit_history_payloads p ON p.edit_id = eh.id "
        "WHERE eh.action_type = 'pipeline_grouping' ORDER BY eh.id"
    ).fetchall()


def test_grouping_snapshot_is_stored_outside_edit_history(app_and_db):
    """The multi-MB before/after snapshot never lands in edit_history.new_value.

    Kept inline it turns every edit_history scan (undo status after each
    flag, the prune inside record_edit) into a walk of the snapshot blobs.
    """
    import json
    app, db = app_and_db
    client = app.test_client()
    ids, original = _seed(db)
    _detach(client)
    after = _load(db)

    rows = _grouping_rows(db)
    assert len(rows) == 1
    meta = json.loads(rows[0]['new_value'])
    assert 'before' not in meta and 'after' not in meta
    snapshot = json.loads(rows[0]['payload'])
    assert snapshot['before'] == original['encounters']
    assert snapshot['after'] == after['encounters']

    # The status poll and the history list never need the snapshot.
    listed = client.get('/api/edit-history').json
    assert listed[0]['new_value'] is None
    assert client.get('/api/undo/status').json['id'] == rows[0]['id']

    # Undo/redo restore from the payload table.
    assert client.post('/api/undo').status_code == 200
    assert _load(db)['encounters'] == original['encounters']
    assert client.post('/api/redo').status_code == 200
    assert _load(db)['encounters'] == after['encounters']

    # A new edit clears the redo stack; its snapshot goes with the row.
    assert client.post('/api/undo').status_code == 200
    _detach(client, 'photo', photo_id=ids[0])
    rows = _grouping_rows(db)
    assert len(rows) == 1
    assert db.conn.execute(
        "SELECT COUNT(*) FROM edit_history_payloads"
    ).fetchone()[0] == 1


def test_species_confirm_grouping_edit_stores_snapshot_outside_row(app_and_db):
    """The species route's combined photo+grouping edit uses the same split."""
    import json
    app, db = app_and_db
    client = app.test_client()
    ids, original = _seed(db)
    cache = _load(db)
    cache['encounters'][0]['bursts'][0]['species_override'] = None
    save_results_raw(cache, os.path.dirname(db._db_path), db._ws_id())
    response = client.post('/api/encounters/species', json={
        'encounter_index': 0, 'burst_index': 0, 'species': 'Verdin',
        'photo_ids': ids[:2],
    })
    assert response.status_code == 200, response.get_json()
    rows = _grouping_rows(db)
    # The photo edit plus the burst-override write must be recorded as one
    # pipeline_grouping row; a skip here would hide that conversion regressing.
    assert rows
    for row in rows:
        meta = json.loads(row['new_value'])
        assert 'before' not in meta and 'after' not in meta
        assert row['payload'] is not None
        assert set(json.loads(row['payload'])) == {'before', 'after'}


def test_retired_stale_grouping_entry_drops_its_snapshot(app_and_db):
    """Retiring a stale combined edit keeps the photo half, frees the blob."""
    import json
    app, db = app_and_db
    client = app.test_client()
    ids, _ = _seed(db)
    client.post('/api/batch/flag', json={'photo_ids': ids, 'flag': 'flagged'})
    _detach(client)
    (row,) = _grouping_rows(db)
    assert row['payload'] is not None
    # Simulate a combined photo+grouping edit so retirement rewrites rather
    # than deletes the row.
    from services.grouping_history import convert_to_grouping_edit
    convert_to_grouping_edit(db, row['id'], {
        **json.loads(row['payload']),
        'photo_edit': {'action_type': 'flag', 'new_value': 'flagged'},
    })
    db.conn.commit()
    updated = _load(db)
    updated['encounters'][0]['bursts'].append({'photo_ids': [ids[0]]})
    save_results_raw(updated, os.path.dirname(db._db_path), db._ws_id())

    assert client.post('/api/undo').status_code == 409
    retired = db.conn.execute(
        "SELECT new_value FROM edit_history WHERE id = ?", (row['id'],),
    ).fetchone()
    assert json.loads(retired['new_value'])['photo_only'] is True
    assert db.conn.execute(
        "SELECT COUNT(*) FROM edit_history_payloads WHERE edit_id = ?", (row['id'],),
    ).fetchone()[0] == 0


def test_legacy_inline_snapshot_still_restores(app_and_db):
    """Rows written before the split (snapshot inline) undo without migration."""
    import json
    app, db = app_and_db
    client = app.test_client()
    ids, original = _seed(db)
    _detach(client)
    after = _load(db)
    (row,) = _grouping_rows(db)
    inline = {**json.loads(row['new_value']), **json.loads(row['payload'])}
    db.conn.execute(
        "UPDATE edit_history SET new_value = ? WHERE id = ?",
        (json.dumps(inline), row['id']),
    )
    db.conn.execute("DELETE FROM edit_history_payloads WHERE edit_id = ?", (row['id'],))
    db.conn.commit()

    assert client.post('/api/undo').status_code == 200
    assert _load(db)['encounters'] == original['encounters']
    assert client.post('/api/redo').status_code == 200
    assert _load(db)['encounters'] == after['encounters']
