"""Queue resume never replays flags; explicit changes compare the displayed flag."""
import pytest
from services import workspace_folders
from services.photo_review import FlagConflict, PhotoReviewService


def test_stale_review_flag_preserves_concurrent_flag_and_history(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()
    before = client.get(f'/api/photos/{photo_id}').get_json()
    assert before['flag'] == 'none'
    assert client.post(f'/api/photos/{photo_id}/flag', json={'flag': 'flagged'}).status_code == 200
    response = client.post(f'/api/photos/{photo_id}/flag', json={
        'flag': 'rejected', 'observed_flag': before['flag'],
    })
    assert response.status_code == 409
    assert response.get_json()['code'] == 'flag_conflict'
    assert client.get(f'/api/photos/{photo_id}').get_json()['flag'] == 'flagged'
    edits = db.edit_history.list_recent()
    assert len([e for e in edits if e['action_type'] == 'flag']) == 1


def test_matching_flag_allows_explicit_change_and_undo(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.post(f'/api/photos/{photo_id}/flag', json={'flag': 'rejected'})
    response = client.post(f'/api/photos/{photo_id}/flag', json={
        'flag': 'flagged', 'observed_flag': 'rejected',
    })
    assert response.status_code == 200
    assert client.get(f'/api/photos/{photo_id}').get_json()['flag'] == 'flagged'
    latest = db.edit_history.list_recent()[0]
    assert latest['action_type'] == 'flag'
    assert db.conn.execute('SELECT old_value FROM edit_history_items WHERE edit_id=?', (latest['id'],)).fetchone()[0] == 'rejected'
    db.undo_last_edit()
    assert db.photos.get(photo_id)['flag'] == 'rejected'


@pytest.mark.parametrize('baseline', [None, True, 4, '', 'accepted', {}, []])
def test_invalid_flag_baseline_is_rejected(client_with_photo, baseline):
    app, _, photo_id = client_with_photo
    client = app.test_client()
    response = client.post(f'/api/photos/{photo_id}/flag', json={'flag': 'rejected', 'observed_flag': baseline})
    assert response.status_code == 400
    assert client.get(f'/api/photos/{photo_id}').get_json()['flag'] == 'none'


def test_flag_compare_occurs_after_writer_lock(client_with_photo, monkeypatch):
    _, db, photo_id = client_with_photo
    original = db._photo_review_repository
    def repository():
        repo = original()
        flag_write = repo.flag_write
        from contextlib import contextmanager
        @contextmanager
        def racing_writer():
            # Another completed writer wins just before this writer enters its lock.
            db.photo_review.set_flag(photo_id, 'flagged')
            with flag_write() as owns_transaction:
                yield owns_transaction
        repo.flag_write = racing_writer
        return repo
    monkeypatch.setattr(db, '_photo_review_repository', repository)
    with pytest.raises(FlagConflict):
        PhotoReviewService(db).set_flag(photo_id, 'rejected', observed_flag='none')
    assert db.photos.get(photo_id)['flag'] == 'flagged'


def test_stale_workspace_does_not_write_even_when_photo_is_shared(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()
    observed_workspace = db.active_workspace_id
    other_workspace = db.create_workspace("Other review")
    workspace_folders.add_workspace_folder(db, other_workspace, db.photos.get(photo_id)['folder_id'])
    assert client.post(f'/api/workspaces/{other_workspace}/activate').status_code == 200
    response = client.post(f'/api/photos/{photo_id}/flag', json={
        'flag': 'rejected', 'observed_flag': 'none', 'observed_workspace_id': observed_workspace,
    })
    assert response.status_code == 409
    assert response.get_json()['code'] == 'workspace_changed'
    assert db.photos.get(photo_id)['flag'] == 'none'


def test_workspace_activation_after_request_db_opens_cannot_write(client_with_photo, monkeypatch):
    from db import Database

    app, db, photo_id = client_with_photo
    observed_workspace = db.active_workspace_id
    other_workspace = db.create_workspace('Concurrent review')
    workspace_folders.add_workspace_folder(db, other_workspace, db.photos.get(photo_id)['folder_id'])
    original = PhotoReviewService.set_flag
    statements = []

    def activate_then_write(service, *args, **kwargs):
        # The request already holds a Database with the old cached workspace.
        assert service.db.active_workspace_id == observed_workspace
        with Database(db._db_path) as concurrent:
            concurrent.workspaces.update(other_workspace, last_opened_at='2999-01-01T00:00:00')
        service.db.conn.set_trace_callback(statements.append)
        return original(service, *args, **kwargs)

    monkeypatch.setattr(PhotoReviewService, 'set_flag', activate_then_write)
    response = app.test_client().post(f'/api/photos/{photo_id}/flag', json={
        'flag': 'rejected', 'observed_flag': 'none', 'observed_workspace_id': observed_workspace,
    })
    assert response.status_code == 409
    assert response.get_json()['code'] == 'workspace_changed'
    assert db.photos.get(photo_id)['flag'] == 'none'
    assert not db.edit_history.list_recent()
    assert db.photo_review.current_flag_note(photo_id) is None
    workspace_read = next(i for i, sql in enumerate(statements) if 'SELECT id FROM workspaces ORDER BY' in sql)
    assert statements.index('BEGIN IMMEDIATE') < workspace_read


def test_matching_workspace_and_flag_guard_allows_write(client_with_photo):
    app, db, photo_id = client_with_photo
    response = app.test_client().post(f'/api/photos/{photo_id}/flag', json={
        'flag': 'flagged', 'observed_flag': 'none', 'observed_workspace_id': db.active_workspace_id,
    })
    assert response.status_code == 200
    assert db.photos.get(photo_id)['flag'] == 'flagged'
    assert len(db.edit_history.list_recent()) == 1
