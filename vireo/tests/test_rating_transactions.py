"""Ratings, pending XMP changes and undo history are one durable operation."""

import sqlite3
from contextlib import closing

import pytest
from services import photo_ingest, workspace_folders
from services.photo_review import PhotoReviewService


@pytest.fixture
def rated_photos(db):
    folder = workspace_folders.add_folder(db, "/photos", name="photos")
    ids = [photo_ingest.add_photo(db, folder, f"bird{i}.jpg", ".jpg", 100, 1.0) for i in range(2)]
    service = PhotoReviewService(db)
    service.set_ratings(ids, 2)
    service.set_ratings(ids, 3)
    db.undo_last_edit()
    return ids


def _snapshot(conn):
    return {
        table: [tuple(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY id")]
        for table in ("photos", "pending_changes", "edit_history", "edit_history_items")
    }


def _write(service, ids, batch):
    return service.set_ratings(ids, 5) if batch else service.set_rating(ids[0], 5)


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("failure", ["queue_change", "record_edit", "commit"])
def test_failed_rating_restores_photos_sync_history_and_redo(db, rated_photos, monkeypatch, batch, failure):
    before = _snapshot(db.conn)
    target = db.conn if failure == "commit" else db
    original = getattr(target, failure)
    calls = 0

    def fail(*args, **kwargs):
        nonlocal calls
        calls += 1
        if failure != "commit":
            original(*args, **kwargs)
            if failure == "queue_change" and batch and calls == 1:
                return
        raise sqlite3.OperationalError("simulated write failure")

    monkeypatch.setattr(target, failure, fail)
    with pytest.raises(sqlite3.OperationalError, match="simulated write failure"):
        _write(PhotoReviewService(db), rated_photos, batch)
    assert not db.in_transaction
    assert _snapshot(db.conn) == before
    with closing(sqlite3.connect(db._db_path)) as observer:
        assert _snapshot(observer) == before


@pytest.mark.parametrize("batch", [False, True])
def test_rating_preserves_outer_transaction_and_defers_history_pruning(db, rated_photos, monkeypatch, batch):
    before = _snapshot(db.conn)
    prunes = []
    monkeypatch.setattr(db, "prune_edit_history", lambda: prunes.append(True))
    db.begin_immediate()
    _write(PhotoReviewService(db), rated_photos, batch)
    assert db.in_transaction
    assert not prunes
    assert db.photos.get(rated_photos[0])["rating"] == 5
    with closing(sqlite3.connect(db._db_path)) as observer:
        assert _snapshot(observer) == before
    db.rollback()
    assert _snapshot(db.conn) == before


@pytest.mark.parametrize("batch", [False, True])
def test_failed_rating_preserves_unrelated_outer_writes(db, rated_photos, monkeypatch, batch):
    db.begin_immediate()
    db.conn.execute("UPDATE photos SET filename='outer.jpg' WHERE id=?", (rated_photos[0],))
    before = _snapshot(db.conn)

    def fail(*args, **kwargs):
        raise sqlite3.OperationalError("simulated history failure")

    monkeypatch.setattr(db, "record_edit", fail)
    with pytest.raises(sqlite3.OperationalError, match="simulated history failure"):
        _write(PhotoReviewService(db), rated_photos, batch)
    assert db.in_transaction
    assert _snapshot(db.conn) == before
    db.commit()
    assert db.photos.get(rated_photos[0])["filename"] == "outer.jpg"


@pytest.mark.parametrize("batch", [False, True])
def test_successful_rating_can_be_undone_and_redone_with_sync(db, rated_photos, batch):
    _write(PhotoReviewService(db), rated_photos, batch)
    ids = rated_photos if batch else rated_photos[:1]
    for rating, operation in [(5, None), (2, db.undo_last_edit), (5, db.redo_last_undo)]:
        if operation:
            operation()
        assert [db.photos.get(pid)["rating"] for pid in ids] == [rating] * len(ids)
        changes = db.pending_changes.list_all()
        for pid in ids:
            pending = [change for change in changes if change["photo_id"] == pid and change["change_type"] == "rating"]
            assert pending[-1]["value"] == str(rating)


def test_batch_without_existing_photos_preserves_redo(db, rated_photos):
    before = _snapshot(db.conn)
    assert PhotoReviewService(db).set_ratings([2**62], 5) == 0
    assert _snapshot(db.conn) == before


def test_single_rating_outside_workspace_changes_nothing(db, rated_photos):
    db.set_active_workspace(db.create_workspace("Other"))
    before = _snapshot(db.conn)
    with pytest.raises(ValueError, match="does not belong"):
        PhotoReviewService(db).set_rating(rated_photos[0], 5)
    assert _snapshot(db.conn) == before
