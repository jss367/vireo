"""A review edit, its sidecar queue row and its undo entry are one unit of work.

Before the unit of work each of these routes committed its edit, then its
queued XMP change, then its history entry, one at a time. A failure between
them left an edit nothing would sync, or one Undo could not reverse. Each test
fails the last write (the history entry) and checks, through a second
connection, that nothing of the edit was kept.
"""

import sqlite3
from contextlib import closing

import pytest
from db import Database


@pytest.fixture
def failing_history(monkeypatch):
    def fail(*_args, **_kwargs):
        raise sqlite3.OperationalError("simulated history failure")

    monkeypatch.setattr(Database, "record_edit", fail)


def _committed(db, sql, *params):
    with closing(sqlite3.connect(db._db_path)) as other:
        return other.execute(sql, params).fetchall()


def _photo_ids(db):
    return [row["id"] for row in db.photos.list_page()]


def _pending(db):
    return _committed(db, "SELECT photo_id, change_type, value FROM pending_changes ORDER BY id")


def test_keyword_add_route_keeps_nothing_when_history_fails(app_and_db, failing_history):
    app, db = app_and_db
    photo = _photo_ids(db)[0]
    before_tags = _committed(db, "SELECT photo_id, keyword_id FROM photo_keywords ORDER BY 1, 2")
    response = app.test_client().post(f"/api/photos/{photo}/keywords", json={"name": "Osprey"})
    assert response.status_code == 500
    after_tags = _committed(db, "SELECT photo_id, keyword_id FROM photo_keywords ORDER BY 1, 2")
    assert after_tags == before_tags
    assert ("keyword_add", "Osprey") not in {(r[1], r[2]) for r in _pending(db)}


def test_keyword_remove_route_keeps_nothing_when_history_fails(app_and_db, failing_history):
    app, db = app_and_db
    photo = _photo_ids(db)[0]
    tags = db.photo_keywords.get_for_photo(photo)
    assert tags
    keyword_id = tags[0]["id"]
    before = _committed(db, "SELECT photo_id, keyword_id FROM photo_keywords ORDER BY 1, 2")
    response = app.test_client().delete(f"/api/photos/{photo}/keywords/{keyword_id}")
    assert response.status_code == 500
    assert _committed(db, "SELECT photo_id, keyword_id FROM photo_keywords ORDER BY 1, 2") == before
    assert _pending(db) == []


def test_wildlife_exclusion_keeps_nothing_when_history_fails(app_and_db, failing_history):
    app, db = app_and_db
    photo = _photo_ids(db)[0]
    response = app.test_client().post(f"/api/photos/{photo}/wildlife_excluded", json={"excluded": True})
    assert response.status_code == 500
    assert _committed(db, "SELECT wildlife_excluded FROM photos WHERE id=?", photo) == [(0,)]


def test_edit_recipe_keeps_nothing_when_history_fails(app_and_db, failing_history):
    app, db = app_and_db
    photo = _photo_ids(db)[0]
    response = app.test_client().put(
        f"/api/photos/{photo}/edit-recipe", json={"recipe": {"adjustments": {"exposure": 0.5}}},
    )
    assert response.status_code == 500
    assert _committed(db, "SELECT COUNT(*) FROM photo_edit_recipes WHERE photo_id=?", photo) == [(0,)]
    assert _pending(db) == []
