"""``PhotoLabelService``: a label change and its undo entry are one unit of work.

Before the unit of work the label committed on its own and the history entry
committed after it, so a failed history write left a label Undo could not
reverse. Now both land together or neither does.
"""

import sqlite3
from contextlib import closing

import pytest
from services import photo_ingest, workspace_folders
from services.photo_labels import PhotoLabelService


@pytest.fixture
def photo_ids(db):
    folder = workspace_folders.add_folder(db, "/photos/labels", name="labels")
    ids = [
        photo_ingest.add_photo(db, folder_id=folder, filename=f"{i}.jpg", extension=".jpg",
                               file_size=100, file_mtime=1.0)
        for i in range(2)
    ]
    db.commit()
    return ids


def _committed_labels(db):
    with closing(sqlite3.connect(db._db_path)) as other:
        return dict(other.execute("SELECT photo_id, color FROM photo_color_labels").fetchall())


def _history_count(db):
    with closing(sqlite3.connect(db._db_path)) as other:
        return other.execute("SELECT COUNT(*) FROM edit_history").fetchone()[0]


def _fail(*_args, **_kwargs):
    raise sqlite3.OperationalError("disk I/O error")


def test_set_label_commits_label_and_history_together(db, photo_ids):
    PhotoLabelService(db).set_label(photo_ids[0], "red")
    assert _committed_labels(db) == {photo_ids[0]: "red"}
    assert _history_count(db) == 1
    assert not db.in_transaction


def test_a_failed_history_write_leaves_no_label(db, photo_ids, monkeypatch):
    monkeypatch.setattr(db, "record_edit", _fail)
    with pytest.raises(sqlite3.OperationalError):
        PhotoLabelService(db).set_label(photo_ids[0], "red")
    assert _committed_labels(db) == {}
    assert db.photo_labels.get(photo_ids[0]) is None


def test_a_failed_batch_history_write_leaves_no_labels(db, photo_ids, monkeypatch):
    monkeypatch.setattr(db, "record_edit", _fail)
    with pytest.raises(sqlite3.OperationalError):
        PhotoLabelService(db).set_labels(photo_ids, "blue")
    assert _committed_labels(db) == {}
