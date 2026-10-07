"""Behavior pins for ``photo_exif_search_text``, metadata search's EXIF prefilter.

Triggers keep each photo's searchable tag values current on every EXIF
write; ``count_exif_search_unindexed`` / ``index_exif_search_batch`` backfill
photos written before them; a changed definition empties the table and
rebuilds the triggers. The structural test at the end keeps the backfill SQL
in ``repositories/exif_search.py``.
"""

import ast
import inspect
import json
import os
import textwrap
import time

import pytest
from db import Database


def _text(db, photo_id):
    row = db.conn.execute(
        "SELECT value_text FROM photo_exif_search_text WHERE photo_id = ?", (photo_id,)
    ).fetchone()
    return None if row is None else row[0].split("\x1f")


@pytest.fixture
def photo(db):
    folder = db.add_folder("/photos/Coast", name="Coast")
    pid = db.add_photo(folder_id=folder, filename="curlew.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)
    return db, pid


def test_stored_text_holds_values_not_tag_names(photo):
    db, pid = photo
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (json.dumps({
        "EXIF": {"GPSLongitude": "122 deg 30' W", "ISO": 800, "Flash": True,
                 "Nothing": None, "Lens": ["70-200mm", "f/2.8"]},
        "XMP": {"Subject": ["Long-billed Curlew"]},
        "File": {"FileSize": 12345, "Preview": "(Binary data 100 bytes)"},
    }), pid))
    db.conn.commit()
    # Managed paths, layout tags and binary placeholders are never searched.
    assert sorted(_text(db, pid)) == ["122 deg 30' W", "70-200mm", "800", "f/2.8", "true"]


def test_photo_without_exif_stores_empty_text(photo):
    db, pid = photo
    assert _text(db, pid) == [""]
    db.conn.execute("UPDATE photos SET exif_data='not json' WHERE id=?", (pid,))
    db.conn.commit()
    assert _text(db, pid) == [""]


def test_text_follows_rewrites_renumbering_and_deletes(photo):
    db, pid = photo
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?",
                    (json.dumps({"EXIF": {"Model": "Z9"}}), pid))
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?",
                    (json.dumps({"EXIF": {"Model": "R5"}}), pid))
    db.conn.commit()
    assert _text(db, pid) == ["R5"]
    db.conn.execute("UPDATE photos SET id=? WHERE id=?", (pid + 100, pid))
    db.conn.commit()
    assert _text(db, pid) is None
    assert _text(db, pid + 100) == ["R5"]
    db.conn.execute("DELETE FROM photos WHERE id=?", (pid + 100,))
    db.conn.commit()
    assert _text(db, pid + 100) is None


def test_backfill_indexes_photos_in_batches(db):
    folder = db.add_folder("/photos/Backlog", name="Backlog")
    pids = [db.add_photo(folder_id=folder, filename=f"{i}.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0) for i in range(5)]
    for pid in pids:
        db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?",
                        (json.dumps({"EXIF": {"Model": f"cam{pid}"}}), pid))
    db.conn.execute("DELETE FROM photo_exif_search_text")
    db.conn.commit()
    unindexed = db.count_exif_search_unindexed()
    assert unindexed >= 5

    after_id, indexed, batches = 0, 0, 0
    while (batch := db.index_exif_search_batch(after_id, 2)) is not None:
        after_id, count = batch
        indexed += count
        batches += 1
        assert not db.conn.in_transaction
    assert indexed == unindexed
    assert batches == -(-unindexed // 2)
    assert db.count_exif_search_unindexed() == 0
    assert all(_text(db, pid) == [f"cam{pid}"] for pid in pids)


def test_backfill_keeps_rows_triggers_wrote(photo):
    db, pid = photo
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?",
                    (json.dumps({"EXIF": {"Model": "Z9"}}), pid))
    db.conn.commit()
    assert db.index_exif_search_batch(0, 100) is None
    assert _text(db, pid) == ["Z9"]


def test_changed_definition_rebuilds_triggers_and_empties_table(tmp_path):
    path = str(tmp_path / "stamp.db")
    db = Database(path)
    folder = db.add_folder("/photos/x", name="x")
    pid = db.add_photo(folder_id=folder, filename="a.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)
    db.conn.execute("DROP TRIGGER trg_photo_exif_search_text_update")
    db.conn.execute("UPDATE db_meta SET value='stale' WHERE key='exif_search_text_definition'")
    db.conn.commit()
    db.close()

    db = Database(path)
    try:
        assert db.count_exif_search_unindexed() == 1
        db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?",
                        (json.dumps({"EXIF": {"Model": "Z9"}}), pid))
        db.conn.commit()
        assert _text(db, pid) == ["Z9"]
    finally:
        db.close()


def test_startup_backfill_job_indexes_unindexed_photos(tmp_path, monkeypatch):
    import config as cfg
    import models
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    from app import create_app

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    db = Database(db_path)
    folder = db.add_folder("/photos/x", name="x")
    pid = db.add_photo(folder_id=folder, filename="a.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?",
                    (json.dumps({"EXIF": {"Model": "Z9"}}), pid))
    db.conn.execute("DELETE FROM photo_exif_search_text")
    db.conn.commit()
    db.close()

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")
    runner = app._job_runner
    app._kickoff_exif_search_backfill()
    deadline = time.time() + 5
    job = None
    while time.time() < deadline:
        jobs = [j for j in runner.list_jobs() if j["type"] == "exif_search_backfill"]
        if jobs and jobs[0]["status"] in ("completed", "failed"):
            job = jobs[0]
            break
        time.sleep(0.05)
    assert job is not None and job["status"] == "completed", job
    assert job["result"] == {"indexed": 1}

    check = Database(db_path)
    try:
        assert _text(check, pid) == ["Z9"]
        assert check.count_exif_search_unindexed() == 0
    finally:
        check.close()

    # Steady state: nothing unindexed, so no second job starts.
    app._kickoff_exif_search_backfill()
    assert len([j for j in runner.list_jobs() if j["type"] == "exif_search_backfill"]) == 1


# -- structure: the backfill SQL lives in the repository ----------------------

_DELEGATING_METHODS = ("count_exif_search_unindexed", "index_exif_search_batch")


@pytest.mark.parametrize("name", _DELEGATING_METHODS)
def test_exif_search_method_delegates_to_repository(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    attrs = {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to ExifSearchRepository"
    )
    assert "_exif_search_repository" in attrs
