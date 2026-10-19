"""Behavior pins for the card-cleanup domain of ``Database``.

``card_cleanup`` reaches its catalog SQL through ``db.card_cleanup``: the rows
sharing a content hash, the scan's one-pass hash index, and the scoped
verdict write that certifies a never-checked archive row only while it is
still the row that was read. The structural tests at the end keep that SQL in
``repositories/card_cleanup.py``.
"""

import ast
import inspect
import textwrap

import pytest
from db import Database
from repositories.card_cleanup import CardCleanupRepository
from services import photo_ingest


@pytest.fixture
def rows(db):
    archive = db.add_folder("/archive", name="archive")
    a = photo_ingest.add_photo(db, archive, "a.jpg", ".jpg", 100, 1.0, file_hash="H")
    b = photo_ingest.add_photo(db, archive, "b.jpg", ".jpg", 200, 2.0, file_hash="OTHER")
    nohash = photo_ingest.add_photo(db, archive, "c.jpg", ".jpg", 300, 3.0)
    homeless_folder = db.add_folder("/gone", name="gone")
    homeless = photo_ingest.add_photo(db, homeless_folder, "d.jpg", ".jpg", 400, 4.0, file_hash="H")
    db.conn.execute("PRAGMA foreign_keys = OFF")
    db.conn.execute("UPDATE photos SET folder_id = 987654 WHERE id = ?", (homeless,))
    db.commit()
    db.conn.execute("PRAGMA foreign_keys = ON")
    return {"a": a, "b": b, "nohash": nohash, "homeless": homeless, "archive": archive}


def test_rows_by_hash_left_joins_the_folder(db, rows):
    found = {r["id"]: r for r in db.card_cleanup.rows_by_hash("H")}
    assert set(found) == {rows["a"], rows["homeless"]}
    a = found[rows["a"]]
    assert (a["folder_id"], a["filename"], a["file_size"], a["file_mtime"],
            a["hash_status"], a["folder_path"]) == (
        rows["archive"], "a.jpg", 100, 1.0, None, "/archive")
    assert found[rows["homeless"]]["folder_path"] is None
    assert db.card_cleanup.rows_by_hash("NONE") == []


def test_hashed_photo_rows_skips_unhashed_photos(db, rows):
    got = sorted((r["filename"], r["file_hash"], r["folder_path"])
                 for r in db.card_cleanup.hashed_photo_rows())
    assert got == [("a.jpg", "H", "/archive"), ("b.jpg", "OTHER", "/archive"),
                   ("d.jpg", "H", None)]


def _photo(db, photo_id):
    return db.conn.execute(
        "SELECT hash_status, hash_checked_at, file_size, file_mtime FROM photos WHERE id = ?",
        (photo_id,),
    ).fetchone()


def test_record_unchecked_hash_verdict_updates_only_the_row_it_read(db, rows):
    row = next(r for r in db.card_cleanup.rows_by_hash("H") if r["id"] == rows["a"])

    assert db.card_cleanup.record_unchecked_hash_verdict(
        row, "H", "ok", "2026-10-17T00:00:00", file_size=111, file_mtime=5.0)
    assert db.in_transaction  # the caller commits
    db.commit()
    assert tuple(_photo(db, rows["a"])) == ("ok", "2026-10-17T00:00:00", 111, 5.0)

    # Already checked: a second verdict for the same read row changes nothing.
    assert not db.card_cleanup.record_unchecked_hash_verdict(
        row, "H", "corrupt", "2026-10-18T00:00:00")
    assert _photo(db, rows["a"])["hash_status"] == "ok"


def test_record_unchecked_hash_verdict_refuses_a_changed_row(db, rows):
    row = next(r for r in db.card_cleanup.rows_by_hash("H") if r["id"] == rows["a"])
    db.conn.execute("UPDATE photos SET file_mtime = 9.0 WHERE id = ?", (rows["a"],))
    db.commit()

    assert not db.card_cleanup.record_unchecked_hash_verdict(
        row, "H", "corrupt", "2026-10-17T00:00:00")
    assert not db.card_cleanup.record_unchecked_hash_verdict(
        {**dict(row), "file_mtime": 9.0}, "WRONG", "corrupt", "2026-10-17T00:00:00")
    assert _photo(db, rows["a"])["hash_status"] is None

    fresh = {**dict(row), "file_mtime": 9.0}
    assert db.card_cleanup.record_unchecked_hash_verdict(
        fresh, "H", "corrupt", "2026-10-17T00:00:00")
    assert tuple(_photo(db, rows["a"])) == ("corrupt", "2026-10-17T00:00:00", 100, 9.0)


# -- structure ------------------------------------------------------------------


def test_card_cleanup_is_a_fresh_repository_on_the_connection_per_access(db):
    first, second = db.card_cleanup, db.card_cleanup
    assert isinstance(first, CardCleanupRepository)
    assert first is not second
    assert first.conn is db.conn


def test_card_cleanup_accessor_builds_through_the_factory():
    accessor = Database.__dict__["card_cleanup"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_card_cleanup_repository" in attrs
    assert "conn" not in attrs
