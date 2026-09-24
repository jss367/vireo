"""Behavior pins for the iNaturalist-submission domain of ``Database``.

The behavior tests exercise ``record_inat_submission`` and
``get_inat_submissions`` only through the public ``Database`` façade, so they
hold regardless of whether the SQL lives in ``db.py`` or in
``repositories/inat.py``; the structural test at the end keeps it in the
repository.
"""

import ast
import inspect
import sqlite3
import textwrap

import pytest
from db import Database


def _photo(db, name="bird.jpg"):
    fid = db.add_folder(f"/photos/{name}", name=name)
    return db.add_photo(
        folder_id=fid, filename=name, extension=".jpg",
        file_size=1000, file_mtime=1.0,
    )


def _insert(db, photo_id, observation_id, submitted_at):
    db.conn.execute(
        "INSERT INTO inat_submissions"
        " (photo_id, observation_id, observation_url, submitted_at)"
        " VALUES (?, ?, ?, ?)",
        (photo_id, observation_id, f"https://inat/{observation_id}", submitted_at),
    )
    db.conn.commit()


# -- record_inat_submission ---------------------------------------------------


def test_record_commits_and_returns_none(db):
    pid = _photo(db)
    result = db.record_inat_submission(pid, 42, "https://inat/42")
    assert result is None
    assert not db.conn.in_transaction

    other = sqlite3.connect(db._db_path)
    try:
        rows = other.execute(
            "SELECT photo_id, observation_id, observation_url, submitted_at"
            " FROM inat_submissions"
        ).fetchall()
    finally:
        other.close()
    assert len(rows) == 1
    assert rows[0][:3] == (pid, 42, "https://inat/42")
    assert rows[0][3]  # server-side default timestamp


def test_record_duplicate_is_ignored_and_still_commits(db):
    pid = _photo(db)
    db.record_inat_submission(pid, 42, "https://inat/42")
    # Same (photo_id, observation_id) with a different URL: INSERT OR IGNORE
    # keeps the original row and does not raise.
    db.record_inat_submission(pid, 42, "https://inat/other")
    assert not db.conn.in_transaction
    rows = db.conn.execute(
        "SELECT observation_url FROM inat_submissions WHERE photo_id = ?", (pid,)
    ).fetchall()
    assert [r[0] for r in rows] == ["https://inat/42"]


def test_record_allows_multiple_observations_per_photo(db):
    pid = _photo(db)
    db.record_inat_submission(pid, 1, "https://inat/1")
    db.record_inat_submission(pid, 2, "https://inat/2")
    count = db.conn.execute(
        "SELECT COUNT(*) FROM inat_submissions WHERE photo_id = ?", (pid,)
    ).fetchone()[0]
    assert count == 2


def test_record_commits_pending_caller_writes(db):
    """The unconditional commit also flushes writes the caller left open."""
    pid = _photo(db)
    db.conn.execute("UPDATE photos SET rating = 5 WHERE id = ?", (pid,))
    assert db.conn.in_transaction
    db.record_inat_submission(pid, 7, "https://inat/7")
    other = sqlite3.connect(db._db_path)
    try:
        rating = other.execute(
            "SELECT rating FROM photos WHERE id = ?", (pid,)
        ).fetchone()[0]
    finally:
        other.close()
    assert rating == 5


def test_record_unknown_photo_raises_integrity_error(db):
    with pytest.raises(sqlite3.IntegrityError):
        db.record_inat_submission(999_999, 1, "https://inat/1")


def test_record_needs_no_active_workspace(db):
    pid = _photo(db)
    db.set_active_workspace(None)
    db.record_inat_submission(pid, 5, "https://inat/5")
    assert db.get_inat_submissions([pid])[pid]["observation_id"] == 5


# -- get_inat_submissions -----------------------------------------------------


@pytest.mark.parametrize("empty", [[], (), None, set()])
def test_get_empty_input_returns_empty_dict_without_querying(db, empty):
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        assert db.get_inat_submissions(empty) == {}
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []


def test_get_empty_input_needs_no_active_workspace(db):
    db.set_active_workspace(None)
    assert db.get_inat_submissions([]) == {}


def test_get_needs_no_active_workspace(db):
    pid = _photo(db)
    db.record_inat_submission(pid, 3, "https://inat/3")
    db.set_active_workspace(None)
    assert set(db.get_inat_submissions([pid])) == {pid}


def test_get_return_shape(db):
    pid = _photo(db)
    _insert(db, pid, 11, "2025-03-04 05:06:07")
    subs = db.get_inat_submissions([pid])
    assert type(subs) is dict
    assert list(subs) == [pid]
    assert type(subs[pid]) is dict
    assert subs[pid] == {
        "photo_id": pid,
        "observation_id": 11,
        "observation_url": "https://inat/11",
        "submitted_at": "2025-03-04 05:06:07",
    }


def test_get_omits_photos_without_submissions(db):
    with_sub = _photo(db, "a.jpg")
    without = _photo(db, "b.jpg")
    _insert(db, with_sub, 1, "2025-01-01 00:00:00")
    subs = db.get_inat_submissions([without, with_sub, 123_456])
    assert set(subs) == {with_sub}


def test_get_maps_each_photo_to_newest_submission(db):
    pid = _photo(db)
    _insert(db, pid, 111, "2026-01-01 00:00:00")
    _insert(db, pid, 222, "2024-01-01 00:00:00")
    _insert(db, pid, 333, "2025-01-01 00:00:00")
    assert db.get_inat_submissions([pid])[pid]["observation_id"] == 111


def test_get_breaks_timestamp_ties_by_highest_id(db):
    pid = _photo(db)
    _insert(db, pid, 500, "2025-01-01 00:00:00")
    _insert(db, pid, 400, "2025-01-01 00:00:00")
    # Same timestamp: the later-inserted row (higher id) wins, not the
    # higher observation id.
    assert db.get_inat_submissions([pid])[pid]["observation_id"] == 400


def test_get_deduplicates_ids_and_accepts_iterables(db):
    a = _photo(db, "a.jpg")
    b = _photo(db, "b.jpg")
    _insert(db, a, 1, "2025-01-01 00:00:00")
    _insert(db, b, 2, "2025-01-01 00:00:00")
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        subs = db.get_inat_submissions(iter([a, b, a, b, a]))
    finally:
        db.conn.set_trace_callback(None)
    assert set(subs) == {a, b}
    selects = [s for s in statements if "inat_submissions" in s]
    # Duplicates are dropped before binding, first-seen order kept: two
    # parameters, not five. The trace shows the statement with values bound.
    assert selects == [
        "SELECT photo_id, observation_id, observation_url, submitted_at"
        " FROM inat_submissions WHERE photo_id IN (%d,%d)"
        " ORDER BY submitted_at DESC, id DESC" % (a, b)
    ]


def test_get_chunks_ids_at_800(db):
    first = _photo(db, "first.jpg")
    last = _photo(db, "last.jpg")
    _insert(db, first, 1, "2025-01-01 00:00:00")
    _insert(db, last, 2, "2025-01-01 00:00:00")
    ids = [first] + list(range(1_000_000, 1_000_799)) + [last]  # 801 ids
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        subs = db.get_inat_submissions(ids)
    finally:
        db.conn.set_trace_callback(None)
    assert set(subs) == {first, last}
    selects = [s for s in statements if "inat_submissions" in s]
    assert len(selects) == 2


def test_get_newest_wins_across_chunks_per_photo(db):
    """A photo lives in exactly one chunk, so per-photo newest-first holds
    even when the input spans several chunks."""
    a = _photo(db, "a.jpg")
    b = _photo(db, "b.jpg")
    _insert(db, a, 10, "2024-01-01 00:00:00")
    _insert(db, a, 11, "2025-01-01 00:00:00")
    _insert(db, b, 20, "2025-01-01 00:00:00")
    _insert(db, b, 21, "2024-01-01 00:00:00")
    ids = [a] + list(range(2_000_000, 2_000_900)) + [b]
    subs = db.get_inat_submissions(ids)
    assert subs[a]["observation_id"] == 11
    assert subs[b]["observation_id"] == 20


def test_get_does_not_open_a_transaction(db):
    pid = _photo(db)
    db.record_inat_submission(pid, 1, "https://inat/1")
    db.get_inat_submissions([pid])
    assert not db.conn.in_transaction


# -- structure: the iNaturalist SQL lives in the repository -------------------

# Database methods whose SQL moved to repositories/inat.py. Each stays on
# Database as a thin wrapper so existing call sites keep working; none may
# reach the connection directly again.
_DELEGATING_INAT_METHODS = (
    "record_inat_submission",
    "get_inat_submissions",
)


@pytest.mark.parametrize("name", _DELEGATING_INAT_METHODS)
def test_inat_method_delegates_to_repository(name):
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
        f"Database.{name} touches self.conn; move the SQL to InatRepository"
    )
    assert "_inat_repository" in attrs, (
        f"Database.{name} no longer delegates to InatRepository"
    )


def test_inat_signatures_unchanged():
    assert list(inspect.signature(Database.record_inat_submission).parameters) == [
        "self", "photo_id", "observation_id", "observation_url",
    ]
    assert list(inspect.signature(Database.get_inat_submissions).parameters) == [
        "self", "photo_ids",
    ]
