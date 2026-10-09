"""Behavior pins for the caches domain of ``Database``.

The behavior tests exercise the preview-cache LRU and offline-original
operations through the ``db.caches`` accessor, the only way callers reach
them; the structural tests at the end keep the SQL in
``repositories/caches.py`` and the old forwarding wrappers off ``Database``.
They pin commit boundaries, return shapes, and which writes go through the
lock-retry helpers (``execute_with_retry`` / ``commit_with_retry``) and which
don't.
"""

import ast
import inspect
import sqlite3
import textwrap
import time

import db as db_module
import pytest
from db import Database
from repositories.caches import CachesRepository


def _reader(db):
    """A second connection: sees only what ``db.caches`` has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _photo(db, name="a.jpg"):
    folder_id = db.add_folder("/tmp/caches")
    return db.add_photo(folder_id, name, ".jpg", file_size=100, file_mtime=1.0)


class _FlakyConn:
    """Wrap a real connection; the first ``fail_execute`` executes and
    ``fail_commit`` commits raise ``OperationalError(message)``."""

    def __init__(self, conn, *, fail_execute=0, fail_commit=0,
                 message="database is locked"):
        self._conn = conn
        self.fail_execute = fail_execute
        self.fail_commit = fail_commit
        self.message = message
        self.execute_calls = 0
        self.commit_calls = 0

    def execute(self, *args):
        self.execute_calls += 1
        if self.fail_execute:
            self.fail_execute -= 1
            raise sqlite3.OperationalError(self.message)
        return self._conn.execute(*args)

    def commit(self):
        self.commit_calls += 1
        if self.fail_commit:
            self.fail_commit -= 1
            raise sqlite3.OperationalError(self.message)
        return self._conn.commit()

    def __getattr__(self, name):
        return getattr(self._conn, name)


@pytest.fixture
def no_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    return sleeps


def _offline_args(photo_id, **overrides):
    args = dict(
        photo_id=photo_id,
        original_path="/cache/a.NEF",
        xmp_path="/cache/a.xmp",
        companion_path="/cache/a.jpg",
        bytes_=1234,
        source_size=5678,
        source_mtime=11.5,
        cached_at=22.25,
        status="cached",
    )
    args.update(overrides)
    return args


# -- preview_cache ------------------------------------------------------------


def test_preview_insert_commits_and_stamps_now(db):
    pid = _photo(db)
    t0 = time.time()
    assert db.caches.preview_insert(pid, 1920, 4096) is None
    t1 = time.time()

    assert not db.conn.in_transaction
    row = _reader(db).execute(
        "SELECT photo_id, size, bytes, last_access_at FROM preview_cache"
    ).fetchone()
    assert (row["photo_id"], row["size"], row["bytes"]) == (pid, 1920, 4096)
    assert t0 <= row["last_access_at"] <= t1


def test_preview_insert_replaces_existing_entry(db):
    pid = _photo(db)
    db.caches.preview_insert(pid, 1920, 100)
    first = db.caches.preview_get(pid, 1920)["last_access_at"]
    time.sleep(0.01)
    db.caches.preview_insert(pid, 1920, 250)

    rows = db.conn.execute("SELECT bytes, last_access_at FROM preview_cache").fetchall()
    assert len(rows) == 1
    assert rows[0]["bytes"] == 250
    assert rows[0]["last_access_at"] > first


def test_preview_insert_keys_on_photo_and_size(db):
    pid = _photo(db)
    db.caches.preview_insert(pid, 1920, 100)
    db.caches.preview_insert(pid, 2560, 200)
    assert db.caches.preview_total_bytes() == 300


def test_preview_insert_enforces_photo_foreign_key(db):
    with pytest.raises(sqlite3.IntegrityError):
        db.caches.preview_insert(999_999, 1920, 100)


def test_preview_insert_does_not_retry_locked_errors(db, no_sleep):
    pid = _photo(db)
    flaky = _FlakyConn(db.conn, fail_execute=1)
    db.conn = flaky
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            db.caches.preview_insert(pid, 1920, 100)
    finally:
        db.conn = flaky._conn
    assert flaky.execute_calls == 1
    assert no_sleep == []


def test_preview_touch_updates_timestamp_and_commits(db):
    pid = _photo(db)
    db.caches.preview_insert(pid, 1920, 100)
    db.conn.execute("UPDATE preview_cache SET last_access_at = 1.0")
    db.conn.commit()

    t0 = time.time()
    assert db.caches.preview_touch(pid, 1920) is None

    assert not db.conn.in_transaction
    row = _reader(db).execute(
        "SELECT bytes, last_access_at FROM preview_cache"
    ).fetchone()
    assert row["bytes"] == 100
    assert row["last_access_at"] >= t0


def test_preview_touch_only_matching_size(db):
    pid = _photo(db)
    db.caches.preview_insert(pid, 1920, 100)
    db.caches.preview_insert(pid, 2560, 200)
    db.conn.execute("UPDATE preview_cache SET last_access_at = 1.0")
    db.conn.commit()

    db.caches.preview_touch(pid, 2560)

    assert db.caches.preview_get(pid, 1920)["last_access_at"] == 1.0
    assert db.caches.preview_get(pid, 2560)["last_access_at"] > 1.0


def test_preview_touch_missing_is_noop(db):
    pid = _photo(db)
    db.caches.preview_touch(pid, 1920)
    assert not db.conn.in_transaction
    assert db.caches.preview_get(pid, 1920) is None
    assert db.caches.preview_oldest_first() == []


def test_preview_delete_removes_only_that_size_and_commits(db):
    pid = _photo(db)
    db.caches.preview_insert(pid, 1920, 100)
    db.caches.preview_insert(pid, 2560, 200)

    assert db.caches.preview_delete(pid, 1920) is None

    assert not db.conn.in_transaction
    rows = _reader(db).execute("SELECT size FROM preview_cache").fetchall()
    assert [r["size"] for r in rows] == [2560]


def test_preview_delete_missing_is_noop(db):
    pid = _photo(db)
    db.caches.preview_delete(pid, 1920)
    assert not db.conn.in_transaction
    assert db.caches.preview_total_bytes() == 0


def test_preview_total_bytes_empty_is_zero(db):
    total = db.caches.preview_total_bytes()
    assert total == 0
    assert isinstance(total, int)


def test_preview_total_bytes_sums_every_row(db):
    p1 = _photo(db, "a.jpg")
    p2 = _photo(db, "b.jpg")
    db.caches.preview_insert(p1, 1920, 100)
    db.caches.preview_insert(p1, 2560, 50)
    db.caches.preview_insert(p2, 1920, 200)
    assert db.caches.preview_total_bytes() == 350


def test_preview_oldest_first_orders_by_last_access(db):
    p1 = _photo(db, "a.jpg")
    p2 = _photo(db, "b.jpg")
    db.caches.preview_insert(p1, 1920, 100)
    db.caches.preview_insert(p2, 2560, 200)
    db.caches.preview_insert(p1, 2560, 300)
    for pid, size, ts in ((p1, 1920, 30.0), (p2, 2560, 10.0), (p1, 2560, 20.0)):
        db.conn.execute(
            "UPDATE preview_cache SET last_access_at=? WHERE photo_id=? AND size=?",
            (ts, pid, size),
        )
    db.conn.commit()

    rows = db.caches.preview_oldest_first()
    assert isinstance(rows, list)
    assert all(isinstance(r, sqlite3.Row) for r in rows)
    assert rows[0].keys() == ["photo_id", "size", "bytes", "last_access_at"]
    assert [tuple(r) for r in rows] == [
        (p2, 2560, 200, 10.0),
        (p1, 2560, 300, 20.0),
        (p1, 1920, 100, 30.0),
    ]


def test_preview_get_returns_row_or_none(db):
    pid = _photo(db)
    db.caches.preview_insert(pid, 1920, 100)

    row = db.caches.preview_get(pid, 1920)
    assert isinstance(row, sqlite3.Row)
    assert row.keys() == ["photo_id", "size", "bytes", "last_access_at"]
    assert (row["photo_id"], row["size"], row["bytes"]) == (pid, 1920, 100)

    assert db.caches.preview_get(pid, 2560) is None
    assert db.caches.preview_get(pid + 1, 1920) is None


# -- offline_originals --------------------------------------------------------


_OFFLINE_COLUMNS = [
    "photo_id", "original_path", "xmp_path", "companion_path", "bytes",
    "source_size", "source_mtime", "cached_at", "status", "error",
]


def test_offline_original_upsert_stores_every_column_and_commits(db):
    pid = _photo(db)
    assert db.caches.offline_original_upsert(**_offline_args(pid, error="boom")) is None

    assert not db.conn.in_transaction
    row = _reader(db).execute(
        "SELECT * FROM offline_originals WHERE photo_id=?", (pid,)
    ).fetchone()
    assert dict(row) == {
        "photo_id": pid,
        "original_path": "/cache/a.NEF",
        "xmp_path": "/cache/a.xmp",
        "companion_path": "/cache/a.jpg",
        "bytes": 1234,
        "source_size": 5678,
        "source_mtime": 11.5,
        "cached_at": 22.25,
        "status": "cached",
        "error": "boom",
    }


def test_offline_original_upsert_positional_and_error_default(db):
    pid = _photo(db)
    db.caches.offline_original_upsert(
        pid, None, None, None, 0, None, None, 5.0, "error"
    )
    row = db.caches.offline_original_get(pid)
    assert tuple(row) == (pid, None, None, None, 0, None, None, 5.0, "error", None)
    assert inspect.signature(CachesRepository.offline_original_upsert).parameters[
        "error"
    ].default is None


def test_offline_original_upsert_replaces_existing_row(db):
    pid = _photo(db)
    db.caches.offline_original_upsert(**_offline_args(pid, error="old"))
    db.caches.offline_original_upsert(**_offline_args(pid, bytes_=99, status="stale"))

    rows = db.conn.execute("SELECT * FROM offline_originals").fetchall()
    assert len(rows) == 1
    assert rows[0]["bytes"] == 99
    assert rows[0]["status"] == "stale"
    assert rows[0]["error"] is None


def test_offline_original_upsert_enforces_photo_foreign_key(db):
    with pytest.raises(sqlite3.IntegrityError):
        db.caches.offline_original_upsert(**_offline_args(999_999))


def test_offline_original_upsert_retries_locked_execute_and_commit(db, no_sleep):
    pid = _photo(db)
    flaky = _FlakyConn(db.conn, fail_execute=2, fail_commit=1)
    db.conn = flaky
    try:
        db.caches.offline_original_upsert(**_offline_args(pid))
    finally:
        db.conn = flaky._conn
    assert flaky.execute_calls == 3
    assert flaky.commit_calls == 2
    assert len(no_sleep) == 3
    row = _reader(db).execute(
        "SELECT status FROM offline_originals WHERE photo_id=?", (pid,)
    ).fetchone()
    assert row["status"] == "cached"


def test_offline_original_upsert_propagates_non_transient_errors(db, no_sleep):
    pid = _photo(db)
    flaky = _FlakyConn(db.conn, fail_execute=1, message="disk I/O error")
    db.conn = flaky
    try:
        with pytest.raises(sqlite3.OperationalError, match="disk I/O"):
            db.caches.offline_original_upsert(**_offline_args(pid))
    finally:
        db.conn = flaky._conn
    assert flaky.execute_calls == 1
    assert no_sleep == []


def test_offline_writes_through_the_accessor_use_patched_db_retry_helpers(db, monkeypatch):
    """A patch of ``db.execute_with_retry`` / ``db.commit_with_retry`` reaches
    writes made through ``db.caches``: the accessor builds its repository
    after the patch, reading the helpers from ``db`` at that moment."""
    pid = _photo(db)
    calls = []
    real_execute = db_module.execute_with_retry
    real_commit = db_module.commit_with_retry

    def execute(conn, sql, params=()):
        calls.append(("execute", conn))
        return real_execute(conn, sql, params)

    def commit(conn):
        calls.append(("commit", conn))
        return real_commit(conn)

    monkeypatch.setattr(db_module, "execute_with_retry", execute)
    monkeypatch.setattr(db_module, "commit_with_retry", commit)

    db.caches.offline_original_upsert(**_offline_args(pid))
    db.caches.offline_original_delete(pid)

    assert calls == [
        ("execute", db.conn), ("commit", db.conn),
        ("execute", db.conn), ("commit", db.conn),
    ]


def test_offline_original_get_returns_row_or_none(db):
    pid = _photo(db)
    assert db.caches.offline_original_get(pid) is None

    db.caches.offline_original_upsert(**_offline_args(pid))
    row = db.caches.offline_original_get(pid)
    assert isinstance(row, sqlite3.Row)
    assert row.keys() == _OFFLINE_COLUMNS
    assert row["original_path"] == "/cache/a.NEF"


def test_offline_original_delete_removes_row_and_commits(db):
    p1 = _photo(db, "a.jpg")
    p2 = _photo(db, "b.jpg")
    db.caches.offline_original_upsert(**_offline_args(p1))
    db.caches.offline_original_upsert(**_offline_args(p2))

    assert db.caches.offline_original_delete(p1) is None

    assert not db.conn.in_transaction
    rows = _reader(db).execute("SELECT photo_id FROM offline_originals").fetchall()
    assert [r["photo_id"] for r in rows] == [p2]
    assert db.caches.offline_original_get(p1) is None


def test_offline_original_delete_missing_is_noop(db):
    pid = _photo(db)
    db.caches.offline_original_delete(pid)
    assert not db.conn.in_transaction
    assert db.caches.offline_original_total_bytes() == 0


def test_offline_original_delete_without_commit_stays_in_the_callers_transaction(db):
    p1 = _photo(db, "a.jpg")
    db.caches.offline_original_upsert(**_offline_args(p1))
    with db.transaction():
        db.begin_immediate()
        assert db.caches.offline_original_delete(p1, _commit=False) is None
        # Still inside the writer-locked transaction, and nothing committed.
        assert db.conn.in_transaction
        assert db.caches.offline_original_get(p1) is None
        assert _reader(db).execute(
            "SELECT COUNT(*) FROM offline_originals"
        ).fetchone()[0] == 1
    assert _reader(db).execute(
        "SELECT COUNT(*) FROM offline_originals"
    ).fetchone()[0] == 0


def test_offline_original_delete_without_commit_rolls_back_with_the_caller(db):
    p1 = _photo(db, "a.jpg")
    db.caches.offline_original_upsert(**_offline_args(p1))
    db.caches.offline_original_delete(p1, _commit=False)
    db.rollback()
    assert db.caches.offline_original_get(p1) is not None


def test_offline_original_delete_retries_locked_errors(db, no_sleep):
    pid = _photo(db)
    db.caches.offline_original_upsert(**_offline_args(pid))
    flaky = _FlakyConn(db.conn, fail_execute=1, fail_commit=1,
                       message="database is busy")
    db.conn = flaky
    try:
        db.caches.offline_original_delete(pid)
    finally:
        db.conn = flaky._conn
    assert flaky.execute_calls == 2
    assert flaky.commit_calls == 2
    assert len(no_sleep) == 2
    assert _reader(db).execute(
        "SELECT COUNT(*) FROM offline_originals"
    ).fetchone()[0] == 0


def test_offline_original_total_bytes_counts_only_cached_rows(db):
    assert db.caches.offline_original_total_bytes() == 0
    p1 = _photo(db, "a.jpg")
    p2 = _photo(db, "b.jpg")
    p3 = _photo(db, "c.jpg")
    db.caches.offline_original_upsert(**_offline_args(p1, bytes_=100))
    db.caches.offline_original_upsert(**_offline_args(p2, bytes_=40))
    db.caches.offline_original_upsert(**_offline_args(p3, bytes_=7, status="error"))
    total = db.caches.offline_original_total_bytes()
    assert total == 140
    assert isinstance(total, int)


def test_caches_are_catalog_wide(db):
    """Neither cache is scoped to the active workspace."""
    pid = _photo(db)
    db.caches.preview_insert(pid, 1920, 100)
    db.caches.offline_original_upsert(**_offline_args(pid))
    db.set_active_workspace(None)

    assert db.caches.preview_get(pid, 1920)["bytes"] == 100
    assert db.caches.preview_total_bytes() == 100
    assert len(db.caches.preview_oldest_first()) == 1
    db.caches.preview_touch(pid, 1920)
    assert db.caches.offline_original_get(pid)["status"] == "cached"
    assert db.caches.offline_original_total_bytes() == 1234
    db.caches.preview_delete(pid, 1920)
    db.caches.offline_original_delete(pid)
    db.caches.preview_insert(pid, 2560, 5)
    db.caches.offline_original_upsert(**_offline_args(pid))


def _seed_both_preview_families(db):
    pid = _photo(db)
    db.caches.preview_insert(pid, 1920, 10)
    db.caches.preview_insert(pid, 2560, 20)
    db.caches.paired_preview_insert(pid, "a.jpg", 30)
    db.caches.paired_preview_insert(pid, "b.jpg", 40)
    db.conn.commit()
    return pid


def _preview_state(db):
    reader = _reader(db)
    plain = {(r["photo_id"], r["size"]) for r in reader.execute("SELECT * FROM preview_cache")}
    paired = {r["filename"] for r in reader.execute("SELECT * FROM paired_preview_cache")}
    return plain, paired


def test_preview_delete_entries_removes_only_the_named_rows_and_commits(db):
    pid = _seed_both_preview_families(db)

    db.caches.preview_delete_entries([(pid, 1920)], ["a.jpg"])

    assert not db.conn.in_transaction
    assert _preview_state(db) == ({(pid, 2560)}, {"b.jpg"})


def test_preview_delete_entries_accepts_one_empty_family(db):
    pid = _seed_both_preview_families(db)

    db.caches.preview_delete_entries([], ["a.jpg", "b.jpg"])

    assert _preview_state(db) == ({(pid, 1920), (pid, 2560)}, set())


def test_paired_preview_delete_removes_one_entry_and_commits(db):
    pid = _seed_both_preview_families(db)

    db.caches.paired_preview_delete("b.jpg")

    assert not db.conn.in_transaction
    assert _preview_state(db) == ({(pid, 1920), (pid, 2560)}, {"a.jpg"})


def test_preview_clear_all_empties_both_families_and_commits(db):
    _seed_both_preview_families(db)

    db.caches.preview_clear_all()

    assert not db.conn.in_transaction
    assert _preview_state(db) == (set(), set())


def test_preview_entry_count_spans_both_families(db):
    assert db.caches.preview_entry_count() == 0
    _seed_both_preview_families(db)
    assert db.caches.preview_entry_count() == 4


def test_preview_average_bytes_skips_empty_entries(db):
    assert db.caches.preview_average_bytes() is None
    pid = _photo(db)
    db.caches.preview_insert(pid, 1920, 0)
    assert db.caches.preview_average_bytes() is None
    db.caches.preview_insert(pid, 2560, 10)
    db.caches.paired_preview_insert(pid, "a.jpg", 30)
    db.conn.commit()
    assert db.caches.preview_average_bytes() == 20


def _temp_tables(db):
    return {
        row[0] for row in db.conn.execute(
            "SELECT name FROM sqlite_temp_master WHERE type = 'table'"
        )
    }


def test_preview_delete_all_except_keeps_named_rows_uncommitted(db):
    pid = _seed_both_preview_families(db)
    statements = []
    db.conn.set_trace_callback(statements.append)
    # 401 kept keys, most of them for rows that don't exist: two staging inserts.
    keep = [(pid, 1920)] + [(10_000 + i, 1) for i in range(400)]
    db.caches.preview_delete_all_except(keep)
    db.conn.set_trace_callback(None)

    assert db.conn.in_transaction
    assert len([s for s in statements if s.startswith("INSERT INTO _pc_failed")]) == 2
    assert "_pc_failed" not in _temp_tables(db)
    rows = {(r["photo_id"], r["size"]) for r in db.conn.execute("SELECT * FROM preview_cache")}
    assert rows == {(pid, 1920)}
    assert db.conn.execute("SELECT COUNT(*) FROM paired_preview_cache").fetchone()[0] == 2
    db.conn.commit()
    assert _preview_state(db) == ({(pid, 1920)}, {"a.jpg", "b.jpg"})


def test_preview_delete_all_except_nothing_kept_deletes_every_ordinary_row(db):
    _seed_both_preview_families(db)
    statements = []
    db.conn.set_trace_callback(statements.append)
    db.caches.preview_delete_all_except([])
    db.conn.set_trace_callback(None)
    assert not any("_pc_failed" in s for s in statements)
    assert db.conn.in_transaction
    db.conn.commit()
    assert _preview_state(db) == (set(), {"a.jpg", "b.jpg"})


def test_preview_delete_all_except_drops_the_temp_table_on_error(db):
    pid = _seed_both_preview_families(db)
    with pytest.raises(sqlite3.Error):
        db.caches.preview_delete_all_except([(pid,)])
    assert "_pc_failed" not in _temp_tables(db)
    db.conn.rollback()
    assert db.caches.preview_entry_count() == 4


def test_offline_original_cached_count_counts_only_cached_rows(db):
    assert db.caches.offline_original_cached_count() == 0
    p1 = _photo(db, "a.jpg")
    p2 = _photo(db, "b.jpg")
    db.caches.offline_original_upsert(**_offline_args(p1))
    db.caches.offline_original_upsert(**_offline_args(p2, status="error"))
    assert db.caches.offline_original_cached_count() == 1


def test_preview_invalidated_reads_the_marker(db):
    from preview_cache import (
        ensure_preview_cache_invalidations_table,
        mark_preview_cache_invalid,
    )

    pid = _photo(db)
    ensure_preview_cache_invalidations_table(db)
    assert db.caches.preview_invalidated(pid, 1920) is False
    mark_preview_cache_invalid(db, pid, 1920)
    assert db.caches.preview_invalidated(pid, 1920) is True
    assert db.caches.preview_invalidated(pid, 2560) is False


# -- structure ----------------------------------------------------------------


_REMOVED_CACHE_WRAPPERS = (
    "preview_cache_insert",
    "preview_cache_touch",
    "preview_cache_delete",
    "preview_cache_entry_count",
    "preview_cache_average_bytes",
    "preview_cache_delete_all_except",
    "preview_cache_total_bytes",
    "preview_cache_oldest_first",
    "preview_cache_get",
    "is_preview_cache_invalid",
    "paired_preview_cache_insert",
    "paired_preview_cache_get",
    "paired_preview_cache_touch",
    "paired_preview_cache_oldest_first",
    "paired_preview_cache_delete",
    "preview_cache_delete_entries",
    "preview_cache_clear_all",
    "offline_original_upsert",
    "offline_original_get",
    "offline_original_delete",
    "offline_original_total_bytes",
    "offline_original_cached_count",
)


def test_caches_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.caches`` builds a new repository each time, never a cached one.

    A cached repository could outlive the connection or a patch of the
    ``db`` retry helpers; a fresh one resolves both at use, exactly as a
    forwarding wrapper called at that moment did.
    """
    first, second = db.caches, db.caches
    assert isinstance(first, CachesRepository)
    assert first is not second
    assert first.conn is db.conn
    assert first.execute_with_retry is db_module.execute_with_retry
    assert first.commit_with_retry is db_module.commit_with_retry


def test_caches_accessor_picks_up_a_retry_helper_patched_after_an_earlier_access(db, monkeypatch):
    pid = _photo(db)
    db.caches.offline_original_upsert(**_offline_args(pid))
    commits = []
    real_commit = db_module.commit_with_retry

    def commit(conn):
        commits.append(conn)
        return real_commit(conn)

    monkeypatch.setattr(db_module, "commit_with_retry", commit)
    db.caches.offline_original_delete(pid)
    assert commits == [db.conn]
    assert db.caches.offline_original_get(pid) is None


def test_caches_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.caches``; Database keeps no aliases."""
    for name in _REMOVED_CACHE_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.caches"
    accessor = Database.__dict__["caches"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_caches_repository" in attrs
    assert "conn" not in attrs


def test_caches_repository_imports_no_db_code():
    import repositories.caches as caches_module

    tree = ast.parse(inspect.getsource(caches_module))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module)
    assert "db" not in imported
