"""Bulk plan/merge locking on shared-memory SQLite; no filesystem or app use."""

import __future__

import ast
import builtins
import contextlib
import posixpath
import sqlite3
import typing
import uuid
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import Mock

VIREO = Path(__file__).resolve().parents[1]


def _selected(module, cls, name, namespace):
    tree = ast.parse((VIREO / module).read_text(encoding="utf-8"))
    body = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls).body
    if module == "db.py" and not any(isinstance(n, ast.FunctionDef) and n.name == name for n in body):
        body = ast.parse((VIREO / "services/duplicate_resolution.py").read_text()).body
        namespace.setdefault("Any", typing.Any)
    node = next(n for n in body if isinstance(n, ast.FunctionDef) and n.name == name)
    nodes = [n for n in body if isinstance(n, ast.ClassDef) and n.name == "_Refused"] + [node]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<bulk memory>", "exec", flags=__future__.annotations.compiler_flag), namespace)
    return namespace[name]


@contextlib.contextmanager
def _flow(*, expected_manual_review=True):
    uri = f"file:invented-bulk-{uuid.uuid4().hex}?mode=memory&cache=shared"
    with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=0)) as conn:
        with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=0)) as scan:
            conn.row_factory = sqlite3.Row
            conn.executescript("""
                CREATE TABLE folders(id INTEGER PRIMARY KEY,path TEXT);
                INSERT INTO folders VALUES(1,'invented/keep'),(2,'invented/other');
                CREATE TABLE photos(id INTEGER PRIMARY KEY,folder_id,filename,file_hash,
                    file_mtime,rating,flag,xmp_import_pending);
                INSERT INTO photos VALUES(1,1,'one.jpg','hash',1,3,NULL,0),
                    (2,2,'two.jpg','hash',2,4,NULL,0);
                CREATE TABLE notes(value TEXT);
            """)
            trace = []
            conn.set_trace_callback(trace.append)
            paths = SimpleNamespace(normpath=posixpath.normpath, join=posixpath.join, exists=lambda path: True)
            def fake_import(name, *args, **kwargs):
                assert name == "duplicates"
                return SimpleNamespace(DupCandidate=lambda **kw: SimpleNamespace(**kw),
                                       resolve_duplicates=lambda candidates: (1, []))
            namespace = {"os": SimpleNamespace(path=paths),
                         "__builtins__": {**vars(builtins), "__import__": fake_import}}
            repo = SimpleNamespace(conn=conn)
            for helper in ("_keep_folder_rows", "keep_folder_preflight"):
                setattr(repo, helper, MethodType(_selected("repositories/duplicates.py", "DuplicatesRepository", helper, namespace), repo))
            repo.keep_folder_plan = MethodType(
                _selected("repositories/duplicates.py", "DuplicatesRepository", "keep_folder_plan", namespace), repo,
            )
            merges = []
            def merge(winner, losers, *, manual_review=False):
                assert manual_review is expected_manual_review
                merges.append((winner, losers))
                with conn:
                    conn.executemany("UPDATE photos SET flag='rejected' WHERE id=?", [(pid,) for pid in losers])
            db = SimpleNamespace(_duplicates_repository=lambda: repo,
                                 begin_immediate=lambda: conn.execute("BEGIN IMMEDIATE"),
                                 transaction=lambda: conn, rollback=conn.rollback,
                                 _apply_winner_loser_merge=merge)
            db.duplicates = repo
            db.bulk = MethodType(_selected("db.py", "Database", "bulk_resolve_by_folder", namespace), db)
            yield conn, scan, db, repo, merges, trace


def test_bulk_writer_lock_precedes_plan_and_blocks_scan_until_merge():
    with _flow() as (conn, scan, db, repo, merges, trace):
        plan = repo.keep_folder_plan
        blocked = []
        def interleave(*args, **kwargs):
            result = plan(*args, **kwargs)
            try:
                scan.execute("UPDATE photos SET xmp_import_pending=1 WHERE id=2")
                scan.commit()
            except sqlite3.OperationalError as error:
                assert "locked" in str(error)
                scan.rollback()
                blocked.append(True)
            return result
        repo.keep_folder_plan = interleave
        result = db.bulk(["hash"], "invented/keep")
        assert blocked == [True], "Scan changed pending state after a stale bulk plan"
        assert result["resolved"][0]["loser_ids"] == [2] and merges == [(1, [2])]
        assert "BEGIN IMMEDIATE" in trace
        assert trace.index("BEGIN IMMEDIATE") < max(i for i, sql in enumerate(trace) if "xmp_import_pending" in sql)
        assert not conn.in_transaction
        scan.execute("UPDATE photos SET xmp_import_pending=1 WHERE id=2")
        scan.commit()


def test_bulk_pending_committed_before_lock_skips_without_writing():
    with _flow() as (conn, scan, db, repo, merges, trace):
        scan.execute("UPDATE photos SET xmp_import_pending=1 WHERE id=2")
        scan.commit()
        result = db.bulk(["hash"], "invented/keep")
        assert result == {"resolved": [], "skipped": [{"file_hash": "hash", "reason": "sidecar keywords pending"}]}
        assert merges == [] and not conn.in_transaction and "ROLLBACK" in trace
        assert conn.execute("SELECT flag FROM photos WHERE id=2").fetchone()[0] is None


def test_bulk_cannot_begin_while_scan_holds_writer_lock():
    with _flow() as (conn, scan, db, repo, merges, trace):
        scan.execute("BEGIN IMMEDIATE")
        scan.execute("UPDATE photos SET xmp_import_pending=1 WHERE id=2")
        repo.keep_folder_plan = Mock(side_effect=AssertionError("Plan ran without owning writer lock"))
        try:
            db.bulk(["hash"], "invented/keep")
        except sqlite3.OperationalError as error:
            assert "locked" in str(error)
        else:
            raise AssertionError("Competing writer was ignored")
        repo.keep_folder_plan.assert_not_called()
        assert merges == [] and not conn.in_transaction and scan.in_transaction
        scan.rollback()


def test_bulk_plan_and_merge_failures_roll_back_partial_writes_and_control_flow():
    for stage in ("plan", "merge"):
        for original in (RuntimeError("invented failure"), KeyboardInterrupt(), SystemExit()):
            with _flow() as (conn, scan, db, repo, merges, trace):
                def fail(*args, error=original, **kwargs):
                    conn.execute("INSERT INTO notes VALUES('partial')")
                    conn.execute("UPDATE photos SET flag='rejected' WHERE id=2")
                    raise error
                if stage == "plan":
                    repo.keep_folder_plan = fail
                else:
                    db._apply_winner_loser_merge = fail
                try:
                    db.bulk(["hash"], "invented/keep")
                except BaseException as error:
                    assert error is original
                else:
                    raise AssertionError("Original failure was swallowed")
                assert not conn.in_transaction
                assert conn.execute("SELECT * FROM notes").fetchall() == []
                assert conn.execute("SELECT flag FROM photos WHERE id=2").fetchone()[0] is None
                assert "ROLLBACK" in trace


def test_bulk_patched_facade_callback_releases_lock_between_groups():
    with _flow() as (conn, scan, db, repo, merges, trace):
        def merge(winner, losers, *, manual_review=False):
            assert manual_review is True
            merges.append((winner, losers))
        db._apply_winner_loser_merge = merge
        result = db.bulk(["hash", "hash"], "invented/keep")
        assert merges == [(1, [2]), (1, [2])] and len(result["resolved"]) == 2
        assert trace.count("BEGIN IMMEDIATE") == 2 and trace.count("COMMIT") == 2
        assert not conn.in_transaction


def test_bulk_nested_begin_does_not_commit_or_rollback_caller_work():
    with _flow() as (conn, scan, db, repo, merges, trace):
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO notes VALUES('caller-owned')")
        try:
            db.bulk(["hash"], "invented/keep")
        except sqlite3.OperationalError:
            pass
        else:
            raise AssertionError("Nested writer transaction was accepted")
        assert conn.in_transaction and merges == []
        assert conn.execute("SELECT value FROM notes").fetchone()[0] == "caller-owned"
        assert "COMMIT" not in trace and "ROLLBACK" not in trace
        assert db.bulk([], "invented/keep") == {"resolved": [], "skipped": []}
        assert conn.in_transaction
        conn.rollback()
        assert conn.execute("SELECT * FROM notes").fetchall() == []


def test_bulk_stale_group_skip_releases_writer_lock():
    with _flow() as (conn, scan, db, repo, merges, trace):
        result = db.bulk(["missing"], "invented/keep")
        assert result == {"resolved": [], "skipped": [{"file_hash": "missing", "reason": "no candidates"}]}
        assert merges == [] and not conn.in_transaction and "ROLLBACK" in trace


def test_bulk_disk_preflight_runs_before_lock_and_locked_plan_never_stats():
    with _flow() as (conn, scan, db, repo, merges, trace):
        paths = repo.keep_folder_preflight.__func__.__globals__["os"].path
        probes = []
        def exists(path):
            assert not conn.in_transaction, "Disk preflight ran while holding the writer lock"
            probes.append(path)
            return True
        paths.exists = exists
        result = db.bulk(["hash"], "invented/keep")
        assert len(probes) == 1 and len(result["resolved"]) == 1
        assert merges == [(1, [2])] and not conn.in_transaction


def test_bulk_revalidates_catalog_identity_membership_and_pending_after_preflight():
    changes = [
        "UPDATE photos SET xmp_import_pending=1 WHERE id=2",
        "UPDATE photos SET flag='rejected' WHERE id=2",
        "UPDATE photos SET filename='replaced.jpg' WHERE id=1",
        "UPDATE photos SET file_mtime=99 WHERE id=1",
        "UPDATE photos SET rating=5 WHERE id=1",
        "UPDATE photos SET folder_id=2 WHERE id=1",
        "UPDATE folders SET path='invented/repointed' WHERE id=1",
        "UPDATE photos SET file_hash='changed' WHERE id=2",
        "INSERT INTO photos VALUES(3,2,'new.jpg','hash',3,0,NULL,0)",
        "DELETE FROM photos WHERE id=1",
    ]
    for sql in changes:
        with _flow() as (conn, scan, db, repo, merges, trace):
            begin = db.begin_immediate
            def change_then_lock(statement=sql, acquire=begin):
                scan.execute(statement)
                scan.commit()
                acquire()
            db.begin_immediate = change_then_lock
            result = db.bulk(["hash"], "invented/keep")
            assert result == {
                "resolved": [], "skipped": [{"file_hash": "hash", "reason": "group changed during preflight"}],
            }, sql
            assert merges == [] and not conn.in_transaction
