"""Automatic resolver races and route responses: memory SQLite and fakes only."""

import ast
import builtins
import contextlib
import runpy
import sqlite3
import typing
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import Mock

VIREO = Path(__file__).resolve().parents[1]
SUPPORT = runpy.run_path(str(Path(__file__).with_name("test_bulk_pending_transaction_memory.py")))


@contextlib.contextmanager
def _flow():
    with SUPPORT["_flow"]() as (conn, scan, db, repo, merges, trace):
        sentinel = object()
        db = _TransactionOwner(**vars(db), _memory_connection=conn)
        paths = repo.keep_folder_preflight.__func__.__globals__["os"].path
        probes = []
        def exists(path):
            assert not conn.in_transaction, "Automatic disk probe held writer lock"
            probes.append(path)
            return True
        paths.exists = exists
        def imports(name, *args, **kwargs):
            if name == "repositories.duplicates":
                return SimpleNamespace(DEFERRED_PLAN=sentinel)
            assert name == "duplicates"
            return SimpleNamespace(
                DupCandidate=lambda **kw: SimpleNamespace(**kw),
                resolve_duplicates=lambda candidates: (1, [(c.id, "invented") for c in candidates if c.id != 1]),
            )
        namespace = {"os": SimpleNamespace(path=paths), "DEFERRED_PLAN": sentinel,
                     "_volume_offline": lambda path: False, "cast": typing.cast,
                     "__builtins__": {**vars(builtins), "__import__": imports}}
        repo._chunks = lambda values: [values] if values else []
        for name in ("_resolution_rows", "resolution_preflight", "resolution_plan"):
            setattr(repo, name, MethodType(SUPPORT["_selected"](
                "repositories/duplicates.py", "DuplicatesRepository", name, namespace,
            ), repo))
        db.auto = MethodType(SUPPORT["_selected"]("db.py", "Database", "apply_duplicate_resolution", namespace), db)
        yield conn, scan, db, repo, merges, trace, paths, probes


class _TransactionOwner(SimpleNamespace):
    @property
    def in_transaction(self):
        return self._memory_connection.in_transaction


def test_auto_scan_deferral_during_disk_probe_prevents_stale_rejection():
    with _flow() as (conn, scan, db, repo, merges, trace, paths, probes):
        exists = paths.exists
        def interleave(path):
            scan.execute("UPDATE photos SET xmp_import_pending=1 WHERE id=2")
            scan.commit()
            return exists(path)
        paths.exists = interleave
        result = db.auto([1, 2])
        assert result.get("deferred") is True and result["rejected"] == 0
        assert merges == [] and not conn.in_transaction
        assert conn.execute("SELECT flag FROM photos WHERE id=2").fetchone()[0] is None


def test_auto_locked_revalidation_blocks_writer_without_disk_probes():
    with _flow() as (conn, scan, db, repo, merges, trace, paths, probes):
        plan = repo.resolution_plan
        blocked = []
        def locked(*args, **kwargs):
            assert conn.in_transaction
            result = plan(*args, **kwargs)
            try:
                scan.execute("UPDATE photos SET xmp_import_pending=1 WHERE id=2")
            except sqlite3.OperationalError as error:
                assert "locked" in str(error)
                scan.rollback()
                blocked.append(True)
            else:
                raise AssertionError("Concurrent writer bypassed resolver lock")
            return result
        repo.resolution_plan = locked
        result = db.auto(iter([1, 2, 2]))
        assert blocked == [True] and len(probes) == 2
        assert result == {"winner_id": 1, "loser_ids": [2], "rejected": 1}
        assert merges == [(1, [2])] and not conn.in_transaction


def test_auto_stale_group_membership_hash_path_and_pending_all_defer():
    changes = [
        "UPDATE photos SET xmp_import_pending=1 WHERE id=2",
        "UPDATE photos SET flag='rejected' WHERE id=2",
        "UPDATE photos SET filename='changed.jpg' WHERE id=1",
        "UPDATE photos SET folder_id=2 WHERE id=1",
        "UPDATE folders SET path='invented/changed' WHERE id=1",
        "UPDATE photos SET file_hash='changed' WHERE id=2",
        "UPDATE photos SET file_mtime=99 WHERE id=1",
        "INSERT INTO photos VALUES(3,2,'new.jpg','hash',3,0,NULL,0)",
        "DELETE FROM photos WHERE id=1",
    ]
    for sql in changes:
        with _flow() as (conn, scan, db, repo, merges, trace, paths, probes):
            begin = db.begin_immediate
            def before_lock(statement=sql, acquire=begin):
                scan.execute(statement)
                scan.commit()
                acquire()
            db.begin_immediate = before_lock
            assert db.auto([1, 2]).get("deferred") is True, sql
            assert merges == [] and not conn.in_transaction


def test_auto_pending_unselected_sibling_defers_until_recovery():
    with _flow() as (conn, scan, db, repo, merges, trace, paths, probes):
        scan.execute("INSERT INTO photos VALUES(3,2,'pending.jpg','hash',3,0,'rejected',1)")
        scan.commit()
        assert db.auto([1, 2]).get("deferred") is True
        assert merges == [] and probes == []
        scan.execute("UPDATE photos SET xmp_import_pending=0 WHERE id=3")
        scan.commit()
        assert db.auto([1, 2])["rejected"] == 1 and len(merges) == 1


def test_auto_outer_transaction_is_deferred_without_disk_or_commit():
    with _flow() as (conn, scan, db, repo, merges, trace, paths, probes):
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO notes VALUES('caller')")
        assert db.auto([1, 2]).get("deferred") is True
        assert conn.in_transaction and probes == [] and merges == []
        assert "COMMIT" not in trace and "ROLLBACK" not in trace
        conn.rollback()
        assert conn.execute("SELECT * FROM notes").fetchall() == []


def test_auto_revalidation_and_merge_errors_rollback_and_preserve_original():
    for stage in ("plan", "merge"):
        for original in (RuntimeError("invented failure"), KeyboardInterrupt(), SystemExit()):
            with _flow() as (conn, scan, db, repo, merges, trace, paths, probes):
                def fail(*args, error=original, **kwargs):
                    conn.execute("INSERT INTO notes VALUES('partial')")
                    conn.execute("UPDATE photos SET flag='rejected' WHERE id=2")
                    raise error
                if stage == "plan":
                    repo.resolution_plan = fail
                else:
                    db._apply_winner_loser_merge = fail
                try:
                    db.auto([1, 2])
                except BaseException as error:
                    assert error is original
                else:
                    raise AssertionError("Original error swallowed")
                assert not conn.in_transaction and conn.execute("SELECT * FROM notes").fetchall() == []
                assert conn.execute("SELECT flag FROM photos WHERE id=2").fetchone()[0] is None


def _keep_route(reason):
    tree = ast.parse((VIREO / "web/duplicates.py").read_text(encoding="utf-8"))
    factory = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "create_duplicates_blueprint")
    mapping = next(n.value for n in factory.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "keep_refusals" for t in n.targets))
    route = next(n for n in factory.body if isinstance(n, ast.FunctionDef) and n.name == "api_duplicates_keep")
    route.decorator_list = []
    lock = SimpleNamespace(acquire=Mock(return_value=True), release=Mock())
    result = {"error": reason} if reason else {"winner_id": 1, "loser_ids": [2]}
    namespace = {"keep_refusals": ast.literal_eval(mapping), "KEEP_CLEANUP_WAIT_SECS": 1,
                 "request": SimpleNamespace(get_json=lambda **kwargs: {"photo_id": 1}),
                 "loser_cleanup_lock": lock, "get_db": lambda: object(),
                 "duplicate_resolution": SimpleNamespace(keep_duplicate_copy=lambda *args: result),
                 "json_error": lambda error, status=400, **kwargs: ({"error": error, **kwargs}, status),
                 "jsonify": lambda value: value}
    exec(compile(ast.Module(body=[route], type_ignores=[]), "<inert keep route>", "exec"), namespace)
    return namespace, lock


def test_keep_pending_refusal_returns_intentional_409_and_releases_lock():
    namespace, lock = _keep_route("sidecar keywords pending")
    body, status = namespace["api_duplicates_keep"]()
    assert status == 409 and body["error"] == "sidecar keywords pending"
    assert "imported" in body["message"]
    lock.release.assert_called_once()


def test_keep_existing_refusals_and_success_keep_response_contract():
    for reason, status in [("not found", 404), ("not a duplicate", 409), ("missing on disk", 409), ("volume offline", 409)]:
        namespace, lock = _keep_route(reason)
        assert namespace["api_duplicates_keep"]()[1] == status
        lock.release.assert_called_once()
    namespace, lock = _keep_route(None)
    assert namespace["api_duplicates_keep"]() == {"winner_id": 1, "loser_ids": [2], "ok": True}
    lock.release.assert_called_once()


def test_auto_offline_and_small_group_keep_deferral_and_noop_contracts():
    with _flow() as (conn, scan, db, repo, merges, trace, paths, probes):
        assert db.auto([1]) == {"winner_id": None, "loser_ids": [], "rejected": 0}
        assert not conn.in_transaction and probes == [] and merges == []
        namespace = repo.resolution_preflight.__func__.__globals__
        def offline(path):
            assert not conn.in_transaction
            return True
        namespace["_volume_offline"] = offline
        assert db.auto([1, 2]).get("deferred") is True
        assert probes == [] and merges == [] and not conn.in_transaction
