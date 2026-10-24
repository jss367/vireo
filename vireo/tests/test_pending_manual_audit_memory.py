"""Pending manual plans and drift reads on invented SQLite and fake paths only."""

import __future__

import ast
import builtins
import contextlib
import posixpath
import sqlite3
import typing
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import Mock

VIREO = Path(__file__).resolve().parents[1]


def _selected(module, name, namespace, cls=None):
    tree = ast.parse((VIREO / module).read_text(encoding="utf-8"))
    body = tree.body if cls is None else next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls).body
    if module == "db.py" and not any(isinstance(n, ast.FunctionDef) and n.name == name for n in body):
        body = ast.parse((VIREO / "services/duplicate_resolution.py").read_text()).body
        namespace["cast"] = typing.cast
    node = next(n for n in body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<manual/audit memory>", "exec", flags=__future__.annotations.compiler_flag), namespace)
    return namespace[name]


@contextlib.contextmanager
def _manual():
    with contextlib.closing(sqlite3.connect(":memory:")) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE folders(id INTEGER PRIMARY KEY,path TEXT);
            INSERT INTO folders VALUES(1,'invented/keep'),(2,'invented/other');
            CREATE TABLE photos(id INTEGER PRIMARY KEY,folder_id INTEGER,filename TEXT,
                file_hash TEXT,file_mtime REAL,rating INTEGER,flag TEXT,xmp_import_pending INTEGER);
            INSERT INTO photos VALUES(1,1,'one.jpg','hash',1,3,'flagged',0),
                (2,2,'two.jpg','hash',2,4,NULL,0),(3,2,'three.jpg','hash',3,0,'rejected',0);
            CREATE TABLE keywords(photo_id INTEGER,name TEXT,source TEXT);
            INSERT INTO keywords VALUES(1,'User keyword','manual'),(2,'Sidecar keyword','xmp');
        """)
        chooser = Mock(return_value=(1, []))
        def fake_import(name, *args, **kwargs):
            assert name == "duplicates"
            return SimpleNamespace(DupCandidate=lambda **kw: SimpleNamespace(**kw), resolve_duplicates=chooser)
        paths = SimpleNamespace(normpath=posixpath.normpath, join=posixpath.join, exists=Mock(return_value=True))
        namespace = {"os": SimpleNamespace(path=paths),
                     "__builtins__": {**vars(builtins), "__import__": fake_import}}
        repo = SimpleNamespace(conn=conn, keep_copy_refusal=lambda pid: None,
                               photo_metadata=lambda pid: SimpleNamespace(rating=4))
        for name in ("_keep_folder_rows", "keep_folder_preflight", "keep_folder_plan", "keep_copy_plan"):
            setattr(repo, name, MethodType(_selected("repositories/duplicates.py", name, namespace, "DuplicatesRepository"), repo))
        events = []
        def merge(*args, **kwargs):
            events.append((args, kwargs))
            conn.commit()
        db = SimpleNamespace(_duplicates_repository=lambda: repo, begin_immediate=lambda: conn.execute("BEGIN IMMEDIATE"),
                             rollback=conn.rollback, transaction=lambda: conn, _apply_winner_loser_merge=merge)
        db.duplicates = repo
        for name in ("keep_duplicate_copy", "bulk_resolve_by_folder"):
            setattr(db, "_fake_" + name, MethodType(_selected("db.py", name, {"os": SimpleNamespace(path=paths)}, "Database"), db))
        yield conn, repo, db, paths, events


def test_manual_folder_defers_pending_kept_loser_and_rejected_members():
    for pending in (1, 2, 3):
        with _manual() as (conn, repo, db, paths, events):
            conn.execute("UPDATE photos SET xmp_import_pending=1 WHERE id=?", (pending,))
            conn.commit()
            before = conn.total_changes
            assert repo.keep_folder_plan("hash", "invented/keep") == (None, None, "sidecar keywords pending")
            assert db._fake_bulk_resolve_by_folder(["hash"], "invented/keep") == {
                "resolved": [], "skipped": [{"file_hash": "hash", "reason": "sidecar keywords pending"}],
            }
            assert events == [] and conn.total_changes == before
            paths.exists.assert_not_called()


def test_manual_copy_defers_pending_chosen_live_and_rejected_members():
    for chosen in (1, 3):
        for pending in (1, 2, 3):
            with _manual() as (conn, repo, db, paths, events):
                conn.execute("UPDATE photos SET xmp_import_pending=1 WHERE id=?", (pending,))
                conn.commit()
                before = conn.total_changes
                assert repo.keep_copy_plan(chosen) == (None, "sidecar keywords pending")
                assert db._fake_keep_duplicate_copy(chosen) == {"error": "sidecar keywords pending"}
                assert not conn.in_transaction and conn.total_changes == before and events == []


def test_manual_operations_resume_only_after_all_members_recover():
    with _manual() as (conn, repo, db, paths, events):
        conn.execute("UPDATE photos SET xmp_import_pending=1 WHERE id IN (1,2)")
        conn.execute("UPDATE photos SET xmp_import_pending=0 WHERE id=1")
        assert repo.keep_folder_plan("hash", "invented/keep")[2] == "sidecar keywords pending"
        assert repo.keep_copy_plan(3)[1] == "sidecar keywords pending"
        conn.execute("UPDATE photos SET xmp_import_pending=0 WHERE id=2")
        conn.commit()
        assert repo.keep_folder_plan("hash", "invented/keep") == (1, [2], None)
        plan, reason = repo.keep_copy_plan(3)
        assert reason is None and plan == {"file_hash": "hash", "loser_ids": [1, 2], "winner_flag": "flagged"}
        assert db._fake_keep_duplicate_copy(3)["winner_id"] == 3
        assert db._fake_bulk_resolve_by_folder(["hash"], "invented/keep")["resolved"][0]["winner_id"] == 1
        assert len(events) == 2
        assert [tuple(row) for row in conn.execute("SELECT * FROM keywords")] == [
            (1, "User keyword", "manual"), (2, "Sidecar keyword", "xmp"),
        ]


def _drift(snapshot, *, stat_error=None):
    rows = [{"id": 1, "folder_id": 7, "filename": "invented.jpg"}]
    keywords = [{"name": "User keyword"}]
    reader = Mock(return_value=snapshot)
    logger = SimpleNamespace(info=Mock(), warning=Mock())
    stat = Mock(side_effect=stat_error)
    paths = SimpleNamespace(splitext=posixpath.splitext, join=posixpath.join, exists=lambda path: True)
    db = SimpleNamespace(photos=SimpleNamespace(list_page=lambda **kwargs: rows),
                         folders=SimpleNamespace(tree=lambda: [{"id": 7, "path": "invented"}]),
                         photo_keywords=SimpleNamespace(get_for_photo=lambda pid: keywords))
    import audit

    namespace = {
        "os": SimpleNamespace(path=paths, stat=stat), "read_keyword_snapshot": reader,
        "read_keywords": lambda path: snapshot[0] if snapshot is not None else set(), "log": logger,
        "sidecar_read_problem": lambda path: ("corrupt", "invented parse error"),
        **{name: getattr(audit, name) for name in (
            "DRIFT_RESOLUTIONS", "_UNREADABLE_SIDECAR_RESOLUTIONS",
            "_UNREADABLE_SIDECAR_ADVICE", "_UNREADABLE_SIDECAR_ERROR",
        )},
    }
    for helper in ("_unreadable_sidecar_drift", "_photo_drift"):
        _selected("audit.py", helper, namespace)
    fn = _selected("audit.py", "check_drift", namespace)
    result = fn(db)
    assert keywords == [{"name": "User keyword"}]
    return result, reader, logger


def test_drift_unreadable_sidecar_is_not_successful_empty_keywords():
    result, reader, logger = _drift(None)
    # Its own row, which claims nothing about the sidecar's keywords.
    assert len(result) == 1 and result[0]["direction"] == "sidecar_unreadable"
    assert result[0]["xmp_value"] is None and result[0]["removed_in_xmp"] == []
    assert result[0]["xmp_error"] == "it is not valid XMP (invented parse error)"
    assert result[0]["resolutions"] == ["use_db"]
    reader.assert_called_once_with("invented/invented.xmp")
    logger.warning.assert_called_once()


def test_drift_readable_empty_missing_and_real_changes_remain_distinct():
    for snapshot in ((set(), [], None, None),):
        result, reader, logger = _drift(snapshot)
        assert len(result) == 1 and result[0]["direction"] == "db_ahead"
        assert result[0]["removed_in_xmp"] == ["User keyword"]
        logger.warning.assert_not_called()
    result, reader, logger = _drift(None, stat_error=FileNotFoundError("invented absence"))
    assert len(result) == 1 and result[0]["direction"] == "db_ahead"
    reader.assert_not_called()
    assert _drift(({"USER KEYWORD"}, [], None, None))[0] == []
    result, _, _ = _drift(({"External"}, [], None, None))
    assert result[0]["added_in_xmp"] == ["External"] and result[0]["direction"] == "both"


def test_drift_stat_access_errors_are_unreadable_rows_not_absence():
    for error in (PermissionError("invented denial"), OSError("invented IO")):
        result, reader, logger = _drift(None, stat_error=error)
        assert len(result) == 1 and result[0]["direction"] == "sidecar_unreadable"
        assert result[0]["xmp_problem"] == "unreadable"
        assert result[0]["xmp_error"] == f"it could not be opened ({error})"
        assert result[0]["removed_in_xmp"] == [] and result[0]["resolutions"] == []
        reader.assert_not_called()
        assert logger.warning.call_args.kwargs == {"exc_info": True}
