"""Invented SQLite :memory: and fake paths only. Never import product modules."""

import __future__

import ast
import builtins
import contextlib
import dataclasses
import itertools
import json
import logging
import posixpath
import sqlite3
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def forbidden(*a, **k):
    raise AssertionError("Unexpected real I/O")


def load(file, ns, classes=None, functions=()):
    tree = ast.parse((ROOT / file).read_text(encoding="utf-8"))
    nodes = []
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in functions:
            nodes.append(n)
        if isinstance(n, ast.ClassDef) and classes and n.name in classes:
            if classes[n.name] is not None:
                n.body = [f for f in n.body if isinstance(f, ast.FunctionDef) and f.name in classes[n.name]]
            nodes.append(n)
    exec(
        compile(
            ast.Module(body=nodes, type_ignores=[]),
            "<inert-" + file + ">",
            "exec",
            flags=__future__.annotations.compiler_flag,
        ),
        ns,
    )


def setup(connection=None, *, seed=True):
    c = connection if connection is not None else sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    if seed:
        c.executescript("""CREATE TABLE folders(id INTEGER PRIMARY KEY,path TEXT);
     CREATE TABLE photos(id INTEGER PRIMARY KEY,folder_id INTEGER,filename TEXT,file_hash TEXT,flag TEXT,rating INTEGER,file_size INTEGER);
     CREATE TABLE photo_keywords(photo_id INTEGER,keyword_id INTEGER,source TEXT,PRIMARY KEY(photo_id,keyword_id));
     CREATE TABLE pending_changes(photo_id INTEGER,value TEXT);
     CREATE TABLE photo_embedded_keyword_offered(photo_id INTEGER,keyword_key TEXT,PRIMARY KEY(photo_id,keyword_key));
     CREATE TABLE duplicate_rejections(photo_id INTEGER PRIMARY KEY);
     CREATE TRIGGER trg_duplicate_rejections_clear AFTER UPDATE OF flag ON photos WHEN NEW.flag != 'rejected' BEGIN DELETE FROM duplicate_rejections WHERE photo_id=NEW.id; END;
     INSERT INTO folders VALUES(1,'/invented/a'),(2,'/invented/b'),(3,'/invented/c');
     INSERT INTO photos VALUES(1,1,'owl.jpg','H','flagged',4,64),(2,2,'owl.jpg','H','rejected',1,64),(3,3,'owl.jpg','H','rejected',2,64);
     INSERT INTO duplicate_rejections VALUES(2),(3);
     INSERT INTO photo_keywords VALUES(1,10,'manual'),(2,10,'accept'),(1,11,'accept');
     INSERT INTO photo_embedded_keyword_offered VALUES(1,'invented-offer');""")
    ns = {
        "__name__": "__main__",
        "__builtins__": {**vars(builtins), "__import__": forbidden},
        "dataclass": dataclasses.dataclass,
        "sqlite3": sqlite3,
        "logging": logging,
        "contextlib": contextlib,
        "os": types.SimpleNamespace(path=types.SimpleNamespace(join=posixpath.join, exists=lambda p: True)),
        "_volume_offline": lambda p: False,
        "embedded_keyword_associations_for_merge": lambda conn, pid, sid, **kw: [
            {"id": r[0]} for r in conn.execute("SELECT keyword_id FROM photo_keywords WHERE photo_id=?", (pid,))
        ],
        "KEYWORD_SOURCE_UNKNOWN": None,
        "KEYWORD_SOURCE_PRECEDENCE": (None, "accept", "manual"),
        "_KEYWORD_SOURCE_RANK": {None: 0, "accept": 1, "manual": 2},
        "_KEYWORD_SOURCE_UNRECOGNIZED_RANK": 1,
    }
    load("duplicates.py", ns, classes={"PhotoMetadata": None, "MergeResult": None}, functions={"merge_metadata"})
    mod = types.SimpleNamespace(PhotoMetadata=ns["PhotoMetadata"], merge_metadata=ns["merge_metadata"])
    ns["__builtins__"]["__import__"] = lambda name, *a, **kw: mod if name == "duplicates" else forbidden()
    load(
        "db.py",
        ns,
        classes={"Database": {"keep_duplicate_copy", "_apply_winner_loser_merge"}},
        functions={"keyword_source_rank", "keyword_source_max"},
    )
    load(
        "repositories/duplicates.py",
        ns,
        classes={
            "DuplicatesRepository": {
                "__init__",
                "keep_copy_plan",
                "keep_copy_refusal",
                "transaction",
                "decision_transaction",
                "photo_metadata",
                "set_flag",
                "set_rating",
                "loser_keyword_rows",
                "copy_loser_embedded_offered_keys",
                "reject",
                "_chunks",
            }
        },
    )
    repo = ns["DuplicatesRepository"](c)
    d = ns["Database"]()
    d._duplicates_repository = lambda: repo
    d.conn = c
    d.duplicates = repo

    def tag(pid, kid, *, source, _commit):
        existing = c.execute(
            "SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?", (pid, kid)
        ).fetchone()
        chosen = ns["keyword_source_max"](source, existing[0] if existing else None)
        c.execute(
            "INSERT INTO photo_keywords VALUES(?,?,?) ON CONFLICT(photo_id,keyword_id) DO UPDATE SET source=excluded.source",
            (pid, kid, chosen),
        )

    d.tag_photo = tag
    return c, repo, d, ns


_MEMORY_NAMES = itertools.count()


def pair(test):
    uri = f"file:vireo-duplicate-check-{next(_MEMORY_NAMES)}?mode=memory&cache=shared"
    a = sqlite3.connect(uri, uri=True, timeout=0)
    b = sqlite3.connect(uri, uri=True, timeout=0)
    test.addCleanup(a.close)
    test.addCleanup(b.close)
    return setup(a), setup(b, seed=False)


def cleanup_route(db, ns, files, *, during_trash=lambda: None, fail_delete=False):
    """Actual cleanup route and helpers, with invented filesystem callbacks."""
    c = db.conn
    events = []
    modules = {
        "audit": types.SimpleNamespace(confirmed_orphan_ids=lambda *a: []),
        "file_identity": types.SimpleNamespace(distinct_existing_file=lambda *a, **k: True),
    }
    ns["__builtins__"]["__import__"] = lambda name, *a, **k: modules[name] if name in modules else forbidden()
    # Real read queries over invented rows; no path/stat methods are exposed.
    db.duplicates.loser_candidate_rows = lambda ids: {
        row["id"]: row
        for row in c.execute(
            "SELECT p.id,p.flag,p.file_hash,p.filename,f.path AS folder_path FROM photos p JOIN folders f ON f.id=p.folder_id"
        )
        if row["id"] in ids
    }
    db.duplicates.live_paths_for_hash = lambda h: c.execute(
        "SELECT f.path,p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.file_hash=? AND p.flag != 'rejected'",
        (h,),
    ).fetchall()

    def delete(ids, *, commit=True):
        if fail_delete:
            raise RuntimeError("invented catalog failure")
        for pid in ids:
            for table in [
                "photo_keywords",
                "pending_changes",
                "photo_embedded_keyword_offered",
                "duplicate_rejections",
            ]:
                c.execute(f"DELETE FROM {table} WHERE photo_id=?", (pid,))
            c.execute("DELETE FROM photos WHERE id=?", (pid,))
        if commit:
            c.commit()
        events.append(("rows", c.in_transaction))
        return {"ids": list(ids), "deleted": len(ids), "files": [{"photo_id": pid} for pid in ids]}

    db.delete_photos = delete
    db.prune_pipeline_cache_for_ids = lambda ids: events.append(("prune", c.in_transaction))

    def trash(paths, **kwargs):
        during_trash()
        events.append(("trash", c.in_transaction))
        for path in paths:
            files.discard(path)  # Invented set only, never os.remove/unlink/Trash.
        return len(paths), set(paths), []

    ns.update(
        get_db=lambda: db,
        loser_cleanup_lock=contextlib.nullcontext(),
        _loser_photo_ids_from_request=lambda: ([2], None),
        network_volume_roots=lambda: [],
        path_on_network_volume=lambda *a: False,
        trash_paths=trash,
        chunked=lambda ids: [ids] if ids else [],
        cleanup_cached_files_for_deleted_photos=lambda files: events.append(("cache", c.in_transaction)),
        invalidate_missing_originals=lambda: events.append(("invalidate", c.in_transaction)),
        jsonify=lambda value: value,
        log=logging.getLogger("invented-cleanup"),
    )
    tree = ast.parse((ROOT / "web/duplicates.py").read_text(encoding="utf-8"))
    wanted = {
        "_classify_loser_candidates",
        "_record_trash_outcomes",
        "_drop_trashed_loser_rows",
        "_finish_trashed_loser_cleanup",
        "_trash_duplicate_losers",
        "_trash_loser_files",
        "api_duplicates_delete_loser_files",
    }
    nodes: list[ast.stmt] = [
        node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    nodes += [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "_DuplicateRowCleanupError"]
    for node in nodes:
        node.decorator_list = []
    exec(
        compile(
            ast.Module(body=nodes, type_ignores=[]),
            "<inert-cleanup-route>",
            "exec",
            flags=__future__.annotations.compiler_flag,
        ),
        ns,
    )
    return ns["api_duplicates_delete_loser_files"], events


class Checks(unittest.TestCase):
    def test_metadata_provenance_flags_repetition_and_no_history(self):
        c, r, d, n = setup()
        self.addCleanup(c.close)
        res = d.keep_duplicate_copy(2)
        self.assertEqual(res["winner_id"], 2)
        self.assertEqual(res["rating"], 4)
        self.assertEqual(res["flag"], "flagged")
        self.assertEqual(
            [tuple(x) for x in c.execute("SELECT id,flag FROM photos ORDER BY id")],
            [(1, "rejected"), (2, "flagged"), (3, "rejected")],
        )
        self.assertEqual(
            c.execute("SELECT source FROM photo_keywords WHERE photo_id=2 AND keyword_id=10").fetchone()[0], "manual"
        )
        self.assertEqual(d.keep_duplicate_copy(2)["loser_ids"], [])
        self.assertEqual(d.keep_duplicate_copy(1)["loser_ids"], [2])
        self.assertEqual(
            c.execute("SELECT keyword_key FROM photo_embedded_keyword_offered WHERE photo_id=2").fetchone()[0],
            "invented-offer",
        )

    def test_reject_failure_rolls_back_metadata_and_flags(self):
        c, r, d, n = setup()
        self.addCleanup(c.close)
        before = list(c.iterdump())
        r.reject = lambda ids: (_ for _ in ()).throw(RuntimeError("invented failure"))
        with self.assertRaisesRegex(RuntimeError, "invented failure"):
            d.keep_duplicate_copy(2)
        self.assertEqual(list(c.iterdump()), before)

    def test_missing_offline_and_unknown_refuse_without_stat_or_mutation(self):
        for case in ["missing", "offline", "unknown", "not_duplicate"]:
            with self.subTest(case=case):
                c, r, d, n = setup()
                self.addCleanup(c.close)
                pid = 2
                if case == "missing":
                    n["os"].path.exists = lambda p: False
                if case == "offline":
                    n["_volume_offline"] = lambda p: True
                    n["os"].path.exists = forbidden
                if case == "unknown":
                    pid = 999
                if case == "not_duplicate":
                    c.execute("UPDATE photos SET file_hash='OTHER' WHERE id=2")
                    c.commit()
                before = list(c.iterdump())
                result = d.keep_duplicate_copy(pid)
                self.assertIn("error", result)
                self.assertEqual(list(c.iterdump()), before)

    def test_restored_scan_reorients_current_flags_and_drops_stale_paths(self):
        c, r, d, n = setup()
        self.addCleanup(c.close)
        d.keep_duplicate_copy(2)
        d.conn = c
        n.update(
            _SQL_PARAM_CHUNK=900,
            KEPT_ANOTHER_COPY_REASON="kept another copy instead",
            _changes_since_scan=lambda *a: (0, 0),
            bucket_unresolved_proposals=lambda p: [],
        )
        load(
            "duplicate_scan.py",
            n,
            functions={"_fetch_photo_rows", "revalidate_scan_result", "_reorient_resolved_proposal"},
        )
        result = {
            "proposals": [
                {
                    "file_hash": "H",
                    "status": "resolved",
                    "winner": {"id": 1, "path": "/invented/a/owl.jpg"},
                    "losers": [
                        {"id": 2, "path": "/invented/b/owl.jpg", "rejected": True},
                        {"id": 3, "path": "/invented/c/owl.jpg", "rejected": True},
                    ],
                }
            ]
        }
        restored = n["revalidate_scan_result"](d, result)
        self.assertEqual(restored["proposals"][0]["winner"]["id"], 2)
        self.assertEqual(restored["proposals"][0]["losers"][0]["reason"], "kept another copy instead")
        c.execute("UPDATE photos SET filename='different.jpg' WHERE id=1")
        self.assertEqual(n["revalidate_scan_result"](d, result)["proposals"], [])

    def test_trash_classifier_rechecks_live_flags_and_distinct_anchor_without_trash(self):
        c, r, d, n = setup()
        self.addCleanup(c.close)
        d.keep_duplicate_copy(2)

        def rows(pids):
            return {
                row["id"]: row
                for row in c.execute(
                    "SELECT p.id,p.flag,p.file_hash,p.filename,f.path AS folder_path FROM photos p JOIN folders f ON f.id=p.folder_id"
                )
                if row["id"] in pids
            }

        def anchors(h):
            return c.execute(
                "SELECT f.path,p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.file_hash=? AND p.flag != 'rejected'",
                (h,),
            ).fetchall()

        db = types.SimpleNamespace(
            duplicates=types.SimpleNamespace(loser_candidate_rows=rows, live_paths_for_hash=anchors)
        )
        verified = [True]
        modules = {
            "audit": types.SimpleNamespace(confirmed_orphan_ids=lambda *a: []),
            "file_identity": types.SimpleNamespace(distinct_existing_file=lambda *a, **kw: verified[0]),
        }
        n["__builtins__"]["__import__"] = lambda name, *a, **kw: modules[name] if name in modules else forbidden()
        n.update(network_volume_roots=lambda: [], path_on_network_volume=lambda *a: False)
        tree = ast.parse((ROOT / "web/duplicates.py").read_text(encoding="utf-8"))
        node = next(
            x for x in ast.walk(tree) if isinstance(x, ast.FunctionDef) and x.name == "_classify_loser_candidates"
        )
        exec(compile(ast.Module(body=[node], type_ignores=[]), "<inert-trash-classifier>", "exec"), n)
        candidates, removed, skipped, roots = n["_classify_loser_candidates"](db, [2])
        self.assertEqual(candidates, [])
        self.assertEqual(removed, [])
        self.assertEqual(skipped[0]["reason"], "photo is not rejected")
        candidates, *_ = n["_classify_loser_candidates"](db, [1])
        self.assertEqual(candidates, [(1, "/invented/a/owl.jpg")])
        verified[0] = False
        candidates, removed, skipped, roots = n["_classify_loser_candidates"](db, [1])
        self.assertEqual(candidates, [])
        self.assertIn("no verified distinct", skipped[0]["reason"])

    def test_keep_switchback_is_not_metadata_undo_and_route_is_explicitly_a_gap(self):
        c, r, d, n = setup()
        self.addCleanup(c.close)
        before = r.photo_metadata(2)
        d.keep_duplicate_copy(2)
        d.keep_duplicate_copy(1)
        after = r.photo_metadata(2)
        self.assertEqual(c.execute("SELECT flag FROM photos WHERE id=1").fetchone()[0], "flagged")
        self.assertNotEqual((before.rating, before.keyword_ids), (after.rating, after.keyword_ids))
        decisions = json.loads((ROOT / "tests/contracts/undo_coverage.json").read_text(encoding="utf-8"))
        route = "POST /api/duplicates/keep"
        self.assertEqual([name for name, group in decisions.items() if route in group["routes"]], ["gap"])
        listed = [entry for group in decisions.values() for entry in group["routes"]]
        self.assertEqual(len(listed), len(set(listed)))

    def test_ast_source_loading_is_independent_of_windows_locale(self):
        original = Path.read_text

        def windows_default(path, encoding=None, errors=None):
            return original(path, encoding=encoding or "cp1252", errors=errors)

        with patch.object(Path, "read_text", windows_default):
            c, repo, db, ns = setup()
            self.addCleanup(c.close)
            self.assertEqual(db.keep_duplicate_copy(2)["winner_id"], 2)

    def test_keeper_planning_serializes_separate_connections(self):
        (c, repo, db, ns), (other, other_repo, other_db, other_ns) = pair(self)
        original = repo.keep_copy_plan
        blocked = []
        other_plans = []
        other_plan = other_repo.keep_copy_plan

        def observe_other_plan(pid):
            other_plans.append(pid)
            return other_plan(pid)

        other_repo.keep_copy_plan = observe_other_plan

        def plan(pid):
            result = original(pid)
            try:
                other_db.keep_duplicate_copy(3)
            except sqlite3.OperationalError:
                blocked.append(True)
            return result

        repo.keep_copy_plan = plan
        db.keep_duplicate_copy(2)
        keepers = [row[0] for row in c.execute("SELECT id FROM photos WHERE flag != 'rejected'")]
        self.assertEqual(keepers, [2])
        self.assertTrue(blocked, "second writer must stop before its plan")
        self.assertEqual(other_plans, [])
        other_db.keep_duplicate_copy(3)
        self.assertEqual(other_plans, [3])
        self.assertEqual([row[0] for row in c.execute("SELECT id FROM photos WHERE flag != 'rejected'")], [3])

    def test_cleanup_holds_writer_through_fake_trash_and_row_removal(self):
        (c, repo, db, ns), (other, other_repo, other_db, other_ns) = pair(self)
        blocked = []

        def interleave():
            try:
                other_db.keep_duplicate_copy(2)
            except sqlite3.OperationalError:
                blocked.append(True)

        files = {"/invented/a/owl.jpg", "/invented/b/owl.jpg", "/invented/c/owl.jpg"}
        route, events = cleanup_route(db, ns, files, during_trash=interleave)
        self.assertTrue(route()["ok"])
        self.assertTrue(blocked, "keep must not promote an already classified loser")
        self.assertEqual(c.execute("SELECT flag FROM photos WHERE id=1").fetchone()[0], "flagged")
        self.assertIsNone(c.execute("SELECT id FROM photos WHERE id=2").fetchone())
        self.assertEqual(other_db.keep_duplicate_copy(2), {"error": "not found"})
        self.assertIn(("trash", True), events)
        self.assertIn(("rows", True), events)
        self.assertIn(("prune", False), events)
        self.assertIn(("cache", False), events)
        self.assertIn("/invented/a/owl.jpg", files)

    def test_cleanup_waits_for_keep_then_rechecks_live_flags(self):
        (c, repo, db, ns), (other, other_repo, other_db, other_ns) = pair(self)
        files = {"/invented/a/owl.jpg", "/invented/b/owl.jpg"}
        route, events = cleanup_route(other_db, other_ns, files)
        blocked = []
        attempted = []

        def exists(path):
            if c.in_transaction and not attempted:
                attempted.append(True)
                try:
                    route()
                except sqlite3.OperationalError:
                    blocked.append(True)
            return path in files

        ns["os"].path.exists = exists
        self.assertEqual(db.keep_duplicate_copy(2)["winner_id"], 2)
        self.assertTrue(blocked)
        result = route()
        self.assertEqual(result["trashed"], 0)
        self.assertEqual(result["skipped"][0]["reason"], "photo is not rejected")
        self.assertIn("/invented/b/owl.jpg", files)

    def test_restored_winner_uses_merged_catalog_rating_and_size(self):
        c, repo, db, ns = setup()
        self.addCleanup(c.close)
        db.keep_duplicate_copy(2)
        ns.update(
            _SQL_PARAM_CHUNK=900,
            KEPT_ANOTHER_COPY_REASON="kept another copy instead",
            _changes_since_scan=lambda *a: (0, 0),
            bucket_unresolved_proposals=lambda p: [],
        )
        load(
            "duplicate_scan.py",
            ns,
            functions={"_fetch_photo_rows", "revalidate_scan_result", "_reorient_resolved_proposal"},
        )
        saved = {
            "proposals": [
                {
                    "file_hash": "H",
                    "status": "resolved",
                    "winner": {"id": 1, "path": "/invented/a/owl.jpg", "rating": 4},
                    "losers": [{"id": 2, "path": "/invented/b/owl.jpg", "rating": 0, "file_size": 0, "rejected": True}],
                }
            ]
        }
        restored = ns["revalidate_scan_result"](db, saved)
        winner = restored["proposals"][0]["winner"]
        self.assertEqual((winner["id"], winner["rating"], winner["file_size"]), (2, 4, 64))
        self.assertNotIn("rejected", winner)
        self.assertEqual(saved["proposals"][0]["losers"][0]["rating"], 0)

    def test_cleanup_failure_rolls_back_rows_and_defers_all_cache_effects(self):
        c, repo, db, ns = setup()
        self.addCleanup(c.close)
        before = list(c.iterdump())
        files = {"/invented/b/owl.jpg"}
        route, events = cleanup_route(db, ns, files, fail_delete=True)
        with self.assertLogs("invented-cleanup", level="ERROR"):
            payload, status = route()
        self.assertEqual(status, 500)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["trashed"], 1)  # Only the invented set changed.
        self.assertEqual(list(c.iterdump()), before)
        self.assertFalse(any(event[0] in {"prune", "cache"} for event in events))
        self.assertFalse(c.in_transaction)


if __name__ == "__main__":
    unittest.main()
