"""Selected AST, invented SQLite and fake finalization only; no product imports."""

import ast
import builtins
import json
import posixpath
import sqlite3
import types
from pathlib import Path

from services import classify_preflight

ROOT = Path(__file__).resolve().parents[1]


def forbidden(*a, **kw):
    raise AssertionError("Forbidden real operation/commit")


def classify_functions(names):
    package = ROOT / "classify_job"
    paths = sorted(package.glob("*.py")) if package.is_dir() else [ROOT / "classify_job.py"]
    return [
        node
        for path in paths
        for node in ast.parse(path.read_text(encoding="utf-8")).body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]


def make_repo():
    root = ROOT
    ns = {"Any": object}
    t = ast.parse((root / "repositories/model_runs.py").read_text(encoding="utf-8"))
    nodes = [n for n in t.body if isinstance(n, (ast.Assign, ast.ClassDef, ast.FunctionDef))]
    exec(
        compile(
            ast.Module(body=nodes, type_ignores=[]),
            "<inert model-runs>",
            "exec",
            flags=__import__("__future__").annotations.compiler_flag,
        ),
        ns,
    )
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    snapshot = json.loads((root / "tests/fixtures/canonical_schema_snapshot.json").read_text(encoding="utf-8"))
    for kind, name, _table, sql in snapshot["sqlite_master"]:
        if kind == "table" and (not name.startswith("sqlite_")):
            c.execute(sql)
    c.execute("INSERT INTO folders(id,path) VALUES(1,'/invented')")
    for pid in range(1, 10):
        c.execute(
            "INSERT INTO photos(id,folder_id,filename,extension,file_hash)VALUES(?,1,?,'jpg',?)",
            (pid, f"invented{pid}.jpg", f"h{pid}"),
        )
        category = "person" if pid == 7 else "animal"
        detector = "full-image" if pid == 5 else "megadetector-v6" if pid == 6 else "invented-detector"
        conf = 0.1 if pid == 6 else 0.9
        c.execute(
            "INSERT INTO detections(id,photo_id,detector_model,detector_confidence,category,runtime_fingerprint)VALUES(?,?,?,?,?,?)",
            (pid, pid, detector, conf, category, "detector-rt"),
        )
        c.execute(
            "INSERT INTO classifier_runs(detection_id,classifier_model,labels_fingerprint,runtime_fingerprint,input_recipe)VALUES(?,?,?,?,?)",
            (
                pid,
                "invented-model",
                "invented-fp",
                "incomplete" if pid == 9 else "classifier-rt",
                "raw-recipe" if pid == 4 else None,
            ),
        )
        if pid not in (2, 8):
            c.execute(
                "INSERT INTO classifier_match_scores(detection_id,classifier_model,labels_fingerprint,max_match_score)VALUES(?,?,?,?)",
                (pid, "invented-model", "other-fp" if pid == 3 else "invented-fp", 0.01),
            )
        if pid == 8:
            c.execute(
                "INSERT INTO predictions(id,detection_id,classifier_model,labels_fingerprint,species,confidence)VALUES(8,8,'invented-model','invented-fp','invented',0)"
            )
    c.commit()
    r = ns["ModelRunsRepository"](c, auto_match_review_marker="AUTO", commit_with_retry=forbidden)
    return (c, r, ns)


def test_shared_cache_output_readers():
    c, r, ns = make_repo()
    try:
        ids = list(range(1, 10))
        before = c.total_changes
        hits = classify_preflight.get_classifier_run_cache_hits(r, ids, "invented-model", "invented-fp", min_conf=0.2)
        assert hits == {1, 5, 8}, hits
        run_filter = r.classifier_cache_run_filter("invented-model", "invented-fp", set(), True)
        for pid, wanted in ((1, True), (2, False), (3, False), (4, False), (5, True), (8, True), (9, False)):
            assert bool(r.cache_coverage_counts([pid], run_filter, 0.2)[1]) == wanted, pid
            assert bool(list(r.covering_classifier_identities([pid], run_filter, 0.2))) == wanted, pid
        assert classify_preflight.get_classifier_run_cache_hits(
            r, [6], "invented-model", "invented-fp", min_conf=0.2, contextual_weak_photo_ids=[6], weak_confidence=0.05
        ) == {6}
        assert not classify_preflight.get_classifier_run_cache_hits(
            r, [1],
            "invented-model",
            "invented-fp",
            min_conf=0.2,
            expected_classifier_runtime_by_detector_runtime={"detector-rt": "wrong-runtime"},
        )
        assert classify_preflight.get_classifier_run_cache_hits(
            r, [1],
            "invented-model",
            "invented-fp",
            min_conf=0.2,
            expected_classifier_runtime_by_detector_runtime={"detector-rt": "classifier-rt"},
        ) == {1}
        assert c.total_changes == before and (not c.in_transaction)
        print(
            "PASS invented match-only, torn, foreign-key output, RAW recipe, full-image, zero-confidence prediction, incomplete, weak-floor and runtime identity controls across preflight/coverage/identity readers"
        )
    finally:
        c.close()


def test_cached_fingerprint_uses_stored_output():
    c, r, ns = make_repo()
    root = ROOT
    try:
        fn = classify_functions({"_cached_labels_fingerprint"})[0]
        q = {
            "_cache_run_filter": lambda db, *args: r.classifier_cache_run_filter(
                "invented-model", "invented-fp", set(), True
            ),
            "__builtins__": {
                **vars(builtins),
                "__import__": lambda name, *a, **kw: (
                    types.SimpleNamespace(_chunks=lambda rows: (rows[i : i + 2] for i in range(0, len(rows), 2)))
                    if name == "db"
                    else forbidden()
                ),
            },
        }
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "<inert fingerprint>", "exec"), q)
        db = types.SimpleNamespace(model_runs=r)
        assert q["_cached_labels_fingerprint"](db, [2, 3, 1], "invented-model", 0.2) == "invented-fp"
        assert q["_cached_labels_fingerprint"](db, [2, 3, 4, 9], "invented-model", 0.2) is None
        assert q["_cached_labels_fingerprint"](db, [], "invented-model", 0.2) is None
        print(
            "PASS cached-only fallback fingerprint names the same match-score-backed identity and skips torn/foreign/recipe/incomplete runs with fake chunk import"
        )
    finally:
        c.close()


def test_cached_only_counts_no_match_without_inventing_predictions():
    c, repo, _ = make_repo()
    names = {"_finalize_cached_only", "_cached_only_raw_results", "_cached_only_reused_count", "_cached_only_identity"}
    nodes = classify_functions(names)
    steps, finalized = ([], [])

    def imports(name, *a, **kw):
        if name == "taxonomy":
            return types.SimpleNamespace(load_local_taxonomy=lambda: None)
        if name == "db":
            return types.SimpleNamespace(_chunks=lambda rows: (rows[i : i + 2] for i in range(0, len(rows), 2)))
        return forbidden(name)

    ns = {
        "__builtins__": {**vars(builtins), "__import__": imports},
        "os": types.SimpleNamespace(path=posixpath),
        "_photo_timestamp": lambda p: None,
        "_cached_prediction_taxonomy": lambda p: {},
        "_finalize_remaining_steps": lambda *a, **kw: None,
        "_cached_only_label_source": lambda *a, **kw: None,
        "_cached_only_detection_map": lambda db, ids: {pid: [{"id": pid}] for pid in ids},
        "_cache_run_filter": lambda db, ids, model, fp, *a: repo.classifier_cache_run_filter(model, fp, set(), True),
    }

    def finalize(db, runner, job, params, tax, total, raw, model, fp):
        finalized.append(list(raw))
        return dict(predictions_stored=len(raw), burst_groups=0, already_labeled=0)

    ns["_finalize_cached_results"] = finalize
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<inert finalize>", "exec"), ns)
    db = types.SimpleNamespace(
        model_runs=repo,
        folders=types.SimpleNamespace(tree=lambda: [{"id": 1, "path": "/invented"}]),
        get_predictions_for_detection=lambda did, **kw: [
            dict(r)
            for r in c.execute(
                "SELECT * FROM predictions WHERE detection_id=? AND classifier_model=? AND (? IS NULL OR labels_fingerprint=?) AND confidence>=0 ORDER BY confidence DESC",
                (did, kw["classifier_model"], kw["labels_fingerprint"], kw["labels_fingerprint"]),
            )
        ],
    )
    runner = types.SimpleNamespace(update_step=lambda *a, **kw: steps.append(kw))
    before = c.total_changes
    try:
        for ids, count, predictions in (([1], 1, 0), ([1, 8], 2, 1), ([2], 0, 0)):
            photos = [dict(id=pid, folder_id=1, filename=f"invented{pid}.jpg") for pid in ids]
            result = ns["_finalize_cached_only"](
                db,
                runner,
                {"id": "invented"},
                object(),
                photos,
                classifier_model="invented-model",
                labels_fingerprint="invented-fp",
                detector_confidence=0.2,
            )
            assert result["already_classified"] == count
            assert result["predictions_stored"] == predictions and len(finalized[-1]) == predictions
            assert steps[-1]["summary"] == (f"{count} cached" if count else "No cached results")
        assert c.total_changes == before
        # A separate RAW recipe can have a stronger prediction while normal
        # coverage comes only from the completed no-match run. Its identity
        # must not replace the original coverage filter for the reuse count.
        c.execute(
            "INSERT INTO classifier_runs(detection_id,classifier_model,labels_fingerprint,input_recipe)VALUES(1,'invented-model','raw-fp','raw-recipe')"
        )
        c.execute(
            "INSERT INTO predictions(detection_id,classifier_model,labels_fingerprint,species,confidence)VALUES(1,'invented-model','raw-fp','invented-raw',0.99)"
        )
        c.commit()
        before = c.total_changes
        result = ns["_finalize_cached_only"](
            db,
            runner,
            {"id": "invented"},
            object(),
            [dict(id=1, folder_id=1, filename="invented1.jpg")],
            classifier_model="invented-model",
            labels_fingerprint=None,
            detector_confidence=0.2,
        )
        assert result["already_classified"] == 1
        assert steps[-1]["summary"] == "1 cached"
        assert result["predictions_stored"] == 0
        assert finalized[-1] == []
        # Resolve both unbound inputs from the accepted run, and preserve a
        # normal prediction on another detection in the same cached identity.
        for model in (None, "invented-model"):
            for ids, expected_ids in (([1], []), ([1, 8], [8])):
                photos = [dict(id=pid, folder_id=1, filename=f"invented{pid}.jpg") for pid in ids]
                raw, resolved_model, resolved_fp = ns["_cached_only_raw_results"](
                    db,
                    photos,
                    {1: "/invented"},
                    {pid: [{"id": pid}] for pid in ids},
                    model,
                    None,
                    0.2,
                )
                assert [row["detection_id"] for row in raw] == expected_ids
                assert (resolved_model, resolved_fp) == ("invented-model", "invented-fp")
        assert c.total_changes == before
    finally:
        c.close()


if __name__ == "__main__":
    for name, check in list(globals().items()):
        if name.startswith("test_"):
            check()
            print("PASS", name)
