"""Selected source definitions and invented SQLite :memory: only."""

import __future__

import ast
import sqlite3
from pathlib import Path

from repositories.top_species import TOP_SPECIES_RANKING_SQL

ROOT = Path(__file__).resolve().parents[1]


def selected_class(text, name, methods, ns):
    cls = next(n for n in ast.parse(text).body if isinstance(n, ast.ClassDef) and n.name == name)
    cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in methods]
    exec(
        compile(
            ast.Module(body=[cls], type_ignores=[]), "<inert-class>", "exec", flags=__future__.annotations.compiler_flag
        ),
        ns,
    )
    return ns[name]


def catalog(photo_source=None):
    ns = {"TOP_SPECIES_RANKING_SQL": TOP_SPECIES_RANKING_SQL}
    cls = selected_class(
        photo_source or (ROOT / "repositories/photos.py").read_text(encoding="utf-8"),
        "PhotoRepository",
        {"_browse_summary_scope", "get_browse_summary", "_browse_summary_aggregates", "_browse_analysis_coverage"},
        ns,
    )
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE folders(id,path,name,status);
        CREATE TABLE photos(id,folder_id,subject_tenengrad,eye_tenengrad,miss_computed_at);
        CREATE TABLE photo_workspace_visibility(photo_id,workspace_id);
        CREATE TABLE detections(id,photo_id,category,detector_model,detector_confidence);
        CREATE TABLE predictions(id,detection_id,species,confidence,classifier_model,labels_fingerprint,created_at);
        CREATE TABLE prediction_review(prediction_id,workspace_id,status);
        CREATE TABLE detector_runs(photo_id,detector_model,box_count);
        INSERT INTO folders VALUES(1,'/invented','one','ok'),
            (2,'/invented-offline','two','missing'),(3,'/invented-partial','three','partial');
        INSERT INTO photos VALUES(1,1,0,0,'computed'),(2,3,NULL,NULL,NULL),
            (3,1,NULL,NULL,NULL),(4,1,NULL,NULL,NULL),(5,1,NULL,NULL,NULL),
            (6,1,NULL,NULL,NULL),(7,1,NULL,NULL,NULL),(8,1,NULL,NULL,NULL),
            (9,1,10,10,'hidden'),(10,2,10,10,'offline');
        INSERT INTO photo_workspace_visibility VALUES(1,1),(2,1),(3,1),(4,1),
            (5,1),(6,1),(7,1),(8,1),(9,2),(10,1);
        INSERT INTO detections VALUES(1,1,'animal','real',.9),(2,1,'animal','real',.8),
            (3,3,'person','real',.9),(4,4,'animal','real',.05),
            (5,5,'animal','real',.9),(7,7,'animal','full-image',1),
            (8,8,'animal','other-model',.9),(9,9,'animal','real',.9),
            (10,10,'animal','real',.9);
        INSERT INTO detector_runs VALUES(1,'real',2),(2,'real',0),(3,'real',1),
            (4,'real',1),(6,'real',1),(7,'full-image',1),(8,'real',1),
            (9,'real',1),(10,'real',1);
    """)
    repo = cls()
    repo.conn, repo.workspace_id = c, 1
    repo.get_folder_subtree_ids = lambda fid: [fid]
    repo._build_collection_query = lambda cid: ("", "", "WHERE p.id=?", [2]) if cid == 1 else None
    repo._build_query_from_rules = lambda rules: ("", "", "WHERE p.id=?", [rules["id"]])
    return c, repo


def summary(repo, floor=0.2, **kwargs):
    return repo.get_browse_summary(
        **kwargs,
        detector_confidence=lambda: floor,
    )


def coverage_controls(photo_source=None):
    c, repo = catalog(photo_source)
    try:
        data = summary(repo)
        assert "with_detection_run" in data["analysis"], "result boxes are not completed-run coverage"
        assert data["total"] == data["filtered_total"] == 8
        assert data["analysis"] == {
            "with_detection_run": 4,
            "with_detection": 3,
            "with_quality": 1,
            "with_eye": 1,
            "with_misses": 1,
        }
        for pid in [1, 2, 3, 4]:
            assert summary(repo, rules={"id": pid})["analysis"]["with_detection_run"] == 1
        for pid in [5, 6, 7, 8]:
            assert summary(repo, rules={"id": pid})["analysis"]["with_detection_run"] == 0
        lower = summary(repo, floor=0.01)["analysis"]
        assert lower["with_detection"] == 4 and lower["with_detection_run"] == 4
        assert summary(repo, folder_id=3)["analysis"]["with_detection_run"] == 1
        assert summary(repo, collection_id=1)["filtered_total"] == 1
        assert all(value == 0 for value in summary(repo, rules={"id": 999})["analysis"].values())
        # Verify the same consistency semantics as the existing detector cache gate.
        ns = {}
        cls = selected_class(
            (ROOT / "repositories/model_runs.py").read_text(encoding="utf-8"),
            "ModelRunsRepository",
            {"get_detector_run_photo_ids"},
            ns,
        )
        runs = cls()
        runs.conn = c
        assert len(runs.get_detector_run_photo_ids("real") & set(range(1, 9))) == 4
    finally:
        c.close()


def test_completed_runs_and_qualifying_results_are_separate():
    coverage_controls()


def test_summary_keeps_caller_transaction_and_cleans_temp_scope():
    c, repo = catalog()
    try:
        c.execute("UPDATE photos SET subject_tenengrad=9 WHERE id=2")
        assert summary(repo)["analysis"]["with_quality"] == 2 and c.in_transaction
        assert not c.execute("SELECT name FROM sqlite_temp_master WHERE name='_browse_summary_ids'").fetchall()
        c.rollback()
        assert c.execute("SELECT subject_tenengrad FROM photos WHERE id=2").fetchone()[0] is None
        try:
            summary(repo, collection_id=999)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid collection widened scope")
    finally:
        c.close()


if __name__ == "__main__":
    test_completed_runs_and_qualifying_results_are_separate()
    test_summary_keeps_caller_transaction_and_cleans_temp_scope()
    print("PASS invented run/result identity, scope, threshold, zero values, TEMP and transaction controls")
