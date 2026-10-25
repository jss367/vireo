"""Behavior pins for the computation-cache domain of ``Database``.

``computation_cache`` exports fully identified detector and classifier runs
as portable artifacts and materializes trusted artifacts onto matching
catalog rows; the catalog SQL both directions run lives in
``repositories/computation_cache.py`` behind the ``db.computation_cache``
accessor. These tests pin what each read returns and that no write commits
on its own (``computation_cache`` commits or rolls back around them). The
end-to-end export/import behavior is covered by ``test_computation_cache.py``;
the structural tests at the end keep the SQL in the repository.
"""

import ast
import contextlib
import inspect
import sqlite3
import textwrap
from pathlib import Path

import computation_cache
import pytest
from db import Database
from repositories.computation_cache import ComputationCacheRepository
from services import photo_ingest, workspace_folders

HASH = "a" * 64
RUNTIME = "b" * 64
INPUT = "c" * 64
FULL = "d" * 64
MODEL = "bioclip"


def _committed(db, sql, params=()):
    """What a second connection sees, i.e. only committed rows."""
    with contextlib.closing(sqlite3.connect(db._db_path)) as other:
        return other.execute(sql, params).fetchall()


@pytest.fixture
def catalog(db):
    """One original-backed photo with a detector run, a box and a classifier run."""
    folder = workspace_folders.add_folder(db, "/photos", name="photos")
    photo = photo_ingest.add_photo(
        db, folder_id=folder, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp=None, width=1, height=1,
    )
    db.conn.execute("UPDATE photos SET file_hash = ? WHERE id = ?", (HASH, photo))
    db.conn.execute(
        "INSERT INTO detector_runs (photo_id, detector_model, runtime_fingerprint, "
        "input_fingerprint) VALUES (?, 'md', ?, ?)",
        (photo, RUNTIME, INPUT),
    )
    detection = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_model, runtime_fingerprint, "
        "box_x, box_y, box_w, box_h, detector_confidence, category) "
        "VALUES (?, 'md', ?, 0.1, 0.2, 0.3, 0.4, 0.9, 'animal')",
        (photo, RUNTIME),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO detections (photo_id, detector_model, runtime_fingerprint, "
        "box_x, box_y, box_w, box_h, detector_confidence, category) "
        "VALUES (?, 'md', ?, 0.5, 0.5, 0.1, 0.1, 0.4, NULL)",
        (photo, RUNTIME),
    )
    db.conn.execute(
        "INSERT INTO classifier_runs (detection_id, classifier_model, labels_fingerprint, "
        "labels_fingerprint_full, runtime_fingerprint, input_fingerprint, input_recipe) "
        "VALUES (?, ?, 'short', ?, ?, ?, 'crops')",
        (detection, MODEL, FULL, RUNTIME, INPUT),
    )
    for species, confidence in (("Robin", 0.2), ("Wren", 0.7), ("avocet", 0.2)):
        db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
            "species, confidence, scientific_name) VALUES (?, ?, 'short', ?, ?, ?)",
            (detection, MODEL, species, confidence, species.lower()),
        )
    db.conn.execute(
        "INSERT INTO labels_fingerprints (fingerprint, full_fingerprint, display_name, "
        "sources_json, label_count) VALUES ('short', ?, 'Birds', '[]', 3)",
        (FULL,),
    )
    db.conn.commit()
    return {"db": db, "photo": photo, "detection": detection}


def test_export_reads(catalog):
    db, photo, detection = catalog["db"], catalog["photo"], catalog["detection"]
    repo = db.computation_cache

    assert repo.detection_runtime(detection)["runtime_fingerprint"] == RUNTIME
    assert repo.detection_runtime(9999) is None

    row = repo.promotable_detection(detection)
    assert (row["photo_id"], row["file_hash"], row["box_w"]) == (photo, HASH, 0.3)
    assert repo.promotable_detection(9999) is None

    species = [r["species"] for r in repo.run_predictions(detection, MODEL, "short")]
    assert species == ["Wren", "avocet", "Robin"]
    assert repo.run_predictions(detection, MODEL, "other") == []

    meta = repo.labels_metadata("short")
    assert (meta["display_name"], meta["label_count"]) == ("Birds", 3)
    assert repo.labels_metadata("missing") is None

    [run] = repo.portable_detector_runs()
    assert (run["photo_id"], run["file_hash"], run["input_fingerprint"]) == (photo, HASH, INPUT)
    boxes = repo.run_detection_boxes(photo, "md", RUNTIME)
    assert [b["detector_confidence"] for b in boxes] == [0.9, 0.4]

    [classifier_run] = repo.portable_classifier_runs()
    assert classifier_run["detector_runtime"] == RUNTIME
    assert classifier_run["input_recipe"] == "crops"
    assert [d["id"] for d in repo.classified_detections(classifier_run)] == [detection]

    detector_counts = repo.exportable_detector_counts()
    assert (detector_counts["portable"], detector_counts["legacy"]) == (1, 0)
    classifier_counts = repo.exportable_classifier_counts()
    assert (classifier_counts["portable"], classifier_counts["missing_hash"]) == (1, 0)


def test_match_summary_and_its_writes(catalog):
    db, detection = catalog["db"], catalog["detection"]
    repo = db.computation_cache
    assert repo.match_summary(detection, MODEL, "short") is None

    repo.insert_match_summary(detection, MODEL, "short", {"max_match_score": 0.3, "score_kind": "cosine"})
    row = repo.match_summary(detection, MODEL, "short")
    assert (row["max_match_score"], row["match_margin"], row["score_kind"]) == (0.3, None, "cosine")
    assert db.in_transaction
    db.rollback()
    assert repo.match_summary(detection, MODEL, "short") is None

    repo.insert_match_summary(detection, MODEL, "short", {"max_match_score": 0.3})
    repo.delete_match_summary(detection, MODEL, "short")
    assert repo.match_summary(detection, MODEL, "short") is None


def test_stamp_portable_run_identity_does_not_commit(catalog):
    db, detection = catalog["db"], catalog["detection"]
    db.conn.execute("UPDATE classifier_runs SET labels_fingerprint_full = NULL")
    db.conn.commit()
    db.computation_cache.stamp_portable_run_identity(
        detection, MODEL, "short", FULL, "e" * 64, "f" * 64,
    )
    assert db.in_transaction
    assert _committed(db, "SELECT labels_fingerprint_full FROM classifier_runs") == [(None,)]
    db.commit()
    assert _committed(
        db, "SELECT labels_fingerprint_full, runtime_fingerprint, input_fingerprint "
        "FROM classifier_runs",
    ) == [(FULL, "e" * 64, "f" * 64)]
    assert {r[0] for r in _committed(db, "SELECT labels_fingerprint_full FROM predictions")} == {FULL}


def test_materialize_reads(catalog):
    db, photo, detection = catalog["db"], catalog["photo"], catalog["detection"]
    repo = db.computation_cache

    assert repo.installed_detector_runtime(HASH, "md")["runtime_fingerprint"] == RUNTIME
    assert repo.installed_classifier_runtime(
        HASH, MODEL, "short", INPUT, "md",
    )["runtime_fingerprint"] == RUNTIME
    assert [r["id"] for r in repo.original_backed_photos(HASH)] == [photo]
    identity = repo.detector_run_identity(photo, "md")
    assert (identity["runtime_fingerprint"], identity["input_fingerprint"]) == (RUNTIME, INPUT)
    assert repo.detector_run_identity(photo, "other") is None
    identity = repo.classifier_run_identity(detection, MODEL, "short")
    assert (identity["runtime_fingerprint"], identity["input_fingerprint"]) == (RUNTIME, INPUT)
    assert repo.detection_exists(detection)
    assert not repo.detection_exists(9999)
    assert repo.full_image_detection(photo) is None

    assert not repo.labels_fingerprint_collision("short", FULL)
    assert repo.labels_fingerprint_collision("short", "9" * 64)
    assert not repo.labels_fingerprint_collision("unknown", "9" * 64)

    # Rejected, companion-backed or working-copy-backed photos never match.
    db.conn.execute("UPDATE photos SET flag = 'rejected'")
    assert repo.original_backed_photos(HASH) == []
    assert repo.installed_detector_runtime(HASH, "md") is None
    assert repo.installed_classifier_runtime(HASH, MODEL, "short", INPUT, "md") is None


def test_manual_review_exists_ignores_auto_match_reviews(catalog):
    db, detection = catalog["db"], catalog["detection"]
    repo = db.computation_cache
    prediction = db.conn.execute("SELECT id FROM predictions LIMIT 1").fetchone()["id"]
    ws = db.active_workspace_id
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status, individual) "
        "VALUES (?, ?, 'accepted', '__vireo_auto_match__')",
        (prediction, ws),
    )
    assert not repo.manual_review_exists(detection, MODEL, "short")
    db.conn.execute("UPDATE prediction_review SET individual = NULL")
    assert repo.manual_review_exists(detection, MODEL, "short")
    db.conn.execute("UPDATE prediction_review SET status = 'pending'")
    assert not repo.manual_review_exists(detection, MODEL, "short")


def test_materialize_writes_do_not_commit(catalog):
    db, detection = catalog["db"], catalog["detection"]
    repo = db.computation_cache

    repo.upsert_imported_labels_fingerprint("short", FULL, None, None)
    meta = repo.labels_metadata("short")
    assert (meta["display_name"], meta["label_count"]) == ("Birds", 3)
    repo.upsert_imported_labels_fingerprint("new", FULL, "New", 9)
    assert repo.labels_metadata("new")["label_count"] == 9

    assert repo.fill_missing_match_score(0.5, detection, MODEL, "short", "Wren") == 1
    assert repo.fill_missing_match_score(0.9, detection, MODEL, "short", "Wren") == 0

    repo.delete_run_predictions(detection, MODEL, "short")
    assert repo.run_predictions(detection, MODEL, "short") == []
    candidate = {"confidence": 0.8, "match_score": 0.25,
                 "taxonomy": {"scientific_name": "Turdus", "class": "Aves", "taxon_id": 7}}
    repo.insert_imported_prediction(detection, MODEL, "short", FULL, "Robin", candidate)
    repo.insert_imported_prediction(detection, MODEL, "short", FULL, "Robin", candidate)
    [row] = db.conn.execute(
        "SELECT species, confidence, category, scientific_name, taxonomy_class, "
        "source_taxon_id, match_score, labels_fingerprint_full FROM predictions"
    ).fetchall()
    assert tuple(row) == ("Robin", 0.8, "new", "Turdus", "Aves", 7, 0.25, FULL)

    repo.upsert_imported_classifier_run(
        detection, MODEL, "short", FULL, "e" * 64, "f" * 64, 1, "raw",
    )
    run = db.conn.execute(
        "SELECT runtime_fingerprint, input_fingerprint, prediction_count, input_recipe "
        "FROM classifier_runs"
    ).fetchone()
    assert tuple(run) == ("e" * 64, "f" * 64, 1, "raw")

    assert db.in_transaction
    db.rollback()
    assert len(repo.run_predictions(detection, MODEL, "short")) == 3
    assert repo.labels_metadata("new") is None


# -- structure ------------------------------------------------------------------


def test_computation_cache_is_a_fresh_repository_on_the_connection_per_access(db):
    first, second = db.computation_cache, db.computation_cache
    assert isinstance(first, ComputationCacheRepository)
    assert first is not second
    assert first.conn is db.conn


def test_computation_cache_accessor_builds_through_its_factory():
    accessor = Database.__dict__["computation_cache"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_computation_cache_repository" in attrs
    assert "conn" not in attrs


def test_computation_cache_module_runs_no_sql():
    """The module reaches the catalog only through ``Database``; no raw connection."""
    tree = ast.parse(Path(inspect.getsourcefile(computation_cache)).read_text(encoding="utf-8"))
    conn_uses = [
        node.lineno for node in ast.walk(tree)
        if (isinstance(node, ast.Attribute) and node.attr in {"conn", "execute", "executemany"})
        or (isinstance(node, ast.Name) and node.id == "conn")
    ]
    assert conn_uses == []
