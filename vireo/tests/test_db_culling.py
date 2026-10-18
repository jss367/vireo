"""Behavior and structure pins for ``db.culling``, the culling analysis's catalog SQL.

``culling.analyze_for_culling`` reads each photo's top prediction, its ranking
signals and the source identity behind a pHash backfill, and stores the hashes
it computes, all through ``db.culling``. The behavior tests pin each read and
the write's transaction; the structural tests keep the accessor a fresh
repository on the connection and the culling module off the connection.
"""

import ast
import inspect
import textwrap
from pathlib import Path

from db import Database
from repositories.culling import CullingRepository

VIREO = Path(__file__).resolve().parents[1]


def _photo(db, folder_id, filename, **columns):
    pid = db.add_photo(
        folder_id=folder_id, filename=filename,
        extension="." + filename.rsplit(".", 1)[1].lower(),
        file_size=100, file_mtime=1.0,
    )
    if columns:
        db.scanning.update_photo(pid, [f"{name}=?" for name in columns], list(columns.values()))
    db.commit()
    return pid


def _prediction(db, photo_id, species, confidence, *, model="m1", box_x=0.1):
    det = db.conn.execute(
        """INSERT INTO detections
             (photo_id, detector_model, runtime_fingerprint,
              box_x, box_y, box_w, box_h, detector_confidence, category)
           VALUES (?, 'md', 'legacy', ?, 0.1, 0.2, 0.2, 0.9, 'animal')""",
        (photo_id, box_x),
    ).lastrowid
    db.conn.execute(
        """INSERT INTO predictions
             (detection_id, classifier_model, labels_fingerprint, species,
              confidence, scientific_name)
           VALUES (?, ?, 'fp', ?, ?, ?)""",
        (det, model, species, confidence, species.lower()),
    )
    db.commit()


def test_top_prediction_is_the_most_confident_across_detections(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    pid = _photo(db, fid, "a.jpg")
    bare = _photo(db, fid, "b.jpg")
    _prediction(db, pid, "Robin", 0.4)
    _prediction(db, pid, "Wren", 0.8, model="m2", box_x=0.5)
    row = db.culling.top_prediction(pid)
    assert (row["species"], row["confidence"], row["classifier_model"]) == ("Wren", 0.8, "m2")
    assert set(row.keys()) == {
        "species", "confidence", "classifier_model",
        "scientific_name", "labels_fingerprint", "source_taxon_id",
    }
    assert db.culling.top_prediction(bare) is None


def test_photo_signals_and_phash_source_read_one_photo(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    pid = _photo(
        db, fid, "a.NEF", quality_score=0.5, sharpness=120.0,
        timestamp="2024-01-01T00:00:00", phash="ff00", working_copy_path="working/1.jpg",
    )
    signals = db.culling.photo_signals(pid)
    assert dict(signals) == {
        "quality_score": 0.5, "sharpness": 120.0, "subject_sharpness": None,
        "extension": ".nef", "timestamp": "2024-01-01T00:00:00",
        "phash": "ff00", "filename": "a.NEF",
    }
    assert tuple(db.culling.phash_source(pid)) == (fid, "a.NEF", "working/1.jpg")
    assert db.culling.photo_signals(9999) is None
    assert db.culling.phash_source(9999) is None


def test_set_phash_writes_without_committing(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    pid = _photo(db, fid, "a.jpg")
    db.culling.set_phash(pid, "abcd")
    assert db.in_transaction
    assert db.culling.photo_signals(pid)["phash"] == "abcd"
    db.rollback()
    assert db.culling.photo_signals(pid)["phash"] is None


# -- structure ------------------------------------------------------------------


def test_culling_is_a_fresh_repository_on_the_connection_per_access(db):
    first, second = db.culling, db.culling
    assert isinstance(first, CullingRepository)
    assert first is not second
    assert first.conn is db.conn


def test_culling_accessor_only_builds_the_repository():
    accessor = Database.__dict__["culling"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_culling_repository" in attrs
    assert "conn" not in attrs


def test_culling_module_never_touches_the_connection():
    tree = ast.parse((VIREO / "culling.py").read_text(encoding="utf-8"))
    uses = [
        node.lineno for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "conn"
    ]
    assert uses == []
