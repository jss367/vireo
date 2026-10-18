"""Behavior pins for the pipeline feature-load domain of ``Database``.

``pipeline.load_photo_features``, the review-readiness counters, the
eye-keypoint helpers and the results-cache reconciliation read through the
``db.pipeline_features`` accessor. These tests pin each repository method on
a real catalog; the structural tests at the end keep the SQL in
``repositories/pipeline_features.py``.
"""

import ast
import inspect
import textwrap

import pytest
from db import Database
from repositories.pipeline_features import PipelineFeaturesRepository


@pytest.fixture
def catalog(db, tmp_path):
    ws = db.require_workspace_id()
    folder = db.add_folder(str(tmp_path), name="Pipeline")
    photos = [
        db.add_photo(folder_id=folder, filename=f"{i}.jpg", extension=".jpg",
                     file_size=1, file_mtime=1, timestamp=f"2026-01-01T00:00:0{i}")
        for i in range(3)
    ]
    dets = {
        photos[0]: db.write_detection_batch(photos[0], "megadetector-v6", [
            {"box": {"x": .1, "y": .1, "w": .3, "h": .3}, "confidence": .9, "category": "animal"},
            {"box": {"x": .5, "y": .5, "w": .3, "h": .3}, "confidence": .15, "category": "animal"},
        ]),
        photos[1]: db.write_detection_batch(photos[1], "megadetector-v6", [
            {"box": {"x": .2, "y": .2, "w": .3, "h": .3}, "confidence": .1, "category": "animal"},
        ]),
    }
    return ws, photos, dets


def test_workspace_membership(db, catalog, tmp_path):
    ws, photos, _dets = catalog
    other = db.create_workspace("Other")
    features = db.pipeline_features
    assert features.workspace_photo_ids(ws) == set(photos)
    assert features.workspace_photo_ids(other) == set()
    ids = list(photos) + [10**9] + list(range(10**6, 10**6 + 1000))
    assert features.present_workspace_photo_ids(ws, ids) == set(photos)
    assert features.present_workspace_photo_ids(other, ids) == set()


def test_feature_rows_whole_workspace_and_staged_scope(db, catalog):
    ws, photos, _dets = catalog
    features = db.pipeline_features
    rows = features.feature_rows(ws, scoped=False)
    assert [r["id"] for r in rows] == photos
    assert rows[0]["dino_subject_embedding"] is None
    features.stage_scope_photo_ids([photos[2], photos[0]])
    assert not db.in_transaction  # the staging commits the TEMP write it opened
    assert [r["id"] for r in features.feature_rows(ws, scoped=True)] == [photos[0], photos[2]]
    features.stage_scope_photo_ids([photos[1]])
    assert [r["id"] for r in features.feature_rows(ws, scoped=True)] == [photos[1]]


def test_staging_inside_an_outer_transaction_leaves_it_open(db, catalog):
    ws, photos, _dets = catalog
    db.begin()
    db.pipeline_features.stage_scope_photo_ids([photos[0]])
    assert db.in_transaction
    db.rollback()


def test_subject_detection_rows_apply_floor_weak_candidates_and_scope(db, catalog):
    ws, photos, dets = catalog
    features = db.pipeline_features
    rows = features.subject_detection_rows(ws, min_conf=.2, weak_confidence=None, scoped=False)
    assert [r["detection_id"] for r in rows] == [dets[photos[0]][0]]

    features.stage_weak_detection_photo_ids({photos[1]})
    rows = features.subject_detection_rows(ws, min_conf=.2, weak_confidence=.05, scoped=False)
    assert [r["detection_id"] for r in rows] == [dets[photos[0]][0], dets[photos[1]][0]]

    features.stage_scope_photo_ids([photos[1]])
    rows = features.subject_detection_rows(ws, min_conf=.2, weak_confidence=.05, scoped=True)
    assert [r["detection_id"] for r in rows] == [dets[photos[1]][0]]


def test_prediction_rows_latest_or_pinned_label_set_and_continuity(db, catalog):
    ws, photos, dets = catalog
    strong, weak = dets[photos[0]]
    db.predictions.add(strong, "Old", 0.95, "m", labels_fingerprint="old")
    db.conn.execute("UPDATE predictions SET created_at='2000-01-01'")
    db.predictions.add(strong, "Robin", 0.9, "m", labels_fingerprint="new")
    db.predictions.add(weak, "Wren", 0.5, "m", labels_fingerprint="new")
    features = db.pipeline_features

    def species(**kw):
        args = {"min_conf": .2, "weak_confidence": None, "scoped": False,
                "labels_fingerprint": None, **kw}
        return [r["species"] for r in features.prediction_rows(ws, **args)]

    assert species() == ["Robin"]
    assert species(labels_fingerprint="old") == ["Old"]
    assert species(min_conf=.1) == ["Robin", "Wren"]
    assert species(full_image_only=True) == []
    features.stage_continuity_detection_ids([weak])
    assert species(continuity_only=True) == ["Wren"]


def test_readiness_counts(db, catalog):
    ws, photos, dets = catalog
    features = db.pipeline_features
    assert features.count_detected_photos(ws, .2) == 1
    assert features.count_detected_photos(ws, .05) == 2

    emb = b"\0" * 16
    db.conn.execute("UPDATE photos SET dino_subject_embedding=?, dino_embedding_variant='v' WHERE id=?",
                    (emb, photos[0]))
    db.conn.execute("UPDATE photos SET dino_subject_embedding=? WHERE id=?", (emb, photos[2]))
    assert features.count_subject_embeddings(ws) == 2
    assert features.count_subject_embeddings(ws, detection_min_conf=.2) == 1
    assert features.count_subject_embeddings(ws, variant="v") == 1
    assert features.count_subject_embeddings(ws, variant="v", legacy_bytes=16) == 2
    assert features.count_subject_embeddings(ws, variant="v", legacy_bytes=8) == 1

    db.predictions.add(dets[photos[0]][0], "Robin", 0.9, "m")
    db.conn.execute("UPDATE photos SET mask_path='/m.png', eye_kp_fingerprint='fp' WHERE id=?",
                    (photos[0],))
    assert features.count_eye_keypoint_attempts(ws, .2, "fp") == 1
    assert features.count_eye_keypoint_attempts(ws, .2, "other") == 0
    assert features.count_eye_keypoint_attempts(ws, .95, "fp") == 0


def test_effective_primary_box_and_taxon_lookup(db, catalog):
    _ws, photos, dets = catalog
    features = db.pipeline_features
    assert features.effective_primary_box(photos[0], .1)["id"] == dets[photos[0]][0]
    assert features.effective_primary_box(photos[1], .2) is None
    assert features.effective_primary_box(photos[2], 0) is None
    assert features.taxon_by_name("No Such Taxon") is None
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank) VALUES (12345, 'Turdus migratorius', 'species')")
    assert features.taxon_by_name("Turdus migratorius")["inat_id"] == 12345


# -- structure ------------------------------------------------------------------


def test_pipeline_features_is_a_fresh_repository_on_the_connection_per_access(db):
    first, second = db.pipeline_features, db.pipeline_features
    assert isinstance(first, PipelineFeaturesRepository)
    assert first is not second
    assert first.conn is db.conn


def test_pipeline_features_accessor_only_builds_the_repository():
    accessor = Database.__dict__["pipeline_features"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_pipeline_features_repository" in attrs
    assert "conn" not in attrs


def test_pipeline_module_has_no_sql():
    """``pipeline.py`` reaches the catalog only through ``Database``."""
    import pipeline

    tree = ast.parse(inspect.getsource(pipeline))
    assert not [n for n in ast.walk(tree) if isinstance(n, ast.Attribute) and n.attr == "conn"]
