"""Behavior pins for the subject-analysis domain of ``Database``.

``subjects.analyze_photo`` / ``sync_primary`` / ``select_primary`` / ``payload``
reach ``detection_subjects``, ``photo_subject_state`` and
``photo_subject_choices`` through the ``db.subjects`` accessor. These tests pin
each repository method on a real catalog; the structural tests at the end keep
the SQL in ``repositories/subjects.py``.
"""

import ast
import inspect
import textwrap

import pytest
from db import Database
from repositories.subjects import SubjectsRepository


@pytest.fixture
def photo(db, tmp_path):
    folder = db.add_folder(str(tmp_path), name="Subjects")
    photo_id = db.add_photo(folder_id=folder, filename="a.jpg", extension=".jpg",
                            file_size=1, file_mtime=1)
    ids = db.write_detection_batch(photo_id, "megadetector-v6", [
        {"box": {"x": .1, "y": .1, "w": .3, "h": .3}, "confidence": .9, "category": "animal"},
        {"box": {"x": .5, "y": .5, "w": .3, "h": .3}, "confidence": .8, "category": "animal"},
    ])
    return photo_id, ids


def _result(detection_id, quality=0.5):
    return {
        "detection_id": detection_id, "crop": {"x": 0, "y": 0, "w": 1, "h": 1},
        "quality_score": quality, "exposure_ev": 0.25,
        "features": {"subject_sharpness": 3.0, "subject_size": 0.09,
                     "subject_clip_high": 0.01, "subject_clip_low": 0.02,
                     "subject_y_median": 120.0},
    }


def _photo_columns(db, photo_id, *columns):
    return tuple(db.conn.execute(
        f"SELECT {', '.join(columns)} FROM photos WHERE id=?", (photo_id,)).fetchone())


def test_save_analyses_upserts_and_skips_missing_detections(db, photo):
    photo_id, ids = photo
    db.subjects.save_analyses("k1", [_result(ids[0]), _result(10**9)])
    assert db.subjects.cached_detection_ids(photo_id, "k1") == {ids[0]}
    assert db.subjects.cached_detection_ids(photo_id, "other") == set()

    db.subjects.save_analyses("k2", [_result(ids[0], quality=0.75), _result(ids[1])])
    assert db.subjects.cached_detection_ids(photo_id, "k1") == set()
    assert db.subjects.cached_detection_ids(photo_id, "k2") == {ids[0], ids[1]}
    row = db.subjects.analysis(ids[0])
    assert row["quality_score"] == 0.75 and row["source_key"] == "k2"
    assert db.subjects.analysis(10**9) is None
    analyses = db.subjects.analyses_for_photo(photo_id)
    assert set(analyses) == {ids[0], ids[1]}
    assert analyses[ids[1]]["exposure_ev"] == 0.25


def test_primary_state_set_replace_and_clear(db, photo):
    photo_id, ids = photo
    assert db.subjects.primary_state(photo_id) is None
    db.subjects.set_primary_state(photo_id, ids[0])
    db.subjects.set_primary_state(photo_id, ids[1])
    assert db.subjects.primary_state(photo_id)["detection_id"] == ids[1]
    db.subjects.clear_primary_state(photo_id)
    assert db.subjects.primary_state(photo_id) is None


def test_choice_set_replace_and_clear(db, photo):
    photo_id, ids = photo
    assert db.subjects.choice(photo_id) is None
    db.subjects.set_choice(photo_id, ids[0])
    db.subjects.set_choice(photo_id, 10**9)  # no FK: a missing choice is remembered
    assert db.subjects.choice(photo_id)["detection_id"] == 10**9
    db.subjects.clear_choice(photo_id)
    assert db.subjects.choice(photo_id) is None


def test_primary_projection_and_clear(db, photo):
    photo_id, _ids = photo
    features = _result(0)["features"]
    db.subjects.set_primary_quality(photo_id, 0.6, 3.0)
    db.subjects.set_primary_box_features(photo_id, features)
    assert _photo_columns(db, photo_id, "quality_score", "subject_sharpness", "subject_size",
                          "subject_clip_high", "subject_clip_low", "subject_y_median") == (
        0.6, 3.0, 0.09, 0.01, 0.02, 120.0)
    db.subjects.clear_primary_features(photo_id)
    assert _photo_columns(db, photo_id, "quality_score", "subject_sharpness", "subject_size",
                          "subject_y_median", "mask_path", "active_mask_variant") == (None,) * 6


def test_has_stale_primary_state_sees_state_row_or_legacy_columns(db, photo):
    photo_id, ids = photo
    assert db.subjects.has_stale_primary_state(photo_id) is False
    db.subjects.set_primary_state(photo_id, ids[0])
    assert db.subjects.has_stale_primary_state(photo_id) is True
    db.subjects.clear_primary_state(photo_id)
    db.subjects.set_primary_quality(photo_id, 0.6, 3.0)
    assert db.subjects.has_stale_primary_state(photo_id) is True
    db.subjects.clear_primary_features(photo_id)
    assert db.subjects.has_stale_primary_state(photo_id) is False


def test_active_mask_reads_the_active_variant_row(db, photo):
    photo_id, _ids = photo
    db.masks_features.upsert_mask(photo_id, "sam2-small", "/m.png", "megadetector-v6",
                                  (.1, .1, .3, .3))
    assert db.subjects.active_mask(photo_id) is None
    db.conn.execute("UPDATE photos SET active_mask_variant='sam2-small' WHERE id=?", (photo_id,))
    mask = db.subjects.active_mask(photo_id)
    assert mask["variant"] == "sam2-small" and mask["prompt_w"] == .3


def test_visible_predictions_filters_floor_rejection_and_stale_labels(db, photo):
    photo_id, ids = photo
    ws = db.require_workspace_id()
    db.predictions.add(ids[0], "Old Label", 0.95, "m", labels_fingerprint="old")
    db.conn.execute("UPDATE predictions SET created_at='2000-01-01'")
    db.predictions.add(ids[0], "Robin", 0.9, "m", labels_fingerprint="new")
    db.predictions.add(ids[0], "Wren", 0.2, "m", labels_fingerprint="new")
    db.predictions.add(ids[1], "Jay", 0.7, "m", labels_fingerprint="new")
    db.predictions.add(ids[1], "Crow", 0.8, "m", labels_fingerprint="new")
    crow = db.conn.execute("SELECT id FROM predictions WHERE species='Crow'").fetchone()["id"]
    db.update_prediction_status(crow, "rejected")

    rows = db.subjects.visible_predictions(ws, photo_id, 0.5)
    assert [(r["species"], r["status"]) for r in rows] == [("Robin", "pending"), ("Jay", "pending")]
    assert [r["species"] for r in db.subjects.visible_predictions(ws, photo_id, 0.0)] == [
        "Robin", "Jay", "Wren"]


# -- structure ------------------------------------------------------------------


def test_subjects_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.subjects`` builds a new repository each time, never a cached one."""
    first, second = db.subjects, db.subjects
    assert isinstance(first, SubjectsRepository)
    assert first is not second
    assert first.conn is db.conn


def test_subjects_accessor_only_builds_the_repository():
    accessor = Database.__dict__["subjects"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_subjects_repository" in attrs
    assert "conn" not in attrs


def test_subjects_module_has_no_sql():
    """``subjects.py`` reaches its tables only through ``db.subjects``."""
    import subjects

    tree = ast.parse(inspect.getsource(subjects))
    assert not [n for n in ast.walk(tree) if isinstance(n, ast.Attribute) and n.attr == "conn"]
