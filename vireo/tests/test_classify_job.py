import dataclasses
import json
import os

import pytest
from PIL import Image

from classify_job import ClassifyParams, run_classify_job


def test_classify_params_is_dataclass():
    """ClassifyParams is a dataclass with all required fields."""
    assert dataclasses.is_dataclass(ClassifyParams)
    fields = {f.name for f in dataclasses.fields(ClassifyParams)}
    assert fields == {
        "collection_id",
        "labels_file",
        "labels_files",
        "model_id",
        "model_name",
        "grouping_window",
        "similarity_threshold",
        "reclassify",
    }


def test_run_classify_job_is_callable():
    """run_classify_job exists and is callable."""
    assert callable(run_classify_job)


# ── Task 2: _load_taxonomy and _load_labels tests ──────────────────────────


class FakeRunner:
    """Minimal runner that records push_event calls."""

    def __init__(self):
        self.events = []

    def push_event(self, job_id, event_type, data):
        self.events.append((job_id, event_type, data))


def _make_job(job_id="classify-test"):
    return {
        "id": job_id,
        "progress": {"current": 0, "total": 0, "current_file": "", "rate": 0},
        "errors": [],
    }


def test_taxonomy_loads_when_file_exists(tmp_path):
    """Phase 1: taxonomy.json is loaded when present."""
    tax_data = {
        "last_updated": "2024-01-01",
        "taxa_by_common": {
            "northern cardinal": {
                "taxon_id": 9083,
                "scientific_name": "Cardinalis cardinalis",
                "common_name": "Northern Cardinal",
                "rank": "species",
                "lineage_names": [
                    "Animalia", "Chordata", "Aves",
                    "Passeriformes", "Cardinalidae", "Cardinalis",
                    "Cardinalis cardinalis",
                ],
                "lineage_ranks": [
                    "kingdom", "phylum", "class",
                    "order", "family", "genus", "species",
                ],
            }
        },
        "taxa_by_scientific": {},
    }
    tax_path = tmp_path / "taxonomy.json"
    tax_path.write_text(json.dumps(tax_data))

    from classify_job import _load_taxonomy

    tax = _load_taxonomy(str(tax_path))
    assert tax is not None
    assert tax.taxa_count >= 1


def test_taxonomy_returns_none_when_missing(tmp_path):
    """Phase 1: returns None when taxonomy.json doesn't exist."""
    from classify_job import _load_taxonomy

    tax = _load_taxonomy(str(tmp_path / "nonexistent.json"))
    assert tax is None


def test_load_labels_from_file(tmp_path):
    """Phase 2: labels loaded from a single file path."""
    labels_file = tmp_path / "labels.txt"
    labels_file.write_text("Northern Cardinal\nBlue Jay\nAmerican Robin\n")

    from classify_job import _load_labels

    labels, use_tol = _load_labels(
        model_type="bioclip",
        model_str="hf-hub:imageomics/bioclip",
        labels_file=str(labels_file),
        labels_files=None,
    )
    assert labels == ["Northern Cardinal", "Blue Jay", "American Robin"]
    assert use_tol is False


def test_load_labels_tol_fallback():
    """Phase 2: Tree of Life mode when no labels and model supports it."""
    from unittest.mock import patch

    from classify_job import _load_labels

    # Mock get_active_labels to return empty so we fall through to ToL
    with patch("classify_job.get_active_labels", return_value=[]):
        labels, use_tol = _load_labels(
            model_type="bioclip",
            model_str="hf-hub:imageomics/bioclip",
            labels_file=None,
            labels_files=None,
        )
    assert labels is None
    assert use_tol is True


def test_load_labels_timm_skips():
    """Phase 2: timm models skip label loading entirely."""
    from classify_job import _load_labels

    labels, use_tol = _load_labels(
        model_type="timm",
        model_str="hf-hub:timm/some_model",
        labels_file=None,
        labels_files=None,
    )
    assert labels is None
    assert use_tol is False


def test_load_labels_raises_when_no_labels_unsupported_model():
    """Phase 2: raises RuntimeError when no labels and model doesn't support ToL."""
    from unittest.mock import patch

    from classify_job import _load_labels

    # Mock get_active_labels to return empty
    with patch("classify_job.get_active_labels", return_value=[]):
        with pytest.raises(RuntimeError, match="No labels available"):
            _load_labels(
                model_type="bioclip",
                model_str="hf-hub:some/unsupported-model",
                labels_file=None,
                labels_files=None,
            )


# ── Task 3: _detect_subjects tests ──────────────────────────────────────────


def test_detect_subjects_returns_detection_map(tmp_path):
    """Phase 5: returns detection map for photos with detectable subjects."""
    from unittest.mock import patch, MagicMock
    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    # Create a real test image
    img = Image.new("RGB", (200, 200), color="green")
    img_path = str(tmp_path / "bird.jpg")
    img.save(img_path)

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "detection_box": None, "detection_conf": None},
    ]
    folders = {10: str(tmp_path)}

    fake_detection = {
        "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
        "confidence": 0.95,
        "category": "animal",
    }

    with patch("classify_job.detect_animals", return_value=[fake_detection]), \
         patch("classify_job.get_primary_detection", return_value=fake_detection), \
         patch("classify_job.compute_sharpness", return_value=50.0):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=False,
            db=MagicMock(),
        )

    assert detected == 1
    assert 1 in detection_map
    assert detection_map[1]["confidence"] == 0.95


def test_detect_subjects_skips_existing_detections(tmp_path):
    """Phase 5: skips photos that already have detection_box (unless reclassify)."""
    import json as _json
    from unittest.mock import patch, MagicMock
    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "detection_box": _json.dumps({"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}),
         "detection_conf": 0.9},
    ]
    folders = {10: str(tmp_path)}

    # detect_animals should NOT be called since photo already has detection
    with patch("classify_job.detect_animals") as mock_detect:
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=False,
            db=MagicMock(),
        )

    mock_detect.assert_not_called()
    assert detected == 1
    assert 1 in detection_map


def test_detect_subjects_graceful_on_import_error():
    """Phase 5: returns empty map if PytorchWildlife not installed."""
    from unittest.mock import MagicMock
    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    # _detect_subjects should handle ImportError gracefully
    detection_map, detected = _detect_subjects(
        photos=[],
        folders={},
        runner=runner,
        job=job,
        reclassify=False,
        db=MagicMock(),
    )
    assert detection_map == {}
    assert detected == 0


# ── Task 4: _classify_photos tests ───────────────────────────────────────────


def test_classify_photos_new_photo(tmp_path):
    """Phase 6: classifies a new photo and returns raw results."""
    from unittest.mock import patch, MagicMock
    import numpy as np
    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    # Create a test image
    img = Image.new("RGB", (200, 200), color="red")
    img_path = tmp_path / "bird.jpg"
    img.save(str(img_path))

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}
    detection_map = {}
    existing_preds = set()

    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "Northern Cardinal", "score": 0.95, "taxonomy": None}]

    mock_clf = MagicMock()
    mock_clf.classify_with_embedding.return_value = (fake_preds, fake_embedding)

    mock_db = MagicMock()
    mock_db.get_photo_embedding.return_value = None

    with patch("classify_job.load_image", return_value=Image.new("RGB", (200, 200))):
        raw_results, failed, skipped = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=existing_preds,
            clf=mock_clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=mock_db,
        )

    assert len(raw_results) == 1
    assert raw_results[0]["prediction"] == "Northern Cardinal"
    assert raw_results[0]["confidence"] == 0.95
    assert failed == 0
    assert skipped == 0
    mock_db.store_photo_embedding.assert_called_once()


def test_classify_photos_skips_existing(tmp_path):
    """Phase 6: skips photos with existing predictions."""
    from unittest.mock import MagicMock
    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}
    existing_preds = {1}  # photo 1 already classified

    mock_clf = MagicMock()
    mock_db = MagicMock()
    mock_db.get_prediction_for_photo.return_value = {
        "species": "Northern Cardinal",
        "confidence": 0.95,
    }
    mock_db.get_photo_embedding.return_value = None

    raw_results, failed, skipped = _classify_photos(
        photos=photos,
        folders=folders,
        detection_map={},
        existing_preds=existing_preds,
        clf=mock_clf,
        model_type="bioclip",
        model_name="BioCLIP",
        runner=runner,
        job=job,
        db=mock_db,
    )

    assert skipped == 1
    assert len(raw_results) == 1
    assert raw_results[0]["_existing"] is True
    mock_clf.classify_with_embedding.assert_not_called()


# ── Task 5: _store_grouped_predictions tests ─────────────────────────────────


def test_store_grouped_predictions_single_photo():
    """Phase 7: single-photo group stores prediction directly."""
    from unittest.mock import MagicMock
    from classify_job import _store_grouped_predictions

    mock_db = MagicMock()

    raw_results = [
        {
            "photo": {"id": 1, "filename": "bird.jpg"},
            "folder_path": "/photos",
            "prediction": "Northern Cardinal",
            "confidence": 0.95,
            "timestamp": None,
            "filename": "bird.jpg",
            "embedding": None,
            "taxonomy": {"order": "Passeriformes", "family": "Cardinalidae"},
        },
    ]

    result = _store_grouped_predictions(
        raw_results=raw_results,
        job_id="classify-test",
        model_name="BioCLIP",
        grouping_window=10,
        similarity_threshold=0.85,
        tax=None,
        db=mock_db,
    )

    assert result["predictions_stored"] == 1
    assert result["burst_groups"] == 0
    mock_db.add_prediction.assert_called_once()
    call_kwargs = mock_db.add_prediction.call_args[1]
    assert call_kwargs["species"] == "Northern Cardinal"
    assert call_kwargs["photo_id"] == 1


def test_store_grouped_predictions_burst_group():
    """Phase 7: multi-photo group computes consensus and stores for all photos."""
    from unittest.mock import MagicMock, patch
    from datetime import datetime
    from classify_job import _store_grouped_predictions

    mock_db = MagicMock()

    raw_results = [
        {
            "photo": {"id": 1, "filename": "bird1.jpg"},
            "folder_path": "/photos",
            "prediction": "Northern Cardinal",
            "confidence": 0.95,
            "timestamp": datetime(2024, 1, 15, 10, 0, 0),
            "filename": "bird1.jpg",
            "embedding": None,
            "taxonomy": None,
        },
        {
            "photo": {"id": 2, "filename": "bird2.jpg"},
            "folder_path": "/photos",
            "prediction": "Northern Cardinal",
            "confidence": 0.90,
            "timestamp": datetime(2024, 1, 15, 10, 0, 3),
            "filename": "bird2.jpg",
            "embedding": None,
            "taxonomy": None,
        },
    ]

    result = _store_grouped_predictions(
        raw_results=raw_results,
        job_id="classify-test",
        model_name="BioCLIP",
        grouping_window=10,
        similarity_threshold=0.85,
        tax=None,
        db=mock_db,
    )

    assert result["predictions_stored"] == 2
    assert result["burst_groups"] >= 1
    assert mock_db.add_prediction.call_count == 2


# ── Task 6: run_classify_job full pipeline test ───────────────────────────────


def test_run_classify_job_full_pipeline(tmp_path):
    """run_classify_job orchestrates all phases end-to-end."""
    from unittest.mock import patch, MagicMock
    import numpy as np
    from classify_job import ClassifyParams, run_classify_job

    runner = FakeRunner()
    job = _make_job()

    # Create test image
    img = Image.new("RGB", (200, 200), color="blue")
    img_path = tmp_path / "bird.jpg"
    img.save(str(img_path))

    # Set up mock DB
    mock_db_instance = MagicMock()
    mock_db_instance.get_collection_photos.return_value = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00",
         "detection_box": None, "detection_conf": None},
    ]
    mock_db_instance.get_folder_tree.return_value = [
        {"id": 10, "path": str(tmp_path), "name": "test"},
    ]
    mock_db_instance.get_existing_prediction_photo_ids.return_value = set()
    mock_db_instance.get_photo_embedding.return_value = None

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }

    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "Northern Cardinal", "score": 0.95, "taxonomy": None}]

    mock_clf = MagicMock()
    mock_clf.classify_with_embedding.return_value = (fake_preds, fake_embedding)

    params = ClassifyParams(
        collection_id="col-1",
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name=None,
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=False,
    )

    with patch("classify_job.Database", return_value=mock_db_instance), \
         patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch("classify_job._load_labels", return_value=(["Northern Cardinal"], False)), \
         patch("classify_job.Classifier", return_value=mock_clf), \
         patch("classify_job._detect_subjects", return_value=({}, 0)):
        result = run_classify_job(job, runner, str(tmp_path / "test.db"), 1, params)

    assert result["total"] == 1
    assert result["predictions_stored"] == 1
    assert result["failed"] == 0
    mock_db_instance.add_prediction.assert_called_once()


# ── Task 7: Integration test — route delegates to run_classify_job ─────────


def test_api_route_calls_run_classify_job(app_and_db):
    """The /api/jobs/classify route delegates to run_classify_job."""
    from classify_job import ClassifyParams

    app, db = app_and_db
    client = app.test_client()

    # Create a collection so the request is valid
    import json as _json
    col_id = db.add_collection("Test", _json.dumps([{"type": "all"}]))

    captured = {}

    def fake_run(job, runner, db_path, workspace_id, params):
        captured["params"] = params
        captured["workspace_id"] = workspace_id
        return {
            "total": 0,
            "predictions_stored": 0,
            "burst_groups": 0,
            "already_classified": 0,
            "already_labeled": 0,
            "detected": 0,
            "failed": 0,
        }

    import classify_job

    original = classify_job.run_classify_job
    classify_job.run_classify_job = fake_run
    try:
        resp = client.post(
            "/api/jobs/classify",
            json={"collection_id": col_id, "model_name": "TestModel"},
        )
    finally:
        classify_job.run_classify_job = original

    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("classify-")
