import dataclasses
import json

import pytest
from classify_job import ClassifyParams, run_classify_job
from PIL import Image


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

    def set_steps(self, job_id, steps):
        pass

    def update_step(self, job_id, step_id, **kwargs):
        pass


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
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    # Create a real test image
    img = Image.new("RGB", (200, 200), color="green")
    img_path = str(tmp_path / "bird.jpg")
    img.save(img_path)

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10},
    ]
    folders = {10: str(tmp_path)}

    fake_detection = {
        "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
        "confidence": 0.95,
        "category": "animal",
    }

    mock_db = MagicMock()
    mock_db.get_existing_detection_photo_ids.return_value = set()
    mock_db.save_detections.return_value = [101]

    with patch("classify_job.detect_animals", return_value=[fake_detection]), \
         patch("classify_job.get_primary_detection", return_value=fake_detection), \
         patch("classify_job.compute_sharpness", return_value=50.0):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    assert detected == 1
    assert 1 in detection_map
    # detection_map now returns a list of detection dicts per photo
    assert isinstance(detection_map[1], list)
    assert len(detection_map[1]) == 1
    assert detection_map[1][0]["confidence"] == 0.95
    assert detection_map[1][0]["id"] == 101


def test_detect_subjects_skips_existing_detections(tmp_path):
    """Phase 5: skips photos that already have detections in the DB (unless reclassify)."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10},
    ]
    folders = {10: str(tmp_path)}

    mock_db = MagicMock()
    # Photo 1 already has detections in the database
    mock_db.get_detector_run_photo_ids.return_value = {1}
    mock_db.get_detections.return_value = [
        {"id": 101, "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
         "detector_confidence": 0.9, "category": "animal"},
    ]

    # detect_animals should NOT be called since photo already has detections
    with patch("classify_job.detect_animals") as mock_detect:
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    mock_detect.assert_not_called()
    assert detected == 1
    assert 1 in detection_map
    assert isinstance(detection_map[1], list)
    assert detection_map[1][0]["id"] == 101


def test_detect_subjects_skips_weight_download_when_all_cached(tmp_path):
    """When every photo is already detected and reclassify=False, no fresh
    MegaDetector pass runs, so the auto-download should be skipped entirely.
    Prevents offline reruns from aborting on missing weights."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    photos = [{"id": 1, "filename": "bird.jpg", "folder_id": 10}]
    folders = {10: str(tmp_path)}

    mock_db = MagicMock()
    mock_db.get_detector_run_photo_ids.return_value = {1}
    mock_db.get_detections.return_value = [
        {"id": 101, "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
         "detector_confidence": 0.9, "category": "animal"},
    ]

    with patch("detector.ensure_megadetector_weights") as mock_ensure:
        _detect_subjects(
            photos=photos, folders=folders, runner=runner, job=job,
            reclassify=False, db=mock_db,
        )

    mock_ensure.assert_not_called()


def test_detect_subjects_skips_weight_download_for_empty_reclassify(tmp_path):
    """An empty photo list with reclassify=True should not trigger the
    MegaDetector download. No photos = no detection pass, so the
    ~300 MB fetch would be pure waste and would also make offline no-op
    reclassifies fatally dependent on the network.

    Regression for Codex P2 review on #535."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    mock_db = MagicMock()
    mock_db.get_detector_run_photo_ids.return_value = set()

    with patch("detector.ensure_megadetector_weights") as mock_ensure:
        _detect_subjects(
            photos=[], folders={}, runner=runner, job=job,
            reclassify=True, db=mock_db,
        )

    mock_ensure.assert_not_called()


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


# ── Task 4: Multi-detection pipeline tests ───────────────────────────────────


def test_detect_batch_marks_processed_before_quality_scoring(tmp_path):
    """_detect_batch must add photo_id to processed_ids as soon as detection
    rows are committed to the DB, before quality-scoring calls.

    If compute_sharpness or update_photo_quality raises after save_detections,
    the outer except catches the exception and processed_ids.add at the end of
    the per-photo loop body is never reached.  The photo would be missing from
    processed_ids, causing the reclassify purge in pipeline_job to skip
    deleting its stale pre-run detection rows — future non-reclassify runs
    would then reuse those stale rows indefinitely.

    Regression for Codex P2 review on #513, classify_job.py line 315.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_batch

    runner = FakeRunner()
    job = _make_job()

    photos = [{"id": 7, "filename": "bird.jpg", "folder_id": 10}]
    folders = {10: str(tmp_path)}

    img = Image.new("RGB", (100, 100), color="red")
    img.save(str(tmp_path / "bird.jpg"))

    fake_detections = [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
         "confidence": 0.9, "category": "animal"},
    ]

    mock_db = MagicMock()
    mock_db.save_detections.return_value = [42]

    # quality scoring raises — simulates compute_sharpness or
    # update_photo_quality failing after the detection row is already saved.
    def raising_sharpness(*args, **kwargs):
        raise RuntimeError("simulated sharpness failure")

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[0]), \
         patch("classify_job.compute_sharpness", side_effect=raising_sharpness):
        detection_map, detected, processed_ids = _detect_batch(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=True,
            db=mock_db,
            already_detected_ids=set(),
        )

    # The detection was saved to the DB before quality scoring raised.
    mock_db.save_detections.assert_called_once()
    # photo 7 must be in processed_ids even though quality scoring raised, so
    # the reclassify purge correctly removes its stale pre-run detection rows.
    assert 7 in processed_ids, (
        "photo_id must be in processed_ids after save_detections even when "
        "quality-scoring raises — regression for Codex P2 on #513 line 315"
    )
    # detection_map should still contain the result from this run
    assert 7 in detection_map


def test_detect_batch_does_not_pass_threshold_to_detector(tmp_path, monkeypatch):
    """detect_animals is called with just the image path — the workspace
    threshold is NOT applied at write time.

    Regression for the detection-storage redesign: the detector writes
    everything above RAW_CONF_FLOOR so results can be globally cached
    across workspaces. Any per-workspace threshold is applied as a
    read-time filter (get_detections / stats queries), not here.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _detect_batch
    from db import Database

    # Real DB with a workspace that overrides detector_confidence
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace(
        "Birds", config_overrides={"detector_confidence": 0.05}
    )
    db.set_active_workspace(ws_id)

    # Isolate global config so we don't read ~/.vireo/config.json
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    img = Image.new("RGB", (100, 100), color="red")
    img_path = str(tmp_path / "bird.jpg")
    img.save(img_path)

    photos = [{"id": 1, "filename": "bird.jpg", "folder_id": 10}]
    folders = {10: str(tmp_path)}

    captured = {}

    def fake_detect(image_path, *args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return []

    runner = FakeRunner()
    job = _make_job()

    with patch("classify_job.detect_animals", side_effect=fake_detect), \
         patch("classify_job.get_primary_detection", return_value=None), \
         patch("classify_job.compute_sharpness", return_value=50.0):
        _detect_batch(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=True,
            db=db,
            already_detected_ids=set(),
        )

    assert "confidence_threshold" not in captured["kwargs"], (
        "detect_animals must not receive confidence_threshold; "
        f"got kwargs={captured['kwargs']!r}"
    )
    assert captured["args"] == (), (
        "detect_animals must only be called with the image path; "
        f"got extra positional args={captured['args']!r}"
    )


def test_detect_batch_stores_all_detections(tmp_path):
    """_detect_batch should store all detections, not just the primary."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(fid, "multi.jpg", ".jpg", 1000, 1234567890.0)
    # Verify that save_detections stores all detections
    detections_list = [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.3}, "confidence": 0.95, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.1, "w": 0.2, "h": 0.3}, "confidence": 0.80, "category": "animal"},
        {"box": {"x": 0.3, "y": 0.5, "w": 0.15, "h": 0.2}, "confidence": 0.60, "category": "animal"},
    ]
    det_ids = db.save_detections(pid, detections_list, detector_model="MDV6")
    assert len(det_ids) == 3
    stored = db.get_detections(pid)
    assert len(stored) == 3


def test_detect_batch_returns_all_detections(tmp_path):
    """_detect_batch should return a list of all detections per photo, not just primary."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_batch

    runner = FakeRunner()
    job = _make_job()

    # Create a real test image
    img = Image.new("RGB", (200, 200), color="green")
    img_path = str(tmp_path / "bird.jpg")
    img.save(img_path)

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10},
    ]
    folders = {10: str(tmp_path)}

    fake_detections = [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3}, "confidence": 0.95, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2}, "confidence": 0.80, "category": "animal"},
    ]

    mock_db = MagicMock()
    mock_db.save_detections.return_value = [101, 102]

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[0]), \
         patch("classify_job.compute_sharpness", return_value=50.0):
        detection_map, detected, _processed = _detect_batch(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
            already_detected_ids=set(),
        )

    assert detected == 1
    assert 1 in detection_map
    assert isinstance(detection_map[1], list)
    assert len(detection_map[1]) == 2
    assert detection_map[1][0]["id"] == 101
    assert detection_map[1][0]["box_x"] == 0.1
    assert detection_map[1][1]["id"] == 102
    assert detection_map[1][1]["box_x"] == 0.5
    mock_db.save_detections.assert_called_once()


def test_detect_batch_skips_empty_photo_on_rerun(tmp_path, monkeypatch):
    """A photo with no animals is recorded in detector_runs; rerun skips detection."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "empty.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )

    call_count = {"n": 0}
    def fake_detect(image_path):
        call_count["n"] += 1
        return []  # no animals

    monkeypatch.setattr("classify_job.detect_animals", fake_detect)
    monkeypatch.setattr("classify_job.get_primary_detection", lambda dets: None)

    import classify_job
    photos = [{"id": photo_id, "folder_id": folder_id, "filename": "empty.jpg"}]
    folders = {folder_id: "/tmp/p"}

    # First call: runs detection
    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 1

    # Second call: should skip because detector_runs has the row
    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 1, "detect_animals should not be re-called for empty photos"


def test_detect_batch_does_not_cache_failed_detector_runs(tmp_path, monkeypatch):
    """When detect_animals returns None (image decode error, ONNX crash,
    etc.), _detect_batch must NOT write a detector_runs row — otherwise
    future non-reclassify passes would skip the photo permanently,
    leaving it without detections unless the user forces --reclassify.
    A legitimate empty scene still gets cached (separate test).
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "broken.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )

    call_count = {"n": 0}
    def failing_detect(image_path):
        call_count["n"] += 1
        return None  # simulate detector failure

    monkeypatch.setattr("classify_job.detect_animals", failing_detect)
    monkeypatch.setattr("classify_job.get_primary_detection", lambda dets: None)

    import classify_job
    photos = [{"id": photo_id, "folder_id": folder_id, "filename": "broken.jpg"}]
    folders = {folder_id: "/tmp/p"}

    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 1, "detector was called"
    # No detector_run row should have been written for the failed run
    assert db.get_detector_run_photo_ids("megadetector-v6") == set()

    # A second pass must call the detector again (no cached "already done")
    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 2, "failed photos must be retried on next pass"


def test_classify_photos_surfaces_cached_full_image_predictions(tmp_path):
    """When a photo has no real detections and the full-image synthetic
    detection is gated by classifier_runs, the cached top prediction
    must still be surfaced into raw_results as `_existing: True` —
    otherwise non-reclassify reruns silently drop those photos from
    downstream grouping.
    """
    from unittest.mock import MagicMock

    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}

    mock_clf = MagicMock()
    mock_db = MagicMock()
    # No real detections → full-image path. Existing full-image
    # detection is cached.
    mock_db.get_detections.return_value = [{"id": 999}]
    mock_db.get_classifier_run_keys.return_value = {("BioCLIP", "fp-x")}
    mock_db.get_predictions_for_detection.return_value = [
        {"species": "Robin", "confidence": 0.9, "detection_id": 999},
    ]
    mock_db.get_photo_embedding.return_value = None

    raw_results, failed, skipped = _classify_photos(
        photos=photos,
        folders=folders,
        detection_map={},  # no real detections → full-image branch
        existing_preds=set(),
        clf=mock_clf,
        model_type="bioclip",
        model_name="BioCLIP",
        runner=runner,
        job=job,
        db=mock_db,
        labels_fingerprint="fp-x",
    )

    assert skipped == 1, "cached full-image detection should count as skipped"
    assert len(raw_results) == 1, "cached full-image prediction must surface"
    assert raw_results[0]["_existing"] is True
    assert raw_results[0]["prediction"] == "Robin"
    assert raw_results[0]["detection_id"] == 999
    mock_clf.classify_with_embedding.assert_not_called()
    mock_clf.classify_batch_with_embedding.assert_not_called()


def test_classify_photos_reuses_full_image_detection_on_rerun(tmp_path, monkeypatch):
    """When a photo has no real detections, classify_photos falls back to a
    synthetic ('full-image') detection. Because save_detections is
    clear-and-reinsert per (photo, detector_model), calling it on every
    pass would generate a new id each time and cascade-delete prior
    predictions/classifier_runs tied to the old id. The non-reclassify
    path must reuse the existing full-image detection instead.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    # Pre-seed a full-image detection that a prior classify pass would have
    # left behind.
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0, "category": "animal"}],
        detector_model="full-image",
    )
    original_det_id = det_ids[0]

    # Sanity check the helper used by the reuse path — must use min_conf=0
    # because the synthetic full-image detection has confidence=0.
    existing = db.get_detections(
        photo_id, detector_model="full-image", min_conf=0,
    )
    assert len(existing) == 1
    assert existing[0]["id"] == original_det_id

    # Simulate what classify_photos does on a subsequent pass: the reuse
    # branch must NOT call save_detections again, or it would cascade-delete
    # any cached predictions attached to the original detection.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) "
        "VALUES (?, 'bioclip', 'fp1', 'Robin', 0.8)",
        (original_det_id,),
    )
    db.conn.commit()

    # Reuse path via the helper
    reused = db.get_detections(
        photo_id, detector_model="full-image", min_conf=0,
    )
    assert reused[0]["id"] == original_det_id
    # Prediction still there
    n = db.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id = ?",
        (original_det_id,),
    ).fetchone()["n"]
    assert n == 1


def test_store_grouped_predictions_writes_active_fingerprint(tmp_path):
    """Predictions produced under a given label set must be written with
    that set's fingerprint, not the default 'legacy'. Otherwise the
    fingerprint-aware skip gate (get_existing_prediction_photo_ids with
    labels_fingerprint=...) would miss them and force reclassification
    on every pass.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )

    import classify_job
    raw_results = [{
        "photo": {
            "id": photo_id, "filename": "a.jpg",
            "folder_id": folder_id, "timestamp": None, "burst_id": None,
        },
        "folder_path": "/tmp/p",
        "detection_id": det_ids[0],
        "prediction": "Robin",
        "confidence": 0.88,
        "alternatives": [],
        "taxonomy": {},
        "timestamp": None,
    }]
    classify_job._store_grouped_predictions(
        raw_results, job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=0,
        similarity_threshold=0.99,
        tax=None,
        db=db,
        labels_fingerprint="fp-active",
    )

    row = db.conn.execute(
        "SELECT labels_fingerprint FROM predictions WHERE species=?", ("Robin",)
    ).fetchone()
    assert row is not None, "prediction was not stored"
    assert row["labels_fingerprint"] == "fp-active"

    # And the fingerprint-aware cache lookup must now find it.
    hits = db.get_existing_prediction_photo_ids(
        "bioclip-2", labels_fingerprint="fp-active",
    )
    assert hits == {photo_id}


def test_classifier_skipped_when_run_already_recorded(tmp_path, monkeypatch):
    """If (detection, classifier_model, fingerprint) already ran, don't invoke again."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    det_id = det_ids[0]

    # Pre-seed a classifier run — any subsequent invocation should bail
    db.record_classifier_run(det_id, "bioclip-2", "abc123", prediction_count=0)

    calls = {"n": 0}
    def fake_classify(*a, **kw):
        calls["n"] += 1
        return []
    monkeypatch.setattr("classify_job._run_classifier_on_detection", fake_classify)

    import classify_job
    classify_job._classify_detection_gated(
        db=db, detection_id=det_id,
        classifier_model="bioclip-2",
        labels_fingerprint="abc123",
        labels=["Robin"], reclassify=False,
    )
    assert calls["n"] == 0, "classifier should be skipped when run key exists"


def test_record_batch_classifier_runs_skips_zero_count(tmp_path):
    """A failed classifier batch (no prediction for a detection) must not be
    cached as a completed run — otherwise the detection is permanently
    stranded on the next non-reclassify pass.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ok, det_failed = db.save_detections(
        photo_id,
        [
            {"box": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}, "confidence": 0.9},
            {"box": {"x": 0.5, "y": 0.5, "w": 0.5, "h": 0.5}, "confidence": 0.8},
        ],
        detector_model="megadetector-v6",
    )

    batch = [
        {"detection_id": det_ok, "img": object()},
        {"detection_id": det_failed, "img": object()},
    ]
    # Only the first detection made it into raw_results (second one failed)
    raw_results = [{"detection_id": det_ok, "species": "Robin", "confidence": 0.9}]

    import classify_job
    classify_job._record_batch_classifier_runs(
        db, batch, "bioclip-2", "abc123", raw_results
    )

    keys_ok = db.get_classifier_run_keys(det_ok)
    keys_failed = db.get_classifier_run_keys(det_failed)
    assert keys_ok == {("bioclip-2", "abc123")}, "successful detection should be cached"
    assert keys_failed == set(), "failed detection must NOT be cached"


def test_classifier_fingerprint_upserted(tmp_path, monkeypatch):
    """When a classifier runs, the labels fingerprint is upserted."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )

    from labels_fingerprint import compute_fingerprint
    labels = ["Robin", "Sparrow"]
    expected_fp = compute_fingerprint(labels)

    import classify_job
    classify_job._record_labels_fingerprint(
        db, fingerprint=expected_fp, labels=labels,
        sources=["/tmp/active.txt"],
    )
    row = db.conn.execute(
        "SELECT display_name, label_count FROM labels_fingerprints WHERE fingerprint=?",
        (expected_fp,),
    ).fetchone()
    assert row["label_count"] == 2


def test_classify_photos_iterates_over_detections(tmp_path):
    """_classify_photos should classify each detection independently."""
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    # Create a test image
    img = Image.new("RGB", (200, 200), color="red")
    img_path = tmp_path / "multi.jpg"
    img.save(str(img_path))

    photos = [
        {"id": 1, "filename": "multi.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}
    # Two detections for photo 1
    detection_map = {
        1: [
            {"id": 101, "box_x": 0.1, "box_y": 0.1, "box_w": 0.3, "box_h": 0.3,
             "confidence": 0.95, "category": "animal"},
            {"id": 102, "box_x": 0.5, "box_y": 0.5, "box_w": 0.2, "box_h": 0.2,
             "confidence": 0.80, "category": "animal"},
        ]
    }
    existing_preds = set()

    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds_1 = [{"species": "Northern Cardinal", "score": 0.95, "taxonomy": None}]
    fake_preds_2 = [{"species": "Blue Jay", "score": 0.88, "taxonomy": None}]

    mock_clf = MagicMock()
    mock_clf.classify_batch_with_embedding.return_value = [
        (fake_preds_1, fake_embedding),
        (fake_preds_2, fake_embedding),
    ]

    mock_db = MagicMock()
    mock_db.get_photo_embedding.return_value = None

    # Use side_effect to return a fresh Image each call, since _prepare_image
    # closes the original image after cropping (resource leak fix).
    with patch("classify_job.load_image", side_effect=lambda *a, **kw: Image.new("RGB", (200, 200))):
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

    assert len(raw_results) == 2
    assert raw_results[0]["detection_id"] == 101
    assert raw_results[0]["prediction"] == "Northern Cardinal"
    assert raw_results[1]["detection_id"] == 102
    assert raw_results[1]["prediction"] == "Blue Jay"
    assert failed == 0
    assert skipped == 0


# ── _classify_photos tests ──────────────────────────────────────────────────


def test_classify_photos_new_photo(tmp_path):
    """Phase 6: classifies a new photo and returns raw results."""
    from unittest.mock import MagicMock, patch

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
    mock_clf.classify_batch_with_embedding.return_value = [(fake_preds, fake_embedding)]

    mock_db = MagicMock()
    mock_db.get_photo_embedding.return_value = None

    # Use side_effect to return a fresh Image each call, since _flush_batch
    # closes images after classification (resource leak fix).
    with patch("classify_job.load_image", side_effect=lambda *a, **kw: Image.new("RGB", (200, 200))):
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
    """Skipping is now per-detection via classifier_runs, not per-photo.

    When a detection's (model, fingerprint) has a cached classifier run,
    the classifier is not re-invoked, but the cached top-1 prediction is
    surfaced into raw_results so downstream grouping still sees it.
    """
    from unittest.mock import MagicMock

    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}

    mock_clf = MagicMock()
    mock_db = MagicMock()
    # Detection 101 has a cached classifier_run for (BioCLIP, legacy).
    mock_db.get_classifier_run_keys.return_value = {("BioCLIP", "legacy")}
    mock_db.get_predictions_for_detection.return_value = [
        {"species": "Northern Cardinal", "confidence": 0.95,
         "detection_id": 101},
    ]
    mock_db.get_photo_embedding.return_value = None

    detection_map = {
        1: [{"id": 101, "box_x": 0.1, "box_y": 0.1,
             "box_w": 0.5, "box_h": 0.5, "confidence": 0.9,
             "category": "animal"}],
    }

    raw_results, failed, skipped = _classify_photos(
        photos=photos,
        folders=folders,
        detection_map=detection_map,
        existing_preds=set(),  # dead parameter post-refactor
        clf=mock_clf,
        model_type="bioclip",
        model_name="BioCLIP",
        runner=runner,
        job=job,
        db=mock_db,
    )

    assert skipped == 1, "cached detection should count as skipped"
    assert len(raw_results) == 1, "cached prediction must be surfaced"
    assert raw_results[0]["_existing"] is True
    assert raw_results[0]["prediction"] == "Northern Cardinal"
    mock_clf.classify_with_embedding.assert_not_called()


# ── Top-N predictions tests ────────────────────────────────────────────────


def test_flush_batch_stores_top_n_predictions(tmp_path):
    """_flush_batch keeps top_k predictions per image, not just top-1."""
    from unittest.mock import MagicMock

    from classify_job import _flush_batch

    db = MagicMock()
    raw_results = []

    # Classifier returns 5 ranked predictions
    all_preds = [
        {"species": "Robin", "score": 0.70, "taxonomy": None},
        {"species": "Sparrow", "score": 0.15, "taxonomy": None},
        {"species": "Finch", "score": 0.10, "taxonomy": None},
        {"species": "Wren", "score": 0.03, "taxonomy": None},
        {"species": "Jay", "score": 0.02, "taxonomy": None},
    ]
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [(all_preds, None)]

    batch = [{
        "photo": {"id": 1, "filename": "bird.jpg", "timestamp": None},
        "detection_id": 10,
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "img": MagicMock(),
    }]

    failed = _flush_batch(batch, clf, "bioclip", "test-model", db, raw_results, top_k=3)
    assert failed == 0
    assert len(raw_results) == 1

    item = raw_results[0]
    # Should have top prediction as before
    assert item["prediction"] == "Robin"
    assert item["confidence"] == 0.70
    # Should also have alternatives list
    assert "alternatives" in item
    assert len(item["alternatives"]) == 2
    assert item["alternatives"][0]["species"] == "Sparrow"
    assert item["alternatives"][1]["species"] == "Finch"


def test_flush_batch_top_k_1_has_empty_alternatives():
    """_flush_batch with top_k=1 (default) produces empty alternatives list."""
    from unittest.mock import MagicMock

    from classify_job import _flush_batch

    db = MagicMock()
    raw_results = []

    all_preds = [
        {"species": "Robin", "score": 0.70, "taxonomy": None},
        {"species": "Sparrow", "score": 0.15, "taxonomy": None},
    ]
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [(all_preds, None)]

    batch = [{
        "photo": {"id": 1, "filename": "bird.jpg", "timestamp": None},
        "detection_id": 10,
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "img": MagicMock(),
    }]

    failed = _flush_batch(batch, clf, "bioclip", "test-model", db, raw_results)
    assert failed == 0
    assert len(raw_results) == 1
    assert raw_results[0]["alternatives"] == []


def test_flush_batch_default_top_k_is_one():
    """Default top_k=1 preserves backward-compatible behavior (no alternatives)."""
    from unittest.mock import MagicMock

    from classify_job import _flush_batch

    db = MagicMock()
    raw_results = []

    all_preds = [
        {"species": "Robin", "score": 0.70, "taxonomy": None},
        {"species": "Sparrow", "score": 0.15, "taxonomy": None},
    ]
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [(all_preds, None)]

    batch = [{
        "photo": {"id": 1, "filename": "bird.jpg", "timestamp": None},
        "detection_id": 10,
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "img": MagicMock(),
    }]

    _flush_batch(batch, clf, "bioclip", "test-model", db, raw_results)
    assert len(raw_results) == 1
    assert raw_results[0]["prediction"] == "Robin"
    assert raw_results[0]["alternatives"] == []


# ── Top-N: _store_grouped_predictions alternatives tests ─────────────────────


def test_store_grouped_predictions_saves_alternatives(tmp_path):
    """_store_grouped_predictions stores alternatives with status='alternative'."""
    from unittest.mock import patch

    from classify_job import _store_grouped_predictions
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0, timestamp="2024-01-15T10:00:00")
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="megadetector-v6")

    raw_results = [{
        "photo": {"id": pid, "filename": "bird.jpg", "timestamp": "2024-01-15T10:00:00"},
        "detection_id": det_ids[0],
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "prediction": "Robin",
        "confidence": 0.85,
        "timestamp": None,
        "filename": "bird.jpg",
        "embedding": None,
        "taxonomy": None,
        "alternatives": [
            {"species": "Sparrow", "confidence": 0.10, "taxonomy": None},
            {"species": "Finch", "confidence": 0.05, "taxonomy": None},
        ],
    }]

    with patch("xmp.read_keywords", return_value=[]), \
         patch("compare.categorize", return_value="new"):
        result = _store_grouped_predictions(
            raw_results=raw_results,
            job_id="test-job-123456",
            model_name="test-model",
            grouping_window=10,
            similarity_threshold=0.85,
            tax=None,
            db=db,
        )

    assert result["predictions_stored"] == 1

    all_preds = db.get_predictions()
    assert len(all_preds) == 3  # 1 pending + 2 alternatives

    pending = db.get_predictions(status="pending")
    assert len(pending) == 1
    assert pending[0]["species"] == "Robin"

    alts = db.get_predictions(status="alternative")
    assert len(alts) == 2
    alt_species = {p["species"] for p in alts}
    assert alt_species == {"Sparrow", "Finch"}


# ── _store_grouped_predictions tests ─────────────────────────────────────────


def test_store_grouped_predictions_single_photo():
    """Phase 7: single-photo group stores prediction directly."""
    from unittest.mock import MagicMock

    from classify_job import _store_grouped_predictions

    mock_db = MagicMock()

    raw_results = [
        {
            "photo": {"id": 1, "filename": "bird.jpg"},
            "detection_id": 101,
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
    assert call_kwargs["detection_id"] == 101


def test_store_grouped_predictions_burst_group():
    """Phase 7: multi-photo group computes consensus and stores for all photos."""
    from datetime import datetime
    from unittest.mock import MagicMock

    from classify_job import _store_grouped_predictions

    mock_db = MagicMock()

    raw_results = [
        {
            "photo": {"id": 1, "filename": "bird1.jpg"},
            "detection_id": 101,
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
            "detection_id": 102,
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
    from unittest.mock import MagicMock, patch

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
         "timestamp": "2024-01-15T10:00:00"},
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
    mock_clf.classify_batch_with_embedding.return_value = [(fake_preds, fake_embedding)]

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

    app, db = app_and_db
    client = app.test_client()

    # Create a collection so the request is valid
    import json as _json
    col_id = db.add_collection("Test", _json.dumps([{"type": "all"}]))

    captured = {}

    def fake_run(job, runner, db_path, workspace_id, params, vireo_dir=None):
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


# ── Task 8: _prepare_image working copy tests ─────────────────────────────────


def test_prepare_image_uses_working_copy(tmp_path):
    """_prepare_image loads from working copy when vireo_dir is provided."""
    from classify_job import _prepare_image

    # Set up vireo_dir with a working copy JPEG
    vireo_dir = tmp_path / "vireo"
    working_dir = vireo_dir / "working"
    working_dir.mkdir(parents=True)

    wc_img = Image.new("RGB", (2000, 1500), color="blue")
    wc_path = working_dir / "42.jpg"
    wc_img.save(str(wc_path), "JPEG")

    photo = {
        "id": 42,
        "folder_id": 10,
        "filename": "bird.nef",
        "working_copy_path": "working/42.jpg",
    }
    folders = {10: str(tmp_path / "photos")}

    # Do NOT create the original file — _prepare_image should use the working copy
    img, folder_path, image_path = _prepare_image(
        photo, folders, None, vireo_dir=str(vireo_dir)
    )

    assert img is not None
    # The result should be thumbnailed to 1024
    assert max(img.size) <= 1024


def test_prepare_image_falls_back_without_working_copy(tmp_path):
    """_prepare_image falls back to load_image when no working copy exists."""

    from classify_job import _prepare_image

    vireo_dir = str(tmp_path / "vireo")

    # Create a real original image
    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    orig_img = Image.new("RGB", (2000, 1500), color="red")
    orig_img.save(str(photos_dir / "bird.jpg"), "JPEG")

    photo = {
        "id": 99,
        "folder_id": 10,
        "filename": "bird.jpg",
        "working_copy_path": None,
    }
    folders = {10: str(photos_dir)}

    img, folder_path, image_path = _prepare_image(
        photo, folders, None, vireo_dir=vireo_dir
    )

    assert img is not None
    assert max(img.size) <= 1024


def test_prepare_image_crops_detection_from_working_copy(tmp_path):
    """_prepare_image crops to detection bbox when using a working copy."""
    from classify_job import _prepare_image

    # Set up vireo_dir with a working copy
    vireo_dir = tmp_path / "vireo"
    working_dir = vireo_dir / "working"
    working_dir.mkdir(parents=True)

    wc_img = Image.new("RGB", (2000, 1500), color="green")
    wc_path = working_dir / "7.jpg"
    wc_img.save(str(wc_path), "JPEG")

    photo = {
        "id": 7,
        "folder_id": 10,
        "filename": "bird.arw",
        "working_copy_path": "working/7.jpg",
    }
    folders = {10: str(tmp_path / "photos")}

    detection = {
        "box_x": 0.2,
        "box_y": 0.2,
        "box_w": 0.4,
        "box_h": 0.4,
    }

    img, folder_path, image_path = _prepare_image(
        photo, folders, detection, vireo_dir=str(vireo_dir)
    )

    assert img is not None
    # Should be cropped and thumbnailed
    assert max(img.size) <= 1024


def test_prepare_image_no_vireo_dir_uses_original(tmp_path):
    """_prepare_image without vireo_dir loads original file directly."""
    from classify_job import _prepare_image

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    orig_img = Image.new("RGB", (800, 600), color="yellow")
    orig_img.save(str(photos_dir / "bird.jpg"), "JPEG")

    photo = {
        "id": 1,
        "folder_id": 10,
        "filename": "bird.jpg",
        "working_copy_path": "working/1.jpg",  # has path but no vireo_dir
    }
    folders = {10: str(photos_dir)}

    img, folder_path, image_path = _prepare_image(
        photo, folders, None  # no vireo_dir
    )

    assert img is not None
