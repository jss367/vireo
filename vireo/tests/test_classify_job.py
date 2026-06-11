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
        self.cancelled = False
        self.steps = []

    def push_event(self, job_id, event_type, data):
        self.events.append((job_id, event_type, data))

    def set_steps(self, job_id, steps):
        pass

    def update_step(self, job_id, step_id, **kwargs):
        self.steps.append((step_id, kwargs))

    def is_cancelled(self, job_id):
        return self.cancelled


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
    mock_db.write_detection_batch.return_value = [101]

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

    If compute_sharpness or update_photo_quality raises after write_detection_batch,
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
    mock_db.write_detection_batch.return_value = [42]

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
    mock_db.write_detection_batch.assert_called_once()
    # photo 7 must be in processed_ids even though quality scoring raised, so
    # the reclassify purge correctly removes its stale pre-run detection rows.
    assert 7 in processed_ids, (
        "photo_id must be in processed_ids after write_detection_batch even when "
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
    mock_db.write_detection_batch.return_value = [101, 102]

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
    mock_db.write_detection_batch.assert_called_once()


def test_detect_batch_handles_same_batch_detection_id_collapse(tmp_path):
    """If DB persistence collapses two detector outputs to one content ID,
    _detect_batch must classify the persisted row instead of strict-zip failing.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_batch

    runner = FakeRunner()
    job = _make_job()

    img = Image.new("RGB", (200, 200), color="green")
    img_path = str(tmp_path / "bird.jpg")
    img.save(img_path)

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10},
    ]
    folders = {10: str(tmp_path)}

    fake_detections = [
        {"box": {"x": 0.10001, "y": 0.2, "w": 0.3, "h": 0.4},
         "confidence": 0.80, "category": "animal"},
        {"box": {"x": 0.10002, "y": 0.2, "w": 0.3, "h": 0.4},
         "confidence": 0.95, "category": "animal"},
    ]

    mock_db = MagicMock()
    mock_db.write_detection_batch.return_value = [101]
    mock_db.get_detections.return_value = [{
        "id": 101,
        "box_x": 0.10002,
        "box_y": 0.2,
        "box_w": 0.3,
        "box_h": 0.4,
        "detector_confidence": 0.95,
        "category": "animal",
        "detector_model": "megadetector-v6",
    }]

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[1]), \
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
    assert detection_map[1] == [{
        "id": 101,
        "box_x": 0.10002,
        "box_y": 0.2,
        "box_w": 0.3,
        "box_h": 0.4,
        "confidence": 0.95,
        "category": "animal",
        "detector_model": "megadetector-v6",
    }]
    mock_db.get_detections.assert_called_once_with(
        1, min_conf=0, detector_model="megadetector-v6",
    )


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


def test_detect_batch_skips_quality_score_when_primary_below_threshold(
    tmp_path, monkeypatch
):
    """Photos whose only detection is below the workspace's
    detector_confidence threshold must NOT get a quality_score (or any
    subject_size / subject_sharpness from the noise box) — those values
    drive the highlights ranking, and a noise box that happens to span the
    frame would otherwise produce a sky-high ``subject_size`` and float
    these no-real-subject photos to the top of highlights.

    Regression for the "Mountain chickadee" highlights bug: photos with
    detector_confidence ~0.02 still received quality_score ~0.86 because
    the noise box covered ~98% of the frame.
    """
    from unittest.mock import patch

    from classify_job import _detect_batch
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "noise.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Pre-populate stale quality fields to confirm the fix also clears
    # rotten state from prior runs (self-healing — see CLAUDE.md memory
    # "App self-heals broken state").
    db.update_photo_quality(
        photo_id,
        subject_sharpness=2900.0,
        subject_size=0.98,
        quality_score=0.86,
        sharpness=2900.0,
    )

    img = Image.new("RGB", (100, 100), color="gray")
    img.save(str(tmp_path / "noise.jpg"))

    # Sub-threshold "noise" detection covering nearly the whole frame —
    # exactly what MegaDetector emits when there's no real subject.
    fake_detections = [
        {"box": {"x": 0.01, "y": 0.01, "w": 0.98, "h": 0.98},
         "confidence": 0.027, "category": "animal"},
    ]

    photos = [{"id": photo_id, "folder_id": folder_id, "filename": "noise.jpg"}]
    folders = {folder_id: str(tmp_path)}

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[0]), \
         patch("classify_job.compute_sharpness", return_value=2900.0):
        _detect_batch(
            photos=photos, folders=folders, runner=None, job={"id": 0},
            reclassify=True, db=db,
            det_conf_threshold=0.2,
            already_detected_ids=set(),
        )

    row = db.conn.execute(
        "SELECT quality_score, subject_size, subject_sharpness FROM photos WHERE id = ?",
        (photo_id,),
    ).fetchone()
    assert row["quality_score"] is None, (
        "sub-threshold detection must not produce a quality_score; "
        f"got {row['quality_score']!r}"
    )
    assert row["subject_size"] is None, (
        "sub-threshold detection must not produce subject_size; "
        f"got {row['subject_size']!r}"
    )
    assert row["subject_sharpness"] is None, (
        "sub-threshold detection must not produce subject_sharpness; "
        f"got {row['subject_sharpness']!r}"
    )


def test_detect_batch_scores_normally_when_primary_at_threshold(
    tmp_path, monkeypatch
):
    """Counterpart to the sub-threshold test: a detection at or above the
    threshold still produces a quality_score so we don't break the happy
    path."""
    from unittest.mock import patch

    from classify_job import _detect_batch
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "bird.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )

    img = Image.new("RGB", (100, 100), color="gray")
    img.save(str(tmp_path / "bird.jpg"))

    fake_detections = [
        {"box": {"x": 0.3, "y": 0.3, "w": 0.4, "h": 0.4},
         "confidence": 0.85, "category": "animal"},
    ]

    photos = [{"id": photo_id, "folder_id": folder_id, "filename": "bird.jpg"}]
    folders = {folder_id: str(tmp_path)}

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[0]), \
         patch("classify_job.compute_sharpness", return_value=1500.0):
        _detect_batch(
            photos=photos, folders=folders, runner=None, job={"id": 0},
            reclassify=True, db=db,
            det_conf_threshold=0.2,
            already_detected_ids=set(),
        )

    row = db.conn.execute(
        "SELECT quality_score, subject_size FROM photos WHERE id = ?",
        (photo_id,),
    ).fetchone()
    assert row["quality_score"] is not None and row["quality_score"] > 0
    assert row["subject_size"] is not None


def test_classify_photos_reclassifies_when_gate_has_no_cached_rows(tmp_path):
    """If classifier_runs has a (model, fp) key but get_predictions_for_detection
    returns nothing (e.g. a prior pass stored `category == 'match'` which
    is intentionally not written, or transient ordering between the run
    record and _store_grouped_predictions), the detection must fall
    through to classification — not short-circuit forever.
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
    mock_clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Robin", "score": 0.9}], None),
    ]
    mock_db = MagicMock()
    # Gate fires (run key present) but no cached prediction rows.
    mock_db.get_classifier_run_keys.return_value = {("BioCLIP", "fp-x")}
    mock_db.get_predictions_for_detection.return_value = []
    mock_db.get_photo_embedding.return_value = None

    # Need a real image on disk so _prepare_image succeeds.
    import os
    img_path = os.path.join(str(tmp_path), "bird.jpg")
    Image.new("RGB", (400, 400), color="green").save(img_path)

    detection_map = {
        1: [{"id": 101, "box_x": 0.1, "box_y": 0.1,
             "box_w": 0.5, "box_h": 0.5, "confidence": 0.9,
             "category": "animal"}],
    }

    _classify_photos(
        photos=photos,
        folders=folders,
        detection_map=detection_map,
        existing_preds=set(),
        clf=mock_clf,
        model_type="bioclip",
        model_name="BioCLIP",
        runner=runner,
        job=job,
        db=mock_db,
        labels_fingerprint="fp-x",
    )

    # The classifier must have actually been invoked — if the gate
    # short-circuited on the empty cached result, this assertion fails.
    assert (
        mock_clf.classify_batch_with_embedding.called
        or mock_clf.classify_with_embedding.called
    ), (
        "Gate fired with no cached rows and short-circuited classification; "
        "the detection is stranded until --reclassify."
    )


def test_reclassify_preserves_cache_on_model_load_failure(tmp_path, monkeypatch):
    """If the classifier fails to load, a reclassify must NOT have already
    purged cached predictions/detections — otherwise weight-corruption
    wipes shared-folder workspaces and there is no replacement.
    """
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    # Seed a collection the classify path can consume.
    coll_id = db.add_collection("c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]')

    # Force the classifier constructor to raise — simulates
    # weight-corruption or missing-weights at load time.
    import classifier as classifier_mod
    class BoomClassifier:
        def __init__(self, *a, **kw):
            raise RuntimeError("simulated weights corruption")
    monkeypatch.setattr(classifier_mod, "Classifier", BoomClassifier)

    runner = FakeRunner()
    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id="BioCLIP",
        model_name="BioCLIP",
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=True,
    )

    # Run should fail (classifier init crashes) but MUST NOT destroy the
    # cached prediction or detection.
    import contextlib
    with contextlib.suppress(Exception):
        run_classify_job(job, runner, db_path, ws, params)

    # Re-open the DB to read post-job state
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    preds_after = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id=?",
        (det_id,),
    ).fetchone()["n"]
    dets_after = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM detections WHERE id=?",
        (det_id,),
    ).fetchone()["n"]
    assert preds_after == 1, (
        "Reclassify purge happened before model load failed — cached "
        "predictions were destroyed without replacement."
    )
    assert dets_after == 1, (
        "Detections purged before model load failure — cache lost."
    )


def test_reclassify_skips_purge_when_cancelled_during_model_load(tmp_path, monkeypatch):
    """If the user cancels while model load / embedding computation is
    running, the destructive reclassify purge (clear_predictions /
    clear_detections) MUST NOT execute. Without the pre-purge cancel
    gate, the post-detection gate returns with predictions_stored=0 but
    the cache is already wiped.
    """
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    # Hermetic global config so the run doesn't read or write the user's
    # ~/.vireo/config.json (per repo testing conventions).
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    coll_id = db.add_collection("c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]')

    runner = FakeRunner()

    # Classifier "loads" successfully but flips the runner to cancelled
    # mid-init, simulating a user clicking cancel during the (otherwise
    # uninterruptible) embedding computation. classify_job imports
    # Classifier at module load time, so the patch has to target that
    # reference rather than classifier.Classifier.
    import classify_job as cj
    class CancellingClassifier:
        def __init__(self, *a, **kw):
            runner.cancelled = True
    monkeypatch.setattr(cj, "Classifier", CancellingClassifier)

    # Bypass the on-disk model registry — the test doesn't need weights
    # since CancellingClassifier ignores its args.
    monkeypatch.setattr(cj, "get_active_model", lambda: {
        "id": "BioCLIP",
        "name": "BioCLIP",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/dev/null",
        "model_type": "bioclip",
        "downloaded": True,
    })

    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id=None,
        model_name="BioCLIP",
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=True,
    )

    result = run_classify_job(job, runner, db_path, ws, params)

    # The cancel-before-purge gate returns a no-op result.
    assert result["predictions_stored"] == 0
    assert result["detected"] == 0

    # And — the whole point of the gate — cached predictions and
    # detections survive the cancelled run intact.
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    preds_after = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id=?",
        (det_id,),
    ).fetchone()["n"]
    dets_after = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM detections WHERE id=?",
        (det_id,),
    ).fetchone()["n"]
    assert preds_after == 1, (
        "Reclassify purge ran despite cancel during model load — "
        "cached predictions were destroyed without replacement."
    )
    assert dets_after == 1, (
        "Reclassify purge ran despite cancel during model load — "
        "cached detections were destroyed without replacement."
    )


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


def test_store_grouped_predictions_persists_match_for_cache(tmp_path, monkeypatch):
    """Already-labeled matches should not be pending review, but they still
    need prediction rows so the next run can reuse the classifier output.
    """
    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]

    monkeypatch.setattr("compare.categorize", lambda *_args, **_kwargs: "match")

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    result = classify_job._store_grouped_predictions(
        raw_results=[{
            "photo": {
                "id": photo_id, "filename": "a.jpg",
                "folder_id": folder_id, "timestamp": None, "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Robin",
            "confidence": 0.88,
            "alternatives": [{"species": "Sparrow", "confidence": 0.12}],
            "taxonomy": {},
            "timestamp": None,
        }],
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=0,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["predictions_stored"] == 0
    assert result["already_labeled"] == 1

    cached = db.get_predictions_for_detection(
        det_id,
        classifier_model="bioclip-2",
        labels_fingerprint="fp-active",
        min_classifier_conf=0,
    )
    assert [row["species"] for row in cached] == ["Robin", "Sparrow"]
    assert cached[0]["category"] == "match"

    reviewed = db.get_predictions(photo_ids=[photo_id])
    statuses = {row["species"]: row["status"] for row in reviewed}
    assert statuses == {"Robin": "accepted", "Sparrow": "alternative"}


def test_store_grouped_predictions_persists_group_match_for_cache(tmp_path, monkeypatch):
    """Already-labeled burst groups cache consensus species per detection."""
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{idx}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for idx in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    monkeypatch.setattr("compare.categorize", lambda *_args, **_kwargs: "match")

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    raw_results = [
        {
            "photo": {
                "id": pid,
                "filename": f"{idx}.jpg",
                "folder_id": folder_id,
                "timestamp": None,
                "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Robin" if idx == 0 else "Sparrow",
            "confidence": 0.9 if idx == 0 else 0.5,
            "alternatives": [],
            "taxonomy": {},
            "timestamp": datetime(2024, 1, 1, 12, 0, idx),
        }
        for idx, (pid, det_id) in enumerate(zip(photo_ids, det_ids, strict=True))
    ]

    result = classify_job._store_grouped_predictions(
        raw_results=raw_results,
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=10,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["predictions_stored"] == 0
    assert result["already_labeled"] == 2
    rows = db.conn.execute(
        "SELECT detection_id, species, category FROM predictions"
    ).fetchall()
    assert {(r["detection_id"], r["species"], r["category"]) for r in rows} == {
        (det_ids[0], "Robin", "match"),
        (det_ids[1], "Robin", "match"),
    }


def test_group_match_drops_per_frame_alternatives(tmp_path, monkeypatch):
    """Burst match caches only the consensus species — per-frame alternatives
    are dropped so a high-confidence dissenting runner-up can't outrank the
    consensus primary via get_predictions_for_detection's confidence ordering.
    """
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{idx}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for idx in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    monkeypatch.setattr("compare.categorize", lambda *_a, **_k: "match")

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    # Both frames agree on "Robin" at 0.6, so consensus is Robin@~0.6.
    # Frame 0 carries a dissenting "Hawk" alternative at 0.95 — higher than
    # the consensus confidence. The old code cached it as an 'alternative'
    # row that won the confidence-DESC ordering and became the cached top-1.
    raw_results = [
        {
            "photo": {
                "id": pid,
                "filename": f"{idx}.jpg",
                "folder_id": folder_id,
                "timestamp": None,
                "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Robin",
            "confidence": 0.6,
            "alternatives": (
                [{"species": "Hawk", "confidence": 0.95}] if idx == 0 else []
            ),
            "taxonomy": {},
            "timestamp": datetime(2024, 1, 1, 12, 0, idx),
        }
        for idx, (pid, det_id) in enumerate(
            zip(photo_ids, det_ids, strict=True)
        )
    ]

    result = classify_job._store_grouped_predictions(
        raw_results=raw_results,
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=10,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["already_labeled"] == 2
    # No "Hawk" alternative row was persisted for either detection.
    rows = db.conn.execute(
        "SELECT detection_id, species, category, status "
        "FROM predictions "
        "LEFT JOIN prediction_review "
        "  ON prediction_review.prediction_id = predictions.id"
    ).fetchall()
    assert {
        (r["detection_id"], r["species"], r["category"], r["status"])
        for r in rows
    } == {
        (det_ids[0], "Robin", "match", "accepted"),
        (det_ids[1], "Robin", "match", "accepted"),
    }
    # The cached top-1 (confidence-DESC) is the consensus species, not Hawk.
    top = db.get_predictions_for_detection(det_ids[0], min_classifier_conf=0)
    assert top[0]["species"] == "Robin"


def test_match_then_unmatch_reenters_pending_on_reuse(tmp_path, monkeypatch):
    """A detection cached as a 'match' is auto-accepted and hidden from the
    review queue.  If the photo's XMP later stops matching, a non-reclassify
    run reuses the cached prediction; the stale 'accepted' review row must be
    downgraded so the prediction re-enters the pending queue.
    """
    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    def run(category, alternatives, extra):
        monkeypatch.setattr("compare.categorize", lambda *_a, **_k: category)
        return classify_job._store_grouped_predictions(
            raw_results=[{
                "photo": {
                    "id": photo_id, "filename": "a.jpg",
                    "folder_id": folder_id, "timestamp": None, "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": det_id,
                "prediction": "Robin",
                "confidence": 0.88,
                "alternatives": alternatives,
                "taxonomy": {},
                "timestamp": None,
                **extra,
            }],
            job_id="job-abc",
            model_name="bioclip-2",
            grouping_window=0,
            similarity_threshold=0.99,
            tax=Tax(),
            db=db,
            labels_fingerprint="fp-active",
        )

    # Run 1: photo already labeled -> match -> auto-accepted, out of queue.
    run("match", [{"species": "Sparrow", "confidence": 0.12}], {})
    statuses = {
        r["species"]: r["status"]
        for r in db.get_predictions(photo_ids=[photo_id])
    }
    assert statuses == {"Robin": "accepted", "Sparrow": "alternative"}

    # Run 2: XMP keyword removed -> no longer a match. The classify gate
    # surfaces the cached prediction (_existing) and skips inference.
    run("disagreement", [], {"_existing": True})

    rows = {r["species"]: r for r in db.get_predictions(photo_ids=[photo_id])}
    assert rows["Robin"]["status"] == "pending"        # back in the queue
    assert rows["Robin"]["category"] == "disagreement"  # stale marker cleared
    assert rows["Sparrow"]["status"] == "alternative"   # still nested


@pytest.mark.parametrize("manual_status", ["accepted", "rejected"])
def test_match_flip_preserves_manual_review_on_reuse(
    tmp_path, monkeypatch, manual_status
):
    """A temporary XMP match must not erase an explicit user decision."""
    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    def run(category):
        monkeypatch.setattr("compare.categorize", lambda *_a, **_k: category)
        return classify_job._store_grouped_predictions(
            raw_results=[{
                "photo": {
                    "id": photo_id, "filename": "a.jpg",
                    "folder_id": folder_id, "timestamp": None, "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": det_id,
                "prediction": "Robin",
                "confidence": 0.88,
                "alternatives": [],
                "taxonomy": {},
                "timestamp": None,
                "_existing": True,
            }],
            job_id="job-abc",
            model_name="bioclip-2",
            grouping_window=0,
            similarity_threshold=0.99,
            tax=Tax(),
            db=db,
            labels_fingerprint="fp-active",
        )

    run("disagreement")
    pred_id = db.get_predictions(photo_ids=[photo_id])[0]["id"]
    db.update_prediction_status(pred_id, manual_status)

    run("match")
    matched = db.get_predictions(photo_ids=[photo_id])[0]
    assert matched["category"] == "match"
    assert matched["status"] == manual_status

    run("disagreement")
    downgraded = db.get_predictions(photo_ids=[photo_id])[0]
    assert downgraded["category"] == "disagreement"
    assert downgraded["status"] == manual_status


def test_group_match_then_unmatch_reenters_pending_on_reuse(tmp_path, monkeypatch):
    """Burst groups cached as 'match' must also re-enter review when the
    photos stop matching and the cached predictions are reused.
    """
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for i in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    def run(category, extra):
        monkeypatch.setattr("compare.categorize", lambda *_a, **_k: category)
        raw = [
            {
                "photo": {
                    "id": pid, "filename": f"{i}.jpg",
                    "folder_id": folder_id, "timestamp": None,
                    "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": did,
                "prediction": "Robin",
                "confidence": 0.9 - (i * 0.01),
                "alternatives": [],
                "taxonomy": {},
                "timestamp": datetime(2024, 1, 1, 12, 0, i),
                **extra,
            }
            for i, (pid, did) in enumerate(
                zip(photo_ids, det_ids, strict=True)
            )
        ]
        return classify_job._store_grouped_predictions(
            raw_results=raw, job_id="job-abc", model_name="bioclip-2",
            grouping_window=10, similarity_threshold=0.99, tax=Tax(),
            db=db, labels_fingerprint="fp-active",
        )

    run("match", {})
    accepted = db.get_predictions(photo_ids=photo_ids, status="accepted")
    assert {r["detection_id"] for r in accepted} == set(det_ids)

    run("disagreement", {"_existing": True})

    pending = db.get_predictions(photo_ids=photo_ids, status="pending")
    by_det = {r["detection_id"]: r for r in pending}
    assert set(by_det) == set(det_ids)
    for r in by_det.values():
        assert r["status"] == "pending"
        assert r["group_id"]  # burst grouping metadata reapplied
        assert r["category"] == "disagreement"


def test_mixed_group_match_then_unmatch_reenters_pending_for_dissenters(
    tmp_path, monkeypatch
):
    """A mixed burst is cached as 'match' under the consensus species for
    every detection. When it later stops matching and the cached rows are
    reused, the downgrade must reconcile by the consensus species so the
    dissenting frame's detection also re-enters the pending queue rather
    than staying durably hidden as an auto-accepted match.
    """
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for i in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    # Frame 0 predicts Robin, frame 1 dissents with Sparrow -> consensus Robin.
    species_by_frame = ["Robin", "Sparrow"]

    def run(category, extra):
        monkeypatch.setattr("compare.categorize", lambda *_a, **_k: category)
        raw = [
            {
                "photo": {
                    "id": pid, "filename": f"{i}.jpg",
                    "folder_id": folder_id, "timestamp": None,
                    "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": did,
                "prediction": species_by_frame[i],
                "confidence": 0.9 if i == 0 else 0.5,
                "alternatives": [],
                "taxonomy": {},
                "timestamp": datetime(2024, 1, 1, 12, 0, i),
                **extra,
            }
            for i, (pid, did) in enumerate(
                zip(photo_ids, det_ids, strict=True)
            )
        ]
        return classify_job._store_grouped_predictions(
            raw_results=raw, job_id="job-abc", model_name="bioclip-2",
            grouping_window=10, similarity_threshold=0.99, tax=Tax(),
            db=db, labels_fingerprint="fp-active",
        )

    run("match", {})
    accepted = db.get_predictions(photo_ids=photo_ids, status="accepted")
    assert {r["detection_id"] for r in accepted} == set(det_ids)
    assert {r["species"] for r in accepted} == {"Robin"}

    run("disagreement", {"_existing": True})

    pending = db.get_predictions(photo_ids=photo_ids, status="pending")
    by_det = {r["detection_id"]: r for r in pending}
    # Both detections, including the dissenting Sparrow frame, re-enter
    # review; none stay hidden as a stale auto-accepted match.
    assert set(by_det) == set(det_ids)
    for r in by_det.values():
        assert r["status"] == "pending"
        assert r["category"] == "disagreement"
    assert not db.get_predictions(photo_ids=photo_ids, status="accepted")


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


def test_classify_detection_gated_does_not_cache_zero_count(tmp_path, monkeypatch):
    """A classify_fn returning [] (transient failure or no-op test stub) must
    NOT be recorded as a completed classifier_run — otherwise the next
    non-reclassify pass short-circuits on the gate and the detection is
    permanently stranded without predictions.

    Mirrors the guard already in _record_batch_classifier_runs and the
    inline pipeline_job branch.
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
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
          "category": "animal"}],
        detector_model="megadetector-v6",
    )
    det_id = det_ids[0]

    import classify_job
    # classify_fn=None returns [] with no side effects (see
    # _run_classifier_on_detection). The gate must NOT write a run row.
    classify_job._classify_detection_gated(
        db=db, detection_id=det_id,
        classifier_model="bioclip-2",
        labels_fingerprint="abc123",
        labels=["Robin"], reclassify=False,
    )
    assert db.get_classifier_run_keys(det_id) == set(), (
        "zero-prediction classify_fn must not record a run key"
    )


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
    mock_db.upsert_photo_embedding.assert_called_once()


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


# ── GPU lock scope ────────────────────────────────────────────────────────


def test_flush_batch_does_not_hold_gpu_lock_around_helper_or_db():
    """``_flush_batch`` must not hold the GPU semaphore around the
    classifier helper call or the DB writes.

    Regression for the Codex P2 on PR #899: the process-wide GPU lock
    has been pushed down into the classifier implementations (around
    ``session.run`` only). At the ``_flush_batch`` level, neither the
    classifier helper invocation nor ``db.upsert_photo_embedding`` should
    see the lock held, so concurrent pipelines' detector/SAM/DINO GPU
    batches aren't blocked while this one does CPU preprocessing or DB
    work. The lock-held-during-``session.run`` half of this guarantee is
    asserted in ``test_classifier.py`` /
    ``test_timm_classifier.py``.
    """
    from unittest.mock import MagicMock

    import pipeline_locks
    from classify_job import _flush_batch

    snapshots = {}

    def record_inside_helper(images, threshold=0):
        snapshots["during_helper"] = pipeline_locks._GPU_SEMAPHORE._value
        return [
            (
                [{"species": "Robin", "score": 0.7, "taxonomy": None}],
                _FakeEmbedding(),
            )
            for _ in images
        ]

    def record_inside_db(photo_id, model_name, embedding_bytes):
        snapshots["during_db_write"] = pipeline_locks._GPU_SEMAPHORE._value

    clf = MagicMock()
    clf.classify_batch_with_embedding.side_effect = record_inside_helper

    db = MagicMock()
    db.upsert_photo_embedding.side_effect = record_inside_db

    raw_results = []
    batch = [{
        "photo": {"id": 1, "filename": "bird.jpg", "timestamp": None},
        "detection_id": 10,
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "img": MagicMock(),
    }]

    baseline = pipeline_locks._GPU_SEMAPHORE._value
    _flush_batch(batch, clf, "bioclip", "test-model", db, raw_results)
    assert pipeline_locks._GPU_SEMAPHORE._value == baseline, (
        "semaphore must be released on the way out"
    )
    assert snapshots["during_helper"] == baseline, (
        "GPU lock must NOT be held around clf.classify_batch_with_embedding "
        "at the _flush_batch level — the lock now lives inside the classifier "
        "helpers, wrapping only ``session.run``"
    )
    assert snapshots["during_db_write"] == baseline, (
        "GPU lock must NOT be held during db.upsert_photo_embedding so "
        "concurrent pipelines can do GPU work while this one persists"
    )


class _FakeEmbedding:
    """Stand-in for a numpy array that implements only ``.tobytes()``."""

    def tobytes(self):
        return b"\x00" * 16


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
    # Subject-skip gate: with no subject types configured the gate is a no-op.
    mock_db_instance.get_subject_types.return_value = set()
    mock_db_instance.filter_out_subject_tagged.side_effect = (
        lambda pids, _types: list(pids)
    )

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


# ── Task 9: Subject-tagged skip-gate ──────────────────────────────────────────


def _setup_two_photo_classify_workspace(tmp_path):
    """Create a real DB with two photos in a static collection.
    p1 is tagged with a 'genre' keyword (Landscape); p2 is untagged.
    Returns (db_path, ws_id, col_id, p1, p2)."""
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder_id = db.add_folder(str(tmp_path / "photos"), name="photos")
    db.add_workspace_folder(ws, folder_id)
    p1 = db.add_photo(
        folder_id, "p1.jpg", extension=".jpg", file_size=100, file_mtime=1.0,
    )
    p2 = db.add_photo(
        folder_id, "p2.jpg", extension=".jpg", file_size=100, file_mtime=2.0,
    )

    # Tag p1 with a genre keyword so it should be filtered out by the skip-gate.
    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    db.tag_photo(p1, scene_kid)

    col_id = db.add_collection(
        "static",
        json.dumps([{"field": "photo_ids", "value": [p1, p2]}]),
    )
    db.conn.close()
    return db_path, ws, col_id, p1, p2


def _run_classify_capturing_photos(db_path, ws, col_id, reclassify):
    """Run run_classify_job with all heavy dependencies stubbed and return
    the list of photo IDs that flowed into _detect_subjects + the events
    pushed by the runner."""
    from unittest.mock import patch

    from classify_job import ClassifyParams, run_classify_job

    runner = FakeRunner()
    job = _make_job()

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }

    captured_photos = []

    def _fake_detect_subjects(photos, folders, runner, job, reclassify, db):
        captured_photos.extend([p["id"] for p in photos])
        return ({}, 0)

    params = ClassifyParams(
        collection_id=col_id,
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name="TestModel",
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=reclassify,
    )

    with patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch(
            "classify_job._load_labels", return_value=(["Northern Cardinal"], False),
         ), \
         patch("classify_job.Classifier"), \
         patch("classify_job._detect_subjects", side_effect=_fake_detect_subjects):
        result = run_classify_job(job, runner, db_path, ws, params)

    return captured_photos, runner.events, result


def test_classify_job_skips_photos_with_subject_keywords(tmp_path):
    """When a photo has a keyword whose type is in the workspace's
    subject_types, the classifier doesn't include it in the run.

    Verified by capturing the photo IDs passed into the (stubbed)
    _detect_subjects step. Only p2 (untagged) should reach detection;
    p1 (tagged 'Landscape', type='genre') is skipped at the load step.
    """
    db_path, ws, col_id, p1, p2 = _setup_two_photo_classify_workspace(tmp_path)

    seen, events, _ = _run_classify_capturing_photos(
        db_path, ws, col_id, reclassify=False,
    )

    assert seen == [p2], (
        f"Expected only p2 ({p2}) to reach the detector, got {seen}"
    )

    # The skip-count should be surfaced in a progress event.
    progress_events = [
        d for (_jid, kind, d) in events
        if kind == "progress" and d.get("skipped_subject")
    ]
    assert progress_events, "Expected a progress event with skipped_subject"
    assert progress_events[0]["skipped_subject"] == 1
    assert progress_events[0]["phase"] == "Step 1/5: Loading photos"


def test_classify_job_reclassify_true_bypasses_subject_skip(tmp_path):
    """With reclassify=True, even subject-tagged photos are reprocessed,
    so users can verify or refresh existing tags."""
    db_path, ws, col_id, p1, p2 = _setup_two_photo_classify_workspace(tmp_path)

    seen, events, _ = _run_classify_capturing_photos(
        db_path, ws, col_id, reclassify=True,
    )

    assert sorted(seen) == sorted([p1, p2]), (
        f"reclassify=True should bypass the skip-gate; got {seen}"
    )
    skip_events = [
        d for (_jid, kind, d) in events
        if kind == "progress" and d.get("skipped_subject")
    ]
    assert not skip_events, (
        f"reclassify=True should not surface a skipped_subject event, got "
        f"{skip_events}"
    )


def test_classify_job_always_skips_wildlife_excluded_photos(tmp_path):
    """Explicit Not Wildlife state skips classification even on reclassify."""
    from db import Database

    db_path, ws, col_id, p1, p2 = _setup_two_photo_classify_workspace(tmp_path)
    db = Database(db_path)
    db.set_active_workspace(ws)
    db.update_photo_wildlife_excluded(p2, True)
    db.conn.close()

    seen, events, _ = _run_classify_capturing_photos(
        db_path, ws, col_id, reclassify=True,
    )

    assert seen == [p1]
    progress_events = [
        d for (_jid, kind, d) in events
        if kind == "progress" and d.get("skipped_wildlife_excluded")
    ]
    assert progress_events
    assert progress_events[0]["skipped_wildlife_excluded"] == 1


def test_classify_job_short_circuits_when_all_photos_skipped(tmp_path):
    """Regression: when the subject-skip filter empties the photo set, the
    job must short-circuit before model load. Loading a model is expensive
    and can fail; doing it for zero photos undermines the skip behavior."""
    from unittest.mock import patch

    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path / "photos"), name="photos")
    db.add_workspace_folder(ws, folder_id)
    p1 = db.add_photo(
        folder_id, "p1.jpg", extension=".jpg", file_size=100, file_mtime=1.0,
    )
    # Tag p1 with a genre keyword so the skip-gate filters it out.
    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    db.tag_photo(p1, scene_kid)
    col_id = db.add_collection(
        "static-only-tagged",
        json.dumps([{"field": "photo_ids", "value": [p1]}]),
    )
    db.conn.close()

    runner = FakeRunner()
    job = _make_job()

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }

    classifier_init_calls = []

    def _record_classifier_init(*args, **kwargs):
        classifier_init_calls.append((args, kwargs))
        raise AssertionError(
            "Classifier should NOT be initialized when no photos remain "
            "after subject-skip filtering."
        )

    params = ClassifyParams(
        collection_id=col_id,
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name="TestModel",
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=False,
    )

    with patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch(
            "classify_job._load_labels", return_value=(["Northern Cardinal"], False),
         ), \
         patch("classify_job.Classifier", side_effect=_record_classifier_init):
        result = run_classify_job(job, runner, db_path, ws, params)

    assert classifier_init_calls == [], (
        "Classifier was initialized despite zero photos to process. "
        "The early-return short-circuit was not taken."
    )
    assert result["total"] == 0
    assert result["predictions_stored"] == 0


def test_classify_job_zero_photos_skips_model_resolution(tmp_path):
    """Regression: when the subject-skip filter empties the photo set, the
    job must short-circuit BEFORE model resolution. Otherwise a user with no
    classifier downloaded would get a misleading 'No model available' error
    for a job that has zero work to do."""
    from unittest.mock import patch

    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path / "photos"), name="photos")
    db.add_workspace_folder(ws, folder_id)
    p1 = db.add_photo(
        folder_id, "p1.jpg", extension=".jpg", file_size=100, file_mtime=1.0,
    )
    # Tag with a genre keyword so the subject-skip gate filters it out.
    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    db.tag_photo(p1, scene_kid)
    col_id = db.add_collection(
        "static-only-tagged",
        json.dumps([{"field": "photo_ids", "value": [p1]}]),
    )
    db.conn.close()

    runner = FakeRunner()
    job = _make_job()

    params = ClassifyParams(
        collection_id=col_id,
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name="TestModel",
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=False,
    )

    # Simulate the "no model downloaded" environment: get_active_model
    # returns None (which would normally raise RuntimeError downstream).
    # The short-circuit must execute first and avoid touching the model.
    with patch("classify_job.get_active_model", return_value=None), \
         patch("classify_job.get_models", return_value=[]):
        result = run_classify_job(job, runner, db_path, ws, params)

    assert result["total"] == 0
    assert result["predictions_stored"] == 0


def test_run_classifier_retries_on_database_is_locked(tmp_path):
    """Per-detection prediction commit must retry transient 'database is locked'.

    Concurrent pipelines on the same SQLite file (observed in production: a
    second pipeline failed at classify after ~5h with 'Fatal: database is
    locked') exceed the 30s busy_timeout under sustained writer contention.
    Without retry the whole stage aborts and the run is lost.
    """
    import sqlite3

    import classify_job
    from db import Database
    from tests.test_scanner import _FlakyConn

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
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
          "category": "animal"}],
        detector_model="megadetector-v6",
    )
    det_id = det_ids[0]

    locked = sqlite3.OperationalError("database is locked")
    db.conn = _FlakyConn(db.conn, fail_on_calls={1: locked, 2: locked})

    classify_job._run_classifier_on_detection(
        db=db, detection_id=det_id,
        classifier_model="bioclip-2",
        labels=["Robin"],
        labels_fingerprint="abc123",
        classify_fn=lambda: [{"species": "Robin", "confidence": 0.9}],
    )

    n = db.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id = ?",
        (det_id,),
    ).fetchone()["n"]
    assert n == 1, "prediction must be persisted after transient lock retries"


# ── Cancellation and weights-degrade regressions ────────────────────────────


def test_detect_subjects_stops_when_cancelled(tmp_path):
    """A cancelled job exits the detection loop without processing photos."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    runner.cancelled = True
    job = _make_job()

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10},
        {"id": 2, "filename": "b.jpg", "folder_id": 10},
    ]
    mock_db = MagicMock()
    # Everything cached → no weights download attempt before the loop.
    mock_db.get_detector_run_photo_ids.return_value = {1, 2}

    detect = MagicMock()
    with patch("classify_job.detect_animals", detect), \
         patch("classify_job.get_primary_detection", MagicMock()):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders={10: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    assert detection_map == {}
    assert detected == 0
    detect.assert_not_called()


def test_detect_subjects_skips_weight_download_when_cancelled(tmp_path):
    """A cancel landing during classifier-init must skip the ~300 MB
    MegaDetector weights download. The per-photo cancel check in the
    detection loop runs too late — hf_hub_download can't be interrupted
    once started, so the gate has to live before the download call."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    runner.cancelled = True
    job = _make_job()

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10},
        {"id": 2, "filename": "b.jpg", "folder_id": 10},
    ]
    mock_db = MagicMock()
    # Nothing cached → needs_fresh_detection is True, the un-fixed code
    # would call ensure_megadetector_weights before noticing the cancel.
    mock_db.get_detector_run_photo_ids.return_value = set()

    detect = MagicMock()
    weights = MagicMock()
    with patch("classify_job.detect_animals", detect), \
         patch("classify_job.get_primary_detection", MagicMock()), \
         patch("detector.ensure_megadetector_weights", weights):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders={10: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    assert detection_map == {}
    assert detected == 0
    weights.assert_not_called()
    detect.assert_not_called()


def test_detect_subjects_reclassify_preserves_unprocessed_photos_on_cancel(tmp_path, monkeypatch):
    """For reclassify runs, cancellation mid-detection must NOT have wiped
    the detections of photos that hadn't been re-detected yet. Doing the
    clear upfront for the whole scope (the old behavior) stranded the
    unprocessed tail with empty state — worse than before the run started.
    The fix clears per-photo immediately before each photo is re-detected,
    so cancelled photos keep their cache.

    The cascaded predictions clear lives in ``_classify_photos`` instead of
    here, so this test verifies both the touched and untouched photos'
    predictions survive ``_detect_subjects`` — the classify loop is what
    rebuilds them.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _detect_subjects
    from db import Database

    # Hermetic global config (per repo testing conventions).
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid_first = db.add_photo(
        folder_id, "first.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    pid_second = db.add_photo(
        folder_id, "second.jpg", extension=".jpg",
        file_size=100, file_mtime=2.0,
    )
    det_first = db.save_detections(pid_first, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    det_second = db.save_detections(pid_second, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(det_first, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")
    db.add_prediction(det_second, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    class FlipRunner(FakeRunner):
        """Cancel flips on right after the first photo has been processed."""

        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            # First call: top of iteration 0 → not cancelled (process first).
            # Subsequent calls (top of iteration 1+) → cancelled (skip rest).
            return self._calls >= 2

    runner = FlipRunner()
    job = _make_job()
    photos = [
        {"id": pid_first, "filename": "first.jpg", "folder_id": folder_id},
        {"id": pid_second, "filename": "second.jpg", "folder_id": folder_id},
    ]

    # detect_animals returns one detection for the first (only) photo
    # that completes; the second photo is never reached. compute_sharpness
    # is patched to a real callable (rather than ``None``) so that a stray
    # invocation would surface as an assertion failure on the real
    # behavior, not a ``TypeError`` from calling ``None``.
    with patch("classify_job.detect_animals",
               return_value=[{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                              "confidence": 0.8, "category": "animal"}]), \
         patch("classify_job.get_primary_detection",
               return_value={"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                             "confidence": 0.8}), \
         patch("classify_job.compute_sharpness", return_value=0.0):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders={folder_id: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=True,
            db=db,
        )

    # The second photo's cached prediction and detection must survive
    # intact: it was never reached, so the per-photo clear never ran.
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    preds_second = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id = ?",
        (det_second,),
    ).fetchone()["n"]
    dets_second = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM detections WHERE id = ?",
        (det_second,),
    ).fetchone()["n"]
    assert preds_second == 1, (
        "Mid-detection cancel on a reclassify run wiped the cache of a "
        "photo that was never reached — the upfront global purge leaked "
        "back in."
    )
    assert dets_second == 1, (
        "Mid-detection cancel on a reclassify run cleared detections of "
        "a photo that was never reached."
    )


def test_detect_subjects_reclassify_tracks_clear_when_detect_returns_none(tmp_path, monkeypatch):
    """For reclassify runs, ``_detect_subjects`` must record a photo for
    rebuild as soon as its prior detections have been cleared — not only
    when ``_detect_batch`` reports it processed.

    Regression for Codex P2: if ``detect_animals`` returns ``None`` (image
    decode failure / ONNX hiccup), ``_detect_batch`` deliberately omits
    the id from ``processed_ids`` so a future non-reclassify pass retries
    it. But in reclassify mode the per-photo ``clear_detections`` has
    already cascaded away the old detections + predictions; if the user
    then cancels before the next iteration, the run_classify_job cancel
    path sees an empty processed set and skips classification, leaving
    the photo with no detections and no predictions.

    The fix marks the photo for rebuild immediately after the clear, so
    the full-image fallback in ``_classify_photos`` rebuilds it.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _detect_subjects
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")

    runner = FakeRunner()
    job = _make_job()
    photos = [{"id": pid, "filename": "a.jpg", "folder_id": folder_id}]

    # detect_animals returns None → _detect_batch hits its early-continue
    # and the id never lands in batch_processed. The clear above still ran.
    with patch("classify_job.detect_animals", return_value=None), \
         patch("classify_job.get_primary_detection", return_value=None), \
         patch("classify_job.compute_sharpness", return_value=0.0):
        _detect_subjects(
            photos=photos,
            folders={folder_id: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=True,
            db=db,
        )

    processed = job.get("_detect_processed_ids")
    assert processed and pid in processed, (
        "Reclassify with detect_animals returning None must still mark "
        "the photo for rebuild — its prior detections were cleared and "
        "the classify path needs to know to replace them. Without this, "
        "a post-detect cancel strands the photo with no detections and "
        "no predictions."
    )


def test_classify_photos_stops_when_cancelled(tmp_path):
    """A cancelled job exits the classification loop without inference."""
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    runner = FakeRunner()
    runner.cancelled = True
    job = _make_job()
    clf = MagicMock()

    with patch("classify_job.load_image", MagicMock()):
        raw_results, failed, skipped = _classify_photos(
            photos=[{"id": 1, "filename": "a.jpg", "folder_id": 10,
                     "timestamp": None}],
            folders={10: str(tmp_path)},
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="test-model",
            runner=runner,
            job=job,
            db=MagicMock(),
        )

    assert raw_results == []
    assert failed == 0
    clf.classify_batch.assert_not_called()
    clf.classify_batch_with_embedding.assert_not_called()


def test_classify_photos_drops_pending_batch_on_mid_loop_cancel(tmp_path):
    """A cancel that lands after some photos queued into ``batch`` but
    before it reaches ``_BATCH_SIZE`` must drop the pending batch — not
    fall through to the post-loop flush. Otherwise the job runs classifier
    inference and writes classifier_runs rows for photos the user just
    cancelled."""
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    # Flip cancelled to True after the first photo has been processed
    # (queued into batch, but batch < _BATCH_SIZE so not yet flushed).
    class FlipRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            # First call: start of iteration 0 — let it through.
            # Second call: start of iteration 1 — flip to cancelled.
            return self._calls >= 2

    runner = FlipRunner()
    job = _make_job()
    clf = MagicMock()

    # Two photos, each with a synthetic full-image detection. _BATCH_SIZE
    # is well over 2, so neither flushes mid-loop — the only flush path is
    # the post-loop one, which must be skipped on cancel.
    mock_db = MagicMock()
    mock_db.get_detections.return_value = []
    mock_db.save_detections.return_value = [101]
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10, "timestamp": None},
        {"id": 2, "filename": "b.jpg", "folder_id": 10, "timestamp": None},
    ]
    folders = {10: str(tmp_path)}

    # Make _prepare_image succeed without touching disk.
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, failed, skipped = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="test-model",
            runner=runner,
            job=job,
            db=mock_db,
        )

    # The post-loop flush must be gated on the cancel — classifier inference
    # never runs, and no classifier_runs rows are written for the pending
    # batch contents.
    clf.classify_batch.assert_not_called()
    clf.classify_batch_with_embedding.assert_not_called()
    mock_db.record_classifier_run.assert_not_called()
    assert raw_results == []


def test_weights_download_failure_degrades_to_full_image(tmp_path):
    """A failed MegaDetector weights download (e.g. network down) must
    degrade to full-image classification like any other detection failure,
    not propagate and fail the job — on reclassify runs the purge has
    already happened by then."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    photos = [{"id": 1, "filename": "a.jpg", "folder_id": 10}]
    mock_db = MagicMock()
    mock_db.get_detector_run_photo_ids.return_value = set()  # needs download

    with patch("classify_job.detect_animals", MagicMock()), \
         patch("classify_job.get_primary_detection", MagicMock()), \
         patch("detector.ensure_megadetector_weights",
               side_effect=RuntimeError("network down")):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders={10: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    assert detection_map == {}
    assert detected == 0
    assert any("Detection unavailable" in e for e in job["errors"])


def test_classify_photos_reclassify_clears_predictions_per_photo(tmp_path):
    """For reclassify runs, _classify_photos must clear each photo's old
    predictions immediately before classifying it. The clear sits inside
    the loop so a mid-classify cancel leaves the unprocessed tail's old
    predictions intact, and a detection-setup failure that skips the
    detect loop entirely still has its stale predictions replaced by the
    fallback full-image classifier rather than coexisting with it.

    Verified at the DB API boundary: clear_predictions is called once per
    photo with the right (model, photo_id, fingerprint) triple.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10, "timestamp": None},
        {"id": 2, "filename": "b.jpg", "folder_id": 10, "timestamp": None},
    ]
    folders = {10: str(tmp_path)}

    mock_db = MagicMock()
    mock_db.get_detections.return_value = []
    mock_db.save_detections.return_value = [101]
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.92, "taxonomy": None}], None),
        ([{"species": "Robin", "score": 0.88, "taxonomy": None}], None),
    ]
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        _classify_photos(
            photos=photos,
            folders=folders,
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=mock_db,
            labels_fingerprint="fp-x",
            reclassify=True,
        )

    clear_calls = mock_db.clear_predictions.call_args_list
    assert len(clear_calls) == 2, (
        f"clear_predictions must be called once per photo for reclassify; "
        f"got {len(clear_calls)} calls"
    )
    photo_ids_cleared = {
        call.kwargs["collection_photo_ids"][0] for call in clear_calls
    }
    assert photo_ids_cleared == {1, 2}, (
        f"clear_predictions must target each photo individually; "
        f"got {photo_ids_cleared}"
    )
    for call in clear_calls:
        assert call.kwargs["model"] == "BioCLIP", (
            f"clear_predictions must scope to the model being run; got "
            f"{call.kwargs.get('model')!r}"
        )
        assert call.kwargs["labels_fingerprint"] == "fp-x", (
            "clear_predictions must scope to the labels fingerprint so "
            "shared-folder workspaces with different label sets don't "
            "wipe each other's cache"
        )


def test_classify_photos_no_reclassify_skips_predictions_clear(tmp_path):
    """The per-photo predictions clear must NOT fire for non-reclassify
    runs — otherwise every cached classification would be invalidated on
    every normal pass."""
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [{"id": 1, "filename": "a.jpg", "folder_id": 10, "timestamp": None}]
    folders = {10: str(tmp_path)}

    mock_db = MagicMock()
    mock_db.get_detections.return_value = []
    mock_db.save_detections.return_value = [101]
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.92, "taxonomy": None}], None),
    ]
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        _classify_photos(
            photos=photos,
            folders=folders,
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=mock_db,
            labels_fingerprint="fp-x",
            reclassify=False,
        )

    mock_db.clear_predictions.assert_not_called()


def test_classify_photos_reclassify_preserves_unclassified_tail_on_cancel(tmp_path, monkeypatch):
    """For reclassify runs, cancelling mid-classify must leave the
    unclassified tail's old predictions intact.

    Regression for Codex P1 review on vireo/classify_job.py line 933.
    Before the fix, the per-photo predictions clear ran in the detection
    loop, so every photo's old predictions were wiped before any
    classification happened — a cancel that landed mid-classify stranded
    the tail with cleared predictions and no replacement. The fix moves
    the clear into the classify loop so unreached photos never have it
    fire.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _classify_photos
    from db import Database

    # Hermetic global config (per repo testing conventions).
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid_first = db.add_photo(
        folder_id, "first.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    pid_second = db.add_photo(
        folder_id, "second.jpg", extension=".jpg",
        file_size=100, file_mtime=2.0,
    )
    det_first = db.save_detections(pid_first, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    det_second = db.save_detections(pid_second, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(det_first, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")
    db.add_prediction(det_second, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    class FlipRunner(FakeRunner):
        """Cancel flips on right after the first photo enters the loop."""

        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            # First call: top of iteration 0 → not cancelled (process first).
            # Subsequent calls: top of iteration 1+ → cancelled.
            return self._calls >= 2

    runner = FlipRunner()
    job = _make_job()
    photos = [
        {"id": pid_first, "filename": "first.jpg", "folder_id": folder_id,
         "timestamp": None},
        {"id": pid_second, "filename": "second.jpg", "folder_id": folder_id,
         "timestamp": None},
    ]
    folders = {folder_id: str(tmp_path)}

    detection_map = {
        pid_first: [{"id": det_first}],
        pid_second: [{"id": det_second}],
    }

    from unittest.mock import MagicMock
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.92, "taxonomy": None}], None),
    ]

    # _prepare_image returns a dummy so we never touch real image files;
    # the test is about the clear-call boundary, not classification accuracy.
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=db,
            labels_fingerprint="legacy",
            reclassify=True,
        )

    # The second photo's cached prediction must survive intact: its
    # iteration never started, so the per-photo clear never ran.
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    preds_second = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id = ?",
        (det_second,),
    ).fetchone()["n"]
    assert preds_second == 1, (
        "Mid-classify cancel on a reclassify run wiped the cache of a "
        "photo whose classify-loop iteration never started — the clear "
        "leaked out of the loop body."
    )


def test_classify_photos_full_image_fallback_replaces_stale_predictions_on_reclassify(tmp_path, monkeypatch):
    """When MegaDetector setup fails for a reclassify run, ``_classify_photos``
    runs full-image classification. The stale detector-based predictions
    must be cleared so they don't coexist with the fallback model's output.

    Regression for Codex P2 review on vireo/classify_job.py line 666.
    Before the fix, the per-photo clear lived inside ``_detect_subjects``
    so it was bypassed entirely when the try block raised; the fallback
    full-image classifier then wrote new predictions alongside the
    untouched stale rows.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _classify_photos
    from db import Database

    # Hermetic global config (per repo testing conventions).
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Pretend a prior megadetector run left a detection + prediction
    # behind. The reclassify-with-weights-failure path must not let this
    # prediction linger alongside the new full-image one.
    stale_det = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.4, "h": 0.4},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(stale_det, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    runner = FakeRunner()
    job = _make_job()

    from unittest.mock import MagicMock
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.88, "taxonomy": None}], None),
    ]

    # detection_map is empty (the simulated setup failure produced
    # nothing) → the function takes the full-image synthetic branch.
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, _, _ = _classify_photos(
            photos=[{"id": pid, "filename": "a.jpg", "folder_id": folder_id,
                     "timestamp": None}],
            folders={folder_id: str(tmp_path)},
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=db,
            labels_fingerprint="legacy",
            reclassify=True,
        )

    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    # Stale BioCLIP prediction on the old megadetector detection must
    # be gone. The fallback classifier wrote a new prediction on the
    # synthetic full-image detection.
    stale_preds = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions "
        "WHERE detection_id = ? AND classifier_model = ?",
        (stale_det, "BioCLIP"),
    ).fetchone()["n"]
    assert stale_preds == 0, (
        "Stale detector-based predictions must be cleared by the classify "
        "loop's per-photo purge when reclassify falls back to full-image "
        "(detection setup failure). Found stale rows still attached to "
        f"the old megadetector detection ({stale_det})."
    )
    # Sanity: the fallback path actually produced a result so the test
    # didn't pass vacuously.
    assert len(raw_results) == 1
    assert raw_results[0]["prediction"] == "Sparrow"


def test_classify_photos_reclassify_flushes_pending_batch_on_cancel(tmp_path):
    """For reclassify runs, a mid-loop cancel must still flush the pending
    batch — its queued photos already had their old predictions cleared by
    the per-photo purge at the top of the iteration, so dropping the batch
    strands them with no predictions until a manual rerun.

    Regression for Codex P1 review on vireo/classify_job.py line 1178.
    Before the fix, the post-loop ``if batch and not cancelled`` gate
    dropped the pending batch on every cancel, including reclassify runs
    where each batched photo had a destructive ``clear_predictions`` fire
    just before it was queued.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid_a = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    pid_b = db.add_photo(
        folder_id, "b.jpg", extension=".jpg",
        file_size=100, file_mtime=2.0,
    )
    det_a = db.save_detections(pid_a, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    det_b = db.save_detections(pid_b, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    # Each photo has a prior prediction; the reclassify clear will wipe
    # them per-photo, and the test asserts the new run replaced them
    # rather than leaving them empty after the cancel.
    db.add_prediction(det_a, species="OldA", confidence=0.5,
                      model="BioCLIP", labels_fingerprint="legacy")
    db.add_prediction(det_b, species="OldB", confidence=0.5,
                      model="BioCLIP", labels_fingerprint="legacy")

    class FlipRunner(FakeRunner):
        """Cancel flips on after the second photo enters the loop."""

        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            # Calls 1 + 2: iterations 0 and 1 — let both through so both
            # land in ``batch``. Calls 3+: cancelled, breaking the loop
            # without flushing mid-iteration (batch < _BATCH_SIZE).
            return self._calls >= 3

    runner = FlipRunner()
    job = _make_job()
    photos = [
        {"id": pid_a, "filename": "a.jpg", "folder_id": folder_id,
         "timestamp": None},
        {"id": pid_b, "filename": "b.jpg", "folder_id": folder_id,
         "timestamp": None},
    ]
    folders = {folder_id: str(tmp_path)}

    detection_map = {
        pid_a: [{"id": det_a}],
        pid_b: [{"id": det_b}],
    }

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "NewA", "score": 0.92, "taxonomy": None}], None),
        ([{"species": "NewB", "score": 0.91, "taxonomy": None}], None),
    ]

    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, _, _ = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=db,
            labels_fingerprint="legacy",
            reclassify=True,
        )

    # The pending batch must have flushed: both photos that had their old
    # predictions cleared got new predictions written, so raw_results
    # contains them both.
    assert len(raw_results) == 2, (
        "Reclassify mid-loop cancel must flush the pending batch — its "
        "queued photos already had their old predictions cleared and "
        "would otherwise be stranded empty. "
        f"Got {len(raw_results)} results."
    )
    predictions = {r["photo"]["id"]: r["prediction"] for r in raw_results}
    assert predictions == {pid_a: "NewA", pid_b: "NewB"}


def test_classify_photos_no_reclassify_drops_pending_batch_on_cancel(tmp_path):
    """The non-reclassify path must still drop the pending batch on cancel.

    Without a reclassify, those photos' cached predictions are untouched,
    so honoring the cancel signal here just skips wasted inference (and
    avoids writing classifier_runs rows for photos the user just said
    they're done with).
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    class FlipRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            return self._calls >= 2

    runner = FlipRunner()
    job = _make_job()
    clf = MagicMock()

    mock_db = MagicMock()
    mock_db.get_detections.return_value = []
    mock_db.save_detections.return_value = [101]
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10, "timestamp": None},
        {"id": 2, "filename": "b.jpg", "folder_id": 10, "timestamp": None},
    ]
    folders = {10: str(tmp_path)}

    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, _, _ = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="test-model",
            runner=runner,
            job=job,
            db=mock_db,
            reclassify=False,
        )

    clf.classify_batch.assert_not_called()
    clf.classify_batch_with_embedding.assert_not_called()
    mock_db.record_classifier_run.assert_not_called()
    assert raw_results == []


def test_classify_photos_finish_cleared_only_ignores_cancel(tmp_path):
    """When ``finish_cleared_only=True`` (set by ``run_classify_job`` after a
    post-detect cancel landed on a reclassify run), the classify loop must
    process every photo despite the cancel signal. The photos in this
    subset already had their old detections + cascaded predictions wiped
    during detection; bailing now would strand them empty.

    Also asserts the per-photo ``clear_predictions`` is NOT re-run — the
    cascade in ``_detect_subjects`` already did that, and re-issuing the
    DELETE would just waste a transaction.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    runner = FakeRunner()
    runner.cancelled = True  # post-detect cancel signal already set
    job = _make_job()

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.92, "taxonomy": None}], None),
    ]

    mock_db = MagicMock()
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    photos = [{"id": 1, "filename": "a.jpg", "folder_id": 10,
               "timestamp": None}]
    # Pre-populated detection_map: detection already done.
    detection_map = {
        1: [{"id": 999, "box_x": 0, "box_y": 0, "box_w": 1, "h": 1,
             "box_h": 1, "confidence": 0.9, "category": "animal"}],
    }
    folders = {10: str(tmp_path)}

    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, _, _ = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=mock_db,
            labels_fingerprint="fp-x",
            reclassify=True,
            finish_cleared_only=True,
        )

    # Classification happened despite cancel = True.
    assert len(raw_results) == 1
    assert raw_results[0]["prediction"] == "Sparrow"
    # Per-photo clear must not have re-fired — the detection-loop cascade
    # already handled it.
    mock_db.clear_predictions.assert_not_called()


def test_run_classify_job_reclassify_cancel_after_detect_classifies_processed(tmp_path):
    """End-to-end: a reclassify run cancelled after detection has processed
    some photos must still classify that processed subset. Without this,
    the photos with completed detection would be stranded with new
    detections but no predictions until a manual rerun.

    Regression for Codex P1 review on vireo/classify_job.py line 1824
    (and the related Iu_1R / IubrR / IupmJ findings that flag the same
    half-state for the reclassify path).
    """
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import ClassifyParams, run_classify_job

    # The runner reports "cancelled" only after _detect_subjects returns,
    # mirroring a real user cancel landing during the detection phase but
    # only being observed when run_classify_job rechecks afterward.
    class PostDetectCancelRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.flipped = False

        def is_cancelled(self, job_id):
            return self.flipped

    runner = PostDetectCancelRunner()
    job = _make_job()

    # Mock DB: two photos in the collection, both with prior detections
    # and predictions. The "processed" one will be rebuilt via the
    # cancel-recovery path; the "untouched" one was never reached by
    # detection so it has no entry in detection_map.
    mock_db_instance = MagicMock()
    mock_db_instance.get_collection_photos.return_value = [
        {"id": 1, "filename": "processed.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
        {"id": 2, "filename": "untouched.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T11:00:00"},
    ]
    mock_db_instance.get_folder_tree.return_value = [
        {"id": 10, "path": str(tmp_path), "name": "test"},
    ]
    mock_db_instance.get_existing_prediction_photo_ids.return_value = set()
    mock_db_instance.get_photo_embedding.return_value = None
    mock_db_instance.get_subject_types.return_value = set()
    mock_db_instance.filter_out_subject_tagged.side_effect = (
        lambda pids, _types: list(pids)
    )
    # The classify loop uses these to gate-check; empty returns force
    # full re-classification (which is what reclassify means anyway).
    mock_db_instance.get_classifier_run_keys.return_value = set()
    mock_db_instance.get_predictions_for_detection.return_value = []
    mock_db_instance.get_detections.return_value = []

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }
    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "NewProcessed", "score": 0.95, "taxonomy": None}]
    mock_clf = MagicMock()
    mock_clf.classify_with_embedding.return_value = (fake_preds, fake_embedding)
    mock_clf.classify_batch_with_embedding.return_value = [
        (fake_preds, fake_embedding)
    ]

    # Stub _detect_subjects: only photo 1 ("processed") completed
    # detection. Flip cancel on as it returns.
    detect_called = {"n": 0}

    def fake_detect_subjects(photos, folders, runner, job, reclassify, db):
        detect_called["n"] += 1
        assert reclassify is True
        runner.flipped = True
        # detection_map only includes the processed photo; the untouched
        # one was never reached (mid-detection cancel).
        return ({1: [{"id": 101, "box_x": 0, "box_y": 0,
                      "box_w": 0.4, "box_h": 0.4, "confidence": 0.85,
                      "category": "animal",
                      "detector_model": "megadetector-v6"}]}, 1)

    params = ClassifyParams(
        collection_id="col-1",
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name=None,
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=True,
    )

    fake_img = MagicMock()
    with patch("classify_job.Database", return_value=mock_db_instance), \
         patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch("classify_job._load_labels",
               return_value=(["NewProcessed"], False)), \
         patch("classify_job.Classifier", return_value=mock_clf), \
         patch("classify_job._detect_subjects",
               side_effect=fake_detect_subjects), \
         patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        result = run_classify_job(job, runner, str(tmp_path / "test.db"),
                                  1, params)

    assert detect_called["n"] == 1, "_detect_subjects must have been called"
    # The processed photo's classification was rebuilt despite the cancel —
    # add_prediction was called for it via finalization.
    assert mock_db_instance.add_prediction.call_count >= 1, (
        "Post-detect cancel on reclassify with non-empty detection_map "
        "must classify the processed subset and store predictions. "
        "add_prediction was never called — the recovery path bailed "
        "instead of classifying."
    )
    # All add_prediction calls must be for the processed photo (id 1).
    # The untouched photo (id 2) is not in detection_map and must not
    # be touched by the recovery path.
    for call in mock_db_instance.add_prediction.call_args_list:
        # add_prediction(detection_id, species=..., ...) — detection_id is
        # the first positional arg. Photo 1's stub detection id is 101.
        det_id = call.args[0] if call.args else call.kwargs.get("detection_id")
        assert det_id == 101, (
            f"Recovery path wrote a prediction for unexpected detection "
            f"{det_id}; only the processed subset should be classified."
        )
    assert result["detected"] == 1


def test_run_classify_job_reclassify_cancel_classifies_empty_scene_processed(tmp_path):
    """End-to-end: a reclassify run cancelled after detection must still
    classify photos that were re-detected as empty scenes (no detections
    in detection_map). Their old detections+predictions were already
    cascaded away by the per-photo ``clear_detections`` call in
    ``_detect_subjects``; without a full-image fallback classify pass they
    would be stranded with cleared predictions and no replacement.

    Regression for the Codex P1 review on commit faa47a43ad
    (vireo/classify_job.py line 1861 — "Track re-detected empty photos
    before returning"). The gate uses ``job["_detect_processed_ids"]``
    rather than ``detection_map.keys()`` so empty-scene photos are
    included in the rebuild subset.
    """
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import ClassifyParams, run_classify_job

    class PostDetectCancelRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.flipped = False

        def is_cancelled(self, job_id):
            return self.flipped

    runner = PostDetectCancelRunner()
    job = _make_job()

    mock_db_instance = MagicMock()
    # Two photos: photo 1 was re-detected as empty (state mutated),
    # photo 2 was never reached.
    mock_db_instance.get_collection_photos.return_value = [
        {"id": 1, "filename": "empty.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
        {"id": 2, "filename": "untouched.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T11:00:00"},
    ]
    mock_db_instance.get_folder_tree.return_value = [
        {"id": 10, "path": str(tmp_path), "name": "test"},
    ]
    mock_db_instance.get_existing_prediction_photo_ids.return_value = set()
    mock_db_instance.get_photo_embedding.return_value = None
    mock_db_instance.get_subject_types.return_value = set()
    mock_db_instance.filter_out_subject_tagged.side_effect = (
        lambda pids, _types: list(pids)
    )
    mock_db_instance.get_classifier_run_keys.return_value = set()
    mock_db_instance.get_predictions_for_detection.return_value = []
    # No prior full-image synthetic detection exists for photo 1 — the
    # classifier creates one for the full-image fallback path.
    mock_db_instance.get_detections.return_value = []
    # save_detections returns a synthetic detection id for the full-image
    # fallback that _classify_photos creates for empty-scene photos.
    mock_db_instance.save_detections.return_value = [201]

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }
    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "NewEmpty", "score": 0.95, "taxonomy": None}]
    mock_clf = MagicMock()
    mock_clf.classify_with_embedding.return_value = (fake_preds, fake_embedding)
    mock_clf.classify_batch_with_embedding.return_value = [
        (fake_preds, fake_embedding)
    ]

    # Stub _detect_subjects: photo 1 was processed but found empty
    # (recorded a detector_runs row, no detection_map entry). Photo 2 was
    # never reached. The processed set is stashed on the job so
    # run_classify_job's rebuild gate can include the empty-scene photo
    # — using detection_map.keys() alone would miss it.
    detect_called = {"n": 0}

    def fake_detect_subjects(photos, folders, runner, job, reclassify, db):
        detect_called["n"] += 1
        assert reclassify is True
        # Mirror what production _detect_subjects does: stash the processed
        # set on job for run_classify_job to consume on cancel.
        job["_detect_processed_ids"] = {1}
        runner.flipped = True
        return ({}, 0)

    params = ClassifyParams(
        collection_id="col-1",
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name=None,
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=True,
    )

    fake_img = MagicMock()
    with patch("classify_job.Database", return_value=mock_db_instance), \
         patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch("classify_job._load_labels",
               return_value=(["NewEmpty"], False)), \
         patch("classify_job.Classifier", return_value=mock_clf), \
         patch("classify_job._detect_subjects",
               side_effect=fake_detect_subjects), \
         patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        result = run_classify_job(job, runner, str(tmp_path / "test.db"),
                                  1, params)

    assert detect_called["n"] == 1, "_detect_subjects must have been called"
    # The empty-scene photo (id 1) had its predictions cascaded away by
    # _detect_subjects.clear_detections; the recovery path must classify
    # it via the full-image fallback so it doesn't end up empty.
    assert mock_db_instance.add_prediction.call_count >= 1, (
        "Post-detect cancel on reclassify with an empty-scene processed "
        "photo must classify it via the full-image fallback. Without the "
        "_detect_processed_ids tracking, the photo is stranded with "
        "cleared predictions and no replacement."
    )
    # All add_prediction calls must be for the empty-scene photo's
    # synthetic full-image detection (id 201). Photo 2 was never
    # processed and must not be touched.
    for call in mock_db_instance.add_prediction.call_args_list:
        det_id = call.args[0] if call.args else call.kwargs.get("detection_id")
        assert det_id == 201, (
            f"Recovery path wrote a prediction for unexpected detection "
            f"{det_id}; only the processed empty-scene photo's synthetic "
            f"full-image detection (201) should be classified."
        )
