"""Tests for classify_job helper extraction."""

from unittest.mock import MagicMock, patch

from classify_job import _detect_batch, _match_stats


def test_detect_batch_returns_detection_map():
    """_detect_batch should return a dict mapping photo_id to detection."""
    photos = [
        {"id": 1, "folder_id": 10, "filename": "a.jpg"},
        {"id": 2, "folder_id": 10, "filename": "b.jpg"},
    ]
    folders = {10: "/photos"}

    mock_db = MagicMock()
    mock_runner = MagicMock()
    mock_job = {"id": "test-1", "progress": {}, "errors": [], "_start_time": 1.0}

    with patch("classify_job.detect_animals", return_value=[]):
        detection_map, detected, processed_ids = _detect_batch(
            photos, folders, mock_runner, mock_job, reclassify=False, db=mock_db,
        )

    assert isinstance(detection_map, dict)
    assert isinstance(detected, int)
    assert isinstance(processed_ids, set)


def test_detect_batch_uses_cached_detection():
    """_detect_batch should reuse existing detections from the database when not reclassifying."""
    photos = [
        {"id": 1, "folder_id": 10, "filename": "a.jpg"},
    ]
    folders = {10: "/photos"}

    mock_db = MagicMock()
    # Mock get_detections to return existing detection data from detections table
    mock_db.get_detections.return_value = [
        {"id": 42, "box_x": 0.1, "box_y": 0.2, "box_w": 0.3, "box_h": 0.4,
         "detector_confidence": 0.95, "category": "animal"},
    ]
    mock_runner = MagicMock()
    mock_job = {"id": "test-1", "progress": {}, "errors": [], "_start_time": 1.0}

    detection_map, detected, processed_ids = _detect_batch(
        photos, folders, mock_runner, mock_job, reclassify=False, db=mock_db,
        already_detected_ids={1},
    )

    assert 1 in detection_map
    assert len(detection_map[1]) == 1
    assert detection_map[1][0]["box_x"] == 0.1
    assert detected == 1
    assert 1 in processed_ids


def test_match_stats_normalizes_top_species_apostrophe():
    """``add_prediction`` folds curly apostrophes in ``species`` before
    storing the row, but the run summary used to record whatever spelling
    the classifier returned. The calibration query compares
    ``classifier_match_scores.top_species`` both to the normalized keyword
    and to the normalized prediction row before looking up its taxon ID, so
    an un-folded curly-apostrophe label would miss both sides of the join
    and drop a confirmed-correct run into the ``incorrect`` bucket, biasing
    the fitted floor. Fold ``top_species`` through the same rule as the
    prediction so both sides of the join agree.
    """
    stats = _match_stats(
        [
            {"species": "Swinhoe’s White-eye", "raw_score": 0.42},
            {"species": "Zebra Finch", "raw_score": 0.11},
        ],
        model_type="timm",
    )
    assert stats is not None
    assert stats["top_species"] == "Swinhoe's White-eye"


def test_match_stats_handles_missing_top_species():
    """``_folded_species_key(None)`` returns ``None`` so a classifier that
    reports no species keeps the ``None`` signal instead of collapsing to
    the empty string that a fold would produce.
    """
    stats = _match_stats(
        [{"species": None, "raw_score": 0.5}],
        model_type="timm",
    )
    assert stats is not None
    assert stats["top_species"] is None
