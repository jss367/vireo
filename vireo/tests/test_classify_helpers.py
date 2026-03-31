"""Tests for classify_job helper extraction."""

from unittest.mock import MagicMock, patch

from classify_job import _detect_batch


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
        detection_map, detected = _detect_batch(
            photos, folders, mock_runner, mock_job, reclassify=False, db=mock_db,
        )

    assert isinstance(detection_map, dict)
    assert isinstance(detected, int)


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

    detection_map, detected = _detect_batch(
        photos, folders, mock_runner, mock_job, reclassify=False, db=mock_db,
        already_detected_ids={1},
    )

    assert 1 in detection_map
    assert len(detection_map[1]) == 1
    assert detection_map[1][0]["box_x"] == 0.1
    assert detected == 1
