"""Tests for classify_job helper extraction."""

from unittest.mock import MagicMock, patch

from classify_job import _detect_batch


def test_detect_batch_returns_detection_map():
    """_detect_batch should return a dict mapping photo_id to detection."""
    photos = [
        {"id": 1, "folder_id": 10, "filename": "a.jpg", "detection_box": None, "detection_conf": None},
        {"id": 2, "folder_id": 10, "filename": "b.jpg", "detection_box": None, "detection_conf": None},
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
    """_detect_batch should reuse existing detection_box when not reclassifying."""
    import json

    box = {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}
    photos = [
        {"id": 1, "folder_id": 10, "filename": "a.jpg",
         "detection_box": json.dumps(box), "detection_conf": 0.95},
    ]
    folders = {10: "/photos"}

    mock_db = MagicMock()
    mock_runner = MagicMock()
    mock_job = {"id": "test-1", "progress": {}, "errors": [], "_start_time": 1.0}

    detection_map, detected = _detect_batch(
        photos, folders, mock_runner, mock_job, reclassify=False, db=mock_db,
    )

    assert 1 in detection_map
    assert detection_map[1]["box"] == box
    assert detected == 1
