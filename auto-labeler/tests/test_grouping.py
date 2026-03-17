# auto-labeler/tests/test_grouping.py
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_group_by_timestamp_basic():
    """Sequential photos within time window are grouped together."""
    from grouping import group_by_timestamp

    photos = [
        {'filename': 'DSC_0001.jpg', 'timestamp': datetime(2019, 3, 17, 10, 0, 0)},
        {'filename': 'DSC_0002.jpg', 'timestamp': datetime(2019, 3, 17, 10, 0, 3)},
        {'filename': 'DSC_0003.jpg', 'timestamp': datetime(2019, 3, 17, 10, 0, 5)},
        {'filename': 'DSC_0004.jpg', 'timestamp': datetime(2019, 3, 17, 10, 5, 0)},  # 5 min gap
    ]

    groups = group_by_timestamp(photos, window_seconds=10)
    assert len(groups) == 2
    assert len(groups[0]) == 3  # first 3 photos
    assert len(groups[1]) == 1  # DSC_0004 alone


def test_group_by_timestamp_all_separate():
    """Photos far apart in time form individual groups."""
    from grouping import group_by_timestamp

    photos = [
        {'filename': 'DSC_0001.jpg', 'timestamp': datetime(2019, 3, 17, 10, 0, 0)},
        {'filename': 'DSC_0002.jpg', 'timestamp': datetime(2019, 3, 17, 10, 1, 0)},
        {'filename': 'DSC_0003.jpg', 'timestamp': datetime(2019, 3, 17, 10, 2, 0)},
    ]

    groups = group_by_timestamp(photos, window_seconds=10)
    assert len(groups) == 3
    assert all(len(g) == 1 for g in groups)


def test_group_by_timestamp_no_timestamp():
    """Photos without timestamps each form their own group."""
    from grouping import group_by_timestamp

    photos = [
        {'filename': 'DSC_0001.jpg', 'timestamp': None},
        {'filename': 'DSC_0002.jpg', 'timestamp': None},
    ]

    groups = group_by_timestamp(photos, window_seconds=10)
    assert len(groups) == 2


def test_consensus_prediction():
    """consensus_prediction returns the most common prediction with averaged confidence."""
    from grouping import consensus_prediction

    predictions = [
        {'prediction': 'Song sparrow', 'confidence': 0.80},
        {'prediction': 'Song sparrow', 'confidence': 0.90},
        {'prediction': 'Lincoln sparrow', 'confidence': 0.60},
    ]

    result = consensus_prediction(predictions)
    assert result['prediction'] == 'Song sparrow'
    assert result['confidence'] == 0.85  # average of 0.80 and 0.90
    assert result['vote_count'] == 2
    assert result['total_votes'] == 3
    assert result['individual_predictions'] == {'Song sparrow': 2, 'Lincoln sparrow': 1}


def test_consensus_prediction_tie():
    """When tied, consensus picks the one with higher average confidence."""
    from grouping import consensus_prediction

    predictions = [
        {'prediction': 'Song sparrow', 'confidence': 0.60},
        {'prediction': 'Lincoln sparrow', 'confidence': 0.90},
    ]

    result = consensus_prediction(predictions)
    assert result['prediction'] == 'Lincoln sparrow'
    assert result['confidence'] == 0.90


def test_consensus_prediction_single():
    """Single prediction returns it directly."""
    from grouping import consensus_prediction

    predictions = [
        {'prediction': 'Song sparrow', 'confidence': 0.85},
    ]

    result = consensus_prediction(predictions)
    assert result['prediction'] == 'Song sparrow'
    assert result['confidence'] == 0.85
    assert result['vote_count'] == 1
    assert result['total_votes'] == 1
