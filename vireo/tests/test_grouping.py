# vireo/tests/test_grouping.py
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


class MockTag:
    """Mock exifread IfdTag with str() returning the value."""

    def __init__(self, value):
        self.values = value

    def __str__(self):
        return self.values


def test_read_exif_timestamp_exifread_fallback(monkeypatch, tmp_path):
    """When Pillow fails (e.g. RAW file), exifread is used as fallback."""
    from grouping import read_exif_timestamp

    # Create a dummy file so the path exists
    raw_file = tmp_path / "DSC_1074.NEF"
    raw_file.write_bytes(b"\x00" * 100)

    # Make Pillow's Image.open raise an exception (simulating RAW file failure)
    import PIL.Image
    original_open = PIL.Image.open

    def failing_open(path):
        raise Exception("cannot identify image file")

    monkeypatch.setattr(PIL.Image, "open", failing_open)

    # Mock exifread.process_file to return EXIF data
    import exifread
    def mock_process_file(f, **kwargs):
        return {"EXIF DateTimeOriginal": MockTag("2024:06:15 14:30:00")}

    monkeypatch.setattr(exifread, "process_file", mock_process_file)

    result = read_exif_timestamp(str(raw_file))
    assert result == datetime(2024, 6, 15, 14, 30, 0)


def test_read_exif_timestamp_both_fail(monkeypatch, tmp_path):
    """Returns None when both Pillow and exifread fail."""
    from grouping import read_exif_timestamp

    raw_file = tmp_path / "DSC_1075.NEF"
    raw_file.write_bytes(b"\x00" * 100)

    # Make Pillow fail
    import PIL.Image

    def failing_open(path):
        raise Exception("cannot identify image file")

    monkeypatch.setattr(PIL.Image, "open", failing_open)

    # Make exifread return empty dict (no tags)
    import exifread

    def mock_process_file(f, **kwargs):
        return {}

    monkeypatch.setattr(exifread, "process_file", mock_process_file)

    result = read_exif_timestamp(str(raw_file))
    assert result is None


def test_read_exif_timestamp_pillow_works(monkeypatch, tmp_path):
    """Pillow path still works for JPEG files (no fallback needed)."""
    from grouping import read_exif_timestamp

    jpg_file = tmp_path / "DSC_0001.jpg"
    jpg_file.write_bytes(b"\x00" * 100)

    # Mock Pillow to succeed with EXIF data
    import PIL.Image
    from PIL.ExifTags import Base as ExifBase

    class MockExif(dict):
        pass

    class MockImg:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def getexif(self):
            exif = MockExif()
            exif[ExifBase.DateTimeOriginal] = "2024:01:10 08:00:00"
            return exif

    def mock_open(path):
        return MockImg()

    monkeypatch.setattr(PIL.Image, "open", mock_open)

    result = read_exif_timestamp(str(jpg_file))
    assert result == datetime(2024, 1, 10, 8, 0, 0)


def test_read_exif_timestamp_exifread_digitized_fallback(monkeypatch, tmp_path):
    """exifread fallback uses DateTimeDigitized when DateTimeOriginal is missing."""
    from grouping import read_exif_timestamp

    raw_file = tmp_path / "DSC_1076.CR2"
    raw_file.write_bytes(b"\x00" * 100)

    import PIL.Image

    def failing_open(path):
        raise Exception("cannot identify image file")

    monkeypatch.setattr(PIL.Image, "open", failing_open)

    import exifread

    def mock_process_file(f, **kwargs):
        return {"EXIF DateTimeDigitized": MockTag("2024:07:20 09:15:30")}

    monkeypatch.setattr(exifread, "process_file", mock_process_file)

    result = read_exif_timestamp(str(raw_file))
    assert result == datetime(2024, 7, 20, 9, 15, 30)
