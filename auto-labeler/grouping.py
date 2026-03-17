"""Group sequential photos by EXIF timestamp proximity."""

import logging
from collections import Counter

log = logging.getLogger(__name__)


def group_by_timestamp(photos, window_seconds=10):
    """Group sequential photos that were taken within a time window.

    Args:
        photos: list of dicts, each with 'filename' and 'timestamp' (datetime or None)
        window_seconds: max seconds between consecutive photos to group them

    Returns:
        list of groups, where each group is a list of photo dicts
    """
    if not photos:
        return []

    groups = []
    current_group = [photos[0]]

    for i in range(1, len(photos)):
        prev = photos[i - 1]
        curr = photos[i]

        # If either has no timestamp, start a new group
        if prev['timestamp'] is None or curr['timestamp'] is None:
            groups.append(current_group)
            current_group = [curr]
            continue

        delta = abs((curr['timestamp'] - prev['timestamp']).total_seconds())
        if delta <= window_seconds:
            current_group.append(curr)
        else:
            groups.append(current_group)
            current_group = [curr]

    groups.append(current_group)
    return groups


def consensus_prediction(predictions):
    """Compute a consensus prediction from multiple individual predictions.

    Args:
        predictions: list of dicts with 'prediction' (str) and 'confidence' (float)

    Returns:
        dict with:
            prediction: the winning species name
            confidence: average confidence of the winning predictions
            vote_count: number of frames agreeing with the consensus
            total_votes: total number of frames
            individual_predictions: dict of species -> count
    """
    counts = Counter(p['prediction'] for p in predictions)
    individual = dict(counts)

    # Group confidences by prediction
    conf_by_pred = {}
    for p in predictions:
        conf_by_pred.setdefault(p['prediction'], []).append(p['confidence'])

    # Pick the most common; break ties by higher average confidence
    best = max(
        counts.keys(),
        key=lambda sp: (counts[sp], sum(conf_by_pred[sp]) / len(conf_by_pred[sp])),
    )

    avg_conf = sum(conf_by_pred[best]) / len(conf_by_pred[best])

    return {
        'prediction': best,
        'confidence': round(avg_conf, 4),
        'vote_count': counts[best],
        'total_votes': len(predictions),
        'individual_predictions': individual,
    }


def read_exif_timestamp(image_path):
    """Read EXIF DateTimeOriginal from an image file.

    Args:
        image_path: path to JPEG/TIFF/RAW file

    Returns:
        datetime or None if not available
    """
    from datetime import datetime
    from PIL import Image
    from PIL.ExifTags import Base as ExifBase

    try:
        img = Image.open(str(image_path))
        exif = img.getexif()
        if not exif:
            return None

        # DateTimeOriginal tag
        dt_str = exif.get(ExifBase.DateTimeOriginal) or exif.get(ExifBase.DateTimeDigitized)
        if dt_str:
            return datetime.strptime(dt_str, "%Y:%m:%d %H:%M:%S")
    except Exception:
        log.debug("Could not read EXIF from %s", image_path)

    return None
