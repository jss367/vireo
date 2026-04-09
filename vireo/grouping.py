"""Group sequential photos by EXIF timestamp proximity and visual similarity."""

import logging
from collections import Counter
from datetime import datetime

import numpy as np

log = logging.getLogger(__name__)


def group_by_timestamp(photos, window_seconds=10):
    """Group sequential photos that were taken within a time window.

    Sorts by timestamp first, then walks sequentially grouping photos
    within the window.

    Args:
        photos: list of dicts, each with 'filename' and 'timestamp' (datetime or None)
        window_seconds: max seconds between consecutive photos to group them

    Returns:
        list of groups, where each group is a list of photo dicts
    """
    if not photos:
        return []

    # Sort by timestamp (None timestamps go to the end)
    photos = sorted(
        photos,
        key=lambda p: p["timestamp"] if p["timestamp"] is not None else datetime.max,
    )

    groups = []
    current_group = [photos[0]]

    for i in range(1, len(photos)):
        prev = photos[i - 1]
        curr = photos[i]

        # If either has no timestamp, start a new group
        if prev["timestamp"] is None or curr["timestamp"] is None:
            groups.append(current_group)
            current_group = [curr]
            continue

        delta = abs((curr["timestamp"] - prev["timestamp"]).total_seconds())
        if delta <= window_seconds:
            current_group.append(curr)
        else:
            groups.append(current_group)
            current_group = [curr]

    groups.append(current_group)
    return groups


def refine_groups_by_similarity(groups, similarity_threshold=0.85):
    """Split time-based groups using cosine similarity of image embeddings.

    For each photo in a time-based group, checks if it's similar to ANY photo
    already in the current subgroup (not just the previous one). This handles
    cases where you briefly pan away and come back to the same subject —
    the returning photos still match the earlier ones in the group.

    Photos without embeddings are kept with their neighbors (benefit of the doubt).

    Args:
        groups: list of groups from group_by_timestamp. Each photo dict
                must have an 'embedding' key (numpy array or None).
        similarity_threshold: minimum cosine similarity to stay in same group (0-1)

    Returns:
        list of refined groups
    """
    refined = []

    for group in groups:
        if len(group) < 2:
            refined.append(group)
            continue

        # Build subgroups: each photo joins the first subgroup it's similar to,
        # or starts a new subgroup if it doesn't match any
        subgroups = [[group[0]]]

        for i in range(1, len(group)):
            curr_emb = group[i].get("embedding")
            placed = False

            if curr_emb is not None:
                # Check against each existing subgroup
                for sg in subgroups:
                    # Check if similar to any member of this subgroup
                    for member in sg:
                        mem_emb = member.get("embedding")
                        if mem_emb is not None:
                            sim = float(np.dot(curr_emb, mem_emb))
                            if sim >= similarity_threshold:
                                sg.append(group[i])
                                placed = True
                                break
                    if placed:
                        break

            if not placed:
                if curr_emb is None:
                    # No embedding — keep with the last subgroup (benefit of the doubt)
                    subgroups[-1].append(group[i])
                else:
                    # Doesn't match any existing subgroup — start a new one
                    log.debug(
                        "New subgroup at %s (no match >= %.3f)",
                        group[i]["filename"],
                        similarity_threshold,
                    )
                    subgroups.append([group[i]])

        refined.extend(subgroups)

    split_count = len(refined) - len(groups)
    if split_count > 0:
        log.info(
            "Similarity refinement: %d time-based groups → %d groups (%d splits)",
            len(groups),
            len(refined),
            split_count,
        )

    return refined


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
    if not predictions:
        return None
    counts = Counter(p["prediction"] for p in predictions)
    individual = dict(counts)

    # Group confidences by prediction
    conf_by_pred = {}
    for p in predictions:
        conf_by_pred.setdefault(p["prediction"], []).append(p["confidence"])

    # Pick by total confidence weight (count × avg_confidence), with count as tiebreaker
    best = max(
        counts.keys(),
        key=lambda sp: (sum(conf_by_pred[sp]), counts[sp]),
    )

    avg_conf = sum(conf_by_pred[best]) / len(conf_by_pred[best])

    return {
        "prediction": best,
        "confidence": round(avg_conf, 4),
        "vote_count": counts[best],
        "total_votes": len(predictions),
        "individual_predictions": individual,
    }


def read_exif_timestamp(image_path):
    """Read EXIF DateTimeOriginal from an image file.

    Tries Pillow first (works for JPEG/TIFF), then falls back to exifread
    which handles RAW formats (NEF, CR2, CR3, ARW, etc.).

    Args:
        image_path: path to JPEG/TIFF/RAW file

    Returns:
        datetime or None if not available
    """
    from datetime import datetime

    from PIL import Image
    from PIL.ExifTags import Base as ExifBase

    # Try Pillow first (fast path for JPEG/TIFF)
    try:
        with Image.open(str(image_path)) as img:
            exif = img.getexif()
            if exif:
                dt_str = exif.get(ExifBase.DateTimeOriginal) or exif.get(
                    ExifBase.DateTimeDigitized
                )
                if dt_str:
                    return datetime.strptime(dt_str, "%Y:%m:%d %H:%M:%S")
        # Pillow opened the file but found no timestamp — no point trying exifread
        return None
    except Exception:
        pass

    # Pillow failed (likely a RAW file) — fall back to exifread
    try:
        import exifread

        with open(str(image_path), "rb") as f:
            tags = exifread.process_file(f, details=False)
        if tags:
            tag = (
                tags.get("EXIF DateTimeOriginal")
                or tags.get("EXIF DateTimeDigitized")
                or tags.get("Image DateTimeOriginal")
                or tags.get("Image DateTime")
            )
            if tag:
                return datetime.strptime(str(tag), "%Y:%m:%d %H:%M:%S")
    except Exception:
        pass

    log.warning("Could not read EXIF from %s", image_path)
    return None
