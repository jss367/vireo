"""Sharpness scoring for burst group culling.

Uses Laplacian variance to measure image focus quality.
Higher score = sharper image.
"""

import logging
import os

import numpy as np
from image_loader import load_image
from PIL import ImageFilter

log = logging.getLogger(__name__)


def compute_sharpness(image_path, region=None):
    """Compute a sharpness score for an image using Laplacian variance.

    Args:
        image_path: path to the image file
        region: optional (x, y, w, h) tuple to score a specific region

    Returns:
        float sharpness score (higher = sharper), or None on failure
    """
    img = load_image(str(image_path), max_size=1024)
    if img is None:
        return None

    # Convert to grayscale
    gray = img.convert("L")

    # Crop to region if specified
    if region:
        x, y, w, h = region
        gray = gray.crop((x, y, x + w, y + h))

    # Apply Laplacian filter (edge detection)
    laplacian = gray.filter(
        ImageFilter.Kernel(
            size=(3, 3),
            kernel=[0, 1, 0, 1, -4, 1, 0, 1, 0],
            scale=1,
            offset=128,
        )
    )

    # Variance of the Laplacian — higher = more edges = sharper
    arr = np.array(laplacian, dtype=np.float64)
    score = float(np.var(arr))

    return round(score, 2)


def score_burst_group(photo_paths):
    """Score all photos in a burst group and rank them.

    Args:
        photo_paths: list of (photo_id, image_path) tuples

    Returns:
        list of {photo_id, path, sharpness, rank, is_best, is_worst}
        sorted by sharpness descending (best first)
    """
    results = []
    for photo_id, path in photo_paths:
        score = compute_sharpness(path)
        results.append(
            {
                "photo_id": photo_id,
                "path": path,
                "sharpness": score if score is not None else 0,
            }
        )

    # Sort by sharpness descending
    results.sort(key=lambda x: x["sharpness"], reverse=True)

    # Assign ranks and best/worst flags
    for i, r in enumerate(results):
        r["rank"] = i + 1
        r["is_best"] = i == 0
        r["is_worst"] = (i == len(results) - 1) and len(results) > 1

    return results


def score_collection_photos(db, collection_id, progress_callback=None):
    """Score all photos in a collection, grouping bursts and ranking within each.

    Args:
        db: Database instance
        collection_id: collection to score (or None for all photos)
        progress_callback: optional callable(current, total, message)

    Returns:
        dict with scored_count, group_count, results (list of scored photos)
    """
    from datetime import datetime

    from grouping import group_by_timestamp, refine_groups_by_similarity

    if collection_id:
        photos = db.get_collection_photos(collection_id, per_page=999999)
    else:
        photos = db.get_photos(per_page=999999)

    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    total = len(photos)

    # Build photo list with timestamps and stored embeddings for grouping
    photo_list = []
    for p in photos:
        folder_path = folders.get(p["folder_id"], "")
        image_path = os.path.join(folder_path, p["filename"])
        timestamp = None
        if p["timestamp"]:
            try:
                timestamp = datetime.fromisoformat(p["timestamp"])
            except Exception:
                pass
        # Load stored embedding if available
        embedding = None
        emb_blob = p["embedding"] if "embedding" in p.keys() else None
        if emb_blob:
            embedding = np.frombuffer(emb_blob, dtype=np.float32)
        photo_list.append(
            {
                "photo_id": p["id"],
                "path": image_path,
                "filename": p["filename"],
                "timestamp": timestamp,
                "embedding": embedding,
            }
        )

    # Group by timestamp, then refine using visual similarity
    groups = group_by_timestamp(photo_list, window_seconds=10)
    groups = refine_groups_by_similarity(groups)

    all_results = []
    scored = 0
    group_count = 0

    for gi, group in enumerate(groups):
        if progress_callback:
            progress_callback(scored, total, f"Scoring group {gi + 1}/{len(groups)}...")

        if len(group) < 2:
            # Single photo — score it but no ranking
            item = group[0]
            score = compute_sharpness(item["path"])
            all_results.append(
                {
                    "photo_id": item["photo_id"],
                    "filename": item["filename"],
                    "sharpness": score if score is not None else 0,
                    "rank": 1,
                    "group_size": 1,
                    "group_id": None,
                    "is_best": False,
                    "is_worst": False,
                }
            )
            scored += 1
            continue

        # Multi-photo group — score and rank
        group_count += 1
        gid = f"burst-{gi + 1:04d}"
        paths = [(item["photo_id"], item["path"]) for item in group]
        ranked = score_burst_group(paths)

        for r in ranked:
            item = next(g for g in group if g["photo_id"] == r["photo_id"])
            all_results.append(
                {
                    "photo_id": r["photo_id"],
                    "filename": item["filename"],
                    "sharpness": r["sharpness"],
                    "rank": r["rank"],
                    "group_size": len(group),
                    "group_id": gid,
                    "is_best": r["is_best"],
                    "is_worst": r["is_worst"],
                }
            )
            scored += 1

    if progress_callback:
        progress_callback(total, total, "Done")

    log.info(
        "Sharpness scoring: %d photos scored, %d burst groups", scored, group_count
    )
    return {
        "scored_count": scored,
        "group_count": group_count,
        "results": all_results,
    }
