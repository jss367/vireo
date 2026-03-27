"""Culling engine: find redundant photos and suggest keepers.

The culling pipeline:
1. Group photos by species (from predictions)
2. Within each species, group by scene (timestamp proximity + perceptual hash similarity)
3. Within each scene, cluster by embedding redundancy (~0.88)
4. For each redundancy cluster, pick the best quality photo as keeper
5. Everything else becomes a reject candidate
"""

import logging
from datetime import datetime

import imagehash
import numpy as np
from PIL import Image

log = logging.getLogger(__name__)


def analyze_for_culling(
    db,
    collection_id=None,
    redundancy_threshold=None,
    separate_file_types=True,
    time_window=None,
    phash_threshold=None,
    cross_bucket_merge=False,
    progress_callback=None,
):
    """Run full culling analysis on a set of photos.

    Args:
        db: Database instance
        collection_id: optional collection to scope to (None = all photos)
        redundancy_threshold: similarity threshold for redundancy (higher = stricter)
        separate_file_types: whether to separate RAW from non-RAW
        time_window: max seconds gap for time bucketing (None = read from config)
        phash_threshold: max Hamming distance for "same scene" (None = read from config)
        cross_bucket_merge: whether to merge time buckets by pHash similarity
        progress_callback: optional callable(message) for status updates

    Returns:
        dict with:
            species_groups: list of species group dicts
            total_photos: int
            suggested_keepers: int
            suggested_rejects: int
    """
    import config as cfg
    user_cfg = cfg.load()
    if redundancy_threshold is None:
        redundancy_threshold = user_cfg.get("redundancy_threshold", 0.88)
    if time_window is None:
        time_window = user_cfg.get("cull_time_window", 60)
    if phash_threshold is None:
        phash_threshold = user_cfg.get("cull_phash_threshold", 19)

    # Get photos with predictions and embeddings
    if collection_id:
        photos = db.get_collection_photos(collection_id, per_page=999999)
    else:
        photos = db.get_photos(per_page=999999)

    photo_ids = [p["id"] for p in photos]
    if not photo_ids:
        return {"species_groups": [], "total_photos": 0, "suggested_keepers": 0, "suggested_rejects": 0}

    # Load predictions
    predictions = {}
    for pid in photo_ids:
        pred = db.conn.execute(
            "SELECT species, confidence FROM predictions WHERE photo_id = ?",
            (pid,),
        ).fetchone()
        if pred:
            predictions[pid] = {
                "species": pred["species"],
                "confidence": pred["confidence"],
            }

    # Load embeddings, quality scores, file extensions, timestamps, phashes, and filenames
    embeddings = {}
    quality = {}
    extensions = {}
    timestamps = {}
    phashes = {}
    filenames = {}
    missing_phash = []
    for pid in photo_ids:
        row = db.conn.execute(
            "SELECT embedding, quality_score, sharpness, subject_sharpness, extension, timestamp, phash, filename FROM photos WHERE id = ?",
            (pid,),
        ).fetchone()
        if row and row["embedding"]:
            embeddings[pid] = np.frombuffer(row["embedding"], dtype=np.float32)
        q = row["quality_score"] or 0 if row else 0
        if q == 0 and row and row["sharpness"]:
            q = row["sharpness"] / 1000.0
        quality[pid] = q
        extensions[pid] = (row["extension"] or "").lower() if row else ""
        filenames[pid] = row["filename"] if row else ""
        if row and row["timestamp"]:
            try:
                timestamps[pid] = datetime.fromisoformat(row["timestamp"])
            except (ValueError, TypeError):
                pass
        if row and row["phash"]:
            phashes[pid] = imagehash.hex_to_hash(row["phash"])
        elif row:
            missing_phash.append(pid)

    # Backfill missing pHashes on the fly
    if missing_phash:
        if progress_callback:
            progress_callback("Computing scene hashes...")
        for pid in missing_phash:
            row = db.conn.execute(
                "SELECT folder_id, filename FROM photos WHERE id = ?", (pid,)
            ).fetchone()
            if not row:
                continue
            folder = db.conn.execute(
                "SELECT path FROM folders WHERE id = ?", (row["folder_id"],)
            ).fetchone()
            if not folder:
                continue
            import os
            image_path = os.path.join(folder["path"], row["filename"])
            try:
                with Image.open(image_path) as img:
                    h = imagehash.phash(img)
                    phashes[pid] = h
                    db.conn.execute(
                        "UPDATE photos SET phash = ? WHERE id = ?",
                        (str(h), pid),
                    )
            except Exception:
                log.debug("Could not compute pHash for photo %d", pid)
        db.conn.commit()

    # Classify extensions as RAW or non-RAW
    from image_loader import RAW_EXTENSIONS

    def _file_type(pid):
        ext = extensions.get(pid, "")
        return "raw" if ext in RAW_EXTENSIONS else "other"

    # Group by species (and optionally file type)
    species_map = {}
    for pid in photo_ids:
        if pid not in predictions:
            continue
        sp = predictions[pid]["species"]
        if separate_file_types:
            key = sp + " [" + _file_type(pid) + "]"
        else:
            key = sp
        if key not in species_map:
            species_map[key] = {"species": sp, "pids": []}
        species_map[key]["pids"].append(pid)

    # Analyze each species group
    species_groups = []
    total_keepers = 0
    total_rejects = 0

    for group_key, group_data in sorted(species_map.items(), key=lambda x: -len(x[1]["pids"])):
        species = group_data["species"]
        pids = group_data["pids"]
        sp_embeddings = {pid: embeddings[pid] for pid in pids if pid in embeddings}

        # Build per-photo metadata for client-side regrouping
        photo_data = []
        for pid in pids:
            ts = timestamps.get(pid)
            photo_data.append({
                "photo_id": pid,
                "quality": quality.get(pid, 0),
                "filename": filenames.get(pid, ""),
                "timestamp": ts.isoformat() if ts else None,
                "phash": str(phashes[pid]) if pid in phashes else None,
            })

        # Pre-compute redundancy clusters from embeddings (expensive — stays server-side)
        if len(sp_embeddings) >= 2:
            redundancy_clusters = _cluster_photos(sp_embeddings, redundancy_threshold)
        else:
            redundancy_clusters = [[pid] for pid in pids]

        # Compute initial scene grouping for the default render
        scene_clusters = _group_into_scenes(
            pids, timestamps, phashes, time_window, phash_threshold, cross_bucket_merge
        )

        scene_groups, sp_keepers, sp_rejects = _build_scene_groups(
            scene_clusters, redundancy_clusters, photo_data, timestamps, filenames
        )

        # Pre-compute pairwise embedding similarities for dev mode overlay
        embedding_sims = {}
        emb_pids = list(sp_embeddings.keys())
        for i in range(len(emb_pids)):
            for j in range(i + 1, len(emb_pids)):
                a, b = emb_pids[i], emb_pids[j]
                sim = float(np.dot(sp_embeddings[a], sp_embeddings[b]))
                key = f"{min(a,b)}-{max(a,b)}"
                embedding_sims[key] = round(sim, 3)

        species_groups.append({
            "species": group_key,
            "photo_count": len(pids),
            "scene_groups": scene_groups,
            "keepers": sp_keepers,
            "rejects": sp_rejects,
            # Raw data for client-side regrouping
            "photos_data": photo_data,
            "redundancy_clusters": [[pid for pid in c] for c in redundancy_clusters],
            "embedding_sims": embedding_sims,
        })
        total_keepers += sp_keepers
        total_rejects += sp_rejects

    return {
        "species_groups": species_groups,
        "total_photos": len(photo_ids),
        "suggested_keepers": total_keepers,
        "suggested_rejects": total_rejects,
    }


def _build_scene_groups(scene_clusters, redundancy_clusters, photo_data, timestamps, filenames):
    """Build scene group dicts with keep/reject decisions from scene clusters + redundancy clusters.

    Returns (scene_groups, keepers_count, rejects_count).
    """
    # Build quality lookup from photo_data
    quality_map = {p["photo_id"]: p["quality"] for p in photo_data}

    # Build a set-based lookup: photo_id -> which redundancy cluster index it belongs to
    pid_to_rc = {}
    for ci, cluster in enumerate(redundancy_clusters):
        for pid in cluster:
            pid_to_rc[pid] = ci

    scene_groups = []
    total_keepers = 0
    total_rejects = 0

    for scene_id, scene_pids in enumerate(scene_clusters):
        scene_pid_set = set(scene_pids)

        # Find which redundancy clusters overlap with this scene
        # For each overlapping cluster, pick best quality within this scene as keeper
        seen_pids = set()
        photos_in_scene = []

        # Group scene pids by their redundancy cluster
        rc_groups = {}
        for pid in scene_pids:
            rc_idx = pid_to_rc.get(pid, pid)  # fallback to self if not in any cluster
            if rc_idx not in rc_groups:
                rc_groups[rc_idx] = []
            rc_groups[rc_idx].append(pid)

        for rc_idx, rc_pids in rc_groups.items():
            ranked = sorted(rc_pids, key=lambda pid: -quality_map.get(pid, 0))
            for i, pid in enumerate(ranked):
                photos_in_scene.append({
                    "photo_id": pid,
                    "quality": quality_map.get(pid, 0),
                    "action": "keep" if i == 0 else "reject",
                    "redundant_with": ranked[0] if i > 0 else None,
                })

        # Sort by filename for burst display order
        photos_in_scene.sort(key=lambda x: x.get("filename", "") or filenames.get(x["photo_id"], ""))

        scene_groups.append({
            "scene_id": scene_id,
            "label": _scene_label(scene_id, scene_pids, timestamps, filenames),
            "photos": photos_in_scene,
        })

        total_keepers += sum(1 for p in photos_in_scene if p["action"] == "keep")
        total_rejects += sum(1 for p in photos_in_scene if p["action"] == "reject")

    return scene_groups, total_keepers, total_rejects


def _scene_label(scene_id, pids, timestamps, filenames):
    """Generate a human-readable label for a scene group."""
    scene_times = sorted(timestamps[pid] for pid in pids if pid in timestamps)
    num = scene_id + 1
    count = len(pids)
    if scene_times:
        start = scene_times[0].strftime("%H:%M:%S")
        end = scene_times[-1].strftime("%H:%M:%S")
        if start == end:
            return f"Scene {num} \u2014 {start} ({count} photos)"
        return f"Scene {num} \u2014 {start} to {end} ({count} photos)"
    return f"Scene {num} ({count} photos)"


def _group_into_scenes(pids, timestamps, phashes, time_window, phash_threshold, cross_bucket_merge):
    """Group photos into scenes using time bucketing and pHash similarity.

    1. Sort by timestamp, bucket by time proximity (single-linkage on time).
    2. Within each time bucket, merge photos with similar pHash.
    3. Optionally merge across time buckets by pHash similarity.

    Returns list of lists of photo_ids.
    """
    # Step 1: Time bucketing
    # Separate photos with and without timestamps
    with_ts = [(pid, timestamps[pid]) for pid in pids if pid in timestamps]
    without_ts = [pid for pid in pids if pid not in timestamps]

    # Sort by timestamp
    with_ts.sort(key=lambda x: x[1])

    # Single-linkage time bucketing
    time_buckets = []
    if with_ts:
        current_bucket = [with_ts[0][0]]
        current_times = [with_ts[0][1]]
        for pid, ts in with_ts[1:]:
            # Check if this photo is within time_window of any photo in the bucket
            min_gap = min(abs((ts - t).total_seconds()) for t in current_times)
            if min_gap <= time_window:
                current_bucket.append(pid)
                current_times.append(ts)
            else:
                time_buckets.append(current_bucket)
                current_bucket = [pid]
                current_times = [ts]
        time_buckets.append(current_bucket)

    # Each no-timestamp photo gets its own bucket
    for pid in without_ts:
        time_buckets.append([pid])

    # Step 2: pHash merging within each time bucket
    merged_buckets = []
    for bucket in time_buckets:
        merged_buckets.extend(_phash_merge(bucket, phashes, phash_threshold))

    # Step 3: Cross-bucket merging by pHash similarity
    if cross_bucket_merge and len(merged_buckets) > 1:
        merged_buckets = _merge_buckets_by_phash(merged_buckets, phashes, phash_threshold)

    return merged_buckets


def _phash_merge(pids, phashes, threshold):
    """Merge photos within a group by pHash Hamming distance (single-linkage).

    Returns list of lists of photo_ids.
    """
    if len(pids) <= 1:
        return [pids]

    clusters = [[pids[0]]]

    for pid in pids[1:]:
        if pid not in phashes:
            clusters.append([pid])
            continue
        placed = False
        for cluster in clusters:
            for member in cluster:
                if member not in phashes:
                    continue
                dist = phashes[pid] - phashes[member]
                if dist <= threshold:
                    cluster.append(pid)
                    placed = True
                    break
            if placed:
                break
        if not placed:
            clusters.append([pid])

    return clusters


def _merge_buckets_by_phash(buckets, phashes, threshold):
    """Merge scene buckets that share similar pHashes across time boundaries.

    If any photo in bucket A has pHash distance <= threshold to any photo in
    bucket B, merge them.
    """
    # Use union-find for efficient merging
    parent = list(range(len(buckets)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # Build per-bucket phash sets for fast lookup
    bucket_hashes = []
    for bucket in buckets:
        hashes = [(pid, phashes[pid]) for pid in bucket if pid in phashes]
        bucket_hashes.append(hashes)

    # Compare adjacent/nearby buckets (not all-pairs — compare each pair)
    for i in range(len(buckets)):
        for j in range(i + 1, len(buckets)):
            if find(i) == find(j):
                continue
            matched = False
            for _, hi in bucket_hashes[i]:
                for _, hj in bucket_hashes[j]:
                    if hi - hj <= threshold:
                        union(i, j)
                        matched = True
                        break
                if matched:
                    break

    # Collect merged buckets
    groups = {}
    for i in range(len(buckets)):
        root = find(i)
        if root not in groups:
            groups[root] = []
        groups[root].extend(buckets[i])

    return list(groups.values())


def _cluster_photos(embeddings_map, threshold):
    """Cluster photos by embedding similarity.

    Uses single-linkage: a photo joins a cluster if it's similar to ANY member.

    Args:
        embeddings_map: dict of photo_id -> numpy embedding
        threshold: minimum cosine similarity to be in the same cluster

    Returns:
        list of lists of photo_ids
    """
    pids = list(embeddings_map.keys())
    if len(pids) <= 1:
        return [pids]

    clusters = [[pids[0]]]

    for i in range(1, len(pids)):
        pid = pids[i]
        emb = embeddings_map[pid]
        placed = False

        for cluster in clusters:
            for member_pid in cluster:
                mem_emb = embeddings_map[member_pid]
                sim = float(np.dot(emb, mem_emb))
                if sim >= threshold:
                    cluster.append(pid)
                    placed = True
                    break
            if placed:
                break

        if not placed:
            clusters.append([pid])

    return clusters
