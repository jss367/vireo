"""Culling engine: find redundant photos and suggest keepers.

The culling pipeline:
1. Group photos by species (from predictions)
2. Within each species, cluster by high similarity (~0.95) = "same moment" (redundant)
3. Within each species, cluster by medium similarity (~0.85) = "different poses" (keep one of each)
4. For each redundancy cluster, pick the best quality photo as keeper
5. Everything else becomes a reject candidate
"""

import logging

import numpy as np

log = logging.getLogger(__name__)


def analyze_for_culling(db, collection_id=None, pose_threshold=0.80, redundancy_threshold=0.88, separate_file_types=True):
    """Run full culling analysis on a set of photos.

    Args:
        db: Database instance
        collection_id: optional collection to scope to (None = all photos)
        pose_threshold: similarity threshold for pose clustering (lower = more groups)
        redundancy_threshold: similarity threshold for redundancy (higher = stricter)

    Returns:
        dict with:
            species_groups: list of species group dicts
            total_photos: int
            suggested_keepers: int
            suggested_rejects: int
    """
    # Get photos with predictions and embeddings
    if collection_id:
        photos = db.get_collection_photos(collection_id, per_page=999999)
    else:
        photos = db.get_photos(per_page=999999)

    photo_ids = [p["id"] for p in photos]
    if not photo_ids:
        return {"species_groups": [], "total_photos": 0, "suggested_keepers": 0, "suggested_rejects": 0}

    # Load predictions — use each photo's individual species, not the group consensus.
    # This ensures mixed-species groups get culled per-species correctly.
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

    # Load embeddings, quality scores, and file extensions
    embeddings = {}
    quality = {}
    extensions = {}
    for pid in photo_ids:
        row = db.conn.execute(
            "SELECT embedding, quality_score, sharpness, subject_sharpness, extension FROM photos WHERE id = ?",
            (pid,),
        ).fetchone()
        if row and row["embedding"]:
            embeddings[pid] = np.frombuffer(row["embedding"], dtype=np.float32)
        q = row["quality_score"] or 0 if row else 0
        # Fall back to sharpness if no quality score
        if q == 0 and row and row["sharpness"]:
            q = row["sharpness"] / 1000.0  # normalize roughly
        quality[pid] = q
        extensions[pid] = (row["extension"] or "").lower() if row else ""

    # Classify extensions as RAW or non-RAW for file type separation
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
        # Get embeddings for this species
        sp_embeddings = {pid: embeddings[pid] for pid in pids if pid in embeddings}

        if len(sp_embeddings) < 2:
            # Single photo or no embeddings — auto-keep
            species_groups.append({
                "species": group_key,
                "photo_count": len(pids),
                "pose_groups": [{
                    "pose_id": 0,
                    "photos": [{"photo_id": pid, "quality": quality.get(pid, 0), "action": "keep"} for pid in pids],
                }],
                "keepers": len(pids),
                "rejects": 0,
            })
            total_keepers += len(pids)
            continue

        # Cluster into poses (medium similarity)
        pose_clusters = _cluster_photos(sp_embeddings, pose_threshold)

        pose_groups = []
        sp_keepers = 0
        sp_rejects = 0

        for pose_id, pose_pids in enumerate(pose_clusters):
            # Within each pose, find redundancy clusters (high similarity)
            pose_embs = {pid: sp_embeddings[pid] for pid in pose_pids if pid in sp_embeddings}

            if len(pose_embs) < 2:
                # Single photo in pose — keep it
                photos_in_pose = [{
                    "photo_id": pid,
                    "quality": quality.get(pid, 0),
                    "action": "keep",
                } for pid in pose_pids]
                pose_groups.append({
                    "pose_id": pose_id,
                    "photos": sorted(photos_in_pose, key=lambda x: -x["quality"]),
                })
                sp_keepers += len(pose_pids)
                continue

            redundancy_clusters = _cluster_photos(pose_embs, redundancy_threshold)

            photos_in_pose = []
            for cluster in redundancy_clusters:
                # Pick the best quality in each redundancy cluster
                ranked = sorted(cluster, key=lambda pid: -quality.get(pid, 0))
                for i, pid in enumerate(ranked):
                    photos_in_pose.append({
                        "photo_id": pid,
                        "quality": quality.get(pid, 0),
                        "action": "keep" if i == 0 else "reject",
                        "redundant_with": ranked[0] if i > 0 else None,
                    })

            # Sort by quality descending
            photos_in_pose.sort(key=lambda x: -x["quality"])
            pose_groups.append({
                "pose_id": pose_id,
                "photos": photos_in_pose,
            })

            sp_keepers += sum(1 for p in photos_in_pose if p["action"] == "keep")
            sp_rejects += sum(1 for p in photos_in_pose if p["action"] == "reject")

        species_groups.append({
            "species": group_key,
            "photo_count": len(pids),
            "pose_groups": pose_groups,
            "keepers": sp_keepers,
            "rejects": sp_rejects,
        })
        total_keepers += sp_keepers
        total_rejects += sp_rejects

    return {
        "species_groups": species_groups,
        "total_photos": len(photo_ids),
        "suggested_keepers": total_keepers,
        "suggested_rejects": total_rejects,
    }


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
