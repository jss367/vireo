"""Pipeline orchestration — ties stages 2-6 together.

Loads photo features from the database, runs encounter segmentation,
burst clustering, quality scoring, MMR selection, and triage.

Entry points:
  - run_grouping(db, config) → encounters with bursts (stages 2-3)
  - run_triage(encounters, config) → labeled photos (stages 4-6)
  - run_full_pipeline(db, config) → everything in one call
"""

import json
import logging
import os
from collections import defaultdict

import numpy as np

log = logging.getLogger(__name__)

# Columns needed from photos table for pipeline stages 2-6
_PIPELINE_PHOTO_COLS = """
    p.id, p.folder_id, p.filename, p.timestamp,
    p.width, p.height, p.latitude, p.longitude,
    p.subject_size,
    p.mask_path, p.subject_tenengrad, p.bg_tenengrad,
    p.crop_complete, p.bg_separation,
    p.subject_clip_high, p.subject_clip_low, p.subject_y_median,
    p.phash_crop,
    p.dino_subject_embedding, p.dino_global_embedding,
    p.focal_length, p.burst_id, p.noise_estimate,
    p.flag, p.rating
"""


def _resolve_collection_photo_ids(db, collection_id):
    """Resolve a collection to a set of photo IDs using the collection rules engine.

    Uses get_collection_photos with a large page size to get all matching photos,
    then extracts just the IDs.

    Args:
        db: Database instance with active workspace
        collection_id: collection ID to resolve

    Returns:
        set of photo IDs (empty set if collection not found or has no photos)
    """
    rows = db.get_collection_photos(collection_id, page=1, per_page=1_000_000)
    return {r["id"] for r in rows} if rows else set()


def load_photo_features(db, collection_id=None, config=None):
    """Load all pipeline-relevant features for workspace photos from the database.

    Returns a list of photo dicts ready for the pipeline stages, with:
    - Numpy arrays for embeddings
    - Species top-5 from predictions
    - All quality feature columns

    Args:
        db: Database instance with active workspace
        collection_id: optional collection ID to scope results
        config: optional dict with settings (e.g. top_k_predictions)

    Returns:
        list of photo dicts
    """
    ws_id = db._ws_id()

    # Resolve collection to photo IDs if scoping is requested
    collection_photo_ids = None
    if collection_id is not None:
        collection_photo_ids = _resolve_collection_photo_ids(db, collection_id)
        if not collection_photo_ids:
            return []

    if collection_photo_ids is not None:
        placeholders = ",".join("?" for _ in collection_photo_ids)
        rows = db.conn.execute(
            f"""SELECT {_PIPELINE_PHOTO_COLS}
                FROM photos p
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                WHERE wf.workspace_id = ? AND p.id IN ({placeholders})
                ORDER BY p.timestamp, p.filename ASC""",
            (ws_id, *collection_photo_ids),
        ).fetchall()
    else:
        rows = db.conn.execute(
            f"""SELECT {_PIPELINE_PHOTO_COLS}
                FROM photos p
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                WHERE wf.workspace_id = ?
                ORDER BY p.timestamp, p.filename ASC""",
            (ws_id,),
        ).fetchall()

    # Load species predictions (top-5 per photo, ordered by confidence).
    # Predictions reference detections (not photos directly), so JOIN through
    # the detections table to get photo_id.
    pred_rows = db.conn.execute(
        """SELECT d.photo_id, pr.species, pr.confidence, pr.model
           FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           JOIN photos p ON p.id = d.photo_id
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE d.workspace_id = ? AND wf.workspace_id = ?
           ORDER BY d.photo_id, pr.confidence DESC""",
        (ws_id, ws_id),
    ).fetchall()

    # Group predictions by photo_id, keep top K
    top_k = (config or {}).get("top_k_predictions", 5)
    species_by_photo = defaultdict(list)
    for pr in pred_rows:
        pid = pr["photo_id"]
        if len(species_by_photo[pid]) < top_k:
            species_by_photo[pid].append((pr["species"], pr["confidence"], pr["model"]))

    # Load primary detection per photo (highest confidence) from detections table.
    # This replaces the old photos.detection_box / photos.detection_conf columns.
    det_rows = db.conn.execute(
        """SELECT d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
                  d.detector_confidence
           FROM detections d
           JOIN photos p ON p.id = d.photo_id
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE d.workspace_id = ? AND wf.workspace_id = ?
           ORDER BY d.photo_id, d.detector_confidence DESC""",
        (ws_id, ws_id),
    ).fetchall()
    primary_det_by_photo = {}
    for dr in det_rows:
        pid = dr["photo_id"]
        if pid not in primary_det_by_photo:
            primary_det_by_photo[pid] = {
                "x": dr["box_x"],
                "y": dr["box_y"],
                "w": dr["box_w"],
                "h": dr["box_h"],
                "detection_conf": dr["detector_confidence"],
            }

    # Load user-confirmed species keywords (alphabetically first wins
    # for photos with multiple species tags — rare but deterministic)
    species_kw_rows = db.conn.execute(
        """SELECT pk.photo_id, k.name
           FROM photo_keywords pk
           JOIN keywords k ON k.id = pk.keyword_id
           WHERE k.is_species = 1
           ORDER BY k.name"""
    ).fetchall()
    confirmed_by_photo = {}
    for row in species_kw_rows:
        confirmed_by_photo.setdefault(row["photo_id"], row["name"])

    photos = []
    for row in rows:
        pid = row["id"]

        # Decode embeddings from BLOBs
        subj_emb = None
        if row["dino_subject_embedding"]:
            subj_emb = np.frombuffer(row["dino_subject_embedding"], dtype=np.float32)

        global_emb = None
        if row["dino_global_embedding"]:
            global_emb = np.frombuffer(row["dino_global_embedding"], dtype=np.float32)

        det = primary_det_by_photo.get(pid)
        det_box = None
        det_conf = None
        if det:
            det_box = {"x": det["x"], "y": det["y"],
                       "w": det["w"], "h": det["h"]}
            det_conf = det["detection_conf"]

        photos.append({
            "id": pid,
            "folder_id": row["folder_id"],
            "filename": row["filename"],
            "timestamp": row["timestamp"],
            "width": row["width"],
            "height": row["height"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "detection_box": det_box,
            "detection_conf": det_conf,
            "subject_size": row["subject_size"],
            "mask_path": row["mask_path"],
            "subject_tenengrad": row["subject_tenengrad"],
            "bg_tenengrad": row["bg_tenengrad"],
            "crop_complete": row["crop_complete"],
            "bg_separation": row["bg_separation"],
            "subject_clip_high": row["subject_clip_high"],
            "subject_clip_low": row["subject_clip_low"],
            "subject_y_median": row["subject_y_median"],
            "phash_crop": row["phash_crop"],
            "dino_subject_embedding": subj_emb,
            "dino_global_embedding": global_emb,
            "species_top5": species_by_photo.get(pid, []),
            "confirmed_species": confirmed_by_photo.get(pid),
            "focal_length": row["focal_length"],
            "burst_id": row["burst_id"],
            "noise_estimate": row["noise_estimate"],
            "flag": row["flag"],
            "rating": row["rating"],
        })

    log.info("Loaded %d photos with pipeline features", len(photos))
    return photos


def run_grouping(photos, config=None):
    """Run encounter segmentation + burst clustering (Stages 2-3).

    Args:
        photos: list of photo dicts from load_photo_features()
        config: optional dict with pipeline thresholds

    Returns:
        list of encounter dicts, each with 'bursts' key
    """
    from bursts import segment_bursts_for_encounters
    from encounters import segment_encounters

    encounters = segment_encounters(photos, config=config)
    encounters = segment_bursts_for_encounters(encounters, config=config)

    total_bursts = sum(e.get("burst_count", 0) for e in encounters)
    log.info(
        "Grouping: %d encounters, %d bursts from %d photos",
        len(encounters),
        total_bursts,
        len(photos),
    )
    return encounters


def run_triage(encounters, config=None):
    """Run scoring + MMR selection + triage (Stages 4-6).

    Args:
        encounters: list of encounter dicts from run_grouping()
        config: optional dict with scoring/selection thresholds

    Returns:
        (encounters, all_photos) — encounters modified in place,
        all_photos is a flat list of every photo with labels
    """
    from scoring import score_encounter
    from selection import triage_encounters

    for enc in encounters:
        score_encounter(enc, config=config)

    encounters, all_photos = triage_encounters(encounters, config=config)
    return encounters, all_photos


def run_full_pipeline(photos, config=None):
    """Run the full pipeline: grouping → scoring → triage (Stages 2-6).

    Args:
        photos: list of photo dicts from load_photo_features()
        config: optional dict with all pipeline thresholds

    Returns:
        dict with:
            encounters: list of encounter dicts
            photos: flat list of all photos with labels
            summary: dict with counts
    """
    encounters = run_grouping(photos, config=config)
    encounters, all_photos = run_triage(encounters, config=config)

    summary = _make_summary(encounters, all_photos)
    log.info(
        "Pipeline complete: %d photos → %d KEEP, %d REVIEW, %d REJECT",
        summary["total_photos"],
        summary["keep_count"],
        summary["review_count"],
        summary["reject_count"],
    )

    return {
        "encounters": encounters,
        "photos": all_photos,
        "summary": summary,
    }


def _clear_derived_scores(photos):
    """Remove derived scoring/triage fields so photos can be re-scored."""
    derived_keys = [
        "focus_score", "exposure_score", "composition_score",
        "area_score", "noise_score", "quality_composite",
        "reject_reasons", "label", "rarity_protected",
    ]
    for p in photos:
        for k in derived_keys:
            p.pop(k, None)


def _make_summary(encounters, all_photos):
    """Build summary stats dict from triage results."""
    return {
        "total_photos": len(all_photos),
        "encounter_count": len(encounters),
        "burst_count": sum(e.get("burst_count", 0) for e in encounters),
        "keep_count": sum(1 for p in all_photos if p.get("label") == "KEEP"),
        "review_count": sum(1 for p in all_photos if p.get("label") == "REVIEW"),
        "reject_count": sum(1 for p in all_photos if p.get("label") == "REJECT"),
        "rarity_protected": sum(
            1 for p in all_photos if p.get("rarity_protected")
        ),
    }


def reflow(encounters, config=None):
    """Re-run only stages 4-6 (scoring + selection + triage) on existing grouping.

    This is the fast path for threshold tuning — no model inference, no regrouping,
    just re-score and re-triage from cached features. Runs in milliseconds.

    Args:
        encounters: list of encounter dicts (with 'photos' and 'bursts' intact)
        config: dict with scoring/selection threshold overrides

    Returns:
        dict with encounters, photos, summary (same format as run_full_pipeline)
    """
    # Clear old derived scores so they're recomputed fresh
    for enc in encounters:
        _clear_derived_scores(enc.get("photos", []))
        for burst in enc.get("bursts", []):
            if isinstance(burst, list):
                _clear_derived_scores(burst)

    encounters, all_photos = run_triage(encounters, config=config)

    summary = _make_summary(encounters, all_photos)
    log.info(
        "Reflow: %d KEEP, %d REVIEW, %d REJECT",
        summary["keep_count"],
        summary["review_count"],
        summary["reject_count"],
    )

    return {
        "encounters": encounters,
        "photos": all_photos,
        "summary": summary,
    }


def _build_species_predictions(photos):
    """Build per-model species prediction breakdown from a list of photo dicts.

    Returns a list of dicts sorted by total count descending, each with:
        species, count, avg_confidence, models: [{model, confidence, photo_count}]
    """
    model_data = defaultdict(lambda: defaultdict(lambda: {"confs": [], "count": 0}))
    for p in photos:
        for entry in (p.get("species_top5") or []):
            sp_name = entry[0]
            sp_conf = entry[1]
            sp_model = entry[2] if len(entry) > 2 else "unknown"
            model_data[sp_name][sp_model]["confs"].append(sp_conf)
            model_data[sp_name][sp_model]["count"] += 1

    result = []
    for sp_name in sorted(model_data, key=lambda s: sum(
        d["count"] for d in model_data[s].values()
    ), reverse=True):
        models = []
        total_count = 0
        total_conf_sum = 0.0
        total_conf_count = 0
        for model_name, data in sorted(model_data[sp_name].items()):
            avg_conf = sum(data["confs"]) / len(data["confs"])
            models.append({
                "model": model_name,
                "confidence": round(avg_conf, 4),
                "photo_count": data["count"],
            })
            total_count += data["count"]
            total_conf_sum += sum(data["confs"])
            total_conf_count += len(data["confs"])
        result.append({
            "species": sp_name,
            "count": total_count,
            "avg_confidence": round(total_conf_sum / total_conf_count, 4) if total_conf_count else 0,
            "models": models,
        })
    return result


def serialize_results(results):
    """Serialize pipeline results to a JSON-safe dict.

    Strips numpy arrays and non-serializable objects so results
    can be saved to disk or returned via API.

    Args:
        results: dict from run_full_pipeline()

    Returns:
        JSON-serializable dict
    """
    def _clean_photo(p):
        """Strip non-serializable fields from a photo dict."""
        cleaned = {}
        for k, v in p.items():
            if isinstance(v, np.ndarray):
                continue  # skip embedding arrays
            if isinstance(v, (np.floating, np.integer)):
                cleaned[k] = float(v)
            else:
                cleaned[k] = v
        return cleaned

    serialized_encounters = []
    for enc in results["encounters"]:
        photos_list = enc.get("photos", [])

        # Derive confirmed_species from photos (any confirmed photo sets encounter)
        enc_confirmed = None
        for p in photos_list:
            if p.get("confirmed_species"):
                enc_confirmed = p["confirmed_species"]
                break

        species_votes = _build_species_predictions(photos_list)

        s_enc = {
            "species": enc.get("species"),
            "confirmed_species": enc_confirmed,
            "species_predictions": species_votes,
            "species_confirmed": enc_confirmed is not None,
            "photo_count": enc.get("photo_count"),
            "burst_count": enc.get("burst_count"),
            "time_range": enc.get("time_range"),
            "photo_ids": [p["id"] for p in photos_list],
        }
        if "bursts" in enc:
            s_enc["bursts"] = []
            for burst in enc["bursts"]:
                burst_ids = [p["id"] for p in burst]
                s_enc["bursts"].append({
                    "photo_ids": burst_ids,
                    "species_predictions": _build_species_predictions(burst),
                    "species_override": None,
                })
        serialized_encounters.append(s_enc)

    return {
        "encounters": serialized_encounters,
        "photos": [_clean_photo(p) for p in results["photos"]],
        "summary": results["summary"],
    }


def save_results(results, cache_dir, workspace_id):
    """Save serialized pipeline results to a JSON cache file.

    Args:
        results: dict from run_full_pipeline()
        cache_dir: directory containing the database (e.g. ~/.vireo/)
        workspace_id: active workspace ID

    Returns:
        path to the saved JSON file
    """
    serialized = serialize_results(results)
    path = os.path.join(cache_dir, f"pipeline_results_ws{workspace_id}.json")
    with open(path, "w") as f:
        json.dump(serialized, f)
    log.info("Pipeline results saved to %s", path)
    return path


def load_results(cache_dir, workspace_id):
    """Load pipeline results from a JSON cache file.

    Args:
        cache_dir: directory containing the database
        workspace_id: active workspace ID

    Returns:
        dict or None if no cache exists
    """
    path = os.path.join(cache_dir, f"pipeline_results_ws{workspace_id}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def load_results_raw(cache_dir, workspace_id):
    """Load raw (already serialized) pipeline results JSON dict.

    Unlike load_results, this returns the dict exactly as stored on disk,
    for in-place mutation by structural edits (detach, species confirm).
    """
    path = os.path.join(cache_dir, f"pipeline_results_ws{workspace_id}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def rebuild_species_predictions(results, photo_ids):
    """Rebuild species_predictions for a subset of photos from cached results.

    Looks up the given photo_ids in results["photos"], extracts their
    species_top5 data, and returns a species_predictions list in the same
    format as serialize_results produces.

    Args:
        results: cached pipeline results dict (with "photos" list)
        photo_ids: list of photo IDs to include

    Returns:
        list of species prediction dicts
    """
    id_set = set(photo_ids)
    subset = [p for p in results.get("photos", []) if p.get("id") in id_set]
    return _build_species_predictions(subset)


def save_results_raw(results, cache_dir, workspace_id):
    """Save an already-serialized results dict back to the JSON cache."""
    path = os.path.join(cache_dir, f"pipeline_results_ws{workspace_id}.json")
    with open(path, "w") as f:
        json.dump(results, f)
    return path
