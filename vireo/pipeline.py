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
    p.dino_embedding_variant,
    p.focal_length, p.burst_id, p.noise_estimate,
    p.flag, p.rating,
    p.eye_x, p.eye_y, p.eye_conf, p.eye_tenengrad
"""


def _embedding_usable(stored_variant, expected_variant, emb_bytes):
    """Decide whether a stored embedding blob can be used under expected_variant.

    - No configured variant → accept whatever is stored (back-compat for tests).
    - stored_variant matches expected → accept.
    - stored_variant is NULL (pre-migration) → accept only when the blob's
      byte-length matches the expected variant's dim × 4 (float32).
    - Otherwise drop. Feeding mismatched-dim vectors to cosine sim raises
      "shapes not aligned" in encounters.sim_embedding.
    """
    if expected_variant is None:
        return True
    if stored_variant == expected_variant:
        return True
    if stored_variant is None:
        try:
            from dino_embed import get_embedding_dim
            expected_dim = get_embedding_dim(expected_variant)
        except Exception:
            return False
        return emb_bytes is not None and (len(emb_bytes) // 4) == expected_dim
    return False


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


def load_photo_features(db, collection_id=None, config=None,
                        labels_fingerprint=None):
    """Load all pipeline-relevant features for workspace photos from the database.

    Returns a list of photo dicts ready for the pipeline stages, with:
    - Numpy arrays for embeddings
    - Species top-5 from predictions
    - All quality feature columns

    Args:
        db: Database instance with active workspace
        collection_id: optional collection ID to scope results
        config: optional dict with settings (e.g. top_k_predictions)
        labels_fingerprint: optional — when set, only predictions produced
            under this label set are considered. When ``None`` (default),
            each (detection, classifier_model) surfaces rows from its most
            recent fingerprint only — otherwise a photo with cached
            predictions from multiple label sets would leak stale species
            into the top-k.

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
                ORDER BY p.timestamp, p.filename ASC, p.id ASC""",
            (ws_id, *collection_photo_ids),
        ).fetchall()
    else:
        rows = db.conn.execute(
            f"""SELECT {_PIPELINE_PHOTO_COLS}
                FROM photos p
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                WHERE wf.workspace_id = ?
                ORDER BY p.timestamp, p.filename ASC, p.id ASC""",
            (ws_id,),
        ).fetchall()

    # Resolve the workspace-effective detector_confidence threshold once.
    # The detections table is global (no workspace_id); threshold filtering
    # happens at read time against the active workspace's effective config.
    import config as cfg
    effective_cfg = db.get_effective_config(cfg.load())
    min_conf = effective_cfg.get("detector_confidence", 0.2)

    # Load species predictions (top-5 per photo, ordered by confidence).
    # Predictions reference detections (not photos directly), so JOIN through
    # the detections table to get photo_id. Only surface predictions whose
    # backing detection passes the workspace threshold — lowering the
    # threshold in workspace config should surface more predictions without
    # rewriting any rows.
    # NOTE: pr.classifier_model aliased to "model" for back-compat with
    # species_top5 tuple shape consumed downstream. Prediction review
    # fields (status/group_id/individual) are Task 25 scope.
    #
    # Fingerprint filter: a detection + classifier_model can have predictions
    # from multiple label sets (fingerprints) when the user rotates labels.
    # If the caller pinned a specific fingerprint, use it; otherwise pick
    # the most recent one per (detection, model) so stale species from an
    # old label set don't leak into the top-k.
    if labels_fingerprint is not None:
        pred_rows = db.conn.execute(
            """SELECT d.photo_id, pr.species, pr.confidence,
                      pr.classifier_model AS model
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?
                 AND pr.labels_fingerprint = ?
               ORDER BY d.photo_id, pr.confidence DESC""",
            (ws_id, min_conf, labels_fingerprint),
        ).fetchall()
    else:
        pred_rows = db.conn.execute(
            """SELECT d.photo_id, pr.species, pr.confidence,
                      pr.classifier_model AS model
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                     SELECT pr2.labels_fingerprint FROM predictions pr2
                     WHERE pr2.detection_id = pr.detection_id
                       AND pr2.classifier_model = pr.classifier_model
                     ORDER BY pr2.created_at DESC, pr2.id DESC
                     LIMIT 1
                 )
               ORDER BY d.photo_id, pr.confidence DESC""",
            (ws_id, min_conf),
        ).fetchall()

    # Group predictions by photo_id, keep top K
    top_k = (config or {}).get("top_k_predictions", 5)
    species_by_photo = defaultdict(list)
    for pr in pred_rows:
        pid = pr["photo_id"]
        if len(species_by_photo[pid]) < top_k:
            species_by_photo[pid].append((pr["species"], pr["confidence"], pr["model"]))

    # Load primary detection per photo (highest confidence) via the global
    # read-time helper. This replaces the old photos.detection_box /
    # photos.detection_conf columns; the helper applies the same threshold
    # resolved above.
    photo_ids_for_dets = [row["id"] for row in rows]
    dets_by_photo = db.get_detections_for_photos(
        photo_ids_for_dets, min_conf=min_conf,
    )
    primary_det_by_photo = {}
    for pid, dets in dets_by_photo.items():
        if not dets:
            continue
        top = dets[0]  # helper returns each list ordered by confidence DESC
        primary_det_by_photo[pid] = {
            "x": top["x"],
            "y": top["y"],
            "w": top["w"],
            "h": top["h"],
            "detection_conf": top["confidence"],
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

    # Only accept embeddings written with the currently configured DINOv2
    # variant. Without this check, switching variants leaves stale embeddings
    # of the old dim in place and the regroup stage crashes with
    # "shapes (1024,) and (768,) not aligned" in encounters.sim_embedding.
    expected_variant = (config or {}).get("pipeline", {}).get("dinov2_variant")
    variant_mismatches = 0

    photos = []
    for row in rows:
        pid = row["id"]

        stored_variant = row["dino_embedding_variant"]

        subj_bytes = row["dino_subject_embedding"]
        subj_emb = None
        if subj_bytes and _embedding_usable(stored_variant, expected_variant, subj_bytes):
            subj_emb = np.frombuffer(subj_bytes, dtype=np.float32)
        elif subj_bytes:
            variant_mismatches += 1

        glob_bytes = row["dino_global_embedding"]
        global_emb = None
        if glob_bytes and _embedding_usable(stored_variant, expected_variant, glob_bytes):
            global_emb = np.frombuffer(glob_bytes, dtype=np.float32)

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
            "eye_x": row["eye_x"],
            "eye_y": row["eye_y"],
            "eye_conf": row["eye_conf"],
            "eye_tenengrad": row["eye_tenengrad"],
        })

    log.info("Loaded %d photos with pipeline features", len(photos))
    if variant_mismatches:
        log.warning(
            "Dropped %d stale subject embeddings that don't match configured "
            "DINOv2 variant %s; those photos will need re-embedding for "
            "grouping to use their embeddings",
            variant_mismatches,
            expected_variant,
        )
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

    out = {
        "encounters": serialized_encounters,
        "photos": [_clean_photo(p) for p in results["photos"]],
        "summary": results["summary"],
    }
    # miss_computed_at is attached by pipeline_job's miss_stage and
    # consumed by pipeline_review's "Review misses" shortcut to gate
    # on actual recomputation in this run. Pass it through when the
    # caller has injected it into results (reflow/regroup-live read
    # it from the cache so the shortcut stays visible after a tweak).
    if results.get("miss_computed_at"):
        out["miss_computed_at"] = results["miss_computed_at"]
    return out


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
    # Preserve miss_computed_at across reflow/regroup-live saves: it's
    # written by pipeline_job's miss_stage and gates the review UI's
    # "Review misses" shortcut on whether misses were recomputed in
    # this pipeline run. reflow/regroup-live don't touch miss flags,
    # so overwriting this marker with a fresh save would make the
    # shortcut hide itself after every threshold tweak.
    if "miss_computed_at" not in serialized and os.path.exists(path):
        try:
            with open(path) as f:
                existing = json.load(f)
            if existing.get("miss_computed_at"):
                serialized["miss_computed_at"] = existing["miss_computed_at"]
        except (OSError, json.JSONDecodeError):
            pass
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


# ---------------------------------------------------------------------------
# Eye-focus keypoint detection stage
# ---------------------------------------------------------------------------
#
# Runs between masking and scoring. For each photo with a subject mask and
# an in-scope (Aves or Mammalia) species classification, route to the
# appropriate ONNX keypoint model, run top-down keypoint detection, apply a
# three-gate trust policy, and persist raw (eye_x, eye_y, eye_conf,
# eye_tenengrad). Scoring picks these up downstream; the normalized
# eye_focus_score is computed ephemerally at scoring time.
#
# Gates:
#   1. Classifier confidence >= C AND species in {Aves, Mammalia}.
#   2. ONNX weights for the routed model present on disk (no auto-download
#      from the stage — weights are user-opt-in via the pipeline page).
#   3. Eye keypoint confidence >= T.
#   4. Eye keypoint falls inside the subject mask.


_EYE_KEYPOINT_MODEL_FOR_CLASS = {
    "Aves": "superanimal-bird",
    "Mammalia": "superanimal-quadruped",
}


def eye_keypoint_stage_preflight(config):
    """Return a short skip reason if the eye-keypoint stage cannot do work.

    Returns None when the stage should run. Shared between
    detect_eye_keypoints_stage and the pipeline job so the job can avoid the
    O(N) eligibility join when the stage is going to short-circuit anyway.
    """
    if not config.get("eye_detect_enabled", True):
        return "Disabled in config"
    import keypoints as kp
    routable = set(_EYE_KEYPOINT_MODEL_FOR_CLASS.values())
    if not any(kp.weights_status(name) == "ready" for name in routable):
        return "No keypoint models installed"
    return None


def _resolve_keypoint_model(db, photo_row):
    """Route a photo to a keypoint model name, or None if out of scope.

    Primary path: use ``taxonomy_class`` stored on the prediction — set by
    classifiers that resolve full iNat lineage. Fallback: look up the
    scientific name in the local taxa table and walk the parent chain with
    classify_to_keypoint_group.
    """
    tax_class = photo_row.get("taxonomy_class")
    if tax_class in _EYE_KEYPOINT_MODEL_FOR_CLASS:
        return _EYE_KEYPOINT_MODEL_FOR_CLASS[tax_class]
    # Fallback: species name -> taxa.inat_id -> classify_to_keypoint_group.
    name = photo_row.get("scientific_name") or photo_row.get("species")
    if not name:
        return None
    from taxonomy import classify_to_keypoint_group
    row = db.conn.execute(
        "SELECT inat_id FROM taxa WHERE name = ? LIMIT 1", (name,)
    ).fetchone()
    if row is None:
        return None
    group = classify_to_keypoint_group(db, row[0] if not hasattr(row, "keys") else row["inat_id"])
    return _EYE_KEYPOINT_MODEL_FOR_CLASS.get(group)


def _load_mask_array(mask_path):
    """Load a saved SAM2 mask PNG as a boolean (H, W) array."""
    from PIL import Image
    mask_img = Image.open(mask_path).convert("L")
    return np.array(mask_img) > 127


def _process_photo_for_eye(db, row, folders, *, C, T, k_window):
    """Run the four-gate policy on a single photo row.

    Persists (eye_x, eye_y, eye_conf, eye_tenengrad) when every gate passes,
    otherwise returns without writing. Called per-photo from the stage loop;
    exceptions bubble up so the stage can log and continue.
    """
    import os

    import keypoints as kp
    from image_loader import load_image
    from quality import compute_eye_tenengrad

    # Gate 1: classifier confidence + species in scope.
    if (row.get("species_conf") or 0.0) < C:
        return
    model_name = _resolve_keypoint_model(db, row)
    if model_name is None:
        return

    # Gate 2: weights present on disk. Stage does NOT auto-download — the
    # user opts in via the pipeline models card. If a partial download left
    # model.onnx but no config.json (or vice versa), skip defensively.
    onnx_path = os.path.join(kp.MODELS_DIR, model_name, "model.onnx")
    config_path = os.path.join(kp.MODELS_DIR, model_name, "config.json")
    if not (os.path.isfile(onnx_path) and os.path.isfile(config_path)):
        return

    folder_path = folders.get(row["folder_id"], "")
    image_path = os.path.join(folder_path, row["filename"])
    if not os.path.isfile(image_path):
        return
    # Use image_loader (same path as detector/masking/sharpness) so RAW
    # inputs decode and EXIF-rotated JPEGs are transposed to match the
    # frame bbox and mask were authored in. max_size=1024 matches the
    # sharpness stage so tenengrad operators are applied at the same
    # pixel scale across the cohort.
    image = load_image(image_path, max_size=1024)
    if image is None:
        return
    image = image.convert("RGB")

    # Normalized 0-1 box → pixel bbox in image coords.
    iw, ih = image.size
    bbox = (
        int(round(row["box_x"] * iw)),
        int(round(row["box_y"] * ih)),
        int(round((row["box_x"] + row["box_w"]) * iw)),
        int(round((row["box_y"] + row["box_h"]) * ih)),
    )
    if bbox[2] <= bbox[0] or bbox[3] <= bbox[1]:
        return

    kps = kp.detect_keypoints(image, bbox, model_name)

    # Resize mask to image dims if needed (masks are typically saved at
    # proxy resolution and must be compared to full-image keypoint coords).
    mask = _load_mask_array(row["mask_path"])
    if mask.shape != (ih, iw):
        from PIL import Image as _PIL
        mask_img = _PIL.fromarray(mask.astype(np.uint8) * 255).resize(
            (iw, ih), _PIL.NEAREST
        )
        mask = np.array(mask_img) > 127

    # Gate 3 + 4: eye keypoint conf >= T AND inside the subject mask.
    eye_candidates = []
    for k_point in kps:
        if k_point["name"] not in ("left_eye", "right_eye"):
            continue
        if k_point["conf"] < T:
            continue
        mx, my = int(k_point["x"]), int(k_point["y"])
        if not (0 <= mx < mask.shape[1] and 0 <= my < mask.shape[0]):
            continue
        if not mask[my, mx]:
            continue
        eye_candidates.append(k_point)

    if not eye_candidates:
        return

    # Pick the eye with the highest windowed tenengrad — "best" eye wins.
    best = None
    best_score = -1.0
    for eye in eye_candidates:
        score = compute_eye_tenengrad(
            image, (eye["x"], eye["y"]), bbox, k=k_window
        )
        if score > best_score:
            best_score = score
            best = eye

    # Persist eye coords normalized to 0-1 against the loaded (oriented)
    # image dims. Two reasons: (a) EXIF-rotated JPEGs would otherwise
    # need the oriented dims stored separately for the lightbox to map
    # pixel coords back to a percentage — photos.width/height come from
    # the un-oriented sensor tag so the math goes wrong on orientation
    # 6/8; (b) this matches the detection-box storage convention
    # (box_x/box_y are also normalized 0-1).
    db.update_photo_pipeline_features(
        row["id"],
        eye_x=best["x"] / float(iw),
        eye_y=best["y"] / float(ih),
        eye_conf=best["conf"],
        eye_tenengrad=best_score,
    )


def detect_eye_keypoints_stage(
    db, config, progress_callback=None,
    collection_id=None, exclude_photo_ids=None,
):
    """Pipeline stage: detect eye keypoints and persist raw tenengrad.

    For each eligible photo (see Database.list_photos_for_eye_keypoint_stage),
    run the routed keypoint model, apply the four-gate trust policy, and
    persist (eye_x, eye_y, eye_conf, eye_tenengrad) for gated-through photos.
    Per-photo exceptions are logged and do not abort the stage.

    Args:
        db: Database with an active workspace.
        config: dict of tunables. Reads:
            - eye_detect_enabled (bool, default True)
            - eye_classifier_conf_gate (float, default 0.5)
            - eye_detection_conf_gate (float, default 0.5)
            - eye_window_k (float, default 0.08)
        progress_callback: optional callable(phase, current, total).
        collection_id: optional collection ID to scope processing to. When
            provided, only photos in that collection are considered — matches
            the scoping that extract/regroup stages already apply so a run
            started for one collection doesn't mutate eye fields elsewhere.
        exclude_photo_ids: optional iterable of photo IDs to exclude. Mirrors
            the preview-deselection filter that extract/regroup honor, so
            photos the user unchecked don't get eye_* values locked in (the
            stage is idempotent via eye_tenengrad IS NULL, so leaking writes
            here would survive future reruns).
    """
    skip_reason = eye_keypoint_stage_preflight(config)
    if skip_reason is not None:
        log.info("Eye-keypoint stage skipped: %s", skip_reason)
        return

    C = config.get("eye_classifier_conf_gate", 0.5)
    T = config.get("eye_detection_conf_gate", 0.5)
    k_window = config.get("eye_window_k", 0.08)

    exclude_set = set(exclude_photo_ids) if exclude_photo_ids else None

    photo_ids = None
    if collection_id is not None:
        photo_ids = _resolve_collection_photo_ids(db, collection_id)
        if not photo_ids:
            return
    if exclude_set and photo_ids is not None:
        photo_ids = {pid for pid in photo_ids if pid not in exclude_set}
        if not photo_ids:
            return
    photos = db.list_photos_for_eye_keypoint_stage(photo_ids=photo_ids)
    if exclude_set and photo_ids is None:
        photos = [p for p in photos if p["id"] not in exclude_set]
    if not photos:
        return
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    total = len(photos)

    for i, row in enumerate(photos):
        if progress_callback:
            progress_callback("Eye keypoints", i, total)
        try:
            _process_photo_for_eye(
                db, row, folders, C=C, T=T, k_window=k_window,
            )
        except Exception:
            log.warning(
                "Eye keypoint detection failed for photo %s", row["id"],
                exc_info=True,
            )

    if progress_callback:
        progress_callback("Eye keypoints", total, total)
