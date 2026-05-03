"""Per-stage pipeline plan for the Pipeline page UI.

Given the current UI selections (collection, models, labels, variants, skip
flags, exclude IDs), return per-stage state + summary so the page renders
accurate "Will run / Will skip / Already done" pills.

This module is the truth source for the user's transparency contract
documented in CORE_PHILOSOPHY.md ("Show the user what's happening / No
black boxes"). The plan must match what the actual pipeline job would do
for the same inputs — pills/summaries derive from the same gates the
stages use, never from coarser proxies like "any prior data exists".
"""

import logging
import os
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class PipelinePlanParams:
    collection_id: int | None = None
    exclude_photo_ids: list = field(default_factory=list)
    skip_classify: bool = False
    skip_extract_masks: bool = False
    skip_eye_keypoints: bool = False
    skip_regroup: bool = False
    model_ids: list = field(default_factory=list)
    labels_files: list = field(default_factory=list)
    reclassify: bool = False


def _plural(n, s="s"):
    return s if n != 1 else ""


def _resolve_models(model_ids):
    """Resolve UI model_ids to the (id, name, model_str, model_type) entries
    written into classifier_runs. Unknown ids are dropped — the plan reflects
    what the classify job would actually run, and the job ignores them.
    """
    if not model_ids:
        return []
    from models import get_models

    by_id = {m["id"]: m for m in get_models()}
    out = []
    for mid in model_ids:
        m = by_id.get(mid)
        if not m:
            continue
        out.append({
            "id": mid,
            "name": m.get("name") or mid,
            "model_str": m.get("model_str") or "",
            "model_type": m.get("model_type", "bioclip"),
            "downloaded": m.get("downloaded", False),
        })
    return out


def _resolve_labels_for_models(models, labels_files, db):
    """Per-model: resolve the labels list and its content fingerprint.

    Mirrors classify_job._load_labels' resolution order so the plan keys
    classifier_runs lookups by the *same* (model_name, fingerprint) pair the
    job would write. Returns: model_id -> {fingerprint, n, blocked?}.
    """
    from labels import get_active_labels, get_saved_labels, load_merged_labels
    from labels_fingerprint import LEGACY_SENTINEL, TOL_SENTINEL, compute_fingerprint

    saved_by_file = {s["labels_file"]: s for s in get_saved_labels()}

    def _load(active_sets):
        try:
            return load_merged_labels(active_sets) if active_sets else []
        except Exception as e:
            log.warning("plan: failed to load labels %s: %s", active_sets, e)
            return []

    if labels_files:
        active_sets = [
            saved_by_file.get(p, {"labels_file": p}) for p in labels_files
        ]
        labels = _load(active_sets)
    else:
        ws_labels = db.get_workspace_active_labels()
        if ws_labels is not None:
            active_sets = [
                saved_by_file.get(p, {"labels_file": p}) for p in ws_labels
            ]
            labels = _load(active_sets)
        else:
            labels = _load(get_active_labels())

    tol_supported = {
        "hf-hub:imageomics/bioclip",
        "hf-hub:imageomics/bioclip-2",
    }
    out = {}
    for m in models:
        if m["model_type"] == "timm":
            out[m["id"]] = {"fingerprint": LEGACY_SENTINEL, "n": 0}
        elif not labels:
            if m["model_str"] in tol_supported:
                out[m["id"]] = {"fingerprint": TOL_SENTINEL, "n": 0}
            else:
                out[m["id"]] = {"fingerprint": None, "n": 0, "blocked": True}
        else:
            out[m["id"]] = {
                "fingerprint": compute_fingerprint(labels),
                "n": len(labels),
            }
    return out


def _classify_plan(db, params, photo_ids):
    if params.skip_classify:
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
        }

    models = _resolve_models(params.model_ids)
    if not models:
        return {
            "state": "will-skip",
            "summary": "No models selected — stage will be skipped",
        }

    label_resolution = _resolve_labels_for_models(models, params.labels_files, db)

    det_counts = db.count_real_detections_in_scope(photo_ids)
    total_dets = det_counts["total_dets"]
    photos_with_dets = det_counts["photos_with_dets"]

    if total_dets == 0:
        return {
            "state": "will-run",
            "summary": (
                "Will run — no detections cached yet "
                "(MegaDetector will run first)"
            ),
            "detail": {
                "total_dets": 0,
                "photos_with_dets": 0,
                "models": [m["name"] for m in models],
            },
        }

    blocked = []
    pending_per_model = {}
    pending_total = 0
    for m in models:
        info = label_resolution[m["id"]]
        if info.get("blocked"):
            blocked.append(m["name"])
            continue
        fp = info["fingerprint"]
        if params.reclassify:
            pending = total_dets
        else:
            pending = db.count_classify_pending_pairs(
                classifier_model=m["name"],
                labels_fingerprint=fp,
                photo_ids=photo_ids,
            )
        if pending:
            pending_per_model[m["name"]] = pending
            pending_total += pending

    if blocked and not pending_total:
        return {
            "state": "will-run",
            "summary": (
                f"Blocked — {len(blocked)} model{_plural(len(blocked))} "
                f"need labels: {', '.join(blocked)}"
            ),
            "detail": {"blocked_models": blocked},
        }

    if pending_total == 0:
        return {
            "state": "done-prior",
            "summary": (
                f"Already classified — {total_dets} "
                f"detection{_plural(total_dets)} across "
                f"{len(models)} model{_plural(len(models))}"
            ),
            "detail": {
                "total_dets": total_dets,
                "photos_with_dets": photos_with_dets,
                "models": [m["name"] for m in models],
            },
        }

    if params.reclassify:
        summary = (
            f"Re-classify — {pending_total} "
            f"detection-model pair{_plural(pending_total)} "
            f"({total_dets} detection{_plural(total_dets)} × "
            f"{len(models)} model{_plural(len(models))})"
        )
    else:
        breakdown = ", ".join(
            f"{n} for {name}" for name, n in pending_per_model.items()
        )
        summary = (
            f"Will classify {pending_total} new "
            f"pair{_plural(pending_total)} ({breakdown})"
        )
    detail = {
        "pending_pairs": pending_total,
        "per_model": pending_per_model,
        "total_dets": total_dets,
        "photos_with_dets": photos_with_dets,
    }
    if blocked:
        detail["blocked_models"] = blocked
    return {"state": "will-run", "summary": summary, "detail": detail}


def _extract_plan(db, params, photo_ids):
    if params.skip_extract_masks:
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
        }
    counts = db.count_photos_pending_masks(photo_ids)
    eligible = counts["eligible"]
    pending = counts["pending"]
    if eligible == 0:
        return {
            "state": "will-run",
            "summary": (
                "Will run after classify produces detections "
                "(no eligible photos yet)"
            ),
            "detail": {"eligible": 0, "pending": 0},
        }
    if pending == 0:
        return {
            "state": "done-prior",
            "summary": (
                f"Masks present for all {eligible} eligible "
                f"photo{_plural(eligible)}"
            ),
            "detail": {"eligible": eligible, "pending": 0},
        }
    return {
        "state": "will-run",
        "summary": (
            f"Will extract masks for {pending} "
            f"photo{_plural(pending)} ({eligible} eligible)"
        ),
        "detail": {"eligible": eligible, "pending": pending},
    }


def _eye_keypoints_plan(db, params, photo_ids, pipeline_cfg):
    if params.skip_eye_keypoints or params.skip_extract_masks:
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
        }
    from pipeline import eye_keypoint_stage_preflight

    skip_reason = eye_keypoint_stage_preflight(pipeline_cfg)
    if skip_reason is not None:
        return {
            "state": "will-skip",
            "summary": f"Will skip — {skip_reason}",
        }

    pending = len(db.list_photos_for_eye_keypoint_stage(photo_ids=photo_ids))
    eligible = db.count_eye_keypoint_eligible(photo_ids)
    processed = max(eligible - pending, 0)

    if eligible == 0:
        return {
            "state": "will-run",
            "summary": (
                "Will run after upstream produces masks + species "
                "predictions (no eligible photos yet)"
            ),
            "detail": {"eligible": 0, "pending": 0},
        }
    if pending == 0:
        return {
            "state": "done-prior",
            "summary": (
                f"Eye keypoints present for all {eligible} eligible "
                f"photo{_plural(eligible)}"
            ),
            "detail": {"eligible": eligible, "pending": 0, "processed": processed},
        }
    return {
        "state": "will-run",
        "summary": (
            f"Will run on {pending} photo{_plural(pending)} "
            f"({processed} of {eligible} already processed)"
        ),
        "detail": {"eligible": eligible, "pending": pending, "processed": processed},
    }


def _regroup_plan(db, params, db_path, ws_id, upstream_will_run, pipeline_cfg):
    if params.skip_regroup:
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
        }
    cache_path = os.path.join(
        os.path.dirname(db_path), f"pipeline_results_ws{ws_id}.json",
    )
    cache_exists = os.path.exists(cache_path)
    if upstream_will_run:
        return {
            "state": "will-run",
            "summary": "Will re-group — upstream stages have new work to do",
            "detail": {"cache_exists": cache_exists, "upstream_will_run": True},
        }
    if cache_exists:
        # Cache exists and nothing upstream needs to run, but check whether
        # the workspace's last_group_fingerprint matches the fingerprint that
        # would be produced by a fresh run with current settings. Mismatch =>
        # encounter/burst params have changed since the cache was written, so
        # the cache is stale even though it's there.
        from pipeline import compute_group_fingerprint
        current_fp = compute_group_fingerprint(pipeline_cfg)
        row = db.conn.execute(
            "SELECT last_group_fingerprint FROM workspaces WHERE id = ?",
            (ws_id,),
        ).fetchone()
        last_fp = row["last_group_fingerprint"] if row else None
        if last_fp is not None and last_fp != current_fp:
            return {
                "state": "will-run",
                "summary": "Will re-group — settings changed since last run",
                "detail": {
                    "cache_exists": True,
                    "upstream_will_run": False,
                    "fingerprint_outdated": True,
                },
            }
        return {
            "state": "done-prior",
            "summary": "Grouping cached from prior run",
            "detail": {"cache_exists": True, "upstream_will_run": False},
        }
    return {
        "state": "will-run",
        "summary": "Will run — no cached grouping yet",
        "detail": {"cache_exists": False, "upstream_will_run": False},
    }


def compute_plan(db, params, db_path):
    """Compute the full per-stage plan for the active workspace.

    Returns:
        {
            "stages": {
                "Classify": {state, summary, detail?},
                "Extract": ...,
                "EyeKeypoints": ...,
                "Group": ...,
            },
            "scope": {"collection_id": int | None, "photo_count": int | None},
        }
    """
    import config as cfg

    effective_cfg = db.get_effective_config(cfg.load())
    pipeline_cfg = effective_cfg.get("pipeline", {})
    ws_id = db._ws_id()

    photo_ids = None
    if params.collection_id is not None:
        from pipeline import _resolve_collection_photo_ids
        photo_ids = _resolve_collection_photo_ids(db, params.collection_id)
        if params.exclude_photo_ids:
            excl = set(params.exclude_photo_ids)
            photo_ids = {pid for pid in photo_ids if pid not in excl}

    classify = _classify_plan(db, params, photo_ids)
    extract = _extract_plan(db, params, photo_ids)
    eye = _eye_keypoints_plan(db, params, photo_ids, pipeline_cfg)
    upstream_will_run = any(
        s["state"] == "will-run" for s in (classify, extract, eye)
    )
    regroup = _regroup_plan(db, params, db_path, ws_id, upstream_will_run, pipeline_cfg)

    return {
        "stages": {
            "Classify": classify,
            "Extract": extract,
            "EyeKeypoints": eye,
            "Group": regroup,
        },
        "scope": {
            "collection_id": params.collection_id,
            "photo_count": (
                len(photo_ids) if photo_ids is not None else None
            ),
        },
    }
