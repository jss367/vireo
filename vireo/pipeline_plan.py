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
    # Absolute file paths for an "import" / "new_images" run — i.e. the
    # files the user is about to ingest into the active workspace.
    # compute_plan splits these into already-known photo_ids (used as the
    # per-stage scope, so the existing real-status queries apply) and a
    # count of genuinely new files. New files are treated as fresh work
    # for every per-photo stage, which is the only honest answer when
    # the next pipeline run *will* process them — replacing the old
    # behaviour of describing the whole active workspace's state, which
    # made an import into a non-empty workspace render as "Already done".
    #
    # ``None`` = field not provided (whole-workspace scope, the existing
    # behaviour for non-import modes). ``[]`` = import mode with every
    # preview file deselected — a genuine no-op run, which must NOT fall
    # back to whole-workspace scope (that would re-introduce the very
    # misleading "Already done" pills this code exists to prevent).
    source_paths: list | None = None


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


def _classify_plan(db, params, photo_ids, new_count=0):
    if params.skip_classify:
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
            "detail": {
                "pending": 0, "eligible": 0,
                "stale": 0, "fingerprint_outdated": False,
            },
        }

    models = _resolve_models(params.model_ids)
    if not models:
        return {
            "state": "will-skip",
            "summary": "No models selected — stage will be skipped",
            "detail": {
                "pending": 0, "eligible": 0,
                "stale": 0, "fingerprint_outdated": False,
            },
        }

    label_resolution = _resolve_labels_for_models(models, params.labels_files, db)

    det_counts = db.count_real_detections_in_scope(photo_ids)
    total_dets = det_counts["total_dets"]
    photos_with_dets = det_counts["photos_with_dets"]

    unblocked_count = sum(
        1 for m in models if not label_resolution[m["id"]].get("blocked")
    )
    eligible = total_dets * unblocked_count

    stale_total = 0
    if total_dets > 0 and not params.reclassify:
        for m in models:
            info = label_resolution[m["id"]]
            if info.get("blocked"):
                continue
            fp = info["fingerprint"]
            stale_total += db.count_classify_stale(
                classifier_model=m["name"],
                labels_fingerprint=fp,
                photo_ids=photo_ids,
            )
    # Reclassify is a user override, not a settings-change signal.
    fingerprint_outdated = stale_total > 0 and not params.reclassify

    if total_dets == 0:
        if new_count > 0:
            summary = (
                f"Will run — {new_count} new photo{_plural(new_count)} "
                f"to detect & classify (MegaDetector runs first)"
            )
        else:
            summary = (
                "Will run — no detections cached yet "
                "(MegaDetector will run first)"
            )
        return {
            "state": "will-run",
            "summary": summary,
            "detail": {
                "total_dets": 0,
                "photos_with_dets": 0,
                "models": [m["name"] for m in models],
                "pending": new_count,
                "eligible": new_count,
                "new_photos": new_count,
                "stale": stale_total,
                "fingerprint_outdated": fingerprint_outdated,
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
        # eligible=0 here even when some unblocked models happen to be
        # fully cached — the stage is functionally blocked on missing
        # labels and the summary text already explains that. Returning
        # eligible>0 with pending=0 would let the UI render
        # "Resume (0 left)" against a state that's actually "Blocked".
        return {
            "state": "will-run",
            "summary": (
                f"Blocked — {len(blocked)} model{_plural(len(blocked))} "
                f"need labels: {', '.join(blocked)}"
            ),
            "detail": {
                "blocked_models": blocked,
                "pending": 0,
                "eligible": 0,
                "stale": stale_total,
                "fingerprint_outdated": fingerprint_outdated,
            },
        }

    if pending_total == 0:
        if new_count > 0:
            # Existing scope is fully classified, but the import will pull
            # in N new photos that need detector + classify. Honest answer
            # is "will run", not "Already done".
            return {
                "state": "will-run",
                "summary": (
                    f"Will run on {new_count} new "
                    f"photo{_plural(new_count)} "
                    f"({total_dets} existing detection"
                    f"{_plural(total_dets)} already classified)"
                ),
                "detail": {
                    "total_dets": total_dets,
                    "photos_with_dets": photos_with_dets,
                    "models": [m["name"] for m in models],
                    "pending": new_count,
                    "eligible": eligible + new_count,
                    "new_photos": new_count,
                    "stale": stale_total,
                    "fingerprint_outdated": fingerprint_outdated,
                },
            }
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
                "pending": 0,
                "eligible": eligible,
                "stale": stale_total,
                "fingerprint_outdated": fingerprint_outdated,
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
    if new_count > 0:
        # Mixed scope: some existing detections still to classify *and*
        # N new photos coming in (each will get its own detections + class).
        summary += (
            f" + {new_count} new photo{_plural(new_count)} "
            f"to detect & classify"
        )
    detail = {
        "pending_pairs": pending_total,
        "per_model": pending_per_model,
        "total_dets": total_dets,
        "photos_with_dets": photos_with_dets,
        "pending": pending_total + new_count,
        "eligible": eligible + new_count,
        "stale": stale_total,
        "fingerprint_outdated": fingerprint_outdated,
    }
    if new_count > 0:
        detail["new_photos"] = new_count
    if blocked:
        detail["blocked_models"] = blocked
    return {"state": "will-run", "summary": summary, "detail": detail}


def _extract_plan(db, params, photo_ids, pipeline_cfg, new_count=0):
    if params.skip_extract_masks:
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
        }
    counts = db.count_photos_pending_masks(photo_ids)
    eligible = counts["eligible"]
    pending = counts["pending"]
    sam2_variant = pipeline_cfg.get("sam2_variant", "sam2-small")
    stale = db.count_extract_stale(sam2_variant, photo_ids)
    if eligible == 0:
        # Without imports: classify hasn't run yet, no photos eligible —
        # the pill renders "Will run" with no count, which is honest.
        # With imports: bound the count by new_count so the pill says
        # "Will run (N)" reflecting at least N about-to-be-imported photos.
        # (Some new photos may turn out to have no detections, so this is
        # an upper bound on extract work, not an exact promise.)
        if new_count > 0:
            return {
                "state": "will-run",
                "summary": (
                    f"Will run after classify produces detections "
                    f"(up to {new_count} new "
                    f"photo{_plural(new_count)} pending)"
                ),
                "detail": {
                    "eligible": new_count, "pending": new_count,
                    "new_photos": new_count,
                    "stale": 0, "fingerprint_outdated": False,
                },
            }
        return {
            "state": "will-run",
            "summary": (
                "Will run after classify produces detections "
                "(no eligible photos yet)"
            ),
            "detail": {"eligible": 0, "pending": 0,
                       "stale": 0, "fingerprint_outdated": False},
        }
    if pending == 0 and stale == 0 and new_count == 0:
        return {
            "state": "done-prior",
            "summary": (
                f"Masks present for all {eligible} eligible "
                f"photo{_plural(eligible)}"
            ),
            "detail": {"eligible": eligible, "pending": 0,
                       "stale": 0, "fingerprint_outdated": False},
        }
    # Stale masks (stored prompt no longer matches current detection) are
    # work the stage will redo, just like photos with no mask at all.
    # Roll them into ``pending`` so the UI's pill text ("N to redo") and
    # progress bar (`(eligible - pending) / eligible`) reflect the actual
    # work — otherwise stale-only states render as "0 to redo" + 100% bar.
    # The two sets are disjoint by construction: pending requires
    # ``mask_path IS NULL`` while ``count_extract_stale`` requires
    # ``mask_path IS NOT NULL``. Without that gate, a photo in an
    # interrupted state (photo_masks row inserted but mask_path not
    # yet updated) would land in both buckets and inflate work past
    # eligible.
    work = pending + stale
    if new_count > 0:
        # Up-to-N new imports will need masks once classify produces
        # detections for them. Treat them as eligible+pending so the pill
        # reflects the run's full work, not just the existing-scope stragglers.
        summary = (
            f"Will extract masks for {work} existing + up to "
            f"{new_count} new photo{_plural(new_count)} "
            f"({eligible + new_count} eligible)"
        )
    else:
        summary = (
            f"Will extract masks for {work} "
            f"photo{_plural(work)} ({eligible} eligible)"
        )
    detail = {
        "eligible": eligible + new_count,
        "pending": work + new_count,
        "stale": stale,
        "fingerprint_outdated": stale > 0,
    }
    if new_count > 0:
        detail["new_photos"] = new_count
    return {
        "state": "will-run",
        "summary": summary,
        "detail": detail,
    }


def _eye_keypoints_plan(db, params, photo_ids, pipeline_cfg, new_count=0):
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
    stale = db.count_eye_keypoint_stale(photo_ids)
    processed = max(eligible - pending, 0)

    if eligible == 0:
        if new_count > 0:
            return {
                "state": "will-run",
                "summary": (
                    f"Will run after upstream produces masks + species "
                    f"predictions (up to {new_count} new "
                    f"photo{_plural(new_count)} pending)"
                ),
                "detail": {
                    "eligible": new_count,
                    "pending": new_count,
                    "new_photos": new_count,
                    "stale": stale,
                    "fingerprint_outdated": stale > 0,
                },
            }
        return {
            "state": "will-run",
            "summary": (
                "Will run after upstream produces masks + species "
                "predictions (no eligible photos yet)"
            ),
            "detail": {
                "eligible": 0,
                "pending": 0,
                "stale": stale,
                "fingerprint_outdated": stale > 0,
            },
        }
    if pending == 0 and new_count == 0:
        return {
            "state": "done-prior",
            "summary": (
                f"Eye keypoints present for all {eligible} eligible "
                f"photo{_plural(eligible)}"
            ),
            "detail": {
                "eligible": eligible,
                "pending": 0,
                "processed": processed,
                "stale": stale,
                "fingerprint_outdated": stale > 0,
            },
        }
    if new_count > 0:
        summary = (
            f"Will run on {pending} existing + up to {new_count} new "
            f"photo{_plural(new_count)} "
            f"({processed} of {eligible} already processed)"
        )
    else:
        summary = (
            f"Will run on {pending} photo{_plural(pending)} "
            f"({processed} of {eligible} already processed)"
        )
    detail = {
        "eligible": eligible + new_count,
        "pending": pending + new_count,
        "processed": processed,
        "stale": stale,
        "fingerprint_outdated": stale > 0,
    }
    if new_count > 0:
        detail["new_photos"] = new_count
    return {
        "state": "will-run",
        "summary": summary,
        "detail": detail,
    }


def _regroup_plan(db, params, db_path, ws_id, upstream_will_run, effective_cfg):
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
        current_fp = compute_group_fingerprint(effective_cfg)
        row = db.conn.execute(
            "SELECT last_group_fingerprint FROM workspaces WHERE id = ?",
            (ws_id,),
        ).fetchone()
        last_fp = row["last_group_fingerprint"] if row else None
        if last_fp is None:
            # Cache file exists but no fingerprint stamp. Two paths land
            # here: (1) a partial regroup wrote subset output to the
            # cache and invalidated the stamp; (2) pre-Phase-1 DBs that
            # cached results before fingerprint stamping existed.
            # Either way the cache no longer represents a fresh
            # full-workspace grouping, so report will-run.
            return {
                "state": "will-run",
                "summary": "Will re-group — cached grouping is partial or untracked",
                "detail": {
                    "cache_exists": True,
                    "upstream_will_run": False,
                    "fingerprint_invalidated": True,
                },
            }
        if last_fp != current_fp:
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


def _empty_import_plan():
    """Per-stage plan when import mode is active but every preview file is
    deselected.

    The per-photo stages (Classify/Extract/EyeKeypoints/Group) are genuine
    no-ops because they only operate on imported photos and the import
    set is empty. Their pill must read as "no work because you've selected
    nothing", not status derived from the active workspace.

    Scan, however, is NOT a guaranteed no-op even with an empty selection:
    in scan-in-place mode the scanner still walks ``params.sources`` and
    only honors per-file deselection via ``skip_paths`` (so unpreviewed
    siblings still get scanned), and in copy mode the scanner walks the
    destination tree when no files were copied. Reporting "will-skip" for
    Scan would lie about substantial directory walking + hashing work the
    next run actually performs — exactly the misleading-pill failure
    CORE_PHILOSOPHY.md prohibits. So Scan reads as "will-run" with a
    summary that names the gap between selection and runtime behavior.
    """
    no_op = {
        "state": "will-skip",
        "summary": "No files selected — nothing to do",
        "detail": {"pending": 0, "eligible": 0},
    }
    return {
        "stages": {
            "Scan": {
                "state": "will-run",
                "summary": (
                    "Will scan source folder(s) — selection only filters "
                    "which files import"
                ),
                "detail": {
                    "eligible": 0, "pending": 0,
                    "new_photos": 0, "already_known": 0,
                },
            },
            "Classify": dict(no_op),
            "Extract": dict(no_op),
            "EyeKeypoints": dict(no_op),
            "Group": dict(no_op),
        },
        "scope": {
            "collection_id": None,
            "photo_count": 0,
            "new_count": 0,
            "known_count": 0,
        },
    }


def _scan_plan(params, new_count, known_count):
    """Plan entry for Scan & Import — only emitted in import / new-images modes.

    Honest answer for the user's "what will this run do" question: how many
    files will it actually ingest (creating photo rows), and how many were
    already in Vireo from a prior import (no-op for those).
    """
    total = new_count + known_count
    if new_count == 0:
        return {
            "state": "done-prior",
            "summary": (
                f"All {total} file{_plural(total)} already imported — "
                f"scan will be a no-op"
            ),
            "detail": {
                "eligible": total, "pending": 0,
                "new_photos": 0, "already_known": known_count,
            },
        }
    if known_count == 0:
        summary = (
            f"Will import {new_count} new file{_plural(new_count)}"
        )
    else:
        summary = (
            f"Will import {new_count} new "
            f"file{_plural(new_count)} ({known_count} already known)"
        )
    return {
        "state": "will-run",
        "summary": summary,
        "detail": {
            "eligible": total,
            "pending": new_count,
            "new_photos": new_count,
            "already_known": known_count,
        },
    }


def compute_plan(db, params, db_path):
    """Compute the full per-stage plan for the active workspace.

    Scope resolution:
      - ``collection_id`` set: scope is the collection's photo set.
      - ``source_paths`` set (import / new-images mode): scope is the
        already-known photos at those paths; truly new files (not yet in
        the photos table) are counted separately and treated as fresh
        work for every per-photo stage. A Scan plan entry describes the
        new-vs-known split so the user can see what Scan will actually do.
      - neither: whole-workspace scope (existing behaviour).

    Returns:
        {
            "stages": {
                "Scan": {state, summary, detail?},   # only in import mode
                "Classify": {state, summary, detail?},
                "Extract": ...,
                "EyeKeypoints": ...,
                "Group": ...,
            },
            "scope": {
                "collection_id": int | None,
                "photo_count": int | None,
                "new_count": int,           # 0 unless source_paths set
                "known_count": int,         # 0 unless source_paths set
            },
        }
    """
    import config as cfg

    effective_cfg = db.get_effective_config(cfg.load())
    pipeline_cfg = effective_cfg.get("pipeline", {})
    ws_id = db._ws_id()

    photo_ids = None
    new_count = 0
    known_count = 0
    if params.collection_id is not None:
        from pipeline import _resolve_collection_photo_ids
        photo_ids = _resolve_collection_photo_ids(db, params.collection_id)
        if params.exclude_photo_ids:
            excl = set(params.exclude_photo_ids)
            photo_ids = {pid for pid in photo_ids if pid not in excl}
    elif params.source_paths is not None:
        if not params.source_paths:
            # Import / new-images mode with every preview file deselected.
            # Per-photo stages are genuine no-ops, but Scan still walks —
            # see _empty_import_plan() for the honesty rationale.
            return _empty_import_plan()
        # Import / new-images mode. Split the file set into already-known
        # photo_ids (used as scope for real-status queries) and a count
        # of genuinely new files (fed into each stage as fresh work).
        # Deduplicate first: overlapping source roots (or a re-added folder
        # in the preview) can land the same path in source_paths twice,
        # and counting the duplicate as "new" would inflate Scan/Classify/
        # Extract estimates and could flip Scan from "done-prior" to
        # "will-run" misleadingly. dict.fromkeys preserves order so the
        # downstream queries see paths in the user's preview order.
        unique_paths = list(dict.fromkeys(params.source_paths))
        known = db.photos_by_paths(unique_paths)
        photo_ids = set(known.values())
        known_count = len(known)
        new_count = len(unique_paths) - known_count

    classify = _classify_plan(db, params, photo_ids, new_count)
    extract = _extract_plan(db, params, photo_ids, pipeline_cfg, new_count)
    eye = _eye_keypoints_plan(db, params, photo_ids, pipeline_cfg, new_count)
    upstream_will_run = any(
        s["state"] == "will-run" for s in (classify, extract, eye)
    )
    regroup = _regroup_plan(db, params, db_path, ws_id, upstream_will_run, effective_cfg)

    stages = {
        "Classify": classify,
        "Extract": extract,
        "EyeKeypoints": eye,
        "Group": regroup,
    }
    if params.source_paths is not None:
        stages["Scan"] = _scan_plan(params, new_count, known_count)

    return {
        "stages": stages,
        "scope": {
            "collection_id": params.collection_id,
            "photo_count": (
                len(photo_ids) if photo_ids is not None else None
            ),
            "new_count": new_count,
            "known_count": known_count,
        },
    }
