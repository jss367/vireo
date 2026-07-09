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
    # Explicit photo-id scope. The Process page's folder scope resolves
    # folder_ids to their workspace subtrees' photos at the API layer
    # (mirroring /api/jobs/pipeline) and passes the ids here so every
    # per-stage real-status query runs over exactly the photos a
    # folder-scoped run would process — never whole-workspace proxies.
    photo_ids: list | None = None
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
    # Paths in ``source_paths`` that the import job will skip via the
    # ``file_hash`` global-dedup gate (copy mode + ``skip_duplicates=True``).
    # ``ingest()`` dedupes by hash, not by source path: a file whose hash
    # already exists at *some* path in the photos table is skipped, even
    # though the source path itself is unknown. Without this list, those
    # files would land in ``new_count`` (their source path doesn't match
    # any photo row), inflating Scan/Classify/Extract estimates for a run
    # that will actually skip them. The frontend already pre-computes
    # hash duplicates via /api/import/check-duplicates and passes the
    # matched source paths back — we use that result instead of re-hashing
    # at plan time, since hashing thousands of files synchronously inside
    # a plan request would block the UI on every settings change.
    hash_duplicate_paths: list | None = None
    # True when the import run stages files on local disk first (copy mode
    # with the "Use local disk while processing" toggle on). This changes
    # what the post-ingest scan walks — the local staging root instead of
    # the real destination — which is why _import_without_new_files() is
    # only safe to assert in this mode (see its docstring).
    local_processing: bool = False
    # Optional API override for the preview tier. Normal pipeline runs leave
    # this unset so the previews substage uses the workspace-effective
    # preview_max_size setting. Explicit 0 means "serve originals"; the
    # previews substage no-ops.
    preview_max_size: int | None = None
    # Distinguishes the identify preset's species-only review path from a
    # generic ``skip_regroup=True`` run. When ``"species"`` the Group stage
    # is NOT skipped — the run enters ``regroup_stage``'s species branch and
    # writes species-review results to ``pipeline_results_ws*.json``. The
    # plan must reflect that work honestly instead of the "Disabled" pill
    # ``skip_regroup=True`` normally implies. Populated by
    # /api/pipeline/plan from the ``strategy`` (or explicit ``review_mode``)
    # in the request body — the same expansion ``/api/jobs/pipeline`` runs.
    review_mode: str | None = None


def _plural(n, s="s"):
    return s if n != 1 else ""


def _classify_would_run_after_auto_skip(params):
    """Mirror ``_apply_no_model_auto_skip`` (app.py) at plan time.

    ``/api/jobs/pipeline`` flips ``skip_classify`` (and by the same rule
    ``skip_regroup``) to True when the caller didn't already disable
    classify AND no downloaded/active model is available. The plan must
    apply the same degradation before promising downstream work — in
    particular the identify preset's species-review branch of
    ``_regroup_plan``, which returns ``will-run`` solely from
    ``review_mode="species"`` and would otherwise advertise
    "Will prepare species review" for a job that will only show the
    no-model warning and skip regroup.

    Returns True when classify would actually run at job time.
    """
    if params.skip_classify:
        return False
    from models import get_active_model, get_models

    requested_ids = list(params.model_ids or [])
    if requested_ids:
        by_id = {m["id"]: m for m in get_models()}
        return all(
            by_id.get(mid, {}).get("downloaded") for mid in requested_ids
        )
    return get_active_model() is not None


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
            # Needed by the label-free ToL check downstream so the planner
            # doesn't return a fingerprint that promises label-free
            # coverage for a model whose ToL artifacts aren't installed.
            "weights_path": m.get("weights_path"),
        })
    return out


def _resolve_labels_for_models(models, labels_files, db):
    """Per-model: resolve the labels list and its content fingerprint.

    Mirrors classify_job._load_labels' resolution order so the plan keys
    classifier_runs lookups by the *same* (model_name, fingerprint) pair the
    job would write. Returns: model_id -> {fingerprint, n, blocked?}.
    """
    from labels import get_active_labels, get_saved_labels, load_merged_labels
    from labels_fingerprint import TOL_SENTINEL, compute_fingerprint

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

    from models import tree_of_life_ready

    out = {}
    for m in models:
        if m["model_type"] == "timm":
            # timm models have an intrinsic, fixed class head. The runtime
            # computes the same sentinel by calling compute_fingerprint(None),
            # and the inventory page keys intrinsic timm coverage this way too.
            out[m["id"]] = {"fingerprint": TOL_SENTINEL, "n": 0}
        elif not labels:
            # tree_of_life_ready (not just supports_tree_of_life) so a
            # model whose ToL artifacts are declared optional and were
            # skipped at download time is treated as blocked in
            # label-free mode instead of returning a plan the pipeline
            # will crash executing.
            if tree_of_life_ready(m["model_str"], m.get("weights_path")):
                out[m["id"]] = {"fingerprint": TOL_SENTINEL, "n": 0}
            else:
                out[m["id"]] = {"fingerprint": None, "n": 0, "blocked": True}
        else:
            out[m["id"]] = {
                "fingerprint": compute_fingerprint(labels),
                "n": len(labels),
            }
    return out


def _import_without_new_files(params, photo_ids, new_count):
    """True when import mode is active but the run brings no per-photo work.

    Every selected file is already imported (``new_count == 0``, typically
    via the hash/metadata duplicate gate) and none of the known copies fall
    into scope at the selected paths (``photo_ids`` empty — the copies live
    at other, already-cataloged paths). The per-photo stages will all
    execute over an empty set, so their "Will run" summaries must say the
    run imports 0 new photos instead of implying work is coming.

    Only asserted for local-processing imports. In plain copy mode the
    claim isn't airtight: when ingest copies nothing (everything
    deduplicated), the post-ingest scan runs with ``restrict=None`` over
    the REAL destination tree, and ``scanner.scan`` fires the photo
    callback for existing cataloged rows there — so downstream
    workspace-scoped stages (classify/extract/regroup) can still find
    real work among previously-unprocessed destination photos. With
    local processing on, the post-ingest scan targets the local staging
    root instead, which stays empty when every file deduplicates (ingest
    creates the staging dir but copies nothing into it), so "nothing
    to …" is genuinely true. Copy mode keeps the pre-existing
    forward-looking summaries and lets Group see upstream will-run.
    """
    return (
        params.local_processing
        and params.source_paths is not None
        and new_count == 0
        and not photo_ids
    )


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

    det_counts = db.count_primary_detections_in_scope(photo_ids)
    total_dets = det_counts["total_dets"]
    photos_with_dets = det_counts["photos_with_dets"]

    unblocked_count = sum(
        1 for m in models if not label_resolution[m["id"]].get("blocked")
    )
    eligible = total_dets * unblocked_count

    # Every selected model is blocked on missing labels and can't run
    # label-free (Tree of Life). The classify stage cannot do any work for
    # ANY scope — including a fresh import with no detections cached yet, so
    # this must run BEFORE the total_dets == 0 early-return below. Surface a
    # distinct "blocked" state (not "will-run") so the UI can gate Start and
    # point the user at Settings > Labels, rather than letting the job crash
    # mid-pipeline at classify_job._load_labels (which is exactly the
    # fresh-install failure this guards against).
    if unblocked_count == 0:
        blocked_all = [m["name"] for m in models]
        return {
            "state": "blocked",
            "summary": (
                "Blocked — download a species list (Settings › Labels) "
                f"for: {', '.join(blocked_all)}"
            ),
            "detail": {
                "blocked_models": blocked_all,
                "pending": 0,
                "eligible": 0,
                "stale": 0,
                "fingerprint_outdated": False,
            },
        }

    stale_total = 0
    if total_dets > 0 and not params.reclassify:
        for m in models:
            info = label_resolution[m["id"]]
            if info.get("blocked"):
                continue
            fp = info["fingerprint"]
            stale_total += db.count_primary_classify_stale(
                classifier_model=m["name"],
                labels_fingerprint=fp,
                photo_ids=photo_ids,
            )
    # Reclassify is a user override, not a settings-change signal.
    fingerprint_outdated = stale_total > 0 and not params.reclassify
    fingerprint_reason = "label_set_changed" if fingerprint_outdated else None

    if total_dets == 0:
        # Mixed shape with no detections cached yet: some selected models
        # can run (label-free, or have labels) and others are blocked on
        # missing labels. The earlier unblocked_count==0 guard doesn't fire
        # here because at least one model is runnable, but classify_job
        # iterates every selected model and the blocked ones will fail at
        # _load_labels once MegaDetector creates detections. Emit "blocked"
        # (gates Start) instead of "will-run" with no blocked_models, so
        # the user fixes labels or deselects the blocked model before
        # launching — same failure this PR is meant to prevent.
        blocked_now = [
            m["name"] for m in models
            if label_resolution[m["id"]].get("blocked")
        ]
        if blocked_now:
            return {
                "state": "blocked",
                "summary": (
                    f"Blocked — {len(blocked_now)} model"
                    f"{_plural(len(blocked_now))} need labels: "
                    f"{', '.join(blocked_now)}"
                ),
                "detail": {
                    "blocked_models": blocked_now,
                    "total_dets": 0,
                    "photos_with_dets": 0,
                    "models": [m["name"] for m in models],
                    "pending": 0,
                    "eligible": 0,
                    "new_photos": new_count,
                    "stale": stale_total,
                    "fingerprint_outdated": fingerprint_outdated,
                    "fingerprint_reason": fingerprint_reason,
                },
            }
        import_no_new = _import_without_new_files(params, photo_ids, new_count)
        if new_count > 0:
            summary = (
                f"Will run — {new_count} new photo{_plural(new_count)} "
                f"to detect & classify (MegaDetector runs first)"
            )
        elif import_no_new:
            # All-duplicates import: MegaDetector will get 0 photos, so
            # "will run first" would falsely promise detection work.
            summary = (
                "Will run — 0 new photos to import, nothing to "
                "detect or classify"
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
                "fingerprint_reason": fingerprint_reason,
                "import_no_new": import_no_new,
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
            pending = db.count_primary_classify_pending_pairs(
                classifier_model=m["name"],
                labels_fingerprint=fp,
                photo_ids=photo_ids,
            )
        if pending:
            pending_per_model[m["name"]] = pending
            pending_total += pending

    if blocked:
        # Any selected model that's blocked on missing labels prevents
        # launching the stage: pipeline_job.classify_stage iterates every
        # selected resolved spec, and the blocked model fails at
        # classify_job._load_labels. Emit "blocked" (gates Start) whether or
        # not the other unblocked models have pending work — returning
        # "will-run" in the mixed pending case left Start enabled and let the
        # missing-labels failure through on launch. The user fixes labels or
        # deselects the blocked model before the rest can run. eligible=0
        # (not eligible>0 with pending=0, which would render "Resume (0
        # left)" against a stage that's actually blocked).
        return {
            "state": "blocked",
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
                "fingerprint_reason": fingerprint_reason,
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
                    "fingerprint_reason": fingerprint_reason,
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
                "fingerprint_reason": fingerprint_reason,
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
        if fingerprint_outdated:
            summary = (
                f"Current label set differs from cached classifications — "
                f"will classify {pending_total} "
                f"pair{_plural(pending_total)} ({breakdown})"
            )
        else:
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
        "fingerprint_reason": fingerprint_reason,
    }
    if new_count > 0:
        detail["new_photos"] = new_count
    return {"state": "will-run", "summary": summary, "detail": detail}


def _extract_plan(db, params, photo_ids, pipeline_cfg, new_count=0):
    if params.skip_extract_masks:
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
        }
    sam2_variant = pipeline_cfg.get("sam2_variant")
    counts = db.count_photos_pending_masks(
        photo_ids, sam2_variant=sam2_variant,
    )
    eligible = counts["eligible"]
    pending = counts["pending"]
    stale = db.count_extract_stale(sam2_variant, photo_ids)
    sam_warning = db.sam_variant_rerun_warning(sam2_variant, photo_ids)
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
        import_no_new = _import_without_new_files(params, photo_ids, new_count)
        if import_no_new:
            # All-duplicates import: classify has 0 photos to produce
            # detections from, so nothing will ever become eligible here.
            summary = "Will run — 0 new photos to import, nothing to extract"
        else:
            summary = (
                "Will run after classify produces detections "
                "(no eligible photos yet)"
            )
        return {
            "state": "will-run",
            "summary": summary,
            "detail": {"eligible": 0, "pending": 0,
                       "stale": 0, "fingerprint_outdated": False,
                       "import_no_new": import_no_new},
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
    # The two sets are disjoint by construction: pending includes missing or
    # incomplete selected-variant rows, while ``count_extract_stale`` requires
    # a complete active row. Without those gates, interrupted rows would land
    # in both buckets and inflate work past eligible.
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
    if sam_warning:
        detail["sam_variant_warning"] = sam_warning
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
        import_no_new = _import_without_new_files(params, photo_ids, new_count)
        if import_no_new:
            # All-duplicates import: upstream stages run over 0 photos, so
            # no masks or predictions are coming for this stage to consume.
            summary = "Will run — 0 new photos to import, nothing to process"
        else:
            summary = (
                "Will run after upstream produces masks + species "
                "predictions (no eligible photos yet)"
            )
        return {
            "state": "will-run",
            "summary": summary,
            "detail": {
                "eligible": 0,
                "pending": 0,
                "stale": stale,
                "fingerprint_outdated": stale > 0,
                "import_no_new": import_no_new,
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


def _previews_plan(db, params, photo_ids, new_count, effective_cfg):
    """Combined plan entry for the Thumbnails & Previews card.

    The card aggregates two pipeline substages (thumbnails + previews) so
    one plan entry has to answer for both. The accurate signal needs the
    active preview size (a 1280px library policy is genuinely "Already
    done" while the same library at 3840px isn't), so the planner consults
    an explicit ``preview_max_size`` API override when present, otherwise
    the workspace-effective config.

    States:
      - ``will-skip``: only when both substages no-op. Today that's
        impossible — thumbnails always runs — so we never emit this.
        ``preview_max_size=0`` skips just the previews substage; the
        thumbnails substage still has work to consider.
      - ``done-prior``: all eligible photos have a cached thumbnail and
        (if applicable) a preview at the configured size, no imports
        coming in.
      - ``will-run``: everything else, with a count of pending work.
    """
    thumb = db.count_photos_missing_thumb(photo_ids)
    eligible = thumb["eligible"]
    thumb_pending = thumb["pending"]

    raw_size = params.preview_max_size
    if raw_size is None:
        raw_size = effective_cfg.get("preview_max_size", 1920)
    try:
        preview_size = int(raw_size or 0)
    except (TypeError, ValueError):
        preview_size = 1920

    # preview_max_size=0 means "serve originals" — the previews substage
    # explicitly skips in pipeline_job.py:1192-1201, so don't count it
    # as work and don't surface a "X previews to generate" number.
    if preview_size == 0:
        previews_skipped = True
        preview_pending = 0
    else:
        previews_skipped = False
        prev = db.count_photos_missing_preview(preview_size, photo_ids)
        preview_pending = prev["pending"]

    if eligible == 0 and new_count == 0:
        # Empty scope. The stages will run (the loop is just empty), but
        # there's nothing to count. Be honest about that — and when the
        # emptiness comes from an all-duplicates import, say so outright:
        # "no photos in scope yet" reads as "photos are coming", but this
        # run will import 0 new photos.
        import_no_new = _import_without_new_files(params, photo_ids, new_count)
        if import_no_new:
            summary = (
                "Will run — 0 new photos to import, nothing to process"
                if not previews_skipped
                else "Will run for thumbnails — 0 new photos to import, "
                     "nothing to process (previews skipped: serves "
                     "originals at full resolution)"
            )
        else:
            summary = (
                "Will run — no photos in scope yet"
                if not previews_skipped
                else "Will run for thumbnails — no photos in scope (previews "
                     "skipped: serves originals at full resolution)"
            )
        return {
            "state": "will-run",
            "summary": summary,
            "detail": {
                "eligible": 0,
                "pending": 0,
                "thumb_pending": 0,
                "preview_pending": 0,
                "preview_size": preview_size,
                "previews_skipped": previews_skipped,
                "new_photos": 0,
                "import_no_new": import_no_new,
            },
        }

    if (
        thumb_pending == 0
        and preview_pending == 0
        and new_count == 0
    ):
        if previews_skipped:
            summary = (
                f"Thumbnails cached for all {eligible} "
                f"photo{_plural(eligible)} (previews skipped: serves "
                f"originals at full resolution)"
            )
        else:
            summary = (
                f"All {eligible} photo{_plural(eligible)} have cached "
                f"thumbnails and {preview_size}px previews"
            )
        return {
            "state": "done-prior",
            "summary": summary,
            "detail": {
                "eligible": eligible,
                "pending": 0,
                "thumb_pending": 0,
                "preview_pending": 0,
                "preview_size": preview_size,
                "previews_skipped": previews_skipped,
                "new_photos": 0,
            },
        }

    parts = []
    if thumb_pending > 0:
        parts.append(f"{thumb_pending} thumbnail{_plural(thumb_pending)}")
    if preview_pending > 0:
        parts.append(
            f"{preview_pending} {preview_size}px "
            f"preview{_plural(preview_pending)}"
        )
    if new_count > 0:
        # Imports add work for both substages: each new photo needs a
        # thumbnail (always) and a preview (unless skipped). Phrase as a
        # single addendum so we don't double-count "+ N new" twice.
        if previews_skipped:
            parts.append(
                f"up to {new_count} new photo{_plural(new_count)} "
                f"to thumbnail"
            )
        else:
            parts.append(
                f"up to {new_count} new photo{_plural(new_count)} "
                f"(thumbnail + preview each)"
            )

    if not parts:
        # Nothing pending in scope but we got here because new_count==0
        # was false above? Shouldn't happen — guard so a logic gap can't
        # render an empty pill summary.
        summary = (
            f"Will run — {eligible} photo{_plural(eligible)} in scope"
        )
    else:
        summary = "Will generate " + ", ".join(parts)
        if previews_skipped:
            # Make the no-op-previews case visible in the summary too. The
            # detail flag is enough for tests, but a user reading just the
            # pill+summary should also see why no previews are listed.
            summary += " (previews skipped: serves originals)"

    thumb_pending_total = thumb_pending + new_count
    preview_pending_total = (
        preview_pending + new_count if not previews_skipped else 0
    )
    # Photo-level "pending" for the pill: a photo missing only a thumb and
    # a different photo missing only a preview each count once toward "next
    # run will touch N photos." ``max(thumb_pending, preview_pending)``
    # undercounts whenever the two missing-sets aren't strict subsets of
    # each other (e.g. one photo just had its preview evicted but kept its
    # thumb, another only ever had a thumb generated). Use the SQL union
    # for the existing-scope contribution; new imports always need both
    # substages, so they add 1 photo each regardless of substage split.
    if previews_skipped:
        existing_pending = thumb_pending
    else:
        union = db.count_photos_missing_thumb_or_preview(
            preview_size, photo_ids,
        )
        existing_pending = union["pending"]
    photos_pending = existing_pending + new_count
    return {
        "state": "will-run",
        "summary": summary,
        "detail": {
            "eligible": eligible + new_count,
            "pending": photos_pending,
            "thumb_pending": thumb_pending_total,
            "preview_pending": preview_pending_total,
            "preview_size": preview_size,
            "previews_skipped": previews_skipped,
            "new_photos": new_count,
        },
    }


def _regroup_plan(db, params, db_path, ws_id, upstream_will_run, effective_cfg,
                  import_no_new=False):
    if params.skip_regroup:
        # The identify preset sets ``skip_regroup=True`` but flags
        # ``review_mode="species"``, and ``regroup_stage`` (pipeline_job.py)
        # then runs the species-review pipeline and overwrites
        # ``pipeline_results_ws*.json`` with review-only output. The stage is
        # NOT actually skipped — reporting "Disabled" would lie about the
        # work the next press performs (and would let the user think the
        # existing full-Group cache survives, when in reality identify wipes
        # ``last_group_fingerprint`` and rewrites the cache).
        #
        # But ``regroup_stage``'s species branch requires classify to
        # produce/have produced species predictions, and
        # ``/api/jobs/pipeline`` auto-flips ``skip_classify=True`` (and
        # ``skip_regroup=True``) when no downloaded/active model is
        # available. Without the same gate here, the plan promises
        # "Will prepare species review" on installs with no model where
        # the job will only surface the no-model warning and skip
        # regroup — the exact transparency failure the review flagged.
        if (
            params.review_mode == "species"
            and not params.skip_classify
            and not import_no_new
            and _classify_would_run_after_auto_skip(params)
        ):
            return {
                "state": "will-run",
                "summary": (
                    "Will prepare species review — no grouping/scoring"
                ),
                "detail": {"review_mode": "species"},
            }
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
        }
    cache_path = os.path.join(
        os.path.dirname(db_path), f"pipeline_results_ws{ws_id}.json",
    )
    cache_exists = os.path.exists(cache_path)
    if import_no_new:
        # All-duplicates local-processing import: ingest copies nothing into
        # the staging root, the post-ingest scan collects 0 photos, so the
        # job never creates a collection (collection_stage returns on
        # `if not collected_photo_ids`) and regroup_stage skips on
        # `not collection_id` (pipeline_job.py). The cache/fingerprint
        # checks below would promise a Group run the job cannot perform —
        # say the truth instead.
        return {
            "state": "will-skip",
            "summary": "Will skip — 0 new photos to import, nothing to group",
            "detail": {
                "cache_exists": cache_exists,
                "upstream_will_run": False,
                "import_no_new": True,
            },
        }
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
            "Previews": dict(no_op),
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


def _scan_plan(params, new_count, known_count, unlinked_folder_count,
               hash_dup_count=0):
    """Plan entry for Scan & Import — only emitted in import / new-images modes.

    Honest answer for the user's "what will this run do" question: how many
    files will it actually ingest (creating photo rows), how many were
    already in Vireo from a prior import (no-op for those), and — when
    every file is already known — whether the scan would still mutate the
    active workspace by linking previously-unseen folders to it.

    ``unlinked_folder_count`` is the count of distinct parent directories
    of the import set whose folder rows are not yet linked to the active
    workspace. ``scanner.scan`` calls ``_ensure_folder`` (which auto-links
    via ``add_folder``) for every walked directory, so a non-zero value
    means scan is *not* a no-op even when ``new_count == 0`` — it will
    insert ``workspace_folders`` rows. Reporting "Already done" in that
    case would lie about user-visible state changes (the photos becoming
    visible in this workspace), violating CORE_PHILOSOPHY.md's
    transparency rule.

    ``hash_dup_count`` covers a copy-mode-only failure mode: even when
    ``unlinked_folder_count == 0`` for the source paths (because we
    excluded hash-dup parents from that calculation — ingest will skip
    those source files, so source folders aren't walked), the post-ingest
    scan in ``run_pipeline_job`` walks the *destination* folders that hold
    the existing copies (and walks the entire destination root if those
    copies live elsewhere). That can still link destination folders to the
    active workspace and surface previously-unseen photos here. So we
    cannot claim "done-prior" when hash dups are in play, even if every
    other signal points to a no-op — the plan must report will-run with a
    summary that names the destination-side work the user is about to
    trigger.
    """
    total = new_count + known_count
    if new_count == 0 and unlinked_folder_count == 0 and hash_dup_count == 0:
        return {
            "state": "done-prior",
            "summary": (
                f"All {total} file{_plural(total)} already imported — "
                f"scan will be a no-op"
            ),
            "detail": {
                "eligible": total, "pending": 0,
                "new_photos": 0, "already_known": known_count,
                "unlinked_folders": 0,
                "hash_duplicates": 0,
            },
        }
    if new_count == 0:
        # All source files already known globally. Two reasons this still
        # isn't a no-op, either of which is enough to flip to will-run:
        #   - unlinked_folder_count > 0: source folder(s) not yet attached
        #     to the active workspace, scan will link them via add_folder.
        #   - hash_dup_count > 0: copy-mode hash dedup. ingest skips the
        #     source file but the post-ingest scan walks destination
        #     folders (or the destination root) to attach any duplicate
        #     destination folders here.
        if hash_dup_count > 0 and unlinked_folder_count > 0:
            summary = (
                f"All {total} file{_plural(total)} already imported "
                f"({hash_dup_count} hash-duplicate"
                f"{_plural(hash_dup_count)}) — scan will link "
                f"{unlinked_folder_count} source "
                f"folder{_plural(unlinked_folder_count)} and walk the "
                f"import destination for any duplicate folders"
            )
        elif hash_dup_count > 0:
            summary = (
                f"All {total} file{_plural(total)} already imported "
                f"({hash_dup_count} hash-duplicate"
                f"{_plural(hash_dup_count)}) — scan will walk the "
                f"import destination to link any duplicate folders to "
                f"this workspace"
            )
        else:
            summary = (
                f"All {total} file{_plural(total)} already imported "
                f"elsewhere — scan will link {unlinked_folder_count} "
                f"folder{_plural(unlinked_folder_count)} to this workspace"
            )
        return {
            "state": "will-run",
            "summary": summary,
            "detail": {
                "eligible": total,
                "pending": 0,
                "new_photos": 0,
                "already_known": known_count,
                "unlinked_folders": unlinked_folder_count,
                "hash_duplicates": hash_dup_count,
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
            "unlinked_folders": unlinked_folder_count,
            "hash_duplicates": hash_dup_count,
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
                "Previews": {state, summary, detail?},
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
    unlinked_folder_count = 0
    hash_dup_count = 0
    if params.photo_ids is not None:
        # Folder scope (Process page): ids were resolved from the folder
        # subtrees at the API layer.
        photo_ids = list(params.photo_ids)
    elif params.collection_id is not None:
        from pipeline import _resolve_collection_photo_ids
        photo_ids = _resolve_collection_photo_ids(db, params.collection_id)
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
        unique_path_set = set(unique_paths)
        known = db.photos_by_paths(unique_paths)
        photo_ids = set(known.values())
        known_at_path_set = set(known.keys())
        # Hash-dedup gate (copy-mode imports with skip_duplicates=True).
        # A file flagged here exists in the global photos table at a
        # different path; ingest() will skip the copy, so the next run
        # creates no new photo row for it and never touches the existing
        # one (the post-import scan walks only the destination tree).
        # Count it as "already known", not new. Defensive intersections:
        #   - ``& unique_path_set`` ignores stale cache entries (the
        #     frontend's ``_duplicateResults`` survives source-folder edits
        #     until the next check completes; a path no longer in the
        #     selection must not influence counts).
        #   - subtract ``known_at_path_set`` so a file that's both an
        #     exact-path match and a hash match isn't double-counted in
        #     ``known_count``.
        hash_dup_paths = (
            set(params.hash_duplicate_paths or [])
            & unique_path_set
        ) - known_at_path_set
        known_count = len(known) + len(hash_dup_paths)
        new_count = len(unique_paths) - known_count
        hash_dup_count = len(hash_dup_paths)
        # photos_by_paths is global — a path is "known" even when the
        # photo's folder belongs to a different workspace. scanner.scan
        # would still attach those folders to the active workspace via
        # add_folder/_ensure_folder, so a "done-prior" Scan claim would
        # be false in that case. Count parent dirs not yet linked here.
        # Hash-dup paths are excluded: ingest skips those files, so their
        # parent dirs aren't walked in copy mode and we mustn't claim
        # scan would link them.
        unlinked_folder_count = db.workspace_unlinked_folder_count(
            {os.path.dirname(p) for p in unique_paths if p not in hash_dup_paths}
        )

    # Exclusions apply in EVERY mode — the running job filters excluded ids
    # in every stage, so a plan that only honored them for collections
    # overstated the pending counts (the proxy drift this module forbids).
    # Whole-workspace scope materializes the id set first so the same
    # subtraction works; _scope_clause stages large sets in a temp table.
    if params.exclude_photo_ids:
        excl = set(params.exclude_photo_ids)
        if photo_ids is None:
            photo_ids = set(db.get_photo_ids())
        photo_ids = {pid for pid in photo_ids if pid not in excl}

    classify = _classify_plan(db, params, photo_ids, new_count)
    extract = _extract_plan(db, params, photo_ids, pipeline_cfg, new_count)
    eye = _eye_keypoints_plan(db, params, photo_ids, pipeline_cfg, new_count)
    previews = _previews_plan(db, params, photo_ids, new_count, effective_cfg)
    # An all-duplicates import leaves every upstream stage "will-run" over
    # an empty photo set — that is not new work, and letting it force the
    # Group stage into "upstream stages have new work to do" would be a
    # false claim. It also means the job never builds a collection, so
    # regroup_stage itself will skip — _regroup_plan reports "Will skip"
    # instead of falling through to its cache/fingerprint check.
    import_no_new = _import_without_new_files(params, photo_ids, new_count)
    upstream_will_run = (
        not import_no_new
        and any(s["state"] == "will-run" for s in (classify, extract, eye))
    )
    regroup = _regroup_plan(
        db, params, db_path, ws_id, upstream_will_run, effective_cfg,
        import_no_new=import_no_new,
    )

    stages = {
        "Previews": previews,
        "Classify": classify,
        "Extract": extract,
        "EyeKeypoints": eye,
        "Group": regroup,
    }
    if params.source_paths is not None:
        stages["Scan"] = _scan_plan(
            params, new_count, known_count, unlinked_folder_count,
            hash_dup_count,
        )

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
