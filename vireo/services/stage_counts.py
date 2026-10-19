"""The pipeline plan's per-stage counters over the active workspace.

Each counter answers one stage's "how much work is left" question for the
Pipeline page: classifiable detections, pending and stale classifier runs
(per detection, per primary detection and per full-image fallback), pending
and stale masks, missing thumbnails and previews, and eye-keypoint
eligibility. A counter composes three things: the workspace-effective
``detector_confidence`` (read through ``Database.get_effective_config`` when
the caller passes no floor), the photo scope (``StatsRepository.scope_clause``,
which stages a large scope in a temp table), and the ``StatsRepository`` read
that runs the SQL.

Every counter checks for an active workspace first, so with none it raises
``RuntimeError`` before it reads config or stages a scope.
``get_classification_inventory`` takes an explicit workspace instead and
reads that workspace's effective config.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import config as cfg

if TYPE_CHECKING:
    from collections.abc import Iterable

    from db import Database
    from repositories.stats import StatsRepository
    from services.service_types import JsonDict


def _scoped_stats(db: Database) -> StatsRepository:
    """The stats repository, once a workspace is known to be active."""
    db.require_workspace_id()
    return db.stats


def _detector_confidence(db: Database, min_conf: float | None) -> float:
    """``min_conf``, or the workspace-effective ``detector_confidence``."""
    if min_conf is not None:
        return min_conf
    return db.get_effective_config(cfg.load()).get("detector_confidence", 0.2)

def count_real_detections_in_scope(
    db: Database, photo_ids: Iterable[int] | None = None, min_conf: float | None = None,
) -> dict[str, int]:
    """Count (photos_with_real_dets, total_real_dets) for the workspace.

    "Real" excludes detector_model='full-image' synthetic anchors.
    ``photo_ids`` scopes to a collection (set/list of ids); None = whole
    workspace.

    Used by the pipeline plan to compute classify scope.
    """
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, min_conf)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_real_detections_in_scope(min_conf, scope_sql, scope_params)


def count_primary_detections_in_scope(
    db: Database, photo_ids: Iterable[int] | None = None, min_conf: float | None = None,
) -> dict[str, int]:
    """Count photos whose primary real detection is pipeline-classifiable.

    The streaming pipeline classifies at most one detection per photo: the
    highest-confidence non-full-image detection above the active threshold.
    This mirrors that gate for the Pipeline page plan so secondary boxes
    do not inflate pending classify work.
    """
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, min_conf)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_primary_detections_in_scope(min_conf, scope_sql, scope_params)


def count_classify_pending_pairs(
    db: Database, classifier_model: str, labels_fingerprint: str,
    photo_ids: Iterable[int] | None = None, min_conf: float | None = None,
) -> int:
    """Count detections in scope that lack a classifier_runs row for
    (classifier_model, labels_fingerprint).

    Mirrors the gate in classify_job._classify_photos: a real detection
    with no row in classifier_runs for the given (model, fp) is one
    unit of pending work for the next classify run.
    """
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, min_conf)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_classify_pending_pairs(
        classifier_model, labels_fingerprint, min_conf, scope_sql,
        scope_params,
    )


def count_primary_classify_pending_pairs(
    db: Database, classifier_model: str, labels_fingerprint: str,
    photo_ids: Iterable[int] | None = None, min_conf: float | None = None,
) -> int:
    """Count primary detections lacking a classifier run for (model, fp).

    Mirrors pipeline_job.classify_stage, which picks one primary detection
    per photo rather than classifying every detection row.
    """
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, min_conf)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_primary_classify_pending_pairs(
        classifier_model, labels_fingerprint, min_conf, scope_sql,
        scope_params,
    )


def count_classify_stale(
    db: Database, classifier_model: str, labels_fingerprint: str,
    photo_ids: Iterable[int] | None = None, min_conf: float | None = None,
) -> int:
    """Count detections in scope that have a stale classifier_runs row
    for ``classifier_model`` (some non-current fingerprint) AND no row
    matching the current ``labels_fingerprint``.

    A detection with a current-fp row is "done" (not stale). A
    detection with no row at all is "never processed" (counted by
    :meth:`count_classify_pending_pairs`, not here). The stale set is
    their disjoint complement: previously processed under settings
    that no longer match.
    """
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, min_conf)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_classify_stale(
        classifier_model, labels_fingerprint, min_conf, scope_sql,
        scope_params,
    )


def count_primary_classify_stale(
    db: Database, classifier_model: str, labels_fingerprint: str,
    photo_ids: Iterable[int] | None = None, min_conf: float | None = None,
) -> int:
    """Count stale classifier runs on primary detections only."""
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, min_conf)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_primary_classify_stale(
        classifier_model, labels_fingerprint, min_conf, scope_sql,
        scope_params,
    )


def count_full_image_fallback_photos(
    db: Database, photo_ids: Iterable[int] | None = None,
    detector_model: str = "megadetector-v6", min_conf: float = 0,
) -> int:
    """Count photos eligible for full-image fallback classification.

    These are photos in the active workspace where the detector has run
    and produced no box the classifier could act on: either
    ``box_count = 0`` or every non-full-image row is below ``min_conf``
    (raw noise). Callers that want to mirror the runtime fallback gate
    pass the workspace's ``detector_confidence``; the default of ``0``
    preserves the pre-noise-fallback semantics where any real row
    disqualified the photo.
    """
    repo = _scoped_stats(db)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_full_image_fallback_photos(
        detector_model, min_conf, scope_sql, scope_params,
    )


def count_full_image_classify_pending_pairs(
    db: Database, classifier_model: str, labels_fingerprint: str,
    photo_ids: Iterable[int] | None = None, detector_model: str = "megadetector-v6",
    min_conf: float = 0,
) -> int:
    """Count fallback photos lacking a classifier run for (model, fp).

    ``min_conf`` mirrors the runtime fallback gate: a photo qualifies
    when no non-full-image detection row is at or above the threshold,
    so a MegaDetector run that produced only noise (< ``min_conf``)
    counts alongside truly empty ``box_count = 0`` runs.
    """
    repo = _scoped_stats(db)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_full_image_classify_pending_pairs(
        classifier_model, labels_fingerprint, detector_model, min_conf,
        scope_sql, scope_params,
    )


def count_full_image_classify_stale(
    db: Database, classifier_model: str, labels_fingerprint: str,
    photo_ids: Iterable[int] | None = None, detector_model: str = "megadetector-v6",
    min_conf: float = 0,
) -> int:
    """Count fallback anchors with stale runs and no current run.

    ``min_conf`` mirrors ``count_full_image_fallback_photos`` so
    photos whose only detections are noise (< ``min_conf``) join the
    stale-anchor scope alongside the ``box_count = 0`` cases.
    """
    repo = _scoped_stats(db)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_full_image_classify_stale(
        classifier_model, labels_fingerprint, detector_model, min_conf,
        scope_sql, scope_params,
    )


def count_photos_pending_masks(
    db: Database, photo_ids: Iterable[int] | None = None, min_conf: float | None = None,
    sam2_variant: str | None = None,
) -> dict[str, int]:
    """Return (pending, eligible) for the extract-masks stage.

    eligible = photos in scope with at least one real detection above the
        workspace's effective detector_confidence
    pending  = eligible photos whose mask_path IS NULL, or when
        ``sam2_variant`` is supplied, whose ``photo_masks`` row for that
        variant is missing/incomplete

    The variant-aware mode mirrors extract_masks_stage's current per-photo
    cache check: masks made by another SAM variant do not make the selected
    variant complete.
    """
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, min_conf)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_photos_pending_masks(
        min_conf, sam2_variant, scope_sql, scope_params,
    )


def count_photos_missing_thumb(
    db: Database, photo_ids: Iterable[int] | None = None,
) -> dict[str, int]:
    """Return (eligible, pending) for the thumbnails substage.

    eligible = photos in scope linked to the active workspace
    pending  = eligible photos whose ``thumb_path IS NULL``

    ``thumbnail_stage``'s per-photo gate is ``os.path.exists`` on
    the cache file, but the photos.thumb_path column is the fast
    proxy: app.py's startup backfill aligns the column with disk
    reality (populates it for legacy rows that already have files,
    clears it for rows whose file has since been deleted), so a
    NULL value is a reliable "needs generating" signal.
    """
    repo = _scoped_stats(db)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_photos_missing_thumb(scope_sql, scope_params)


def count_photos_missing_preview(
    db: Database, size: int, photo_ids: Iterable[int] | None = None,
) -> dict[str, int]:
    """Return (eligible, pending) for the previews substage at ``size``.

    eligible = photos in scope linked to the active workspace
    pending  = eligible photos with no ``preview_cache`` row at ``size``

    ``previews_stage`` gates on ``os.path.exists`` of the cache
    file, but writes (or refreshes) a ``preview_cache`` row for
    every photo it processes — whether already-cached or freshly
    generated. Eviction (``preview_cache_max_mb``) deletes the
    file and the row together. So the table is a reliable index
    for "preview present on disk at this size".
    """
    repo = _scoped_stats(db)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_photos_missing_preview(size, scope_sql, scope_params)


def count_photos_missing_thumb_or_preview(
    db: Database, size: int, photo_ids: Iterable[int] | None = None,
) -> dict[str, int]:
    """Return (eligible, pending) where ``pending`` counts photos
    missing a thumbnail OR a preview at ``size`` (or both) — i.e.
    the union of the two substages' work sets.

    The Thumbnails & Previews card needs the photo-level union for
    its "Resume (N left)" framing: a photo missing only a thumb and
    a different photo missing only a preview each represent one
    photo the next pipeline run will touch. Falling back to
    ``max(thumb_pending, preview_pending)`` undercounts whenever
    the two missing-sets aren't strict subsets of each other.
    """
    repo = _scoped_stats(db)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_photos_missing_thumb_or_preview(size, scope_sql, scope_params)


def count_extract_stale(
    db: Database, sam2_variant: str | None, photo_ids: Iterable[int] | None = None,
    detector_confidence: float | None = None,
) -> int:
    """Count photos in scope that "look done" (``photos.mask_path``
    is set) but whose ``photo_masks`` row for ``sam2_variant`` has a
    stored prompt that no longer matches the photo's primary
    detection.

    Reuses the staleness predicate from ``masks_features.find_stale`` — a
    mask is fresh only when its stored ``(detector_model,
    prompt_xywh)`` equals the selected non-full-image
    detection on the same photo (with optional ``detector_confidence``
    floor). Filtered by ``sam2_variant`` so a stale mask under a
    different variant doesn't pollute the count for the currently
    configured variant.

    Photos without a current primary detection (no non-full-image
    detection at or above ``detector_confidence``) are excluded:
    they aren't eligible for the extract stage, so a leftover
    ``photo_masks`` row from a prior detector run isn't "stale work
    to redo" — it's just an orphan that storage cleanup handles.
    Counting those would inflate ``detail.stale`` and keep the
    stage flagged Outdated/Will run forever in mixed workspaces.

    The active-mask and variant-path gates keep this count disjoint from
    ``count_photos_pending_masks``'s ``pending``. Photos with no active
    mask, no selected-variant row, or an incomplete selected-variant path
    are already pending — they will be re-extracted regardless of whether
    their ``photo_masks`` row's prompt matches — so counting them here
    would double-count when a planner combines ``pending + stale`` as
    total work.
    """
    repo = _scoped_stats(db)
    detector_confidence = _detector_confidence(db, detector_confidence)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_extract_stale(
        sam2_variant, detector_confidence, scope_sql, scope_params,
    )


def count_eye_keypoint_eligible(
    db: Database, photo_ids: Iterable[int] | None = None,
) -> int:
    """Count photos eligible for the eye-keypoint stage, ignoring the
    ``eye_tenengrad IS NULL`` idempotency gate.

    Eligibility = active mask present for the currently-selected
    primary detection + at least one prediction on that detection.
    Mirrors ``list_photos_for_eye_keypoint_stage``'s selected-primary
    + active-mask predicates minus the "not yet processed" filter,
    so the plan can distinguish "no eligible photos" from "all
    eligible photos already processed". Counting on the loose
    mask+detection+prediction join would include photos whose only
    prediction sits on a non-primary detection — the stage cannot
    produce eye keypoints for those, so review readiness would
    repeatedly flag missing keypoints while the plan reported the
    stage complete (Codex r4056621190).
    """
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, None)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_eye_keypoint_eligible(min_conf, scope_sql, scope_params)


def count_eye_keypoint_stale(
    db: Database, photo_ids: Iterable[int] | None = None,
) -> int:
    """Count photos in scope whose eye_tenengrad is set under a
    non-current eye_kp_fingerprint. Mirrors
    ``count_eye_keypoint_eligible``'s selected-primary + active-mask
    join shape and adds the staleness predicate.

    A NULL fingerprint on a row with eye_tenengrad set is treated as
    stale — only the migration backfill should produce that state,
    and even there the user is expected to re-run after a model
    change to restamp.
    """
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, None)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_eye_keypoint_stale(min_conf, scope_sql, scope_params)


def count_eye_keypoint_attemptable(
    db: Database, min_species_conf: float, photo_ids: Iterable[int] | None = None,
) -> int:
    """Count photos whose top-routable prediction would actually be
    attempted by the eye-keypoint stage under the current config.

    Tighter than ``count_eye_keypoint_eligible``: eligible photos
    include ones whose selected primary detection's top prediction
    will be skipped at Gate 1 (classifier confidence below
    ``min_species_conf``) or fail taxonomy routing (anything outside
    the keys of ``pipeline._EYE_KEYPOINT_MODEL_FOR_CLASS``). Those
    photos never get an ``eye_kp_fingerprint`` stamped — by design,
    so a future config change can retry them — so they would
    permanently inflate ``eye_target`` and trip the "computed
    without eye keypoints" banner on every run.

    Match ``list_photos_for_eye_keypoint_stage``'s selected-primary
    + active-mask predicates and its "best routable row per photo"
    selection so a taxonomy-bearing prediction wins over a
    taxonomy-less one on the *same* selected detection (Codex
    r4056621190). Predictions that route only via the scientific
    name → taxa-table fallback are *not* counted here (the SQL
    filter is taxonomy_class-only); those photos will still be
    attempted by the stage but will be undercounted in the target,
    which keeps ``attempts >= target`` and means the banner won't
    lie — at worst it stays quiet when it could have surfaced.
    """
    repo = _scoped_stats(db)
    min_conf = _detector_confidence(db, None)
    scope_sql, scope_params = repo.scope_clause(photo_ids)
    return repo.count_eye_keypoint_attemptable(
        min_species_conf, min_conf, scope_sql, scope_params,
    )


def get_classification_inventory(
    db: Database, workspace_id: int, min_conf: float | None = None,
    median_sample_per_pair: int = 2000,
) -> JsonDict:
    """Per-(model × fingerprint) coverage stats for ``workspace_id``.

    Returns::

        {
          "total_real_detections": int,
          "pairs": [
            {
              "classifier_model": str,
              "labels_fingerprint": str,
              "classified_dets": int,
              "photos_covered": int,
              "last_run": str | None,
              "predictions_count": int,
              "median_top1_conf": float | None,
              "median_sample_size": int,
            }
          ],
          "total_predictions_rows": int,
        }

    Caller (the endpoint) is responsible for joining this against the
    on-disk model registry / label files to identify never-run, stale,
    and legacy combinations.
    """
    if min_conf is None:
        saved_active = db.active_workspace_id
        try:
            db.set_active_workspace(workspace_id)
            min_conf = _detector_confidence(db, None)
        finally:
            db.set_active_workspace(saved_active)

    total_real_detections, pair_rows, pred_counts = (
        db.stats.classification_inventory_counts(workspace_id, min_conf)
    )

    # Median top-1 confidence per pair, via a sampled top-1-per-detection set.
    # Bounded by median_sample_per_pair to keep total work small.
    medians = db.stats.sampled_top1_medians(
        workspace_id, min_conf, median_sample_per_pair,
    )

    pairs = []
    for r in pair_rows:
        key = (r["classifier_model"], r["labels_fingerprint"])
        med, sample_size = medians.get(key, (None, 0))
        pairs.append({
            "classifier_model": r["classifier_model"],
            "labels_fingerprint": r["labels_fingerprint"],
            "classified_dets": r["classified_dets"] or 0,
            "photos_covered": r["photos_covered"] or 0,
            "last_run": r["last_run"],
            "predictions_count": pred_counts.get(key, 0),
            "median_top1_conf": med,
            "median_sample_size": sample_size,
        })

    total_pred_rows = sum(pred_counts.values())

    return {
        "total_real_detections": total_real_detections,
        "pairs": pairs,
        "total_predictions_rows": total_pred_rows,
    }
