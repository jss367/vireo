"""Subject-mask workflows: the active variant, stale cleanup, stage scopes.

Each function composes the workspace-effective ``detector_confidence``
(through ``Database.get_effective_config``), the photo scope where one
applies (``StatsRepository.scope_clause``) and the ``db.masks_features``
read or write that runs the SQL. Functions that read config or a scope check
for an active workspace first, so with none they raise ``RuntimeError``
before reading config or staging a scope.

``Database._safe_remove_mask_file`` (the masks-directory containment check
for file deletes) stays on ``Database``: the masks repository receives it as
a callback when it is built.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import config as cfg

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from db import Database
    from services.service_types import JsonDict


def set_active_mask_variant(
    db: Database, photo_id: int, variant: str, _commit: bool = True, *,
    weak_rescue_min_conf: float | None = None,
) -> None:
    """Mark `variant` as active for `photo_id` and denormalize its
    fields into the photos row (mask_path + per-mask features) so
    downstream readers (scoring, pipeline) see the active mask.

    ``_commit=False`` lets bulk callers (e.g. the
    ``/api/pipeline/active-mask-variant`` endpoint) batch many
    per-photo updates into a single commit, instead of paying a WAL
    fsync per photo. Bulk callers MUST call ``commit_with_retry``
    themselves once the loop completes.
    """
    effective = db.get_effective_config(cfg.load())
    min_conf = effective.get("detector_confidence", 0.2)
    db.masks_features.set_active_variant(
        photo_id, variant, min_conf, _commit,
        weak_rescue_min_conf=weak_rescue_min_conf,
    )


def delete_stale_masks(db: Database, detector_confidence: float | None = None) -> int:
    """Remove rows + files for masks whose prompt no longer matches
    the current primary detection. Skips active variants (caller can
    re-run them through the pipeline instead of dropping the
    currently-displayed mask).

    ``detector_confidence`` is forwarded to ``find_stale`` so the
    deletion set matches the count the storage card shows.
    """
    repo = db.masks_features
    stale = repo.find_stale(detector_confidence=detector_confidence)
    return repo.delete_stale(stale)


def sam_variant_rerun_warning(
    db: Database,
    sam2_variant: str | None,
    photo_ids: Iterable[int] | None = None,
    min_conf: float | None = None,
    selected_max_ratio: float = 0.25,
    alternate_min_ratio: float = 0.80,
) -> JsonDict | None:
    """Warn when selected SAM coverage is poor but another variant is high.

    The target set matches the extract-masks stage's existing-photo
    eligibility: active-workspace photos in scope with at least one real
    detection above the workspace detector-confidence floor. This keeps a
    workspace-level SAM configuration from looking empty just because a
    different variant already produced masks for those same target photos.
    """
    if not sam2_variant or sam2_variant == "unknown":
        return None
    floor: float = (
        min_conf if min_conf is not None
        else db.get_effective_config(cfg.load()).get("detector_confidence", 0.2)
    )

    # Resolve the workspace before staging the photo scope, as the
    # eagerly scoped repository did.
    db.require_workspace_id()
    repo = db.masks_features
    scope_sql, scope_params = db.stats.scope_clause(photo_ids)
    return repo.sam_variant_rerun_warning(
        sam2_variant, floor, scope_sql, scope_params,
        selected_max_ratio=selected_max_ratio,
        alternate_min_ratio=alternate_min_ratio,
    )


def get_photos_missing_masks(
    db: Database, folder_ids: Sequence[int] | None = None,
) -> list[JsonDict]:
    """Get photos that have detections but no masks yet.

    Returns photos that have at least one detection in the current workspace
    but no mask_path set. Each row includes the selected primary
    detection box.

    Args:
        folder_ids: optional list of folder IDs to filter by.
                    If None, returns all workspace photos without masks.
    Returns:
        list of dicts with id, folder_id, filename, detection_box (JSON string), detection_conf
    """
    # Resolve the workspace before reading the config, as the eagerly
    # scoped repository did.
    db.require_workspace_id()
    repo = db.masks_features
    min_conf = db.get_effective_config(cfg.load()).get(
        "detector_confidence", 0.2
    )
    return repo.photos_missing_masks(folder_ids, min_conf)


def list_photos_for_eye_keypoint_stage(
    db: Database, photo_ids: Iterable[int] | None = None,
) -> list[dict[str, Any]]:
    """Return photos eligible for the eye-focus keypoint stage.

    Eligibility:
      * photo is in the active workspace (via workspace_folders)
      * mask_path is set (SAM2 produced a subject mask)
      * has at least one non-synthetic detection (excludes full-image
        rows that exist only to anchor predictions)
      * has at least one prediction on that detection
      * has not been attempted with the current keypoint model fingerprint
        yet. The stage stamps eye_kp_fingerprint even when no trustworthy
        eye is found, so no-eye photos do not rerun forever.
      * (optional) photo.id is in ``photo_ids`` when provided — lets the
        caller scope the stage to a collection so a pipeline run doesn't
        touch unrelated photos elsewhere in the workspace

    Returns one row per photo. Only the effective primary detection
    can supply eye predictions, resolved with the same
    threshold-aware ordering (``subjects.primary_order_sql``) that
    mask extraction and ``set_active_mask_variant`` use: user
    override in ``photo_subject_choices`` → subject-analysis quality
    score → detector confidence. Anchoring on the current floor
    (not the cached ``photo_subject_state``) means a workspace
    raising ``detector_confidence`` above the previously stored
    subject still surfaces the new primary for the eye stage.
    Within the chosen detection, predictions carrying routable
    taxonomy info (``taxonomy_class`` or ``scientific_name`` set)
    rank ahead of predictions missing both, so a top-confidence but
    taxonomy-less prediction never masks a routable one that
    ``_resolve_keypoint_model`` could actually run. Each row is a
    dict with the fields the eye stage needs to run without further
    DB calls: id, folder_id, filename, width, height, mask_path,
    box_x/y/w/h (normalized 0-1), species_conf, taxonomy_class,
    scientific_name, species.
    """
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    # Resolve the workspace before reading the config or staging the
    # photo scope (an empty scope returns early), as the eagerly scoped
    # repository did.
    db.require_workspace_id()
    repo = db.masks_features
    min_conf = db.get_effective_config(cfg.load()).get(
        "detector_confidence", 0.2
    )
    if photo_ids is not None:
        photo_ids = list(photo_ids)
        if not photo_ids:
            return []
    extra_where, scope_params = db.stats.scope_clause(photo_ids)
    return repo.list_photos_for_eye_keypoint_stage(
        min_conf, extra_where, scope_params,
        eye_kp_fingerprint_version=EYE_KP_FINGERPRINT_VERSION,
    )
