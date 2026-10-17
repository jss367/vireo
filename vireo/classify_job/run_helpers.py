"""Helpers of the classify job run: empty results, the cache peek, heal re-arming."""

from dataclasses import dataclass, field

from classify_job.common import log


def _empty_classify_result(total, detected=0):
    """The result of a run that stored no predictions."""
    return {
        "total": total,
        "predictions_stored": 0,
        "burst_groups": 0,
        "already_classified": 0,
        "already_labeled": 0,
        "detected": detected,
        "failed": 0,
    }


def _rearm_label_desc_heal(peek_model):
    """Re-arm any pending background self-heal.

    Called before the cached-only shortcut returns. The shortcut skips classifier acquisition entirely, so
    ``acquire_cached_classifier``'s ``notify_reuse`` hook — the only place
    the timm ``label_descriptions.json`` bounded-retry state machine fires
    from cache hits — never runs on such jobs. Without this, an
    installation whose first heal failed during a transient outage would
    stay ``failed`` for the rest of the process's life whenever every
    job's photos are already cached, leaking raw scientific names
    indefinitely. The helper checks the file itself and no-ops when the
    mapping is already usable or the state machine is in-flight/backing
    off, so calling it here is cheap on healthy installs.
    """
    if peek_model and peek_model.get("model_type") == "timm":
        _heal_model_str = peek_model.get("model_str")
        _heal_model_dir = peek_model.get("weights_path")
        if _heal_model_str and _heal_model_dir:
            try:
                from timm_classifier import (
                    rearm_pending_label_desc_heal,
                )
                rearm_pending_label_desc_heal(
                    _heal_model_str, _heal_model_dir,
                )
            except Exception:
                log.info(
                    "Re-arming label_descriptions heal for %s failed",
                    _heal_model_str, exc_info=True,
                )


@dataclass
class _CachePeek:
    """What the cached-only shortcut checks a run's cached results against."""

    peek_model: dict | None = None
    desired_classifier_model: str | None = None
    # ``model_id_missing`` distinguishes "the requested model really is
    # gone from the catalog" (a stale job referencing a deleted model)
    # from a transient ``get_models()`` failure. The catch-all in
    # ``_peek_requested_classifier`` nulls out the transient case; a bare
    # miss stays flagged so we can raise the definitive error before the
    # cache shortcut.
    model_id_missing: bool = False
    desired_labels_fingerprint: str | None = None
    desired_labels_fingerprint_full: str | None = None
    peek_labels: list | None = None
    peek_use_tol: bool = False
    peek_label_metas: list[dict] = field(default_factory=list)
    peek_succeeded: bool = False
    desired_model_identity: dict | None = None
    detector_confidence: float | None = None
    taxonomy_identity: str | None = None
