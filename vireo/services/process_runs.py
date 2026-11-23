"""What a Process run would cover, so a caller can start exactly the run it described.

``describe_run`` goes out with the plan. It names what decides a run's outcome
and can change between describing it and starting it: the workspace, the photos,
the models, the species lists, the stage flags and settings, and the review
decisions a re-classify would discard. ``/api/jobs/pipeline`` takes the
workspace and photo digest back as ``expected_run`` and refuses a run whose
scope is no longer the one described.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from config import config_value
from services import workspace_config

if TYPE_CHECKING:
    from db import Database
    from pipeline_plan import PipelinePlanParams
    from services.service_types import JsonDict

#: The settings a run reads from the workspace when it starts.
_SETTINGS = ("sam2_variant", "dinov2_variant", "proxy_longest_edge")


def photo_digest(photo_ids: Iterable[int]) -> str:
    """A fingerprint of a set of photo ids, whatever their order."""
    text = ",".join(str(pid) for pid in sorted(set(photo_ids)))
    return hashlib.sha256(text.encode()).hexdigest()


def expected_run_error(expected: object, workspace_id: int | None, photo_ids: Iterable[int]) -> tuple[str, int] | None:
    """Why a run over ``photo_ids`` is not the ``expected`` one: ``(message, status)``, or None."""
    if (
        not isinstance(expected, dict)
        or not isinstance(expected.get("photo_digest"), str)
        or isinstance(expected.get("workspace_id"), bool)
        or not isinstance(expected.get("workspace_id"), int)
    ):
        return "expected_run needs an integer workspace_id and a photo_digest string", 400
    if expected["workspace_id"] != workspace_id:
        return "The active workspace changed since this run was described", 409
    if expected["photo_digest"] != photo_digest(photo_ids):
        return "The photos in scope changed since this run was described", 409
    return None


def _label_files(db: Database, params: PipelinePlanParams) -> list[str]:
    """The species lists a run would read: its own, the workspace's, or the global ones."""
    if params.labels_files:
        return list(params.labels_files)
    files = workspace_config.get_workspace_active_labels(db)
    if files is not None:
        return [str(path) for path in files]
    from labels import get_active_labels

    return [entry["labels_file"] for entry in get_active_labels()]


def describe_run(
    db: Database, params: PipelinePlanParams, photo_ids: Iterable[int], miss_enabled: bool | None,
) -> JsonDict:
    """The parts of a run over ``photo_ids`` that must not change before it starts.

    ``miss_enabled`` is the run's own switch; None defers to the workspace, as
    the misses stage does. ``replaced_reviews`` counts accepted and rejected
    predictions, in every workspace, that a re-classify may discard. Species
    keywords those decisions saved stay on the photos.
    """
    import config as cfg
    from pipeline_plan import run_models

    effective = db.get_effective_config(cfg.load())
    pipeline_cfg: dict[str, Any] = effective.get("pipeline", {})
    ids = sorted(set(photo_ids))
    models, fingerprints = run_models(db, params)
    reviews = {"predictions": 0, "photos": 0}
    if params.reclassify and models:
        reviews = db.model_runs.count_reviewed_predictions(ids)
    misses = pipeline_cfg.get("miss_enabled", True) if miss_enabled is None else miss_enabled
    return {
        "workspace_id": db.require_workspace_id(),
        "photo_count": len(ids),
        "photo_digest": photo_digest(ids),
        "models": models,
        "labels_files": _label_files(db, params),
        "label_fingerprints": fingerprints,
        "settings": {
            **{key: pipeline_cfg.get(key) for key in _SETTINGS},
            "detector_confidence": config_value(effective, "detector_confidence"),
        },
        "reclassify": params.reclassify,
        "misses": bool(misses) and bool(models) and not params.skip_regroup,
        "replaced_reviews": reviews,
    }

