"""What a Process run would cover, so a caller can start exactly the run it described.

``describe_run`` goes out with the plan. It names what decides a run's outcome
and can change before the run begins: the workspace, the photos, the models, the
species lists and their contents, the stage settings, and the review decisions a
re-classify would discard. ``/api/jobs/pipeline`` takes it back as
``expected_run``. The route compares it with the run it is about to queue, and
the worker compares it again before its first stage, since a queued run can wait
hours.
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

#: What each part of a description is called in a refusal.
_PART_NAMES = {
    "workspace_id": "the active workspace",
    "photo_count": "the photos in scope",
    "photo_digest": "the photos in scope",
    "models": "the models",
    "labels_files": "the species lists",
    "label_fingerprints": "the species lists",
    "settings": "the stage settings",
    "reclassify": "re-classify",
    "misses": "whether misses are marked",
    "replaced_reviews": "the reviewed predictions it would replace",
}


class RunChangedError(RuntimeError):
    """A queued run is no longer the one that was described; nothing ran."""


def _digest(parts: Iterable[str]) -> str:
    return hashlib.sha256(",".join(parts).encode()).hexdigest()


def photo_digest(photo_ids: Iterable[int]) -> str:
    """A fingerprint of a set of photo ids, whatever their order."""
    return _digest(str(pid) for pid in sorted(set(photo_ids)))


def undescribable(body: JsonDict) -> str | None:
    """Why no description covers this request, or None.

    These fields change the scope or the species lists in ways the plan does
    not model, so a description that ignored them would be of another run.
    """
    fields = [key for key in ("exclude_paths", "labels_file") if body.get(key)]
    return f"a described run does not take {', '.join(fields)}" if fields else None


def expected_run_error(expected: object, actual: JsonDict) -> tuple[str, int] | None:
    """Why the ``actual`` run is not the ``expected`` one: ``(message, status)``, or None."""
    if not isinstance(expected, dict) or not set(actual) <= set(expected):
        return "expected_run must be the run a plan described", 400
    changed = list(dict.fromkeys(_PART_NAMES[key] for key in actual if expected[key] != actual[key]))
    if not changed:
        return None
    return f"The run changed since it was described ({', '.join(changed)}). Nothing was started.", 409


def _label_files(db: Database, params: PipelinePlanParams) -> list[str]:
    """The species lists a run would read: its own, the workspace's, or the global ones."""
    if params.labels_files:
        return list(params.labels_files)
    files = workspace_config.get_workspace_active_labels(db)
    if files is not None:
        return [str(path) for path in files]
    from labels import get_active_labels

    return [entry["labels_file"] for entry in get_active_labels()]


def _replaced_reviews(db: Database, photo_ids: Iterable[int]) -> JsonDict:
    """The accepted and rejected predictions a re-classify may discard: how many, and which."""
    rows = db.model_runs.reviewed_predictions(photo_ids)
    return {
        "predictions": len({row["prediction_id"] for row in rows}),
        "photos": len({row["photo_id"] for row in rows}),
        "digest": _digest(sorted(
            f"{row['prediction_id']}:{row['workspace_id']}:{row['status']}" for row in rows
        )),
    }


def describe_run(
    db: Database, params: PipelinePlanParams, photo_ids: Iterable[int], miss_enabled: bool | None,
) -> JsonDict:
    """The parts of a run over ``photo_ids`` that must not change before it starts.

    ``miss_enabled`` is the run's own switch; None defers to the workspace, as
    the misses stage does. ``replaced_reviews`` covers every workspace. Species
    keywords those decisions saved stay on the photos.
    """
    import config as cfg
    from pipeline_plan import run_models

    effective = db.get_effective_config(cfg.load())
    pipeline_cfg: dict[str, Any] = effective.get("pipeline", {})
    ids = sorted(set(photo_ids))
    models, fingerprints = run_models(db, params)
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
        "replaced_reviews": _replaced_reviews(db, ids if params.reclassify and models else []),
    }


def refuse_changed_run(
    db_path: str, params: PipelinePlanParams, miss_enabled: bool | None, expected: JsonDict,
) -> None:
    """Raise ``RunChangedError`` when a run leaving the queue is not the one described.

    ``params.photo_ids`` are the photos the run was pinned to at launch, and
    ``expected`` was checked against the workspace then, so both name the run's
    own. Runs on the job's thread, on its own connection.
    """
    from db import Database

    db = Database(db_path)
    try:
        db.set_active_workspace(expected["workspace_id"])
        mismatch = expected_run_error(expected, describe_run(db, params, params.photo_ids or [], miss_enabled))
    finally:
        db.close()
    if mismatch is not None:
        raise RunChangedError(mismatch[0])
