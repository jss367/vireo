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

import contextlib
import hashlib
import json
import os
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from config import config_value
from services import workspace_config

if TYPE_CHECKING:
    from db import Database
    from pipeline_plan import PipelinePlanParams
    from services.service_types import JsonDict

#: The stage settings a description spells out; its ``digest`` covers the rest.
_SETTINGS = ("sam2_variant", "dinov2_variant", "proxy_longest_edge")

#: Settings no Process stage reads: credentials, other programs, imports, syncing,
#: cache limits, culling and what pages show. Every other setting is part of a
#: run, so one added later is until it is listed here.
NOT_STAGE_SETTINGS = frozenset({
    "move_keep_visible_in_other_workspaces", "keyword_case", "sync_flags_to_xmp",
    "write_assigned_location_to_xmp", "write_location_keywords_to_xmp", "max_edit_history",
    "grouping_history_max_mb", "retained_cache_max_mb", "retained_cache_max_age_days",
    "inat_token", "hf_token", "google_maps_api_key", "google_maps_prefer_english",
    "scan_roots", "scan_workers", "setup_complete", "rsync_bin", "ssh_bin", "remote_targets",
    "darktable_bin", "external_editor", "external_editors", "export_presets", "report_url",
    "darktable_style", "darktable_output_format", "darktable_output_dir", "darktable_auto_convert_dng",
    "dng_converter_bin", "open_in_browser", "time_format", "browse_card_fields", "filter_shortcuts",
    "photos_per_page", "working_copy_cache_max_mb", "preview_cache_max_mb", "browse_thumb_default",
    "browse_stack_time_gap", "browse_stack_split_mode", "cull_time_window", "cull_phash_threshold",
    "ingest", "numbered_navigation_mode", "keyboard_shortcuts",
})

#: Every request field that decides what a run does, and the value an omitted
#: one has. The scope, the models and the species lists are described apart.
_OPTIONS = {
    "skip_classify": False,
    "skip_extract_masks": False,
    "skip_eye_keypoints": False,
    "skip_regroup": False,
    "review_mode": None,
    "miss_enabled": None,
    "eye_detect_override": None,
    "reclassify": False,
    "raw_subject_analysis": False,
    "download_taxonomy": True,
    "preview_max_size": None,
}

#: What each part of a description is called in a refusal.
_PART_NAMES = {
    "workspace_id": "the active workspace",
    "photo_count": "the photos in scope",
    "photo_digest": "the photos in scope",
    "models": "the models",
    "model_files": "the installed model files",
    "labels_files": "the species lists",
    "label_fingerprints": "the species lists",
    "settings": "the stage settings",
    "options": "the stages and options",
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

    A described run has one scope, and none of the fields that change its
    photos or species lists in ways the plan does not model, so the description
    cannot be of a run the launch would refuse or run differently.
    """
    scopes = [key for key in ("photo_ids", "folder_ids", "collection_id") if body.get(key) is not None]
    if len(scopes) != 1:
        return "a described run needs exactly one of photo_ids, folder_ids and collection_id"
    fields = [key for key in ("labels_file", "source", "sources", "source_snapshot_id")
              if body.get(key)]
    if body.get("exclude_paths") and body.get("folder_ids") is None:
        fields.append("exclude_paths")
    fields += [key for key in ("source_paths", "hash_duplicate_paths") if body.get(key) is not None]
    if fields:
        return f"a described run does not take {', '.join(fields)}"
    if not isinstance(body.get("miss_enabled"), bool | None):
        return "miss_enabled must be a boolean or null"
    return None


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


def _file_state(path: str) -> list[Any]:
    """What changes when a file is replaced, rewritten or removed; its contents are not read."""
    try:
        stat = os.stat(path)
    except OSError:
        return [path, None]
    return [path, stat.st_size, stat.st_mtime_ns, stat.st_ino]


def _other_classifiers(selected: set[Any]) -> set[str]:
    """The folders and files of classifiers this run does not use, installed or not yet."""
    import models as model_registry

    root = model_registry.DEFAULT_MODELS_DIR
    folders = {
        os.path.join(root, known["id"]) for known in model_registry.KNOWN_MODELS if known["id"] not in selected
    }
    folders.update(
        model["weights_path"] for model in model_registry.get_models()
        if model.get("id") not in selected and model.get("weights_path")
    )
    return {os.path.abspath(folder) for folder in folders}


def _model_files_digest(models: Iterable[JsonDict]) -> str:
    """A fingerprint of the files a run's models are loaded from.

    Covers the models folder, which holds the detector, mask, embedding and eye
    models beside the classifiers, so no stage's model needs naming here; a
    custom classifier's own files; and the taxonomy its names resolve through.
    Other classifiers are left out, so installing one does not fail a queued
    run. Multi-gigabyte weights make hashing contents too slow for a preview.
    """
    import models as model_registry
    import taxonomy

    described = [
        {key: model.get(key) for key in ("id", "model_str", "model_type", "weights_path")} for model in models
    ]
    others = _other_classifiers({model["id"] for model in described})
    paths = [taxonomy.TAXONOMY_JSON_PATH]
    for folder, subfolders, names in os.walk(os.path.abspath(model_registry.DEFAULT_MODELS_DIR)):
        subfolders[:] = [name for name in subfolders if os.path.join(folder, name) not in others]
        files = (os.path.join(folder, name) for name in names)
        paths.extend(path for path in files if path not in others)
    for model in described:
        weights = model["weights_path"] or ""
        paths.append(weights)
        if os.path.isdir(weights):
            # A folder removed here leaves its own entry, so it reads as gone.
            with contextlib.suppress(OSError):
                paths.extend(os.path.join(weights, name) for name in os.listdir(weights))
    state = [described, [_file_state(path) for path in sorted(set(paths))]]
    return _digest([json.dumps(state, sort_keys=True, default=str)])


def _replaced_reviews(db: Database, photo_ids: Iterable[int]) -> JsonDict:
    """The accepted and rejected predictions a re-classify may discard: how many, and
    a fingerprint of their whole review rows."""
    rows = db.model_runs.reviewed_predictions(photo_ids)
    return {
        "predictions": len({row["prediction_id"] for row in rows}),
        "photos": len({row["photo_id"] for row in rows}),
        "digest": _digest(sorted(
            json.dumps(dict(row), sort_keys=True, default=str) for row in rows
        )),
    }


def _settings(effective: JsonDict) -> JsonDict:
    """The settings the stages would read, all of them in ``digest``."""
    import config as cfg

    # ``display`` is an older home of the thumbnail size, which the media stage
    # still reads. Keys outside the defaults are the app's own records.
    read = {
        key: effective.get(key) for key in ("display", *cfg.DEFAULTS) if key not in NOT_STAGE_SETTINGS
    }
    pipeline_cfg: dict[str, Any] = dict(effective.get("pipeline", {}))
    # Chooses what runs after an import; no stage reads it.
    pipeline_cfg.pop("default_process_id", None)
    read["pipeline"] = pipeline_cfg
    return {
        **{key: pipeline_cfg.get(key) for key in _SETTINGS},
        "detector_confidence": config_value(effective, "detector_confidence"),
        "digest": _digest([json.dumps(read, sort_keys=True, default=str)]),
    }


def describe_run(
    db: Database, params: PipelinePlanParams, photo_ids: Iterable[int], body: JsonDict,
) -> JsonDict:
    """The parts of a run over ``photo_ids`` that must not change before it starts.

    ``params`` are the plan's parameters for the request ``body``. A null
    ``miss_enabled`` defers to the workspace, as the misses stage does.
    ``replaced_reviews`` covers every workspace. Species keywords those
    decisions saved stay on the photos.
    """
    import config as cfg
    from pipeline_plan import run_models

    effective = db.get_effective_config(cfg.load())
    ids = sorted(set(photo_ids))
    models, fingerprints = run_models(db, params)
    options = {key: body.get(key, default) for key, default in _OPTIONS.items()}
    misses = options["miss_enabled"]
    if misses is None:
        misses = effective.get("pipeline", {}).get("miss_enabled", True)
    return {
        "workspace_id": db.require_workspace_id(),
        "photo_count": len(ids),
        "photo_digest": photo_digest(ids),
        "models": [{"id": m["id"], "name": m["name"]} for m in models],
        "model_files": _model_files_digest(models),
        "labels_files": _label_files(db, params),
        "label_fingerprints": fingerprints,
        "settings": _settings(effective),
        "options": options,
        "misses": bool(misses) and bool(models) and not params.skip_regroup,
        "replaced_reviews": _replaced_reviews(db, ids if params.reclassify and models else []),
    }


def refuse_changed_run(db_path: str, collection_id: int | None, params: PipelinePlanParams, body: JsonDict) -> None:
    """Raise ``RunChangedError`` when a run leaving the queue is not the one described.

    The stages read their photos from ``collection_id``, made at launch from the
    described ones, so its members now are the run's scope: editing or deleting
    it, or deleting a photo, changes the run. Photos on an offline drive stay
    members. The ``expected_run`` in ``body`` was checked against the workspace
    at launch, so it names the run's own. Runs on the job's thread, on its own
    connection.
    """
    from db import Database

    expected = body["expected_run"]
    db = Database(db_path)
    try:
        db.set_active_workspace(expected["workspace_id"])
        rows = db.collections.get_photos(collection_id, per_page=1_000_000, include_offline_folders=True)
        mismatch = expected_run_error(expected, describe_run(db, params, [row["id"] for row in rows], body))
    finally:
        db.close()
    if mismatch is not None:
        raise RunChangedError(mismatch[0])
