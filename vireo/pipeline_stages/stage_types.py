"""Type names shared by the pipeline stages' annotations.

Nothing here runs: the aliases are plain ``dict`` types, the TypedDicts
describe dicts the stages and ``pipeline_job`` already build, and the
protocols name the callables ``pipeline_job`` injects when it wires a run.
A stage can therefore say what it receives without importing the
orchestrator, which stages never do.
"""

from __future__ import annotations

import queue
import sqlite3
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, NotRequired, Protocol, TypedDict

if TYPE_CHECKING:
    from classifier import Classifier
    from db import Database
    from taxonomy import Taxonomy
    from timm_classifier import TimmClassifier

# A model record as ``models.get_models()`` lists it and the run resolves it
# (``id``, ``name``, ``model_str``, ``weights_path``, ``model_type``,
# ``files``, ``hf_subdir``, ...). The keys present depend on the model's
# source (built-in, Hugging Face, custom), so the values are ``Any``.
ModelSpec = dict[str, Any]

# A detection as the stages pass it around: a fresh detector result or a
# ``detections`` row turned into a dict. The two spell the confidence
# differently (``confidence`` / ``detector_confidence``) and carry different
# optional keys, so the values are ``Any``.
DetectionDict = dict[str, Any]

# What the model-loader stage shares with detect and classify: the loaded
# model's ``ModelBundle`` keys plus ``tax``, ``taxonomy_identity``,
# ``resolved_specs``, ``pending_specs`` and ``preload_error``. Bundle keys
# are dropped and replaced as classify moves from one model to the next, so
# it stays a plain dict.
LoadedModels = dict[str, Any]

# A photo the stages read by column name: a catalog ``sqlite3.Row``, or the
# dict a repository builds from one.
PhotoRecord = sqlite3.Row | dict[str, Any]

# A photo's normalized edit recipe (``Database.get_photo_edit_recipe``): a
# JSON object of develop settings whose keys depend on the edits made.
EditRecipe = dict[str, Any]

# The scanner-to-thumbnail hand-off queue: ``(photo_id, path)`` items, then
# the run's end-of-stream sentinel object, so the item type is ``Any``.
ScanQueue = queue.Queue[Any]

# ``thumbnails.generate_thumbnail``: renders a photo's thumbnail and returns
# its path, or None when no source could be decoded. Recipe and RAW-decode
# options arrive as keywords that vary by caller.
GenerateThumbnail = Callable[..., "str | None"]


class ClassifierHandle(Protocol):
    """The refcounted cache handle ``acquire_cached_classifier`` returns."""

    # ``model_cache`` holds whatever its factory built; for this handle that
    # is the classifier ``load_model_bundle`` constructed.
    def __enter__(self) -> Any: ...

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None: ...

    def release(self) -> None: ...


class ModelBundle(TypedDict):
    """A loaded classifier and the label identity classify records with it."""

    clf: Classifier | TimmClassifier
    _cache_handle: ClassifierHandle
    model_type: str
    model_name: str
    model_str: str
    labels: list[str] | None
    label_source: str
    labels_fingerprint: str
    labels_fingerprint_full: str | None
    classifier_model_identity: dict[str, Any] | None
    use_tol: bool
    active_model: ModelSpec


class DetectState(TypedDict):
    """What the detect stage hands the classify stage.

    Populated even on an early exit so classify can tell "detection ran but
    produced nothing" from "detection never executed".
    """

    photos: list[sqlite3.Row]
    folders: dict[int, str]
    # photo id -> that photo's detections from this run.
    detections: dict[int, list[DetectionDict]]
    # Photos whose detect iteration completed.
    processed_ids: set[int]
    # photo id -> its detection ids before a reclassify, for the purge.
    pre_run_det_ids: dict[int, set[int]]
    total_detected: int
    ran: bool
    # What ``materialize_local_store`` reused from the computation cache.
    portable_reused: NotRequired[dict[str, Any]]


class SourceOfflineState(TypedDict):
    """Photos classify skipped because their folder went offline mid-run."""

    skipped_photo_ids: set[int]


class LoadModelBundle(Protocol):
    """``models.load_model_bundle`` bound to one run."""

    def __call__(
        self, active_model: ModelSpec, tax: Taxonomy | None, thread_db: Database,
        progress_step: str = ...,
    ) -> ModelBundle: ...


class CachedClassifyDetections(Protocol):
    """``pipeline_job._cached_classify_detections``."""

    def __call__(
        self, detections: Iterable[DetectionDict], floor: float, *,
        contextual_weak: bool = ...,
    ) -> list[DetectionDict]: ...


class ClassificationEtaProgress(Protocol):
    """``pipeline_job._classification_eta_progress``."""

    def __call__(
        self, *, total: int, seen: int, cached_estimate: int, cache_hits: int,
        inference_attempts: int, classified: int, elapsed: float,
        cache_overcount: int = ..., unclassifiable_estimate: int = ...,
        unclassifiable_seen: int = ...,
    ) -> dict[str, Any]: ...


# ``pipeline_job._record_unattempted_cache_hit``: ``(photo_id, inferred,
# attempted, cache_hits)`` -> whether the photo joined ``cache_hits``.
RecordUnattemptedCacheHit = Callable[[int, set[int], set[int], set[int]], bool]

# Drops the photos the user excluded in the run's preview.
FilterExcluded = Callable[[list[sqlite3.Row]], list[sqlite3.Row]]

# ``(scope, reason)`` for an unreachable source folder/image, else None.
SourceOfflineReason = Callable[[str, str], "tuple[str, str] | None"]

# The ids among ``folder_ids`` whose folder is still unreachable.
StillOfflineFolderIds = Callable[["Database", Iterable[int]], set[int]]

# ``pipeline_job._recipe_render_source``: the file an edited photo renders
# from, given ``(photo, recipe, max_size, vireo_dir, folders)``.
RecipeRenderSource = Callable[
    ["PhotoRecord | None", "EditRecipe | None", int, "str | None", dict[int, str]], str,
]

# ``pipeline_job._retry_thumbnail_with_companion`` / ``..._with_working_copy``:
# ``(db, generate_thumbnail, photo, photo_id, raw_source_path, cache_dir,
# thumb_size, recipe, folder_or_vireo_dir)`` -> the retried thumbnail's path.
ThumbnailRetry = Callable[
    [
        "Database", GenerateThumbnail, "PhotoRecord | None", int, str, str, int,
        "EditRecipe | None", "str | None",
    ],
    "str | None",
]

# ``pipeline_job._thumb_raw_decode_kwargs``: ``(photo, recipe)`` -> the
# RAW-decode keywords for ``generate_thumbnail``.
ThumbRawDecodeKwargs = Callable[["PhotoRecord | None", "EditRecipe | None"], dict[str, Any]]

# ``pipeline_job._thumb_min_source_size_kwargs``: ``(photo, recipe,
# thumb_size, source_path)`` -> the minimum-source-size keywords.
ThumbMinSourceSizeKwargs = Callable[
    ["PhotoRecord | None", "EditRecipe | None", int, str], dict[str, Any],
]
