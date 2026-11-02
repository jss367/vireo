"""The classify stage's cache preflight over the classifier-run cache.

Before the streaming pipeline classifies a batch, it asks which photos would
be served entirely from cached classifier runs and which the runtime skips
without inference, so the ETA projects only real work. Each function reads
the workspace-effective ``detector_confidence`` (through
``Database.get_effective_config``) and hands it to the ``db.model_runs``
query that mirrors the runtime's gate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import config as cfg
from config import config_value

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from db import Database


def get_classifier_run_cache_hits(
    db: Database,
    photo_ids: Sequence[int],
    classifier_model: str,
    labels_fingerprint: str,
    *,
    contextual_weak_photo_ids: Iterable[int] | None = None,
    weak_confidence: float | None = None,
    fresh_detections_by_photo: Mapping[int, Sequence[Mapping[str, Any]]] | None = None,
    fresh_processed_photo_ids: Iterable[int] | None = None,
    expected_classifier_runtime_by_detector_runtime: Mapping[Any, str | None] | None = None,
) -> set[int]:
    """Return the SET of photo IDs from ``photo_ids`` that the classify
    preflight would consider fully cached under
    (classifier_model, labels_fingerprint).

    Used by the streaming pipeline's classify stage to pre-flight how
    many photos will hit the cache vs. require fresh inference — and,
    because ``_classification_eta_progress`` reconciles the estimate
    from observed misses, also to know WHICH photos the preflight
    expected to be cached. Only observed misses on preflight-expected
    photos deflate the projected cache hits; misses on photos the
    preflight never counted are unrelated to the estimate and must
    not scale it down.

    Photo-scoped to match runtime accounting: the classify loop keeps
    its ``cached`` bucket photo-scoped (see pipeline_job.py — a photo
    lands there only if every processed detection is a cache hit; the
    moment any detection runs fresh inference it is promoted to
    ``count``). So the preflight only counts a photo as cached when
    NO qualifying detection would need fresh inference — otherwise a
    multi-subject photo with one cached and one uncached detection
    would inflate ``cached_estimate`` up to ``total`` and the banner
    would misread as "no work to classify" when there is real work
    remaining.

    Above-threshold animal detections must all have matching run keys.
    Photos with no usable animal target and no confident non-animal box
    count through their synthetic full-image anchor, including photos
    whose only animal detections are below the workspace floor.

    ``contextual_weak_photo_ids`` marks photos the classify loop will
    rescue with a lowered ``weak_confidence`` floor. For those photos
    the runtime picks one qualifying weak-threshold detection to
    classify, so the preflight counts the photo when at least one of
    its weak-threshold animal detections carries a matching run key.
    Without this, a rerun over a cached weak tail is scored as pure
    uncached work and the ETA can substantially overstate the time
    remaining.

    When the detector stage ran, ``fresh_detections_by_photo`` and
    ``fresh_processed_photo_ids`` scope processed photos to the exact
    in-memory candidates the classify runtime will consume. This avoids
    stale rows from another detector either adding uncached work or hiding
    a fresh uncached target. Photos the detector failed to process retain
    the DB fallback, matching the classify loop.

    ``expected_classifier_runtime_by_detector_runtime`` maps each
    ``detections.runtime_fingerprint`` value present in this batch to the
    classifier_runtime_fingerprint the runtime gate will accept for
    classifier_runs rows anchored on that detection. When provided, the
    preflight matches the runtime's per-detection gate: a
    ``classifier_runs`` row counts only when its ``runtime_fingerprint``
    equals the expected value for its detection's detector runtime, or is
    the ``'legacy'`` sentinel, or the prediction carries a real
    prediction_review override. Without this predicate the preflight
    overcounts obsolete-runtime rows the runtime will reject and the
    observation-based correction cannot deflate the estimate until those
    rejected rows are actually visited — leading
    ``_classification_eta_progress`` to report "finishing…" while
    substantial inference work still remains. Passing ``None`` applies
    no runtime filter; this is the correct choice on reclassify runs,
    where the runtime gate is
    bypassed anyway. Values may be ``None`` when the caller cannot
    compute an expected fingerprint (e.g. portable identity not wired
    up): matching detections then skip the runtime filter, mirroring
    the unfiltered ``get_classifier_run_keys`` fallback the runtime
    applies.
    """
    if not photo_ids:
        return set()
    min_conf = config_value(db.get_effective_config(cfg.load()), "detector_confidence")
    return db.model_runs.get_classifier_run_cache_hits(
        photo_ids,
        classifier_model,
        labels_fingerprint,
        min_conf=min_conf,
        contextual_weak_photo_ids=contextual_weak_photo_ids,
        weak_confidence=weak_confidence,
        fresh_detections_by_photo=fresh_detections_by_photo,
        fresh_processed_photo_ids=fresh_processed_photo_ids,
        expected_classifier_runtime_by_detector_runtime=expected_classifier_runtime_by_detector_runtime,
    )


def count_classifier_runs(
    db: Database,
    photo_ids: Sequence[int],
    classifier_model: str,
    labels_fingerprint: str,
    *,
    contextual_weak_photo_ids: Iterable[int] | None = None,
    weak_confidence: float | None = None,
    fresh_detections_by_photo: Mapping[int, Sequence[Mapping[str, Any]]] | None = None,
    fresh_processed_photo_ids: Iterable[int] | None = None,
    expected_classifier_runtime_by_detector_runtime: Mapping[Any, str | None] | None = None,
) -> int:
    """Return ``len(get_classifier_run_cache_hits(...))``.

    Retained as a thin count-only shim so callers that don't need the
    photo-id set (or preexisting tests) keep working; the id set is
    the newer surface the pipeline uses to scope its overcount
    tracker.
    """
    return len(get_classifier_run_cache_hits(
        db,
        photo_ids,
        classifier_model,
        labels_fingerprint,
        contextual_weak_photo_ids=contextual_weak_photo_ids,
        weak_confidence=weak_confidence,
        fresh_detections_by_photo=fresh_detections_by_photo,
        fresh_processed_photo_ids=fresh_processed_photo_ids,
        expected_classifier_runtime_by_detector_runtime=(
            expected_classifier_runtime_by_detector_runtime
        ),
    ))


def get_unclassifiable_photos(
    db: Database,
    photo_ids: Sequence[int],
    *,
    contextual_weak_photo_ids: Iterable[int] | None = None,
    weak_confidence: float | None = None,
    fresh_detections_by_photo: Mapping[int, Sequence[Mapping[str, Any]]] | None = None,
    fresh_processed_photo_ids: Iterable[int] | None = None,
) -> set[int]:
    """Return photo IDs the classify runtime skips without inference.

    Photos with no usable animal crop normally receive full-image
    classification. The remaining deterministic skip is a photo with no
    eligible animal target but a confident non-animal (person/vehicle)
    detection. Excluding that tail keeps it out of the ETA's projected
    inference work.

    ``fresh_detections_by_photo`` is an in-memory ``{photo_id:
    [detection_dict, ...]}`` map (the ``detect_state["detections"]``
    that ``_detect_batch`` populated). When provided, the "any
    animal detection >= floor?" predicate is answered from this
    fresh set instead of the DB — mirroring what the runtime's
    ``photo_dets`` filter actually sees. This matters whenever the
    current detector pass replaces the runtime candidates while older
    rows from another detector model remain in the DB. Without this
    override, stale rows can make the preflight choose a different crop
    or skip outcome than runtime.
    Each detection dict must expose ``confidence`` (or
    ``detector_confidence``) and ``category`` (defaulting to
    ``"animal"`` when absent, matching ``_detect_batch``'s output).

    ``fresh_processed_photo_ids`` is the ``detect_state["processed_ids"]``
    set — the photos ``_detect_batch`` actually completed. When a
    MegaDetector pass raises for a single image, ``_detect_batch``
    leaves that photo out of both ``detections`` and
    ``processed_ids`` and the runtime's per-photo classify body
    falls back to ``detection_store.get_detections()`` (see
    ``pipeline_job.py`` ``photo_dets`` else branch). Treating a
    missing entry in ``fresh_detections_by_photo`` as "no fresh
    animal above the floor" would mark such a photo unclassifiable
    even though runtime will infer, subtracting real work from the
    ETA's ``remaining_uncached`` and reporting "finishing…"
    prematurely. When both this set and ``fresh_detections_by_photo``
    are provided, photos NOT in the processed set defer to the DB
    predicate for that photo, matching what the runtime actually
    sees.
    """
    if not photo_ids:
        return set()
    min_conf = config_value(db.get_effective_config(cfg.load()), "detector_confidence")
    return db.model_runs.get_unclassifiable_photos(
        photo_ids,
        min_conf=min_conf,
        contextual_weak_photo_ids=contextual_weak_photo_ids,
        weak_confidence=weak_confidence,
        fresh_detections_by_photo=fresh_detections_by_photo,
        fresh_processed_photo_ids=fresh_processed_photo_ids,
    )
