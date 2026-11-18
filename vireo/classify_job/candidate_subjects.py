"""Ask the classifier about person/vehicle regions on photos with no animal region.

MegaDetector calls some animals "person" or "vehicle", and a photo whose
only confident region carries one of those labels is otherwise skipped by
classification, masks and quality analysis: the bird is in the frame and
nothing downstream sees it. Before a classify pass picks its targets,
``examine_candidates`` classifies the strongest such region of each of those
photos. A region ``background_match.is_recognized_animal`` accepts is kept:
its detection row becomes an ``animal`` region (``db.candidate_subjects``),
so the classify pass, mask extraction, subject analysis and grouping treat
it like any other. A region it rejects is recorded, so the next run does not
read the photo again; a person or a car stays out of species classification.

The check runs once per (region, classifier model, label set). It needs
cosine scores and background prompts, so a timm classifier examines nothing.

The detect phase hands classify an in-memory copy of its boxes that still
carries the detector's category; ``mark_kept_regions`` and ``kept_regions``
let each classify path bring that copy in line with the catalog.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import background_match
from classify_job.classify_pass import (
    ClassifierModel,
    _classify_batch_images,
    _match_stats,
    measure_seconds,
)
from classify_job.common import log
from resource_ledger import ResourceWaitCancelled

if TYPE_CHECKING:
    from classify_job.classify_types import DetectionDict, DetectionMap, PhotoRecord
    from db import Database
    from PIL import Image

    # Loads the crop of one detection of one photo; None when it cannot be read.
    PrepareCrop = Callable[[PhotoRecord, DetectionDict], "Image.Image | None"]


@dataclass
class CandidateOutcome:
    """What one ``examine_candidates`` pass did."""

    examined: int = 0
    """Regions the classifier answered about, kept or not."""
    kept_photo_ids: set[int] = field(default_factory=set)
    """Photos whose region was kept as an animal subject by this pass."""
    unread: int = 0
    """Regions whose crop could not be loaded or scored; they are asked
    about again on the next run."""


def _region(row: PhotoRecord) -> DetectionDict:
    """A stored region as the classify job's detection dict."""
    return {
        "id": row["id"],
        "box_x": row["box_x"],
        "box_y": row["box_y"],
        "box_w": row["box_w"],
        "box_h": row["box_h"],
        "confidence": row["detector_confidence"],
        "category": row["category"],
        "detector_model": row["detector_model"],
    }


def examine_candidates(
    db: Database,
    photos: Sequence[PhotoRecord],
    model: ClassifierModel,
    *,
    min_conf: float,
    prepare_crop: PrepareCrop,
    should_stop: Callable[[], object] | None = None,
    recheck: bool = False,
    timings: dict[str, float] | None = None,
) -> CandidateOutcome:
    """Classify each photo's pending candidate region and keep the recognized ones.

    ``photos`` is the scope to look in; only photos with a pending candidate
    (``db.candidate_subjects.pending``) are read. ``min_conf`` is the
    workspace's detector confidence floor. ``recheck`` asks again about
    regions already examined, for a run that re-infers on purpose.
    """
    outcome = CandidateOutcome()
    if model.model_type == "timm" or not photos:
        return outcome
    by_id = {photo["id"]: photo for photo in photos}
    pending = db.candidate_subjects.pending(
        by_id, min_conf, model.model_name, model.labels_fingerprint or "legacy",
        recheck=recheck,
    )
    for row in pending:
        if should_stop is not None and should_stop():
            break
        region = _region(row)
        img = prepare_crop(by_id[row["photo_id"]], region)
        if img is None:
            outcome.unread += 1
            continue
        photo = by_id[row["photo_id"]]
        try:
            with measure_seconds(timings, "inference"):
                results, _failed = _classify_batch_images(
                    [{"photo": photo, "img": img}], [img], model.clf, model.model_type,
                    propagate_cancel=True,
                )
        except ResourceWaitCancelled:
            break
        finally:
            img.close()
        stats = _match_stats(results[0][0], model.model_type) if results[0] else None
        if stats is None:
            # Inference failed or reported no raw scores to judge by; the
            # region is asked about again on the next run.
            outcome.unread += 1
            continue
        kept = background_match.is_recognized_animal(
            stats["max_match_score"], stats["background_score"], stats["score_kind"],
        )
        with measure_seconds(timings, "storage"), db.transaction():
            recorded = db.candidate_subjects.record(
                row["id"],
                {
                    **stats,
                    "detector_category": row["category"],
                    "classifier_model": model.model_name,
                    "labels_fingerprint": model.labels_fingerprint or "legacy",
                },
                promoted=kept,
            )
        if not recorded:
            # A concurrent re-detect retired the region during inference.
            continue
        outcome.examined += 1
        if kept:
            outcome.kept_photo_ids.add(row["photo_id"])
    if outcome.examined or outcome.unread:
        log.info(
            "Person/vehicle regions examined: %d, kept as subjects: %d, unreadable: %d",
            outcome.examined, len(outcome.kept_photo_ids), outcome.unread,
        )
    return outcome


def kept_regions(db: Database, photo_ids: Iterable[int]) -> DetectionMap:
    """Each photo's kept candidate regions as classifier-candidate detections."""
    return {
        photo_id: [_region(row) for row in rows]
        for photo_id, rows in db.candidate_subjects.kept_regions(photo_ids).items()
    }


def mark_kept_regions(db: Database, detections_by_photo: DetectionMap) -> None:
    """Give kept regions their ``animal`` category in a detect-phase copy.

    The copy was built from detector output, which says "person" or
    "vehicle" for a region the catalog keeps as an animal.
    """
    kept = db.candidate_subjects.kept_regions(detections_by_photo)
    for photo_id, rows in kept.items():
        kept_ids = {row["id"] for row in rows}
        for detection in detections_by_photo.get(photo_id) or ():
            if detection.get("id") in kept_ids:
                detection["category"] = "animal"
