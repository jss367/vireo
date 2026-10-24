"""Types, optional model dependencies and small helpers shared by the classify job.

``ClassifyParams`` (the parsed request), ``UnusableLabelsError``, the
package's logger, and the detector / sharpness / image-loader / classifier
imports with their fallbacks when a dependency is missing. The fallbacks live
here once, so every module sees the same ``ClassificationCancelled`` class.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime

    from classify_job.classify_types import PhotoRecord
    from services.service_types import JobDict, JsonDict
    from taxonomy import Taxonomy

try:
    from detector import detect_animals, get_primary_detection
except ImportError:
    detect_animals = None
    get_primary_detection = None

try:
    from sharpness import compute_sharpness
except ImportError:
    compute_sharpness = None

try:
    from image_loader import load_image, load_working_image
except ImportError:
    load_image = None
    load_working_image = None


def _classification_cancelled_standin() -> Any:
    """Stand-in for ``classifier.ClassificationCancelled`` without the classifier.

    Typed ``Any`` because it takes the place of that class, which is not
    importable here. It keeps the class's name for the checks that recognize
    a cancelled classification by name.
    """

    class ClassificationCancelled(RuntimeError):
        pass

    return ClassificationCancelled


try:
    from classifier import ClassificationCancelled, Classifier
except ImportError:
    Classifier = None
    ClassificationCancelled = _classification_cancelled_standin()

try:
    from timm_classifier import TimmClassifier
except ImportError:
    TimmClassifier = None

# The package's one logger, named ``classify_job`` as it was before the
# module became a package.
log = logging.getLogger("classify_job")


class UnusableLabelsError(RuntimeError):
    """Every prompt in the selected label set was dropped as ambiguous.

    Its own class because the cache peek in ``run_classify_job``
    deliberately swallows label-resolution failures (missing weights on a
    fresh install, ToL artifacts absent) and falls back to model-only
    cache filtering. Swallowing *this* one would let
    ``_finalize_cached_only`` report success from a different label space
    for a request the authoritative load refuses.
    """


@dataclass
class ClassifyParams:
    """Parameters for a classification job, parsed from the request body."""

    collection_id: str
    labels_file: str | None
    labels_files: list[str] | None
    model_id: str | None
    model_name: str | None
    grouping_window: int
    similarity_threshold: float
    reclassify: bool


def _load_job_taxonomy(job: JobDict) -> Taxonomy | None:
    """Load the local taxonomy, recording on ``job`` why a run goes without it.

    A taxonomy file that exists but cannot be read leaves the run without
    the taxonomy (or on the older fallback copy). Classification still
    produces valid predictions, so this is a notice in the job's result
    (``taxonomy_notice``) and its step summary, not a failure.
    """
    from taxonomy import load_local_taxonomy, taxonomy_fallback_notice

    failures: list[dict[str, str]] = []
    tax = load_local_taxonomy(failures=failures)
    notice = taxonomy_fallback_notice(tax, failures)
    if notice:
        job["_taxonomy_notice"] = notice
    return tax


def taxonomy_step_summary(job: JobDict, tax: Taxonomy | None) -> str:
    """The ``load_taxonomy`` step's summary, naming an unreadable file."""
    if job.get("_taxonomy_notice"):
        return "Taxonomy unreadable" if tax is None else "Taxonomy loaded from the older copy"
    return "Taxonomy loaded" if tax else "No taxonomy"


def with_taxonomy_notice(job: JobDict, result: JsonDict) -> JsonDict:
    """Add the run's taxonomy notice to its result, when there is one."""
    notice = job.get("_taxonomy_notice")
    if notice:
        result["taxonomy_notice"] = notice
    return result


def _photo_timestamp(photo: PhotoRecord) -> datetime | None:
    """Parse a photo row's stored ISO timestamp, or ``None``.

    A malformed stored value only costs the prediction its capture time, so it
    is logged rather than failing the photo's classification.
    """
    raw = photo["timestamp"]
    if not raw:
        return None
    from datetime import datetime

    try:
        return datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        log.debug("Ignoring unparseable photo timestamp %r", raw)
        return None
