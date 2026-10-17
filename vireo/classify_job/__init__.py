"""Classification job logic extracted from app.py.

This package contains the background work function for the /api/jobs/classify
endpoint. The route handler in app.py parses the request and delegates here.

Modules, by responsibility:

- ``common``: ``ClassifyParams``, ``UnusableLabelsError``, the logger and the
  optional model dependencies.
- ``label_loading``: selecting, loading and fingerprinting the label set.
- ``gating``: per-detection classification behind the classifier_runs gate.
- ``cache_coverage``: whether cached classifier runs cover the collection.
- ``label_source``: naming the label source for the Jobs page.
- ``detection``: subject detection and the classifier's candidates.
- ``classify_pass``: batching images through the classifier.
- ``store_predictions``: grouping and storing predictions.
- ``cached_only``: finalizing from cached predictions alone.
- ``run``, ``job_run``, ``run_helpers``: the entry point and
  ``_ClassifyJobRun``.

This module re-exports what code outside the package imports. Patch a name in
the module that looks it up (``classify_job.detection.detect_animals``), not
here: a patch of ``classify_job.<name>`` reaches only callers that read the
package attribute (the pipeline stages, ``web.job_launchers``).
"""

from classify_job.classify_pass import (
    _BATCH_SIZE,
    _flush_batch,
    _prepare_image,
    _publish_classifier_runs_for_raw_results,
    _record_batch_classifier_runs,
)
from classify_job.common import ClassifyParams, UnusableLabelsError
from classify_job.detection import _detect_batch
from classify_job.label_loading import (
    _load_labels,
    _record_labels_fingerprint,
    _reuse_saved_label_embeddings,
)
from classify_job.label_source import _sources_from_metas, describe_label_source
from classify_job.run import run_classify_job
from classify_job.store_predictions import (
    _cached_prediction_taxonomy,
    _store_grouped_predictions,
)

__all__ = [
    "_BATCH_SIZE",
    "ClassifyParams",
    "UnusableLabelsError",
    "_cached_prediction_taxonomy",
    "_detect_batch",
    "_flush_batch",
    "_load_labels",
    "_prepare_image",
    "_publish_classifier_runs_for_raw_results",
    "_record_batch_classifier_runs",
    "_record_labels_fingerprint",
    "_reuse_saved_label_embeddings",
    "_sources_from_metas",
    "_store_grouped_predictions",
    "describe_label_source",
    "run_classify_job",
]
