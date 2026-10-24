"""The classify job's entry point, ``run_classify_job``, and its phase driver."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from classify_job.common import log
from classify_job.job_run import _ClassifyJobRun
from classify_job.run_helpers import _with_common_names_notice
from db import Database

if TYPE_CHECKING:
    from classify_job.common import ClassifyParams
    from jobs import JobRunner
    from services.service_types import JobDict, JsonDict


def run_classify_job(
    job: JobDict, runner: JobRunner, db_path: str, workspace_id: int, params: ClassifyParams,
    vireo_dir: str | None = None, computation_cache_dir: str | None = None,
) -> JsonDict:
    """Execute classification job. Called by JobRunner in a background thread.

    Args:
        job: job dict from JobRunner (has id, progress, errors, etc.)
        runner: JobRunner instance for push_event()
        db_path: path to SQLite database
        workspace_id: active workspace ID
        params: ClassifyParams with request parameters
        vireo_dir: optional path to ~/.vireo/; when set, classification uses
            pre-extracted working copy JPEGs instead of decoding RAW files.
        computation_cache_dir: portable-cache root the HTTP import route
            writes to (``app.config["COMPUTATION_CACHE_DIR"]``). When
            provided, background materialization reads from the same store
            so bundles imported before their photos were cataloged still
            plant on the next classify run outside the default location.
    """
    thread_db = Database(db_path)
    classify = _ClassifyJobRun(
        job, runner, thread_db, params, vireo_dir, computation_cache_dir,
    )
    try:
        thread_db.set_active_workspace(workspace_id)
        job["_start_time"] = time.time()

        # Stash the job's configured cache root so downstream helpers
        # publish freshly computed artifacts into the SAME cache the
        # status / export / catalog-reapplication paths use when
        # ``COMPUTATION_CACHE_DIR`` is overridden. Without this,
        # ``publish_detection_artifact`` and ``promote_and_publish_classifier_run``
        # fall back to the default ``~/.vireo/computation-cache`` and the
        # newly written artifacts disappear from the configured cache as
        # soon as the backing database rows are removed. The path (not the
        # ``ArtifactStore`` instance) is stashed so ``jsonify(job)`` on the
        # /api/jobs/<id> route keeps working — the helpers reconstruct the
        # lightweight wrapper on demand.
        job["_computation_cache_dir"] = computation_cache_dir

        runner.set_steps(job["id"], [
            {"id": "load_photos", "label": "Load photos"},
            {"id": "load_taxonomy", "label": "Load taxonomy"},
            {"id": "load_model", "label": "Load model"},
            {"id": "detect", "label": "Detect subjects"},
            {"id": "classify", "label": "Classify species"},
            {"id": "finalize", "label": "Finalize results"},
        ])

        return _run_classify_phases(classify, job, runner, params)
    finally:
        if classify.classifier_cache_handle is not None:
            classify.classifier_cache_handle.release()
        thread_db.close()


def _run_classify_phases(
    classify: _ClassifyJobRun, job: JobDict, runner: JobRunner, params: ClassifyParams,
) -> JsonDict:
    """Run ``run_classify_job``'s phases in order and return its result.

    Each early return ends the job there: no photos, a cancel before an
    expensive phase, or a collection the cache already covers.
    """
    classify.load_photos()

    # If the subject-skip filter (or an empty source collection) left
    # nothing to classify, short-circuit before model resolution. Model
    # resolution can fail with RuntimeError when no model is downloaded,
    # which would surface as a hard error for a job that has no work.
    if classify.total == 0:
        return classify.finish_without_photos()

    classify.apply_local_computation_cache()

    # Cancellation gate before the expensive phases (model resolution,
    # weight download, inference). The job loops below also check
    # per-photo; _run_job flips the terminal status to 'cancelled'.
    if runner.is_cancelled(job["id"]):
        log.info("Classify job cancelled before model resolution")
        return classify.cancelled_before_start(
            ["load_taxonomy", "load_model", "detect", "classify",
             "finalize"],
        )

    # Model resolution is otherwise unavoidable — get_active_model()
    # raises "No model available" on a fresh install with no downloads.
    # But when materialization has already covered every photo, the
    # classifier is not going to add any work: skip resolution and
    # finish so an all-cache-hit run on a fresh machine still succeeds.
    #
    # Opportunistically resolve the requested classifier name and its
    # labels_fingerprint before checking so a cache built with a
    # DIFFERENT model or label set does not silently pass as
    # "satisfied".  When we can't resolve either (no downloaded model
    # AND no labels file), we still fall through to the permissive
    # any-run check — the whole point of the short-circuit is the
    # fresh-install case where nothing else is available.
    peek = classify.peek_cache_constraints()
    if not params.reclassify and classify.cache_satisfied(peek):
        log.info(
            "Classify job: every photo has cached classifier runs — "
            "skipping model resolution and detection",
        )
        # Route reused rows through the ordinary finalize path so
        # imported predictions get auto-match reconciliation against
        # destination XMP/taxonomy and burst grouping metadata, which
        # the earlier early-return silently skipped.
        return classify.finalize_cached_only(peek)

    classify.resolve_model()
    classify.load_taxonomy_and_labels()

    cancelled = classify.load_classifier()
    if cancelled is not None:
        return cancelled
    classify.record_loaded_classifier()

    # Classifier init succeeded — now it's safe to start the
    # reclassify purge for any failure before this point would leave
    # the cache intact (see ``_ClassifyJobRun.load_classifier``).
    #
    # Nothing is cleared upfront: ``_detect_subjects`` replaces each
    # photo's detections as its new result lands, and
    # ``_classify_photos`` clears a photo's predictions for this model
    # only when it reaches that photo. A cancel therefore leaves every
    # photo it did not reach with its old state intact.
    #
    # This pre-detection cancel gate still applies: a cancel that
    # landed during taxonomy/label/model init would otherwise advance
    # into the detection loop and start rewriting rows before the
    # user's cancel takes effect.
    if runner.is_cancelled(job["id"]):
        log.info("Classify job cancelled before reclassify purge")
        return classify.cancelled_before_start(
            ["detect", "classify", "finalize"],
        )

    cancelled = classify.detect_subjects()
    if cancelled is not None:
        return cancelled

    if not params.reclassify:
        classify.reapply_local_computation_cache()

    classify.classify_photos()
    return _with_common_names_notice(job, classify.store_predictions())
