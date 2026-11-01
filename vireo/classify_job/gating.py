"""Per-detection classification behind the classifier_runs gate."""

from __future__ import annotations

from typing import TYPE_CHECKING

from keyword_normalization import normalize_keyword_display

if TYPE_CHECKING:
    from collections.abc import Callable

    from classify_job.classify_types import Prediction
    from db import Database


def _run_classifier_on_detection(
    db: Database, detection_id: int, classifier_model: str, labels: list[str] | None,
    labels_fingerprint: str, classify_fn: Callable[[], list[Prediction] | None] | None = None,
) -> list[Prediction]:
    """Run the classifier for a single detection and persist results.

    This is a thin adapter that the gate wrapper calls. ``classify_fn`` is an
    injection seam for the higher-level classify/pipeline code that already
    has a loaded model bundle and prepared image — it should return a list of
    prediction dicts that get stored in the ``predictions`` table for this
    (detection, classifier_model, labels_fingerprint) triple.

    Returns the list of prediction dicts that were stored (may be empty).
    """
    if classify_fn is None:
        # No classifier plugged in — return [] without side effects. The
        # gate wrapper treats a zero-prediction return as a failed attempt
        # and does NOT record a classifier_run row, so the next call will
        # retry. Used in tests that just exercise the gating logic without
        # actually running a model.
        return []

    predictions = classify_fn() or []
    # Persist predictions with the new (classifier_model, labels_fingerprint)
    # identity. INSERT OR REPLACE on the UNIQUE
    # (detection_id, classifier_model, labels_fingerprint, species) so a
    # re-classify with reclassify=True refreshes the row in place.
    for pred in predictions:
        species = pred.get("species")
        if not species:
            continue
        # Fold the label's spelling into keyword-storage form before it lands
        # in predictions.species. Several bundled label files carry curly
        # apostrophes (`Bosc’s Fringe-toed lizard`, `Geoffroy’s Tamarin`),
        # and predictions are matched against keywords.name with exact and
        # COLLATE NOCASE compares -- neither of which folds U+2019. Storing
        # the raw label left an accepted `Swinhoe's white-eye` keyword unable
        # to match its own `Swinhoe’s White-eye` prediction. Normalizing here
        # rather than at label-load keeps labels_fingerprint (derived from the
        # raw label file) stable, so this does not invalidate cached
        # classifier runs or trigger a reclassify.
        normalized_species = normalize_keyword_display(species)
        if normalized_species:
            species = normalized_species
            pred["species"] = normalized_species
        confidence = pred.get("confidence") or pred.get("score")
        tax = pred.get("taxonomy") or {}
        db.predictions.replace_row(
            detection_id,
            classifier_model,
            labels_fingerprint,
            species,
            confidence,
            pred.get("category", "new"),
            tax,
            # Carried through so this seam does not silently drop the one
            # signal that distinguishes a match from the closest label in
            # a list that had nothing right in it. The run-level summary
            # is not written here: classify_fn returns whatever it chooses
            # to return, so this path cannot know it saw the whole label
            # list, and guessing would put a fabricated "best over all
            # labels" into the table calibration reads.
            pred.get("raw_score"),
        )
    return predictions


def _classify_detection_gated(db: Database, detection_id: int, classifier_model: str,
                               labels_fingerprint: str, labels: list[str] | None, reclassify: bool,
                               classify_fn: Callable[[], list[Prediction] | None] | None = None) -> list[Prediction]:
    """Run the classifier only if we haven't already for this triple.

    The gate is keyed on (detection_id, classifier_model, labels_fingerprint):
    if a row exists in classifier_runs and reclassify is False, the classifier
    is not invoked. After a successful invocation that produced at least one
    prediction, the classifier_runs row is written (or refreshed) so
    subsequent passes skip.

    Mirrors ``_record_batch_classifier_runs`` and the inline pipeline_job
    guard: a zero-count run is treated as a failed attempt, not a completed
    one. Recording it would permanently strand the detection on the next
    non-reclassify pass — the cache would claim "done" with no rows to show.
    """
    if not reclassify:
        existing = db.model_runs.get_classifier_run_keys(detection_id)
        if (classifier_model, labels_fingerprint) in existing:
            return []
    # One unit: the predictions and the run row that marks them done land
    # together. Inference runs before the first write, so it holds no lock.
    with db.transaction():
        predictions = _run_classifier_on_detection(
            db, detection_id, classifier_model, labels,
            labels_fingerprint=labels_fingerprint,
            classify_fn=classify_fn,
        )
        if predictions:
            db.model_runs.record_classifier_run(
                detection_id, classifier_model, labels_fingerprint,
                prediction_count=len(predictions),
            )
    return predictions
