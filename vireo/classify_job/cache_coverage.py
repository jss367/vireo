"""Whether cached classifier runs already cover a collection.

``_all_photos_cache_satisfied`` decides when a run can finalize from cached
predictions instead of loading a model; the SQL lives in
``ModelRunsRepository``.
"""




def _all_photos_cache_satisfied(
    db, photo_ids, classifier_model=None, labels_fingerprint=None,
    detector_confidence=0.0,
    model_identity=None, labels_fingerprint_full=None,
    taxonomy_identity="no-tax",
):
    """True when every classifiable detection already has a matching run.

    Used to short-circuit model resolution when a bundle import (or an
    earlier run) has already produced results for the whole collection.
    A fresh install with no downloaded classifier weights can then reuse
    imported predictions instead of failing with "No model available".

    "Satisfied" here means: every photo in the collection has at least
    one detection eligible at the workspace threshold (with synthetic
    full-image anchors always eligible), and every eligible detection has a
    matching ``classifier_runs`` row.  When ``classifier_model`` and/or
    ``labels_fingerprint`` are given, matching also requires the run to
    carry that exact model name / label set — a photo whose detections
    only carry runs for a different model or label set is treated as
    unsatisfied so the caller falls through to real classification with
    the user's requested identity.  A photo with no detections at all is
    also unsatisfied so we still fall through to detection.

    When either filter is unsupplied (fresh-install fallback path or a
    partially resolved request where only one of model/fingerprint is
    known), we additionally require every covered detection to share the
    SAME ``(classifier_model, labels_fingerprint)`` pair.  Otherwise the
    downstream cached-only finalize adopts the first row's identity and
    reconciles every other row against it — silently swapping the
    unbound component of the classifier identity on detections covered
    by a different pair.

    When both ``model_identity`` and ``labels_fingerprint_full`` are
    given, coverage additionally filters by the derived classifier
    runtime.  Without it, an old classifier_runs row (weights or
    preprocessing changed under the same ``classifier_model``) still
    satisfies this join and the caller shortcuts to
    ``_finalize_cached_only`` on stale results — the ordinary
    ``_classify_photos`` gate would reject that mismatch via
    ``_runtime_aware_run_keys``.  ``runtime_fingerprint = 'legacy'``
    stays accepted for pre-portable rows, EXCEPT that an unreviewed
    legacy row for a timm classifier is dropped once
    ``label_descriptions.json`` has transitioned from missing to present:
    that mapping is what turns a raw class id into the stored common
    name, so a pre-heal legacy row's species is stale relative to what
    a run made today would emit (Codex #1560 P2). Manually reviewed rows
    (real accept/reject decisions, not auto-match) remain authoritative
    across runtime changes — including that enrichment change — until an
    explicit reclassify.

    A classifier_run row is only counted as covered when at least one
    matching ``predictions`` row exists.  Local jobs write classifier_runs
    in ``_record_batch_classifier_runs`` *before* ``_store_grouped_predictions``
    persists the corresponding prediction rows; if the job crashes or
    finalization raises in that window, a naive coverage count would still
    call every detection "covered" and the retry would enter
    ``_finalize_cached_only`` and report success without repairing the
    missing prediction rows.
    """
    if not photo_ids:
        return False
    from db import _chunks  # module-level helper, avoids exceeding SQLITE_MAX_VARIABLE_NUMBER

    run_filter = _cache_run_filter(
        db, photo_ids, classifier_model, labels_fingerprint,
        model_identity, labels_fingerprint_full, taxonomy_identity,
    )

    total = 0
    covered = 0
    covered_photos = 0
    distinct_identities = set()
    identity_gate = classifier_model is None or labels_fingerprint is None

    # Chunk ``photo_ids`` so a large collection (per_page=999999) does
    # not blow past SQLite's default SQLITE_MAX_VARIABLE_NUMBER (999)
    # and fail the whole classify job.
    for chunk in _chunks(photo_ids):
        chunk_total, chunk_covered, chunk_photos = (
            db.model_runs.cache_coverage_counts(
                chunk, run_filter, detector_confidence,
            )
        )
        total += chunk_total
        covered += chunk_covered
        covered_photos += chunk_photos

        if identity_gate and _chunk_has_mixed_identities(
            db, chunk, run_filter, detector_confidence, distinct_identities,
        ):
            return False

    if total == 0 or covered != total:
        return False
    return covered_photos == len(photo_ids)


def _cache_run_filter(
    db, photo_ids, classifier_model, labels_fingerprint,
    model_identity, labels_fingerprint_full, taxonomy_identity,
):
    """The classifier_runs filters ``_all_photos_cache_satisfied`` joins on.

    Returns ``(filter_sql, filter_args, runtime_clause, runtime_args)``:
    the requested model / label-set filter and the runtime filter.
    """
    expected_runtimes = _expected_classifier_runtimes(
        db, photo_ids, model_identity, labels_fingerprint_full,
        taxonomy_identity,
    )
    accept_legacy = (
        _accepts_legacy_runtime(model_identity) if expected_runtimes else True
    )
    return db.model_runs.classifier_cache_run_filter(
        classifier_model, labels_fingerprint, expected_runtimes, accept_legacy,
    )


def _expected_classifier_runtimes(
    db, photo_ids, model_identity, labels_fingerprint_full, taxonomy_identity,
):
    """The classifier runtime_fingerprints a current run would carry.

    A classifier runtime encodes (model_identity, labels_full,
    detector_runtime), so each distinct detector runtime in the
    collection produces its own expected classifier runtime.  Empty
    when the caller couldn't resolve either input (fresh install with
    no downloaded weights) — the runtime filter is then omitted and
    we keep the historic model/labels-only behavior.
    """
    from db import _chunks

    expected_runtimes = set()
    if model_identity is not None and labels_fingerprint_full:
        try:
            from computation_cache import classifier_runtime_fingerprint

            detector_runtimes = set()
            for chunk in _chunks(photo_ids):
                for runtime in db.model_runs.detector_runtime_fingerprints(chunk):
                    if runtime:
                        detector_runtimes.add(runtime)
            for dr in detector_runtimes:
                expected = classifier_runtime_fingerprint(
                    model_identity, labels_fingerprint_full, dr,
                    taxonomy_identity=taxonomy_identity,
                )
                if expected:
                    expected_runtimes.add(expected)
        except (OSError, ValueError):
            expected_runtimes = set()
    return expected_runtimes


def _accepts_legacy_runtime(model_identity):
    """Whether ``runtime_fingerprint = 'legacy'`` rows still satisfy the cache.

    Only consulted when the coverage queries filter by expected runtimes.
    """
    # Grandfather ``runtime_fingerprint = 'legacy'`` for pre-portable
    # rows, EXCEPT when the classifier is timm and its
    # ``label_descriptions.json`` enrichment has since transitioned
    # from missing to present. That mapping is what turns a raw
    # class id into the stored common name, so an unreviewed legacy
    # timm row's species is stale relative to what a run made today
    # would emit ("Bubulcus ibis" -> "Western Cattle-Egret"); keeping
    # the exception would leave the scientific names indefinitely
    # unless the user asked for a forced reclassify (Codex #1560 P2).
    # The manual-review branch of the runtime filter
    # (``ModelRunsRepository.classifier_cache_run_filter``) still
    # authorizes such rows — an explicit accept/reject is authoritative
    # across enrichment changes, matching the runtime-change pin exception.
    accept_legacy = True
    if isinstance(model_identity, dict) and (
        model_identity.get("model_type") == "timm"
    ):
        from computation_cache import (
            LABEL_DESCRIPTIONS_IDENTITY_KEY,
            NO_LABEL_DESCRIPTIONS,
        )
        current_ld_identity = model_identity.get(
            LABEL_DESCRIPTIONS_IDENTITY_KEY,
        )
        if (
            current_ld_identity
            and current_ld_identity != NO_LABEL_DESCRIPTIONS
        ):
            accept_legacy = False
    return accept_legacy


def _chunk_has_mixed_identities(
    db, chunk, run_filter, detector_confidence, distinct_identities,
):
    """Add this chunk's covering (model, fp) pairs; True once two are seen.

    Track how many distinct (model, fp) pairs cover this chunk's
    classifier_runs so the caller can't finalize under a single identity
    when several are actually present.  We short-circuit as soon as two
    are seen.  Same predictions-must-exist filter as the coverage query:
    a torn classifier_runs row without predictions is not a real identity
    that can satisfy the cache. ``distinct_identities`` accumulates
    across chunks.
    """
    for identity_row in db.model_runs.covering_classifier_identities(
        chunk, run_filter, detector_confidence,
    ):
        distinct_identities.add(
            (identity_row["m"], identity_row["fp"])
        )
        if len(distinct_identities) > 1:
            return True
    return False
